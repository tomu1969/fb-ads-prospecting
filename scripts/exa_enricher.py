"""Exa Enricher Module: Fast contact discovery via Exa API

Used as Stage 0 in the Contact Enricher Pipeline (Module 3.6).
Runs before expensive GPT-4o agent strategies.

Key features:
- Fast web search via Exa API
- Email extraction from search results
- Hunter.io verification for quality gate
- Early exit if valid email found

Usage:
    from exa_enricher import enrich_with_exa

    result = await enrich_with_exa("Company Name", "https://company.com")
    # Returns: {email, name, position, hunter_status, stage_found, cost}
"""

import os
import re
import requests
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

EXA_API_KEY = os.getenv('EXA_API_KEY')
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')

# Exa API endpoint
EXA_API_URL = "https://api.exa.ai/search"

# Cost tracking
COST_EXA_SEARCH = 0.001  # Estimate per search


def search_exa(query: str, num_results: int = 5) -> list:
    """Search Exa for a query and return results with text content."""
    if not EXA_API_KEY:
        print("    [Exa] No API key configured")
        return []

    try:
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-api-key": EXA_API_KEY
        }

        payload = {
            "query": query,
            "numResults": num_results,
            "type": "auto",
            "contents": {
                "text": {"maxCharacters": 5000}
            }
        }

        response = requests.post(EXA_API_URL, headers=headers, json=payload, timeout=15)

        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            print(f"    [Exa] API error: {response.status_code}")
            return []
    except Exception as e:
        print(f"    [Exa] Search error: {e}")
        return []


def extract_emails_from_text(text: str) -> list:
    """Extract email addresses from text using regex."""
    if not text:
        return []

    # Email regex pattern
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)

    # Filter out common false positives
    filtered = []
    skip_patterns = ['sentry', 'wixpress', 'example.com', 'test.com', 'email.com']

    for email in emails:
        email_lower = email.lower()
        if not any(skip in email_lower for skip in skip_patterns):
            filtered.append(email.lower())

    return list(set(filtered))


def extract_contact_name_from_text(text: str, company_name: str) -> tuple:
    """Try to extract a contact name and position from text."""
    name = ""
    position = ""

    # Look for common patterns like "Contact: John Smith" or "Owner: Jane Doe"
    patterns = [
        r'(?:owner|founder|ceo|president|director|manager|agent|broker|realtor)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+)[,\s]+(?:owner|founder|ceo|president|director|manager|agent|broker|realtor)',
        r'(?:contact|reach|call)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            break

    # Try to find position
    position_patterns = [
        r'(owner|founder|ceo|president|director|manager|real estate agent|broker|realtor)',
    ]
    for pattern in position_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            position = match.group(1).title()
            break

    return name, position


def verify_email_with_hunter(email: str) -> tuple:
    """Verify an email address with Hunter.io."""
    if not HUNTER_API_KEY or not email:
        return None, None

    try:
        resp = requests.get(
            'https://api.hunter.io/v2/email-verifier',
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            return data.get('status'), data.get('score')
        return None, None
    except:
        return None, None


async def enrich_with_exa(company_name: str, website_url: str) -> dict:
    """Search Exa for contact info and verify with Hunter.

    Args:
        company_name: The company/page name to search for
        website_url: The company website (used to filter results)

    Returns:
        dict with: email, name, position, hunter_status, hunter_score,
                   stage_found, source, cost
    """
    result = {
        'email': '',
        'name': '',
        'position': '',
        'phone': '',
        'hunter_status': '',
        'hunter_score': 0,
        'stage_found': 'exa_not_found',
        'source': '',
        'cost': COST_EXA_SEARCH
    }

    # Build search queries
    queries = [
        f'"{company_name}" contact email',
        f'"{company_name}" owner founder email',
    ]

    all_emails = []
    all_text = ""

    for query in queries:
        print(f"    [Exa] Searching: {query[:50]}...")
        results = search_exa(query, num_results=3)

        for r in results:
            text = r.get('text', '')
            url = r.get('url', '')

            # Collect text for name extraction
            all_text += " " + text

            # Extract emails
            emails = extract_emails_from_text(text)
            for email in emails:
                all_emails.append({'email': email, 'source': url})

        # Early exit if we found emails
        if all_emails:
            break

    if not all_emails:
        print(f"    [Exa] No emails found")
        return result

    print(f"    [Exa] Found {len(all_emails)} candidate emails, verifying...")

    # Try to verify emails with Hunter, best first
    for candidate in all_emails[:5]:  # Limit to 5 verifications
        email = candidate['email']
        status, score = verify_email_with_hunter(email)

        result['cost'] += 0.01  # Hunter verification cost

        if status in ['valid', 'accept_all']:
            # Found a valid email!
            name, position = extract_contact_name_from_text(all_text, company_name)

            result['email'] = email
            result['name'] = name
            result['position'] = position
            result['hunter_status'] = status
            result['hunter_score'] = score or 0
            result['stage_found'] = 'exa'
            result['source'] = candidate['source']

            print(f"    [Exa] SUCCESS: {email} ({status})")
            return result
        elif status:
            # Keep best non-valid result as fallback
            if not result['email']:
                result['email'] = email
                result['hunter_status'] = status
                result['hunter_score'] = score or 0
                result['source'] = candidate['source']

    # No valid email found
    if result['email']:
        result['stage_found'] = 'exa_unverified'
        print(f"    [Exa] Found email but not verified: {result['email']}")
    else:
        print(f"    [Exa] No valid emails found")

    return result


# For testing directly
if __name__ == "__main__":
    import asyncio

    async def test():
        # Test with a company
        result = await enrich_with_exa("River Hills Properties", "https://riverhillspropertiesllc.com")
        print("\nResult:")
        for k, v in result.items():
            print(f"  {k}: {v}")

    asyncio.run(test())
