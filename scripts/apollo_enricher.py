"""Apollo Enricher Module - B2B contact discovery via Apollo.io API.

Part of the waterfall enrichment strategy:
Hunter (free) -> Exa ($0.001) -> Apollo ($0.02-0.03) -> AI Agents ($0.03-0.08)

Apollo.io provides access to 275M+ B2B contacts with verified emails,
offering higher coverage than web scraping alone.

Usage:
    from scripts.apollo_enricher import enrich_with_apollo, search_apollo_alternatives

    # For main pipeline
    result = enrich_with_apollo("Company Name", "https://company.com")

    # For bounce recovery
    alternatives = search_apollo_alternatives("company.com", exclude_email="bounced@company.com")
"""

import os
import logging
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# Apollo API configuration
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"

# Hunter.io for verification
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
HUNTER_VERIFY_URL = "https://api.hunter.io/v2/email-verifier"

# Cost tracking
COST_APOLLO_SEARCH = 0.01  # Estimate per search
COST_APOLLO_ENRICH = 0.02  # Estimate per bulk_match (reveals emails)

# Logging
logger = logging.getLogger(__name__)


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract domain from URL, removing www prefix."""
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        if domain.startswith('www.'):
            domain = domain[4:]
        # Remove any path components
        domain = domain.split('/')[0]
        return domain.lower() if domain else None
    except Exception:
        return None


def verify_with_hunter(email: str) -> Dict[str, Any]:
    """Verify email using Hunter.io API.

    Returns:
        dict with 'status' (valid/invalid/accept_all/unknown) and 'score' (0-100)
    """
    if not HUNTER_API_KEY or not email:
        return {"status": "unknown", "score": 0}

    try:
        resp = requests.get(
            HUNTER_VERIFY_URL,
            params={"email": email, "api_key": HUNTER_API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "status": data.get("status", "unknown"),
                "score": data.get("score", 0)
            }
        else:
            logger.warning(f"Hunter verification failed: {resp.status_code}")
            return {"status": "unknown", "score": 0}
    except Exception as e:
        logger.error(f"Hunter verification error: {e}")
        return {"status": "unknown", "score": 0}


def search_apollo(domain: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Search Apollo.io for contacts at a domain and reveal their emails.

    Uses two API calls per contact:
    1. mixed_people/api_search - Find people at domain (get IDs)
    2. people/match - Reveal email using person ID (uses 1 credit each)

    Args:
        domain: Company domain to search
        api_key: Apollo API key (defaults to env var)

    Returns:
        dict with 'emails' (list) and 'contacts' (list of dicts)
    """
    key = api_key or APOLLO_API_KEY

    if not domain or not key:
        return {"emails": [], "contacts": []}

    try:
        # Step 1: Search for people at the domain
        logger.debug(f"[Apollo] Searching for contacts at {domain}")

        search_response = requests.post(
            APOLLO_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": key
            },
            json={
                "q_organization_domains": domain,
                "per_page": 5  # Limit to conserve credits
            },
            timeout=10
        )

        if not search_response.ok:
            logger.warning(f"[Apollo] Search error for {domain}: {search_response.status_code}")
            return {"emails": [], "contacts": []}

        search_data = search_response.json()
        people = search_data.get("people", [])

        if not people:
            logger.debug(f"[Apollo] No people found at {domain}")
            return {"emails": [], "contacts": []}

        # Filter to people with emails
        people_with_email = [p for p in people if p.get("has_email")]

        if not people_with_email:
            logger.debug(f"[Apollo] No people with emails at {domain}")
            return {"emails": [], "contacts": []}

        # Step 2: Reveal emails using people/match with ID (1 credit per person)
        logger.debug(f"[Apollo] Revealing emails for {len(people_with_email)} contacts")

        emails = []
        contacts = []

        # Limit to 3 to conserve credits
        for person in people_with_email[:3]:
            person_id = person.get("id")
            if not person_id:
                continue

            try:
                match_response = requests.post(
                    APOLLO_MATCH_URL,
                    headers={
                        "Content-Type": "application/json",
                        "X-Api-Key": key
                    },
                    json={
                        "id": person_id,
                        "reveal_personal_emails": False
                    },
                    timeout=10
                )

                if match_response.ok:
                    match_data = match_response.json()
                    matched_person = match_data.get("person", {})

                    if matched_person and matched_person.get("email"):
                        email = matched_person.get("email")
                        emails.append(email)
                        contacts.append({
                            "name": matched_person.get("name", ""),
                            "title": matched_person.get("title", ""),
                            "email": email,
                            "linkedin_url": matched_person.get("linkedin_url", "")
                        })
                        logger.debug(f"[Apollo] Found: {email}")
            except Exception as e:
                logger.warning(f"[Apollo] Match error for {person_id}: {e}")
                continue

        logger.info(f"[Apollo] Found {len(emails)} emails at {domain}")
        return {"emails": emails, "contacts": contacts}

    except requests.exceptions.Timeout:
        logger.error(f"[Apollo] Timeout for {domain}")
        return {"emails": [], "contacts": []}
    except requests.exceptions.RequestException as e:
        logger.error(f"[Apollo] Request error for {domain}: {e}")
        return {"emails": [], "contacts": []}
    except Exception as e:
        logger.error(f"[Apollo] Unexpected error for {domain}: {e}")
        return {"emails": [], "contacts": []}


def enrich_with_apollo(
    company_name: str,
    website_url: str,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """Enrich a contact using Apollo.io with Hunter verification.

    This is the main function for the enrichment pipeline.

    Args:
        company_name: Name of the company
        website_url: Company website URL
        api_key: Optional Apollo API key

    Returns:
        dict with email, name, position, hunter_status, hunter_score, stage_found, cost
    """
    result = {
        "email": None,
        "name": None,
        "position": None,
        "phone": None,
        "hunter_status": None,
        "hunter_score": 0,
        "stage_found": "apollo",
        "source": "apollo.io",
        "cost": COST_APOLLO_SEARCH
    }

    # Extract domain from URL
    domain = extract_domain(website_url)
    if not domain:
        logger.warning(f"[Apollo] Could not extract domain from {website_url}")
        return result

    # Search Apollo for contacts
    apollo_result = search_apollo(domain, api_key)

    if not apollo_result["emails"]:
        return result

    # Add enrich cost if we found people
    result["cost"] += COST_APOLLO_ENRICH

    # Verify each email with Hunter and pick the best one
    best_email = None
    best_contact = None
    best_status = None
    best_score = -1

    for i, email in enumerate(apollo_result["emails"]):
        contact = apollo_result["contacts"][i] if i < len(apollo_result["contacts"]) else {}

        verification = verify_with_hunter(email)
        status = verification.get("status", "unknown")
        score = verification.get("score", 0)

        logger.debug(f"[Apollo] {email}: Hunter status={status}, score={score}")

        # Prioritize: valid > accept_all > unknown > invalid
        status_priority = {"valid": 4, "accept_all": 3, "unknown": 2, "invalid": 1}
        current_priority = status_priority.get(status, 0)
        best_priority = status_priority.get(best_status, 0)

        # Update best if this is better
        if current_priority > best_priority or (current_priority == best_priority and score > best_score):
            best_email = email
            best_contact = contact
            best_status = status
            best_score = score

        # Early exit if we found a valid email
        if status == "valid":
            break

    # Populate result with best match
    if best_email:
        result["email"] = best_email
        result["hunter_status"] = best_status
        result["hunter_score"] = best_score

        if best_contact:
            result["name"] = best_contact.get("name")
            result["position"] = best_contact.get("title")

    return result


def search_apollo_alternatives(
    domain: str,
    exclude_email: Optional[str] = None,
    api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Search Apollo for alternative contacts at a domain.

    Used for bounce recovery - finds other contacts when original email bounced.

    Args:
        domain: Company domain to search
        exclude_email: Email to exclude (the bounced one)
        api_key: Optional Apollo API key

    Returns:
        List of contact dicts with email, name, title, confidence
    """
    apollo_result = search_apollo(domain, api_key)

    if not apollo_result["contacts"]:
        return []

    # Filter out the excluded email
    alternatives = []
    for contact in apollo_result["contacts"]:
        email = contact.get("email", "")
        if exclude_email and email.lower() == exclude_email.lower():
            continue

        alternatives.append({
            "email": email,
            "name": contact.get("name", ""),
            "title": contact.get("title", ""),
            "linkedin_url": contact.get("linkedin_url", ""),
            "confidence": 85,  # Apollo doesn't provide confidence, assume high
            "source": "apollo"
        })

    return alternatives


if __name__ == "__main__":
    """Test Apollo enricher with sample domains."""
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set in environment")
        sys.exit(1)

    test_cases = [
        ("HubSpot", "https://www.hubspot.com"),
        ("Salesforce", "https://www.salesforce.com"),
    ]

    for company, url in test_cases:
        print(f"\n{'='*50}")
        print(f"Testing: {company} ({url})")
        print('='*50)

        result = enrich_with_apollo(company, url)

        print(f"Email: {result['email']}")
        print(f"Name: {result['name']}")
        print(f"Position: {result['position']}")
        print(f"Hunter Status: {result['hunter_status']}")
        print(f"Hunter Score: {result['hunter_score']}")
        print(f"Cost: ${result['cost']:.3f}")
