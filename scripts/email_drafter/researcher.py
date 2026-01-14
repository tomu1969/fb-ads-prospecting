"""Researcher module - Multi-source Exa research for prospect personalization.

Gathers information from:
- Company website (hiring notices, team info, offers)
- LinkedIn profile (headline, achievements, recent posts)
- Social media (Instagram, Twitter content)
"""

import os
import re
import requests
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

# Configuration
EXA_API_KEY = os.getenv('EXA_API_KEY')
EXA_API_URL = "https://api.exa.ai/search"


async def search_exa(query: str, num_results: int = 3) -> List[Dict]:
    """
    Execute a search query via Exa API.

    Args:
        query: Search query string
        num_results: Number of results to return

    Returns:
        List of result dicts with 'url' and 'text' keys
    """
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
                "text": {"maxCharacters": 3000}
            }
        }

        response = requests.post(EXA_API_URL, headers=headers, json=payload, timeout=15)

        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            print(f"    [Exa] API error: {response.status_code}")
            return []
    except requests.Timeout:
        print("    [Exa] Request timeout")
        return []
    except Exception as e:
        print(f"    [Exa] Search error: {e}")
        return []


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        return domain.replace('www.', '')
    except:
        return None


# Major real estate franchises - search for agent's personal site instead
FRANCHISE_DOMAINS = {
    'remax.com', 'remax.ca', 'coldwellbanker.com', 'century21.com',
    'kellerwilliams.com', 'kw.com', 'sothebysrealty.com', 'compass.com',
    'realogy.com', 'exp.com', 'exprealty.com', 'berkshirehathaway.com',
    'bhhs.com', 'corcoran.com', 'elliman.com', 'christies.com',
    'onesothebysrealty.com', 'sothebys.com'
}


def is_franchise_domain(domain: str) -> bool:
    """Check if domain belongs to a major franchise."""
    if not domain:
        return False
    domain_lower = domain.lower()
    return any(franchise in domain_lower for franchise in FRANCHISE_DOMAINS)


def extract_findings_from_text(text: str) -> List[str]:
    """
    Extract hook-worthy findings from text.

    Looks for:
    - Hiring mentions
    - Numbers/statistics
    - Achievements
    - Unique phrases
    """
    findings = []

    if not text:
        return findings

    text_lower = text.lower()

    # Hiring signals
    hiring_patterns = [
        r'hiring\s+\d+\s+(?:new\s+)?agents?',
        r'looking\s+for\s+\d+\s+agents?',
        r'join\s+(?:our\s+)?team',
        r'(?:we\s+are|we\'re)\s+hiring',
        r'\d+%?\s+lead\s+surplus',
    ]

    for pattern in hiring_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            findings.append(f"hiring: {match}")

    # Achievement patterns
    achievement_patterns = [
        r'(?:just\s+)?closed\s+(?:my\s+|our\s+)?\d+(?:th|st|nd|rd)?\s+deal',
        r'\$\d+(?:\.\d+)?[MBK]?\s+(?:in\s+)?sales',
        r'top\s+\d+%',
        r'#?\d+\s+(?:team|agent|realtor)',
    ]

    for pattern in achievement_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            findings.append(f"achievement: {match}")

    return findings


async def research_company_website(
    company_name: str,
    website_url: str,
    contact_name: str = None
) -> Dict[str, Any]:
    """
    Research company website for personalization hooks.

    For franchise agents (RE/MAX, Coldwell Banker, etc.), searches for
    the agent's personal website or team page instead of franchise homepage.

    Args:
        company_name: Company name
        website_url: Company website URL
        contact_name: Agent's name (for franchise personalization)

    Returns:
        Dict with 'findings' and 'sources'
    """
    findings = []
    sources = []

    domain = extract_domain(website_url)
    if not domain:
        return {'findings': [], 'sources': []}

    # Check if this is a franchise agent
    if is_franchise_domain(domain) and contact_name:
        # For franchise agents, search for their personal site/team page
        print(f"    [Exa] Detected franchise domain, searching for agent's personal site...")
        queries = [
            f'"{contact_name}" realtor website',           # Agent's personal site
            f'"{contact_name}" "{company_name}" team',     # Agent's team page
            f'site:{domain} "{contact_name}"',             # Profile on franchise site
            f'"{contact_name}" real estate achievements awards',  # Personal achievements
        ]
    else:
        # Standard company website search
        queries = [
            f'site:{domain} hiring join team careers',
            f'site:{domain} about team agents',
            f'"{company_name}" announcement news',
        ]

    for query in queries:
        print(f"    [Exa] Searching: {query[:50]}...")
        results = await search_exa(query, num_results=2)

        for r in results:
            url = r.get('url', '')
            text = r.get('text', '')

            if url:
                sources.append(url)

            # Extract findings from text
            found = extract_findings_from_text(text)
            findings.extend(found)

            # Also store raw text snippets if interesting
            if text and len(text) > 50:
                # Look for quotable phrases
                if any(kw in text.lower() for kw in ['hiring', 'team', 'agent', 'lead', 'surplus', 'award', 'top', 'million']):
                    findings.append(text[:200])

    return {
        'findings': list(set(findings))[:10],  # Dedupe and limit
        'sources': list(set(sources))
    }


async def research_linkedin_profile(
    contact_name: str,
    company_name: str,
    linkedin_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Research prospect's LinkedIn profile.

    Args:
        contact_name: Person's name
        company_name: Company name
        linkedin_url: Direct LinkedIn URL if known

    Returns:
        Dict with 'headline', 'findings', 'sources'
    """
    findings = []
    sources = []
    headline = None

    # Build search query
    if linkedin_url:
        query = f'site:linkedin.com {linkedin_url}'
    else:
        query = f'"{contact_name}" {company_name} linkedin'

    print(f"    [Exa] LinkedIn search: {query[:50]}...")
    results = await search_exa(query, num_results=3)

    for r in results:
        url = r.get('url', '')
        text = r.get('text', '')

        if 'linkedin.com' in url:
            sources.append(url)

            # Try to extract headline (usually first line)
            if text and not headline:
                lines = text.split('\n')
                for line in lines[:3]:
                    if len(line) > 10 and len(line) < 200:
                        headline = line.strip()
                        break

            # Extract achievements
            found = extract_findings_from_text(text)
            findings.extend(found)

    return {
        'headline': headline,
        'findings': list(set(findings))[:5],
        'sources': list(set(sources))
    }


async def research_social_media(
    contact_name: str,
    company_name: Optional[str] = None,
    instagram_handle: Optional[str] = None,
    twitter_handle: Optional[str] = None
) -> Dict[str, Any]:
    """
    Research prospect's social media presence.

    Args:
        contact_name: Person's name
        company_name: Company name
        instagram_handle: Instagram handle if known
        twitter_handle: Twitter handle if known

    Returns:
        Dict with 'instagram', 'twitter', 'sources'
    """
    instagram_findings = []
    twitter_findings = []
    sources = []

    # Instagram research
    if instagram_handle:
        query = f'"{instagram_handle}" OR "@{instagram_handle}" instagram'
        print(f"    [Exa] Instagram search: {query[:50]}...")
        results = await search_exa(query, num_results=2)

        for r in results:
            url = r.get('url', '')
            text = r.get('text', '')
            if 'instagram' in url.lower():
                sources.append(url)
                if text:
                    instagram_findings.append(text[:300])

    # Twitter research
    if company_name:
        query = f'"{contact_name}" {company_name} twitter'
        print(f"    [Exa] Twitter search: {query[:50]}...")
        results = await search_exa(query, num_results=2)

        for r in results:
            url = r.get('url', '')
            text = r.get('text', '')
            if 'twitter' in url.lower() or 'x.com' in url.lower():
                sources.append(url)
                if text:
                    twitter_findings.append(text[:300])

    return {
        'instagram': instagram_findings[:3],
        'twitter': twitter_findings[:3],
        'sources': list(set(sources))
    }


async def research_prospect(
    contact_name: str,
    company_name: str,
    website_url: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    instagram_handle: Optional[str] = None,
    twitter_handle: Optional[str] = None,
    ad_texts: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Aggregate research from all sources for a prospect.

    This is the main entry point for the researcher module.

    Args:
        contact_name: Prospect's name
        company_name: Company name
        website_url: Company website
        linkedin_url: LinkedIn profile URL
        instagram_handle: Instagram handle
        twitter_handle: Twitter handle
        ad_texts: Ad content from CSV (passed through)

    Returns:
        Aggregated research dict with 'company', 'personal', 'ad_content', 'sources'
    """
    print(f"\n  Researching: {contact_name} @ {company_name}")

    all_sources = []

    # Company website research
    company_data = {'website_findings': [], 'recent_news': []}
    if website_url:
        web_result = await research_company_website(company_name, website_url, contact_name)
        company_data['website_findings'] = web_result.get('findings', [])
        all_sources.extend(web_result.get('sources', []))

    # LinkedIn research
    linkedin_data = await research_linkedin_profile(contact_name, company_name, linkedin_url)
    all_sources.extend(linkedin_data.get('sources', []))

    # Social media research
    social_data = await research_social_media(
        contact_name,
        company_name,
        instagram_handle,
        twitter_handle
    )
    all_sources.extend(social_data.get('sources', []))

    return {
        'company': company_data,
        'personal': {
            'linkedin': {
                'headline': linkedin_data.get('headline'),
                'findings': linkedin_data.get('findings', []),
            },
            'social_media': {
                'instagram': social_data.get('instagram', []),
                'twitter': social_data.get('twitter', []),
            }
        },
        'ad_content': ad_texts or [],
        'sources': list(set(all_sources))
    }


# For direct testing
if __name__ == "__main__":
    import asyncio

    async def test():
        result = await research_prospect(
            contact_name="Test Agent",
            company_name="Test Realty",
            website_url="https://example.com"
        )
        print("\nResult:")
        import json
        print(json.dumps(result, indent=2))

    asyncio.run(test())
