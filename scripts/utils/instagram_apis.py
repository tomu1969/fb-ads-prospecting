"""Paid Instagram API integrations for fast handle lookup."""

import os
from typing import List, Optional

# Try to import Apify client, but make it optional
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    ApifyClient = None


def get_apify_client():
    """Get Apify client if available, otherwise return None."""
    if not APIFY_AVAILABLE:
        return None
    
    api_token = os.getenv('APIFY_API_TOKEN')
    if not api_token:
        return None
    
    try:
        return ApifyClient(api_token)
    except Exception:
        return None


async def search_apify_instagram(company_name: str, website_url: str = "") -> List[str]:
    """
    Search for Instagram handles using Apify Instagram Scraper.
    
    Args:
        company_name: Company name to search for
        website_url: Company website URL (optional)
        
    Returns:
        List of Instagram handles found
    """
    if not os.getenv('USE_PAID_INSTAGRAM_API', 'false').lower() == 'true':
        return []
    
    client = get_apify_client()
    if not client:
        return []
    
    try:
        # Use Apify's Instagram Profile Scraper
        # Note: This is a placeholder - actual implementation depends on Apify actor
        # You would need to configure the specific actor ID and input format
        
        # Example actor: apify/instagram-scraper
        # Input format: {"usernames": ["username1", "username2"]}
        
        # For now, return empty list - user needs to configure their Apify actor
        # This is a template for future integration
        return []
    except Exception:
        return []


def is_paid_api_enabled() -> bool:
    """Check if paid Instagram API is enabled."""
    return os.getenv('USE_PAID_INSTAGRAM_API', 'false').lower() == 'true' and get_apify_client() is not None

