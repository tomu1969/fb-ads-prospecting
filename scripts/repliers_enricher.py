"""
Repliers Agent Enricher - Enrich real estate agents with Tech Stack, Meta Ads & Instagram

Enriches agents from the Repliers MLS data with:
1. Personal Website - Agent's own site (NOT brokerage franchise pages)
2. Tech Stack - CRM, pixels, scheduling tools, chat widgets via Apify BuiltWith
3. Meta Ads - Whether they run Facebook/Instagram ads
4. Instagram Handle - Agent's Instagram profile

Input: output/repliers/top_agents_2025_full.csv
Output: output/repliers/top_agents_2025_enriched.csv

Usage:
    python scripts/repliers_enricher.py --input output/repliers/top_agents_2025_full.csv --all
    python scripts/repliers_enricher.py --input output/repliers/top_agents_2025_full.csv --limit 5
    python scripts/repliers_enricher.py --dry-run
    python scripts/repliers_enricher.py --websites-only
    python scripts/repliers_enricher.py --techstack-only
    python scripts/repliers_enricher.py --metaads-only
    python scripts/repliers_enricher.py --instagram-only
"""

import os
import sys
import json
import time
import argparse
import logging
import re
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Try to import Groq client for LLM-based Instagram discovery
try:
    from groq import Groq
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')
    groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
except ImportError:
    groq_client = None
    logger.debug("groq not installed. LLM Instagram search will be skipped.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/repliers/top_agents_2025_full.csv"
DEFAULT_OUTPUT = "output/repliers/top_agents_2025_enriched.csv"

# Apify configuration
APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')

# Apify actors
BUILTWITH_ACTOR = "datavoyantlab/builtwith-bulk-scraper"  # Tech stack detection
FB_ADS_ACTOR = "curious_coder/facebook-ads-library-scraper"  # Meta ads
INSTAGRAM_ACTOR = "apify/instagram-search-scraper"  # Instagram search

# Try to import Apify client
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    ApifyClient = None
    logger.warning("apify-client not installed. Install with: pip install apify-client")


# =============================================================================
# FRANCHISE DOMAINS TO SKIP
# =============================================================================

FRANCHISE_DOMAINS = {
    # Major real estate brokerages (franchise sites, not personal)
    'elliman.com', 'compass.com', 'onesothebysrealty.com', 'bhhs.com',
    'coldwellbanker.com', 'coldwellbankerhomes.com', 'remax.com', 'kw.com',
    'keyes.com', 'floridamoves.com', 'corcoran.com', 'sothebysrealty.com',
    'century21.com', 'exp.com', 'exprealty.com', 'bfrg.com', 'realogy.com',
    'serhant.com', 'berkshirehathawayhs.com', 'realtyone.com', 'kwrealty.com',
    'movoto.com', 'opendoor.com', 'redfin.com', 'zillow.com', 'realtor.com',
    'cbmiami.com', 'ewm.com', 'onesothebys.com', 'faustocommercial.com',

    # Generic email providers
    'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'aol.com',
    'icloud.com', 'me.com', 'msn.com', 'live.com', 'mail.com',
    'comcast.net', 'att.net', 'verizon.net', 'bellsouth.net',
}


# =============================================================================
# TECH STACK SIGNATURES (from tech_stack_enricher.py)
# =============================================================================

TECH_SIGNATURES = {
    # CRMs
    "hubspot": {"patterns": [r"js\.hs-scripts\.com", r"js\.hsforms\.net", r"hbspt\.forms", r"hubspot\.com"], "category": "crm", "display_name": "HubSpot"},
    "salesforce": {"patterns": [r"salesforce\.com", r"force\.com", r"pardot\.com"], "category": "crm", "display_name": "Salesforce"},
    "followupboss": {"patterns": [r"followupboss\.com", r"fub\.com"], "category": "crm", "display_name": "Follow Up Boss"},
    "kvcore": {"patterns": [r"kvcore\.com", r"platform\.kvcore", r"idx\.kvcore"], "category": "crm", "display_name": "kvCORE"},
    "liondesk": {"patterns": [r"liondesk\.com"], "category": "crm", "display_name": "LionDesk"},
    "cinc": {"patterns": [r"cincpro\.com", r"cinc\.com"], "category": "crm", "display_name": "CINC"},
    "boomtown": {"patterns": [r"boomtownroi\.com"], "category": "crm", "display_name": "BoomTown"},
    "realgeeks": {"patterns": [r"realgeeks\.com"], "category": "crm", "display_name": "Real Geeks"},
    "chime": {"patterns": [r"chime\.me", r"chimecentral\.com"], "category": "crm", "display_name": "Chime"},
    "zoho": {"patterns": [r"zoho\.com", r"zohocdn\.com"], "category": "crm", "display_name": "Zoho CRM"},
    "pipedrive": {"patterns": [r"pipedrive\.com"], "category": "crm", "display_name": "Pipedrive"},

    # Marketing Pixels
    "meta_pixel": {"patterns": [r"connect\.facebook\.net", r"fbq\s*\(", r"facebook\.com/tr"], "category": "pixel", "display_name": "Meta Pixel"},
    "google_analytics": {"patterns": [r"google-analytics\.com", r"googletagmanager\.com", r"gtag\s*\("], "category": "pixel", "display_name": "Google Analytics"},
    "google_ads": {"patterns": [r"googleadservices\.com", r"google_conversion"], "category": "pixel", "display_name": "Google Ads"},
    "tiktok_pixel": {"patterns": [r"analytics\.tiktok\.com", r"ttq\.track"], "category": "pixel", "display_name": "TikTok Pixel"},
    "linkedin_insight": {"patterns": [r"snap\.licdn\.com", r"linkedin\.com/px"], "category": "pixel", "display_name": "LinkedIn Insight"},
    "hotjar": {"patterns": [r"static\.hotjar\.com", r"hotjar\.com"], "category": "pixel", "display_name": "Hotjar"},

    # Scheduling Tools
    "calendly": {"patterns": [r"calendly\.com", r"assets\.calendly\.com"], "category": "scheduling", "display_name": "Calendly"},
    "acuity": {"patterns": [r"acuityscheduling\.com"], "category": "scheduling", "display_name": "Acuity Scheduling"},
    "hubspot_meetings": {"patterns": [r"meetings\.hubspot\.com"], "category": "scheduling", "display_name": "HubSpot Meetings"},

    # Chat Widgets
    "intercom": {"patterns": [r"intercom\.io", r"widget\.intercom\.io"], "category": "chat", "display_name": "Intercom"},
    "drift": {"patterns": [r"drift\.com", r"js\.driftt\.com"], "category": "chat", "display_name": "Drift"},
    "livechat": {"patterns": [r"livechatinc\.com"], "category": "chat", "display_name": "LiveChat"},
    "tawk": {"patterns": [r"tawk\.to"], "category": "chat", "display_name": "Tawk.to"},
    "zendesk_chat": {"patterns": [r"zopim\.com", r"zendesk\.com.*chat"], "category": "chat", "display_name": "Zendesk Chat"},
    "crisp": {"patterns": [r"crisp\.chat"], "category": "chat", "display_name": "Crisp"},

    # IDX/MLS (Real Estate Specific)
    "idx_broker": {"patterns": [r"idxbroker\.com", r"idx-broker"], "category": "idx", "display_name": "IDX Broker"},
    "showcase_idx": {"patterns": [r"showcaseidx\.com"], "category": "idx", "display_name": "Showcase IDX"},
    "ihomefinder": {"patterns": [r"ihomefinder\.com"], "category": "idx", "display_name": "iHomefinder"},
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_apify_client() -> Optional[Any]:
    """Get Apify client if available."""
    if not APIFY_AVAILABLE:
        logger.warning("Apify client not installed")
        return None
    if not APIFY_API_TOKEN:
        logger.warning("APIFY_API_TOKEN not found in environment")
        return None
    try:
        return ApifyClient(APIFY_API_TOKEN)
    except Exception as e:
        logger.error(f"Failed to create Apify client: {e}")
        return None


def extract_domain_from_email(email: str) -> tuple[Optional[str], str]:
    """
    Extract personal website domain from email address.

    Returns:
        (domain, source) where source is 'personal_domain', 'skipped_franchise',
        'skipped_generic', or 'invalid_email'
    """
    if not email or pd.isna(email):
        return None, "invalid_email"

    email = str(email).strip().lower()

    if '@' not in email:
        return None, "invalid_email"

    try:
        domain = email.split('@')[1].strip()
    except IndexError:
        return None, "invalid_email"

    if not domain or '.' not in domain:
        return None, "invalid_email"

    # Check if it's a franchise/brokerage domain
    if domain in FRANCHISE_DOMAINS:
        return None, "skipped_franchise"

    # Check for partial matches (e.g., subdomain.elliman.com)
    for franchise in FRANCHISE_DOMAINS:
        if domain.endswith('.' + franchise):
            return None, "skipped_franchise"

    return domain, "personal_domain"


# =============================================================================
# STEP 1: WEBSITE DISCOVERY
# =============================================================================

def enrich_websites(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract personal website URLs from email domains.
    Skips franchise/brokerage domains.
    """
    logger.info("Step 1: Website Discovery from Email Domains")

    # Initialize columns
    if 'website_url' not in df.columns:
        df['website_url'] = ""
    if 'website_source' not in df.columns:
        df['website_source'] = ""

    stats = {
        "personal_domain": 0,
        "skipped_franchise": 0,
        "skipped_generic": 0,
        "invalid_email": 0,
        "already_had": 0,
    }

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Extracting websites"):
        # Skip if already has website (check for non-empty, non-NaN)
        existing_url = row.get('website_url')
        if pd.notna(existing_url) and str(existing_url).strip() and str(existing_url).strip().lower() != 'nan':
            stats["already_had"] += 1
            continue

        email = row.get('agent_email', '')
        domain, source = extract_domain_from_email(email)

        if domain:
            df.at[idx, 'website_url'] = f"https://{domain}"
            df.at[idx, 'website_source'] = source
            stats["personal_domain"] += 1
        else:
            df.at[idx, 'website_source'] = source
            stats[source] = stats.get(source, 0) + 1

    # Print stats
    print(f"\nWebsite Discovery Results:")
    print(f"  Personal domains found: {stats['personal_domain']}")
    print(f"  Skipped (franchise): {stats['skipped_franchise']}")
    print(f"  Invalid/no email: {stats['invalid_email']}")
    print(f"  Already had website: {stats['already_had']}")

    return df


# =============================================================================
# STEP 2: TECH STACK DETECTION VIA APIFY BUILTWITH
# =============================================================================

def detect_tech_from_html(html: str) -> List[Dict]:
    """Detect technologies from HTML content using regex patterns."""
    if not html:
        return []

    detected = []
    html_lower = html.lower()

    for tech_id, config in TECH_SIGNATURES.items():
        for pattern in config["patterns"]:
            try:
                if re.search(pattern, html_lower, re.IGNORECASE):
                    detected.append({
                        "id": tech_id,
                        "name": config["display_name"],
                        "category": config["category"],
                    })
                    break
            except re.error:
                pass

    return detected


def run_builtwith_apify(urls: List[str], client: Any) -> Dict[str, Dict]:
    """
    Run BuiltWith actor on Apify to detect tech stacks.

    Returns dict mapping URL to tech stack data.
    """
    if not urls:
        return {}

    logger.info(f"Running BuiltWith actor for {len(urls)} URLs...")

    # Extract domains from URLs
    domains = []
    for url in urls:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            if domain.startswith('www.'):
                domain = domain[4:]
            domains.append(domain)
        except Exception:
            domains.append(url.replace('https://', '').replace('http://', '').replace('www.', ''))

    try:
        run_input = {
            "domains": domains,
        }

        run = client.actor(BUILTWITH_ACTOR).call(run_input=run_input, timeout_secs=300)

        results = {}
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # The actor returns domain, not url
            domain = item.get("domain", "")
            url = f"https://{domain}"

            # Extract technologies from various possible structures
            all_tech_names = []

            # Primary structure: "technologies" is a dict with category keys and tech lists
            # e.g., {"Analytics and Tracking": ["Hotjar", "Google Analytics"], "Widgets": [...]}
            technologies = item.get("technologies", {})
            if isinstance(technologies, dict):
                for cat_name, tech_list in technologies.items():
                    if isinstance(tech_list, list):
                        for tech in tech_list:
                            if isinstance(tech, str):
                                all_tech_names.append(tech.lower())
                            elif isinstance(tech, dict):
                                name = tech.get("name") or tech.get("technology")
                                if name:
                                    all_tech_names.append(str(name).lower())
            elif isinstance(technologies, list):
                # Fallback: technologies is a flat list
                for tech in technologies:
                    if isinstance(tech, str):
                        all_tech_names.append(tech.lower())
                    elif isinstance(tech, dict):
                        name = tech.get("name") or tech.get("technology")
                        if name:
                            all_tech_names.append(str(name).lower())

            logger.debug(f"Found {len(all_tech_names)} technologies for {domain}")

            # Parse technologies into our format
            tech_data = {
                "has_crm": False,
                "crm_name": "",
                "has_marketing_pixel": False,
                "pixel_types": [],
                "has_scheduling_tool": False,
                "scheduling_tool": "",
                "has_chat_widget": False,
                "chat_widget": "",
                "has_idx": False,
                "idx_provider": "",
                "tech_stack_raw": all_tech_names,
                "tech_count": len(all_tech_names),
            }

            # Categorize technologies
            for tech_name in all_tech_names:
                # Check CRM
                for crm_key in ["hubspot", "salesforce", "followupboss", "kvcore", "liondesk", "cinc", "boomtown", "realgeeks", "chime", "zoho", "pipedrive"]:
                    if crm_key in tech_name:
                        tech_data["has_crm"] = True
                        tech_data["crm_name"] = tech_name.title()
                        break

                # Check Pixels
                for pixel in ["facebook", "meta pixel", "google analytics", "google tag", "tiktok", "linkedin", "hotjar"]:
                    if pixel in tech_name:
                        tech_data["has_marketing_pixel"] = True
                        if tech_name.title() not in tech_data["pixel_types"]:
                            tech_data["pixel_types"].append(tech_name.title())

                # Check Scheduling
                for sched in ["calendly", "acuity", "scheduling"]:
                    if sched in tech_name:
                        tech_data["has_scheduling_tool"] = True
                        tech_data["scheduling_tool"] = tech_name.title()

                # Check Chat
                for chat in ["intercom", "drift", "livechat", "tawk", "zendesk", "crisp"]:
                    if chat in tech_name:
                        tech_data["has_chat_widget"] = True
                        tech_data["chat_widget"] = tech_name.title()

                # Check IDX
                for idx in ["idx", "mls", "ihomefinder", "showcase"]:
                    if idx in tech_name:
                        tech_data["has_idx"] = True
                        tech_data["idx_provider"] = tech_name.title()

            results[url] = tech_data
            logger.debug(f"Tech stack for {domain}: {tech_data['tech_count']} technologies found")

        return results

    except Exception as e:
        logger.error(f"BuiltWith actor error: {e}")
        return {}


def enrich_techstack(df: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
    """
    Enrich agents with tech stack data via Apify BuiltWith.
    """
    logger.info("Step 2: Tech Stack Detection via Apify BuiltWith")

    # Initialize columns
    tech_columns = [
        "has_crm", "crm_name", "has_marketing_pixel", "pixel_types",
        "has_scheduling_tool", "scheduling_tool", "has_chat_widget",
        "chat_widget", "has_idx", "idx_provider", "tech_stack_raw", "tech_count"
    ]
    for col in tech_columns:
        if col not in df.columns:
            if col.startswith("has_"):
                df[col] = False
            elif col == "tech_count":
                df[col] = 0
            elif col in ["pixel_types", "tech_stack_raw"]:
                df[col] = "[]"
            else:
                df[col] = ""

    # Get URLs to process (only personal websites)
    urls_to_process = []
    idx_to_url = {}

    for idx, row in df.iterrows():
        website_url = str(row.get('website_url', '')).strip()
        if website_url and website_url.startswith('http') and row.get('website_source') == 'personal_domain':
            # Skip if already has tech stack data
            if row.get('tech_count', 0) > 0:
                continue
            urls_to_process.append(website_url)
            idx_to_url[website_url] = idx

    logger.info(f"Found {len(urls_to_process)} personal websites to analyze")

    if dry_run:
        print(f"\n[DRY RUN] Would analyze tech stack for {len(urls_to_process)} websites")
        for url in urls_to_process[:10]:
            print(f"  - {url}")
        if len(urls_to_process) > 10:
            print(f"  ... and {len(urls_to_process) - 10} more")
        return df

    if not urls_to_process:
        print("No websites to process for tech stack")
        return df

    # Get Apify client
    client = get_apify_client()
    if not client:
        logger.error("Cannot run tech stack enrichment without Apify client")
        return df

    # Process in batches to avoid timeout
    BATCH_SIZE = 50
    all_results = {}

    for i in range(0, len(urls_to_process), BATCH_SIZE):
        batch = urls_to_process[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i // BATCH_SIZE + 1} ({len(batch)} URLs)")

        batch_results = run_builtwith_apify(batch, client)
        all_results.update(batch_results)

        if i + BATCH_SIZE < len(urls_to_process):
            time.sleep(2)  # Brief pause between batches

    # Update DataFrame
    stats = {"found_tech": 0, "no_tech": 0}

    for url, tech_data in all_results.items():
        if url in idx_to_url:
            idx = idx_to_url[url]
            for key, value in tech_data.items():
                if key == "pixel_types" or key == "tech_stack_raw":
                    df.at[idx, key] = json.dumps(value) if isinstance(value, list) else value
                else:
                    df.at[idx, key] = value

            if tech_data.get("tech_count", 0) > 0:
                stats["found_tech"] += 1
            else:
                stats["no_tech"] += 1

    print(f"\nTech Stack Results:")
    print(f"  Websites with tech detected: {stats['found_tech']}")
    print(f"  Websites with no tech: {stats['no_tech']}")

    return df


# =============================================================================
# STEP 3: META ADS DETECTION
# =============================================================================

def check_meta_ads_apify(client: Any, agent_name: str, city: str = "", timeout: int = 60) -> Dict:
    """
    Check if an agent runs Meta ads via Apify FB Ads Library scraper.

    Returns dict with:
        - has_meta_ads: bool
        - meta_ad_count: int
        - meta_page_names: list of page names
        - meta_ad_confidence: 'high', 'medium', 'low'
    """
    result = {
        "has_meta_ads": False,
        "meta_ad_count": 0,
        "meta_page_names": [],
        "meta_ad_confidence": "",
    }

    try:
        # Build search query with city for disambiguation
        search_query = agent_name
        if city:
            # Extract first city if multiple
            first_city = city.split(',')[0].strip()
            search_query = f"{agent_name} {first_city}"

        # Build Facebook Ad Library URL
        base_url = 'https://www.facebook.com/ads/library/'
        params = [
            'active_status=active',
            'ad_type=all',
            'country=US',
            'search_type=keyword_unordered',
            'media_type=all',
            f'q={quote_plus(search_query)}',
        ]
        search_url = base_url + '?' + '&'.join(params)

        run_input = {
            'urls': [{'url': search_url}],
            'maxAds': 10,
        }

        run = client.actor(FB_ADS_ACTOR).call(run_input=run_input, timeout_secs=timeout)

        status = run.get('status')
        if status in ['SUCCEEDED', 'TIMED-OUT']:
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                result['meta_ad_count'] = len(items)
                result['has_meta_ads'] = len(items) > 0

                # Extract page names and check for matches
                page_names = set()
                name_parts = [p.lower() for p in agent_name.split() if len(p) > 2]
                matches = 0

                for item in items:
                    page_name = item.get('pageName') or item.get('page_name') or item.get('advertiserName') or ''
                    if page_name:
                        page_names.add(page_name)
                        page_lower = page_name.lower()
                        # Count how many name parts appear in page name
                        name_matches = sum(1 for part in name_parts if part in page_lower)
                        if name_matches >= 2 or (len(name_parts) == 1 and name_matches == 1):
                            matches += 1

                result['meta_page_names'] = list(page_names)

                # Determine confidence
                if matches >= 2:
                    result['meta_ad_confidence'] = 'high'
                elif matches == 1:
                    result['meta_ad_confidence'] = 'medium'
                elif result['has_meta_ads']:
                    result['meta_ad_confidence'] = 'low'

    except Exception as e:
        logger.debug(f"Meta ads check error for {agent_name}: {e}")

    return result


def enrich_metaads(df: pd.DataFrame, dry_run: bool = False, delay: float = 1.0) -> pd.DataFrame:
    """
    Enrich agents with Meta ads data via Apify FB Ads Library.
    """
    logger.info("Step 3: Meta Ads Detection via Apify FB Ads Library")

    # Initialize columns
    meta_columns = ["has_meta_ads", "meta_ad_count", "meta_page_names", "meta_ad_confidence"]
    for col in meta_columns:
        if col not in df.columns:
            if col.startswith("has_"):
                df[col] = False
            elif col == "meta_ad_count":
                df[col] = 0
            else:
                df[col] = ""

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has meta ads data
        if row.get('meta_ad_count', 0) > 0 or row.get('has_meta_ads') == True:
            continue

        agent_name = str(row.get('agent_name', '')).strip()
        if agent_name:
            rows_to_process.append((idx, agent_name, row.get('cities', '')))

    logger.info(f"Found {len(rows_to_process)} agents to check for Meta ads")

    if dry_run:
        print(f"\n[DRY RUN] Would check Meta ads for {len(rows_to_process)} agents")
        for idx, name, cities in rows_to_process[:10]:
            city = cities.split(',')[0].strip() if cities else ''
            print(f"  - {name} ({city})")
        if len(rows_to_process) > 10:
            print(f"  ... and {len(rows_to_process) - 10} more")
        return df

    if not rows_to_process:
        print("No agents to process for Meta ads")
        return df

    # Get Apify client
    client = get_apify_client()
    if not client:
        logger.error("Cannot run Meta ads detection without Apify client")
        return df

    # Process each agent
    stats = {"with_ads": 0, "no_ads": 0, "errors": 0, "high_confidence": 0}

    for idx, agent_name, cities in tqdm(rows_to_process, desc="Checking Meta ads"):
        try:
            city = cities.split(',')[0].strip() if cities else ''
            meta_data = check_meta_ads_apify(client, agent_name, city)

            # Update DataFrame
            df.at[idx, 'has_meta_ads'] = meta_data['has_meta_ads']
            df.at[idx, 'meta_ad_count'] = meta_data['meta_ad_count']
            df.at[idx, 'meta_page_names'] = '|'.join(meta_data['meta_page_names']) if meta_data['meta_page_names'] else ''
            df.at[idx, 'meta_ad_confidence'] = meta_data['meta_ad_confidence']

            if meta_data['has_meta_ads']:
                stats["with_ads"] += 1
                if meta_data['meta_ad_confidence'] == 'high':
                    stats["high_confidence"] += 1
            else:
                stats["no_ads"] += 1

            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error checking Meta ads for {agent_name}: {e}")
            stats["errors"] += 1

    print(f"\nMeta Ads Results:")
    print(f"  Agents with ads: {stats['with_ads']}")
    print(f"  High confidence matches: {stats['high_confidence']}")
    print(f"  Agents without ads: {stats['no_ads']}")
    print(f"  Errors: {stats['errors']}")

    return df


# =============================================================================
# STEP 4: INSTAGRAM HANDLE DISCOVERY (Website Scraping + LLM)
# =============================================================================

# Headers for web requests
WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# False positives to filter out
INSTAGRAM_FALSE_POSITIVES = {
    'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www', 'reel',
    'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
    'font', 'keyframes', 'charset', 'instagram', 'share', 'help',
}


def is_valid_instagram_handle(handle: str) -> bool:
    """Check if handle is valid Instagram format."""
    if not handle or not isinstance(handle, str):
        return False

    handle = handle.strip().lstrip('@')

    # Must be 1-30 characters, alphanumeric + underscore + period
    if len(handle) < 1 or len(handle) > 30:
        return False

    # Must match Instagram username pattern
    pattern = r'^[a-zA-Z0-9_.]+$'
    if not re.match(pattern, handle):
        return False

    # Filter false positives
    if handle.lower() in INSTAGRAM_FALSE_POSITIVES:
        return False

    return True


def extract_instagram_from_html(html: str) -> List[str]:
    """Extract Instagram handles from HTML content."""
    if not html:
        return []

    handles = set()

    # Pattern 1: Instagram URLs (most reliable)
    url_pattern = r'(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)/?'
    for match in re.finditer(url_pattern, html, re.I):
        handle = match.group(1).lower()
        if is_valid_instagram_handle(handle):
            handles.add(f"@{handle}")

    return list(handles)


def scrape_website_for_instagram(url: str, max_pages: int = 3) -> List[str]:
    """Scrape agent's website for Instagram links."""
    if not url or pd.isna(url):
        return []

    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"

    handles = set()
    pages_to_check = ['/', '/about', '/contact', '/about-us']

    for page_path in pages_to_check[:max_pages]:
        try:
            full_url = urljoin(url, page_path)
            response = requests.get(full_url, headers=WEB_HEADERS, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                found = extract_instagram_from_html(response.text)
                handles.update(found)
                # Early exit if found on homepage
                if page_path == '/' and handles:
                    break
            time.sleep(0.3)
        except Exception:
            continue

    return list(handles)


def search_instagram_with_llm(agent_name: str, city: str = "", website_url: str = "") -> Dict:
    """Use Groq LLM to reason about likely Instagram handle."""
    if not groq_client:
        return {"instagram_handle": "", "instagram_confidence": ""}

    try:
        prompt = f"""Find the most likely Instagram handle for this real estate agent:

NAME: {agent_name}
LOCATION: {city or 'Florida'}
WEBSITE: {website_url or 'Unknown'}

Based on common Instagram naming patterns for real estate agents, what is the most likely Instagram handle?

Common patterns for real estate agents:
- firstname.lastname (e.g., @jill.hertzberg)
- firstnamelastname (e.g., @jillhertzberg)
- firstnamelastname_realtor (e.g., @jillhertzberg_realtor)
- lastname_realestate (e.g., @hertzberg_realestate)
- thefirstnamelastname (e.g., @thejillhertzberg)

Return ONLY the handle in format @username (no explanation). If you cannot determine it, return NOT_FOUND."""

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an expert at finding Instagram profiles for real estate agents. Return only the @username format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=100
        )

        result = response.choices[0].message.content.strip()

        if result and 'NOT_FOUND' not in result:
            # Extract handle from response
            match = re.search(r'@([a-zA-Z0-9_.]+)', result)
            if match:
                handle = f"@{match.group(1).lower()}"
                if is_valid_instagram_handle(handle):
                    return {"instagram_handle": handle, "instagram_confidence": "llm_guess"}

    except Exception as e:
        logger.debug(f"LLM Instagram search error: {e}")

    return {"instagram_handle": "", "instagram_confidence": ""}


def generate_instagram_patterns(agent_name: str) -> List[str]:
    """Generate likely Instagram handle patterns from agent name."""
    if not agent_name:
        return []

    # Clean name
    parts = [p.lower().strip() for p in agent_name.split() if len(p) > 1]
    if len(parts) < 2:
        return []

    first = parts[0]
    last = parts[-1]

    patterns = [
        f"@{first}{last}",           # jillhertzberg
        f"@{first}.{last}",          # jill.hertzberg
        f"@{first}_{last}",          # jill_hertzberg
        f"@{first}{last}realtor",    # jillhertzbergrealtor
        f"@{first}{last}_realtor",   # jillhertzberg_realtor
        f"@{first}{last}realestate", # jillhertzbergrealestate
        f"@the{first}{last}",        # thejillhertzberg
        f"@{last}{first}",           # hertzbergjill
        f"@{last}_realestate",       # hertzberg_realestate
    ]

    return patterns


def verify_instagram_handle(handle: str) -> bool:
    """Verify if Instagram handle exists by checking the profile page."""
    if not handle:
        return False

    username = handle.lstrip('@')
    url = f"https://www.instagram.com/{username}/"

    try:
        response = requests.get(url, headers=WEB_HEADERS, timeout=10)
        if response.status_code != 200:
            return False

        # Check page title - valid profiles have descriptive titles
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.find('title')
        if title:
            title_text = title.text.strip()
            # Valid profiles have bullet point and more content in title
            if '•' in title_text and len(title_text) > 15:
                return True

        return False
    except Exception:
        return False


def search_instagram_combined(agent_name: str, city: str = "", website_url: str = "") -> Dict:
    """
    Combined Instagram search using multiple strategies:
    1. Website scraping (most reliable)
    2. LLM reasoning (good for guessing)
    3. Pattern generation + verification (fallback)
    """
    result = {
        "instagram_handle": "",
        "instagram_followers": 0,
        "instagram_bio": "",
        "instagram_confidence": "",
    }

    # Strategy 1: Scrape agent's personal website
    if website_url and pd.notna(website_url) and str(website_url).strip():
        handles = scrape_website_for_instagram(str(website_url))
        if handles:
            result["instagram_handle"] = handles[0]
            result["instagram_confidence"] = "high"  # From website = high confidence
            return result

    # Strategy 2: Use LLM to reason about likely handle
    llm_result = search_instagram_with_llm(agent_name, city, website_url)
    if llm_result.get("instagram_handle"):
        # Verify the LLM guess
        if verify_instagram_handle(llm_result["instagram_handle"]):
            result["instagram_handle"] = llm_result["instagram_handle"]
            result["instagram_confidence"] = "high"  # Verified = high confidence
            return result
        else:
            # LLM guess not verified, still include as low confidence
            result["instagram_handle"] = llm_result["instagram_handle"]
            result["instagram_confidence"] = "low"
            return result

    # Strategy 3: Pattern generation + verification (slow, do limited)
    patterns = generate_instagram_patterns(agent_name)
    for pattern in patterns[:3]:  # Only check top 3 patterns
        if verify_instagram_handle(pattern):
            result["instagram_handle"] = pattern
            result["instagram_confidence"] = "medium"
            return result
        time.sleep(0.5)  # Rate limiting

    return result


def search_instagram_apify(client: Any, agent_name: str, city: str = "") -> Dict:
    """
    Search for agent's Instagram handle via Apify Instagram Search.
    NOTE: This actor has low accuracy - prefer search_instagram_combined instead.

    Returns dict with:
        - instagram_handle: str
        - instagram_followers: int
        - instagram_bio: str
        - instagram_confidence: 'high', 'medium', 'low'
    """
    result = {
        "instagram_handle": "",
        "instagram_followers": 0,
        "instagram_bio": "",
        "instagram_confidence": "",
    }

    try:
        # Build search query
        search_query = f"{agent_name} real estate"
        if city:
            first_city = city.split(',')[0].strip()
            search_query = f"{agent_name} {first_city} real estate"

        run_input = {
            "search": search_query,
            "resultsLimit": 10,
            "searchType": "user",
        }

        run = client.actor(INSTAGRAM_ACTOR).call(run_input=run_input, timeout_secs=60)

        status = run.get('status')
        if status in ['SUCCEEDED', 'TIMED-OUT']:
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())

                # Parse agent name for matching
                name_parts = [p.lower() for p in agent_name.split() if len(p) > 2]
                real_estate_keywords = ['realtor', 'agent', 'broker', 'real estate', 'properties', 'realty', 'homes', 'luxury']

                best_match = None
                best_score = 0

                for item in items:
                    username = item.get('username', '')
                    full_name = item.get('fullName', '') or item.get('full_name', '')
                    bio = item.get('biography', '') or item.get('bio', '')
                    followers = item.get('followersCount', 0) or item.get('followers', 0)
                    verified = item.get('verified', False)

                    # Calculate match score
                    score = 0

                    # Check name match
                    full_name_lower = full_name.lower()
                    name_matches = sum(1 for part in name_parts if part in full_name_lower)
                    score += name_matches * 30

                    # Check username for name parts
                    username_lower = username.lower().replace('_', ' ').replace('.', ' ')
                    username_matches = sum(1 for part in name_parts if part in username_lower)
                    score += username_matches * 20

                    # Check bio for real estate keywords
                    bio_lower = (bio or '').lower()
                    if any(kw in bio_lower for kw in real_estate_keywords):
                        score += 25

                    # Bonus for verified or high followers
                    if verified:
                        score += 20
                    if followers > 10000:
                        score += 10
                    elif followers > 1000:
                        score += 5

                    if score > best_score:
                        best_score = score
                        best_match = {
                            "username": username,
                            "full_name": full_name,
                            "bio": bio,
                            "followers": followers,
                            "score": score,
                        }

                if best_match:
                    result["instagram_handle"] = f"@{best_match['username']}"
                    result["instagram_followers"] = best_match['followers']
                    result["instagram_bio"] = (best_match['bio'] or '')[:200]

                    # Determine confidence
                    if best_match['score'] >= 70:
                        result["instagram_confidence"] = 'high'
                    elif best_match['score'] >= 40:
                        result["instagram_confidence"] = 'medium'
                    else:
                        result["instagram_confidence"] = 'low'

    except Exception as e:
        logger.debug(f"Instagram search error for {agent_name}: {e}")

    return result


def enrich_instagram(df: pd.DataFrame, dry_run: bool = False, delay: float = 1.0) -> pd.DataFrame:
    """
    Enrich agents with Instagram handles using website scraping + LLM reasoning.
    Much more accurate than the Apify Instagram Search actor.
    """
    logger.info("Step 4: Instagram Handle Discovery (Website + LLM)")

    # Show method status
    if groq_client:
        print("  Using: Website scraping + Groq LLM reasoning")
    else:
        print("  Using: Website scraping + pattern matching (no Groq API key)")

    # Initialize columns
    ig_columns = ["instagram_handle", "instagram_followers", "instagram_bio", "instagram_confidence"]
    for col in ig_columns:
        if col not in df.columns:
            if col == "instagram_followers":
                df[col] = 0
            else:
                df[col] = ""

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has high confidence instagram handle
        existing_handle = row.get('instagram_handle', '')
        existing_conf = row.get('instagram_confidence', '')
        if existing_handle and str(existing_handle).strip() and existing_conf == 'high':
            continue

        agent_name = str(row.get('agent_name', '')).strip()
        if agent_name:
            website_url = row.get('website_url', '')
            cities = row.get('cities', '')
            rows_to_process.append((idx, agent_name, cities, website_url))

    logger.info(f"Found {len(rows_to_process)} agents to search on Instagram")

    if dry_run:
        print(f"\n[DRY RUN] Would search Instagram for {len(rows_to_process)} agents")
        for idx, name, cities, website in rows_to_process[:10]:
            city = cities.split(',')[0].strip() if cities else ''
            site = website if website else 'no website'
            print(f"  - {name} ({city}) - {site}")
        if len(rows_to_process) > 10:
            print(f"  ... and {len(rows_to_process) - 10} more")
        return df

    if not rows_to_process:
        print("No agents to process for Instagram")
        return df

    # Process each agent (no Apify client needed)
    stats = {"found": 0, "not_found": 0, "errors": 0, "high_confidence": 0, "from_website": 0}

    for idx, agent_name, cities, website_url in tqdm(rows_to_process, desc="Searching Instagram"):
        try:
            city = cities.split(',')[0].strip() if cities else ''
            ig_data = search_instagram_combined(agent_name, city, website_url)

            # Update DataFrame
            df.at[idx, 'instagram_handle'] = ig_data['instagram_handle']
            df.at[idx, 'instagram_followers'] = ig_data['instagram_followers']
            df.at[idx, 'instagram_bio'] = ig_data['instagram_bio']
            df.at[idx, 'instagram_confidence'] = ig_data['instagram_confidence']

            if ig_data['instagram_handle']:
                stats["found"] += 1
                if ig_data['instagram_confidence'] == 'high':
                    stats["high_confidence"] += 1
                    stats["from_website"] += 1  # High confidence = from website
            else:
                stats["not_found"] += 1

            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error searching Instagram for {agent_name}: {e}")
            stats["errors"] += 1

    print(f"\nInstagram Search Results:")
    print(f"  Handles found: {stats['found']}")
    print(f"  From website (high confidence): {stats['from_website']}")
    print(f"  From LLM/pattern (lower confidence): {stats['found'] - stats['from_website']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Errors: {stats['errors']}")

    return df


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main function with CLI interface."""

    print(f"\n{'='*60}")
    print("REPLIERS AGENT ENRICHER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(
        description='Enrich Repliers agents with tech stack, Meta ads, and Instagram',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT,
                       help='Input CSV file')
    parser.add_argument('--output', type=str, default=DEFAULT_OUTPUT,
                       help='Output CSV file')
    parser.add_argument('--all', action='store_true',
                       help='Process all agents (default: test mode with 5)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of agents to process')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making API calls')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between API calls in seconds (default: 1.0)')

    # Step-specific flags
    parser.add_argument('--websites-only', action='store_true',
                       help='Only run Step 1: Website Discovery')
    parser.add_argument('--techstack-only', action='store_true',
                       help='Only run Step 2: Tech Stack Detection')
    parser.add_argument('--metaads-only', action='store_true',
                       help='Only run Step 3: Meta Ads Detection')
    parser.add_argument('--instagram-only', action='store_true',
                       help='Only run Step 4: Instagram Handle Discovery')

    args = parser.parse_args()

    # Determine which steps to run
    run_all_steps = not any([args.websites_only, args.techstack_only, args.metaads_only, args.instagram_only])

    # Resolve paths
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = BASE_DIR / args.input

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = BASE_DIR / args.output

    # Check input file
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        return 1

    # Load CSV - prefer output file if it exists (to preserve previous enrichments)
    if output_path.exists() and not run_all_steps:
        load_path = output_path
        print(f"\nLoading from enriched file: {load_path}")
    else:
        load_path = input_path
        print(f"\nInput:  {input_path}")
    print(f"Output: {output_path}")

    df = pd.read_csv(load_path)
    original_count = len(df)
    print(f"Loaded {original_count} agents")

    # Apply limit
    if args.limit:
        df = df.head(args.limit)
        print(f"Limited to {len(df)} agents")
    elif not args.all:
        df = df.head(5)
        print(f"Test mode: Processing first {len(df)} agents")
        print("(Use --all to process all agents)")

    if args.dry_run:
        print("Mode: DRY RUN (no API calls)")

    # Check Apify availability
    if not args.dry_run and not args.websites_only:
        if not APIFY_API_TOKEN:
            print("\nWARNING: APIFY_API_TOKEN not found in environment")
            print("API-based enrichments (tech stack, meta ads, instagram) will be skipped")
        elif not APIFY_AVAILABLE:
            print("\nWARNING: apify-client not installed")
            print("Install with: pip install apify-client")

    # Run enrichment steps
    if run_all_steps or args.websites_only:
        print(f"\n{'='*60}")
        df = enrich_websites(df)

    if run_all_steps or args.techstack_only:
        print(f"\n{'='*60}")
        df = enrich_techstack(df, dry_run=args.dry_run)

    if run_all_steps or args.metaads_only:
        print(f"\n{'='*60}")
        df = enrich_metaads(df, dry_run=args.dry_run, delay=args.delay)

    if run_all_steps or args.instagram_only:
        print(f"\n{'='*60}")
        df = enrich_instagram(df, dry_run=args.dry_run, delay=args.delay)

    # Save output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\n{'='*60}")
    print(f"Saved {len(df)} agents to: {output_path}")

    # Print final summary
    print(f"\n{'='*60}")
    print("ENRICHMENT SUMMARY")
    print(f"{'='*60}")

    # Website stats
    if 'website_url' in df.columns:
        with_website = df['website_url'].notna() & (df['website_url'] != '')
        print(f"Agents with personal website: {with_website.sum()}/{len(df)}")

    # Tech stack stats
    if 'has_crm' in df.columns:
        print(f"Agents with CRM: {df['has_crm'].sum()}/{len(df)}")
    if 'has_marketing_pixel' in df.columns:
        print(f"Agents with marketing pixel: {df['has_marketing_pixel'].sum()}/{len(df)}")
    if 'has_scheduling_tool' in df.columns:
        print(f"Agents with scheduling tool: {df['has_scheduling_tool'].sum()}/{len(df)}")

    # Meta ads stats
    if 'has_meta_ads' in df.columns:
        print(f"Agents with Meta ads: {df['has_meta_ads'].sum()}/{len(df)}")
        if 'meta_ad_confidence' in df.columns:
            high_conf = (df['meta_ad_confidence'] == 'high').sum()
            print(f"  High confidence matches: {high_conf}")

    # Instagram stats
    if 'instagram_handle' in df.columns:
        with_ig = df['instagram_handle'].notna() & (df['instagram_handle'] != '')
        print(f"Agents with Instagram handle: {with_ig.sum()}/{len(df)}")
        if 'instagram_confidence' in df.columns:
            high_conf = (df['instagram_confidence'] == 'high').sum()
            print(f"  High confidence matches: {high_conf}")

    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
