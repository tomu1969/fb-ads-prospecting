"""
Tech Stack Enricher - Module 3.11 - Operational Maturity Signals

Detects CRM, marketing pixels, scheduling tools, and chat widgets on websites
to assess operational maturity. Sophisticated tech stacks indicate willingness
to pay for AI automation.

Input: processed/03g_gmaps.csv (from Module 3.10)
Output: processed/03h_techstack.csv

Data extracted:
- has_crm: CRM system detected
- crm_name: HubSpot, Salesforce, FollowUpBoss, etc.
- has_marketing_pixel: Meta/Google pixel detected
- pixel_types: List of detected pixels
- has_scheduling_tool: Calendly, Acuity, etc.
- scheduling_tool: Tool name
- has_chat_widget: Live chat detected
- chat_widget: Intercom, Drift, etc.
- has_lead_form: Contact/lead form detected
- tech_stack_raw: All detected technologies

Usage:
    python scripts/tech_stack_enricher.py           # Test mode (3 contacts)
    python scripts/tech_stack_enricher.py --all     # Process all contacts
    python scripts/tech_stack_enricher.py --csv output/prospects.csv  # Standalone
"""

import os
import sys
import re
import argparse
import logging
import time
import json
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('tech_stack_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent

# Pipeline input/output paths
INPUT_BASE = "03g_gmaps.csv"
OUTPUT_BASE = "03h_techstack.csv"

# Request settings
REQUEST_TIMEOUT = 15
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# =============================================================================
# TECHNOLOGY DETECTION SIGNATURES
# =============================================================================

TECH_SIGNATURES = {
    # -------------------------------------------------------------------------
    # CRMs (Real Estate focused)
    # -------------------------------------------------------------------------
    "hubspot": {
        "patterns": [
            r"js\.hs-scripts\.com",
            r"js\.hsforms\.net",
            r"hbspt\.forms",
            r"hs-script-loader",
            r"hubspot\.com",
            r"_hsq\.push",
        ],
        "category": "crm",
        "display_name": "HubSpot",
    },
    "salesforce": {
        "patterns": [
            r"salesforce\.com",
            r"force\.com",
            r"pardot\.com",
            r"sfdc",
        ],
        "category": "crm",
        "display_name": "Salesforce",
    },
    "followupboss": {
        "patterns": [
            r"followupboss\.com",
            r"fub\.com",
            r"followup\.boss",
        ],
        "category": "crm",
        "display_name": "Follow Up Boss",
    },
    "kvcore": {
        "patterns": [
            r"kvcore\.com",
            r"platform\.kvcore",
            r"idx\.kvcore",
        ],
        "category": "crm",
        "display_name": "kvCORE",
    },
    "liondesk": {
        "patterns": [
            r"liondesk\.com",
            r"ld\.link",
        ],
        "category": "crm",
        "display_name": "LionDesk",
    },
    "cinc": {
        "patterns": [
            r"cincpro\.com",
            r"cinc\.com",
        ],
        "category": "crm",
        "display_name": "CINC",
    },
    "boomtown": {
        "patterns": [
            r"boomtownroi\.com",
            r"boomtown\.com",
        ],
        "category": "crm",
        "display_name": "BoomTown",
    },
    "realgeeks": {
        "patterns": [
            r"realgeeks\.com",
        ],
        "category": "crm",
        "display_name": "Real Geeks",
    },
    "chime": {
        "patterns": [
            r"chime\.me",
            r"chimecentral\.com",
        ],
        "category": "crm",
        "display_name": "Chime",
    },
    "zoho": {
        "patterns": [
            r"zoho\.com",
            r"zohocdn\.com",
        ],
        "category": "crm",
        "display_name": "Zoho CRM",
    },
    "pipedrive": {
        "patterns": [
            r"pipedrive\.com",
            r"pipedrivewebforms",
        ],
        "category": "crm",
        "display_name": "Pipedrive",
    },

    # -------------------------------------------------------------------------
    # Marketing Pixels
    # -------------------------------------------------------------------------
    "meta_pixel": {
        "patterns": [
            r"connect\.facebook\.net",
            r"fbq\s*\(",
            r"facebook-pixel",
            r"facebook\.com/tr",
            r"_fbq",
        ],
        "category": "pixel",
        "display_name": "Meta Pixel",
    },
    "google_analytics": {
        "patterns": [
            r"google-analytics\.com",
            r"googletagmanager\.com",
            r"gtag\s*\(",
            r"ga\s*\(\s*['\"]create['\"]",
            r"analytics\.js",
            r"gtm\.js",
        ],
        "category": "pixel",
        "display_name": "Google Analytics",
    },
    "google_ads": {
        "patterns": [
            r"googleadservices\.com",
            r"google_conversion",
            r"googleads\.g\.doubleclick",
            r"gtag.*config.*AW-",
        ],
        "category": "pixel",
        "display_name": "Google Ads",
    },
    "tiktok_pixel": {
        "patterns": [
            r"analytics\.tiktok\.com",
            r"ttq\.track",
        ],
        "category": "pixel",
        "display_name": "TikTok Pixel",
    },
    "linkedin_insight": {
        "patterns": [
            r"snap\.licdn\.com",
            r"linkedin\.com/px",
            r"_linkedin_data_partner_id",
        ],
        "category": "pixel",
        "display_name": "LinkedIn Insight",
    },
    "hotjar": {
        "patterns": [
            r"static\.hotjar\.com",
            r"hotjar\.com",
            r"hj\s*\(",
        ],
        "category": "pixel",
        "display_name": "Hotjar",
    },

    # -------------------------------------------------------------------------
    # Scheduling Tools
    # -------------------------------------------------------------------------
    "calendly": {
        "patterns": [
            r"calendly\.com",
            r"assets\.calendly\.com",
        ],
        "category": "scheduling",
        "display_name": "Calendly",
    },
    "acuity": {
        "patterns": [
            r"acuityscheduling\.com",
            r"squareup\.com/appointments",
        ],
        "category": "scheduling",
        "display_name": "Acuity Scheduling",
    },
    "hubspot_meetings": {
        "patterns": [
            r"meetings\.hubspot\.com",
            r"hs-meetings",
        ],
        "category": "scheduling",
        "display_name": "HubSpot Meetings",
    },
    "booksy": {
        "patterns": [
            r"booksy\.com",
        ],
        "category": "scheduling",
        "display_name": "Booksy",
    },
    "setmore": {
        "patterns": [
            r"setmore\.com",
        ],
        "category": "scheduling",
        "display_name": "Setmore",
    },
    "appointy": {
        "patterns": [
            r"appointy\.com",
        ],
        "category": "scheduling",
        "display_name": "Appointy",
    },

    # -------------------------------------------------------------------------
    # Chat Widgets
    # -------------------------------------------------------------------------
    "intercom": {
        "patterns": [
            r"intercom\.io",
            r"widget\.intercom\.io",
            r"intercomcdn\.com",
            r"Intercom\s*\(",
        ],
        "category": "chat",
        "display_name": "Intercom",
    },
    "drift": {
        "patterns": [
            r"drift\.com",
            r"js\.driftt\.com",
            r"driftt\.com",
        ],
        "category": "chat",
        "display_name": "Drift",
    },
    "livechat": {
        "patterns": [
            r"livechatinc\.com",
            r"cdn\.livechatinc\.com",
            r"__lc\.license",
        ],
        "category": "chat",
        "display_name": "LiveChat",
    },
    "tawk": {
        "patterns": [
            r"tawk\.to",
            r"embed\.tawk\.to",
        ],
        "category": "chat",
        "display_name": "Tawk.to",
    },
    "zendesk_chat": {
        "patterns": [
            r"zopim\.com",
            r"zendesk\.com.*chat",
            r"static\.zdassets\.com",
        ],
        "category": "chat",
        "display_name": "Zendesk Chat",
    },
    "freshchat": {
        "patterns": [
            r"freshchat\.com",
            r"wchat\.freshchat\.com",
        ],
        "category": "chat",
        "display_name": "Freshchat",
    },
    "crisp": {
        "patterns": [
            r"crisp\.chat",
            r"client\.crisp\.chat",
        ],
        "category": "chat",
        "display_name": "Crisp",
    },
    "olark": {
        "patterns": [
            r"olark\.com",
            r"static\.olark\.com",
        ],
        "category": "chat",
        "display_name": "Olark",
    },
    "hubspot_chat": {
        "patterns": [
            r"js\.usemessages\.com",
            r"hubspot.*conversations",
        ],
        "category": "chat",
        "display_name": "HubSpot Chat",
    },

    # -------------------------------------------------------------------------
    # Lead Forms & Conversion Tools
    # -------------------------------------------------------------------------
    "typeform": {
        "patterns": [
            r"typeform\.com",
            r"embed\.typeform\.com",
        ],
        "category": "form",
        "display_name": "Typeform",
    },
    "jotform": {
        "patterns": [
            r"jotform\.com",
            r"cdn\.jotfor\.ms",
        ],
        "category": "form",
        "display_name": "JotForm",
    },
    "wufoo": {
        "patterns": [
            r"wufoo\.com",
        ],
        "category": "form",
        "display_name": "Wufoo",
    },
    "gravity_forms": {
        "patterns": [
            r"gravityforms",
            r"gform_",
        ],
        "category": "form",
        "display_name": "Gravity Forms",
    },
    "contact_form_7": {
        "patterns": [
            r"wpcf7",
            r"contact-form-7",
        ],
        "category": "form",
        "display_name": "Contact Form 7",
    },

    # -------------------------------------------------------------------------
    # IDX/MLS (Real Estate Specific)
    # -------------------------------------------------------------------------
    "idx_broker": {
        "patterns": [
            r"idxbroker\.com",
            r"idx-broker",
        ],
        "category": "idx",
        "display_name": "IDX Broker",
    },
    "showcase_idx": {
        "patterns": [
            r"showcaseidx\.com",
        ],
        "category": "idx",
        "display_name": "Showcase IDX",
    },
    "ihomefinder": {
        "patterns": [
            r"ihomefinder\.com",
        ],
        "category": "idx",
        "display_name": "iHomefinder",
    },
}


def fetch_website_html(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
    """
    Fetch HTML content from a website.

    Args:
        url: Website URL
        timeout: Request timeout in seconds

    Returns:
        HTML content or None if failed
    """
    if not url:
        return None

    # Ensure URL has scheme
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'

    try:
        response = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=timeout,
            allow_redirects=True,
            verify=True
        )
        response.raise_for_status()
        return response.text
    except requests.exceptions.SSLError:
        # Try without SSL verification as fallback
        try:
            response = requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=timeout,
                allow_redirects=True,
                verify=False
            )
            return response.text
        except Exception:
            pass
    except requests.exceptions.RequestException as e:
        logger.debug(f"Failed to fetch {url}: {e}")
    except Exception as e:
        logger.debug(f"Unexpected error fetching {url}: {e}")

    return None


def detect_technologies(html: str) -> List[Dict]:
    """
    Detect technologies from HTML content.

    Args:
        html: HTML content to analyze

    Returns:
        List of detected technologies with name and category
    """
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
                    break  # Found this tech, move to next
            except re.error as e:
                logger.warning(f"Invalid regex pattern for {tech_id}: {e}")

    return detected


def detect_generic_lead_form(html: str) -> bool:
    """
    Detect generic lead/contact forms that aren't from known providers.

    Args:
        html: HTML content

    Returns:
        True if lead form detected
    """
    if not html:
        return False

    html_lower = html.lower()

    # Look for form elements with contact/lead-related attributes
    form_indicators = [
        r'<form[^>]*(?:contact|lead|inquiry|schedule|get-started|request)',
        r'<form[^>]*id\s*=\s*["\'][^"\']*(?:contact|lead|inquiry)[^"\']*["\']',
        r'<form[^>]*class\s*=\s*["\'][^"\']*(?:contact|lead|inquiry)[^"\']*["\']',
        r'type\s*=\s*["\']email["\'][^>]*placeholder\s*=\s*["\'][^"\']*email',
        r'<input[^>]*name\s*=\s*["\'](?:email|phone|name)["\']',
    ]

    for pattern in form_indicators:
        if re.search(pattern, html_lower, re.IGNORECASE):
            return True

    return False


def aggregate_tech_stack(detected: List[Dict], has_generic_form: bool) -> Dict[str, Any]:
    """
    Aggregate detected technologies by category.

    Args:
        detected: List of detected technologies
        has_generic_form: Whether generic lead form was detected

    Returns:
        Aggregated tech stack data
    """
    # Group by category
    crms = [t for t in detected if t["category"] == "crm"]
    pixels = [t for t in detected if t["category"] == "pixel"]
    scheduling = [t for t in detected if t["category"] == "scheduling"]
    chat = [t for t in detected if t["category"] == "chat"]
    forms = [t for t in detected if t["category"] == "form"]
    idx = [t for t in detected if t["category"] == "idx"]

    # Check for lead forms (either known provider or generic)
    has_lead_form = len(forms) > 0 or has_generic_form

    return {
        "has_crm": len(crms) > 0,
        "crm_name": crms[0]["name"] if crms else "",
        "has_marketing_pixel": len(pixels) > 0,
        "pixel_types": json.dumps([p["name"] for p in pixels]) if pixels else "[]",
        "has_scheduling_tool": len(scheduling) > 0,
        "scheduling_tool": scheduling[0]["name"] if scheduling else "",
        "has_chat_widget": len(chat) > 0,
        "chat_widget": chat[0]["name"] if chat else "",
        "has_lead_form": has_lead_form,
        "has_idx": len(idx) > 0,
        "idx_provider": idx[0]["name"] if idx else "",
        "tech_stack_raw": json.dumps([t["name"] for t in detected]) if detected else "[]",
        "tech_count": len(detected),
    }


def enrich_with_tech_stack(row: Dict, delay: float = 0.5) -> Dict[str, Any]:
    """
    Enrich a single contact with tech stack data.

    Args:
        row: Contact row as dict
        delay: Delay before request

    Returns:
        Dict with tech stack fields
    """
    website_url = str(row.get("website_url", "")).strip()

    # Default empty result
    empty_result = {
        "has_crm": False,
        "crm_name": "",
        "has_marketing_pixel": False,
        "pixel_types": "[]",
        "has_scheduling_tool": False,
        "scheduling_tool": "",
        "has_chat_widget": False,
        "chat_widget": "",
        "has_lead_form": False,
        "has_idx": False,
        "idx_provider": "",
        "tech_stack_raw": "[]",
        "tech_count": 0,
    }

    if not website_url or website_url.lower() in ["nan", "none", ""]:
        return empty_result

    # Rate limiting
    time.sleep(delay)

    # Fetch HTML
    html = fetch_website_html(website_url)

    if not html:
        logger.debug(f"Could not fetch HTML for: {website_url}")
        return empty_result

    # Detect technologies
    detected = detect_technologies(html)
    has_generic_form = detect_generic_lead_form(html)

    # Aggregate results
    result = aggregate_tech_stack(detected, has_generic_form)

    if detected:
        logger.debug(f"Found {len(detected)} technologies on {website_url}: {[t['name'] for t in detected]}")

    return result


def enrich_tech_stack(
    csv_path: Path,
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    delay: float = 0.5
) -> Dict:
    """
    Enrich CSV with tech stack detection data.

    Args:
        csv_path: Input CSV path
        output_path: Output CSV path
        limit: Max contacts to process
        dry_run: Preview without making requests
        delay: Delay between requests

    Returns:
        Stats dictionary
    """
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return {"error": str(e)}

    if "website_url" not in df.columns:
        logger.error("CSV must have 'website_url' column")
        return {"error": "Missing required column: website_url"}

    # Initialize tech stack columns if not exist
    tech_columns = [
        "has_crm", "crm_name", "has_marketing_pixel", "pixel_types",
        "has_scheduling_tool", "scheduling_tool", "has_chat_widget",
        "chat_widget", "has_lead_form", "has_idx", "idx_provider",
        "tech_stack_raw", "tech_count"
    ]
    for col in tech_columns:
        if col not in df.columns:
            if col.startswith("has_"):
                df[col] = False
            elif col == "tech_count":
                df[col] = 0
            elif col == "pixel_types" or col == "tech_stack_raw":
                df[col] = "[]"
            else:
                df[col] = ""

    # Stats
    stats = {
        "total": len(df),
        "processed": 0,
        "found_tech": 0,
        "skipped": 0,
        "errors": 0,
        "already_had": 0,
        "with_crm": 0,
        "with_pixel": 0,
        "with_scheduling": 0,
        "with_chat": 0,
    }

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has tech stack data
        if row.get("tech_count", 0) > 0:
            stats["already_had"] += 1
            continue

        website_url = str(row.get("website_url", "")).strip()
        if not website_url or website_url.lower() in ["nan", "none", ""]:
            stats["skipped"] += 1
            continue

        rows_to_process.append((idx, row.to_dict()))

    # Apply limit
    if limit:
        rows_to_process = rows_to_process[:limit]

    logger.info(f"Processing {len(rows_to_process)} contacts for tech stack detection")

    if dry_run:
        logger.info("DRY RUN - No requests will be made")
        for idx, row in rows_to_process[:10]:
            logger.info(f"  Would analyze: {row.get('website_url')}")
        return stats

    # Process each contact
    for idx, row in tqdm(rows_to_process, desc="Detecting tech stacks"):
        try:
            tech_data = enrich_with_tech_stack(row, delay=delay)

            # Update dataframe
            for key, value in tech_data.items():
                df.at[idx, key] = value

            if tech_data.get("tech_count", 0) > 0:
                stats["found_tech"] += 1

            # Track specific tech categories
            if tech_data.get("has_crm"):
                stats["with_crm"] += 1
            if tech_data.get("has_marketing_pixel"):
                stats["with_pixel"] += 1
            if tech_data.get("has_scheduling_tool"):
                stats["with_scheduling"] += 1
            if tech_data.get("has_chat_widget"):
                stats["with_chat"] += 1

            stats["processed"] += 1

        except Exception as e:
            logger.error(f"Error processing {row.get('website_url')}: {e}")
            stats["errors"] += 1

    # Save output
    if output_path is None:
        output_path = csv_path.parent / f"{csv_path.stem}_techstack{csv_path.suffix}"

    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved to {output_path}")

    return stats


def print_summary(stats: Dict):
    """Print enrichment summary."""
    print("\n" + "=" * 60)
    print("TECH STACK ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"Total contacts:        {stats.get('total', 0)}")
    print(f"Already had data:      {stats.get('already_had', 0)}")
    print(f"Processed:             {stats.get('processed', 0)}")
    print(f"Found tech:            {stats.get('found_tech', 0)}")
    print(f"Skipped (no website):  {stats.get('skipped', 0)}")
    print(f"Errors:                {stats.get('errors', 0)}")
    print("-" * 60)
    print("Technology Breakdown:")
    print(f"  With CRM:            {stats.get('with_crm', 0)}")
    print(f"  With Pixel:          {stats.get('with_pixel', 0)}")
    print(f"  With Scheduling:     {stats.get('with_scheduling', 0)}")
    print(f"  With Chat:           {stats.get('with_chat', 0)}")
    print("=" * 60)


def main():
    """Main function with pipeline and standalone mode support."""

    print(f"\n{'='*60}")
    print("MODULE 3.11: TECH STACK ENRICHER")
    print(f"{'='*60}")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Detect tech stack (CRM, pixels, scheduling, chat) on websites',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--csv', type=str,
                       help='Input CSV file (standalone mode)')
    parser.add_argument('--output', type=str,
                       help='Output CSV path')
    parser.add_argument('--all', action='store_true',
                       help='Process all contacts (default: test mode with 3)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making requests')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='Delay between requests in seconds (default: 0.5)')

    args = parser.parse_args()

    # Determine input file
    run_id = get_run_id_from_env()

    if args.csv:
        # Standalone mode
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv
        standalone_mode = True
    else:
        # Pipeline mode
        if run_id:
            input_name = get_versioned_filename(INPUT_BASE, run_id)
            csv_path = BASE_DIR / "processed" / input_name
        else:
            csv_path = BASE_DIR / "processed" / INPUT_BASE

        # Try latest symlink if versioned doesn't exist
        if not csv_path.exists():
            latest_path = BASE_DIR / "processed" / INPUT_BASE
            if latest_path.exists() or latest_path.is_symlink():
                csv_path = latest_path

        # Fallback chain
        if not csv_path.exists():
            for fallback in ["03g_gmaps.csv", "03f_linkedin.csv", "03e_names.csv", "03d_final.csv"]:
                fallback_path = BASE_DIR / "processed" / fallback
                if fallback_path.exists():
                    csv_path = fallback_path
                    logger.info(f"Using fallback input: {fallback_path}")
                    break

        standalone_mode = False

    if not csv_path.exists():
        print(f"ERROR: Input file not found: {csv_path}")
        if not standalone_mode:
            print("Make sure Module 3.10 (Google Maps Enricher) has run first.")
        return 1

    # Determine output file
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    elif standalone_mode:
        output_path = csv_path.parent / f"{csv_path.stem}_techstack{csv_path.suffix}"
    else:
        if run_id:
            output_name = get_versioned_filename(OUTPUT_BASE, run_id)
            output_path = BASE_DIR / "processed" / output_name
        else:
            output_path = BASE_DIR / "processed" / OUTPUT_BASE

    # Determine limit
    if args.limit:
        limit = args.limit
    elif args.all:
        limit = None
    else:
        limit = 3
        print("Test mode: Processing first 3 contacts")
        print("(Use --all to process all contacts)")

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.dry_run:
        print("Mode: DRY RUN")

    # Run enrichment
    stats = enrich_tech_stack(
        csv_path=csv_path,
        output_path=output_path,
        limit=limit,
        dry_run=args.dry_run,
        delay=args.delay
    )

    # Create latest symlink in pipeline mode
    if not standalone_mode and run_id and output_path.exists():
        latest_path = create_latest_symlink(output_path, OUTPUT_BASE)
        if latest_path:
            print(f"Latest symlink: {latest_path}")

    print_summary(stats)
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
