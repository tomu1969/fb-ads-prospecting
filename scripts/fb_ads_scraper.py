"""
Facebook Ad Library Scraper

Scrapes Facebook advertisers using the Apify actor curious_coder/facebook-ads-library-scraper.
Supports interactive mode with preview counts and keyword modification.

Usage:
    # Interactive mode
    python scripts/fb_ads_scraper.py

    # CLI mode
    python scripts/fb_ads_scraper.py --query "real estate miami" --count 50

    # Housing ads
    python scripts/fb_ads_scraper.py --ad-type housing_ads --query "miami" --count 50

    # Dry run
    python scripts/fb_ads_scraper.py --query "test" --dry-run

Output includes rich data: spend, reach, impressions, targeted countries, and more.
"""

import os
import sys
import json
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / 'config'
OUTPUT_DIR = BASE_DIR / 'output'
HISTORY_FILE = CONFIG_DIR / 'scraped_advertisers.json'

APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
APIFY_ACTOR_ID = 'curious_coder/facebook-ads-library-scraper'

# Search parameter options
AD_TYPES = {
    '1': ('all', 'All ads'),
    '2': ('political_and_issue_ads', 'Political & Issue ads'),
    '3': ('housing_ads', 'Housing ads'),
    '4': ('employment_ads', 'Employment ads'),
    '5': ('credit_ads', 'Credit ads'),
}

STATUS_OPTIONS = {
    '1': ('active', 'Active only'),
    '2': ('inactive', 'Inactive only'),
    '3': ('all', 'All'),
}

MEDIA_TYPES = {
    '1': ('all', 'All'),
    '2': ('video', 'Video only'),
    '3': ('image', 'Image only'),
}

PLATFORMS = ['facebook', 'instagram', 'messenger', 'audience_network']

# URL parameter mappings for curious_coder actor
AD_TYPE_URL_MAP = {
    'all': 'all',
    'political_and_issue_ads': 'political_and_issue_ads',
    'housing_ads': 'housing',
    'employment_ads': 'employment',
    'credit_ads': 'credit',
}

# Location presets for quick selection
# Note: FB Ad Library only supports country-level filtering
# For city/state targeting, use keywords in the search query
LOCATION_PRESETS = {
    # Countries
    'us': {'country': 'US', 'name': 'United States'},
    'uk': {'country': 'GB', 'name': 'United Kingdom'},
    'canada': {'country': 'CA', 'name': 'Canada'},
    'australia': {'country': 'AU', 'name': 'Australia'},
    'germany': {'country': 'DE', 'name': 'Germany'},
    'spain': {'country': 'ES', 'name': 'Spain'},
    'mexico': {'country': 'MX', 'name': 'Mexico'},
    'france': {'country': 'FR', 'name': 'France'},
    'brazil': {'country': 'BR', 'name': 'Brazil'},
}

# Interactive location menu options
LOCATION_MENU = {
    '1': 'us',
    '2': 'uk',
    '3': 'canada',
    '4': 'australia',
    '5': 'germany',
    '6': 'spain',
    '7': 'mexico',
    '8': 'france',
    '9': 'brazil',
    '10': 'other',  # Enter ISO code
}


def get_apify_client():
    """Get Apify client with lazy import."""
    try:
        from apify_client import ApifyClient
        if not APIFY_API_TOKEN:
            return None
        return ApifyClient(APIFY_API_TOKEN)
    except ImportError:
        print("ERROR: apify-client not installed. Run: pip install apify-client")
        return None


def load_history() -> Dict:
    """Load scrape history from JSON file."""
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'advertisers': {}, 'searches': []}


def save_history(history: Dict):
    """Save scrape history to JSON file."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)


def build_search_url(query: str = '', country: str = 'US', ad_type: str = 'all',
                     status: str = 'active', media_type: str = 'all',
                     platforms: str = 'all', start_date: str = None,
                     end_date: str = None) -> str:
    """Build Facebook Ad Library search URL for curious_coder actor.

    Args:
        query: Search keywords (optional - can browse by filters alone)
        country: ISO country code
        ad_type: Ad category filter
        status: Active status filter
        media_type: Media type filter
        platforms: Platform filter (not used - curious_coder doesn't support)
        start_date: Start date filter (not used - curious_coder doesn't support)
        end_date: End date filter (not used - curious_coder doesn't support)
    """
    base = 'https://www.facebook.com/ads/library/'

    # Map ad_type to URL format
    url_ad_type = AD_TYPE_URL_MAP.get(ad_type, 'all')

    params = [
        f'active_status={status}',
        f'ad_type={url_ad_type}',
        f'country={country}',
        'search_type=keyword_unordered',
        f'media_type={media_type}',
    ]

    # Only add query if provided
    if query:
        params.append(f'q={quote_plus(query)}')

    return base + '?' + '&'.join(params)


def get_preview_count(client, config: Dict) -> Tuple[int, Optional[str]]:
    """Get preview count of ads matching criteria without full download.

    Note: curious_coder actor doesn't support preview counts efficiently,
    so this returns an estimate based on a quick scrape.
    """
    try:
        # Build search URL
        search_url = build_search_url(
            query=config.get('query', ''),
            country=config.get('country', 'US'),
            ad_type=config.get('ad_type', 'all'),
            status=config.get('status', 'active'),
            media_type=config.get('media', 'all'),
        )

        run_input = {
            'urls': [{'url': search_url}],
            'maxAds': 10,  # Quick sample
        }

        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input, timeout_secs=60)

        if run.get('status') in ['SUCCEEDED', 'TIMED-OUT']:
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                # If we got 10 items quickly, there are likely many more
                if len(items) >= 10:
                    return 1000, None  # Estimate: many results available
                return len(items) * 10, None  # Rough estimate

        return 0, "Could not get preview count"
    except Exception as e:
        return 0, str(e)


def scrape_ads(client, config: Dict, limit: int = 100) -> Tuple[List[Dict], Optional[str]]:
    """Scrape ads from Facebook Ad Library using curious_coder actor."""
    try:
        # Build search URL with all filters
        search_url = build_search_url(
            query=config.get('query', ''),
            country=config.get('country', 'US'),
            ad_type=config.get('ad_type', 'all'),
            status=config.get('status', 'active'),
            media_type=config.get('media', 'all'),
        )

        run_input = {
            'urls': [{'url': search_url}],
            'maxAds': limit,
        }

        print(f"\nStarting Apify actor: {APIFY_ACTOR_ID}")
        print(f"Query: {config.get('query') or '(browse all)'}")
        print(f"Country: {config['country']}, Ad Type: {config['ad_type']}, Status: {config['status']}")
        print(f"Max results: {limit}")
        print(f"URL: {search_url}")

        # Use longer timeout for larger scrapes
        timeout = max(300, limit * 3)  # At least 5 min, or 3 sec per ad
        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input, timeout_secs=timeout)

        status = run.get('status')
        if status in ['SUCCEEDED', 'TIMED-OUT']:
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                if status == 'TIMED-OUT' and items:
                    print(f"Note: Actor timed out but collected {len(items)} ads")
                return items, None
            return [], "No dataset returned"
        else:
            return [], f"Actor run failed: {status}"

    except Exception as e:
        return [], str(e)


def filter_duplicates(ads: List[Dict], history: Dict, force: bool = False) -> Tuple[List[Dict], int]:
    """Filter out already-scraped advertisers."""
    if force:
        return ads, 0

    existing = set(history.get('advertisers', {}).keys())
    new_ads = []
    duplicates = 0

    for ad in ads:
        page_id = str(ad.get('page_id', ad.get('pageId', '')))
        if page_id and page_id in existing:
            duplicates += 1
        else:
            new_ads.append(ad)

    return new_ads, duplicates


def update_history(history: Dict, ads: List[Dict], config: Dict, duplicates: int):
    """Update history with newly scraped advertisers."""
    today = datetime.now().strftime('%Y-%m-%d')

    for ad in ads:
        page_id = str(ad.get('page_id', ad.get('pageId', '')))
        if not page_id:
            continue

        if page_id in history['advertisers']:
            history['advertisers'][page_id]['last_scraped'] = today
            history['advertisers'][page_id]['scrape_count'] += 1
        else:
            history['advertisers'][page_id] = {
                'page_name': ad.get('page_name', ad.get('pageName', 'Unknown')),
                'first_scraped': today,
                'last_scraped': today,
                'scrape_count': 1
            }

    history['searches'].append({
        'query': config['query'] or '(browse by filters)',
        'location': config.get('location_name', config.get('country', 'US')),
        'date': today,
        'count': len(ads) + duplicates,
        'new_advertisers': len(ads),
        'duplicates_skipped': duplicates
    })

    return history


def get_ad_text(ad: Dict) -> str:
    """Extract ad text from curious_coder actor output.

    The curious_coder actor stores ad creative in the 'snapshot' field,
    which contains nested data about the ad content.
    """
    # Try snapshot first (curious_coder format)
    snapshot = ad.get('snapshot', {})
    if isinstance(snapshot, dict):
        # Try various nested fields in snapshot
        body = snapshot.get('body', {})
        if isinstance(body, dict):
            text = body.get('text', '')
            if text:
                return text.strip()

        # Try cards (carousel ads)
        cards = snapshot.get('cards', [])
        if cards and isinstance(cards, list):
            texts = []
            for card in cards[:3]:  # First 3 cards
                if isinstance(card, dict):
                    card_body = card.get('body', '')
                    if card_body:
                        texts.append(card_body)
            if texts:
                return ' | '.join(texts)

        # Try title
        title = snapshot.get('title', '')
        if title:
            return title.strip()

    # Fallback to direct fields
    text_fields = ['text', 'title', 'body', 'message', 'description']
    for field in text_fields:
        value = ad.get(field)
        if value and isinstance(value, str) and value.strip():
            return value.strip()

    return ''


def convert_to_pipeline_format(ads: List[Dict]) -> pd.DataFrame:
    """Convert curious_coder Apify results to pipeline-compatible format.

    Extracts and groups ads by advertiser (page), capturing:
    - Ad texts from 'snapshot' field
    - Spend, reach, impressions data (new in curious_coder)
    - Start dates, platforms, active status, etc.
    - Targeted countries
    """
    rows = []

    # Group by advertiser
    advertisers = {}
    for ad in ads:
        page_id = str(ad.get('page_id', ''))
        page_name = ad.get('page_name', 'Unknown')

        if not page_id:
            continue

        if page_id not in advertisers:
            advertisers[page_id] = {
                'page_name': page_name,
                'page_id': page_id,
                'ads': [],
                'platforms': set(),
                'first_ad_date': None,
                'is_active': False,
                'categories': set(),
                'ad_titles': [],
                'link_urls': set(),
                'total_spend': 0,
                'currency': None,
                'max_reach': 0,
                'targeted_countries': set(),
                'ad_library_url': None,
            }

        advertisers[page_id]['ads'].append(ad)

        # Collect platforms (curious_coder uses publisher_platform as list)
        platforms = ad.get('publisher_platform', [])
        if isinstance(platforms, list):
            advertisers[page_id]['platforms'].update(platforms)
        elif platforms:
            advertisers[page_id]['platforms'].add(platforms)

        # Track first ad date
        start_date = ad.get('start_date')
        if start_date:
            if not advertisers[page_id]['first_ad_date'] or start_date < advertisers[page_id]['first_ad_date']:
                advertisers[page_id]['first_ad_date'] = start_date

        # Check if any ad is active
        if ad.get('is_active', False):
            advertisers[page_id]['is_active'] = True

        # Collect categories
        categories = ad.get('categories', [])
        if isinstance(categories, list):
            advertisers[page_id]['categories'].update(categories)

        # Accumulate spend data
        spend = ad.get('spend', {})
        if isinstance(spend, dict):
            lower = spend.get('lower_bound', 0) or 0
            upper = spend.get('upper_bound', 0) or 0
            avg_spend = (lower + upper) / 2 if upper else lower
            advertisers[page_id]['total_spend'] += avg_spend
            if not advertisers[page_id]['currency']:
                advertisers[page_id]['currency'] = ad.get('currency')

        # Track reach estimate
        reach = ad.get('reach_estimate', {})
        if isinstance(reach, dict):
            upper_reach = reach.get('upper_bound', 0) or 0
            if upper_reach > advertisers[page_id]['max_reach']:
                advertisers[page_id]['max_reach'] = upper_reach

        # Collect targeted countries
        countries = ad.get('targeted_or_reached_countries', [])
        if isinstance(countries, list):
            advertisers[page_id]['targeted_countries'].update(countries)

        # Collect link URLs from snapshot
        snapshot = ad.get('snapshot', {})
        if isinstance(snapshot, dict):
            link_url = snapshot.get('link_url') or snapshot.get('page_welcome_message')
            if link_url and isinstance(link_url, str):
                advertisers[page_id]['link_urls'].add(link_url)

            # Collect titles from snapshot
            title = snapshot.get('title')
            if title and title not in advertisers[page_id]['ad_titles']:
                advertisers[page_id]['ad_titles'].append(title)

        # Ad library URL for the page
        if not advertisers[page_id]['ad_library_url']:
            advertisers[page_id]['ad_library_url'] = ad.get('ad_library_url')

    # Convert to rows
    for page_id, data in advertisers.items():
        # Extract ad texts
        ad_texts = []
        for ad in data['ads']:
            text = get_ad_text(ad)
            if text and text not in ad_texts:  # Dedupe
                ad_texts.append(text)

        rows.append({
            'page_name': data['page_name'],
            'page_id': page_id,
            'page_url': data['ad_library_url'],  # Use ad library URL as page reference
            'page_category': ', '.join(data['categories']) if data['categories'] else None,
            'ad_count': len(data['ads']),
            'text': ' | '.join(ad_texts[:10]),  # Pipeline expects 'text' field
            'ad_texts': json.dumps(ad_texts[:10]),
            'ad_titles': json.dumps(data['ad_titles'][:5]),
            'platforms': json.dumps(list(data['platforms'])),
            'is_active': data['is_active'],
            'start_date': data['first_ad_date'],
            'link_urls': json.dumps(list(data['link_urls'])[:5]),
            # New fields from curious_coder
            'total_spend': round(data['total_spend'], 2) if data['total_spend'] else None,
            'currency': data['currency'],
            'max_reach': data['max_reach'] if data['max_reach'] else None,
            'targeted_countries': json.dumps(list(data['targeted_countries'])) if data['targeted_countries'] else None,
        })

    return pd.DataFrame(rows)


def save_results(df: pd.DataFrame, output_path: Path = None) -> Path:
    """Save results to CSV."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not output_path:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = OUTPUT_DIR / f'fb_ads_scraped_{timestamp}.csv'

    df.to_csv(output_path, index=False, encoding='utf-8')
    return output_path


def print_banner():
    """Print welcome banner."""
    print("\n" + "=" * 62)
    print("           Facebook Ad Library Scraper")
    print("=" * 62 + "\n")


def print_config(config: Dict):
    """Print current configuration."""
    print("\n" + "-" * 62)
    print("SEARCH CONFIGURATION:")

    # Show location in friendly format
    location_name = config.get('location_name', config['country'])
    print(f"  Location:    {location_name}")

    # Show keywords (or indicate if browsing by filters only)
    if config.get('query'):
        print(f"  Keywords:    {config['query']}")
    else:
        print(f"  Keywords:    (none - browsing by filters)")

    print(f"  Ad Type:     {config['ad_type']}")
    print(f"  Status:      {config['status']}")
    print(f"  Platforms:   {config['platforms']}")
    print(f"  Media:       {config['media']}")
    if config.get('start_date'):
        print(f"  Date Range:  {config['start_date']} to {config.get('end_date', 'present')}")
    print(f"  Max Results: {config['count']}")
    print("-" * 62)


def print_summary(total: int, new: int, duplicates: int, output_path: Path = None):
    """Print results summary."""
    print("\n" + "-" * 62)
    print("RESULTS SUMMARY:")
    print(f"  Total ads found:       {total}")
    print(f"  New advertisers:       {new}  {'-> saved' if output_path else ''}")
    print(f"  Already in database:   {duplicates}  (skipped)")
    if output_path:
        print(f"  Output file:           {output_path}")
    print("-" * 62 + "\n")


def prompt_choice(prompt: str, options: Dict, default: str = '1') -> str:
    """Prompt user for numbered choice."""
    print(f"\n{prompt}:")
    for key, (value, label) in options.items():
        default_marker = " (default)" if key == default else ""
        print(f"  [{key}] {label}{default_marker}")

    choice = input(f"Select [{default}]: ").strip() or default
    return options.get(choice, options[default])[0]


def prompt_location() -> Tuple[str, str]:
    """Prompt user for location selection.

    Note: FB Ad Library only supports country-level filtering.
    For city/state targeting, use keywords in the search query.

    Returns:
        Tuple of (country_code, location_name)
    """
    print("\nCountry (FB Ad Library only supports country-level filtering):")
    print("  [1] United States")
    print("  [2] United Kingdom")
    print("  [3] Canada")
    print("  [4] Australia")
    print("  [5] Germany")
    print("  [6] Spain")
    print("  [7] Mexico")
    print("  [8] France")
    print("  [9] Brazil")
    print("  [10] Other (enter ISO code)")

    choice = input("Select [1]: ").strip() or '1'

    if choice == '10':
        # Other country
        country = input("Enter ISO country code (e.g., IT, JP, NL): ").strip().upper() or 'US'
        return country, country
    else:
        # Preset selection
        preset_key = LOCATION_MENU.get(choice, 'us')
        if preset_key in LOCATION_PRESETS:
            preset = LOCATION_PRESETS[preset_key]
            return preset['country'], preset['name']
        # Default to US
        return 'US', 'United States'


def interactive_mode() -> Dict:
    """Run interactive mode to collect search parameters."""
    print_banner()

    # Location selection first (country-level only)
    country, location_name = prompt_location()

    # Ad Type
    ad_type = prompt_choice("Ad Type", AD_TYPES, '1')

    # Keywords (optional) - location is handled via country filter, not keywords
    print("\nSearch Keywords (optional, press Enter to browse all by filters):")
    print("  Tip: Use broad terms first (e.g., 'real estate') for more results.")
    print("       Avoid city names in keywords - use country filter instead.")
    query = input("Keywords: ").strip()

    status = prompt_choice("Active Status", STATUS_OPTIONS, '1')

    print("\nPlatforms (comma-separated, or 'all'):")
    print(f"  {', '.join(PLATFORMS)}")
    platforms_input = input("Select [all]: ").strip() or 'all'
    platforms = platforms_input if platforms_input != 'all' else ','.join(PLATFORMS)

    media = prompt_choice("Media Type", MEDIA_TYPES, '1')

    print("\nDate Range:")
    start_date = input("  Start date (YYYY-MM-DD, or blank): ").strip() or None
    end_date = input("  End date (YYYY-MM-DD, or blank): ").strip() or None

    count = input("\nMax Results [500]: ").strip()
    count = int(count) if count.isdigit() else 500

    return {
        'query': query,
        'country': country,
        'location_name': location_name,
        'ad_type': ad_type,
        'status': status,
        'platforms': platforms,
        'media': media,
        'start_date': start_date,
        'end_date': end_date,
        'count': count,
    }


def modification_loop(client, config: Dict, history: Dict, force: bool = False) -> Tuple[Dict, bool]:
    """Loop allowing user to modify search and preview counts."""
    while True:
        print_config(config)

        # Show search summary
        query_display = config.get('query') or '(no keywords - browsing by filters)'
        location_display = config.get('location_name', config.get('country', 'US'))
        ad_type_display = config.get('ad_type', 'all')

        print(f"\nReady to scrape:")
        print(f"  Query: {query_display}")
        print(f"  Location: {location_display}")
        print(f"  Ad Type: {ad_type_display}")

        print("\nOptions:")
        print(f"  [1] Start download (up to {config['count']} ads)")
        print("  [2] Modify search keywords")
        print("  [3] Change max results")
        print("  [4] Cancel")

        try:
            choice = input("\nSelect [1]: ").strip() or '1'
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled.")
            return config, False

        if choice == '1':
            return config, True
        elif choice == '2':
            new_query = input("\nNew keywords (or blank to clear): ").strip()
            config['query'] = new_query
        elif choice == '3':
            new_count = input("\nNew max results: ").strip()
            if new_count.isdigit():
                config['count'] = int(new_count)
        elif choice == '4':
            return config, False
        else:
            print("Invalid choice")


def print_locations():
    """Print available location presets."""
    print("\nAvailable Location Presets:")
    print("-" * 40)
    print("\nUS States:")
    for key in ['us', 'us-fl', 'us-ca', 'us-tx', 'us-ny']:
        preset = LOCATION_PRESETS[key]
        hint = f" (adds '{preset['query_hint']}' keyword)" if preset['query_hint'] else ""
        print(f"  {key:12} {preset['name']}{hint}")

    print("\nUS Metros:")
    for key in ['miami', 'la', 'nyc', 'chicago', 'houston', 'dallas']:
        preset = LOCATION_PRESETS[key]
        print(f"  {key:12} {preset['name']} (adds '{preset['query_hint']}' keyword)")

    print("\nOther Countries:")
    for key in ['uk', 'canada', 'australia', 'germany', 'spain', 'mexico']:
        preset = LOCATION_PRESETS[key]
        print(f"  {key:12} {preset['name']}")

    print("\nUsage:")
    print("  python scripts/fb_ads_scraper.py --location miami --ad-type housing_ads")
    print("  python scripts/fb_ads_scraper.py --location us-fl --query 'real estate'")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Facebook Ad Library using Apify',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--query', type=str, default='',
                        help='Search keywords (optional - can browse by category/location alone)')
    parser.add_argument('--location', type=str,
                        help='Location preset (miami, la, us-fl, uk, etc.) Use --list-locations to see all')
    parser.add_argument('--list-locations', action='store_true',
                        help='List available location presets')
    parser.add_argument('--country', type=str, default='US', help='ISO country code (default: US)')
    parser.add_argument('--ad-type', type=str, default='all',
                        choices=['all', 'political_and_issue_ads', 'housing_ads', 'employment_ads', 'credit_ads'],
                        help='Ad type filter')
    parser.add_argument('--status', type=str, default='active',
                        choices=['active', 'inactive', 'all'],
                        help='Ad status filter')
    parser.add_argument('--platforms', type=str, default='all',
                        help='Comma-separated platforms or "all"')
    parser.add_argument('--media', type=str, default='all',
                        choices=['all', 'video', 'image'],
                        help='Media type filter')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--count', type=int, default=500, help='Max results to fetch (default: 500)')
    parser.add_argument('--output', type=str, help='Output CSV path')
    parser.add_argument('--dry-run', action='store_true', help='Show config without running')
    parser.add_argument('--force', action='store_true', help='Ignore deduplication')
    parser.add_argument('--clear-history', action='store_true', help='Clear scrape history')
    parser.add_argument('--interactive', action='store_true', help='Run interactive mode')

    args = parser.parse_args()

    # List locations if requested
    if args.list_locations:
        print_locations()
        return

    # Clear history if requested
    if args.clear_history:
        if HISTORY_FILE.exists():
            HISTORY_FILE.unlink()
            print("History cleared.")
        else:
            print("No history file found.")
        return

    # Determine mode - interactive if explicitly requested or no CLI args provided
    has_cli_args = args.location or args.query or args.ad_type != 'all'
    if args.interactive or not has_cli_args:
        config = interactive_mode()
    else:
        # Build config from CLI args
        country = args.country
        location_name = args.country
        query = args.query or ''

        # Apply location preset if specified
        if args.location:
            preset_key = args.location.lower()
            if preset_key in LOCATION_PRESETS:
                preset = LOCATION_PRESETS[preset_key]
                country = preset['country']
                location_name = preset['name']
                # Combine preset query hint with user query
                if preset['query_hint']:
                    if query:
                        query = f"{preset['query_hint']} {query}"
                    else:
                        query = preset['query_hint']
            else:
                print(f"WARNING: Unknown location preset '{args.location}'. Use --list-locations to see options.")
                print(f"Falling back to country code: {args.country}")

        config = {
            'query': query,
            'country': country,
            'location_name': location_name,
            'ad_type': args.ad_type,
            'status': args.status,
            'platforms': args.platforms,
            'media': args.media,
            'start_date': args.start_date,
            'end_date': args.end_date,
            'count': args.count,
        }

    # Dry run - just show config
    if args.dry_run:
        print_config(config)
        print("\nDRY RUN - No scraping performed")
        return

    # Check Apify token
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not found in environment.")
        print("Add it to your .env file.")
        sys.exit(1)

    # Get client
    client = get_apify_client()
    if not client:
        print("ERROR: Could not initialize Apify client")
        sys.exit(1)

    # Load history
    history = load_history()

    # Modification loop (interactive mode)
    if args.interactive or not has_cli_args:
        config, proceed = modification_loop(client, config, history, args.force)
        if not proceed:
            print("\nCancelled.")
            return
    else:
        print_config(config)

    # Scrape
    print("\nScraping ads...")
    ads, error = scrape_ads(client, config, config['count'])

    if error:
        print(f"ERROR: {error}")
        sys.exit(1)

    if not ads:
        print("No ads found matching your criteria.")
        return

    print(f"Found {len(ads)} ads")

    # Filter duplicates
    new_ads, duplicates = filter_duplicates(ads, history, args.force)

    if not new_ads:
        print_summary(len(ads), 0, duplicates)
        print("All advertisers already in database. Use --force to re-download.")
        return

    # Convert and save
    df = convert_to_pipeline_format(new_ads)

    output_path = Path(args.output) if args.output else None
    output_path = save_results(df, output_path)

    # Update history
    history = update_history(history, new_ads, config, duplicates)
    save_history(history)

    # Summary
    print_summary(len(ads), len(new_ads), duplicates, output_path)

    print(f"Output ready for pipeline:")
    print(f"  python run_pipeline.py --input {output_path}")


if __name__ == '__main__':
    main()
