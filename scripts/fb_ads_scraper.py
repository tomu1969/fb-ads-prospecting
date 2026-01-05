"""
Facebook Ad Library Scraper

Scrapes Facebook advertisers using the Apify actor dz_omar/facebook-ads-scraper-pro.
Supports interactive mode with preview counts and keyword modification.

Usage:
    # Interactive mode
    python scripts/fb_ads_scraper.py

    # CLI mode
    python scripts/fb_ads_scraper.py --query "real estate miami" --count 50

    # Dry run
    python scripts/fb_ads_scraper.py --query "test" --dry-run
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
APIFY_ACTOR_ID = 'dz_omar/facebook-ads-scraper-pro'

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


def build_search_url(query: str, country: str = 'US', ad_type: str = 'all',
                     status: str = 'active', media_type: str = 'all',
                     platforms: str = 'all', start_date: str = None,
                     end_date: str = None) -> str:
    """Build Facebook Ad Library search URL."""
    base = 'https://www.facebook.com/ads/library/'
    params = [
        f'active_status={status}',
        f'ad_type={ad_type}',
        f'country={country}',
        f'q={quote_plus(query)}',
        'search_type=keyword_unordered',
        f'media_type={media_type}',
    ]

    if start_date:
        params.append(f'start_date_min={start_date}')
    if end_date:
        params.append(f'start_date_max={end_date}')

    return base + '?' + '&'.join(params)


def get_preview_count(client, config: Dict) -> Tuple[int, Optional[str]]:
    """Get preview count of ads matching criteria without full download."""
    url = build_search_url(
        query=config['query'],
        country=config['country'],
        ad_type=config['ad_type'],
        status=config['status'],
        media_type=config['media'],
        start_date=config.get('start_date'),
        end_date=config.get('end_date')
    )

    try:
        run_input = {
            'urls': [{'url': url}],
            'count': 1,  # Just get count
            'period': '',
            'scrapePageAds.activeStatus': config['status'],
            'scrapePageAds.countryCode': config['country'],
        }

        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input, timeout_secs=120)

        if run.get('status') == 'SUCCEEDED':
            # Try to get total from dataset
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                # Estimate based on pagination info if available
                return len(items) * 100, None  # Rough estimate

        return 0, "Could not get preview count"
    except Exception as e:
        return 0, str(e)


def scrape_ads(client, config: Dict, limit: int = 100) -> Tuple[List[Dict], Optional[str]]:
    """Scrape ads from Facebook Ad Library."""
    url = build_search_url(
        query=config['query'],
        country=config['country'],
        ad_type=config['ad_type'],
        status=config['status'],
        media_type=config['media'],
        start_date=config.get('start_date'),
        end_date=config.get('end_date')
    )

    try:
        run_input = {
            'urls': [{'url': url}],
            'count': limit,
            'period': '',
            'scrapePageAds.activeStatus': config['status'],
            'scrapePageAds.countryCode': config['country'],
            'proxyConfig': {
                'useApifyProxy': True,
            },
        }

        print(f"\nStarting Apify actor: {APIFY_ACTOR_ID}")
        print(f"Search URL: {url[:80]}...")

        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)

        if run.get('status') == 'SUCCEEDED':
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                return items, None
            return [], "No dataset returned"
        else:
            return [], f"Actor run failed: {run.get('status', 'UNKNOWN')}"

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
        'query': config['query'],
        'date': today,
        'count': len(ads) + duplicates,
        'new_advertisers': len(ads),
        'duplicates_skipped': duplicates
    })

    return history


def convert_to_pipeline_format(ads: List[Dict]) -> pd.DataFrame:
    """Convert Apify results to pipeline-compatible format."""
    rows = []

    # Group by advertiser
    advertisers = {}
    for ad in ads:
        page_id = str(ad.get('page_id', ad.get('pageId', '')))
        page_name = ad.get('page_name', ad.get('pageName', 'Unknown'))

        if page_id not in advertisers:
            advertisers[page_id] = {
                'page_name': page_name,
                'page_id': page_id,
                'ads': [],
                'platforms': set(),
                'first_ad_date': None,
                'is_active': False,
            }

        advertisers[page_id]['ads'].append(ad)

        # Collect platforms
        platforms = ad.get('platforms', ad.get('publisherPlatform', []))
        if isinstance(platforms, list):
            advertisers[page_id]['platforms'].update(platforms)
        elif platforms:
            advertisers[page_id]['platforms'].add(platforms)

        # Track first ad date
        start_date = ad.get('ad_delivery_start_time', ad.get('startDate'))
        if start_date:
            if not advertisers[page_id]['first_ad_date'] or start_date < advertisers[page_id]['first_ad_date']:
                advertisers[page_id]['first_ad_date'] = start_date

        # Check if any ad is active
        if ad.get('is_active', ad.get('isActive', False)):
            advertisers[page_id]['is_active'] = True

    # Convert to rows
    for page_id, data in advertisers.items():
        ad_texts = []
        for ad in data['ads']:
            body = ad.get('ad_creative_body', ad.get('adCreativeBody', ''))
            if body:
                ad_texts.append(body)

        rows.append({
            'page_name': data['page_name'],
            'ad_count': len(data['ads']),
            'total_page_likes': 0,  # Not always available
            'ad_texts': json.dumps(ad_texts[:10]),  # Limit to 10
            'platforms': json.dumps(list(data['platforms'])),
            'is_active': data['is_active'],
            'first_ad_date': data['first_ad_date'],
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
    print(f"  Keywords:    {config['query']}")
    print(f"  Country:     {config['country']}")
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


def interactive_mode() -> Dict:
    """Run interactive mode to collect search parameters."""
    print_banner()

    # Collect parameters
    query = input("Search Keywords (required): ").strip()
    if not query:
        print("ERROR: Keywords are required")
        sys.exit(1)

    country = input("\nCountry [US]: ").strip().upper() or 'US'

    ad_type = prompt_choice("Ad Type", AD_TYPES, '1')
    status = prompt_choice("Active Status", STATUS_OPTIONS, '1')

    print("\nPlatforms (comma-separated, or 'all'):")
    print(f"  {', '.join(PLATFORMS)}")
    platforms_input = input("Select [all]: ").strip() or 'all'
    platforms = platforms_input if platforms_input != 'all' else ','.join(PLATFORMS)

    media = prompt_choice("Media Type", MEDIA_TYPES, '1')

    print("\nDate Range:")
    start_date = input("  Start date (YYYY-MM-DD, or blank): ").strip() or None
    end_date = input("  End date (YYYY-MM-DD, or blank): ").strip() or None

    count = input("\nMax Results [100]: ").strip()
    count = int(count) if count.isdigit() else 100

    return {
        'query': query,
        'country': country,
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

        print("\nChecking ad count...")
        # Note: Preview count is an estimate - actual scraping gives exact count

        print("\nOptions:")
        print(f"  [1] Download up to {config['count']} ads")
        print("  [2] Modify search keywords")
        print("  [3] Change max results")
        print("  [4] Cancel")

        choice = input("\nSelect [1]: ").strip() or '1'

        if choice == '1':
            return config, True
        elif choice == '2':
            new_query = input("\nNew keywords: ").strip()
            if new_query:
                config['query'] = new_query
        elif choice == '3':
            new_count = input("\nNew max results: ").strip()
            if new_count.isdigit():
                config['count'] = int(new_count)
        elif choice == '4':
            return config, False
        else:
            print("Invalid choice")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Facebook Ad Library using Apify',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--query', type=str, help='Search keywords')
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
    parser.add_argument('--count', type=int, default=100, help='Max results to fetch')
    parser.add_argument('--output', type=str, help='Output CSV path')
    parser.add_argument('--dry-run', action='store_true', help='Show config without running')
    parser.add_argument('--force', action='store_true', help='Ignore deduplication')
    parser.add_argument('--clear-history', action='store_true', help='Clear scrape history')
    parser.add_argument('--interactive', action='store_true', help='Run interactive mode')

    args = parser.parse_args()

    # Clear history if requested
    if args.clear_history:
        if HISTORY_FILE.exists():
            HISTORY_FILE.unlink()
            print("History cleared.")
        else:
            print("No history file found.")
        return

    # Determine mode
    if args.interactive or not args.query:
        config = interactive_mode()
    else:
        config = {
            'query': args.query,
            'country': args.country,
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
    if args.interactive or not args.query:
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
