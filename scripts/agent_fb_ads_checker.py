"""
Agent Facebook Ads Checker

Rapid loop to check if individual MLS agents (not brokerages) run Facebook ads.
Uses FB Ads Library via Apify to search by agent name.

Usage:
    python scripts/agent_fb_ads_checker.py --csv output/mls_miami_agents.csv --limit 20
"""

import os
import sys
import csv
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'output'

APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
APIFY_ACTOR_ID = 'curious_coder/facebook-ads-library-scraper'


def get_apify_client():
    """Get Apify client."""
    try:
        from apify_client import ApifyClient
        if not APIFY_API_TOKEN:
            print("ERROR: APIFY_API_TOKEN not found in environment")
            return None
        return ApifyClient(APIFY_API_TOKEN)
    except ImportError:
        print("ERROR: apify-client not installed. Run: pip install apify-client")
        return None


def build_search_url(query: str, country: str = 'US') -> str:
    """Build Facebook Ad Library search URL for an agent name."""
    base = 'https://www.facebook.com/ads/library/'
    params = [
        'active_status=active',
        'ad_type=all',
        f'country={country}',
        'search_type=keyword_unordered',
        'media_type=all',
        f'q={quote_plus(query)}',
    ]
    return base + '?' + '&'.join(params)


def check_agent_ads(client, agent_name: str, timeout: int = 60) -> dict:
    """Check if an agent has Facebook ads.

    Returns dict with:
        - has_ads: bool
        - ad_count: int
        - page_names: list of advertiser page names found
        - likely_match: bool - True if page name contains agent name parts
        - error: str or None
    """
    result = {
        'agent_name': agent_name,
        'has_ads': False,
        'ad_count': 0,
        'page_names': [],
        'likely_match': False,
        'error': None,
    }

    try:
        search_url = build_search_url(agent_name)

        run_input = {
            'urls': [{'url': search_url}],
            'maxAds': 10,  # Get a few more to find page names
        }

        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input, timeout_secs=timeout)

        status = run.get('status')
        if status in ['SUCCEEDED', 'TIMED-OUT']:
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                result['ad_count'] = len(items)
                result['has_ads'] = len(items) > 0

                # Extract unique page names and check for matches
                page_names = set()
                name_parts = agent_name.lower().split()
                for item in items:
                    # Try different field names for advertiser
                    page_name = item.get('pageName') or item.get('page_name') or item.get('advertiserName') or ''
                    if page_name:
                        page_names.add(page_name)
                        # Check if agent name parts appear in page name
                        page_lower = page_name.lower()
                        if any(part in page_lower for part in name_parts if len(part) > 2):
                            result['likely_match'] = True

                result['page_names'] = list(page_names)
        else:
            result['error'] = f"Actor status: {status}"

    except Exception as e:
        result['error'] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description='Check MLS agents for Facebook ads')
    parser.add_argument('--csv', default='output/mls_miami_agents.csv', help='Input CSV with agent_name column')
    parser.add_argument('--limit', type=int, default=10, help='Number of agents to check')
    parser.add_argument('--output', default='output/agent_fb_ads_check.csv', help='Output CSV path')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be checked without calling API')
    args = parser.parse_args()

    # Read agent names
    csv_path = BASE_DIR / args.csv if not args.csv.startswith('/') else Path(args.csv)

    agents = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'agent_name' in row:
                agents.append({
                    'agent_name': row['agent_name'],
                    'brokerage': row.get('brokerage', ''),
                    'agent_email': row.get('agent_email', ''),
                })

    print(f"Found {len(agents)} agents in CSV")
    agents = agents[:args.limit]
    print(f"Checking {len(agents)} agents for Facebook ads...")

    if args.dry_run:
        print("\n[DRY RUN] Would check these agents:")
        for i, agent in enumerate(agents, 1):
            url = build_search_url(agent['agent_name'])
            print(f"  {i}. {agent['agent_name']} ({agent['brokerage']})")
            print(f"     URL: {url}")
        return

    # Get Apify client
    client = get_apify_client()
    if not client:
        sys.exit(1)

    # Check each agent
    results = []
    for i, agent in enumerate(agents, 1):
        print(f"\n[{i}/{len(agents)}] Checking: {agent['agent_name']}")

        result = check_agent_ads(client, agent['agent_name'])
        result['brokerage'] = agent['brokerage']
        result['agent_email'] = agent['agent_email']
        results.append(result)

        if result['has_ads']:
            match_str = " [LIKELY MATCH]" if result.get('likely_match') else ""
            print(f"  ✓ HAS ADS (found {result['ad_count']} ads){match_str}")
            if result['page_names']:
                print(f"    Pages: {', '.join(result['page_names'][:3])}")  # Limit to 3
        else:
            print(f"  ✗ No ads found")
        if result['error']:
            print(f"  Error: {result['error']}")

        # Small delay to avoid rate limiting
        if i < len(agents):
            time.sleep(1)

    # Save results
    output_path = BASE_DIR / args.output if not args.output.startswith('/') else Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['agent_name', 'brokerage', 'agent_email', 'has_ads', 'ad_count', 'likely_match', 'page_names', 'error'])
        writer.writeheader()
        for r in results:
            r['page_names'] = '|'.join(r['page_names']) if r['page_names'] else ''
            writer.writerow(r)

    print(f"\n{'='*60}")
    print("RESULTS SUMMARY")
    print(f"{'='*60}")

    with_ads = sum(1 for r in results if r['has_ads'])
    likely_matches = sum(1 for r in results if r.get('likely_match'))
    print(f"Agents checked: {len(results)}")
    print(f"Agents with FB ads found: {with_ads} ({100*with_ads/len(results):.0f}%)")
    print(f"Likely personal ads (name match): {likely_matches}")
    print(f"Agents without ads: {len(results) - with_ads}")
    print(f"\nResults saved to: {output_path}")


if __name__ == '__main__':
    main()
