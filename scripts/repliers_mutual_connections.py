"""
Repliers Mutual Connections Finder - Find LinkedIn mutual connections for entry path optimization

Uses Apify actor saswave/linkedin-mutual-connections-parser to find mutual connections
between your LinkedIn profile and target agents.

PREREQUISITES:
1. Export LinkedIn session cookies using EditThisCookie browser extension
2. Save cookies to config/linkedin_cookies.json
3. Set your LinkedIn profile URL in .env as LINKEDIN_MY_PROFILE

Input: output/repliers/top_agents_2025_enriched.csv (with linkedin_profile column)
Output: Same file with mutual_connections columns added

Usage:
    python scripts/repliers_mutual_connections.py --all
    python scripts/repliers_mutual_connections.py --limit 10
    python scripts/repliers_mutual_connections.py --dry-run
"""

import os
import sys
import json
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_mutual_connections.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/repliers/top_agents_2025_enriched.csv"
COOKIES_FILE = "config/linkedin_cookies.json"

# Apify API
APIFY_API_KEY = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
APIFY_MUTUAL_ACTOR = "dead00/linkedin-mutual-connection-analyzer"

# Your LinkedIn profile (for finding mutual connections)
MY_LINKEDIN_PROFILE = os.getenv('LINKEDIN_MY_PROFILE', '')


def load_linkedin_cookies() -> Optional[List]:
    """Load LinkedIn session cookies from file as JSON array."""
    cookies_path = BASE_DIR / COOKIES_FILE

    if not cookies_path.exists():
        logger.warning(f"Cookies file not found: {cookies_path}")
        logger.info("To export cookies:")
        logger.info("1. Install EditThisCookie browser extension")
        logger.info("2. Log into LinkedIn")
        logger.info("3. Click EditThisCookie → Export → Copy")
        logger.info(f"4. Save to {cookies_path}")
        return None

    try:
        with open(cookies_path, 'r') as f:
            cookies = json.load(f)

        # Actor expects cookies as JSON array
        if isinstance(cookies, list):
            return cookies
        else:
            logger.error("Cookies must be a JSON array (exported from EditThisCookie)")
            return None

    except Exception as e:
        logger.error(f"Error loading cookies: {e}")
        return None


def get_mutual_connections_apify(target_url: str, cookies: List) -> Dict:
    """
    Find mutual connections using Apify actor.

    Returns:
        Dict with mutual_count, mutual_names (first 5), and raw data
    """
    result = {
        "mutual_count": 0,
        "mutual_names": "",
        "mutual_connections_raw": "",
    }

    if not APIFY_API_KEY:
        logger.error("APIFY_API_KEY not set")
        return result

    if not target_url or '/in/' not in target_url:
        return result

    try:
        from apify_client import ApifyClient
        client = ApifyClient(APIFY_API_KEY)

        # Actor expects 'url' (singular) and 'cookies' (JSON array)
        run_input = {
            "url": target_url,
            "cookies": cookies,
        }

        logger.debug(f"Checking mutual connections for: {target_url}")

        run = client.actor(APIFY_MUTUAL_ACTOR).call(
            run_input=run_input,
            timeout_secs=120
        )

        if run.get('status') != 'SUCCEEDED':
            logger.debug(f"Actor run failed: {run.get('status')}")
            return result

        items = list(client.dataset(run['defaultDatasetId']).iterate_items())

        if not items:
            return result

        # Process results - actor returns mutual connections directly
        all_mutuals = items  # Each item IS a mutual connection

        if all_mutuals:
            result['mutual_count'] = len(all_mutuals)

            # Extract names (first 5)
            names = []
            for conn in all_mutuals[:5]:
                name = conn.get('name') or conn.get('fullName') or conn.get('firstName', '') + ' ' + conn.get('lastName', '')
                name = name.strip()
                if name:
                    names.append(name)
            result['mutual_names'] = "; ".join(names)

            # Store raw JSON for later analysis
            result['mutual_connections_raw'] = json.dumps(all_mutuals[:10])

        return result

    except Exception as e:
        logger.warning(f"Apify error for {target_url}: {e}")
        return result


def save_incremental(df: pd.DataFrame, output_path: str, batch_name: str = "") -> None:
    """Save DataFrame incrementally."""
    try:
        output_path = Path(output_path)
        df.to_csv(output_path, index=False)

        if batch_name:
            backup_dir = output_path.parent / "backups"
            backup_dir.mkdir(exist_ok=True)
            backup_path = backup_dir / f"{output_path.stem}_{batch_name}.csv"
            df.to_csv(backup_path, index=False)
            logger.info(f"Saved: {len(df)} rows (backup: {batch_name})")
    except Exception as e:
        logger.error(f"Save error: {e}")


def main():
    print(f"\n{'='*60}")
    print("REPLIERS MUTUAL CONNECTIONS FINDER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Find mutual LinkedIn connections for Repliers agents')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--all', action='store_true', help='Process all agents with LinkedIn profiles')
    parser.add_argument('--limit', type=int, help='Limit agents to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--reset', action='store_true', help='Reset existing mutual connection data')

    args = parser.parse_args()

    # Resolve path
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = BASE_DIR / args.input

    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return 1

    # Load data
    df = pd.read_csv(input_path)
    print(f"\nLoaded {len(df)} agents from {input_path.name}")

    # Check for LinkedIn profiles
    has_linkedin = df['linkedin_profile'].notna() & (df['linkedin_profile'] != '') & (df['linkedin_profile'].astype(str).str.lower() != 'nan')
    linkedin_count = has_linkedin.sum()
    print(f"Agents with LinkedIn profiles: {linkedin_count}")

    if linkedin_count == 0:
        print("No LinkedIn profiles found. Run repliers_linkedin_enricher.py first.")
        return 1

    # Initialize columns
    if 'mutual_count' not in df.columns or args.reset:
        df['mutual_count'] = 0
        df['mutual_names'] = ""
        df['mutual_connections_raw'] = ""

    # Load cookies
    cookies = load_linkedin_cookies()
    if not cookies and not args.dry_run:
        print("\nERROR: LinkedIn cookies required for mutual connections check")
        print("See instructions above to export cookies.")
        return 1

    # Check API key
    if not APIFY_API_KEY:
        print("\nERROR: APIFY_API_TOKEN not found in .env")
        return 1

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if no LinkedIn profile
        linkedin_url = row.get('linkedin_profile', '')
        if pd.isna(linkedin_url) or not str(linkedin_url).strip() or str(linkedin_url).strip().lower() == 'nan':
            continue

        # Skip if already has mutual connections data (unless reset)
        existing_count = row.get('mutual_count', 0)
        if pd.notna(existing_count) and existing_count > 0 and not args.reset:
            continue

        agent_name = str(row.get('agent_name', '')).strip()
        rows_to_process.append((idx, agent_name, str(linkedin_url).strip()))

    # Apply limit
    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
    elif not args.all:
        rows_to_process = rows_to_process[:3]
        print("Test mode: Processing first 3 agents (use --all for all)")

    print(f"Agents to check for mutual connections: {len(rows_to_process)}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would check mutual connections for {len(rows_to_process)} agents:")
        for idx, name, url in rows_to_process[:10]:
            print(f"  - {name}: {url}")
        if len(rows_to_process) > 10:
            print(f"  ... and {len(rows_to_process) - 10} more")
        return 0

    if not rows_to_process:
        print("No agents to process")
        return 0

    # Process agents
    stats = {"found": 0, "not_found": 0, "errors": 0, "total_mutuals": 0}
    BATCH_SIZE = 5
    output_path = str(input_path)

    print(f"\nNote: Each check uses ~$0.01-0.02 of Apify credits")
    print(f"Estimated cost: ${len(rows_to_process) * 0.015:.2f}")

    for i, (idx, agent_name, linkedin_url) in enumerate(tqdm(rows_to_process, desc="Finding mutual connections")):
        try:
            result = get_mutual_connections_apify(linkedin_url, cookies)

            df.at[idx, 'mutual_count'] = result['mutual_count']
            df.at[idx, 'mutual_names'] = result['mutual_names']
            df.at[idx, 'mutual_connections_raw'] = result['mutual_connections_raw']

            if result['mutual_count'] > 0:
                stats["found"] += 1
                stats["total_mutuals"] += result['mutual_count']
                logger.info(f"Found {result['mutual_count']} mutual(s) for {agent_name}: {result['mutual_names'][:50]}")
            else:
                stats["not_found"] += 1

            # Rate limit to avoid LinkedIn blocks
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error processing {agent_name}: {e}")
            stats["errors"] += 1

        # Incremental save
        if (i + 1) % BATCH_SIZE == 0:
            save_incremental(df, output_path, f"mutual_batch_{(i + 1) // BATCH_SIZE}")

    # Final save
    df.to_csv(input_path, index=False)

    # Summary
    print(f"\n{'='*60}")
    print("MUTUAL CONNECTIONS RESULTS")
    print(f"{'='*60}")
    print(f"  Agents with mutual connections: {stats['found']}")
    print(f"  Total mutual connections found: {stats['total_mutuals']}")
    print(f"  Agents without mutuals: {stats['not_found']}")
    print(f"  Errors: {stats['errors']}")
    print(f"\nSaved to: {input_path}")

    # Show top agents by mutual connections
    df_with_mutuals = df[df['mutual_count'] > 0].sort_values('mutual_count', ascending=False)
    if len(df_with_mutuals) > 0:
        print(f"\nTop agents by mutual connections:")
        for _, row in df_with_mutuals.head(10).iterrows():
            print(f"  {row['mutual_count']:3d} mutuals - {row['agent_name']}")
            mutual_names = row.get('mutual_names', '')
            if pd.notna(mutual_names) and isinstance(mutual_names, str) and mutual_names:
                print(f"       Names: {mutual_names[:60]}...")

    return 0


if __name__ == '__main__':
    sys.exit(main())
