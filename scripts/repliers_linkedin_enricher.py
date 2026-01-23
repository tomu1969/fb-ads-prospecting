"""
Repliers LinkedIn Enricher - Find LinkedIn profiles for real estate agents

Uses Exa API to search for personal LinkedIn profiles using agent name + brokerage.
Falls back to Apify actor when Exa is exhausted.

Input: output/repliers/top_agents_2025_enriched.csv
Output: Same file with linkedin_profile column added

Usage:
    python scripts/repliers_linkedin_enricher.py --all
    python scripts/repliers_linkedin_enricher.py --limit 10
    python scripts/repliers_linkedin_enricher.py --dry-run
"""

import os
import re
import sys
import json
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List
from difflib import SequenceMatcher

import pandas as pd
import requests
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
        logging.FileHandler('repliers_linkedin_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/repliers/top_agents_2025_enriched.csv"

# Exa API
EXA_API_KEY = os.getenv('EXA_API_KEY')
EXA_API_URL = "https://api.exa.ai/search"

# Apify API (fallback)
APIFY_API_KEY = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
APIFY_LINKEDIN_ACTOR = "harvestapi/linkedin-profile-search-by-name"

# Track API state
_exa_exhausted = False

# Company noise words to remove
COMPANY_NOISE_WORDS = [
    'realtor', 'realty', 'real estate', 'properties', 'property',
    'group', 'team', 'associates', 'llc', 'inc', 'corp', 'co',
    'investments', 'capital', 'partners', 'agency', 'international',
    'bhhs', 'berkshire hathaway', 'coldwell banker', 'keller williams',
    'compass', 'sotheby', 'douglas elliman', 'exp', 'century 21', 're/max'
]


def clean_company_name(company: str) -> str:
    """Remove noise words from company/brokerage name."""
    if not company:
        return ""

    cleaned = company.lower().strip()

    for word in COMPANY_NOISE_WORDS:
        cleaned = re.sub(rf'\b{re.escape(word)}\b', '', cleaned, flags=re.IGNORECASE)

    # Clean up extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned if len(cleaned) > 2 else ""


def extract_username_from_url(url: str) -> str:
    """Extract LinkedIn username from profile URL."""
    if not url:
        return ""

    match = re.search(r'linkedin\.com/in/([^/?#]+)', url, re.IGNORECASE)
    if match:
        return match.group(1).lower().rstrip('/')
    return ""


def name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity ratio between two names."""
    if not name1 or not name2:
        return 0.0

    n1 = re.sub(r'[^a-z\s]', '', name1.lower())
    n2 = re.sub(r'[^a-z\s]', '', name2.lower())

    return SequenceMatcher(None, n1, n2).ratio()


def search_linkedin_exa(name: str, company: str = "") -> Optional[str]:
    """Search for LinkedIn profile using Exa API."""
    global _exa_exhausted

    if _exa_exhausted or not EXA_API_KEY:
        return None

    # Build search query
    clean_company = clean_company_name(company)
    if clean_company:
        query = f'site:linkedin.com/in "{name}" "{clean_company}"'
    else:
        query = f'site:linkedin.com/in "{name}" real estate'

    try:
        headers = {
            "x-api-key": EXA_API_KEY,
            "Content-Type": "application/json"
        }

        payload = {
            "query": query,
            "numResults": 5,
            "type": "keyword",
            "contents": {
                "text": {"maxCharacters": 500}
            }
        }

        response = requests.post(EXA_API_URL, headers=headers, json=payload, timeout=30)

        if response.status_code == 402:
            logger.warning("Exa API credits exhausted, switching to Apify fallback")
            _exa_exhausted = True
            return None

        if response.status_code != 200:
            logger.debug(f"Exa API error: {response.status_code}")
            return None

        data = response.json()
        results = data.get('results', [])

        if not results:
            return None

        # Score and rank results
        name_parts = [p.lower() for p in name.split() if len(p) > 1]
        best_match = None
        best_score = 0

        for result in results:
            url = result.get('url', '')
            text = result.get('text', '')

            if '/in/' not in url:
                continue

            username = extract_username_from_url(url)
            if not username:
                continue

            score = 0

            # Check if name parts appear in username
            username_clean = username.replace('-', ' ').replace('_', ' ').replace('.', ' ')
            for part in name_parts:
                if part in username_clean:
                    score += 30

            # Check text content for name
            if text:
                text_lower = text.lower()
                for part in name_parts:
                    if part in text_lower:
                        score += 15

                # Bonus for real estate keywords
                if any(kw in text_lower for kw in ['real estate', 'realtor', 'broker', 'agent']):
                    score += 10

            if score > best_score:
                best_score = score
                best_match = url

        # Require minimum score
        if best_score >= 30:
            # Clean URL
            if '?' in best_match:
                best_match = best_match.split('?')[0]
            return best_match

        return None

    except Exception as e:
        logger.debug(f"Exa search error for {name}: {e}")
        return None


def search_linkedin_apify(name: str, company: str = "") -> Optional[str]:
    """Search for LinkedIn profile using Apify actor (fallback)."""
    if not APIFY_API_KEY:
        return None

    try:
        from apify_client import ApifyClient
        client = ApifyClient(APIFY_API_KEY)

        # Parse name into first/last
        parts = name.split()
        if len(parts) < 2:
            return None

        first_name = parts[0]
        last_name = ' '.join(parts[1:])

        run_input = {
            "firstName": first_name,
            "lastName": last_name,
            "company": company or "",
        }

        run = client.actor(APIFY_LINKEDIN_ACTOR).call(
            run_input=run_input,
            timeout_secs=60
        )

        if run.get('status') != 'SUCCEEDED':
            return None

        items = list(client.dataset(run['defaultDatasetId']).iterate_items())

        if not items:
            return None

        # Find best match
        name_parts = [p.lower() for p in name.split() if len(p) > 1]
        best_match = None
        best_score = 0

        for item in items:
            url = item.get('profileUrl') or item.get('url') or ''
            profile_name = item.get('name') or item.get('fullName') or ''

            if '/in/' not in url:
                continue

            score = 0

            # Check name match
            if profile_name:
                score += name_similarity(name, profile_name) * 50

            # Check username
            username = extract_username_from_url(url)
            if username:
                username_clean = username.replace('-', ' ').replace('_', ' ')
                for part in name_parts:
                    if part in username_clean:
                        score += 20

            if score > best_score:
                best_score = score
                best_match = url

        if best_score >= 30:
            if '?' in best_match:
                best_match = best_match.split('?')[0]
            return best_match

        return None

    except Exception as e:
        logger.debug(f"Apify search error for {name}: {e}")
        return None


def find_linkedin_profile(name: str, company: str = "") -> Dict:
    """Find LinkedIn profile using Exa (primary) or Apify (fallback)."""
    result = {
        "linkedin_profile": "",
        "linkedin_source": "",
    }

    # Try Exa first
    profile_url = search_linkedin_exa(name, company)
    if profile_url:
        result["linkedin_profile"] = profile_url
        result["linkedin_source"] = "exa"
        return result

    # Fallback to Apify
    profile_url = search_linkedin_apify(name, company)
    if profile_url:
        result["linkedin_profile"] = profile_url
        result["linkedin_source"] = "apify"
        return result

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
    print("REPLIERS LINKEDIN ENRICHER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Find LinkedIn profiles for Repliers agents')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--all', action='store_true', help='Process all agents')
    parser.add_argument('--limit', type=int, help='Limit agents to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--reset', action='store_true', help='Reset existing LinkedIn data')

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

    # Initialize columns
    if 'linkedin_profile' not in df.columns or args.reset:
        df['linkedin_profile'] = ""
        df['linkedin_source'] = ""

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has LinkedIn profile
        existing = row.get('linkedin_profile', '')
        if pd.notna(existing) and str(existing).strip() and str(existing).strip().lower() != 'nan' and not args.reset:
            continue

        agent_name = str(row.get('agent_name', '')).strip()
        if not agent_name or len(agent_name) < 3:
            continue

        brokerage = str(row.get('brokerage', '')).strip()
        rows_to_process.append((idx, agent_name, brokerage))

    # Apply limit
    if args.limit:
        rows_to_process = rows_to_process[:args.limit]
    elif not args.all:
        rows_to_process = rows_to_process[:5]
        print("Test mode: Processing first 5 agents (use --all for all)")

    print(f"Agents to process: {len(rows_to_process)}")

    # Check API keys
    if not EXA_API_KEY:
        print("\nWARNING: EXA_API_KEY not found")
    if not APIFY_API_KEY:
        print("WARNING: APIFY_API_TOKEN not found (fallback unavailable)")

    if args.dry_run:
        print(f"\n[DRY RUN] Would search LinkedIn for {len(rows_to_process)} agents:")
        for idx, name, company in rows_to_process[:10]:
            print(f"  - {name} ({company})")
        if len(rows_to_process) > 10:
            print(f"  ... and {len(rows_to_process) - 10} more")
        return 0

    if not rows_to_process:
        print("No agents to process")
        return 0

    # Process agents
    stats = {"found": 0, "not_found": 0, "errors": 0, "exa": 0, "apify": 0}
    BATCH_SIZE = 10
    output_path = str(input_path)

    for i, (idx, agent_name, brokerage) in enumerate(tqdm(rows_to_process, desc="Finding LinkedIn profiles")):
        try:
            result = find_linkedin_profile(agent_name, brokerage)

            df.at[idx, 'linkedin_profile'] = result['linkedin_profile']
            df.at[idx, 'linkedin_source'] = result['linkedin_source']

            if result['linkedin_profile']:
                stats["found"] += 1
                if result['linkedin_source'] == 'exa':
                    stats["exa"] += 1
                else:
                    stats["apify"] += 1
                logger.debug(f"Found: {agent_name} -> {result['linkedin_profile']}")
            else:
                stats["not_found"] += 1

            # Brief delay to be polite to APIs
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error processing {agent_name}: {e}")
            stats["errors"] += 1

        # Incremental save
        if (i + 1) % BATCH_SIZE == 0:
            save_incremental(df, output_path, f"linkedin_batch_{(i + 1) // BATCH_SIZE}")

    # Final save
    df.to_csv(input_path, index=False)

    # Summary
    print(f"\n{'='*60}")
    print("LINKEDIN ENRICHMENT RESULTS")
    print(f"{'='*60}")
    print(f"  Profiles found: {stats['found']}")
    print(f"    - via Exa: {stats['exa']}")
    print(f"    - via Apify: {stats['apify']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Errors: {stats['errors']}")
    print(f"\nSaved to: {input_path}")

    # Show sample results
    found_df = df[df['linkedin_profile'].notna() & (df['linkedin_profile'] != '')]
    if len(found_df) > 0:
        print(f"\nSample results:")
        for _, row in found_df.head(5).iterrows():
            print(f"  {row['agent_name']}: {row['linkedin_profile']}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
