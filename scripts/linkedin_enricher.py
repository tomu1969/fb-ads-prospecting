"""
LinkedIn Profile Enricher - Module 3.9 - Find personal LinkedIn profiles via Exa API

Runs automatically after Contact Name Resolver (Module 3.8) in the pipeline.
Searches for personal LinkedIn profiles (/in/) using contact name + company.

Input: processed/03e_names.csv (from Module 3.8)
Output: processed/03f_linkedin.csv

Usage:
    python scripts/linkedin_enricher.py           # Test mode (3 contacts)
    python scripts/linkedin_enricher.py --all    # Process all contacts
    python scripts/linkedin_enricher.py --csv output/prospects.csv  # Standalone mode
"""

import os
import re
import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Tuple
from difflib import SequenceMatcher

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink
from utils.enrichment_config import should_run_module

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('linkedin_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent

# Pipeline input/output paths
INPUT_BASE = "03e_names.csv"
OUTPUT_BASE = "03f_linkedin.csv"

# Exa API
EXA_API_KEY = os.getenv('EXA_API_KEY')
EXA_API_URL = "https://api.exa.ai/search"

# Invalid names to skip
INVALID_NAMES = {
    'none', 'none none', 'nan', 'n/a', 'null', 'undefined', 'unknown',
    'meet our team', 'social media', 'featured video', 'your information',
    'contact', 'info', 'support', 'team', 'admin'
}

# Company name suffixes to remove for cleaner searches
COMPANY_NOISE_WORDS = [
    'realtor', 'realty', 'real estate', 'properties', 'property',
    'group', 'team', 'associates', 'llc', 'inc', 'corp', 'co',
    'investments', 'investing', 'capital', 'partners', 'agency'
]


def clean_company_name(company: str) -> str:
    """Remove noise words from company name for better search results."""
    if not company:
        return company

    cleaned = company.lower()

    # Remove common suffixes
    for word in COMPANY_NOISE_WORDS:
        # Remove as suffix (with optional punctuation)
        cleaned = re.sub(rf'\s+{word}[.,]?\s*$', '', cleaned, flags=re.IGNORECASE)
        # Remove with "&" prefix like "& Associates"
        cleaned = re.sub(rf'\s+&\s+{word}[.,]?\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove trailing punctuation
    cleaned = cleaned.strip(' .,&-')

    # Title case the result
    return cleaned.title() if cleaned else company


def name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity ratio between two names (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()


def search_exa_linkedin(name: str, company: str = None, num_results: int = 5) -> List[Dict]:
    """
    Search Exa for LinkedIn profiles matching name + company.

    Returns list of results with URLs and text snippets.
    """
    if not EXA_API_KEY:
        logger.warning("EXA_API_KEY not configured")
        return []

    # Build search query targeting LinkedIn personal profiles
    if company:
        cleaned_company = clean_company_name(company)
        query = f'site:linkedin.com/in "{name}" "{cleaned_company}"'
    else:
        query = f'site:linkedin.com/in "{name}"'

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
                "text": {"maxCharacters": 1000}
            }
        }

        response = requests.post(EXA_API_URL, headers=headers, json=payload, timeout=15)

        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            logger.error(f"Exa API error: {response.status_code} - {response.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"Exa search error: {e}")
        return []


def extract_linkedin_profile_url(results: List[Dict], name: str, company: str = None) -> Optional[str]:
    """
    Extract the best matching personal LinkedIn profile URL from Exa results.

    Uses fuzzy name matching and company verification for better accuracy.
    Returns URL like https://linkedin.com/in/username or None.
    """
    if not results:
        return None

    name_parts = name.lower().split()
    first_name = name_parts[0] if name_parts else ''
    last_name = name_parts[-1] if len(name_parts) > 1 else ''

    # Score each result
    scored_results = []

    for result in results:
        url = result.get('url', '')

        # Must be a personal profile (/in/), not company page
        if '/in/' not in url:
            continue
        if '/company/' in url:
            continue

        url_lower = url.lower()
        text_lower = result.get('text', '').lower()
        title_lower = result.get('title', '').lower()

        score = 0

        # Extract username from URL for matching
        username_match = re.search(r'/in/([^/?]+)', url_lower)
        username = username_match.group(1) if username_match else ''

        # Score: Name parts in URL username
        if first_name and first_name in username:
            score += 30
        if last_name and last_name in username:
            score += 30

        # Score: Name parts in profile text
        if first_name and first_name in text_lower:
            score += 15
        if last_name and last_name in text_lower:
            score += 15

        # Score: Fuzzy match on title (usually contains full name)
        if title_lower:
            title_sim = name_similarity(name, title_lower.split('-')[0].strip())
            score += int(title_sim * 40)

        # Score: Company name in profile text (if provided)
        if company:
            cleaned_company = clean_company_name(company).lower()
            if cleaned_company and len(cleaned_company) > 2:
                if cleaned_company in text_lower:
                    score += 25
                elif any(word in text_lower for word in cleaned_company.split() if len(word) > 3):
                    score += 10

        # Minimum threshold - at least first or last name should match somewhere
        if score >= 30:
            scored_results.append((score, url.split('?')[0], result))

    # Sort by score descending
    scored_results.sort(key=lambda x: x[0], reverse=True)

    if scored_results:
        logger.debug(f"Top match for '{name}': score={scored_results[0][0]}, url={scored_results[0][1]}")
        return scored_results[0][1]

    return None


def is_valid_name(name: str) -> bool:
    """Check if name is valid for LinkedIn search."""
    if not name or pd.isna(name):
        return False
    name_lower = str(name).lower().strip()
    if name_lower in INVALID_NAMES:
        return False
    if len(name_lower) < 3:
        return False
    if "'s listings" in name_lower:
        return False
    return True


def enrich_linkedin_profiles(
    csv_path: Path,
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    delay: float = 0.5
) -> Dict:
    """
    Enrich CSV with personal LinkedIn profile URLs.

    Args:
        csv_path: Input CSV with contact_name and page_name columns
        output_path: Output CSV path (default: adds _linkedin suffix)
        limit: Max contacts to process
        dry_run: Preview without API calls
        delay: Delay between API calls (seconds)

    Returns:
        Stats dictionary
    """
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return {'error': str(e)}

    if 'contact_name' not in df.columns or 'page_name' not in df.columns:
        logger.error("CSV must have 'contact_name' and 'page_name' columns")
        return {'error': 'Missing required columns'}

    # Add linkedin_profile column if not exists
    if 'linkedin_profile' not in df.columns:
        df['linkedin_profile'] = ''

    # Stats
    stats = {
        'total': len(df),
        'processed': 0,
        'found': 0,
        'skipped': 0,
        'errors': 0,
        'already_had': 0
    }

    # Filter rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has personal LinkedIn profile
        existing = str(row.get('linkedin_profile', '') or row.get('linkedin_url', ''))
        if existing and '/in/' in existing:
            stats['already_had'] += 1
            continue

        name = str(row.get('contact_name', '')).strip()
        company = str(row.get('page_name', '')).strip()

        if not is_valid_name(name):
            stats['skipped'] += 1
            continue

        rows_to_process.append((idx, name, company))

    # Apply limit
    if limit:
        rows_to_process = rows_to_process[:limit]

    logger.info(f"Processing {len(rows_to_process)} contacts for LinkedIn profiles")

    if dry_run:
        logger.info("DRY RUN - No API calls will be made")
        for idx, name, company in rows_to_process[:10]:
            logger.info(f"  Would search: \"{name}\" at \"{company}\"")
        return stats

    # Process each contact
    for idx, name, company in tqdm(rows_to_process, desc="Finding LinkedIn profiles"):
        try:
            # Search Exa with company name
            results = search_exa_linkedin(name, company)

            # Extract best profile URL
            profile_url = extract_linkedin_profile_url(results, name, company)

            # Fallback: Try name-only search if no results
            if not profile_url:
                logger.debug(f"Trying fallback search for: {name}")
                results_fallback = search_exa_linkedin(name, company=None)
                profile_url = extract_linkedin_profile_url(results_fallback, name, company)
                time.sleep(delay)  # Extra delay for fallback

            if profile_url:
                df.at[idx, 'linkedin_profile'] = profile_url
                stats['found'] += 1
                logger.debug(f"Found: {name} -> {profile_url}")
            else:
                logger.debug(f"Not found: {name} at {company}")

            stats['processed'] += 1

            # Rate limiting
            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error processing {name}: {e}")
            stats['errors'] += 1

    # Save output
    if output_path is None:
        output_path = csv_path.parent / f"{csv_path.stem}_linkedin{csv_path.suffix}"

    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved to {output_path}")

    return stats


def print_summary(stats: Dict):
    """Print enrichment summary."""
    print("\n" + "=" * 60)
    print("LINKEDIN ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"Total contacts:      {stats.get('total', 0)}")
    print(f"Already had profile: {stats.get('already_had', 0)}")
    print(f"Processed:           {stats.get('processed', 0)}")
    print(f"Found profiles:      {stats.get('found', 0)}")
    print(f"Skipped (bad name):  {stats.get('skipped', 0)}")
    print(f"Errors:              {stats.get('errors', 0)}")
    print("=" * 60)


def main():
    """Main function with pipeline and standalone mode support."""

    # Check if module should run based on enrichment config
    if not should_run_module("linkedin_enricher"):
        print(f"\n{'='*60}")
        print("MODULE 3.9: LINKEDIN PROFILE ENRICHER")
        print(f"{'='*60}")
        print("⏭️  SKIPPED: LinkedIn enrichment not selected in configuration")
        print("   No changes made to input file.")
        return 0

    print(f"\n{'='*60}")
    print("MODULE 3.9: LINKEDIN PROFILE ENRICHER")
    print(f"{'='*60}")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Enrich contacts with personal LinkedIn profiles via Exa',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--csv', type=str,
                       help='Input CSV file (standalone mode). If not provided, uses pipeline input.')
    parser.add_argument('--output', type=str,
                       help='Output CSV path (default: pipeline output or same as input)')
    parser.add_argument('--all', action='store_true',
                       help='Process all contacts (default: test mode with 3 contacts)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process (alternative to --all)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making API calls')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='Delay between API calls in seconds (default: 0.5)')

    args = parser.parse_args()

    # Determine input file
    run_id = get_run_id_from_env()

    if args.csv:
        # Standalone mode - use provided CSV
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv
        standalone_mode = True
    else:
        # Pipeline mode - auto-detect input
        if run_id:
            input_name = get_versioned_filename(INPUT_BASE, run_id)
            csv_path = BASE_DIR / "processed" / input_name
        else:
            csv_path = BASE_DIR / "processed" / INPUT_BASE

        # Try latest symlink if versioned file doesn't exist
        if not csv_path.exists():
            latest_path = BASE_DIR / "processed" / INPUT_BASE
            if latest_path.exists() or latest_path.is_symlink():
                csv_path = latest_path

        # Fallback: try 03d_final.csv if 03e_names.csv doesn't exist
        if not csv_path.exists():
            fallback_base = "03d_final.csv"
            if run_id:
                fallback_name = get_versioned_filename(fallback_base, run_id)
                fallback_path = BASE_DIR / "processed" / fallback_name
            else:
                fallback_path = BASE_DIR / "processed" / fallback_base

            if not fallback_path.exists():
                fallback_path = BASE_DIR / "processed" / fallback_base

            if fallback_path.exists() or fallback_path.is_symlink():
                csv_path = fallback_path
                logger.info(f"Using fallback input: {fallback_path}")

        standalone_mode = False

    if not csv_path.exists():
        print(f"ERROR: Input file not found: {csv_path}")
        if not standalone_mode:
            print("Make sure Module 3.8 (Contact Name Resolver) has run first.")
        return 1

    # Determine output file
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    elif standalone_mode:
        # For standalone, add _linkedin suffix
        output_path = csv_path.parent / f"{csv_path.stem}_linkedin{csv_path.suffix}"
    else:
        # Pipeline mode - write to new file
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
        limit = 3  # Test mode
        print("Test mode: Processing first 3 contacts")
        print("(Use --all to process all contacts)")

    # Check API key
    if not EXA_API_KEY and not args.dry_run:
        print("ERROR: EXA_API_KEY not found in environment")
        print("Add it to your .env file")
        return 1

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.dry_run:
        print("Mode: DRY RUN")

    # Run enrichment
    stats = enrich_linkedin_profiles(
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
            print(f"✓ Latest symlink: {latest_path}")

    print_summary(stats)
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
