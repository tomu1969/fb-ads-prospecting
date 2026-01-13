"""
Contact Name Resolver - Module 3.8 - Find contact names using multiple data sources

Runs automatically after Instagram Enricher (Module 3.7) in the pipeline.

Priority order:
1. Existing contact_name (if valid)
2. hunter_contact_name field
3. scraper_contact_name field
4. team_members field
5. Extract from page_name (patterns like "John Smith, Realtor")
6. Exa company owner search (optional, costs API credits)

Input: processed/03d_final.csv (from Module 3.7)
Output: processed/03e_names.csv

Usage:
    python scripts/contact_name_resolver.py           # Test mode (3 contacts)
    python scripts/contact_name_resolver.py --all    # Process all contacts
    python scripts/contact_name_resolver.py --all --use-exa  # Include Exa search
    python scripts/contact_name_resolver.py --csv output/prospects.csv  # Standalone mode
"""

import os
import re
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List, Tuple

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
        logging.FileHandler('contact_name_resolver.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
EXA_API_KEY = os.getenv('EXA_API_KEY')
EXA_API_URL = "https://api.exa.ai/search"

# Pipeline input/output paths
INPUT_BASE = "03d_final.csv"
OUTPUT_BASE = "03e_names.csv"

# Invalid names to skip
INVALID_NAMES = {
    'none', 'none none', 'nan', 'n/a', 'null', 'undefined', 'unknown',
    'meet our team', 'social media', 'featured video', 'your information',
    'contact', 'info', 'support', 'team', 'admin', 'founding member'
}

# Common first names for validation
COMMON_FIRST_NAMES = {
    'james', 'john', 'robert', 'michael', 'david', 'william', 'richard', 'joseph', 'thomas', 'charles',
    'mary', 'patricia', 'jennifer', 'linda', 'elizabeth', 'barbara', 'susan', 'jessica', 'sarah', 'karen',
    'alex', 'brian', 'chris', 'daniel', 'eric', 'frank', 'george', 'henry', 'ivan', 'jack',
    'kevin', 'larry', 'mark', 'nick', 'paul', 'ryan', 'scott', 'steve', 'tom', 'mike',
    'amy', 'anna', 'beth', 'carol', 'diana', 'emily', 'grace', 'helen', 'jane', 'kate',
    'lisa', 'maria', 'nancy', 'olivia', 'rachel', 'sandra', 'tina', 'wendy', 'amber', 'devon',
    'jeremy', 'brendon', 'fausto', 'leah', 'molly', 'kyle', 'ross', 'seth', 'tim', 'dale',
    'luiz', 'matthew', 'lindsey', 'suzanne', 'aimee', 'candis', 'markus', 'bob', 'rosana',
    'bonnie', 'liz', 'michelle', 'robin', 'nolan', 'donald', 'tiffany', 'brent', 'vanessa',
    'todd', 'marco', 'bill', 'ina', 'tyler', 'jonathan', 'laura', 'marcus', 'kalyani',
    'amy', 'don', 'paulina', 'aracely', 'joaquin', 'mandy', 'peter', 'jerry', 'carla',
    'gary', 'kim', 'jason', 'cindy', 'eli', 'claudia', 'jerad'
}


def is_valid_name(name: str) -> bool:
    """Check if name is a valid person name."""
    if not name or pd.isna(name):
        return False
    name_str = str(name).strip().lower()
    if name_str in INVALID_NAMES:
        return False
    if len(name_str) < 3:
        return False
    if "'s listings" in name_str:
        return False
    # Should have at least 2 parts (first + last)
    if len(name_str.split()) < 2:
        return False
    return True


def looks_like_person_name(name: str) -> bool:
    """Check if name looks like a real person name (not company)."""
    if not name:
        return False
    parts = name.lower().split()
    if len(parts) < 2:
        return False
    first = parts[0]
    if first in COMMON_FIRST_NAMES:
        return True
    # Check if it's capitalized and reasonable length
    if len(first) >= 3 and len(first) <= 12 and name[0].isupper():
        non_names = ['river', 'lake', 'hill', 'ocean', 'mountain', 'percent',
                     'prime', 'best', 'top', 'first', 'the', 'west', 'east']
        if first not in non_names:
            return True
    return False


def extract_name_from_page_name(page_name: str) -> Optional[str]:
    """Extract person name from page/company name using patterns."""
    if not page_name or pd.isna(page_name):
        return None

    page_name = str(page_name).strip()

    patterns = [
        # "John Smith, Realtor" or "John Smith, Real Estate Agent"
        (r'^([A-Z][a-z]+ [A-Z][a-z\']+(?:\s+[A-Z][a-z]+)?),\s*.+', None),

        # "John Smith - Title/Company"
        (r'^([A-Z][a-z]+ [A-Z][a-z\']+(?:\s+[A-Z][a-z]+)?)\s*[-–—]\s*.+',
         ['real', 'estate', 'home', 'property', 'group', 'team', 'door', 'hill']),

        # "John Smith Real Estate/Realty"
        (r'^([A-Z][a-z]+ [A-Z][a-z\']+)\s+(?:Real Estate|Realty|Properties|Homes)$', None),

        # "The John Smith Team/Group"
        (r'^The\s+([A-Z][a-z]+ [A-Z][a-z\']+)\s+(?:Team|Group)$', None),

        # "FirstName LastName Location Realtor" (e.g., "Claudia Sanchez Texas Realtor")
        (r'^([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:Texas|Florida|California|Chicago|[A-Z][a-z]+)\s+(?:Realtor|Title|Realty)', None),

        # "FirstName LastName Brokerage" (e.g., "Thomas Upton Compass Real Estate")
        (r'^([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:Compass|Keller Williams|RE/MAX|Coldwell|Century|NextHome|EXP|Better Homes)', None),

        # "FirstName LastName/Extra"
        (r'^([A-Z][a-z]+ [A-Z][a-z\']+)[/-].+', None),

        # Just "FirstName LastName" (company name IS the person)
        (r'^([A-Z][a-z]+ [A-Z][a-z]+)$', None),
    ]

    for pattern, exclusions in patterns:
        match = re.match(pattern, page_name, re.IGNORECASE)
        if match:
            name = match.group(1)
            if looks_like_person_name(name):
                if exclusions and any(word in name.lower() for word in exclusions):
                    continue
                return name

    return None


def extract_from_team_members(team_members: str) -> Optional[str]:
    """Extract first valid name from team_members JSON field."""
    if not team_members or pd.isna(team_members) or str(team_members) in ['[]', '{}', 'nan']:
        return None

    try:
        import json
        members = json.loads(str(team_members).replace("'", '"'))
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict):
                    name = member.get('name', '')
                    if is_valid_name(name):
                        return name
    except:
        pass

    return None


def search_exa_company_owner(company: str) -> Tuple[Optional[str], Optional[str]]:
    """Search Exa for company owner/founder and return (name, linkedin_url)."""
    if not EXA_API_KEY:
        return None, None

    query = f'"{company}" owner OR founder OR CEO site:linkedin.com'

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-api-key": EXA_API_KEY
    }

    payload = {
        "query": query,
        "numResults": 3,
        "type": "auto",
        "contents": {"text": {"maxCharacters": 500}}
    }

    try:
        response = requests.post(EXA_API_URL, headers=headers, json=payload, timeout=15)
        if response.status_code == 200:
            results = response.json().get('results', [])

            for result in results:
                url = result.get('url', '')
                title = result.get('title', '')
                text = result.get('text', '')

                if '/in/' not in url:
                    continue

                # Extract name from title
                match = re.match(r'^([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', title)
                if match:
                    name = match.group(1)
                    if not is_valid_name(name):
                        continue
                    # Verify company appears somewhere
                    company_words = [w.lower() for w in company.split() if len(w) > 3]
                    if any(w in text.lower() or w in title.lower() for w in company_words):
                        return name, url.split('?')[0]
    except Exception as e:
        logger.error(f"Exa search error for {company}: {e}")

    return None, None


def resolve_contact_name(row: pd.Series, use_exa: bool = False) -> Tuple[Optional[str], Optional[str], str]:
    """
    Resolve contact name from multiple sources.

    Returns: (name, linkedin_url, source)
    """
    page_name = row.get('page_name', '')

    # 1. Check existing contact_name
    existing = row.get('contact_name', '')
    if is_valid_name(existing):
        return str(existing), None, 'existing'

    # 2. Check hunter_contact_name
    hunter_name = row.get('hunter_contact_name', '')
    if is_valid_name(hunter_name):
        return str(hunter_name), None, 'hunter'

    # 3. Check scraper_contact_name
    scraper_name = row.get('scraper_contact_name', '')
    if is_valid_name(scraper_name):
        return str(scraper_name), None, 'scraper'

    # 4. Check team_members
    team_name = extract_from_team_members(row.get('team_members', ''))
    if team_name:
        return team_name, None, 'team_members'

    # 5. Extract from page_name
    extracted = extract_name_from_page_name(page_name)
    if extracted:
        return extracted, None, 'page_name'

    # 6. Exa company owner search (optional)
    if use_exa and page_name:
        name, linkedin = search_exa_company_owner(page_name)
        if name:
            return name, linkedin, 'exa_owner'

    return None, None, 'not_found'


def resolve_all_contacts(
    csv_path: Path,
    output_path: Optional[Path] = None,
    use_exa: bool = False,
    dry_run: bool = False,
    limit: Optional[int] = None
) -> Dict:
    """Resolve contact names for all rows in CSV."""

    df = pd.read_csv(csv_path, encoding='utf-8')

    stats = {
        'total': len(df),
        'existing': 0,
        'hunter': 0,
        'scraper': 0,
        'team_members': 0,
        'page_name': 0,
        'exa_owner': 0,
        'not_found': 0
    }

    # Find rows needing resolution
    rows_to_process = []
    for idx, row in df.iterrows():
        if not is_valid_name(row.get('contact_name', '')):
            rows_to_process.append(idx)
        else:
            stats['existing'] += 1

    if limit:
        rows_to_process = rows_to_process[:limit]

    logger.info(f"Processing {len(rows_to_process)} contacts needing name resolution")

    if dry_run:
        logger.info("DRY RUN - no changes will be made")
        for idx in rows_to_process[:10]:
            row = df.loc[idx]
            name, linkedin, source = resolve_contact_name(row, use_exa=False)
            if name:
                logger.info(f"  Would resolve: {row.get('page_name', '')} -> {name} [{source}]")
        return stats

    # Process rows
    for idx in tqdm(rows_to_process, desc="Resolving contact names"):
        row = df.loc[idx]
        name, linkedin, source = resolve_contact_name(row, use_exa=use_exa)

        stats[source] += 1

        if name:
            df.at[idx, 'contact_name'] = name
            if linkedin and (pd.isna(df.at[idx, 'linkedin_profile']) or df.at[idx, 'linkedin_profile'] == ''):
                df.at[idx, 'linkedin_profile'] = linkedin
            logger.debug(f"Resolved: {row.get('page_name', '')} -> {name} [{source}]")

        if use_exa and source == 'not_found':
            time.sleep(0.5)  # Rate limit for Exa

    # Save output
    if output_path is None:
        output_path = csv_path

    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved to {output_path}")

    return stats


def print_summary(stats: Dict):
    """Print resolution summary."""
    print("\n" + "=" * 60)
    print("CONTACT NAME RESOLUTION SUMMARY")
    print("=" * 60)
    print(f"Total contacts:         {stats.get('total', 0)}")
    print(f"Already had name:       {stats.get('existing', 0)}")
    print(f"From hunter:            {stats.get('hunter', 0)}")
    print(f"From scraper:           {stats.get('scraper', 0)}")
    print(f"From team_members:      {stats.get('team_members', 0)}")
    print(f"From page_name:         {stats.get('page_name', 0)}")
    print(f"From Exa owner search:  {stats.get('exa_owner', 0)}")
    print(f"Not found:              {stats.get('not_found', 0)}")
    print("=" * 60)

    resolved = stats['total'] - stats['not_found']
    print(f"Resolution rate: {resolved}/{stats['total']} ({100*resolved/stats['total']:.1f}%)")


def main():
    """Main function with pipeline and standalone mode support."""

    # Check if module should run based on enrichment config
    if not should_run_module("contact_name_resolver"):
        print(f"\n{'='*60}")
        print("MODULE 3.8: CONTACT NAME RESOLVER")
        print(f"{'='*60}")
        print("⏭️  SKIPPED: Contact name resolution not selected in configuration")
        print("   No changes made to input file.")
        return 0

    print(f"\n{'='*60}")
    print("MODULE 3.8: CONTACT NAME RESOLVER")
    print(f"{'='*60}")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Resolve contact names from multiple data sources',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--csv', type=str,
                       help='Input CSV file (standalone mode). If not provided, uses pipeline input.')
    parser.add_argument('--output', type=str,
                       help='Output CSV path (default: pipeline output or same as input)')
    parser.add_argument('--all', action='store_true',
                       help='Process all contacts (default: test mode with 3 contacts)')
    parser.add_argument('--use-exa', action='store_true',
                       help='Use Exa API to search for company owners (costs API credits)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making changes')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process (alternative to --all)')

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

        standalone_mode = False

    if not csv_path.exists():
        print(f"ERROR: Input file not found: {csv_path}")
        if not standalone_mode:
            print("Make sure Module 3.7 (Instagram Enricher) has run first.")
        return 1

    # Determine output file
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    elif standalone_mode:
        output_path = csv_path  # Update in place for standalone
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

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.use_exa:
        print("Mode: Using Exa API for owner search (costs credits)")

    # Run resolution
    stats = resolve_all_contacts(
        csv_path=csv_path,
        output_path=output_path,
        use_exa=args.use_exa,
        dry_run=args.dry_run,
        limit=limit
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
