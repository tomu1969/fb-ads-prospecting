"""
Contact Name Resolver - Find contact names using multiple data sources

Priority order:
1. Existing contact_name (if valid)
2. hunter_contact_name field
3. scraper_contact_name field
4. team_members field
5. Extract from page_name (patterns like "John Smith, Realtor")
6. Exa company owner search (optional, costs API credits)

Usage:
    python scripts/contact_name_resolver.py --csv output/prospects.csv
    python scripts/contact_name_resolver.py --csv output/prospects.csv --use-exa
    python scripts/contact_name_resolver.py --csv output/prospects.csv --dry-run
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
    parser = argparse.ArgumentParser(
        description='Resolve contact names from multiple data sources',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--csv', type=str, required=True,
                       help='Input CSV file')
    parser.add_argument('--output', type=str,
                       help='Output CSV path (default: update input file)')
    parser.add_argument('--use-exa', action='store_true',
                       help='Use Exa API to search for company owners (costs API credits)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making changes')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process')

    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = BASE_DIR / args.csv

    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)

    output_path = None
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output

    print(f"\nProcessing: {csv_path}")
    if args.use_exa:
        print("MODE: Using Exa API for owner search")

    stats = resolve_all_contacts(
        csv_path=csv_path,
        output_path=output_path,
        use_exa=args.use_exa,
        dry_run=args.dry_run,
        limit=args.limit
    )

    print_summary(stats)


if __name__ == '__main__':
    main()
