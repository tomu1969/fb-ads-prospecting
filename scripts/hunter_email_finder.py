#!/usr/bin/env python3
"""
Hunter.io Email Finder - Find emails by name and company.

Uses Hunter's Email Finder API to look up emails using first name, last name,
and company domain.
"""

import os
import re
import time
import logging
import argparse
import pandas as pd
import requests
from pathlib import Path
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
        logging.FileHandler('hunter_email_finder.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/hubspot_import_enriched.csv"

# API config
API_KEY = os.getenv('HUNTER_API_KEY')
BASE_URL = 'https://api.hunter.io/v2'

# Common real estate brokerage domains (for fallback)
BROKERAGE_DOMAINS = {
    'exp realty': 'exprealty.com',
    'keller williams': 'kw.com',
    'coldwell banker': 'coldwellbanker.com',
    'compass': 'compass.com',
    're/max': 'remax.com',
    'remax': 'remax.com',
    'century 21': 'century21.com',
    'berkshire hathaway': 'bhhsamb.com',
    'sotheby': 'sothebysrealty.com',
    'douglas elliman': 'elliman.com',
    'engel & völkers': 'evrealestate.com',
    'engel & volkers': 'evrealestate.com',
    'united realty group': 'unitedrealtygroupfl.com',
    'la rosa realty': 'larosarealty.com',
    'one world realty': 'oneworldrealtymiami.com',
}


def company_to_domain(company_name: str) -> str | None:
    """
    Convert company name to likely domain.

    Args:
        company_name: Company name like "Exp Realty, Llc"

    Returns:
        Domain string or None
    """
    if not company_name or pd.isna(company_name):
        return None

    company_lower = company_name.lower().strip()

    # Check known brokerages
    for pattern, domain in BROKERAGE_DOMAINS.items():
        if pattern in company_lower:
            return domain

    # Try to construct domain from company name
    # Remove common suffixes
    cleaned = re.sub(r',?\s*(llc|inc|corp|ltd|group|realty|real estate|associates?|properties|team)\.?$', '', company_lower, flags=re.IGNORECASE)
    cleaned = re.sub(r'[^\w\s]', '', cleaned).strip()

    if cleaned:
        # Convert to domain format
        domain = cleaned.replace(' ', '') + '.com'
        return domain

    return None


def find_email(first_name: str, last_name: str, domain: str) -> dict:
    """
    Find email using Hunter's Email Finder API.

    Args:
        first_name: First name
        last_name: Last name
        domain: Company domain

    Returns:
        Dict with email, confidence, and status
    """
    if not API_KEY:
        return {'email': None, 'error': 'No API key'}

    if not domain:
        return {'email': None, 'error': 'No domain'}

    try:
        resp = requests.get(
            f'{BASE_URL}/email-finder',
            params={
                'domain': domain,
                'first_name': first_name,
                'last_name': last_name,
                'api_key': API_KEY
            },
            timeout=15
        )

        if resp.status_code == 200:
            data = resp.json().get('data', {})
            email = data.get('email')
            confidence = data.get('score', 0)

            if email:
                return {
                    'email': email,
                    'confidence': confidence,
                    'domain_used': domain,
                    'error': None
                }
            return {'email': None, 'error': 'No email found'}

        elif resp.status_code == 400:
            return {'email': None, 'error': 'Invalid request'}
        elif resp.status_code == 401:
            return {'email': None, 'error': 'Invalid API key'}
        elif resp.status_code == 429:
            return {'email': None, 'error': 'Rate limited'}
        else:
            return {'email': None, 'error': f'HTTP {resp.status_code}'}

    except Exception as e:
        return {'email': None, 'error': str(e)}


def check_api_quota() -> dict:
    """Check Hunter API quota."""
    if not API_KEY:
        return {'error': 'No API key'}

    try:
        resp = requests.get(
            f'{BASE_URL}/account',
            params={'api_key': API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            return {
                'requests_used': data.get('requests', {}).get('searches', {}).get('used', 0),
                'requests_available': data.get('requests', {}).get('searches', {}).get('available', 0),
                'plan': data.get('plan_name', 'Unknown')
            }
        return {'error': f'HTTP {resp.status_code}'}
    except Exception as e:
        return {'error': str(e)}


def main():
    print(f"\n{'='*60}")
    print("HUNTER EMAIL FINDER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Find emails using Hunter.io Email Finder')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--all', action='store_true', help='Process all contacts missing email')
    parser.add_argument('--limit', type=int, help='Limit contacts to process')
    parser.add_argument('--check-quota', action='store_true', help='Check API quota only')

    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: HUNTER_API_KEY not found in .env")
        return 1

    # Check quota
    quota = check_api_quota()
    if 'error' in quota:
        print(f"ERROR checking quota: {quota['error']}")
        return 1

    print(f"API Plan: {quota['plan']}")
    print(f"Requests: {quota['requests_used']} used / {quota['requests_available']} available")

    if args.check_quota:
        return 0

    # Load data
    input_path = BASE_DIR / args.input
    df = pd.read_csv(input_path)
    print(f"\nLoaded {len(df)} contacts from {input_path.name}")

    # Find rows missing email
    def needs_email(row):
        email = row.get('Email')
        if pd.isna(email) or email == '' or str(email) == 'nan':
            return True
        return False

    rows_to_lookup = [idx for idx, row in df.iterrows() if needs_email(row)]
    print(f"Contacts needing email: {len(rows_to_lookup)}")

    # Check quota vs needed
    if len(rows_to_lookup) > quota['requests_available']:
        print(f"WARNING: Need {len(rows_to_lookup)} lookups but only {quota['requests_available']} available")

    # Apply limit
    if args.limit:
        rows_to_lookup = rows_to_lookup[:args.limit]
    elif not args.all:
        rows_to_lookup = rows_to_lookup[:10]
        print("Test mode: Processing first 10 contacts (use --all for all)")

    if not rows_to_lookup:
        print("No contacts to lookup")
        return 0

    # Initialize columns
    if 'Email Source' not in df.columns:
        df['Email Source'] = ''
    if 'Hunter Confidence' not in df.columns:
        df['Hunter Confidence'] = ''

    # Process contacts
    stats = {'found': 0, 'not_found': 0, 'errors': 0}
    output_path = str(input_path)
    BATCH_SIZE = 10

    for i, idx in enumerate(tqdm(rows_to_lookup, desc="Finding emails")):
        row = df.loc[idx]
        first_name = str(row.get('First Name', '')).strip()
        last_name = str(row.get('Last Name', '')).strip()
        company = str(row.get('Company Name', '')).strip()

        if not first_name or not last_name:
            stats['errors'] += 1
            continue

        # Get domain from company
        domain = company_to_domain(company)

        if not domain:
            logger.debug(f"No domain for: {company}")
            stats['not_found'] += 1
            continue

        # Find email
        result = find_email(first_name, last_name, domain)

        if result.get('email'):
            df.at[idx, 'Email'] = result['email']
            df.at[idx, 'Email Source'] = 'hunter_finder'
            df.at[idx, 'Hunter Confidence'] = result.get('confidence', '')
            stats['found'] += 1
            logger.info(f"Found: {first_name} {last_name} ({domain}) -> {result['email']}")
        else:
            stats['not_found'] += 1
            logger.debug(f"Not found: {first_name} {last_name} ({domain}): {result.get('error')}")

        # Rate limit (Hunter allows 10/sec but be conservative)
        time.sleep(0.5)

        # Incremental save
        if (i + 1) % BATCH_SIZE == 0:
            df.to_csv(output_path, index=False)
            logger.info(f"Progress saved: {i + 1}/{len(rows_to_lookup)}")

    # Final save
    df.to_csv(output_path, index=False)
    df.to_excel(output_path.replace('.csv', '.xlsx'), index=False)

    # Summary
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"  Emails found: {stats['found']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Errors: {stats['errors']}")

    # Final stats
    df_final = pd.read_csv(output_path)
    with_email = len(df_final[df_final['Email'].notna() & (df_final['Email'] != '')])
    print(f"\nFinal total with email: {with_email} / {len(df_final)}")
    print(f"\nSaved to: {output_path}")

    return 0


if __name__ == '__main__':
    exit(main())
