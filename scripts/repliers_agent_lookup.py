#!/usr/bin/env python3
"""
Repliers Agent Lookup - Find email and phone for agents by name using MLS data.

Searches Repliers MLS listings by agent name to find their contact info.

Usage:
    python scripts/repliers_agent_lookup.py --input output/hubspot_import_enriched.csv --all
    python scripts/repliers_agent_lookup.py --input output/hubspot_import_enriched.csv --limit 10
    python scripts/repliers_agent_lookup.py --test  # Test with a few names
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

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
        logging.FileHandler('repliers_agent_lookup.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/hubspot_import_enriched.csv"
DEFAULT_OUTPUT = "output/hubspot_import_enriched.csv"

# API config
API_BASE_URL = "https://api.repliers.io"
REPLIERS_API_KEY = os.getenv('REPLIERS_API_KEY')


def search_agent_by_name(agent_name: str, city: str = "Miami") -> dict:
    """
    Search Repliers MLS for an agent by name.

    Args:
        agent_name: Full name of the agent
        city: City to search in

    Returns:
        Dict with email and phone if found, else empty dict
    """
    if not REPLIERS_API_KEY:
        return {}

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'REPLIERS-API-KEY': REPLIERS_API_KEY
    }

    # Search sold listings (more likely to have agent data)
    params = {
        'city': city,
        'status': 'U',
        'lastStatus': 'Sld',
        'agent': agent_name,
        'resultsPerPage': 5
    }

    try:
        response = requests.get(
            f"{API_BASE_URL}/listings",
            headers=headers,
            params=params,
            timeout=15
        )

        if response.status_code != 200:
            logger.debug(f"API error for {agent_name}: {response.status_code}")
            return {}

        data = response.json()
        listings = data.get('listings', [])

        if not listings:
            # Try active listings
            params['status'] = 'A'
            del params['lastStatus']
            response = requests.get(
                f"{API_BASE_URL}/listings",
                headers=headers,
                params=params,
                timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                listings = data.get('listings', [])

        if not listings:
            return {}

        # Extract agent info from first listing
        for listing in listings:
            agents = listing.get('agents', [])
            for agent in agents:
                # Verify name matches (case-insensitive)
                found_name = agent.get('name', '').lower()
                search_name = agent_name.lower()

                # Check if names are similar (handle variations like Jr., PA, etc.)
                if search_name in found_name or found_name in search_name:
                    phones = agent.get('phones', [])
                    return {
                        'email': agent.get('email'),
                        'phone': phones[0] if phones else None,
                        'phone2': phones[1] if len(phones) > 1 else None,
                        'agent_id': agent.get('agentId'),
                        'brokerage': listing.get('agents', [{}])[0].get('brokerage', {}).get('name'),
                        'mls_name': agent.get('name')
                    }

        return {}

    except Exception as e:
        logger.debug(f"Error searching for {agent_name}: {e}")
        return {}


def main():
    print(f"\n{'='*60}")
    print("REPLIERS AGENT LOOKUP")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Lookup agent contact info from Repliers MLS')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--output', type=str, default=DEFAULT_OUTPUT, help='Output CSV')
    parser.add_argument('--all', action='store_true', help='Process all contacts missing email')
    parser.add_argument('--limit', type=int, help='Limit contacts to process')
    parser.add_argument('--test', action='store_true', help='Test with sample names')
    parser.add_argument('--city', type=str, default='Miami', help='City to search')

    args = parser.parse_args()

    if not REPLIERS_API_KEY:
        print("ERROR: REPLIERS_API_KEY not found in .env")
        return 1

    # Test mode
    if args.test:
        test_names = ['Mike DeVito', 'Oscar Arellano', 'Melanie Hyer', 'NonExistent Agent']
        print("\nTest mode - searching for sample agents...")
        for name in test_names:
            result = search_agent_by_name(name, args.city)
            if result:
                print(f"  {name} -> {result.get('email')}, {result.get('phone')}")
            else:
                print(f"  {name} -> Not found")
            time.sleep(0.5)
        return 0

    # Load data
    input_path = BASE_DIR / args.input
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} contacts from {input_path.name}")

    # Find rows missing email
    def needs_email(row):
        email = row.get('Email')
        if pd.isna(email) or email == '' or str(email) == 'nan':
            return True
        return False

    rows_to_lookup = [idx for idx, row in df.iterrows() if needs_email(row)]
    print(f"Contacts needing email: {len(rows_to_lookup)}")

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

    # Process contacts
    stats = {'found_email': 0, 'found_phone': 0, 'not_found': 0}
    output_path = str(BASE_DIR / args.output)
    BATCH_SIZE = 10

    for i, idx in enumerate(tqdm(rows_to_lookup, desc="Looking up agents")):
        row = df.loc[idx]
        first_name = str(row.get('First Name', '')).strip()
        last_name = str(row.get('Last Name', '')).strip()
        full_name = f"{first_name} {last_name}".strip()

        if not full_name or full_name == 'nan nan':
            continue

        # Use contact's city, fallback to Miami
        city = str(row.get('City', 'Miami')).strip()
        if not city or city == 'nan' or city.lower() == 'nan':
            city = 'Miami'

        result = search_agent_by_name(full_name, city)

        if result.get('email'):
            df.at[idx, 'Email'] = result['email']
            df.at[idx, 'Email Source'] = 'repliers'
            stats['found_email'] += 1
            logger.info(f"Found: {full_name} -> {result['email']}")

            # Update phone if missing
            current_phone = row.get('Phone Number')
            if (pd.isna(current_phone) or current_phone == '') and result.get('phone'):
                df.at[idx, 'Phone Number'] = result['phone']
                stats['found_phone'] += 1
        else:
            stats['not_found'] += 1

        # Rate limit
        time.sleep(0.3)

        # Incremental save
        if (i + 1) % BATCH_SIZE == 0:
            df.to_csv(output_path, index=False)
            logger.info(f"Progress saved: {i + 1}/{len(rows_to_lookup)}")

    # Final save
    df.to_csv(output_path, index=False)
    df.to_excel(output_path.replace('.csv', '.xlsx'), index=False)

    # Summary
    print(f"\n{'='*60}")
    print("LOOKUP RESULTS")
    print(f"{'='*60}")
    print(f"  Emails found: {stats['found_email']}")
    print(f"  Phones found: {stats['found_phone']}")
    print(f"  Not found: {stats['not_found']}")

    # Final stats
    df_final = pd.read_csv(output_path)
    with_email = len(df_final[df_final['Email'].notna() & (df_final['Email'] != '')])
    with_phone = len(df_final[df_final['Phone Number'].notna() & (df_final['Phone Number'] != '')])

    print(f"\nFinal totals:")
    print(f"  Contacts with email: {with_email} / {len(df_final)}")
    print(f"  Contacts with phone: {with_phone} / {len(df_final)}")
    print(f"\nSaved to: {output_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
