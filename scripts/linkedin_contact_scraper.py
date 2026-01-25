"""
LinkedIn Contact Scraper - Extract emails from LinkedIn profile URLs via Apify

Uses Apify actor harvestapi/linkedin-profile-scraper to extract contact info
from LinkedIn profile URLs that we already have.

Usage:
    python scripts/linkedin_contact_scraper.py --input output/hubspot_import_enriched.csv --dry-run
    python scripts/linkedin_contact_scraper.py --input output/hubspot_import_enriched.csv --all
    python scripts/linkedin_contact_scraper.py --input output/hubspot_import_enriched.csv --limit 10
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from apify_client import ApifyClient

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('linkedin_contact_scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Apify config
APIFY_API_KEY = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
ACTOR_ID = "dev_fusion/linkedin-profile-scraper"  # Works for URL-based scraping

# Batch size for Apify calls
BATCH_SIZE = 10  # Smaller batches for more reliable processing


def scrape_linkedin_profiles(urls: List[str], include_email: bool = True) -> List[Dict]:
    """
    Scrape LinkedIn profiles using Apify dev_fusion actor.

    Args:
        urls: List of LinkedIn profile URLs
        include_email: Not used for this actor (always includes email if available)

    Returns:
        List of profile data dicts with emails
    """
    if not APIFY_API_KEY:
        logger.error("APIFY_API_KEY not configured")
        return []

    client = ApifyClient(APIFY_API_KEY)

    run_input = {
        "profileUrls": urls,
    }

    logger.info(f"Scraping {len(urls)} LinkedIn profiles via Apify (dev_fusion)...")

    try:
        run = client.actor(ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=300,  # 5 min timeout for batch
            memory_mbytes=512,
        )

        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        logger.info(f"Got {len(results)} profile results")
        return results

    except Exception as e:
        logger.error(f"Apify error: {e}")
        return []


def extract_contact_info(profile_data: Dict) -> Dict:
    """Extract email and phone from Apify dev_fusion profile data."""
    result = {
        'email': None,
        'phone': None,
        'linkedin_url': profile_data.get('linkedinUrl', '') or profile_data.get('linkedinPublicUrl', ''),
        'full_name': profile_data.get('fullName', '') or profile_data.get('name', ''),
        'company': profile_data.get('companyName', ''),
    }

    # Email field from dev_fusion actor
    email = profile_data.get('email')
    if email and '@' in str(email):
        result['email'] = email

    # Phone - dev_fusion uses mobileNumber
    phone = (
        profile_data.get('mobileNumber') or
        profile_data.get('phone') or
        profile_data.get('phoneNumber')
    )
    if phone:
        result['phone'] = phone

    return result


def main():
    print("=" * 60)
    print("LINKEDIN CONTACT SCRAPER")
    print("=" * 60)

    parser = argparse.ArgumentParser(description='Extract emails from LinkedIn profile URLs')
    parser.add_argument('--input', required=True, help='Input CSV with LinkedIn URL column')
    parser.add_argument('--output', help='Output CSV (default: overwrites input)')
    parser.add_argument('--all', action='store_true', help='Process all contacts missing email')
    parser.add_argument('--limit', type=int, help='Limit contacts to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--no-email-search', action='store_true', help='Skip email search (cheaper)')
    args = parser.parse_args()

    # Load CSV
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return 1

    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} contacts")

    # Find LinkedIn URL column
    linkedin_col = None
    for col in ['LinkedIn URL', 'linkedin_url', 'linkedin_profile', 'LinkedIn']:
        if col in df.columns:
            linkedin_col = col
            break

    if not linkedin_col:
        print("ERROR: No LinkedIn URL column found")
        return 1

    # Find email column
    email_col = None
    for col in ['Email', 'email', 'work_email']:
        if col in df.columns:
            email_col = col
            break

    if not email_col:
        print("ERROR: No Email column found")
        return 1

    # Find phone column
    phone_col = None
    for col in ['Phone Number', 'phone', 'Phone', 'phone_number']:
        if col in df.columns:
            phone_col = col
            break

    # Filter: has LinkedIn URL, missing email
    has_linkedin = df[linkedin_col].notna() & (df[linkedin_col] != '') & df[linkedin_col].str.contains('/in/', na=False)
    no_email = df[email_col].isna() | (df[email_col] == '')

    if phone_col:
        no_phone = df[phone_col].isna() | (df[phone_col] == '')
        targets = df[has_linkedin & no_email & no_phone].copy()
    else:
        targets = df[has_linkedin & no_email].copy()

    print(f"Contacts with LinkedIn but missing email: {len(targets)}")

    # Apply limit
    if args.limit:
        targets = targets.head(args.limit)
    elif not args.all:
        targets = targets.head(5)  # Test mode
        print("Test mode: Processing first 5 contacts (use --all for all)")

    if len(targets) == 0:
        print("No contacts to process")
        return 0

    print(f"Will process: {len(targets)} contacts")

    # Dry run - just show what would be done
    if args.dry_run:
        print("\nDRY RUN - Would scrape these profiles:")
        for _, row in targets.head(10).iterrows():
            name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
            url = row[linkedin_col]
            print(f"  {name}: {url}")
        if len(targets) > 10:
            print(f"  ... and {len(targets) - 10} more")

        # Cost estimate
        cost = len(targets) / 1000 * (10 if not args.no_email_search else 4)
        print(f"\nEstimated cost: ${cost:.2f}")
        return 0

    # Check API key
    if not APIFY_API_KEY:
        print("ERROR: APIFY_API_KEY not found in .env")
        return 1

    # Process in batches
    urls = targets[linkedin_col].tolist()
    url_to_idx = {row[linkedin_col]: idx for idx, row in targets.iterrows()}

    stats = {'found_email': 0, 'found_phone': 0, 'errors': 0}

    for i in range(0, len(urls), BATCH_SIZE):
        batch_urls = urls[i:i + BATCH_SIZE]
        print(f"\nBatch {i // BATCH_SIZE + 1}: Processing {len(batch_urls)} profiles...")

        results = scrape_linkedin_profiles(batch_urls, include_email=not args.no_email_search)

        for profile in results:
            contact = extract_contact_info(profile)
            linkedin_url = contact['linkedin_url']

            # Find matching row by URL
            matching_idx = None
            for url, idx in url_to_idx.items():
                if url and linkedin_url and (url in linkedin_url or linkedin_url in url):
                    matching_idx = idx
                    break

            if matching_idx is None:
                continue

            # Update email
            if contact['email']:
                df.at[matching_idx, email_col] = contact['email']
                if 'Email Source' in df.columns:
                    df.at[matching_idx, 'Email Source'] = 'linkedin'
                stats['found_email'] += 1
                logger.info(f"Found email: {contact['full_name']} -> {contact['email']}")

            # Update phone
            if contact['phone'] and phone_col:
                df.at[matching_idx, phone_col] = contact['phone']
                stats['found_phone'] += 1
                logger.info(f"Found phone: {contact['full_name']} -> {contact['phone']}")

        # Save incrementally
        output_path = Path(args.output) if args.output else input_path
        df.to_csv(output_path, index=False)
        print(f"Progress saved: {min(i + BATCH_SIZE, len(urls))}/{len(urls)}")

        # Rate limit between batches
        if i + BATCH_SIZE < len(urls):
            time.sleep(2)

    # Final summary
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Processed:    {len(urls)}")
    print(f"Emails found: {stats['found_email']}")
    print(f"Phones found: {stats['found_phone']}")
    print(f"Errors:       {stats['errors']}")
    print(f"\nSaved to: {output_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
