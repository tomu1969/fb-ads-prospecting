"""
HubSpot Contact Enricher - Find missing emails and phones for HubSpot import

Enriches contacts missing email/phone using:
1. Website discovery from company name (DuckDuckGo)
2. Email finding (Hunter.io, Apollo.io, Exa)
3. Phone scraping from websites

Input: output/hubspot_import_merged.csv
Output: output/hubspot_import_enriched.csv

Usage:
    python scripts/hubspot_enricher.py --all
    python scripts/hubspot_enricher.py --limit 10
    python scripts/hubspot_enricher.py --dry-run
"""

import os
import sys
import re
import json
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List

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
        logging.FileHandler('hubspot_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/hubspot_import_merged.csv"
DEFAULT_OUTPUT = "output/hubspot_import_enriched.csv"

# API Keys
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
EXA_API_KEY = os.getenv('EXA_API_KEY')


def search_website_duckduckgo(company_name: str, location: str = "Florida") -> Optional[str]:
    """Find company website using DuckDuckGo."""
    if not company_name or len(company_name) < 3:
        return None

    try:
        from duckduckgo_search import DDGS

        query = f"{company_name} {location} real estate website"

        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=3))

        if not results:
            return None

        # Filter for likely company websites
        for result in results:
            url = result.get('href', '')
            # Skip social media and directories
            skip_domains = ['facebook.com', 'linkedin.com', 'instagram.com', 'twitter.com',
                          'zillow.com', 'realtor.com', 'yelp.com', 'yellowpages.com']
            if any(domain in url.lower() for domain in skip_domains):
                continue
            return url

        return results[0].get('href') if results else None

    except Exception as e:
        logger.debug(f"DuckDuckGo error for {company_name}: {e}")
        return None


def find_email_hunter(domain: str, first_name: str, last_name: str) -> Optional[Dict]:
    """Find email using Hunter.io."""
    if not HUNTER_API_KEY or not domain:
        return None

    try:
        # Email finder endpoint
        url = "https://api.hunter.io/v2/email-finder"
        params = {
            "domain": domain,
            "first_name": first_name,
            "last_name": last_name,
            "api_key": HUNTER_API_KEY
        }

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json().get('data', {})
            email = data.get('email')
            if email:
                return {
                    'email': email,
                    'confidence': data.get('score', 0),
                    'source': 'hunter'
                }

        return None

    except Exception as e:
        logger.debug(f"Hunter error for {domain}: {e}")
        return None


def find_email_apollo(first_name: str, last_name: str, company: str) -> Optional[Dict]:
    """Find email using Apollo.io."""
    if not APOLLO_API_KEY:
        return None

    try:
        url = "https://api.apollo.io/v1/people/match"
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }

        payload = {
            "api_key": APOLLO_API_KEY,
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": company,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=15)

        if response.status_code == 200:
            data = response.json().get('person', {})
            email = data.get('email')
            if email:
                return {
                    'email': email,
                    'confidence': 80,
                    'source': 'apollo',
                    'phone': data.get('phone_numbers', [{}])[0].get('number') if data.get('phone_numbers') else None
                }

        return None

    except Exception as e:
        logger.debug(f"Apollo error for {first_name} {last_name}: {e}")
        return None


def find_email_exa(name: str, company: str) -> Optional[Dict]:
    """Find email using Exa API search."""
    if not EXA_API_KEY:
        return None

    try:
        url = "https://api.exa.ai/search"
        headers = {
            "x-api-key": EXA_API_KEY,
            "Content-Type": "application/json"
        }

        query = f'"{name}" "{company}" email contact real estate'

        payload = {
            "query": query,
            "numResults": 3,
            "type": "keyword",
            "contents": {
                "text": {"maxCharacters": 1000}
            }
        }

        response = requests.post(url, headers=headers, json=payload, timeout=15)

        if response.status_code == 200:
            results = response.json().get('results', [])

            for result in results:
                text = result.get('text', '')
                # Extract email from text
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
                if email_match:
                    email = email_match.group(0).lower()
                    # Validate it's not a generic email
                    if not any(x in email for x in ['example', 'test', 'noreply', 'info@', 'contact@']):
                        return {
                            'email': email,
                            'confidence': 60,
                            'source': 'exa'
                        }

        return None

    except Exception as e:
        logger.debug(f"Exa error for {name}: {e}")
        return None


def scrape_phone_from_website(url: str) -> Optional[str]:
    """Scrape phone number from website."""
    if not url:
        return None

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            text = response.text

            # Phone patterns
            patterns = [
                r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # (123) 456-7890
                r'\+1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # +1 123 456 7890
            ]

            for pattern in patterns:
                matches = re.findall(pattern, text)
                if matches:
                    # Clean and return first match
                    phone = re.sub(r'[^\d]', '', matches[0])
                    if len(phone) == 10:
                        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
                    elif len(phone) == 11 and phone.startswith('1'):
                        return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"

        return None

    except Exception as e:
        logger.debug(f"Scrape error for {url}: {e}")
        return None


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        domain = domain.replace('www.', '')
        return domain
    except:
        return ""


def safe_str(value, default: str = "") -> str:
    """Safely convert a value to string, handling NaN/None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    if pd.isna(value):
        return default
    return str(value).strip()


def enrich_contact(row: pd.Series, skip_website: bool = False) -> Dict:
    """Enrich a single contact with missing email/phone."""
    result = {
        'email': safe_str(row.get('Email')),
        'phone': safe_str(row.get('Phone Number')),
        'website': safe_str(row.get('Website URL')),
        'email_source': '',
        'enriched': False
    }

    first_name = safe_str(row.get('First Name'))
    last_name = safe_str(row.get('Last Name'))
    company = safe_str(row.get('Company Name'))
    full_name = f"{first_name} {last_name}".strip()

    # Skip if already has email
    if result['email'] and '@' in result['email']:
        return result

    # Step 1: Find website if missing (skip if --skip-website flag)
    if not skip_website and not result['website'] and company:
        website = search_website_duckduckgo(company)
        if website:
            result['website'] = website
            logger.debug(f"Found website for {company}: {website}")
        time.sleep(0.5)  # Rate limit

    domain = extract_domain(result['website'])

    # Step 2: Try Hunter.io (if we have domain)
    if domain and first_name and last_name:
        hunter_result = find_email_hunter(domain, first_name, last_name)
        if hunter_result:
            result['email'] = hunter_result['email']
            result['email_source'] = 'hunter'
            result['enriched'] = True
            logger.info(f"Hunter found: {full_name} -> {result['email']}")
            return result
        time.sleep(0.3)

    # Step 3: Try Apollo.io
    if first_name and last_name and company:
        apollo_result = find_email_apollo(first_name, last_name, company)
        if apollo_result:
            result['email'] = apollo_result['email']
            result['email_source'] = 'apollo'
            result['enriched'] = True
            if apollo_result.get('phone') and not result['phone']:
                result['phone'] = apollo_result['phone']
            logger.info(f"Apollo found: {full_name} -> {result['email']}")
            return result
        time.sleep(0.3)

    # Step 4: Try Exa search
    if full_name and company:
        exa_result = find_email_exa(full_name, company)
        if exa_result:
            result['email'] = exa_result['email']
            result['email_source'] = 'exa'
            result['enriched'] = True
            logger.info(f"Exa found: {full_name} -> {result['email']}")
            return result

    # Step 5: Try to scrape phone if we have website but no phone
    if result['website'] and not result['phone']:
        phone = scrape_phone_from_website(result['website'])
        if phone:
            result['phone'] = phone
            result['enriched'] = True
            logger.debug(f"Scraped phone for {full_name}: {phone}")

    return result


def save_incremental(df: pd.DataFrame, output_path: str, batch_name: str = "") -> None:
    """Save DataFrame incrementally."""
    try:
        df.to_csv(output_path, index=False)
        if batch_name:
            backup_dir = Path(output_path).parent / "backups"
            backup_dir.mkdir(exist_ok=True)
            backup_path = backup_dir / f"hubspot_enriched_{batch_name}.csv"
            df.to_csv(backup_path, index=False)
            logger.info(f"Saved: {len(df)} rows (backup: {batch_name})")
    except Exception as e:
        logger.error(f"Save error: {e}")


def main():
    print(f"\n{'='*60}")
    print("HUBSPOT CONTACT ENRICHER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Enrich contacts for HubSpot import')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--output', type=str, default=DEFAULT_OUTPUT, help='Output CSV')
    parser.add_argument('--all', action='store_true', help='Process all contacts missing email')
    parser.add_argument('--limit', type=int, help='Limit contacts to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--skip-website', action='store_true', help='Skip website discovery (faster)')

    args = parser.parse_args()

    # Load data
    input_path = BASE_DIR / args.input
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} contacts from {input_path.name}")

    # Find rows needing enrichment (missing email)
    df['_needs_enrichment'] = df['Email'].isna() | (df['Email'] == '') | (df['Email'] == 'nan')
    rows_to_enrich = df[df['_needs_enrichment']].index.tolist()

    print(f"Contacts needing email enrichment: {len(rows_to_enrich)}")

    # Check API keys
    apis = []
    if HUNTER_API_KEY: apis.append("Hunter.io")
    if APOLLO_API_KEY: apis.append("Apollo.io")
    if EXA_API_KEY: apis.append("Exa")
    print(f"Available APIs: {', '.join(apis) if apis else 'None'}")

    if not apis:
        print("\nWARNING: No API keys configured. Set HUNTER_API_KEY, APOLLO_API_KEY, or EXA_API_KEY in .env")

    # Apply limit
    if args.limit:
        rows_to_enrich = rows_to_enrich[:args.limit]
    elif not args.all:
        rows_to_enrich = rows_to_enrich[:5]
        print("Test mode: Processing first 5 contacts (use --all for all)")

    if args.dry_run:
        print(f"\n[DRY RUN] Would enrich {len(rows_to_enrich)} contacts")
        for idx in rows_to_enrich[:10]:
            row = df.loc[idx]
            print(f"  - {row['First Name']} {row['Last Name']} ({row['Company Name']})")
        return 0

    if not rows_to_enrich:
        print("No contacts to enrich")
        return 0

    # Initialize new columns
    if 'Email Source' not in df.columns:
        df['Email Source'] = ''

    # Process contacts
    stats = {'found_email': 0, 'found_phone': 0, 'found_website': 0, 'errors': 0}
    output_path = str(BASE_DIR / args.output)
    BATCH_SIZE = 10

    for i, idx in enumerate(tqdm(rows_to_enrich, desc="Enriching contacts")):
        try:
            row = df.loc[idx]
            result = enrich_contact(row, skip_website=args.skip_website)

            # Update DataFrame
            if result['email'] and '@' in str(result['email']):
                df.at[idx, 'Email'] = result['email']
                df.at[idx, 'Email Source'] = result['email_source']
                stats['found_email'] += 1

            if result['phone']:
                df.at[idx, 'Phone Number'] = result['phone']
                stats['found_phone'] += 1

            existing_website = safe_str(row.get('Website URL'))
            if result['website'] and not existing_website:
                df.at[idx, 'Website URL'] = result['website']
                stats['found_website'] += 1

        except Exception as e:
            logger.error(f"Error enriching row {idx}: {e}")
            stats['errors'] += 1

        # Incremental save
        if (i + 1) % BATCH_SIZE == 0:
            save_incremental(df, output_path, f"batch_{(i + 1) // BATCH_SIZE}")

    # Final save
    df = df.drop(columns=['_needs_enrichment'], errors='ignore')
    df.to_csv(output_path, index=False)

    # Also save as Excel
    excel_path = output_path.replace('.csv', '.xlsx')
    df.to_excel(excel_path, index=False)

    # Summary
    print(f"\n{'='*60}")
    print("ENRICHMENT RESULTS")
    print(f"{'='*60}")
    print(f"  Emails found: {stats['found_email']}")
    print(f"  Phones found: {stats['found_phone']}")
    print(f"  Websites found: {stats['found_website']}")
    print(f"  Errors: {stats['errors']}")

    # Final stats
    df_final = pd.read_csv(output_path)
    with_email = len(df_final[df_final['Email'].notna() & (df_final['Email'] != '')])
    with_phone = len(df_final[df_final['Phone Number'].notna() & (df_final['Phone Number'] != '')])

    print(f"\nFinal totals:")
    print(f"  Contacts with email: {with_email} / {len(df_final)}")
    print(f"  Contacts with phone: {with_phone} / {len(df_final)}")
    print(f"\nSaved to:")
    print(f"  - {output_path}")
    print(f"  - {excel_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
