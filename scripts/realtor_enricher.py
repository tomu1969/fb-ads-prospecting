"""
Realtor Agent Enricher - Get emails for real estate agents from LinkedIn profiles

A 3-stage pipeline to enrich realtor contacts that only have LinkedIn URLs:
1. Fix CSV structure (move LinkedIn URLs from email to linkedin_url column)
2. Apollo people/match API (LinkedIn URL → Email, ~$0.02/query)
3. Apify LinkedIn scraper fallback (~$0.01-0.02/profile)

Usage:
    # Dry run - show what would be done
    python scripts/realtor_enricher.py --input output/realtor_agents.csv --dry-run

    # Fix CSV structure only
    python scripts/realtor_enricher.py --input output/realtor_agents.csv --fix-only

    # Run Apollo enrichment only
    python scripts/realtor_enricher.py --input output/realtor_agents.csv --stage apollo

    # Run Apify fallback only (for those without email after Apollo)
    python scripts/realtor_enricher.py --input output/realtor_agents.csv --stage apify

    # Full pipeline: fix → apollo → apify
    python scripts/realtor_enricher.py --input output/realtor_agents.csv --all
"""

import os
import sys
import argparse
import logging
import time
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any

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
        logging.FileHandler('realtor_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# API Configuration
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"
APIFY_API_KEY = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
APIFY_ACTOR_ID = "dev_fusion/linkedin-profile-scraper"

# Costs per operation
COST_APOLLO_MATCH = 0.02
COST_APIFY_PROFILE = 0.015

# Batch size for Apify
APIFY_BATCH_SIZE = 10

# Generic email prefixes to skip for name inference
GENERIC_EMAIL_PREFIXES = {
    'facturas', 'info', 'admin', 'support', 'ventas', 'contacto',
    'efacturacliente', 'noreply', 'no-reply', 'hello', 'sales',
    'contact', 'team', 'help', 'billing', 'accounts', 'office'
}


def infer_name_from_email(email: Optional[str]) -> Optional[str]:
    """Infer name from email pattern like first.last@domain.com.

    Args:
        email: Email address to parse

    Returns:
        Inferred name like "First Last" or None if not parseable
    """
    if not email or not isinstance(email, str):
        return None

    # Get local part (before @)
    if '@' not in email:
        return None

    local_part = email.split('@')[0].lower().strip()

    # Skip generic/role-based emails
    if local_part in GENERIC_EMAIL_PREFIXES:
        return None

    # Try to split on common separators: . _ -
    for sep in ['.', '_', '-']:
        if sep in local_part:
            parts = local_part.split(sep)
            if len(parts) == 2:
                first, last = parts[0], parts[1]
                # Both parts must be > 1 char (skip initials like j.smith)
                if len(first) > 1 and len(last) > 1:
                    return f"{first.title()} {last.title()}"

    return None


def get_name_from_apollo(email: str, api_key: Optional[str] = None) -> Optional[str]:
    """Query Apollo people/match with email to get name.

    Args:
        email: Email address to look up
        api_key: Apollo API key (defaults to env var)

    Returns:
        Person's name or None if not found
    """
    key = api_key or APOLLO_API_KEY

    if not key:
        logger.debug("APOLLO_API_KEY not configured for name lookup")
        return None

    if not email:
        return None

    try:
        response = requests.post(
            APOLLO_MATCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": key
            },
            json={"email": email},
            timeout=15
        )

        if not response.ok:
            logger.debug(f"Apollo API error: {response.status_code} for {email}")
            return None

        data = response.json()
        person = data.get('person')

        if not person:
            return None

        # Return full name or first_name as fallback
        return person.get('name') or person.get('first_name')

    except Exception as e:
        logger.debug(f"Apollo name lookup error for {email}: {e}")
        return None


def run_name_enrichment(
    df: pd.DataFrame,
    use_apollo: bool = True,
    dry_run: bool = False,
    save_path: Optional[Path] = None
) -> pd.DataFrame:
    """Enrich missing names from email addresses.

    Stage 0: Infer from email (free)
    Stage 1: Apollo people/match (paid)

    Args:
        df: DataFrame with 'name' and 'email' columns
        use_apollo: Whether to use Apollo API for lookups
        dry_run: If True, don't make API calls
        save_path: Path to save incremental progress

    Returns:
        DataFrame with enriched names
    """
    df = df.copy()

    # Find contacts missing names but having email
    needs_name = df[
        (df['name'].isna() | (df['name'] == '') | (df['name'].str.strip() == '')) &
        df['email'].notna() &
        (df['email'] != '') &
        (~df['email'].str.contains('linkedin.com', na=False))
    ]

    logger.info(f"Contacts needing name enrichment: {len(needs_name)}")

    if len(needs_name) == 0:
        print("No contacts need name enrichment")
        return df

    stats = {'inferred': 0, 'apollo': 0, 'not_found': 0}

    if dry_run:
        print("\nDRY RUN - Would process these contacts:")
        for _, row in needs_name.iterrows():
            email = row['email']
            inferred = infer_name_from_email(email)
            status = f"→ {inferred}" if inferred else "(needs Apollo)"
            print(f"  {email} {status}")
        print(f"\nEstimated Apollo queries: {len(needs_name) - sum(1 for _, r in needs_name.iterrows() if infer_name_from_email(r['email']))}")
        return df

    print("\n=== NAME ENRICHMENT ===")

    # Process each contact
    for idx in tqdm(needs_name.index, desc="Enriching names"):
        row = df.loc[idx]
        email = row['email']

        # Stage 0: Try to infer from email
        inferred_name = infer_name_from_email(email)
        if inferred_name:
            df.at[idx, 'name'] = inferred_name
            stats['inferred'] += 1
            logger.debug(f"Inferred: {email} → {inferred_name}")
            continue

        # Stage 1: Apollo lookup
        if use_apollo and APOLLO_API_KEY:
            apollo_name = get_name_from_apollo(email)
            if apollo_name:
                df.at[idx, 'name'] = apollo_name
                stats['apollo'] += 1
                logger.info(f"Apollo: {email} → {apollo_name}")
                time.sleep(0.5)  # Rate limit
                continue

        stats['not_found'] += 1
        logger.debug(f"Not found: {email}")

    # Save if path provided
    if save_path:
        df.to_csv(save_path, index=False)

    # Summary
    print("\n" + "=" * 60)
    print("NAME ENRICHMENT RESULTS")
    print("=" * 60)
    print(f"Inferred from email: {stats['inferred']}")
    print(f"Found via Apollo:    {stats['apollo']}")
    print(f"Not found:           {stats['not_found']}")
    print(f"Total processed:     {len(needs_name)}")

    if stats['apollo'] > 0:
        print(f"Estimated cost:      ${stats['apollo'] * COST_APOLLO_MATCH:.2f}")

    return df


def is_linkedin_url(value: Any) -> bool:
    """Check if a value is a LinkedIn profile URL."""
    if not value or not isinstance(value, str):
        return False
    value = value.strip().lower()
    return 'linkedin.com/in/' in value


def fix_csv_structure(df: pd.DataFrame) -> pd.DataFrame:
    """Move LinkedIn URLs from email column to linkedin_url column.

    Args:
        df: DataFrame with potential LinkedIn URLs in email column

    Returns:
        DataFrame with proper structure (linkedin_url column, clean email column)
    """
    df = df.copy()

    # Add linkedin_url column if missing
    if 'linkedin_url' not in df.columns:
        df['linkedin_url'] = ''

    # Add phone column if missing
    if 'phone' not in df.columns:
        df['phone'] = ''

    # Add email_source column if missing
    if 'email_source' not in df.columns:
        df['email_source'] = ''

    # Find rows where email is actually a LinkedIn URL
    moved_count = 0
    for idx, row in df.iterrows():
        email_val = row.get('email', '')
        if is_linkedin_url(email_val):
            # Move to linkedin_url column
            df.at[idx, 'linkedin_url'] = email_val
            df.at[idx, 'email'] = ''
            moved_count += 1

    logger.info(f"Moved {moved_count} LinkedIn URLs from email to linkedin_url column")
    return df


def enrich_via_apollo_match(linkedin_url: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Query Apollo people/match API with LinkedIn URL.

    Args:
        linkedin_url: LinkedIn profile URL
        api_key: Apollo API key (defaults to env var)

    Returns:
        dict with email, name, phone, company, source if found
    """
    key = api_key or APOLLO_API_KEY
    result = {
        'email': None,
        'name': None,
        'phone': None,
        'company': None,
        'source': None,
    }

    if not key:
        logger.error("APOLLO_API_KEY not configured")
        return result

    if not linkedin_url:
        return result

    try:
        response = requests.post(
            APOLLO_MATCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": key
            },
            json={
                "linkedin_url": linkedin_url,
                "reveal_personal_emails": False
            },
            timeout=15
        )

        if not response.ok:
            logger.warning(f"Apollo API error: {response.status_code} for {linkedin_url}")
            return result

        data = response.json()
        person = data.get('person')

        if not person:
            logger.debug(f"No match found in Apollo for {linkedin_url}")
            return result

        # Extract email
        email = person.get('email')
        if email and '@' in str(email):
            result['email'] = email
            result['source'] = 'apollo'

        # Extract name
        result['name'] = person.get('name') or person.get('first_name', '')

        # Extract phone
        phone_numbers = person.get('phone_numbers', [])
        if phone_numbers and isinstance(phone_numbers, list):
            result['phone'] = phone_numbers[0].get('number') if phone_numbers else None

        # Extract company
        org = person.get('organization', {})
        if org:
            result['company'] = org.get('name')

        if result['email']:
            logger.info(f"Apollo found: {linkedin_url} → {result['email']}")

        return result

    except requests.exceptions.Timeout:
        logger.error(f"Apollo timeout for {linkedin_url}")
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"Apollo request error for {linkedin_url}: {e}")
        return result
    except Exception as e:
        logger.error(f"Apollo unexpected error for {linkedin_url}: {e}")
        return result


def enrich_via_apify(linkedin_urls: List[str]) -> List[Dict[str, Any]]:
    """Scrape LinkedIn profiles via Apify.

    Args:
        linkedin_urls: List of LinkedIn profile URLs to scrape

    Returns:
        List of dicts with linkedin_url, email, phone, name
    """
    if not APIFY_API_KEY:
        logger.error("APIFY_API_KEY not configured")
        return []

    if not linkedin_urls:
        return []

    try:
        from apify_client import ApifyClient
    except ImportError:
        logger.error("apify-client not installed. Run: pip install apify-client")
        return []

    client = ApifyClient(APIFY_API_KEY)

    run_input = {
        "profileUrls": linkedin_urls,
    }

    logger.info(f"Scraping {len(linkedin_urls)} LinkedIn profiles via Apify...")

    try:
        run = client.actor(APIFY_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=300,
            memory_mbytes=512,
        )

        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            profile = {
                'linkedin_url': item.get('linkedinUrl', '') or item.get('linkedinPublicUrl', ''),
                'email': None,
                'phone': None,
                'name': item.get('fullName', '') or item.get('name', ''),
            }

            # Extract email
            email = item.get('email')
            if email and '@' in str(email):
                profile['email'] = email

            # Extract phone
            phone = (
                item.get('mobileNumber') or
                item.get('phone') or
                item.get('phoneNumber')
            )
            if phone:
                profile['phone'] = phone

            results.append(profile)

        logger.info(f"Apify returned {len(results)} profiles")
        return results

    except Exception as e:
        logger.error(f"Apify error: {e}")
        return []


def run_enrichment_pipeline(
    df: pd.DataFrame,
    stages: List[str],
    dry_run: bool = False,
    save_path: Optional[Path] = None
) -> pd.DataFrame:
    """Run enrichment pipeline on DataFrame.

    Args:
        df: DataFrame with linkedin_url column
        stages: List of stages to run ('apollo', 'apify')
        dry_run: If True, don't make API calls
        save_path: Path to save incremental progress

    Returns:
        Enriched DataFrame
    """
    df = df.copy()

    # Ensure required columns exist
    for col in ['linkedin_url', 'email', 'phone', 'email_source']:
        if col not in df.columns:
            df[col] = ''

    # Find contacts needing enrichment (have LinkedIn URL, no email)
    needs_enrichment = df[
        df['linkedin_url'].notna() &
        (df['linkedin_url'] != '') &
        (df['email'].isna() | (df['email'] == ''))
    ]

    logger.info(f"Contacts needing enrichment: {len(needs_enrichment)}")

    if dry_run:
        print("\nDRY RUN - Would process these contacts:")
        for _, row in needs_enrichment.head(10).iterrows():
            print(f"  {row['name']}: {row['linkedin_url']}")
        if len(needs_enrichment) > 10:
            print(f"  ... and {len(needs_enrichment) - 10} more")

        # Cost estimate
        apollo_cost = len(needs_enrichment) * COST_APOLLO_MATCH if 'apollo' in stages else 0
        apify_estimate = len(needs_enrichment) * 0.5 * COST_APIFY_PROFILE  # Assume 50% fallback
        print(f"\nEstimated cost:")
        if 'apollo' in stages:
            print(f"  Apollo: ${apollo_cost:.2f} ({len(needs_enrichment)} queries × ${COST_APOLLO_MATCH})")
        if 'apify' in stages:
            print(f"  Apify (fallback): ~${apify_estimate:.2f}")
        print(f"  Total: ~${apollo_cost + apify_estimate:.2f}")
        return df

    stats = {'apollo_found': 0, 'apify_found': 0, 'total_processed': 0}

    # Stage 1: Apollo enrichment
    if 'apollo' in stages:
        print("\n=== STAGE 1: Apollo people/match ===")

        if not APOLLO_API_KEY:
            print("WARNING: APOLLO_API_KEY not set, skipping Apollo stage")
        else:
            for idx in tqdm(needs_enrichment.index, desc="Apollo"):
                row = df.loc[idx]
                linkedin_url = row['linkedin_url']

                if not linkedin_url or (row['email'] and row['email'] != ''):
                    continue

                result = enrich_via_apollo_match(linkedin_url)

                if result.get('email'):
                    df.at[idx, 'email'] = result['email']
                    df.at[idx, 'email_source'] = 'apollo'
                    stats['apollo_found'] += 1

                    if result.get('phone') and not row.get('phone'):
                        df.at[idx, 'phone'] = result['phone']

                stats['total_processed'] += 1

                # Save incrementally every 10 records
                if save_path and stats['total_processed'] % 10 == 0:
                    df.to_csv(save_path, index=False)
                    logger.debug(f"Progress saved: {stats['total_processed']} processed")

                # Rate limit
                time.sleep(0.5)

            # Save after Apollo stage
            if save_path:
                df.to_csv(save_path, index=False)

            print(f"Apollo found: {stats['apollo_found']} emails")

    # Stage 2: Apify fallback
    if 'apify' in stages:
        print("\n=== STAGE 2: Apify LinkedIn scraper ===")

        # Find remaining contacts without email
        still_needs = df[
            df['linkedin_url'].notna() &
            (df['linkedin_url'] != '') &
            (df['email'].isna() | (df['email'] == ''))
        ]

        logger.info(f"Contacts for Apify fallback: {len(still_needs)}")

        if len(still_needs) == 0:
            print("No contacts need Apify fallback")
        elif not APIFY_API_KEY:
            print("WARNING: APIFY_API_KEY not set, skipping Apify stage")
        else:
            # Process in batches
            urls = still_needs['linkedin_url'].tolist()
            url_to_idx = {row['linkedin_url']: idx for idx, row in still_needs.iterrows()}

            for i in range(0, len(urls), APIFY_BATCH_SIZE):
                batch_urls = urls[i:i + APIFY_BATCH_SIZE]
                print(f"Batch {i // APIFY_BATCH_SIZE + 1}: Processing {len(batch_urls)} profiles...")

                results = enrich_via_apify(batch_urls)

                for profile in results:
                    linkedin_url = profile.get('linkedin_url', '')

                    # Find matching row
                    matching_idx = None
                    for url, idx in url_to_idx.items():
                        if url and linkedin_url and (url in linkedin_url or linkedin_url in url):
                            matching_idx = idx
                            break

                    if matching_idx is None:
                        continue

                    if profile.get('email'):
                        df.at[matching_idx, 'email'] = profile['email']
                        df.at[matching_idx, 'email_source'] = 'apify'
                        stats['apify_found'] += 1

                    if profile.get('phone') and not df.at[matching_idx, 'phone']:
                        df.at[matching_idx, 'phone'] = profile['phone']

                # Save incrementally
                if save_path:
                    df.to_csv(save_path, index=False)

                # Rate limit between batches
                if i + APIFY_BATCH_SIZE < len(urls):
                    time.sleep(2)

            print(f"Apify found: {stats['apify_found']} emails")

    # Final summary
    total_found = stats['apollo_found'] + stats['apify_found']
    print("\n" + "=" * 60)
    print("ENRICHMENT RESULTS")
    print("=" * 60)
    print(f"Processed:      {stats['total_processed']}")
    print(f"Apollo found:   {stats['apollo_found']}")
    print(f"Apify found:    {stats['apify_found']}")
    print(f"Total found:    {total_found}")

    # Cost summary
    cost = stats['total_processed'] * COST_APOLLO_MATCH + stats['apify_found'] * COST_APIFY_PROFILE
    print(f"Estimated cost: ${cost:.2f}")

    return df


def main():
    print("=" * 60)
    print("REALTOR AGENT ENRICHER")
    print("=" * 60)

    parser = argparse.ArgumentParser(
        description='Enrich realtor contacts with LinkedIn URLs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--input', required=True, help='Input CSV file')
    parser.add_argument('--output', help='Output CSV (default: overwrites input)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making API calls')
    parser.add_argument('--fix-only', action='store_true', help='Only fix CSV structure, no enrichment')
    parser.add_argument('--stage', choices=['apollo', 'apify'], help='Run specific stage only')
    parser.add_argument('--all', action='store_true', help='Run full pipeline (fix + apollo + apify)')
    parser.add_argument('--names', action='store_true', help='Enrich missing names from emails')
    parser.add_argument('--no-apollo', action='store_true', help='Skip Apollo API calls (for --names)')
    args = parser.parse_args()

    # Load CSV
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return 1

    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} contacts from {input_path}")

    output_path = Path(args.output) if args.output else input_path

    # Create backup
    if output_path.exists() and output_path == input_path:
        backup_path = input_path.with_suffix('.csv.backup')
        shutil.copy(input_path, backup_path)
        logger.info(f"Backup created: {backup_path}")

    # Step 1: Fix CSV structure
    print("\n=== STEP 1: Fix CSV Structure ===")

    # Count LinkedIn URLs in email column before fix
    linkedin_in_email = df[df['email'].apply(is_linkedin_url) if 'email' in df.columns else False]
    print(f"LinkedIn URLs in email column: {len(linkedin_in_email)}")

    df = fix_csv_structure(df)

    # Save after structure fix
    df.to_csv(output_path, index=False)
    print(f"CSV structure fixed, saved to {output_path}")

    if args.fix_only:
        print("\n--fix-only specified, skipping enrichment")
        return 0

    # Name enrichment mode
    if args.names:
        print("\n=== NAME ENRICHMENT MODE ===")

        # Count contacts missing names
        no_name = df[
            (df['name'].isna() | (df['name'] == '') | (df['name'].str.strip() == '')) &
            df['email'].notna() & (df['email'] != '')
        ]
        print(f"Contacts missing names (with email): {len(no_name)}")

        df = run_name_enrichment(
            df,
            use_apollo=not args.no_apollo,
            dry_run=args.dry_run,
            save_path=output_path if not args.dry_run else None
        )

        # Final save
        if not args.dry_run:
            df.to_csv(output_path, index=False)
            print(f"\nSaved to: {output_path}")

        # Final stats
        still_no_name = df[
            (df['name'].isna() | (df['name'] == '') | (df['name'].str.strip() == ''))
        ]
        print(f"\n=== FINAL STATS ===")
        print(f"Total contacts:        {len(df)}")
        print(f"Still missing names:   {len(still_no_name)}")
        return 0

    # Determine stages to run
    if args.all:
        stages = ['apollo', 'apify']
    elif args.stage:
        stages = [args.stage]
    else:
        stages = ['apollo', 'apify']  # Default to full pipeline

    # Run enrichment
    df = run_enrichment_pipeline(
        df,
        stages=stages,
        dry_run=args.dry_run,
        save_path=output_path if not args.dry_run else None
    )

    # Final save
    if not args.dry_run:
        df.to_csv(output_path, index=False)
        print(f"\nSaved to: {output_path}")

    # Final stats
    total = len(df)
    with_email = df['email'].notna() & (df['email'] != '')
    with_linkedin = df['linkedin_url'].notna() & (df['linkedin_url'] != '')

    print("\n=== FINAL STATS ===")
    print(f"Total contacts:     {total}")
    print(f"With email:         {with_email.sum()}")
    print(f"With LinkedIn URL:  {with_linkedin.sum()}")
    print(f"LinkedIn only:      {(with_linkedin & ~with_email).sum()}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
