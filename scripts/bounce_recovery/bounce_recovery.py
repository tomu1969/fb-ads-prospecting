"""Bounce Recovery - Find alternative emails for bounced contacts.

Attempts to recover bounced contacts by:
1. Trying generic email patterns (info@, contact@, etc.)
2. Searching Hunter.io for alternative contacts at the same domain
3. Searching Apollo.io B2B database for alternative contacts

Usage:
    python scripts/bounce_recovery/bounce_recovery.py \
        --input config/bounced_contacts.csv \
        --output output/recovered_contacts.csv
"""

import argparse
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
HUNTER_BASE_URL = 'https://api.hunter.io/v2'

# Apollo enricher import (for Strategy 3)
try:
    from scripts.apollo_enricher import search_apollo_alternatives
    APOLLO_AVAILABLE = True
except ImportError:
    try:
        # Try relative import when running from scripts dir
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from apollo_enricher import search_apollo_alternatives
        APOLLO_AVAILABLE = True
    except ImportError:
        APOLLO_AVAILABLE = False

# MillionVerifier import (for Strategy 0 - re-verify original)
try:
    from scripts.email_verifier.verifier import verify_email as millionverify_email, VerificationStatus
    MILLIONVERIFIER_AVAILABLE = True
except ImportError:
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from email_verifier.verifier import verify_email as millionverify_email, VerificationStatus
        MILLIONVERIFIER_AVAILABLE = True
    except ImportError:
        MILLIONVERIFIER_AVAILABLE = False

# Generic email patterns to try (in order of priority)
GENERIC_PATTERNS = [
    'info',
    'contact',
    'hello',
    'sales',
    'support',
    'admin',
    'office',
    'team',
    'inquiries',
    'general'
]


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the recovery module."""
    logger = logging.getLogger('bounce_recovery')
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def extract_domain(email: Optional[str]) -> Optional[str]:
    """Extract domain from email address."""
    if not email or not isinstance(email, str) or '@' not in email:
        return None
    return email.split('@')[1].lower()


def extract_domain_from_url(url: Optional[str]) -> Optional[str]:
    """Extract domain from website URL."""
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.replace('www.', '').lower()
        return domain.split('/')[0]  # Remove any path
    except:
        return None


def generate_generic_emails(domain: Optional[str]) -> List[str]:
    """Generate common generic email patterns for a domain."""
    if not domain:
        return []
    return [f'{prefix}@{domain}' for prefix in GENERIC_PATTERNS]


def verify_email_with_hunter(email: str) -> Dict[str, Any]:
    """Verify email using Hunter.io API."""
    if not HUNTER_API_KEY:
        return {'status': 'unknown', 'error': 'No Hunter API key'}

    try:
        resp = requests.get(
            f'{HUNTER_BASE_URL}/email-verifier',
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            return {
                'status': data.get('status'),
                'score': data.get('score'),
                'deliverable': data.get('status') in ['valid', 'accept_all']
            }
        elif resp.status_code == 429:
            return {'status': 'rate_limited', 'error': 'Hunter API rate limit'}
        else:
            return {'status': 'error', 'error': f'HTTP {resp.status_code}'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def try_generic_emails(domain: str, bounced_email: str) -> Optional[str]:
    """Try generic email patterns and verify with Hunter.

    Args:
        domain: The email domain
        bounced_email: The original bounced email (to exclude)

    Returns:
        First valid generic email found, or None
    """
    logger = logging.getLogger('bounce_recovery')
    generics = generate_generic_emails(domain)

    for generic in generics:
        if generic.lower() == bounced_email.lower():
            continue

        logger.debug(f"  Trying generic: {generic}")
        result = verify_email_with_hunter(generic)

        # Consider 'accept_all' as potentially valid for generic emails
        # Generic emails at catch-all domains are often real
        if result.get('status') in ['valid', 'accept_all']:
            logger.info(f"  Found valid generic: {generic} ({result.get('status')})")
            return generic

        time.sleep(0.3)  # Rate limiting

    return None


def get_hunter_alternatives(
    domain: str,
    exclude: Optional[str] = None,
    limit: int = 5
) -> List[Dict[str, Any]]:
    """Search Hunter for alternative contacts at domain.

    Args:
        domain: The email domain to search
        exclude: Email to exclude from results
        limit: Maximum alternatives to return

    Returns:
        List of alternative contacts with email and confidence
    """
    logger = logging.getLogger('bounce_recovery')

    if not HUNTER_API_KEY:
        logger.warning("No Hunter API key configured")
        return []

    try:
        resp = requests.get(
            f'{HUNTER_BASE_URL}/domain-search',
            params={
                'domain': domain,
                'api_key': HUNTER_API_KEY,
                'limit': limit + 1  # Extra in case we need to exclude
            },
            timeout=10
        )

        if resp.status_code == 200:
            data = resp.json().get('data', {})
            emails_data = data.get('emails', [])

            # Sort by confidence and filter out excluded email
            alternatives = []
            for contact in sorted(emails_data, key=lambda x: x.get('confidence', 0), reverse=True):
                email = contact.get('value', '').lower()
                if exclude and email == exclude.lower():
                    continue

                alternatives.append({
                    'email': email,
                    'confidence': contact.get('confidence', 0),
                    'first_name': contact.get('first_name', ''),
                    'last_name': contact.get('last_name', ''),
                    'position': contact.get('position', '')
                })

                if len(alternatives) >= limit:
                    break

            return alternatives

        elif resp.status_code == 429:
            logger.warning("Hunter API rate limit reached")
            return []
        else:
            logger.warning(f"Hunter API error: HTTP {resp.status_code}")
            return []

    except Exception as e:
        logger.error(f"Hunter API error: {e}")
        return []


def recover_contact(
    email: str,
    domain: str,
    try_reverify: bool = True,
    try_generics: bool = True,
    try_hunter: bool = True,
    try_apollo: bool = True
) -> Dict[str, Any]:
    """Attempt to recover a bounced contact.

    Args:
        email: The bounced email address
        domain: The domain to search for alternatives
        try_reverify: Whether to re-verify original email (bounces can be temporary)
        try_generics: Whether to try generic email patterns
        try_hunter: Whether to search Hunter for alternatives
        try_apollo: Whether to search Apollo B2B database

    Returns:
        Dict with recovery results
    """
    logger = logging.getLogger('bounce_recovery')
    result = {
        'original_email': email,
        'domain': domain,
        'recovered': False,
        'new_email': None,
        'recovery_method': None,
        'alternatives': []
    }

    # Strategy 0: Re-verify original email (bounces can be temporary)
    # Uses MillionVerifier for 99%+ accuracy
    if try_reverify and MILLIONVERIFIER_AVAILABLE and email:
        logger.debug(f"Strategy 0: Re-verifying original email {email}")
        try:
            verification = millionverify_email(email)
            if verification.status == VerificationStatus.OK:
                logger.info(f"Original email {email} is now valid (was temporary bounce)")
                result['recovered'] = True
                result['new_email'] = email
                result['recovery_method'] = 'reverify_original'
                return result
            else:
                logger.debug(f"Original email still invalid: {verification.status.value}")
        except Exception as e:
            logger.warning(f"Re-verification failed: {e}")

    # Strategy 1: Try generic emails
    if try_generics:
        logger.debug(f"Strategy 1: Trying generic emails for {domain}")
        generic = try_generic_emails(domain, email)
        if generic:
            result['recovered'] = True
            result['new_email'] = generic
            result['recovery_method'] = 'generic_email'
            return result

    # Strategy 2: Search Hunter for alternative contacts
    if try_hunter:
        logger.debug(f"Strategy 2: Searching Hunter for alternatives at {domain}")
        alternatives = get_hunter_alternatives(domain, exclude=email)
        result['alternatives'] = alternatives

        if alternatives:
            # Take the highest confidence alternative
            best = alternatives[0]
            result['recovered'] = True
            result['new_email'] = best['email']
            result['recovery_method'] = 'hunter_alt'
            result['new_contact_name'] = f"{best.get('first_name', '')} {best.get('last_name', '')}".strip()
            result['new_contact_position'] = best.get('position', '')
            return result

    # Strategy 3: Search Apollo B2B database for alternative contacts
    if try_apollo and APOLLO_AVAILABLE:
        logger.debug(f"Strategy 3: Searching Apollo for alternatives at {domain}")
        try:
            apollo_alternatives = search_apollo_alternatives(domain, exclude_email=email)
            if apollo_alternatives:
                best = apollo_alternatives[0]
                result['recovered'] = True
                result['new_email'] = best['email']
                result['recovery_method'] = 'apollo_alt'
                result['new_contact_name'] = best.get('name', '')
                result['new_contact_position'] = best.get('title', '')
                result['alternatives'].extend(apollo_alternatives)
                return result
        except Exception as e:
            logger.warning(f"Apollo search failed: {e}")

    return result


def process_bounced_csv(
    csv_path: str,
    output_path: Optional[str] = None,
    dry_run: bool = False,
    delay: float = 1.0
) -> List[Dict[str, Any]]:
    """Process bounced contacts CSV and attempt recovery.

    Args:
        csv_path: Path to bounced contacts CSV
        output_path: Path for recovered contacts output
        dry_run: If True, don't make API calls
        delay: Delay between recovery attempts

    Returns:
        List of recovery results
    """
    logger = logging.getLogger('bounce_recovery')

    df = pd.read_csv(csv_path)
    results = []

    logger.info(f"Processing {len(df)} bounced contacts from {csv_path}")

    for idx, row in df.iterrows():
        email = row.get('primary_email', '')
        page_name = row.get('page_name', 'Unknown')
        website_url = row.get('website_url', '')

        # Get domain from email or website
        domain = extract_domain(email) or extract_domain_from_url(website_url)

        logger.info(f"\n[{idx+1}/{len(df)}] {page_name}")
        logger.info(f"  Bounced: {email}")
        logger.info(f"  Domain: {domain}")

        if not domain:
            logger.warning(f"  Cannot determine domain, skipping")
            results.append({
                'original_email': email,
                'recovered': False,
                'recovery_method': None,
                'error': 'No domain found'
            })
            continue

        if dry_run:
            logger.info(f"  [DRY RUN] Would attempt recovery for {domain}")
            results.append({
                'original_email': email,
                'recovered': False,
                'recovery_method': 'dry_run'
            })
            continue

        # Attempt recovery
        result = recover_contact(email, domain)
        result['page_name'] = page_name
        result['contact_name'] = row.get('contact_name', '')
        result['hook_used'] = row.get('hook_used', '')
        result['hook_source'] = row.get('hook_source', '')
        result['website_url'] = website_url

        if result['recovered']:
            logger.info(f"  RECOVERED: {result['new_email']} ({result['recovery_method']})")
        else:
            logger.warning(f"  NOT RECOVERED - no alternatives found")

        results.append(result)

        # Delay between contacts
        if idx < len(df) - 1 and delay > 0:
            time.sleep(delay)

    # Save results
    if output_path:
        # Create output dataframe
        output_data = []
        for r in results:
            output_data.append({
                'page_name': r.get('page_name', ''),
                'contact_name': r.get('new_contact_name') or r.get('contact_name', ''),
                'original_email': r.get('original_email', ''),
                'recovered_email': r.get('new_email', ''),
                'recovery_method': r.get('recovery_method', ''),
                'recovered': r.get('recovered', False),
                'hook_used': r.get('hook_used', ''),
                'hook_source': r.get('hook_source', ''),
                'website_url': r.get('website_url', ''),
                'alternatives_found': len(r.get('alternatives', []))
            })

        output_df = pd.DataFrame(output_data)
        output_df.to_csv(output_path, index=False)
        logger.info(f"\nSaved recovery results to {output_path}")

    return results


def print_recovery_summary(results: List[Dict[str, Any]]) -> None:
    """Print summary of recovery results."""
    logger = logging.getLogger('bounce_recovery')

    total = len(results)
    recovered = sum(1 for r in results if r.get('recovered'))
    unrecoverable = total - recovered

    methods = {}
    for r in results:
        method = r.get('recovery_method')
        if method and r.get('recovered'):
            methods[method] = methods.get(method, 0) + 1

    logger.info("\n" + "=" * 50)
    logger.info("RECOVERY SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total bounced: {total}")
    logger.info(f"Recovered: {recovered} ({100*recovered/total:.1f}%)")
    logger.info(f"Unrecoverable: {unrecoverable}")
    logger.info("")
    logger.info("Recovery methods:")
    for method, count in sorted(methods.items(), key=lambda x: -x[1]):
        logger.info(f"  {method}: {count}")
    logger.info("=" * 50)


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Bounce Recovery - Find alternative emails for bounced contacts'
    )

    parser.add_argument('--input', type=str, required=True,
                       help='Input CSV with bounced contacts')
    parser.add_argument('--output', type=str,
                       help='Output CSV for recovered contacts')
    parser.add_argument('--dry-run', action='store_true',
                       help='Do not make API calls')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between recovery attempts (default: 1.0)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')

    return parser.parse_args(args)


def main():
    """Main entry point."""
    args = parse_args(sys.argv[1:])
    logger = setup_logging(verbose=args.verbose)

    # Set default output path if not specified
    output_path = args.output
    if not output_path:
        base = os.path.splitext(args.input)[0]
        output_path = f"{base}_recovered.csv"

    logger.info("=" * 60)
    logger.info("BOUNCE RECOVERY")
    logger.info("=" * 60)
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {output_path}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("")

    results = process_bounced_csv(
        csv_path=args.input,
        output_path=output_path,
        dry_run=args.dry_run,
        delay=args.delay
    )

    print_recovery_summary(results)


if __name__ == '__main__':
    main()
