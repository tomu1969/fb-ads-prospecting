"""Email Verifier - Professional email verification with catch-all detection.

Uses MillionVerifier API for high-accuracy verification (99%+).
Supports single email and bulk verification with CSV processing.
"""

import os
import re
import time
import logging
import argparse
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Configuration
MILLIONVERIFIER_API_KEY = os.getenv('MILLIONVERIFIER_API_KEY')
MILLIONVERIFIER_URL = 'https://api.millionverifier.com/api/v3/'

# Generic email prefixes that are often catch-all
GENERIC_PREFIXES = {
    'info', 'sales', 'contact', 'hello', 'support', 'admin',
    'office', 'team', 'general', 'inquiries', 'help', 'service',
    'mail', 'enquiries', 'marketing', 'billing', 'accounts'
}

logger = logging.getLogger(__name__)


class VerificationStatus(Enum):
    """Email verification status codes."""
    OK = 'ok'                    # Verified deliverable
    CATCH_ALL = 'catch_all'      # Server accepts all (risky)
    INVALID = 'invalid'          # Hard bounce guaranteed
    UNKNOWN = 'unknown'          # Could not verify
    ERROR = 'error'              # API/network error


@dataclass
class VerificationResult:
    """Result of email verification."""
    email: str
    status: VerificationStatus
    is_catch_all: bool = False
    is_deliverable: bool = False
    confidence: int = 0
    is_free: bool = False
    is_role: bool = False
    error: Optional[str] = None
    raw_response: Dict[str, Any] = field(default_factory=dict)

    @property
    def safe_to_send(self) -> bool:
        """Check if email is safe to send (high confidence delivery)."""
        return (
            self.status == VerificationStatus.OK and
            not self.is_catch_all and
            self.is_deliverable and
            self.confidence >= 70
        )


def extract_domain(email: Optional[str]) -> Optional[str]:
    """Extract domain from email address."""
    if not email or not isinstance(email, str):
        return None
    if '@' not in email:
        return None
    return email.split('@')[1].lower()


def is_generic_email(email: Optional[str]) -> bool:
    """Check if email uses a generic prefix (info@, sales@, etc.)."""
    if not email or not isinstance(email, str):
        return False
    if '@' not in email:
        return False

    prefix = email.split('@')[0].lower()
    return prefix in GENERIC_PREFIXES


def verify_email(
    email: str,
    api_key: Optional[str] = None,
    timeout: int = 30
) -> VerificationResult:
    """
    Verify a single email using MillionVerifier API.

    Args:
        email: Email address to verify.
        api_key: MillionVerifier API key (uses env var if not provided).
        timeout: Request timeout in seconds.

    Returns:
        VerificationResult with status and metadata.
    """
    api_key = api_key or MILLIONVERIFIER_API_KEY

    if not api_key:
        return VerificationResult(
            email=email,
            status=VerificationStatus.UNKNOWN,
            error='No API key provided (set MILLIONVERIFIER_API_KEY)'
        )

    try:
        response = requests.get(
            MILLIONVERIFIER_URL,
            params={
                'api': api_key,
                'email': email
            },
            timeout=timeout
        )

        if response.status_code != 200:
            return VerificationResult(
                email=email,
                status=VerificationStatus.ERROR,
                error=f'API error: HTTP {response.status_code}'
            )

        data = response.json()
        return _parse_millionverifier_response(email, data)

    except requests.exceptions.Timeout:
        return VerificationResult(
            email=email,
            status=VerificationStatus.ERROR,
            error='API timeout'
        )
    except requests.exceptions.RequestException as e:
        return VerificationResult(
            email=email,
            status=VerificationStatus.ERROR,
            error=f'Request error: {str(e)}'
        )
    except Exception as e:
        return VerificationResult(
            email=email,
            status=VerificationStatus.ERROR,
            error=f'Unexpected error: {str(e)}'
        )


def _parse_millionverifier_response(email: str, data: Dict[str, Any]) -> VerificationResult:
    """Parse MillionVerifier API response into VerificationResult."""
    result_code = data.get('result', '').lower()
    result_code_num = data.get('resultcode', 0)

    # Map MillionVerifier results to our status
    # Result codes: 1=ok, 2=invalid, 3=unknown, 4=catch_all
    status_map = {
        'ok': VerificationStatus.OK,
        'valid': VerificationStatus.OK,
        'invalid': VerificationStatus.INVALID,
        'unknown': VerificationStatus.UNKNOWN,
        'catch_all': VerificationStatus.CATCH_ALL,
        'disposable': VerificationStatus.INVALID,
    }

    status = status_map.get(result_code, VerificationStatus.UNKNOWN)

    # Determine deliverability and confidence
    is_catch_all = result_code == 'catch_all' or result_code_num == 4
    is_deliverable = status == VerificationStatus.OK
    is_free = data.get('free', False)
    is_role = data.get('role', False)

    # Calculate confidence score
    if status == VerificationStatus.OK:
        confidence = 95 if not is_role else 85
    elif status == VerificationStatus.CATCH_ALL:
        # Catch-all: lower confidence, especially for generic emails
        confidence = 30 if is_generic_email(email) else 50
    elif status == VerificationStatus.INVALID:
        confidence = 0
    else:
        confidence = 20

    return VerificationResult(
        email=email,
        status=status,
        is_catch_all=is_catch_all,
        is_deliverable=is_deliverable,
        confidence=confidence,
        is_free=is_free,
        is_role=is_role,
        raw_response=data
    )


def verify_emails_bulk(
    emails: List[str],
    api_key: Optional[str] = None,
    delay: float = 0.5,
    progress_callback: Optional[callable] = None
) -> List[VerificationResult]:
    """
    Verify multiple emails with rate limiting.

    Args:
        emails: List of email addresses.
        api_key: MillionVerifier API key.
        delay: Seconds between API calls (rate limiting).
        progress_callback: Optional callback(current, total) for progress.

    Returns:
        List of VerificationResults.
    """
    results = []
    total = len(emails)

    for i, email in enumerate(emails, 1):
        result = verify_email(email, api_key=api_key)
        results.append(result)

        if progress_callback:
            progress_callback(i, total)

        # Rate limiting (skip delay on last item)
        if i < total and delay > 0:
            time.sleep(delay)

    return results


def verify_csv(
    input_path: str,
    output_path: str,
    email_column: str = 'primary_email',
    api_key: Optional[str] = None,
    delay: float = 0.5,
    strict: bool = False
) -> Dict[str, Any]:
    """
    Verify emails from CSV file and save results.

    Args:
        input_path: Path to input CSV.
        output_path: Path to output CSV.
        email_column: Column name containing emails.
        api_key: MillionVerifier API key.
        delay: Seconds between API calls.
        strict: If True, only include safe_to_send emails in output.

    Returns:
        Summary statistics.
    """
    df = pd.read_csv(input_path)

    if email_column not in df.columns:
        raise ValueError(f"Column '{email_column}' not found in CSV")

    emails = df[email_column].dropna().tolist()
    total = len(emails)

    logger.info(f"Verifying {total} emails from {input_path}")

    stats = {
        'total': total,
        'ok': 0,
        'catch_all': 0,
        'invalid': 0,
        'unknown': 0,
        'error': 0,
        'safe_to_send': 0
    }

    # Initialize result columns
    df['verify_status'] = ''
    df['verify_confidence'] = 0
    df['is_catch_all'] = False
    df['safe_to_send'] = False

    for i, (idx, row) in enumerate(df.iterrows(), 1):
        email = row.get(email_column)

        if pd.isna(email) or not email:
            continue

        result = verify_email(str(email), api_key=api_key)

        # Update dataframe
        df.loc[idx, 'verify_status'] = result.status.value
        df.loc[idx, 'verify_confidence'] = result.confidence
        df.loc[idx, 'is_catch_all'] = result.is_catch_all
        df.loc[idx, 'safe_to_send'] = result.safe_to_send

        # Update stats
        stats[result.status.value] = stats.get(result.status.value, 0) + 1
        if result.safe_to_send:
            stats['safe_to_send'] += 1

        logger.info(f"[{i}/{total}] {email}: {result.status.value} (conf: {result.confidence})")

        if i < total and delay > 0:
            time.sleep(delay)

    # Filter if strict mode
    if strict:
        df = df[df['safe_to_send'] == True]
        logger.info(f"Strict mode: kept {len(df)} safe emails")

    df.to_csv(output_path, index=False)
    logger.info(f"Results saved to {output_path}")

    return stats


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Verify email deliverability using MillionVerifier API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Verify single email
  python verifier.py --email test@example.com

  # Verify CSV file
  python verifier.py --csv contacts.csv --output verified.csv

  # Verify and filter (strict mode)
  python verifier.py --csv contacts.csv --output verified.csv --strict
        """
    )

    parser.add_argument(
        '--email',
        type=str,
        help='Single email to verify'
    )

    parser.add_argument(
        '--csv',
        type=str,
        help='CSV file with emails to verify'
    )

    parser.add_argument(
        '--output',
        type=str,
        help='Output CSV path (for --csv mode)'
    )

    parser.add_argument(
        '--column',
        type=str,
        default='primary_email',
        help='Email column name in CSV (default: primary_email)'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Seconds between API calls (default: 0.5)'
    )

    parser.add_argument(
        '--strict',
        action='store_true',
        help='Only output emails that are safe to send'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    if not MILLIONVERIFIER_API_KEY:
        logger.error("MILLIONVERIFIER_API_KEY not set in environment")
        logger.error("Sign up at https://www.millionverifier.com/ and add key to .env")
        return 1

    if args.email:
        # Single email mode
        result = verify_email(args.email)
        print(f"\nEmail: {result.email}")
        print(f"Status: {result.status.value}")
        print(f"Confidence: {result.confidence}%")
        print(f"Catch-all: {result.is_catch_all}")
        print(f"Safe to send: {result.safe_to_send}")
        if result.error:
            print(f"Error: {result.error}")
        return 0 if result.safe_to_send else 1

    elif args.csv:
        # CSV mode
        if not args.output:
            args.output = args.csv.replace('.csv', '_verified.csv')

        stats = verify_csv(
            input_path=args.csv,
            output_path=args.output,
            email_column=args.column,
            delay=args.delay,
            strict=args.strict
        )

        print("\n" + "=" * 50)
        print("VERIFICATION SUMMARY")
        print("=" * 50)
        print(f"Total: {stats['total']}")
        print(f"OK (verified): {stats['ok']}")
        print(f"Catch-all (risky): {stats['catch_all']}")
        print(f"Invalid (bounce): {stats['invalid']}")
        print(f"Unknown: {stats['unknown']}")
        print(f"Errors: {stats['error']}")
        print("-" * 50)
        print(f"Safe to send: {stats['safe_to_send']}")
        print("=" * 50)

        return 0

    else:
        logger.error("Specify --email or --csv")
        return 1


if __name__ == '__main__':
    exit(main())
