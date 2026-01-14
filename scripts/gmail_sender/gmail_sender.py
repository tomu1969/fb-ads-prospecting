"""Gmail Sender Module - Send cold emails via SMTP.

Sends drafted emails from CSV using Gmail SMTP with App Password authentication.
Includes comprehensive logging, dry-run mode, resume capability, and advanced
email verification with MillionVerifier API and multi-factor scoring.
"""

import os
import re
import smtplib
import logging
import argparse
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Tuple, Optional, Dict, Any, List

import pandas as pd
import requests
from dotenv import load_dotenv

# Import email verifier for advanced verification
try:
    from scripts.email_verifier.verifier import (
        verify_email as verify_email_api,
        VerificationStatus,
        is_generic_email
    )
    from scripts.email_verifier.scorer import (
        calculate_send_score,
        score_for_sending,
        SendRecommendation
    )
    EMAIL_VERIFIER_AVAILABLE = True
except ImportError:
    EMAIL_VERIFIER_AVAILABLE = False

load_dotenv()

# Hunter.io configuration
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
HUNTER_BASE_URL = 'https://api.hunter.io/v2'
MILLIONVERIFIER_API_KEY = os.getenv('MILLIONVERIFIER_API_KEY')

# Configuration
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')  # Login address
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
GMAIL_SEND_AS = os.getenv('GMAIL_SEND_AS', GMAIL_ADDRESS)  # "From" address (alias)
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Do not contact list path
DO_NOT_CONTACT_PATH = 'config/do_not_contact.csv'

# Module logger
logger = logging.getLogger(__name__)


def load_do_not_contact_list() -> set:
    """
    Load the do_not_contact list from CSV.

    Returns:
        Set of email addresses that should not be contacted.
    """
    blocked_emails = set()

    if not os.path.exists(DO_NOT_CONTACT_PATH):
        logger.debug(f"No do_not_contact file found at {DO_NOT_CONTACT_PATH}")
        return blocked_emails

    try:
        df = pd.read_csv(DO_NOT_CONTACT_PATH)
        if 'email' in df.columns:
            blocked_emails = set(df['email'].dropna().str.lower().str.strip())
            logger.info(f"Loaded {len(blocked_emails)} emails from do_not_contact list")
    except Exception as e:
        logger.warning(f"Error loading do_not_contact list: {e}")

    return blocked_emails


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging for the module.

    Args:
        verbose: If True, set level to DEBUG. Otherwise INFO.

    Returns:
        Configured logger instance.
    """
    level = logging.DEBUG if verbose else logging.INFO

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers = []

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('gmail_sender.log')
        ]
    )

    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(level)

    return module_logger


def is_valid_email(email: Optional[str]) -> bool:
    """
    Validate email format.

    Args:
        email: Email address to validate.

    Returns:
        True if valid email format, False otherwise.
    """
    if not email or not isinstance(email, str):
        return False

    # Basic email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))


def verify_email_with_hunter(email: str) -> Dict[str, Any]:
    """
    Verify email deliverability using Hunter.io API.

    Args:
        email: Email address to verify.

    Returns:
        Dict with status, deliverable flag, and details.
    """
    if not HUNTER_API_KEY:
        logger.warning("No HUNTER_API_KEY configured, skipping verification")
        return {'status': 'unchecked', 'deliverable': True, 'error': 'No API key'}

    try:
        resp = requests.get(
            f'{HUNTER_BASE_URL}/email-verifier',
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=10
        )

        if resp.status_code == 200:
            data = resp.json().get('data', {})
            status = data.get('status')
            # Consider 'valid' and 'accept_all' as deliverable
            deliverable = status in ['valid', 'accept_all']
            return {
                'status': status,
                'deliverable': deliverable,
                'score': data.get('score'),
                'error': None
            }
        elif resp.status_code == 429:
            logger.warning("Hunter API rate limit reached")
            return {'status': 'rate_limited', 'deliverable': True, 'error': 'Rate limited'}
        else:
            return {'status': 'error', 'deliverable': True, 'error': f'HTTP {resp.status_code}'}

    except Exception as e:
        logger.warning(f"Hunter verification error: {e}")
        return {'status': 'error', 'deliverable': True, 'error': str(e)}


def send_email(
    to: str,
    subject: str,
    body: str,
    login_address: str,
    password: str,
    send_as_address: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Send an email via Gmail SMTP.

    Args:
        to: Recipient email address.
        subject: Email subject line.
        body: Email body content.
        login_address: Email address for SMTP authentication.
        password: Gmail app password.
        send_as_address: "From" address (alias). Defaults to login_address.

    Returns:
        Tuple of (success: bool, error: Optional[str])
    """
    from_address = send_as_address or login_address

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Connect and send (login with main account, send from alias)
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(login_address, password)
            server.send_message(msg)

        logger.info(f"✓ Sent to {to}")
        return True, None

    except Exception as e:
        # Convert exception to clean string (handles tuples from SMTP errors)
        if isinstance(e.args, tuple) and len(e.args) >= 2:
            error_msg = f"{e.args[0]}: {e.args[1].decode() if isinstance(e.args[1], bytes) else e.args[1]}"
        else:
            error_msg = str(e)
        logger.error(f"✗ Failed to send to {to}: {error_msg}")
        return False, error_msg


def process_csv(
    csv_path: str,
    dry_run: bool = False,
    login_address: Optional[str] = None,
    password: Optional[str] = None,
    send_as_address: Optional[str] = None,
    limit: Optional[int] = None,
    delay: float = 1.0,
    skip_sent: bool = True,
    verify_first: bool = False,
    skip_invalid: bool = False,
    verify_api: bool = False,
    min_score: int = 70,
    skip_catch_all: bool = False
) -> Dict[str, Any]:
    """
    Process CSV file and send emails.

    Args:
        csv_path: Path to CSV file with email drafts.
        dry_run: If True, preview without sending.
        login_address: Email address for SMTP login.
        password: Gmail app password.
        send_as_address: "From" address (alias) to appear on emails.
        limit: Maximum number of emails to process.
        delay: Seconds to wait between sends.
        skip_sent: Skip rows already marked as sent.
        verify_first: Verify email with Hunter before sending.
        skip_invalid: Skip emails that fail verification.
        verify_api: Use MillionVerifier API for verification.
        min_score: Minimum score threshold (0-100) for sending.
        skip_catch_all: Skip catch-all domain emails.

    Returns:
        Results dictionary with counts and timing.
    """
    start_time = time.time()

    # Use environment variables if not provided
    login_address = login_address or GMAIL_ADDRESS
    password = password or GMAIL_APP_PASSWORD
    send_as_address = send_as_address or GMAIL_SEND_AS or login_address

    logger.info(f"Loading {csv_path}...")
    df = pd.read_csv(csv_path)

    # Initialize status columns if they don't exist (as string type to avoid FutureWarning)
    if 'send_status' not in df.columns:
        df['send_status'] = ''
    else:
        df['send_status'] = df['send_status'].fillna('').astype(str)

    if 'sent_at' not in df.columns:
        df['sent_at'] = ''
    else:
        df['sent_at'] = df['sent_at'].fillna('').astype(str)

    if 'send_error' not in df.columns:
        df['send_error'] = ''
    else:
        df['send_error'] = df['send_error'].fillna('').astype(str)

    # Count already sent
    already_sent = df[df['send_status'] == 'sent'].shape[0]

    # Filter rows to process
    if skip_sent:
        to_process = df[df['send_status'] != 'sent'].copy()
    else:
        to_process = df.copy()

    total_to_process = len(to_process)
    logger.info(f"Found {total_to_process} emails to process ({already_sent} already sent)")

    # Apply limit
    if limit and limit < total_to_process:
        to_process = to_process.head(limit)
        total_to_process = limit

    # Track results
    results = {
        'total': total_to_process,
        'sent': 0,
        'failed': 0,
        'skipped': already_sent if skip_sent else 0,
        'skipped_low_score': 0,
        'skipped_catch_all': 0,
        'skipped_unsubscribed': 0,
        'duration': 0
    }

    # Load do_not_contact list
    do_not_contact = load_do_not_contact_list()

    # Check if API verification is available
    if verify_api and not EMAIL_VERIFIER_AVAILABLE:
        logger.warning("Email verifier module not available, falling back to Hunter")
        verify_api = False

    if verify_api and not MILLIONVERIFIER_API_KEY:
        logger.warning("MILLIONVERIFIER_API_KEY not set, falling back to Hunter")
        verify_api = False

    # Process each row
    for i, (idx, row) in enumerate(to_process.iterrows(), 1):
        email = row.get('primary_email', '')
        subject = row.get('subject_line', '')
        body = row.get('email_body', '')

        logger.info(f"[{i}/{total_to_process}] Processing {email}...")

        # Validate email
        if not is_valid_email(email):
            logger.warning(f"Invalid email format: {email}")
            df.loc[idx, 'send_status'] = 'failed'
            df.loc[idx, 'send_error'] = 'Invalid email format'
            results['failed'] += 1
            df.to_csv(csv_path, index=False)
            continue

        # Check do_not_contact list (unsubscribes)
        if email.lower().strip() in do_not_contact:
            logger.warning(f"⛔ BLOCKED: {email} is on do_not_contact list (unsubscribed)")
            df.loc[idx, 'send_status'] = 'blocked_unsubscribed'
            df.loc[idx, 'send_error'] = 'On do_not_contact list'
            results['skipped_unsubscribed'] += 1
            df.to_csv(csv_path, index=False)
            continue

        # Advanced API verification with scoring
        if verify_api:
            logger.debug(f"  Verifying {email} with MillionVerifier API...")
            api_result = verify_email_api(email, api_key=MILLIONVERIFIER_API_KEY)

            # Get Hunter confidence if available
            hunter_confidence = row.get('hunter_confidence', None)
            if pd.isna(hunter_confidence):
                hunter_confidence = None

            # Calculate score
            score = calculate_send_score(
                email=email,
                verification_result=api_result,
                hunter_confidence=hunter_confidence
            )

            # Store verification results
            df.loc[idx, 'api_verify_status'] = api_result.status.value
            df.loc[idx, 'is_catch_all'] = api_result.is_catch_all
            df.loc[idx, 'send_score'] = score.total_score
            df.loc[idx, 'score_recommendation'] = score.recommendation.value

            logger.info(f"  Score: {score.total_score}/100 ({score.recommendation.value})")

            # Skip catch-all if requested
            if skip_catch_all and api_result.is_catch_all:
                logger.warning(f"  Skipping catch-all domain email: {email}")
                df.loc[idx, 'send_status'] = 'skipped_catch_all'
                df.loc[idx, 'send_error'] = 'Catch-all domain'
                results['skipped_catch_all'] += 1
                df.to_csv(csv_path, index=False)
                continue

            # Skip if below minimum score
            if score.total_score < min_score:
                logger.warning(f"  Score {score.total_score} below threshold {min_score}")
                df.loc[idx, 'send_status'] = 'skipped_low_score'
                df.loc[idx, 'send_error'] = f'Score {score.total_score} < {min_score}'
                results['skipped_low_score'] += 1
                df.to_csv(csv_path, index=False)
                continue

            time.sleep(0.5)  # Rate limiting for API

        # Legacy Hunter verification (fallback)
        elif verify_first:
            logger.debug(f"  Verifying {email} with Hunter...")
            verification = verify_email_with_hunter(email)
            df.loc[idx, 'hunter_verify_status'] = verification.get('status', '')

            if not verification.get('deliverable', True):
                logger.warning(f"  Email not deliverable: {email} ({verification.get('status')})")
                if skip_invalid:
                    df.loc[idx, 'send_status'] = 'skipped_invalid'
                    df.loc[idx, 'send_error'] = f"Hunter: {verification.get('status')}"
                    results['skipped'] = results.get('skipped', 0) + 1
                    df.to_csv(csv_path, index=False)
                    continue
                else:
                    logger.warning(f"  Proceeding anyway (use --skip-invalid to skip)")

            time.sleep(0.3)  # Rate limiting for Hunter API

        if dry_run:
            # Dry run - just mark status
            logger.info(f"[DRY RUN] Would send to: {email}")
            logger.debug(f"  Subject: {subject}")
            logger.debug(f"  Body preview: {body[:100]}...")
            df.loc[idx, 'send_status'] = 'dry_run'
            df.loc[idx, 'sent_at'] = datetime.now().isoformat()
        else:
            # Actually send
            success, error = send_email(
                to=email,
                subject=subject,
                body=body,
                login_address=login_address,
                password=password,
                send_as_address=send_as_address
            )

            if success:
                df.loc[idx, 'send_status'] = 'sent'
                df.loc[idx, 'sent_at'] = datetime.now().isoformat()
                df.loc[idx, 'send_error'] = ''
                results['sent'] += 1
            else:
                df.loc[idx, 'send_status'] = 'failed'
                df.loc[idx, 'send_error'] = error
                results['failed'] += 1

            # Delay between sends (not in dry run)
            if i < total_to_process and delay > 0:
                time.sleep(delay)

        # Save after each email (resume safety)
        df.to_csv(csv_path, index=False)

    results['duration'] = round(time.time() - start_time, 1)

    # Print summary
    print_results_summary(results)

    return results


def print_results_summary(results: Dict[str, Any]) -> None:
    """
    Log a formatted results summary.

    Args:
        results: Results dictionary from process_csv.
    """
    logger.info("=" * 50)
    logger.info("RESULTS SUMMARY")
    logger.info(f"Total processed: {results['total']}")
    logger.info(f"Sent: {results['sent']} | Failed: {results['failed']} | Skipped: {results['skipped']}")
    if results.get('skipped_unsubscribed', 0) > 0:
        logger.info(f"⛔ Blocked (unsubscribed): {results['skipped_unsubscribed']}")
    if results.get('skipped_low_score', 0) > 0:
        logger.info(f"Skipped (low score): {results['skipped_low_score']}")
    if results.get('skipped_catch_all', 0) > 0:
        logger.info(f"Skipped (catch-all): {results['skipped_catch_all']}")
    logger.info(f"Duration: {results['duration']}s")
    logger.info("=" * 50)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command line arguments.

    Args:
        args: List of arguments (for testing). None uses sys.argv.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description='Send cold emails via Gmail SMTP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview first 3 emails (dry run)
  python gmail_sender.py --csv output/email_drafts_v2.csv --dry-run --limit 3

  # Send 5 emails with 2 second delay
  python gmail_sender.py --csv output/email_drafts_v2.csv --limit 5 --delay 2.0

  # Send all remaining emails
  python gmail_sender.py --csv output/email_drafts_v2.csv
        """
    )

    parser.add_argument(
        '--csv',
        type=str,
        required=True,
        help='Path to CSV file with email drafts'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview emails without sending'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of emails to process'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Seconds to wait between sends (default: 1.0)'
    )

    parser.add_argument(
        '--skip-sent',
        action='store_true',
        default=True,
        help='Skip emails already marked as sent (default: True)'
    )

    parser.add_argument(
        '--no-skip-sent',
        action='store_false',
        dest='skip_sent',
        help='Process all emails including already sent'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose (DEBUG) logging'
    )

    parser.add_argument(
        '--verify-first',
        action='store_true',
        help='Verify emails with Hunter before sending'
    )

    parser.add_argument(
        '--skip-invalid',
        action='store_true',
        help='Skip emails that fail Hunter verification (use with --verify-first)'
    )

    parser.add_argument(
        '--verify-api',
        action='store_true',
        help='Use MillionVerifier API for advanced verification (requires MILLIONVERIFIER_API_KEY)'
    )

    parser.add_argument(
        '--min-score',
        type=int,
        default=70,
        help='Minimum send score threshold 0-100 (default: 70, use with --verify-api)'
    )

    parser.add_argument(
        '--skip-catch-all',
        action='store_true',
        help='Skip emails on catch-all domains (use with --verify-api)'
    )

    return parser.parse_args(args)


def main():
    """Main entry point."""
    args = parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose)

    # Validate credentials
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.error("Missing GMAIL_ADDRESS or GMAIL_APP_PASSWORD in .env file")
        logger.error("Please set these environment variables:")
        logger.error("  GMAIL_ADDRESS=your_email@gmail.com")
        logger.error("  GMAIL_APP_PASSWORD=your_app_password")
        return 1

    # Validate CSV exists
    if not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        return 1

    logger.info("=" * 50)
    logger.info("GMAIL SENDER")
    logger.info(f"Login: {GMAIL_ADDRESS}")
    if GMAIL_SEND_AS and GMAIL_SEND_AS != GMAIL_ADDRESS:
        logger.info(f"Send As: {GMAIL_SEND_AS}")
    else:
        logger.info(f"From: {GMAIL_ADDRESS}")
    logger.info(f"CSV: {args.csv}")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    if args.verify_api:
        logger.info(f"Verify: MillionVerifier API (min score: {args.min_score})")
        if args.skip_catch_all:
            logger.info("Skip catch-all: Yes")
    elif args.verify_first:
        logger.info(f"Verify: Hunter.io (skip invalid: {args.skip_invalid})")
    if args.limit:
        logger.info(f"Limit: {args.limit}")
    logger.info("=" * 50)

    # Process
    results = process_csv(
        csv_path=args.csv,
        dry_run=args.dry_run,
        limit=args.limit,
        delay=args.delay,
        skip_sent=args.skip_sent,
        verify_first=args.verify_first,
        skip_invalid=args.skip_invalid,
        verify_api=args.verify_api,
        min_score=args.min_score,
        skip_catch_all=args.skip_catch_all
    )

    return 0 if results['failed'] == 0 else 1


if __name__ == '__main__':
    exit(main())
