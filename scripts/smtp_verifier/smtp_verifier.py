"""SMTP Email Verifier - Verify email addresses exist without sending emails.

Uses SMTP RCPT TO command to verify mailbox exists at the mail server.
This is more reliable than pattern-based verification (like Hunter.io).

Usage:
    # Verify single email
    python scripts/smtp_verifier/smtp_verifier.py --email test@example.com

    # Verify CSV of emails
    python scripts/smtp_verifier/smtp_verifier.py --csv contacts.csv --output verified.csv

    # With custom timeout
    python scripts/smtp_verifier/smtp_verifier.py --csv contacts.csv --timeout 5
"""

import argparse
import logging
import re
import smtplib
import socket
import sys
import time
from typing import Dict, List, Optional, Any

import dns.resolver
import pandas as pd


# SMTP response codes mapping
SMTP_STATUS_MAP = {
    250: 'valid',      # Mailbox exists
    251: 'valid',      # User not local, will forward
    252: 'catch_all',  # Cannot verify, but will accept
    450: 'unknown',    # Mailbox busy, retry later
    451: 'unknown',    # Local error, retry later
    550: 'invalid',    # Mailbox doesn't exist
    551: 'invalid',    # User not local
    552: 'valid',      # Mailbox full (exists but full)
    553: 'invalid',    # Invalid mailbox name
}


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the verifier."""
    logger = logging.getLogger('smtp_verifier')
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def is_valid_email_format(email: Optional[str]) -> bool:
    """Check if email has valid format."""
    if not email or not isinstance(email, str):
        return False

    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def get_mx_record(domain: str) -> Optional[str]:
    """Get the primary MX record for a domain.

    Args:
        domain: The email domain to lookup

    Returns:
        The hostname of the mail server, or None if not found
    """
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        # Sort by preference (lower = higher priority)
        sorted_mx = sorted(mx_records, key=lambda x: x.preference)
        if sorted_mx:
            # Remove trailing dot from hostname
            return sorted_mx[0].exchange.to_text().rstrip('.')
        return None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
        return None
    except Exception:
        return None


def verify_email(
    email: str,
    timeout: int = 10,
    sender_domain: str = 'gmail.com'
) -> Dict[str, Any]:
    """Verify if an email address exists using SMTP RCPT TO.

    Args:
        email: Email address to verify
        timeout: Connection timeout in seconds
        sender_domain: Domain to use in MAIL FROM command

    Returns:
        Dict with status, code, and message
    """
    result = {
        'email': email,
        'status': 'unknown',
        'code': None,
        'message': '',
        'mx_host': None
    }

    # Validate email format
    if not is_valid_email_format(email):
        result['status'] = 'invalid'
        result['message'] = 'Invalid email format'
        return result

    # Extract domain
    domain = email.split('@')[1]

    # Get MX record
    mx_host = get_mx_record(domain)
    if not mx_host:
        result['status'] = 'invalid'
        result['message'] = f'No MX record found for {domain}'
        return result

    result['mx_host'] = mx_host

    try:
        # Connect to mail server
        with smtplib.SMTP(timeout=timeout) as server:
            server.connect(mx_host, 25)
            server.ehlo(sender_domain)

            # Some servers require STARTTLS
            try:
                server.starttls()
                server.ehlo(sender_domain)
            except smtplib.SMTPNotSupportedError:
                pass  # Server doesn't support STARTTLS, continue anyway

            # Send MAIL FROM
            server.mail(f'verify@{sender_domain}')

            # Send RCPT TO - this is where verification happens
            code, message = server.rcpt(email)

            result['code'] = code
            result['message'] = message.decode() if isinstance(message, bytes) else str(message)
            result['status'] = SMTP_STATUS_MAP.get(code, 'unknown')

            # Quit gracefully
            try:
                server.quit()
            except:
                pass

    except smtplib.SMTPServerDisconnected as e:
        result['status'] = 'unknown'
        result['message'] = f'Server disconnected: {str(e)}'
    except smtplib.SMTPConnectError as e:
        result['status'] = 'unknown'
        result['message'] = f'Connection error: {str(e)}'
    except socket.timeout:
        result['status'] = 'unknown'
        result['message'] = 'Connection timeout'
    except socket.gaierror as e:
        result['status'] = 'unknown'
        result['message'] = f'DNS error: {str(e)}'
    except Exception as e:
        result['status'] = 'unknown'
        result['message'] = f'Error: {str(e)}'

    return result


def verify_csv(
    csv_path: str,
    output_path: Optional[str] = None,
    update_file: bool = False,
    email_column: str = 'primary_email',
    limit: Optional[int] = None,
    timeout: int = 10,
    delay: float = 1.0
) -> List[Dict[str, Any]]:
    """Verify emails from a CSV file.

    Args:
        csv_path: Path to input CSV
        output_path: Path for output CSV (optional)
        update_file: If True, update the input file in place
        email_column: Name of column containing emails
        limit: Maximum number of emails to verify
        timeout: SMTP timeout per email
        delay: Delay between verifications (seconds)

    Returns:
        List of verification results
    """
    logger = logging.getLogger('smtp_verifier')

    df = pd.read_csv(csv_path)
    results = []

    # Determine how many to process
    total = len(df)
    if limit:
        total = min(limit, total)

    logger.info(f"Verifying {total} emails from {csv_path}")

    for idx, row in df.head(total).iterrows():
        email = row.get(email_column, '')

        if not email or pd.isna(email):
            results.append({
                'email': '',
                'smtp_status': 'invalid',
                'smtp_code': None,
                'smtp_message': 'Empty email'
            })
            continue

        logger.debug(f"Verifying {email}...")

        verification = verify_email(email, timeout=timeout)

        results.append({
            'email': email,
            'smtp_status': verification['status'],
            'smtp_code': verification['code'],
            'smtp_message': verification['message'],
            'mx_host': verification.get('mx_host')
        })

        status_emoji = {
            'valid': '✓',
            'invalid': '✗',
            'catch_all': '~',
            'unknown': '?'
        }.get(verification['status'], '?')

        logger.info(f"  [{status_emoji}] {email}: {verification['status']}")

        # Add delay between requests to avoid rate limiting
        if idx < total - 1 and delay > 0:
            time.sleep(delay)

    # Update dataframe with results
    for idx, result in enumerate(results):
        if idx < len(df):
            df.loc[idx, 'smtp_status'] = result['smtp_status']
            df.loc[idx, 'smtp_code'] = result.get('smtp_code')
            df.loc[idx, 'smtp_message'] = result.get('smtp_message', '')

    # Save results
    if update_file:
        df.to_csv(csv_path, index=False)
        logger.info(f"Updated {csv_path} with verification results")

    if output_path:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved verification results to {output_path}")

    return results


def print_summary(results: List[Dict[str, Any]]) -> None:
    """Print summary of verification results."""
    logger = logging.getLogger('smtp_verifier')

    status_counts = {
        'valid': 0,
        'invalid': 0,
        'catch_all': 0,
        'unknown': 0
    }

    for r in results:
        status = r.get('smtp_status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    logger.info("\n" + "=" * 50)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total: {len(results)}")
    logger.info(f"Valid: {status_counts['valid']}")
    logger.info(f"Invalid: {status_counts['invalid']}")
    logger.info(f"Catch-all: {status_counts['catch_all']}")
    logger.info(f"Unknown: {status_counts['unknown']}")
    logger.info("=" * 50)


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='SMTP Email Verifier - Verify emails exist without sending'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--email', type=str, help='Single email to verify')
    group.add_argument('--csv', type=str, help='CSV file with emails to verify')

    parser.add_argument('--output', type=str, help='Output CSV path for results')
    parser.add_argument('--email-column', type=str, default='primary_email',
                       help='Column name containing emails (default: primary_email)')
    parser.add_argument('--limit', type=int, help='Maximum emails to verify')
    parser.add_argument('--timeout', type=int, default=10,
                       help='SMTP timeout in seconds (default: 10)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between verifications (default: 1.0)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('--update-file', action='store_true',
                       help='Update input CSV with results')

    return parser.parse_args(args)


def main():
    """Main entry point."""
    args = parse_args(sys.argv[1:])
    logger = setup_logging(verbose=args.verbose)

    if args.email:
        # Single email verification
        logger.info(f"Verifying: {args.email}")
        result = verify_email(args.email, timeout=args.timeout)

        print(f"\nEmail: {result['email']}")
        print(f"Status: {result['status']}")
        print(f"SMTP Code: {result['code']}")
        print(f"MX Host: {result['mx_host']}")
        print(f"Message: {result['message']}")

    elif args.csv:
        # Batch verification
        results = verify_csv(
            csv_path=args.csv,
            output_path=args.output,
            update_file=args.update_file,
            email_column=args.email_column,
            limit=args.limit,
            timeout=args.timeout,
            delay=args.delay
        )

        print_summary(results)


if __name__ == '__main__':
    main()
