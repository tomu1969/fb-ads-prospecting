"""Gmail Inbox Checker - Check for bounces and replies.

Connects to Gmail via IMAP to find:
1. Bounce notifications (delivery failures)
2. Replies to sent emails

Usage:
    python scripts/gmail_sender/inbox_checker.py --hours 24
"""

import argparse
import imaplib
import email
from email.header import decode_header
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

GMAIL_USER = os.getenv('GMAIL_ADDRESS', os.getenv('GMAIL_USER', os.getenv('GMAIL_LOGIN')))
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')

# Known bounce sender patterns
BOUNCE_SENDERS = [
    'mailer-daemon@',
    'postmaster@',
    'mail-delivery-subsystem@',
    'noreply@',
]

# Bounce subject patterns
BOUNCE_SUBJECTS = [
    'delivery status notification',
    'undeliverable',
    'mail delivery failed',
    'returned mail',
    'delivery failure',
    'message not delivered',
    'delivery has failed',
    'could not be delivered',
]


def decode_mime_header(header_value):
    """Decode MIME encoded header."""
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    result = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            result.append(part.decode(encoding or 'utf-8', errors='replace'))
        else:
            result.append(part)
    return ''.join(result)


def extract_bounced_email(msg):
    """Extract the bounced recipient email from a bounce notification."""
    # Check body for bounced address
    bounced_email = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == 'text/plain':
                try:
                    body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    # Look for email patterns after common bounce phrases
                    patterns = [
                        r'(?:could not be delivered to|failed for|rejected by|undeliverable to)[:\s]+<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',
                        r'(?:recipient|address)[:\s]+<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',
                        r'<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>.*(?:rejected|failed|undeliverable)',
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, body, re.IGNORECASE)
                        if match:
                            bounced_email = match.group(1)
                            break
                    if bounced_email:
                        break
                except:
                    pass
    else:
        try:
            body = msg.get_payload(decode=True).decode('utf-8', errors='replace')
            match = re.search(r'<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>', body)
            if match:
                bounced_email = match.group(1)
        except:
            pass

    return bounced_email


def is_bounce(from_addr, subject):
    """Check if email is a bounce notification."""
    from_lower = from_addr.lower()
    subject_lower = subject.lower()

    # Check sender
    for pattern in BOUNCE_SENDERS:
        if pattern in from_lower:
            return True

    # Check subject
    for pattern in BOUNCE_SUBJECTS:
        if pattern in subject_lower:
            return True

    return False


def check_inbox(hours=24, verbose=False):
    """Check Gmail inbox for bounces and replies."""

    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print("Error: GMAIL_USER and GMAIL_APP_PASSWORD must be set in .env")
        return None, None

    print(f"Connecting to Gmail as {GMAIL_USER}...")

    # Connect to Gmail IMAP
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)

    bounces = []
    replies = []

    # Calculate date filter
    since_date = (datetime.now() - timedelta(hours=hours)).strftime('%d-%b-%Y')

    # Check INBOX for replies and bounces
    mail.select('INBOX')
    _, message_ids = mail.search(None, f'(SINCE {since_date})')

    print(f"Checking {len(message_ids[0].split())} messages from last {hours} hours...")

    for msg_id in message_ids[0].split():
        _, msg_data = mail.fetch(msg_id, '(RFC822)')
        email_body = msg_data[0][1]
        msg = email.message_from_bytes(email_body)

        from_addr = decode_mime_header(msg['From'])
        subject = decode_mime_header(msg['Subject'])
        date = msg['Date']

        if is_bounce(from_addr, subject):
            bounced_email = extract_bounced_email(msg)
            bounces.append({
                'type': 'bounce',
                'from': from_addr,
                'subject': subject,
                'date': date,
                'bounced_email': bounced_email,
            })
            if verbose:
                print(f"  [BOUNCE] {bounced_email or 'unknown'} - {subject[:50]}")
        else:
            # Check if it's a reply (not from ourselves)
            if GMAIL_USER.lower() not in from_addr.lower():
                # Skip automated messages
                if not any(x in from_addr.lower() for x in ['noreply', 'no-reply', 'notifications', 'mailer']):
                    replies.append({
                        'type': 'reply',
                        'from': from_addr,
                        'subject': subject,
                        'date': date,
                    })
                    if verbose:
                        print(f"  [REPLY] {from_addr} - {subject[:50]}")

    mail.logout()

    return bounces, replies


def main():
    parser = argparse.ArgumentParser(description='Check Gmail for bounces and replies')
    parser.add_argument('--hours', type=int, default=24, help='Check emails from last N hours (default: 24)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show details for each message')
    args = parser.parse_args()

    bounces, replies = check_inbox(hours=args.hours, verbose=args.verbose)

    if bounces is None:
        return

    print("\n" + "=" * 60)
    print("INBOX CHECK RESULTS")
    print("=" * 60)

    print(f"\nBounces: {len(bounces)}")
    if bounces:
        for b in bounces:
            email_addr = b['bounced_email'] or 'unknown'
            print(f"  - {email_addr}")

    print(f"\nReplies: {len(replies)}")
    if replies:
        for r in replies:
            print(f"  - {r['from'][:40]} | {r['subject'][:40]}")

    print()


if __name__ == '__main__':
    main()
