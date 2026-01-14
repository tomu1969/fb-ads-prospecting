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
    'mail-delivery-subsystem@googlemail.com',
]

# Bounce subject patterns
BOUNCE_SUBJECTS = [
    'delivery status notification (failure)',
    'undeliverable:',
    'mail delivery failed',
    'returned mail',
    'delivery failure',
    'message not delivered',
    'delivery has failed',
    'could not be delivered',
    'undeliverable mail:',
]

# Patterns that indicate NOT a bounce (false positives)
NOT_BOUNCE_SENDERS = [
    'drive-shares',
    'docs.google.com',
    'calendar-notification',
    '@google.com',
    'comments-noreply@docs.google.com',
    'noreply@google.com',
    '@metabase',
]

NOT_BOUNCE_SUBJECTS = [
    'documento compartido',
    'shared with you',
    'document shared',
    'invitation:',
    'invitación:',
    'kpi dashboard',
    'proposal',
    'plan recaudo',
    'proactive weekly',
    'notification proposal',
    'from metabase',
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
    bounced_email = None

    # 1. Check headers first (most reliable)
    # X-Failed-Recipients header (common in Gmail bounces)
    failed_recipients = msg.get('X-Failed-Recipients')
    if failed_recipients:
        match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', failed_recipients)
        if match:
            return match.group(1)

    # 2. Check for delivery-status parts (RFC 3464)
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()

            # Check message/delivery-status parts
            if content_type == 'message/delivery-status':
                try:
                    payload = part.get_payload()
                    if isinstance(payload, list):
                        for subpart in payload:
                            if hasattr(subpart, 'items'):
                                for key, value in subpart.items():
                                    if key.lower() in ['final-recipient', 'original-recipient']:
                                        match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', str(value))
                                        if match:
                                            return match.group(1)
                    elif hasattr(payload, 'as_string'):
                        payload_str = payload.as_string()
                        match = re.search(r'Final-Recipient:.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', payload_str, re.IGNORECASE)
                        if match:
                            return match.group(1)
                except:
                    pass

            # Check text/plain parts for email patterns
            if content_type == 'text/plain':
                try:
                    body = part.get_payload(decode=True)
                    if body:
                        body = body.decode('utf-8', errors='replace')
                        email_match = extract_email_from_body(body)
                        if email_match:
                            return email_match
                except:
                    pass

    # 3. For non-multipart messages
    else:
        try:
            body = msg.get_payload(decode=True)
            if body:
                body = body.decode('utf-8', errors='replace')
                return extract_email_from_body(body)
        except:
            pass

    return bounced_email


def extract_email_from_body(body):
    """Extract bounced email address from message body text."""
    # Patterns ordered by specificity (most specific first)
    patterns = [
        # Microsoft Office 365 / Exchange format
        r'Remote Server returned[^<]*<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>',
        r"couldn't be delivered to\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        r'Delivery has failed to these recipients.*?<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>',

        # Gmail DSN format
        r'Address not found.*?<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>',
        r'The email account.*?<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>.*?does not exist',

        # Standard DSN patterns
        r'Final-Recipient:.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'Original-Recipient:.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',

        # Generic bounce patterns
        r'(?:could not be delivered to|failed for|rejected by|undeliverable to)[:\s]*<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',
        r'(?:recipient|to address)[:\s]*<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',

        # "Your message to X" pattern
        r'Your message to\s+<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',
        r'message sent to\s+<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?',

        # Fallback: first email in angle brackets (but not sender addresses)
        r'<([a-zA-Z0-9._%+-]+@(?!lahaus\.com|googlemail\.com|google\.com)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>',
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if match:
            email = match.group(1).lower().strip()
            # Skip common false positives
            if not any(skip in email for skip in ['mailer-daemon', 'postmaster', 'noreply', 'lahaus.com']):
                return email

    return None


def is_bounce(from_addr, subject):
    """Check if email is a bounce notification."""
    from_lower = from_addr.lower()
    subject_lower = subject.lower()

    # First, check for false positives (Google Docs, etc.)
    for pattern in NOT_BOUNCE_SENDERS:
        if pattern in from_lower:
            return False

    for pattern in NOT_BOUNCE_SUBJECTS:
        if pattern in subject_lower:
            return False

    # Check sender for bounce indicators
    for pattern in BOUNCE_SENDERS:
        if pattern in from_lower:
            return True

    # Check subject for bounce indicators
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
