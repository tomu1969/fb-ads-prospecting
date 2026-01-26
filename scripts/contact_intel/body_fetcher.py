"""Fetch email bodies on-demand via Gmail API.

Caches bodies in extractions.db to avoid re-fetching.
Handles multipart MIME messages (prefers text/plain, falls back to HTML).
"""

import base64
import logging
import re
import sqlite3
from typing import Dict, List, Optional

from scripts.contact_intel.config import DATA_DIR, get_token_path
from scripts.contact_intel.extraction_db import get_cached_body, save_email_body

logger = logging.getLogger(__name__)

EMAILS_DB = DATA_DIR / "emails.db"


def _get_gmail_service(account_name: str):
    """Get Gmail API service for account.

    Args:
        account_name: Name of the Gmail account (e.g., "tujaguarcapital").

    Returns:
        Gmail API service object.

    Raises:
        ImportError: If google-api-python-client is not installed.
        ValueError: If no credentials are found for the account.
    """
    try:
        from googleapiclient.discovery import build
        from google.auth.transport.requests import Request
    except ImportError:
        raise ImportError("Install google-api-python-client: pip install google-api-python-client google-auth")

    from scripts.contact_intel.gmail_sync import load_oauth_credentials

    token_path = get_token_path(account_name)
    creds = load_oauth_credentials(str(token_path))

    if not creds:
        raise ValueError(f"No credentials for account: {account_name}")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build('gmail', 'v1', credentials=creds, cache_discovery=False)


def _extract_body_from_payload(payload: dict) -> str:
    """Extract text body from Gmail API message payload.

    Handles:
    - Direct body data (simple messages)
    - Multipart messages (prefers text/plain over text/html)
    - Nested multipart structures (recursive extraction)
    - HTML fallback with tag stripping

    Args:
        payload: Gmail API message payload dict.

    Returns:
        Extracted body text (empty string if none found).
    """
    # Empty payload
    if not payload:
        return ""

    # Direct body data (simple message)
    if 'body' in payload and payload['body'].get('data'):
        body_text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        return body_text

    # Multipart message - prefer text/plain
    if 'parts' in payload:
        # First pass: look for text/plain
        for part in payload['parts']:
            mime_type = part.get('mimeType', '')

            # Direct text/plain
            if mime_type == 'text/plain':
                if 'body' in part and part['body'].get('data'):
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')

            # Nested multipart - recurse
            if 'parts' in part:
                nested = _extract_body_from_payload(part)
                if nested:
                    return nested

        # Second pass: fallback to HTML (strip tags)
        for part in payload['parts']:
            if part.get('mimeType') == 'text/html':
                if 'body' in part and part['body'].get('data'):
                    html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    # Strip HTML tags
                    body_text = re.sub(r'<[^>]+>', ' ', html)
                    # Normalize whitespace
                    body_text = re.sub(r'\s+', ' ', body_text).strip()
                    # Truncate to prevent huge bodies
                    return body_text[:5000]

    return ""


def fetch_body(message_id: str, account_name: str = "tujaguarcapital") -> Optional[str]:
    """Fetch email body, using cache if available.

    Args:
        message_id: Gmail message ID.
        account_name: Gmail account name (default: "tujaguarcapital").

    Returns:
        Email body text, or None on error.
    """
    # Check cache first
    cached = get_cached_body(message_id)
    if cached is not None:
        logger.debug(f"Cache hit for message {message_id}")
        return cached

    logger.debug(f"Cache miss for message {message_id}, fetching from API")

    try:
        service = _get_gmail_service(account_name)
        msg = service.users().messages().get(
            userId='me',
            id=message_id,
            format='full',
        ).execute()

        payload = msg.get('payload', {})
        body = _extract_body_from_payload(payload)

        # Cache the result
        save_email_body(message_id, body)
        logger.debug(f"Fetched and cached body for {message_id} ({len(body)} chars)")
        return body

    except Exception as e:
        logger.error(f"Error fetching body for {message_id}: {e}")
        return None


def get_contact_emails_with_body(
    contact_email: str,
    limit: int = 3,
    account_name: str = "tujaguarcapital",
) -> List[Dict]:
    """Get recent emails from/to a contact with bodies.

    Queries the emails database for messages involving the contact,
    then fetches bodies (from cache or API).

    Args:
        contact_email: Email address of the contact.
        limit: Maximum number of emails to return (default: 3).
        account_name: Gmail account name for API calls (default: "tujaguarcapital").

    Returns:
        List of dicts with keys: subject, date, body, from.
    """
    conn = sqlite3.connect(EMAILS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Find emails where contact is sender OR in recipients
        cursor.execute("""
            SELECT message_id, subject, date, from_email
            FROM emails
            WHERE from_email = ? OR to_emails LIKE ?
            ORDER BY date DESC
            LIMIT ?
        """, (contact_email, f'%{contact_email}%', limit))

        rows = cursor.fetchall()
    finally:
        conn.close()

    emails = []
    for row in rows:
        msg_id = row['message_id']

        # Clean up message_id if it has email-style format
        # e.g., "<abc123@mail.gmail.com>" -> "abc123"
        if msg_id.startswith('<'):
            msg_id = msg_id[1:]
        if msg_id.endswith('>'):
            msg_id = msg_id[:-1]
        if '@' in msg_id:
            msg_id = msg_id.split('@')[0]

        body = fetch_body(msg_id, account_name)

        emails.append({
            'subject': row['subject'] or '',
            'date': row['date'] or '',
            'body': body or '',
            'from': row['from_email'],
        })

    logger.info(f"Retrieved {len(emails)} emails with bodies for {contact_email}")
    return emails
