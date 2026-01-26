"""Gmail Sync Module - Fetch emails from multiple Gmail accounts.

Supports both OAuth 2.0 (Gmail API) and IMAP authentication methods.
Stores email metadata in SQLite for later analysis.

Usage:
    # Test sync (100 emails)
    python scripts/contact_intel/gmail_sync.py --account tujaguarcapital --limit 100

    # Full sync from date
    python scripts/contact_intel/gmail_sync.py --account all --since 2024-01-01

    # Check sync status
    python scripts/contact_intel/gmail_sync.py --status
"""

import argparse
import email
import imaplib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gmail_sync.log'),
    ]
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "contact_intel"
CONFIG_DIR = PROJECT_ROOT / "config"

# Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


# =============================================================================
# Credential Loading
# =============================================================================

def load_oauth_credentials(token_path: str):
    """Load OAuth credentials from token file.

    Args:
        token_path: Path to the token JSON file.

    Returns:
        Credentials object or None if file doesn't exist.
    """
    try:
        from google.oauth2.credentials import Credentials
    except ImportError:
        logger.error("Missing google-auth library. Install with: pip install google-auth")
        return None

    if not os.path.exists(token_path):
        logger.warning(f"Token file not found: {token_path}")
        return None

    try:
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        return creds
    except Exception as e:
        logger.error(f"Failed to load OAuth credentials: {e}")
        return None


def load_imap_credentials() -> Optional[Dict[str, str]]:
    """Load IMAP credentials from environment variables.

    Returns:
        Dict with 'email' and 'password' keys, or None if missing.
    """
    email_addr = os.environ.get("GMAIL_ADDRESS") or os.environ.get("GMAIL_USER")
    password = os.environ.get("GMAIL_APP_PASSWORD")

    if not email_addr or not password:
        return None

    return {
        "email": email_addr,
        "password": password,
    }


# =============================================================================
# Email Header Parsing
# =============================================================================

def decode_mime_header(header_value: str) -> str:
    """Decode MIME-encoded header value.

    Args:
        header_value: Raw header value (may be MIME-encoded).

    Returns:
        Decoded string.
    """
    if not header_value:
        return ""

    decoded_parts = decode_header(header_value)
    result = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            charset = encoding or 'utf-8'
            try:
                result.append(part.decode(charset, errors='replace'))
            except (UnicodeDecodeError, LookupError):
                result.append(part.decode('utf-8', errors='replace'))
        else:
            result.append(part)
    return ''.join(result)


def extract_email_address(addr_string: str) -> tuple:
    """Extract name and email from address string.

    Args:
        addr_string: Address string like "John Doe <john@example.com>"

    Returns:
        Tuple of (name, email).
    """
    if not addr_string:
        return ("", "")

    name, email_addr = parseaddr(addr_string)
    name = decode_mime_header(name)
    return (name.strip(), email_addr.lower().strip())


def extract_email_list(header_value: str) -> List[str]:
    """Extract list of email addresses from header.

    Args:
        header_value: Header value with one or more addresses.

    Returns:
        List of email addresses.
    """
    if not header_value:
        return []

    # Split by comma and extract each address
    addresses = []
    for addr in header_value.split(','):
        _, email_addr = extract_email_address(addr.strip())
        if email_addr:
            addresses.append(email_addr)
    return addresses


def parse_email_headers(raw_email: str) -> Dict:
    """Parse email headers from raw email string.

    Args:
        raw_email: Raw email message as string or bytes.

    Returns:
        Dict with extracted header fields.
    """
    if isinstance(raw_email, bytes):
        raw_email = raw_email.decode('utf-8', errors='replace')

    msg = email.message_from_string(raw_email)

    # Extract From
    from_header = msg.get('From', '')
    from_name, from_email = extract_email_address(from_header)

    # Extract To, CC, BCC
    to_emails = extract_email_list(msg.get('To', ''))
    cc_emails = extract_email_list(msg.get('Cc', ''))
    bcc_emails = extract_email_list(msg.get('Bcc', ''))

    # Extract Subject
    subject = decode_mime_header(msg.get('Subject', ''))

    # Extract Message-ID and threading info
    message_id = msg.get('Message-ID', '')
    in_reply_to = msg.get('In-Reply-To', '')

    # Parse date
    date_str = msg.get('Date', '')
    date_obj = None
    if date_str:
        try:
            date_obj = parsedate_to_datetime(date_str)
        except (ValueError, TypeError):
            pass

    return {
        "from_email": from_email,
        "from_name": from_name,
        "to_emails": to_emails,
        "cc_emails": cc_emails,
        "bcc_emails": bcc_emails,
        "subject": subject,
        "message_id": message_id,
        "in_reply_to": in_reply_to if in_reply_to else None,
        "date": date_obj,
    }


# =============================================================================
# Sync State Management
# =============================================================================

class SyncStateManager:
    """Manages incremental sync state per account."""

    def __init__(self, state_file: str):
        """Initialize state manager.

        Args:
            state_file: Path to JSON file storing sync state.
        """
        self.state_file = Path(state_file)
        self._state = self._load_state()

    def _load_state(self) -> Dict:
        """Load state from file."""
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text())
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_state(self):
        """Save state to file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self._state, indent=2))

    def get_last_sync(self, account: str) -> Optional[datetime]:
        """Get last sync timestamp for account.

        Args:
            account: Account name.

        Returns:
            datetime of last sync or None if never synced.
        """
        if account not in self._state:
            return None

        last_sync_str = self._state[account].get("last_sync")
        if not last_sync_str:
            return None

        try:
            return datetime.fromisoformat(last_sync_str)
        except ValueError:
            return None

    def update_last_sync(self, account: str, sync_time: datetime):
        """Update last sync timestamp for account.

        Args:
            account: Account name.
            sync_time: Sync timestamp.
        """
        if account not in self._state:
            self._state[account] = {}
        self._state[account]["last_sync"] = sync_time.isoformat()
        self._save_state()

    def get_all_accounts(self) -> List[str]:
        """Get list of all tracked accounts."""
        return list(self._state.keys())


# =============================================================================
# Account Configuration (for test compatibility)
# =============================================================================

@dataclass
class AccountConfig:
    """Configuration for a Gmail account.

    This is a simplified dataclass for test compatibility.
    For production use, prefer GmailAccount from models.py.
    """
    name: str
    auth_type: str  # 'oauth' or 'imap'
    token_path: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None

    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        if self.auth_type == 'oauth':
            return bool(self.token_path)
        elif self.auth_type == 'imap':
            return bool(self.email and self.password)
        return False


def get_configured_accounts() -> List[AccountConfig]:
    """Get list of configured Gmail accounts.

    Returns:
        List of AccountConfig objects.
    """
    accounts = []

    # Check for OAuth accounts (token files in data/contact_intel/)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for token_file in DATA_DIR.glob("*_token.json"):
        account_name = token_file.stem.replace("_token", "")
        accounts.append(AccountConfig(
            name=account_name,
            auth_type="oauth",
            token_path=str(token_file),
        ))

    # Check for IMAP account
    imap_creds = load_imap_credentials()
    if imap_creds:
        accounts.append(AccountConfig(
            name="lahaus",
            auth_type="imap",
            email=imap_creds["email"],
            password=imap_creds["password"],
        ))

    return accounts


# =============================================================================
# Gmail Syncer
# =============================================================================

class GmailSyncer:
    """Syncs emails from Gmail accounts to SQLite database."""

    def __init__(self, db_path: str = None):
        """Initialize syncer.

        Args:
            db_path: Path to SQLite database. Defaults to data/contact_intel/emails.db.
        """
        if db_path is None:
            db_path = str(DATA_DIR / "emails.db")
        self.db_path = db_path
        self.state_manager = SyncStateManager(str(DATA_DIR / "sync_state.json"))

    def init_db(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                message_id TEXT UNIQUE NOT NULL,
                thread_id TEXT,
                from_email TEXT NOT NULL,
                from_name TEXT,
                to_emails TEXT,
                cc_emails TEXT,
                bcc_emails TEXT,
                subject TEXT,
                date TIMESTAMP,
                in_reply_to TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_account ON emails(account)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_email)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(thread_id)")
        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def save_email(self, email_data: Dict) -> bool:
        """Save email metadata to database.

        Args:
            email_data: Dict with email fields.

        Returns:
            True if saved, False if duplicate.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR IGNORE INTO emails
                (account, message_id, thread_id, from_email, from_name,
                 to_emails, cc_emails, bcc_emails, subject, date, in_reply_to)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                email_data["account"],
                email_data["message_id"],
                email_data.get("thread_id"),
                email_data["from_email"],
                email_data.get("from_name"),
                json.dumps(email_data.get("to_emails", [])),
                json.dumps(email_data.get("cc_emails", [])),
                json.dumps(email_data.get("bcc_emails", [])),
                email_data.get("subject"),
                email_data.get("date"),
                email_data.get("in_reply_to"),
            ))
            conn.commit()
            return conn.total_changes > 0
        finally:
            conn.close()

    def save_emails_batch(
        self,
        emails: List[Dict],
        progress_callback: Callable = None,
        batch_size: int = 100,
    ) -> int:
        """Save batch of emails with progress reporting.

        Args:
            emails: List of email data dicts.
            progress_callback: Optional callback(current, total, message).
            batch_size: Save to disk every N emails.

        Returns:
            Number of emails saved.
        """
        saved_count = 0
        total = len(emails)

        conn = sqlite3.connect(self.db_path)
        try:
            for i, email_data in enumerate(emails):
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO emails
                        (account, message_id, thread_id, from_email, from_name,
                         to_emails, cc_emails, bcc_emails, subject, date, in_reply_to)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        email_data["account"],
                        email_data["message_id"],
                        email_data.get("thread_id"),
                        email_data["from_email"],
                        email_data.get("from_name"),
                        json.dumps(email_data.get("to_emails", [])),
                        json.dumps(email_data.get("cc_emails", [])),
                        json.dumps(email_data.get("bcc_emails", [])),
                        email_data.get("subject"),
                        email_data.get("date"),
                        email_data.get("in_reply_to"),
                    ))
                    saved_count += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate

                # Commit and report progress every batch_size
                if (i + 1) % batch_size == 0:
                    conn.commit()
                    if progress_callback:
                        progress_callback(i + 1, total, f"Saved {i + 1}/{total} emails")

            conn.commit()
            if progress_callback:
                progress_callback(total, total, f"Saved {total}/{total} emails")

        finally:
            conn.close()

        return saved_count

    def get_email_counts(self) -> Dict[str, int]:
        """Get email count per account.

        Returns:
            Dict mapping account name to email count.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT account, COUNT(*) FROM emails GROUP BY account
        """)
        counts = dict(cursor.fetchall())
        conn.close()
        return counts

    def _get_gmail_service(self, creds):
        """Build Gmail API service."""
        try:
            from googleapiclient.discovery import build
        except ImportError:
            logger.error("Missing google-api-python-client. Install with: pip install google-api-python-client")
            return None

        return build('gmail', 'v1', credentials=creds)

    def _fetch_messages_oauth(
        self,
        service,
        since_date: datetime = None,
        limit: int = None,
    ) -> List[Dict]:
        """Fetch messages using Gmail API (OAuth).

        Args:
            service: Gmail API service.
            since_date: Only fetch emails after this date.
            limit: Maximum number of emails to fetch.

        Returns:
            List of message metadata dicts.
        """
        # Build query
        query_parts = []
        if since_date:
            date_str = since_date.strftime("%Y/%m/%d")
            query_parts.append(f"after:{date_str}")

        query = " ".join(query_parts) if query_parts else None

        messages = []
        page_token = None

        while True:
            try:
                results = service.users().messages().list(
                    userId='me',
                    q=query,
                    maxResults=min(limit or 500, 500),
                    pageToken=page_token,
                ).execute()

                batch = results.get('messages', [])
                messages.extend(batch)

                if limit and len(messages) >= limit:
                    messages = messages[:limit]
                    break

                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                break

        return messages

    def _get_message_details_oauth(self, service, message_id: str) -> Dict:
        """Get full message details using Gmail API.

        Args:
            service: Gmail API service.
            message_id: Gmail message ID.

        Returns:
            Message metadata dict.
        """
        try:
            msg = service.users().messages().get(
                userId='me',
                id=message_id,
                format='metadata',
                metadataHeaders=['From', 'To', 'Cc', 'Bcc', 'Subject', 'Date', 'Message-ID', 'In-Reply-To'],
            ).execute()

            # Extract headers
            headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}

            from_name, from_email = extract_email_address(headers.get('From', ''))

            return {
                "message_id": headers.get('Message-ID', f"<{message_id}@gmail>"),
                "thread_id": msg.get('threadId'),
                "from_email": from_email,
                "from_name": from_name,
                "to_emails": extract_email_list(headers.get('To', '')),
                "cc_emails": extract_email_list(headers.get('Cc', '')),
                "bcc_emails": extract_email_list(headers.get('Bcc', '')),
                "subject": decode_mime_header(headers.get('Subject', '')),
                "date": self._parse_date(headers.get('Date', '')),
                "in_reply_to": headers.get('In-Reply-To'),
            }
        except Exception as e:
            logger.error(f"Error getting message {message_id}: {e}")
            return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return parsedate_to_datetime(date_str)
        except (ValueError, TypeError):
            return None

    def _sync_oauth_account(
        self,
        config: AccountConfig,
        since_date: datetime = None,
        limit: int = None,
        progress_callback: Callable = None,
        save_batch_size: int = 100,
    ) -> int:
        """Sync emails from OAuth account with incremental saves.

        Args:
            config: Account configuration.
            since_date: Only fetch emails after this date.
            limit: Maximum number of emails to fetch.
            progress_callback: Optional progress callback.
            save_batch_size: Save to database every N emails (default 100).

        Returns:
            Number of emails synced.
        """
        logger.info(f"Syncing OAuth account: {config.name}")

        creds = load_oauth_credentials(config.token_path)
        if not creds:
            logger.error(f"Failed to load credentials for {config.name}")
            return 0

        # Refresh if needed
        if creds.expired and creds.refresh_token:
            try:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Failed to refresh credentials: {e}")
                return 0

        service = self._get_gmail_service(creds)
        if not service:
            return 0

        # Use last sync time if no since_date provided
        if not since_date:
            since_date = self.state_manager.get_last_sync(config.name)

        logger.info(f"Fetching messages since: {since_date or 'all time'}")

        # Fetch message list
        messages = self._fetch_messages_oauth(service, since_date, limit)
        logger.info(f"Found {len(messages)} messages to sync")

        # Fetch details and save incrementally
        total = len(messages)
        total_saved = 0
        batch = []

        conn = sqlite3.connect(self.db_path)

        try:
            for i, msg in enumerate(messages):
                details = self._get_message_details_oauth(service, msg['id'])
                if details:
                    details['account'] = config.name
                    batch.append(details)

                # Save batch incrementally
                if len(batch) >= save_batch_size:
                    saved = self._save_batch_to_db(conn, batch)
                    total_saved += saved
                    batch = []
                    logger.info(f"[{i + 1}/{total}] Saved {total_saved} emails (fetched {i + 1})")
                    if progress_callback:
                        progress_callback(i + 1, total, f"Saved {total_saved}/{i + 1} fetched")

                elif (i + 1) % 100 == 0:
                    logger.info(f"[{i + 1}/{total}] Fetching... (batch: {len(batch)})")
                    if progress_callback:
                        progress_callback(i + 1, total, f"Fetching {i + 1}/{total}")

            # Save remaining batch
            if batch:
                saved = self._save_batch_to_db(conn, batch)
                total_saved += saved
                logger.info(f"[{total}/{total}] Final save: {saved} emails")

        finally:
            conn.close()

        # Update sync state
        self.state_manager.update_last_sync(config.name, datetime.now())

        logger.info(f"Synced {total_saved} emails from {config.name}")
        return total_saved

    def _save_batch_to_db(self, conn: sqlite3.Connection, emails: List[Dict]) -> int:
        """Save a batch of emails to the database.

        Args:
            conn: SQLite connection.
            emails: List of email data dicts.

        Returns:
            Number of emails saved (excluding duplicates).
        """
        saved = 0
        for email_data in emails:
            try:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO emails
                    (account, message_id, thread_id, from_email, from_name,
                     to_emails, cc_emails, bcc_emails, subject, date, in_reply_to)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    email_data["account"],
                    email_data["message_id"],
                    email_data.get("thread_id"),
                    email_data["from_email"],
                    email_data.get("from_name"),
                    json.dumps(email_data.get("to_emails", [])),
                    json.dumps(email_data.get("cc_emails", [])),
                    json.dumps(email_data.get("bcc_emails", [])),
                    email_data.get("subject"),
                    email_data.get("date"),
                    email_data.get("in_reply_to"),
                ))
                if cursor.rowcount > 0:
                    saved += 1
            except sqlite3.IntegrityError:
                pass  # Duplicate

        conn.commit()
        return saved

    def _sync_imap_account(
        self,
        config: AccountConfig,
        since_date: datetime = None,
        limit: int = None,
        progress_callback: Callable = None,
        save_batch_size: int = 100,
    ) -> int:
        """Sync emails from IMAP account with incremental saves.

        Args:
            config: Account configuration.
            since_date: Only fetch emails after this date.
            limit: Maximum number of emails to fetch.
            progress_callback: Optional progress callback.
            save_batch_size: Save to database every N emails (default 100).

        Returns:
            Number of emails synced.
        """
        logger.info(f"Syncing IMAP account: {config.name}")

        # Use last sync time if no since_date provided
        if not since_date:
            since_date = self.state_manager.get_last_sync(config.name)
            if not since_date:
                since_date = datetime.now() - timedelta(days=365)  # Default to 1 year

        try:
            mail = imaplib.IMAP4_SSL('imap.gmail.com')
            mail.login(config.email, config.password)
            mail.select('INBOX')

            # Search for emails since date
            date_str = since_date.strftime('%d-%b-%Y')
            _, message_ids = mail.search(None, f'(SINCE {date_str})')

            msg_ids = message_ids[0].split()
            if limit:
                msg_ids = msg_ids[:limit]

            logger.info(f"Found {len(msg_ids)} messages to sync")

            total = len(msg_ids)
            total_saved = 0
            batch = []

            conn = sqlite3.connect(self.db_path)

            try:
                for i, msg_id in enumerate(msg_ids):
                    _, msg_data = mail.fetch(msg_id, '(RFC822)')
                    raw_email = msg_data[0][1]

                    headers = parse_email_headers(raw_email)
                    headers['account'] = config.name
                    headers['thread_id'] = None  # IMAP doesn't have thread IDs

                    batch.append(headers)

                    # Save batch incrementally
                    if len(batch) >= save_batch_size:
                        saved = self._save_batch_to_db(conn, batch)
                        total_saved += saved
                        batch = []
                        logger.info(f"[{i + 1}/{total}] Saved {total_saved} emails")
                        if progress_callback:
                            progress_callback(i + 1, total, f"Saved {total_saved}/{i + 1}")

                    elif (i + 1) % 100 == 0:
                        logger.info(f"[{i + 1}/{total}] Fetching... (batch: {len(batch)})")
                        if progress_callback:
                            progress_callback(i + 1, total, f"Fetching {i + 1}/{total}")

                # Save remaining batch
                if batch:
                    saved = self._save_batch_to_db(conn, batch)
                    total_saved += saved
                    logger.info(f"[{total}/{total}] Final save: {saved} emails")

            finally:
                conn.close()

            mail.logout()

            # Update sync state
            self.state_manager.update_last_sync(config.name, datetime.now())

            logger.info(f"Synced {total_saved} emails from {config.name}")
            return total_saved

        except Exception as e:
            logger.error(f"IMAP sync error: {e}")
            return 0

    def sync_accounts(
        self,
        accounts: List[AccountConfig],
        since_date: datetime = None,
        limit: int = None,
        progress_callback: Callable = None,
    ) -> Dict[str, int]:
        """Sync multiple accounts.

        Args:
            accounts: List of account configs.
            since_date: Only fetch emails after this date.
            limit: Maximum emails per account.
            progress_callback: Optional progress callback.

        Returns:
            Dict mapping account name to emails synced.
        """
        results = {}

        for account in accounts:
            if not account.is_valid():
                logger.warning(f"Invalid config for account: {account.name}")
                results[account.name] = 0
                continue

            if account.auth_type == 'oauth':
                results[account.name] = self._sync_oauth_account(
                    account, since_date, limit, progress_callback
                )
            elif account.auth_type == 'imap':
                results[account.name] = self._sync_imap_account(
                    account, since_date, limit, progress_callback
                )

        return results

    def get_status(self) -> Dict:
        """Get sync status for all accounts.

        Returns:
            Dict with account status info.
        """
        # Initialize DB if needed
        self.init_db()

        email_counts = self.get_email_counts()
        status = {
            "database": self.db_path,
            "accounts": {},
        }

        for account in get_configured_accounts():
            last_sync = self.state_manager.get_last_sync(account.name)
            status["accounts"][account.name] = {
                "auth_type": account.auth_type,
                "last_sync": last_sync.isoformat() if last_sync else "never",
                "email_count": email_counts.get(account.name, 0),
            }

        return status


# =============================================================================
# CLI Interface
# =============================================================================

def parse_args(args=None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Sync emails from Gmail accounts to SQLite database.'
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Account name to sync (or "all" for all accounts)',
    )
    parser.add_argument(
        '--since',
        type=str,
        help='Only sync emails since this date (YYYY-MM-DD)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of emails to sync per account',
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show sync status for all accounts',
    )
    return parser.parse_args(args)


def main():
    args = parse_args()

    syncer = GmailSyncer()
    syncer.init_db()

    if args.status:
        status = syncer.get_status()
        print("\nGmail Sync Status")
        print("=" * 50)
        print(f"Database: {status['database']}")
        print("\nAccounts:")
        for name, info in status['accounts'].items():
            print(f"  - {name}")
            print(f"      Type: {info['auth_type']}")
            print(f"      Last sync: {info['last_sync']}")
            print(f"      Emails: {info['email_count']:,}")
        print()
        return

    # Parse since date
    since_date = None
    if args.since:
        try:
            since_date = datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format: {args.since}. Use YYYY-MM-DD.")
            return

    # Get accounts to sync
    all_accounts = get_configured_accounts()

    if not all_accounts:
        logger.error("No Gmail accounts configured.")
        logger.info("For OAuth: Run 'python scripts/contact_intel/google_auth.py --auth'")
        logger.info("For IMAP: Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD in .env")
        return

    if args.account == 'all':
        accounts = all_accounts
    elif args.account:
        accounts = [a for a in all_accounts if a.name == args.account]
        if not accounts:
            logger.error(f"Account not found: {args.account}")
            logger.info(f"Available: {[a.name for a in all_accounts]}")
            return
    else:
        # Default to first account
        accounts = [all_accounts[0]]
        logger.info(f"Using default account: {accounts[0].name}")

    # Progress callback
    def progress(current, total, message):
        logger.info(f"[{current}/{total}] {message}")

    # Sync
    results = syncer.sync_accounts(accounts, since_date, args.limit, progress)

    # Summary
    print("\nSync Complete")
    print("=" * 50)
    total = 0
    for account, count in results.items():
        print(f"  {account}: {count:,} emails")
        total += count
    print(f"  Total: {total:,} emails")


if __name__ == '__main__':
    main()
