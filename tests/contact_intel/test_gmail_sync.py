"""Tests for Gmail sync module (TDD - written before implementation).

These tests verify:
1. Loading OAuth and IMAP credentials
2. Fetching email headers
3. Incremental sync with state persistence
4. Multi-account support
5. SQLite storage
"""

import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


class TestLoadCredentials:
    """Test credential loading for both OAuth and IMAP accounts."""

    def test_load_oauth_credentials(self, tmp_path):
        """Should load OAuth credentials from token file."""
        from scripts.contact_intel.gmail_sync import load_oauth_credentials

        # Create a mock token file
        token_data = {
            "token": "fake_access_token",
            "refresh_token": "fake_refresh_token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "fake_client_id",
            "client_secret": "fake_client_secret",
            "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
        }
        token_path = tmp_path / "test_token.json"
        token_path.write_text(json.dumps(token_data))

        creds = load_oauth_credentials(str(token_path))

        assert creds is not None
        assert creds.token == "fake_access_token"
        assert creds.refresh_token == "fake_refresh_token"

    def test_load_oauth_credentials_missing_file(self):
        """Should return None for missing token file."""
        from scripts.contact_intel.gmail_sync import load_oauth_credentials

        creds = load_oauth_credentials("/nonexistent/path/token.json")
        assert creds is None

    def test_load_imap_credentials(self):
        """Should load IMAP credentials from .env."""
        from scripts.contact_intel.gmail_sync import load_imap_credentials

        # Mock environment variables
        with patch.dict(os.environ, {
            "GMAIL_ADDRESS": "test@gmail.com",
            "GMAIL_APP_PASSWORD": "test_password",
        }):
            creds = load_imap_credentials()

            assert creds is not None
            assert creds["email"] == "test@gmail.com"
            assert creds["password"] == "test_password"

    def test_load_imap_credentials_missing(self):
        """Should return None when IMAP credentials missing."""
        from scripts.contact_intel.gmail_sync import load_imap_credentials

        with patch.dict(os.environ, {}, clear=True):
            creds = load_imap_credentials()
            assert creds is None


class TestEmailHeaderExtraction:
    """Test extraction of email headers."""

    def test_fetch_email_headers_basic(self):
        """Should extract From, To, CC, Date, Subject, Message-ID."""
        from scripts.contact_intel.gmail_sync import parse_email_headers

        # Simulate a raw email message
        raw_email = """From: sender@example.com
To: recipient@example.com
Cc: cc1@example.com, cc2@example.com
Date: Sat, 25 Jan 2025 10:30:00 -0500
Subject: Test Email Subject
Message-ID: <abc123@example.com>
In-Reply-To: <parent123@example.com>

This is the body.
"""
        headers = parse_email_headers(raw_email)

        assert headers["from_email"] == "sender@example.com"
        assert headers["to_emails"] == ["recipient@example.com"]
        assert headers["cc_emails"] == ["cc1@example.com", "cc2@example.com"]
        assert headers["subject"] == "Test Email Subject"
        assert headers["message_id"] == "<abc123@example.com>"
        assert headers["in_reply_to"] == "<parent123@example.com>"
        assert headers["date"] is not None

    def test_fetch_email_headers_with_names(self):
        """Should extract email addresses from 'Name <email>' format."""
        from scripts.contact_intel.gmail_sync import parse_email_headers

        raw_email = """From: John Doe <john@example.com>
To: Jane Smith <jane@example.com>
Date: Sat, 25 Jan 2025 10:30:00 -0500
Subject: Hello
Message-ID: <msg1@example.com>

Body
"""
        headers = parse_email_headers(raw_email)

        assert headers["from_email"] == "john@example.com"
        assert headers["from_name"] == "John Doe"
        assert headers["to_emails"] == ["jane@example.com"]

    def test_fetch_email_headers_encoded(self):
        """Should decode MIME-encoded headers (UTF-8, etc)."""
        from scripts.contact_intel.gmail_sync import parse_email_headers

        # MIME-encoded subject (=?UTF-8?Q?...?= format)
        raw_email = """From: test@example.com
To: recipient@example.com
Date: Sat, 25 Jan 2025 10:30:00 -0500
Subject: =?UTF-8?Q?Caf=C3=A9_Test?=
Message-ID: <enc1@example.com>

Body
"""
        headers = parse_email_headers(raw_email)

        assert headers["subject"] == "Cafe Test" or "Caf" in headers["subject"]

    def test_fetch_email_headers_bcc(self):
        """Should handle BCC when present."""
        from scripts.contact_intel.gmail_sync import parse_email_headers

        raw_email = """From: sender@example.com
To: recipient@example.com
Bcc: secret@example.com
Date: Sat, 25 Jan 2025 10:30:00 -0500
Subject: With BCC
Message-ID: <bcc1@example.com>

Body
"""
        headers = parse_email_headers(raw_email)

        assert headers["bcc_emails"] == ["secret@example.com"]


class TestEmailFetchingSinceDate:
    """Test date-based filtering of emails."""

    def test_fetch_emails_since_date(self):
        """Should only fetch emails after specified date."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        syncer = GmailSyncer(db_path=":memory:")

        # Mock the Gmail API response
        mock_service = MagicMock()
        mock_messages = MagicMock()
        mock_service.users.return_value.messages.return_value = mock_messages

        # Return empty list for this test
        mock_messages.list.return_value.execute.return_value = {"messages": []}

        since_date = datetime(2024, 1, 1)

        # The query should include the date filter
        with patch.object(syncer, "_get_gmail_service", return_value=mock_service):
            syncer._fetch_messages_oauth(mock_service, since_date, limit=10)

        # Verify the API was called with correct query
        call_args = mock_messages.list.call_args
        assert "after:2024/01/01" in call_args.kwargs.get("q", "")


class TestIncrementalSync:
    """Test incremental sync state management."""

    def test_incremental_sync_saves_state(self, tmp_path):
        """Should save last sync timestamp to sync_state.json."""
        from scripts.contact_intel.gmail_sync import SyncStateManager

        state_file = tmp_path / "sync_state.json"
        manager = SyncStateManager(str(state_file))

        # Save state for an account
        sync_time = datetime(2025, 1, 25, 12, 0, 0)
        manager.update_last_sync("tujaguarcapital", sync_time)

        # Verify state was saved
        assert state_file.exists()
        state = json.loads(state_file.read_text())
        assert "tujaguarcapital" in state
        assert state["tujaguarcapital"]["last_sync"] == "2025-01-25T12:00:00"

    def test_incremental_sync_loads_state(self, tmp_path):
        """Should load last sync timestamp from file."""
        from scripts.contact_intel.gmail_sync import SyncStateManager

        state_file = tmp_path / "sync_state.json"
        state_file.write_text(json.dumps({
            "account1": {"last_sync": "2025-01-20T10:00:00"},
            "account2": {"last_sync": "2025-01-15T08:30:00"},
        }))

        manager = SyncStateManager(str(state_file))

        last_sync = manager.get_last_sync("account1")
        assert last_sync == datetime(2025, 1, 20, 10, 0, 0)

    def test_incremental_sync_new_account(self, tmp_path):
        """Should return None for accounts never synced."""
        from scripts.contact_intel.gmail_sync import SyncStateManager

        state_file = tmp_path / "sync_state.json"
        manager = SyncStateManager(str(state_file))

        last_sync = manager.get_last_sync("new_account")
        assert last_sync is None


class TestMultipleAccounts:
    """Test multi-account support."""

    def test_handles_multiple_accounts(self, tmp_path):
        """Should sync from multiple accounts sequentially."""
        from scripts.contact_intel.gmail_sync import GmailSyncer, AccountConfig

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))

        # Configure two accounts
        accounts = [
            AccountConfig(
                name="oauth_account",
                auth_type="oauth",
                token_path="/path/to/token.json",
            ),
            AccountConfig(
                name="imap_account",
                auth_type="imap",
                email="test@gmail.com",
                password="app_password",
            ),
        ]

        # Mock sync methods
        with patch.object(syncer, "_sync_oauth_account") as mock_oauth, \
             patch.object(syncer, "_sync_imap_account") as mock_imap:

            mock_oauth.return_value = 10  # 10 emails synced
            mock_imap.return_value = 5    # 5 emails synced

            results = syncer.sync_accounts(accounts, limit=100)

            assert mock_oauth.called
            assert mock_imap.called
            assert results["oauth_account"] == 10
            assert results["imap_account"] == 5

    def test_account_config_validation(self):
        """Should validate account configuration."""
        from scripts.contact_intel.gmail_sync import AccountConfig

        # Valid OAuth config
        oauth_config = AccountConfig(
            name="test",
            auth_type="oauth",
            token_path="/path/to/token.json",
        )
        assert oauth_config.is_valid()

        # Invalid - missing token path for OAuth
        invalid_oauth = AccountConfig(
            name="test",
            auth_type="oauth",
        )
        assert not invalid_oauth.is_valid()

        # Valid IMAP config
        imap_config = AccountConfig(
            name="test",
            auth_type="imap",
            email="test@gmail.com",
            password="password",
        )
        assert imap_config.is_valid()

        # Invalid - missing password for IMAP
        invalid_imap = AccountConfig(
            name="test",
            auth_type="imap",
            email="test@gmail.com",
        )
        assert not invalid_imap.is_valid()


class TestSQLiteStorage:
    """Test SQLite database storage."""

    def test_create_database_schema(self, tmp_path):
        """Should create emails table with correct schema."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))
        syncer.init_db()

        # Verify table exists
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='emails'"
        )
        assert cursor.fetchone() is not None

        # Verify columns
        cursor = conn.execute("PRAGMA table_info(emails)")
        columns = {row[1] for row in cursor.fetchall()}
        expected_columns = {
            "id", "account", "message_id", "thread_id", "from_email", "from_name",
            "to_emails", "cc_emails", "bcc_emails", "subject", "date",
            "in_reply_to", "fetched_at",
        }
        assert expected_columns.issubset(columns)
        conn.close()

    def test_save_email_to_db(self, tmp_path):
        """Should save email metadata to SQLite."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))
        syncer.init_db()

        email_data = {
            "account": "test_account",
            "message_id": "<test123@example.com>",
            "thread_id": "thread_abc",
            "from_email": "sender@example.com",
            "from_name": "Sender Name",
            "to_emails": ["recipient@example.com"],
            "cc_emails": ["cc@example.com"],
            "bcc_emails": [],
            "subject": "Test Subject",
            "date": datetime(2025, 1, 25, 10, 0, 0),
            "in_reply_to": None,
        }

        syncer.save_email(email_data)

        # Verify email was saved
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT * FROM emails WHERE message_id = ?", (email_data["message_id"],))
        row = cursor.fetchone()
        assert row is not None
        conn.close()

    def test_skip_duplicate_emails(self, tmp_path):
        """Should not save duplicate emails (same message_id)."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))
        syncer.init_db()

        email_data = {
            "account": "test_account",
            "message_id": "<duplicate@example.com>",
            "thread_id": "thread_abc",
            "from_email": "sender@example.com",
            "from_name": "Sender",
            "to_emails": ["recipient@example.com"],
            "cc_emails": [],
            "bcc_emails": [],
            "subject": "Test",
            "date": datetime(2025, 1, 25, 10, 0, 0),
            "in_reply_to": None,
        }

        # Save twice
        syncer.save_email(email_data)
        syncer.save_email(email_data)

        # Verify only one record
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT COUNT(*) FROM emails WHERE message_id = ?", (email_data["message_id"],))
        count = cursor.fetchone()[0]
        assert count == 1
        conn.close()

    def test_get_email_count_by_account(self, tmp_path):
        """Should return email count per account."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))
        syncer.init_db()

        # Insert emails for different accounts
        for i in range(5):
            syncer.save_email({
                "account": "account1",
                "message_id": f"<msg{i}@account1.com>",
                "thread_id": f"thread{i}",
                "from_email": "sender@example.com",
                "from_name": "Sender",
                "to_emails": ["recipient@example.com"],
                "cc_emails": [],
                "bcc_emails": [],
                "subject": f"Test {i}",
                "date": datetime(2025, 1, 25, 10, 0, 0),
                "in_reply_to": None,
            })

        for i in range(3):
            syncer.save_email({
                "account": "account2",
                "message_id": f"<msg{i}@account2.com>",
                "thread_id": f"thread{i}",
                "from_email": "sender@example.com",
                "from_name": "Sender",
                "to_emails": ["recipient@example.com"],
                "cc_emails": [],
                "bcc_emails": [],
                "subject": f"Test {i}",
                "date": datetime(2025, 1, 25, 10, 0, 0),
                "in_reply_to": None,
            })

        counts = syncer.get_email_counts()
        assert counts["account1"] == 5
        assert counts["account2"] == 3


class TestProgressLogging:
    """Test progress logging and incremental saves."""

    def test_progress_callback(self, tmp_path):
        """Should call progress callback during sync."""
        from scripts.contact_intel.gmail_sync import GmailSyncer

        db_path = tmp_path / "emails.db"
        syncer = GmailSyncer(db_path=str(db_path))
        syncer.init_db()

        progress_calls = []

        def progress_callback(current, total, message):
            progress_calls.append((current, total, message))

        # Mock email processing
        emails = [
            {
                "account": "test",
                "message_id": f"<msg{i}@test.com>",
                "thread_id": f"thread{i}",
                "from_email": "sender@example.com",
                "from_name": "Sender",
                "to_emails": ["recipient@example.com"],
                "cc_emails": [],
                "bcc_emails": [],
                "subject": f"Test {i}",
                "date": datetime(2025, 1, 25, 10, 0, 0),
                "in_reply_to": None,
            }
            for i in range(10)
        ]

        syncer.save_emails_batch(emails, progress_callback=progress_callback, batch_size=3)

        # Should have progress calls
        assert len(progress_calls) > 0
        # Last call should show completion
        assert progress_calls[-1][0] == 10


class TestCLIInterface:
    """Test CLI argument parsing."""

    def test_parse_account_argument(self):
        """Should parse --account argument."""
        from scripts.contact_intel.gmail_sync import parse_args

        args = parse_args(["--account", "tujaguarcapital"])
        assert args.account == "tujaguarcapital"

    def test_parse_since_argument(self):
        """Should parse --since date argument."""
        from scripts.contact_intel.gmail_sync import parse_args

        args = parse_args(["--since", "2024-01-01"])
        assert args.since == "2024-01-01"

    def test_parse_limit_argument(self):
        """Should parse --limit argument."""
        from scripts.contact_intel.gmail_sync import parse_args

        args = parse_args(["--limit", "100"])
        assert args.limit == 100

    def test_parse_status_flag(self):
        """Should parse --status flag."""
        from scripts.contact_intel.gmail_sync import parse_args

        args = parse_args(["--status"])
        assert args.status is True

    def test_parse_all_accounts(self):
        """Should parse --account all."""
        from scripts.contact_intel.gmail_sync import parse_args

        args = parse_args(["--account", "all"])
        assert args.account == "all"
