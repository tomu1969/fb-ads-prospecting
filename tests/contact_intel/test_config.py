"""Tests for Contact Intelligence configuration.

TDD: These tests are written first, before implementation.
"""

import pytest
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestLoadGmailAccounts:
    """Tests for loading Gmail account configurations."""

    def test_load_gmail_accounts(self):
        """Should load both OAuth and IMAP account configs."""
        from scripts.contact_intel.config import get_gmail_accounts
        from scripts.contact_intel.models import AuthType

        accounts = get_gmail_accounts()

        # Should return a list of GmailAccount objects
        assert isinstance(accounts, list)

        # In the real environment, we should have at least one account
        # (either from OAuth client secrets or IMAP env vars)
        # This test may pass with empty list in CI without credentials

    def test_load_oauth_accounts_from_client_secrets(self):
        """Should discover OAuth accounts from client_secret files."""
        from scripts.contact_intel.config import get_oauth_accounts

        # This should find accounts based on *client_secret*.json files in config/
        accounts = get_oauth_accounts()

        # Each account should have name, email (if token exists), auth_type=OAUTH
        for account in accounts:
            assert account.name
            assert account.auth_type.value == "oauth"

    def test_load_imap_accounts_from_env(self):
        """Should load IMAP accounts from environment variables."""
        from scripts.contact_intel.config import get_imap_accounts

        # Mock environment variables
        with patch.dict(os.environ, {
            "GMAIL_ADDRESS": "test@example.com",
            "GMAIL_APP_PASSWORD": "test-app-password",
        }):
            accounts = get_imap_accounts()

            # Should find the IMAP account
            assert len(accounts) >= 1
            imap_account = accounts[0]
            assert imap_account.auth_type.value == "imap"
            assert imap_account.email == "test@example.com"
            assert imap_account.app_password == "test-app-password"


class TestGetAccountCredentials:
    """Tests for retrieving specific account credentials."""

    def test_get_account_credentials(self):
        """Should return correct credentials for account type."""
        from scripts.contact_intel.config import get_account
        from scripts.contact_intel.models import AuthType

        # Test getting a specific account by name
        # This depends on actual config, so we test the interface

        # If account doesn't exist, should return None
        account = get_account("nonexistent_account_xyz")
        assert account is None

    def test_get_account_by_name(self):
        """Should find account by name."""
        from scripts.contact_intel.config import get_account, get_gmail_accounts

        accounts = get_gmail_accounts()
        if accounts:
            # Get the first account by name
            first_account = accounts[0]
            found = get_account(first_account.name)
            assert found is not None
            assert found.name == first_account.name

    def test_get_account_by_email(self):
        """Should find account by email address."""
        from scripts.contact_intel.config import get_account_by_email, get_gmail_accounts

        accounts = get_gmail_accounts()
        if accounts:
            # Find accounts that have email set
            accounts_with_email = [a for a in accounts if a.email]
            if accounts_with_email:
                first_account = accounts_with_email[0]
                found = get_account_by_email(first_account.email)
                assert found is not None
                assert found.email == first_account.email


class TestConfigPaths:
    """Tests for configuration path management."""

    def test_config_paths(self):
        """Should return correct paths for data, config, tokens."""
        from scripts.contact_intel.config import (
            BASE_DIR,
            CONFIG_DIR,
            DATA_DIR,
            CONTACT_INTEL_CONFIG,
        )

        # BASE_DIR should be the project root (fb-ads-prospecting)
        assert BASE_DIR.exists()
        assert BASE_DIR.name == "fb-ads-prospecting"

        # CONFIG_DIR should be config/
        assert CONFIG_DIR == BASE_DIR / "config"

        # DATA_DIR should be data/contact_intel/
        assert DATA_DIR == BASE_DIR / "data" / "contact_intel"

        # CONTACT_INTEL_CONFIG should be config/contact_intel/
        assert CONTACT_INTEL_CONFIG == BASE_DIR / "config" / "contact_intel"

    def test_get_token_path(self):
        """Should return correct token path for account."""
        from scripts.contact_intel.config import get_token_path, DATA_DIR

        token_path = get_token_path("myaccount")
        assert token_path == DATA_DIR / "myaccount_token.json"

    def test_ensure_data_dir_exists(self):
        """Should create data directory if it doesn't exist."""
        from scripts.contact_intel.config import ensure_data_dir

        # This should create the directory (idempotent)
        ensure_data_dir()

        from scripts.contact_intel.config import DATA_DIR
        assert DATA_DIR.exists()


class TestSyncState:
    """Tests for sync state management."""

    def test_get_sync_state(self):
        """Should load sync state (last sync times per account)."""
        from scripts.contact_intel.config import get_sync_state

        state = get_sync_state()

        # Should return a dict
        assert isinstance(state, dict)

        # If state exists, should have account names as keys
        # Values should have last_sync timestamps

    def test_save_sync_state(self):
        """Should save sync state."""
        from scripts.contact_intel.config import (
            get_sync_state,
            save_sync_state,
            DATA_DIR,
        )
        from datetime import datetime, timezone

        # Save some state
        test_state = {
            "test_account": {
                "last_sync": datetime.now(timezone.utc).isoformat(),
                "message_count": 100,
            }
        }

        save_sync_state(test_state)

        # Load it back
        loaded = get_sync_state()
        assert "test_account" in loaded
        assert loaded["test_account"]["message_count"] == 100

        # Cleanup: remove test data
        sync_file = DATA_DIR / "sync_state.json"
        if sync_file.exists():
            # Restore original or delete test entry
            del loaded["test_account"]
            if loaded:
                save_sync_state(loaded)
            else:
                sync_file.unlink()

    def test_sync_state_empty_file(self):
        """Should handle missing sync state file gracefully."""
        from scripts.contact_intel.config import get_sync_state, DATA_DIR
        import os

        sync_file = DATA_DIR / "sync_state.json"

        # Backup existing file if present
        backup_path = None
        if sync_file.exists():
            backup_path = sync_file.with_suffix(".json.bak")
            sync_file.rename(backup_path)

        try:
            state = get_sync_state()
            assert state == {}
        finally:
            # Restore backup
            if backup_path and backup_path.exists():
                backup_path.rename(sync_file)


class TestPersonalEmailDomains:
    """Tests for personal email domain detection."""

    def test_is_personal_email_domain(self):
        """Should identify personal email providers."""
        from scripts.contact_intel.config import is_personal_email_domain

        # Personal domains
        assert is_personal_email_domain("gmail.com") is True
        assert is_personal_email_domain("yahoo.com") is True
        assert is_personal_email_domain("hotmail.com") is True
        assert is_personal_email_domain("outlook.com") is True
        assert is_personal_email_domain("icloud.com") is True
        assert is_personal_email_domain("aol.com") is True
        assert is_personal_email_domain("protonmail.com") is True

        # Company domains
        assert is_personal_email_domain("company.com") is False
        assert is_personal_email_domain("acme.co") is False
        assert is_personal_email_domain("startup.io") is False

    def test_personal_domains_list(self):
        """Should have a comprehensive list of personal domains."""
        from scripts.contact_intel.config import PERSONAL_EMAIL_DOMAINS

        # Should be a set for fast lookup
        assert isinstance(PERSONAL_EMAIL_DOMAINS, (set, frozenset))

        # Should include common providers
        expected = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}
        assert expected.issubset(PERSONAL_EMAIL_DOMAINS)
