"""Configuration management for Contact Intelligence System.

Handles:
- Multi-account Gmail configuration (OAuth + IMAP)
- Path management for data, config, and tokens
- Sync state persistence
- Personal email domain detection
"""

import json
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from .models import AuthType, GmailAccount

# Load environment variables
load_dotenv()

# ============================================================================
# Path Configuration
# ============================================================================

BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data" / "contact_intel"
CONTACT_INTEL_CONFIG = CONFIG_DIR / "contact_intel"

# ============================================================================
# Personal Email Domains
# ============================================================================

PERSONAL_EMAIL_DOMAINS = frozenset({
    # Google
    "gmail.com",
    "googlemail.com",
    # Microsoft
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    # Yahoo
    "yahoo.com",
    "ymail.com",
    "yahoo.co.uk",
    "yahoo.fr",
    "yahoo.de",
    "yahoo.es",
    "yahoo.com.mx",
    "yahoo.com.br",
    "yahoo.com.ar",
    # Apple
    "icloud.com",
    "me.com",
    "mac.com",
    # AOL
    "aol.com",
    "aim.com",
    # ProtonMail
    "protonmail.com",
    "proton.me",
    "pm.me",
    # Other common providers
    "mail.com",
    "email.com",
    "zoho.com",
    "gmx.com",
    "gmx.net",
    "web.de",
    "t-online.de",
    "comcast.net",
    "verizon.net",
    "att.net",
    "sbcglobal.net",
    "bellsouth.net",
    "earthlink.net",
    "cox.net",
    "charter.net",
    # International
    "qq.com",
    "163.com",
    "126.com",
    "yeah.net",
    "sina.com",
    "sohu.com",
    "rediffmail.com",
    "mail.ru",
    "yandex.ru",
    "yandex.com",
    "libero.it",
    "virgilio.it",
    "laposte.net",
    "orange.fr",
    "free.fr",
    "wanadoo.fr",
    "sfr.fr",
    "o2.pl",
    "wp.pl",
    "onet.pl",
    "interia.pl",
})


def is_personal_email_domain(domain: str) -> bool:
    """Check if a domain is a personal email provider.

    Args:
        domain: Email domain like "gmail.com"

    Returns:
        True if it's a personal email provider, False otherwise
    """
    return domain.lower() in PERSONAL_EMAIL_DOMAINS


# ============================================================================
# Path Utilities
# ============================================================================

def ensure_data_dir() -> Path:
    """Ensure the data directory exists.

    Returns:
        Path to the data directory
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR


def get_token_path(account_name: str) -> Path:
    """Get path to store OAuth tokens for an account.

    Args:
        account_name: Name of the account (e.g., "tujaguarcapital")

    Returns:
        Path to token file
    """
    ensure_data_dir()
    return DATA_DIR / f"{account_name}_token.json"


# ============================================================================
# Account Discovery
# ============================================================================

def get_oauth_accounts() -> List[GmailAccount]:
    """Discover OAuth accounts from client_secret files.

    Looks for *client_secret*.json files in config/ directory
    and checks if corresponding tokens exist.

    Returns:
        List of GmailAccount objects for OAuth accounts
    """
    accounts = []

    for client_secret_file in CONFIG_DIR.glob("*client_secret*.json"):
        try:
            # Extract account name from filename
            # e.g., "tujaguarcapital_client_secret_xxx.json" -> "tujaguarcapital"
            name = client_secret_file.stem.split("_client_secret")[0]

            # Check if token exists (to get email)
            token_path = get_token_path(name)
            email = None

            if token_path.exists():
                try:
                    with open(token_path) as f:
                        token_data = json.load(f)
                        # Token may contain email in various places
                        email = token_data.get("email")
                except (json.JSONDecodeError, KeyError):
                    pass

            accounts.append(GmailAccount(
                name=name,
                email=email,
                auth_type=AuthType.OAUTH,
                token_path=str(token_path),
            ))
        except Exception:
            # Skip files that don't match expected format
            continue

    return accounts


def get_imap_accounts() -> List[GmailAccount]:
    """Load IMAP accounts from environment variables.

    Looks for:
    - GMAIL_ADDRESS / GMAIL_APP_PASSWORD (default account)
    - GMAIL_{NAME}_ADDRESS / GMAIL_{NAME}_APP_PASSWORD (named accounts)

    Returns:
        List of GmailAccount objects for IMAP accounts
    """
    accounts = []

    # Check for default IMAP account
    default_email = os.getenv("GMAIL_ADDRESS")
    default_password = os.getenv("GMAIL_APP_PASSWORD")

    if default_email and default_password:
        # Extract name from email (e.g., "tomasuribe@lahaus.com" -> "lahaus")
        name = "default"
        if "@" in default_email:
            domain = default_email.split("@")[1]
            name = domain.split(".")[0]  # "lahaus.com" -> "lahaus"

        accounts.append(GmailAccount(
            name=name,
            email=default_email,
            auth_type=AuthType.IMAP,
            app_password=default_password,
        ))

    # Check for named accounts (GMAIL_PERSONAL_ADDRESS, etc.)
    for key, value in os.environ.items():
        if key.startswith("GMAIL_") and key.endswith("_ADDRESS"):
            # Extract account name
            # GMAIL_PERSONAL_ADDRESS -> PERSONAL
            name_part = key[6:-8]  # Remove "GMAIL_" prefix and "_ADDRESS" suffix

            if name_part and name_part != "ADDRESS":  # Skip malformed
                password_key = f"GMAIL_{name_part}_APP_PASSWORD"
                password = os.getenv(password_key)

                if password:
                    accounts.append(GmailAccount(
                        name=name_part.lower(),
                        email=value,
                        auth_type=AuthType.IMAP,
                        app_password=password,
                    ))

    return accounts


def get_gmail_accounts() -> List[GmailAccount]:
    """Load all configured Gmail accounts (OAuth + IMAP).

    Returns:
        List of all GmailAccount objects
    """
    oauth_accounts = get_oauth_accounts()
    imap_accounts = get_imap_accounts()

    return oauth_accounts + imap_accounts


def get_account(name: str) -> Optional[GmailAccount]:
    """Get a specific account by name.

    Args:
        name: Account name (e.g., "tujaguarcapital" or "lahaus")

    Returns:
        GmailAccount if found, None otherwise
    """
    for account in get_gmail_accounts():
        if account.name == name:
            return account
    return None


def get_account_by_email(email: str) -> Optional[GmailAccount]:
    """Get a specific account by email address.

    Args:
        email: Email address (e.g., "tu@jaguarcapital.co")

    Returns:
        GmailAccount if found, None otherwise
    """
    email = email.lower()
    for account in get_gmail_accounts():
        if account.email and account.email.lower() == email:
            return account
    return None


# ============================================================================
# Sync State Management
# ============================================================================

def get_sync_state() -> dict:
    """Load sync state (last sync times per account).

    Returns:
        Dict with account names as keys, containing:
        - last_sync: ISO timestamp of last sync
        - message_count: Number of messages synced
    """
    sync_file = DATA_DIR / "sync_state.json"

    if not sync_file.exists():
        return {}

    try:
        with open(sync_file) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_sync_state(state: dict) -> None:
    """Save sync state.

    Args:
        state: Dict with account names as keys
    """
    ensure_data_dir()
    sync_file = DATA_DIR / "sync_state.json"

    with open(sync_file, "w") as f:
        json.dump(state, f, indent=2, default=str)
