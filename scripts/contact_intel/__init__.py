# Contact Intel Module
# Tools for analyzing Gmail and LinkedIn connections to find warm intro paths

from .models import (
    AuthType,
    Company,
    EmailMessage,
    GmailAccount,
    Person,
    Relationship,
)
from .config import (
    BASE_DIR,
    CONFIG_DIR,
    DATA_DIR,
    CONTACT_INTEL_CONFIG,
    PERSONAL_EMAIL_DOMAINS,
    ensure_data_dir,
    get_account,
    get_account_by_email,
    get_gmail_accounts,
    get_imap_accounts,
    get_oauth_accounts,
    get_sync_state,
    get_token_path,
    is_personal_email_domain,
    save_sync_state,
)

__all__ = [
    # Models
    "AuthType",
    "Company",
    "EmailMessage",
    "GmailAccount",
    "Person",
    "Relationship",
    # Config
    "BASE_DIR",
    "CONFIG_DIR",
    "DATA_DIR",
    "CONTACT_INTEL_CONFIG",
    "PERSONAL_EMAIL_DOMAINS",
    "ensure_data_dir",
    "get_account",
    "get_account_by_email",
    "get_gmail_accounts",
    "get_imap_accounts",
    "get_oauth_accounts",
    "get_sync_state",
    "get_token_path",
    "is_personal_email_domain",
    "save_sync_state",
]
