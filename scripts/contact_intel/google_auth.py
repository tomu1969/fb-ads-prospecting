"""Google OAuth 2.0 Authentication for Gmail + Contacts API.

This script handles OAuth authentication for Google Workspace accounts
where app passwords are disabled. It will:
1. Open a browser for authorization
2. Exchange the code for access + refresh tokens
3. Save tokens for future use (auto-refresh)

Usage:
    # First-time auth (opens browser)
    python scripts/contact_intel/google_auth.py --auth

    # Test connection
    python scripts/contact_intel/google_auth.py --test

    # List available accounts
    python scripts/contact_intel/google_auth.py --list
"""

import argparse
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Paths
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "contact_intel"

# OAuth scopes we need
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',      # Read emails
    'https://www.googleapis.com/auth/contacts.readonly',   # Read contacts
    'https://www.googleapis.com/auth/userinfo.email',      # Get user email
]


def get_client_secrets():
    """Find all OAuth client secret files in config directory."""
    secrets = []
    for f in CONFIG_DIR.glob("*client_secret*.json"):
        try:
            with open(f) as fp:
                data = json.load(fp)
                client_id = data.get("installed", {}).get("client_id", "")
                # Extract account name from filename
                name = f.stem.split("_client_secret")[0]
                secrets.append({
                    "name": name,
                    "path": f,
                    "client_id": client_id,
                })
        except Exception as e:
            logger.warning(f"Could not read {f}: {e}")
    return secrets


def get_token_path(account_name: str) -> Path:
    """Get path to store OAuth tokens for an account."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"{account_name}_token.json"


def authenticate(client_secret_path: Path, account_name: str):
    """Run OAuth flow and save credentials."""
    try:
        import os
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        logger.error("Missing Google auth libraries. Install with:")
        logger.error("  pip install google-auth google-auth-oauthlib google-api-python-client")
        return None

    # Allow Google to modify scopes (e.g., adding 'openid')
    os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'

    token_path = get_token_path(account_name)
    creds = None

    # Check for existing credentials
    if token_path.exists():
        logger.info(f"Found existing token at {token_path}")
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    # If no valid credentials, run the flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired token...")
            creds.refresh(Request())
        else:
            logger.info("Starting OAuth flow - a browser will open for authorization...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path),
                SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save credentials
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
        logger.info(f"Credentials saved to {token_path}")

    return creds


def test_gmail_connection(creds):
    """Test Gmail API connection by listing labels."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        logger.error("Missing Google API client. Install with:")
        logger.error("  pip install google-api-python-client")
        return False

    try:
        service = build('gmail', 'v1', credentials=creds)

        # Get user profile
        profile = service.users().getProfile(userId='me').execute()
        email = profile.get('emailAddress')
        total_messages = profile.get('messagesTotal', 0)

        logger.info(f"Connected to Gmail: {email}")
        logger.info(f"Total messages: {total_messages:,}")

        # List some labels
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        logger.info(f"Found {len(labels)} labels")

        return True
    except Exception as e:
        logger.error(f"Gmail API error: {e}")
        return False


def test_contacts_connection(creds):
    """Test Google Contacts API connection."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return False

    try:
        service = build('people', 'v1', credentials=creds)

        # Get total contacts count
        results = service.people().connections().list(
            resourceName='people/me',
            pageSize=10,
            personFields='names,emailAddresses'
        ).execute()

        connections = results.get('connections', [])
        total = results.get('totalPeople', len(connections))

        logger.info(f"Contacts API connected. Found {total:,} contacts")

        # Show first few
        for person in connections[:3]:
            names = person.get('names', [])
            emails = person.get('emailAddresses', [])
            name = names[0].get('displayName') if names else 'Unknown'
            email = emails[0].get('value') if emails else 'No email'
            logger.info(f"  - {name} ({email})")

        return True
    except Exception as e:
        logger.error(f"Contacts API error: {e}")
        return False


def list_accounts():
    """List all configured Google accounts."""
    secrets = get_client_secrets()

    if not secrets:
        logger.warning("No OAuth client secrets found in config/")
        logger.info("Add a client_secret*.json file from Google Cloud Console")
        return

    logger.info(f"Found {len(secrets)} OAuth client(s):")
    for s in secrets:
        token_path = get_token_path(s['name'])
        status = "authenticated" if token_path.exists() else "not authenticated"
        logger.info(f"  - {s['name']}: {status}")
        logger.info(f"    Client ID: {s['client_id'][:30]}...")


def main():
    parser = argparse.ArgumentParser(description='Google OAuth authentication')
    parser.add_argument('--auth', action='store_true', help='Run OAuth flow')
    parser.add_argument('--test', action='store_true', help='Test connection')
    parser.add_argument('--list', action='store_true', help='List accounts')
    parser.add_argument('--account', type=str, help='Account name (from client secret filename)')
    args = parser.parse_args()

    if args.list:
        list_accounts()
        return

    # Find client secrets
    secrets = get_client_secrets()
    if not secrets:
        logger.error("No OAuth client secrets found in config/")
        return

    # Select account
    if args.account:
        secret = next((s for s in secrets if s['name'] == args.account), None)
        if not secret:
            logger.error(f"Account '{args.account}' not found")
            logger.info(f"Available: {[s['name'] for s in secrets]}")
            return
    else:
        # Use first available
        secret = secrets[0]
        logger.info(f"Using account: {secret['name']}")

    if args.auth or args.test:
        creds = authenticate(secret['path'], secret['name'])
        if not creds:
            return

        if args.test:
            logger.info("\nTesting Gmail API...")
            gmail_ok = test_gmail_connection(creds)

            logger.info("\nTesting Contacts API...")
            contacts_ok = test_contacts_connection(creds)

            if gmail_ok and contacts_ok:
                logger.info("\nAll connections successful!")
            else:
                logger.warning("\nSome connections failed. Check errors above.")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
