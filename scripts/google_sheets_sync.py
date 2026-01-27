"""
Google Sheets Sync - Sync master CSV to Google Sheets

Automatically updates a Google Sheet whenever the master CSV changes.
Can be called after any enrichment or data addition.

SETUP:
1. Go to https://console.cloud.google.com/
2. Create a new project (or use existing)
3. Enable "Google Sheets API" and "Google Drive API"
4. Create credentials -> Service Account
5. Download JSON key file -> save as config/google_service_account.json
6. Create a Google Sheet and share it with the service account email
7. Copy the Sheet ID from the URL and set GOOGLE_SHEET_ID in .env

Usage:
    python scripts/google_sheets_sync.py                    # Full sync
    python scripts/google_sheets_sync.py --create           # Create new sheet
    python scripts/google_sheets_sync.py --input FILE.csv   # Sync specific file
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/prospects_master.csv"
CREDENTIALS_FILE = "config/google-services-key.json"

# Google Sheets config
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID', '')
SHEET_NAME = "Master Contacts"


def get_google_client():
    """Initialize Google Sheets client."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.error("Missing dependencies. Run: pip install gspread google-auth")
        sys.exit(1)

    creds_path = BASE_DIR / CREDENTIALS_FILE
    if not creds_path.exists():
        logger.error(f"Credentials not found: {creds_path}")
        logger.info("See script docstring for setup instructions")
        sys.exit(1)

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    client = gspread.authorize(creds)

    return client


def create_new_sheet(client, title: str = None) -> str:
    """Create a new Google Sheet and return its ID."""
    if not title:
        title = f"Prospects Master - {datetime.now().strftime('%Y-%m-%d')}"

    sheet = client.create(title)
    logger.info(f"Created new sheet: {title}")
    logger.info(f"Sheet ID: {sheet.id}")
    logger.info(f"URL: https://docs.google.com/spreadsheets/d/{sheet.id}")
    logger.info(f"\nAdd this to your .env file:")
    logger.info(f"GOOGLE_SHEET_ID={sheet.id}")

    return sheet.id


def sync_to_sheet(df: pd.DataFrame, sheet_id: str, worksheet_name: str = SHEET_NAME):
    """Sync DataFrame to Google Sheet."""
    client = get_google_client()

    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        logger.error(f"Cannot open sheet {sheet_id}: {e}")
        logger.info("Make sure the sheet is shared with the service account email")
        sys.exit(1)

    # Get or create worksheet
    try:
        worksheet = sheet.worksheet(worksheet_name)
        logger.info(f"Updating existing worksheet: {worksheet_name}")
    except:
        worksheet = sheet.add_worksheet(title=worksheet_name, rows=len(df)+1, cols=len(df.columns))
        logger.info(f"Created new worksheet: {worksheet_name}")

    # Clear existing data
    worksheet.clear()

    # Prepare data (handle NaN, convert to strings)
    df_clean = df.fillna('').astype(str)

    # Select key columns for the sheet (to avoid hitting cell limits)
    key_columns = [
        'contact_name', 'primary_email', 'primary_phone', 'company_name',
        'website_url', 'instagram_handle', 'linkedin_profile', 'linkedin_url',
        'source', 'lead_score', 'lead_tier', 'gmaps_rating', 'gmaps_review_count',
        'has_meta_ads', 'has_marketing_pixel', 'scrape_date'
    ]

    # Use only columns that exist
    available_cols = [c for c in key_columns if c in df_clean.columns]
    if not available_cols:
        available_cols = list(df_clean.columns)[:20]  # Fallback to first 20

    df_subset = df_clean[available_cols]

    # Convert to list of lists
    data = [df_subset.columns.tolist()] + df_subset.values.tolist()

    # Update in batches (Google Sheets has limits)
    BATCH_SIZE = 1000
    total_rows = len(data)

    logger.info(f"Syncing {total_rows-1} rows, {len(available_cols)} columns...")

    for i in range(0, total_rows, BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        start_row = i + 1
        end_row = start_row + len(batch) - 1
        end_col = chr(ord('A') + len(available_cols) - 1)

        cell_range = f"A{start_row}:{end_col}{end_row}"
        worksheet.update(cell_range, batch, value_input_option='RAW')
        logger.info(f"  Updated rows {start_row}-{end_row}")

    # Format header row
    worksheet.format('1:1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })

    # Freeze header row
    worksheet.freeze(rows=1)

    logger.info(f"Sync complete: {total_rows-1} contacts")
    logger.info(f"View at: https://docs.google.com/spreadsheets/d/{sheet_id}")

    return True


def main():
    parser = argparse.ArgumentParser(description='Sync master CSV to Google Sheets')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV file')
    parser.add_argument('--create', action='store_true', help='Create new Google Sheet')
    parser.add_argument('--sheet-id', type=str, help='Override sheet ID')
    parser.add_argument('--worksheet', type=str, default=SHEET_NAME, help='Worksheet name')

    args = parser.parse_args()

    client = get_google_client()

    # Create new sheet if requested
    if args.create:
        sheet_id = create_new_sheet(client)
        print(f"\nSheet created! Add to .env: GOOGLE_SHEET_ID={sheet_id}")
        return 0

    # Determine sheet ID
    sheet_id = args.sheet_id or GOOGLE_SHEET_ID
    if not sheet_id:
        logger.error("No sheet ID. Run with --create or set GOOGLE_SHEET_ID in .env")
        return 1

    # Load CSV
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = BASE_DIR / args.input

    if not input_path.exists():
        logger.error(f"File not found: {input_path}")
        return 1

    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} contacts from {input_path.name}")

    # Sync to sheet
    sync_to_sheet(df, sheet_id, args.worksheet)

    return 0


if __name__ == '__main__':
    sys.exit(main())
