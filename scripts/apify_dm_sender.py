"""
Apify Instagram DM Sender Script

Sends Instagram direct messages to contacts via Apify actor.
Reads Instagram handles from CSV and sends personalized messages.

Usage:
    python scripts/apify_dm_sender.py --csv output/prospects.csv --message "Hi {contact_name}!"
    python scripts/apify_dm_sender.py --csv output/prospects.csv --dry-run --limit 3
"""

import os
import sys
import re
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).parent.parent
APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')

# Apify actor for Instagram DMs
APIFY_ACTOR_ID = os.getenv('APIFY_DM_ACTOR_ID', 'am_production/instagram-direct-messages-dms-automation')


def get_apify_client():
    """Get Apify client."""
    try:
        from apify_client import ApifyClient
        if not APIFY_API_TOKEN:
            return None
        return ApifyClient(APIFY_API_TOKEN)
    except ImportError:
        print("ERROR: apify-client not installed. Run: pip install apify-client")
        return None


def parse_instagram_handles(handles_str: str) -> List[str]:
    """Parse Instagram handles from CSV column."""
    if pd.isna(handles_str) or not handles_str:
        return []

    handles_str = str(handles_str).strip()
    if not handles_str or handles_str == '[]':
        return []

    # Handle list-like strings: ['handle1', 'handle2']
    if handles_str.startswith('[') and handles_str.endswith(']'):
        try:
            import ast
            handles_list = ast.literal_eval(handles_str)
            if isinstance(handles_list, list):
                handles_str = ','.join(str(h) for h in handles_list)
        except:
            pass

    # Split by comma
    handles = [h.strip() for h in handles_str.split(',')]

    # Clean each handle
    cleaned = []
    for handle in handles:
        handle = handle.strip().strip("'\"")
        if not handle:
            continue
        if handle.startswith('@'):
            handle = handle[1:]
        # Validate format
        if re.match(r'^[a-zA-Z0-9._]+$', handle):
            cleaned.append(handle.lower())

    return cleaned


def get_contact_name(row: pd.Series) -> str:
    """Extract contact first name from row."""
    # Try matched_name first
    if 'matched_name' in row.index:
        name = str(row.get('matched_name', '')).strip()
        if name and name.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return name.split()[0] if name.split() else name

    # Try contact_name
    if 'contact_name' in row.index:
        name = str(row.get('contact_name', '')).strip()
        if name and name.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return name.split()[0] if name.split() else name

    return ''


def get_company_name(row: pd.Series) -> str:
    """Extract company name from row."""
    if 'page_name' in row.index:
        company = str(row.get('page_name', '')).strip()
        if company and company.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return company

    if 'company' in row.index:
        company = str(row.get('company', '')).strip()
        if company and company.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return company

    return ''


def format_message(template: str, contact_name: str, company_name: str, instagram_handle: str) -> str:
    """Format message template with variables."""
    message = template
    message = message.replace('{contact_name}', contact_name or 'there')
    message = message.replace('{company_name}', company_name or 'your company')
    message = message.replace('{instagram_handle}', instagram_handle or '')
    return message


def send_dm_via_apify(client, username: str, message: str) -> Tuple[bool, Optional[str]]:
    """
    Send Instagram DM via Apify actor (am_production/instagram-direct-messages-dms-automation).

    Returns:
        Tuple of (success, error_message)
    """
    # Get Instagram session/cookies
    instagram_session = os.getenv('INSTAGRAM_SESSION_ID')
    if not instagram_session:
        return False, "INSTAGRAM_SESSION_ID not found in environment"

    # Build cookies string
    instagram_cookies = f"sessionid={instagram_session}"

    try:
        # Input format for am_production/instagram-direct-messages-dms-automation
        # Cookies must include domain for browser context
        cookies_array = [
            {
                "name": "sessionid",
                "value": instagram_session,
                "domain": ".instagram.com",
                "path": "/"
            }
        ]

        run_input = {
            "influencers": [username],
            "messages": [message],
            "INSTAGRAM_COOKIES": cookies_array,
        }

        # Call the actor
        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)

        # Check results
        if run.get('status') == 'SUCCEEDED':
            return True, None
        else:
            return False, f"Actor run failed: {run.get('status', 'UNKNOWN')}"

    except Exception as e:
        return False, str(e)


def process_csv_and_send(csv_path: Path, message_template: str,
                         dry_run: bool = False, limit: Optional[int] = None,
                         delay: float = 2.0, exclude_handles: Optional[List[str]] = None) -> Dict:
    """Process CSV and send DMs."""
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        return {'error': f"Failed to read CSV: {e}", 'results': []}

    # Check for instagram_handles column
    if 'instagram_handles' not in df.columns:
        return {'error': "CSV missing 'instagram_handles' column", 'results': []}

    # Get Apify client
    client = None
    if not dry_run:
        client = get_apify_client()
        if not client:
            return {'error': "Apify client not available. Check APIFY_API_TOKEN.", 'results': []}

    # Track results
    processed_handles = set()
    results = []
    sent_count = 0
    skipped_count = 0
    error_count = 0

    # Process rows
    total_rows = len(df)
    if limit:
        total_rows = min(total_rows, limit)

    row_count = 0
    with tqdm(total=total_rows, desc="Processing", unit="row") as pbar:
        for idx, row in df.iterrows():
            if limit and row_count >= limit:
                break

            handles = parse_instagram_handles(row.get('instagram_handles', ''))
            if not handles:
                pbar.update(1)
                row_count += 1
                continue

            contact_name = get_contact_name(row)
            company_name = get_company_name(row)

            for handle in handles:
                if handle in processed_handles:
                    continue
                # Check exclusion list
                if exclude_handles and handle.lower() in [h.lower() for h in exclude_handles]:
                    skipped_count += 1
                    continue
                processed_handles.add(handle)

                # Format message
                message = format_message(message_template, contact_name, company_name, handle)

                result = {
                    'instagram_handle': handle,
                    'contact_name': contact_name,
                    'company_name': company_name,
                    'message': message,
                    'status': 'pending',
                    'error': None,
                    'timestamp': datetime.now().isoformat()
                }

                if dry_run:
                    result['status'] = 'dry_run'
                    results.append(result)
                    sent_count += 1
                else:
                    # Send via Apify
                    success, error = send_dm_via_apify(client, handle, message)

                    if success:
                        result['status'] = 'sent'
                        sent_count += 1
                    else:
                        result['status'] = 'error'
                        result['error'] = error
                        error_count += 1

                    results.append(result)

                    # Rate limiting
                    time.sleep(delay)

            pbar.update(1)
            row_count += 1

    return {
        'error': None,
        'total_handles': len(processed_handles),
        'sent': sent_count,
        'skipped': skipped_count,
        'errors': error_count,
        'results': results
    }


def save_results(results: List[Dict], output_dir: Path) -> Path:
    """Save results to CSV."""
    if not results:
        return None

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'apify_dm_results_{timestamp}.csv'
    output_path = output_dir / filename

    df = pd.DataFrame(results)
    df.to_csv(output_path, index=False, encoding='utf-8')

    return output_path


def print_sample_message(results: List[Dict]):
    """Print a sample message for preview."""
    if not results:
        print("\nNo messages to preview.")
        return

    print("\n" + "=" * 60)
    print("SAMPLE MESSAGE PREVIEW")
    print("=" * 60)

    sample = results[0]
    print(f"\nTo: @{sample['instagram_handle']}")
    print(f"Contact: {sample['contact_name'] or 'N/A'}")
    print(f"Company: {sample['company_name'] or 'N/A'}")
    print(f"\nMessage:")
    print("-" * 40)
    print(sample['message'])
    print("-" * 40)
    print("=" * 60 + "\n")


def print_summary(results: Dict):
    """Print summary."""
    print("\n" + "=" * 60)
    print("APIFY DM SENDER SUMMARY")
    print("=" * 60)

    if results.get('error'):
        print(f"ERROR: {results['error']}")
        return

    print(f"Total handles processed: {results['total_handles']}")
    print(f"Messages sent:           {results['sent']}")
    print(f"Errors:                  {results['errors']}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Send Instagram DMs via Apify',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--csv', type=str, required=True,
                       help='Path to CSV with instagram_handles column')
    parser.add_argument('--message', type=str,
                       help='Message template. Variables: {contact_name}, {company_name}, {instagram_handle}')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview messages without sending')
    parser.add_argument('--limit', type=int,
                       help='Limit number of rows to process')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between sends in seconds (default: 2)')
    parser.add_argument('--exclude', type=str, nargs='+',
                       help='Instagram handles to exclude (already messaged)')

    args = parser.parse_args()

    # Validate CSV path
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = BASE_DIR / args.csv

    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)

    # Get message
    message_template = args.message
    if not message_template:
        message_template = input("Enter message template: ").strip()
        if not message_template:
            print("ERROR: Message is required")
            sys.exit(1)

    # Validate Apify token (unless dry run)
    if not args.dry_run and not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not found in environment.")
        print("Add it to your .env file.")
        sys.exit(1)

    print(f"\nProcessing: {csv_path}")
    if args.dry_run:
        print("MODE: DRY RUN (no messages will be sent)\n")

    # Process
    results = process_csv_and_send(
        csv_path=csv_path,
        message_template=message_template,
        dry_run=args.dry_run,
        limit=args.limit,
        delay=args.delay,
        exclude_handles=args.exclude
    )

    # Show sample if dry run
    if args.dry_run and results.get('results'):
        print_sample_message(results['results'])

    # Print summary
    print_summary(results)

    # Save results
    if results.get('results'):
        output_dir = BASE_DIR / 'output'
        output_dir.mkdir(exist_ok=True)
        output_path = save_results(results['results'], output_dir)
        if output_path:
            print(f"Results saved to: {output_path}\n")

    if results.get('error'):
        sys.exit(1)


if __name__ == '__main__':
    main()
