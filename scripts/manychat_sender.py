"""
ManyChat Instagram Bulk Messaging Script

Sends bulk messages to Instagram contacts via ManyChat API by reading Instagram handles
from CSV files, finding subscribers, and sending customized messages.

Usage:
    python scripts/manychat_sender.py --csv output/hubspot_contacts.csv --message "Hi {contact_name}!..."
    python scripts/manychat_sender.py --csv output/hubspot_contacts.csv --dry-run
"""

import os
import sys
import time
import re
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).parent.parent
MANYCHAT_API_KEY = os.getenv('MANYCHAT_API_KEY')
MANYCHAT_BASE_URL = 'https://api.manychat.com/fb'


class ManyChatClient:
    """Client for interacting with ManyChat API."""
    
    def __init__(self, api_key: str, delay: float = 0.1):
        """
        Initialize ManyChat API client.
        
        Args:
            api_key: ManyChat API key
            delay: Delay in seconds between API requests (default 0.1 = 10 req/sec)
        """
        self.api_key = api_key
        self.delay = delay
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        if not api_key:
            raise ValueError("MANYCHAT_API_KEY not found in environment variables")
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                     json_data: Optional[Dict] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Make an API request with rate limiting and error handling.
        
        Returns:
            Tuple of (response_data, error_message)
        """
        url = f"{MANYCHAT_BASE_URL}{endpoint}"
        
        try:
            time.sleep(self.delay)  # Rate limiting
            
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=json_data,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json(), None
            elif response.status_code == 401:
                return None, "Authentication failed - check API key"
            elif response.status_code == 429:
                return None, "Rate limit exceeded - try increasing delay"
            else:
                error_msg = f"API error {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f": {error_data['message']}"
                except:
                    error_msg += f": {response.text[:200]}"
                return None, error_msg
                
        except requests.exceptions.Timeout:
            return None, "Request timeout"
        except requests.exceptions.RequestException as e:
            return None, f"Request error: {str(e)}"
        except Exception as e:
            return None, f"Unexpected error: {str(e)}"
    
    def find_subscriber_by_name(self, name: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Find subscriber by name.

        Args:
            name: Contact's full name

        Returns:
            Tuple of (subscriber_id, error_message). subscriber_id is None if not found.
        """
        if not name or not name.strip():
            return None, "Name is required"

        # Clean name for search
        name = name.strip()

        data, error = self._make_request('GET', '/subscriber/findByName', params={'name': name})
        
        if error:
            return None, error
        
        if not data:
            return None, "Invalid API response"
        
        if data.get('status') != 'success':
            return None, "Subscriber not found"
        
        subscribers = data.get('data', [])
        if not subscribers or len(subscribers) == 0:
            return None, "Subscriber not found"
        
        # If multiple results, use the first one
        # In future, could add more sophisticated matching
        subscriber = subscribers[0]
        subscriber_id = subscriber.get('id')
        
        if not subscriber_id:
            return None, "Subscriber ID not found in response"
        
        return subscriber_id, None

    def find_subscriber_by_email(self, email: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Find subscriber by email address.

        Args:
            email: Contact's email address

        Returns:
            Tuple of (subscriber_id, error_message). subscriber_id is None if not found.
        """
        if not email or not email.strip():
            return None, "Email is required"

        email = email.strip().lower()

        data, error = self._make_request('GET', '/subscriber/findBySystemField',
                                         params={'email': email})

        if error:
            return None, error

        if not data:
            return None, "Invalid API response"

        if data.get('status') != 'success':
            return None, "Subscriber not found"

        subscribers = data.get('data', [])
        if not subscribers or len(subscribers) == 0:
            return None, "Subscriber not found"

        subscriber = subscribers[0]
        subscriber_id = subscriber.get('id')

        if not subscriber_id:
            return None, "Subscriber ID not found in response"

        return subscriber_id, None

    def find_subscriber_by_phone(self, phone: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Find subscriber by phone number.

        Args:
            phone: Contact's phone number (with country code)

        Returns:
            Tuple of (subscriber_id, error_message). subscriber_id is None if not found.
        """
        if not phone or not phone.strip():
            return None, "Phone is required"

        # Clean phone - keep + and digits only
        phone = ''.join(c for c in phone.strip() if c.isdigit() or c == '+')
        if not phone:
            return None, "Invalid phone number"

        data, error = self._make_request('GET', '/subscriber/findBySystemField',
                                         params={'phone': phone})

        if error:
            return None, error

        if not data:
            return None, "Invalid API response"

        if data.get('status') != 'success':
            return None, "Subscriber not found"

        subscribers = data.get('data', [])
        if not subscribers or len(subscribers) == 0:
            return None, "Subscriber not found"

        subscriber = subscribers[0]
        subscriber_id = subscriber.get('id')

        if not subscriber_id:
            return None, "Subscriber ID not found in response"

        return subscriber_id, None

    def find_subscriber_by_custom_field(self, field_name: str, field_value: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Find subscriber by custom field value (e.g., Instagram handle stored in custom field).

        Args:
            field_name: Name of the custom field
            field_value: Value to search for

        Returns:
            Tuple of (subscriber_id, error_message). subscriber_id is None if not found.
        """
        if not field_value or not field_value.strip():
            return None, f"{field_name} value is required"

        field_value = field_value.strip()

        data, error = self._make_request('GET', '/subscriber/findByCustomField',
                                         params={'field_name': field_name, 'field_value': field_value})

        if error:
            return None, error

        if not data:
            return None, "Invalid API response"

        if data.get('status') != 'success':
            return None, "Subscriber not found"

        subscribers = data.get('data', [])
        if not subscribers or len(subscribers) == 0:
            return None, "Subscriber not found"

        subscriber = subscribers[0]
        subscriber_id = subscriber.get('id')

        if not subscriber_id:
            return None, "Subscriber ID not found in response"

        return subscriber_id, None

    def send_message(self, subscriber_id: int, message: str, message_tag: str = 'ACCOUNT_UPDATE') -> Tuple[bool, Optional[str]]:
        """
        Send a message to a subscriber via Instagram.

        Args:
            subscriber_id: ManyChat subscriber ID
            message: Message text to send
            message_tag: Message tag for compliance (default: ACCOUNT_UPDATE)

        Returns:
            Tuple of (success, error_message)
        """
        # Instagram-specific payload format
        payload = {
            'subscriber_id': subscriber_id,
            'data': {
                'version': 'v2',
                'content': {
                    'type': 'instagram',
                    'messages': [
                        {
                            'type': 'text',
                            'text': message
                        }
                    ]
                }
            },
            'message_tag': message_tag
        }

        data, error = self._make_request('POST', '/sending/sendContent', json_data=payload)
        
        if error:
            return False, error
        
        if data and data.get('status') == 'success':
            return True, None
        else:
            return False, "Failed to send message"


def parse_instagram_handles(handles_str: str) -> List[str]:
    """
    Parse comma-separated Instagram handles from CSV column.
    
    Handles formats like:
    - "@handle1, @handle2, @handle3"
    - "@handle1,@handle2"
    - "handle1, handle2"
    
    Returns:
        List of cleaned handles (without @ prefix)
    """
    if pd.isna(handles_str) or not handles_str:
        return []
    
    handles_str = str(handles_str).strip()
    if not handles_str:
        return []
    
    # Split by comma
    handles = [h.strip() for h in handles_str.split(',')]
    
    # Clean each handle: remove @, remove whitespace
    cleaned_handles = []
    for handle in handles:
        handle = handle.strip()
        if not handle:
            continue
        # Remove @ if present
        if handle.startswith('@'):
            handle = handle[1:]
        # Validate handle format (alphanumeric, underscores, periods)
        if re.match(r'^[a-zA-Z0-9._]+$', handle):
            cleaned_handles.append(handle.lower())
    
    return cleaned_handles


def get_contact_name(row: pd.Series) -> str:
    """Extract contact FIRST name only from row, trying multiple column names."""
    # Try different name column combinations
    if 'firstname' in row.index:
        first = str(row.get('firstname', '')).strip()
        if first and first.lower() not in ['none', 'nan', 'null', 'n/a', 'none none', '']:
            return first
    
    # For full names, extract only the first name
    if 'matched_name' in row.index:
        name = str(row.get('matched_name', '')).strip()
        if name and name.lower() not in ['none', 'nan', 'null', 'n/a', 'none none', '']:
            # Split and return only first name
            first_name = name.split()[0] if name.split() else name
            return first_name
    
    if 'contact_name' in row.index:
        name = str(row.get('contact_name', '')).strip()
        if name and name.lower() not in ['none', 'nan', 'null', 'n/a', 'none none', '']:
            # Split and return only first name
            first_name = name.split()[0] if name.split() else name
            return first_name
    
    return ''


def get_company_name(row: pd.Series) -> str:
    """Extract company name from row, trying multiple column names."""
    if 'company' in row.index:
        company = str(row.get('company', '')).strip()
        if company and company.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return company

    if 'page_name' in row.index:
        company = str(row.get('page_name', '')).strip()
        if company and company.lower() not in ['none', 'nan', 'null', 'n/a', '']:
            return company

    return ''


def get_email(row: pd.Series) -> str:
    """Extract email from row, trying multiple column names."""
    email_cols = ['primary_email', 'email', 'Email', 'EMAIL', 'contact_email', 'work_email', 'emails']
    for col in email_cols:
        if col in row.index:
            email = str(row.get(col, '')).strip()
            # Handle list-like strings: ['email@example.com']
            if email.startswith('[') and email.endswith(']'):
                try:
                    import ast
                    email_list = ast.literal_eval(email)
                    if email_list and len(email_list) > 0:
                        email = email_list[0]
                except:
                    pass
            if email and email.lower() not in ['none', 'nan', 'null', 'n/a', '', '[]'] and '@' in email:
                return email.lower()
    return ''


def get_phone(row: pd.Series) -> str:
    """Extract phone from row, trying multiple column names."""
    phone_cols = ['phone', 'Phone', 'PHONE', 'phone_number', 'mobile', 'cell']
    for col in phone_cols:
        if col in row.index:
            phone = str(row.get(col, '')).strip()
            if phone and phone.lower() not in ['none', 'nan', 'null', 'n/a', '']:
                # Keep only digits and +
                cleaned = ''.join(c for c in phone if c.isdigit() or c == '+')
                if len(cleaned) >= 7:  # Minimum valid phone length
                    return cleaned
    return ''


def format_message(template: str, contact_name: str, company_name: str, instagram_handle: str) -> str:
    """
    Format message template with variables.
    
    Supported variables:
    - {contact_name}
    - {company_name}
    - {instagram_handle} - replaced with just the handle (no @ prefix)
    
    Args:
        template: Message template string
        contact_name: Contact's name
        company_name: Company name
        instagram_handle: Instagram handle (without @)
        
    Returns:
        Formatted message string
    """
    message = template
    message = message.replace('{contact_name}', contact_name or 'there')
    message = message.replace('{company_name}', company_name or 'your company')
    # Replace with just the handle - template can include @ if needed (e.g., @{instagram_handle})
    message = message.replace('{instagram_handle}', instagram_handle if instagram_handle else '')
    return message


def process_csv_and_send(client: ManyChatClient, csv_path: Path, message_template: str, 
                        message_tag: str, dry_run: bool = False) -> Dict:
    """
    Process CSV file and send messages via ManyChat.
    
    Returns:
        Dictionary with processing results
    """
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        return {
            'error': f"Failed to read CSV file: {str(e)}",
            'total_handles': 0,
            'sent': 0,
            'skipped': 0,
            'errors': 0,
            'results': []
        }
    
    # Check if instagram_handles column exists
    if 'instagram_handles' not in df.columns:
        return {
            'error': "CSV file does not contain 'instagram_handles' column",
            'total_handles': 0,
            'sent': 0,
            'skipped': 0,
            'errors': 0,
            'results': []
        }
    
    # Track processed handles to avoid duplicates
    processed_handles = set()
    results = []
    sent_count = 0
    skipped_count = 0
    error_count = 0
    
    # Process each row
    total_rows = len(df)
    
    with tqdm(total=total_rows, desc="Processing rows", unit="row") as pbar:
        for idx, row in df.iterrows():
            # Parse Instagram handles
            handles_str = row.get('instagram_handles', '')
            handles = parse_instagram_handles(handles_str)
            
            if not handles:
                pbar.update(1)
                continue
            
            # Get contact info
            contact_name = get_contact_name(row)
            company_name = get_company_name(row)
            email = get_email(row)
            phone = get_phone(row)

            # Process each handle
            for handle in handles:
                # Skip if already processed
                if handle in processed_handles:
                    continue
                processed_handles.add(handle)

                # Format message
                message = format_message(message_template, contact_name, company_name, handle)

                if dry_run:
                    # Dry run: just log what would be sent
                    results.append({
                        'instagram_handle': handle,
                        'contact_name': contact_name,
                        'company_name': company_name,
                        'email': email,
                        'phone': phone,
                        'message': message,
                        'status': 'dry_run',
                        'subscriber_id': None,
                        'lookup_method': None,
                        'error_message': None
                    })
                    sent_count += 1
                else:
                    # Try to find subscriber: email > phone > name (in order of reliability)
                    subscriber_id = None
                    find_error = None
                    lookup_method = None

                    # 1. Try email first (most reliable)
                    if email:
                        subscriber_id, find_error = client.find_subscriber_by_email(email)
                        if subscriber_id:
                            lookup_method = 'email'

                    # 2. Try phone if email didn't work
                    if not subscriber_id and phone:
                        subscriber_id, find_error = client.find_subscriber_by_phone(phone)
                        if subscriber_id:
                            lookup_method = 'phone'

                    # 3. Try name as fallback
                    if not subscriber_id and contact_name:
                        subscriber_id, find_error = client.find_subscriber_by_name(contact_name)
                        if subscriber_id:
                            lookup_method = 'name'

                    # Build error message if not found
                    if not subscriber_id:
                        if not email and not phone and not contact_name:
                            find_error = "No email, phone, or name available for lookup"
                        elif not find_error:
                            find_error = "Subscriber not found"

                    if find_error or not subscriber_id:
                        # Subscriber not found - skip
                        results.append({
                            'instagram_handle': handle,
                            'contact_name': contact_name,
                            'company_name': company_name,
                            'email': email,
                            'phone': phone,
                            'message': message,
                            'status': 'skipped',
                            'subscriber_id': None,
                            'lookup_method': None,
                            'error_message': find_error or "Subscriber not found"
                        })
                        skipped_count += 1
                    else:
                        # Send message
                        success, send_error = client.send_message(subscriber_id, message, message_tag)

                        if success:
                            results.append({
                                'instagram_handle': handle,
                                'contact_name': contact_name,
                                'company_name': company_name,
                                'email': email,
                                'phone': phone,
                                'message': message,
                                'status': 'sent',
                                'subscriber_id': subscriber_id,
                                'lookup_method': lookup_method,
                                'error_message': None
                            })
                            sent_count += 1
                        else:
                            results.append({
                                'instagram_handle': handle,
                                'contact_name': contact_name,
                                'company_name': company_name,
                                'email': email,
                                'phone': phone,
                                'message': message,
                                'status': 'error',
                                'subscriber_id': subscriber_id,
                                'lookup_method': lookup_method,
                                'error_message': send_error
                            })
                            error_count += 1
            
            pbar.update(1)
    
    return {
        'error': None,
        'total_handles': len(processed_handles),
        'sent': sent_count,
        'skipped': skipped_count,
        'errors': error_count,
        'results': results
    }


def save_results_csv(results: List[Dict], output_dir: Path) -> Path:
    """Save processing results to CSV file."""
    if not results:
        return None
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'manychat_sent_{timestamp}.csv'
    output_path = output_dir / filename
    
    df = pd.DataFrame(results)
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    return output_path


def print_summary(results: Dict):
    """Print summary report of processing results."""
    print("\n" + "=" * 60)
    print("MANYCHAT MESSAGING SUMMARY")
    print("=" * 60)
    
    if results.get('error'):
        print(f"ERROR: {results['error']}")
        print("=" * 60 + "\n")
        return
    
    print(f"Total unique handles processed: {results['total_handles']}")
    print(f"Messages sent successfully:      {results['sent']}")
    print(f"Handles skipped (not found):     {results['skipped']}")
    print(f"Errors encountered:              {results['errors']}")
    print("=" * 60)
    
    if results['errors'] > 0:
        print("\nError details:")
        error_results = [r for r in results['results'] if r['status'] == 'error']
        for r in error_results[:10]:  # Show first 10 errors
            print(f"  - @{r['instagram_handle']}: {r['error_message']}")
        if len(error_results) > 10:
            print(f"  ... and {len(error_results) - 10} more errors")
    
    if results['skipped'] > 0:
        print("\nSkipped handles (subscriber not found):")
        skipped_results = [r for r in results['results'] if r['status'] == 'skipped']
        for r in skipped_results[:10]:  # Show first 10 skipped
            print(f"  - @{r['instagram_handle']} ({r['contact_name'] or 'No name'})")
        if len(skipped_results) > 10:
            print(f"  ... and {len(skipped_results) - 10} more skipped")
    
    print("=" * 60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Send bulk Instagram messages via ManyChat API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send messages with custom template
  python scripts/manychat_sender.py --csv output/hubspot_contacts.csv \\
      --message "Hi {contact_name}! We noticed {company_name} on Instagram (@{instagram_handle}). Would love to connect!"
  
  # Dry run to test without sending
  python scripts/manychat_sender.py --csv output/hubspot_contacts.csv \\
      --message "Test message" --dry-run
  
  # Custom message tag and delay
  python scripts/manychat_sender.py --csv output/hubspot_contacts.csv \\
      --message "Hello {contact_name}!" --message-tag POST_PURCHASE_UPDATE --delay 0.2
        """
    )
    
    parser.add_argument('--csv', type=str, required=True,
                       help='Path to CSV file containing Instagram handles')
    parser.add_argument('--message', type=str,
                       help='Message template (use {contact_name}, {company_name}, {instagram_handle} as variables). If not provided, will prompt.')
    parser.add_argument('--message-tag', type=str, default='ACCOUNT_UPDATE',
                       help='ManyChat message tag (default: ACCOUNT_UPDATE)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay in seconds between API calls (default: 0.1)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Test mode - validate CSV and template without sending messages')
    
    args = parser.parse_args()
    
    # Validate API key
    if not MANYCHAT_API_KEY:
        print("ERROR: MANYCHAT_API_KEY not found in environment variables.")
        print("Please add MANYCHAT_API_KEY to your .env file.")
        sys.exit(1)
    
    # Validate CSV path
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = BASE_DIR / args.csv
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)
    
    # Get message template
    message_template = args.message
    if not message_template:
        message_template = input("Enter message template (use {contact_name}, {company_name}, {instagram_handle}): ").strip()
        if not message_template:
            print("ERROR: Message template is required")
            sys.exit(1)
    
    # Validate template has at least one variable
    if '{contact_name}' not in message_template and '{company_name}' not in message_template and '{instagram_handle}' not in message_template:
        print("WARNING: Message template doesn't contain any variables.")
        print("Consider using {contact_name}, {company_name}, or {instagram_handle}")
        confirm = input("Continue anyway? (y/n): ").strip().lower()
        if confirm != 'y':
            sys.exit(0)
    
    # Initialize client
    try:
        client = ManyChatClient(MANYCHAT_API_KEY, delay=args.delay)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    
    # Process and send
    print(f"\nProcessing CSV: {csv_path}")
    if args.dry_run:
        print("DRY RUN MODE - No messages will be sent\n")
    
    results = process_csv_and_send(
        client=client,
        csv_path=csv_path,
        message_template=message_template,
        message_tag=args.message_tag,
        dry_run=args.dry_run
    )
    
    # Print summary
    print_summary(results)
    
    # Save results to CSV
    if results.get('results'):
        output_dir = BASE_DIR / 'output'
        output_dir.mkdir(exist_ok=True)
        results_path = save_results_csv(results['results'], output_dir)
        if results_path:
            print(f"Results saved to: {results_path}\n")
    
    # Exit with error code if there were errors
    if results.get('error'):
        sys.exit(1)


if __name__ == '__main__':
    main()

