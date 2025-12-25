"""Module 3.5: Hunter.io Email Enrichment"""

import os
import sys
import time
import pandas as pd
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

API_KEY = os.getenv('HUNTER_API_KEY')
BASE_URL = 'https://api.hunter.io/v2'

def get_domain(url):
    """Extract domain from URL."""
    if pd.isna(url) or not url:
        return None
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    domain = domain.replace('www.', '')
    return domain

def search_domain(domain):
    """Find all emails, contacts, and phone numbers for a domain."""
    if not domain:
        return [], None, None, [], None

    try:
        resp = requests.get(
            f'{BASE_URL}/domain-search',
            params={'domain': domain, 'api_key': API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            emails_data = data.get('emails', [])

            # Extract all emails
            emails = [e['value'] for e in emails_data]

            # Extract phone numbers from contacts (Hunter includes phone_number field)
            hunter_phones = []
            for contact in emails_data:
                phone = contact.get('phone_number')
                if phone:
                    hunter_phones.append(phone)

            # Get best contact (highest confidence)
            best_contact = None
            if emails_data:
                sorted_contacts = sorted(emails_data, key=lambda x: x.get('confidence', 0), reverse=True)
                best = sorted_contacts[0]
                best_contact = {
                    'first_name': best.get('first_name', ''),
                    'last_name': best.get('last_name', ''),
                    'position': best.get('position', ''),
                    'email': best.get('value', ''),
                    'confidence': best.get('confidence', 0),
                    'phone': best.get('phone_number', '')  # Include phone from best contact
                }

            return emails, best_contact, data.get('organization'), hunter_phones, None
        return [], None, None, [], None
    except Exception as e:
        print(f"  Error searching {domain}: {e}")
        return [], None, None, [], str(e)

def verify_email(email):
    """Verify an email address."""
    if not email:
        return None, None

    try:
        resp = requests.get(
            f'{BASE_URL}/email-verifier',
            params={'email': email, 'api_key': API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get('data', {})
            return data.get('status'), data.get('score')
        return None, None
    except:
        return None, None

def enrich_row(row):
    """Enrich a single row with Hunter data."""
    domain = get_domain(row.get('website_url'))

    # Search domain for emails, contacts, and phones
    hunter_emails, best_contact, org, hunter_phones, error = search_domain(domain)

    # Get contact details
    contact_name = ''
    contact_position = ''
    primary_email = ''
    contact_phone = ''
    confidence = None

    if best_contact:
        first = best_contact.get('first_name', '')
        last = best_contact.get('last_name', '')
        contact_name = f"{first} {last}".strip()
        contact_position = best_contact.get('position', '')
        primary_email = best_contact.get('email', '')
        contact_phone = best_contact.get('phone', '')
        confidence = best_contact.get('confidence')

    # Verify primary email
    verified_status = None
    if primary_email:
        verified_status, _ = verify_email(primary_email)

    return {
        'hunter_emails': hunter_emails,
        'hunter_phones': hunter_phones,
        'contact_name': contact_name,
        'contact_position': contact_position,
        'contact_phone': contact_phone,
        'primary_email': primary_email,
        'email_confidence': confidence,
        'email_verified': verified_status
    }

def merge_phones(existing_phones, hunter_phones, contact_phone):
    """Merge phone numbers from different sources, avoiding duplicates."""
    import ast

    # Parse existing phones
    phones = set()
    if existing_phones and str(existing_phones) not in ['[]', 'nan', '']:
        try:
            if isinstance(existing_phones, str):
                parsed = ast.literal_eval(existing_phones)
                phones.update(parsed)
            elif isinstance(existing_phones, list):
                phones.update(existing_phones)
        except (ValueError, SyntaxError):
            pass

    # Add Hunter phones
    for phone in hunter_phones:
        if phone:
            phones.add(phone)

    # Add contact phone
    if contact_phone:
        phones.add(contact_phone)

    return list(phones)


def enrich_all(df):
    """Enrich all rows with Hunter data."""
    results = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Hunter enrichment"):
        enriched = enrich_row(row)
        results.append(enriched)
        time.sleep(1)  # Rate limit

    # Update dataframe with Hunter data
    df['hunter_emails'] = [r['hunter_emails'] for r in results]
    df['contact_name'] = [r['contact_name'] for r in results]
    df['contact_position'] = [r['contact_position'] for r in results]
    df['primary_email'] = [r['primary_email'] for r in results]
    df['email_confidence'] = [r['email_confidence'] for r in results]
    df['email_verified'] = [r['email_verified'] for r in results]

    # Merge phones: existing (from scraper) + Hunter phones
    merged_phones = []
    for idx, row in df.iterrows():
        existing = row.get('phones', [])
        hunter_phones = results[idx]['hunter_phones']
        contact_phone = results[idx]['contact_phone']
        merged = merge_phones(existing, hunter_phones, contact_phone)
        merged_phones.append(merged)

    df['phones'] = merged_phones

    return df


def merge_manual_contacts(df):
    """Merge manually researched contacts from config/manual_contacts.csv."""
    from pathlib import Path

    manual_path = Path(__file__).parent.parent / "config" / "manual_contacts.csv"
    if not manual_path.exists():
        print("No manual contacts file found")
        return df

    manual_df = pd.read_csv(manual_path)
    print(f"Loaded {len(manual_df)} manual contacts from {manual_path.name}")

    # Create lookup by page_name
    manual_lookup = {row['page_name']: row for _, row in manual_df.iterrows()}

    updated = 0
    for idx, row in df.iterrows():
        page_name = row['page_name']
        if page_name in manual_lookup:
            manual = manual_lookup[page_name]

            # Only update if Hunter didn't find data
            if pd.isna(row.get('primary_email')) or not row.get('primary_email'):
                df.at[idx, 'primary_email'] = manual['primary_email']
                df.at[idx, 'email_verified'] = 'manual'

                # Update hunter_emails list
                current = str(row.get('hunter_emails', '[]'))
                if current == '[]' or current == 'nan' or pd.isna(row.get('hunter_emails')):
                    df.at[idx, 'hunter_emails'] = f"['{manual['primary_email']}']"

                updated += 1

            # Always update contact name if manual has one and current is empty
            if manual.get('contact_name') and (pd.isna(row.get('contact_name')) or not row.get('contact_name')):
                df.at[idx, 'contact_name'] = manual['contact_name']
                df.at[idx, 'contact_position'] = manual.get('contact_position', '')

    print(f"Merged {updated} manual contacts")
    return df


if __name__ == '__main__':
    input_path = 'processed/03_contacts.csv'
    output_path = 'processed/03b_hunter.csv'

    print(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} rows")

    # Test mode: first 3 rows
    if '--all' not in sys.argv:
        print("Testing with first 3 rows (use --all for full run)")
        df = df.head(3)

    df = enrich_all(df)

    # Merge manual contacts (from config/manual_contacts.csv)
    df = merge_manual_contacts(df)

    df.to_csv(output_path, index=False)
    print(f"\nSaved: {output_path}")

    # Show results
    print("\nResults:")
    for _, row in df.iterrows():
        print(f"  {row['page_name']}:")
        print(f"    Contact: {row['contact_name']} ({row['contact_position']})")
        print(f"    Email: {row['primary_email']} (verified: {row['email_verified']})")
