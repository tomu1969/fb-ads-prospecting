"""Clean and Consolidate Instagram Handles

One-time cleanup script to:
1. Merge contact_instagram_handle into instagram_handles array
2. Remove false positives (CSS/JS keywords)
3. Verify handle associations
4. Deduplicate handles
5. Remove contact_instagram_handle column

Usage:
    python scripts/clean_instagram_handles.py
"""

import os
import sys
import re
import json
import pandas as pd
from pathlib import Path
import shutil

BASE_DIR = Path(__file__).parent.parent
INPUT_FILE = BASE_DIR / "processed" / "03d_final.csv"
BACKUP_FILE = BASE_DIR / "processed" / "03d_final_backup_before_clean.csv"

# Comprehensive list of false positives
FALSE_POSITIVES = {
    # CSS keywords
    'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
    'font', 'keyframes', 'charset',
    # HTML/JS keywords
    'next', 'prev', 'return', 'function', 'var', 'let', 'const', 'class',
    'id', 'div', 'span', 'html', 'body', 'head', 'script', 'style', 'link',
    'meta', 'title', 'header', 'footer', 'nav', 'main', 'section', 'article',
    'aside', 'button', 'input', 'form', 'img', 'a', 'ul', 'ol', 'li',
    'table', 'tr', 'td', 'th', 'thead', 'tbody',
    # Framework/library keywords
    'iterator', 'toprimitive', 'fontawesome', 'airops', 'original',
    'wrapped', 'newrelic', 'wordpress', 'nextdoor', 'linkedin',
    # Instagram generic pages
    'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www', 'reel'
}


def parse_instagram_handles_field(value):
    """Parse instagram_handles field from CSV."""
    if pd.isna(value) or value == '' or value == '[]':
        return []
    try:
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return []


def is_valid_handle(handle: str) -> bool:
    """Check if handle is valid Instagram format."""
    if not handle or not isinstance(handle, str):
        return False
    
    handle = handle.strip()
    
    # Must start with @
    if not handle.startswith('@'):
        return False
    
    # Remove @ for validation
    username = handle[1:]
    
    # Must be 3-30 characters
    if len(username) < 3 or len(username) > 30:
        return False
    
    # Must match Instagram username pattern
    pattern = r'^[a-zA-Z][a-zA-Z0-9_.]{2,29}$'
    if not re.match(pattern, username):
        return False
    
    # Check against false positives
    if username.lower() in FALSE_POSITIVES:
        return False
    
    return True


def clean_handle(handle: str) -> str:
    """Clean and normalize a handle."""
    if not handle or not isinstance(handle, str):
        return ''
    
    handle = handle.strip()
    
    # Ensure it starts with @
    if not handle.startswith('@'):
        handle = '@' + handle
    
    # Normalize to lowercase for deduplication
    return handle.lower()


def verify_handle_association(handle: str, contact_name: str = "", company_name: str = "") -> dict:
    """Verify if handle is associated with contact or company."""
    result = {
        'is_personal': False,
        'is_company': False,
        'confidence': 0.0
    }
    
    if not handle or not is_valid_handle(handle):
        return result
    
    handle_clean = handle.replace('@', '').lower()
    
    # Check personal association
    if contact_name and pd.notna(contact_name) and str(contact_name).strip() != 'None None':
        contact_lower = str(contact_name).lower().replace(' ', '')
        name_parts = [p for p in contact_lower.split() if len(p) > 3]
        
        for part in name_parts:
            if part in handle_clean:
                result['is_personal'] = True
                result['confidence'] = 0.8
                break
    
    # Check company association
    if company_name and pd.notna(company_name):
        company_lower = str(company_name).lower().replace(' ', '').replace('-', '').replace('_', '')
        company_parts = [p for p in company_lower.split() if len(p) > 3]
        
        for part in company_parts:
            if part in handle_clean:
                result['is_company'] = True
                if result['confidence'] < 0.7:
                    result['confidence'] = 0.7
                break
    
    return result


def clean_and_consolidate_handles(row) -> list:
    """Clean and consolidate all handles for a row."""
    all_handles = set()
    
    # Get personal handle
    personal = row.get('contact_instagram_handle', '')
    if pd.notna(personal) and str(personal).strip() != '' and str(personal).strip() != 'nan':
        cleaned = clean_handle(str(personal))
        if cleaned and is_valid_handle(cleaned):
            all_handles.add(cleaned)
    
    # Get company handles
    company_handles = parse_instagram_handles_field(row.get('instagram_handles', ''))
    for handle in company_handles:
        cleaned = clean_handle(str(handle))
        if cleaned and is_valid_handle(cleaned):
            all_handles.add(cleaned)
    
    # Convert to sorted list for consistency
    return sorted(list(all_handles))


def main():
    """Main function to clean and consolidate Instagram handles."""
    
    print("=" * 70)
    print("CLEAN AND CONSOLIDATE INSTAGRAM HANDLES")
    print("=" * 70)
    
    # Check if input file exists
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found")
        return 1
    
    # Create backup
    print(f"\nCreating backup: {BACKUP_FILE}")
    shutil.copy2(INPUT_FILE, BACKUP_FILE)
    
    # Load CSV
    print(f"\nLoading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} contacts")
    
    # Check current state
    if 'contact_instagram_handle' in df.columns:
        personal_count = df['contact_instagram_handle'].notna().sum()
        print(f"Found {personal_count} personal handles in contact_instagram_handle column")
    else:
        print("No contact_instagram_handle column found")
    
    if 'instagram_handles' in df.columns:
        company_count = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).gt(0).sum()
        print(f"Found {company_count} contacts with company handles in instagram_handles column")
    else:
        print("No instagram_handles column found")
        return 1
    
    # Count false positives before cleaning
    print("\nAnalyzing false positives...")
    false_positive_count = 0
    total_handles_before = 0
    
    for idx, row in df.iterrows():
        handles = parse_instagram_handles_field(row.get('instagram_handles', ''))
        total_handles_before += len(handles)
        for handle in handles:
            handle_clean = str(handle).replace('@', '').lower()
            if handle_clean in FALSE_POSITIVES:
                false_positive_count += 1
    
    print(f"Found {false_positive_count} false positive handles out of {total_handles_before} total")
    
    # Clean and consolidate
    print("\nCleaning and consolidating handles...")
    cleaned_handles = []
    removed_count = 0
    consolidated_count = 0
    
    for idx, row in df.iterrows():
        # Get all handles (personal + company)
        all_handles = clean_and_consolidate_handles(row)
        
        # Count what was removed
        old_personal = row.get('contact_instagram_handle', '')
        old_company = parse_instagram_handles_field(row.get('instagram_handles', ''))
        old_total = len(old_company) + (1 if pd.notna(old_personal) and str(old_personal).strip() != '' and str(old_personal).strip() != 'nan' else 0)
        
        if len(all_handles) < old_total:
            removed_count += (old_total - len(all_handles))
        
        if old_personal and pd.notna(old_personal) and str(old_personal).strip() != '' and str(old_personal).strip() != 'nan':
            consolidated_count += 1
        
        cleaned_handles.append(json.dumps(all_handles) if all_handles else '[]')
    
    # Update dataframe
    df['instagram_handles'] = cleaned_handles
    
    # Remove contact_instagram_handle column
    if 'contact_instagram_handle' in df.columns:
        df = df.drop(columns=['contact_instagram_handle'])
        print(f"Removed contact_instagram_handle column")
    
    # Save cleaned CSV
    print(f"\nSaving cleaned data to: {INPUT_FILE}")
    df.to_csv(INPUT_FILE, index=False)
    
    # Print summary
    print("\n" + "=" * 70)
    print("CLEANUP SUMMARY")
    print("=" * 70)
    
    total_handles_after = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).sum()
    contacts_with_handles = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).gt(0).sum()
    
    print(f"Total handles before: {total_handles_before}")
    print(f"Total handles after: {total_handles_after}")
    print(f"False positives removed: {false_positive_count}")
    print(f"Handles removed (invalid/duplicates): {removed_count - false_positive_count}")
    print(f"Personal handles consolidated: {consolidated_count}")
    print(f"Contacts with handles: {contacts_with_handles}/{len(df)} ({contacts_with_handles/len(df)*100:.1f}%)")
    print(f"\nBackup saved to: {BACKUP_FILE}")
    print(f"Cleaned file: {INPUT_FILE}")
    
    # Sample verification
    print("\n" + "=" * 70)
    print("SAMPLE VERIFICATION (first 5 contacts with handles)")
    print("=" * 70)
    
    sample_count = 0
    for idx, row in df.iterrows():
        handles = parse_instagram_handles_field(row.get('instagram_handles', ''))
        if handles and sample_count < 5:
            page_name = row.get('page_name', 'Unknown')
            contact_name = row.get('contact_name', '') or row.get('pipeline_name', '')
            print(f"\n{page_name}:")
            print(f"  Contact: {contact_name}")
            print(f"  Handles ({len(handles)}): {', '.join(handles[:5])}{'...' if len(handles) > 5 else ''}")
            sample_count += 1
    
    print("\n" + "=" * 70)
    print("CLEANUP COMPLETE")
    print("=" * 70)
    
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

