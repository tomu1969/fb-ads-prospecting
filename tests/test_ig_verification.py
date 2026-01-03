#!/usr/bin/env python3
"""Simple test script to verify Instagram handles exist."""

import pandas as pd
import requests
import time
from pathlib import Path
from bs4 import BeautifulSoup

# Headers to avoid blocking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def verify_handle(handle: str) -> dict:
    """Verify if Instagram handle exists by checking both status code and page content."""
    # Remove @ if present
    username = handle.replace('@', '').strip()
    if not username:
        return {'exists': False, 'error': 'Empty username'}
    
    url = f"https://www.instagram.com/{username}/"
    
    try:
        # Use GET request to check page content
        response = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        
        # Check status code first
        if response.status_code != 200:
            return {
                'exists': False,
                'status_code': response.status_code,
                'error': None
            }
        
        # Instagram returns 200 even for unavailable profiles, so check page title
        # Valid profiles have descriptive titles like "Name (@username) • Instagram photos and videos"
        # Invalid/unavailable profiles just have "Instagram" as the title
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            return {'exists': False, 'status_code': response.status_code, 'error': f'Parse error: {str(e)[:50]}'}
        
        # Check page title (most reliable indicator when not logged in)
        page_title_tag = soup.find('title')
        page_title = page_title_tag.text.strip() if page_title_tag else ''
        
        # Profile exists if title is not just "Instagram" and has substantial content
        exists = page_title != 'Instagram' and len(page_title) > 15 and '•' in page_title
        
        return {
            'exists': exists,
            'status_code': response.status_code,
            'error': None,
            'page_title': page_title
        }
    except requests.Timeout:
        return {'exists': False, 'status_code': None, 'error': 'Timeout'}
    except requests.RequestException as e:
        return {'exists': False, 'status_code': None, 'error': str(e)[:50]}
    except Exception as e:
        return {'exists': False, 'status_code': None, 'error': f'Unexpected error: {str(e)[:50]}'}


def main():
    # Read the CSV file
    csv_path = Path(__file__).parent.parent / 'output' / '20260102_211543_hubspot_leads_prospects_final.csv'
    
    if not csv_path.exists():
        print(f"ERROR: File not found: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    
    print("=" * 60)
    print("Instagram Handle Verification Test")
    print("=" * 60)
    print()
    
    total_handles = 0
    verified_exists = 0
    not_found = 0
    errors = 0
    
    # Process each row
    for idx, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')
        handles_str = row.get('instagram_handles', '')
        
        # Skip if no handles
        if pd.isna(handles_str) or not handles_str or handles_str.strip() == '':
            continue
        
        # Parse comma-separated handles
        handles = [h.strip() for h in str(handles_str).split(',') if h.strip()]
        
        if not handles:
            continue
        
        print(f"\n{page_name}:")
        print(f"  Handles: {', '.join(handles)}")
        
        # Verify each handle
        for handle in handles:
            total_handles += 1
            result = verify_handle(handle)
            
            if result['exists']:
                title_preview = result.get('page_title', '')[:60] if result.get('page_title') else 'N/A'
                print(f"    {handle}: ✓ EXISTS (status: {result['status_code']})")
                print(f"      Title: {title_preview}")
                verified_exists += 1
            elif result['error']:
                print(f"    {handle}: ✗ ERROR - {result['error']}")
                errors += 1
            else:
                print(f"    {handle}: ✗ NOT FOUND (status: {result.get('status_code', 'unknown')})")
                not_found += 1
            
            # Rate limiting - wait between requests
            time.sleep(1.5)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total handles tested: {total_handles}")
    print(f"Verified exists: {verified_exists}")
    print(f"Not found: {not_found}")
    print(f"Errors: {errors}")
    print("=" * 60)


if __name__ == '__main__':
    main()

