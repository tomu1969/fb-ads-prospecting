"""
Module 2: Company Enricher
Finds company websites via DuckDuckGo search.

Input: processed/01_loaded.csv
Output: processed/02_enriched.csv (adds: website_url, search_confidence, linkedin_url)
"""

import os
import time
import re
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from ddgs import DDGS  # Use newer ddgs package
from tqdm import tqdm

# Import run ID utilities
import sys
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink

# Manual override file path - create this CSV to override DuckDuckGo results
# Format: page_name,website_url
OVERRIDE_FILE = Path(__file__).parent.parent / "config" / "website_overrides.csv"

EXCLUDED_DOMAINS = [
    'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
    'yelp.com', 'yellowpages.com', 'bbb.org', 'manta.com',
    'mapquest.com', 'chamberofcommerce.com', 'crunchbase.com',
    # Q&A and unrelated sites
    'answers.com', 'quora.com', 'reddit.com', 'zhihu.com',
    'wikipedia.org', 'wikihow.com', 'youtube.com', 'tiktok.com',
    # Generic directories
    'zillow.com', 'realtor.com', 'redfin.com', 'trulia.com',
    'homes.com', 'apartments.com', 'apartmentlist.com',
    # Other noise
    'amazon.com', 'ebay.com', 'pinterest.com', 'medium.com',
    # Tracking/redirect domains
    'bing.com', 'duckduckgo.com',
]

RATE_LIMIT_SECONDS = 0.5  # Reduced from 2s for faster processing
REQUEST_TIMEOUT = 5
MAX_SEARCH_RESULTS = 10

# Load manual overrides at module load
_overrides = {}
if OVERRIDE_FILE.exists():
    try:
        override_df = pd.read_csv(OVERRIDE_FILE)
        for _, row in override_df.iterrows():
            _overrides[row['page_name'].strip().lower()] = row['website_url'].strip()
        print(f"Loaded {len(_overrides)} website overrides from {OVERRIDE_FILE}")
    except Exception as e:
        print(f"Warning: Could not load overrides: {e}")

# Cache for search results (avoid re-searching same companies)
_search_cache = {}


def search_company(page_name: str) -> dict:
    """Search DuckDuckGo for company website and LinkedIn."""
    result = {'website_url': '', 'search_confidence': 0.0, 'linkedin_url': ''}

    # Check cache first
    cache_key = page_name.strip().lower()
    if cache_key in _search_cache:
        return _search_cache[cache_key].copy()

    # Check for manual override first
    override_key = page_name.strip().lower()
    if override_key in _overrides:
        result['website_url'] = _overrides[override_key]
        result['search_confidence'] = 1.0
        _search_cache[cache_key] = result.copy()
        return result

    # Extract key words for more targeted queries
    words = page_name.split()
    short_name = ' '.join(words[:3]) if len(words) > 3 else page_name

    # Try multiple query strategies and combine all results
    queries = [
        f'"{page_name}"',  # Exact match
        f'"{short_name}" official website',  # Shorter exact match
        f'{page_name} official site',  # With "official site"
        f'{short_name} real estate app',  # With context
        f'{page_name} .com',  # Try to find .com sites
    ]

    try:
        all_results = []
        with DDGS() as ddgs:
            for query in queries:
                try:
                    search_results = list(ddgs.text(query, region='us-en', max_results=5))
                    all_results.extend(search_results)
                    time.sleep(0.1)  # Reduced delay between queries
                except Exception:
                    continue

        if not all_results:
            return result

        website_url, confidence = extract_website_url(all_results, page_name)
        linkedin_url = extract_linkedin_url(all_results)

        if website_url and validate_url(website_url):
            result['website_url'] = website_url
            result['search_confidence'] = confidence

        result['linkedin_url'] = linkedin_url

    except Exception as e:
        print(f"Search error for '{page_name}': {e}")

    # Cache result
    _search_cache[cache_key] = result.copy()
    return result


def extract_website_url(results: list, page_name: str) -> tuple:
    """Extract best website URL from search results, prioritizing domain matches."""
    # Clean page name for matching
    page_name_clean = re.sub(r'[^\w\s]', '', page_name.lower())
    page_name_words = set(page_name_clean.split())

    # Also create a compact version (e.g., "10X Evolution" -> "10xevolution")
    page_name_compact = re.sub(r'[^a-z0-9]', '', page_name.lower())

    candidates = []

    for result in results:
        url = result.get('href', '')
        if not url:
            continue

        # Parse domain
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
        except Exception:
            continue

        # Skip excluded domains
        if any(excluded in domain for excluded in EXCLUDED_DOMAINS):
            continue

        # Skip LinkedIn (captured separately)
        if 'linkedin.com' in domain:
            continue

        # Calculate confidence based on domain matching page name
        domain_clean = domain.replace('www.', '').split('.')[0]
        domain_compact = re.sub(r'[^a-z0-9]', '', domain_clean)

        # Check for exact or close domain match
        if page_name_compact == domain_compact:
            # Exact match
            confidence = 0.99
        elif page_name_compact.startswith(domain_compact):
            # Page name starts with domain - prefer longer domain matches
            # e.g., "nexthomegulfcoast" (16 chars) scores higher than "nexthome" (8 chars)
            match_ratio = len(domain_compact) / len(page_name_compact)
            confidence = 0.90 + (0.08 * match_ratio)  # Range: 0.90-0.98
        elif domain_compact.startswith(page_name_compact[:min(5, len(page_name_compact))]):
            # Domain starts with beginning of page name
            confidence = 0.85
        elif domain_compact in page_name_compact:
            # Domain is contained in page name - less confident
            # Shorter matches at the start are better
            match_ratio = len(domain_compact) / len(page_name_compact)
            confidence = 0.7 + (0.2 * match_ratio)
        elif page_name_compact in domain_compact:
            confidence = 0.9
        else:
            # Check word overlap
            domain_words = set(re.split(r'[\W_]+', domain_clean))
            matching_words = page_name_words & domain_words

            if matching_words:
                confidence = min(0.8, 0.5 + len(matching_words) * 0.15)
            else:
                confidence = 0.3

        candidates.append((url, confidence, domain))

    # Sort by confidence and return best match
    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        best = candidates[0]
        return best[0], best[1]

    return '', 0.0


def extract_linkedin_url(results: list) -> str:
    """Extract LinkedIn company page URL from search results."""
    for result in results:
        url = result.get('href', '')
        if 'linkedin.com/company' in url:
            return url
    return ''


def validate_url(url: str) -> bool:
    """Validate URL is accessible.

    Many sites block HEAD requests (return 403/405), so we:
    1. Try HEAD first
    2. Fall back to GET if HEAD fails
    3. Accept 403 as valid (server exists, just blocking bots)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

    try:
        # Try HEAD first
        response = requests.head(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers=headers
        )
        # Accept 2xx, 3xx, and 403 (blocked but server exists)
        if response.status_code < 400 or response.status_code == 403:
            return True

        # If HEAD failed with 405 (Method Not Allowed), try GET
        if response.status_code == 405:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                headers=headers,
                stream=True  # Don't download full content
            )
            response.close()
            return response.status_code < 400 or response.status_code == 403

        return False
    except Exception:
        return False


def enrich_all(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich all rows in DataFrame with website data."""
    df = df.copy()
    df['website_url'] = ''
    df['search_confidence'] = 0.0
    df['linkedin_url'] = ''

    # Parallel processing with ThreadPoolExecutor
    BATCH_SIZE = 10  # Process 10 companies concurrently

    def process_company(idx):
        """Process a single company and return results."""
        page_name = df.at[idx, 'page_name']
        if not page_name:
            return idx, {'website_url': '', 'search_confidence': 0.0, 'linkedin_url': ''}
        result = search_company(page_name)
        time.sleep(RATE_LIMIT_SECONDS)  # Rate limit per request
        return idx, result

    # Process in parallel
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        futures = {executor.submit(process_company, idx): idx for idx in df.index}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Enriching companies"):
            idx, result = future.result()
            df.at[idx, 'website_url'] = result['website_url']
            df.at[idx, 'search_confidence'] = result['search_confidence']
            df.at[idx, 'linkedin_url'] = result['linkedin_url']

    return df


if __name__ == "__main__":
    import sys
    import os
    import json
    
    # Check if module should run based on enrichment config
    from utils.enrichment_config import should_run_module
    if not should_run_module("enricher"):
        print("=== Enricher Module ===")
        print("⏭️  SKIPPED: Website enrichment not selected in configuration")
        print("   Copying input file to output to maintain pipeline continuity...")
        
        base_path = Path(__file__).parent.parent
        run_id = get_run_id_from_env()
        base_input = "01_loaded.csv"
        base_output = "02_enriched.csv"
        
        if run_id:
            input_name = get_versioned_filename(base_input, run_id)
            output_name = get_versioned_filename(base_output, run_id)
        else:
            input_name = base_input
            output_name = base_output
        
        input_file = base_path / "processed" / input_name
        output_file = base_path / "processed" / output_name
        
        if not input_file.exists():
            latest_input = base_path / "processed" / base_input
            if latest_input.exists() or latest_input.is_symlink():
                input_file = latest_input
        
        if input_file.exists():
            import shutil
            output_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(input_file, output_file)
            if run_id:
                create_latest_symlink(output_file, base_output)
            print(f"✓ Copied {input_file} to {output_file}")
        exit(0)

    run_all = "--all" in sys.argv
    base_path = Path(__file__).parent.parent
    # Get versioned filenames
    run_id = get_run_id_from_env()
    base_input = "01_loaded.csv"
    base_output = "02_enriched.csv"
    
    if run_id:
        input_name = get_versioned_filename(base_input, run_id)
        output_name = get_versioned_filename(base_output, run_id)
    else:
        # Fallback to default names
        input_name = base_input
        output_name = base_output
    
    input_file = base_path / "processed" / input_name
    output_file = base_path / "processed" / output_name
    
    # Also try latest symlink if versioned file doesn't exist
    if not input_file.exists():
        latest_input = base_path / "processed" / base_input
        if latest_input.exists() or latest_input.is_symlink():
            input_file = latest_input

    print(f"=== Enricher Module {'Full Run' if run_all else 'Test'} ===\n")
    print(f"Loading: {input_file}")

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        print("Run loader.py first to create 01_loaded.csv")
        exit(1)

    df = pd.read_csv(input_file, encoding='utf-8')
    print(f"Loaded {len(df)} records")

    df_test = df.copy() if run_all else df.head(3).copy()
    print(f"Processing {len(df_test)} rows...")

    df_enriched = enrich_all(df_test)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df_enriched.to_csv(output_file, index=False, encoding='utf-8')
    
    # Create latest symlink
    if run_id:
        latest_path = create_latest_symlink(output_file, base_output)
        if latest_path:
            print(f"✓ Latest symlink: {latest_path}")
    print(f"Saved: {output_file}")

    # Show results
    print("\nResults:")
    for _, row in df_enriched.iterrows():
        print(f"  {row['page_name']}: {row['website_url']} (conf: {row['search_confidence']:.2f}")
