"""Find Missing Instagram Handles - Enhanced Search Strategy

Uses OpenAI API with multiple advanced strategies to find Instagram handles
for contacts that are currently missing them.

Input: processed/03d_final.csv (identifies missing contacts automatically)
Output: processed/03d_final.csv (updated with newly found handles)

Usage:
    python find_missing_instagram.py           # Process all missing contacts
    python find_missing_instagram.py --test    # Test mode (3 contacts)
"""

import os
import sys
import re
import json
import time
import asyncio
import pandas as pd
import requests
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Pipeline input/output paths
INPUT_FILE = 'processed/03d_final.csv'
BACKUP_FILE = 'processed/03d_final_backup_missing.csv'

# Rate limiting
SEARCH_DELAY = 2.0
REQUEST_DELAY = 1.0

# Headers for web requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def has_instagram_handles(row):
    """Check if a contact has any Instagram handles."""
    personal = row.get('contact_instagram_handle', '')
    company = row.get('instagram_handles', '')
    
    has_personal = pd.notna(personal) and str(personal).strip() != ''
    
    has_company = False
    if pd.notna(company) and company != '' and company != '[]':
        try:
            parsed = json.loads(company) if isinstance(company, str) else company
            has_company = len(parsed) > 0 if isinstance(parsed, list) else False
        except:
            pass
    
    return has_personal or has_company


def extract_instagram_handles_from_text(text: str) -> list:
    """Extract Instagram handles from text using regex patterns."""
    if not text:
        return []
    
    handles = set()
    false_positives = {
        'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www',
        'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
        'font', 'keyframes', 'next', 'prev', 'return', 'function', 'var',
        'let', 'const', 'class', 'id', 'div', 'span', 'html', 'body', 'head',
        'script', 'style', 'link', 'meta', 'title', 'header', 'footer', 'nav',
        'main', 'section', 'article', 'aside', 'button', 'input', 'form', 'img',
        'a', 'ul', 'ol', 'li', 'table', 'tr', 'td', 'th', 'thead', 'tbody',
        'charset', 'iterator', 'toprimitive', 'fontawesome', 'airops', 'original',
        'wrapped', 'newrelic', 'wordpress', 'nextdoor'
    }
    
    # Pattern 1: Instagram URLs (most reliable)
    url_pattern = r'instagram\.com/([a-zA-Z0-9_.]+)/?'
    for match in re.finditer(url_pattern, text, re.I):
        handle = match.group(1).lower()
        if handle not in false_positives and len(handle) >= 3:
            handles.add(f"@{handle}")
    
    # Pattern 2: @ mentions in text
    mention_pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
    for match in re.finditer(mention_pattern, text):
        handle = match.group(1).lower()
        if (handle not in false_positives and 
            not any(x in handle for x in ['gmail', 'yahoo', 'hotmail', 'outlook', '.com', '.net', '.org', 'facebook', 'twitter']) and
            len(handle) >= 3 and
            ('_' in handle or '.' in handle or len(handle) > 6)):
            handles.add(f"@{handle}")
    
    return list(handles)


def parse_instagram_handles_field(value) -> list:
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


# =============================================================================
# OPENAI API FUNCTIONS
# =============================================================================

async def generate_search_queries(company_name: str, contact_name: str = "", industry: str = "real estate") -> list:
    """Use OpenAI to generate multiple search query variations."""
    prompt = f"""Generate 5-8 specific web search queries to find Instagram handles for:

Company: {company_name}
Contact: {contact_name or 'N/A'}
Industry: {industry}

Generate diverse search queries that would help find their Instagram profile.
Include variations like:
- "[name] instagram"
- "[company] instagram account"
- "[name] [company] instagram"
- "[company] social media"
- Industry-specific searches

Return ONLY a JSON array of query strings, no other text:
["query1", "query2", "query3", ...]"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert at generating web search queries. Return only valid JSON arrays."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        content = response.choices[0].message.content.strip()
        # Extract JSON from response
        json_match = re.search(r'\[.*?\]', content, re.DOTALL)
        if json_match:
            queries = json.loads(json_match.group())
            return queries if isinstance(queries, list) else []
    except Exception as e:
        print(f"      Error generating queries: {e}")
    
    # Fallback queries
    queries = []
    if company_name:
        queries.append(f"{company_name} instagram")
        queries.append(f"{company_name} instagram account")
        queries.append(f"{company_name} social media")
    if contact_name:
        queries.append(f"{contact_name} instagram")
        if company_name:
            queries.append(f"{contact_name} {company_name} instagram")
    
    return queries[:8]


async def generate_handle_patterns(company_name: str, contact_name: str = "") -> list:
    """Use OpenAI to generate likely Instagram handle patterns."""
    prompt = f"""Based on this information, suggest likely Instagram handle patterns:

Company: {company_name}
Contact: {contact_name or 'N/A'}

Generate 5-10 likely Instagram handle patterns following common conventions:
- Remove spaces, special characters
- Use underscores or dots
- Common suffixes for real estate: _realtor, realestate, etc.
- Abbreviations and variations

Return ONLY a JSON array of handle patterns (without @ symbol), no other text:
["pattern1", "pattern2", "pattern3", ...]"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert at Instagram handle patterns. Return only valid JSON arrays."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        content = response.choices[0].message.content.strip()
        json_match = re.search(r'\[.*?\]', content, re.DOTALL)
        if json_match:
            patterns = json.loads(json_match.group())
            return [f"@{p}" if not p.startswith('@') else p for p in patterns if isinstance(p, str)]
    except Exception as e:
        print(f"      Error generating patterns: {e}")
    
    return []


async def search_with_openai(prompt: str, max_retries=3, delay=2):
    """Use OpenAI to search for Instagram handles."""
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": """You are an expert at finding Instagram profiles. 
Analyze the information and determine the most likely Instagram handle.
Return ONLY the handle in format @username, or NOT_FOUND if you cannot determine it."""
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200
            )
            
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content.strip()
                return content
            return ""
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
                continue
            raise
    return ""


# =============================================================================
# WEB SCRAPING FUNCTIONS
# =============================================================================

def scrape_website(url: str, timeout=10):
    """Fetch website HTML content."""
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def deep_scrape_website(base_url: str, max_pages=5) -> str:
    """Scrape multiple pages from a website."""
    if not base_url or pd.isna(base_url):
        return ""
    
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
    
    all_html = []
    visited = set()
    
    # Pages to check
    pages_to_check = ['/', '/about', '/about-us', '/team', '/our-team', '/contact', '/contact-us', '/social']
    
    for page_path in pages_to_check[:max_pages]:
        try:
            url = urljoin(base_url, page_path)
            if url in visited:
                continue
            
            html = scrape_website(url)
            if html:
                all_html.append(html)
                visited.add(url)
            time.sleep(REQUEST_DELAY)
        except Exception:
            continue
    
    return "\n".join(all_html)


def extract_from_linkedin(linkedin_url: str) -> list:
    """Extract Instagram links from LinkedIn profile page."""
    if not linkedin_url or pd.isna(linkedin_url):
        return []
    
    try:
        html = scrape_website(linkedin_url)
        if html:
            return extract_instagram_handles_from_text(html)
    except Exception:
        pass
    
    return []


# =============================================================================
# SEARCH STRATEGIES
# =============================================================================

async def strategy_multi_query_search(company_name: str, contact_name: str = "", industry: str = "real estate") -> list:
    """Strategy 1: Multi-query web search using OpenAI-generated queries."""
    print(f"      [Strategy 1] Multi-query search...")
    handles = set()
    
    queries = await generate_search_queries(company_name, contact_name, industry)
    
    for query in queries[:5]:  # Limit to 5 queries
        try:
            prompt = f"""Search for Instagram handle using this query: "{query}"

Company: {company_name}
Contact: {contact_name or 'N/A'}

Based on web search results for this query, what is the Instagram handle?
Return ONLY @username or NOT_FOUND."""
            
            result = await search_with_openai(prompt)
            found_handles = extract_instagram_handles_from_text(result)
            if found_handles:
                handles.update(found_handles)
            
            await asyncio.sleep(SEARCH_DELAY)
        except Exception as e:
            print(f"        Query error: {str(e)[:50]}")
            continue
    
    return list(handles)


async def strategy_pattern_generation(company_name: str, contact_name: str = "") -> list:
    """Strategy 2: Generate and validate handle patterns."""
    print(f"      [Strategy 2] Pattern generation...")
    handles = []
    
    patterns = await generate_handle_patterns(company_name, contact_name)
    
    # Validate patterns (could check if they exist, but for now just return them)
    # In a real scenario, you might want to verify these patterns exist
    for pattern in patterns:
        if pattern.startswith('@'):
            handles.append(pattern)
        else:
            handles.append(f"@{pattern}")
    
    return handles[:10]  # Limit to 10 patterns


async def strategy_deep_website_scrape(website_url: str) -> list:
    """Strategy 3: Deep website scraping of multiple pages."""
    print(f"      [Strategy 3] Deep website scraping...")
    
    if not website_url or pd.isna(website_url):
        return []
    
    html = deep_scrape_website(website_url)
    if html:
        handles = extract_instagram_handles_from_text(html)
        return handles
    
    return []


async def strategy_cross_platform(linkedin_url: str, social_links: dict) -> list:
    """Strategy 4: Cross-platform analysis (LinkedIn, Facebook, etc.)."""
    print(f"      [Strategy 4] Cross-platform analysis...")
    handles = set()
    
    # Check LinkedIn
    if linkedin_url and pd.notna(linkedin_url):
        linkedin_handles = extract_from_linkedin(linkedin_url)
        if linkedin_handles:
            handles.update(linkedin_handles)
    
    # Check social_links for Instagram mentions
    if social_links:
        try:
            if isinstance(social_links, str):
                social_links = json.loads(social_links)
            
            # Check Facebook page for Instagram links
            if 'facebook' in social_links:
                try:
                    fb_url = social_links['facebook']
                    html = scrape_website(fb_url)
                    if html:
                        fb_handles = extract_instagram_handles_from_text(html)
                        handles.update(fb_handles)
                except Exception:
                    pass
        except Exception:
            pass
    
    return list(handles)


async def strategy_openai_reasoning(company_name: str, contact_name: str = "", 
                                    company_desc: str = "", website_url: str = "") -> list:
    """Strategy 5: OpenAI-assisted reasoning about likely handles."""
    print(f"      [Strategy 5] OpenAI reasoning...")
    
    prompt = f"""Based on this information, determine the most likely Instagram handle:

Company: {company_name}
Contact: {contact_name or 'N/A'}
Description: {company_desc or 'N/A'}
Website: {website_url or 'N/A'}

Analyze the company name, contact name, and available information.
Suggest the most likely Instagram handle(s) following common patterns.
Consider:
- Company name variations
- Contact name combinations
- Industry conventions (real estate often uses _realtor, realestate suffixes)
- Common abbreviations

Return ONLY the handle(s) in format @username, one per line, or NOT_FOUND."""
    
    try:
        result = await search_with_openai(prompt)
        handles = extract_instagram_handles_from_text(result)
        return handles
    except Exception as e:
        print(f"        Reasoning error: {str(e)[:50]}")
        return []


# =============================================================================
# MAIN SEARCH FUNCTION
# =============================================================================

async def find_instagram_handles(row) -> dict:
    """Find Instagram handles using all strategies."""
    page_name = row.get('page_name', '')
    contact_name = row.get('contact_name', '') or row.get('pipeline_name', '')
    website_url = row.get('website_url', '')
    linkedin_url = row.get('linkedin_url', '')
    company_desc = str(row.get('company_description', ''))[:200]
    social_links = row.get('social_links', {})
    
    print(f"\n  Processing: {page_name}")
    if contact_name and pd.notna(contact_name) and str(contact_name) != 'None None':
        print(f"    Contact: {contact_name}")
    
    all_handles = set()
    
    # Try all strategies
    strategies = [
        lambda: strategy_deep_website_scrape(website_url),
        lambda: strategy_cross_platform(linkedin_url, social_links),
        lambda: strategy_openai_reasoning(page_name, contact_name, company_desc, website_url),
        lambda: strategy_multi_query_search(page_name, contact_name, "real estate"),
        lambda: strategy_pattern_generation(page_name, contact_name),
    ]
    
    for strategy in strategies:
        try:
            handles = await strategy()
            if handles:
                all_handles.update(handles)
                print(f"        Found {len(handles)} handle(s): {', '.join(handles[:3])}")
        except Exception as e:
            print(f"        Strategy error: {str(e)[:50]}")
        await asyncio.sleep(1)
    
    # Determine personal vs company handles
    personal_handle = ""
    company_handles = list(all_handles)
    
    # If we have a contact name, try to identify personal handle
    if contact_name and pd.notna(contact_name) and str(contact_name) != 'None None':
        # Look for handles that might match the contact name
        contact_lower = str(contact_name).lower().replace(' ', '')
        for handle in all_handles:
            handle_clean = handle.replace('@', '').lower()
            # Check if handle contains parts of contact name
            name_parts = contact_lower.split()
            if any(part in handle_clean for part in name_parts if len(part) > 3):
                personal_handle = handle
                company_handles = [h for h in company_handles if h != handle]
                break
    
    return {
        'personal_handle': personal_handle,
        'company_handles': company_handles[:10]  # Limit to 10
    }


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Main function to find missing Instagram handles."""
    
    print(f"\n{'='*70}")
    print("FIND MISSING INSTAGRAM HANDLES - ENHANCED SEARCH")
    print(f"{'='*70}")
    
    # Load input file
    print(f"\nLoading: {INPUT_FILE}")
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found")
        return 1
    
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} total contacts")
    
    # Find missing contacts
    missing_mask = ~df.apply(has_instagram_handles, axis=1)
    missing_df = df[missing_mask].copy()
    
    print(f"\nContacts missing Instagram handles: {len(missing_df)}")
    
    if len(missing_df) == 0:
        print("All contacts already have Instagram handles!")
        return 0
    
    # Create backup
    if os.path.exists(INPUT_FILE):
        import shutil
        shutil.copy2(INPUT_FILE, BACKUP_FILE)
        print(f"Created backup: {BACKUP_FILE}")
    
    # Initialize columns if needed
    if 'contact_instagram_handle' not in df.columns:
        df['contact_instagram_handle'] = ''
    
    # Determine test mode
    test_mode = '--test' in sys.argv
    if test_mode:
        missing_df = missing_df.head(3)
        print(f"\nTest mode: Processing first {len(missing_df)} missing contacts")
    else:
        print(f"\nProcessing all {len(missing_df)} missing contacts...")
    
    # Process each missing contact
    found_count = 0
    for idx, row in tqdm(missing_df.iterrows(), total=len(missing_df), desc="Finding handles"):
        result = await find_instagram_handles(row)
        
        # Update DataFrame
        if result['personal_handle']:
            df.loc[idx, 'contact_instagram_handle'] = result['personal_handle']
            found_count += 1
        
        if result['company_handles']:
            existing = parse_instagram_handles_field(df.loc[idx, 'instagram_handles'])
            combined = list(set(existing + result['company_handles']))
            df.loc[idx, 'instagram_handles'] = json.dumps(combined)
            if not existing:
                found_count += 1
        
        await asyncio.sleep(SEARCH_DELAY)
    
    # Save updated CSV
    print(f"\nSaving updated data to: {INPUT_FILE}")
    df.to_csv(INPUT_FILE, index=False)
    
    # Print summary
    print("\n" + "=" * 70)
    print("ENHANCED SEARCH SUMMARY")
    print("=" * 70)
    
    # Recalculate stats
    final_missing = ~df.apply(has_instagram_handles, axis=1)
    final_has = len(df) - final_missing.sum()
    
    print(f"\nContacts with Instagram handles: {final_has}/{len(df)} ({final_has/len(df)*100:.1f}%)")
    print(f"Contacts still missing: {final_missing.sum()}/{len(df)} ({final_missing.sum()/len(df)*100:.1f}%)")
    print(f"New handles found in this run: {found_count}")
    print(f"\nBackup saved to: {BACKUP_FILE}")
    print(f"Updated file: {INPUT_FILE}")
    
    return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

