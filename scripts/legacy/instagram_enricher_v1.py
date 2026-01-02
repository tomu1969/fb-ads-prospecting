"""Instagram Handle Enricher - Find personal and company Instagram handles for contacts.

Module 3.7 in the FB Ads Library Pipeline (optional).
Enriches contacts with Instagram handles by searching for:
1. Personal Instagram handles for contact persons
2. Company Instagram handles for missing entries

Input: processed/03d_final.csv
Output: processed/03d_final.csv (updated with Instagram handles)

Usage:
    python instagram_enricher.py           # Test mode (3 contacts)
    python instagram_enricher.py --all     # Process all contacts
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

load_dotenv()

# Initialize OpenAI client
client = OpenAI()

# OpenAI Agents SDK imports (fallback)
try:
    from agents import Agent, Runner, WebSearchTool
    HAS_AGENTS_SDK = True
except ImportError:
    HAS_AGENTS_SDK = False
    print("Warning: agents SDK not available, using OpenAI API directly")

# Pipeline input/output paths
INPUT_FILE = 'processed/03d_final.csv'
BACKUP_FILE = 'processed/03d_final_backup.csv'

# Rate limiting
SEARCH_DELAY = 2.0  # Seconds between searches


# =============================================================================
# INSTAGRAM HANDLE EXTRACTION
# =============================================================================

def extract_instagram_handles_from_text(text: str) -> list:
    """Extract Instagram handles from text using regex patterns."""
    if not text:
        return []
    
    handles = set()
    
    # Common false positives to filter out
    false_positives = {
        'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www',
        'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
        'font', 'keyframes', 'next', 'prev', 'return', 'function', 'var',
        'let', 'const', 'class', 'id', 'div', 'span', 'html', 'body', 'head',
        'script', 'style', 'link', 'meta', 'title', 'header', 'footer', 'nav',
        'main', 'section', 'article', 'aside', 'button', 'input', 'form', 'img',
        'a', 'ul', 'ol', 'li', 'table', 'tr', 'td', 'th', 'thead', 'tbody'
    }
    
    # Pattern 1: Instagram URLs (most reliable)
    url_pattern = r'instagram\.com/([a-zA-Z0-9_.]+)/?'
    for match in re.finditer(url_pattern, text, re.I):
        handle = match.group(1).lower()
        # Filter out common non-handle patterns
        if handle not in false_positives and len(handle) >= 3:
            handles.add(f"@{handle}")
    
    # Pattern 2: @ mentions in text (only if they look like real handles)
    mention_pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
    for match in re.finditer(mention_pattern, text):
        handle = match.group(1).lower()
        # Filter out common non-handle patterns
        if (handle not in false_positives and 
            not any(x in handle for x in ['gmail', 'yahoo', 'hotmail', 'outlook', '.com', '.net', '.org', 'facebook', 'twitter']) and
            len(handle) >= 3 and
            # Prefer handles that look like usernames (not single words that are too generic)
            ('_' in handle or '.' in handle or len(handle) > 5)):
            handles.add(f"@{handle}")
    
    return list(handles)


def parse_instagram_handles_field(value) -> list:
    """Parse instagram_handles field from CSV (can be JSON string or empty)."""
    if pd.isna(value) or value == '' or value == '[]':
        return []
    
    try:
        # Try parsing as JSON string
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    
    return []


# =============================================================================
# AGENT SETUP
# =============================================================================

INSTAGRAM_SEARCH_AGENT_INSTRUCTIONS = """You are an expert at finding Instagram profiles for people and businesses using web search.

## YOUR GOAL
Find Instagram handles by searching the web and extracting from search results.

## SEARCH STRATEGY
1. Use web_search tool to search for "[name/company] instagram"
2. Look for Instagram profile links in search results (instagram.com/username)
3. Extract the Instagram handle (username after instagram.com/)
4. Also look for @ mentions in search result text
5. Verify the handle belongs to the person/company (check context)

## IMPORTANT
- Search multiple variations if needed
- Look for official profiles first
- Filter out generic Instagram pages (explore, p, etc.)
- Return only handles that clearly match the person/company

## OUTPUT FORMAT
Return ONLY the Instagram handle(s) you find, one per line, in format:
@username1
@username2

If no Instagram handle is found, return: NOT_FOUND

Be specific - only return handles that clearly belong to the person/company being searched.
"""

# Initialize agent only if SDK is available
if HAS_AGENTS_SDK:
    instagram_search_agent = Agent(
        name="InstagramSearcher",
        model="gpt-4o",
        instructions=INSTAGRAM_SEARCH_AGENT_INSTRUCTIONS,
        tools=[WebSearchTool()],
    )
else:
    instagram_search_agent = None


# =============================================================================
# SEARCH FUNCTIONS WITH RETRY LOGIC
# =============================================================================

async def search_with_openai_direct(prompt: str, max_retries=3, delay=2):
    """Use OpenAI API directly to find Instagram handles."""
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": """You are an expert at finding Instagram profiles. 
Analyze the information provided and determine the most likely Instagram handle.
Return ONLY the handle in format @username, or NOT_FOUND if you cannot determine it.
Be specific and only return handles that clearly match the person/company."""
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=300
            )
            
            # Extract content from response
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content
                return content.strip() if content else ""
            
            return ""
        except Exception as e:
            error_str = str(e).lower()
            if '503' in error_str or 'server error' in error_str or 'rate limit' in error_str:
                if attempt < max_retries - 1:
                    wait_time = delay * (attempt + 1)
                    print(f"      Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
            raise
    return ""


async def search_with_retry(agent, prompt, max_retries=3, delay=2):
    """Run agent with retry logic for handling 503 errors."""
    # Try direct OpenAI API first if agents SDK has issues
    if not HAS_AGENTS_SDK:
        return await search_with_openai_direct(prompt, max_retries, delay)
    
    for attempt in range(max_retries):
        try:
            result = await Runner.run(agent, prompt)
            return result.final_output.strip()
        except Exception as e:
            error_str = str(e).lower()
            if '503' in error_str or 'server error' in error_str or 'retry' in error_str or 'max retries' in error_str:
                if attempt < max_retries - 1:
                    wait_time = delay * (attempt + 1)
                    print(f"      Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Fallback to direct OpenAI API
                    print(f"      Falling back to direct OpenAI API...")
                    return await search_with_openai_direct(prompt, max_retries=2, delay=1)
            # If it's not a retryable error or we've exhausted retries, raise
            raise
    return ""


async def search_personal_instagram(contact_name: str, company_name: str = "", position: str = "") -> str:
    """Search for personal Instagram handle for a contact person."""
    if not contact_name or contact_name.strip() == "":
        return ""
    
    # Build multiple search queries for better results
    queries = []
    queries.append(f"{contact_name} instagram")
    if company_name:
        queries.append(f"{contact_name} {company_name} instagram")
    if position:
        position_keywords = [w for w in position.split() if w.lower() not in ['the', 'a', 'an', 'at', 'in', 'of', 'for']]
        if position_keywords:
            queries.append(f"{contact_name} {position_keywords[0]} instagram")
    
    try:
        prompt = f"""Find the Instagram handle for this person:

NAME: {contact_name}
COMPANY: {company_name or 'Unknown'}
POSITION: {position or 'Unknown'}

Search the web for their Instagram profile. Look for:
- Instagram profile links (instagram.com/username)
- @ mentions in search results
- Social media profiles

Return ONLY the Instagram handle in format @username.
If you find multiple possible handles, return the most likely one for this person.
If no handle is found, return: NOT_FOUND"""
        
        if HAS_AGENTS_SDK and instagram_search_agent:
            output = await search_with_retry(instagram_search_agent, prompt)
        else:
            output = await search_with_openai_direct(prompt)
        
        if not output or "NOT_FOUND" in output:
            return ""
        
        # Extract handle from output
        handles = extract_instagram_handles_from_text(output)
        if handles:
            return handles[0]  # Return first match
        
        # Try to extract from the full output text
        if "@" in output:
            # Look for @username pattern
            match = re.search(r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})', output)
            if match:
                return f"@{match.group(1).lower()}"
        
        return ""
    except Exception as e:
        print(f"      Error searching for {contact_name}: {e}")
        return ""


async def scrape_website_for_instagram(url: str) -> list:
    """Scrape website directly for Instagram handles."""
    if not url:
        return []
    
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if response.status_code != 200:
            return []
        
        html = response.text
        handles = extract_instagram_handles_from_text(html)
        return handles
    except Exception:
        return []


async def search_company_instagram(company_name: str, website_url: str = "") -> list:
    """Search for company Instagram handles."""
    if not company_name:
        return []
    
    try:
        prompt = f"""Find the Instagram handle(s) for this company:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

Search the web for their official Instagram profile(s). Look for:
- Instagram profile links (instagram.com/username)
- @ mentions in search results
- Social media links on their website
- Business directory listings

Return all Instagram handles you find, one per line in format @username.
If multiple handles exist (e.g., main account, location-specific), return all of them.
If no handles are found, return: NOT_FOUND"""
        
        if HAS_AGENTS_SDK and instagram_search_agent:
            output = await search_with_retry(instagram_search_agent, prompt)
        else:
            output = await search_with_openai_direct(prompt)
        
        if not output or "NOT_FOUND" in output:
            return []
        
        # Extract handles from output
        handles = extract_instagram_handles_from_text(output)
        
        # Also search the output text directly
        if not handles and "@" in output:
            matches = re.findall(r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})', output)
            handles = [f"@{m.lower()}" for m in matches]
        
        # Filter out common false positives
        filtered_handles = []
        false_positives = {
            'instagram', 'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www',
            'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
            'font', 'keyframes', 'next', 'prev', 'wordpress', 'return', 'function',
            'var', 'let', 'const', 'class', 'id', 'div', 'span', 'html', 'body'
        }
        
        for handle in handles:
            handle_clean = handle.replace('@', '').lower()
            # Only keep handles that:
            # 1. Are not in false positives list
            # 2. Are at least 3 characters
            # 3. Either have underscore/dot (likely real username) OR are longer than 6 chars
            if (handle_clean not in false_positives and 
                len(handle_clean) >= 3 and
                ('_' in handle_clean or '.' in handle_clean or len(handle_clean) > 6)):
                filtered_handles.append(handle)
        
        return list(set(filtered_handles))[:10]  # Deduplicate and limit to 10
    except Exception as e:
        print(f"      Error searching for {company_name}: {e}")
        return []


# =============================================================================
# MAIN PROCESSING
# =============================================================================

async def enrich_instagram_handles(df: pd.DataFrame, run_all: bool = False) -> pd.DataFrame:
    """Enrich DataFrame with Instagram handles."""
    
    # Create backup
    if os.path.exists(INPUT_FILE):
        import shutil
        shutil.copy2(INPUT_FILE, BACKUP_FILE)
        print(f"Created backup: {BACKUP_FILE}")
    
    # Initialize new columns if they don't exist
    if 'contact_instagram_handle' not in df.columns:
        df['contact_instagram_handle'] = ''
    
    # Determine which rows to process
    if run_all:
        rows_to_process = df
        indices_to_process = df.index.tolist()
        print(f"\nProcessing all {len(rows_to_process)} contacts...")
    else:
        rows_to_process = df.head(3)
        indices_to_process = rows_to_process.index.tolist()
        print(f"\nTest mode: Processing first {len(rows_to_process)} contacts...")
        print("(Use --all to process all contacts)")
    
    # Process each row
    for pos, (idx, row) in enumerate(tqdm(rows_to_process.iterrows(), total=len(rows_to_process), desc="Enriching Instagram handles")):
        page_name = row.get('page_name', '')
        contact_name = row.get('contact_name', '') or row.get('pipeline_name', '')
        contact_position = row.get('contact_position', '') or row.get('pipeline_position', '')
        website_url = row.get('website_url', '')
        existing_handles = parse_instagram_handles_field(row.get('instagram_handles', ''))
        
        print(f"\n  [{pos + 1}/{len(rows_to_process)}] {page_name}")
        
        # 1. Search for personal Instagram handle
        if contact_name and pd.notna(contact_name) and str(contact_name).strip() and str(contact_name).strip() != 'None None':
            current_personal = df.loc[idx, 'contact_instagram_handle']
            if pd.isna(current_personal) or str(current_personal).strip() == '':
                print(f"    Searching personal Instagram for: {contact_name}")
                try:
                    personal_handle = await search_personal_instagram(str(contact_name), page_name, str(contact_position) if contact_position else '')
                    if personal_handle:
                        df.loc[idx, 'contact_instagram_handle'] = personal_handle
                        print(f"    ✓ Found: {personal_handle}")
                    else:
                        print(f"    ✗ Not found")
                except Exception as e:
                    print(f"    ✗ Error: {str(e)[:100]}")
                await asyncio.sleep(SEARCH_DELAY)
        
        # 2. Search for company Instagram handles if missing
        if not existing_handles or len(existing_handles) == 0:
            print(f"    Searching company Instagram for: {page_name}")
            company_handles = []
            
            # First try: scrape website directly
            if website_url and pd.notna(website_url) and str(website_url).strip():
                print(f"      Checking website: {website_url}")
                website_handles = await scrape_website_for_instagram(str(website_url))
                if website_handles:
                    company_handles.extend(website_handles)
                    print(f"      Found {len(website_handles)} handle(s) on website")
            
            # Second try: use AI search if website didn't yield results
            if not company_handles:
                try:
                    ai_handles = await search_company_instagram(str(page_name), str(website_url) if website_url and pd.notna(website_url) else '')
                    if ai_handles:
                        company_handles.extend(ai_handles)
                except Exception as e:
                    print(f"      AI search error: {str(e)[:50]}")
            
            if company_handles:
                # Update instagram_handles column
                df.loc[idx, 'instagram_handles'] = json.dumps(list(set(company_handles))[:10])
                print(f"    ✓ Found {len(set(company_handles))} handle(s): {', '.join(set(company_handles))}")
            else:
                print(f"    ✗ Not found")
            await asyncio.sleep(SEARCH_DELAY)
        else:
            print(f"    Company handles already exist: {len(existing_handles)} handle(s)")
    
    return df


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Main function to run Instagram handle enrichment."""
    
    print(f"\n{'='*60}")
    print("INSTAGRAM HANDLE ENRICHER")
    print(f"{'='*60}")
    
    # Load input file
    print(f"\nLoading: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found")
        print("Make sure the pipeline has run and generated the final CSV.")
        return 1
    
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} contacts")
    
    # Check for existing Instagram data
    has_personal = 'contact_instagram_handle' in df.columns
    has_company = 'instagram_handles' in df.columns
    
    if has_personal:
        existing_personal = df['contact_instagram_handle'].notna().sum()
        print(f"Existing personal handles: {existing_personal}/{len(df)}")
    
    if has_company:
        existing_company = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).gt(0).sum()
        print(f"Existing company handles: {existing_company}/{len(df)}")
    
    # Determine run mode
    run_all = '--all' in sys.argv
    
    # Process contacts
    enriched_df = await enrich_instagram_handles(df, run_all=run_all)
    
    # Save updated CSV
    print(f"\nSaving updated data to: {INPUT_FILE}")
    enriched_df.to_csv(INPUT_FILE, index=False)
    
    # Print summary
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    
    # Personal handles summary
    if 'contact_instagram_handle' in enriched_df.columns:
        personal_found = enriched_df['contact_instagram_handle'].notna().sum()
        personal_total = enriched_df['contact_name'].notna().sum() + enriched_df['pipeline_name'].notna().sum()
        print(f"\nPersonal Instagram handles:")
        print(f"  Found: {personal_found}/{len(enriched_df)}")
        if personal_total > 0:
            print(f"  Coverage: {personal_found/personal_total*100:.1f}% of contacts with names")
    
    # Company handles summary
    if 'instagram_handles' in enriched_df.columns:
        company_handles_count = enriched_df['instagram_handles'].apply(parse_instagram_handles_field).apply(len)
        company_found = (company_handles_count > 0).sum()
        print(f"\nCompany Instagram handles:")
        print(f"  Found: {company_found}/{len(enriched_df)}")
        print(f"  Total handles: {company_handles_count.sum()}")
    
    print(f"\nBackup saved to: {BACKUP_FILE}")
    print(f"Updated file: {INPUT_FILE}")
    
    return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

