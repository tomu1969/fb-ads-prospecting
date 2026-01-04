"""Instagram Handle Enricher - Unified Module 3.7 (OPTIMIZED)

Combines basic and enhanced search strategies to find Instagram handles for contacts.
Runs automatically after Module 3.6 (Agent Enricher) in the pipeline.

OPTIMIZED: Fast mode is now the default (~1 min for 59 contacts with Groq).
Uses: cache check → Apify API → website scrape (2 pages) → Groq LLM reasoning

Input: processed/03d_final.csv (from Module 3.6)
Output: processed/03d_final.csv (updated with Instagram handles)

Usage:
    python instagram_enricher.py           # Test mode (3 contacts, fast mode)
    python instagram_enricher.py --all     # Process all contacts (fast mode)
    python instagram_enricher.py --all --full  # Full comprehensive search (slower)
    python instagram_enricher.py --verify  # Verify handles exist (slower, filters invalid handles)
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

# Import run ID utilities
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink
from utils.redis_cache import get_cached_handles, cache_handles, is_redis_available
from utils.instagram_apis import search_apify_instagram, is_paid_api_enabled

load_dotenv()

# Initialize LLM clients (Groq preferred, OpenAI as fallback)
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Try to initialize Groq client (10-20x faster)
groq_client = None
if GROQ_API_KEY:
    try:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_API_KEY)
    except ImportError:
        print("⚠️  Groq library not installed. Install with: pip install groq")
    except Exception as e:
        print(f"⚠️  Failed to initialize Groq client: {e}")

# Initialize OpenAI client as fallback
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print(f"⚠️  Failed to initialize OpenAI client: {e}")

# Use Groq if available, otherwise fall back to OpenAI
llm_client = groq_client if groq_client else openai_client
llm_provider = "groq" if groq_client else "openai"

# OpenAI Agents SDK imports (fallback)
try:
    from agents import Agent, Runner, WebSearchTool
    HAS_AGENTS_SDK = True
except ImportError:
    HAS_AGENTS_SDK = False

# Pipeline input/output paths
BASE_DIR = Path(__file__).parent.parent
INPUT_FILE = 'processed/03d_final.csv'
BACKUP_FILE = 'processed/03d_final_backup.csv'

# Rate limiting (reduced for Groq's higher rate limits)
SEARCH_DELAY = 0.1 if groq_client else 1.0  # Groq handles rate limiting better

# Caching for website scraping (avoid re-scraping same URLs)
_website_cache = {}
REQUEST_DELAY = 0.2  # Reduced from 1.0 for faster scraping

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
    
    has_personal = pd.notna(personal) and str(personal).strip() != '' and str(personal).strip() != 'nan'
    
    has_company = False
    if pd.notna(company) and company != '' and company != '[]':
        try:
            parsed = json.loads(company) if isinstance(company, str) else company
            has_company = len(parsed) > 0 if isinstance(parsed, list) else False
        except:
            pass
    
    return has_personal or has_company


# Comprehensive list of false positives (same as clean script)
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


def is_valid_handle(handle: str) -> bool:
    """Check if handle is valid Instagram format and not a false positive."""
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


def extract_instagram_handles_from_text(text: str) -> list:
    """Extract Instagram handles from text using regex patterns."""
    if not text:
        return []
    
    handles = set()
    
    # Pattern 1: Instagram URLs (most reliable)
    url_pattern = r'instagram\.com/([a-zA-Z0-9_.]+)/?'
    for match in re.finditer(url_pattern, text, re.I):
        handle = match.group(1).lower()
        handle_with_at = f"@{handle}"
        if is_valid_handle(handle_with_at):
            handles.add(handle_with_at)
    
    # Pattern 2: @ mentions in text
    mention_pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
    for match in re.finditer(mention_pattern, text):
        handle = match.group(1)
        handle_with_at = f"@{handle}"
        if (is_valid_handle(handle_with_at) and 
            not any(x in handle.lower() for x in ['gmail', 'yahoo', 'hotmail', 'outlook', '.com', '.net', '.org', 'facebook', 'twitter']) and
            ('_' in handle or '.' in handle or len(handle) > 6)):
            handles.add(handle_with_at.lower())
    
    return list(handles)


def verify_instagram_handle(handle: str, timeout: int = 10) -> dict:
    """Verify if Instagram handle exists by checking page title.
    
    Note: Instagram's anti-bot measures may serve generic pages to automated requests,
    making verification less reliable. This function works best when Instagram serves
    proper HTML content.
    
    Returns:
        dict with 'exists' (bool), 'status_code' (int), 'error' (str or None), 'page_title' (str)
    """
    # Remove @ if present
    username = handle.replace('@', '').strip()
    if not username:
        return {'exists': False, 'error': 'Empty username', 'status_code': None, 'page_title': ''}
    
    url = f"https://www.instagram.com/{username}/"
    
    try:
        # Use GET request to check page content
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        
        # Check status code first
        if response.status_code != 200:
            return {
                'exists': False,
                'status_code': response.status_code,
                'error': None,
                'page_title': ''
            }
        
        # Instagram returns 200 even for unavailable profiles, so check page title
        # Valid profiles have descriptive titles like "Name (@username) • Instagram photos and videos"
        # Invalid/unavailable profiles just have "Instagram" as the title
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            return {'exists': False, 'status_code': response.status_code, 'error': f'Parse error: {str(e)[:50]}', 'page_title': ''}
        
        # Check page title (most reliable indicator when Instagram serves proper HTML)
        page_title_tag = soup.find('title')
        page_title = page_title_tag.text.strip() if page_title_tag else ''
        
        # Profile exists if title is not just "Instagram" and has substantial content with bullet
        # Note: Instagram's anti-bot may serve generic pages, so this may not always work
        exists = page_title != 'Instagram' and len(page_title) > 15 and '•' in page_title
        
        return {
            'exists': exists,
            'status_code': response.status_code,
            'error': None,
            'page_title': page_title
        }
    except requests.Timeout:
        return {'exists': False, 'status_code': None, 'error': 'Timeout', 'page_title': ''}
    except requests.RequestException as e:
        return {'exists': False, 'status_code': None, 'error': str(e)[:50], 'page_title': ''}
    except Exception as e:
        return {'exists': False, 'status_code': None, 'error': f'Unexpected error: {str(e)[:50]}', 'page_title': ''}


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


def deep_scrape_website(base_url: str, max_pages=2) -> str:
    """Scrape multiple pages from a website.

    OPTIMIZED: Only scrape homepage + about page by default (was 5 pages).
    Instagram handles are typically on the homepage or about page.
    """
    if not base_url or pd.isna(base_url):
        return ""

    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url

    all_html = []
    visited = set()
    # OPTIMIZED: Reduced from 8 pages to 4 most likely pages
    pages_to_check = ['/', '/about', '/contact', '/about-us']

    for page_path in pages_to_check[:max_pages]:
        try:
            url = urljoin(base_url, page_path)
            if url in visited:
                continue
            html = scrape_website(url)
            if html:
                all_html.append(html)
                visited.add(url)
                # OPTIMIZATION: If we found Instagram link on homepage, skip other pages
                if page_path == '/' and 'instagram.com' in html.lower():
                    break
            time.sleep(REQUEST_DELAY)
        except Exception:
            continue

    return "\n".join(all_html)


async def scrape_website_for_instagram(url: str, use_cache: bool = True) -> list:
    """Scrape website directly for Instagram handles (with caching)."""
    if not url:
        return []
    
    # Check cache first if enabled
    if use_cache and is_redis_available():
        cached = get_cached_handles("", url)
        if cached:
            return cached
    
    try:
        html = deep_scrape_website(url)
        if html:
            handles = extract_instagram_handles_from_text(html)
            # Cache results if Redis is available
            if use_cache and is_redis_available() and handles:
                cache_handles("", url, handles)
            return handles
    except Exception:
        pass
    return []


# =============================================================================
# LLM API FUNCTIONS (Groq or OpenAI)
# =============================================================================

async def search_with_llm(prompt: str, max_retries=3, delay=0.2):
    """Use LLM (Groq preferred, OpenAI fallback) to find Instagram handles."""
    if not llm_client:
        return ""
    
    for attempt in range(max_retries):
        try:
            if llm_provider == "groq":
                # Groq API (10-20x faster)
                response = groq_client.chat.completions.create(
                    model="llama-3.3-70b-versatile",  # Fast and accurate (updated from deprecated llama-3.1)
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
                    max_tokens=300
                )
            else:
                # OpenAI API (fallback)
                response = openai_client.chat.completions.create(
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
                    max_tokens=300
                )
            
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content.strip()
                return content
            return ""
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
                continue
            # If Groq fails and OpenAI is available, try OpenAI
            if llm_provider == "groq" and openai_client and attempt == max_retries - 1:
                try:
                    response = openai_client.chat.completions.create(
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
                        max_tokens=300
                    )
                    if response.choices and len(response.choices) > 0:
                        return response.choices[0].message.content.strip()
                except Exception:
                    pass
            raise
    return ""


async def generate_search_queries(company_name: str, contact_name: str = "", industry: str = "real estate") -> list:
    """Use LLM to generate multiple search query variations."""
    if not llm_client:
        # Fallback queries
        queries = []
        if company_name:
            queries.append(f"{company_name} instagram")
            queries.append(f"{company_name} instagram account")
        if contact_name:
            queries.append(f"{contact_name} instagram")
            if company_name:
                queries.append(f"{contact_name} {company_name} instagram")
        return queries[:8]
    
    prompt = f"""Generate 5-8 specific web search queries to find Instagram handles for:

Company: {company_name}
Contact: {contact_name or 'N/A'}
Industry: {industry}

Generate diverse search queries that would help find their Instagram profile.
Return ONLY a JSON array of query strings, no other text:
["query1", "query2", "query3", ...]"""

    try:
        if llm_provider == "groq":
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are an expert at generating web search queries. Return only valid JSON arrays."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=300
            )
        else:
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at generating web search queries. Return only valid JSON arrays."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=300
            )
        
        content = response.choices[0].message.content.strip()
        json_match = re.search(r'\[.*?\]', content, re.DOTALL)
        if json_match:
            queries = json.loads(json_match.group())
            return queries if isinstance(queries, list) else []
    except Exception as e:
        pass
    
    # Fallback queries
    queries = []
    if company_name:
        queries.append(f"{company_name} instagram")
        queries.append(f"{company_name} instagram account")
    if contact_name:
        queries.append(f"{contact_name} instagram")
        if company_name:
            queries.append(f"{contact_name} {company_name} instagram")
    return queries[:8]


async def generate_handle_patterns(company_name: str, contact_name: str = "") -> list:
    """Use LLM to generate likely Instagram handle patterns."""
    if not llm_client:
        return []
    
    prompt = f"""Based on this information, suggest likely Instagram handle patterns:

Company: {company_name}
Contact: {contact_name or 'N/A'}

Generate 5-10 likely Instagram handle patterns following common conventions.
Return ONLY a JSON array of handle patterns (without @ symbol), no other text:
["pattern1", "pattern2", "pattern3", ...]"""

    try:
        if llm_provider == "groq":
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are an expert at Instagram handle patterns. Return only valid JSON arrays."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=300
            )
        else:
            response = openai_client.chat.completions.create(
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
    except Exception:
        pass
    return []


# =============================================================================
# BASIC ENRICHMENT (Strategy 1)
# =============================================================================

async def search_personal_instagram(contact_name: str, company_name: str = "", position: str = "") -> str:
    """Search for personal Instagram handle for a contact person."""
    if not contact_name or contact_name.strip() == "" or str(contact_name).strip() == 'None None':
        return ""
    
    try:
        prompt = f"""Find the Instagram handle for this person:

NAME: {contact_name}
COMPANY: {company_name or 'Unknown'}
POSITION: {position or 'Unknown'}

Search the web for their Instagram profile. Return ONLY the handle in format @username.
If no handle is found, return: NOT_FOUND"""
        
        output = await search_with_llm(prompt)
        
        if not output or "NOT_FOUND" in output:
            return ""
        
        handles = extract_instagram_handles_from_text(output)
        if handles:
            return handles[0]
        
        if "@" in output:
            match = re.search(r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})', output)
            if match:
                return f"@{match.group(1).lower()}"
        
        return ""
    except Exception as e:
        return ""


async def search_company_instagram(company_name: str, website_url: str = "") -> list:
    """Search for company Instagram handles."""
    if not company_name:
        return []
    
    try:
        prompt = f"""Find the Instagram handle(s) for this company:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

Search the web for their official Instagram profile(s). Return all handles in format @username.
If no handles are found, return: NOT_FOUND"""
        
        output = await search_with_llm(prompt)
        
        if not output or "NOT_FOUND" in output:
            return []
        
        handles = extract_instagram_handles_from_text(output)
        
        if not handles and "@" in output:
            matches = re.findall(r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})', output)
            handles = [f"@{m.lower()}" for m in matches]
        
        # Filter false positives
        false_positives = {
            'instagram', 'p', 'explore', 'accounts', 'direct', 'stories', 'reels', 'www',
            'graph', 'context', 'type', 'todo', 'media', 'import', 'supports',
            'font', 'keyframes', 'next', 'prev', 'wordpress', 'charset', 'iterator',
            'toprimitive', 'fontawesome', 'airops', 'original', 'wrapped', 'newrelic', 'nextdoor', 'linkedin'
        }
        
        filtered = []
        for handle in handles:
            handle_clean = handle.replace('@', '').lower()
            if (handle_clean not in false_positives and 
                len(handle_clean) >= 3 and
                ('_' in handle_clean or '.' in handle_clean or len(handle_clean) > 6)):
                filtered.append(handle)
        
        return list(set(filtered))[:10]
    except Exception:
        return []


# =============================================================================
# ENHANCED SEARCH STRATEGIES (for missing handles)
# =============================================================================

async def strategy_multi_query_search(company_name: str, contact_name: str = "", industry: str = "real estate") -> list:
    """Enhanced Strategy 1: Multi-query web search."""
    handles = set()
    queries = await generate_search_queries(company_name, contact_name, industry)
    
    for query in queries[:5]:
        try:
            prompt = f"""Search for Instagram handle using this query: "{query}"

Company: {company_name}
Contact: {contact_name or 'N/A'}

Based on web search results, what is the Instagram handle?
Return ONLY @username or NOT_FOUND."""
            
            result = await search_with_llm(prompt)
            found_handles = extract_instagram_handles_from_text(result)
            if found_handles:
                handles.update(found_handles)
            await asyncio.sleep(SEARCH_DELAY)
        except Exception:
            continue
    
    return list(handles)


async def strategy_pattern_generation(company_name: str, contact_name: str = "") -> list:
    """Enhanced Strategy 2: Generate and validate handle patterns."""
    patterns = await generate_handle_patterns(company_name, contact_name)
    return [p if p.startswith('@') else f"@{p}" for p in patterns[:10]]


async def strategy_deep_website_scrape(website_url: str) -> list:
    """Enhanced Strategy 3: Deep website scraping."""
    if not website_url or pd.isna(website_url):
        return []
    html = deep_scrape_website(website_url)
    if html:
        return extract_instagram_handles_from_text(html)
    return []


async def strategy_cross_platform(linkedin_url: str, social_links: dict) -> list:
    """Enhanced Strategy 4: Cross-platform analysis."""
    handles = set()
    
    if linkedin_url and pd.notna(linkedin_url):
        try:
            html = scrape_website(linkedin_url)
            if html:
                handles.update(extract_instagram_handles_from_text(html))
        except Exception:
            pass
    
    if social_links:
        try:
            if isinstance(social_links, str):
                social_links = json.loads(social_links)
            if 'facebook' in social_links:
                try:
                    html = scrape_website(social_links['facebook'])
                    if html:
                        handles.update(extract_instagram_handles_from_text(html))
                except Exception:
                    pass
        except Exception:
            pass
    
    return list(handles)


async def strategy_openai_reasoning(company_name: str, contact_name: str = "", 
                                    company_desc: str = "", website_url: str = "") -> list:
    """Enhanced Strategy 5: LLM-assisted reasoning (Groq or OpenAI)."""
    prompt = f"""Based on this information, determine the most likely Instagram handle:

Company: {company_name}
Contact: {contact_name or 'N/A'}
Description: {company_desc or 'N/A'}
Website: {website_url or 'N/A'}

Analyze and suggest the most likely Instagram handle(s). Return ONLY @username or NOT_FOUND."""
    
    try:
        result = await search_with_llm(prompt)
        handles = extract_instagram_handles_from_text(result)
        return handles
    except Exception:
        return []


# =============================================================================
# MAIN ENRICHMENT FUNCTION
# =============================================================================

async def enrich_contact_instagram(row, skip_enhanced=False, verify_handles=False) -> list:
    """Enrich a single contact with Instagram handles. Returns list of all handles.
    
    Args:
        row: DataFrame row with contact data
        skip_enhanced: If True, skip enhanced search strategies
        verify_handles: If True, verify handles exist via HTTP requests (slower but more accurate)
    """
    page_name = row.get('page_name', '')
    contact_name = row.get('contact_name', '') or row.get('pipeline_name', '')
    contact_position = row.get('contact_position', '') or row.get('pipeline_position', '')
    website_url = row.get('website_url', '')
    linkedin_url = row.get('linkedin_url', '')
    company_desc = str(row.get('company_description', ''))[:200]
    social_links = row.get('social_links', {})
    existing_handles = parse_instagram_handles_field(row.get('instagram_handles', ''))
    
    # OPTIMIZED ORDER: Fastest first, early exit on success
    
    # 1. Check Redis cache (0s) - FASTEST
    if is_redis_available():
        cached_handles = get_cached_handles(page_name, website_url)
        if cached_handles:
            return sorted([h.lower() for h in cached_handles])[:20]
    
    # Early exit: If contact already has valid handles and skip_enhanced is True, skip processing
    if skip_enhanced and existing_handles and len(existing_handles) > 0:
        valid_existing = [h for h in existing_handles if is_valid_handle(h)]
        if valid_existing:
            return sorted([h.lower() for h in valid_existing])[:20]
    
    all_handles = set(existing_handles)  # Start with existing handles
    
    # 2. Check paid API if enabled (0.5s) - FAST
    if is_paid_api_enabled():
        try:
            paid_handles = await search_apify_instagram(page_name, website_url)
            for handle in paid_handles:
                if is_valid_handle(handle):
                    all_handles.add(handle.lower())
            # Early exit if found via paid API
            if len(all_handles) > len(existing_handles):
                handles_list = sorted(list(all_handles))[:20]
                # Cache results
                if is_redis_available():
                    cache_handles(page_name, website_url, handles_list)
                return handles_list
        except Exception:
            pass
    
    # 3. Website scraping with cache (0-2s) - FAST
    if website_url and pd.notna(website_url):
        try:
            website_handles = await scrape_website_for_instagram(str(website_url), use_cache=True)
            # Filter and add valid handles
            for handle in website_handles:
                if is_valid_handle(handle):
                    all_handles.add(handle.lower())
            # Early exit if found
            if len(all_handles) > len(existing_handles) and skip_enhanced:
                handles_list = sorted(list(all_handles))[:20]
                if is_redis_available():
                    cache_handles(page_name, website_url, handles_list)
                return handles_list
        except Exception:
            pass
    
    # 4. Groq LLM reasoning (0.2s) - FAST
    if llm_client:
        try:
            prompt = f"""Based on this information, determine the most likely Instagram handle:

Company: {page_name}
Contact: {contact_name or 'N/A'}
Description: {company_desc or 'N/A'}
Website: {website_url or 'N/A'}

Analyze and suggest the most likely Instagram handle(s). Return ONLY @username or NOT_FOUND."""
            result = await search_with_llm(prompt)
            handles = extract_instagram_handles_from_text(result)
            for handle in handles:
                if is_valid_handle(handle):
                    all_handles.add(handle.lower())
            # Early exit if found
            if len(all_handles) > len(existing_handles):
                handles_list = sorted(list(all_handles))[:20]
                if is_redis_available():
                    cache_handles(page_name, website_url, handles_list)
                return handles_list
        except Exception:
            pass
    
    # FAST MODE EXIT: If skip_enhanced and we found handles, return early
    if skip_enhanced and len(all_handles) > len(existing_handles):
        handles_list = sorted(list(all_handles))[:20]
        if is_redis_available():
            cache_handles(page_name, website_url, handles_list)
        return handles_list

    # 5. Personal handle search (2-3s) - MEDIUM (skip in fast mode)
    if not skip_enhanced and contact_name and pd.notna(contact_name) and str(contact_name).strip() and str(contact_name).strip() != 'None None':
        try:
            personal = await search_personal_instagram(str(contact_name), page_name, str(contact_position) if contact_position else '')
            if personal and is_valid_handle(personal):
                all_handles.add(personal.lower())
        except Exception:
            pass

    # 6. Company handle search (2-3s) - MEDIUM (skip in fast mode)
    if not skip_enhanced and len(all_handles) == len(existing_handles):
        try:
            company_handles = await search_company_instagram(str(page_name), str(website_url) if website_url and pd.notna(website_url) else '')
            for handle in company_handles:
                if is_valid_handle(handle):
                    all_handles.add(handle.lower())
        except Exception:
            pass

    # 7. ENHANCED SEARCH: Only if still missing and not skipped (4-8s) - SLOW
    if not skip_enhanced and len(all_handles) == len(existing_handles):
        # Try enhanced strategies (limit to 3 most effective ones)
        strategies = [
            lambda: strategy_deep_website_scrape(website_url),
            lambda: strategy_openai_reasoning(page_name, contact_name, company_desc, website_url),
            lambda: strategy_multi_query_search(page_name, contact_name, "real estate"),
        ]
        
        for strategy in strategies:
            try:
                handles = await strategy()
                for handle in handles:
                    if is_valid_handle(handle):
                        all_handles.add(handle.lower())
                # Early exit if we found handles
                if len(all_handles) > len(existing_handles):
                    break
                await asyncio.sleep(0.1)  # Reduced delay for Groq
            except Exception:
                continue
    
    # Cache results before returning
    handles_list = sorted(list(all_handles))[:20]
    if is_redis_available() and handles_list:
        cache_handles(page_name, website_url, handles_list)
    
    # Verify handles if requested (note: Instagram's anti-bot measures may limit effectiveness)
    if verify_handles:
        verified_handles = []
        for handle in handles_list:
            # Verify the handle exists
            verification = verify_instagram_handle(handle)
            if verification['exists']:
                verified_handles.append(handle)
            elif verification.get('error'):
                # If there's an error (timeout, etc.), include it anyway (better to include than exclude on error)
                verified_handles.append(handle)
            # If exists=False and no error, skip it (profile unavailable)
            
            # Rate limiting between verifications
            await asyncio.sleep(1.5)
        
        return verified_handles
    else:
        return handles_list


# =============================================================================
# MAIN PROCESSING
# =============================================================================

async def enrich_instagram_handles(df: pd.DataFrame, run_all: bool = False, skip_enhanced: bool = False, verify_handles: bool = False) -> pd.DataFrame:
    """Enrich DataFrame with Instagram handles.
    
    Args:
        df: DataFrame with contact data
        run_all: If True, process all rows; if False, process first 3 (test mode)
        skip_enhanced: If True, skip enhanced search strategies
        verify_handles: If True, verify handles exist via HTTP requests (slower but filters invalid handles)
    """
    
    # Create backup
    if os.path.exists(INPUT_FILE):
        import shutil
        shutil.copy2(INPUT_FILE, BACKUP_FILE)
        print(f"Created backup: {BACKUP_FILE}")
    
    # Ensure instagram_handles column exists
    if 'instagram_handles' not in df.columns:
        df['instagram_handles'] = '[]'
    
    # Determine which rows to process
    if run_all:
        rows_to_process = df
        print(f"\nProcessing all {len(rows_to_process)} contacts...")
    else:
        rows_to_process = df.head(3)
        print(f"\nTest mode: Processing first {len(rows_to_process)} contacts...")
        print("(Use --all to process all contacts)")
    
    # Process in parallel batches for better performance
    # Increased to 20 for Groq's higher rate limits (30k RPM vs OpenAI's 500 RPM)
    BATCH_SIZE = 20 if groq_client else 3  # Process 20 contacts concurrently with Groq, 3 with OpenAI
    found_count = 0
    
    rows_list = list(rows_to_process.iterrows())
    
    # Process in batches with per-contact progress tracking
    with tqdm(total=len(rows_list), desc="Enriching Instagram handles") as pbar:
        for batch_start in range(0, len(rows_list), BATCH_SIZE):
            batch = rows_list[batch_start:batch_start + BATCH_SIZE]
            
            # Process batch concurrently
            batch_tasks = [
                enrich_contact_instagram(row, skip_enhanced=skip_enhanced, verify_handles=verify_handles)
                for idx, row in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Update DataFrame with results
            for (idx, row), handles in zip(batch, batch_results):
                page_name = row.get('page_name', '')
                pos = batch_start + batch.index((idx, row)) + 1
                print(f"\n  [{pos}/{len(rows_list)}] {page_name}")
                
                if isinstance(handles, Exception):
                    print(f"    ✗ Error: {str(handles)[:100]}")
                    pbar.update(1)
                    continue
                
                # Update instagram_handles column with all handles
                if handles:
                    existing = parse_instagram_handles_field(df.loc[idx, 'instagram_handles'])
                    # Merge and deduplicate (case-insensitive)
                    combined = list(set([h.lower() for h in existing + handles]))
                    df.loc[idx, 'instagram_handles'] = json.dumps(sorted(combined))
                    if len(combined) > len(existing):
                        found_count += 1
                    print(f"    ✓ Found {len(handles)} new handle(s), total: {len(combined)}")
                else:
                    print(f"    - No handles found")
                
                pbar.update(1)
            
            # Small delay between batches to avoid rate limits (only if needed)
            if batch_start + BATCH_SIZE < len(rows_list):
                # Groq handles rate limiting better, so minimal delay
                await asyncio.sleep(SEARCH_DELAY)
    
    return df, found_count


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Main function to run Instagram handle enrichment."""
    
    # Check if module should run based on enrichment config
    from utils.enrichment_config import should_run_module
    if not should_run_module("instagram_enricher"):
        print(f"\n{'='*60}")
        print("MODULE 3.7: INSTAGRAM HANDLE ENRICHER")
        print(f"{'='*60}")
        print("⏭️  SKIPPED: Instagram handle enrichment not selected in configuration")
        print("   No changes made to input file.")
        return 0
    
    print(f"\n{'='*60}")
    print("MODULE 3.7: INSTAGRAM HANDLE ENRICHER")
    print(f"{'='*60}")
    
    # Show optimization status
    if groq_client:
        print(f"⚡ Using Groq LLM (10-20x faster than OpenAI)")
    elif openai_client:
        print(f"⚠️  Using OpenAI (slower). Set GROQ_API_KEY for faster performance.")
    else:
        print(f"⚠️  No LLM client available. Set GROQ_API_KEY or OPENAI_API_KEY.")
    
    if is_redis_available():
        print(f"✓ Redis caching enabled")
    else:
        print(f"ℹ️  Redis caching disabled (optional for faster re-runs)")
    
    if is_paid_api_enabled():
        print(f"✓ Paid Instagram API enabled")
    
    # Get versioned input file
    run_id = get_run_id_from_env()
    base_name = "03d_final.csv"
    
    if run_id:
        input_name = get_versioned_filename(base_name, run_id)
        input_file = BASE_DIR / "processed" / input_name
    else:
        input_file = BASE_DIR / INPUT_FILE
    
    # Also try latest symlink if versioned file doesn't exist
    if not input_file.exists():
        latest_input = BASE_DIR / "processed" / base_name
        if latest_input.exists() or latest_input.is_symlink():
            input_file = latest_input
    
    print(f"\nLoading: {input_file}")
    
    if not input_file.exists():
        print(f"Error: {input_file} not found")
        print("Make sure Module 3.6 (Agent Enricher) has run first.")
        return 1
    
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} contacts")
    
    # Check for existing Instagram data
    if 'instagram_handles' in df.columns:
        existing_handles = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).gt(0).sum()
        total_handles = df['instagram_handles'].apply(parse_instagram_handles_field).apply(len).sum()
        print(f"Existing handles: {existing_handles}/{len(df)} contacts ({total_handles} total handles)")
    
    # Determine run mode
    run_all = '--all' in sys.argv
    # OPTIMIZED: Fast mode is now the default (skip slow searches)
    # Use --full for comprehensive search (slow but more thorough)
    skip_enhanced = '--full' not in sys.argv  # Default to fast mode

    if skip_enhanced:
        print("⚡ Fast mode: Using quick methods only (cache, website scrape, LLM reasoning)")
        print("   Use --full for comprehensive search (slower but more thorough)")
    else:
        print("🔍 Full mode: Using all search methods (slower)")

    # Process contacts
    # Check for --verify flag
    verify_handles = "--verify" in sys.argv
    
    enriched_df, found_count = await enrich_instagram_handles(df, run_all=run_all, skip_enhanced=skip_enhanced, verify_handles=verify_handles)
    
    if verify_handles:
        print("\n⚠️  Note: Handle verification enabled. This will be slower due to HTTP requests.")
        print("   Instagram's anti-bot measures may limit verification effectiveness.")
    
    # Save updated CSV to versioned file
    print(f"\nSaving updated data to: {input_file}")
    enriched_df.to_csv(input_file, index=False)
    
    # Create latest symlink
    if run_id:
        latest_path = create_latest_symlink(input_file, base_name)
        if latest_path:
            print(f"✓ Latest symlink: {latest_path}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    
    # Instagram handles summary
    if 'instagram_handles' in enriched_df.columns:
        handles_count = enriched_df['instagram_handles'].apply(parse_instagram_handles_field).apply(len)
        contacts_with_handles = (handles_count > 0).sum()
        total_handles = handles_count.sum()
        print(f"\nInstagram handles:")
        print(f"  Contacts with handles: {contacts_with_handles}/{len(enriched_df)} ({contacts_with_handles/len(enriched_df)*100:.1f}%)")
        print(f"  Total handles: {total_handles}")
        print(f"  Average handles per contact: {total_handles/len(enriched_df):.1f}")
    
    print(f"\nNew handles found in this run: {found_count}")
    print(f"Backup saved to: {BACKUP_FILE}")
    print(f"Updated file: {INPUT_FILE}")
    
    return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
