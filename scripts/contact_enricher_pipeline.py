"""Contact Enricher Pipeline v4: Cascading Verification

Module 3.6 in the FB Ads Library Pipeline.
Runs after Hunter (3.5) to find emails for contacts Hunter couldn't verify.

A cost-effective pipeline that combines:
- Stage 1: v2.1 (fast/cheap) with rotating strategies
- Stage 2: v3.1 (creative/thorough) with feedback loop
- Hunter.io verification as the quality gate

Key features:
- Early exit when Hunter validates an email
- v2.1 evidence passed to v3.1 reasoning agent
- Cost tracking per contact
- Full status output with hunter_status column
- Merge logic to combine results with Hunter output

Input: processed/03b_hunter.csv (from Hunter module)
Output: processed/03c_enriched.csv (enriched contacts)
        processed/03d_final.csv (merged with Hunter results)

Usage:
    python contact_enricher_pipeline.py           # Test mode (3 contacts)
    python contact_enricher_pipeline.py --all     # Process all needing enrichment
    python contact_enricher_pipeline.py --specific # Use manual_contacts.csv

Budget: Max $0.42/contact
Expected: 45-55% verified emails
"""

import os
import sys
import json
import re
import time
import asyncio
import requests
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm

# OpenAI Agents SDK imports
from agents import Agent, Runner, WebSearchTool, function_tool

load_dotenv()

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')

# Pipeline input/output paths
INPUT_FILE = 'processed/03b_hunter.csv'
OUTPUT_ENRICHED = 'processed/03c_enriched.csv'
OUTPUT_FINAL = 'processed/03d_final.csv'

# Cost tracking (estimates based on GPT-4o pricing)
COST_V2_1_RUN = 0.03  # Per strategy run
COST_V3_1_ITERATION = 0.05  # Per two-agent iteration
COST_HUNTER_VERIFICATION = 0.01  # Per verification


# =============================================================================
# PIPELINE FILTER & MERGE FUNCTIONS
# =============================================================================

def get_contacts_to_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Get contacts where Hunter found no verified email.

    Filters for:
    - primary_email is empty/NaN
    - email_verified is False/NaN
    - Has a valid website_url (search_confidence > 0.3)

    Returns DataFrame of contacts needing enrichment.
    """
    # Check for email_verified column (may not exist if Hunter was skipped)
    if 'email_verified' in df.columns:
        mask = (
            df['primary_email'].isna() |
            (df['primary_email'] == '') |
            (df['email_verified'] == False) |
            df['email_verified'].isna()
        )
    else:
        mask = df['primary_email'].isna() | (df['primary_email'] == '')

    # Also require a website to search
    has_website = ~df['website_url'].isna() & (df['website_url'] != '')

    # Check for search_confidence if it exists
    if 'search_confidence' in df.columns:
        good_website = has_website & (df['search_confidence'] > 0.3)
    else:
        good_website = has_website

    return df[mask & good_website].copy()


def merge_results(hunter_df: pd.DataFrame, enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Merge pipeline enrichment results into the main Hunter dataset.

    Priority:
    1. Pipeline verified email (hunter_status in ['valid', 'accept_all'])
    2. Original Hunter email (if verified)
    3. Best available email (even if not verified)

    Also merges phone numbers from pipeline into phones column.

    Returns merged DataFrame with all original columns plus enrichment columns.
    """
    import ast

    merged = hunter_df.copy()

    # Add enrichment columns if they don't exist
    enrichment_cols = [
        'pipeline_email', 'pipeline_phone', 'pipeline_name', 'pipeline_position',
        'pipeline_confidence', 'pipeline_hunter_status', 'pipeline_hunter_score',
        'enrichment_stage', 'enrichment_cost', 'enrichment_source'
    ]
    for col in enrichment_cols:
        if col not in merged.columns:
            merged[col] = None

    # Process each enriched contact
    for _, enriched_row in enriched_df.iterrows():
        page_name = enriched_row['page_name']
        mask = merged['page_name'] == page_name

        if not mask.any():
            continue

        # Store pipeline results in enrichment columns
        merged.loc[mask, 'pipeline_email'] = enriched_row.get('email', '')
        merged.loc[mask, 'pipeline_phone'] = enriched_row.get('phone', '')
        merged.loc[mask, 'pipeline_name'] = enriched_row.get('name', '')
        merged.loc[mask, 'pipeline_position'] = enriched_row.get('position', '')
        merged.loc[mask, 'pipeline_confidence'] = enriched_row.get('confidence', 0)
        merged.loc[mask, 'pipeline_hunter_status'] = enriched_row.get('hunter_status', '')
        merged.loc[mask, 'pipeline_hunter_score'] = enriched_row.get('hunter_score', 0)
        merged.loc[mask, 'enrichment_stage'] = enriched_row.get('stage_found', '')
        merged.loc[mask, 'enrichment_cost'] = enriched_row.get('total_cost', 0)
        merged.loc[mask, 'enrichment_source'] = enriched_row.get('source', '')

        # Merge phone into phones column if pipeline found one and original is empty
        pipeline_phone = enriched_row.get('phone', '')
        if pipeline_phone:
            existing_phones = merged.loc[mask, 'phones'].values[0]
            # Check if existing phones is empty
            has_phones = False
            if existing_phones and str(existing_phones) not in ['[]', 'nan', '']:
                try:
                    parsed = ast.literal_eval(str(existing_phones)) if isinstance(existing_phones, str) else existing_phones
                    has_phones = bool(parsed)
                except (ValueError, SyntaxError):
                    pass

            if not has_phones:
                merged.loc[mask, 'phones'] = f"['{pipeline_phone}']"

        # If pipeline found a verified email, update primary fields
        pipeline_status = enriched_row.get('hunter_status', '')
        if pipeline_status in ['valid', 'accept_all']:
            # Check if original had a verified email (treat NaN as False)
            if 'email_verified' in merged.columns:
                orig_verified = merged.loc[mask, 'email_verified'].values[0]
                orig_verified = bool(orig_verified) if pd.notna(orig_verified) else False
            else:
                orig_verified = False

            if not orig_verified:
                # Pipeline found better email - update primary fields
                merged.loc[mask, 'primary_email'] = enriched_row.get('email', '')
                merged.loc[mask, 'email_verified'] = True
                merged.loc[mask, 'contact_name'] = enriched_row.get('name', '') or merged.loc[mask, 'contact_name'].values[0]
                merged.loc[mask, 'contact_position'] = enriched_row.get('position', '') or merged.loc[mask, 'contact_position'].values[0]

    return merged


# =============================================================================
# HUNTER.IO VERIFICATION
# =============================================================================

def verify_with_hunter(email: str) -> dict:
    """Verify email with Hunter.io API.

    Returns:
        {
            'status': 'valid'|'invalid'|'accept_all'|'unknown'|'error',
            'score': 0-100,
            'is_deliverable': bool
        }
    """
    if not email or '@' not in email:
        return {'status': 'invalid', 'score': 0, 'is_deliverable': False}

    if not HUNTER_API_KEY:
        # No API key - assume valid to continue pipeline
        return {'status': 'not_checked', 'score': 0, 'is_deliverable': True}

    try:
        resp = requests.get(
            'https://api.hunter.io/v2/email-verifier',
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=15
        )

        if resp.status_code == 401:
            return {'status': 'error', 'score': 0, 'is_deliverable': False}

        if resp.status_code == 429:
            # Rate limited - assume valid to not block pipeline
            return {'status': 'rate_limited', 'score': 0, 'is_deliverable': True}

        if resp.status_code == 202:
            # Still processing - wait and retry once
            time.sleep(3)
            resp = requests.get(
                'https://api.hunter.io/v2/email-verifier',
                params={'email': email, 'api_key': HUNTER_API_KEY},
                timeout=15
            )

        if resp.status_code != 200:
            return {'status': 'error', 'score': 0, 'is_deliverable': False}

        data = resp.json().get('data', {})
        status = data.get('status', 'unknown')
        score = data.get('score', 0)

        # Determine if deliverable
        is_deliverable = status in ['valid', 'accept_all'] or score >= 80

        return {
            'status': status,
            'score': score,
            'is_deliverable': is_deliverable
        }

    except requests.exceptions.Timeout:
        return {'status': 'timeout', 'score': 0, 'is_deliverable': False}
    except Exception as e:
        return {'status': 'error', 'score': 0, 'is_deliverable': False}


# =============================================================================
# CUSTOM TOOLS (shared by both stages)
# =============================================================================

@function_tool
def crawl_website(url: str) -> str:
    """Crawl a website's contact, about, and team pages for emails and contact info."""
    if not url:
        return "No URL provided"

    evidence = []
    visited = set()
    to_visit = [url]

    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    domain = domain.replace('www.', '')

    contact_paths = ['/contact', '/contact-us', '/about', '/about-us', '/team', '/our-team']
    for path in contact_paths:
        contact_url = f"https://{domain}{path}"
        if contact_url not in to_visit:
            to_visit.insert(1, contact_url)

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

    pages_crawled = 0
    max_pages = 5

    while to_visit and pages_crawled < max_pages:
        current_url = to_visit.pop(0)
        if current_url in visited:
            continue

        try:
            resp = requests.get(current_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue

            visited.add(current_url)
            pages_crawled += 1

            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)

            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            if emails:
                unique_emails = list(set(emails))
                evidence.append(f"EMAILS FOUND on {current_url}: {', '.join(unique_emails)}")

            phones = re.findall(r'[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}', text)
            if phones:
                unique_phones = list(set(phones))[:3]
                evidence.append(f"PHONES FOUND: {', '.join(unique_phones)}")

            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'linkedin.com/in/' in href or 'linkedin.com/company/' in href:
                    evidence.append(f"LINKEDIN: {href}")
                    break

            for elem in soup.find_all(['h1', 'h2', 'h3', 'p', 'div', 'span']):
                text_content = elem.get_text(strip=True)
                titles = ['owner', 'founder', 'ceo', 'president', 'broker', 'agent', 'manager', 'director', 'realtor']
                if any(title in text_content.lower() for title in titles):
                    if len(text_content) < 150:
                        evidence.append(f"POSSIBLE CONTACT: {text_content}")

            time.sleep(0.3)
        except Exception:
            continue

    if not evidence:
        return f"No contact information found on {url}"
    return "\n".join(evidence[:20])


@function_tool
def generate_email_patterns(first_name: str, last_name: str, domain: str) -> str:
    """Generate likely email address patterns for a person at a company."""
    if not first_name or not domain:
        return "Cannot generate patterns without first_name and domain"

    first = first_name.lower().strip()
    last = last_name.lower().strip() if last_name else ""

    patterns = []
    if last:
        patterns.extend([
            f"{first}.{last}@{domain}",
            f"{first[0]}{last}@{domain}",
            f"{first}{last[0]}@{domain}",
            f"{first}_{last}@{domain}",
            f"{first}{last}@{domain}",
        ])
    patterns.extend([f"{first}@{domain}", f"info@{domain}", f"contact@{domain}"])

    return f"LIKELY EMAIL PATTERNS for {first_name} {last_name} at {domain}:\n" + "\n".join(patterns[:5])


@function_tool
def verify_email(email: str) -> str:
    """Verify if an email address is deliverable using Hunter.io API."""
    result = verify_with_hunter(email)
    return f"VERIFICATION for {email}: status={result['status']}, score={result['score']}"


# =============================================================================
# STAGE 1: v2.1 SEARCH STRATEGIES
# =============================================================================

SEARCH_STRATEGIES = [
    ("strict", "Find contacts ONLY from company website or direct web mentions. Use crawl_website first."),
    ("medium", "Expand to LinkedIn profiles, industry databases, Zillow/Realtor profiles, news articles."),
    ("broad", "Cast wide net - social profiles, business registries, interviews, podcast appearances.")
]

V2_1_AGENT_INSTRUCTIONS = """You are an expert at finding business contact information for sales outreach.

## YOUR GOAL
Find the PRIMARY contact person (owner, founder, broker, manager) and their EMAIL and PHONE for the given company.

## SEARCH STRATEGY
Follow the STRATEGY instruction in the prompt carefully:
- STRICT: Only use crawl_website and direct web search on company domain
- MEDIUM: Include LinkedIn, industry databases (Zillow, Realtor.com), press releases
- BROAD: Cast wide net - social profiles, interviews, podcast appearances, any public mention

## STRATEGY ORDER:
1. FIRST: Use web_search to find "[company name] owner email" or "[company name] contact"
2. THEN: Use crawl_website on the company's website (this extracts both emails AND phones)
3. IF you find a NAME but no email: Use generate_email_patterns
4. IF you find a profile: Search for that person's email and phone

## CONFIDENCE SCORING:
- 0.9: Email found directly on website or verified source
- 0.8: Email found via web search with clear attribution
- 0.6: Email pattern generated from verified name + domain
- 0.3: Generic email like info@ or contact@
- 0.0: No email found

## OUTPUT FORMAT (respond with ONLY this JSON):
{
    "name": "Full Name or empty string",
    "position": "Job Title or empty string",
    "email": "email@domain.com or empty string",
    "phone": "phone number or empty string",
    "confidence": 0.0,
    "source": "Where you found the info",
    "queries_tried": ["query1", "query2"],
    "evidence_found": "What you discovered"
}
"""

v2_1_agent = Agent(
    name="ContactResearcherV2",
    model="gpt-4o",
    instructions=V2_1_AGENT_INSTRUCTIONS,
    tools=[WebSearchTool(), crawl_website, generate_email_patterns, verify_email],
)


def parse_json_result(output: str) -> dict:
    """Parse JSON from agent output."""
    code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', output, re.DOTALL)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1))
        except json.JSONDecodeError:
            pass

    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', output, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {}


async def run_v2_1_strategy(company_name: str, website_url: str, strategy_name: str, strategy_desc: str) -> dict:
    """Run a single v2.1 search with given strategy."""
    prompt = f"""Find the best contact email for this company:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

SEARCH STRATEGY ({strategy_name.upper()}): {strategy_desc}

Search for the owner, founder, broker, or main contact person.
Return structured JSON with name, position, email, confidence, source, queries_tried, evidence_found."""

    try:
        result = await Runner.run(v2_1_agent, prompt)
        parsed = parse_json_result(result.final_output)
        parsed['strategy'] = strategy_name
        return parsed
    except Exception as e:
        return {'email': '', 'confidence': 0, 'source': f'error: {str(e)}', 'strategy': strategy_name}


async def stage_v2_1(company_name: str, website_url: str) -> dict:
    """Run v2.1 with rotating strategies until Hunter validates.

    Returns immediately when Hunter says 'valid' or 'accept_all'.
    """
    best_result = None
    total_cost = 0.0
    search_history = []

    for strategy_name, strategy_desc in SEARCH_STRATEGIES:
        print(f"      [{strategy_name}] Searching...")

        result = await run_v2_1_strategy(company_name, website_url, strategy_name, strategy_desc)
        total_cost += COST_V2_1_RUN

        # Record for feedback to v3.1
        search_history.append({
            'strategy': strategy_name,
            'queries_tried': result.get('queries_tried', []),
            'evidence_found': result.get('evidence_found', ''),
            'result': result.get('email', 'NOT FOUND'),
            'confidence': result.get('confidence', 0)
        })

        # Track best result
        if not best_result or result.get('confidence', 0) > best_result.get('confidence', 0):
            best_result = result.copy()

        # Check with Hunter if we found an email
        if result.get('email'):
            hunter = verify_with_hunter(result['email'])
            total_cost += COST_HUNTER_VERIFICATION

            result['hunter_status'] = hunter['status']
            result['hunter_score'] = hunter['score']

            if hunter['is_deliverable']:
                result['stage_found'] = f"v2.1_{strategy_name}"
                result['cost'] = total_cost
                result['search_history'] = search_history
                print(f"      ✓ Hunter validated: {hunter['status']} (score: {hunter['score']})")
                return result

            print(f"      → Hunter: {hunter['status']} (score: {hunter['score']}), continuing...")

        await asyncio.sleep(1)

    # Return best result if no valid email found
    if best_result:
        best_result['hunter_status'] = best_result.get('hunter_status', 'not_checked')
        best_result['hunter_score'] = best_result.get('hunter_score', 0)
        best_result['stage_found'] = 'v2.1_exhausted'
        best_result['cost'] = total_cost
        best_result['search_history'] = search_history

    return best_result or {
        'email': '', 'confidence': 0, 'hunter_status': 'not_checked',
        'hunter_score': 0, 'stage_found': 'v2.1_exhausted',
        'cost': total_cost, 'search_history': search_history
    }


# =============================================================================
# STAGE 2: v3.1 TWO-AGENT LOOP
# =============================================================================

EXECUTOR_INSTRUCTIONS = """You are a contact search executor who provides DETAILED feedback.

## AVAILABLE TOOLS
1. **web_search** - Your PRIMARY tool. Use this FIRST.
2. **crawl_website** - Use on company website AND profiles found (extracts emails AND phones)
3. **generate_email_patterns** - Create likely email formats when you have name + domain
4. **verify_email** - Check if email is deliverable

## EXECUTION RULES
1. Execute the specific_queries from the reasoning agent
2. Execute ALL suggested queries, not just the first one
3. Report ALL information found, even partial info (email, phone, name)

## OUTPUT FORMAT (JSON only):
{
    "name": "Contact name or empty",
    "email": "email@domain.com or empty",
    "phone": "phone number or empty",
    "position": "Job title or empty",
    "confidence": 0.0-1.0,
    "source": "Where found",
    "queries_tried": ["query1", "query2"],
    "evidence_found": "What you discovered",
    "obstacles": "What prevented finding better results"
}
"""

REASONING_INSTRUCTIONS = """You are a contact research strategist who LEARNS from each attempt.

## YOUR ROLE
1. Analyze the company type (real estate, law firm, service business)
2. Review what's been tried before
3. Suggest a NEW strategy that addresses previous failures
4. Provide SPECIFIC search queries

## STRATEGY OPTIONS
- **WEBSITE_CRAWL**: Direct website /contact, /about, /team
- **SOCIAL_PROFILES**: "[company] zillow", "[name] realtor zillow", "[company] site:linkedin.com"
- **INDUSTRY_DIRECTORY**: "[company] nextdoor", "[company] yelp", "[company] pissedconsumer contact"
- **NEWS_MENTIONS**: "[company] owner interview", "[founder] press release"
- **BUSINESS_REGISTRY**: "[company] [state] business registration"
- **PATTERN_GENERATION**: Generate patterns from known name + domain

## OUTPUT FORMAT (JSON only):
{
    "company_analysis": "Brief analysis of company type",
    "strategy": "STRATEGY_NAME",
    "specific_queries": ["exact query 1", "exact query 2", "query 3"],
    "target_sites": ["site1.com", "site2.com"],
    "reasoning": "Why this strategy should work"
}
"""

executor_agent = Agent(
    name="ContactExecutor",
    model="gpt-4o",
    instructions=EXECUTOR_INSTRUCTIONS,
    tools=[WebSearchTool(), crawl_website, generate_email_patterns, verify_email],
)

reasoning_agent = Agent(
    name="ContactReasoner",
    model="gpt-4o",
    instructions=REASONING_INSTRUCTIONS,
)


async def stage_v3_1(company_name: str, website_url: str, v2_evidence: dict, max_iterations: int = 5) -> dict:
    """Run v3.1 two-agent loop until Hunter validates.

    Passes v2.1 evidence to reasoning agent so it doesn't repeat failed strategies.
    """
    search_history = v2_evidence.get('search_history', [])
    best_result = None
    total_cost = 0.0

    for iteration in range(max_iterations):
        print(f"      [v3.1 iter {iteration + 1}/{max_iterations}]")

        # === REASONING AGENT ===
        reasoning_prompt = f"""Analyze this company and provide a NEW search strategy:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}
ITERATION: {iteration + 1} of {max_iterations}

## PREVIOUS ATTEMPTS (learn from these):
"""
        for i, attempt in enumerate(search_history):
            reasoning_prompt += f"""
Attempt {i + 1}:
  Strategy: {attempt.get('strategy', 'unknown')}
  Queries tried: {attempt.get('queries_tried', [])}
  Evidence found: {attempt.get('evidence_found', 'none')}
  Result: {attempt.get('result', 'NOT FOUND')}
  Confidence: {attempt.get('confidence', 0)}
"""

        reasoning_prompt += """
Based on what's been tried, suggest a NEW approach. Don't repeat failed strategies.
Respond with JSON: company_analysis, strategy, specific_queries, target_sites, reasoning"""

        try:
            reasoning_result = await Runner.run(reasoning_agent, reasoning_prompt)
            strategy = parse_json_result(reasoning_result.final_output)

            if not strategy.get('strategy'):
                strategy = {
                    'strategy': 'WEBSITE_CRAWL',
                    'specific_queries': [f"{company_name} owner email contact"],
                    'reasoning': 'Fallback'
                }

            print(f"        Strategy: {strategy.get('strategy', 'unknown')}")
        except Exception as e:
            strategy = {'strategy': 'FALLBACK', 'specific_queries': [f"{company_name} contact email"]}

        # === EXECUTOR AGENT ===
        executor_prompt = f"""Execute this search strategy to find contact info:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

## STRATEGY FROM REASONING AGENT:
{json.dumps(strategy, indent=2)}

Execute the specific_queries. Report everything you find.
Respond with JSON: name, email, position, confidence, source, queries_tried, evidence_found, obstacles"""

        try:
            executor_result = await Runner.run(executor_agent, executor_prompt)
            execution = parse_json_result(executor_result.final_output)
            execution.setdefault('queries_tried', strategy.get('specific_queries', []))
            execution.setdefault('evidence_found', '')
            execution.setdefault('obstacles', '')
        except Exception as e:
            execution = {'email': '', 'confidence': 0, 'source': f'error: {str(e)}'}

        total_cost += COST_V3_1_ITERATION

        # Record for next iteration
        search_history.append({
            'strategy': strategy.get('strategy', 'unknown'),
            'queries_tried': execution.get('queries_tried', []),
            'evidence_found': execution.get('evidence_found', ''),
            'result': execution.get('email', 'NOT FOUND'),
            'confidence': execution.get('confidence', 0),
            'obstacles': execution.get('obstacles', '')
        })

        # Track best result
        if not best_result or execution.get('confidence', 0) > best_result.get('confidence', 0):
            best_result = execution.copy()
            best_result['strategy_used'] = strategy.get('strategy', 'unknown')

        # Check with Hunter if we found an email
        if execution.get('email'):
            hunter = verify_with_hunter(execution['email'])
            total_cost += COST_HUNTER_VERIFICATION

            execution['hunter_status'] = hunter['status']
            execution['hunter_score'] = hunter['score']

            if hunter['is_deliverable']:
                execution['stage_found'] = f"v3.1_iter{iteration + 1}"
                execution['cost'] = total_cost
                execution['search_history'] = search_history
                print(f"        ✓ Hunter validated: {hunter['status']} (score: {hunter['score']})")
                return execution

            print(f"        → Hunter: {hunter['status']}, continuing...")

        await asyncio.sleep(1)

    # Return best result
    if best_result:
        best_result['hunter_status'] = best_result.get('hunter_status', 'not_checked')
        best_result['hunter_score'] = best_result.get('hunter_score', 0)
        best_result['stage_found'] = 'v3.1_exhausted'
        best_result['cost'] = total_cost
        best_result['search_history'] = search_history

    return best_result or {
        'email': '', 'confidence': 0, 'hunter_status': 'not_checked',
        'stage_found': 'v3.1_exhausted', 'cost': total_cost,
        'search_history': search_history
    }


# =============================================================================
# PIPELINE COORDINATOR
# =============================================================================

async def enrich_contact(company_name: str, website_url: str) -> dict:
    """Main pipeline: v2.1 → v3.1 → output with Hunter status and phone."""

    print(f"\n  Processing: {company_name}")

    # STAGE 1: v2.1 loop
    print(f"    Stage 1: v2.1 (fast search)...")
    v2_result = await stage_v2_1(company_name, website_url)

    if v2_result.get('hunter_status') in ['valid', 'accept_all']:
        return {
            'page_name': company_name,
            'website_url': website_url,
            'email': v2_result.get('email', ''),
            'phone': v2_result.get('phone', ''),
            'name': v2_result.get('name', ''),
            'position': v2_result.get('position', ''),
            'confidence': v2_result.get('confidence', 0),
            'hunter_status': v2_result.get('hunter_status', ''),
            'hunter_score': v2_result.get('hunter_score', 0),
            'stage_found': v2_result.get('stage_found', ''),
            'source': v2_result.get('source', ''),
            'total_cost': v2_result.get('cost', 0)
        }

    # STAGE 2: v3.1 loop (only for unfound/invalid)
    print(f"    Stage 2: v3.1 (creative search)...")
    v3_result = await stage_v3_1(company_name, website_url, v2_evidence=v2_result)

    total_cost = v2_result.get('cost', 0) + v3_result.get('cost', 0)

    # Combine phones from both stages
    phone = v3_result.get('phone', '') or v2_result.get('phone', '')

    if v3_result.get('hunter_status') in ['valid', 'accept_all']:
        return {
            'page_name': company_name,
            'website_url': website_url,
            'email': v3_result.get('email', ''),
            'phone': phone,
            'name': v3_result.get('name', ''),
            'position': v3_result.get('position', ''),
            'confidence': v3_result.get('confidence', 0),
            'hunter_status': v3_result.get('hunter_status', ''),
            'hunter_score': v3_result.get('hunter_score', 0),
            'stage_found': v3_result.get('stage_found', ''),
            'source': v3_result.get('source', ''),
            'total_cost': total_cost
        }

    # UNFOUND - return best attempt
    best = v3_result if v3_result.get('confidence', 0) > v2_result.get('confidence', 0) else v2_result

    return {
        'page_name': company_name,
        'website_url': website_url,
        'email': best.get('email', ''),
        'phone': phone,
        'name': best.get('name', ''),
        'position': best.get('position', ''),
        'confidence': best.get('confidence', 0),
        'hunter_status': best.get('hunter_status', 'not_checked'),
        'hunter_score': best.get('hunter_score', 0),
        'stage_found': 'unfound',
        'source': best.get('source', ''),
        'total_cost': total_cost
    }


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Run pipeline on companies.

    Modes:
    - Default (no args): Test mode - process first 3 contacts needing enrichment
    - --all: Full run - process all contacts needing enrichment
    - --specific: Use manual_contacts.csv for testing
    """

    print(f"\n{'='*60}")
    print("MODULE 3.6: AGENT ENRICHER (Pipeline v4)")
    print(f"{'='*60}")

    # Load input file
    print(f"\nLoading: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found")
        print("Make sure Module 3.5 (Hunter) has run first.")
        return None

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} total contacts")

    # Determine which companies to process
    if '--specific' in sys.argv:
        # Test with manual_contacts.csv
        manual_path = 'config/manual_contacts.csv'
        if os.path.exists(manual_path):
            manual_df = pd.read_csv(manual_path)
            manual_names = set(manual_df['page_name'].tolist())
            test_df = df[df['page_name'].isin(manual_names)].copy()
            print(f"\nRunning on {len(test_df)} companies from manual_contacts.csv")
        else:
            print("config/manual_contacts.csv not found")
            return None
    else:
        # Filter to contacts needing enrichment
        needs_enrichment = get_contacts_to_enrich(df)
        print(f"Contacts needing enrichment: {len(needs_enrichment)}")

        if '--all' in sys.argv:
            test_df = needs_enrichment
            print(f"\n--all mode: Processing all {len(test_df)} contacts")
        else:
            # Test mode: first 3
            test_df = needs_enrichment.head(3)
            print(f"\nTest mode: Processing first {len(test_df)} contacts")
            print("(Use --all to process all contacts)")

    if len(test_df) == 0:
        print("\nNo contacts need enrichment. All have verified emails from Hunter.")
        # Still create output files for pipeline continuity
        df.to_csv(OUTPUT_FINAL, index=False)
        print(f"Copied {INPUT_FILE} to {OUTPUT_FINAL}")
        return df

    # Process each company
    results = []
    for idx, row in tqdm(test_df.iterrows(), total=len(test_df), desc="Pipeline enrichment"):
        result = await enrich_contact(row['page_name'], row.get('website_url', ''))
        results.append(result)
        await asyncio.sleep(2)

    # Save enrichment results
    results_df = pd.DataFrame(results)
    results_df.to_csv(OUTPUT_ENRICHED, index=False)
    print(f"\nSaved enrichment results to: {OUTPUT_ENRICHED}")

    # Merge with original Hunter data
    print(f"\nMerging results with Hunter data...")
    merged_df = merge_results(df, results_df)
    merged_df.to_csv(OUTPUT_FINAL, index=False)
    print(f"Saved merged results to: {OUTPUT_FINAL}")

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE v4 RESULTS")
    print("=" * 60)

    valid = sum(1 for r in results if r['hunter_status'] == 'valid')
    accept_all = sum(1 for r in results if r['hunter_status'] == 'accept_all')
    invalid = sum(1 for r in results if r['hunter_status'] == 'invalid')
    unfound = sum(1 for r in results if r['stage_found'] == 'unfound')
    total_cost = sum(r['total_cost'] for r in results)

    print(f"Total processed: {len(results)}")
    print(f"Valid emails: {valid} ({valid/len(results)*100:.0f}%)" if results else "")
    print(f"Accept-all: {accept_all}")
    print(f"Invalid: {invalid}")
    print(f"Unfound: {unfound}")
    print(f"Total cost: ${total_cost:.2f}")
    print(f"Avg cost/contact: ${total_cost/len(results):.2f}" if results else "")

    # Stage breakdown
    stage_counts = {}
    for r in results:
        stage = r.get('stage_found', 'unknown')
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

    print(f"\nStage breakdown:")
    for stage, count in sorted(stage_counts.items(), key=lambda x: -x[1]):
        print(f"  - {stage}: {count}")

    print("\nDetailed results:")
    for r in results:
        status = r['hunter_status']
        indicator = "✓" if status in ['valid', 'accept_all'] else "✗" if status == 'invalid' else "?"
        print(f"  {indicator} {r['page_name']} [{r['stage_found']}]")
        print(f"      Email: {r['email'] or 'NOT FOUND'}")
        print(f"      Hunter: {status} (score: {r['hunter_score']}) | Cost: ${r['total_cost']:.2f}")

    # Compare with manual contacts if --specific
    if '--specific' in sys.argv:
        print("\n" + "=" * 60)
        print("COMPARISON WITH MANUAL RESEARCH")
        print("=" * 60)

        manual_df = pd.read_csv('config/manual_contacts.csv')
        manual_lookup = {row['page_name']: row['primary_email'] for _, row in manual_df.iterrows()}

        exact = 0
        verified_found = 0

        for r in results:
            manual_email = manual_lookup.get(r['page_name'], '')
            pipeline_email = r['email']
            hunter_status = r['hunter_status']

            if not manual_email:
                continue

            if pipeline_email.lower() == manual_email.lower():
                print(f"  EXACT: {r['page_name']}")
                print(f"         {pipeline_email} (Hunter: {hunter_status})")
                exact += 1
            elif hunter_status in ['valid', 'accept_all']:
                print(f"  VERIFIED (different): {r['page_name']}")
                print(f"         Manual: {manual_email}")
                print(f"         Found:  {pipeline_email} (Hunter: {hunter_status})")
                verified_found += 1
            else:
                print(f"  MISS: {r['page_name']}")
                print(f"        Manual: {manual_email}")
                print(f"        Found:  {pipeline_email or 'NOT FOUND'} (Hunter: {hunter_status})")

        total = len([r for r in results if manual_lookup.get(r['page_name'])])
        print(f"\nSummary:")
        print(f"  Exact match: {exact}/{total}")
        print(f"  Verified (different email): {verified_found}/{total}")
        print(f"  Total deliverable: {exact + verified_found}/{total} ({(exact + verified_found)/total*100:.0f}%)")

    # Print merge summary
    print("\n" + "=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)

    # Count verified emails in final output
    verified_count = 0
    if 'email_verified' in merged_df.columns:
        # email_verified contains strings like 'valid', 'accept_all', 'manual', etc.
        verified_count = merged_df['email_verified'].apply(
            lambda x: str(x).lower() in ['valid', 'accept_all', 'manual', 'true']
        ).sum()

    total_with_email = merged_df['primary_email'].notna().sum()

    print(f"Total contacts: {len(merged_df)}")
    print(f"Contacts with email: {total_with_email} ({total_with_email/len(merged_df)*100:.0f}%)")
    print(f"Verified emails: {verified_count} ({verified_count/len(merged_df)*100:.0f}%)")
    print(f"\nOutput files:")
    print(f"  - {OUTPUT_ENRICHED} (enrichment results only)")
    print(f"  - {OUTPUT_FINAL} (merged with Hunter data)")

    return merged_df


if __name__ == '__main__':
    asyncio.run(main())
