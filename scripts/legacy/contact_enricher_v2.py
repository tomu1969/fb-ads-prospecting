"""Module 3.6v2.1: AI Agent-Powered Contact Enrichment with Widening Search

Uses OpenAI Agents SDK with WebSearchTool and custom tools for intelligent
contact discovery. The agent autonomously decides which tools to use.

v2.1 Improvements:
- Widening search loop: retries with broader strategies if first pass fails
- Email verification using Hunter.io API (optional)
- Tracks which search strategy succeeded
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

# =============================================================================
# SEARCH STRATEGIES (for widening loop)
# =============================================================================

SEARCH_STRATEGIES = [
    ("strict", "Find contacts ONLY from company website or direct web mentions. Use crawl_website first on the provided URL."),
    ("medium", "Expand search to LinkedIn profiles, industry databases, Zillow/Realtor profiles, and news articles about the company."),
    ("broad", "Cast a wide net - search social profiles, business registries, interviews, podcast appearances, any public mention of contact info.")
]


# =============================================================================
# CUSTOM TOOLS
# =============================================================================

@function_tool
def crawl_website(url: str) -> str:
    """Crawl a website's contact, about, and team pages for emails and contact info.

    Args:
        url: The website URL to crawl (e.g., https://example.com)
    """
    if not url:
        return "No URL provided"

    evidence = []
    visited = set()
    to_visit = [url]

    # Extract domain
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    domain = domain.replace('www.', '')

    # Add common contact page paths
    contact_paths = ['/contact', '/contact-us', '/about', '/about-us', '/team', '/our-team']
    for path in contact_paths:
        contact_url = f"https://{domain}{path}"
        if contact_url not in to_visit:
            to_visit.insert(1, contact_url)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }

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

            # Find emails
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            if emails:
                unique_emails = list(set(emails))
                evidence.append(f"EMAILS FOUND on {current_url}: {', '.join(unique_emails)}")

            # Find phone numbers
            phones = re.findall(r'[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}', text)
            if phones:
                unique_phones = list(set(phones))[:3]  # Limit to 3 phones
                evidence.append(f"PHONES FOUND: {', '.join(unique_phones)}")

            # Find social links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'linkedin.com/in/' in href or 'linkedin.com/company/' in href:
                    evidence.append(f"LINKEDIN: {href}")
                    break

            # Look for names with titles
            for elem in soup.find_all(['h1', 'h2', 'h3', 'p', 'div', 'span']):
                text_content = elem.get_text(strip=True)
                titles = ['owner', 'founder', 'ceo', 'president', 'broker', 'agent', 'manager', 'director', 'realtor']
                if any(title in text_content.lower() for title in titles):
                    if len(text_content) < 150:
                        evidence.append(f"POSSIBLE CONTACT: {text_content}")

            time.sleep(0.3)

        except Exception as e:
            continue

    if not evidence:
        return f"No contact information found on {url}"

    return "\n".join(evidence[:20])


@function_tool
def generate_email_patterns(first_name: str, last_name: str, domain: str) -> str:
    """Generate likely email address patterns for a person at a company.

    Args:
        first_name: Person's first name (e.g., "John")
        last_name: Person's last name (e.g., "Smith")
        domain: Company email domain without @ (e.g., "company.com")
    """
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

    patterns.extend([
        f"{first}@{domain}",
        f"info@{domain}",
        f"contact@{domain}",
    ])

    return f"LIKELY EMAIL PATTERNS for {first_name} {last_name} at {domain}:\n" + "\n".join(patterns[:5])


@function_tool
def verify_email(email: str) -> str:
    """Verify if an email address is deliverable using Hunter.io API.

    Call this to check if a found or generated email is valid.

    Args:
        email: The email address to verify (e.g., "john@company.com")

    Returns:
        Verification result with status (valid/invalid/accept_all/unknown) and score (0-100)
    """
    if not email or '@' not in email:
        return "Invalid email format - cannot verify"

    if not HUNTER_API_KEY:
        return "HUNTER_API_KEY not configured - skipping verification, assume valid"

    try:
        resp = requests.get(
            'https://api.hunter.io/v2/email-verifier',
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=10
        )

        if resp.status_code == 401:
            return "Hunter API authentication failed - assume email is valid"

        if resp.status_code == 429:
            return "Hunter API rate limited - assume email is valid"

        if resp.status_code != 200:
            return f"Hunter API error ({resp.status_code}) - assume email is valid"

        data = resp.json().get('data', {})
        status = data.get('status', 'unknown')  # valid, invalid, accept_all, unknown
        score = data.get('score', 0)  # 0-100

        result = f"VERIFICATION for {email}: status={status}, score={score}"

        if status == 'valid':
            result += " - Email is verified and deliverable"
        elif status == 'invalid':
            result += " - Email may not be deliverable"
        elif status == 'accept_all':
            result += " - Domain accepts all emails, delivery likely"
        else:
            result += " - Could not verify, use with caution"

        return result

    except requests.exceptions.Timeout:
        return "Hunter API timeout - assume email is valid"
    except Exception as e:
        return f"Verification error: {str(e)} - assume email is valid"


# =============================================================================
# AGENT CONFIGURATION
# =============================================================================

AGENT_INSTRUCTIONS = """You are an expert at finding business contact information for sales outreach.

## YOUR GOAL
Find the PRIMARY contact person (owner, founder, broker, manager) and their EMAIL for the given company.

## SEARCH STRATEGY
Follow the STRATEGY instruction in the prompt carefully:
- STRICT: Only use crawl_website and direct web search on company domain
- MEDIUM: Include LinkedIn, industry databases (Zillow, Realtor.com), press releases
- BROAD: Cast wide net - social profiles, interviews, podcast appearances, any public mention

## STRATEGY (follow this order):
1. FIRST: Use web_search to find "[company name] owner email" or "[company name] contact"
2. THEN: Use crawl_website on the company's website to find emails on contact/about pages
3. IF you find a NAME but no email: Use generate_email_patterns to create likely patterns
4. IF you find a LinkedIn profile: Search for that person's email
5. OPTIONAL: Use verify_email to check if an email is deliverable (boosts confidence)

## IMPORTANT RULES:
- NEVER invent or guess emails without evidence
- Prefer personal emails (john@company.com) over generic ones (info@company.com)
- If you find multiple people, choose: owner > founder > broker > manager > agent
- If an email is found directly on website/profile, trust it

## CONFIDENCE SCORING:
- 0.9: Email found directly on website or verified source
- 0.8: Email found via web search with clear attribution
- 0.7: Email verified as "valid" by verify_email tool
- 0.6: Email pattern generated from verified name + domain
- 0.3: Generic email like info@ or contact@
- 0.0: No email found

## OUTPUT FORMAT (respond with ONLY this JSON):
{
    "name": "Full Name or empty string if not found",
    "position": "Job Title or empty string if not found",
    "email": "email@domain.com or empty string if not found",
    "confidence": 0.0,
    "source": "Brief description of where you found the info"
}
"""

# Create the agent
contact_agent = Agent(
    name="ContactResearcher",
    instructions=AGENT_INSTRUCTIONS,
    tools=[WebSearchTool(), crawl_website, generate_email_patterns, verify_email],
)


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def is_good_result(result: dict, min_confidence: float = 0.7) -> bool:
    """Check if result meets quality threshold for early exit."""
    if not result.get('email') or '@' not in result.get('email', ''):
        return False
    if result.get('confidence', 0) < min_confidence:
        return False
    return True


def parse_json_result(output: str) -> dict:
    """Parse JSON from agent output."""
    json_match = re.search(r'\{[^{}]*\}', output, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {
        "name": "",
        "position": "",
        "email": "",
        "confidence": 0.0,
        "source": "parse_error: " + output[:100]
    }


async def find_contact_single(company_name: str, website_url: str, strategy_name: str, strategy_desc: str) -> dict:
    """Run a single search pass with the given strategy."""
    prompt = f"""Find the best contact email for this company:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

SEARCH STRATEGY ({strategy_name.upper()}): {strategy_desc}

Search for the owner, founder, broker, or main contact person.
Return structured JSON with name, position, email, confidence, source."""

    try:
        result = await Runner.run(contact_agent, prompt)
        return parse_json_result(result.final_output)
    except Exception as e:
        return {
            "name": "",
            "position": "",
            "email": "",
            "confidence": 0.0,
            "source": f"error: {str(e)}"
        }


async def find_contact(company_name: str, website_url: str, max_attempts: int = 3) -> dict:
    """Find contact info with widening search loop.

    Tries progressively broader search strategies until a good result is found.
    """
    best_result = None

    for attempt, (strategy_name, strategy_desc) in enumerate(SEARCH_STRATEGIES[:max_attempts]):
        print(f"    [{strategy_name}] Attempt {attempt + 1}/{max_attempts}...")

        result = await find_contact_single(company_name, website_url, strategy_name, strategy_desc)

        # Track best result across attempts
        if not best_result or result.get('confidence', 0) > best_result.get('confidence', 0):
            best_result = result.copy()
            best_result['search_strategy'] = strategy_name

        # Early exit if good result found
        if is_good_result(result):
            print(f"    [✓] Found good result on {strategy_name} strategy")
            return best_result

        # Short delay between attempts
        if attempt < max_attempts - 1:
            await asyncio.sleep(1)

    # Return best attempt if all strategies exhausted
    if best_result:
        best_result['search_strategy'] = best_result.get('search_strategy', 'exhausted')
    return best_result or {
        "name": "",
        "position": "",
        "email": "",
        "confidence": 0.0,
        "source": "all_strategies_exhausted",
        "search_strategy": "exhausted"
    }


def find_contact_sync(company_name: str, website_url: str) -> dict:
    """Synchronous wrapper for find_contact."""
    return asyncio.run(find_contact(company_name, website_url))


async def main():
    """Run contact enrichment on companies missing email data."""

    # Load data
    input_path = 'processed/03b_hunter.csv'
    print(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} rows")

    # Determine which companies to process
    if '--specific' in sys.argv:
        # Run on companies from manual_contacts.csv for comparison
        manual_path = 'config/manual_contacts.csv'
        if os.path.exists(manual_path):
            manual_df = pd.read_csv(manual_path)
            manual_names = set(manual_df['page_name'].tolist())

            # Get from enriched data (has website URLs)
            input_orig = 'processed/02_enriched.csv'
            df_orig = pd.read_csv(input_orig)
            test_df = df_orig[df_orig['page_name'].isin(manual_names)].copy()
            print(f"\nRunning on {len(test_df)} companies from manual_contacts.csv for comparison")
        else:
            print("manual_contacts.csv not found")
            return
    elif '--test' in sys.argv:
        # Test mode: first 3 companies missing emails
        mask = df['primary_email'].isna() | (df['primary_email'] == '')
        has_website = ~df['website_url'].isna() & (df['search_confidence'] > 0.3)
        test_df = df[mask & has_website].head(3).copy()
        print(f"\nTesting with first 3 companies")
    else:
        # All companies missing emails
        mask = df['primary_email'].isna() | (df['primary_email'] == '')
        has_website = ~df['website_url'].isna() & (df['search_confidence'] > 0.3)
        test_df = df[mask & has_website].copy()
        print(f"\nProcessing {len(test_df)} companies with missing emails")

    # Process each company
    results = []
    for idx, row in tqdm(test_df.iterrows(), total=len(test_df), desc="Agent enrichment"):
        page_name = row['page_name']
        website_url = row.get('website_url', '')

        print(f"\n  Processing: {page_name}")
        contact = await find_contact(page_name, website_url)

        results.append({
            'page_name': page_name,
            'website_url': website_url,
            'agent_email': contact.get('email', ''),
            'agent_name': contact.get('name', ''),
            'agent_position': contact.get('position', ''),
            'agent_confidence': contact.get('confidence', 0),
            'agent_source': contact.get('source', ''),
            'search_strategy': contact.get('search_strategy', 'unknown'),
        })

        # Rate limit
        await asyncio.sleep(2)

    # Save results
    results_df = pd.DataFrame(results)
    output_path = 'processed/03c_agent_enriched.csv'
    results_df.to_csv(output_path, index=False)
    print(f"\nSaved results to: {output_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("AGENT ENRICHMENT v2.1 RESULTS (with widening search)")
    print("=" * 60)

    found_emails = sum(1 for r in results if r['agent_email'])
    high_conf = sum(1 for r in results if r['agent_confidence'] >= 0.7)
    med_conf = sum(1 for r in results if 0.3 <= r['agent_confidence'] < 0.7)

    # Count by strategy
    strategy_counts = {}
    for r in results:
        strat = r.get('search_strategy', 'unknown')
        strategy_counts[strat] = strategy_counts.get(strat, 0) + 1

    print(f"Total processed: {len(results)}")
    print(f"Emails found: {found_emails} ({found_emails/len(results)*100:.0f}%)")
    print(f"High confidence (>=0.7): {high_conf}")
    print(f"Medium confidence (0.3-0.7): {med_conf}")
    print(f"\nStrategy breakdown:")
    for strat, count in strategy_counts.items():
        print(f"  - {strat}: {count}")

    print("\nDetailed results:")
    for r in results:
        conf = r['agent_confidence']
        strat = r.get('search_strategy', 'unknown')
        indicator = "+" if conf >= 0.7 else "~" if conf >= 0.3 else "?"
        print(f"  {indicator} {r['page_name']} [{strat}]")
        print(f"      Email: {r['agent_email'] or 'NOT FOUND'} (conf: {conf:.2f})")
        if r['agent_name']:
            print(f"      Contact: {r['agent_name']} - {r['agent_position']}")
        if r['agent_source']:
            print(f"      Source: {r['agent_source'][:60]}...")

    # Compare with manual contacts if running in --specific mode
    if '--specific' in sys.argv:
        print("\n" + "=" * 60)
        print("COMPARISON WITH MANUAL RESEARCH")
        print("=" * 60)

        manual_df = pd.read_csv('config/manual_contacts.csv')
        manual_lookup = {row['page_name']: row['primary_email'] for _, row in manual_df.iterrows()}

        exact = 0
        partial = 0
        miss = 0

        for r in results:
            manual_email = manual_lookup.get(r['page_name'], '')
            agent_email = r['agent_email']

            if not manual_email:
                continue

            if agent_email.lower() == manual_email.lower():
                print(f"  EXACT: {r['page_name']}")
                print(f"         Agent: {agent_email}")
                exact += 1
            elif agent_email and '@' in agent_email and '@' in manual_email and manual_email.split('@')[1] == agent_email.split('@')[1]:
                print(f"  PARTIAL: {r['page_name']}")
                print(f"           Manual: {manual_email}")
                print(f"           Agent:  {agent_email}")
                partial += 1
            else:
                print(f"  MISS: {r['page_name']}")
                print(f"        Manual: {manual_email}")
                print(f"        Agent:  {agent_email or 'NOT FOUND'}")
                miss += 1

        total = exact + partial + miss
        print(f"\nSummary: {exact} exact, {partial} partial, {miss} miss")
        if total > 0:
            print(f"Exact match rate: {exact/total*100:.0f}%")

    return results_df


if __name__ == '__main__':
    asyncio.run(main())
