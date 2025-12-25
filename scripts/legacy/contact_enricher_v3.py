"""Module 3.6v3: Two-Agent Contact Enrichment with Iterative Feedback Loop

Architecture:
- REASONING AGENT (gpt-5.2-thinking): Analyzes company, receives feedback, refines strategy
- EXECUTOR AGENT (gpt-5.2-instant): Executes searches, reports detailed evidence
- COORDINATOR: Python async loop managing feedback between agents

Key improvement over v2.1: Instead of hardcoded widening strategies, the reasoning
agent LEARNS from each execution attempt and adapts its strategy based on:
- What searches were tried
- What evidence was found (even if not the email)
- What obstacles prevented success

v3 Target: 55%+ exact match (up from 45% in v2.1)
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
# CUSTOM TOOLS (reused from v2.1)
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
                unique_phones = list(set(phones))[:3]
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

    Args:
        email: The email address to verify (e.g., "john@company.com")
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
        status = data.get('status', 'unknown')
        score = data.get('score', 0)

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
# EXECUTOR AGENT - Executes search strategies and reports detailed feedback
# =============================================================================

EXECUTOR_INSTRUCTIONS = """You are a contact search executor who provides DETAILED feedback.

## YOUR ROLE
Execute the search strategy provided by the reasoning agent and report EVERYTHING you find.
Your detailed feedback helps the reasoning agent learn and improve on the next iteration.

## AVAILABLE TOOLS
1. **web_search** - Your PRIMARY tool. Use this FIRST for every strategy.
   - Search "[company] owner email contact"
   - Search "[company] founder name"
   - Search "[person name] [company] email"
   - Search for the company on specific sites: "[company] site:zillow.com" or "[company] site:linkedin.com"
2. **crawl_website** - Use on company website AND on any profiles found via web search
3. **generate_email_patterns** - Create likely email formats when you have name + domain
4. **verify_email** - Check if email is deliverable (optional)

## EXECUTION RULES - CRITICAL
1. **ALWAYS start with web_search** using the specific_queries provided
2. Execute ALL queries in specific_queries, not just the first one
3. If web search finds a profile URL, use crawl_website on that URL
4. If you find a name but no email, use generate_email_patterns
5. Report ALL information found, even if not the email we're looking for

## FEEDBACK IS CRITICAL
The reasoning agent LEARNS from your feedback to choose better strategies next time.
Always report these fields thoroughly:
- **queries_tried**: List EVERY EXACT search query you executed (this is crucial!)
- **evidence_found**: ALL useful information: names, phone numbers, addresses, social profiles, even partial info
- **obstacles**: Why you couldn't find the email (no contact page, website down, only generic email, etc.)

## CONFIDENCE SCORING
- 0.9: Email found directly on website or verified source with clear attribution
- 0.8: Email found via web search with citation
- 0.7: Email verified as "valid" by verify_email tool
- 0.6: Email pattern generated from verified name + domain
- 0.3: Generic email like info@ or contact@
- 0.0: No email found

## OUTPUT FORMAT (respond with ONLY this JSON):
{
    "name": "Contact name or empty string",
    "email": "email@domain.com or empty string",
    "position": "Job title or empty string",
    "confidence": 0.0,
    "source": "Where you found this information",
    "queries_tried": ["exact query 1", "exact query 2"],
    "evidence_found": "Detailed description of what you discovered, even partial info like names, phones, social profiles",
    "obstacles": "What prevented finding better results"
}
"""

executor_agent = Agent(
    name="ContactExecutor",
    model="gpt-4o",  # gpt-5.2-instant not yet available, using gpt-4o
    instructions=EXECUTOR_INSTRUCTIONS,
    tools=[WebSearchTool(), crawl_website, generate_email_patterns, verify_email],
)


# =============================================================================
# REASONING AGENT - Analyzes company and learns from each attempt
# =============================================================================

REASONING_INSTRUCTIONS = """You are a contact research strategist who LEARNS from each attempt.

## YOUR ROLE
1. Analyze the company and its characteristics
2. Review what's been tried before (if any)
3. Suggest a NEW strategy that addresses previous failures
4. Provide SPECIFIC guidance for the executor

## COMPANY TYPE ANALYSIS
Identify the company type to choose appropriate search strategies:
- **Real Estate**: Try Zillow, Realtor.com, MLS profiles, state licensing boards
- **Law Firm**: Try Avvo, Martindale-Hubbell, state bar associations
- **Construction/Contractor**: Try HomeAdvisor, Angi, BBB, contractor license boards
- **Service Business**: Try Nextdoor, Yelp, Google Business, local directories
- **General Business**: Try LinkedIn, company website, news articles, press releases

## LEARNING FROM PREVIOUS ATTEMPTS
When you see previous attempts in the prompt:
1. Analyze WHY each attempt failed based on the obstacles reported
2. Look for clues in "evidence_found" that weren't fully exploited
3. If website only had generic email, try industry-specific profiles
4. If social profiles failed, try business registries or news
5. If a name was found but no email, suggest pattern generation
6. NEVER repeat the same strategy or queries that already failed

## STRATEGY OPTIONS
Choose ONE strategy and provide 3-5 SPECIFIC search queries:

- **WEBSITE_CRAWL**: Direct website analysis
  - Best for: Companies with professional websites
  - Queries: "[company] contact email", "[company] about us owner"
  - Crawl: /contact, /about, /team, /staff pages

- **SOCIAL_PROFILES**: Platform-specific searches (BEST FOR REALTORS)
  - Realtors: "[company name] zillow", "[owner name] realtor zillow", "[company] site:zillow.com"
  - Lawyers: "[name] attorney avvo", "[company] site:avvo.com"
  - General: "[owner name] [company] linkedin"

- **INDUSTRY_DIRECTORY**: Professional directories and review sites
  - Real estate: "[company] mls listing", "[company] realtor.com"
  - Home services: "[company] nextdoor", "[company] yelp contact", "[company] angi"
  - Any: "[company] pissedconsumer contact" (often has customer service emails)

- **NEWS_MENTIONS**: Press and media coverage
  - "[company name] owner interview"
  - "[founder name] [company] press release"
  - "[company] featured local news email"

- **BUSINESS_REGISTRY**: Official records
  - "[company name] [state] business registration"
  - "[company name] LLC registered agent email"
  - "[company] sunbiz florida" (for FL companies)

- **PATTERN_GENERATION**: Generate likely patterns when name is known
  - Only use after confirming person's name from previous evidence
  - Provide: first name, last name, domain

## OUTPUT FORMAT (respond with ONLY this JSON):
{
    "company_analysis": "Brief analysis: company type, size, likely contact patterns",
    "strategy": "STRATEGY_NAME from above options",
    "specific_queries": ["exact search query 1", "exact search query 2", "query 3"],
    "target_sites": ["site1.com", "site2.com"],
    "reasoning": "Why this strategy should work, especially if addressing previous failures"
}
"""

reasoning_agent = Agent(
    name="ContactReasoner",
    model="gpt-4o",  # gpt-5.2-thinking not yet available, using gpt-4o
    instructions=REASONING_INSTRUCTIONS,
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_json_result(output: str) -> dict:
    """Parse JSON from agent output, handling markdown code blocks."""
    # Try to extract JSON from markdown code block first
    code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', output, re.DOTALL)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try direct JSON match
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', output, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {}


def is_good_result(result: dict, min_confidence: float = 0.7) -> bool:
    """Check if result meets quality threshold for early exit."""
    if not result.get('email') or '@' not in result.get('email', ''):
        return False
    if result.get('confidence', 0) < min_confidence:
        return False
    return True


def detect_company_type(company_name: str, website_url: str) -> str:
    """Detect company type for initial strategy hints."""
    name_lower = company_name.lower()

    if any(word in name_lower for word in ['realty', 'real estate', 'realtor', 'homes', 'property', 'properties']):
        return "real_estate"
    elif any(word in name_lower for word in ['law', 'attorney', 'legal', 'pa', 'esq']):
        return "law_firm"
    elif any(word in name_lower for word in ['construction', 'builder', 'contractor', 'roofing', 'plumbing']):
        return "construction"
    elif any(word in name_lower for word in ['handyman', 'plumber', 'electrician', 'hvac', 'cleaning']):
        return "service"
    else:
        return "general"


# =============================================================================
# COORDINATOR - Two-Agent Feedback Loop
# =============================================================================

async def find_contact_v3(company_name: str, website_url: str, max_iterations: int = 3) -> dict:
    """Two-agent contact enrichment with iterative feedback loop.

    The key innovation: the reasoning agent receives FULL feedback from each
    execution attempt, allowing it to learn and adapt its strategy.
    """

    # Accumulates feedback across iterations
    search_history = []
    best_result = None
    company_type = detect_company_type(company_name, website_url)

    for iteration in range(max_iterations):
        print(f"\n    [Iteration {iteration + 1}/{max_iterations}]")

        # =====================================================================
        # PHASE 1: REASONING AGENT ANALYZES & STRATEGIZES
        # =====================================================================

        reasoning_prompt = f"""Analyze this company and provide a search strategy:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}
DETECTED TYPE: {company_type}
ITERATION: {iteration + 1} of {max_iterations}

"""
        # Add accumulated feedback from ALL previous iterations
        if search_history:
            reasoning_prompt += "## PREVIOUS ATTEMPTS (learn from these failures):\n"
            for i, attempt in enumerate(search_history):
                reasoning_prompt += f"""
### Attempt {i + 1}:
- **Strategy Used**: {attempt['strategy']}
- **Queries Tried**: {attempt['queries_tried']}
- **Evidence Found**: {attempt['evidence_found']}
- **Result**: {attempt['result']}
- **Confidence**: {attempt['confidence']}
- **Obstacles**: {attempt['obstacles']}
"""
            reasoning_prompt += """
Based on what's been tried and WHY it failed, suggest a NEW approach.
DO NOT repeat failed queries. Address the specific obstacles mentioned above.

"""

        reasoning_prompt += """## YOUR TASK:
1. Analyze the company (type, likely contact patterns)
2. If there are previous attempts, explain what went wrong and how to improve
3. Provide a NEW strategy with SPECIFIC search queries
4. Focus on queries that haven't been tried yet

Respond with JSON containing: company_analysis, strategy, specific_queries, target_sites, reasoning
"""

        print(f"      → Reasoning agent analyzing...")
        try:
            reasoning_result = await Runner.run(reasoning_agent, reasoning_prompt)
            strategy = parse_json_result(reasoning_result.final_output)

            if not strategy.get('strategy'):
                strategy = {
                    'strategy': 'WEBSITE_CRAWL',
                    'specific_queries': [f"{company_name} owner email contact"],
                    'target_sites': [website_url] if website_url else [],
                    'reasoning': 'Fallback to basic search'
                }

            print(f"      → Strategy: {strategy.get('strategy', 'unknown')}")

        except Exception as e:
            print(f"      → Reasoning error: {e}")
            strategy = {
                'strategy': 'WEBSITE_CRAWL',
                'specific_queries': [f"{company_name} contact email"],
                'reasoning': f'Error fallback: {str(e)}'
            }

        # =====================================================================
        # PHASE 2: EXECUTOR AGENT EXECUTES STRATEGY
        # =====================================================================

        executor_prompt = f"""Execute this search strategy to find contact info:

COMPANY: {company_name}
WEBSITE: {website_url or 'Unknown'}

## STRATEGY FROM REASONING AGENT:
{json.dumps(strategy, indent=2)}

## YOUR TASK:
1. Execute the specific_queries suggested above
2. Use your tools: web_search, crawl_website, generate_email_patterns, verify_email
3. Report EVERYTHING you find - names, phones, social profiles, even partial info
4. Explain obstacles if you couldn't find the email

CRITICAL: Provide detailed feedback in queries_tried, evidence_found, and obstacles fields.
This feedback helps the reasoning agent improve on the next iteration.

Respond with JSON containing: name, email, position, confidence, source, queries_tried, evidence_found, obstacles
"""

        print(f"      → Executor agent searching...")
        try:
            executor_result = await Runner.run(executor_agent, executor_prompt)
            execution = parse_json_result(executor_result.final_output)

            # Ensure all feedback fields exist
            execution.setdefault('queries_tried', strategy.get('specific_queries', []))
            execution.setdefault('evidence_found', 'No detailed evidence reported')
            execution.setdefault('obstacles', 'No obstacles reported')

        except Exception as e:
            print(f"      → Executor error: {e}")
            execution = {
                'name': '',
                'email': '',
                'confidence': 0,
                'source': f'Error: {str(e)}',
                'queries_tried': strategy.get('specific_queries', []),
                'evidence_found': 'Execution failed',
                'obstacles': str(e)
            }

        # =====================================================================
        # PHASE 3: RECORD FEEDBACK FOR NEXT ITERATION
        # =====================================================================

        search_history.append({
            'strategy': strategy.get('strategy', 'unknown'),
            'queries_tried': execution.get('queries_tried', []),
            'evidence_found': execution.get('evidence_found', 'nothing reported'),
            'result': execution.get('email', 'NOT FOUND') or 'NOT FOUND',
            'confidence': execution.get('confidence', 0),
            'obstacles': execution.get('obstacles', 'none reported')
        })

        # Track best result across all iterations
        current_confidence = execution.get('confidence', 0)
        if not best_result or current_confidence > best_result.get('confidence', 0):
            best_result = {
                'name': execution.get('name', ''),
                'email': execution.get('email', ''),
                'position': execution.get('position', ''),
                'confidence': current_confidence,
                'source': execution.get('source', ''),
                'iteration': iteration + 1,
                'strategy_used': strategy.get('strategy', 'unknown'),
            }

        # Early exit if good result found
        if is_good_result(execution):
            print(f"      ✓ Found good result on iteration {iteration + 1}")
            best_result['search_history'] = search_history
            best_result['total_iterations'] = iteration + 1
            return best_result

        conf_display = execution.get('confidence', 0)
        print(f"      → Confidence: {conf_display:.2f}, continuing to next iteration...")

        # Short delay between iterations
        if iteration < max_iterations - 1:
            await asyncio.sleep(1)

    # Return best result after all iterations exhausted
    if best_result:
        best_result['search_history'] = search_history
        best_result['total_iterations'] = max_iterations

    return best_result or {
        'name': '',
        'email': '',
        'position': '',
        'confidence': 0,
        'source': 'all_iterations_exhausted',
        'search_history': search_history,
        'total_iterations': max_iterations
    }


def find_contact_sync(company_name: str, website_url: str) -> dict:
    """Synchronous wrapper for find_contact_v3."""
    return asyncio.run(find_contact_v3(company_name, website_url))


# =============================================================================
# MAIN FUNCTION
# =============================================================================

async def main():
    """Run two-agent contact enrichment on companies."""

    # Load data
    input_path = 'processed/03b_hunter.csv'
    print(f"Loading: {input_path}")

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found")
        return

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
    for idx, row in tqdm(test_df.iterrows(), total=len(test_df), desc="Two-agent enrichment"):
        page_name = row['page_name']
        website_url = row.get('website_url', '')

        print(f"\n  Processing: {page_name}")
        contact = await find_contact_v3(page_name, website_url)

        results.append({
            'page_name': page_name,
            'website_url': website_url,
            'agent_email': contact.get('email', ''),
            'agent_name': contact.get('name', ''),
            'agent_position': contact.get('position', ''),
            'agent_confidence': contact.get('confidence', 0),
            'agent_source': contact.get('source', ''),
            'strategy_used': contact.get('strategy_used', 'unknown'),
            'total_iterations': contact.get('total_iterations', 0),
        })

        # Rate limit
        await asyncio.sleep(2)

    # Save results
    results_df = pd.DataFrame(results)
    output_path = 'processed/03c_agent_enriched_v3.csv'
    results_df.to_csv(output_path, index=False)
    print(f"\nSaved results to: {output_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("TWO-AGENT ENRICHMENT v3 RESULTS")
    print("=" * 60)

    found_emails = sum(1 for r in results if r['agent_email'])
    high_conf = sum(1 for r in results if r['agent_confidence'] >= 0.7)
    med_conf = sum(1 for r in results if 0.3 <= r['agent_confidence'] < 0.7)

    # Count by strategy
    strategy_counts = {}
    for r in results:
        strat = r.get('strategy_used', 'unknown')
        strategy_counts[strat] = strategy_counts.get(strat, 0) + 1

    # Average iterations
    avg_iterations = sum(r.get('total_iterations', 0) for r in results) / len(results) if results else 0

    print(f"Total processed: {len(results)}")
    print(f"Emails found: {found_emails} ({found_emails/len(results)*100:.0f}%)" if results else "No results")
    print(f"High confidence (>=0.7): {high_conf}")
    print(f"Medium confidence (0.3-0.7): {med_conf}")
    print(f"Average iterations needed: {avg_iterations:.1f}")

    print(f"\nStrategy breakdown:")
    for strat, count in sorted(strategy_counts.items(), key=lambda x: -x[1]):
        print(f"  - {strat}: {count}")

    print("\nDetailed results:")
    for r in results:
        conf = r['agent_confidence']
        strat = r.get('strategy_used', 'unknown')
        iters = r.get('total_iterations', 0)
        indicator = "+" if conf >= 0.7 else "~" if conf >= 0.3 else "?"
        print(f"  {indicator} {r['page_name']} [{strat}] ({iters} iterations)")
        print(f"      Email: {r['agent_email'] or 'NOT FOUND'} (conf: {conf:.2f})")
        if r['agent_name']:
            print(f"      Contact: {r['agent_name']} - {r['agent_position']}")
        if r['agent_source']:
            print(f"      Source: {r['agent_source'][:80]}...")

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
            elif agent_email and '@' in agent_email and '@' in manual_email:
                manual_domain = manual_email.split('@')[1].lower()
                agent_domain = agent_email.split('@')[1].lower()
                if manual_domain == agent_domain:
                    print(f"  PARTIAL: {r['page_name']}")
                    print(f"           Manual: {manual_email}")
                    print(f"           Agent:  {agent_email}")
                    partial += 1
                else:
                    print(f"  MISS: {r['page_name']}")
                    print(f"        Manual: {manual_email}")
                    print(f"        Agent:  {agent_email}")
                    miss += 1
            else:
                print(f"  MISS: {r['page_name']}")
                print(f"        Manual: {manual_email}")
                print(f"        Agent:  {agent_email or 'NOT FOUND'}")
                miss += 1

        total = exact + partial + miss
        print(f"\nSummary: {exact} exact, {partial} partial, {miss} miss")
        if total > 0:
            print(f"Exact match rate: {exact/total*100:.0f}%")
            print(f"Found rate: {(exact + partial)/total*100:.0f}%")

    return results_df


if __name__ == '__main__':
    asyncio.run(main())
