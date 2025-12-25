"""Module 3.6: AI-Powered Contact Enrichment with Web Search

Uses OpenAI's web_search tool and structured outputs to find contact information
for companies where Hunter.io didn't find results.
"""

import os
import sys
import json
import re
import time
import requests
import pandas as pd
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

load_dotenv()

client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Response schema for structured outputs
CONTACT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "contact_info",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "contacts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "position": {"type": "string"},
                            "email": {"type": "string"},
                            "confidence": {"type": "number"},
                            "source": {"type": "string"}
                        },
                        "required": ["name", "position", "email", "confidence", "source"],
                        "additionalProperties": False
                    }
                },
                "company_phone": {"type": "string"},
                "company_email": {"type": "string"},
                "social_links": {
                    "type": "object",
                    "properties": {
                        "linkedin": {"type": "string"},
                        "facebook": {"type": "string"},
                        "twitter": {"type": "string"}
                    },
                    "required": ["linkedin", "facebook", "twitter"],
                    "additionalProperties": False
                }
            },
            "required": ["contacts", "company_phone", "company_email", "social_links"],
            "additionalProperties": False
        }
    }
}


def extract_domain(url):
    """Extract clean domain from URL."""
    if not url or pd.isna(url):
        return None
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    return domain.replace('www.', '')


def crawl_website(url, max_pages=5):
    """Crawl a website to extract contact evidence."""
    if not url:
        return ""

    evidence = []
    visited = set()
    to_visit = [url]

    # Common contact page paths
    contact_paths = ['/contact', '/contact-us', '/about', '/about-us', '/team', '/our-team']

    # Add contact pages to visit first
    domain = extract_domain(url)
    for path in contact_paths:
        contact_url = f"https://{domain}{path}"
        if contact_url not in to_visit:
            to_visit.insert(1, contact_url)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }

    pages_crawled = 0
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

            # Extract text content
            text = soup.get_text(separator=' ', strip=True)

            # Find emails via regex
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            if emails:
                evidence.append(f"Emails found on {current_url}: {', '.join(set(emails))}")

            # Find phone numbers
            phones = re.findall(r'[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}', text)
            if phones:
                evidence.append(f"Phones found on {current_url}: {', '.join(set(phones))}")

            # Find social links
            social_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'linkedin.com' in href:
                    social_links.append(f"LinkedIn: {href}")
                elif 'facebook.com' in href:
                    social_links.append(f"Facebook: {href}")
                elif 'twitter.com' in href or 'x.com' in href:
                    social_links.append(f"Twitter: {href}")

            if social_links:
                evidence.append(f"Social links: {', '.join(set(social_links))}")

            # Look for team/about sections
            for elem in soup.find_all(['h1', 'h2', 'h3', 'p', 'div', 'span']):
                text_content = elem.get_text(strip=True)
                # Look for name/title patterns
                if any(title in text_content.lower() for title in ['owner', 'founder', 'ceo', 'president', 'broker', 'agent', 'manager', 'director']):
                    if len(text_content) < 200:  # Avoid huge blocks
                        evidence.append(f"Possible contact: {text_content}")

            time.sleep(0.5)  # Rate limit

        except Exception as e:
            continue

    return "\n".join(evidence[:50])  # Limit evidence size


def enrich_with_ai(company_name, website_url, crawl_evidence, use_web_search=True):
    """Use OpenAI to analyze evidence and find contacts."""

    domain = extract_domain(website_url) or company_name.lower().replace(' ', '')

    # Build prompt
    prompt = f"""Find contact information for: {company_name}
Website: {website_url or 'Unknown'}
Domain: {domain}

Evidence from website crawl:
{crawl_evidence if crawl_evidence else 'No direct evidence found from website crawl.'}

Instructions:
1. Analyze the evidence to extract contact names, positions, and emails
2. If emails are found directly, use them with high confidence (0.9+)
3. If only partial info found, generate likely email patterns (confidence 0.5-0.7)
4. For company email, prefer contact@, info@, or hello@ patterns
5. Set empty string "" for any field where no data is found
6. Confidence should be 0.0-1.0 where 1.0 means definitely correct

Return structured contact information."""

    try:
        # Use web search if enabled and no direct evidence found
        tools = []
        if use_web_search and (not crawl_evidence or 'Emails found' not in crawl_evidence):
            tools = [{"type": "web_search_preview"}]

        response = client.responses.create(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": prompt}],
            text={"format": CONTACT_SCHEMA},
            tools=tools if tools else None
        )

        # Parse response
        result = json.loads(response.output_text)
        return result

    except Exception as e:
        print(f"  AI enrichment error: {e}")
        # Return empty structure on error
        return {
            "contacts": [],
            "company_phone": "",
            "company_email": "",
            "social_links": {"linkedin": "", "facebook": "", "twitter": ""}
        }


def enrich_with_ai_chat(company_name, website_url, crawl_evidence):
    """Fallback: Use chat completions API with function calling for contact extraction."""

    domain = extract_domain(website_url) or company_name.lower().replace(' ', '')

    prompt = f"""Find contact information for: {company_name}
Website: {website_url or 'Unknown'}
Domain: {domain}

Evidence from website crawl:
{crawl_evidence if crawl_evidence else 'No direct evidence found from website crawl.'}

Instructions:
1. Analyze the evidence to extract contact names, positions, and emails
2. If emails are found directly, use them with high confidence (0.9+)
3. If only partial info found, generate likely email patterns (confidence 0.5-0.7)
4. For company email, prefer contact@, info@, or hello@ patterns
5. Set empty string "" for any field where no data is found

Return as JSON with this exact structure:
{{
  "contacts": [
    {{"name": "...", "position": "...", "email": "...", "confidence": 0.9, "source": "website"}}
  ],
  "company_phone": "...",
  "company_email": "...",
  "social_links": {{"linkedin": "", "facebook": "", "twitter": ""}}
}}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a contact information extraction expert. Always return valid JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )

        result = json.loads(response.choices[0].message.content)

        # Ensure required fields exist
        if "contacts" not in result:
            result["contacts"] = []
        if "company_phone" not in result:
            result["company_phone"] = ""
        if "company_email" not in result:
            result["company_email"] = ""
        if "social_links" not in result:
            result["social_links"] = {"linkedin": "", "facebook": "", "twitter": ""}

        return result

    except Exception as e:
        print(f"  AI chat enrichment error: {e}")
        return {
            "contacts": [],
            "company_phone": "",
            "company_email": "",
            "social_links": {"linkedin": "", "facebook": "", "twitter": ""}
        }


def enrich_company(page_name, website_url):
    """Full enrichment pipeline for a single company."""
    print(f"\n  Processing: {page_name}")

    # Step 1: Crawl website for evidence
    print(f"    Crawling website...")
    evidence = crawl_website(website_url)

    # Step 2: Use AI to analyze and enrich (chat completions API)
    print(f"    Running AI analysis...")
    result = enrich_with_ai_chat(page_name, website_url, evidence)

    return result


def main():
    """Run contact enrichment on companies missing email data."""

    # Load the data
    input_path = 'processed/03b_hunter.csv'
    print(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} rows")

    # Find rows with missing email
    mask = df['primary_email'].isna() | (df['primary_email'] == '') | (df['primary_email'].astype(str).str.strip() == '')
    missing_df = df[mask].copy()

    # Also filter to only those with websites
    has_website = ~missing_df['website_url'].isna() & (missing_df['website_url'] != '') & (missing_df['search_confidence'] > 0.3)
    missing_df = missing_df[has_website]

    print(f"\nFound {len(missing_df)} companies with missing emails but valid websites")

    if '--test' in sys.argv:
        print("Testing with first 3 companies")
        missing_df = missing_df.head(3)
    elif '--specific' in sys.argv:
        # Run on specific companies from manual_contacts.csv
        manual_path = 'config/manual_contacts.csv'
        if os.path.exists(manual_path):
            manual_df = pd.read_csv(manual_path)
            manual_names = set(manual_df['page_name'].tolist())

            # Find these in original data (before Hunter enrichment)
            input_orig = 'processed/02_enriched.csv'
            df_orig = pd.read_csv(input_orig)
            missing_df = df_orig[df_orig['page_name'].isin(manual_names)].copy()
            print(f"Running on {len(missing_df)} companies from manual_contacts.csv for comparison")

    results = []
    for idx, row in tqdm(missing_df.iterrows(), total=len(missing_df), desc="Contact enrichment"):
        page_name = row['page_name']
        website_url = row.get('website_url', '')

        result = enrich_company(page_name, website_url)

        # Extract best contact
        best_contact = None
        if result.get('contacts'):
            # Sort by confidence
            sorted_contacts = sorted(result['contacts'], key=lambda x: x.get('confidence', 0), reverse=True)
            best_contact = sorted_contacts[0]

        results.append({
            'page_name': page_name,
            'website_url': website_url,
            'enriched_email': best_contact.get('email', '') if best_contact else result.get('company_email', ''),
            'enriched_name': best_contact.get('name', '') if best_contact else '',
            'enriched_position': best_contact.get('position', '') if best_contact else '',
            'confidence': best_contact.get('confidence', 0) if best_contact else 0,
            'source': best_contact.get('source', 'ai_enrichment') if best_contact else 'ai_enrichment',
            'company_phone': result.get('company_phone', ''),
            'linkedin': result.get('social_links', {}).get('linkedin', ''),
            'all_contacts': json.dumps(result.get('contacts', []))
        })

        time.sleep(1)  # Rate limit

    # Save results
    results_df = pd.DataFrame(results)
    output_path = 'processed/03c_enriched.csv'
    results_df.to_csv(output_path, index=False)
    print(f"\nSaved enrichment results to: {output_path}")

    # Print summary
    print("\n" + "="*60)
    print("ENRICHMENT RESULTS")
    print("="*60)

    found_emails = sum(1 for r in results if r['enriched_email'])
    high_confidence = sum(1 for r in results if r['confidence'] >= 0.7)

    print(f"Total processed: {len(results)}")
    print(f"Emails found: {found_emails} ({found_emails/len(results)*100:.0f}%)")
    print(f"High confidence (>=0.7): {high_confidence}")

    print("\nDetailed results:")
    for r in results:
        conf_indicator = "✓" if r['confidence'] >= 0.7 else "~" if r['confidence'] >= 0.5 else "?"
        print(f"  {conf_indicator} {r['page_name']}")
        print(f"      Email: {r['enriched_email'] or 'NOT FOUND'} (conf: {r['confidence']:.2f})")
        if r['enriched_name']:
            print(f"      Contact: {r['enriched_name']} - {r['enriched_position']}")

    return results_df


if __name__ == '__main__':
    main()
