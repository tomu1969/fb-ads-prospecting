"""
Module 3.5: Apollo.io Email Enrichment
Enriches prospects with email addresses using Apollo.io API
Only queries Apollo for rows that don't have emails from scraping
"""

import os
import json
import ast
from urllib.parse import urlparse
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()
parent_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
if os.path.exists(parent_env_path):
    load_dotenv(parent_env_path)

# Apollo API endpoints
APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/people/bulk_match"


def parse_list_field(value):
    """Parse a list field that may be stored as string."""
    if isinstance(value, list):
        return value
    if pd.isna(value) or value == '' or value == '[]':
        return []
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return []


def extract_domain(url):
    """Extract domain from URL."""
    if not url or pd.isna(url):
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return None


def search_apollo(domain, api_key):
    """Search Apollo.io for contacts at a domain and reveal emails."""
    if not domain or not api_key:
        return {"emails": [], "contacts": []}

    try:
        # Step 1: Search for people at the domain
        search_response = requests.post(
            APOLLO_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": api_key
            },
            json={
                "q_organization_domains": domain,
                "per_page": 5
            },
            timeout=10
        )

        if not search_response.ok:
            print(f"Apollo search error for {domain}: {search_response.status_code}")
            return {"emails": [], "contacts": []}

        search_data = search_response.json()
        people = search_data.get("people", [])

        if not people:
            return {"emails": [], "contacts": []}

        # Step 2: Use bulk_match to reveal emails for found people
        # Build match requests for people who have emails
        match_details = []
        for person in people:
            if person.get("has_email"):
                match_details.append({
                    "first_name": person.get("first_name", ""),
                    "last_name": person.get("last_name", ""),
                    "organization_name": person.get("organization", {}).get("name", ""),
                    "domain": domain
                })

        if not match_details:
            return {"emails": [], "contacts": []}

        # Call bulk_match to reveal emails
        enrich_response = requests.post(
            APOLLO_ENRICH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": api_key
            },
            json={"details": match_details[:5]},  # Limit to 5 to conserve credits
            timeout=15
        )

        emails = []
        contacts = []

        if enrich_response.ok:
            enrich_data = enrich_response.json()
            matches = enrich_data.get("matches", [])

            for match in matches:
                if match is None:
                    continue
                email = match.get("email")
                if email:
                    emails.append(email)
                    name = f"{match.get('first_name', '')} {match.get('last_name', '')}".strip()
                    contacts.append({
                        "name": name,
                        "title": match.get("title", ""),
                        "email": email,
                        "linkedin_url": match.get("linkedin_url", "")
                    })
        else:
            print(f"Apollo enrich error for {domain}: {enrich_response.status_code} - {enrich_response.text[:200]}")

        return {"emails": emails, "contacts": contacts}

    except requests.RequestException as e:
        print(f"Apollo API request failed for {domain}: {e}")
        return {"emails": [], "contacts": []}


def enrich_missing_emails(df):
    """
    Enrich rows that have no emails with Apollo.io data.
    Only uses API credits for rows without emails.
    """
    api_key = os.getenv("APOLLO_API_KEY")

    if not api_key:
        print("Warning: APOLLO_API_KEY not set. Skipping Apollo enrichment.")
        return df

    # Track stats
    rows_needing_enrichment = 0
    rows_enriched = 0

    # Find rows without emails
    for idx, row in df.iterrows():
        existing_emails = parse_list_field(row.get("emails", []))
        if existing_emails:
            continue  # Skip rows that already have emails

        rows_needing_enrichment += 1

    if rows_needing_enrichment == 0:
        print("All rows already have emails. Skipping Apollo enrichment.")
        return df

    print(f"Apollo enrichment: {rows_needing_enrichment} rows need emails")

    # Process rows needing enrichment
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Apollo enrichment"):
        existing_emails = parse_list_field(row.get("emails", []))
        if existing_emails:
            continue  # Skip rows that already have emails

        # Extract domain from website URL
        website_url = row.get("website_url", "")
        domain = extract_domain(website_url)

        if not domain:
            continue

        # Query Apollo.io
        result = search_apollo(domain, api_key)

        if result["emails"]:
            # Update emails column
            df.at[idx, "emails"] = json.dumps(result["emails"])

            # Update contact name if we found one and don't have one
            if result["contacts"] and not row.get("contact_name"):
                first_contact = result["contacts"][0]
                df.at[idx, "contact_name"] = first_contact.get("name", "")
                if first_contact.get("title"):
                    df.at[idx, "contact_position"] = first_contact.get("title", "")

            rows_enriched += 1

    print(f"Apollo enrichment complete: {rows_enriched}/{rows_needing_enrichment} rows enriched")

    return df


if __name__ == "__main__":
    # Test with a sample domain
    api_key = os.getenv("APOLLO_API_KEY")

    if not api_key:
        print("APOLLO_API_KEY not set in environment")
        exit(1)

    test_domains = ["10xevolution.us", "5-x.com", "arialuxerealty.com"]

    for domain in test_domains:
        print(f"\n=== Testing: {domain} ===")
        result = search_apollo(domain, api_key)
        print(f"Emails found: {result['emails']}")
        print(f"Contacts: {json.dumps(result['contacts'], indent=2)}")
