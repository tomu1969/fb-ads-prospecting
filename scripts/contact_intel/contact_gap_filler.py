"""Fill missing company data for contacts using Apollo API.

Finds contacts without WORKS_AT edges and uses Apollo to discover
their company and role information.

Usage:
    python -m scripts.contact_intel.contact_gap_filler
    python -m scripts.contact_intel.contact_gap_filler --status
"""

import argparse
import logging
import os
import time
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('contact_gap_filler.log'),
    ]
)
logger = logging.getLogger(__name__)

# Apollo API
APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/people/match"

# Rate limiting (Apollo allows 50 req/min on free tier)
REQUEST_DELAY = 1.5  # seconds between requests

# Cost tracking
COST_PER_LOOKUP = 0.01  # Estimated cost per Apollo lookup

# Skip these email domains (personal/generic/automated)
SKIP_DOMAINS = {
    # Personal
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
    'protonmail.com', 'zoho.com', 'mail.com', 'ymail.com',
    # Automated/system
    'craigslist.org', 'reply.craigslist.org', 'hous.craigslist.org',
    'upwork.com', 'mg.upwork.com', 'podio.com', 'reply.podio.com',
    'intercom.io', 'intercom-mail.com', 'mailchimp.com', 'sendgrid.net',
    'amazonses.com', 'bounce.linkedin.com', 'linkedin.com',
    'imip.me.com', 'slack.com', 'asana.com', 'trello.com',
    'notifications.google.com', 'noreply.github.com', 'github.com',
    'calendar-server.bounces.google.com',
}

# Regex for valid email format
import re
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
HASH_PATTERN = re.compile(r'[a-f0-9]{20,}')


def extract_domain(email: str) -> Optional[str]:
    """Extract domain from email address."""
    if not email or '@' not in email:
        return None
    return email.split('@')[1].lower()


def lookup_apollo(email: str, name: Optional[str] = None) -> Optional[Dict]:
    """Look up person in Apollo by email.

    Args:
        email: Email address to look up
        name: Optional name for better matching

    Returns:
        Dict with company, role, linkedin_url, or None if not found
    """
    if not APOLLO_API_KEY:
        logger.warning("APOLLO_API_KEY not set")
        return None

    domain = extract_domain(email)

    params = {
        "email": email,
    }

    # Add name if available
    if name:
        parts = name.split()
        if len(parts) >= 2:
            params["first_name"] = parts[0]
            params["last_name"] = " ".join(parts[1:])
        elif len(parts) == 1:
            params["first_name"] = parts[0]

    # Add domain for better matching
    if domain and domain not in SKIP_DOMAINS:
        params["organization_domain"] = domain

    try:
        response = requests.post(
            APOLLO_ENRICH_URL,
            json=params,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": APOLLO_API_KEY,
            },
            timeout=30,
        )

        if response.status_code == 200:
            data = response.json()
            person = data.get("person")

            if person:
                org = person.get("organization", {}) or {}
                return {
                    "company": org.get("name"),
                    "role": person.get("title"),
                    "linkedin_url": person.get("linkedin_url"),
                    "company_domain": org.get("primary_domain"),
                    "company_industry": org.get("industry"),
                }

        elif response.status_code == 429:
            logger.warning("Apollo rate limit hit, waiting...")
            time.sleep(60)  # Wait a minute on rate limit
            return None

        else:
            logger.debug(f"Apollo lookup failed for {email}: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Apollo API error for {email}: {e}")
        return None


def get_contacts_without_company(gb: GraphBuilder, limit: Optional[int] = None) -> List[Dict]:
    """Get Person nodes without WORKS_AT edges.

    Pre-filters in Cypher to exclude obviously invalid emails.

    Args:
        gb: GraphBuilder instance
        limit: Optional limit on results

    Returns:
        List of dicts with email, name
    """
    limit_clause = f"LIMIT {limit}" if limit else ""

    with gb.driver.session() as session:
        # Pre-filter in Cypher for better performance
        result = session.run(f"""
            MATCH (p:Person)
            WHERE NOT (p)-[:WORKS_AT]->(:Company)
            AND p.primary_email IS NOT NULL
            AND p.primary_email =~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{{2,}}$'
            AND NOT p.primary_email CONTAINS 'mailto:'
            AND NOT p.primary_email CONTAINS \"'\"
            AND size(split(p.primary_email, '@')[0]) < 40
            RETURN p.primary_email as email, p.name as name
            ORDER BY rand()
            {limit_clause}
        """)
        return [dict(record) for record in result]


def create_works_at_edge(
    gb: GraphBuilder,
    email: str,
    company_name: str,
    role: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    source: str = 'apollo',
):
    """Create Company node and WORKS_AT edge.

    Args:
        gb: GraphBuilder instance
        email: Person's email
        company_name: Company name
        role: Person's role/title
        linkedin_url: LinkedIn profile URL
        source: Source of the data
    """
    with gb.driver.session() as session:
        # Create/update company
        session.run("""
            MERGE (c:Company {name: $company_name})
            ON CREATE SET c.created_at = datetime()
            SET c.updated_at = datetime()
        """, company_name=company_name)

        # Create WORKS_AT edge
        session.run("""
            MATCH (p:Person {primary_email: $email})
            MATCH (c:Company {name: $company_name})
            MERGE (p)-[r:WORKS_AT]->(c)
            SET r.role = $role,
                r.source = $source,
                r.confidence = 0.8,
                r.created_at = datetime()
        """, email=email, company_name=company_name, role=role, source=source)

        # Update person with LinkedIn URL if found
        if linkedin_url:
            session.run("""
                MATCH (p:Person {primary_email: $email})
                SET p.linkedin_url = $linkedin_url
            """, email=email, linkedin_url=linkedin_url)


def run_gap_filling(limit: Optional[int] = None) -> Dict:
    """Run gap filling for contacts without company data.

    Args:
        limit: Optional limit on contacts to process

    Returns:
        Stats dict
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not set")
        return {'error': 'APOLLO_API_KEY not set'}

    gb = GraphBuilder()
    gb.connect()

    stats = {
        'total': 0,
        'enriched': 0,
        'skipped_personal': 0,
        'skipped_invalid': 0,
        'not_found': 0,
        'errors': 0,
        'api_calls': 0,
        'cost_usd': 0.0,
    }

    try:
        # Get contacts without company
        contacts = get_contacts_without_company(gb, limit)
        stats['total'] = len(contacts)
        logger.info(f"Found {len(contacts)} contacts without company data")

        if not contacts:
            logger.info("No contacts to process")
            return stats

        for i, contact in enumerate(contacts):
            email = contact['email']
            name = contact['name']
            domain = extract_domain(email)

            # Validate email format
            if not EMAIL_PATTERN.match(email):
                logger.debug(f"Skipping malformed email: {email}")
                stats['skipped_invalid'] += 1
                continue

            # Skip hash-like usernames (automated emails)
            username = email.split('@')[0]
            if HASH_PATTERN.search(username) or len(username) > 40:
                logger.debug(f"Skipping automated email: {email}")
                stats['skipped_invalid'] += 1
                continue

            # Skip personal/automated email domains
            if domain in SKIP_DOMAINS:
                logger.debug(f"Skipping personal/automated email: {email}")
                stats['skipped_personal'] += 1
                continue

            # Rate limiting
            time.sleep(REQUEST_DELAY)

            # Look up in Apollo
            try:
                result = lookup_apollo(email, name)
                stats['api_calls'] += 1
                stats['cost_usd'] += COST_PER_LOOKUP

                if result and result.get('company'):
                    create_works_at_edge(
                        gb,
                        email=email,
                        company_name=result['company'],
                        role=result.get('role'),
                        linkedin_url=result.get('linkedin_url'),
                    )
                    stats['enriched'] += 1
                    logger.info(f"[{i + 1}/{len(contacts)}] {email} -> {result['company']}")
                else:
                    stats['not_found'] += 1
                    logger.debug(f"[{i + 1}/{len(contacts)}] {email} -> not found")

            except Exception as e:
                logger.error(f"Error processing {email}: {e}")
                stats['errors'] += 1

            # Progress log every 50 contacts
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(contacts)} contacts processed")
                logger.info(f"Stats so far: enriched={stats['enriched']}, not_found={stats['not_found']}")

    finally:
        gb.close()

    logger.info(f"Gap filling complete: {stats}")
    return stats


def show_status():
    """Show gap filling status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count contacts with/without company
            result = session.run("""
                MATCH (p:Person)
                OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
                RETURN
                    count(DISTINCT p) as total_contacts,
                    count(DISTINCT c) as with_company,
                    count(DISTINCT CASE WHEN c IS NULL THEN p END) as without_company
            """)
            counts = result.single()

            # Count by email domain type
            result = session.run("""
                MATCH (p:Person)
                WHERE NOT (p)-[:WORKS_AT]->(:Company)
                WITH p, split(p.primary_email, '@')[1] as domain
                WITH domain,
                     CASE WHEN domain IN ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com'] THEN 'personal' ELSE 'business' END as domain_type
                RETURN domain_type, count(*) as count
            """)
            domain_counts = {'personal': 0, 'business': 0}
            for record in result:
                domain_counts[record['domain_type']] = record['count']

            # Source of WORKS_AT edges
            result = session.run("""
                MATCH ()-[r:WORKS_AT]->()
                RETURN r.source as source, count(*) as count
                ORDER BY count DESC
            """)
            sources = [dict(record) for record in result]

        print("\n" + "=" * 50)
        print("CONTACT GAP FILLING STATUS")
        print("=" * 50)
        print(f"Total contacts:           {counts['total_contacts']:,}")
        print(f"With company (WORKS_AT):  {counts['with_company']:,}")
        print(f"Without company:          {counts['without_company']:,}")

        if counts['without_company'] > 0:
            print(f"\nContacts without company:")
            print(f"  Personal email domains: {domain_counts['personal']:,} (will skip)")
            print(f"  Business email domains: {domain_counts['business']:,} (can enrich)")

        if sources:
            print(f"\nWORKS_AT edge sources:")
            for row in sources:
                source = row['source'] or 'unknown'
                print(f"  {source:20} {row['count']:,}")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fill missing company data for contacts using Apollo API'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show gap filling status')
    parser.add_argument('--run', action='store_true',
                        help='Run gap filling')
    parser.add_argument('--limit', type=int,
                        help='Limit number of contacts to process')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.run:
        stats = run_gap_filling(limit=args.limit)
        print("\n" + "=" * 50)
        print("CONTACT GAP FILLING RESULTS")
        print("=" * 50)
        print(f"Total contacts:           {stats.get('total', 0):,}")
        print(f"Enriched:                 {stats.get('enriched', 0):,}")
        print(f"Skipped (personal email): {stats.get('skipped_personal', 0):,}")
        print(f"Skipped (invalid format): {stats.get('skipped_invalid', 0):,}")
        print(f"Not found:                {stats.get('not_found', 0):,}")
        print(f"API calls:                {stats.get('api_calls', 0)}")
        print(f"Estimated cost:           ${stats.get('cost_usd', 0):.2f}")
        print(f"Errors:                   {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
