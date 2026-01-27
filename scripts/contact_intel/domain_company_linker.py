"""Link contacts to companies based on email domain.

Free alternative to API lookups - infers company from email domain.
Creates WORKS_AT edges with source='domain_inferred'.

Usage:
    python -m scripts.contact_intel.domain_company_linker --run
    python -m scripts.contact_intel.domain_company_linker --status
"""

import argparse
import logging
import re
from typing import Dict, List, Optional, Set

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('domain_company_linker.log'),
    ]
)
logger = logging.getLogger(__name__)

# Personal/generic email domains to skip
SKIP_DOMAINS = {
    # Personal email providers
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
    'protonmail.com', 'zoho.com', 'mail.com', 'ymail.com', 'inbox.com',
    'fastmail.com', 'tutanota.com', 'pm.me', 'hey.com',
    # Regional personal email
    'qq.com', '163.com', '126.com', 'sina.com', 'naver.com',
    'daum.net', 'hanmail.net', 'web.de', 'gmx.de', 'gmx.net',
    't-online.de', 'orange.fr', 'free.fr', 'laposte.net',
    # ISP/telecom (often personal)
    'comcast.net', 'verizon.net', 'att.net', 'sbcglobal.net',
    'cox.net', 'charter.net', 'earthlink.net', 'bellsouth.net',
    # Automated/system domains
    'craigslist.org', 'reply.craigslist.org', 'hous.craigslist.org',
    'upwork.com', 'mg.upwork.com', 'fiverr.com',
    'linkedin.com', 'bounce.linkedin.com', 'facebookmail.com',
    'slack.com', 'asana.com', 'trello.com', 'notion.so',
    'intercom.io', 'intercom-mail.com', 'zendesk.com',
    'mailchimp.com', 'sendgrid.net', 'amazonses.com',
    'mailgun.org', 'postmarkapp.com', 'sparkpostmail.com',
    'github.com', 'noreply.github.com', 'gitlab.com',
    'calendar-server.bounces.google.com', 'notifications.google.com',
    'docusign.net', 'hellosign.com',
}

# Valid email pattern
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def extract_domain(email: str) -> Optional[str]:
    """Extract domain from email address."""
    if not email or '@' not in email:
        return None
    return email.split('@')[1].lower()


def clean_company_name(domain: str) -> str:
    """Convert domain to a cleaner company name.

    Examples:
        cbre.com -> CBRE
        goldmansachs.com -> Goldman Sachs
        jpmorgan.com -> JPMorgan
    """
    # Remove TLD
    name = domain.rsplit('.', 1)[0]

    # Remove common subdomains
    for prefix in ['mail.', 'email.', 'smtp.', 'mx.', 'www.']:
        if name.startswith(prefix):
            name = name[len(prefix):]

    # Handle .co domains (e.g., company.co.uk -> company)
    if '.' in name:
        parts = name.split('.')
        # If it's like "company.co" or "company.com", take first part
        if parts[-1] in ['co', 'com', 'net', 'org', 'edu']:
            name = parts[0]
        else:
            # Otherwise join with spaces (e.g., sub.company -> sub company)
            name = ' '.join(parts)

    # Capitalize
    # Special cases for known acronyms
    acronyms = {'ibm', 'cbre', 'jll', 'pwc', 'ey', 'kpmg', 'hsbc', 'ups', 'dhl',
                'bmw', 'sap', 'hbo', 'cnn', 'bbc', 'nbc', 'abc', 'espn', 'mit',
                'ucla', 'usc', 'nyu', 'kkr', 'bcg'}

    if name.lower() in acronyms:
        return name.upper()

    # Title case for normal names
    return name.title()


def get_contacts_without_company(gb: GraphBuilder) -> List[Dict]:
    """Get contacts without WORKS_AT edges, with valid business email domains."""
    with gb.driver.session() as session:
        result = session.run("""
            MATCH (p:Person)
            WHERE NOT (p)-[:WORKS_AT]->(:Company)
            AND p.primary_email IS NOT NULL
            AND p.primary_email =~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'
            RETURN p.primary_email as email, p.name as name
        """)
        return [dict(record) for record in result]


def link_contact_to_company(
    gb: GraphBuilder,
    email: str,
    domain: str,
    company_name: str,
):
    """Create Company node and WORKS_AT edge based on domain."""
    with gb.driver.session() as session:
        # Create/merge company by domain (use domain as unique identifier)
        session.run("""
            MERGE (c:Company {domain: $domain})
            ON CREATE SET
                c.name = $company_name,
                c.created_at = datetime(),
                c.source = 'domain_inferred'
            SET c.updated_at = datetime()
        """, domain=domain, company_name=company_name)

        # Create WORKS_AT edge
        session.run("""
            MATCH (p:Person {primary_email: $email})
            MATCH (c:Company {domain: $domain})
            MERGE (p)-[r:WORKS_AT]->(c)
            ON CREATE SET
                r.source = 'domain_inferred',
                r.confidence = 0.6,
                r.created_at = datetime()
        """, email=email, domain=domain)


def run_domain_linking() -> Dict:
    """Link contacts to companies based on email domain.

    Returns:
        Stats dict with counts
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    gb = GraphBuilder()
    gb.connect()

    stats = {
        'total': 0,
        'linked': 0,
        'skipped_personal': 0,
        'skipped_invalid': 0,
        'companies_created': 0,
        'errors': 0,
    }

    # Track new companies
    seen_domains: Set[str] = set()

    try:
        # Get contacts without company
        contacts = get_contacts_without_company(gb)
        stats['total'] = len(contacts)
        logger.info(f"Found {len(contacts)} contacts without company")

        if not contacts:
            return stats

        # Get existing company domains
        with gb.driver.session() as session:
            result = session.run("""
                MATCH (c:Company)
                WHERE c.domain IS NOT NULL
                RETURN c.domain as domain
            """)
            existing_domains = {r['domain'] for r in result}

        for i, contact in enumerate(contacts):
            email = contact['email']

            # Validate email format
            if not EMAIL_PATTERN.match(email):
                stats['skipped_invalid'] += 1
                continue

            # Extract domain
            domain = extract_domain(email)
            if not domain:
                stats['skipped_invalid'] += 1
                continue

            # Skip personal/automated domains
            if domain in SKIP_DOMAINS:
                stats['skipped_personal'] += 1
                continue

            # Also skip if domain looks like a subdomain of skip domains
            base_domain = '.'.join(domain.split('.')[-2:])
            if base_domain in SKIP_DOMAINS:
                stats['skipped_personal'] += 1
                continue

            try:
                # Get clean company name
                company_name = clean_company_name(domain)

                # Track if this is a new company
                if domain not in existing_domains and domain not in seen_domains:
                    stats['companies_created'] += 1
                    seen_domains.add(domain)

                # Link contact to company
                link_contact_to_company(gb, email, domain, company_name)
                stats['linked'] += 1

            except Exception as e:
                logger.error(f"Error linking {email}: {e}")
                stats['errors'] += 1

            # Progress log
            if (i + 1) % 1000 == 0:
                logger.info(f"Progress: {i + 1}/{len(contacts)} contacts processed")

    finally:
        gb.close()

    logger.info(f"Domain linking complete: {stats}")
    return stats


def show_status():
    """Show domain linking status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count WORKS_AT by source
            result = session.run("""
                MATCH ()-[r:WORKS_AT]->()
                RETURN r.source as source, count(*) as count
                ORDER BY count DESC
            """)
            sources = [dict(r) for r in result]

            # Count companies by source
            result = session.run("""
                MATCH (c:Company)
                RETURN c.source as source, count(*) as count
                ORDER BY count DESC
            """)
            company_sources = [dict(r) for r in result]

            # Count contacts with/without company
            result = session.run("""
                MATCH (p:Person)
                OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
                RETURN
                    count(DISTINCT p) as total,
                    count(DISTINCT c) as with_company
            """)
            counts = result.single()

            # Sample domain-inferred companies
            result = session.run("""
                MATCH (c:Company)
                WHERE c.source = 'domain_inferred'
                RETURN c.name as name, c.domain as domain
                LIMIT 10
            """)
            samples = [dict(r) for r in result]

        print("\n" + "=" * 50)
        print("DOMAIN COMPANY LINKING STATUS")
        print("=" * 50)
        print(f"Total contacts:           {counts['total']:,}")
        print(f"With company (WORKS_AT):  {counts['with_company']:,}")
        print(f"Without company:          {counts['total'] - counts['with_company']:,}")

        print(f"\nWORKS_AT edges by source:")
        for row in sources:
            source = row['source'] or 'unknown'
            print(f"  {source:20} {row['count']:,}")

        print(f"\nCompanies by source:")
        for row in company_sources:
            source = row['source'] or 'unknown'
            print(f"  {source:20} {row['count']:,}")

        if samples:
            print(f"\nSample domain-inferred companies:")
            for s in samples:
                print(f"  {s['domain']:30} -> {s['name']}")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Link contacts to companies based on email domain'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show linking status')
    parser.add_argument('--run', action='store_true',
                        help='Run domain linking')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.run:
        stats = run_domain_linking()
        print("\n" + "=" * 50)
        print("DOMAIN COMPANY LINKING RESULTS")
        print("=" * 50)
        print(f"Total contacts processed: {stats.get('total', 0):,}")
        print(f"Linked to companies:      {stats.get('linked', 0):,}")
        print(f"New companies created:    {stats.get('companies_created', 0):,}")
        print(f"Skipped (personal):       {stats.get('skipped_personal', 0):,}")
        print(f"Skipped (invalid):        {stats.get('skipped_invalid', 0):,}")
        print(f"Errors:                   {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
