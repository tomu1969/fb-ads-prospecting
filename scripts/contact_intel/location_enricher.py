"""Location enrichment for contact intelligence graph.

Layered enrichment pipeline:
  Layer 1 (ccTLD):       Infer country from email domain TLD (~4,300 nodes, free)
  Layer 2 (Company Geo): Extract city/country from company name patterns (~130 nodes, free)
  Layer 3 (Apollo API):  Full city/state/country via Apollo people/match (~$0.02/call)

Usage:
    python -m scripts.contact_intel.location_enricher --status
    python -m scripts.contact_intel.location_enricher --all --dry-run
    python -m scripts.contact_intel.location_enricher --all
    python -m scripts.contact_intel.location_enricher --apollo --target-industry "Real Estate" --dry-run
"""

import argparse
import logging
import os
import re
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ============================================================
# ccTLD → Country mapping
# ============================================================

CCTLD_COUNTRY = {
    # Latin America
    'co': 'Colombia',
    'com.co': 'Colombia',
    'edu.co': 'Colombia',
    'gov.co': 'Colombia',
    'mx': 'Mexico',
    'com.mx': 'Mexico',
    'br': 'Brazil',
    'com.br': 'Brazil',
    'pe': 'Peru',
    'com.pe': 'Peru',
    'cl': 'Chile',
    'ar': 'Argentina',
    'com.ar': 'Argentina',
    # Europe
    'es': 'Spain',
    'uk': 'United Kingdom',
    'co.uk': 'United Kingdom',
    # US-specific TLDs
    'edu': 'United States',
    'gov': 'United States',
}

# Generic TLDs that should NOT map to a country
GENERIC_TLDS = {'com', 'net', 'org', 'io', 'info', 'biz', 'app', 'dev'}


def cctld_to_country(tld: str) -> Optional[str]:
    """Look up a TLD (without leading dot) in the ccTLD→Country map.

    Args:
        tld: TLD string without leading dot, e.g. 'co', 'com.co', 'edu'.

    Returns:
        Country name string, or None if generic/unknown.
    """
    if not tld:
        return None
    tld = tld.lower().strip('.')
    return CCTLD_COUNTRY.get(tld)


def extract_country_from_email(email: str) -> Optional[str]:
    """Infer country from email domain ccTLD.

    Returns None for generic TLDs (.com, .net, .org).
    Handles compound TLDs: .com.co → Colombia, .edu.co → Colombia.

    Args:
        email: Email address string.

    Returns:
        Country name string, or None if cannot be determined.
    """
    if not email or '@' not in email:
        return None

    domain = email.split('@', 1)[1].lower()
    parts = domain.split('.')

    if len(parts) < 2:
        return None

    # Try compound TLD first (last 2 parts): e.g., com.co, co.uk, edu.co
    if len(parts) >= 3:
        compound_tld = '.'.join(parts[-2:])
        country = CCTLD_COUNTRY.get(compound_tld)
        if country is not None:
            return country

    # Fall back to single TLD (last part): e.g., co, mx, edu
    single_tld = parts[-1]
    if single_tld in GENERIC_TLDS:
        return None

    return CCTLD_COUNTRY.get(single_tld)


# ============================================================
# Neo4j enrichment
# ============================================================

FIND_EMAILS_QUERY = """
MATCH (p:Person)
WHERE p.country IS NULL
  AND p.primary_email IS NOT NULL
  AND p.primary_email CONTAINS '@'
  AND NOT p.primary_email STARTS WITH 'li://'
RETURN p.primary_email as email
"""

SET_COUNTRY_QUERY = """
MATCH (p:Person {primary_email: $email})
WHERE p.country IS NULL
SET p.country = $country, p.location_source = 'cctld'
"""


def enrich_cctld(session) -> int:
    """Enrich Person nodes with country inferred from email ccTLD.

    Sets country and location_source='cctld' on Person nodes
    where country IS NULL and the email domain has a recognizable ccTLD.

    Args:
        session: Neo4j session object.

    Returns:
        Number of Person nodes enriched.
    """
    result = session.run(FIND_EMAILS_QUERY)
    emails = [record['email'] for record in result]

    logger.info(f"Found {len(emails)} Person nodes without country")

    enriched = 0
    for email in emails:
        country = extract_country_from_email(email)
        if country:
            session.run(SET_COUNTRY_QUERY, email=email, country=country)
            enriched += 1
            logger.debug(f"  {email} → {country}")

    logger.info(f"Enriched {enriched}/{len(emails)} Person nodes via ccTLD")
    return enriched


# ============================================================
# Slice 2: Company Name → City/Country geo extraction
# ============================================================

CITY_GEO = {
    r'medell[ií]n': {'city': 'Medellín', 'state': 'Antioquia', 'country': 'Colombia'},
    r'bogot[áa]': {'city': 'Bogotá', 'country': 'Colombia'},
    r'miami': {'city': 'Miami', 'state': 'Florida', 'country': 'United States'},
    r'new york|nyc': {'city': 'New York', 'state': 'New York', 'country': 'United States'},
    r'lima': {'city': 'Lima', 'country': 'Peru'},
    r'mexico city|ciudad de m[ée]xico|cdmx': {'city': 'Mexico City', 'country': 'Mexico'},
    r'santiago': {'city': 'Santiago', 'country': 'Chile'},
    r's[ãa]o paulo': {'city': 'São Paulo', 'country': 'Brazil'},
    r'buenos aires': {'city': 'Buenos Aires', 'country': 'Argentina'},
    r'cartagena': {'city': 'Cartagena', 'country': 'Colombia'},
    r'barranquilla': {'city': 'Barranquilla', 'country': 'Colombia'},
    r'\bcali\b': {'city': 'Cali', 'country': 'Colombia'},
}

COUNTRY_GEO = {
    r'colombia\b': 'Colombia',
    r'\bm[ée]xico\b': 'Mexico',
    r'\bbrasil\b|\bbrazil\b': 'Brazil',
    r'\bper[úu]\b': 'Peru',
    r'\bchile\b': 'Chile',
    r'\bargentina\b': 'Argentina',
}


def extract_geo_from_company(company_name: str) -> Dict[str, str]:
    """Extract city/state/country from company name patterns.

    City patterns checked first (more specific), then country patterns.
    Returns empty dict if no match.

    Args:
        company_name: Company name string to extract geo from.

    Returns:
        Dict with keys like 'city', 'state', 'country' if matched, else {}.
    """
    if not company_name:
        return {}

    name_lower = company_name.lower()

    # Check city patterns first (more specific)
    for pattern, geo in CITY_GEO.items():
        if re.search(pattern, name_lower):
            return dict(geo)

    # Fall back to country patterns
    for pattern, country in COUNTRY_GEO.items():
        if re.search(pattern, name_lower):
            return {'country': country}

    return {}


# ============================================================
# Neo4j enrichment — Company Geo
# ============================================================

FIND_COMPANY_GEO_QUERY = """
MATCH (p:Person)
WHERE (p.primary_email STARTS WITH 'li://' OR p.primary_email STARTS WITH 'li-name://')
  AND p.linkedin_company IS NOT NULL
  AND p.city IS NULL
RETURN p.primary_email as email, p.linkedin_company as company
"""

SET_COMPANY_GEO_QUERY = """
MATCH (p:Person {primary_email: $email})
SET p.city = CASE WHEN p.city IS NULL THEN $city ELSE p.city END,
    p.state = CASE WHEN p.state IS NULL THEN $state ELSE p.state END,
    p.country = CASE WHEN p.country IS NULL THEN $country ELSE p.country END,
    p.location_source = CASE WHEN p.location_source IS NULL THEN 'company_geo' ELSE p.location_source END
"""


def enrich_company_geo(session) -> int:
    """SET city/state/country from linkedin_company on synthetic nodes.

    Only sets fields that are currently NULL (never overwrites).
    Sets location_source='company_geo' on enriched nodes.

    Args:
        session: Neo4j session object.

    Returns:
        Number of Person nodes enriched.
    """
    result = session.run(FIND_COMPANY_GEO_QUERY)
    records = [{'email': record['email'], 'company': record['company']} for record in result]

    logger.info(f"Found {len(records)} synthetic Person nodes with company, no city")

    enriched = 0
    for record in records:
        geo = extract_geo_from_company(record['company'])
        if geo:
            session.run(
                SET_COMPANY_GEO_QUERY,
                email=record['email'],
                city=geo.get('city'),
                state=geo.get('state'),
                country=geo.get('country'),
            )
            enriched += 1
            logger.debug(f"  {record['email']} ({record['company']}) → {geo}")

    logger.info(f"Enriched {enriched}/{len(records)} synthetic nodes via company geo")
    return enriched


# ============================================================
# Slice 3: Apollo API → Full Location enrichment
# ============================================================

APOLLO_COST_PER_CALL = 0.02

FIND_APOLLO_TARGETS_QUERY = """
MATCH (p:Person)
WHERE p.city IS NULL
  AND p.linkedin_url IS NOT NULL
  AND (p.primary_email STARTS WITH 'li://' OR p.primary_email STARTS WITH 'li-name://')
WITH p
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
WHERE $industry IS NULL OR c.industry = $industry
WITH p, c
WHERE $industry IS NULL OR c IS NOT NULL
RETURN p.primary_email as email, p.linkedin_url as url, p.linkedin_company as company
"""

SET_APOLLO_GEO_QUERY = """
MATCH (p:Person {primary_email: $email})
SET p.city = CASE WHEN p.city IS NULL AND $city IS NOT NULL THEN $city ELSE p.city END,
    p.state = CASE WHEN p.state IS NULL AND $state IS NOT NULL THEN $state ELSE p.state END,
    p.country = CASE WHEN p.country IS NULL AND $country IS NOT NULL THEN $country ELSE p.country END,
    p.location_source = CASE WHEN p.location_source IS NULL THEN 'apollo' ELSE p.location_source END
"""


def get_location_from_apollo(linkedin_url: str, api_key: str = None) -> Dict[str, str]:
    """Query Apollo people/match by linkedin_url.

    Returns {city, state, country} or empty dict on failure.

    API endpoint: POST https://api.apollo.io/api/v1/people/match
    Body: {"linkedin_url": linkedin_url}
    Header: {"x-api-key": api_key}

    Response fields of interest:
    - person.city
    - person.state
    - person.country

    Args:
        linkedin_url: LinkedIn profile URL.
        api_key: Apollo API key. Falls back to APOLLO_API_KEY env var.

    Returns:
        Dict with keys like 'city', 'state', 'country' if found, else {}.
    """
    if not api_key:
        api_key = os.getenv('APOLLO_API_KEY')
    if not api_key:
        logger.error("No Apollo API key provided or found in APOLLO_API_KEY env var")
        return {}

    try:
        response = requests.post(
            'https://api.apollo.io/api/v1/people/match',
            json={'linkedin_url': linkedin_url},
            headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        person = data.get('person', {})
        if not person:
            return {}

        result = {}
        if person.get('city'):
            result['city'] = person['city']
        if person.get('state'):
            result['state'] = person['state']
        if person.get('country'):
            result['country'] = person['country']
        return result

    except Exception as e:
        logger.error(f"Apollo API error for {linkedin_url}: {e}")
        return {}


def enrich_apollo(session, industry_filter: str = None,
                  limit: int = None, dry_run: bool = True) -> Dict:
    """Enrich Person nodes via Apollo API.

    Queries Apollo people/match for LinkedIn-only nodes that lack city,
    and sets city/state/country + location_source='apollo'.

    Args:
        session: Neo4j session object.
        industry_filter: Only enrich nodes at companies in this industry.
        limit: Max API calls to make.
        dry_run: Preview without making API calls (default True for safety).

    Returns:
        Dict with: enriched, skipped, errors, api_calls, estimated_cost.
    """
    api_key = os.getenv('APOLLO_API_KEY')
    stats = {'enriched': 0, 'skipped': 0, 'errors': 0, 'api_calls': 0, 'estimated_cost': 0.0}

    result = session.run(FIND_APOLLO_TARGETS_QUERY, industry=industry_filter)
    targets = [{'email': r['email'], 'url': r['url'], 'company': r['company']} for r in result]

    if limit:
        targets = targets[:limit]

    logger.info(f"Found {len(targets)} Apollo targets (dry_run={dry_run})")

    if dry_run:
        stats['skipped'] = len(targets)
        for t in targets:
            logger.info(f"  [DRY RUN] Would query Apollo for {t['url']} ({t['company']})")
        return stats

    for i, target in enumerate(targets):
        try:
            geo = get_location_from_apollo(target['url'], api_key=api_key)
            stats['api_calls'] += 1
            stats['estimated_cost'] += APOLLO_COST_PER_CALL

            if geo:
                session.run(
                    SET_APOLLO_GEO_QUERY,
                    email=target['email'],
                    city=geo.get('city'),
                    state=geo.get('state'),
                    country=geo.get('country'),
                )
                stats['enriched'] += 1
                logger.info(
                    f"  [{i + 1}/{len(targets)}] {target['email']} → {geo}"
                )
            else:
                stats['skipped'] += 1
                logger.debug(
                    f"  [{i + 1}/{len(targets)}] {target['email']} — no location from Apollo"
                )

            # Rate limiting between API calls
            time.sleep(0.5)

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"  [{i + 1}/{len(targets)}] Error enriching {target['email']}: {e}")

    logger.info(
        f"Apollo enrichment complete: {stats['enriched']} enriched, "
        f"{stats['skipped']} skipped, {stats['errors']} errors, "
        f"{stats['api_calls']} API calls (${stats['estimated_cost']:.2f})"
    )
    return stats


# ============================================================
# CLI Orchestration
# ============================================================

STATUS_QUERY = """
MATCH (p:Person)
RETURN
  count(p) as total,
  count(p.country) as with_country,
  count(p.city) as with_city,
  count(p.state) as with_state
"""

STATUS_BY_SOURCE_QUERY = """
MATCH (p:Person)
WHERE p.location_source IS NOT NULL
RETURN p.location_source as source, count(p) as count
ORDER BY count DESC
"""

STATUS_TOP_COUNTRIES_QUERY = """
MATCH (p:Person)
WHERE p.country IS NOT NULL
RETURN p.country as country, count(p) as count
ORDER BY count DESC
LIMIT 10
"""

STATUS_TOP_CITIES_QUERY = """
MATCH (p:Person)
WHERE p.city IS NOT NULL
RETURN p.city as city, count(p) as count
ORDER BY count DESC
LIMIT 10
"""


def show_status(session):
    """Show location enrichment coverage statistics."""
    # Overall coverage
    result = session.run(STATUS_QUERY)
    record = result.single()
    total = record['total']
    with_country = record['with_country']
    with_city = record['with_city']
    with_state = record['with_state']

    logger.info("\n=== Location Enrichment Status ===")
    logger.info(f"  Total Person nodes:     {total:,}")
    logger.info(f"  With country:           {with_country:,} ({100*with_country/total:.1f}%)" if total else "  With country:           0")
    logger.info(f"  With city:              {with_city:,} ({100*with_city/total:.1f}%)" if total else "  With city:              0")
    logger.info(f"  With state:             {with_state:,} ({100*with_state/total:.1f}%)" if total else "  With state:             0")

    # By source
    result = session.run(STATUS_BY_SOURCE_QUERY)
    records = list(result)
    if records:
        logger.info("\n  By source:")
        for r in records:
            logger.info(f"    {r['source']}: {r['count']:,}")

    # Top countries
    result = session.run(STATUS_TOP_COUNTRIES_QUERY)
    records = list(result)
    if records:
        logger.info("\n  Top countries:")
        for r in records:
            logger.info(f"    {r['country']}: {r['count']:,}")

    # Top cities
    result = session.run(STATUS_TOP_CITIES_QUERY)
    records = list(result)
    if records:
        logger.info("\n  Top cities:")
        for r in records:
            logger.info(f"    {r['city']}: {r['count']:,}")


def run_enrichment(session, cctld=False, company_geo=False, apollo=False,
                   all_layers=False, industry_filter=None,
                   limit=None, dry_run=True):
    """Run location enrichment pipeline.

    Args:
        session: Neo4j session
        cctld: Run ccTLD layer
        company_geo: Run company geo layer
        apollo: Run Apollo API layer
        all_layers: Run all free layers (ccTLD + company geo)
        industry_filter: Filter Apollo by industry
        limit: Max nodes to enrich (Apollo only)
        dry_run: Preview only
    """
    results = {}

    if cctld or all_layers:
        logger.info("\n--- Layer 1: ccTLD → Country ---")
        count = enrich_cctld(session)
        results['cctld'] = count

    if company_geo or all_layers:
        logger.info("\n--- Layer 2: Company Name → City/Country ---")
        count = enrich_company_geo(session)
        results['company_geo'] = count

    if apollo:
        logger.info("\n--- Layer 3: Apollo API → Full Location ---")
        stats = enrich_apollo(
            session,
            industry_filter=industry_filter,
            limit=limit,
            dry_run=dry_run,
        )
        results['apollo'] = stats

    logger.info("\n=== Enrichment Summary ===")
    for layer, result in results.items():
        if isinstance(result, dict):
            logger.info(f"  {layer}: {result['enriched']} enriched, {result['api_calls']} API calls (${result['estimated_cost']:.2f})")
        else:
            logger.info(f"  {layer}: {result} nodes enriched")

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Location enrichment for contact intelligence graph'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show enrichment coverage')
    parser.add_argument('--all', action='store_true',
                        help='Run all free layers (ccTLD + company geo)')
    parser.add_argument('--cctld', action='store_true',
                        help='Layer 1: ccTLD country inference')
    parser.add_argument('--company-geo', action='store_true',
                        help='Layer 2: Company name geo patterns')
    parser.add_argument('--apollo', action='store_true',
                        help='Layer 3: Apollo API lookup')
    parser.add_argument('--target-industry', type=str, default=None,
                        help='Filter Apollo by company industry (e.g., "Real Estate")')
    parser.add_argument('--limit', type=int, default=None,
                        help='Max nodes to enrich (Apollo only)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview only, no changes')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('location_enricher.log'),
        ]
    )

    if not any([args.status, args.all, args.cctld, args.company_geo, args.apollo]):
        parser.print_help()
        return

    from scripts.contact_intel.graph_builder import GraphBuilder

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            if args.status:
                show_status(session)

            if args.all or args.cctld or args.company_geo or args.apollo:
                run_enrichment(
                    session,
                    cctld=args.cctld,
                    company_geo=args.company_geo,
                    apollo=args.apollo,
                    all_layers=args.all,
                    industry_filter=args.target_industry,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
    finally:
        gb.close()


if __name__ == '__main__':
    main()
