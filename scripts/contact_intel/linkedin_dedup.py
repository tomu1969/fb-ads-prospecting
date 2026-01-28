"""LinkedIn ↔ Email Graph Deduplication.

Merges synthetic LinkedIn Person nodes (li://) onto existing email-based Person nodes
when there's a high-confidence match. Conservative: only merges on normalized full-name
match, never on first+last partial matching (too many false positives with Latin names).

V2 additions:
  - Email-prefix name enrichment (backfill nameless nodes from email prefixes)
  - Name normalization (strip suffixes, accents, handle Last/First format)
  - Domain-company matching (Tier 3)

Tiers:
  1. Case-insensitive exact name, 1:1 real-node match → auto-merge
  2. Case-insensitive exact name, 1:many, with linkedin_company tie-break → merge if
     exactly one candidate company matches
  3. Name match + email domain matches LinkedIn company (new in V2)

Usage:
    python -m scripts.contact_intel.linkedin_dedup --dry-run      # preview only
    python -m scripts.contact_intel.linkedin_dedup --merge         # execute merges
    python -m scripts.contact_intel.linkedin_dedup --enrich-only   # just backfill names
    python -m scripts.contact_intel.linkedin_dedup --report        # generate CSV report
"""

import argparse
import csv
import logging
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('linkedin_dedup.log'),
    ],
)
logger = logging.getLogger(__name__)

REPORT_PATH = Path('output/linkedin_dedup_report.csv')

# Relationship types to transfer from synthetic → real node
EDGE_TYPES = [
    'LINKEDIN_CONNECTED',
    'LINKEDIN_MESSAGED',
    'ENDORSED',
]

# Generic email prefixes that don't indicate a person's name
GENERIC_PREFIXES = {
    'info', 'support', 'admin', 'noreply', 'no-reply', 'hello', 'contact',
    'sales', 'office', 'team', 'help', 'service', 'billing', 'webmaster',
    'postmaster', 'mailer-daemon', 'root', 'abuse', 'security', 'marketing',
    'newsletter', 'feedback', 'enquiry', 'inquiry', 'orders', 'accounts',
    'hr', 'jobs', 'careers', 'press', 'media', 'legal', 'compliance',
}

# Generic email domains — never use for company matching
GENERIC_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com',
    'aol.com', 'live.com', 'me.com', 'msn.com', 'mail.com',
    'protonmail.com', 'zoho.com', 'ymail.com', 'googlemail.com',
    'comcast.net', 'att.net', 'verizon.net', 'sbcglobal.net', 'cox.net',
    'bellsouth.net', 'earthlink.net', 'mac.com',
}

# Professional suffixes to strip during name normalization
SUFFIXES = {
    'cfa', 'mba', 'phd', 'md', 'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv',
    'cpa', 'esq', 'pe', 'pmp', 'ceo', 'cto', 'cfo', 'coo', 'dds', 'dvm',
    'rn', 'bsn', 'msn', 'lcsw', 'cfp', 'chfc', 'clc', 'cpcc', 'acc',
    'pcc', 'mcc', 'sphr', 'shrm-cp', 'shrm-scp',
}


# ============================================================
# Slice 1: Email-prefix name extraction
# ============================================================


def extract_name_from_email(email: Optional[str]) -> Optional[str]:
    """Derive probable name from email prefix.

    'john.smith@company.com' → 'John Smith'
    'info@company.com' → None (generic)
    'john@company.com' → None (single word, insufficient signal)
    """
    if not email or not isinstance(email, str):
        return None

    # Skip synthetic emails
    if email.startswith('li://') or email.startswith('li-name://'):
        return None

    # Must have @
    if '@' not in email:
        return None

    prefix = email.split('@')[0].lower()

    # Skip generic prefixes
    if prefix in GENERIC_PREFIXES:
        return None

    # Split on common separators: dot, underscore, hyphen
    parts = re.split(r'[._\-]', prefix)

    # Strip trailing numbers from each part
    parts = [re.sub(r'\d+$', '', p) for p in parts]

    # Filter empty parts and pure-numeric parts
    parts = [p for p in parts if p and not p.isdigit()]

    # Need at least 2 parts for a first+last name
    if len(parts) < 2:
        return None

    # Check if any part is all-numeric (no name signal left)
    if all(p.isdigit() for p in parts):
        return None

    # Title-case each part
    name = ' '.join(p.capitalize() for p in parts)
    return name


def enrich_nameless_nodes(session) -> int:
    """Set derived names on Person nodes that have no name.

    Uses email prefix extraction. Never overwrites existing names.
    Marks source as 'email_prefix' for auditability.

    Returns count of enriched nodes.
    """
    result = session.run("""
        MATCH (p:Person)
        WHERE p.name IS NULL
          AND p.primary_email IS NOT NULL
          AND NOT p.primary_email STARTS WITH 'li://'
          AND NOT p.primary_email STARTS WITH 'li-name://'
        RETURN p.primary_email as email
    """)

    enriched = 0
    for record in result:
        email = record['email']
        name = extract_name_from_email(email)
        if name:
            session.run("""
                MATCH (p:Person {primary_email: $email})
                WHERE p.name IS NULL
                SET p.name = $name, p.name_source = 'email_prefix'
            """, email=email, name=name)
            enriched += 1

    return enriched


# ============================================================
# Slice 2: Name normalization
# ============================================================


def normalize_name_for_dedup(name: Optional[str]) -> str:
    """Aggressive normalization for matching purposes only.

    Strips accents, suffixes, handles Last/First format.
    Never modifies stored names — only used for comparison.

    Returns lowercase normalized string, or empty string for None/empty input.
    """
    if not name:
        return ''

    # Strip accents using unicode normalization
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')

    # Handle "Last, First" format: exactly 2 comma-separated parts
    # where neither part contains suffix keywords
    if ',' in name:
        comma_parts = [p.strip() for p in name.split(',')]
        if len(comma_parts) == 2:
            # Check if second part looks like a first name (not a suffix)
            second_words = comma_parts[1].lower().split()
            if second_words and second_words[0] not in SUFFIXES:
                # Flip to "First Last" format
                name = f"{comma_parts[1]} {comma_parts[0]}"
            else:
                # Just remove the comma (it's separating name from suffix)
                name = ' '.join(comma_parts)

    # Lowercase
    name = name.lower()

    # Strip suffixes
    words = name.split()
    cleaned = []
    for word in words:
        # Strip trailing periods/commas for comparison
        stripped = word.rstrip('.,')
        if stripped in SUFFIXES:
            continue
        cleaned.append(word.rstrip('.,'))
    name = ' '.join(cleaned)

    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    return name


# ============================================================
# Slice 3: Domain-company matching (Tier 3)
# ============================================================


def find_tier3_candidates(session) -> List[Dict]:
    """Tier 3: Name match + email domain matches LinkedIn company.

    Matches synthetic nodes where:
    1. Normalized name matches a real node's name (or email-derived name)
    2. Real node's email domain contains the LinkedIn company name
       (e.g., @cbre.com ↔ 'CBRE')

    Excludes generic domains (gmail.com, yahoo.com, etc.).
    """
    result = session.run("""
        MATCH (synth:Person)
        WHERE (synth.primary_email STARTS WITH 'li://' OR synth.primary_email STARTS WITH 'li-name://')
          AND synth.name IS NOT NULL AND trim(synth.name) <> ''
          AND synth.linkedin_company IS NOT NULL AND trim(synth.linkedin_company) <> ''
        WITH synth,
             toLower(replace(trim(synth.name), '  ', ' ')) as synth_norm,
             toLower(trim(synth.linkedin_company)) as li_co
        MATCH (real:Person)
        WHERE NOT real.primary_email STARTS WITH 'li://'
          AND NOT real.primary_email STARTS WITH 'li-name://'
          AND real.name IS NOT NULL AND trim(real.name) <> ''
          AND real.primary_email CONTAINS '@'
        WITH synth, synth_norm, li_co, real,
             toLower(replace(trim(real.name), '  ', ' ')) as real_norm,
             split(real.primary_email, '@')[1] as real_domain
        WHERE real_domain CONTAINS li_co
          AND size(li_co) >= 3
        RETURN synth.primary_email as synth_email,
               real.primary_email as real_email,
               synth.name as synth_name,
               real.name as real_name,
               synth.linkedin_url as synth_url,
               synth.linkedin_company as synth_company,
               synth.linkedin_position as synth_position,
               real_domain as real_domain
    """)

    raw_candidates = [dict(r) for r in result]

    # Post-filter in Python:
    # 1. Exclude generic domains
    # 2. Apply name normalization for matching
    filtered = []
    for c in raw_candidates:
        domain = c.get('real_domain', '')
        if domain in GENERIC_DOMAINS:
            continue

        synth_norm = normalize_name_for_dedup(c.get('synth_name'))
        real_norm = normalize_name_for_dedup(c.get('real_name'))

        if synth_norm and real_norm and synth_norm == real_norm:
            filtered.append({
                'synth_email': c['synth_email'],
                'real_email': c['real_email'],
                'name': c['synth_name'],
                'synth_url': c.get('synth_url'),
                'synth_company': c.get('synth_company'),
                'synth_position': c.get('synth_position'),
            })

    # Deduplicate: if a synth node matches multiple real nodes, skip (ambiguous)
    synth_counts: Dict[str, int] = {}
    for c in filtered:
        synth_counts[c['synth_email']] = synth_counts.get(c['synth_email'], 0) + 1

    return [c for c in filtered if synth_counts[c['synth_email']] == 1]


# ============================================================
# Original tier functions
# ============================================================


def find_tier1_candidates(session) -> List[Dict]:
    """Tier 1: Case-insensitive exact name match, unambiguous 1:1.

    Returns list of {synth_email, real_email, name, synth_url, synth_company, synth_position}.
    """
    result = session.run("""
        MATCH (synth:Person)
        WHERE (synth.primary_email STARTS WITH 'li://' OR synth.primary_email STARTS WITH 'li-name://')
          AND synth.name IS NOT NULL AND trim(synth.name) <> ''
        WITH synth,
             toLower(replace(trim(synth.name), '  ', ' ')) as norm_name
        MATCH (real:Person)
        WHERE NOT real.primary_email STARTS WITH 'li://'
          AND NOT real.primary_email STARTS WITH 'li-name://'
          AND real.name IS NOT NULL
          AND toLower(replace(trim(real.name), '  ', ' ')) = norm_name
        WITH synth, collect(DISTINCT real) as reals
        WHERE size(reals) = 1
        WITH synth, reals[0] as real
        RETURN synth.primary_email as synth_email,
               real.primary_email as real_email,
               synth.name as name,
               synth.linkedin_url as synth_url,
               synth.linkedin_company as synth_company,
               synth.linkedin_position as synth_position
    """)
    return [dict(r) for r in result]


def find_tier2_candidates(session) -> List[Dict]:
    """Tier 2: Case-insensitive exact name, 1:many, resolved by company match.

    Only returns candidates where exactly one real node's company matches
    the synthetic node's linkedin_company.
    """
    result = session.run("""
        MATCH (synth:Person)
        WHERE (synth.primary_email STARTS WITH 'li://' OR synth.primary_email STARTS WITH 'li-name://')
          AND synth.name IS NOT NULL AND trim(synth.name) <> ''
          AND synth.linkedin_company IS NOT NULL AND trim(synth.linkedin_company) <> ''
        WITH synth,
             toLower(replace(trim(synth.name), '  ', ' ')) as norm_name,
             toLower(trim(synth.linkedin_company)) as li_co
        MATCH (real:Person)
        WHERE NOT real.primary_email STARTS WITH 'li://'
          AND NOT real.primary_email STARTS WITH 'li-name://'
          AND real.name IS NOT NULL
          AND toLower(replace(trim(real.name), '  ', ' ')) = norm_name
        WITH synth, li_co, collect(DISTINCT real) as reals
        WHERE size(reals) > 1
        UNWIND reals as real
        OPTIONAL MATCH (real)-[:WORKS_AT]->(c:Company)
        WITH synth, li_co, real,
             CASE WHEN c IS NOT NULL AND toLower(c.name) CONTAINS li_co THEN true
                  WHEN real.linkedin_company IS NOT NULL AND toLower(real.linkedin_company) CONTAINS li_co THEN true
                  ELSE false
             END as company_match
        WITH synth, collect(CASE WHEN company_match THEN real END) as matched_reals
        WITH synth, [r IN matched_reals WHERE r IS NOT NULL] as matched_reals
        WHERE size(matched_reals) = 1
        WITH synth, matched_reals[0] as real
        RETURN synth.primary_email as synth_email,
               real.primary_email as real_email,
               synth.name as name,
               synth.linkedin_url as synth_url,
               synth.linkedin_company as synth_company,
               synth.linkedin_position as synth_position
    """)
    return [dict(r) for r in result]


def merge_node(session, synth_email: str, real_email: str, synth_url: str,
               synth_company: str, synth_position: str):
    """Merge a synthetic node into a real node.

    1. Transfer all inbound/outbound edges from synthetic → real
    2. Copy linkedin properties to real node
    3. Delete synthetic node
    """
    # Step 1: Copy linkedin properties onto real node
    session.run("""
        MATCH (real:Person {primary_email: $real_email})
        SET real.linkedin_url = COALESCE($url, real.linkedin_url),
            real.linkedin_company = COALESCE($company, real.linkedin_company),
            real.linkedin_position = COALESCE($position, real.linkedin_position),
            real.updated_at = datetime()
    """, real_email=real_email, url=synth_url, company=synth_company, position=synth_position)

    # Step 2: Transfer inbound edges (X)-[r]->(synth) → (X)-[r]->(real)
    for edge_type in EDGE_TYPES:
        session.run(f"""
            MATCH (source)-[old:{edge_type}]->(synth:Person {{primary_email: $synth_email}})
            MATCH (real:Person {{primary_email: $real_email}})
            WHERE source <> real
            MERGE (source)-[new:{edge_type}]->(real)
            SET new += properties(old)
            DELETE old
        """, synth_email=synth_email, real_email=real_email)

    # Step 3: Transfer outbound edges (synth)-[r]->(X) → (real)-[r]->(X)
    for edge_type in EDGE_TYPES:
        session.run(f"""
            MATCH (synth:Person {{primary_email: $synth_email}})-[old:{edge_type}]->(target)
            MATCH (real:Person {{primary_email: $real_email}})
            WHERE target <> real
            MERGE (real)-[new:{edge_type}]->(target)
            SET new += properties(old)
            DELETE old
        """, synth_email=synth_email, real_email=real_email)

    # Step 4: Delete synthetic node (only if no remaining edges)
    session.run("""
        MATCH (synth:Person {primary_email: $synth_email})
        WHERE NOT exists { (synth)-[]-() }
        DELETE synth
    """, synth_email=synth_email)

    # Fallback: detach delete if somehow edges remain
    session.run("""
        MATCH (synth:Person {primary_email: $synth_email})
        DETACH DELETE synth
    """, synth_email=synth_email)


def run_dedup(dry_run: bool = True, report: bool = False, enrich_only: bool = False):
    """Run deduplication pipeline.

    Pipeline order:
    1. Enrich nameless nodes (email-prefix name extraction)
    2. Find Tier 1 candidates (exact name, 1:1)
    3. Find Tier 2 candidates (exact name, company tie-break)
    4. Find Tier 3 candidates (name + domain-company match)
    5. Deduplicate candidates across tiers
    6. Merge (or dry-run report)
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Phase 1: Enrich nameless nodes
            logger.info("Phase 1: Enriching nameless nodes from email prefixes...")
            enriched_count = enrich_nameless_nodes(session)
            logger.info(f"  Enriched {enriched_count} nameless nodes with email-derived names")

            if enrich_only:
                print(f"\n{'='*60}")
                print("ENRICHMENT COMPLETE")
                print(f"{'='*60}")
                print(f"Nodes enriched with email-derived names: {enriched_count}")
                return

            # Phase 2: Find candidates across all tiers
            logger.info("Finding Tier 1 candidates (exact name, 1:1)...")
            tier1 = find_tier1_candidates(session)
            logger.info(f"  Tier 1: {len(tier1)} candidates")

            logger.info("Finding Tier 2 candidates (exact name, company tie-break)...")
            tier2 = find_tier2_candidates(session)
            logger.info(f"  Tier 2: {len(tier2)} candidates")

            logger.info("Finding Tier 3 candidates (name + domain-company match)...")
            tier3 = find_tier3_candidates(session)
            logger.info(f"  Tier 3: {len(tier3)} candidates")

            all_candidates = []
            for c in tier1:
                c['tier'] = 1
                all_candidates.append(c)
            for c in tier2:
                c['tier'] = 2
                all_candidates.append(c)
            for c in tier3:
                c['tier'] = 3
                all_candidates.append(c)

            # Deduplicate (a synth node might appear in multiple tiers; prefer lowest tier)
            seen = set()
            unique_candidates = []
            for c in all_candidates:
                if c['synth_email'] not in seen:
                    seen.add(c['synth_email'])
                    unique_candidates.append(c)

            logger.info(f"\nTotal unique merge candidates: {len(unique_candidates)}")

            if report:
                _write_report(unique_candidates)
                return

            if dry_run:
                print(f"\n{'='*60}")
                print("DEDUP DRY RUN (V2)")
                print(f"{'='*60}")
                print(f"Nodes enriched (email-prefix):     {enriched_count}")
                print(f"Tier 1 (exact name, 1:1):          {len(tier1)}")
                print(f"Tier 2 (name + company):           {len(tier2)}")
                print(f"Tier 3 (name + domain-company):    {len(tier3)}")
                print(f"Total unique merges:               {len(unique_candidates)}")
                print(f"\nSample merges:")
                for c in unique_candidates[:15]:
                    print(f"  [T{c['tier']}] {c['name']}")
                    print(f"       {c['synth_email']} → {c['real_email']}")
                print(f"\nRun with --merge to execute.")
                return

            # Execute merges
            logger.info(f"\nExecuting {len(unique_candidates)} merges...")
            merged = 0
            errors = 0
            for i, c in enumerate(unique_candidates):
                try:
                    merge_node(
                        session,
                        synth_email=c['synth_email'],
                        real_email=c['real_email'],
                        synth_url=c.get('synth_url'),
                        synth_company=c.get('synth_company'),
                        synth_position=c.get('synth_position'),
                    )
                    merged += 1
                    if (i + 1) % 50 == 0:
                        logger.info(f"  [{i+1}/{len(unique_candidates)}] merged={merged}, errors={errors}")
                except Exception as e:
                    logger.error(f"  Error merging {c['synth_email']}: {e}")
                    errors += 1

            print(f"\n{'='*60}")
            print("DEDUP COMPLETE (V2)")
            print(f"{'='*60}")
            print(f"Enriched: {enriched_count}")
            print(f"Merged:   {merged}")
            print(f"Errors:   {errors}")
            print(f"Skipped:  {len(unique_candidates) - merged - errors}")

            # Post-merge stats
            result = session.run("""
                MATCH (p:Person)
                WHERE p.primary_email STARTS WITH 'li://' OR p.primary_email STARTS WITH 'li-name://'
                RETURN count(p) as remaining
            """)
            remaining = result.single()['remaining']
            print(f"\nRemaining synthetic nodes: {remaining}")

    finally:
        gb.close()


def _write_report(candidates: List[Dict]):
    """Write merge candidates to CSV for review."""
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'tier', 'name', 'synth_email', 'real_email',
            'synth_url', 'synth_company', 'synth_position',
        ])
        writer.writeheader()
        for c in candidates:
            writer.writerow({
                'tier': c['tier'],
                'name': c['name'],
                'synth_email': c['synth_email'],
                'real_email': c['real_email'],
                'synth_url': c.get('synth_url', ''),
                'synth_company': c.get('synth_company', ''),
                'synth_position': c.get('synth_position', ''),
            })
    logger.info(f"Report written to {REPORT_PATH} ({len(candidates)} rows)")


def main():
    parser = argparse.ArgumentParser(description='Deduplicate LinkedIn synthetic nodes (V2)')
    parser.add_argument('--dry-run', action='store_true', help='Preview merges without executing')
    parser.add_argument('--merge', action='store_true', help='Execute merges')
    parser.add_argument('--enrich-only', action='store_true', help='Only backfill names from email prefixes')
    parser.add_argument('--report', action='store_true', help='Write CSV report of candidates')

    args = parser.parse_args()

    if args.enrich_only:
        run_dedup(dry_run=True, enrich_only=True)
    elif args.report:
        run_dedup(dry_run=True, report=True)
    elif args.merge:
        run_dedup(dry_run=False)
    elif args.dry_run:
        run_dedup(dry_run=True)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
