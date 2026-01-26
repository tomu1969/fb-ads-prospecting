"""Contact prioritization for LLM extraction.

Prioritizes contacts by:
1. Target industry (real estate, finance, tech)
2. Engagement level (replied vs one-way)
3. Email count

Excludes:
- Internal domains
- Automated emails (noreply, etc)
- One-way outbound (sent but never replied)
"""

import logging
import sqlite3
from typing import Dict, List, Set, Tuple

from scripts.contact_intel.config import DATA_DIR

logger = logging.getLogger(__name__)

EMAILS_DB = DATA_DIR / "emails.db"

# Internal domains to skip
INTERNAL_DOMAINS = frozenset({
    'jaguarcapital.co',
    'tujaguarcapital.com',
    'lahaus.com',
    'nuestro.co',
    'nuestrocartago.co',
    'nuestrouraba.com',
})

# Automated email patterns to skip
AUTOMATED_PATTERNS = (
    'noreply',
    'no-reply',
    'notifications',
    'mailer-daemon',
    'postmaster',
    'donotreply',
    'auto-confirm',
    'bounce',
    'calendar-notification',
    'notify',
)

# Target industry keywords
TARGET_KEYWORDS = {
    'real_estate': [
        'jll', 'cbre', 'compass', 'colliers', 'cushman', 'cushwake',
        'remax', 'century21', 'coldwellbanker', 'sothebys', 'zillow',
        'realty', 'properties', 'inmobiliaria', 'finca', 'estate',
        'broker', 'realtor', 'agent', 'officer',
    ],
    'finance': [
        'bank', 'capital', 'invest', 'fund', 'asset', 'wealth',
        'partners', 'ventures', 'vc', 'equity', 'holdings',
        'management', 'advisors', 'advisory', 'securities',
        'goldman', 'morgan', 'jpmorgan', 'blackstone', 'kkr',
        'sequoia', 'a16z', 'accel', 'kaszek', 'softbank',
    ],
    'tech': [
        'google', 'microsoft', 'amazon', 'meta', 'apple', 'nvidia',
        'salesforce', 'oracle', 'ibm', 'adobe', 'stripe', 'openai',
        'tech', 'software', 'app', 'ai', 'data', 'cloud', 'saas',
        'digital', 'labs', 'io', 'dev', 'engineering', 'platform',
    ],
}


def _get_domain(email: str) -> str:
    """Extract domain from email."""
    if '@' in email:
        return email.split('@')[1].lower()
    return ''


def _is_internal(email: str) -> bool:
    """Check if email is from internal domain."""
    domain = _get_domain(email)
    return domain in INTERNAL_DOMAINS


def _is_automated(email: str) -> bool:
    """Check if email appears to be automated."""
    email_lower = email.lower()
    return any(pattern in email_lower for pattern in AUTOMATED_PATTERNS)


def _is_target_industry(email: str) -> Tuple[bool, str]:
    """Check if email domain matches target industry."""
    domain = _get_domain(email)
    for industry, keywords in TARGET_KEYWORDS.items():
        if any(kw in domain for kw in keywords):
            return True, industry
    return False, ''


def get_contacts_who_replied(my_emails: Set[str], db_path=None) -> Set[str]:
    """Get contacts who have sent us at least one email."""
    db = db_path or EMAILS_DB
    logger.info(f"Fetching contacts who replied from {db}")

    conn = sqlite3.connect(db)
    try:
        cursor = conn.cursor()
        # Check if emails table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='emails'")
        if not cursor.fetchone():
            logger.warning(f"No 'emails' table found in {db}")
            return set()

        cursor.execute("SELECT DISTINCT from_email FROM emails")
        from_emails = {row[0].lower() for row in cursor.fetchall() if row[0]}
    except sqlite3.Error as e:
        logger.error(f"Database error in get_contacts_who_replied: {e}")
        return set()
    finally:
        conn.close()

    my_emails_lower = {e.lower() for e in my_emails}
    replied = from_emails - my_emails_lower
    logger.info(f"Found {len(replied)} unique contacts who replied")
    return replied


def get_prioritized_contacts(
    my_emails: Set[str],
    limit: int = 2500,
    already_extracted: Set[str] = None,
) -> List[Dict]:
    """Get prioritized list of contacts for extraction."""
    logger.info(f"Getting prioritized contacts (limit={limit})")
    already_extracted = already_extracted or set()
    already_extracted_lower = {e.lower() for e in already_extracted}
    my_emails_lower = {e.lower() for e in my_emails}

    conn = sqlite3.connect(EMAILS_DB)
    try:
        cursor = conn.cursor()

        # Check if emails table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='emails'")
        if not cursor.fetchone():
            logger.warning(f"No 'emails' table found in {EMAILS_DB}")
            return []

        # Get all contacts who sent us emails
        cursor.execute("""
            SELECT from_email, from_name, COUNT(*) as email_count
            FROM emails
            GROUP BY from_email
        """)
        contacts_from = {}
        for row in cursor.fetchall():
            if row[0]:
                contacts_from[row[0].lower()] = {'name': row[1], 'sent_to_us': row[2]}

        # Get contacts we sent emails to
        cursor.execute("SELECT to_emails FROM emails")
        emails_we_sent = {}
        for row in cursor.fetchall():
            if row[0]:
                for email in row[0].split(','):
                    email = email.strip().lower()
                    if email:
                        emails_we_sent[email] = emails_we_sent.get(email, 0) + 1
    except sqlite3.Error as e:
        logger.error(f"Database error in get_prioritized_contacts: {e}")
        return []
    finally:
        conn.close()

    logger.info(f"Loaded {len(contacts_from)} contacts who sent emails, {len(emails_we_sent)} we sent to")

    # Get contacts who replied
    replied_contacts = get_contacts_who_replied(my_emails)

    # Build prioritized lists
    tier1, tier2, tier3 = [], [], []

    all_contacts = set(contacts_from.keys()) | set(emails_we_sent.keys())
    logger.info(f"Total unique contacts to evaluate: {len(all_contacts)}")

    # Track filter statistics
    skipped_extracted = 0
    skipped_self = 0
    skipped_internal = 0
    skipped_automated = 0
    skipped_one_way = 0

    for email in all_contacts:
        email_lower = email.lower()

        # Skip filters
        if email_lower in already_extracted_lower:
            skipped_extracted += 1
            continue
        if email_lower in my_emails_lower:
            skipped_self += 1
            continue
        if _is_internal(email_lower):
            skipped_internal += 1
            continue
        if _is_automated(email_lower):
            skipped_automated += 1
            continue

        has_replied = email_lower in replied_contacts

        # Skip one-way outbound
        if not has_replied and email_lower in emails_we_sent and email_lower not in contacts_from:
            skipped_one_way += 1
            continue

        info = contacts_from.get(email_lower, {})
        name = info.get('name', '')
        sent_to_us = info.get('sent_to_us', 0)
        we_sent = emails_we_sent.get(email_lower, 0)
        total_emails = sent_to_us + we_sent

        is_target, industry = _is_target_industry(email_lower)

        contact = {
            'email': email_lower,
            'name': name or email_lower.split('@')[0],
            'industry': industry,
            'email_count': total_emails,
        }

        if is_target and has_replied:
            contact['tier'] = 1
            tier1.append(contact)
        elif total_emails >= 3:
            contact['tier'] = 2
            tier2.append(contact)
        elif has_replied:
            contact['tier'] = 3
            tier3.append(contact)

    # Log filter statistics
    logger.debug(f"Skipped - already extracted: {skipped_extracted}, self: {skipped_self}, "
                 f"internal: {skipped_internal}, automated: {skipped_automated}, one-way: {skipped_one_way}")

    # Sort by email count
    tier1.sort(key=lambda x: x['email_count'], reverse=True)
    tier2.sort(key=lambda x: x['email_count'], reverse=True)
    tier3.sort(key=lambda x: x['email_count'], reverse=True)

    result = tier1 + tier2 + tier3
    logger.info(f"Prioritized: Tier1={len(tier1)}, Tier2={len(tier2)}, Tier3={len(tier3)}, Total={len(result)}")

    return result[:limit]


def get_contact_stats(my_emails: Set[str]) -> Dict:
    """Get statistics about contact prioritization."""
    contacts = get_prioritized_contacts(my_emails, limit=100000)

    tier_counts = {1: 0, 2: 0, 3: 0}
    industry_counts = {}

    for c in contacts:
        tier_counts[c['tier']] = tier_counts.get(c['tier'], 0) + 1
        if c['industry']:
            industry_counts[c['industry']] = industry_counts.get(c['industry'], 0) + 1

    return {
        'total': len(contacts),
        'tier_1_target_industry': tier_counts.get(1, 0),
        'tier_2_active': tier_counts.get(2, 0),
        'tier_3_replied': tier_counts.get(3, 0),
        'by_industry': industry_counts,
    }
