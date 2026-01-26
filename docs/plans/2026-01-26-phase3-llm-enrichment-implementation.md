# Phase 3: LLM Enrichment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract company, role, and topics from emails using Groq API with $50 budget, partial saves for crash recovery.

**Architecture:** Fetch email bodies on-demand via Gmail API, prioritize contacts by industry/engagement, extract via Groq Llama 3.3 70B, cache in SQLite, sync to Neo4j.

**Tech Stack:** Groq API, Gmail API (OAuth), SQLite, Neo4j, Python 3.11

---

## Task 1: Create extractions.db Schema

**Files:**
- Create: `scripts/contact_intel/extraction_db.py`

**Step 1: Write the extraction database module**

```python
"""SQLite database for LLM extraction results.

Handles:
- Contact extraction results (company, role, topics)
- Email body cache (fetched on-demand)
- Budget/run tracking
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from scripts.contact_intel.config import DATA_DIR, ensure_data_dir

EXTRACTIONS_DB = DATA_DIR / "extractions.db"


def get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating DB if needed."""
    ensure_data_dir()
    conn = sqlite3.connect(EXTRACTIONS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the extractions database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    # Contact extractions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_extractions (
            email TEXT PRIMARY KEY,
            name TEXT,
            company TEXT,
            role TEXT,
            topics TEXT,
            confidence REAL,
            source_emails TEXT,
            extracted_at TIMESTAMP,
            model TEXT
        )
    """)

    # Email bodies cache
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_bodies (
            message_id TEXT PRIMARY KEY,
            body TEXT,
            fetched_at TIMESTAMP
        )
    """)

    # Extraction runs for budget tracking
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS extraction_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tokens_used INTEGER DEFAULT 0,
            cost_usd REAL DEFAULT 0,
            contacts_processed INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running'
        )
    """)

    conn.commit()
    conn.close()


def save_extraction(
    email: str,
    name: str,
    company: Optional[str],
    role: Optional[str],
    topics: List[str],
    confidence: float,
    source_emails: List[str],
    model: str,
):
    """Save extraction result for a contact (partial save)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO contact_extractions
        (email, name, company, role, topics, confidence, source_emails, extracted_at, model)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        email,
        name,
        company,
        role,
        json.dumps(topics),
        confidence,
        json.dumps(source_emails),
        datetime.now().isoformat(),
        model,
    ))
    conn.commit()
    conn.close()


def get_extracted_emails() -> set:
    """Get set of emails already extracted."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM contact_extractions WHERE extracted_at IS NOT NULL")
    emails = {row['email'] for row in cursor.fetchall()}
    conn.close()
    return emails


def save_email_body(message_id: str, body: str):
    """Cache an email body."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO email_bodies (message_id, body, fetched_at)
        VALUES (?, ?, ?)
    """, (message_id, body, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_cached_body(message_id: str) -> Optional[str]:
    """Get cached email body if available."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT body FROM email_bodies WHERE message_id = ?", (message_id,))
    row = cursor.fetchone()
    conn.close()
    return row['body'] if row else None


def start_extraction_run() -> int:
    """Start a new extraction run, return run ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO extraction_runs DEFAULT VALUES")
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return run_id


def update_run_stats(run_id: int, tokens: int, cost: float, contacts: int):
    """Update extraction run statistics."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE extraction_runs
        SET tokens_used = tokens_used + ?,
            cost_usd = cost_usd + ?,
            contacts_processed = contacts_processed + ?
        WHERE id = ?
    """, (tokens, cost, contacts, run_id))
    conn.commit()
    conn.close()


def get_run_stats(run_id: int) -> Dict:
    """Get stats for an extraction run."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM extraction_runs WHERE id = ?", (run_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else {}


def complete_run(run_id: int):
    """Mark extraction run as completed."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE extraction_runs SET status = 'completed' WHERE id = ?", (run_id,))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Initialized {EXTRACTIONS_DB}")
```

**Step 2: Run to initialize DB**

Run: `python scripts/contact_intel/extraction_db.py`
Expected: "Initialized data/contact_intel/extractions.db"

**Step 3: Commit**

```bash
git add scripts/contact_intel/extraction_db.py
git commit -m "feat(contact-intel): add extraction database schema"
```

---

## Task 2: Create Groq Client with Rate Limiting

**Files:**
- Create: `scripts/contact_intel/groq_client.py`

**Step 1: Write the Groq client module**

```python
"""Groq API client with rate limiting and cost tracking.

Uses Llama 3.3 70B for entity extraction from emails.
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Groq pricing (per 1M tokens)
INPUT_COST_PER_M = 0.59
OUTPUT_COST_PER_M = 0.79

# Rate limiting
REQUESTS_PER_MINUTE = 30
REQUEST_DELAY = 60.0 / REQUESTS_PER_MINUTE  # 2 seconds


@dataclass
class ExtractionResult:
    """Result from LLM extraction."""
    company: Optional[str]
    role: Optional[str]
    topics: List[str]
    confidence: float
    input_tokens: int
    output_tokens: int
    cost_usd: float


SYSTEM_PROMPT = """You extract structured contact information from emails. Return valid JSON only."""

USER_PROMPT_TEMPLATE = """Extract the sender's professional information from these emails.

Contact: {name} <{email}>

{emails_text}

Return JSON:
{{
  "company": "Company name or null",
  "role": "Job title or null",
  "topics": ["topic1", "topic2"],
  "confidence": 0.0-1.0
}}

Rules:
- Extract company/role from email signature first
- If no signature, infer role from email content and context
- Topics: 1-3 main professional topics discussed
- Confidence: 0.8+ if signature found, 0.5-0.8 if inferred from content"""


class GroqClient:
    """Groq API client for entity extraction."""

    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not set in environment")
        self.model = model
        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            sleep_time = REQUEST_DELAY - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in USD."""
        input_cost = (input_tokens / 1_000_000) * INPUT_COST_PER_M
        output_cost = (output_tokens / 1_000_000) * OUTPUT_COST_PER_M
        return input_cost + output_cost

    def extract_contact_info(
        self,
        email: str,
        name: str,
        emails: List[Dict[str, str]],
    ) -> ExtractionResult:
        """Extract company, role, topics from contact's emails.

        Args:
            email: Contact's email address
            name: Contact's name
            emails: List of dicts with 'subject', 'date', 'body' keys

        Returns:
            ExtractionResult with extracted info and usage stats
        """
        try:
            from groq import Groq
        except ImportError:
            raise ImportError("Install groq: pip install groq")

        # Format emails for prompt
        emails_text = ""
        for i, e in enumerate(emails, 1):
            emails_text += f"\n--- Email {i} ---\n"
            emails_text += f"Subject: {e.get('subject', 'N/A')}\n"
            emails_text += f"Date: {e.get('date', 'N/A')}\n"
            emails_text += f"Body:\n{e.get('body', '')[:3000]}\n"  # Truncate long bodies

        user_prompt = USER_PROMPT_TEMPLATE.format(
            name=name,
            email=email,
            emails_text=emails_text,
        )

        # Rate limit
        self._rate_limit()

        # Call Groq API
        client = Groq(api_key=self.api_key)

        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"},
            )
        except Exception as e:
            logger.error(f"Groq API error: {e}")
            # Return empty result on error
            return ExtractionResult(
                company=None,
                role=None,
                topics=[],
                confidence=0.0,
                input_tokens=0,
                output_tokens=0,
                cost_usd=0.0,
            )

        # Parse response
        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens
        cost = self._calculate_cost(input_tokens, output_tokens)

        # Parse JSON response
        try:
            content = response.choices[0].message.content
            data = json.loads(content)
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning(f"Failed to parse Groq response: {e}")
            data = {}

        return ExtractionResult(
            company=data.get("company"),
            role=data.get("role"),
            topics=data.get("topics", []),
            confidence=data.get("confidence", 0.0),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
        )


if __name__ == "__main__":
    # Quick test
    client = GroqClient()
    result = client.extract_contact_info(
        email="test@example.com",
        name="Test User",
        emails=[{
            "subject": "Meeting tomorrow",
            "date": "2024-01-15",
            "body": "Hi, let's discuss the project.\n\nBest,\nJohn Smith\nSenior Engineer at Acme Corp",
        }],
    )
    print(f"Company: {result.company}")
    print(f"Role: {result.role}")
    print(f"Topics: {result.topics}")
    print(f"Confidence: {result.confidence}")
    print(f"Cost: ${result.cost_usd:.4f}")
```

**Step 2: Test the client**

Run: `python scripts/contact_intel/groq_client.py`
Expected: Extraction result with company/role/topics and cost

**Step 3: Commit**

```bash
git add scripts/contact_intel/groq_client.py
git commit -m "feat(contact-intel): add Groq client with rate limiting"
```

---

## Task 3: Create Contact Prioritization Module

**Files:**
- Create: `scripts/contact_intel/contact_prioritizer.py`

**Step 1: Write the prioritization module**

```python
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

# Target industry keywords (domain must contain one of these)
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
    """Check if email domain matches target industry.

    Returns:
        Tuple of (is_match, industry_name)
    """
    domain = _get_domain(email)
    for industry, keywords in TARGET_KEYWORDS.items():
        if any(kw in domain for kw in keywords):
            return True, industry
    return False, ''


def get_contacts_who_replied(my_emails: Set[str]) -> Set[str]:
    """Get contacts who have sent us at least one email (replied).

    Args:
        my_emails: Set of our own email addresses

    Returns:
        Set of email addresses that have sent us emails
    """
    conn = sqlite3.connect(EMAILS_DB)
    cursor = conn.cursor()

    # Get all from_emails that are not our own
    cursor.execute("SELECT DISTINCT from_email FROM emails")
    from_emails = {row[0] for row in cursor.fetchall()}

    conn.close()

    # Filter out our own emails
    replied = from_emails - my_emails
    return replied


def get_prioritized_contacts(
    my_emails: Set[str],
    limit: int = 2500,
    already_extracted: Set[str] = None,
) -> List[Dict]:
    """Get prioritized list of contacts for extraction.

    Priority order:
    1. Target industry + replied
    2. Active external (3+ emails)
    3. Any replied

    Excludes:
    - Internal domains
    - Automated emails
    - One-way outbound
    - Already extracted

    Args:
        my_emails: Set of our own email addresses
        limit: Max contacts to return
        already_extracted: Set of already extracted emails to skip

    Returns:
        List of dicts with email, name, tier, industry
    """
    already_extracted = already_extracted or set()

    conn = sqlite3.connect(EMAILS_DB)
    cursor = conn.cursor()

    # Get all unique contacts with email counts
    cursor.execute("""
        SELECT
            from_email,
            from_name,
            COUNT(*) as email_count
        FROM emails
        GROUP BY from_email
    """)

    contacts_from = {row[0]: {'name': row[1], 'sent_to_us': row[2]} for row in cursor.fetchall()}

    # Get counts of emails we sent to each contact
    cursor.execute("""
        SELECT to_emails, COUNT(*) as cnt
        FROM emails
        GROUP BY to_emails
    """)

    # Parse to_emails (comma-separated) and count
    emails_we_sent = {}
    for row in cursor.fetchall():
        to_list = row[0] if row[0] else ''
        for email in to_list.split(','):
            email = email.strip().lower()
            if email:
                emails_we_sent[email] = emails_we_sent.get(email, 0) + row[1]

    conn.close()

    # Get contacts who replied (sent us at least 1 email)
    replied_contacts = get_contacts_who_replied(my_emails)

    # Build prioritized list
    tier1 = []  # Target industry + replied
    tier2 = []  # Active external (3+ emails exchanged)
    tier3 = []  # Any replied

    all_contacts = set(contacts_from.keys()) | set(emails_we_sent.keys())

    for email in all_contacts:
        email_lower = email.lower()

        # Skip filters
        if email_lower in already_extracted:
            continue
        if email_lower in my_emails:
            continue
        if _is_internal(email_lower):
            continue
        if _is_automated(email_lower):
            continue

        # Check if they replied
        has_replied = email_lower in replied_contacts

        # Skip one-way outbound (we sent, they never replied)
        if not has_replied and email_lower in emails_we_sent:
            continue

        # Get contact info
        info = contacts_from.get(email_lower, {})
        name = info.get('name', '')
        sent_to_us = info.get('sent_to_us', 0)
        we_sent = emails_we_sent.get(email_lower, 0)
        total_emails = sent_to_us + we_sent

        # Check target industry
        is_target, industry = _is_target_industry(email_lower)

        contact = {
            'email': email_lower,
            'name': name or email_lower.split('@')[0],
            'industry': industry,
            'email_count': total_emails,
        }

        # Assign to tier
        if is_target and has_replied:
            contact['tier'] = 1
            tier1.append(contact)
        elif total_emails >= 3:
            contact['tier'] = 2
            tier2.append(contact)
        elif has_replied:
            contact['tier'] = 3
            tier3.append(contact)

    # Sort each tier by email count (descending)
    tier1.sort(key=lambda x: x['email_count'], reverse=True)
    tier2.sort(key=lambda x: x['email_count'], reverse=True)
    tier3.sort(key=lambda x: x['email_count'], reverse=True)

    # Combine and limit
    result = tier1 + tier2 + tier3

    logger.info(f"Prioritized contacts: Tier 1={len(tier1)}, Tier 2={len(tier2)}, Tier 3={len(tier3)}")

    return result[:limit]


def get_contact_stats(my_emails: Set[str]) -> Dict:
    """Get statistics about contact prioritization."""
    already_extracted = set()  # Fresh count

    contacts = get_prioritized_contacts(my_emails, limit=100000, already_extracted=already_extracted)

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # My email addresses
    my_emails = {'tu@jaguarcapital.co', 'tomas@tujaguarcapital.com', 'tomasuribe@lahaus.com'}

    stats = get_contact_stats(my_emails)
    print(f"\nContact Prioritization Stats:")
    print(f"  Total eligible: {stats['total']}")
    print(f"  Tier 1 (target industry): {stats['tier_1_target_industry']}")
    print(f"  Tier 2 (active 3+): {stats['tier_2_active']}")
    print(f"  Tier 3 (any replied): {stats['tier_3_replied']}")
    print(f"\n  By industry: {stats['by_industry']}")
```

**Step 2: Test prioritization stats**

Run: `python scripts/contact_intel/contact_prioritizer.py`
Expected: Stats showing tier breakdown and industry counts

**Step 3: Commit**

```bash
git add scripts/contact_intel/contact_prioritizer.py
git commit -m "feat(contact-intel): add contact prioritization by industry"
```

---

## Task 4: Create Email Body Fetcher

**Files:**
- Create: `scripts/contact_intel/body_fetcher.py`

**Step 1: Write the body fetcher module**

```python
"""Fetch email bodies on-demand via Gmail API.

Caches bodies in extractions.db to avoid re-fetching.
"""

import base64
import logging
import sqlite3
from typing import Dict, List, Optional

from scripts.contact_intel.config import DATA_DIR, get_token_path
from scripts.contact_intel.extraction_db import get_cached_body, save_email_body
from scripts.contact_intel.gmail_sync import load_oauth_credentials

logger = logging.getLogger(__name__)

EMAILS_DB = DATA_DIR / "emails.db"


def _get_gmail_service(account_name: str):
    """Get Gmail API service for account."""
    try:
        from googleapiclient.discovery import build
        from google.auth.transport.requests import Request
    except ImportError:
        raise ImportError("Install google-api-python-client: pip install google-api-python-client")

    token_path = get_token_path(account_name)
    creds = load_oauth_credentials(str(token_path))

    if not creds:
        raise ValueError(f"No credentials for account: {account_name}")

    # Refresh if needed
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build('gmail', 'v1', credentials=creds, cache_discovery=False)


def _extract_body_from_payload(payload: dict) -> str:
    """Extract text body from Gmail API message payload.

    Handles multipart messages and base64 encoding.
    """
    body_text = ""

    # Check if body data is directly in payload
    if 'body' in payload and payload['body'].get('data'):
        body_text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        return body_text

    # Handle multipart messages
    if 'parts' in payload:
        for part in payload['parts']:
            mime_type = part.get('mimeType', '')

            # Prefer text/plain
            if mime_type == 'text/plain':
                if 'body' in part and part['body'].get('data'):
                    body_text = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    return body_text

            # Recurse into nested parts
            if 'parts' in part:
                nested = _extract_body_from_payload(part)
                if nested:
                    return nested

        # Fall back to text/html if no plain text
        for part in payload['parts']:
            if part.get('mimeType') == 'text/html':
                if 'body' in part and part['body'].get('data'):
                    html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    # Basic HTML stripping
                    import re
                    body_text = re.sub(r'<[^>]+>', ' ', html)
                    body_text = re.sub(r'\s+', ' ', body_text).strip()
                    return body_text[:5000]  # Limit HTML-derived text

    return body_text


def fetch_body(message_id: str, account_name: str = "tujaguarcapital") -> Optional[str]:
    """Fetch email body, using cache if available.

    Args:
        message_id: Gmail message ID (from emails.db)
        account_name: Account to fetch from

    Returns:
        Email body text or None
    """
    # Check cache first
    cached = get_cached_body(message_id)
    if cached is not None:
        return cached

    # Fetch from Gmail API
    try:
        service = _get_gmail_service(account_name)

        # Get full message
        msg = service.users().messages().get(
            userId='me',
            id=message_id,
            format='full',
        ).execute()

        payload = msg.get('payload', {})
        body = _extract_body_from_payload(payload)

        # Cache it
        save_email_body(message_id, body)

        return body

    except Exception as e:
        logger.error(f"Error fetching body for {message_id}: {e}")
        return None


def get_contact_emails_with_body(
    contact_email: str,
    limit: int = 3,
    account_name: str = "tujaguarcapital",
) -> List[Dict]:
    """Get recent emails from/to a contact with bodies.

    Args:
        contact_email: Contact's email address
        limit: Max emails to fetch
        account_name: Account for Gmail API

    Returns:
        List of dicts with subject, date, body
    """
    conn = sqlite3.connect(EMAILS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get recent emails involving this contact
    # Look in from_email and to_emails
    cursor.execute("""
        SELECT message_id, subject, date, from_email
        FROM emails
        WHERE from_email = ? OR to_emails LIKE ?
        ORDER BY date DESC
        LIMIT ?
    """, (contact_email, f'%{contact_email}%', limit))

    rows = cursor.fetchall()
    conn.close()

    emails = []
    for row in rows:
        # Extract Gmail message ID (strip brackets and @gmail suffix if present)
        msg_id = row['message_id']
        if msg_id.startswith('<'):
            msg_id = msg_id[1:]
        if msg_id.endswith('>'):
            msg_id = msg_id[:-1]
        if '@' in msg_id:
            msg_id = msg_id.split('@')[0]

        body = fetch_body(msg_id, account_name)

        emails.append({
            'subject': row['subject'] or '',
            'date': row['date'] or '',
            'body': body or '',
            'from': row['from_email'],
        })

    return emails


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test with a sample contact
    test_email = "test@example.com"  # Replace with real email
    emails = get_contact_emails_with_body(test_email, limit=1)

    for e in emails:
        print(f"Subject: {e['subject']}")
        print(f"Date: {e['date']}")
        print(f"Body preview: {e['body'][:200]}...")
```

**Step 2: Commit**

```bash
git add scripts/contact_intel/body_fetcher.py
git commit -m "feat(contact-intel): add on-demand email body fetcher"
```

---

## Task 5: Create Main Entity Extractor

**Files:**
- Create: `scripts/contact_intel/entity_extractor.py`

**Step 1: Write the main extractor module**

```python
"""Entity Extractor - Extract company, role, topics from emails via Groq.

Main entry point for Phase 3 LLM Enrichment.

Usage:
    # Show status and eligible contacts
    python scripts/contact_intel/entity_extractor.py --status

    # Run extraction with $50 budget
    python scripts/contact_intel/entity_extractor.py --budget 50

    # Resume interrupted extraction
    python scripts/contact_intel/entity_extractor.py --resume

    # Sync extractions to Neo4j
    python scripts/contact_intel/entity_extractor.py --sync
"""

import argparse
import logging
from typing import Set

from scripts.contact_intel.body_fetcher import get_contact_emails_with_body
from scripts.contact_intel.contact_prioritizer import get_contact_stats, get_prioritized_contacts
from scripts.contact_intel.extraction_db import (
    complete_run,
    get_extracted_emails,
    get_run_stats,
    init_db,
    save_extraction,
    start_extraction_run,
    update_run_stats,
)
from scripts.contact_intel.groq_client import GroqClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('entity_extractor.log'),
    ]
)
logger = logging.getLogger(__name__)

# My email addresses (to exclude)
MY_EMAILS: Set[str] = {
    'tu@jaguarcapital.co',
    'tomas@tujaguarcapital.com',
    'tomasuribe@lahaus.com',
}


def show_status():
    """Show extraction status and eligible contacts."""
    init_db()

    already_extracted = get_extracted_emails()
    stats = get_contact_stats(MY_EMAILS)

    print("\n" + "=" * 50)
    print("ENTITY EXTRACTION STATUS")
    print("=" * 50)
    print(f"\nEligible contacts: {stats['total']}")
    print(f"  Tier 1 (target industry): {stats['tier_1_target_industry']}")
    print(f"  Tier 2 (active 3+): {stats['tier_2_active']}")
    print(f"  Tier 3 (any replied): {stats['tier_3_replied']}")
    print(f"\nBy industry: {stats['by_industry']}")
    print(f"\nAlready extracted: {len(already_extracted)}")
    print(f"Remaining: {stats['total'] - len(already_extracted)}")

    # Estimate cost
    remaining = stats['total'] - len(already_extracted)
    est_cost = min(remaining, 2500) * 0.02
    print(f"\nEstimated cost for 2500 contacts: ${est_cost:.2f}")


def run_extraction(budget: float = 50.0, resume: bool = False):
    """Run entity extraction with budget limit.

    Args:
        budget: Maximum USD to spend
        resume: Whether to resume previous run
    """
    init_db()

    already_extracted = get_extracted_emails() if resume else set()
    logger.info(f"Starting extraction (budget=${budget}, resume={resume})")
    logger.info(f"Already extracted: {len(already_extracted)} contacts")

    # Get prioritized contacts
    contacts = get_prioritized_contacts(
        my_emails=MY_EMAILS,
        limit=5000,  # Get more than we need, will stop at budget
        already_extracted=already_extracted,
    )

    if not contacts:
        logger.info("No contacts to extract")
        return

    logger.info(f"Contacts to process: {len(contacts)}")

    # Initialize Groq client
    client = GroqClient()

    # Start tracking run
    run_id = start_extraction_run()
    total_cost = 0.0
    total_contacts = 0
    total_tokens = 0

    try:
        for i, contact in enumerate(contacts):
            # Check budget
            if total_cost >= budget:
                logger.info(f"Budget exhausted: ${total_cost:.2f} >= ${budget}")
                break

            email = contact['email']
            name = contact['name']

            # Fetch email bodies
            emails = get_contact_emails_with_body(email, limit=3)

            if not emails:
                logger.debug(f"No emails found for {email}")
                continue

            # Extract via Groq
            result = client.extract_contact_info(
                email=email,
                name=name,
                emails=emails,
            )

            # Save immediately (partial save for crash recovery)
            save_extraction(
                email=email,
                name=name,
                company=result.company,
                role=result.role,
                topics=result.topics,
                confidence=result.confidence,
                source_emails=[e.get('subject', '') for e in emails],
                model=client.model,
            )

            # Update stats
            total_cost += result.cost_usd
            total_tokens += result.input_tokens + result.output_tokens
            total_contacts += 1

            # Update run stats
            update_run_stats(run_id, result.input_tokens + result.output_tokens, result.cost_usd, 1)

            # Log progress
            if (i + 1) % 10 == 0:
                logger.info(
                    f"[{i + 1}/{len(contacts)}] Processed {total_contacts} contacts, "
                    f"cost=${total_cost:.2f}, tokens={total_tokens:,}"
                )

            # Log extraction result
            logger.debug(
                f"  {email}: company={result.company}, role={result.role}, "
                f"topics={result.topics}, conf={result.confidence:.2f}"
            )

    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")

    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise

    finally:
        complete_run(run_id)
        logger.info(f"\nExtraction complete:")
        logger.info(f"  Contacts processed: {total_contacts}")
        logger.info(f"  Total cost: ${total_cost:.2f}")
        logger.info(f"  Total tokens: {total_tokens:,}")


def sync_to_neo4j():
    """Sync extractions to Neo4j graph."""
    # Import here to avoid circular dependency
    from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j
    sync_extractions_to_neo4j()


def main():
    parser = argparse.ArgumentParser(
        description='Extract company, role, topics from emails via Groq'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show extraction status')
    parser.add_argument('--budget', type=float, default=50.0,
                        help='Maximum USD to spend (default: 50)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume previous extraction')
    parser.add_argument('--sync', action='store_true',
                        help='Sync extractions to Neo4j')
    parser.add_argument('--email', type=str,
                        help='Extract single contact by email')

    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.sync:
        sync_to_neo4j()
    elif args.email:
        # Single contact extraction
        init_db()
        client = GroqClient()
        emails = get_contact_emails_with_body(args.email, limit=3)
        if emails:
            result = client.extract_contact_info(
                email=args.email,
                name=args.email.split('@')[0],
                emails=emails,
            )
            print(f"Company: {result.company}")
            print(f"Role: {result.role}")
            print(f"Topics: {result.topics}")
            print(f"Confidence: {result.confidence}")
            print(f"Cost: ${result.cost_usd:.4f}")
        else:
            print(f"No emails found for {args.email}")
    else:
        run_extraction(budget=args.budget, resume=args.resume)


if __name__ == "__main__":
    main()
```

**Step 2: Commit**

```bash
git add scripts/contact_intel/entity_extractor.py
git commit -m "feat(contact-intel): add main entity extractor with budget control"
```

---

## Task 6: Create Neo4j Sync Module

**Files:**
- Create: `scripts/contact_intel/extraction_sync.py`

**Step 1: Write the Neo4j sync module**

```python
"""Sync LLM extractions to Neo4j graph.

Creates/updates:
- Company nodes
- Topic nodes
- WORKS_AT edges (Person → Company)
- DISCUSSED edges (Person → Topic)
"""

import json
import logging
import sqlite3
from typing import Dict, List, Optional

from scripts.contact_intel.config import DATA_DIR
from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

logger = logging.getLogger(__name__)

EXTRACTIONS_DB = DATA_DIR / "extractions.db"


def _normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ''
    # Remove common suffixes, lowercase
    normalized = name.lower().strip()
    for suffix in [' inc', ' inc.', ' llc', ' ltd', ' corp', ' corporation', ' co', ' co.']:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    return normalized.strip()


def sync_extractions_to_neo4j():
    """Sync all extractions to Neo4j."""
    if not neo4j_available():
        logger.error("Neo4j not available")
        return

    # Load extractions
    conn = sqlite3.connect(EXTRACTIONS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT email, name, company, role, topics, confidence
        FROM contact_extractions
        WHERE extracted_at IS NOT NULL
    """)

    extractions = cursor.fetchall()
    conn.close()

    logger.info(f"Syncing {len(extractions)} extractions to Neo4j")

    # Connect to Neo4j
    gb = GraphBuilder()
    gb.connect()

    try:
        companies_created = 0
        topics_created = 0
        works_at_created = 0
        discussed_created = 0

        for row in extractions:
            email = row['email']
            company = row['company']
            role = row['role']
            topics_json = row['topics']
            confidence = row['confidence'] or 0.0

            # Parse topics
            try:
                topics = json.loads(topics_json) if topics_json else []
            except json.JSONDecodeError:
                topics = []

            # Create Company node and WORKS_AT edge
            if company:
                normalized = _normalize_company_name(company)

                with gb.driver.session() as session:
                    # Create Company node
                    session.run("""
                        MERGE (c:Company {normalized_name: $normalized})
                        ON CREATE SET c.name = $name, c.created_at = datetime()
                    """, normalized=normalized, name=company)
                    companies_created += 1

                    # Create WORKS_AT edge
                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (c:Company {normalized_name: $normalized})
                        MERGE (p)-[r:WORKS_AT]->(c)
                        ON CREATE SET r.role = $role, r.confidence = $confidence, r.created_at = datetime()
                        ON MATCH SET r.role = $role, r.confidence = $confidence, r.updated_at = datetime()
                    """, email=email, normalized=normalized, role=role, confidence=confidence)
                    works_at_created += 1

            # Create Topic nodes and DISCUSSED edges
            for topic in topics:
                if not topic:
                    continue

                topic_normalized = topic.lower().strip()

                with gb.driver.session() as session:
                    # Create Topic node
                    session.run("""
                        MERGE (t:Topic {name: $name})
                        ON CREATE SET t.created_at = datetime()
                    """, name=topic_normalized)
                    topics_created += 1

                    # Create DISCUSSED edge
                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (t:Topic {name: $name})
                        MERGE (p)-[r:DISCUSSED]->(t)
                        ON CREATE SET r.created_at = datetime()
                        ON MATCH SET r.updated_at = datetime()
                    """, email=email, name=topic_normalized)
                    discussed_created += 1

        logger.info(f"Sync complete:")
        logger.info(f"  Companies: {companies_created}")
        logger.info(f"  Topics: {topics_created}")
        logger.info(f"  WORKS_AT edges: {works_at_created}")
        logger.info(f"  DISCUSSED edges: {discussed_created}")

    finally:
        gb.close()


def get_sync_stats() -> Dict:
    """Get stats about synced data in Neo4j."""
    if not neo4j_available():
        return {'error': 'Neo4j not available'}

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            companies = session.run("MATCH (c:Company) RETURN count(c) as c").single()['c']
            topics = session.run("MATCH (t:Topic) RETURN count(t) as c").single()['c']
            works_at = session.run("MATCH ()-[r:WORKS_AT]->() RETURN count(r) as c").single()['c']
            discussed = session.run("MATCH ()-[r:DISCUSSED]->() RETURN count(r) as c").single()['c']

        return {
            'companies': companies,
            'topics': topics,
            'works_at_edges': works_at,
            'discussed_edges': discussed,
        }
    finally:
        gb.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_extractions_to_neo4j()
```

**Step 2: Commit**

```bash
git add scripts/contact_intel/extraction_sync.py
git commit -m "feat(contact-intel): add Neo4j sync for extractions"
```

---

## Task 7: Integration Test

**Step 1: Run status check**

Run: `python scripts/contact_intel/entity_extractor.py --status`
Expected: Shows tier breakdown and estimated costs

**Step 2: Test single contact extraction**

Run: `python scripts/contact_intel/entity_extractor.py --email "someone@example.com"` (use real email)
Expected: Shows extracted company/role/topics

**Step 3: Run small budget test**

Run: `python scripts/contact_intel/entity_extractor.py --budget 1`
Expected: Extracts ~50 contacts, stops at $1

**Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix(contact-intel): integration test fixes"
```

---

## Task 8: Final Commit and Push

**Step 1: Final commit**

```bash
git add -A
git commit -m "feat(contact-intel): complete Phase 3 LLM enrichment pipeline

- entity_extractor.py: Main extraction with budget control
- groq_client.py: Groq API with rate limiting
- contact_prioritizer.py: Tier-based contact prioritization
- body_fetcher.py: On-demand email body fetching
- extraction_db.py: SQLite cache with partial saves
- extraction_sync.py: Neo4j sync for Company/Topic nodes

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

**Step 2: Push**

```bash
git push origin main
```

---

## Summary

| File | Purpose |
|------|---------|
| `extraction_db.py` | SQLite schema for extractions + bodies |
| `groq_client.py` | Groq API with rate limiting + cost tracking |
| `contact_prioritizer.py` | Tier-based prioritization by industry |
| `body_fetcher.py` | On-demand Gmail body fetching |
| `entity_extractor.py` | Main CLI with --status, --budget, --sync |
| `extraction_sync.py` | Sync to Neo4j (Company, Topic, edges) |

**Key features:**
- Partial saves after each contact (crash-safe)
- Budget tracking with automatic stop
- Resume capability
- Priority tiers (target industry → active → replied)
- Rate limiting (30 req/min)
