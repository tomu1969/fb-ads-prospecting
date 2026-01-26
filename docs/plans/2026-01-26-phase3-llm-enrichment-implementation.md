# Phase 3: LLM Enrichment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract company, role, and topics from emails using Groq API with $50 budget, partial saves for crash recovery.

**Architecture:** Fetch email bodies on-demand via Gmail API, prioritize contacts by industry/engagement, extract via Groq Llama 3.3 70B, cache in SQLite, sync to Neo4j.

**Tech Stack:** Groq API, Gmail API (OAuth), SQLite, Neo4j, Python 3.11, pytest

---

## Task 1: Set Up Test Infrastructure

**Files:**
- Create: `scripts/contact_intel/tests/__init__.py`
- Create: `scripts/contact_intel/tests/conftest.py`

**Step 1: Create test directory and fixtures**

```python
# scripts/contact_intel/tests/__init__.py
"""Tests for contact intelligence modules."""
```

```python
# scripts/contact_intel/tests/conftest.py
"""Shared test fixtures for contact intelligence tests."""

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield Path(path)
    os.unlink(path)


@pytest.fixture
def mock_emails_db(temp_db):
    """Create a mock emails.db with test data."""
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE emails (
            id INTEGER PRIMARY KEY,
            account TEXT,
            message_id TEXT,
            thread_id TEXT,
            from_email TEXT,
            from_name TEXT,
            to_emails TEXT,
            cc_emails TEXT,
            bcc_emails TEXT,
            subject TEXT,
            date TEXT,
            in_reply_to TEXT,
            fetched_at TEXT
        )
    """)

    # Insert test data
    test_emails = [
        ('acc1', 'msg1', 't1', 'john@realty.com', 'John Smith', 'tu@jaguarcapital.co', '', '', 'Property inquiry', '2024-01-15', None, '2024-01-15'),
        ('acc1', 'msg2', 't1', 'tu@jaguarcapital.co', 'Tomas', 'john@realty.com', '', '', 'Re: Property inquiry', '2024-01-16', 'msg1', '2024-01-16'),
        ('acc1', 'msg3', 't2', 'jane@techstartup.io', 'Jane Doe', 'tu@jaguarcapital.co', '', '', 'Investment opportunity', '2024-01-17', None, '2024-01-17'),
        ('acc1', 'msg4', 't3', 'noreply@notifications.com', 'System', 'tu@jaguarcapital.co', '', '', 'Alert', '2024-01-18', None, '2024-01-18'),
        ('acc1', 'msg5', 't4', 'tu@jaguarcapital.co', 'Tomas', 'cold@example.com', '', '', 'Hello', '2024-01-19', None, '2024-01-19'),
    ]

    cursor.executemany("""
        INSERT INTO emails (account, message_id, thread_id, from_email, from_name, to_emails, cc_emails, bcc_emails, subject, date, in_reply_to, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, test_emails)

    conn.commit()
    conn.close()

    return temp_db


@pytest.fixture
def my_emails():
    """Set of user's own email addresses."""
    return {'tu@jaguarcapital.co', 'tomas@tujaguarcapital.com'}
```

**Step 2: Verify test setup works**

Run: `pytest scripts/contact_intel/tests/ --collect-only`
Expected: Shows conftest.py fixtures collected

**Step 3: Commit**

```bash
git add scripts/contact_intel/tests/
git commit -m "test(contact-intel): add test infrastructure and fixtures"
```

---

## Task 2: Extraction Database - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_extraction_db.py`
- Create: `scripts/contact_intel/extraction_db.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_extraction_db.py
"""Tests for extraction database module."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


class TestExtractionDB:
    """Tests for extraction_db module."""

    @pytest.fixture(autouse=True)
    def setup_temp_db(self, temp_db):
        """Use temporary database for each test."""
        self.db_path = temp_db
        # Patch DATA_DIR to use temp location
        self.patcher = patch('scripts.contact_intel.extraction_db.EXTRACTIONS_DB', temp_db)
        self.patcher.start()

    def teardown_method(self):
        self.patcher.stop()

    def test_init_db_creates_tables(self):
        """Should create contact_extractions, email_bodies, extraction_runs tables."""
        from scripts.contact_intel.extraction_db import init_db, get_connection

        init_db()

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert 'contact_extractions' in tables
        assert 'email_bodies' in tables
        assert 'extraction_runs' in tables

    def test_save_extraction_creates_record(self):
        """Should save extraction and retrieve it."""
        from scripts.contact_intel.extraction_db import init_db, save_extraction, get_connection

        init_db()
        save_extraction(
            email='test@example.com',
            name='Test User',
            company='Acme Corp',
            role='Engineer',
            topics=['tech', 'software'],
            confidence=0.85,
            source_emails=['msg1', 'msg2'],
            model='llama-3.3-70b',
        )

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM contact_extractions WHERE email = ?", ('test@example.com',))
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row['company'] == 'Acme Corp'
        assert row['role'] == 'Engineer'
        assert json.loads(row['topics']) == ['tech', 'software']
        assert row['confidence'] == 0.85

    def test_save_extraction_updates_existing(self):
        """Should update existing record on conflict."""
        from scripts.contact_intel.extraction_db import init_db, save_extraction, get_connection

        init_db()

        # First save
        save_extraction(
            email='test@example.com',
            name='Test User',
            company='Old Corp',
            role='Junior',
            topics=['old'],
            confidence=0.5,
            source_emails=['msg1'],
            model='llama-3.3-70b',
        )

        # Update
        save_extraction(
            email='test@example.com',
            name='Test User',
            company='New Corp',
            role='Senior',
            topics=['new'],
            confidence=0.9,
            source_emails=['msg2'],
            model='llama-3.3-70b',
        )

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT company, role FROM contact_extractions WHERE email = ?", ('test@example.com',))
        row = cursor.fetchone()
        conn.close()

        assert row['company'] == 'New Corp'
        assert row['role'] == 'Senior'

    def test_get_extracted_emails_returns_set(self):
        """Should return set of already extracted emails."""
        from scripts.contact_intel.extraction_db import init_db, save_extraction, get_extracted_emails

        init_db()

        save_extraction('a@test.com', 'A', 'Co', 'Role', [], 0.5, [], 'model')
        save_extraction('b@test.com', 'B', 'Co', 'Role', [], 0.5, [], 'model')

        extracted = get_extracted_emails()

        assert extracted == {'a@test.com', 'b@test.com'}

    def test_email_body_cache(self):
        """Should cache and retrieve email bodies."""
        from scripts.contact_intel.extraction_db import init_db, save_email_body, get_cached_body

        init_db()

        save_email_body('msg123', 'Hello, this is the body.')

        cached = get_cached_body('msg123')
        assert cached == 'Hello, this is the body.'

        # Non-existent returns None
        assert get_cached_body('nonexistent') is None

    def test_extraction_run_tracking(self):
        """Should track extraction runs with stats."""
        from scripts.contact_intel.extraction_db import (
            init_db, start_extraction_run, update_run_stats, get_run_stats, complete_run
        )

        init_db()

        run_id = start_extraction_run()
        assert run_id > 0

        update_run_stats(run_id, tokens=1000, cost=0.01, contacts=5)
        update_run_stats(run_id, tokens=2000, cost=0.02, contacts=10)

        stats = get_run_stats(run_id)
        assert stats['tokens_used'] == 3000
        assert stats['cost_usd'] == 0.03
        assert stats['contacts_processed'] == 15
        assert stats['status'] == 'running'

        complete_run(run_id)
        stats = get_run_stats(run_id)
        assert stats['status'] == 'completed'
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_extraction_db.py -v`
Expected: FAIL - ModuleNotFoundError: No module named 'scripts.contact_intel.extraction_db'

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/extraction_db.py
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
from typing import Dict, List, Optional, Set

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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_bodies (
            message_id TEXT PRIMARY KEY,
            body TEXT,
            fetched_at TIMESTAMP
        )
    """)

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


def get_extracted_emails() -> Set[str]:
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
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_extraction_db.py -v`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_extraction_db.py scripts/contact_intel/extraction_db.py
git commit -m "feat(contact-intel): add extraction database with TDD"
```

---

## Task 3: Contact Prioritizer - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_contact_prioritizer.py`
- Create: `scripts/contact_intel/contact_prioritizer.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_contact_prioritizer.py
"""Tests for contact prioritization module."""

import sqlite3
from unittest.mock import patch

import pytest


class TestContactPrioritizer:
    """Tests for contact prioritization logic."""

    def test_is_internal_domain(self):
        """Should identify internal company domains."""
        from scripts.contact_intel.contact_prioritizer import _is_internal

        assert _is_internal('john@jaguarcapital.co') is True
        assert _is_internal('jane@lahaus.com') is True
        assert _is_internal('bob@external.com') is False

    def test_is_automated_email(self):
        """Should identify automated/noreply emails."""
        from scripts.contact_intel.contact_prioritizer import _is_automated

        assert _is_automated('noreply@company.com') is True
        assert _is_automated('no-reply@service.io') is True
        assert _is_automated('notifications@app.com') is True
        assert _is_automated('john@company.com') is False

    def test_is_target_industry_real_estate(self):
        """Should identify real estate industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@compass.com')
        assert is_match is True
        assert industry == 'real_estate'

        is_match, industry = _is_target_industry('jane@cbre.com')
        assert is_match is True

        is_match, industry = _is_target_industry('bob@acmerealty.com')
        assert is_match is True

    def test_is_target_industry_finance(self):
        """Should identify finance/VC industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@acmecapital.com')
        assert is_match is True
        assert industry == 'finance'

        is_match, industry = _is_target_industry('jane@xyzpartners.com')
        assert is_match is True

        is_match, industry = _is_target_industry('bob@kaszek.com')
        assert is_match is True

    def test_is_target_industry_tech(self):
        """Should identify tech industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@google.com')
        assert is_match is True
        assert industry == 'tech'

        is_match, industry = _is_target_industry('jane@techstartup.io')
        assert is_match is True

    def test_is_target_industry_no_match(self):
        """Should return False for non-target industries."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@randomcompany.com')
        assert is_match is False
        assert industry == ''

    def test_get_prioritized_contacts_excludes_internal(self, mock_emails_db, my_emails):
        """Should exclude internal domain contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'tu@jaguarcapital.co' not in emails

    def test_get_prioritized_contacts_excludes_automated(self, mock_emails_db, my_emails):
        """Should exclude noreply/automated contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'noreply@notifications.com' not in emails

    def test_get_prioritized_contacts_excludes_one_way_outbound(self, mock_emails_db, my_emails):
        """Should exclude contacts we emailed but never replied."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        # cold@example.com only received email, never sent one
        assert 'cold@example.com' not in emails

    def test_get_prioritized_contacts_includes_replied(self, mock_emails_db, my_emails):
        """Should include contacts who replied to us."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'john@realty.com' in emails
        assert 'jane@techstartup.io' in emails

    def test_get_prioritized_contacts_assigns_tiers(self, mock_emails_db, my_emails):
        """Should assign priority tiers correctly."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        # john@realty.com is target industry (real estate) + replied = tier 1
        john = next((c for c in contacts if c['email'] == 'john@realty.com'), None)
        assert john is not None
        assert john['tier'] == 1
        assert john['industry'] == 'real_estate'

    def test_get_prioritized_contacts_respects_limit(self, mock_emails_db, my_emails):
        """Should respect the limit parameter."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=1)

        assert len(contacts) <= 1

    def test_get_prioritized_contacts_skips_already_extracted(self, mock_emails_db, my_emails):
        """Should skip already extracted contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        already_extracted = {'john@realty.com'}

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100, already_extracted=already_extracted)

        emails = [c['email'] for c in contacts]
        assert 'john@realty.com' not in emails
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_contact_prioritizer.py -v`
Expected: FAIL - ModuleNotFoundError

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/contact_prioritizer.py
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
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT from_email FROM emails")
    from_emails = {row[0].lower() for row in cursor.fetchall() if row[0]}
    conn.close()

    my_emails_lower = {e.lower() for e in my_emails}
    return from_emails - my_emails_lower


def get_prioritized_contacts(
    my_emails: Set[str],
    limit: int = 2500,
    already_extracted: Set[str] = None,
) -> List[Dict]:
    """Get prioritized list of contacts for extraction."""
    already_extracted = already_extracted or set()
    already_extracted_lower = {e.lower() for e in already_extracted}
    my_emails_lower = {e.lower() for e in my_emails}

    conn = sqlite3.connect(EMAILS_DB)
    cursor = conn.cursor()

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

    conn.close()

    # Get contacts who replied
    replied_contacts = get_contacts_who_replied(my_emails)

    # Build prioritized lists
    tier1, tier2, tier3 = [], [], []

    all_contacts = set(contacts_from.keys()) | set(emails_we_sent.keys())

    for email in all_contacts:
        email_lower = email.lower()

        # Skip filters
        if email_lower in already_extracted_lower:
            continue
        if email_lower in my_emails_lower:
            continue
        if _is_internal(email_lower):
            continue
        if _is_automated(email_lower):
            continue

        has_replied = email_lower in replied_contacts

        # Skip one-way outbound
        if not has_replied and email_lower in emails_we_sent and email_lower not in contacts_from:
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

    # Sort by email count
    tier1.sort(key=lambda x: x['email_count'], reverse=True)
    tier2.sort(key=lambda x: x['email_count'], reverse=True)
    tier3.sort(key=lambda x: x['email_count'], reverse=True)

    result = tier1 + tier2 + tier3
    logger.info(f"Prioritized: Tier1={len(tier1)}, Tier2={len(tier2)}, Tier3={len(tier3)}")

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
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_contact_prioritizer.py -v`
Expected: All 13 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_contact_prioritizer.py scripts/contact_intel/contact_prioritizer.py
git commit -m "feat(contact-intel): add contact prioritizer with TDD"
```

---

## Task 4: Groq Client - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_groq_client.py`
- Create: `scripts/contact_intel/groq_client.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_groq_client.py
"""Tests for Groq API client."""

from unittest.mock import MagicMock, patch

import pytest


class TestGroqClient:
    """Tests for GroqClient."""

    def test_calculate_cost(self):
        """Should calculate cost correctly."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        # 1M input tokens at $0.59, 1M output at $0.79
        cost = client._calculate_cost(1_000_000, 1_000_000)
        assert abs(cost - 1.38) < 0.01

        # 500 input, 100 output
        cost = client._calculate_cost(500, 100)
        expected = (500 / 1_000_000) * 0.59 + (100 / 1_000_000) * 0.79
        assert abs(cost - expected) < 0.0001

    def test_extract_contact_info_success(self):
        """Should extract contact info from mock API response."""
        from scripts.contact_intel.groq_client import GroqClient

        # Mock Groq response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"company": "Acme Corp", "role": "Engineer", "topics": ["tech"], "confidence": 0.9}'
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 50

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('scripts.contact_intel.groq_client.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.return_value = mock_response

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

        assert result.company == 'Acme Corp'
        assert result.role == 'Engineer'
        assert result.topics == ['tech']
        assert result.confidence == 0.9
        assert result.input_tokens == 500
        assert result.output_tokens == 50
        assert result.cost_usd > 0

    def test_extract_contact_info_handles_api_error(self):
        """Should return empty result on API error."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('scripts.contact_intel.groq_client.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.side_effect = Exception("API Error")

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

        assert result.company is None
        assert result.role is None
        assert result.topics == []
        assert result.confidence == 0.0

    def test_extract_contact_info_handles_invalid_json(self):
        """Should handle invalid JSON response."""
        from scripts.contact_intel.groq_client import GroqClient

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = 'not valid json'
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 50

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('scripts.contact_intel.groq_client.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.return_value = mock_response

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

        # Should return empty but not crash
        assert result.company is None
        assert result.input_tokens == 500

    def test_rate_limiting(self):
        """Should enforce rate limiting between requests."""
        from scripts.contact_intel.groq_client import GroqClient, REQUEST_DELAY
        import time

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        # Set last request time to now
        client.last_request_time = time.time()

        start = time.time()
        client._rate_limit()
        elapsed = time.time() - start

        # Should have waited approximately REQUEST_DELAY seconds
        assert elapsed >= REQUEST_DELAY * 0.9  # Allow 10% tolerance

    def test_missing_api_key_raises(self):
        """Should raise error if API key not set."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {}, clear=True):
            # Remove GROQ_API_KEY if exists
            import os
            if 'GROQ_API_KEY' in os.environ:
                del os.environ['GROQ_API_KEY']

            with pytest.raises(ValueError, match="GROQ_API_KEY"):
                GroqClient()
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_groq_client.py -v`
Expected: FAIL - ModuleNotFoundError

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/groq_client.py
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
        """Extract company, role, topics from contact's emails."""
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
            emails_text += f"Body:\n{e.get('body', '')[:3000]}\n"

        user_prompt = USER_PROMPT_TEMPLATE.format(
            name=name,
            email=email,
            emails_text=emails_text,
        )

        self._rate_limit()

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
            return ExtractionResult(
                company=None, role=None, topics=[], confidence=0.0,
                input_tokens=0, output_tokens=0, cost_usd=0.0,
            )

        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens
        cost = self._calculate_cost(input_tokens, output_tokens)

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
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_groq_client.py -v`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_groq_client.py scripts/contact_intel/groq_client.py
git commit -m "feat(contact-intel): add Groq client with TDD"
```

---

## Task 5: Body Fetcher - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_body_fetcher.py`
- Create: `scripts/contact_intel/body_fetcher.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_body_fetcher.py
"""Tests for email body fetcher."""

from unittest.mock import MagicMock, patch

import pytest


class TestBodyFetcher:
    """Tests for body fetching module."""

    def test_extract_body_from_plain_text(self):
        """Should extract body from plain text payload."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload
        import base64

        body_text = "Hello, this is a test email."
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        payload = {
            'body': {'data': encoded}
        }

        result = _extract_body_from_payload(payload)
        assert result == body_text

    def test_extract_body_from_multipart(self):
        """Should extract text/plain from multipart message."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload
        import base64

        body_text = "Plain text body"
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        payload = {
            'parts': [
                {'mimeType': 'text/html', 'body': {'data': base64.urlsafe_b64encode(b'<html>HTML</html>').decode()}},
                {'mimeType': 'text/plain', 'body': {'data': encoded}},
            ]
        }

        result = _extract_body_from_payload(payload)
        assert result == body_text

    def test_extract_body_falls_back_to_html(self):
        """Should strip HTML if no plain text available."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload
        import base64

        html = "<html><body><p>Hello</p> <b>World</b></body></html>"
        encoded = base64.urlsafe_b64encode(html.encode()).decode()

        payload = {
            'parts': [
                {'mimeType': 'text/html', 'body': {'data': encoded}},
            ]
        }

        result = _extract_body_from_payload(payload)
        assert 'Hello' in result
        assert 'World' in result
        assert '<html>' not in result

    def test_fetch_body_uses_cache(self):
        """Should return cached body without API call."""
        from scripts.contact_intel.body_fetcher import fetch_body

        with patch('scripts.contact_intel.body_fetcher.get_cached_body') as mock_cache:
            mock_cache.return_value = "Cached body content"

            result = fetch_body('msg123', 'test_account')

            assert result == "Cached body content"
            mock_cache.assert_called_once_with('msg123')

    def test_fetch_body_calls_api_on_cache_miss(self):
        """Should fetch from Gmail API if not cached."""
        from scripts.contact_intel.body_fetcher import fetch_body
        import base64

        body_text = "Fresh from API"
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        mock_service = MagicMock()
        mock_service.users().messages().get().execute.return_value = {
            'payload': {'body': {'data': encoded}}
        }

        with patch('scripts.contact_intel.body_fetcher.get_cached_body', return_value=None):
            with patch('scripts.contact_intel.body_fetcher._get_gmail_service', return_value=mock_service):
                with patch('scripts.contact_intel.body_fetcher.save_email_body') as mock_save:
                    result = fetch_body('msg123', 'test_account')

        assert result == body_text
        mock_save.assert_called_once_with('msg123', body_text)

    def test_get_contact_emails_with_body(self, mock_emails_db):
        """Should get emails for contact with bodies."""
        from scripts.contact_intel.body_fetcher import get_contact_emails_with_body

        with patch('scripts.contact_intel.body_fetcher.EMAILS_DB', mock_emails_db):
            with patch('scripts.contact_intel.body_fetcher.fetch_body', return_value="Test body"):
                emails = get_contact_emails_with_body('john@realty.com', limit=2)

        assert len(emails) <= 2
        assert all('body' in e for e in emails)
        assert all('subject' in e for e in emails)
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_body_fetcher.py -v`
Expected: FAIL - ModuleNotFoundError

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/body_fetcher.py
"""Fetch email bodies on-demand via Gmail API.

Caches bodies in extractions.db to avoid re-fetching.
"""

import base64
import logging
import re
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
        raise ImportError("Install google-api-python-client")

    token_path = get_token_path(account_name)
    creds = load_oauth_credentials(str(token_path))

    if not creds:
        raise ValueError(f"No credentials for account: {account_name}")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build('gmail', 'v1', credentials=creds, cache_discovery=False)


def _extract_body_from_payload(payload: dict) -> str:
    """Extract text body from Gmail API message payload."""
    body_text = ""

    # Direct body data
    if 'body' in payload and payload['body'].get('data'):
        body_text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        return body_text

    # Multipart - prefer text/plain
    if 'parts' in payload:
        for part in payload['parts']:
            mime_type = part.get('mimeType', '')
            if mime_type == 'text/plain':
                if 'body' in part and part['body'].get('data'):
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
            if 'parts' in part:
                nested = _extract_body_from_payload(part)
                if nested:
                    return nested

        # Fallback to HTML
        for part in payload['parts']:
            if part.get('mimeType') == 'text/html':
                if 'body' in part and part['body'].get('data'):
                    html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    body_text = re.sub(r'<[^>]+>', ' ', html)
                    body_text = re.sub(r'\s+', ' ', body_text).strip()
                    return body_text[:5000]

    return body_text


def fetch_body(message_id: str, account_name: str = "tujaguarcapital") -> Optional[str]:
    """Fetch email body, using cache if available."""
    cached = get_cached_body(message_id)
    if cached is not None:
        return cached

    try:
        service = _get_gmail_service(account_name)
        msg = service.users().messages().get(
            userId='me',
            id=message_id,
            format='full',
        ).execute()

        payload = msg.get('payload', {})
        body = _extract_body_from_payload(payload)

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
    """Get recent emails from/to a contact with bodies."""
    conn = sqlite3.connect(EMAILS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

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
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_body_fetcher.py -v`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_body_fetcher.py scripts/contact_intel/body_fetcher.py
git commit -m "feat(contact-intel): add body fetcher with TDD"
```

---

## Task 6: Entity Extractor CLI - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_entity_extractor.py`
- Create: `scripts/contact_intel/entity_extractor.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_entity_extractor.py
"""Tests for entity extractor main module."""

from unittest.mock import MagicMock, patch

import pytest


class TestEntityExtractor:
    """Tests for entity extractor."""

    def test_show_status_runs_without_error(self, capsys):
        """Should show status without crashing."""
        from scripts.contact_intel.entity_extractor import show_status

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_contact_stats', return_value={
                    'total': 100,
                    'tier_1_target_industry': 20,
                    'tier_2_active': 50,
                    'tier_3_replied': 30,
                    'by_industry': {'real_estate': 10, 'finance': 5},
                }):
                    show_status()

        captured = capsys.readouterr()
        assert 'Eligible contacts: 100' in captured.out
        assert 'Tier 1' in captured.out

    def test_run_extraction_respects_budget(self):
        """Should stop when budget is exhausted."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=10.0  # $10 per extraction
        )

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts', return_value=[
                    {'email': 'a@test.com', 'name': 'A', 'tier': 1, 'industry': 'tech'},
                    {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                    {'email': 'c@test.com', 'name': 'C', 'tier': 1, 'industry': 'tech'},
                ]):
                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction'):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            # Budget of $15 should only process 1 contact ($10 each)
                                            run_extraction(budget=15.0, resume=False)

        # Should have stopped after 1-2 contacts due to budget
        assert mock_client.return_value.extract_contact_info.call_count <= 2

    def test_run_extraction_saves_after_each_contact(self):
        """Should save extraction immediately after each contact (partial save)."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=0.01
        )

        save_calls = []

        def track_save(*args, **kwargs):
            save_calls.append(args)

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts', return_value=[
                    {'email': 'a@test.com', 'name': 'A', 'tier': 1, 'industry': 'tech'},
                    {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                ]):
                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction', side_effect=track_save):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            run_extraction(budget=50.0, resume=False)

        # Should have saved after each of the 2 contacts
        assert len(save_calls) == 2

    def test_run_extraction_resumes_from_extracted(self):
        """Should skip already extracted contacts on resume."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=0.01
        )

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            # a@test.com already extracted
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value={'a@test.com'}):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts') as mock_prioritize:
                    mock_prioritize.return_value = [
                        {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                    ]

                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction'):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            run_extraction(budget=50.0, resume=True)

        # Should have passed already_extracted to prioritizer
        call_args = mock_prioritize.call_args
        assert 'a@test.com' in call_args.kwargs.get('already_extracted', set())
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_entity_extractor.py -v`
Expected: FAIL - ModuleNotFoundError

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/entity_extractor.py
"""Entity Extractor - Extract company, role, topics from emails via Groq.

Usage:
    python scripts/contact_intel/entity_extractor.py --status
    python scripts/contact_intel/entity_extractor.py --budget 50
    python scripts/contact_intel/entity_extractor.py --resume
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
    init_db,
    save_extraction,
    start_extraction_run,
    update_run_stats,
)
from scripts.contact_intel.groq_client import GroqClient

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

    remaining = stats['total'] - len(already_extracted)
    est_cost = min(remaining, 2500) * 0.02
    print(f"\nEstimated cost for 2500 contacts: ${est_cost:.2f}")


def run_extraction(budget: float = 50.0, resume: bool = False):
    """Run entity extraction with budget limit."""
    init_db()

    already_extracted = get_extracted_emails() if resume else set()
    logger.info(f"Starting extraction (budget=${budget}, resume={resume})")
    logger.info(f"Already extracted: {len(already_extracted)} contacts")

    contacts = get_prioritized_contacts(
        my_emails=MY_EMAILS,
        limit=5000,
        already_extracted=already_extracted,
    )

    if not contacts:
        logger.info("No contacts to extract")
        return

    logger.info(f"Contacts to process: {len(contacts)}")

    client = GroqClient()
    run_id = start_extraction_run()
    total_cost = 0.0
    total_contacts = 0
    total_tokens = 0

    try:
        for i, contact in enumerate(contacts):
            if total_cost >= budget:
                logger.info(f"Budget exhausted: ${total_cost:.2f} >= ${budget}")
                break

            email = contact['email']
            name = contact['name']

            emails = get_contact_emails_with_body(email, limit=3)
            if not emails:
                logger.debug(f"No emails found for {email}")
                continue

            result = client.extract_contact_info(email=email, name=name, emails=emails)

            # Partial save immediately
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

            total_cost += result.cost_usd
            total_tokens += result.input_tokens + result.output_tokens
            total_contacts += 1

            update_run_stats(run_id, result.input_tokens + result.output_tokens, result.cost_usd, 1)

            if (i + 1) % 10 == 0:
                logger.info(f"[{i + 1}/{len(contacts)}] Processed {total_contacts}, cost=${total_cost:.2f}")

    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise
    finally:
        complete_run(run_id)
        logger.info(f"\nExtraction complete: {total_contacts} contacts, ${total_cost:.2f}, {total_tokens:,} tokens")


def sync_to_neo4j():
    """Sync extractions to Neo4j graph."""
    from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j
    sync_extractions_to_neo4j()


def main():
    parser = argparse.ArgumentParser(description='Extract entities from emails via Groq')
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--budget', type=float, default=50.0, help='Max USD to spend')
    parser.add_argument('--resume', action='store_true', help='Resume previous run')
    parser.add_argument('--sync', action='store_true', help='Sync to Neo4j')
    parser.add_argument('--email', type=str, help='Extract single contact')

    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.sync:
        sync_to_neo4j()
    elif args.email:
        init_db()
        client = GroqClient()
        emails = get_contact_emails_with_body(args.email, limit=3)
        if emails:
            result = client.extract_contact_info(args.email, args.email.split('@')[0], emails)
            print(f"Company: {result.company}\nRole: {result.role}\nTopics: {result.topics}\nConfidence: {result.confidence}\nCost: ${result.cost_usd:.4f}")
        else:
            print(f"No emails found for {args.email}")
    else:
        run_extraction(budget=args.budget, resume=args.resume)


if __name__ == "__main__":
    main()
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_entity_extractor.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_entity_extractor.py scripts/contact_intel/entity_extractor.py
git commit -m "feat(contact-intel): add entity extractor CLI with TDD"
```

---

## Task 7: Neo4j Sync - Tests First

**Files:**
- Create: `scripts/contact_intel/tests/test_extraction_sync.py`
- Create: `scripts/contact_intel/extraction_sync.py`

**Step 1: Write failing tests**

```python
# scripts/contact_intel/tests/test_extraction_sync.py
"""Tests for Neo4j extraction sync."""

from unittest.mock import MagicMock, patch

import pytest


class TestExtractionSync:
    """Tests for extraction sync to Neo4j."""

    def test_normalize_company_name(self):
        """Should normalize company names for matching."""
        from scripts.contact_intel.extraction_sync import _normalize_company_name

        assert _normalize_company_name('Acme Corp') == 'acme'
        assert _normalize_company_name('Google Inc.') == 'google'
        assert _normalize_company_name('Amazon LLC') == 'amazon'
        assert _normalize_company_name('Test Co') == 'test'
        assert _normalize_company_name('  Spaces  ') == 'spaces'
        assert _normalize_company_name(None) == ''
        assert _normalize_company_name('') == ''

    def test_sync_creates_company_nodes(self):
        """Should create Company nodes from extractions."""
        from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        with patch('scripts.contact_intel.extraction_sync.neo4j_available', return_value=True):
            with patch('scripts.contact_intel.extraction_sync.GraphBuilder', return_value=mock_gb):
                with patch('scripts.contact_intel.extraction_sync.sqlite3') as mock_sqlite:
                    mock_conn = MagicMock()
                    mock_cursor = MagicMock()
                    mock_cursor.fetchall.return_value = [
                        {'email': 'test@example.com', 'name': 'Test', 'company': 'Acme Corp', 'role': 'Engineer', 'topics': '["tech"]', 'confidence': 0.9}
                    ]
                    mock_conn.cursor.return_value = mock_cursor
                    mock_sqlite.connect.return_value = mock_conn

                    sync_extractions_to_neo4j()

        # Should have run Cypher queries for Company and WORKS_AT
        assert mock_session.run.called

    def test_sync_creates_topic_nodes(self):
        """Should create Topic nodes and DISCUSSED edges."""
        from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        with patch('scripts.contact_intel.extraction_sync.neo4j_available', return_value=True):
            with patch('scripts.contact_intel.extraction_sync.GraphBuilder', return_value=mock_gb):
                with patch('scripts.contact_intel.extraction_sync.sqlite3') as mock_sqlite:
                    mock_conn = MagicMock()
                    mock_cursor = MagicMock()
                    mock_cursor.fetchall.return_value = [
                        {'email': 'test@example.com', 'name': 'Test', 'company': None, 'role': None, 'topics': '["real estate", "investment"]', 'confidence': 0.8}
                    ]
                    mock_conn.cursor.return_value = mock_cursor
                    mock_sqlite.connect.return_value = mock_conn

                    sync_extractions_to_neo4j()

        # Should have created Topic nodes
        assert mock_session.run.called

    def test_sync_handles_neo4j_unavailable(self, capsys):
        """Should handle Neo4j not being available."""
        from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j

        with patch('scripts.contact_intel.extraction_sync.neo4j_available', return_value=False):
            sync_extractions_to_neo4j()

        # Should not crash, just log error
```

**Step 2: Run tests - verify they fail**

Run: `pytest scripts/contact_intel/tests/test_extraction_sync.py -v`
Expected: FAIL - ModuleNotFoundError

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/extraction_sync.py
"""Sync LLM extractions to Neo4j graph."""

import json
import logging
import sqlite3
from typing import Dict

from scripts.contact_intel.config import DATA_DIR
from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

logger = logging.getLogger(__name__)

EXTRACTIONS_DB = DATA_DIR / "extractions.db"


def _normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ''
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

            try:
                topics = json.loads(topics_json) if topics_json else []
            except json.JSONDecodeError:
                topics = []

            if company:
                normalized = _normalize_company_name(company)

                with gb.driver.session() as session:
                    session.run("""
                        MERGE (c:Company {normalized_name: $normalized})
                        ON CREATE SET c.name = $name, c.created_at = datetime()
                    """, normalized=normalized, name=company)
                    companies_created += 1

                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (c:Company {normalized_name: $normalized})
                        MERGE (p)-[r:WORKS_AT]->(c)
                        ON CREATE SET r.role = $role, r.confidence = $confidence, r.created_at = datetime()
                        ON MATCH SET r.role = $role, r.confidence = $confidence, r.updated_at = datetime()
                    """, email=email, normalized=normalized, role=role, confidence=confidence)
                    works_at_created += 1

            for topic in topics:
                if not topic:
                    continue

                topic_normalized = topic.lower().strip()

                with gb.driver.session() as session:
                    session.run("""
                        MERGE (t:Topic {name: $name})
                        ON CREATE SET t.created_at = datetime()
                    """, name=topic_normalized)
                    topics_created += 1

                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (t:Topic {name: $name})
                        MERGE (p)-[r:DISCUSSED]->(t)
                        ON CREATE SET r.created_at = datetime()
                        ON MATCH SET r.updated_at = datetime()
                    """, email=email, name=topic_normalized)
                    discussed_created += 1

        logger.info(f"Sync complete: {companies_created} companies, {topics_created} topics, {works_at_created} WORKS_AT, {discussed_created} DISCUSSED")

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
```

**Step 4: Run tests - verify they pass**

Run: `pytest scripts/contact_intel/tests/test_extraction_sync.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/tests/test_extraction_sync.py scripts/contact_intel/extraction_sync.py
git commit -m "feat(contact-intel): add Neo4j sync with TDD"
```

---

## Task 8: Run All Tests & Final Commit

**Step 1: Run all tests**

Run: `pytest scripts/contact_intel/tests/ -v`
Expected: All tests PASS

**Step 2: Final commit**

```bash
git add -A
git commit -m "feat(contact-intel): complete Phase 3 LLM enrichment with TDD

Modules:
- extraction_db.py: SQLite cache with partial saves
- groq_client.py: Groq API with rate limiting
- contact_prioritizer.py: Industry-based prioritization
- body_fetcher.py: On-demand Gmail body fetching
- entity_extractor.py: Main CLI
- extraction_sync.py: Neo4j sync

All modules developed with TDD - tests written first.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

**Step 3: Push**

```bash
git push origin main
```

---

## Summary

| Module | Test File | Tests |
|--------|-----------|-------|
| `extraction_db.py` | `test_extraction_db.py` | 6 tests |
| `contact_prioritizer.py` | `test_contact_prioritizer.py` | 13 tests |
| `groq_client.py` | `test_groq_client.py` | 6 tests |
| `body_fetcher.py` | `test_body_fetcher.py` | 6 tests |
| `entity_extractor.py` | `test_entity_extractor.py` | 4 tests |
| `extraction_sync.py` | `test_extraction_sync.py` | 4 tests |

**Total: ~39 tests**

**Key TDD principles followed:**
1. Tests written before implementation
2. Run tests to verify they fail
3. Write minimal code to pass
4. Refactor while green
5. Commit after each module
