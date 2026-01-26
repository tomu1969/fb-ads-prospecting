"""SQLite database for LLM extraction results.

Handles:
- Contact extraction results (company, role, topics)
- Email body cache (fetched on-demand)
- Budget/run tracking
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from scripts.contact_intel.config import DATA_DIR, ensure_data_dir

logger = logging.getLogger(__name__)

EXTRACTIONS_DB = DATA_DIR / "extractions.db"


@contextmanager
def get_db_connection():
    """Context manager for safe database connections.

    Ensures connections are always closed, even on error.
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating DB if needed."""
    ensure_data_dir()
    conn = sqlite3.connect(EXTRACTIONS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the extractions database schema."""
    logger.info("Initializing extraction database at %s", EXTRACTIONS_DB)
    with get_db_connection() as conn:
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
        logger.debug("Database schema initialized successfully")


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
    """Save extraction result for a contact (partial save).

    Args:
        email: Contact email address (required, non-empty)
        name: Contact name
        company: Company name (optional)
        role: Job role (optional)
        topics: List of interest topics
        confidence: Confidence score between 0 and 1
        source_emails: List of source email message IDs
        model: LLM model used for extraction

    Raises:
        ValueError: If email is empty, confidence out of range, or invalid list types
    """
    # Input validation
    if not email or not isinstance(email, str):
        raise ValueError("email must be a non-empty string")
    if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
        raise ValueError("confidence must be a number between 0 and 1")
    if not isinstance(topics, list):
        raise ValueError("topics must be a list")
    if not isinstance(source_emails, list):
        raise ValueError("source_emails must be a list")

    logger.debug("Saving extraction for %s (confidence=%.2f)", email, confidence)
    with get_db_connection() as conn:
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
        logger.debug("Extraction saved for %s", email)


def get_extracted_emails() -> Set[str]:
    """Get set of emails already extracted."""
    logger.debug("Fetching extracted emails")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT email FROM contact_extractions WHERE extracted_at IS NOT NULL")
        emails = {row['email'] for row in cursor.fetchall()}
        logger.debug("Found %d extracted emails", len(emails))
        return emails


def save_email_body(message_id: str, body: str):
    """Cache an email body."""
    logger.debug("Caching email body for message_id=%s", message_id)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO email_bodies (message_id, body, fetched_at)
            VALUES (?, ?, ?)
        """, (message_id, body, datetime.now().isoformat()))
        conn.commit()


def get_cached_body(message_id: str) -> Optional[str]:
    """Get cached email body if available."""
    logger.debug("Looking up cached body for message_id=%s", message_id)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT body FROM email_bodies WHERE message_id = ?", (message_id,))
        row = cursor.fetchone()
        if row:
            logger.debug("Cache hit for message_id=%s", message_id)
            return row['body']
        logger.debug("Cache miss for message_id=%s", message_id)
        return None


def start_extraction_run() -> int:
    """Start a new extraction run, return run ID."""
    logger.info("Starting new extraction run")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO extraction_runs DEFAULT VALUES")
        run_id = cursor.lastrowid
        conn.commit()
        logger.info("Started extraction run_id=%d", run_id)
        return run_id


def update_run_stats(run_id: int, tokens: int, cost: float, contacts: int):
    """Update extraction run statistics."""
    logger.debug("Updating run_id=%d: tokens=%d, cost=%.4f, contacts=%d",
                 run_id, tokens, cost, contacts)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE extraction_runs
            SET tokens_used = tokens_used + ?,
                cost_usd = cost_usd + ?,
                contacts_processed = contacts_processed + ?
            WHERE id = ?
        """, (tokens, cost, contacts, run_id))
        conn.commit()


def get_run_stats(run_id: int) -> Dict:
    """Get stats for an extraction run.

    Returns empty dict {} if run_id doesn't exist. This is intentional behavior
    to allow callers to check if a run exists by testing `if stats:`.
    """
    logger.debug("Fetching stats for run_id=%d", run_id)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM extraction_runs WHERE id = ?", (run_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        # Return empty dict for non-existent runs (allows `if stats:` checks)
        logger.debug("No stats found for run_id=%d", run_id)
        return {}


def complete_run(run_id: int):
    """Mark extraction run as completed."""
    logger.info("Completing extraction run_id=%d", run_id)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE extraction_runs SET status = 'completed' WHERE id = ?", (run_id,))
        conn.commit()
        logger.info("Extraction run_id=%d marked as completed", run_id)
