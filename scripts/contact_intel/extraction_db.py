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
