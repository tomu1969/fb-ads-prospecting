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
