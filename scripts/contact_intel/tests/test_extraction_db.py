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
        # Patch EXTRACTIONS_DB to use temp location
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
