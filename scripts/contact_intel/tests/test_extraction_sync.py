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
