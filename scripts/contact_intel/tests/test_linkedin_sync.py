"""Tests for LinkedIn CSV sync module."""

import os
import tempfile
from pathlib import Path

import pytest


class TestParseLinkedInCSV:
    """Tests for parse_linkedin_csv function."""

    def test_parse_connections_csv(self):
        """Should parse valid LinkedIn Connections.csv with 3 rows."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        # Create temp CSV file with LinkedIn format
        csv_content = """First Name,Last Name,Email Address,Company,Position,Connected On
John,Smith,john@acme.com,Acme Corp,CEO,15 Jan 2024
Jane,Doe,jane@startup.io,TechStartup,CTO,20 Feb 2024
Bob,Wilson,,Big Corp,Manager,01 Mar 2024"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            result = parse_linkedin_csv(csv_path)

            assert len(result) == 3

            # Check first row
            assert result[0]['first_name'] == 'John'
            assert result[0]['last_name'] == 'Smith'
            assert result[0]['email'] == 'john@acme.com'
            assert result[0]['company'] == 'Acme Corp'
            assert result[0]['position'] == 'CEO'
            assert result[0]['connected_on'] == '15 Jan 2024'

            # Check second row
            assert result[1]['first_name'] == 'Jane'
            assert result[1]['last_name'] == 'Doe'
            assert result[1]['email'] == 'jane@startup.io'
            assert result[1]['company'] == 'TechStartup'
            assert result[1]['position'] == 'CTO'
            assert result[1]['connected_on'] == '20 Feb 2024'

            # Check third row - missing email should be None
            assert result[2]['first_name'] == 'Bob'
            assert result[2]['last_name'] == 'Wilson'
            assert result[2]['email'] is None
            assert result[2]['company'] == 'Big Corp'
            assert result[2]['position'] == 'Manager'
            assert result[2]['connected_on'] == '01 Mar 2024'

        finally:
            os.unlink(csv_path)

    def test_parse_empty_csv(self):
        """Should handle headers-only CSV gracefully (return empty list)."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        # Create temp CSV with only headers
        csv_content = """First Name,Last Name,Email Address,Company,Position,Connected On"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            result = parse_linkedin_csv(csv_path)
            assert result == []
        finally:
            os.unlink(csv_path)

    def test_parse_missing_file(self):
        """Should raise FileNotFoundError for missing files."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        non_existent_path = Path('/nonexistent/path/Connections.csv')

        with pytest.raises(FileNotFoundError):
            parse_linkedin_csv(non_existent_path)


class TestLinkedInSchema:
    """Tests for LinkedIn schema in Neo4j."""

    def test_setup_linkedin_schema(self):
        """Should create index on linkedin_url."""
        from unittest.mock import MagicMock

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        from scripts.contact_intel.graph_builder import GraphBuilder
        gb = GraphBuilder()
        gb.driver = mock_driver
        gb.setup_linkedin_schema()

        # Should have created linkedin_url index
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('linkedin_url' in c for c in calls)


class TestLinkedInSync:
    """Tests for syncing LinkedIn connections to Neo4j."""

    def test_create_linkedin_connected_edge(self):
        """Should create LINKEDIN_CONNECTED relationship."""
        from unittest.mock import MagicMock

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@acme.com',
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        result = create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        assert result is True
        # Should have run MERGE for LINKEDIN_CONNECTED
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('LINKEDIN_CONNECTED' in c for c in calls)

    def test_skip_connection_without_email(self):
        """Should skip connections without email address."""
        from unittest.mock import MagicMock

        mock_gb = MagicMock()

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': None,  # No email
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        result = create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        assert result is False
        mock_gb.driver.session.assert_not_called()

    def test_skip_connection_with_empty_email(self):
        """Should skip connections with empty string email."""
        from unittest.mock import MagicMock

        mock_gb = MagicMock()

        connection = {
            'first_name': 'Jane',
            'last_name': 'Doe',
            'email': '',  # Empty string email
            'company': 'TechCo',
            'position': 'CTO',
            'connected_on': '20 Feb 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        result = create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        assert result is False
        mock_gb.driver.session.assert_not_called()

    def test_creates_person_node_with_linkedin_properties(self):
        """Should create Person node with company and position from LinkedIn."""
        from unittest.mock import MagicMock

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        connection = {
            'first_name': 'Alice',
            'last_name': 'Johnson',
            'email': 'alice@startup.io',
            'company': 'StartupCo',
            'position': 'VP Engineering',
            'connected_on': '10 Mar 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        # Check that Person MERGE was called with correct parameters
        calls = mock_session.run.call_args_list
        # First call should be for creating/updating the Person node
        first_call_args = str(calls[0])
        assert 'MERGE' in first_call_args
        assert 'Person' in first_call_args
        assert 'alice@startup.io' in first_call_args or 'email' in first_call_args
