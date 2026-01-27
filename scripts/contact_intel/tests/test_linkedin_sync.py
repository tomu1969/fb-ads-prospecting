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
