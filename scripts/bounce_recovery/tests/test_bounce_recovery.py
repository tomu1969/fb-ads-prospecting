"""Tests for Bounce Recovery module.

TDD approach - tests written before implementation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import tempfile
import os


class TestDomainExtraction:
    """Tests for extracting domain from email."""

    def test_extract_domain_from_email(self):
        """Should extract domain from email address."""
        from scripts.bounce_recovery.bounce_recovery import extract_domain

        assert extract_domain('test@example.com') == 'example.com'
        assert extract_domain('user@subdomain.example.org') == 'subdomain.example.org'

    def test_extract_domain_handles_invalid(self):
        """Should return None for invalid emails."""
        from scripts.bounce_recovery.bounce_recovery import extract_domain

        assert extract_domain('invalid') is None
        assert extract_domain('') is None
        assert extract_domain(None) is None


class TestGenericEmails:
    """Tests for generating generic email variants."""

    def test_generate_generic_emails(self):
        """Should generate common generic email patterns."""
        from scripts.bounce_recovery.bounce_recovery import generate_generic_emails

        generics = generate_generic_emails('example.com')

        assert 'info@example.com' in generics
        assert 'contact@example.com' in generics
        assert 'hello@example.com' in generics
        assert 'sales@example.com' in generics

    def test_generate_generic_emails_empty_domain(self):
        """Should return empty list for invalid domain."""
        from scripts.bounce_recovery.bounce_recovery import generate_generic_emails

        assert generate_generic_emails('') == []
        assert generate_generic_emails(None) == []


class TestHunterAlternatives:
    """Tests for finding alternative contacts via Hunter."""

    @patch('scripts.bounce_recovery.bounce_recovery.requests.get')
    def test_get_hunter_alternatives(self, mock_get):
        """Should return alternative emails from Hunter domain search."""
        from scripts.bounce_recovery.bounce_recovery import get_hunter_alternatives

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            'data': {
                'emails': [
                    {'value': 'john@example.com', 'confidence': 90},
                    {'value': 'jane@example.com', 'confidence': 85},
                ]
            }
        }

        alternatives = get_hunter_alternatives('example.com', exclude='bounced@example.com')

        emails = [a['email'] for a in alternatives]
        assert 'john@example.com' in emails
        assert 'jane@example.com' in emails

    @patch('scripts.bounce_recovery.bounce_recovery.requests.get')
    def test_get_hunter_alternatives_excludes_bounced(self, mock_get):
        """Should exclude the originally bounced email."""
        from scripts.bounce_recovery.bounce_recovery import get_hunter_alternatives

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            'data': {
                'emails': [
                    {'value': 'bounced@example.com', 'confidence': 90},
                    {'value': 'other@example.com', 'confidence': 85},
                ]
            }
        }

        alternatives = get_hunter_alternatives('example.com', exclude='bounced@example.com')

        emails = [a['email'] for a in alternatives]
        assert 'bounced@example.com' not in emails
        assert 'other@example.com' in emails


class TestRecoveryStrategies:
    """Tests for recovery strategy execution."""

    @pytest.fixture
    def bounced_csv(self):
        """Create a temporary bounced contacts CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,page_name,contact_name,website_url\n")
            f.write("bad@example.com,Example Co,John Doe,https://example.com\n")
            f.write("invalid@test.org,Test Inc,Jane Doe,https://test.org\n")
            return f.name

    @patch('scripts.bounce_recovery.bounce_recovery.try_generic_emails')
    @patch('scripts.bounce_recovery.bounce_recovery.get_hunter_alternatives')
    def test_recover_contact_tries_strategies_in_order(self, mock_hunter, mock_generic, bounced_csv):
        """Should try recovery strategies in order."""
        from scripts.bounce_recovery.bounce_recovery import recover_contact

        mock_generic.return_value = None  # First strategy fails
        mock_hunter.return_value = [{'email': 'alt@example.com', 'confidence': 90, 'first_name': '', 'last_name': '', 'position': ''}]  # Second succeeds

        result = recover_contact(
            email='bad@example.com',
            domain='example.com'
        )

        mock_generic.assert_called_once()
        mock_hunter.assert_called_once()
        assert result['new_email'] == 'alt@example.com'
        assert result['recovery_method'] == 'hunter_alt'

        os.unlink(bounced_csv)

    @patch('scripts.bounce_recovery.bounce_recovery.try_generic_emails')
    def test_recover_contact_generic_success(self, mock_generic):
        """Should use generic email when found."""
        from scripts.bounce_recovery.bounce_recovery import recover_contact

        mock_generic.return_value = 'info@example.com'

        result = recover_contact(
            email='bad@example.com',
            domain='example.com'
        )

        assert result['new_email'] == 'info@example.com'
        assert result['recovery_method'] == 'generic_email'


class TestBatchRecovery:
    """Tests for batch recovery processing."""

    @pytest.fixture
    def bounced_csv(self):
        """Create a temporary bounced contacts CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,page_name,contact_name,website_url\n")
            f.write("bad@example.com,Example Co,John Doe,https://example.com\n")
            f.write("invalid@test.org,Test Inc,Jane Doe,https://test.org\n")
            return f.name

    @patch('scripts.bounce_recovery.bounce_recovery.recover_contact')
    def test_process_bounced_csv(self, mock_recover, bounced_csv):
        """Should process all bounced contacts."""
        from scripts.bounce_recovery.bounce_recovery import process_bounced_csv

        mock_recover.return_value = {
            'recovered': True,
            'new_email': 'info@example.com',
            'recovery_method': 'generic_email'
        }

        results = process_bounced_csv(bounced_csv)

        assert mock_recover.call_count == 2
        assert len(results) == 2

        os.unlink(bounced_csv)

    @patch('scripts.bounce_recovery.bounce_recovery.recover_contact')
    def test_process_bounced_csv_saves_output(self, mock_recover, bounced_csv):
        """Should save recovered contacts to output file."""
        from scripts.bounce_recovery.bounce_recovery import process_bounced_csv

        mock_recover.return_value = {
            'recovered': True,
            'new_email': 'info@example.com',
            'recovery_method': 'generic_email'
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as out:
            output_path = out.name

        process_bounced_csv(bounced_csv, output_path=output_path)

        assert os.path.exists(output_path)
        df = pd.read_csv(output_path)
        assert 'recovered_email' in df.columns
        assert 'recovery_method' in df.columns

        os.unlink(bounced_csv)
        os.unlink(output_path)


class TestResultsSummary:
    """Tests for recovery results summary."""

    def test_print_recovery_summary(self, caplog):
        """Should print summary of recovery results."""
        from scripts.bounce_recovery.bounce_recovery import print_recovery_summary
        import logging

        results = [
            {'email': 'a@test.com', 'recovered': True, 'recovery_method': 'generic_email'},
            {'email': 'b@test.com', 'recovered': True, 'recovery_method': 'hunter_alt'},
            {'email': 'c@test.com', 'recovered': False, 'recovery_method': None},
        ]

        with caplog.at_level(logging.INFO):
            print_recovery_summary(results)

        assert 'Recovered: 2' in caplog.text
        assert 'Unrecoverable: 1' in caplog.text


class TestCLIArguments:
    """Tests for CLI argument parsing."""

    def test_parse_args_input(self):
        """Should parse input file path."""
        from scripts.bounce_recovery.bounce_recovery import parse_args

        args = parse_args(['--input', 'bounced.csv'])
        assert args.input == 'bounced.csv'

    def test_parse_args_output(self):
        """Should parse output file path."""
        from scripts.bounce_recovery.bounce_recovery import parse_args

        args = parse_args(['--input', 'bounced.csv', '--output', 'recovered.csv'])
        assert args.output == 'recovered.csv'

    def test_parse_args_dry_run(self):
        """Should parse dry-run flag."""
        from scripts.bounce_recovery.bounce_recovery import parse_args

        args = parse_args(['--input', 'bounced.csv', '--dry-run'])
        assert args.dry_run is True

    def test_parse_args_defaults(self):
        """Should have sensible defaults."""
        from scripts.bounce_recovery.bounce_recovery import parse_args

        args = parse_args(['--input', 'bounced.csv'])
        assert args.dry_run is False
        assert args.verbose is False
