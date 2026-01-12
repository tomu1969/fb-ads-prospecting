"""Tests for SMTP Verifier module.

TDD approach - tests written before implementation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import tempfile
import os


class TestMXLookup:
    """Tests for MX record lookup functionality."""

    @patch('scripts.smtp_verifier.smtp_verifier.dns.resolver.resolve')
    def test_get_mx_record_returns_host(self, mock_resolve):
        """get_mx_record should return the mail server hostname."""
        from scripts.smtp_verifier.smtp_verifier import get_mx_record

        # Mock MX record response
        mock_mx = Mock()
        mock_mx.exchange = Mock()
        mock_mx.exchange.to_text.return_value = 'mail.example.com.'
        mock_mx.preference = 10
        mock_resolve.return_value = [mock_mx]

        result = get_mx_record('example.com')
        assert result == 'mail.example.com'

    @patch('scripts.smtp_verifier.smtp_verifier.dns.resolver.resolve')
    def test_get_mx_record_returns_lowest_preference(self, mock_resolve):
        """Should return the MX record with lowest preference (highest priority)."""
        from scripts.smtp_verifier.smtp_verifier import get_mx_record

        # Mock multiple MX records
        mock_mx1 = Mock()
        mock_mx1.exchange.to_text.return_value = 'backup.example.com.'
        mock_mx1.preference = 20

        mock_mx2 = Mock()
        mock_mx2.exchange.to_text.return_value = 'primary.example.com.'
        mock_mx2.preference = 10

        mock_resolve.return_value = [mock_mx1, mock_mx2]

        result = get_mx_record('example.com')
        assert result == 'primary.example.com'

    @patch('scripts.smtp_verifier.smtp_verifier.dns.resolver.resolve')
    def test_get_mx_record_handles_no_mx(self, mock_resolve):
        """Should return None when no MX record exists."""
        from scripts.smtp_verifier.smtp_verifier import get_mx_record
        import dns.resolver

        mock_resolve.side_effect = dns.resolver.NoAnswer()

        result = get_mx_record('example.com')
        assert result is None

    @patch('scripts.smtp_verifier.smtp_verifier.dns.resolver.resolve')
    def test_get_mx_record_handles_nxdomain(self, mock_resolve):
        """Should return None when domain doesn't exist."""
        from scripts.smtp_verifier.smtp_verifier import get_mx_record
        import dns.resolver

        mock_resolve.side_effect = dns.resolver.NXDOMAIN()

        result = get_mx_record('nonexistent-domain-12345.com')
        assert result is None


class TestSMTPVerify:
    """Tests for SMTP verification functionality."""

    @patch('scripts.smtp_verifier.smtp_verifier.get_mx_record')
    @patch('scripts.smtp_verifier.smtp_verifier.smtplib.SMTP')
    def test_verify_email_valid(self, mock_smtp, mock_mx):
        """verify_email should return 'valid' for existing mailbox."""
        from scripts.smtp_verifier.smtp_verifier import verify_email

        mock_mx.return_value = 'mail.example.com'
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = Mock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = Mock(return_value=False)
        mock_server.rcpt.return_value = (250, b'OK')

        result = verify_email('test@example.com')
        assert result['status'] == 'valid'
        assert result['code'] == 250

    @patch('scripts.smtp_verifier.smtp_verifier.get_mx_record')
    @patch('scripts.smtp_verifier.smtp_verifier.smtplib.SMTP')
    def test_verify_email_invalid(self, mock_smtp, mock_mx):
        """verify_email should return 'invalid' for non-existent mailbox."""
        from scripts.smtp_verifier.smtp_verifier import verify_email

        mock_mx.return_value = 'mail.example.com'
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = Mock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = Mock(return_value=False)
        mock_server.rcpt.return_value = (550, b'User unknown')

        result = verify_email('nonexistent@example.com')
        assert result['status'] == 'invalid'
        assert result['code'] == 550

    @patch('scripts.smtp_verifier.smtp_verifier.get_mx_record')
    @patch('scripts.smtp_verifier.smtp_verifier.smtplib.SMTP')
    def test_verify_email_catch_all(self, mock_smtp, mock_mx):
        """verify_email should return 'catch_all' when server accepts all."""
        from scripts.smtp_verifier.smtp_verifier import verify_email

        mock_mx.return_value = 'mail.example.com'
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = Mock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = Mock(return_value=False)
        mock_server.rcpt.return_value = (252, b'Cannot verify')

        result = verify_email('test@example.com')
        assert result['status'] == 'catch_all'
        assert result['code'] == 252

    @patch('scripts.smtp_verifier.smtp_verifier.get_mx_record')
    def test_verify_email_no_mx(self, mock_mx):
        """verify_email should return 'invalid' when no MX record."""
        from scripts.smtp_verifier.smtp_verifier import verify_email

        mock_mx.return_value = None

        result = verify_email('test@nonexistent.com')
        assert result['status'] == 'invalid'
        assert 'no MX' in result['message'].lower() or result['code'] is None

    @patch('scripts.smtp_verifier.smtp_verifier.get_mx_record')
    @patch('scripts.smtp_verifier.smtp_verifier.smtplib.SMTP')
    def test_verify_email_connection_error(self, mock_smtp, mock_mx):
        """verify_email should return 'unknown' on connection error."""
        from scripts.smtp_verifier.smtp_verifier import verify_email

        mock_mx.return_value = 'mail.example.com'
        mock_smtp.side_effect = Exception("Connection refused")

        result = verify_email('test@example.com')
        assert result['status'] == 'unknown'
        assert 'error' in result['message'].lower() or 'connection' in result['message'].lower()


class TestEmailValidation:
    """Tests for email format validation."""

    def test_is_valid_email_format_valid(self):
        """Should return True for valid email format."""
        from scripts.smtp_verifier.smtp_verifier import is_valid_email_format

        assert is_valid_email_format('test@example.com') is True
        assert is_valid_email_format('user.name@domain.co.uk') is True
        assert is_valid_email_format('user+tag@example.org') is True

    def test_is_valid_email_format_invalid(self):
        """Should return False for invalid email format."""
        from scripts.smtp_verifier.smtp_verifier import is_valid_email_format

        assert is_valid_email_format('invalid-email') is False
        assert is_valid_email_format('@example.com') is False
        assert is_valid_email_format('user@') is False
        assert is_valid_email_format('') is False
        assert is_valid_email_format(None) is False


class TestBatchVerification:
    """Tests for batch email verification."""

    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,page_name,contact_name\n")
            f.write("test1@example.com,Company A,John\n")
            f.write("test2@example.com,Company B,Jane\n")
            f.write("test3@example.com,Company C,Bob\n")
            return f.name

    @patch('scripts.smtp_verifier.smtp_verifier.verify_email')
    def test_verify_csv_returns_results(self, mock_verify, sample_csv):
        """verify_csv should return verification results."""
        from scripts.smtp_verifier.smtp_verifier import verify_csv

        mock_verify.return_value = {'status': 'valid', 'code': 250, 'message': 'OK'}

        results = verify_csv(sample_csv)

        assert len(results) == 3
        assert all(r['smtp_status'] == 'valid' for r in results)

        os.unlink(sample_csv)

    @patch('scripts.smtp_verifier.smtp_verifier.verify_email')
    def test_verify_csv_updates_file(self, mock_verify, sample_csv):
        """verify_csv should add smtp_status column to CSV."""
        from scripts.smtp_verifier.smtp_verifier import verify_csv

        mock_verify.return_value = {'status': 'valid', 'code': 250, 'message': 'OK'}

        verify_csv(sample_csv, update_file=True)

        df = pd.read_csv(sample_csv)
        assert 'smtp_status' in df.columns
        assert all(df['smtp_status'] == 'valid')

        os.unlink(sample_csv)

    @patch('scripts.smtp_verifier.smtp_verifier.verify_email')
    def test_verify_csv_with_output(self, mock_verify, sample_csv):
        """verify_csv should write to output file if specified."""
        from scripts.smtp_verifier.smtp_verifier import verify_csv

        mock_verify.return_value = {'status': 'valid', 'code': 250, 'message': 'OK'}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as out:
            output_path = out.name

        verify_csv(sample_csv, output_path=output_path)

        assert os.path.exists(output_path)
        df = pd.read_csv(output_path)
        assert 'smtp_status' in df.columns

        os.unlink(sample_csv)
        os.unlink(output_path)

    @patch('scripts.smtp_verifier.smtp_verifier.verify_email')
    def test_verify_csv_respects_limit(self, mock_verify, sample_csv):
        """verify_csv should respect --limit parameter."""
        from scripts.smtp_verifier.smtp_verifier import verify_csv

        mock_verify.return_value = {'status': 'valid', 'code': 250, 'message': 'OK'}

        results = verify_csv(sample_csv, limit=2)

        assert mock_verify.call_count == 2
        assert len([r for r in results if 'smtp_status' in r]) == 2

        os.unlink(sample_csv)


class TestCLIArguments:
    """Tests for CLI argument parsing."""

    def test_parse_args_single_email(self):
        """Should parse single email verification."""
        from scripts.smtp_verifier.smtp_verifier import parse_args

        args = parse_args(['--email', 'test@example.com'])
        assert args.email == 'test@example.com'
        assert args.csv is None

    def test_parse_args_csv(self):
        """Should parse CSV verification."""
        from scripts.smtp_verifier.smtp_verifier import parse_args

        args = parse_args(['--csv', 'contacts.csv'])
        assert args.csv == 'contacts.csv'
        assert args.email is None

    def test_parse_args_output(self):
        """Should parse output file path."""
        from scripts.smtp_verifier.smtp_verifier import parse_args

        args = parse_args(['--csv', 'contacts.csv', '--output', 'verified.csv'])
        assert args.output == 'verified.csv'

    def test_parse_args_timeout(self):
        """Should parse timeout parameter."""
        from scripts.smtp_verifier.smtp_verifier import parse_args

        args = parse_args(['--email', 'test@example.com', '--timeout', '5'])
        assert args.timeout == 5

    def test_parse_args_defaults(self):
        """Should have sensible defaults."""
        from scripts.smtp_verifier.smtp_verifier import parse_args

        args = parse_args(['--email', 'test@example.com'])
        assert args.timeout == 10
        assert args.verbose is False


class TestResultsSummary:
    """Tests for results summary functionality."""

    def test_print_summary_logs_output(self, caplog):
        """print_summary should log verification results."""
        from scripts.smtp_verifier.smtp_verifier import print_summary
        import logging

        results = [
            {'email': 'a@test.com', 'smtp_status': 'valid'},
            {'email': 'b@test.com', 'smtp_status': 'valid'},
            {'email': 'c@test.com', 'smtp_status': 'invalid'},
            {'email': 'd@test.com', 'smtp_status': 'catch_all'},
            {'email': 'e@test.com', 'smtp_status': 'unknown'},
        ]

        with caplog.at_level(logging.INFO):
            print_summary(results)

        assert 'Valid: 2' in caplog.text
        assert 'Invalid: 1' in caplog.text
