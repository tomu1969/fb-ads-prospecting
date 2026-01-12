"""Tests for Gmail Sender module.

TDD approach - tests written before implementation.
"""

import pytest
import pandas as pd
import tempfile
import os
import logging
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


class TestSetupLogging:
    """Tests for logging configuration."""

    def test_setup_logging_returns_logger(self):
        """setup_logging should return a logger instance."""
        from scripts.gmail_sender.gmail_sender import setup_logging

        logger = setup_logging()
        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_setup_logging_default_level_info(self):
        """Default logging level should be INFO."""
        from scripts.gmail_sender.gmail_sender import setup_logging

        logger = setup_logging(verbose=False)
        assert logger.level == logging.INFO or logger.getEffectiveLevel() == logging.INFO

    def test_setup_logging_verbose_level_debug(self):
        """Verbose mode should set logging level to DEBUG."""
        from scripts.gmail_sender.gmail_sender import setup_logging

        logger = setup_logging(verbose=True)
        assert logger.level == logging.DEBUG or logger.getEffectiveLevel() == logging.DEBUG


class TestSendEmail:
    """Tests for email sending functionality."""

    @patch('scripts.gmail_sender.gmail_sender.smtplib.SMTP')
    def test_send_email_success(self, mock_smtp):
        """send_email should return True on successful send."""
        from scripts.gmail_sender.gmail_sender import send_email

        # Configure mock
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = Mock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = Mock(return_value=False)

        success, error = send_email(
            to="test@example.com",
            subject="Test Subject",
            body="Test body content",
            login_address="sender@example.com",
            password="test_password"
        )

        assert success is True
        assert error is None

    @patch('scripts.gmail_sender.gmail_sender.smtplib.SMTP')
    def test_send_email_failure_returns_error(self, mock_smtp):
        """send_email should return False and error message on failure."""
        from scripts.gmail_sender.gmail_sender import send_email

        # Configure mock to raise exception
        mock_smtp.side_effect = Exception("Connection failed")

        success, error = send_email(
            to="test@example.com",
            subject="Test Subject",
            body="Test body content",
            login_address="sender@example.com",
            password="test_password"
        )

        assert success is False
        assert error is not None
        assert "Connection failed" in error

    @patch('scripts.gmail_sender.gmail_sender.smtplib.SMTP')
    def test_send_email_uses_tls(self, mock_smtp):
        """send_email should use TLS for secure connection."""
        from scripts.gmail_sender.gmail_sender import send_email

        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = Mock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = Mock(return_value=False)

        send_email(
            to="test@example.com",
            subject="Test Subject",
            body="Test body content",
            login_address="sender@example.com",
            password="test_password"
        )

        mock_server.starttls.assert_called_once()


class TestProcessCsv:
    """Tests for CSV processing functionality."""

    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,subject_line,email_body,page_name,contact_name\n")
            f.write("test1@example.com,Subject 1,Body 1,Company A,John\n")
            f.write("test2@example.com,Subject 2,Body 2,Company B,Jane\n")
            f.write("test3@example.com,Subject 3,Body 3,Company C,Bob\n")
            return f.name

    @pytest.fixture
    def csv_with_sent(self):
        """Create a CSV with some already-sent emails."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,subject_line,email_body,send_status,sent_at\n")
            f.write("test1@example.com,Subject 1,Body 1,sent,2026-01-11T10:00:00\n")
            f.write("test2@example.com,Subject 2,Body 2,,\n")
            f.write("test3@example.com,Subject 3,Body 3,,\n")
            return f.name

    def test_process_csv_dry_run_does_not_send(self, sample_csv):
        """Dry run should not actually send emails."""
        from scripts.gmail_sender.gmail_sender import process_csv

        with patch('scripts.gmail_sender.gmail_sender.send_email') as mock_send:
            results = process_csv(
                csv_path=sample_csv,
                dry_run=True,
                login_address="sender@example.com",
                password="test_password"
            )

            # send_email should not be called in dry run
            mock_send.assert_not_called()

        os.unlink(sample_csv)

    def test_process_csv_dry_run_marks_status(self, sample_csv):
        """Dry run should mark emails with 'dry_run' status."""
        from scripts.gmail_sender.gmail_sender import process_csv

        with patch('scripts.gmail_sender.gmail_sender.send_email'):
            results = process_csv(
                csv_path=sample_csv,
                dry_run=True,
                login_address="sender@example.com",
                password="test_password"
            )

        # Check the CSV was updated
        df = pd.read_csv(sample_csv)
        assert all(df['send_status'] == 'dry_run')

        os.unlink(sample_csv)

    @patch('scripts.gmail_sender.gmail_sender.send_email')
    def test_process_csv_updates_status_on_send(self, mock_send, sample_csv):
        """Sending should update status to 'sent'."""
        from scripts.gmail_sender.gmail_sender import process_csv

        mock_send.return_value = (True, None)

        results = process_csv(
            csv_path=sample_csv,
            dry_run=False,
            login_address="sender@example.com",
            password="test_password",
            delay=0  # No delay for tests
        )

        df = pd.read_csv(sample_csv)
        assert all(df['send_status'] == 'sent')
        assert all(df['sent_at'].notna())

        os.unlink(sample_csv)

    @patch('scripts.gmail_sender.gmail_sender.send_email')
    def test_process_csv_updates_status_on_failure(self, mock_send, sample_csv):
        """Failed sends should update status to 'failed' with error."""
        from scripts.gmail_sender.gmail_sender import process_csv

        mock_send.return_value = (False, "SMTP error")

        results = process_csv(
            csv_path=sample_csv,
            dry_run=False,
            login_address="sender@example.com",
            password="test_password",
            delay=0
        )

        df = pd.read_csv(sample_csv)
        assert all(df['send_status'] == 'failed')
        assert all(df['send_error'] == 'SMTP error')

        os.unlink(sample_csv)

    def test_skip_already_sent(self, csv_with_sent):
        """Should skip emails already marked as 'sent'."""
        from scripts.gmail_sender.gmail_sender import process_csv

        with patch('scripts.gmail_sender.gmail_sender.send_email') as mock_send:
            mock_send.return_value = (True, None)

            results = process_csv(
                csv_path=csv_with_sent,
                dry_run=False,
                login_address="sender@example.com",
                password="test_password",
                skip_sent=True,
                delay=0
            )

            # Only 2 emails should be processed (test2 and test3)
            assert mock_send.call_count == 2
            assert results['skipped'] == 1

        os.unlink(csv_with_sent)

    @patch('scripts.gmail_sender.gmail_sender.send_email')
    def test_limit_parameter(self, mock_send, sample_csv):
        """--limit should cap the number of emails processed."""
        from scripts.gmail_sender.gmail_sender import process_csv

        mock_send.return_value = (True, None)

        results = process_csv(
            csv_path=sample_csv,
            dry_run=False,
            login_address="sender@example.com",
            password="test_password",
            limit=2,
            delay=0
        )

        # Only 2 emails should be sent
        assert mock_send.call_count == 2
        assert results['sent'] == 2

        os.unlink(sample_csv)


class TestResultsSummary:
    """Tests for results summary functionality."""

    def test_results_summary_structure(self):
        """Results dict should have expected keys."""
        from scripts.gmail_sender.gmail_sender import process_csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("primary_email,subject_line,email_body\n")
            f.write("test@example.com,Subject,Body\n")
            csv_path = f.name

        with patch('scripts.gmail_sender.gmail_sender.send_email') as mock_send:
            mock_send.return_value = (True, None)

            results = process_csv(
                csv_path=csv_path,
                dry_run=True,
                login_address="sender@example.com",
                password="test_password"
            )

        assert 'total' in results
        assert 'sent' in results
        assert 'failed' in results
        assert 'skipped' in results
        assert 'duration' in results

        os.unlink(csv_path)

    def test_print_results_summary_logs_output(self, caplog):
        """print_results_summary should log the summary."""
        from scripts.gmail_sender.gmail_sender import print_results_summary

        results = {
            'total': 10,
            'sent': 8,
            'failed': 1,
            'skipped': 1,
            'duration': 15.5
        }

        with caplog.at_level(logging.INFO):
            print_results_summary(results)

        assert "RESULTS SUMMARY" in caplog.text
        assert "Total processed: 10" in caplog.text
        assert "Sent: 8" in caplog.text


class TestCLIArguments:
    """Tests for CLI argument parsing."""

    def test_parse_args_defaults(self):
        """Default arguments should be set correctly."""
        from scripts.gmail_sender.gmail_sender import parse_args

        args = parse_args(['--csv', 'test.csv'])

        assert args.csv == 'test.csv'
        assert args.dry_run is False
        assert args.limit is None
        assert args.delay == 1.0
        assert args.skip_sent is True

    def test_parse_args_dry_run(self):
        """--dry-run flag should be parsed."""
        from scripts.gmail_sender.gmail_sender import parse_args

        args = parse_args(['--csv', 'test.csv', '--dry-run'])
        assert args.dry_run is True

    def test_parse_args_limit(self):
        """--limit should be parsed as integer."""
        from scripts.gmail_sender.gmail_sender import parse_args

        args = parse_args(['--csv', 'test.csv', '--limit', '5'])
        assert args.limit == 5

    def test_parse_args_delay(self):
        """--delay should be parsed as float."""
        from scripts.gmail_sender.gmail_sender import parse_args

        args = parse_args(['--csv', 'test.csv', '--delay', '2.5'])
        assert args.delay == 2.5


class TestEmailValidation:
    """Tests for email validation."""

    def test_validates_email_format(self):
        """Should validate email format before sending."""
        from scripts.gmail_sender.gmail_sender import is_valid_email

        assert is_valid_email("test@example.com") is True
        assert is_valid_email("invalid-email") is False
        assert is_valid_email("") is False
        assert is_valid_email(None) is False
