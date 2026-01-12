"""Tests for email verifier module."""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.email_verifier.verifier import (
    verify_email,
    verify_emails_bulk,
    VerificationResult,
    VerificationStatus,
    is_generic_email,
    extract_domain
)


class TestVerificationStatus:
    """Test VerificationStatus enum."""

    def test_status_values(self):
        """Verify all expected status values exist."""
        assert VerificationStatus.OK.value == 'ok'
        assert VerificationStatus.CATCH_ALL.value == 'catch_all'
        assert VerificationStatus.INVALID.value == 'invalid'
        assert VerificationStatus.UNKNOWN.value == 'unknown'
        assert VerificationStatus.ERROR.value == 'error'


class TestVerificationResult:
    """Test VerificationResult dataclass."""

    def test_result_creation(self):
        """Test creating a verification result."""
        result = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )
        assert result.email == 'test@example.com'
        assert result.status == VerificationStatus.OK
        assert result.is_catch_all is False
        assert result.is_deliverable is True
        assert result.confidence == 95

    def test_result_safe_to_send(self):
        """Test safe_to_send property."""
        # OK status is safe
        ok_result = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )
        assert ok_result.safe_to_send is True

        # Catch-all is risky
        catch_all_result = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.CATCH_ALL,
            is_catch_all=True,
            is_deliverable=True,
            confidence=50
        )
        assert catch_all_result.safe_to_send is False

        # Invalid is not safe
        invalid_result = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.INVALID,
            is_catch_all=False,
            is_deliverable=False,
            confidence=0
        )
        assert invalid_result.safe_to_send is False


class TestHelperFunctions:
    """Test helper functions."""

    def test_extract_domain(self):
        """Test domain extraction."""
        assert extract_domain('test@example.com') == 'example.com'
        assert extract_domain('user@sub.domain.com') == 'sub.domain.com'
        assert extract_domain('invalid') is None
        assert extract_domain('') is None
        assert extract_domain(None) is None

    def test_is_generic_email(self):
        """Test generic email detection."""
        # Generic prefixes
        assert is_generic_email('info@example.com') is True
        assert is_generic_email('sales@example.com') is True
        assert is_generic_email('contact@example.com') is True
        assert is_generic_email('support@example.com') is True
        assert is_generic_email('admin@example.com') is True
        assert is_generic_email('hello@example.com') is True
        assert is_generic_email('team@example.com') is True
        assert is_generic_email('office@example.com') is True

        # Named contacts (not generic)
        assert is_generic_email('john@example.com') is False
        assert is_generic_email('john.smith@example.com') is False
        assert is_generic_email('jsmith@example.com') is False

        # Edge cases
        assert is_generic_email('') is False
        assert is_generic_email(None) is False


class TestVerifyEmail:
    """Test single email verification."""

    @patch('scripts.email_verifier.verifier.requests.get')
    def test_verify_valid_email(self, mock_get):
        """Test verifying a valid email."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'result': 'ok',
            'resultcode': 1,
            'free': False,
            'role': False
        }
        mock_get.return_value = mock_response

        result = verify_email('valid@example.com', api_key='test_key')

        assert result.status == VerificationStatus.OK
        assert result.is_deliverable is True
        assert result.confidence >= 90

    @patch('scripts.email_verifier.verifier.requests.get')
    def test_verify_catch_all_email(self, mock_get):
        """Test verifying a catch-all email."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'result': 'catch_all',
            'resultcode': 4,
            'free': False,
            'role': True
        }
        mock_get.return_value = mock_response

        result = verify_email('info@catchall.com', api_key='test_key')

        assert result.status == VerificationStatus.CATCH_ALL
        assert result.is_catch_all is True
        assert result.confidence < 70

    @patch('scripts.email_verifier.verifier.requests.get')
    def test_verify_invalid_email(self, mock_get):
        """Test verifying an invalid email."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'result': 'invalid',
            'resultcode': 2,
            'free': False,
            'role': False
        }
        mock_get.return_value = mock_response

        result = verify_email('invalid@example.com', api_key='test_key')

        assert result.status == VerificationStatus.INVALID
        assert result.is_deliverable is False
        assert result.confidence == 0

    @patch('scripts.email_verifier.verifier.requests.get')
    def test_verify_api_error(self, mock_get):
        """Test handling API errors."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = verify_email('test@example.com', api_key='test_key')

        assert result.status == VerificationStatus.ERROR
        assert 'API error' in result.error

    def test_verify_without_api_key(self):
        """Test verification without API key."""
        result = verify_email('test@example.com', api_key=None)

        assert result.status == VerificationStatus.UNKNOWN
        assert 'No API key' in result.error


class TestVerifyEmailsBulk:
    """Test bulk email verification."""

    @patch('scripts.email_verifier.verifier.verify_email')
    def test_bulk_verify(self, mock_verify):
        """Test bulk verification from list."""
        mock_verify.side_effect = [
            VerificationResult(
                email='valid@example.com',
                status=VerificationStatus.OK,
                is_catch_all=False,
                is_deliverable=True,
                confidence=95
            ),
            VerificationResult(
                email='invalid@example.com',
                status=VerificationStatus.INVALID,
                is_catch_all=False,
                is_deliverable=False,
                confidence=0
            )
        ]

        emails = ['valid@example.com', 'invalid@example.com']
        results = verify_emails_bulk(emails, api_key='test_key')

        assert len(results) == 2
        assert results[0].status == VerificationStatus.OK
        assert results[1].status == VerificationStatus.INVALID

    @patch('scripts.email_verifier.verifier.verify_email')
    def test_bulk_verify_with_delay(self, mock_verify):
        """Test bulk verification respects delay."""
        mock_verify.return_value = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        emails = ['test1@example.com', 'test2@example.com']
        results = verify_emails_bulk(emails, api_key='test_key', delay=0.01)

        assert len(results) == 2
        assert mock_verify.call_count == 2


class TestIntegration:
    """Integration tests (require API key)."""

    @pytest.mark.skipif(
        not os.getenv('MILLIONVERIFIER_API_KEY'),
        reason='MILLIONVERIFIER_API_KEY not set'
    )
    def test_real_api_verification(self):
        """Test with real API (only runs if API key set)."""
        api_key = os.getenv('MILLIONVERIFIER_API_KEY')

        # Test with a known disposable email domain
        result = verify_email('test@mailinator.com', api_key=api_key)

        # Should detect as risky/invalid (disposable domain)
        assert result.status in [
            VerificationStatus.INVALID,
            VerificationStatus.CATCH_ALL,
            VerificationStatus.UNKNOWN
        ]
