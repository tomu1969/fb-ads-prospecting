"""Tests for email body fetcher."""

import base64
from unittest.mock import MagicMock, patch

import pytest


class TestBodyFetcher:
    """Tests for body fetching module."""

    def test_extract_body_from_plain_text(self):
        """Should extract body from plain text payload."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload

        body_text = "Hello, this is a test email."
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        payload = {
            'body': {'data': encoded}
        }

        result = _extract_body_from_payload(payload)
        assert result == body_text

    def test_extract_body_from_multipart(self):
        """Should extract text/plain from multipart message."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload

        body_text = "Plain text body"
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        payload = {
            'parts': [
                {'mimeType': 'text/html', 'body': {'data': base64.urlsafe_b64encode(b'<html>HTML</html>').decode()}},
                {'mimeType': 'text/plain', 'body': {'data': encoded}},
            ]
        }

        result = _extract_body_from_payload(payload)
        assert result == body_text

    def test_extract_body_falls_back_to_html(self):
        """Should strip HTML if no plain text available."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload

        html = "<html><body><p>Hello</p> <b>World</b></body></html>"
        encoded = base64.urlsafe_b64encode(html.encode()).decode()

        payload = {
            'parts': [
                {'mimeType': 'text/html', 'body': {'data': encoded}},
            ]
        }

        result = _extract_body_from_payload(payload)
        assert 'Hello' in result
        assert 'World' in result
        assert '<html>' not in result

    def test_extract_body_from_nested_multipart(self):
        """Should handle nested multipart structure."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload

        body_text = "Nested plain text"
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        payload = {
            'parts': [
                {
                    'mimeType': 'multipart/alternative',
                    'parts': [
                        {'mimeType': 'text/plain', 'body': {'data': encoded}},
                        {'mimeType': 'text/html', 'body': {'data': base64.urlsafe_b64encode(b'<html>HTML</html>').decode()}},
                    ]
                }
            ]
        }

        result = _extract_body_from_payload(payload)
        assert result == body_text

    def test_extract_body_empty_payload(self):
        """Should return empty string for empty payload."""
        from scripts.contact_intel.body_fetcher import _extract_body_from_payload

        payload = {}
        result = _extract_body_from_payload(payload)
        assert result == ""

    def test_fetch_body_uses_cache(self):
        """Should return cached body without API call."""
        from scripts.contact_intel.body_fetcher import fetch_body

        with patch('scripts.contact_intel.body_fetcher.get_cached_body') as mock_cache:
            mock_cache.return_value = "Cached body content"

            result = fetch_body('msg123', 'test_account')

            assert result == "Cached body content"
            mock_cache.assert_called_once_with('msg123')

    def test_fetch_body_calls_api_on_cache_miss(self):
        """Should fetch from Gmail API if not cached."""
        from scripts.contact_intel.body_fetcher import fetch_body

        body_text = "Fresh from API"
        encoded = base64.urlsafe_b64encode(body_text.encode()).decode()

        mock_service = MagicMock()
        mock_service.users().messages().get().execute.return_value = {
            'payload': {'body': {'data': encoded}}
        }

        with patch('scripts.contact_intel.body_fetcher.get_cached_body', return_value=None):
            with patch('scripts.contact_intel.body_fetcher._get_gmail_service', return_value=mock_service):
                with patch('scripts.contact_intel.body_fetcher.save_email_body') as mock_save:
                    result = fetch_body('msg123', 'test_account')

        assert result == body_text
        mock_save.assert_called_once_with('msg123', body_text)

    def test_fetch_body_returns_none_on_error(self):
        """Should return None on API error."""
        from scripts.contact_intel.body_fetcher import fetch_body

        mock_service = MagicMock()
        mock_service.users().messages().get().execute.side_effect = Exception("API Error")

        with patch('scripts.contact_intel.body_fetcher.get_cached_body', return_value=None):
            with patch('scripts.contact_intel.body_fetcher._get_gmail_service', return_value=mock_service):
                result = fetch_body('msg123', 'test_account')

        assert result is None

    def test_get_contact_emails_with_body(self, mock_emails_db):
        """Should get emails for contact with bodies."""
        from scripts.contact_intel.body_fetcher import get_contact_emails_with_body

        with patch('scripts.contact_intel.body_fetcher.EMAILS_DB', mock_emails_db):
            with patch('scripts.contact_intel.body_fetcher.fetch_body', return_value="Test body"):
                emails = get_contact_emails_with_body('john@realty.com', limit=2)

        assert len(emails) <= 2
        assert all('body' in e for e in emails)
        assert all('subject' in e for e in emails)

    def test_get_contact_emails_with_body_includes_to_emails(self, mock_emails_db):
        """Should find emails where contact is in to_emails."""
        from scripts.contact_intel.body_fetcher import get_contact_emails_with_body

        with patch('scripts.contact_intel.body_fetcher.EMAILS_DB', mock_emails_db):
            with patch('scripts.contact_intel.body_fetcher.fetch_body', return_value="Test body"):
                # john@realty.com is in to_emails of msg2 (reply from Tomas)
                emails = get_contact_emails_with_body('john@realty.com', limit=5)

        # Should find emails where john is sender OR recipient
        assert len(emails) >= 1

    def test_get_contact_emails_with_body_empty_for_unknown(self, mock_emails_db):
        """Should return empty list for unknown contact."""
        from scripts.contact_intel.body_fetcher import get_contact_emails_with_body

        with patch('scripts.contact_intel.body_fetcher.EMAILS_DB', mock_emails_db):
            with patch('scripts.contact_intel.body_fetcher.fetch_body', return_value="Test body"):
                emails = get_contact_emails_with_body('unknown@example.com', limit=5)

        assert emails == []
