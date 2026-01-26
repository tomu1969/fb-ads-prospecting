"""Tests for Groq API client."""

from unittest.mock import MagicMock, patch

import pytest


class TestGroqClient:
    """Tests for GroqClient."""

    def test_calculate_cost(self):
        """Should calculate cost correctly."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        # 1M input tokens at $0.59, 1M output at $0.79
        cost = client._calculate_cost(1_000_000, 1_000_000)
        assert abs(cost - 1.38) < 0.01

        # 500 input, 100 output
        cost = client._calculate_cost(500, 100)
        expected = (500 / 1_000_000) * 0.59 + (100 / 1_000_000) * 0.79
        assert abs(cost - expected) < 0.0001

    def test_extract_contact_info_success(self):
        """Should extract contact info from mock API response."""
        from scripts.contact_intel.groq_client import GroqClient

        # Mock Groq response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"company": "Acme Corp", "role": "Engineer", "topics": ["tech"], "confidence": 0.9}'
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 50

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('groq.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.return_value = mock_response

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

            assert result.company == 'Acme Corp'
            assert result.role == 'Engineer'
            assert result.topics == ['tech']
            assert result.confidence == 0.9
            assert result.input_tokens == 500
            assert result.output_tokens == 50
            assert result.cost_usd > 0

    def test_extract_contact_info_handles_api_error(self):
        """Should return empty result on API error."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('groq.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.side_effect = Exception("API Error")

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

            assert result.company is None
            assert result.role is None
            assert result.topics == []
            assert result.confidence == 0.0

    def test_extract_contact_info_handles_invalid_json(self):
        """Should handle invalid JSON response."""
        from scripts.contact_intel.groq_client import GroqClient

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = 'not valid json'
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 50

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        with patch('groq.Groq') as mock_groq:
            mock_groq.return_value.chat.completions.create.return_value = mock_response

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

            # Should return empty but not crash
            assert result.company is None
            assert result.input_tokens == 500

    def test_rate_limiting(self):
        """Should enforce rate limiting between requests."""
        from scripts.contact_intel.groq_client import GroqClient, REQUEST_DELAY
        import time

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient()

        # Set last request time to now
        client.last_request_time = time.time()

        start = time.time()
        client._rate_limit()
        elapsed = time.time() - start

        # Should have waited approximately REQUEST_DELAY seconds
        assert elapsed >= REQUEST_DELAY * 0.9  # Allow 10% tolerance

    def test_missing_api_key_raises(self):
        """Should raise error if API key not set."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {}, clear=True):
            # Remove GROQ_API_KEY if exists
            import os
            if 'GROQ_API_KEY' in os.environ:
                del os.environ['GROQ_API_KEY']

            with pytest.raises(ValueError, match="GROQ_API_KEY"):
                GroqClient()
