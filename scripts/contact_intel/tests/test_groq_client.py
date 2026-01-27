"""Tests for LLM client (OpenAI and Groq)."""

import time
from unittest.mock import MagicMock, patch

import pytest


class TestGroqClient:
    """Tests for GroqClient (multi-provider LLM client)."""

    def test_calculate_cost_openai(self):
        """Should calculate OpenAI cost correctly."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key', 'LLM_PROVIDER': 'openai'}):
            client = GroqClient(provider='openai')

        # OpenAI GPT-4o-mini: $0.15 input, $0.60 output per 1M tokens
        cost = client._calculate_cost(1_000_000, 1_000_000)
        assert abs(cost - 0.75) < 0.01  # $0.15 + $0.60 = $0.75

    def test_calculate_cost_groq(self):
        """Should calculate Groq cost correctly."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            client = GroqClient(provider='groq')

        # Groq: $0.59 input, $0.79 output per 1M tokens
        cost = client._calculate_cost(1_000_000, 1_000_000)
        assert abs(cost - 1.38) < 0.01  # $0.59 + $0.79 = $1.38

    def test_extract_contact_info_success_openai(self):
        """Should extract contact info from OpenAI API response."""
        from scripts.contact_intel.groq_client import GroqClient

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"company": "Acme Corp", "role": "Engineer", "topics": ["tech"], "confidence": 0.9}'
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 50

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            client = GroqClient(provider='openai')

        with patch('openai.OpenAI') as mock_openai:
            mock_openai.return_value.chat.completions.create.return_value = mock_response

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

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            client = GroqClient(provider='openai')

        with patch('openai.OpenAI') as mock_openai:
            mock_openai.return_value.chat.completions.create.side_effect = Exception("API Error")

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

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            client = GroqClient(provider='openai')

        with patch('openai.OpenAI') as mock_openai:
            mock_openai.return_value.chat.completions.create.return_value = mock_response

            result = client.extract_contact_info(
                email='test@example.com',
                name='Test User',
                emails=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}],
            )

        # Should return empty but not crash
        assert result.company is None
        assert result.input_tokens == 500

    def test_rate_limiting_openai(self):
        """Should enforce rate limiting for OpenAI (faster)."""
        from scripts.contact_intel.groq_client import GroqClient, OPENAI_REQUEST_DELAY

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            client = GroqClient(provider='openai')

        # Set last request time to now
        client.last_request_time = time.time()

        start = time.time()
        client._rate_limit()
        elapsed = time.time() - start

        # Should have waited approximately OPENAI_REQUEST_DELAY seconds
        assert elapsed >= OPENAI_REQUEST_DELAY * 0.9  # Allow 10% tolerance

    def test_missing_openai_api_key_raises(self):
        """Should raise error if OpenAI API key not set."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                GroqClient(provider='openai')

    def test_missing_groq_api_key_raises(self):
        """Should raise error if Groq API key not set."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="GROQ_API_KEY"):
                GroqClient(provider='groq')

    def test_default_provider_is_openai(self):
        """Should default to OpenAI provider."""
        from scripts.contact_intel.groq_client import GroqClient

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key', 'LLM_PROVIDER': 'openai'}):
            client = GroqClient()

        assert client.provider == 'openai'
        assert client.model == 'gpt-4o-mini'
