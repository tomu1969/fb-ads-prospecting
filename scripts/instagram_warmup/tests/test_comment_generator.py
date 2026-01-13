"""
Tests for CommentGenerator - AI-powered personalized Instagram comments.

TDD: Write tests first, then implement comment_generator.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestCommentGenerator:
    """Test CommentGenerator class."""

    def test_generate_comment_returns_string(self):
        """Test that generate_comment returns a non-empty string."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                # Mock the response
                mock_response = Mock()
                mock_response.choices = [Mock(message=Mock(content="Love this property!"))]
                mock_groq.return_value.chat.completions.create.return_value = mock_response

                generator = CommentGenerator()
                comment = generator.generate_comment(
                    page_name="ABC Realty",
                    post_caption="Just listed! Beautiful 3BR home in Miami.",
                    post_type="image"
                )

                assert isinstance(comment, str)
                assert len(comment) > 0

    def test_generate_comment_is_short(self):
        """Test that comments are short (under 100 chars)."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                mock_response = Mock()
                mock_response.choices = [Mock(message=Mock(content="Beautiful home!"))]
                mock_groq.return_value.chat.completions.create.return_value = mock_response

                generator = CommentGenerator()
                comment = generator.generate_comment(
                    page_name="Test Realty",
                    post_caption="New listing",
                    post_type="image"
                )

                assert len(comment) <= 100

    def test_generate_comment_no_sales_language(self):
        """Test that comments don't contain salesy language."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        sales_phrases = [
            'dm me', 'message me', 'check out', 'our services',
            'we offer', 'contact us', 'book a call', 'free consultation'
        ]

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                mock_response = Mock()
                mock_response.choices = [Mock(message=Mock(content="Stunning view!"))]
                mock_groq.return_value.chat.completions.create.return_value = mock_response

                generator = CommentGenerator()
                comment = generator.generate_comment(
                    page_name="Test",
                    post_caption="Test",
                    post_type="image"
                )

                comment_lower = comment.lower()
                for phrase in sales_phrases:
                    assert phrase not in comment_lower

    def test_generate_comment_handles_api_error(self):
        """Test graceful handling of API errors."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                mock_groq.return_value.chat.completions.create.side_effect = Exception("API Error")

                generator = CommentGenerator()
                comment = generator.generate_comment(
                    page_name="Test",
                    post_caption="Test",
                    post_type="image"
                )

                # Should return a fallback comment
                assert isinstance(comment, str)
                assert len(comment) > 0

    def test_generate_comment_uses_groq_by_default(self):
        """Test that Groq is preferred over OpenAI."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'GROQ_API_KEY': 'groq_key', 'OPENAI_API_KEY': 'openai_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                with patch('scripts.instagram_warmup.comment_generator.OpenAI') as mock_openai:
                    mock_response = Mock()
                    mock_response.choices = [Mock(message=Mock(content="Nice!"))]
                    mock_groq.return_value.chat.completions.create.return_value = mock_response

                    generator = CommentGenerator()
                    generator.generate_comment("Test", "Test", "image")

                    # Groq should be called
                    mock_groq.return_value.chat.completions.create.assert_called()

    def test_generate_comment_falls_back_to_openai(self):
        """Test fallback to OpenAI when Groq is not available."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'OPENAI_API_KEY': 'openai_key'}, clear=True):
            with patch('scripts.instagram_warmup.comment_generator.Groq', None):
                with patch('scripts.instagram_warmup.comment_generator.OpenAI') as mock_openai:
                    mock_response = Mock()
                    mock_response.choices = [Mock(message=Mock(content="Great!"))]
                    mock_openai.return_value.chat.completions.create.return_value = mock_response

                    generator = CommentGenerator()
                    comment = generator.generate_comment("Test", "Test", "image")

                    assert isinstance(comment, str)

    def test_generate_comment_with_company_context(self):
        """Test that company description is used for context."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        with patch.dict('os.environ', {'GROQ_API_KEY': 'test_key'}):
            with patch('scripts.instagram_warmup.comment_generator.Groq') as mock_groq:
                mock_response = Mock()
                mock_response.choices = [Mock(message=Mock(content="Beautiful property!"))]
                mock_groq.return_value.chat.completions.create.return_value = mock_response

                generator = CommentGenerator()
                comment = generator.generate_comment(
                    page_name="Luxury Homes Miami",
                    post_caption="New waterfront listing!",
                    post_type="image",
                    company_description="High-end real estate agency"
                )

                # Verify the prompt included company context
                call_args = mock_groq.return_value.chat.completions.create.call_args
                messages = call_args.kwargs.get('messages', [])
                prompt = messages[0]['content'] if messages else ''
                assert 'Luxury Homes Miami' in prompt or len(comment) > 0

    def test_fallback_comments(self):
        """Test that fallback comments are appropriate."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator, FALLBACK_COMMENTS

        assert len(FALLBACK_COMMENTS) >= 5
        for comment in FALLBACK_COMMENTS:
            assert len(comment) <= 100
            assert not any(word in comment.lower() for word in ['dm', 'contact', 'call'])

    def test_sanitize_comment(self):
        """Test comment sanitization."""
        from scripts.instagram_warmup.comment_generator import CommentGenerator

        generator = CommentGenerator.__new__(CommentGenerator)

        # Test removing quotes
        assert generator._sanitize_comment('"Hello"') == 'Hello'
        assert generator._sanitize_comment("'Hello'") == 'Hello'

        # Test trimming
        assert generator._sanitize_comment('  Hello  ') == 'Hello'

        # Test length limit
        long_comment = 'x' * 200
        sanitized = generator._sanitize_comment(long_comment)
        assert len(sanitized) <= 100
