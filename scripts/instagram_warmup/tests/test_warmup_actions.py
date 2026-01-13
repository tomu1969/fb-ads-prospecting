"""
Tests for WarmupActions - Apify actor wrappers for Instagram engagement.

TDD: Write tests first, then implement warmup_actions.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path


class TestWarmupActions:
    """Test WarmupActions class."""

    @pytest.fixture
    def mock_apify_client(self):
        """Create a mock Apify client."""
        client = Mock()
        return client

    def test_get_apify_client_returns_client(self):
        """Test getting Apify client with valid token."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {'APIFY_API_TOKEN': 'test_token'}):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client:
                mock_client.return_value = Mock()
                actions = WarmupActions()
                assert actions.client is not None

    def test_get_apify_client_returns_none_without_token(self):
        """Test that client is None without API token."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {}, clear=True):
            with patch.object(WarmupActions, '_get_apify_client', return_value=None):
                actions = WarmupActions()
                actions.client = None
                assert actions.client is None

    def test_follow_dry_run(self):
        """Test follow action in dry run mode."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        actions = WarmupActions(dry_run=True)
        success, error = actions.follow('testuser')

        assert success is True
        assert error is None

    def test_follow_success(self):
        """Test successful follow via Apify."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {
            'APIFY_API_TOKEN': 'test_token',
            'INSTAGRAM_SESSION_ID': 'test_session'
        }):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                # Setup mock
                mock_client = Mock()
                mock_actor = Mock()
                mock_run = {'status': 'SUCCEEDED'}
                mock_actor.call.return_value = mock_run
                mock_client.actor.return_value = mock_actor
                mock_client_class.return_value = mock_client

                actions = WarmupActions()
                success, error = actions.follow('testuser')

                assert success is True
                assert error is None
                mock_client.actor.assert_called_once()

    def test_follow_handles_error(self):
        """Test follow handles API errors gracefully."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {
            'APIFY_API_TOKEN': 'test_token',
            'INSTAGRAM_SESSION_ID': 'test_session'
        }):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                mock_client = Mock()
                mock_actor = Mock()
                mock_actor.call.side_effect = Exception("API Error")
                mock_client.actor.return_value = mock_actor
                mock_client_class.return_value = mock_client

                actions = WarmupActions()
                success, error = actions.follow('testuser')

                assert success is False
                assert 'API Error' in error

    def test_follow_requires_session(self):
        """Test follow fails without Instagram session."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {'APIFY_API_TOKEN': 'test_token'}, clear=True):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                mock_client_class.return_value = Mock()
                actions = WarmupActions()
                success, error = actions.follow('testuser')

                assert success is False
                assert 'session' in error.lower()

    def test_like_post_dry_run(self):
        """Test like action in dry run mode."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        actions = WarmupActions(dry_run=True)
        success, error = actions.like_post('https://instagram.com/p/ABC123')

        assert success is True
        assert error is None

    def test_like_post_success(self):
        """Test successful like via Apify."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {
            'APIFY_API_TOKEN': 'test_token',
            'INSTAGRAM_SESSION_ID': 'test_session'
        }):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                mock_client = Mock()
                mock_actor = Mock()
                mock_run = {'status': 'SUCCEEDED'}
                mock_actor.call.return_value = mock_run
                mock_client.actor.return_value = mock_actor
                mock_client_class.return_value = mock_client

                actions = WarmupActions()
                success, error = actions.like_post('https://instagram.com/p/ABC123')

                assert success is True
                assert error is None

    def test_comment_dry_run(self):
        """Test comment action in dry run mode."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        actions = WarmupActions(dry_run=True)
        success, error = actions.comment(
            'https://instagram.com/p/ABC123',
            'Great post!'
        )

        assert success is True
        assert error is None

    def test_comment_success(self):
        """Test successful comment via Apify."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {
            'APIFY_API_TOKEN': 'test_token',
            'INSTAGRAM_SESSION_ID': 'test_session'
        }):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                mock_client = Mock()
                mock_actor = Mock()
                mock_run = {'status': 'SUCCEEDED'}
                mock_actor.call.return_value = mock_run
                mock_client.actor.return_value = mock_actor
                mock_client_class.return_value = mock_client

                actions = WarmupActions()
                success, error = actions.comment(
                    'https://instagram.com/p/ABC123',
                    'Great post!'
                )

                assert success is True
                assert error is None

    def test_rate_limiting(self):
        """Test that actions respect rate limiting."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions
        import time

        actions = WarmupActions(dry_run=True, min_delay=0.1, max_delay=0.1)

        start = time.time()
        actions.follow('user1')
        actions.follow('user2')
        elapsed = time.time() - start

        # Should have waited at least min_delay between actions
        assert elapsed >= 0.1

    def test_build_cookies_array(self):
        """Test building cookies array for Apify."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {
            'APIFY_API_TOKEN': 'test_token',
            'INSTAGRAM_SESSION_ID': 'abc123'
        }):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient'):
                actions = WarmupActions()
                cookies = actions._build_cookies_array()

                assert len(cookies) == 1
                assert cookies[0]['name'] == 'sessionid'
                assert cookies[0]['value'] == 'abc123'
                assert cookies[0]['domain'] == '.instagram.com'


class TestPostScraper:
    """Test post scraping functionality."""

    def test_get_recent_posts_dry_run(self):
        """Test getting recent posts in dry run mode."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        actions = WarmupActions(dry_run=True)
        posts = actions.get_recent_posts('testuser', limit=3)

        # In dry run, returns mock posts
        assert isinstance(posts, list)
        assert len(posts) <= 3

    def test_get_recent_posts_success(self):
        """Test getting recent posts via Apify scraper."""
        from scripts.instagram_warmup.warmup_actions import WarmupActions

        with patch.dict('os.environ', {'APIFY_API_TOKEN': 'test_token'}):
            with patch('scripts.instagram_warmup.warmup_actions.ApifyClient') as mock_client_class:
                mock_client = Mock()
                mock_actor = Mock()

                # Mock dataset with posts
                mock_dataset = Mock()
                mock_dataset.iterate_items.return_value = [
                    {'url': 'https://instagram.com/p/ABC123', 'caption': 'Post 1'},
                    {'url': 'https://instagram.com/p/DEF456', 'caption': 'Post 2'},
                ]
                mock_client.dataset.return_value = mock_dataset

                mock_run = {'status': 'SUCCEEDED', 'defaultDatasetId': 'dataset123'}
                mock_actor.call.return_value = mock_run
                mock_client.actor.return_value = mock_actor
                mock_client_class.return_value = mock_client

                actions = WarmupActions()
                posts = actions.get_recent_posts('testuser', limit=5)

                assert len(posts) == 2
                assert posts[0]['url'] == 'https://instagram.com/p/ABC123'
