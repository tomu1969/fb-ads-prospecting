"""
WarmupActions - Apify actor wrappers for Instagram engagement.

Provides follow, like, and comment actions via Apify actors.
"""

import os
import time
import random
import logging
from typing import List, Dict, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Try to import ApifyClient
try:
    from apify_client import ApifyClient
except ImportError:
    ApifyClient = None
    logger.warning("apify-client not installed. Run: pip install apify-client")

# Apify actor IDs
FOLLOW_ACTOR_ID = os.getenv('APIFY_FOLLOW_ACTOR_ID', 'am_production/instagram-auto-follow-unfollow')
LIKE_ACTOR_ID = os.getenv('APIFY_LIKE_ACTOR_ID', 'apify/instagram-post-scraper')  # We'll use scraper + manual like
COMMENT_ACTOR_ID = os.getenv('APIFY_COMMENT_ACTOR_ID', 'deepanshusharm/instagram-comment-bot')
POST_SCRAPER_ACTOR_ID = os.getenv('APIFY_POST_SCRAPER_ID', 'apify/instagram-post-scraper')


class WarmupActions:
    """Handles Instagram warm-up actions via Apify."""

    def __init__(self, dry_run: bool = False, min_delay: float = 30.0, max_delay: float = 120.0):
        """
        Initialize WarmupActions.

        Args:
            dry_run: If True, don't actually call Apify APIs
            min_delay: Minimum delay between actions (seconds)
            max_delay: Maximum delay between actions (seconds)
        """
        self.dry_run = dry_run
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.client = self._get_apify_client()
        self._last_action_time = 0

    def _get_apify_client(self) -> Optional['ApifyClient']:
        """Get Apify client."""
        if ApifyClient is None:
            return None

        token = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
        if not token:
            logger.warning("APIFY_API_TOKEN not found in environment")
            return None

        return ApifyClient(token)

    def _get_instagram_session(self) -> Optional[str]:
        """Get Instagram session ID from environment."""
        return os.getenv('INSTAGRAM_SESSION_ID')

    def _build_cookies_array(self) -> List[Dict]:
        """Build cookies array for Apify actors."""
        session_id = self._get_instagram_session()
        if not session_id:
            return []

        return [
            {
                "name": "sessionid",
                "value": session_id,
                "domain": ".instagram.com",
                "path": "/"
            }
        ]

    def _wait_for_rate_limit(self):
        """Wait for rate limiting between actions."""
        if self._last_action_time > 0:
            elapsed = time.time() - self._last_action_time
            delay = random.uniform(self.min_delay, self.max_delay)
            if elapsed < delay:
                sleep_time = delay - elapsed
                logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._last_action_time = time.time()

    def follow(self, username: str) -> Tuple[bool, Optional[str]]:
        """
        Follow an Instagram user.

        Args:
            username: Instagram username (without @)

        Returns:
            Tuple of (success, error_message)
        """
        username = username.lstrip('@').lower()
        logger.info(f"Following @{username}" + (" [DRY RUN]" if self.dry_run else ""))

        if self.dry_run:
            self._wait_for_rate_limit()
            return True, None

        if not self.client:
            return False, "Apify client not available"

        session_id = self._get_instagram_session()
        if not session_id:
            return False, "INSTAGRAM_SESSION_ID not found in environment"

        self._wait_for_rate_limit()

        try:
            cookies = self._build_cookies_array()

            run_input = {
                "usernames": [username],
                "action": "follow",
                "INSTAGRAM_COOKIES": cookies,
            }

            run = self.client.actor(FOLLOW_ACTOR_ID).call(run_input=run_input)

            if run.get('status') == 'SUCCEEDED':
                logger.info(f"Successfully followed @{username}")
                return True, None
            else:
                error = f"Actor run failed: {run.get('status', 'UNKNOWN')}"
                logger.error(error)
                return False, error

        except Exception as e:
            error = str(e)
            logger.error(f"Error following @{username}: {error}")
            return False, error

    def like_post(self, post_url: str) -> Tuple[bool, Optional[str]]:
        """
        Like an Instagram post.

        Args:
            post_url: URL of the Instagram post

        Returns:
            Tuple of (success, error_message)
        """
        logger.info(f"Liking post: {post_url}" + (" [DRY RUN]" if self.dry_run else ""))

        if self.dry_run:
            self._wait_for_rate_limit()
            return True, None

        if not self.client:
            return False, "Apify client not available"

        session_id = self._get_instagram_session()
        if not session_id:
            return False, "INSTAGRAM_SESSION_ID not found in environment"

        self._wait_for_rate_limit()

        try:
            cookies = self._build_cookies_array()

            # Use a like-specific actor or the post scraper with like action
            run_input = {
                "directUrls": [post_url],
                "INSTAGRAM_COOKIES": cookies,
                "likePost": True,  # If the actor supports this
            }

            run = self.client.actor(LIKE_ACTOR_ID).call(run_input=run_input)

            if run.get('status') == 'SUCCEEDED':
                logger.info(f"Successfully liked post: {post_url}")
                return True, None
            else:
                error = f"Actor run failed: {run.get('status', 'UNKNOWN')}"
                logger.error(error)
                return False, error

        except Exception as e:
            error = str(e)
            logger.error(f"Error liking post: {error}")
            return False, error

    def comment(self, post_url: str, comment_text: str) -> Tuple[bool, Optional[str]]:
        """
        Comment on an Instagram post.

        Args:
            post_url: URL of the Instagram post
            comment_text: The comment to post

        Returns:
            Tuple of (success, error_message)
        """
        logger.info(f"Commenting on post: {post_url}" + (" [DRY RUN]" if self.dry_run else ""))
        logger.debug(f"Comment: {comment_text}")

        if self.dry_run:
            self._wait_for_rate_limit()
            return True, None

        if not self.client:
            return False, "Apify client not available"

        session_id = self._get_instagram_session()
        if not session_id:
            return False, "INSTAGRAM_SESSION_ID not found in environment"

        self._wait_for_rate_limit()

        try:
            cookies = self._build_cookies_array()

            run_input = {
                "postUrls": [post_url],
                "comments": [comment_text],
                "INSTAGRAM_COOKIES": cookies,
            }

            run = self.client.actor(COMMENT_ACTOR_ID).call(run_input=run_input)

            if run.get('status') == 'SUCCEEDED':
                logger.info(f"Successfully commented on: {post_url}")
                return True, None
            else:
                error = f"Actor run failed: {run.get('status', 'UNKNOWN')}"
                logger.error(error)
                return False, error

        except Exception as e:
            error = str(e)
            logger.error(f"Error commenting: {error}")
            return False, error

    def get_recent_posts(self, username: str, limit: int = 5) -> List[Dict]:
        """
        Get recent posts from an Instagram user.

        Args:
            username: Instagram username (without @)
            limit: Maximum number of posts to return

        Returns:
            List of post dictionaries with 'url' and 'caption'
        """
        username = username.lstrip('@').lower()
        logger.info(f"Getting recent posts for @{username}" + (" [DRY RUN]" if self.dry_run else ""))

        if self.dry_run:
            # Return mock posts for dry run
            return [
                {'url': f'https://instagram.com/p/mock{i}', 'caption': f'Mock post {i}'}
                for i in range(min(limit, 3))
            ]

        if not self.client:
            logger.error("Apify client not available")
            return []

        try:
            run_input = {
                "usernames": [username],
                "resultsLimit": limit,
                "resultsType": "posts",
            }

            run = self.client.actor(POST_SCRAPER_ACTOR_ID).call(run_input=run_input)

            if run.get('status') != 'SUCCEEDED':
                logger.error(f"Post scraper failed: {run.get('status')}")
                return []

            # Get results from dataset
            dataset_id = run.get('defaultDatasetId')
            if not dataset_id:
                return []

            posts = []
            for item in self.client.dataset(dataset_id).iterate_items():
                posts.append({
                    'url': item.get('url', ''),
                    'caption': item.get('caption', ''),
                    'likes': item.get('likesCount', 0),
                    'comments': item.get('commentsCount', 0),
                    'timestamp': item.get('timestamp', ''),
                })

            logger.info(f"Found {len(posts)} posts for @{username}")
            return posts[:limit]

        except Exception as e:
            logger.error(f"Error getting posts for @{username}: {e}")
            return []
