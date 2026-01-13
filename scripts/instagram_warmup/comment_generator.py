"""
CommentGenerator - AI-powered personalized Instagram comments.

Generates short, genuine comments for Instagram posts using Groq/OpenAI.
"""

import os
import random
import logging
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Try to import LLM clients
try:
    from groq import Groq
except ImportError:
    Groq = None
    logger.debug("Groq not installed")

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None
    logger.debug("OpenAI not installed")

# Fallback comments when API fails
FALLBACK_COMMENTS = [
    "Love this!",
    "Great post!",
    "This is amazing",
    "So inspiring",
    "Beautiful work",
    "Really impressive",
    "Nice one!",
    "This is fantastic",
    "Awesome content",
    "Looking great!",
]

COMMENT_PROMPT = """Generate a genuine, personalized Instagram comment for this business post.

Company: {page_name}
Industry Context: {company_description}
Post Caption: {post_caption}
Post Type: {post_type}

Requirements:
- Keep it SHORT (5-15 words max)
- Sound natural and human
- Reference something specific from their post if possible
- Do NOT be salesy or mention your business
- Do NOT ask questions or try to start a conversation
- Use casual tone, occasional emoji is okay but not required
- Do NOT include quotes around the comment

Examples of good comments:
- This property is stunning! Love the natural lighting
- Great tips here - the third one really resonated
- The market insight here is spot on
- Beautiful work on this one

Generate exactly 1 short comment (no quotes, no explanation):"""


class CommentGenerator:
    """Generates personalized Instagram comments using LLMs."""

    def __init__(self):
        """Initialize with Groq (preferred) or OpenAI."""
        self.groq_client = None
        self.openai_client = None

        # Try Groq first (faster)
        groq_key = os.getenv('GROQ_API_KEY')
        if groq_key and Groq:
            try:
                self.groq_client = Groq(api_key=groq_key)
                logger.debug("Using Groq for comment generation")
            except Exception as e:
                logger.warning(f"Failed to initialize Groq: {e}")

        # Fall back to OpenAI
        if not self.groq_client:
            openai_key = os.getenv('OPENAI_API_KEY')
            if openai_key and OpenAI:
                try:
                    self.openai_client = OpenAI(api_key=openai_key)
                    logger.debug("Using OpenAI for comment generation")
                except Exception as e:
                    logger.warning(f"Failed to initialize OpenAI: {e}")

    def generate_comment(
        self,
        page_name: str,
        post_caption: str,
        post_type: str = "image",
        company_description: str = ""
    ) -> str:
        """
        Generate a personalized comment for an Instagram post.

        Args:
            page_name: Name of the company/page
            post_caption: Caption of the post
            post_type: Type of post (image, video, reel)
            company_description: Optional context about the company

        Returns:
            Generated comment string
        """
        prompt = COMMENT_PROMPT.format(
            page_name=page_name,
            company_description=company_description or "Business",
            post_caption=post_caption[:500] if post_caption else "No caption",
            post_type=post_type
        )

        try:
            if self.groq_client:
                response = self.groq_client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=0.8
                )
                comment = response.choices[0].message.content.strip()

            elif self.openai_client:
                response = self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=0.8
                )
                comment = response.choices[0].message.content.strip()

            else:
                logger.warning("No LLM client available, using fallback")
                return self._get_fallback_comment()

            return self._sanitize_comment(comment)

        except Exception as e:
            logger.error(f"Error generating comment: {e}")
            return self._get_fallback_comment()

    def _sanitize_comment(self, comment: str) -> str:
        """Clean up generated comment."""
        if not comment:
            return self._get_fallback_comment()

        # Remove surrounding quotes
        comment = comment.strip().strip('"\'')

        # Remove any leading/trailing whitespace
        comment = comment.strip()

        # Truncate if too long
        if len(comment) > 100:
            # Try to cut at a word boundary
            comment = comment[:97]
            last_space = comment.rfind(' ')
            if last_space > 50:
                comment = comment[:last_space]
            comment = comment.rstrip('.,!?') + '...'

        return comment

    def _get_fallback_comment(self) -> str:
        """Return a random fallback comment."""
        return random.choice(FALLBACK_COMMENTS)

    def is_available(self) -> bool:
        """Check if an LLM client is available."""
        return self.groq_client is not None or self.openai_client is not None
