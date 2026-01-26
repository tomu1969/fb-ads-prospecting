"""Groq API client with rate limiting and cost tracking.

Uses Llama 3.3 70B for entity extraction from emails.
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from dotenv import load_dotenv
from groq import Groq

load_dotenv()

logger = logging.getLogger(__name__)

# Groq pricing (per 1M tokens)
INPUT_COST_PER_M = 0.59
OUTPUT_COST_PER_M = 0.79

# Rate limiting
REQUESTS_PER_MINUTE = 30
REQUEST_DELAY = 60.0 / REQUESTS_PER_MINUTE  # 2 seconds


@dataclass
class ExtractionResult:
    """Result from LLM extraction."""
    company: Optional[str]
    role: Optional[str]
    topics: List[str]
    confidence: float
    input_tokens: int
    output_tokens: int
    cost_usd: float


SYSTEM_PROMPT = """You extract structured contact information from emails. Return valid JSON only."""

USER_PROMPT_TEMPLATE = """Extract the sender's professional information from these emails.

Contact: {name} <{email}>

{emails_text}

Return JSON:
{{
  "company": "Company name or null",
  "role": "Job title or null",
  "topics": ["topic1", "topic2"],
  "confidence": 0.0-1.0
}}

Rules:
- Extract company/role from email signature first
- If no signature, infer role from email content and context
- Topics: 1-3 main professional topics discussed
- Confidence: 0.8+ if signature found, 0.5-0.8 if inferred from content"""


class GroqClient:
    """Groq API client for entity extraction."""

    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not set in environment")
        self.model = model
        self.last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            sleep_time = REQUEST_DELAY - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in USD."""
        input_cost = (input_tokens / 1_000_000) * INPUT_COST_PER_M
        output_cost = (output_tokens / 1_000_000) * OUTPUT_COST_PER_M
        return input_cost + output_cost

    def extract_contact_info(
        self,
        email: str,
        name: str,
        emails: List[Dict[str, str]],
    ) -> ExtractionResult:
        """Extract company, role, topics from contact's emails."""
        # Format emails for prompt
        emails_text = ""
        for i, e in enumerate(emails, 1):
            emails_text += f"\n--- Email {i} ---\n"
            emails_text += f"Subject: {e.get('subject', 'N/A')}\n"
            emails_text += f"Date: {e.get('date', 'N/A')}\n"
            emails_text += f"Body:\n{e.get('body', '')[:3000]}\n"

        user_prompt = USER_PROMPT_TEMPLATE.format(
            name=name,
            email=email,
            emails_text=emails_text,
        )

        self._rate_limit()

        client = Groq(api_key=self.api_key)

        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"},
            )
        except Exception as e:
            logger.error(f"Groq API error: {e}")
            return ExtractionResult(
                company=None, role=None, topics=[], confidence=0.0,
                input_tokens=0, output_tokens=0, cost_usd=0.0,
            )

        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens
        cost = self._calculate_cost(input_tokens, output_tokens)

        try:
            content = response.choices[0].message.content
            data = json.loads(content)
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning(f"Failed to parse Groq response: {e}")
            data = {}

        return ExtractionResult(
            company=data.get("company"),
            role=data.get("role"),
            topics=data.get("topics", []),
            confidence=data.get("confidence", 0.0),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
        )
