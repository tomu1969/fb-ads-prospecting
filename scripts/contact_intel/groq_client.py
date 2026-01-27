"""LLM client with rate limiting and cost tracking.

Supports OpenAI (GPT-4o-mini) and Groq (Llama 3.3 70B) for entity extraction.
Default: OpenAI GPT-4o-mini (cheaper, no daily token limit).
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Provider selection (openai or groq)
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()

# OpenAI GPT-4o-mini pricing (per 1M tokens) - DEFAULT
OPENAI_INPUT_COST_PER_M = 0.15
OPENAI_OUTPUT_COST_PER_M = 0.60

# Groq pricing (per 1M tokens)
GROQ_INPUT_COST_PER_M = 0.59
GROQ_OUTPUT_COST_PER_M = 0.79

# Rate limiting (OpenAI is more generous)
OPENAI_REQUEST_DELAY = 0.1  # 10 requests/second is fine
GROQ_REQUEST_DELAY = 2.0    # 30 requests/minute

# Email body truncation limit (chars) to keep prompts within token limits
MAX_BODY_LENGTH = 3000


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
    """LLM client for entity extraction. Supports OpenAI and Groq."""

    def __init__(self, model: str = None, provider: str = None):
        """Initialize client.

        Args:
            model: Model name. Default depends on provider.
            provider: 'openai' or 'groq'. Default from LLM_PROVIDER env var.
        """
        self.provider = provider or LLM_PROVIDER

        if self.provider == "openai":
            self.api_key = os.getenv("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY not set in environment")
            self.model = model or "gpt-4o-mini"
            self.input_cost_per_m = OPENAI_INPUT_COST_PER_M
            self.output_cost_per_m = OPENAI_OUTPUT_COST_PER_M
            self.request_delay = OPENAI_REQUEST_DELAY
        else:
            self.api_key = os.getenv("GROQ_API_KEY")
            if not self.api_key:
                raise ValueError("GROQ_API_KEY not set in environment")
            self.model = model or "llama-3.3-70b-versatile"
            self.input_cost_per_m = GROQ_INPUT_COST_PER_M
            self.output_cost_per_m = GROQ_OUTPUT_COST_PER_M
            self.request_delay = GROQ_REQUEST_DELAY

        self.last_request_time = 0
        logger.info(f"Using {self.provider} provider with model {self.model}")

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in USD."""
        input_cost = (input_tokens / 1_000_000) * self.input_cost_per_m
        output_cost = (output_tokens / 1_000_000) * self.output_cost_per_m
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
            emails_text += f"Body:\n{e.get('body', '')[:MAX_BODY_LENGTH]}\n"

        user_prompt = USER_PROMPT_TEMPLATE.format(
            name=name,
            email=email,
            emails_text=emails_text,
        )

        self._rate_limit()

        if self.provider == "openai":
            return self._call_openai(user_prompt)
        else:
            return self._call_groq(user_prompt)

    def _call_openai(self, user_prompt: str) -> ExtractionResult:
        """Call OpenAI API."""
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Install openai SDK: pip install openai")

        client = OpenAI(api_key=self.api_key)

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
            logger.error(f"OpenAI API error: {e}")
            return ExtractionResult(
                company=None, role=None, topics=[], confidence=0.0,
                input_tokens=0, output_tokens=0, cost_usd=0.0,
            )

        return self._parse_response(response)

    def _call_groq(self, user_prompt: str) -> ExtractionResult:
        """Call Groq API."""
        try:
            from groq import Groq
        except ImportError:
            raise ImportError("Install groq SDK: pip install groq")

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

        return self._parse_response(response)

    def _parse_response(self, response) -> ExtractionResult:
        """Parse LLM response into ExtractionResult."""
        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens
        cost = self._calculate_cost(input_tokens, output_tokens)

        try:
            content = response.choices[0].message.content
            data = json.loads(content)
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning(f"Failed to parse LLM response: {e}")
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
