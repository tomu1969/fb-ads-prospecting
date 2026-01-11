"""Configuration for Email Drafter module."""

import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
EXA_API_KEY = os.getenv('EXA_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Exa API
EXA_API_URL = "https://api.exa.ai/search"

# OpenAI
OPENAI_MODEL = "gpt-4o"

# Defaults
DEFAULT_SENDER_NAME = "Tomás"
DEFAULT_TEMPLATE = "cold_outreach"

# Cost tracking (estimates)
COST_EXA_SEARCH = 0.001  # Per search
COST_GPT4O_ANALYZER = 0.015  # Per analysis
COST_GPT4O_COMPOSER = 0.015  # Per email

# The standard offer (constant across all emails)
STANDARD_OFFER = """We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?"""

# Hook types for classification
HOOK_TYPES = [
    'story',      # Personal stories shared in ads/posts
    'hiring',     # Hiring notices, team growth
    'achievement', # Milestones, awards, deals closed
    'offer',      # Special promotions, unique offers
    'milestone',  # Business milestones
    'personal',   # Personal details from social media
]

# Hook sources
HOOK_SOURCES = [
    'ad',         # From their Facebook ad content
    'website',    # From their company website
    'linkedin',   # From their LinkedIn profile
    'instagram',  # From their Instagram
    'twitter',    # From their Twitter
]
