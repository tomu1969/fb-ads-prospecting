"""Email Drafter Module - Hyper-personalized cold email generation.

This standalone module researches prospects via Exa API, analyzes the data
to find the best personalization hook, and generates cold outreach emails.

Components:
- researcher.py: Multi-source Exa research (website, LinkedIn, social)
- analyzer.py: LLM-powered hook selection
- composer.py: Email generation
- drafter.py: Main orchestrator
"""

# Import available components (others will be added as built)
from .researcher import research_prospect
from .analyzer import analyze_and_select_hook
from .composer import compose_email

__all__ = ['research_prospect', 'analyze_and_select_hook', 'compose_email']
