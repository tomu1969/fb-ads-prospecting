"""Analyzer module - LLM-powered hook selection for email personalization.

Analyzes all research data (ads, website, LinkedIn, social) and selects
the best personalization hook for cold outreach.
"""

import os
import json
from typing import Dict, Any, List
from openai import AsyncOpenAI

from dotenv import load_dotenv

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')

# Initialize OpenAI client
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# Valid values for hook classification
VALID_HOOK_SOURCES = ['ad', 'website', 'linkedin', 'instagram', 'twitter']
VALID_HOOK_TYPES = ['story', 'hiring', 'achievement', 'offer', 'milestone', 'personal']


def build_analyzer_prompt(research: Dict[str, Any]) -> str:
    """
    Build the analysis prompt from research data.

    Args:
        research: Research dict from researcher module

    Returns:
        Formatted prompt string for the LLM
    """
    # Extract data sections
    company_data = research.get('company', {})
    personal_data = research.get('personal', {})
    ad_content = research.get('ad_content', [])

    website_findings = company_data.get('website_findings', [])

    linkedin_data = personal_data.get('linkedin', {})
    linkedin_headline = linkedin_data.get('headline')
    linkedin_findings = linkedin_data.get('findings', [])

    social_media = personal_data.get('social_media', {})
    instagram_content = social_media.get('instagram', [])
    twitter_content = social_media.get('twitter', [])

    # Build prompt sections
    prompt = """You are an expert at finding personalization hooks for cold outreach emails to real estate professionals.

AVAILABLE DATA ON PROSPECT:

## Their Facebook Ads:
"""
    if ad_content:
        for ad in ad_content:
            prompt += f"- {ad}\n"
    else:
        prompt += "- No ad content available\n"

    prompt += """
## Their Company Website (via Exa):
"""
    if website_findings:
        for finding in website_findings:
            prompt += f"- {finding}\n"
    else:
        prompt += "- No website findings available\n"

    prompt += """
## Their LinkedIn Profile:
"""
    if linkedin_headline:
        prompt += f"Headline: {linkedin_headline}\n"
    if linkedin_findings:
        for finding in linkedin_findings:
            prompt += f"- {finding}\n"
    if not linkedin_headline and not linkedin_findings:
        prompt += "- No LinkedIn data available\n"

    prompt += """
## Their Social Media:
### Instagram:
"""
    if instagram_content:
        for item in instagram_content:
            prompt += f"- {item}\n"
    else:
        prompt += "- No Instagram content available\n"

    prompt += """
### Twitter:
"""
    if twitter_content:
        for item in twitter_content:
            prompt += f"- {item}\n"
    else:
        prompt += "- No Twitter content available\n"

    prompt += """
---

YOUR TASK:
Analyze ALL the data above and select the SINGLE BEST hook for a personalized cold email.

HOOK CRITERIA (ranked by priority):
1. **Uniqueness** - Something ONLY this person/company would have
2. **Recency** - Recent events/posts are more relevant
3. **Vulnerability** - Personal stories they've shared publicly
4. **Growth signals** - Hiring, expansion, achievements
5. **Specific numbers/quotes** - Exact phrases we can reference

IMPORTANT - FRANCHISE AGENTS:
If this agent works for a major franchise (RE/MAX, Coldwell Banker, Keller Williams, Sotheby's, etc.):
- PRIORITIZE the agent's PERSONAL achievements, team page, or individual website
- AVOID using generic franchise-level information (corporate stats, franchise history)
- Look for: their personal sales records, team awards, local market expertise, personal story
- The hook should feel personalized to THIS specific agent, not their franchise

OUTPUT FORMAT (respond with ONLY valid JSON):
{
    "chosen_hook": "The specific detail to reference in the email",
    "hook_source": "ad|website|linkedin|instagram|twitter",
    "hook_type": "story|hiring|achievement|offer|milestone|personal",
    "problem_framing": "How this connects to needing instant lead response and automated nurturing to close more deals",
    "confidence": 85,
    "reasoning": "Why this hook is better than alternatives"
}

EXAMPLES OF GOOD HOOKS:
- "50% lead surplus" banner on website -> lots of leads that need fast follow-up
- Personal story about learning to swim at 40 -> high-engagement content attracts leads that need nurturing
- "Just closed 50th deal this year" LinkedIn post -> success that could scale with better lead conversion
- Hiring post for 2 new agents -> growing team needs systems to respond to leads faster
- High ad spend or multiple platforms -> investing in leads that need instant response to convert
- [FRANCHISE] "Top 1% agent in Denver market" on their team page -> personal achievement, not franchise stat
- [FRANCHISE] "The Smith Team closed $50M in 2025" -> team-specific, not Coldwell Banker corporate

AVOID:
- Generic facts anyone could find
- Old/stale information
- Things that don't connect to lead response speed or conversion challenges
- [FRANCHISE] Generic franchise stats like "RE/MAX has 100,000 agents worldwide"
- [FRANCHISE] Corporate franchise news that doesn't mention the specific agent

If no good hooks are available, set confidence to 0 and explain why in reasoning.

Respond with ONLY the JSON object, no additional text."""

    return prompt


def parse_llm_response(content: str) -> Dict[str, Any]:
    """
    Parse the LLM response into a structured dict.

    Args:
        content: Raw LLM response string

    Returns:
        Parsed dict with hook data
    """
    try:
        # Try to extract JSON from the response
        # Handle cases where LLM adds markdown code blocks
        if '```json' in content:
            content = content.split('```json')[1].split('```')[0]
        elif '```' in content:
            content = content.split('```')[1].split('```')[0]

        result = json.loads(content.strip())

        # Validate and normalize fields
        result['chosen_hook'] = str(result.get('chosen_hook', ''))
        result['hook_source'] = result.get('hook_source', 'ad')
        result['hook_type'] = result.get('hook_type', 'offer')
        result['problem_framing'] = str(result.get('problem_framing', ''))
        result['confidence'] = int(result.get('confidence', 0))
        result['reasoning'] = str(result.get('reasoning', ''))

        # Ensure valid enum values
        if result['hook_source'] not in VALID_HOOK_SOURCES:
            result['hook_source'] = 'ad'
        if result['hook_type'] not in VALID_HOOK_TYPES:
            result['hook_type'] = 'offer'

        # Clamp confidence to valid range
        result['confidence'] = max(0, min(100, result['confidence']))

        return result

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"    [Analyzer] Failed to parse LLM response: {e}")
        return get_fallback_result("Failed to parse LLM response")


def get_fallback_result(reason: str = "Unknown error") -> Dict[str, Any]:
    """
    Return a fallback result when analysis fails.

    Args:
        reason: Explanation for the fallback

    Returns:
        Default hook dict with zero confidence
    """
    return {
        'chosen_hook': '',
        'hook_source': 'ad',
        'hook_type': 'offer',
        'problem_framing': '',
        'confidence': 0,
        'reasoning': f'Fallback result: {reason}'
    }


async def analyze_and_select_hook(research: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze research data and select the best personalization hook.

    Uses an LLM to review all available data (ads, website, LinkedIn, social)
    and choose the most effective hook for cold outreach.

    Args:
        research: Research dict from researcher module containing:
            - company: {website_findings: [...]}
            - personal: {linkedin: {...}, social_media: {...}}
            - ad_content: [...]
            - sources: [...]

    Returns:
        Dict with:
            - chosen_hook: str - The specific detail to reference
            - hook_source: str - 'ad', 'website', 'linkedin', 'instagram', 'twitter'
            - hook_type: str - 'story', 'hiring', 'achievement', 'offer', 'milestone', 'personal'
            - problem_framing: str - How to connect to lead overflow
            - confidence: int - 0-100 quality score
            - reasoning: str - Why this hook was chosen
    """
    if not OPENAI_API_KEY:
        print("    [Analyzer] No OpenAI API key configured")
        return get_fallback_result("No API key")

    # Check if we have any data to analyze
    has_data = False
    if research.get('ad_content'):
        has_data = True
    if research.get('company', {}).get('website_findings'):
        has_data = True
    if research.get('personal', {}).get('linkedin', {}).get('headline'):
        has_data = True
    if research.get('personal', {}).get('linkedin', {}).get('findings'):
        has_data = True
    if research.get('personal', {}).get('social_media', {}).get('instagram'):
        has_data = True
    if research.get('personal', {}).get('social_media', {}).get('twitter'):
        has_data = True

    if not has_data:
        print("    [Analyzer] No research data available")
        return get_fallback_result("No research data available")

    try:
        prompt = build_analyzer_prompt(research)

        print("    [Analyzer] Analyzing research data...")

        response = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at analyzing prospect data and selecting the best personalization hooks for cold outreach emails. Always respond with valid JSON only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=500
        )

        content = response.choices[0].message.content
        result = parse_llm_response(content)

        print(f"    [Analyzer] Selected hook: {result['chosen_hook'][:50]}... (confidence: {result['confidence']})")

        return result

    except Exception as e:
        print(f"    [Analyzer] Error: {e}")
        return get_fallback_result(str(e))


# For direct testing
if __name__ == "__main__":
    import asyncio

    async def test():
        test_research = {
            'company': {
                'website_findings': ['Hiring 2 new agents', '50% lead surplus']
            },
            'personal': {
                'linkedin': {
                    'headline': 'Top 1% Realtor in Miami',
                    'findings': ['Closed 50th deal this year']
                },
                'social_media': {
                    'instagram': ['Just listed a beautiful beachfront property'],
                    'twitter': []
                }
            },
            'ad_content': ['Looking for motivated buyers in Miami?'],
            'sources': ['example.com', 'linkedin.com/in/test']
        }

        result = await analyze_and_select_hook(test_research)
        print("\nResult:")
        print(json.dumps(result, indent=2))

    asyncio.run(test())
