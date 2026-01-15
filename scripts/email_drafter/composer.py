"""Composer module - Email generation using LLM.

Takes contact data and selected hook, generates personalized cold email.

NEW APPROACH (v2):
- No templated pitch - every email is unique
- Lead with curiosity about THEIR situation
- Soft mention of what we do, not a hard pitch
- Question-based CTA instead of "would you be open to"
"""

import os
import json
from typing import Dict, Any
from openai import AsyncOpenAI
from groq import AsyncGroq

from dotenv import load_dotenv

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_MODEL = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')
DEFAULT_SENDER_NAME = os.getenv('EMAIL_SENDER_NAME', 'Tomas')

# Initialize clients
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
groq_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

# What we do (context for the LLM, not to be used verbatim)
PRODUCT_CONTEXT = """LaHaus AI helps real estate teams respond to leads in under 60 seconds
using AI-powered instant response. Teams using it typically see 2x conversion rates because
speed-to-lead matters more than almost anything else in real estate."""


def get_first_name(full_name) -> str:
    """Extract first name from full name."""
    if not full_name or (isinstance(full_name, float)) or str(full_name).lower() in ['nan', 'none', '']:
        return "there"
    name_str = str(full_name).strip()
    if not name_str:
        return "there"
    return name_str.split()[0]


def build_composer_prompt(
    contact: Dict[str, Any],
    hook: Dict[str, Any],
    sender_name: str
) -> str:
    """
    Build the email composition prompt using the new approach.

    New approach:
    - Genuinely personalized (no template pitch)
    - Lead with curiosity about their business
    - Ask a real question (don't assume pain)
    - Soft mention of what we do
    """
    first_name = get_first_name(contact.get('contact_name'))
    company_name = contact.get('page_name', 'your company')

    chosen_hook = hook.get('chosen_hook', '')
    hook_source = hook.get('hook_source', 'ad')
    hook_type = hook.get('hook_type', 'offer')

    # Map hook source to natural language
    source_phrases = {
        'ad': 'their Facebook ad',
        'website': 'their website',
        'linkedin': 'their LinkedIn',
        'instagram': 'their Instagram',
        'twitter': 'their Twitter'
    }
    source_phrase = source_phrases.get(hook_source, 'online')

    prompt = f"""Write a cold email that feels genuinely personal, not templated.

RECIPIENT:
- Name: {first_name}
- Company: {company_name}
- Notable detail: {chosen_hook}
- Found on: {source_phrase}
- Detail type: {hook_type}

WHAT WE DO (for context - don't copy this verbatim):
{PRODUCT_CONTEXT}

SENDER: {sender_name}

---

WRITE AN EMAIL FOLLOWING THESE PRINCIPLES:

1. **SUBJECT LINE** (4-7 words)
   - Reference their specific situation
   - No clickbait, no emojis, no "Quick question"
   - Examples: "5 years at #1", "San Antonio's fastest growing", "10 years as Top Workplace"

2. **OPENING** (1-2 sentences)
   - Acknowledge their achievement/situation genuinely
   - Show you actually understand what it means
   - NO "I noticed" or "I came across" - just state the observation directly

3. **CURIOUS QUESTION** (1-2 sentences)
   - Ask something that shows you understand their world
   - Frame it as genuine curiosity, not a leading sales question
   - Connect naturally to lead response/speed IF it makes sense
   - Examples:
     - "At that growth rate, is your team keeping up with inbound inquiries?"
     - "Do you find high-end buyers expect faster response times?"
     - "With a larger team, is consistent response time harder or easier?"

4. **SOFT CONTEXT** (1 sentence max)
   - Briefly mention what we do in context of their situation
   - NOT a pitch - just context
   - Examples:
     - "We've been helping a few fast-scaling teams with that exact problem."
     - "We work with some teams on instant lead response."
     - "It's something we help brokerages solve."

5. **WARM CLOSE** (1 sentence)
   - End with warmth, not a sales push
   - Examples: "Either way, congrats on the momentum." / "Curious to hear how you've approached it."

6. **SIGN OFF** (EXACTLY this format)
   {sender_name}
   LaHaus Co-Founder

---

RULES:
- Total email body: 50-70 words MAX
- Sound like a human, not a sales robot
- Don't force a connection if it doesn't make sense
- Never use: "Would you be open to", "I'd love to", "Let me know if", "I noticed", "I came across"
- The entire email should feel written specifically for THIS person
- ALWAYS end with "{sender_name}\\nLaHaus Co-Founder" - never omit the title

---

OUTPUT FORMAT (respond with valid JSON only):
{{
  "subject_line": "your subject here",
  "email_body": "Hi [Name],\\n\\n[Your email here]\\n\\n[Sender name]\\nLaHaus Co-Founder"
}}
"""

    return prompt


def parse_json_response(response: str) -> Dict[str, str]:
    """
    Parse LLM response as JSON.

    Returns dict with subject_line and email_body.
    """
    response = response.strip()

    # Try to find JSON in the response
    try:
        # Handle case where response might have markdown code blocks
        if '```json' in response:
            start = response.find('```json') + 7
            end = response.find('```', start)
            response = response[start:end].strip()
        elif '```' in response:
            start = response.find('```') + 3
            end = response.find('```', start)
            response = response[start:end].strip()

        data = json.loads(response)
        return {
            'subject_line': data.get('subject_line', 'Quick question'),
            'email_body': data.get('email_body', '')
        }
    except json.JSONDecodeError:
        # Fallback: try to extract subject and body manually
        lines = response.split('\n')
        subject = "Quick question"
        body = response

        for i, line in enumerate(lines):
            if line.lower().startswith('subject:'):
                subject = line.split(':', 1)[1].strip()
                body = '\n'.join(lines[i+1:]).strip()
                break

        return {
            'subject_line': subject,
            'email_body': body
        }


def get_fallback_email(contact: Dict[str, Any], hook: Dict[str, Any], sender_name: str) -> Dict[str, Any]:
    """
    Return a fallback email when generation fails.
    Uses the new approach style.
    """
    first_name = get_first_name(contact.get('contact_name'))
    company_name = contact.get('page_name', 'your company')
    chosen_hook = hook.get('chosen_hook', '')

    # Create a simple but genuine fallback
    if chosen_hook:
        hook_summary = chosen_hook[:50] + '...' if len(chosen_hook) > 50 else chosen_hook
        body = f"""Hi {first_name},

{hook_summary} — that caught my attention.

Curious: how does your team handle lead response when things get busy? It's a challenge I hear about a lot.

{sender_name}
LaHaus Co-Founder"""
    else:
        body = f"""Hi {first_name},

Came across {company_name} and was curious about something.

How does your team handle lead response when things get busy? It's a challenge I hear about a lot in real estate.

{sender_name}
LaHaus Co-Founder"""

    return {
        'subject_line': f'{company_name[:30]}' if company_name else 'Quick question',
        'email_body': body,
        'hook_used': chosen_hook,
        'hook_source': hook.get('hook_source', ''),
        'error': True
    }


async def compose_email(
    contact: Dict[str, Any],
    hook: Dict[str, Any],
    sender_name: str = None
) -> Dict[str, Any]:
    """
    Generate a personalized cold email using the new approach.

    New approach:
    - No templated pitch
    - Genuinely personalized
    - Question-based CTA
    - Soft mention of what we do

    Args:
        contact: Contact dict with contact_name, page_name, primary_email
        hook: Hook dict with chosen_hook, hook_source, hook_type
        sender_name: Name to sign email with

    Returns:
        Dict with subject_line, email_body, hook_used, hook_source
    """
    if sender_name is None:
        sender_name = DEFAULT_SENDER_NAME

    prompt = build_composer_prompt(contact, hook, sender_name)
    system_msg = """You are an expert at writing cold emails that feel personal and human.
Your emails get responses because they show genuine curiosity about the recipient's business,
not because they have clever sales tactics. You write like a peer, not a salesperson."""

    # Try Groq first (faster, usually available), then OpenAI
    if groq_client:
        try:
            print(f"    [Composer] Generating email for {contact.get('contact_name', 'Unknown')}...")

            response = await groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=400
            )

            content = response.choices[0].message.content
            parsed = parse_json_response(content)

            result = {
                'subject_line': parsed['subject_line'],
                'email_body': parsed['email_body'],
                'hook_used': hook.get('chosen_hook', ''),
                'hook_source': hook.get('hook_source', '')
            }

            print(f"    [Composer] Generated: {parsed['subject_line'][:40]}...")
            return result

        except Exception as e:
            print(f"    [Composer] Groq error: {e}")

    # Fallback to OpenAI
    if openai_client:
        try:
            print(f"    [Composer] Trying OpenAI for {contact.get('contact_name', 'Unknown')}...")

            response = await openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=400
            )

            content = response.choices[0].message.content
            parsed = parse_json_response(content)

            result = {
                'subject_line': parsed['subject_line'],
                'email_body': parsed['email_body'],
                'hook_used': hook.get('chosen_hook', ''),
                'hook_source': hook.get('hook_source', '')
            }

            print(f"    [Composer] (OpenAI) Generated: {parsed['subject_line'][:40]}...")
            return result

        except Exception as e:
            print(f"    [Composer] OpenAI error: {e}")

    # No LLM available
    if not openai_client and not groq_client:
        print("    [Composer] No LLM API keys configured")

    return get_fallback_email(contact, hook, sender_name)


# For direct testing
if __name__ == "__main__":
    import asyncio

    async def test():
        # Test with real-ish data
        test_cases = [
            {
                'contact': {
                    'contact_name': 'Tim Ellis',
                    'page_name': 'REEP Equity',
                    'primary_email': 'tim@reepequity.com'
                },
                'hook': {
                    'chosen_hook': 'Named Top 5 Fastest Growing Business in San Antonio two years in a row',
                    'hook_source': 'website',
                    'hook_type': 'achievement'
                }
            },
            {
                'contact': {
                    'contact_name': 'Ross Howatt',
                    'page_name': 'The Newcomer Group',
                    'primary_email': 'ross@thenewcomergroup.com'
                },
                'hook': {
                    'chosen_hook': '#1 real estate team for high-end properties in St. Augustine for 5 years',
                    'hook_source': 'ad',
                    'hook_type': 'achievement'
                }
            }
        ]

        for i, case in enumerate(test_cases, 1):
            print(f"\n{'='*60}")
            print(f"TEST {i}: {case['contact']['contact_name']} @ {case['contact']['page_name']}")
            print('='*60)

            result = await compose_email(case['contact'], case['hook'])

            print(f"\nSubject: {result['subject_line']}")
            print("-" * 40)
            print(result['email_body'])
            print("-" * 40)

    asyncio.run(test())
