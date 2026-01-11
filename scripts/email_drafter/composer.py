"""Composer module - Email generation using LLM.

Takes contact data and selected hook, generates personalized cold email.
"""

import os
import re
from typing import Dict, Any, Tuple
from openai import AsyncOpenAI

from dotenv import load_dotenv

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')
DEFAULT_SENDER_NAME = os.getenv('EMAIL_SENDER_NAME', 'Tomas')

# Initialize OpenAI client
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# The standard offer (constant across all emails)
STANDARD_OFFER = """We help 100+ realtors and real estate teams close more deals by responding to leads instantly and nurturing them automatically until they're ready to buy. Would you be open to seeing how they do it?"""


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
    Build the email composition prompt.

    Args:
        contact: Contact dict with contact_name, page_name, etc.
        hook: Hook dict from analyzer with chosen_hook, problem_framing, etc.
        sender_name: Name to sign the email with

    Returns:
        Formatted prompt string for the LLM
    """
    first_name = get_first_name(contact.get('contact_name'))
    company_name = contact.get('page_name', 'your company')

    chosen_hook = hook.get('chosen_hook', '')
    hook_source = hook.get('hook_source', 'ad')
    hook_type = hook.get('hook_type', 'offer')
    problem_framing = hook.get('problem_framing', '')

    # Map hook source to natural language
    source_phrases = {
        'ad': 'in your Facebook ad',
        'website': 'on your website',
        'linkedin': 'on your LinkedIn profile',
        'instagram': 'on your Instagram',
        'twitter': 'on your Twitter'
    }
    source_phrase = source_phrases.get(hook_source, 'online')

    prompt = f"""You write hyper-personalized cold emails for real estate lead services.

RECIPIENT:
- First Name: {first_name}
- Company: {company_name}

PERSONALIZATION HOOK:
- Detail: {chosen_hook}
- Found: {source_phrase}
- Type: {hook_type}
- Why it matters: {problem_framing}

STANDARD OFFER (use exactly):
"{STANDARD_OFFER}"

SENDER NAME: {sender_name}

---

YOUR TASK:
Write a short email (~80 words max) following this EXACT structure:

1. **SUBJECT LINE**: Short, specific reference to the hook (5-8 words)

2. **HOOK** (1-2 sentences): Reference the specific detail from above.
   - Quote exact phrases when possible (use quotation marks)
   - Be specific about where you found it: {source_phrase}
   - Example: 'I was on your site and saw the banner regarding your "50% lead surplus"'

3. **PROBLEM** (1-2 sentences): Connect that detail to the need for faster lead response and better conversion.
   - Frame it as: more leads/success means more opportunities slipping through without instant follow-up
   - Use the problem framing provided: "{problem_framing}"

4. **OFFER** (exactly this text): "{STANDARD_OFFER}"

5. **SIGN OFF**: "Thanks,\\n{sender_name}\\nCofounder, LaHaus AI"

RULES:
- Start with "Subject: " followed by the subject line, then a blank line, then the email body
- Never use generic phrases like "I came across your company" or "Hope this finds you well"
- The hook must reference the SPECIFIC detail provided above
- Keep total body under 100 words
- Be conversational but professional
- DO NOT use bullet points or numbered lists in the email body

OUTPUT FORMAT:
Subject: [your subject line here]

Hi {first_name},

[Hook paragraph]

[Problem paragraph]

[Offer paragraph - use EXACTLY the standard offer text]

Thanks,
{sender_name}
Cofounder, LaHaus AI"""

    return prompt


def parse_email_response(response: str) -> Tuple[str, str]:
    """
    Parse the LLM response to extract subject and body.

    Args:
        response: Raw LLM response string

    Returns:
        Tuple of (subject_line, email_body)
    """
    response = response.strip()

    subject = ""
    body = ""

    # Try to extract subject line
    if response.lower().startswith('subject:'):
        lines = response.split('\n', 1)
        subject_line = lines[0]
        # Remove "Subject:" prefix
        subject = re.sub(r'^subject:\s*', '', subject_line, flags=re.IGNORECASE).strip()

        if len(lines) > 1:
            body = lines[1].strip()
    else:
        # No subject line found, use entire response as body
        body = response
        # Try to generate a subject from the first line
        first_line = body.split('\n')[0] if body else ""
        if first_line.startswith('Hi ') or first_line.startswith('Hello '):
            subject = "Quick question"
        else:
            subject = first_line[:50] if first_line else "Quick question"

    return subject, body


def get_fallback_email(contact: Dict[str, Any], sender_name: str) -> Dict[str, Any]:
    """
    Return a fallback email when generation fails.

    Args:
        contact: Contact dict
        sender_name: Sender name

    Returns:
        Fallback email dict
    """
    first_name = get_first_name(contact.get('contact_name'))

    return {
        'subject_line': 'Quick question',
        'email_body': f"""Hi {first_name},

[Error generating personalized email - please compose manually]

{STANDARD_OFFER}

Thanks,
{sender_name}
Cofounder, LaHaus AI""",
        'hook_used': '',
        'hook_source': '',
        'error': True
    }


async def compose_email(
    contact: Dict[str, Any],
    hook: Dict[str, Any],
    sender_name: str = None
) -> Dict[str, Any]:
    """
    Generate a personalized cold email using the selected hook.

    Args:
        contact: Contact dict with:
            - contact_name: Recipient's name
            - page_name: Company name
            - primary_email: Email address
        hook: Hook dict from analyzer with:
            - chosen_hook: The specific detail to reference
            - hook_source: Where the hook was found
            - hook_type: Category of hook
            - problem_framing: How to connect to lead overflow
        sender_name: Name to sign the email with (default from config)

    Returns:
        Dict with:
            - subject_line: Email subject
            - email_body: Full email text
            - hook_used: The hook that was referenced
            - hook_source: Source of the hook
    """
    if sender_name is None:
        sender_name = DEFAULT_SENDER_NAME

    if not OPENAI_API_KEY:
        print("    [Composer] No OpenAI API key configured")
        return get_fallback_email(contact, sender_name)

    try:
        prompt = build_composer_prompt(contact, hook, sender_name)

        print(f"    [Composer] Generating email for {contact.get('contact_name', 'Unknown')}...")

        response = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert cold email copywriter specializing in real estate services. Write concise, personalized emails that reference specific details about the recipient."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=400
        )

        content = response.choices[0].message.content
        subject, body = parse_email_response(content)

        result = {
            'subject_line': subject,
            'email_body': body,
            'hook_used': hook.get('chosen_hook', ''),
            'hook_source': hook.get('hook_source', '')
        }

        print(f"    [Composer] Generated email with subject: {subject[:40]}...")

        return result

    except Exception as e:
        print(f"    [Composer] Error: {e}")
        return get_fallback_email(contact, sender_name)


# For direct testing
if __name__ == "__main__":
    import asyncio

    async def test():
        test_contact = {
            'contact_name': 'John Doe',
            'page_name': 'Example Realty',
            'primary_email': 'john@example.com'
        }

        test_hook = {
            'chosen_hook': '50% lead surplus and hiring 2 new agents',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'When hiring while having lead surplus, response times suffer'
        }

        result = await compose_email(test_contact, test_hook)
        print("\nGenerated Email:")
        print(f"Subject: {result['subject_line']}")
        print("-" * 40)
        print(result['email_body'])
        print("-" * 40)
        print(f"Hook used: {result['hook_used']}")
        print(f"Hook source: {result['hook_source']}")

    asyncio.run(test())
