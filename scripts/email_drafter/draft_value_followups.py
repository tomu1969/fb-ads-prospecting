"""Draft value-add follow-up emails for batches 2 and 3.

Unlike "Quick follow-up" approach that triggered 100% unsubscribes,
this provides genuine value - an insight, stat, or question.
"""

import asyncio
import pandas as pd
import sys
from datetime import datetime
from openai import AsyncOpenAI
from groq import AsyncGroq
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize clients
groq_client = AsyncGroq(api_key=os.getenv('GROQ_API_KEY'))

# Do not contact list
DO_NOT_CONTACT = [
    'christopher.mosley@era.com',
    'dylan@luisandrewgroup.com',
    'donald.mcvicar@domorealestate.com',
    'bob@eveloteam.com'
]

# Known bounces
BOUNCED_EMAILS = [
    'emily@delriotitle.net',  # Batch 2 bounce
    'david@homesusa.com'       # Batch 3 bounce
]


async def generate_value_followup(contact: dict, original_email: dict) -> dict:
    """Generate a value-add follow-up email."""

    first_name = contact['contact_name'].split()[0] if contact['contact_name'] else 'there'
    company = contact['page_name']
    original_hook = original_email.get('hook_used', '')

    prompt = f"""Write a follow-up email that provides VALUE, not just a reminder.

CONTEXT:
- Recipient: {first_name} at {company}
- Original email sent 2 days ago about: {original_hook[:100]}
- They haven't replied yet

YOUR TASK:
Write a SHORT follow-up that shares ONE valuable insight about lead response in their industry.

RULES:
1. DO NOT say "following up", "circling back", "checking in", or "touching base"
2. DO NOT reference the previous email directly
3. DO provide a genuine insight, stat, or observation they'd find useful
4. Keep it under 50 words
5. End with a soft question, not a pitch
6. Sign off with just "Tomas" (no title needed for follow-ups)

GOOD EXAMPLES:
- "Saw an interesting stat: 78% of deals go to the first responder. Made me think of your team."
- "Quick thought: luxury buyers expect sub-5-minute response. How does your team handle after-hours inquiries?"
- "Noticed more teams using AI for initial lead response. Curious if that's on your radar?"

BAD EXAMPLES (DO NOT USE):
- "Just following up on my last email..."
- "Wanted to circle back..."
- "Did you get a chance to see my previous message?"

OUTPUT FORMAT (JSON only):
{{
  "subject_line": "short, intriguing subject (3-5 words, no 'following up')",
  "email_body": "Hi {first_name},\\n\\n[Your value-add message]\\n\\nTomas"
}}
"""

    try:
        response = await groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You write brief, valuable follow-up emails. No sales fluff."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.8,
            max_tokens=200
        )

        content = response.choices[0].message.content.strip()

        # Parse JSON
        if '```json' in content:
            start = content.find('```json') + 7
            end = content.find('```', start)
            content = content[start:end].strip()
        elif '```' in content:
            start = content.find('```') + 3
            end = content.find('```', start)
            content = content[start:end].strip()

        import json
        data = json.loads(content)

        return {
            'subject_line': data.get('subject_line', 'Quick thought'),
            'email_body': data.get('email_body', '')
        }

    except Exception as e:
        print(f"    Error generating follow-up: {e}")
        # Fallback
        return {
            'subject_line': 'Quick thought on lead response',
            'email_body': f"""Hi {first_name},

Saw a stat recently: 78% of deals go to whoever responds first. Made me think of teams running FB ads.

How does your team handle after-hours inquiries?

Tomas"""
        }


async def draft_followups_for_batch(batch_file: str, batch_name: str) -> list:
    """Draft value-add follow-ups for a batch."""

    base_path = '/Users/tomas/Desktop/fb-ads-prospecting'
    df = pd.read_csv(f'{base_path}/{batch_file}')

    # Filter out bounces and do_not_contact
    df = df[~df['primary_email'].str.lower().isin([e.lower() for e in BOUNCED_EMAILS])]
    df = df[~df['primary_email'].str.lower().isin([e.lower() for e in DO_NOT_CONTACT])]
    df = df[df['send_status'] == 'sent']  # Only follow up on sent emails

    results = []

    print(f"\n{'=' * 70}")
    print(f"DRAFTING VALUE-ADD FOLLOW-UPS FOR {batch_name}")
    print(f"Eligible contacts: {len(df)}")
    print("=" * 70)

    for i, row in df.iterrows():
        contact = {
            'contact_name': row['contact_name'] if pd.notna(row['contact_name']) else '',
            'page_name': row['page_name'],
            'primary_email': row['primary_email']
        }

        original_email = {
            'hook_used': row['hook_used'] if pd.notna(row['hook_used']) else '',
            'subject_line': row['subject_line']
        }

        print(f"\n[{len(results)+1}] {contact['contact_name'] or 'Unknown'} @ {contact['page_name']}")

        result = await generate_value_followup(contact, original_email)

        print(f"    Subject: {result['subject_line']}")

        results.append({
            'page_name': contact['page_name'],
            'contact_name': contact['contact_name'],
            'primary_email': contact['primary_email'],
            'subject_line': result['subject_line'],
            'email_body': result['email_body'],
            'original_subject': original_email['subject_line'],
            'followup_type': 'value_add',
            'batch_source': batch_name,
            'draft_timestamp': datetime.now().isoformat()
        })

    return results


async def main():
    """Draft follow-ups for batches 2 and 3."""

    all_results = []

    # Batch 2
    batch2_results = await draft_followups_for_batch(
        'output/email_campaign/drafts_batch2_ready_to_send.csv',
        'Batch 2'
    )
    all_results.extend(batch2_results)

    # Batch 3
    batch3_results = await draft_followups_for_batch(
        'output/email_campaign/drafts_batch3.csv',
        'Batch 3'
    )
    all_results.extend(batch3_results)

    # Save results
    base_path = '/Users/tomas/Desktop/fb-ads-prospecting'
    output_df = pd.DataFrame(all_results)
    output_file = f'{base_path}/output/email_campaign/followup_value_batches_2_3.csv'
    output_df.to_csv(output_file, index=False)

    print(f"\n{'=' * 70}")
    print(f"SUMMARY")
    print(f"=" * 70)
    print(f"Batch 2 follow-ups: {len(batch2_results)}")
    print(f"Batch 3 follow-ups: {len(batch3_results)}")
    print(f"Total: {len(all_results)}")
    print(f"Saved to: {output_file}")
    print("=" * 70)

    return all_results


if __name__ == "__main__":
    asyncio.run(main())
