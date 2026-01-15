"""Draft test batch using new composer approach."""

import asyncio
import pandas as pd
import sys
from datetime import datetime

sys.path.insert(0, '.')
from composer import compose_email


async def draft_test_batch():
    """Draft emails for test prospects using new composer."""

    # Load original drafts to get prospect data
    df = pd.read_csv('output/email_campaign/drafts_v2.csv')

    # Filter to good prospects
    df = df[df['hook_type'].isin(['achievement', 'milestone', 'hiring'])]
    df = df[df['confidence_score'] >= 85]
    test_prospects = df.head(5)

    results = []

    print("=" * 70)
    print("DRAFTING TEST BATCH WITH NEW COMPOSER")
    print("=" * 70)

    for i, row in test_prospects.iterrows():
        contact = {
            'contact_name': row['contact_name'] if pd.notna(row['contact_name']) else '',
            'page_name': row['page_name'],
            'primary_email': row['primary_email']
        }

        hook = {
            'chosen_hook': row['hook_used'],
            'hook_source': row['hook_source'],
            'hook_type': row['hook_type']
        }

        print(f"\n{'─' * 70}")
        print(f"Prospect: {contact['contact_name'] or 'Unknown'} @ {contact['page_name']}")
        print(f"Email: {contact['primary_email']}")
        print(f"Hook: {hook['chosen_hook'][:60]}...")
        print("─" * 70)

        # Generate new email
        result = await compose_email(contact, hook)

        print(f"\n[NEW] Subject: {result['subject_line']}")
        print("-" * 40)
        print(result['email_body'])
        print("-" * 40)

        # Show old email for comparison
        print(f"\n[OLD] Subject: {row['subject_line']}")
        print("-" * 40)
        print(row['email_body'][:400] + "..." if len(row['email_body']) > 400 else row['email_body'])
        print("-" * 40)

        results.append({
            'page_name': contact['page_name'],
            'contact_name': contact['contact_name'],
            'primary_email': contact['primary_email'],
            'subject_line': result['subject_line'],
            'email_body': result['email_body'],
            'hook_used': result['hook_used'],
            'hook_source': result['hook_source'],
            'hook_type': hook['hook_type'],
            'confidence_score': 90,
            'draft_timestamp': datetime.now().isoformat(),
            'approach': 'new_v2'
        })

    # Save results
    output_df = pd.DataFrame(results)
    output_file = 'output/email_campaign/drafts_test_new_approach.csv'
    output_df.to_csv(output_file, index=False)

    print(f"\n{'=' * 70}")
    print(f"SAVED: {output_file}")
    print(f"Total drafts: {len(results)}")
    print("=" * 70)

    return results


if __name__ == "__main__":
    asyncio.run(draft_test_batch())
