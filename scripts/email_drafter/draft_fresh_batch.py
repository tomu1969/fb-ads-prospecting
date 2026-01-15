"""Draft fresh batch of emails for uncontacted prospects."""

import asyncio
import pandas as pd
import sys
from datetime import datetime

sys.path.insert(0, '.')
from composer import compose_email


async def draft_fresh_batch():
    """Draft emails for uncontacted real estate prospects."""

    # Load master file
    base_path = '/Users/tomas/Desktop/fb-ads-prospecting'
    master = pd.read_csv(f'{base_path}/output/prospects_master.csv')

    # Load sent emails
    drafts_v2 = pd.read_csv(f'{base_path}/output/email_campaign/drafts_v2.csv')

    # Get emails that were sent
    sent_emails = set(drafts_v2['primary_email'].dropna().str.lower())

    # Find uncontacted prospects with valid emails
    master['email_lower'] = master['primary_email'].str.lower()
    uncontacted = master[~master['email_lower'].isin(sent_emails)]
    uncontacted = uncontacted[uncontacted['primary_email'].notna()]
    uncontacted = uncontacted[uncontacted['primary_email'].str.contains('@', na=False)]

    # Filter for real estate prospects
    keywords = ['real estate', 'realty', 'homes', 'properties', 'realtor',
                'christie', 'corcoran', 'remax', 're/max', 'sotheby']
    uncontacted['is_real_estate'] = uncontacted['page_name'].str.lower().apply(
        lambda x: any(kw in str(x).lower() for kw in keywords) if pd.notna(x) else False
    )

    prospects = uncontacted[uncontacted['is_real_estate']].head(5)

    results = []

    print("=" * 70)
    print("DRAFTING FRESH BATCH - NEW COMPOSER APPROACH")
    print("=" * 70)

    for i, row in prospects.iterrows():
        contact = {
            'contact_name': row['contact_name'] if pd.notna(row['contact_name']) else '',
            'page_name': row['page_name'],
            'primary_email': row['primary_email']
        }

        # Build hook from company_description
        description = str(row['company_description']) if pd.notna(row['company_description']) else ''

        # Determine hook type based on content
        hook_type = 'offer'  # Default
        if any(word in description.lower() for word in ['#1', 'top', 'leading', 'award', 'best']):
            hook_type = 'achievement'
        elif any(word in description.lower() for word in ['saved', 'helped', 'sold']):
            hook_type = 'milestone'

        hook = {
            'chosen_hook': description[:300] if description else f"Running ads on Facebook",
            'hook_source': 'website',
            'hook_type': hook_type
        }

        print(f"\n{'─' * 70}")
        print(f"Prospect: {contact['contact_name'] or 'Unknown'} @ {contact['page_name']}")
        print(f"Email: {contact['primary_email']}")
        print(f"Hook: {hook['chosen_hook'][:80]}...")
        print("─" * 70)

        # Generate email
        result = await compose_email(contact, hook)

        print(f"\nSubject: {result['subject_line']}")
        print("-" * 40)
        print(result['email_body'])
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
            'confidence_score': 85,
            'draft_timestamp': datetime.now().isoformat(),
            'approach': 'new_v2'
        })

    # Save results
    output_df = pd.DataFrame(results)
    output_file = f'{base_path}/output/email_campaign/drafts_fresh_batch.csv'
    output_df.to_csv(output_file, index=False)

    print(f"\n{'=' * 70}")
    print(f"SAVED: {output_file}")
    print(f"Total drafts: {len(results)}")
    print("=" * 70)

    return results


if __name__ == "__main__":
    asyncio.run(draft_fresh_batch())
