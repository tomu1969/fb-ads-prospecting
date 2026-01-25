#!/usr/bin/env python3
"""
Enrich HubSpot contacts by matching against existing all_agents database.
No API calls - instant matching.
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
HUBSPOT_CSV = BASE_DIR / "output/hubspot_import_enriched.csv"
AGENTS_CSV = BASE_DIR / "output/repliers/all_agents_2025.csv"


def normalize_name(first, last):
    """Normalize name for matching."""
    first = str(first).strip().lower() if pd.notna(first) else ''
    last = str(last).strip().lower() if pd.notna(last) else ''
    return f"{first} {last}".strip()


def main():
    print(f"\n{'='*60}")
    print("HUBSPOT LOCAL ENRICHER")
    print(f"{'='*60}")

    # Load data
    hubspot = pd.read_csv(HUBSPOT_CSV)
    agents = pd.read_csv(AGENTS_CSV)

    print(f"Loaded {len(hubspot)} HubSpot contacts")
    print(f"Loaded {len(agents)} agents from database")

    # Create lookup dict from agents
    agents['match_name'] = agents['agent_name'].str.lower().str.strip()
    agent_lookup = agents.set_index('match_name')[['agent_email', 'agent_phone']].to_dict('index')

    # Track stats
    stats = {'enriched_email': 0, 'enriched_phone': 0, 'already_complete': 0}

    for idx, row in hubspot.iterrows():
        # Skip if already has both
        has_email = pd.notna(row['Email']) and row['Email'] != ''
        has_phone = pd.notna(row['Phone Number']) and row['Phone Number'] != ''

        if has_email and has_phone:
            stats['already_complete'] += 1
            continue

        # Try to match
        match_name = normalize_name(row['First Name'], row['Last Name'])
        if match_name in agent_lookup:
            agent_data = agent_lookup[match_name]

            if not has_email and agent_data.get('agent_email'):
                hubspot.at[idx, 'Email'] = agent_data['agent_email']
                hubspot.at[idx, 'Email Source'] = 'repliers_db'
                stats['enriched_email'] += 1
                logger.info(f"Enriched email: {match_name} -> {agent_data['agent_email']}")

            if not has_phone and agent_data.get('agent_phone'):
                hubspot.at[idx, 'Phone Number'] = agent_data['agent_phone']
                stats['enriched_phone'] += 1
                logger.info(f"Enriched phone: {match_name} -> {agent_data['agent_phone']}")

    # Save
    hubspot.to_csv(HUBSPOT_CSV, index=False)
    hubspot.to_excel(str(HUBSPOT_CSV).replace('.csv', '.xlsx'), index=False)

    # Summary
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"  Already complete: {stats['already_complete']}")
    print(f"  Emails enriched: {stats['enriched_email']}")
    print(f"  Phones enriched: {stats['enriched_phone']}")

    # Final counts
    final = pd.read_csv(HUBSPOT_CSV)
    with_email = len(final[final['Email'].notna() & (final['Email'] != '')])
    with_phone = len(final[final['Phone Number'].notna() & (final['Phone Number'] != '')])
    print(f"\nFinal totals:")
    print(f"  With email: {with_email}/{len(final)}")
    print(f"  With phone: {with_phone}/{len(final)}")


if __name__ == '__main__':
    main()
