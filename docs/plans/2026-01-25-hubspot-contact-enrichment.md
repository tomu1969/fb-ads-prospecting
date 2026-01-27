# HubSpot Contact Enrichment Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enrich 278 incomplete contacts in `output/hubspot_import_enriched.csv` with email and phone from Repliers MLS data.

**Architecture:** Two-phase enrichment: (1) instant local matching against existing `all_agents_2025.csv` database for 37 contacts, (2) API lookups via Repliers for remaining 241 contacts using city-aware search.

**Tech Stack:** Python, pandas, Repliers MLS API

---

## Summary

| Phase | Contacts | Method | Cost |
|-------|----------|--------|------|
| 1. Local Match | 37 | Match against all_agents_2025.csv | Free, instant |
| 2. API Lookup | 241 | Repliers API search by name+city | ~$0 (free API) |

**Expected Outcome:** ~200+ contacts enriched (assuming 80% hit rate on API lookups)

---

### Task 1: Create Local Enrichment Script

**Files:**
- Create: `scripts/hubspot_local_enricher.py`

**Step 1: Write the script**

```python
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
```

**Step 2: Run the local enrichment**

Run: `python scripts/hubspot_local_enricher.py`
Expected: ~37 contacts enriched from local database

**Step 3: Commit**

```bash
git add scripts/hubspot_local_enricher.py
git commit -m "feat: add local enricher for HubSpot contacts from agents DB"
```

---

### Task 2: Update repliers_agent_lookup.py for Multi-City Support

**Files:**
- Modify: `scripts/repliers_agent_lookup.py`

**Step 1: Update the script to use contact's city**

Change the main loop (around line 204) to pass the contact's city:

```python
# In the main loop, around line 213:
city = str(row.get('City', 'Miami')).strip()
if not city or city == 'nan':
    city = 'Miami'

result = search_agent_by_name(full_name, city)
```

**Step 2: Test with a few contacts**

Run: `python scripts/repliers_agent_lookup.py --input output/hubspot_import_enriched.csv --limit 5`
Expected: Should search each contact's actual city

**Step 3: Commit**

```bash
git add scripts/repliers_agent_lookup.py
git commit -m "fix: use contact's city for Repliers lookup instead of defaulting to Miami"
```

---

### Task 3: Run Full API Enrichment

**Step 1: Run the API lookup for all remaining contacts**

Run: `python scripts/repliers_agent_lookup.py --input output/hubspot_import_enriched.csv --all`

Expected runtime: ~5-10 min for 241 contacts (0.3s delay per API call)
Expected: 150-200 contacts enriched (60-80% hit rate typical)

**Step 2: Check results**

Run:
```python
python3 -c "
import pandas as pd
df = pd.read_csv('output/hubspot_import_enriched.csv')
no_email = df['Email'].isna() | (df['Email'] == '')
no_phone = df['Phone Number'].isna() | (df['Phone Number'] == '')
print(f'Missing email: {no_email.sum()}')
print(f'Missing phone: {no_phone.sum()}')
print(f'Missing both: {(no_email & no_phone).sum()}')
"
```

---

### Task 4: Analyze Remaining Gaps

**Step 1: Export list of still-incomplete contacts for manual review**

```python
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('output/hubspot_import_enriched.csv')
df['no_email'] = df['Email'].isna() | (df['Email'] == '')
df['no_phone'] = df['Phone Number'].isna() | (df['Phone Number'] == '')
incomplete = df[df['no_email'] | df['no_phone']]
incomplete[['First Name', 'Last Name', 'Company Name', 'City', 'LinkedIn URL']].to_csv(
    'output/contacts_still_incomplete.csv', index=False
)
print(f"Exported {len(incomplete)} still-incomplete contacts to output/contacts_still_incomplete.csv")
EOF
```

**Step 2: Decide on fallback enrichment**

Options for remaining contacts:
- Manual lookup via LinkedIn URLs (71% have LinkedIn)
- Hunter.io email finder
- Apollo.io B2B database
- Mark as "unreachable" and deprioritize

---

## Execution Order

1. **Task 1** - Local enrichment (instant, ~37 contacts)
2. **Task 2** - Fix city support in API script
3. **Task 3** - API enrichment (~241 contacts, ~10 min)
4. **Task 4** - Analyze gaps and decide next steps
