# Phase 3: LLM Enrichment Design

**Date:** 2026-01-26
**Status:** Design Approved
**Goal:** Extract company, role, and topics from emails using Groq API

---

## Overview

Enrich the contact graph with structured data extracted from email content via LLM. Prioritize high-value external contacts within a $50 budget (~2,500 contacts).

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Body fetching | On-demand | Avoid re-syncing 141k emails |
| Emails per contact | Last 3 | Balance accuracy vs cost |
| Extraction scope | Company + Role + Topics | Full value in one pass |
| Storage | SQLite cache + Neo4j | Resumable, debuggable |
| Budget | $50 (~2,500 contacts) | Prioritize high-value contacts |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 3: LLM ENRICHMENT                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  emails.db ──► entity_extractor.py ──► extractions.db ──► Neo4j│
│     │              │                        │                   │
│     │         [Groq API]                    │                   │
│     │         Llama 3.3 70B                 │                   │
│     │              │                        │                   │
│     └──► fetch body on-demand              │                   │
│                    │                        │                   │
│              ┌─────┴─────┐                  │                   │
│              │ Extract:  │                  │                   │
│              │ • Company │──────────────────┘                   │
│              │ • Role    │                                      │
│              │ • Topics  │                                      │
│              └───────────┘                                      │
│                                                                 │
│  Neo4j additions:                                               │
│  • Company nodes                                                │
│  • Topic nodes                                                  │
│  • WORKS_AT edges (Person → Company)                           │
│  • DISCUSSED edges (Person → Topic)                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Model

### SQLite `extractions.db` Schema

```sql
-- Raw extraction results (one per contact)
CREATE TABLE contact_extractions (
    email TEXT PRIMARY KEY,           -- Contact's email (unique key)
    name TEXT,                        -- Contact's name
    company TEXT,                     -- Extracted company name
    role TEXT,                        -- Extracted job title
    topics TEXT,                      -- JSON array of topics
    confidence REAL,                  -- 0-1 extraction confidence
    source_emails TEXT,               -- JSON array of message_ids used
    extracted_at TIMESTAMP,
    model TEXT                        -- e.g. "llama-3.3-70b-versatile"
);

-- Track which emails we've fetched bodies for
CREATE TABLE email_bodies (
    message_id TEXT PRIMARY KEY,
    body TEXT,
    fetched_at TIMESTAMP
);

-- Budget tracking
CREATE TABLE extraction_runs (
    id INTEGER PRIMARY KEY,
    started_at TIMESTAMP,
    tokens_used INTEGER DEFAULT 0,
    cost_usd REAL DEFAULT 0,
    contacts_processed INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'     -- running, completed, paused
);
```

### Neo4j Additions

```cypher
-- Company node
(:Company {name, domain, normalized_name})

-- Topic node
(:Topic {name, category})

-- New edges
(:Person)-[:WORKS_AT {role, confidence, extracted_at}]->(:Company)
(:Person)-[:DISCUSSED {frequency, last_mentioned}]->(:Topic)
```

---

## Contact Prioritization

### Priority Tiers (process in order until budget exhausted)

| Tier | Criteria | Est. Count |
|------|----------|------------|
| **1 - Target Industry** | External + real estate/finance/tech domains + replied at least once | ~500-800 |
| **2 - Active External** | External + 3+ emails exchanged | ~1,500 |
| **3 - Any Replied** | External + they replied at least once | ~2,000 |
| **Skip** | Internal domains, noreply/automated, one-way outbound | ~10k+ |

### Skip Criteria

- **Internal domains:** jaguarcapital.co, lahaus.com, nuestro.co, etc.
- **Automated emails:** noreply@, notifications@, mailer-daemon@
- **One-way outbound:** Emails I sent that never received a reply

### Target Industry Keywords

```python
TARGET_KEYWORDS = {
    'real_estate': [
        'jll', 'cbre', 'compass', 'colliers', 'cushman', 'cushwake',
        'remax', 'century21', 'coldwellbanker', 'sothebys', 'zillow',
        'realty', 'properties', 'inmobiliaria', 'finca', 'estate',
        'broker', 'realtor', 'agent', 'officer',
    ],

    'finance': [
        'bank', 'capital', 'invest', 'fund', 'asset', 'wealth',
        'partners', 'ventures', 'vc', 'equity', 'holdings',
        'management', 'advisors', 'advisory', 'securities',
        'goldman', 'morgan', 'jpmorgan', 'blackstone', 'kkr',
        'sequoia', 'a16z', 'accel', 'kaszek', 'softbank',
    ],

    'tech': [
        # Big tech
        'google', 'microsoft', 'amazon', 'meta', 'apple', 'nvidia',
        'salesforce', 'oracle', 'ibm', 'adobe', 'stripe', 'openai',
        # Generic keywords
        'tech', 'software', 'app', 'ai', 'data', 'cloud', 'saas',
        'digital', 'labs', 'io', 'dev', 'engineering', 'platform',
    ],
}
```

---

## Extraction Pipeline

### Flow per Contact

1. Get contact's last 3 emails from emails.db (by from_email or to_emails)
2. For each email, fetch body via Gmail API (cache in email_bodies table)
3. Build prompt with email headers + bodies
4. Call Groq API (Llama 3.3 70B) → JSON response
5. Parse and validate response
6. Save to contact_extractions table
7. **Partial save after each contact** (crash-safe)
8. After all contacts: sync to Neo4j

### Batching & Saves

- Process contacts one at a time
- **Save to SQLite immediately after each extraction** (crash-safe)
- Update budget tracking after each API call
- Log progress every 10 contacts

### Rate Limiting

- Groq free tier: 30 requests/min
- Add 2-second delay between requests
- Exponential backoff on rate limit errors

### Resume Logic

```python
# Get already-extracted contacts
extracted = SELECT email FROM contact_extractions WHERE extracted_at IS NOT NULL

# Get current budget spent
spent = SELECT SUM(cost_usd) FROM extraction_runs WHERE status = 'running'

# Skip extracted, stop if budget exceeded
```

---

## Groq Prompt Design

**Model:** `llama-3.3-70b-versatile`
- $0.59/1M input tokens, $0.79/1M output tokens
- ~500 tokens/email avg → ~$0.02 per contact

**System prompt:**
```
You extract structured contact information from emails. Return valid JSON only.
```

**User prompt template:**
```
Extract the sender's professional information from these emails.

Contact: {name} <{email}>

--- Email 1 ---
Subject: {subject}
Date: {date}
Body:
{body}

--- Email 2 ---
...

Return JSON:
{
  "company": "Company name or null",
  "role": "Job title or null",
  "topics": ["topic1", "topic2"],
  "confidence": 0.0-1.0
}

Rules:
- Extract company/role from email signature first
- If no signature, infer role from email content and context
- Topics: 1-3 main professional topics discussed
- Confidence: 0.8+ if signature found, 0.5-0.8 if inferred from content
```

---

## File Structure

```
scripts/contact_intel/
├── entity_extractor.py      # Main extraction pipeline
├── groq_client.py           # Groq API wrapper with rate limiting
└── extraction_sync.py       # Sync extractions.db → Neo4j

data/contact_intel/
└── extractions.db           # SQLite cache (auto-created)

config/contact_intel/
└── target_industries.json   # Editable keyword list
```

---

## CLI Interface

```bash
# Show how many contacts will be processed per tier
python scripts/contact_intel/entity_extractor.py --status

# Run extraction (processes by priority tier until $50 budget)
python scripts/contact_intel/entity_extractor.py --budget 50

# Resume interrupted run
python scripts/contact_intel/entity_extractor.py --resume

# Sync extractions to Neo4j
python scripts/contact_intel/entity_extractor.py --sync

# Test with single contact
python scripts/contact_intel/entity_extractor.py --email "chad@example.com"
```

---

## Budget Tracking

- Track tokens used per request (input + output)
- Calculate cost: `(input_tokens * 0.59 + output_tokens * 0.79) / 1_000_000`
- Log running total after each extraction
- Stop when `spent + estimated_next > budget`
- Final report: contacts processed, cost, tokens used

**Estimated:** ~2,500 contacts at ~$0.02 each = ~$50

---

## Success Criteria

1. **Coverage:** Extract company/role for 70%+ of processed contacts
2. **Accuracy:** 80%+ confidence scores on average
3. **Budget:** Stay within $50
4. **Resumable:** Can stop and resume without reprocessing
5. **Neo4j:** WORKS_AT and DISCUSSED edges populated

---

## Implementation Tasks

```
□ Create extractions.db schema
□ Create groq_client.py with rate limiting
□ Create contact prioritization query
□ Implement body fetching (Gmail API, cached)
□ Implement extraction loop with partial saves
□ Implement budget tracking
□ Implement --status, --resume, --sync CLI
□ Create extraction_sync.py for Neo4j
□ Test with 10 contacts
□ Run full extraction
```
