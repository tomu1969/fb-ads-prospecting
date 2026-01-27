# Gmail Contact Intelligence System

**Date:** 2025-01-25
**Status:** Phase 1-4 Complete, V2 Scoring Implemented
**Goal:** Build a contact intelligence layer from Gmail + LinkedIn to find warm intro paths to prospects
**Last Updated:** 2026-01-27

---

## Current Status (2026-01-27)

### What's Working

| Component | Status | Notes |
|-----------|--------|-------|
| **Gmail Sync** | ✅ Done | 140k+ emails synced to SQLite |
| **Neo4j Graph** | ✅ Done | Running in Docker, 11,766 KNOWS edges |
| **Basic Graph Build** | ✅ Done | Person nodes, KNOWS relationships |
| **CC_TOGETHER Edges** | ✅ Done | Detects shared email circles |
| **WORKS_AT Edges** | ✅ Done | Domain-inferred + industry classified |
| **Topic Extraction** | ✅ Done | DISCUSSED edges from email subjects |
| **LinkedIn Integration** | ✅ Done | LINKEDIN_CONNECTED edges from export |
| **V2 Relationship Scoring** | ✅ Done | Penalizes newsletters/group emails |
| **Natural Language Query** | ✅ Done | `graph_query.py --interactive` |
| **Overnight Enrichment** | ✅ Done | Industry classification, domain linking |

### Graph Statistics

```
Nodes:
- Person: ~15,000
- Company: ~5,000 (3,029 domain-inferred)
- Topic: ~2,000

Edges:
- KNOWS: 11,766 (with strength_score_v2)
- WORKS_AT: ~7,000
- CC_TOGETHER: ~3,000
- DISCUSSED: ~8,000
- LINKEDIN_CONNECTED: pending LinkedIn export
```

### Key Files

```
scripts/contact_intel/
├── gmail_sync.py              # ✅ Email sync to SQLite
├── graph_builder.py           # ✅ Build Neo4j graph
├── graph_query.py             # ✅ Natural language queries (GPT-4o-mini → Cypher)
├── linkedin_sync.py           # ✅ Import LinkedIn connections CSV
├── relationship_strength.py   # ✅ V1 scoring (deprecated)
├── relationship_strength_v2.py # ✅ V2 scoring (current)
├── industry_classifier.py     # ✅ GPT-4o-mini industry classification
├── domain_company_linker.py   # ✅ Free company inference from email domain
├── contact_gap_filler.py      # ⏸️ Apollo API enrichment (costly, optional)
├── overnight_enrichment.py    # ✅ Orchestrator for all enrichment tasks
└── incremental_graph_builder.py # ✅ Build graph incrementally

data/contact_intel/
├── emails.db                  # SQLite with 140k+ emails
├── sync_state.json            # Last sync timestamp
└── tujaguarcapital_token.json # OAuth token
```

### Quick Commands

```bash
# Query the graph (natural language)
python -m scripts.contact_intel.graph_query "who do I know at Google"
python -m scripts.contact_intel.graph_query "my strongest contacts"
python -m scripts.contact_intel.graph_query --interactive

# Run overnight enrichment
python -m scripts.contact_intel.overnight_enrichment --status
python -m scripts.contact_intel.overnight_enrichment --run

# Check relationship scores
python -m scripts.contact_intel.relationship_strength_v2 --status
python -m scripts.contact_intel.relationship_strength_v2 --test "email@example.com"
```

### V2 Scoring Formula

The new scoring formula (0-100) considers:
- **Volume** (0-35): Logarithmic scaling of total emails
- **Recency** (0-25): Exponential decay over 365 days
- **Reciprocity** (0-25): Balanced send/receive ratio
- **Reply Rate** (0-15): How often they reply to your emails
- **Group Penalty** (0.5x-1.0x): Penalizes mass emails (>5 recipients)
- **Newsletter Penalty** (0.7x): Detects one-way subscriptions

Score interpretation:
- 70-100: Strong relationship
- 40-69: Medium relationship
- 10-39: Weak relationship
- 0-9: Minimal (newsletter/one-off)

### Next Steps

1. **LinkedIn Integration** - Import LinkedIn connections CSV to add LINKEDIN_CONNECTED edges
2. **Warm Intro Paths** - Implement `find_paths.py` for batch processing prospects
3. **Semantic Search** - Add ChromaDB for "who do I know in fintech" queries
4. **LLM Enrichment** - Use Groq to extract company/role from email signatures

### To Resume Work

```bash
# Start Neo4j
docker start neo4j

# Check graph status
python -m scripts.contact_intel.overnight_enrichment --status

# Interactive query
python -m scripts.contact_intel.graph_query --interactive
```

---

## Overview

Transform 12,000+ contacts and full email history (multiple Gmail accounts) into a queryable knowledge graph that identifies the best entry path to any prospect.

### Primary Use Case

```
Input:  prospects_master.csv (target contacts to reach)
Output: entry_paths.csv (with intro chains, connectors, warmth scores, suggested openers)
```

### Data Sources

| Source | Access Method | Data |
|--------|---------------|------|
| Gmail (LaHaus) | IMAP + App Password | Work emails, professional network |
| Gmail (Jaguar Capital) | OAuth 2.0 (Gmail API) | 140k+ messages, personal/business network |
| LinkedIn | API/Scraping | Connections, mutual connections, degrees |
| Google Contacts | OAuth 2.0 (People API) | 12k contacts with basic metadata |

**Note:** Google Workspace accounts with app passwords disabled use OAuth 2.0.
Credentials stored in `data/contact_intel/{account}_token.json`.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├────────────────────┬────────────────────┬───────────────────────┤
│   Gmail (IMAP)     │  LinkedIn (API)    │  Google Contacts      │
│   - Full history   │  - Connections     │  - 12k contacts       │
│   - Multi-account  │  - Mutual conns    │  - Basic metadata     │
└────────┬───────────┴─────────┬──────────┴───────────┬───────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER                              │
│   - Email parsing (headers, threads, frequency)                 │
│   - LLM extraction via Groq (entities, topics, intros)          │
│   - Relationship detection (CC patterns, intro phrases)         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                                 │
├────────────────────────────┬────────────────────────────────────┤
│   Neo4j (Graph DB)         │   ChromaDB (Vector DB)             │
│   - Relationship graph     │   - Semantic search                │
│   - Path finding           │   - "Who in fintech?"              │
│   - Intro chains           │   - Topic similarity               │
└────────────────────────────┴────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    QUERY INTERFACE                               │
│   CLI: python scripts/contact_intel/find_paths.py               │
│   Input: prospects_master.csv                                   │
│   Output: entry_paths.csv (with intro chains, warmth scores)    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Model

### Entities (Nodes)

| Entity Type | Attributes | Source |
|-------------|------------|--------|
| **Person** | name, emails[], linkedin_url, company, role, last_contact, contact_strength | Gmail, LinkedIn, Google Contacts |
| **Company** | name, domain, industry, size | Email domains, LinkedIn |
| **Topic** | name, category | LLM-extracted from email content |
| **EmailThread** | subject, date_range, participant_count | Gmail |

### Relationships (Edges)

| Relationship | Between | Attributes |
|--------------|---------|------------|
| **KNOWS** | Person → Person | strength (1-10), first_contact, last_contact, email_count, introduced_by |
| **WORKS_AT** | Person → Company | role, start_date, is_current |
| **DISCUSSED** | Person → Topic | frequency, recency, sentiment |
| **INTRODUCED** | Person → Person | date, context, via_person |
| **CC_TOGETHER** | Person → Person | count (proxy for "in same circles") |
| **LINKEDIN_CONNECTED** | Person → Person | degree, mutual_count |

---

## Module Structure

```
scripts/contact_intel/
├── __init__.py
├── README.md
│
├── # ─── DATA INGESTION (Module A) ───
├── gmail_sync.py                  # IMAP sync for multiple accounts
├── contacts_import.py             # Google Contacts API import
├── linkedin_sync.py               # LinkedIn connections sync
│
├── # ─── EXTRACTION (Module B) ───
├── extractor.py                   # Orchestrates extraction pipeline
├── email_parser.py                # Parse email headers, threads
├── entity_extractor.py            # LLM-powered entity extraction (Groq)
├── relationship_extractor.py      # Detect intros, CC patterns, frequency
├── topic_extractor.py             # Extract topics from email content
│
├── # ─── GRAPH OPERATIONS (Module C) ───
├── graph_builder.py               # Build/update Neo4j graph
├── graph_queries.py               # Cypher query library
├── path_finder.py                 # Find intro paths for prospects
│
├── # ─── QUERY INTERFACE (Module D) ───
├── find_paths.py                  # Main CLI entry point
├── semantic_search.py             # ChromaDB vector search
│
├── # ─── UTILITIES (Module E) ───
├── config.py                      # Multi-account config, API keys
├── models.py                      # Pydantic models (Person, Company, etc.)
└── deduper.py                     # Contact deduplication logic

config/contact_intel/
├── gmail_accounts.json            # Multi-account credentials reference
├── entity_prompts.json            # LLM extraction prompts
└── relationship_weights.json      # Scoring weights for path ranking

data/contact_intel/
├── emails.db                      # SQLite - raw email cache
├── graph.db                       # Neo4j data
├── vectors/                       # ChromaDB persistence
└── sync_state.json                # Last sync timestamps per account
```

---

## LLM Strategy

**Provider:** Groq (existing API key in .env)

| Task | Model | Cost Estimate |
|------|-------|---------------|
| Entity extraction | Llama 3.3 70B | $0.59/1M input |
| Simple parsing | Llama 3.1 8B | $0.05/1M input |
| Opener generation | Llama 3.3 70B | $0.79/1M output |

**Estimated Total Cost:** ~$10-50 for full history processing

**Optimizations:**
- Batch API (50% discount)
- Prompt caching (50% on repeated prefixes)
- Sample emails per contact (not all emails)

---

## Implementation Phases

Each phase is independently testable and delivers working functionality.

---

### Phase 1: Foundation

**Goal:** Sync emails → Build basic graph → Query one prospect

**Modules:** A1 (gmail_sync), C1 (graph_builder), D1 (find_paths basic)

**Can Run in Parallel:**
- Agent 1: `gmail_sync.py` + tests
- Agent 2: `graph_builder.py` + tests
- Agent 3: `models.py` + `config.py`

**Tasks:**

```
□ P1.1 Set up Neo4j locally (Docker)
□ P1.2 Create models.py - Pydantic models for Person, Email, Relationship
□ P1.3 Create config.py - Multi-account Gmail config loader
□ P1.4 Create gmail_sync.py - IMAP fetch for multiple accounts
□ P1.5 Create email_parser.py - Extract From/To/CC/Date/Subject
□ P1.6 Create graph_builder.py - Build Person nodes + KNOWS edges
□ P1.7 Create find_paths.py - Basic CLI for direct connection lookup
□ P1.8 Integration test: full flow from sync to query
```

**Tests (TDD - write first):**

```python
# tests/contact_intel/test_gmail_sync.py
def test_connect_to_gmail_with_app_password():
    """Should connect to Gmail via IMAP using app password."""

def test_fetch_emails_since_date():
    """Should fetch emails from specified date range."""

def test_handles_multiple_accounts():
    """Should sync from multiple Gmail accounts sequentially."""

def test_incremental_sync_only_new_emails():
    """Should only fetch emails newer than last sync timestamp."""

# tests/contact_intel/test_email_parser.py
def test_extract_sender_recipient():
    """Should extract From, To, CC, BCC from email headers."""

def test_extract_thread_id():
    """Should group emails by thread/conversation."""

def test_handles_encoded_headers():
    """Should decode MIME-encoded headers (UTF-8, etc)."""

# tests/contact_intel/test_graph_builder.py
def test_create_person_node():
    """Should create Person node with email as unique key."""

def test_create_knows_relationship():
    """Should create KNOWS edge between two people who emailed."""

def test_update_relationship_strength():
    """Should increment email_count on existing relationship."""

def test_dedupe_person_by_email():
    """Should merge Person nodes with same email address."""
```

**Deliverable:**
```bash
python scripts/contact_intel/find_paths.py --query "chad@example.com"
# → "You emailed Chad 12 times, last contact: 2024-11-15"
```

---

### Phase 2: Intro Paths

**Goal:** Find one-hop introductions (friend of friend)

**Modules:** B1 (relationship_extractor), C2 (graph_queries)

**Can Run in Parallel:**
- Agent 1: `relationship_extractor.py` - CC pattern detection
- Agent 2: `graph_queries.py` - Cypher query library
- Agent 3: Path scoring algorithm

**Tasks:**

```
□ P2.1 Add CC_TOGETHER relationship detection
□ P2.2 Add INTRODUCED relationship (detect intro patterns in body)
□ P2.3 Create graph_queries.py - Cypher query library
□ P2.4 Implement one-hop path query
□ P2.5 Add connector strength scoring (frequency × recency)
□ P2.6 Update find_paths.py with intro path output
```

**Tests (TDD):**

```python
# tests/contact_intel/test_relationship_extractor.py
def test_detect_cc_together():
    """Should create CC_TOGETHER edge when two people CC'd on same email."""

def test_detect_intro_phrase():
    """Should detect 'I'd like to introduce you to...' patterns."""

def test_detect_cc_introduction():
    """Should detect intro when A emails B and CCs C for first time."""

# tests/contact_intel/test_graph_queries.py
def test_find_direct_connection():
    """Should find direct KNOWS relationship."""

def test_find_one_hop_path():
    """Should find friend-of-friend path."""

def test_rank_connectors_by_strength():
    """Should rank multiple connectors by relationship strength."""
```

**Intro Detection Patterns:**

```python
INTRO_PATTERNS = [
    r"i'd like to introduce you to",
    r"i want to introduce",
    r"connecting you with",
    r"you should meet",
    r"i think you two should connect",
    r"meet my friend",
    r"putting you in touch with",
    r"looping in",
]
```

**Deliverable:**
```bash
python scripts/contact_intel/find_paths.py --query "chad@example.com"
# → "Ask Sarah (sarah@co.com) - she knows Chad, you emailed her 45 times"
```

---

### Phase 3: LLM Enrichment

**Goal:** Extract topics, context, and relationship quality

**Modules:** B2 (entity_extractor), B3 (topic_extractor)

**Can Run in Parallel:**
- Agent 1: `entity_extractor.py` - Company/Role extraction
- Agent 2: `topic_extractor.py` - Topic extraction
- Agent 3: Prompt engineering + testing

**Tasks:**

```
□ P3.1 Create entity_extractor.py - Groq Llama 3.3 70B integration
□ P3.2 Design extraction prompts (config/entity_prompts.json)
□ P3.3 Extract Company + Role from email signatures
□ P3.4 Create topic_extractor.py - Extract discussion topics
□ P3.5 Add DISCUSSED edges (Person → Topic)
□ P3.6 Add WORKS_AT edges (Person → Company)
□ P3.7 Generate suggested_opener based on shared context
□ P3.8 Implement batch processing with checkpoints
```

**Tests (TDD):**

```python
# tests/contact_intel/test_entity_extractor.py
def test_extract_company_from_signature():
    """Should extract company name from email signature."""

def test_extract_role_from_signature():
    """Should extract job title from email signature."""

def test_batch_extraction_with_checkpoint():
    """Should save progress every N emails for resumability."""

# tests/contact_intel/test_topic_extractor.py
def test_extract_topics_from_email():
    """Should extract main topics discussed in email."""

def test_categorize_topic():
    """Should categorize topic (real estate, finance, etc)."""
```

**Extraction Prompt (Groq):**

```json
{
  "entity_extraction": {
    "model": "llama-3.3-70b-versatile",
    "system": "Extract structured data from email. Return JSON only.",
    "user_template": "Email:\nFrom: {from}\nTo: {to}\nSubject: {subject}\nBody: {body}\n\nExtract:\n- sender_company\n- sender_role\n- topics (list of 1-3 main topics)\n- is_introduction (boolean)\n- introduced_person (if is_introduction)"
  }
}
```

**Deliverable:**
```bash
python scripts/contact_intel/find_paths.py --query "chad@example.com"
# → "Ask Sarah - you both discussed 'Miami luxury real estate'.
#    Suggested opener: 'Sarah mentioned you're focused on Fisher Island...'"
```

---

### Phase 4: LinkedIn Integration

**Goal:** Enrich with LinkedIn connections + mutual connections

**Modules:** A2 (linkedin_sync)

**Dependencies:** Existing `scripts/linkedin_enricher.py`

**Tasks:**

```
□ P4.1 Create linkedin_sync.py - Import LinkedIn connections
□ P4.2 Add LINKEDIN_CONNECTED edges with degree + mutual_count
□ P4.3 Merge LinkedIn profiles with email-derived Person nodes
□ P4.4 Update path scoring to weight LinkedIn mutuals
□ P4.5 Add mutual connection names to output
```

**Tests (TDD):**

```python
# tests/contact_intel/test_linkedin_sync.py
def test_import_linkedin_connections():
    """Should create LINKEDIN_CONNECTED edges from connections data."""

def test_merge_linkedin_with_email_person():
    """Should merge LinkedIn profile with existing Person node by email."""

def test_fetch_mutual_connections():
    """Should fetch and store mutual connection count."""
```

**Deliverable:**
```bash
python scripts/contact_intel/find_paths.py --query "chad@example.com"
# → "3 mutual LinkedIn connections. Strongest: Sarah (47 mutuals, 2nd degree)"
```

---

### Phase 5: Semantic Search + Batch Processing

**Goal:** Natural language queries + process full prospect lists

**Modules:** D2 (semantic_search), D3 (batch mode)

**Can Run in Parallel:**
- Agent 1: `semantic_search.py` - ChromaDB integration
- Agent 2: Batch CSV processing mode
- Agent 3: Opener generation pipeline

**Tasks:**

```
□ P5.1 Set up ChromaDB for contact embeddings
□ P5.2 Create semantic_search.py - "who do I know in fintech"
□ P5.3 Add batch mode to find_paths.py (CSV input → CSV output)
□ P5.4 Add suggested_opener generation for each prospect
□ P5.5 Add progress logging and incremental save
□ P5.6 Add --limit and --resume flags
```

**Tests (TDD):**

```python
# tests/contact_intel/test_semantic_search.py
def test_search_by_industry():
    """Should find contacts in specified industry."""

def test_search_by_topic():
    """Should find contacts who discussed specific topic."""

# tests/contact_intel/test_batch_processing.py
def test_process_csv_input():
    """Should read prospects from CSV and find paths for each."""

def test_incremental_save():
    """Should save progress after each batch."""

def test_resume_from_checkpoint():
    """Should resume from last processed row."""
```

**Deliverable:**
```bash
python scripts/contact_intel/find_paths.py --input prospects_master.csv --output entry_paths.csv
# → Processes 200 prospects, outputs entry_paths.csv with all columns
```

---

### Phase 6: Maintenance + Operations

**Goal:** Keep graph fresh, handle duplicates, production hardening

**Modules:** E1 (deduper), A3 (incremental sync)

**Tasks:**

```
□ P6.1 Add incremental sync (only new emails since last run)
□ P6.2 Add relationship decay (reduce strength over time)
□ P6.3 Create deduper.py - Merge duplicate contacts
□ P6.4 Add sync status dashboard
□ P6.5 Add cron job configuration for daily/weekly sync
□ P6.6 Add data export/backup utilities
```

---

## CLI Reference

```bash
# ─── SYNC COMMANDS ───
python scripts/contact_intel/gmail_sync.py --account all          # Sync all accounts
python scripts/contact_intel/gmail_sync.py --account personal     # Sync specific account
python scripts/contact_intel/gmail_sync.py --since 2024-01-01     # Sync from date
python scripts/contact_intel/linkedin_sync.py                      # Sync LinkedIn

# ─── QUERY COMMANDS ───
python scripts/contact_intel/find_paths.py --query "chad@example.com"
python scripts/contact_intel/find_paths.py --query "who can intro me to Chad Carroll"
python scripts/contact_intel/find_paths.py --search "real estate investors Miami"
python scripts/contact_intel/find_paths.py --input prospects.csv --output paths.csv

# ─── MAINTENANCE ───
python scripts/contact_intel/find_paths.py --stats                # Graph statistics
python scripts/contact_intel/deduper.py --dry-run                 # Preview deduplication
python scripts/contact_intel/deduper.py --execute                 # Run deduplication
```

---

## Output Schema

**entry_paths.csv columns:**

| Column | Type | Description |
|--------|------|-------------|
| prospect_name | str | From input CSV |
| prospect_email | str | From input CSV |
| prospect_company | str | From input CSV |
| path_type | enum | direct, one_hop, company_connection, topic_affinity, linkedin_mutual, cold |
| path_strength | int | 1-100 score |
| connector_name | str | Person who can make intro |
| connector_email | str | How to reach connector |
| connector_strength | int | 1-10 your relationship strength |
| last_contact_date | date | When you last emailed connector |
| shared_topics | str | Topics you discussed with connector |
| mutual_connections | int | LinkedIn mutual count |
| suggested_opener | str | LLM-generated intro request |

---

## Environment Variables

Add to `.env`:

```bash
# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_secure_password

# ChromaDB (optional - defaults to local)
CHROMA_PERSIST_DIR=data/contact_intel/vectors
```

## Authentication Setup

**OAuth 2.0 Accounts (Google Workspace):**
- Client secrets: `config/{account}_client_secret_*.json`
- Tokens: `data/contact_intel/{account}_token.json`
- Auth script: `python scripts/contact_intel/google_auth.py --auth --account {name}`

**IMAP Accounts (Personal Gmail with App Password):**
- Add to `.env`: `GMAIL_PERSONAL_ADDRESS` and `GMAIL_PERSONAL_APP_PASSWORD`

**Authenticated Accounts:**
| Account | Method | Email | Messages |
|---------|--------|-------|----------|
| tujaguarcapital | OAuth 2.0 | tu@jaguarcapital.co | 140,761 |
| lahaus | IMAP | tomasuribe@lahaus.com | TBD |

---

## Dependencies

Add to `requirements.txt`:

```
# Graph Database
neo4j>=5.0

# Vector Database
chromadb>=0.4

# LLM (already have GROQ_API_KEY)
groq>=0.4

# Google APIs
google-auth>=2.0
google-api-python-client>=2.0

# Data Models
pydantic>=2.0
```

---

## Parallel Development Guide

### Module Dependencies

```
Module A (Ingestion)     Module B (Extraction)     Module C (Graph)      Module D (Query)
─────────────────────    ─────────────────────     ────────────────      ────────────────
A1: gmail_sync      ───────────────────────────→  C1: graph_builder  →  D1: find_paths
A2: linkedin_sync   ───────────────────────────→       ↓
A3: contacts_import ───────────────────────────→       ↓
                         B1: relationship_ext  ──→  C2: graph_queries
                         B2: entity_extractor  ──→       ↓
                         B3: topic_extractor   ──→       ↓
                                                        ↓
                                               D2: semantic_search
```

### Agent Assignment Strategy

**Phase 1 (3 agents parallel):**
- Agent 1: `gmail_sync.py` + `email_parser.py` + tests
- Agent 2: `graph_builder.py` + Neo4j setup + tests
- Agent 3: `models.py` + `config.py` + tests

**Phase 2 (2 agents parallel):**
- Agent 1: `relationship_extractor.py` + tests
- Agent 2: `graph_queries.py` + `path_finder.py` + tests

**Phase 3 (3 agents parallel):**
- Agent 1: `entity_extractor.py` + Groq integration + tests
- Agent 2: `topic_extractor.py` + tests
- Agent 3: Prompt engineering + integration tests

**Sync Points (Ralph Loop):**
- After Phase 1: Integration test full flow
- After Phase 3: Review extraction quality, tune prompts
- After Phase 5: End-to-end test with real prospects

---

## Success Criteria

1. **Coverage:** Find entry paths for >50% of prospects (vs 0% today)
2. **Quality:** Connector suggestions are actually warm (emailed 3+ times)
3. **Speed:** Process 200 prospects in <5 minutes
4. **Cost:** Full history processing <$50

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Gmail rate limits | Batch fetching, incremental sync, respect quotas |
| Neo4j complexity | Start with embedded mode, upgrade to cloud if needed |
| LLM extraction quality | Sample and validate before full run, tune prompts |
| Duplicate contacts | Dedupe by email first, then fuzzy name matching |
| Personal Gmail auth | Generate app password, document setup steps |

---

## Next Steps (Updated 2026-01-27)

### Immediate (Next Session)
1. **Import LinkedIn connections** - Get CSV export from LinkedIn, run `linkedin_sync.py`
2. **Test warm intro queries** - Try "how can I connect with someone at [company]"

### Short Term
3. **Implement `find_paths.py`** - Batch process prospects_master.csv to find intro paths
4. **Add ChromaDB semantic search** - Enable "who do I know in fintech" queries
5. **LLM signature extraction** - Extract company/role from email signatures (Groq)

### Medium Term
6. **Suggested openers** - Generate personalized intro request messages
7. **Incremental sync** - Daily cron job to keep graph fresh
8. **Relationship decay** - Reduce strength scores over time

### Completed ✅
- ~~Set up personal Gmail app password~~ (using OAuth 2.0)
- ~~Install Neo4j locally (Docker)~~
- ~~Phase 1: Foundation (gmail_sync, graph_builder)~~
- ~~Phase 2: Intro Paths (CC_TOGETHER, path queries)~~
- ~~Phase 3: LLM Enrichment (industry classification, topics)~~
- ~~Phase 4: LinkedIn Integration (linkedin_sync.py ready)~~
- ~~V2 Relationship Scoring (penalizes newsletters/group emails)~~
