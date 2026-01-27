# Overnight Graph Enrichment Plan

> **For Claude:** Execute these tasks sequentially. Each script saves progress incrementally.

**Goal:** Enrich the contact graph overnight with relationship strength, industry tags, and missing company data.

**Estimated Runtime:** 4-6 hours total
**Estimated Cost:** ~$10-15 (mostly Apollo API)

---

## Task 1: Relationship Strength Scoring (Free, ~30 min)

**Files:**
- Create: `scripts/contact_intel/relationship_strength.py`
- Test: `scripts/contact_intel/tests/test_relationship_strength.py`

**What it does:**
- Query all KNOWS edges from Neo4j
- Calculate strength_score (0-100) based on:
  - email_count (more emails = stronger)
  - recency (recent contact = warmer)
  - bidirectional (both sent = stronger)
- Update KNOWS edges with: `strength_score`, `is_bidirectional`, `days_since_contact`

**Formula:**
```
base_score = min(email_count * 5, 40)  # 0-40 points for volume
recency_score = max(0, 30 - (days_since_contact / 30))  # 0-30 points for recency
bidirectional_bonus = 30 if is_bidirectional else 0  # 30 points if mutual
strength_score = base_score + recency_score + bidirectional_bonus
```

---

## Task 2: Industry Classification (Cheap, ~1-2 hrs)

**Files:**
- Create: `scripts/contact_intel/industry_classifier.py`
- Test: `scripts/contact_intel/tests/test_industry_classifier.py`

**What it does:**
- Query all Company nodes from Neo4j (4,149 companies)
- Batch process 10 companies at a time through GPT-4o-mini
- Classify into standard industries:
  - Real Estate, Finance, Technology, Healthcare, Legal, Marketing, Consulting,
  - Construction, Education, Retail, Manufacturing, Media, Other
- Update Company nodes with: `industry` property

**Batching for efficiency:**
- Send 10 companies per API call
- ~415 API calls total
- At $0.15/1M input tokens, ~$0.50 total

---

## Task 3: Fill Missing Company Data (~2-3 hrs, ~$10)

**Files:**
- Create: `scripts/contact_intel/contact_gap_filler.py`
- Test: `scripts/contact_intel/tests/test_contact_gap_filler.py`

**What it does:**
- Query Person nodes WITHOUT WORKS_AT edges (~1,100 contacts)
- For each, call Apollo API to find company/role
- Create WORKS_AT edges for found data
- Track which contacts couldn't be enriched

**Apollo strategy:**
- Use email domain to find company
- Use name + email to find person
- Cost: ~$0.01-0.02 per lookup

---

## Task 4: Orchestrator Script

**Files:**
- Create: `scripts/contact_intel/overnight_enrichment.py`

**What it does:**
- Runs Task 1 → Task 2 → Task 3 sequentially
- Logs progress to `overnight_enrichment.log`
- Saves checkpoints after each task
- Handles interruptions gracefully (can resume)

**Usage:**
```bash
python -m scripts.contact_intel.overnight_enrichment
```
