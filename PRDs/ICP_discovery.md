# PRD — ICP Discovery from Facebook Ads Library (Conversational AI)

## 1. Objective

Build a **sector-agnostic system** that uses Facebook Ads Library data to identify **Ideal Customer Profiles (ICPs)** with:
- **Money**: clear willingness and ability to pay (ad spend intensity, maturity).
- **Urgency**: strong need for fast lead response.
- **Conversational necessity**: lead conversion depends on dialogue, qualification, or follow-up.

The output is a **ranked list of ICP clusters and advertisers** that are strong candidates for an AI assistant that qualifies and responds to leads conversationally.

---

## 2. Scope

### In Scope
- Facebook Ads Library data scraped via Apify actor.
- Behavioral analysis based on **how advertisers acquire and handle leads**, not industry.
- Deterministic + explainable rules (no black-box ML required for v1).
- Modular pipeline that can be iterated and reweighted later.

### Out of Scope (v1)
- CRM integration.
- Attribution to downstream revenue.
- Predicting exact ad spend if not provided.

---

## 3. Data Inputs

### Source
Facebook Ads Library Scraper

### Input Schema (provided by actor)
Top-level and snapshot fields as documented by the user (see full field list in prompt).

---

## 4. System Architecture (Modules)

The system is composed of **8 discrete modules**. Each module has a clear input/output contract so it can be built, tested, and iterated independently.

---

## Module 0 — Data Normalization

### Purpose
Normalize raw scraper output into a clean, consistent ad-level table.

### Inputs
Raw actor output (JSON rows).

### Processing
- Parse timestamps: `start_date`, `end_date` → datetime.
- Normalize text fields:
  - `body.text`, `title`, `link_description`, `cta_text`.
- Extract:
  - `domain` from `link_url` (fallback to `caption`).
- Create media flags:
  - `has_image`, `has_video`, `has_carousel`.
- Standardize list fields:
  - `publisher_platform`, `categories`, `page_categories`, `targeted_or_reached_countries`.

### Outputs
`ads_clean`  
One row per ad with normalized fields.

---

## Module 1 — Advertiser (Page-Level) Aggregation

### Purpose
Convert ad-level data into **advertiser-level behavior**, which is the unit of ICP analysis.

### Group By
- `page_id` (primary)
- `page_name` (label)

### Derived Metrics
- **Ad Volume**
  - `active_ads`
  - `total_ads`
  - `distinct_collation_count`
- **Velocity & Iteration**
  - `new_ads_30d`
  - `creative_refresh_rate`
- **Always-on Behavior**
  - `days_live_per_ad`
  - `always_on_share` (ads live ≥21 or ≥30 days)
- **Budget Proxies**
  - Use `spend` if available.
  - Else use midpoint of `impressions_with_index`.
- **Scale Proxies**
  - `page_like_count`
- **Geo Breadth**
  - `country_count`
- **Platform Mix**
  - Distribution of `publisher_platform`.

### Outputs
`pages_agg`  
One row per advertiser with aggregated behavioral metrics.

---

## Module 2 — Conversational Necessity Gate (Hard Filter)

### Purpose
Eliminate advertisers whose business **does not require conversation** to convert leads.

### 2.1 Destination Type Classification

#### Inputs
- `link_url`
- `cta_type`
- `cta_text`
- `publisher_platform`
- `cards[*].link_url`, `cards[*].cta_type`

#### Destination Labels
- `MESSAGE`: Messenger / WhatsApp / IG DM links.
- `CALL`: `CALL_NOW` or `tel:` links.
- `FORM`: Lead forms or form-like flows.
- `WEB`: Everything else.

Destination is computed at **ad-level**, then rolled up to advertiser-level shares:
- `share_message`, `share_call`, `share_form`, `share_web`.

---

### 2.2 Transactional Exclusion Rules

#### Drop Advertiser If (ALL true):
- `share_message`, `share_call`, and `share_form` are all near zero.
- Dominant CTA is transactional (e.g. BUY, SHOP, DOWNLOAD).
- Copy emphasizes instant purchase (price/discount) with no dialogue cues.

#### Keep Advertiser If (ANY true):
- Meaningful `share_message` or `share_call`.
- Meaningful `share_form` **and** copy implies follow-up or qualification (e.g. apply, contact, quote).

### Outputs
`pages_candidate`  
Subset of advertisers where conversation is plausibly core to conversion.

---

## Module 3 — Money Score

### Purpose
Estimate **willingness and ability to pay** using ad behavior proxies.

### Inputs
From `pages_candidate`:
- `active_ads`
- `new_ads_30d`
- `always_on_share`
- `creative_refresh_rate`
- `distinct_collation_count`
- `spend` or `impressions_with_index`
- `page_like_count`

### Scoring (0–50)
- Spend / impressions proxy: 0–20
- Active ad volume: 0–10
- Always-on share: 0–10
- Velocity + refresh: 0–10

### Outputs
`money_score` per advertiser.

---

## Module 4 — Urgency Score

### Purpose
Measure how **time-sensitive** lead response is.

### Inputs
- Destination shares (`share_message`, `share_call`, `share_form`)
- `cta_type`, `cta_text`
- Text fields (`body.text`, `title`, `link_description`)
- `publisher_platform`

### Scoring (0–50)
- Message / call share: 0–25
- Form share: 0–10
- Immediacy language (NLP / keywords): 0–10
- Qualification complexity: 0–5

### Outputs
`urgency_score` per advertiser.

---

## Module 5 — Conversational Fit Score

### Purpose
Assess whether an AI **qualifier** (not just auto-reply) adds real value.

### Signals
- Presence of qualification language (eligibility, requirements, apply).
- Multi-step intent implied across carousel cards.
- Combination of form + follow-up CTAs.

### Scoring
0–30 (independent from money/urgency).

### Outputs
`fit_score` per advertiser.

---

## Module 6 — Behavioral Clustering (ICP Discovery)

### Purpose
Identify **ICPs based on acquisition behavior**, not sector.

### Features Used
- Destination shares
- Platform mix
- Velocity / always-on metrics
- (Optional) Text embeddings of ad copy

### Expected Clusters
- Message-first closers
- Form-driven call setters
- Multi-offer funnel operators
- Call-first operators

### Outputs
`icp_clusters` with:
- Cluster size
- Median money / urgency / fit scores
- Representative advertisers and ads

---

## Module 7 — ICP Ranking & Selection

### Purpose
Decide **who to sell to first**.

### Ranking Formula
`total_score = w1 * money_score + w2 * urgency_score + w3 * fit_score`

### Outputs
1. Ranked list of advertisers.
2. Ranked list of ICP clusters.
3. Diagnostic fields per advertiser:
   - Destination mix
   - Always-on share
   - Velocity
   - Scores breakdown

---

## Module 8 — Iteration Loop (Post-MVP)

### Purpose
Continuously improve ICP accuracy using sales outcomes.

### Inputs (future)
- Demo booked
- Closed / not closed
- Retention

### Action
- Reweight money / urgency / fit scores based on real performance.

---

## 5. Non-Goals / Guardrails

- Do **not** classify by industry.
- Do **not** rely on ad copy semantics alone.
- Do **not** include advertisers that can convert without talking to users.

---

## 6. Success Criteria

- ≥80% of top-ranked advertisers plausibly need conversational lead handling.
- Clear, explainable ICP clusters usable by sales and GTM.
- Pipeline can run end-to-end deterministically on new Ads Library scrapes.

---

## 7. MVP Recommendation

For fastest validation, implement first:
1. Module 0 — Normalization  
2. Module 1 — Page aggregation  
3. Module 2 — Conversational necessity gate  
4. Modules 3 + 4 — Money & Urgency scores  

Clustering (Module 6) can follow once rankings look directionally correct.
