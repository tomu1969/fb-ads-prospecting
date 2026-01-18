# PRD: Lead Scoring Enrichment (Google Maps + Tech Stack)

## Overview

Extend the prospecting pipeline with two new enrichment modules and a lead scoring system to identify high-intent buyers for LaHaus AI. These modules add **lead volume proxies** (Google Maps reviews) and **operational maturity signals** (tech stack detection) to enable prioritized outreach.

**Goal**: Score leads 0-15 based on likelihood to buy AI automation, enabling SDRs to focus on hot leads first.

---

## Business Context

### Why This Matters

LaHaus AI targets businesses with:
1. **High lead volume** (100+ leads/month) — they feel response time pain
2. **Operational maturity** — they'll pay for automation (not tire-kickers)
3. **Decision-maker access** — owner/broker can buy without committee

### Current Gap

| Signal | Current State | After Integration |
|--------|---------------|-------------------|
| Lead volume proxy | FB ad count only | + Google reviews, review velocity |
| Operational maturity | None | CRM, pixel, scheduling tool detection |
| Composite score | None | 0-15 buyer intent score |

---

## New Module Specifications

### Module 3.10: Google Maps Enricher (`scripts/google_maps_enricher.py`)

**Purpose**: Enrich leads with Google Business Profile data as a proxy for lead volume and local market presence.

**Input**: `processed/03f_linkedin.csv` (or latest pipeline stage)
**Output**: `processed/03g_gmaps.csv`

#### Data Points Extracted

| Field | Type | Description | Scoring Use |
|-------|------|-------------|-------------|
| `gmaps_rating` | float | Star rating (1.0-5.0) | Quality indicator |
| `gmaps_review_count` | int | Total review count | Lead volume proxy |
| `gmaps_place_id` | str | Google Place ID | Deduplication |
| `gmaps_business_status` | str | OPERATIONAL, CLOSED, etc. | Filter inactive |
| `gmaps_types` | list | Business categories | Segment validation |
| `gmaps_url` | str | Google Maps URL | Reference |
| `gmaps_phone` | str | Listed phone | Contact enrichment |
| `gmaps_address` | str | Business address | Geo-targeting |
| `gmaps_multi_location` | bool | Has multiple locations | Scale indicator |

#### Implementation Options

**Option A: Apify Actor (Recommended)**
- Actor: `compass/crawler-google-places` or `apify/google-maps-scraper`
- Cost: ~$0.001-0.01 per search
- Pros: Reliable, maintained, handles anti-bot
- Cons: External dependency

**Option B: Google Places API (Official)**
- API: Places API (New) - Place Search + Place Details
- Cost: $0.017 per Place Details call (first 100K/month)
- Pros: Official, reliable, structured data
- Cons: Higher cost at scale

**Recommended**: Start with Apify for cost efficiency, migrate to Places API if volume justifies.

#### Search Strategy

```python
def search_google_maps(company_name: str, website_url: str, location: str = None) -> dict:
    """
    Search Google Maps for business listing.

    Strategy:
    1. Search by company name + location (if known)
    2. Validate match by comparing website URL
    3. Fall back to name-only search if no match
    4. Return best match with confidence score
    """
    queries = [
        f"{company_name} {location}" if location else company_name,
        f"{company_name} real estate",  # Industry-specific
    ]

    for query in queries:
        results = apify_google_maps_search(query, limit=5)
        match = find_best_match(results, website_url, company_name)
        if match and match['confidence'] > 0.7:
            return match

    return None
```

#### Match Validation

To avoid false positives, validate matches by:
1. **Website URL match** — Listed website matches our `website_url`
2. **Name similarity** — SequenceMatcher ratio > 0.8
3. **Location proximity** — If we have address data
4. **Category alignment** — Business type matches expected (real estate, etc.)

#### Output Schema (adds to pipeline)

```
gmaps_rating, gmaps_review_count, gmaps_place_id, gmaps_business_status,
gmaps_types, gmaps_url, gmaps_phone, gmaps_address, gmaps_multi_location,
gmaps_match_confidence
```

#### API/Actor Details

**Apify Actor: `compass/crawler-google-places`**

```python
from apify_client import ApifyClient

def search_with_apify(query: str, limit: int = 5) -> list:
    client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

    run_input = {
        "searchStringsArray": [query],
        "maxCrawledPlacesPerSearch": limit,
        "language": "en",
        "includeReviews": False,  # Save cost, just need counts
    }

    run = client.actor("compass/crawler-google-places").call(run_input=run_input)

    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append({
            "name": item.get("title"),
            "rating": item.get("totalScore"),
            "review_count": item.get("reviewsCount"),
            "place_id": item.get("placeId"),
            "website": item.get("website"),
            "phone": item.get("phone"),
            "address": item.get("address"),
            "business_status": item.get("openingHours", {}).get("status"),
            "types": item.get("categories", []),
            "url": item.get("url"),
        })

    return results
```

---

### Module 3.11: Tech Stack Enricher (`scripts/tech_stack_enricher.py`)

**Purpose**: Detect CRM, marketing pixels, scheduling tools, and chat widgets to assess operational maturity.

**Input**: `processed/03g_gmaps.csv`
**Output**: `processed/03h_techstack.csv`

#### Data Points Extracted

| Field | Type | Description | Scoring Use |
|-------|------|-------------|-------------|
| `has_crm` | bool | CRM system detected | +2 points |
| `crm_name` | str | HubSpot, Salesforce, etc. | Segmentation |
| `has_marketing_pixel` | bool | Meta/Google pixel detected | +1 point |
| `pixel_types` | list | Which pixels found | Segmentation |
| `has_scheduling_tool` | bool | Calendly, etc. detected | +1 point |
| `scheduling_tool` | str | Tool name | Segmentation |
| `has_chat_widget` | bool | Live chat detected | +1 point |
| `chat_widget` | str | Intercom, Drift, etc. | Segmentation |
| `has_lead_form` | bool | Contact/lead form detected | +1 point |
| `tech_stack_raw` | list | All detected technologies | Reference |

#### Implementation Options

**Option A: Apify BuiltWith Actor**
- Actor: `epctex/builtwith-scraper` or similar
- Cost: ~$0.01-0.02 per domain
- Pros: Comprehensive tech detection
- Cons: External dependency, may be slow

**Option B: Wappalyzer API**
- API: Wappalyzer Technology Lookup API
- Cost: $0.01 per lookup (paid plans)
- Pros: Fast, accurate, structured categories
- Cons: Requires API key, limited free tier

**Option C: Self-hosted Detection (Recommended for MVP)**
- Approach: Fetch homepage HTML, detect signatures
- Cost: Free (just HTTP requests)
- Pros: No external dependency, instant
- Cons: Less comprehensive, maintenance burden

**Recommended**: Start with self-hosted detection for common tools, add Wappalyzer for comprehensive coverage later.

#### Detection Signatures (Self-Hosted)

```python
TECH_SIGNATURES = {
    # CRMs
    "hubspot": {
        "patterns": [
            r"hs-script-loader",
            r"js\.hs-scripts\.com",
            r"hbspt\.forms",
            r"hubspot\.com",
        ],
        "category": "crm",
    },
    "salesforce": {
        "patterns": [
            r"salesforce\.com",
            r"pardot\.com",
            r"sfdc-form",
        ],
        "category": "crm",
    },
    "followupboss": {
        "patterns": [
            r"followupboss\.com",
            r"fub\.com",
        ],
        "category": "crm",
    },
    "kvcore": {
        "patterns": [
            r"kvcore\.com",
            r"platform\.kvcore",
        ],
        "category": "crm",
    },

    # Marketing Pixels
    "meta_pixel": {
        "patterns": [
            r"connect\.facebook\.net",
            r"fbq\(",
            r"facebook-pixel",
        ],
        "category": "pixel",
    },
    "google_analytics": {
        "patterns": [
            r"google-analytics\.com",
            r"googletagmanager\.com",
            r"gtag\(",
            r"ga\(",
        ],
        "category": "pixel",
    },
    "google_ads": {
        "patterns": [
            r"googleadservices\.com",
            r"google_conversion",
        ],
        "category": "pixel",
    },

    # Scheduling Tools
    "calendly": {
        "patterns": [
            r"calendly\.com",
            r"assets\.calendly\.com",
        ],
        "category": "scheduling",
    },
    "acuity": {
        "patterns": [
            r"acuityscheduling\.com",
        ],
        "category": "scheduling",
    },

    # Chat Widgets
    "intercom": {
        "patterns": [
            r"intercom\.io",
            r"widget\.intercom\.io",
        ],
        "category": "chat",
    },
    "drift": {
        "patterns": [
            r"drift\.com",
            r"js\.driftt\.com",
        ],
        "category": "chat",
    },
    "livechat": {
        "patterns": [
            r"livechatinc\.com",
            r"cdn\.livechatinc\.com",
        ],
        "category": "chat",
    },
    "tawk": {
        "patterns": [
            r"tawk\.to",
            r"embed\.tawk\.to",
        ],
        "category": "chat",
    },

    # Lead Forms (generic detection)
    "lead_form": {
        "patterns": [
            r"<form[^>]*(?:contact|lead|inquiry|schedule|get-started)",
            r"type=[\"'](?:email|tel)[\"']",
        ],
        "category": "form",
    },
}
```

#### Detection Function

```python
import re
import requests
from typing import Dict, List, Any

def detect_tech_stack(website_url: str) -> Dict[str, Any]:
    """
    Detect technologies used on a website.

    Returns:
        dict with has_crm, crm_name, has_pixel, pixel_types, etc.
    """
    try:
        response = requests.get(
            website_url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TechStackBot/1.0)"},
        )
        html = response.text.lower()
    except Exception as e:
        logger.warning(f"Failed to fetch {website_url}: {e}")
        return empty_result()

    detected = []

    for tech_name, config in TECH_SIGNATURES.items():
        for pattern in config["patterns"]:
            if re.search(pattern, html, re.IGNORECASE):
                detected.append({
                    "name": tech_name,
                    "category": config["category"],
                })
                break  # Found this tech, move to next

    # Aggregate by category
    crms = [t for t in detected if t["category"] == "crm"]
    pixels = [t for t in detected if t["category"] == "pixel"]
    scheduling = [t for t in detected if t["category"] == "scheduling"]
    chat = [t for t in detected if t["category"] == "chat"]
    forms = [t for t in detected if t["category"] == "form"]

    return {
        "has_crm": len(crms) > 0,
        "crm_name": crms[0]["name"] if crms else None,
        "has_marketing_pixel": len(pixels) > 0,
        "pixel_types": [p["name"] for p in pixels],
        "has_scheduling_tool": len(scheduling) > 0,
        "scheduling_tool": scheduling[0]["name"] if scheduling else None,
        "has_chat_widget": len(chat) > 0,
        "chat_widget": chat[0]["name"] if chat else None,
        "has_lead_form": len(forms) > 0,
        "tech_stack_raw": [t["name"] for t in detected],
    }
```

#### Output Schema (adds to pipeline)

```
has_crm, crm_name, has_marketing_pixel, pixel_types, has_scheduling_tool,
scheduling_tool, has_chat_widget, chat_widget, has_lead_form, tech_stack_raw
```

---

### Module 3.12: Lead Scorer (`scripts/lead_scorer.py`)

**Purpose**: Calculate composite buyer intent score (0-15) based on all enrichment signals.

**Input**: `processed/03h_techstack.csv`
**Output**: `processed/03i_scored.csv`

#### Scoring Formula

```python
SCORING_WEIGHTS = {
    # Lead Volume Signals (max 6 points)
    "active_fb_ads": 3,           # has active ads in FB Ads Library
    "multiple_ad_creatives": 2,   # ad_count >= 3
    "high_review_count": 2,       # gmaps_review_count >= 30
    "very_high_review_count": 1,  # gmaps_review_count >= 100 (bonus)

    # Operational Maturity Signals (max 5 points)
    "has_crm": 2,                 # CRM detected
    "has_marketing_pixel": 1,     # Meta/Google pixel
    "has_scheduling_tool": 1,     # Calendly, etc.
    "has_chat_widget": 1,         # Live chat

    # Contact Quality Signals (max 4 points)
    "owner_or_broker": 2,         # contact_position contains owner/broker/founder
    "email_and_phone": 2,         # Both email and phone found
    "verified_email": 1,          # Email verified (bonus, replaces above)

    # Brand Presence Signals (max 2 points)
    "business_instagram": 1,      # Has business Instagram
    "good_google_rating": 1,      # gmaps_rating >= 4.0
}

MAX_SCORE = 15  # Theoretical max (some bonuses overlap)
```

#### Score Calculation

```python
def calculate_lead_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate composite lead score based on all signals.

    Returns:
        dict with score, tier, and breakdown
    """
    score = 0
    breakdown = []

    # Lead Volume Signals
    if row.get("ad_count", 0) > 0:
        score += 3
        breakdown.append("active_fb_ads:+3")

    if row.get("ad_count", 0) >= 3:
        score += 2
        breakdown.append("multiple_creatives:+2")

    review_count = row.get("gmaps_review_count", 0) or 0
    if review_count >= 30:
        score += 2
        breakdown.append("reviews_30+:+2")
    if review_count >= 100:
        score += 1
        breakdown.append("reviews_100+:+1")

    # Operational Maturity
    if row.get("has_crm"):
        score += 2
        breakdown.append("has_crm:+2")

    if row.get("has_marketing_pixel"):
        score += 1
        breakdown.append("has_pixel:+1")

    if row.get("has_scheduling_tool"):
        score += 1
        breakdown.append("has_scheduling:+1")

    if row.get("has_chat_widget"):
        score += 1
        breakdown.append("has_chat:+1")

    # Contact Quality
    position = str(row.get("contact_position", "")).lower()
    if any(title in position for title in ["owner", "broker", "founder", "principal", "ceo"]):
        score += 2
        breakdown.append("decision_maker:+2")

    has_email = bool(row.get("primary_email"))
    has_phone = bool(row.get("phones"))
    if has_email and has_phone:
        score += 2
        breakdown.append("email+phone:+2")
    elif has_email:
        score += 1
        breakdown.append("email_only:+1")

    # Brand Presence
    if row.get("instagram_handles"):
        score += 1
        breakdown.append("has_instagram:+1")

    rating = row.get("gmaps_rating", 0) or 0
    if rating >= 4.0:
        score += 1
        breakdown.append("good_rating:+1")

    # Determine tier
    if score >= 12:
        tier = "HOT"
    elif score >= 8:
        tier = "WARM"
    elif score >= 5:
        tier = "COOL"
    else:
        tier = "COLD"

    return {
        "lead_score": score,
        "lead_tier": tier,
        "score_breakdown": "|".join(breakdown),
    }
```

#### Tier Definitions

| Tier | Score | Action | Priority |
|------|-------|--------|----------|
| HOT | 12-15 | Direct pitch, book demo | P0 - Same day |
| WARM | 8-11 | Educate + demo CTA | P1 - Within 48h |
| COOL | 5-7 | Nurture sequence | P2 - Weekly |
| COLD | 0-4 | Low priority / skip | P3 - Monthly or skip |

#### Output Schema (adds to pipeline)

```
lead_score, lead_tier, score_breakdown
```

---

## Pipeline Integration

### Updated Pipeline Flow

```
Module 1        Module 2        Module 3       Module 3.5     Module 3.6
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  Loader  │──▶│ Enricher │──▶│ Scraper  │──▶│  Hunter  │──▶│  Agent   │
└──────────┘   └──────────┘   └──────────┘   └──────────┘   │ Enricher │
                                                            └──────────┘
                                                                  │
     ┌────────────────────────────────────────────────────────────┘
     ▼
Module 3.7      Module 3.8      Module 3.9     Module 3.10    Module 3.11
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│Instagram │──▶│  Name    │──▶│ LinkedIn │──▶│  Google  │──▶│  Tech    │
│ Enricher │   │ Resolver │   │ Enricher │   │   Maps   │   │  Stack   │
└──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘
                                                                  │
     ┌────────────────────────────────────────────────────────────┘
     ▼
Module 3.12     Module 4        Module 5
┌──────────┐   ┌──────────┐   ┌──────────┐
│   Lead   │──▶│ Exporter │──▶│ Validator│
│  Scorer  │   │          │   │          │
└──────────┘   └──────────┘   └──────────┘
```

### File Output Sequence

```
processed/03f_linkedin.csv
    ↓
processed/03g_gmaps.csv       (+ Google Maps data)
    ↓
processed/03h_techstack.csv   (+ Tech stack data)
    ↓
processed/03i_scored.csv      (+ Lead scores)
    ↓
output/prospects_final.csv    (includes all enrichment + scores)
output/hubspot/contacts.csv   (includes lead_score, lead_tier)
```

### CLI Integration

```bash
# Run individual modules
python scripts/google_maps_enricher.py              # Test mode (3 contacts)
python scripts/google_maps_enricher.py --all        # All contacts
python scripts/tech_stack_enricher.py --all         # All contacts
python scripts/lead_scorer.py --all                 # Score all contacts

# Run via pipeline
python run_pipeline.py --all                        # Full pipeline with scoring
python run_pipeline.py --all --from 3.10            # Resume from Google Maps
python run_pipeline.py --all --skip-scoring         # Skip new modules (legacy)
```

---

## HubSpot Integration

### New Fields for HubSpot Export

Add these fields to `output/hubspot/contacts.csv`:

| HubSpot Property | Source Field | Type |
|-----------------|--------------|------|
| `lead_score` | `lead_score` | Number |
| `lead_tier` | `lead_tier` | Dropdown (HOT/WARM/COOL/COLD) |
| `google_review_count` | `gmaps_review_count` | Number |
| `google_rating` | `gmaps_rating` | Number |
| `has_crm` | `has_crm` | Checkbox |
| `crm_platform` | `crm_name` | Text |
| `tech_stack` | `tech_stack_raw` | Text (comma-separated) |

### HubSpot Workflow Triggers

| Trigger | Action |
|---------|--------|
| `lead_tier` = HOT | Notify SDR immediately, assign to priority queue |
| `lead_tier` = WARM | Add to 48h follow-up sequence |
| `lead_tier` = COOL | Add to weekly nurture sequence |
| `lead_tier` = COLD | Add to monthly newsletter only |

---

## Cost Analysis

### Per-Contact Costs (Estimated)

| Module | Method | Cost | Notes |
|--------|--------|------|-------|
| Google Maps | Apify | $0.005 | ~200 searches = $1 |
| Google Maps | Places API | $0.017 | Official, more reliable |
| Tech Stack | Self-hosted | $0.00 | Just HTTP requests |
| Tech Stack | Wappalyzer | $0.01 | If needed later |
| Lead Scorer | Local | $0.00 | Pure computation |

**Total added cost**: ~$0.005-0.02 per contact

### ROI Justification

- Current pipeline cost: ~$0.42/contact max
- New modules add: ~$0.01/contact
- **Value**: Prioritize 20% of leads (HOT+WARM) that generate 80% of conversions

---

## Implementation Plan

### Phase 1: Google Maps Enricher (Day 1-2)
1. Set up Apify client and actor
2. Implement search + match validation
3. Add to pipeline after LinkedIn enricher
4. Test with 20 contacts

### Phase 2: Tech Stack Enricher (Day 2-3)
1. Implement self-hosted signature detection
2. Test against known websites
3. Add to pipeline after Google Maps
4. Expand signatures as needed

### Phase 3: Lead Scorer (Day 3-4)
1. Implement scoring formula
2. Add tier classification
3. Integrate with exporter (HubSpot fields)
4. Validate scores against manual assessment

### Phase 4: Pipeline Integration (Day 4-5)
1. Update `run_pipeline.py` orchestrator
2. Add CLI flags for new modules
3. Update CLAUDE.md documentation
4. Run full pipeline test

---

## Testing Strategy

### Unit Tests

```python
# tests/test_google_maps_enricher.py
def test_search_finds_business():
    result = search_google_maps("Compass Real Estate", "https://compass.com")
    assert result["gmaps_review_count"] > 0

def test_match_validation_rejects_wrong_business():
    result = search_google_maps("Random Name", "https://specific-site.com")
    assert result is None or result["gmaps_match_confidence"] < 0.5

# tests/test_tech_stack_enricher.py
def test_detects_hubspot():
    result = detect_tech_stack("https://www.hubspot.com")
    assert result["has_crm"] == True

def test_detects_calendly():
    # Use a known site with Calendly
    result = detect_tech_stack("https://calendly.com")
    assert result["has_scheduling_tool"] == True

# tests/test_lead_scorer.py
def test_hot_lead_scoring():
    row = {
        "ad_count": 5,
        "gmaps_review_count": 150,
        "has_crm": True,
        "has_marketing_pixel": True,
        "contact_position": "Broker/Owner",
        "primary_email": "john@example.com",
        "phones": "555-1234",
    }
    result = calculate_lead_score(pd.Series(row))
    assert result["lead_tier"] == "HOT"
    assert result["lead_score"] >= 12
```

### Integration Tests

```bash
# Run new modules on sample data
python scripts/google_maps_enricher.py --limit 5
python scripts/tech_stack_enricher.py --limit 5
python scripts/lead_scorer.py --limit 5

# Verify output schema
python -c "import pandas as pd; df = pd.read_csv('processed/03i_scored.csv'); print(df[['page_name', 'lead_score', 'lead_tier', 'score_breakdown']].head())"
```

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Google Maps match rate | >70% | Contacts with valid gmaps data |
| Tech stack detection rate | >50% | Contacts with at least 1 tech detected |
| Score distribution | 20% HOT/WARM | Tier breakdown across leads |
| Conversion lift | +30% | HOT tier vs overall conversion rate |

---

## Dependencies

### New Python Packages

```
apify-client>=1.0.0     # Google Maps actor
```

### Environment Variables

```
APIFY_API_TOKEN=apify_api_...   # For Google Maps actor
WAPPALYZER_API_KEY=...          # Optional, for enhanced tech detection
```

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Google Maps rate limiting | Slow enrichment | Use Apify (handles rate limits) |
| False positive tech detection | Bad scoring | Require 2+ pattern matches |
| Apify actor changes | Breaking changes | Pin actor version, monitor |
| Over-scoring FB-heavy leads | Bias | Weight review count equally |

---

## Future Enhancements

1. **Review velocity** — Track review growth rate (not just count)
2. **Competitor analysis** — Detect if they use competitor tools
3. **Website traffic estimation** — SimilarWeb/Semrush integration
4. **Social proof scoring** — LinkedIn follower count, IG engagement
5. **ML-based scoring** — Train model on closed-won deals
