# PRD: Facebook Ads Library Prospecting Pipeline

## Overview
Automated pipeline to convert Facebook Ads Library data into qualified prospects with personalized outreach emails for House AI assistant.

## Input Data
- **Source**: `/fb_ads_library_prospecting/input/FB Ad library scraping.xlsx`
- **Records**: 150 advertisers
- **Key fields**: `page_name`, `text`, `page_likes`, `ad_category`, `is_active`, `start_date`, `platforms`

---

## Pipeline Architecture

```
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│ Module 1 │──▶│ Module 2 │──▶│ Module 3 │──▶│Module 3.5│──▶│ Module 4 │──▶│ Module 5 │──▶│ Module 6 │
│  Loader  │   │ Enricher │   │ Scraper  │   │  Hunter  │   │ Composer │   │ Exporter │   │Validator │
└──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘
     │              │              │              │              │              │              │
  Excel→DF     DuckDuckGo      Website       Hunter.io       OpenAI        CSV/JSON       Quality
               Search          Scraping      Email API       GPT-4o                        Report
```

---

## Module Specifications

### Module 1: Data Loader (`scripts/loader.py`)
**Purpose**: Load and normalize input Excel data

**Input**: `input/FB Ad library scraping.xlsx`
**Output**: `processed/01_loaded.csv`

**Functions**:
- `load_excel()` - Read Excel file into DataFrame
- `normalize_page_names()` - Clean company names (remove emojis, special chars)
- `deduplicate()` - Group by page_name, aggregate ad stats
- `filter_relevant()` - Keep only HOUSING category or real estate keywords

**Output Schema**:
```
page_name, ad_count, total_page_likes, ad_texts[], platforms[], is_active, first_ad_date
```

---

### Module 2: Company Enricher (`scripts/enricher.py`)
**Purpose**: Find company websites via search

**Input**: `processed/01_loaded.csv`
**Output**: `processed/02_enriched.csv`

**Functions**:
- `search_company(page_name)` - DuckDuckGo search: `"{page_name}" real estate website`
- `extract_website_url()` - Parse search results for official website
- `validate_url()` - Check URL is valid and responsive
- `rate_limiter()` - 1 request per 2 seconds to avoid blocks

**Dependencies**: `duckduckgo_search` library

**Output Schema** (adds):
```
website_url, search_confidence, linkedin_url (if found)
```

---

### Module 3: Contact Scraper (`scripts/scraper.py`)
**Purpose**: Extract contact information from websites

**Input**: `processed/02_enriched.csv`
**Output**: `processed/03_contacts.csv`

**Functions**:
- `scrape_website(url)` - Fetch website HTML
- `find_contact_page()` - Look for /contact, /about, /team pages
- `extract_contact_details()` - Extract contact name and position from team/about pages
- `extract_emails()` - Regex pattern for email addresses
- `extract_phone()` - Regex pattern for phone numbers
- `extract_social_links()` - Find LinkedIn, Twitter profiles
- `extract_company_info()` - Company description, services offered

**Dependencies**: `requests`, `beautifulsoup4`, `lxml`

**Output Schema** (adds):
```
contact_name, contact_position, emails[], phones[], company_description, services[], social_links{}
```

---

### Module 3.5: Email Hunter (`scripts/hunter.py`)
**Purpose**: Enrich contact data with verified emails via Hunter.io API

**Input**: `processed/03_contacts.csv`
**Output**: `processed/03b_hunter.csv`

**Functions**:
- `search_domain(domain)` - Find all emails for a company domain
- `verify_email(email)` - Verify emails found by scraper
- `find_email(domain, first_name, last_name)` - Find specific person's email

**API Endpoints**:
```
Domain Search:  https://api.hunter.io/v2/domain-search?domain={domain}&api_key={key}
Email Finder:   https://api.hunter.io/v2/email-finder?domain={domain}&first_name={first}&last_name={last}&api_key={key}
Email Verifier: https://api.hunter.io/v2/email-verifier?email={email}&api_key={key}
```

**Dependencies**: `requests` (Hunter.io REST API)

**Output Schema** (adds):
```
hunter_emails[], email_confidence, email_verified
```

---

### Module 4: Email Composer (`scripts/composer.py`)
**Purpose**: Generate personalized outreach emails using AI

**Input**: `processed/03b_hunter.csv`
**Output**: `processed/04_emails.csv`

**Functions**:
- `build_prospect_context(row)` - Compile all data about prospect
- `generate_email(context)` - Call OpenAI to create personalized email
- `validate_email_content()` - Check for placeholders, minimum length

**AI Prompt Template**:
```
You are writing a cold outreach email for House AI, an AI assistant for real estate agents.

Prospect Info:
- Contact: {contact_name}, {contact_position}
- Company: {page_name}
- Ad Activity: {ad_count} ads running since {first_ad_date}
- Page Likes: {page_likes}
- Their Services: {services}
- Ad Copy Themes: {ad_summary}

Write a short, personalized email (max 150 words) that:
1. Addresses the contact by name
2. References something specific about their business
3. Connects their ad activity to House AI's value prop
4. Has a clear, low-friction CTA
```

**Output Schema** (adds):
```
email_subject, email_body, personalization_points[]
```

---

### Module 5: Exporter (`scripts/exporter.py`)
**Purpose**: Export final prospect data in usable formats

**Input**: `processed/04_emails.csv`
**Output**:
- `output/prospects_final.csv`
- `output/prospects_final.xlsx`
- `output/email_drafts.json`

**Functions**:
- `export_csv()` - Full data export
- `export_excel()` - Formatted Excel with columns
- `export_email_json()` - Ready for email automation tools
- `generate_summary_report()` - Pipeline statistics

---

### Module 6: Validator (`scripts/validator.py`)
**Purpose**: Validate pipeline output quality and data coherence

**Input**: All pipeline output files
**Output**: Validation report (console)

**Functions**:
- `check_missing_fields()` - Identify prospects missing email or contact
- `check_coherence()` - Verify data matches source (ad counts, etc.)
- `check_email_quality()` - Detect bad patterns (Hi None, placeholders)
- `check_enrichment_success()` - Report enrichment statistics
- `generate_report()` - Comprehensive validation report

**Checks Performed**:
1. Missing email and contact data
2. Data coherence with source CSV
3. Email quality (no placeholders, correct pluralization)
4. Enrichment success rates (websites found, emails verified)

**Exit Codes**:
- `0`: All checks passed
- `1`: Issues found (report printed)

---

### Progress Monitor (`scripts/progress.py`)
**Purpose**: Real-time pipeline progress monitoring

**Usage**: Run in separate terminal while pipeline executes
```bash
python scripts/progress.py
```

**Features**:
- Shows status of each module (pending/running/done)
- Visual progress bars with row counts
- Auto-refreshes every 2 seconds
- File modification timestamps

---

## Orchestrator (`run_pipeline.py`)
**Purpose**: Run full pipeline with logging and error handling

**Usage**:
```bash
# Test mode (3 rows) - quick validation
python run_pipeline.py

# Full run (all rows)
python run_pipeline.py --all

# Resume from specific module
python run_pipeline.py --all --from 3.5
```

**Pipeline Sequence**:
1. Loader → Load and normalize Excel data
2. Enricher → Find company websites via search
3. Scraper → Extract contacts from websites
4. Hunter → Verify and enrich emails via Hunter.io
5. Composer → Generate personalized emails with GPT-4o
6. Exporter → Export to CSV/Excel/JSON
7. Validator → Quality check and report

---

## File Structure

```
fb_ads_library_prospecting/
├── run_pipeline.py       # Main orchestrator
├── scripts/
│   ├── loader.py         # Module 1: Load Excel
│   ├── enricher.py       # Module 2: Find websites
│   ├── scraper.py        # Module 3: Scrape contacts
│   ├── hunter.py         # Module 3.5: Hunter.io emails
│   ├── composer.py       # Module 4: Generate emails
│   ├── exporter.py       # Module 5: Export outputs
│   ├── validator.py      # Module 6: Quality check
│   └── progress.py       # Real-time progress monitor
├── input/
│   └── FB Ad library scraping.xlsx
├── processed/
│   ├── 01_loaded.csv
│   ├── 02_enriched.csv
│   ├── 03_contacts.csv
│   ├── 03b_hunter.csv
│   └── 04_emails.csv
├── output/
│   ├── prospects_final.csv
│   ├── prospects_final.xlsx
│   └── email_drafts.json
├── config/
│   ├── settings.yaml
│   └── website_overrides.csv
├── requirements.txt
├── .env
└── README.md
```

---

## Dependencies

```
pandas>=2.0.0
openpyxl>=3.1.0
requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
duckduckgo-search>=4.0.0
openai>=1.0.0
python-dotenv>=1.0.0
tqdm>=4.65.0
pyyaml>=6.0.0
```

---

## Configuration (`config/settings.yaml`)

```yaml
search:
  delay_seconds: 2
  max_retries: 3
  keywords: ["real estate", "realtor", "realty"]

scraper:
  timeout_seconds: 10
  max_pages_per_site: 5
  contact_pages: ["/contact", "/about", "/team", "/agents"]

composer:
  model: "gpt-4o-mini"
  max_tokens: 500
  temperature: 0.7

export:
  include_failed: false
  date_format: "%Y-%m-%d"
```

---

## Error Handling

Each module saves intermediate results, allowing:
- Resume from any step after failure
- Manual review/correction between steps
- Parallel processing of independent records

---

## Success Metrics

- **Conversion Rate**: % of page_names → valid emails
- **Contact Rate**: % of websites → extracted contacts
- **Email Quality**: Manual review of 10% sample
- **Target**: 50+ qualified prospects with emails from 150 input records

---

## Parallel Development Guide

### Terminal Commands

Each terminal runs Claude Code with a specific module assignment:

```bash
# Terminal 1 - Loader
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 1 (loader.py). File: scripts/loader.py only."

# Terminal 2 - Enricher
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 2 (enricher.py). File: scripts/enricher.py only."

# Terminal 3 - Scraper
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 3 (scraper.py). File: scripts/scraper.py only."

# Terminal 3.5 - Hunter
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 3.5 (hunter.py). File: scripts/hunter.py only."

# Terminal 4 - Composer
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 4 (composer.py). File: scripts/composer.py only."

# Terminal 5 - Exporter
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 5 (exporter.py). File: scripts/exporter.py only."
```

### File Ownership (No Conflicts)

| Terminal | Owns | Never Touch |
|----------|------|-------------|
| T1 | `scripts/loader.py` | enricher, scraper, hunter, composer, exporter |
| T2 | `scripts/enricher.py` | loader, scraper, hunter, composer, exporter |
| T3 | `scripts/scraper.py` | loader, enricher, hunter, composer, exporter |
| T3.5 | `scripts/hunter.py` | loader, enricher, scraper, composer, exporter |
| T4 | `scripts/composer.py` | loader, enricher, scraper, hunter, exporter |
| T5 | `scripts/exporter.py` | loader, enricher, scraper, hunter, composer |

Shared files (`utils.py`, `main.py`) are built AFTER all modules complete.

### Code Style Rules

1. **Minimal first**: Get it working with fewest lines possible
2. **No premature abstraction**: Only refactor if needed
3. **Clear names**: Functions describe what they do
4. **No comments unless complex**: Code should be self-documenting
5. **Single responsibility**: Each function does one thing

### Data Contracts (All Modules Must Follow)

**01_loaded.csv columns:**
```
page_name,ad_count,total_page_likes,ad_texts,platforms,is_active,first_ad_date
```

**02_enriched.csv adds:**
```
website_url,search_confidence,linkedin_url
```

**03_contacts.csv adds:**
```
contact_name,contact_position,emails,phones,company_description,services,social_links
```

**03b_hunter.csv adds:**
```
hunter_emails,email_confidence,email_verified
```

**04_emails.csv adds:**
```
email_subject,email_body,personalization_points
```

### Module Test Commands

Each module must include a `if __name__ == "__main__":` block for standalone testing:

```bash
# Test each module independently
python scripts/loader.py      # Creates processed/01_loaded.csv
python scripts/enricher.py    # Reads 01, creates 02 (test with 3 rows)
python scripts/scraper.py     # Reads 02, creates 03 (test with 3 rows)
python scripts/hunter.py      # Reads 03, creates 03b (test with 3 rows)
python scripts/composer.py    # Reads 03b, creates 04 (test with 3 rows)
python scripts/exporter.py    # Reads 04, creates output files
```

---

## Integration Testing

After all modules complete:

```bash
# Terminal 6 - Integration
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build main.py orchestrator. Test full pipeline. Fix any interface mismatches between modules."
```

### Integration Checklist

1. Run full pipeline: `python scripts/main.py`
2. Verify each CSV has expected columns
3. Check row counts match through pipeline
4. Validate sample emails are personalized
5. Confirm output files are generated

### Common Interface Fixes

| Issue | Fix |
|-------|-----|
| Column name mismatch | Align to contract above |
| List stored as string | Use `ast.literal_eval()` to parse |
| Missing columns | Add with empty/null defaults |
| Encoding errors | Use `encoding='utf-8'` everywhere |
