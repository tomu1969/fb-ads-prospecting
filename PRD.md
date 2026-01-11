# PRD: Prospecting Pipeline

## Overview
Automated pipeline to convert lead data from any source into qualified prospects with verified contact information, ready for HubSpot import. The pipeline is input-agnostic, supporting CSV, Excel, JSON, and TSV formats with AI-powered field mapping. Originally designed for Facebook Ads Library data, but now accepts any lead source.

## Input Data
- **Default Source**: `/fb_ads_library_prospecting/input/FB Ad library scraping.xlsx`
- **Supported Formats**: CSV, Excel (.xlsx, .xls), JSON, TSV
- **Custom Input**: Use `--input` flag to specify any file format
- **Records**: Variable (depends on input file)
- **Key fields**: `page_name` (required), plus optional fields that can be mapped

---

## Pipeline Architecture

```
Module 1        Module 2        Module 3       Module 3.5     Module 3.6          Module 3.7     Module 4        Module 5
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐       ┌──────────┐   ┌──────────┐   ┌──────────┐
│  Loader  │──▶│ Enricher │──▶│ Scraper  │──▶│  Hunter  │──▶│  Agent   │──────▶│Instagram │──▶│ Exporter │──▶│ Validator│
│  Smart   │   │          │   │          │   │          │   │ Enricher │       │          │   │          │   │          │
│ Adapter  │   │          │   │          │   │          │   │ (Exa+AI) │       │          │   │          │   │          │
└──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘       └──────────┘   └──────────┘   └──────────┘
     │              │              │              │                │                   │              │              │
Any Format    DuckDuckGo      Website       Hunter.io      Stage 0: Exa API     OpenAI API      HubSpot       Quality
(CSV/Excel/   Search          Scraping      Email API      Stage 1+: Agents     Instagram       CSV          Report
 JSON/TSV)                                                  (Cost-Optimized)     Handles
+ OpenAI
Mapping
```

---

## Module Specifications

### Module 1: Smart Input Adapter (`scripts/loader.py`)
**Purpose**: Load and normalize input data from any file format (CSV, Excel, JSON, TSV) with AI-powered field mapping

**Input**: Any file format (CSV, Excel, JSON, TSV) via `--input` flag, or default `input/FB Ad library scraping.xlsx`
**Output**: `processed/01_loaded.csv`

**Features**:
- **Auto-detection**: Automatically detects file format (CSV, Excel, JSON, TSV)
- **AI-powered mapping**: Uses OpenAI GPT-4o to analyze file structure and suggest field mappings
- **Interactive verification**: CLI prompts to confirm or correct AI suggestions
- **Mapping reuse**: Saves mappings to `config/field_mappings/` for future imports
- **Backwards compatible**: Automatically detects FB Ads Library format and uses legacy loader

**Functions**:
- `detect_file_format()` - Auto-detect file format from extension
- `load_file()` - Load file with encoding detection (CSV) or format-specific loader
- `analyze_schema_with_openai()` - Use OpenAI to suggest field mappings
- `interactive_field_mapping()` - CLI prompts for user verification
- `transform_to_pipeline_schema()` - Transform user's file to pipeline schema
- `save_mapping()` / `load_mapping()` - Save/load field mappings for reuse
- `check_fb_ads_format()` - Detect FB Ads Library format for backwards compatibility
- `load_fb_ads_format()` - Use legacy loader for FB Ads Library files

**Required Field Mapping**:
- `page_name` (company/business name) - **REQUIRED**

**Optional Field Mappings**:
- `ad_count` - Number of ads/entries (default: 1)
- `total_page_likes` - Social media metrics (default: 0)
- `ad_texts` - Marketing text or descriptions (default: [""])
- `platforms` - Platforms where content appears (default: ["UNKNOWN"])
- `is_active` - Active status (default: True)
- `first_ad_date` - Date of first appearance (default: today)

**Output Schema**:
```
page_name, ad_count, total_page_likes, ad_texts[], platforms[], is_active, first_ad_date
```

**Usage**:
```bash
# Interactive mapping (first time)
python scripts/loader.py --input my_leads.csv

# Uses saved mapping (subsequent runs)
python scripts/loader.py --input my_leads.csv

# Via pipeline
python run_pipeline.py --input my_leads.csv --all
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

### Module 3.6: Contact Enricher Pipeline (`scripts/contact_enricher_pipeline.py`)
**Purpose**: Fill missing contact data using AI-powered strategies with cost-optimized multi-stage approach

**Input**: `processed/03b_hunter.csv` (from Module 3.5)
**Output**: `processed/03c_enriched.csv` (fallback enrichment), `processed/03d_final.csv` (merged final data)

**Architecture**: Multi-stage enrichment with early exit optimization

#### Stage 0: Exa API Search (`scripts/exa_enricher.py`)
**Purpose**: Fast and cost-effective contact discovery (runs first)

**Functions**:
- `search_exa(query, num_results)` - Web search via Exa API
- `extract_emails_from_text(text)` - Regex-based email extraction
- `extract_contact_name_from_text(text, company_name)` - Pattern-based name/position extraction
- `verify_email_with_hunter(email)` - Hunter.io verification for quality gate
- `enrich_with_exa(company_name, website_url)` - Main enrichment function

**Search Queries**:
```python
queries = [
    f'"{company_name}" contact email',
    f'"{company_name}" owner founder email'
]
```

**Early Exit Strategy**:
- Searches Exa API for contact pages (5 results per query)
- Extracts emails with regex patterns
- Verifies emails with Hunter.io
- **Exits early if valid email found** (status: 'valid' or 'accept_all')
- Falls back to Stage 1+ only if no valid email found

**Cost**: ~$0.001 per search + $0.01 per Hunter verification
**Success Rate**: ~30-40% (catches easy wins)

**Output Fields**:
```
email, name, position, hunter_status, hunter_score, stage_found, source, cost
```

#### Stage 1-3: OpenAI Agents (Fallback)
**Purpose**: Deep enrichment using multi-strategy AI agents (only runs if Stage 0 fails)

**Strategies**:
1. **Web Search + Extraction** - DuckDuckGo search + page scraping
2. **LinkedIn Deep Dive** - LinkedIn profile search and extraction
3. **Domain Intelligence** - WHOIS, DNS records, contact forms

**Functions**:
- `run_agent_strategy(company_name, website_url, strategy)` - Execute agent workflow
- `merge_enrichment_results(original, enriched)` - Merge new data with existing

**Cost**: ~$0.05-0.15 per contact
**Success Rate**: ~50-70% (thorough but expensive)

**Agent Tools**:
- `WebSearchTool` - DuckDuckGo search
- Custom function tools for scraping and extraction

**Dependencies**: `openai`, `requests`, `beautifulsoup4`, `anthropic` (OpenAI Agents API)

**Output Schema** (updates):
```
primary_email, contact_name, contact_position, phone, enrichment_source, enrichment_confidence, stage_found, cost
```

**Cost Optimization**:
- Stage 0 reduces costs by 90% by catching easy wins early
- Only ~60-70% of contacts need expensive Stage 1+ enrichment
- Overall cost: ~$0.03-0.05 per contact (vs $0.05-0.15 without Exa)

**Flags**:
- `--all`: Process all contacts (default: test mode)
- `--skip-exa`: Skip Stage 0 (Exa API) and go directly to agents
- `--max-cost`: Set maximum enrichment cost per contact

---

### Module 3.7: Instagram Enricher (`scripts/instagram_enricher.py`)
**Purpose**: Find and enrich Instagram handles for contacts

**Input**: `processed/03d_final.csv` (from Module 3.6)
**Output**: `processed/03d_final.csv` (updated with Instagram handles)

**Functions**:
- `enrich_contact_instagram(row)` - Enrich a single contact with Instagram handles
- `search_personal_instagram()` - Search for personal Instagram handles
- `search_company_instagram()` - Search for company Instagram handles
- `scrape_website_for_instagram()` - Scrape website for Instagram links
- `extract_instagram_handles_from_text()` - Extract handles from text with false positive filtering
- `is_valid_handle()` - Validate handle format and filter false positives
- `verify_instagram_handle()` - Verify if handle exists via HTTP request (checks page title)
- Enhanced search strategies: multi-query search, pattern generation, deep website scraping, cross-platform analysis

**Search Strategies**:
1. **Basic Enrichment**: Website scraping + OpenAI search
2. **Enhanced Search** (if handles still missing):
   - Deep website scraping (multiple pages)
   - Cross-platform analysis (LinkedIn, Facebook)
   - OpenAI reasoning search
   - Multi-query web search
   - Pattern generation and validation

**False Positive Filtering**:
- Automatically filters CSS/JS keywords (e.g., `@graph`, `@context`, `@type`, `@media`)
- Validates handle format (must start with `@`, 3-30 characters, valid username pattern)
- Removes generic Instagram pages (`@explore`, `@accounts`, etc.)

**Handle Verification** (`--verify` flag):
- Verifies handles exist by checking Instagram profile page titles
- Filters out handles that return "Profile isn't available" errors
- Adds ~1.5 seconds per handle (significant slowdown)
- **Limitation**: Instagram's anti-bot measures may serve generic pages, limiting verification effectiveness
- Handles with verification errors are included to avoid false negatives

**Output Schema** (updates):
```
instagram_handles (JSON array internally): ["@handle1", "@handle2", "@handle3"]
Note: Exported as comma-separated strings in output files: "@handle1, @handle2, @handle3"
```

**Flags**:
- `--all`: Process all contacts (default: test mode with 3 contacts)
- `--skip-enhanced`: Skip enhanced search (faster, lower coverage)
- `--verify`: Verify handles exist via HTTP requests (slower, filters invalid handles)

**Dependencies**: `openai`, `requests`, `beautifulsoup4`, `tqdm`

---

### Module 4: Exporter (`scripts/exporter.py`)
**Purpose**: Export final prospect data in usable formats

**Input**: `processed/03d_final.csv` (from Module 3.7)
**Output** (3 files total, plus versioned copies):
- `output/prospects_final.csv` - Complete prospect data (all columns, comma-separated format)
- `output/prospects_final.xlsx` - Excel version with formatting
- `output/hubspot_contacts.csv` - HubSpot-ready import file (includes instagram_handles as comma-separated)

**Functions**:
- `export_csv()` - Full data export (formats instagram_handles as comma-separated)
- `export_excel()` - Formatted Excel with columns (formats instagram_handles as comma-separated)
- `export_hubspot()` - HubSpot-ready import file (includes instagram_handles column)
- `generate_summary_report()` - Pipeline statistics

**Output Format Notes**:
- `instagram_handles` column is formatted as comma-separated strings in all exports (e.g., `"@handle1, @handle2"`)
- All exports include the `instagram_handles` column (previously missing from HubSpot CSV)
- Email drafts are now created directly in HubSpot, not exported as JSON

---

### Module 5: Validator (`scripts/validator.py`)
**Purpose**: Validate pipeline output quality and data coherence

**Input**: All pipeline output files
**Output**: Validation report (console)

**Functions**:
- `check_contact_completeness()` - Identify prospects missing email or contact
- `check_email_verification()` - Check email verification status
- `check_phone_coverage()` - Report phone number coverage
- `check_website_coverage()` - Report website enrichment coverage
- `check_instagram_handles()` - Validate Instagram handle coverage and format
- `check_hubspot_export()` - Validate HubSpot export file
- `generate_report()` - Comprehensive validation report

**Checks Performed**:
1. Missing email and contact data
2. Email verification status
3. Phone number coverage
4. Website enrichment coverage
5. Instagram handle coverage and validation (format, false positives)
6. HubSpot export file validation
7. Enrichment success rates (websites found, emails verified)

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

# Full run (all rows) with default input
python run_pipeline.py --all

# Full run with custom input file
python run_pipeline.py --input path/to/your/file.csv --all

# Resume from specific module
python run_pipeline.py --all --from 3.5
python run_pipeline.py --input my_leads.csv --all --from 3.5
```

**Pipeline Sequence**:
1. Loader → Load and normalize data (any format) with AI-powered field mapping
2. Enricher → Find company websites via search
3. Scraper → Extract contacts from websites
4. Hunter → Verify and enrich emails via Hunter.io
5. Agent Enricher → AI agent fallback enrichment
6. Instagram Enricher → Find and enrich Instagram handles
7. Exporter → Export to CSV/Excel/HubSpot CSV
8. Validator → Quality check and report

---

## File Structure

```
fb_ads_library_prospecting/
├── run_pipeline.py       # Main orchestrator
├── scripts/
│   ├── loader.py         # Module 1: Smart input adapter (any format)
│   ├── enricher.py       # Module 2: Find websites
│   ├── scraper.py        # Module 3: Scrape contacts
│   ├── hunter.py         # Module 3.5: Hunter.io emails
│   ├── exa_enricher.py   # Module 3.6 Stage 0: Fast Exa API contact discovery
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent enrichment (Exa + OpenAI)
│   ├── instagram_enricher.py  # Module 3.7: Instagram handle enrichment
│   ├── clean_instagram_handles.py  # Utility: Clean Instagram handles
│   ├── find_missing_instagram.py    # Utility: Find missing Instagram handles
│   ├── exporter.py       # Module 4: Export outputs
│   ├── validator.py      # Module 5: Quality check
│   ├── fb_ads_scraper.py # Facebook Ads Library scraper
│   ├── apify_dm_sender.py # Instagram DM sender (Apify)
│   ├── manychat_sender.py # Instagram DM sender (ManyChat)
│   └── _archived/        # Archived scripts (legacy versions)
├── input/
│   └── FB Ad library scraping.xlsx  # Default input (any format supported)
├── processed/
│   ├── 01_loaded.csv
│   ├── 02_enriched.csv
│   ├── 03_contacts.csv
│   ├── 03b_hunter.csv
│   ├── 03c_enriched.csv
│   ├── 03d_final.csv     # Final data (input for Exporter)
│   └── legacy/           # Old processed files
├── output/
│   ├── prospects_final.csv
│   ├── prospects_final.xlsx
│   ├── hubspot_contacts.csv
│   └── legacy/           # Old output files
├── config/
│   ├── website_overrides.csv
│   ├── manual_contacts.csv
│   ├── do_not_contact.csv
│   └── field_mappings/    # Saved field mapping configurations (JSON)
├── tests/                # Unit and integration tests
│   ├── test_instagram_enrichment.py
│   └── test_pipeline_integration.py
├── requirements.txt
├── .env
├── README.md
└── PRD.md
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

## Configuration

### Environment Variables (.env)
```
OPENAI_API_KEY=sk-...
HUNTER_API_KEY=...
```

### Website Overrides (config/website_overrides.csv)
Manually specify websites for prospects that can't be found automatically.

### Manual Contacts (config/manual_contacts.csv)
Add manually researched contacts.

**Note**: Email drafts are now created directly in HubSpot, not generated by the pipeline.

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

# Terminal 4 - Agent Enricher
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 3.6 (contact_enricher_pipeline.py). File: scripts/contact_enricher_pipeline.py only."

# Terminal 5 - Instagram Enricher
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 3.7 (instagram_enricher.py). File: scripts/instagram_enricher.py only."

# Terminal 6 - Exporter
cd /Users/tomas/Desktop/ai_pbx/fb_ads_library_prospecting
claude "Read PRD.md. Build Module 4 (exporter.py). File: scripts/exporter.py only."
```

### File Ownership (No Conflicts)

| Terminal | Owns | Never Touch |
|----------|------|-------------|
| T1 | `scripts/loader.py` | enricher, scraper, hunter, contact_enricher, instagram_enricher, exporter |
| T2 | `scripts/enricher.py` | loader, scraper, hunter, contact_enricher, instagram_enricher, exporter |
| T3 | `scripts/scraper.py` | loader, enricher, hunter, contact_enricher, instagram_enricher, exporter |
| T3.5 | `scripts/hunter.py` | loader, enricher, scraper, contact_enricher, instagram_enricher, exporter |
| T3.6 | `scripts/contact_enricher_pipeline.py` | loader, enricher, scraper, hunter, instagram_enricher, exporter |
| T3.7 | `scripts/instagram_enricher.py` | loader, enricher, scraper, hunter, contact_enricher, exporter |
| T4 | `scripts/exporter.py` | loader, enricher, scraper, hunter, contact_enricher, instagram_enricher |

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
hunter_emails[],email_confidence,email_verified
```

**03c_enriched.csv adds:**
```
pipeline_email,pipeline_phone,pipeline_name,pipeline_position,pipeline_confidence,enrichment_stage,enrichment_cost,enrichment_source
```

**03d_final.csv adds:**
```
instagram_handles (JSON array): ["@handle1", "@handle2", "@handle3"]
```

**Note**: The `instagram_handles` column contains all Instagram handles (both personal and company) in a single JSON array. False positives are automatically filtered. The `contact_instagram_handle` column was removed and consolidated into `instagram_handles`.

### Module Test Commands

Each module must include a `if __name__ == "__main__":` block for standalone testing:

```bash
# Test each module independently
python scripts/loader.py                    # Creates processed/01_loaded.csv (default input)
python scripts/loader.py --input my_file.csv  # Interactive mapping for custom input
python scripts/enricher.py    # Reads 01, creates 02 (test with 3 rows)
python scripts/scraper.py     # Reads 02, creates 03 (test with 3 rows)
python scripts/hunter.py       # Reads 03, creates 03b (test with 3 rows)
python scripts/contact_enricher_pipeline.py  # Reads 03b, creates 03c (test with 3 rows)
python scripts/instagram_enricher.py  # Reads 03d, updates 03d (test with 3 rows)
python scripts/exporter.py    # Reads 03d, creates output files
python scripts/validator.py   # Validates all output files
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
