# Prospecting Pipeline

Automated pipeline to convert lead data (from any source) into qualified prospects with verified contact information, ready for HubSpot import. Supports CSV, Excel, JSON, and TSV input formats with AI-powered field mapping.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys
cp .env.example .env
# Edit .env with your OPENAI_API_KEY and HUNTER_API_KEY

# Run full pipeline with default input file
python run_pipeline.py --all

# Or use a custom input file (CSV, Excel, JSON, TSV)
python run_pipeline.py --input path/to/your/file.csv --all

# Or run in test mode (3 rows)
python run_pipeline.py
```

## Pipeline Overview

```
Module 1    Module 2    Module 3    Module 3.5   Module 3.6      Module 3.7        Module 4    Module 5
Loader  →  Enricher →  Scraper  →   Hunter   → Agent Enricher → Instagram Enricher → Exporter → Validator
  │           │           │            │             │                 │              │           │
Smart      Search      Scrape      Hunter.io    OpenAI Agents      OpenAI API      HubSpot     Quality
Adapter    Websites    Contacts    Emails       (fallback)        Instagram       CSV         Report
(Any       (DuckDuckGo)             (Hunter.io)                    (OpenAI API)     (HubSpot)   (Quality
Format)                                                                                Import)    Report)
```
```

## Project Structure

```
fb_ads_library_prospecting/
├── run_pipeline.py           # Main orchestrator
├── scripts/
│   ├── loader.py             # Module 1: Smart input adapter (any format)
│   ├── enricher.py           # Module 2: Find company websites
│   ├── scraper.py            # Module 3: Scrape contacts from websites
│   ├── hunter.py             # Module 3.5: Hunter.io email/phone enrichment
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent fallback enrichment
│   ├── instagram_enricher.py # Module 3.7: Instagram handle enrichment
│   ├── clean_instagram_handles.py  # Utility: Clean and consolidate Instagram handles
│   ├── exporter.py           # Module 4: Export to HubSpot CSV
│   ├── validator.py          # Module 5: Quality validation
│   └── legacy/               # Archived scripts (including loader_fb_ads.py)
├── input/                    # Source files (CSV, Excel, JSON, TSV)
├── processed/                # Intermediate CSV files
│   └── legacy/               # Old processed files
├── output/                   # Final export files
│   └── legacy/               # Old output files
└── config/                   # Settings and overrides
    └── field_mappings/       # Saved field mapping configurations
```
```

## Usage

### Run Full Pipeline
```bash
# With default input file
python run_pipeline.py --all

# With custom input file
python run_pipeline.py --input path/to/your/file.csv --all
```

### Run Individual Modules
```bash
# Module 1: Smart Loader (interactive field mapping)
python scripts/loader.py
python scripts/loader.py --input path/to/your/file.csv

# Other modules
python scripts/enricher.py --all
python scripts/scraper.py --all
python scripts/hunter.py --all
python scripts/contact_enricher_pipeline.py --all
python scripts/instagram_enricher.py --all
python scripts/instagram_enricher.py --all --verify  # With handle verification (slower)
python scripts/exporter.py
python scripts/validator.py
```

### Resume from Specific Module
```bash
python run_pipeline.py --all --from 3.5   # Resume from Hunter
python run_pipeline.py --all --from 3.6   # Resume from Agent Enricher
python run_pipeline.py --all --from 3.7   # Resume from Instagram Enricher
python run_pipeline.py --all --from 4     # Resume from Exporter
```

### Smart Input Adapter (Module 1)

The pipeline is **input-agnostic** - it accepts **any input file format** (CSV, Excel, JSON, TSV) and uses OpenAI to intelligently map your columns to the pipeline's required schema. This means you can use leads from:
- LinkedIn exports
- CRM exports (Salesforce, HubSpot, etc.)
- Manual CSV files
- Facebook Ads Library data (original use case)
- Any other lead source

**Features:**
- **Auto-detection**: Automatically detects file format (CSV, Excel, JSON, TSV)
- **AI-powered mapping**: Uses OpenAI GPT-4o to analyze your file structure and suggest field mappings
- **Interactive verification**: CLI prompts to confirm or correct AI suggestions
- **Mapping reuse**: Saves mappings to `config/field_mappings/` for future imports of the same file format
- **Backwards compatible**: Automatically detects FB Ads Library format and uses optimized legacy loader

**Example:**
```bash
# First time: Interactive mapping (AI suggests, you verify)
python scripts/loader.py --input my_leads.csv

# Next time: Uses saved mapping automatically (no prompts needed)
python scripts/loader.py --input my_leads.csv

# Via full pipeline
python run_pipeline.py --input my_leads.csv --all
```

**Supported Input Formats:**
- CSV (`.csv`) - Most common, works with any delimiter
- Excel (`.xlsx`, `.xls`) - Supports multiple sheets
- JSON (`.json`) - Array of objects or nested structures
- TSV (`.tsv`) - Tab-separated values

**Required Field:**
- `page_name` (company/business name) - **REQUIRED** - Must be mapped from your input file

**Optional Fields (can be mapped or will use defaults):**
- `ad_count` - Number of ads/entries (default: 1)
- `total_page_likes` - Social media metrics (default: 0)
- `ad_texts` - Marketing text or descriptions (default: [""])
- `platforms` - Platforms (Facebook, Instagram, etc.) (default: ["UNKNOWN"])
- `is_active` - Active status (default: True)
- `first_ad_date` - Date of first appearance (default: today)

## Output Files

The pipeline generates **3 output files** (plus versioned copies with timestamps):

| File | Description |
|------|-------------|
| `output/hubspot_contacts.csv` | HubSpot-ready import file (contacts with email) |
| `output/prospects_final.csv` | Complete prospect data (all columns, comma-separated format) |
| `output/prospects_final.xlsx` | Excel version with formatting |

### HubSpot CSV Columns

| Column | HubSpot Property | Source |
|--------|------------------|--------|
| `email` | Email (unique ID) | Hunter.io / Agent Enricher |
| `firstname` | First Name | Parsed from contact_name |
| `lastname` | Last Name | Parsed from contact_name |
| `company` | Company | page_name |
| `jobtitle` | Job Title | contact_position |
| `website` | Website | website_url |
| `phone` | Phone | Scraper / Hunter.io |
| `fb_ad_count` | Custom: FB Ad Count | Source data |
| `fb_page_likes` | Custom: FB Page Likes | Source data |
| `ad_platforms` | Custom: Ad Platforms | Source data |
| `email_verified` | Custom: Email Verified | Hunter.io verification |
| `instagram_handles` | Custom: Instagram Handles | Instagram Enricher (comma-separated: "@handle1, @handle2") |
| `linkedin_url` | Custom: LinkedIn URL | Enricher / Scraper |
| `enrichment_source` | Custom: Enrichment Source | Tracking field |

## Configuration

### Environment Variables (.env)
```
OPENAI_API_KEY=sk-...
HUNTER_API_KEY=...
```

### Website Overrides (config/website_overrides.csv)
Manually specify websites for prospects that can't be found automatically:
```csv
page_name,website_url
"Company Name","https://company.com"
```

### Manual Contacts (config/manual_contacts.csv)
Add manually researched contacts:
```csv
page_name,primary_email,contact_name,contact_position
"Company Name","email@company.com","John Doe","Broker"
```

## Example Pipeline Results

The pipeline processes any input file format and enriches prospects with contact information. Results vary based on input data quality and size.

**Typical Enrichment Metrics:**
- Website discovery rate: ~80-90%
- Email discovery rate: ~70-85%
- Phone number coverage: ~60-75%
- Instagram handle coverage: ~85-95%

### Enrichment Sources

| Source | Description |
|--------|-------------|
| Hunter.io | Primary email enrichment and verification |
| Agent Enricher | AI agent fallback for missing contacts |
| Website Scraping | Direct extraction from company websites |
| Manual Overrides | Config files for manual additions |
| Instagram Enricher | Multi-strategy Instagram handle discovery |

## Pipeline Data Flow

```
01_loaded.csv      → Raw data from input file (any format: CSV, Excel, JSON, TSV)
02_enriched.csv    → + website_url, linkedin_url, search_confidence
03_contacts.csv    → + emails, phones from website scraping
03b_hunter.csv     → + primary_email, contact_name, email_verified from Hunter
03c_enriched.csv   → Agent enrichment results (for unfound contacts)
03d_final.csv      → Merged final data (input for Instagram Enricher)
03d_final.csv      → + instagram_handles (JSON array) from Instagram Enricher
                    → Final data (input for Exporter)
```

**Note:** Instagram handles are stored internally as JSON arrays: `["@handle1", "@handle2", "@handle3"]`, but are exported as **comma-separated strings** in all output files (e.g., `"@handle1, @handle2, @handle3"`). The `instagram_handles` column is included in all exports, including the HubSpot CSV. Both personal and company handles are consolidated into this single column. False positives (CSS/JS keywords) are automatically filtered out.

## Troubleshooting

### No emails found for a prospect
1. Check if website was found in `02_enriched.csv`
2. Try adding website manually to `config/website_overrides.csv`
3. Re-run from Hunter module: `python run_pipeline.py --from 3.5 --all`

### Missing phone numbers
1. Phone numbers are extracted from websites (Scraper) and Hunter.io
2. Agent Enricher also searches for phone numbers as fallback
3. Consider adding to `config/manual_contacts.csv`

### Rate limiting errors
- Enricher: Uses 2-second delays between requests
- Hunter: Uses 1-second delays
- Agent Enricher: Uses OpenAI API with built-in rate limiting
- Instagram Enricher: Uses 2-second delays between searches
- Increase delays in scripts if needed

### Instagram handles not found
1. Instagram Enricher uses multiple strategies: website scraping, OpenAI search, and enhanced search
2. Handles are automatically filtered to remove false positives (CSS/JS keywords)
3. All handles are consolidated into `instagram_handles` column (comma-separated in exports)
4. Use `--verify` flag to verify handles exist: `python scripts/instagram_enricher.py --all --verify`
   - **Note**: Verification is slower (~1.5s per handle) and may be limited by Instagram's anti-bot measures
5. Run cleanup script if needed: `python scripts/clean_instagram_handles.py`

## API Requirements

| Service | Purpose | Free Tier |
|---------|---------|-----------|
| OpenAI | Agent enrichment, website analysis, Instagram search | Pay per use |
| Hunter.io | Email finding/verification | 25 searches/month |
| DuckDuckGo | Website search | Unlimited (rate limited) |

## License

Internal use only - LaHaus AI
