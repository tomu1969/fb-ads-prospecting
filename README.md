# Facebook Ads Library Prospecting Pipeline

Automated pipeline to convert Facebook Ads Library data into qualified prospects with verified contact information, ready for HubSpot import.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys
cp .env.example .env
# Edit .env with your OPENAI_API_KEY and HUNTER_API_KEY

# Run full pipeline
python run_pipeline.py --all

# Or run in test mode (3 rows)
python run_pipeline.py
```

## Pipeline Overview

```
Module 1    Module 2    Module 3    Module 3.5   Module 3.6      Module 4    Module 5
Loader  →  Enricher →  Scraper  →   Hunter   → Agent Enricher → Exporter → Validator
  │           │           │            │             │              │           │
Excel      Search      Scrape      Hunter.io    OpenAI Agents   HubSpot     Quality
           Websites    Contacts    Emails       (fallback)      CSV         Report
```

## Project Structure

```
fb_ads_library_prospecting/
├── run_pipeline.py           # Main orchestrator
├── scripts/
│   ├── loader.py             # Module 1: Load Excel data
│   ├── enricher.py           # Module 2: Find company websites
│   ├── scraper.py            # Module 3: Scrape contacts from websites
│   ├── hunter.py             # Module 3.5: Hunter.io email/phone enrichment
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent fallback enrichment
│   ├── exporter.py           # Module 4: Export to HubSpot CSV
│   ├── validator.py          # Module 5: Quality validation
│   └── legacy/               # Archived scripts
├── input/                    # Source Excel file
├── processed/                # Intermediate CSV files
│   └── legacy/               # Old processed files
├── output/                   # Final export files
│   └── legacy/               # Old output files
└── config/                   # Settings and overrides
```

## Usage

### Run Full Pipeline
```bash
python run_pipeline.py --all
```

### Run Individual Modules
```bash
python scripts/loader.py
python scripts/enricher.py --all
python scripts/scraper.py --all
python scripts/hunter.py --all
python scripts/contact_enricher_pipeline.py --all
python scripts/exporter.py
python scripts/validator.py
```

### Resume from Specific Module
```bash
python run_pipeline.py --all --from 3.5   # Resume from Hunter
python run_pipeline.py --all --from 3.6   # Resume from Agent Enricher
python run_pipeline.py --all --from 4     # Resume from Exporter
```

## Output Files

| File | Description |
|------|-------------|
| `output/hubspot_contacts.csv` | HubSpot-ready import file (contacts with email) |
| `output/prospects_final.csv` | Complete prospect data (all columns) |
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

## Current Pipeline Results (December 2025)

| Metric | Count | % |
|--------|-------|---|
| Total Prospects | 59 | 100% |
| Ready for HubSpot (has email) | 48 | 81.4% |
| Verified emails | 37 | 62.7% |
| With phone number | 39 | 66.1% |
| Complete (email + contact + phone) | 23 | 39.0% |
| Quality score | - | 59.0% |

### Enrichment Sources

| Source | Count | Description |
|--------|-------|-------------|
| Hunter.io | 46 | Primary email enrichment |
| Agent v2.1 | 7 | AI agent strict search |
| Agent v3.1 | 2 | AI agent iterative search |
| Manual | 10 | Manual overrides |
| Unfound | 11 | No email found |

## Pipeline Data Flow

```
01_loaded.csv      → Raw data from Excel (59 advertisers)
02_enriched.csv    → + website_url, linkedin_url, search_confidence
03_contacts.csv    → + emails, phones from website scraping
03b_hunter.csv     → + primary_email, contact_name, email_verified from Hunter
03c_enriched.csv   → Agent enrichment results (for unfound contacts)
03d_final.csv      → Merged final data (input for Exporter)
```

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
- Increase delays in scripts if needed

## API Requirements

| Service | Purpose | Free Tier |
|---------|---------|-----------|
| OpenAI | Agent enrichment, website analysis | Pay per use |
| Hunter.io | Email finding/verification | 25 searches/month |
| DuckDuckGo | Website search | Unlimited (rate limited) |

## License

Internal use only - LaHaus AI
