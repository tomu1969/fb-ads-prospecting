# FB Ads Prospecting Pipeline

Lead generation pipeline that scrapes Facebook Ad Library, enriches contacts, and enables Instagram DM outreach.

## Quick Commands

```bash
# Run full pipeline
python run_pipeline.py --input input/your_file.csv

# Scrape Facebook Ads Library
python scripts/fb_ads_scraper.py --search "real estate Miami" --limit 100

# Send Instagram DMs
python scripts/apify_dm_sender.py --csv output/prospects_final.csv --message "Hi {contact_name}!" --dry-run
```

## Project Structure

```
├── run_pipeline.py          # Main orchestrator
├── scripts/
│   ├── loader.py            # Module 1: Smart input adapter
│   ├── enricher.py          # Module 2: Website discovery
│   ├── scraper.py           # Module 3: Contact extraction
│   ├── hunter.py            # Module 3.5: Email verification
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent enrichment
│   ├── instagram_enricher.py # Module 3.7: Instagram handle discovery
│   ├── exporter.py          # Module 4: CSV/Excel/HubSpot export
│   ├── validator.py         # Module 5: Quality validation
│   ├── fb_ads_scraper.py    # Facebook Ads Library scraper
│   ├── apify_dm_sender.py   # Instagram DM sender (Apify)
│   └── manychat_sender.py   # Instagram DM sender (ManyChat)
├── config/
│   ├── field_mappings/      # Auto-generated field mappings
│   ├── website_overrides.csv # Manual website corrections
│   └── do_not_contact.csv   # Exclusion list
├── input/                   # Raw input files
├── processed/               # Intermediate pipeline outputs
└── output/                  # Final exports
```

## Pipeline Stages

1. **Loader** - Reads CSV/Excel, AI-maps fields to standard schema
2. **Enricher** - Discovers websites via DuckDuckGo search
3. **Scraper** - Extracts contacts from websites
4. **Hunter** - Verifies emails via Hunter.io
5. **Contact Enricher** - AI agent fallback for missing data
6. **Instagram Enricher** - Finds Instagram handles
7. **Exporter** - Outputs CSV/Excel/HubSpot format

## Key Files

- `run_pipeline.py` - Entry point, CLI options, module orchestration
- `scripts/loader.py` - AI field mapping logic
- `scripts/apify_dm_sender.py` - Instagram DM automation
- `README.md` - Full documentation
- `PRD.md` - Technical architecture

## Data Flow

```
input/*.csv → processed/01_loaded.csv → 02_enriched.csv → 03_contacts.csv
    → 03b_hunter.csv → 03d_final.csv → output/prospects_final.csv
```
