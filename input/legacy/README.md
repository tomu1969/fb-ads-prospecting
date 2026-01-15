# Archived Input Files

This folder contains older input files that have been processed and are no longer actively used.

## Archived on 2026-01-14

### Facebook Ads Scraper Outputs
- `fb_ads_scraped_20260105_143549.csv` - Early scrape (Jan 5, 13:35)
- `fb_ads_scraped_20260105_161625.csv` - Intermediate scrape (Jan 5, 16:16)
- `FB Ad library scraping.csv` - Original manual scrape (pre-pipeline)
- `FB Ad library scraping.xlsx` - Excel version of manual scrape

**Current files**:
- `input/fb_ads_scraped_20260105_162923.csv` (most recent FB ads scrape)
- `input/hubspot_leads.csv` (CRM export)
- `input/top_real_estate_agents_socials.csv` (Jan 13, most recent)

## Recovery

These files are NOT in git (in .gitignore). To use an archived file:
```bash
cp input/legacy/[filename] input/[filename]
python run_pipeline.py --input input/[filename] --all
```
