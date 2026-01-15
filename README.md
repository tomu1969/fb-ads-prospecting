# FB Ads Prospecting Pipeline

Automated pipeline to scrape Facebook advertisers, enrich leads with contact information, and send cold emails and Instagram DMs for outreach. Supports CSV, Excel, JSON, and TSV input formats with AI-powered field mapping.

## Repository Overview

This repository contains four main components:

1. **FB Ads Scraper** - Scrape advertisers from Facebook Ad Library
2. **Enrichment Pipeline** - Find websites, emails, phones, and Instagram handles
3. **Email Outreach** - Draft personalized emails, verify deliverability, and send via Gmail
4. **DM Senders** - Send personalized Instagram messages via Apify or ManyChat

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys
cp .env.example .env
# Edit .env with your API keys (see API Requirements section)

# Option 1: Full workflow (scrape → enrich → outreach)
python scripts/fb_ads_scraper.py --query "real estate miami" --count 100
python run_pipeline.py --input output/fb_ads_scraped_*.csv --all
python scripts/apify_dm_sender.py --csv output/prospects_final.csv --message "Hi {contact_name}!" --dry-run

# Option 2: Run pipeline with existing data
python run_pipeline.py --all
```

---

## Facebook Ads Scraper

Scrapes Facebook advertisers from the Ad Library using the Apify actor `dz_omar/facebook-ads-scraper-pro`.

### Features
- Interactive mode with search preview
- Multiple ad type filters (all, housing, political, employment, credit)
- Country-level location filtering
- Status filtering (active/inactive/all)
- Deduplication with history tracking
- Housing ads workaround for broken category filter

### Usage

```bash
# Interactive mode
python scripts/fb_ads_scraper.py

# CLI mode - basic search
python scripts/fb_ads_scraper.py --query "real estate miami" --count 100

# Search with filters
python scripts/fb_ads_scraper.py \
  --query "real estate" \
  --location us \
  --ad-type all \
  --status active \
  --count 200

# Housing ads (uses keyword workaround)
python scripts/fb_ads_scraper.py --ad-type housing_ads --query "miami" --count 50

# Browse by filters only (no keywords)
python scripts/fb_ads_scraper.py --location us --ad-type political_and_issue_ads

# Dry run (preview config)
python scripts/fb_ads_scraper.py --query "test" --count 50 --dry-run

# Force re-scrape (ignore deduplication)
python scripts/fb_ads_scraper.py --query "test" --force
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--query` | Search keywords | (none) |
| `--location` | Location preset (us, uk, canada, etc.) | us |
| `--country` | ISO country code | US |
| `--ad-type` | all, housing_ads, political_and_issue_ads, employment_ads, credit_ads | all |
| `--status` | active, inactive, all | active |
| `--media` | all, video, image | all |
| `--count` | Max results to fetch | 500 |
| `--start-date` | Start date (YYYY-MM-DD) | (none) |
| `--end-date` | End date (YYYY-MM-DD) | (none) |
| `--output` | Custom output path | auto-generated |
| `--dry-run` | Preview config without scraping | false |
| `--force` | Ignore deduplication | false |
| `--list-locations` | Show available location presets | - |

### Output Format

The scraper outputs a CSV compatible with the enrichment pipeline:

| Column | Description |
|--------|-------------|
| `page_name` | Advertiser name |
| `page_id` | Facebook page ID |
| `page_likes` | Page follower count |
| `page_url` | Facebook page URL |
| `page_category` | Business category |
| `ad_count` | Number of ads found |
| `text` | Ad text content |
| `platforms` | Platforms (Facebook, Instagram, etc.) |
| `is_active` | Active status |
| `start_date` | Ad start date |
| `link_urls` | Destination URLs from ads |

### Query Strategy Tips

For best results:
- **Use broad keywords** (e.g., "real estate") instead of city-specific terms
- **Avoid duplicate queries** like "miami real estate" and "real estate miami" (same results)
- **Use country filter** instead of keywords for location targeting
- **Start broad, then narrow** - generic queries first, specific ones later

### Housing Ads Note

The Apify actor has a bug where `HOUSING_ADS` category returns 0 results. The scraper automatically applies a workaround:
- Changes `ad_type` to `all`
- Adds "real estate" keyword to filter results

---

## Enrichment Pipeline

Converts lead data into qualified prospects with verified contact information.

### Pipeline Flow

```
Module 1    Module 2    Module 3    Module 3.5   Module 3.6            Module 3.7
Loader  →  Enricher →  Scraper  →   Hunter   → Agent Enricher    → Instagram Enricher →
  │           │           │            │          (Exa + OpenAI)          │
Smart      Search      Scrape      Hunter.io   Stage 0: Exa API      OpenAI/Groq
Adapter    Websites    Contacts    Emails      Stage 1+: Agents      Instagram

   Module 3.8           Module 3.9        Module 4    Module 5
→ Contact Name      → LinkedIn        → Exporter → Validator
   Resolver            Enricher            │           │
      │                   │             HubSpot     Quality
  Multi-source         Exa API           CSV        Report
   Name Finder        Profiles
```

#### Module 3.6: Contact Enricher Details

The Contact Enricher uses a multi-stage approach for cost-effective enrichment:

**Stage 0 - Exa API (Fast & Cheap)**
- Web search via Exa API for contact pages
- Email extraction with regex patterns
- Hunter.io verification for quality gate
- Early exit if valid email found (~$0.001 per search)

**Stage 1+ - OpenAI Agents (Thorough)**
- Multi-strategy agent workflows
- Deep web scraping and analysis
- Manual enrichment requests
- Used only when Stage 0 fails (~$0.05-0.15 per contact)

This tiered approach reduces costs by 90% while maintaining high success rates.

### Usage

```bash
# Interactive mode (prompts for everything)
python run_pipeline.py

# Full run with file selection
python run_pipeline.py --all

# Custom input file
python run_pipeline.py --input path/to/leads.csv --all

# Test mode (3 rows only)
python run_pipeline.py --test

# Resume from specific module
python run_pipeline.py --from 3.5 --all   # Resume from Hunter
python run_pipeline.py --from 3.7 --all   # Resume from Instagram Enricher

# Speed modes
python run_pipeline.py --all --fast        # Hunter only (faster)
python run_pipeline.py --all --speed-full  # Hunter + AI agents (thorough)
```

### Run Individual Modules

```bash
python scripts/loader.py --input file.csv     # Smart loader with AI mapping
python scripts/enricher.py --all              # Find company websites
python scripts/scraper.py --all               # Scrape contacts from sites
python scripts/hunter.py --all                # Hunter.io email lookup
python scripts/contact_enricher_pipeline.py --all  # AI fallback enrichment
python scripts/instagram_enricher.py --all    # Find Instagram handles
python scripts/contact_name_resolver.py --all # Resolve contact names
python scripts/linkedin_enricher.py --all     # Find LinkedIn profiles
python scripts/exporter.py                    # Export to HubSpot format
python scripts/validator.py                   # Quality validation report
```

### Data Files

```
processed/01_loaded.csv     → Standardized input data
processed/02_enriched.csv   → + website_url, linkedin_url
processed/03_contacts.csv   → + scraped emails, phones
processed/03b_hunter.csv    → + Hunter.io verified emails
processed/03d_final.csv     → + Instagram handles
processed/03e_names.csv     → + resolved contact names
processed/03f_linkedin.csv  → + LinkedIn profiles (final)

output/prospects_master.csv      → Primary contact database
output/prospects_master.xlsx     → Excel version
output/hubspot/contacts.csv      → HubSpot-ready import file
output/email_campaign/drafts.csv → Email drafts for sending
```

### Enrichment Results

| Metric | Typical Rate |
|--------|--------------|
| Website discovery | 80-90% |
| Email discovery | 70-85% |
| Phone coverage | 60-75% |
| Instagram handles | 85-95% |
| Contact names | 90-95% |
| LinkedIn profiles | 60-80% |

---

## Email Verification & Sending

Advanced email verification and cold email sending with bounce recovery.

### Email Verification (MillionVerifier)

Verify emails with 99%+ accuracy, including catch-all detection:

```bash
# Verify single email
python scripts/email_verifier/verifier.py --email test@example.com

# Verify CSV of emails (with scoring)
python scripts/email_verifier/verifier.py \
  --csv output/email_campaign/drafts.csv \
  --output output/email_campaign/verified_drafts.csv

# Show detailed analysis
python scripts/email_verifier/verifier.py --email test@example.com --verbose
```

Features:
- Catch-all domain detection (avoids sending to catch-all addresses)
- Multi-factor scoring (confidence, deliverability, role-based, free provider)
- Generic email detection (info@, sales@, etc.)
- Bulk verification with progress tracking
- API: MillionVerifier (99%+ accuracy, ~$0.50 per 1,000 verifications)

### Gmail Sender (SMTP)

Send cold emails via Gmail with advanced verification:

```bash
# Dry run (preview without sending)
python scripts/gmail_sender/gmail_sender.py \
  --csv output/email_campaign/verified_drafts.csv \
  --dry-run \
  --limit 5

# Send emails (with built-in verification)
python scripts/gmail_sender/gmail_sender.py \
  --csv output/email_campaign/verified_drafts.csv \
  --limit 50 \
  --delay 10

# Resume from failure
python scripts/gmail_sender/gmail_sender.py \
  --csv output/email_campaign/verified_drafts.csv \
  --resume
```

Features:
- Dry-run mode for testing
- Resume capability (tracks sent emails)
- Built-in email verification (MillionVerifier + Hunter.io)
- Multi-factor send scoring (skip risky emails)
- Customizable delays between sends
- Comprehensive logging (console + file)
- Gmail App Password authentication (no OAuth required)

CSV Format Requirements:
- Required columns: `email`, `subject`, `body`
- Optional columns: `contact_name`, `company_name` (for personalization)

### Bounce Recovery

Recover bounced contacts by finding alternative emails:

```bash
# Recover from bounced contacts CSV
python scripts/bounce_recovery/bounce_recovery.py \
  --input config/bounced_contacts.csv

# With specific strategies
python scripts/bounce_recovery/bounce_recovery.py \
  --input config/bounced_contacts.csv \
  --output output/email_campaign/recovered_contacts.csv \
  --strategies 0,1,2,3
```

Recovery Strategies:
1. **Strategy 0**: Re-verify original email (sometimes bounces are temporary)
2. **Strategy 1**: Try generic patterns (info@, contact@, hello@, sales@)
3. **Strategy 2**: Search Hunter.io for alternative contacts at same domain
4. **Strategy 3**: Search Apollo.io B2B database for alternative contacts

CSV Format Requirements:
- Required columns: `email`, `company_name`, `website_url`
- Optional: `contact_name`, `bounce_reason`

---

## Instagram DM Senders

Send personalized Instagram messages to enriched prospects.

### Apify DM Sender (Recommended)

```bash
# Preview messages (dry run)
python scripts/apify_dm_sender.py \
  --csv output/prospects_final.csv \
  --message "Hey {contact_name} — quick question about {company_name}..." \
  --dry-run

# Send to all contacts
python scripts/apify_dm_sender.py \
  --csv output/prospects_final.csv \
  --message "Hi {contact_name}! Interested in discussing {company_name}?"

# Send with exclusions and limits
python scripts/apify_dm_sender.py \
  --csv output/prospects_final.csv \
  --message "Your message here" \
  --limit 20 \
  --exclude handle1 handle2 \
  --first-handle-only

# Custom delay between sends
python scripts/apify_dm_sender.py \
  --csv output/prospects_final.csv \
  --message "Your message" \
  --delay 3.0
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--csv` | Path to CSV with instagram_handles | (required) |
| `--message` | Message template | (required) |
| `--dry-run` | Preview without sending | false |
| `--limit` | Max rows to process | (all) |
| `--delay` | Seconds between sends | 2.0 |
| `--exclude` | Handles to skip | (none) |
| `--first-handle-only` | Only first handle per row | false |

### Message Template Variables

| Variable | Description | Fallback |
|----------|-------------|----------|
| `{contact_name}` | Contact's first name | "there" |
| `{company_name}` | Company/page name | "your company" |
| `{instagram_handle}` | Instagram handle | - |

### ManyChat Sender (Alternative)

```bash
python scripts/manychat_sender.py \
  --csv output/hubspot/contacts.csv \
  --message "Hi {contact_name}! Interested in {company_name}?" \
  --dry-run
```

### Output

Results saved to `output/apify_dm_results_TIMESTAMP.csv`:
- `instagram_handle`, `contact_name`, `company_name`
- `message`, `status` (sent/error/dry_run)
- `error`, `timestamp`

---

## Complete Workflow Example

```bash
# Step 1: Scrape Facebook advertisers
python scripts/fb_ads_scraper.py \
  --query "real estate miami" \
  --ad-type all \
  --status active \
  --count 100

# Step 2: Run enrichment pipeline
python run_pipeline.py \
  --input output/fb_ads_scraped_*.csv \
  --all

# Step 3: Draft personalized emails (→ output/email_campaign/drafts.csv)
python scripts/email_drafter/drafter.py \
  --input output/prospects_final.csv \
  --limit 5

# Step 4: Verify emails (catch-all detection with MillionVerifier)
python scripts/email_verifier/verifier.py \
  --csv output/email_campaign/drafts.csv \
  --output output/email_campaign/verified_drafts.csv

# Step 5: Send emails via Gmail (dry-run first!)
python scripts/gmail_sender/gmail_sender.py \
  --csv output/email_campaign/verified_drafts.csv \
  --dry-run \
  --limit 5

# Step 6: Send emails for real
python scripts/gmail_sender/gmail_sender.py \
  --csv output/email_campaign/verified_drafts.csv \
  --limit 50

# Step 7 (if bounces occur): Recover bounced contacts (→ output/email_campaign/)
python scripts/bounce_recovery/bounce_recovery.py \
  --input config/bounced_contacts.csv

# Alternative: Send Instagram DMs instead
python scripts/apify_dm_sender.py \
  --csv output/prospects_final.csv \
  --message "Hey {contact_name} — saw your ads for {company_name}. Quick question..." \
  --first-handle-only
```

---

## Project Structure

```
fb-ads-prospecting/
├── run_pipeline.py              # Main pipeline orchestrator
├── scripts/
│   ├── fb_ads_scraper.py        # Facebook Ad Library scraper
│   ├── loader.py                # Module 1: Smart input adapter
│   ├── enricher.py              # Module 2: Website discovery
│   ├── scraper.py               # Module 3: Contact extraction
│   ├── hunter.py                # Module 3.5: Hunter.io enrichment
│   ├── exa_enricher.py          # Module 3.6 Stage 0: Exa API contact discovery
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent fallback (Exa + OpenAI)
│   ├── apollo_enricher.py       # Module 3.6.5: Apollo.io B2B contact database
│   ├── instagram_enricher.py    # Module 3.7: Instagram handles
│   ├── contact_name_resolver.py # Module 3.8: Multi-source name finder
│   ├── linkedin_enricher.py     # Module 3.9: LinkedIn profile finder (Exa)
│   ├── exporter.py              # Module 4: HubSpot export
│   ├── validator.py             # Module 5: Quality validation
│   ├── apify_dm_sender.py       # Instagram DM sender (Apify)
│   ├── manychat_sender.py       # Instagram DM sender (ManyChat)
│   ├── email_drafter/           # Email drafting module
│   │   ├── drafter.py           # Main orchestrator
│   │   ├── researcher.py        # Prospect research via Exa
│   │   ├── analyzer.py          # Hook selection logic
│   │   └── composer.py          # Email generation
│   ├── email_verifier/          # MillionVerifier email verification
│   │   ├── verifier.py          # Email verification with catch-all detection
│   │   └── scorer.py            # Multi-factor scoring for send decisions
│   ├── gmail_sender/            # Gmail email sender (SMTP with app password)
│   │   └── gmail_sender.py      # Send cold emails via Gmail
│   ├── smtp_verifier/           # SMTP email verifier (no external API)
│   │   └── smtp_verifier.py     # Verify emails via SMTP RCPT TO
│   ├── bounce_recovery/         # Bounce recovery module
│   │   └── bounce_recovery.py   # Find alternative emails for bounced contacts
│   ├── instagram_warmup/        # Instagram warm-up automation
│   ├── utils/                   # Shared utilities
│   └── _archived/               # Legacy scripts (superseded)
├── config/
│   ├── field_mappings/          # Saved field mappings
│   ├── legacy/                  # Archived config files
│   ├── website_overrides.csv    # Manual website mappings
│   ├── manual_contacts.csv      # Manual contact data
│   └── bounced_contacts.csv     # Track bounced emails for recovery
├── docs/
│   ├── PRD.md                   # Product Requirements Document
│   └── ARCHITECTURE.md          # Technical architecture details
├── input/                       # Source files
│   └── legacy/                  # Archived input files
├── processed/                   # Intermediate pipeline files
│   └── legacy/                  # Archived intermediate files
├── output/                      # Final exports (organized by type)
│   ├── prospects_master.csv    # Primary contact database
│   ├── email_campaign/         # Email drafts & campaign files
│   │   └── legacy/              # Archived campaign files
│   ├── gmail_logs/             # Gmail inbox snapshots
│   ├── hubspot/                # HubSpot CRM exports
│   └── legacy/                 # Archived output files
└── tests/                       # Unit tests
```

---

## Configuration

### Environment Variables (.env)

```bash
# Required
OPENAI_API_KEY=sk-...           # AI enrichment
HUNTER_API_KEY=...              # Email verification

# For FB Scraper
APIFY_API_TOKEN=apify_api_...   # FB scraping & DM sending

# For Instagram DM
INSTAGRAM_SESSION_ID=...        # From browser cookies

# Optional
EXA_API_KEY=...                 # Fast contact discovery (Stage 0 enrichment)
GROQ_API_KEY=...                # Faster Instagram search
MANYCHAT_API_KEY=...            # ManyChat integration
```

### Manual Overrides

**config/website_overrides.csv** - Specify websites manually:
```csv
page_name,website_url
"Company Name","https://company.com"
```

**config/manual_contacts.csv** - Add contact data:
```csv
page_name,primary_email,contact_name,contact_position
"Company Name","email@company.com","John Doe","Broker"
```

---

## API Requirements

| Service | Purpose | Free Tier |
|---------|---------|-----------|
| OpenAI | AI enrichment, analysis | Pay per use |
| Hunter.io | Email verification | 25/month |
| Apify | FB scraping, DM sending | Pay per use |
| Exa | Fast contact discovery (optional) | 1000 searches/month |
| DuckDuckGo | Website search | Unlimited |
| Groq | Fast LLM (optional) | Free tier |

---

## Troubleshooting

### FB Scraper returns 0 results
- Check if query is too specific
- Try broader keywords
- For housing ads, the workaround is automatic

### No emails found
1. Check `02_enriched.csv` for website discovery
2. Add website to `config/website_overrides.csv`
3. Re-run: `python run_pipeline.py --from 3.5 --all`

### Instagram handles not found
1. Run with full mode: `python scripts/instagram_enricher.py --all --full`
2. Add `--verify` flag to validate handles
3. Check `config/manual_contacts.csv`

### DM sending errors
- Verify `INSTAGRAM_SESSION_ID` is valid (refresh from browser)
- Check handle blocklist filtering
- Use `--dry-run` to preview before sending

---

## License

Internal use only - LaHaus AI
