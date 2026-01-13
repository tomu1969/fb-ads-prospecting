# FB Ads Prospecting Pipeline

Lead generation pipeline that scrapes Facebook Ad Library, enriches contacts, and enables Instagram DM outreach.

## Quick Commands

```bash
# Run full pipeline
python run_pipeline.py --input input/your_file.csv

# Scrape Facebook Ads Library
python scripts/fb_ads_scraper.py --search "real estate Miami" --limit 100

# Draft personalized emails (outputs to output/email_campaign/drafts.csv)
python scripts/email_drafter/drafter.py --input output/prospects_final.csv --limit 5

# Verify emails before sending (catch-all detection)
python scripts/email_verifier/verifier.py --csv output/email_campaign/drafts.csv --output output/email_campaign/verified_drafts.csv

# Send cold emails via Gmail (dry-run first!)
python scripts/gmail_sender/gmail_sender.py --csv output/email_campaign/verified_drafts.csv --dry-run --limit 5

# Recover bounced contacts (outputs to output/email_campaign/recovered_contacts.csv)
python scripts/bounce_recovery/bounce_recovery.py --input config/bounced_contacts.csv

# Instagram Warm-Up (5-7 days before DM) - MANUAL MODE
python scripts/instagram_warmup/warmup_orchestrator.py --init --csv output/prospects_master.csv
python scripts/instagram_warmup/warmup_orchestrator.py --manual --limit 10  # Generate today's checklist
python scripts/instagram_warmup/warmup_orchestrator.py --mark-done          # After completing tasks
python scripts/instagram_warmup/warmup_orchestrator.py --status             # Check progress

# Send Instagram DMs (only to warmed-up prospects)
python scripts/apify_dm_sender.py --csv output/prospects_final.csv --message "Hi {contact_name}!" --dry-run

# Contact Name Resolution (find names from multiple sources)
python scripts/contact_name_resolver.py --csv output/prospects.csv --dry-run
python scripts/contact_name_resolver.py --csv output/prospects.csv --use-exa  # Use Exa API for owner search

# LinkedIn Profile Enrichment (find personal LinkedIn profiles)
python scripts/linkedin_enricher.py --csv output/prospects.csv --dry-run
python scripts/linkedin_enricher.py --csv output/prospects.csv --limit 10
python scripts/linkedin_enricher.py --csv output/prospects.csv --retry-missing  # Re-process unfound
```

## Project Structure

```
├── run_pipeline.py          # Main orchestrator
├── scripts/
│   ├── loader.py            # Module 1: Smart input adapter
│   ├── enricher.py          # Module 2: Website discovery
│   ├── scraper.py           # Module 3: Contact extraction
│   ├── hunter.py            # Module 3.5: Email verification (Hunter.io)
│   ├── exa_enricher.py      # Module 3.6 Stage 0: Fast Exa API contact discovery
│   ├── contact_enricher_pipeline.py  # Module 3.6: AI agent enrichment (Exa + OpenAI)
│   ├── apollo_enricher.py   # Module 3.6.5: Apollo.io B2B contact database
│   ├── instagram_enricher.py # Module 3.7: Instagram handle discovery
│   ├── contact_name_resolver.py # Module 3.8: Multi-source contact name finder
│   ├── linkedin_enricher.py  # Module 3.9: Personal LinkedIn profile finder (Exa)
│   ├── exporter.py          # Module 4: CSV/Excel/HubSpot export
│   ├── validator.py         # Module 5: Quality validation
│   ├── fb_ads_scraper.py    # Facebook Ads Library scraper
│   ├── apify_dm_sender.py   # Instagram DM sender (Apify)
│   ├── manychat_sender.py   # Instagram DM sender (ManyChat)
│   ├── email_drafter/       # Email drafting module (research + compose)
│   ├── email_verifier/      # MillionVerifier email verification (99%+ accuracy)
│   ├── gmail_sender/        # Gmail email sender (SMTP with app password)
│   ├── smtp_verifier/       # SMTP email verifier (no external API)
│   ├── bounce_recovery/     # Bounce recovery (find alternative emails)
│   ├── instagram_warmup/    # Instagram warm-up automation (follow, like, comment)
│   └── _archived/           # Legacy scripts (superseded implementations)
├── config/
│   ├── field_mappings/      # Auto-generated field mappings
│   ├── website_overrides.csv # Manual website corrections
│   ├── do_not_contact.csv   # Exclusion list
│   ├── bounced_contacts.csv # Track bounced emails for recovery
│   ├── warmup_state.csv     # Instagram warm-up progress tracker
│   └── warmup_config.json   # Warm-up phase configuration
├── input/                   # Raw input files
├── processed/               # Intermediate pipeline outputs
│   └── legacy/              # Archived intermediate files
└── output/                  # Final exports (organized by type)
    ├── prospects_master.csv     # Primary contact database
    ├── prospects_master.xlsx    # Excel version
    ├── prospects_final.csv      # Symlink → master
    ├── email_campaign/          # Email drafts & campaign files
    │   ├── drafts.csv           # Generated email drafts
    │   ├── campaign_log.md      # Campaign tracking
    │   └── warmup/              # Daily warmup checklists
    ├── gmail_logs/              # Gmail inbox snapshots
    ├── hubspot/                 # HubSpot CRM exports
    │   └── contacts.csv         # HubSpot-compatible format
    └── legacy/                  # Archived output files
```

## Pipeline Stages

1. **Loader** - Reads CSV/Excel, AI-maps fields to standard schema
2. **Enricher** - Discovers websites via DuckDuckGo search
3. **Scraper** - Extracts contacts from websites
4. **Hunter** - Verifies emails via Hunter.io
5. **Contact Enricher** - Stage 0: Exa API, Stage 1+: OpenAI Agents fallback
6. **Instagram Enricher** - Finds Instagram handles
7. **Contact Name Resolver** - Finds names from hunter, scraper, page_name, Exa owner search
8. **LinkedIn Enricher** - Finds personal LinkedIn profiles via Exa
9. **Exporter** - Outputs CSV/Excel/HubSpot format

## Key Files

- `run_pipeline.py` - Entry point, CLI options, module orchestration
- `scripts/loader.py` - AI field mapping logic
- `scripts/apify_dm_sender.py` - Instagram DM automation
- `README.md` - Full documentation
- `PRD.md` - Technical architecture

## Data Flow

```
input/*.csv → processed/01_loaded.csv → 02_enriched.csv → 03_contacts.csv
    → 03b_hunter.csv → 03c_enriched.csv (Exa + Agents) → 03d_final.csv
    → output/prospects_master.csv         # Primary contacts
    → output/hubspot/contacts.csv         # CRM export
    → output/email_campaign/drafts.csv    # Email drafts
```

## Development Practices

### Core Principles
1. **Test-Driven Development (TDD)** - Write tests first, then implement
2. **Modular Architecture** - Prefer small, focused modules with clear interfaces
3. **Thin Slice Development** - Build the minimum vertical slice that connects functionality end-to-end before expanding

### TDD Workflow
1. Write tests first in `tests/` folder within the module
2. Implement code to pass tests
3. Run tests after implementation: `pytest <module>/tests/ -v`

### Modular Design
- Each module should have a single responsibility
- Prefer composition over inheritance
- Keep dependencies explicit and minimal
- Design for testability (dependency injection, clear interfaces)

### Thin Slices
- Start with the simplest working version that touches all layers
- Validate the integration path before adding complexity
- Expand horizontally only after vertical slice works

### Logging Standards
All scripts should include comprehensive logging:
- Use Python's `logging` module
- Log to both console and file
- Include timestamps: `%(asctime)s [%(levelname)s] %(message)s`
- Log progress: `[{i}/{n}] Processing...`
- Log results summary at completion
- Log all errors with context

Example:
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('script_name.log')
    ]
)
logger = logging.getLogger(__name__)
```
