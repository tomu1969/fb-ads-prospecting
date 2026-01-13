# FB Ads Prospecting Pipeline

Lead generation pipeline that scrapes Facebook Ad Library, enriches contacts, and enables Instagram DM outreach.

## Quick Commands

```bash
# Run full pipeline
python run_pipeline.py --input input/your_file.csv

# Scrape Facebook Ads Library
python scripts/fb_ads_scraper.py --search "real estate Miami" --limit 100

# === EMAIL CAMPAIGN PIPELINE ===

# Full email pipeline (draft → verify → fix → send)
python scripts/email_pipeline.py --input output/to_email.csv --all

# Draft, verify, fix only (no sending)
python scripts/email_pipeline.py --input output/to_email.csv --all --no-send

# Verify and fix existing drafts
python scripts/email_pipeline.py --drafts output/email_campaign/drafts.csv --verify-only

# Send previously fixed drafts
python scripts/email_pipeline.py --drafts output/email_campaign/drafts_fixed.csv --send-only

# === INDIVIDUAL EMAIL TOOLS ===

# Draft personalized emails
python scripts/email_drafter/drafter.py --input output/prospects_final.csv --limit 5

# Verify email deliverability (MillionVerifier API)
python scripts/email_verifier/verifier.py --csv output/email_campaign/drafts.csv

# Verify email deliverability (SMTP method - no API required)
python scripts/smtp_verifier/smtp_verifier.py --csv output/email_campaign/drafts.csv

# Fix email issues automatically
python scripts/email_verifier/fixer.py --drafts output/email_campaign/drafts.csv

# Send emails via Gmail (dry-run first!)
python scripts/gmail_sender/gmail_sender.py --csv output/email_campaign/drafts_fixed.csv --dry-run --limit 5

# Recover bounced contacts
python scripts/bounce_recovery/bounce_recovery.py --input config/bounced_contacts.csv

# Instagram Warm-Up (5-7 days before DM) - MANUAL MODE
python scripts/instagram_warmup/warmup_orchestrator.py --init --csv output/prospects_master.csv
python scripts/instagram_warmup/warmup_orchestrator.py --manual --limit 10  # Generate today's checklist
python scripts/instagram_warmup/warmup_orchestrator.py --mark-done          # After completing tasks
python scripts/instagram_warmup/warmup_orchestrator.py --status             # Check progress

# Send Instagram DMs (only to warmed-up prospects)
python scripts/apify_dm_sender.py --csv output/prospects_final.csv --message "Hi {contact_name}!" --dry-run

# Contact Name Resolution (Module 3.8 - runs automatically in pipeline)
python scripts/contact_name_resolver.py           # Test mode (3 contacts)
python scripts/contact_name_resolver.py --all     # Process all contacts
python scripts/contact_name_resolver.py --all --use-exa  # Include Exa owner search

# LinkedIn Profile Enrichment (Module 3.9 - runs automatically in pipeline)
python scripts/linkedin_enricher.py               # Test mode (3 contacts)
python scripts/linkedin_enricher.py --all         # Process all contacts
python scripts/linkedin_enricher.py --csv output/prospects.csv  # Standalone mode
```

## Claude Code Agents

Custom agents available via Claude Code's Task tool:

### Growth Analyst (`@growth-analyst`)
Designs, executes, and analyzes growth experiments. Manages multi-step experiments (email sequences, A/B tests, channel comparisons), tracks metrics at each step, and extracts learnings.

```bash
# Invoke via conversation:
"Design an email sequence experiment for real estate leads"
"What experiments should we run to improve response rates?"
"Check the status of experiment 001"
"Analyze the results of the completed experiment"
```

Experiment state is stored in `experiments/` folder:
- `experiments/index.json` - Master registry of all experiments
- `experiments/active/` - Running experiments with state.json
- `experiments/completed/` - Finished experiments with results.md
- `experiments/templates/` - Reusable experiment templates

### Hygiene Master (`@hygiene-master`)
Performs comprehensive repository maintenance, cleanup, and organization.

```bash
# Invoke via conversation:
"Clean up the repository"
"Archive legacy scripts"
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
│   ├── email_pipeline.py    # End-to-end email campaign orchestrator
│   ├── email_drafter/       # Email drafting module (research + compose)
│   ├── email_verifier/      # Email verification & fixing (quality checks)
│   │   ├── verifier.py      # Validation checks (name, domain, template vars)
│   │   ├── fixer.py         # Auto-fix issues (greeting, name extraction)
│   │   └── checks.py        # Individual check functions
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
├── docs/
│   ├── PRD.md               # Product Requirements Document
│   └── ARCHITECTURE.md      # Technical architecture details
├── experiments/             # Growth experiment tracking
│   ├── index.json           # Master registry of all experiments
│   ├── active/              # Running experiments (state.json, logs/)
│   ├── completed/           # Finished experiments (results.md)
│   └── templates/           # Reusable experiment templates
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
- `scripts/email_pipeline.py` - End-to-end email campaign workflow (draft → verify → fix → send)
- `scripts/loader.py` - AI field mapping logic
- `scripts/apify_dm_sender.py` - Instagram DM automation
- `README.md` - Full documentation and usage guide
- `CLAUDE.md` - Quick reference and development practices (this file)
- `docs/PRD.md` - Product requirements and feature specifications
- `docs/ARCHITECTURE.md` - Technical architecture and design decisions

## Data Flow

```
input/*.csv → processed/01_loaded.csv → 02_enriched.csv → 03_contacts.csv
    → 03b_hunter.csv → 03c_enriched.csv (Exa + Agents) → 03d_final.csv
    → 03e_names.csv (Contact Name Resolver) → 03f_linkedin.csv (LinkedIn Enricher)
    → output/prospects_master.csv         # Primary contacts
    → output/hubspot/contacts.csv         # CRM export

Email Campaign Flow:
    prospects_master.csv → [email_drafter] → drafts.csv
    → [verifier] → verification_report.csv
    → [fixer] → drafts_fixed.csv
    → [verifier] → verification_report_after.csv
    → [gmail_sender] → sent emails
```

## Development Practices

### Core Principles
1. **Test-Driven Development (TDD)** - Write tests first, then implement
2. **Modular Architecture** - Prefer small, focused modules with clear interfaces
3. **Thin Slice Development** - Build the minimum vertical slice that connects functionality end-to-end before expanding

### TDD Workflow
1. Write failing tests first in `tests/` folder that define expected behavior
2. Implement the minimum code to pass tests
3. Refactor while keeping tests green
4. Run tests after implementation: `pytest <module>/tests/ -v`

### Modular Design
- Each module should have a single responsibility
- Prefer composition over inheritance
- Keep dependencies explicit and minimal
- Design for testability (dependency injection, clear interfaces)

### Thin Slices
- Start with the simplest working version that touches all layers
- Validate the integration path before adding complexity
- Expand horizontally only after vertical slice works
- Prefer working software over comprehensive features

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
