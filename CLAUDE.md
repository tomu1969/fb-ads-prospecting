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

# === CONTACT INTELLIGENCE GRAPH ===

# Query your contact network (natural language → Neo4j)
python -m scripts.contact_intel.graph_query "who do I know at Google?"
python -m scripts.contact_intel.graph_query "tech people in my network"
python -m scripts.contact_intel.graph_query "warm intro path to Compass"
python -m scripts.contact_intel.graph_query --interactive  # Interactive mode

# Contact Name Resolution (Module 3.8 - runs automatically in pipeline)
python scripts/contact_name_resolver.py           # Test mode (3 contacts)
python scripts/contact_name_resolver.py --all     # Process all contacts
python scripts/contact_name_resolver.py --all --use-exa  # Include Exa owner search

# LinkedIn Profile Enrichment (Module 3.9 - runs automatically in pipeline)
python scripts/linkedin_enricher.py               # Test mode (3 contacts)
python scripts/linkedin_enricher.py --all         # Process all contacts
python scripts/linkedin_enricher.py --csv output/prospects.csv  # Standalone mode

# === LEAD SCORING PIPELINE ===

# Google Maps Enrichment (Module 3.10 - review count, rating as lead volume proxy)
python scripts/google_maps_enricher.py            # Test mode (3 contacts)
python scripts/google_maps_enricher.py --all      # Process all contacts

# Tech Stack Detection (Module 3.11 - CRM, pixels, scheduling tools)
python scripts/tech_stack_enricher.py             # Test mode (3 contacts)
python scripts/tech_stack_enricher.py --all       # Process all contacts

# Lead Scoring (Module 3.12 - composite 0-15 score with tier classification)
python scripts/lead_scorer.py                     # Test mode (3 contacts)
python scripts/lead_scorer.py --all               # Score all contacts
# Tiers: HOT (12-15), WARM (8-11), COOL (5-7), COLD (0-4)

# === MASTER CSV MANAGEMENT ===

# The master CSV (output/prospects_master.csv) is the single source of truth
# for all prospecting contacts. All scrapes and enrichments flow into it.

# Check enrichment status
python scripts/master_manager.py status

# Merge new pipeline output into master
python scripts/master_manager.py merge --input processed/03i_scored.csv

# Run all enrichments on contacts missing data
python scripts/master_manager.py enrich --all

# Run specific enrichment only
python scripts/master_manager.py enrich --type google_maps
python scripts/master_manager.py enrich --type tech_stack
python scripts/master_manager.py enrich --type linkedin
python scripts/master_manager.py enrich --type lead_score

# Full sync: merge + enrich in one step
python scripts/master_manager.py sync --input processed/03i_scored.csv

# Deduplicate master only
python scripts/master_manager.py dedupe

# === ICP DISCOVERY PIPELINE ===
# Behavior-based ICP identification from FB Ads Library data
# Identifies advertisers with Money, Urgency, and Conversational Necessity

# Full ICP discovery pipeline
python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads_scraped_broad.csv

# Test mode (limit ads)
python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads.csv --limit 100

# Check pipeline status
python scripts/icp_discovery/run_icp_pipeline.py --status

# Resume from specific module
python scripts/icp_discovery/run_icp_pipeline.py --from m2

# Run individual modules
python scripts/icp_discovery/m0_normalizer.py --csv output/fb_ads_broad.csv        # Normalize + classify destinations
python scripts/icp_discovery/m1_aggregator.py                                        # Aggregate to page level
python scripts/icp_discovery/m2_conv_gate.py                                         # Filter transactional advertisers
python scripts/icp_discovery/m3_money_score.py                                       # Score 0-50 (ad volume, velocity)
python scripts/icp_discovery/m4_urgency_score.py                                     # Score 0-50 (MESSAGE/CALL share)

# Run tests
pytest scripts/icp_discovery/tests/ -v
```

## Contact Graph Queries (Automatic)

**When the user asks about their network, contacts, connections, or "who do I know" questions, automatically query the Neo4j graph:**

```bash
python -m scripts.contact_intel.graph_query "user's question here"
```

Examples of questions that trigger graph queries:
- "Who do I know at [company]?"
- "Find contacts in tech/finance/real estate"
- "How can I get introduced to [person]?"
- "List people who discuss [topic]"

**Query modes:**
- Industry questions → Use topics (DISCUSSED relationship)
- Specific company → Use company name (WORKS_AT relationship)
- Warm intros → Use path queries (KNOWS*1..2)

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

### Closed Lost Analyst (`@closed-lost-analyst`)
Performs comprehensive closed lost deal audits from HubSpot CRM. Analyzes loss reasons, revenue impact, owner performance, lead source effectiveness, and CRM hygiene issues.

```bash
# Invoke via conversation:
"Run a closed lost audit for the last 30 days"
"Compare this month vs last month closed lost performance"
"Analyze lead source impact on closed lost deals"
"Generate a performance audit for the sales team"
```

Reports are saved to `hubspot_funnel/reports/`:
- `closed_lost_diagnostic_YYYYMMDD.md` - Full diagnostic report
- `auditoria_comparativa_YYYYMMDD.md` - Period comparison
- `lead_source_comparison_YYYYMMDD.md` - Channel analysis

## HubSpot API Integration

The project uses HubSpot CRM API v3 for sales analytics and marketing automation.

### Configuration
- API Key: `.env` → `HUBSPOT_API_KEY`
- Pipeline: LaHaus AI (ID: `719833388`)
- Owners: `hubspot_funnel/config/deal_owners.json`

### Pipeline Stages
| Stage | ID | Triggers |
|-------|----|----|
| Calificado | 1102547555 | Lead qualified |
| Demo Agendado | 1049659495 | → Pre-demo email |
| Demo Presentado | 1049659496 | → Proposal email |
| Propuesta Aceptada | 1110769786 | - |
| Suscripción Activa | 1092762538 | Won |
| Cerrada Perdida | 1102535235 | Lost |

### Current API Usage
| Endpoint | Script | Purpose |
|----------|--------|---------|
| `POST /crm/v3/objects/deals/search` | `closed_lost_analysis.py` | Deal analytics |
| `POST /crm/v3/objects/{calls,meetings,emails}/search` | `owner_performance_analysis.py` | Activity metrics |

### Email Templates
Templates in `config/email_templates/` can be:
- Sent via Claude (`scripts/email_templates/template_sender.py --template <id>`)
- Synced to HubSpot Marketing Hub for workflow automation

```bash
# Load and preview a template
python scripts/email_templates/template_loader.py --template pre_demo_confirmation --preview

# Send template-based email (dry-run first!)
python scripts/email_templates/template_sender.py --template pre_demo_confirmation --deal-id 12345 --dry-run

# Sync templates to HubSpot
python scripts/hubspot_templates.py push --template pre_demo_confirmation
python scripts/hubspot_templates.py list
python scripts/hubspot_templates.py sync-all
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
│   ├── google_maps_enricher.py # Module 3.10: Google Maps data (reviews, rating)
│   ├── tech_stack_enricher.py  # Module 3.11: Tech stack detection (CRM, pixels)
│   ├── lead_scorer.py       # Module 3.12: Composite lead scoring (0-15)
│   ├── master_manager.py    # Master CSV management (merge, dedupe, enrich)
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
│   ├── icp_discovery/       # ICP Discovery Pipeline (behavior-based)
│   │   ├── run_icp_pipeline.py   # Pipeline orchestrator
│   │   ├── m0_normalizer.py      # Normalize raw data, classify destinations
│   │   ├── m1_aggregator.py      # Aggregate to page level
│   │   ├── m2_conv_gate.py       # Conversational necessity filter
│   │   ├── m3_money_score.py     # Money score (0-50)
│   │   ├── m4_urgency_score.py   # Urgency score (0-50)
│   │   └── constants.py          # CTA mappings, keywords, thresholds
│   ├── email_templates/     # Sales pipeline email templates
│   │   ├── template_loader.py   # Load + substitute variables
│   │   └── template_sender.py   # Send via Gmail
│   ├── hubspot_templates.py # HubSpot template sync
│   └── _archived/           # Legacy scripts (superseded implementations)
├── config/
│   ├── email_templates/     # Sales pipeline email templates
│   │   ├── templates.json       # Template registry with metadata
│   │   ├── pipeline/            # Stage-triggered emails
│   │   │   ├── pre_demo_confirmation.md
│   │   │   └── post_demo_proposal.md
│   │   └── followup/            # Follow-up sequence
│   │       ├── day3_value.md
│   │       ├── day7_social_proof.md
│   │       └── day14_breakup.md
│   ├── field_mappings/      # Auto-generated field mappings
│   ├── legacy/              # Archived config files
│   ├── website_overrides.csv # Manual website corrections
│   ├── do_not_contact.csv   # Exclusion list
│   ├── bounced_contacts.csv # Track bounced emails for recovery
│   ├── warmup_state.csv     # Instagram warm-up progress tracker
│   └── warmup_config.json   # Warm-up phase configuration
├── docs/
│   ├── PRD.md               # Product Requirements Document
│   └── ARCHITECTURE.md      # Technical architecture details
├── PRDs/                    # Feature PRDs
│   └── lead_scoring_enrichment.md  # Google Maps + Tech Stack + Scoring
├── experiments/             # Growth experiment tracking
│   ├── index.json           # Master registry of all experiments
│   ├── active/              # Running experiments (state.json, logs/)
│   ├── completed/           # Finished experiments (results.md)
│   └── templates/           # Reusable experiment templates
├── hubspot_funnel/          # HubSpot CRM analytics and reporting
│   ├── closed_lost_analysis.py  # Closed lost deal analysis script
│   ├── config/              # Configuration files
│   │   └── deal_owners.json     # Target deal owner IDs
│   ├── reports/             # Generated analysis reports
│   │   ├── closed_lost_diagnostic_*.md  # Loss reason diagnostics
│   │   ├── auditoria_comparativa_*.md   # Period comparisons
│   │   └── lead_source_comparison_*.md  # Channel analysis
│   └── schema/              # HubSpot schema definitions
│       └── deals_properties.json    # Deal property mappings
├── input/                   # Raw input files
│   └── legacy/              # Archived input files
├── processed/               # Intermediate pipeline outputs
│   └── legacy/              # Archived intermediate files
└── output/                  # Final exports (organized by type)
    ├── prospects_master.csv     # Primary contact database
    ├── prospects_master.xlsx    # Excel version
    ├── prospects_final.csv      # Symlink → master
    ├── email_campaign/          # Email drafts & campaign files
    │   ├── drafts.csv           # Generated email drafts
    │   ├── campaign_log.md      # Campaign tracking
    │   ├── warmup/              # Daily warmup checklists
    │   └── legacy/              # Archived campaign files
    ├── gmail_logs/              # Gmail inbox snapshots
    ├── hubspot/                 # HubSpot CRM exports
    │   └── contacts.csv         # HubSpot-compatible format
    ├── icp_discovery/           # ICP Discovery intermediate outputs
    │   ├── 00_ads_normalized.csv    # Normalized ad-level data
    │   ├── 01_pages_aggregated.csv  # Page-level aggregated
    │   ├── 02_pages_candidate.csv   # Passed conv. gate
    │   ├── 03_money_scored.csv      # With money scores
    │   ├── 04_urgency_scored.csv    # Urgency scored
    │   ├── 05_fit_scored.csv        # Fit scored
    │   └── 06_clustered.csv         # Final clustered
    ├── icp_exploration/         # ICP Discovery final reports
    │   ├── classified_advertisers.csv  # Sector-classified advertisers
    │   ├── icp_analysis_report.md      # Analysis summary
    │   ├── sector_classifications.csv  # Sector assignments
    │   └── vertical_deep_dive.md       # Vertical-specific insights
    └── legacy/                  # Archived output files
        └── backups/             # Auto-generated backups
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
9. **Google Maps Enricher** - Adds review count, rating (lead volume proxy)
10. **Tech Stack Enricher** - Detects CRM, pixels, scheduling tools
11. **Lead Scorer** - Calculates composite 0-15 score with tier classification
12. **Exporter** - Outputs CSV/Excel/HubSpot format

## Key Files

- `run_pipeline.py` - Entry point, CLI options, module orchestration
- `scripts/email_pipeline.py` - End-to-end email campaign workflow (draft → verify → fix → send)
- `scripts/loader.py` - AI field mapping logic
- `scripts/apify_dm_sender.py` - Instagram DM automation
- `README.md` - Full documentation and usage guide
- `CLAUDE.md` - Quick reference and development practices (this file)
- `docs/PRD.md` - Product requirements and feature specifications
- `docs/ARCHITECTURE.md` - Technical architecture and design decisions
- `PRDs/lead_scoring_enrichment.md` - Google Maps + Tech Stack + Lead Scoring PRD

## Data Flow

```
input/*.csv → processed/01_loaded.csv → 02_enriched.csv → 03_contacts.csv
    → 03b_hunter.csv → 03c_enriched.csv (Exa + Agents) → 03d_final.csv
    → 03e_names.csv (Contact Name Resolver) → 03f_linkedin.csv (LinkedIn Enricher)
    → 03g_gmaps.csv (Google Maps) → 03h_techstack.csv (Tech Stack)
    → 03i_scored.csv (Lead Scoring)
    → output/prospects_master.csv         # Primary contacts (with lead_score, lead_tier)
    → output/hubspot/contacts.csv         # CRM export

Email Campaign Flow:
    prospects_master.csv → [email_drafter] → drafts.csv
    → [verifier] → verification_report.csv
    → [fixer] → drafts_fixed.csv
    → [verifier] → verification_report_after.csv
    → [gmail_sender] → sent emails

ICP Discovery Flow (behavior-based):
    fb_ads_scraped_broad.csv (raw ad-level)
    → [m0_normalizer] → 00_ads_normalized.csv (destination types: MESSAGE/CALL/FORM/WEB)
    → [m1_aggregator] → 01_pages_aggregated.csv (page-level shares, velocity)
    → [m2_conv_gate] → 02_pages_candidate.csv (filtered: keeps conversational, drops transactional)
    → [m3_money_score] → 03_money_scored.csv (money 0-50: ad volume, always-on, velocity)
    → [m4_urgency_score] → 04_urgency_scored.csv (urgency 0-50: MESSAGE/CALL share, keywords)
    → icp_report.md (ranked advertisers with combined scores)
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

### Incremental Saving (CRITICAL for Enrichment Scripts)
Long-running enrichment scripts MUST save progress incrementally to prevent data loss:

**Requirements:**
- Save to disk after each batch or every N records (e.g., every 10-50 items)
- Create automatic backups before starting any enrichment
- Support `--resume` flag to continue from where it left off
- Never use `--limit` flags that truncate the output file

**Pattern:**
```python
# Save after each batch
for i, batch in enumerate(batches):
    results = process_batch(batch)
    df.update(results)

    # Incremental save every batch
    df.to_csv(output_path, index=False)
    logger.info(f"Progress saved: {(i+1)*batch_size}/{total} records")

# Backup before starting
import shutil
shutil.copy(output_path, f"{output_path}.backup")
```

**Why:** A 2+ hour enrichment that fails or is interrupted loses ALL progress if it only saves at the end. Incremental saves allow resumption and prevent data loss.

### Script Reuse Policy

**Always check existing scripts before creating new ones:**
- Search `scripts/` for similar functionality before writing new code
- Existing enrichment scripts: `hubspot_enricher.py`, `apollo_enricher.py`, `exa_enricher.py`, `hunter.py`, `repliers_agent_lookup.py`
- Prefer extending existing scripts over creating new ones
- Use `--help` to discover existing script capabilities
