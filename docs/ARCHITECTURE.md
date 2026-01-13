# Archived Scripts

This directory contains legacy scripts that have been superseded by newer implementations.

## Archived Files

### Contact Enrichment Evolution
- `contact_enricher.py` - Original contact enrichment implementation
- `contact_enricher_v2.py` - Second iteration with improved extraction
- `contact_enricher_v3.py` - Third iteration before OpenAI Agents migration
- **Current**: `../contact_enricher_pipeline.py` - OpenAI Agents-based enrichment with Exa integration

### Instagram Discovery Evolution
- `instagram_enricher_v1.py` - Original Instagram handle discovery
- `find_missing_instagram_v1.py` - First version of missing handle finder
- **Current**: `../instagram_enricher.py` and `../find_missing_instagram.py`

### Deprecated Modules
- `apollo_enricher.py` - Apollo.io integration (removed in favor of Hunter.io + OpenAI)
- `composer.py` - Message composition utilities (deprecated)
- `loader_fb_ads.py` - Original FB Ads loader (replaced by universal `../loader.py`)
- `main.py` - Old entry point (replaced by `../../run_pipeline.py`)
- `progress.py` - Progress tracking utilities (integrated into individual modules)

## Archive Date
Last updated: 2026-01-11

## Recovery
These files are preserved for reference but should not be used in production. To recover functionality, refer to the current implementations in the parent `scripts/` directory.
