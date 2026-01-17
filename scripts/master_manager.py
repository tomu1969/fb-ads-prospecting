"""
Master Manager - Merge, deduplicate, and incrementally enrich prospects_master.csv

The single source of truth for all prospecting contacts. All scrapes and enrichments
flow into this master file.

Usage:
    # Merge new data into master
    python scripts/master_manager.py merge --input processed/03i_scored.csv

    # Run all enrichments on contacts missing data
    python scripts/master_manager.py enrich --all

    # Run specific enrichment
    python scripts/master_manager.py enrich --type google_maps
    python scripts/master_manager.py enrich --type tech_stack
    python scripts/master_manager.py enrich --type linkedin
    python scripts/master_manager.py enrich --type lead_score

    # Full sync: merge + enrich
    python scripts/master_manager.py sync --input processed/03i_scored.csv

    # Deduplicate master only
    python scripts/master_manager.py dedupe
"""

import os
import sys
import argparse
import logging
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('master_manager.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = Path(__file__).parent
MASTER_PATH = BASE_DIR / "output" / "prospects_master.csv"
BACKUPS_DIR = BASE_DIR / "output" / "backups"

# Enrichment configuration
ENRICHMENT_CONFIG = {
    'google_maps': {
        'check_column': 'gmaps_place_id',
        'is_missing': lambda v: pd.isna(v) or str(v).strip() == '',
        'script': 'google_maps_enricher.py',
        'description': 'Google Maps data (rating, reviews)',
    },
    'tech_stack': {
        'check_column': 'tech_count',
        'is_missing': lambda v: pd.isna(v) or (isinstance(v, (int, float)) and v == 0) or str(v).strip() in ['', '0', '0.0'],
        'script': 'tech_stack_enricher.py',
        'description': 'Tech stack detection (CRM, pixels)',
    },
    'linkedin': {
        'check_column': 'linkedin_profile',
        'is_missing': lambda v: pd.isna(v) or '/in/' not in str(v),
        'script': 'linkedin_enricher.py',
        'description': 'LinkedIn profile URLs',
    },
    'lead_score': {
        'check_column': 'lead_score',
        'is_missing': lambda v: pd.isna(v) or str(v).strip() == '',
        'script': 'lead_scorer.py',
        'description': 'Lead scoring (0-15 points)',
        'always_run': True,  # Lead scores should be recalculated after other enrichments
    },
}


def normalize_url(url: str) -> str:
    """Normalize URL for comparison (remove protocol, www, trailing slash)."""
    if not url or pd.isna(url):
        return ''
    url = str(url).lower().strip()
    # Remove protocol
    url = url.replace('https://', '').replace('http://', '')
    # Remove www
    url = url.replace('www.', '')
    # Remove trailing slash
    url = url.rstrip('/')
    return url


def load_master() -> pd.DataFrame:
    """Load prospects_master.csv, creating empty DataFrame if doesn't exist."""
    if MASTER_PATH.exists():
        df = pd.read_csv(MASTER_PATH, encoding='utf-8')
        logger.info(f"Loaded master: {len(df)} rows, {len(df.columns)} columns")
        return df
    else:
        logger.info("Master file not found, creating new one")
        return pd.DataFrame()


def backup_master() -> Optional[Path]:
    """Create timestamped backup of master CSV before modifications."""
    if not MASTER_PATH.exists():
        logger.info("No master file to backup")
        return None

    # Ensure backups directory exists
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"prospects_master_{timestamp}.csv"
    backup_path = BACKUPS_DIR / backup_name

    shutil.copy2(MASTER_PATH, backup_path)
    logger.info(f"Created backup: {backup_path}")

    # Keep only last 5 backups
    backups = sorted(BACKUPS_DIR.glob("prospects_master_*.csv"), reverse=True)
    for old_backup in backups[5:]:
        old_backup.unlink()
        logger.debug(f"Removed old backup: {old_backup}")

    return backup_path


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates using priority: page_name > primary_email > website_url.
    Keep the row with the most non-null values.
    """
    if len(df) == 0:
        return df

    original_count = len(df)

    # Calculate data richness (number of non-null values per row)
    df['_data_richness'] = df.notna().sum(axis=1)

    # Sort by richness descending (keep richest row)
    df = df.sort_values('_data_richness', ascending=False)

    # Deduplicate by page_name (primary key)
    df = df.drop_duplicates(subset='page_name', keep='first')

    # Deduplicate by primary_email (if not empty)
    email_mask = df['primary_email'].notna() & (df['primary_email'] != '')
    df_with_email = df[email_mask].drop_duplicates(subset='primary_email', keep='first')
    df_without_email = df[~email_mask]
    df = pd.concat([df_with_email, df_without_email], ignore_index=True)

    # Deduplicate by normalized website_url
    if 'website_url' in df.columns:
        df['_norm_url'] = df['website_url'].apply(normalize_url)
        url_mask = df['_norm_url'].notna() & (df['_norm_url'] != '')
        df_with_url = df[url_mask].drop_duplicates(subset='_norm_url', keep='first')
        df_without_url = df[~url_mask]
        df = pd.concat([df_with_url, df_without_url], ignore_index=True)
        df = df.drop(columns=['_norm_url'])

    # Clean up
    df = df.drop(columns=['_data_richness'])
    df = df.reset_index(drop=True)

    removed = original_count - len(df)
    if removed > 0:
        logger.info(f"Deduplicated: removed {removed} duplicates, {len(df)} remaining")

    return df


def merge_dataframes(master: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """
    Merge incoming data with master using fill-gaps strategy.

    1. Align columns - add any new columns from incoming to master
    2. Concatenate dataframes
    3. Deduplicate by page_name (keep row with most non-null values)
    """
    if len(master) == 0:
        logger.info("Master is empty, using incoming data as master")
        return incoming.copy()

    if len(incoming) == 0:
        logger.info("Incoming data is empty, keeping master unchanged")
        return master.copy()

    # Get all columns from both dataframes
    all_columns = list(master.columns)
    for col in incoming.columns:
        if col not in all_columns:
            all_columns.append(col)

    # Ensure master has all columns
    for col in all_columns:
        if col not in master.columns:
            master[col] = None

    # Ensure incoming has all columns
    for col in all_columns:
        if col not in incoming.columns:
            incoming[col] = None

    # Reorder columns to match
    master = master[all_columns]
    incoming = incoming[all_columns]

    # Concatenate
    combined = pd.concat([master, incoming], ignore_index=True)
    logger.info(f"Concatenated: {len(master)} master + {len(incoming)} incoming = {len(combined)} total")

    # Deduplicate
    combined = deduplicate(combined)

    return combined


def get_rows_needing_enrichment(df: pd.DataFrame, enrichment_type: str) -> pd.DataFrame:
    """Return rows that need the specified enrichment."""
    config = ENRICHMENT_CONFIG.get(enrichment_type)
    if not config:
        logger.error(f"Unknown enrichment type: {enrichment_type}")
        return pd.DataFrame()

    # If always_run flag is set, return all rows
    if config.get('always_run'):
        return df

    col = config['check_column']
    if col not in df.columns:
        logger.info(f"Column '{col}' not in dataframe, all rows need enrichment")
        return df

    mask = df[col].apply(config['is_missing'])
    needs_enrichment = df[mask]
    logger.info(f"Found {len(needs_enrichment)}/{len(df)} rows needing {enrichment_type} enrichment")
    return needs_enrichment


def run_enricher(csv_path: Path, enrichment_type: str, limit: Optional[int] = None) -> bool:
    """
    Run the specified enricher script on a CSV file.

    Returns True if successful, False otherwise.
    """
    config = ENRICHMENT_CONFIG.get(enrichment_type)
    if not config:
        logger.error(f"Unknown enrichment type: {enrichment_type}")
        return False

    script_path = SCRIPTS_DIR / config['script']
    if not script_path.exists():
        logger.error(f"Enricher script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path), '--csv', str(csv_path), '--all']

    if limit:
        cmd = [sys.executable, str(script_path), '--csv', str(csv_path), '--limit', str(limit)]

    # Add extra args for specific enrichers (e.g., --apify-only for linkedin)
    extra_args = config.get('extra_args', [])
    cmd.extend(extra_args)

    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
        if result.returncode != 0:
            logger.error(f"Enricher failed: {result.stderr[:500]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"Enricher timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"Error running enricher: {e}")
        return False


def run_incremental_enrichment(
    master_df: pd.DataFrame,
    enrichment_type: str,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Run enricher only on rows needing it, then merge results back.

    1. Filter to rows needing enrichment
    2. Save filtered rows to temp file
    3. Run enricher on temp file
    4. Load results and merge back to master
    """
    config = ENRICHMENT_CONFIG.get(enrichment_type)
    if not config:
        return master_df

    needs_enrichment = get_rows_needing_enrichment(master_df, enrichment_type)

    if len(needs_enrichment) == 0:
        logger.info(f"No rows need {enrichment_type} enrichment")
        return master_df

    # Apply limit if specified
    if limit and len(needs_enrichment) > limit:
        needs_enrichment = needs_enrichment.head(limit)
        logger.info(f"Limited to {limit} rows for {enrichment_type}")

    # Save to temp file
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as f:
        temp_path = Path(f.name)

    needs_enrichment.to_csv(temp_path, index=False, encoding='utf-8')
    logger.info(f"Saved {len(needs_enrichment)} rows to temp file for enrichment")

    # Run enricher
    success = run_enricher(temp_path, enrichment_type)

    if not success:
        logger.warning(f"Enrichment failed for {enrichment_type}, keeping original data")
        temp_path.unlink()
        return master_df

    # Load enriched results
    try:
        enriched = pd.read_csv(temp_path, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to read enriched results: {e}")
        temp_path.unlink()
        return master_df

    # Merge enriched data back to master by page_name
    for _, enriched_row in enriched.iterrows():
        page_name = enriched_row.get('page_name')
        if not page_name:
            continue

        mask = master_df['page_name'] == page_name
        if not mask.any():
            continue

        # Update only the enriched columns (non-null values)
        for col in enriched.columns:
            if col == 'page_name':
                continue
            if pd.notna(enriched_row[col]) and str(enriched_row[col]).strip() != '':
                # Ensure column exists in master
                if col not in master_df.columns:
                    master_df[col] = None
                master_df.loc[mask, col] = enriched_row[col]

    # Cleanup
    temp_path.unlink()

    logger.info(f"Merged {enrichment_type} enrichment results back to master")
    return master_df


def save_master(df: pd.DataFrame) -> Path:
    """Save updated master CSV."""
    # Ensure output directory exists
    MASTER_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(MASTER_PATH, index=False, encoding='utf-8')
    logger.info(f"Saved master: {len(df)} rows, {len(df.columns)} columns to {MASTER_PATH}")
    return MASTER_PATH


def cmd_merge(args):
    """Handle merge command."""
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = BASE_DIR / args.input

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    # Load incoming data
    incoming = pd.read_csv(input_path, encoding='utf-8')
    logger.info(f"Loaded incoming: {len(incoming)} rows, {len(incoming.columns)} columns")

    # Backup current master
    backup_master()

    # Load and merge
    master = load_master()
    merged = merge_dataframes(master, incoming)

    # Save
    save_master(merged)

    print(f"\nMerge complete: {len(master)} + {len(incoming)} = {len(merged)} contacts")
    return 0


def cmd_enrich(args):
    """Handle enrich command."""
    # Backup current master
    backup_master()

    # Load master
    master = load_master()
    if len(master) == 0:
        logger.error("Master is empty, nothing to enrich")
        return 1

    # Determine which enrichments to run
    if args.all:
        enrichment_types = ['google_maps', 'tech_stack', 'linkedin', 'lead_score']
    elif args.type:
        enrichment_types = [args.type]
    else:
        logger.error("Specify --all or --type <enrichment_type>")
        return 1

    # Run enrichments in order
    for etype in enrichment_types:
        print(f"\n{'='*60}")
        print(f"Running {etype} enrichment...")
        print(f"{'='*60}")

        master = run_incremental_enrichment(master, etype, limit=args.limit)

    # Save updated master
    save_master(master)

    print(f"\nEnrichment complete: {len(master)} contacts processed")
    return 0


def cmd_sync(args):
    """Handle sync command (merge + enrich)."""
    # First merge
    result = cmd_merge(args)
    if result != 0:
        return result

    # Then enrich
    args.all = True
    args.type = None
    args.limit = None
    return cmd_enrich(args)


def cmd_dedupe(args):
    """Handle dedupe command."""
    # Backup current master
    backup_master()

    # Load and dedupe
    master = load_master()
    if len(master) == 0:
        logger.error("Master is empty, nothing to deduplicate")
        return 1

    original_count = len(master)
    master = deduplicate(master)

    # Save
    save_master(master)

    removed = original_count - len(master)
    print(f"\nDeduplication complete: {original_count} -> {len(master)} contacts ({removed} removed)")
    return 0


def cmd_status(args):
    """Handle status command - show enrichment status."""
    master = load_master()
    if len(master) == 0:
        print("Master is empty")
        return 0

    print(f"\n{'='*60}")
    print("MASTER CSV STATUS")
    print(f"{'='*60}")
    print(f"Total contacts: {len(master)}")
    print(f"Total columns:  {len(master.columns)}")
    print(f"\nEnrichment Coverage:")
    print("-" * 60)

    for etype, config in ENRICHMENT_CONFIG.items():
        col = config['check_column']
        if col in master.columns:
            needs = get_rows_needing_enrichment(master, etype)
            has = len(master) - len(needs)
            pct = round(has * 100 / len(master), 1)
            print(f"  {etype:15} {has:4}/{len(master):4} ({pct:5.1f}%) - {config['description']}")
        else:
            print(f"  {etype:15}    0/{len(master):4} (  0.0%) - column missing")

    print(f"{'='*60}")
    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Manage prospects_master.csv - merge, dedupe, and enrich',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  merge   Merge incoming CSV into master
  enrich  Run enrichments on contacts missing data
  sync    Merge + enrich in one step
  dedupe  Deduplicate master
  status  Show enrichment status

Examples:
  python scripts/master_manager.py merge --input processed/03i_scored.csv
  python scripts/master_manager.py enrich --all
  python scripts/master_manager.py enrich --type google_maps --limit 10
  python scripts/master_manager.py sync --input processed/03i_scored.csv
  python scripts/master_manager.py status
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Merge command
    merge_parser = subparsers.add_parser('merge', help='Merge incoming CSV into master')
    merge_parser.add_argument('--input', '-i', required=True, help='Input CSV to merge')

    # Enrich command
    enrich_parser = subparsers.add_parser('enrich', help='Run enrichments')
    enrich_parser.add_argument('--all', action='store_true', help='Run all enrichments')
    enrich_parser.add_argument('--type', '-t', choices=list(ENRICHMENT_CONFIG.keys()),
                              help='Specific enrichment to run')
    enrich_parser.add_argument('--limit', '-l', type=int, help='Limit rows to process')

    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Merge + enrich in one step')
    sync_parser.add_argument('--input', '-i', required=True, help='Input CSV to merge')

    # Dedupe command
    subparsers.add_parser('dedupe', help='Deduplicate master')

    # Status command
    subparsers.add_parser('status', help='Show enrichment status')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    print(f"\n{'='*60}")
    print("MASTER MANAGER")
    print(f"{'='*60}")
    print(f"Master file: {MASTER_PATH}")

    if args.command == 'merge':
        return cmd_merge(args)
    elif args.command == 'enrich':
        return cmd_enrich(args)
    elif args.command == 'sync':
        return cmd_sync(args)
    elif args.command == 'dedupe':
        return cmd_dedupe(args)
    elif args.command == 'status':
        return cmd_status(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
