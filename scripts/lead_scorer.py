"""
Lead Scorer - Module 3.12 - Calculate composite buyer intent scores

Runs automatically after Tech Stack Enricher (Module 3.11) in the pipeline.
Calculates lead scores (0-15) based on all enrichment signals and assigns tiers.

Input: processed/03h_techstack.csv (from Module 3.11)
Output: processed/03i_scored.csv

Scoring Formula (max 15 points):
- Lead Volume Signals (max 6): FB ads, multiple creatives, review count
- Operational Maturity (max 5): CRM, pixel, scheduling, chat
- Contact Quality (max 4): Decision-maker, email+phone

Tiers:
- HOT (12-15): Direct pitch, book demo - Same day priority
- WARM (8-11): Educate + demo CTA - Within 48h
- COOL (5-7): Nurture sequence - Weekly
- COLD (0-4): Low priority / skip - Monthly or skip

Usage:
    python scripts/lead_scorer.py               # Test mode (3 contacts)
    python scripts/lead_scorer.py --all         # Process all contacts
    python scripts/lead_scorer.py --csv output/prospects.csv  # Standalone mode
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink
from utils.enrichment_config import should_run_module

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('lead_scorer.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent

# Pipeline input/output paths
INPUT_BASE = "03h_techstack.csv"
OUTPUT_BASE = "03i_scored.csv"

# Scoring weights
SCORING_CONFIG = {
    # Lead Volume Signals (max 6 points)
    "active_fb_ads": {
        "points": 3,
        "description": "Has active Facebook ads"
    },
    "multiple_creatives": {
        "points": 2,
        "description": "Running 3+ ad creatives"
    },
    "high_review_count": {
        "points": 2,
        "description": "30+ Google reviews"
    },
    "very_high_review_count": {
        "points": 1,
        "description": "100+ Google reviews (bonus)"
    },

    # Operational Maturity Signals (max 5 points)
    "has_crm": {
        "points": 2,
        "description": "CRM system detected"
    },
    "has_marketing_pixel": {
        "points": 1,
        "description": "Marketing pixel detected"
    },
    "has_scheduling_tool": {
        "points": 1,
        "description": "Scheduling tool detected"
    },
    "has_chat_widget": {
        "points": 1,
        "description": "Chat widget detected"
    },

    # Contact Quality Signals (max 4 points)
    "decision_maker": {
        "points": 2,
        "description": "Owner/Broker/Founder"
    },
    "email_and_phone": {
        "points": 2,
        "description": "Both email and phone available"
    },
    "email_only": {
        "points": 1,
        "description": "Email available"
    },

    # Brand Presence Signals (max 2 points)
    "has_instagram": {
        "points": 1,
        "description": "Has business Instagram"
    },
    "good_rating": {
        "points": 1,
        "description": "4.0+ Google rating"
    },
}

# Decision-maker title patterns
DECISION_MAKER_TITLES = [
    "owner", "broker", "founder", "principal", "ceo",
    "president", "director", "managing", "partner"
]

# Tier definitions
TIER_THRESHOLDS = {
    "HOT": 12,   # 12-15 points
    "WARM": 8,   # 8-11 points
    "COOL": 5,   # 5-7 points
    "COLD": 0,   # 0-4 points
}

TIER_ACTIONS = {
    "HOT": "Direct pitch, book demo - Same day priority",
    "WARM": "Educate + demo CTA - Within 48h",
    "COOL": "Nurture sequence - Weekly",
    "COLD": "Low priority / skip - Monthly or skip",
}


def calculate_lead_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate composite lead score based on all enrichment signals.

    Args:
        row: DataFrame row with enrichment data

    Returns:
        dict with lead_score, lead_tier, and score_breakdown
    """
    score = 0
    breakdown = []

    # === Lead Volume Signals ===

    # Active FB ads (+3)
    ad_count = row.get("ad_count", 0) or 0
    try:
        ad_count = int(ad_count)
    except (ValueError, TypeError):
        ad_count = 0

    if ad_count > 0:
        score += SCORING_CONFIG["active_fb_ads"]["points"]
        breakdown.append(f"active_fb_ads:+{SCORING_CONFIG['active_fb_ads']['points']}")

    # Multiple creatives (+2)
    if ad_count >= 3:
        score += SCORING_CONFIG["multiple_creatives"]["points"]
        breakdown.append(f"multiple_creatives:+{SCORING_CONFIG['multiple_creatives']['points']}")

    # Review count
    review_count = row.get("gmaps_review_count", 0) or 0
    try:
        review_count = int(review_count)
    except (ValueError, TypeError):
        review_count = 0

    if review_count >= 30:
        score += SCORING_CONFIG["high_review_count"]["points"]
        breakdown.append(f"reviews_30+:+{SCORING_CONFIG['high_review_count']['points']}")

    if review_count >= 100:
        score += SCORING_CONFIG["very_high_review_count"]["points"]
        breakdown.append(f"reviews_100+:+{SCORING_CONFIG['very_high_review_count']['points']}")

    # === Operational Maturity Signals ===

    # CRM (+2)
    if row.get("has_crm"):
        score += SCORING_CONFIG["has_crm"]["points"]
        breakdown.append(f"has_crm:+{SCORING_CONFIG['has_crm']['points']}")

    # Marketing pixel (+1)
    if row.get("has_marketing_pixel"):
        score += SCORING_CONFIG["has_marketing_pixel"]["points"]
        breakdown.append(f"has_pixel:+{SCORING_CONFIG['has_marketing_pixel']['points']}")

    # Scheduling tool (+1)
    if row.get("has_scheduling_tool"):
        score += SCORING_CONFIG["has_scheduling_tool"]["points"]
        breakdown.append(f"has_scheduling:+{SCORING_CONFIG['has_scheduling_tool']['points']}")

    # Chat widget (+1)
    if row.get("has_chat_widget"):
        score += SCORING_CONFIG["has_chat_widget"]["points"]
        breakdown.append(f"has_chat:+{SCORING_CONFIG['has_chat_widget']['points']}")

    # === Contact Quality Signals ===

    # Decision-maker position (+2)
    position = str(row.get("contact_position", "") or "").lower()
    if any(title in position for title in DECISION_MAKER_TITLES):
        score += SCORING_CONFIG["decision_maker"]["points"]
        breakdown.append(f"decision_maker:+{SCORING_CONFIG['decision_maker']['points']}")

    # Email and phone availability
    primary_email = row.get("primary_email", "") or ""
    phones = row.get("phones", "") or ""

    has_email = bool(primary_email and str(primary_email).strip() and str(primary_email).lower() != "nan")
    has_phone = bool(phones and str(phones).strip() and str(phones) not in ["[]", "nan"])

    if has_email and has_phone:
        score += SCORING_CONFIG["email_and_phone"]["points"]
        breakdown.append(f"email+phone:+{SCORING_CONFIG['email_and_phone']['points']}")
    elif has_email:
        score += SCORING_CONFIG["email_only"]["points"]
        breakdown.append(f"email_only:+{SCORING_CONFIG['email_only']['points']}")

    # === Brand Presence Signals ===

    # Instagram (+1)
    instagram = row.get("instagram_handles", "") or ""
    has_instagram = bool(instagram and str(instagram).strip() and str(instagram) not in ["[]", "nan"])
    if has_instagram:
        score += SCORING_CONFIG["has_instagram"]["points"]
        breakdown.append(f"has_instagram:+{SCORING_CONFIG['has_instagram']['points']}")

    # Good Google rating (+1)
    rating = row.get("gmaps_rating", 0) or 0
    try:
        rating = float(rating)
    except (ValueError, TypeError):
        rating = 0

    if rating >= 4.0:
        score += SCORING_CONFIG["good_rating"]["points"]
        breakdown.append(f"good_rating:+{SCORING_CONFIG['good_rating']['points']}")

    # === Determine Tier ===
    if score >= TIER_THRESHOLDS["HOT"]:
        tier = "HOT"
    elif score >= TIER_THRESHOLDS["WARM"]:
        tier = "WARM"
    elif score >= TIER_THRESHOLDS["COOL"]:
        tier = "COOL"
    else:
        tier = "COLD"

    return {
        "lead_score": score,
        "lead_tier": tier,
        "score_breakdown": "|".join(breakdown) if breakdown else "no_signals",
    }


def score_leads(
    csv_path: Path,
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    dry_run: bool = False
) -> Dict:
    """
    Score all leads in a CSV file.

    Args:
        csv_path: Input CSV with enrichment data
        output_path: Output CSV path
        limit: Max contacts to process
        dry_run: Preview without saving

    Returns:
        Stats dictionary
    """
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return {'error': str(e)}

    # Stats
    stats = {
        'total': len(df),
        'scored': 0,
        'tiers': {'HOT': 0, 'WARM': 0, 'COOL': 0, 'COLD': 0},
        'avg_score': 0,
        'max_score': 0,
        'min_score': 15,
    }

    # Apply limit
    if limit:
        df_to_process = df.head(limit).copy()
    else:
        df_to_process = df.copy()

    logger.info(f"Scoring {len(df_to_process)} contacts")

    if dry_run:
        logger.info("DRY RUN - No file will be saved")
        for idx, row in df_to_process.head(10).iterrows():
            result = calculate_lead_score(row)
            logger.info(f"  {row.get('page_name', 'Unknown')[:40]}: "
                       f"Score={result['lead_score']} Tier={result['lead_tier']}")
        return stats

    # Calculate scores
    scores = []
    for idx, row in tqdm(df_to_process.iterrows(), total=len(df_to_process), desc="Scoring leads"):
        result = calculate_lead_score(row)
        scores.append(result)

        # Update stats
        stats['scored'] += 1
        stats['tiers'][result['lead_tier']] += 1
        stats['max_score'] = max(stats['max_score'], result['lead_score'])
        stats['min_score'] = min(stats['min_score'], result['lead_score'])

    # Add scores to dataframe
    scores_df = pd.DataFrame(scores)
    df_to_process['lead_score'] = scores_df['lead_score'].values
    df_to_process['lead_tier'] = scores_df['lead_tier'].values
    df_to_process['score_breakdown'] = scores_df['score_breakdown'].values

    # Calculate average
    stats['avg_score'] = round(df_to_process['lead_score'].mean(), 2)

    # If we limited, we need to merge back with original
    if limit:
        # Update the original dataframe with scores for processed rows
        df['lead_score'] = ''
        df['lead_tier'] = ''
        df['score_breakdown'] = ''

        for idx, row in df_to_process.iterrows():
            df.at[idx, 'lead_score'] = row['lead_score']
            df.at[idx, 'lead_tier'] = row['lead_tier']
            df.at[idx, 'score_breakdown'] = row['score_breakdown']

        df_output = df
    else:
        df_output = df_to_process

    # Save output
    if output_path is None:
        output_path = csv_path.parent / f"{csv_path.stem}_scored{csv_path.suffix}"

    df_output.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved to {output_path}")

    return stats


def print_summary(stats: Dict):
    """Print scoring summary with tier breakdown."""
    print("\n" + "=" * 60)
    print("LEAD SCORING SUMMARY")
    print("=" * 60)
    print(f"Total contacts:        {stats.get('total', 0)}")
    print(f"Scored:                {stats.get('scored', 0)}")
    print(f"Average score:         {stats.get('avg_score', 0)}")
    print(f"Score range:           {stats.get('min_score', 0)} - {stats.get('max_score', 0)}")
    print("-" * 60)
    print("TIER BREAKDOWN:")
    tiers = stats.get('tiers', {})
    total = stats.get('scored', 1) or 1

    for tier in ['HOT', 'WARM', 'COOL', 'COLD']:
        count = tiers.get(tier, 0)
        pct = round(count * 100 / total, 1)
        bar = "#" * int(pct / 5)
        action = TIER_ACTIONS.get(tier, "")
        print(f"  {tier:5} {count:4} ({pct:5.1f}%) {bar:20} {action}")

    print("=" * 60)


def main():
    """Main function with pipeline and standalone mode support."""

    # Check if module should run based on enrichment config
    if not should_run_module("lead_scorer"):
        print(f"\n{'='*60}")
        print("MODULE 3.12: LEAD SCORER")
        print(f"{'='*60}")
        print("SKIPPED: Lead scoring not selected in configuration")
        print("   No changes made to input file.")
        return 0

    print(f"\n{'='*60}")
    print("MODULE 3.12: LEAD SCORER")
    print(f"{'='*60}")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Score leads based on enrichment signals',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--csv', type=str,
                       help='Input CSV file (standalone mode). If not provided, uses pipeline input.')
    parser.add_argument('--output', type=str,
                       help='Output CSV path (default: pipeline output or same as input)')
    parser.add_argument('--all', action='store_true',
                       help='Process all contacts (default: test mode with 3 contacts)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process (alternative to --all)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without saving')

    args = parser.parse_args()

    # Determine input file
    run_id = get_run_id_from_env()

    if args.csv:
        # Standalone mode - use provided CSV
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv
        standalone_mode = True
    else:
        # Pipeline mode - auto-detect input
        if run_id:
            input_name = get_versioned_filename(INPUT_BASE, run_id)
            csv_path = BASE_DIR / "processed" / input_name
        else:
            csv_path = BASE_DIR / "processed" / INPUT_BASE

        # Try latest symlink if versioned file doesn't exist
        if not csv_path.exists():
            latest_path = BASE_DIR / "processed" / INPUT_BASE
            if latest_path.exists() or latest_path.is_symlink():
                csv_path = latest_path

        # Fallback: try 03g_gmaps.csv if 03h_techstack.csv doesn't exist
        if not csv_path.exists():
            fallback_base = "03g_gmaps.csv"
            if run_id:
                fallback_name = get_versioned_filename(fallback_base, run_id)
                fallback_path = BASE_DIR / "processed" / fallback_name
            else:
                fallback_path = BASE_DIR / "processed" / fallback_base

            if not fallback_path.exists():
                fallback_path = BASE_DIR / "processed" / fallback_base

            if fallback_path.exists() or fallback_path.is_symlink():
                csv_path = fallback_path
                logger.info(f"Using fallback input: {fallback_path}")

        standalone_mode = False

    if not csv_path.exists():
        print(f"ERROR: Input file not found: {csv_path}")
        if not standalone_mode:
            print("Make sure Module 3.11 (Tech Stack Enricher) has run first.")
        return 1

    # Determine output file
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    elif standalone_mode:
        # For standalone, add _scored suffix
        output_path = csv_path.parent / f"{csv_path.stem}_scored{csv_path.suffix}"
    else:
        # Pipeline mode - write to new file
        if run_id:
            output_name = get_versioned_filename(OUTPUT_BASE, run_id)
            output_path = BASE_DIR / "processed" / output_name
        else:
            output_path = BASE_DIR / "processed" / OUTPUT_BASE

    # Determine limit
    if args.limit:
        limit = args.limit
    elif args.all:
        limit = None
    else:
        limit = 3  # Test mode
        print("Test mode: Processing first 3 contacts")
        print("(Use --all to process all contacts)")

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.dry_run:
        print("Mode: DRY RUN")

    # Run scoring
    stats = score_leads(
        csv_path=csv_path,
        output_path=output_path,
        limit=limit,
        dry_run=args.dry_run
    )

    # Create latest symlink in pipeline mode
    if not standalone_mode and run_id and output_path.exists():
        latest_path = create_latest_symlink(output_path, OUTPUT_BASE)
        if latest_path:
            print(f"Latest symlink: {latest_path}")

    print_summary(stats)
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
