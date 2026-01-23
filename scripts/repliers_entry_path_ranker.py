"""
Repliers Entry Path Ranker - Rank optimal outreach paths for each agent

Analyzes all enrichment data to recommend the best entry path for connecting
with each real estate agent. Scores and ranks by:

1. **Direct Paths** (highest conversion):
   - Mutual LinkedIn connections (warm intro potential)
   - Shared industry groups
   - Common alma mater

2. **Professional Paths**:
   - LinkedIn profile available (InMail possible)
   - Email verified (cold email)
   - Meta ads active (they're investing in marketing)

3. **Social Paths**:
   - Instagram handle (DM/follow)
   - Facebook presence via Meta ads

Input: output/repliers/top_agents_2025_enriched.csv
Output: Same file with entry_path_score and recommended_path columns

Usage:
    python scripts/repliers_entry_path_ranker.py
    python scripts/repliers_entry_path_ranker.py --export-top 50
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_entry_path_ranker.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = "output/repliers/top_agents_2025_enriched.csv"

# Scoring weights (total max ~100)
SCORING_WEIGHTS = {
    # Warm paths (highest value - these convert best)
    'mutual_connections': 40,       # Warm intro via mutual
    'mutual_per_connection': 5,     # Bonus per mutual (capped at 5)

    # Professional paths
    'has_linkedin': 15,             # Can send InMail
    'has_email': 10,                # Can cold email
    'has_meta_ads': 10,             # Active marketer (receptive to marketing)
    'has_marketing_pixel': 5,       # Tech-savvy, tracking-focused

    # Social paths
    'has_instagram': 8,             # Can DM/follow/engage

    # Volume signals (indicates success/worth pursuing)
    'high_volume': 5,               # Top producer (worth the effort)
    'has_idx': 2,                   # Has IDX = serious about web presence
}

# Entry path recommendations (priority order)
ENTRY_PATHS = [
    ('mutual_intro', 'Ask {mutual_name} for an intro', 90),
    ('linkedin_connect', 'LinkedIn connect + personalized note', 70),
    ('linkedin_inmail', 'LinkedIn InMail (premium)', 60),
    ('instagram_engage', 'Instagram: follow + engage + DM', 50),
    ('cold_email', 'Cold email with value prop', 40),
    ('facebook_message', 'Facebook page message', 30),
    ('cold_call', 'Cold call (if phone available)', 20),
]


def calculate_entry_score(row: pd.Series) -> Tuple[int, str, str]:
    """
    Calculate entry path score and recommend best approach.

    Returns:
        Tuple of (score, recommended_path, path_details)
    """
    score = 0
    details = []

    # Mutual connections (highest value)
    mutual_count = row.get('mutual_count', 0)
    if pd.notna(mutual_count) and mutual_count > 0:
        mutual_count = int(mutual_count)
        score += SCORING_WEIGHTS['mutual_connections']
        score += min(mutual_count, 5) * SCORING_WEIGHTS['mutual_per_connection']
        details.append(f"{mutual_count} mutual(s)")

    # LinkedIn profile
    linkedin = row.get('linkedin_profile', '')
    has_linkedin = pd.notna(linkedin) and str(linkedin).strip() and str(linkedin).lower() != 'nan'
    if has_linkedin:
        score += SCORING_WEIGHTS['has_linkedin']
        details.append("LinkedIn")

    # Email
    email = row.get('email', '')
    has_email = pd.notna(email) and str(email).strip() and '@' in str(email)
    if has_email:
        score += SCORING_WEIGHTS['has_email']
        details.append("email")

    # Meta ads (active marketer)
    has_meta = row.get('has_meta_ads', False)
    if pd.notna(has_meta) and (has_meta == True or str(has_meta).lower() == 'true'):
        score += SCORING_WEIGHTS['has_meta_ads']
        details.append("Meta ads")

    # Marketing pixel (tech-savvy)
    has_pixel = row.get('has_marketing_pixel', False)
    if pd.notna(has_pixel) and (has_pixel == True or str(has_pixel).lower() == 'true'):
        score += SCORING_WEIGHTS['has_marketing_pixel']
        details.append("tracking pixel")

    # Instagram
    instagram = row.get('instagram_handle', '')
    has_instagram = pd.notna(instagram) and str(instagram).strip() and str(instagram).lower() != 'nan'
    if has_instagram:
        score += SCORING_WEIGHTS['has_instagram']
        details.append("Instagram")

    # Volume (top producer)
    sold_volume = row.get('sold_volume', 0)
    if pd.notna(sold_volume) and sold_volume > 50000000:  # $50M+
        score += SCORING_WEIGHTS['high_volume']
        details.append("high volume")

    # IDX
    has_idx = row.get('has_idx', False)
    if pd.notna(has_idx) and (has_idx == True or str(has_idx).lower() == 'true'):
        score += SCORING_WEIGHTS['has_idx']

    # Determine recommended path
    mutual_names = row.get('mutual_names', '')
    if pd.isna(mutual_names) or not isinstance(mutual_names, str):
        mutual_names = ''
    first_mutual = mutual_names.split(';')[0].strip() if mutual_names else ''

    if mutual_count and mutual_count > 0:
        recommended = f"Ask {first_mutual or 'mutual connection'} for intro"
        path_type = "warm_intro"
    elif has_linkedin:
        recommended = "LinkedIn connect + personalized note"
        path_type = "linkedin"
    elif has_instagram:
        recommended = "Instagram: follow, engage, then DM"
        path_type = "instagram"
    elif has_email:
        recommended = "Cold email with value prop"
        path_type = "email"
    elif has_meta:
        recommended = "Facebook page message"
        path_type = "facebook"
    else:
        recommended = "Research more contact info"
        path_type = "research"

    path_details = ", ".join(details) if details else "limited data"

    return score, recommended, path_type, path_details


def main():
    print(f"\n{'='*60}")
    print("REPLIERS ENTRY PATH RANKER")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Rank optimal entry paths for Repliers agents')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT, help='Input CSV')
    parser.add_argument('--export-top', type=int, help='Export top N agents to separate file')

    args = parser.parse_args()

    # Resolve path
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = BASE_DIR / args.input

    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return 1

    # Load data
    df = pd.read_csv(input_path)
    print(f"\nLoaded {len(df)} agents from {input_path.name}")

    # Calculate scores for all rows
    print("\nCalculating entry path scores...")
    scores = []
    recommendations = []
    path_types = []
    path_details = []

    for idx, row in df.iterrows():
        score, rec, ptype, details = calculate_entry_score(row)
        scores.append(score)
        recommendations.append(rec)
        path_types.append(ptype)
        path_details.append(details)

    df['entry_path_score'] = scores
    df['recommended_path'] = recommendations
    df['entry_path_type'] = path_types
    df['entry_path_signals'] = path_details

    # Sort by score descending
    df = df.sort_values('entry_path_score', ascending=False)

    # Save
    df.to_csv(input_path, index=False)
    print(f"Saved scores to: {input_path}")

    # Summary statistics
    print(f"\n{'='*60}")
    print("ENTRY PATH ANALYSIS")
    print(f"{'='*60}")

    # Score distribution
    print("\nScore Distribution:")
    bins = [(80, 100, "Excellent (80-100)"),
            (60, 79, "Good (60-79)"),
            (40, 59, "Moderate (40-59)"),
            (20, 39, "Limited (20-39)"),
            (0, 19, "Poor (0-19)")]

    for low, high, label in bins:
        count = len(df[(df['entry_path_score'] >= low) & (df['entry_path_score'] <= high)])
        pct = count / len(df) * 100
        bar = "█" * int(pct / 5)
        print(f"  {label:20} {count:4d} ({pct:5.1f}%) {bar}")

    # Path type distribution
    print("\nRecommended Path Distribution:")
    path_counts = df['entry_path_type'].value_counts()
    for path, count in path_counts.items():
        pct = count / len(df) * 100
        print(f"  {path:15} {count:4d} ({pct:5.1f}%)")

    # Top agents
    print(f"\nTop 15 Agents by Entry Score:")
    print("-" * 80)
    for i, (_, row) in enumerate(df.head(15).iterrows(), 1):
        score = row['entry_path_score']
        name = row['agent_name'][:25]
        rec = row['recommended_path'][:35]
        signals = row['entry_path_signals'][:30]
        print(f"{i:2d}. {score:3.0f} pts | {name:25} | {rec:35} | {signals}")

    # Warm intro opportunities (mutual connections)
    df_warm = df[df['entry_path_type'] == 'warm_intro']
    if len(df_warm) > 0:
        print(f"\n{'='*60}")
        print(f"WARM INTRO OPPORTUNITIES ({len(df_warm)} agents)")
        print(f"{'='*60}")
        for i, (_, row) in enumerate(df_warm.head(20).iterrows(), 1):
            name = row['agent_name'][:25]
            mutual = row.get('mutual_names', '')
            if pd.isna(mutual) or not isinstance(mutual, str):
                mutual = ''
            mutual = mutual[:40]
            volume = row.get('sold_volume', 0)
            vol_str = f"${volume/1e6:.1f}M" if pd.notna(volume) and volume > 0 else ""
            print(f"{i:2d}. {name:25} via {mutual:40} {vol_str}")

    # Export top agents if requested
    if args.export_top:
        top_df = df.head(args.export_top)
        export_path = input_path.parent / f"top_{args.export_top}_entry_paths.csv"
        top_df.to_csv(export_path, index=False)
        print(f"\nExported top {args.export_top} agents to: {export_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
