"""
Module 3: Money Score (0-50) - HARDENED

Estimates willingness and ability to pay using ad behavior as spend proxy.
Since actual spend data is 99.98% missing in FB Ads Library, uses ad volume,
velocity, always-on behavior, and scale as proxies.

HARDENED RULES:
- ad_count CAPPED at 30% contribution (15/50 max points)
- Uses log scale for ad volume and velocity to prevent outlier bias
- always_on + velocity weighted higher (50% of score)

Input: output/icp_discovery/02_pages_candidate.csv
Output: output/icp_discovery/03_money_scored.csv

Usage:
    python scripts/icp_discovery/m3_money_score.py
    python scripts/icp_discovery/m3_money_score.py --csv output/icp_discovery/02_pages_candidate.csv
"""

import os
import sys
import argparse
import logging
import math
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from tqdm import tqdm

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from constants import SCALE_THRESHOLDS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m3_money_score.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '02_pages_candidate.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '03_money_scored.csv'


def score_from_thresholds(value: float, thresholds: list) -> int:
    """Get score based on threshold list [(threshold, score), ...]"""
    for threshold, score in thresholds:
        if value >= threshold:
            return score
    return 0


def calculate_ad_volume_score(active_ads: int) -> int:
    """
    Calculate ad volume score using log scale.

    HARDENED: Capped at 15 points (30% of total 50).
    Uses log2 scale to prevent content farms from dominating.

    log2(100+1) ≈ 6.66 → 15 points (cap)
    log2(50+1) ≈ 5.67 → 13 points
    log2(20+1) ≈ 4.39 → 10 points
    log2(10+1) ≈ 3.46 → 7 points
    log2(5+1) ≈ 2.58 → 5 points
    """
    if active_ads <= 0:
        return 0

    # Cap at 100 ads to prevent outliers
    capped_ads = min(active_ads, 100)

    # Log scale: log2(ads+1) * 2.25, capped at 15
    raw_score = math.log2(capped_ads + 1) * 2.25
    return min(15, int(raw_score))


def calculate_velocity_score(new_ads_30d: int) -> int:
    """
    Calculate velocity score using log scale.

    FIX 6: Scale velocity safely with log scale.
    Prevents high-velocity spammers from getting max score.

    log2(10+1) ≈ 3.46 → 7 points
    log2(5+1) ≈ 2.58 → 5 points
    log2(2+1) ≈ 1.58 → 3 points
    """
    if new_ads_30d <= 0:
        return 0

    # Log scale: log2(velocity+1) * 2, capped at 10
    raw_score = math.log2(new_ads_30d + 1) * 2
    return min(10, int(raw_score))


def calculate_always_on_score(always_on_share: float) -> int:
    """
    Calculate always-on score.

    HARDENED: Weighted higher (15 points max) because sustained
    ad running indicates real budget commitment.

    Linear scale: 100% always-on = 15 points
    """
    if always_on_share <= 0:
        return 0

    # Linear scale: always_on_share * 15, capped at 15
    return min(15, int(always_on_share * 15))


def calculate_scale_score(page_like_count: int) -> int:
    """
    Calculate scale score using log scale.

    page_like_count as brand maturity/size signal.

    log10(100000+1) = 5 → 10 points
    log10(10000+1) ≈ 4 → 8 points
    log10(1000+1) ≈ 3 → 6 points
    """
    if page_like_count <= 0:
        return 0

    # Log scale: log10(likes+1) * 2, capped at 10
    raw_score = math.log10(page_like_count + 1) * 2
    return min(10, int(raw_score))


def calculate_money_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate Money Score (0-50) using ad behavior as spend proxy.

    HARDENED COMPONENTS:
    - Ad Volume Score (0-15): Log scale, CAPPED at 30% contribution
    - Always-On Score (0-15): Linear scale, WEIGHTED HIGHER
    - Velocity Score (0-10): Log scale to prevent spammer bias
    - Scale Proxy Score (0-10): Log scale on page_like_count

    Document: ad_count max contribution = 15/50 = 30%
    Document: always_on + velocity = 25/50 = 50% (behavior weighted higher)

    Returns dict with money_score and breakdown.
    """
    breakdown = []

    # === AD VOLUME SCORE (0-15) — CAPPED, log scale ===
    active_ads = row.get('active_ads', 0)
    try:
        active_ads = int(active_ads) if pd.notna(active_ads) else 0
    except (ValueError, TypeError):
        active_ads = 0

    ad_volume_score = calculate_ad_volume_score(active_ads)
    breakdown.append(f"ad_volume:{ad_volume_score}")

    # === ALWAYS-ON SCORE (0-15) — WEIGHTED HIGHER ===
    always_on_share = row.get('always_on_share', 0)
    try:
        always_on_share = float(always_on_share) if pd.notna(always_on_share) else 0
    except (ValueError, TypeError):
        always_on_share = 0

    always_on_score = calculate_always_on_score(always_on_share)
    breakdown.append(f"always_on:{always_on_score}")

    # === VELOCITY SCORE (0-10) — log scale ===
    new_ads_30d = row.get('new_ads_30d', 0)
    try:
        new_ads_30d = int(new_ads_30d) if pd.notna(new_ads_30d) else 0
    except (ValueError, TypeError):
        new_ads_30d = 0

    velocity_score = calculate_velocity_score(new_ads_30d)
    breakdown.append(f"velocity:{velocity_score}")

    # === SCALE PROXY SCORE (0-10) ===
    page_like_count = row.get('page_like_count', 0)
    try:
        page_like_count = int(page_like_count) if pd.notna(page_like_count) else 0
    except (ValueError, TypeError):
        page_like_count = 0

    scale_score = calculate_scale_score(page_like_count)
    breakdown.append(f"scale:{scale_score}")

    # === TOTAL ===
    total_score = ad_volume_score + always_on_score + velocity_score + scale_score

    return {
        'money_score': min(50, total_score),
        'money_breakdown': '|'.join(breakdown),
        'money_ad_volume': ad_volume_score,
        'money_always_on': always_on_score,
        'money_velocity': velocity_score,
        'money_scale': scale_score,
    }


def score_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate money score for all advertisers.

    Returns DataFrame with money_score columns added.
    """
    logger.info(f"Calculating money scores for {len(df)} advertisers...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Scoring"):
        try:
            scores = calculate_money_score(row)
            results.append(scores)
        except Exception as e:
            logger.warning(f"Error scoring {row.get('page_name', idx)}: {e}")
            results.append({
                'money_score': 0,
                'money_breakdown': 'error',
                'money_ad_volume': 0,
                'money_always_on': 0,
                'money_velocity': 0,
                'money_scale': 0,
            })

    result_df = pd.DataFrame(results)
    for col in result_df.columns:
        df[col] = result_df[col].values

    # Handle empty dataframe
    if len(df) == 0:
        logger.warning("No advertisers to score")
        return df

    # Log distribution
    logger.info("Money score distribution:")
    bins = [0, 10, 20, 30, 40, 50]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-50']
    df['_money_bin'] = pd.cut(df['money_score'], bins=bins, labels=labels, include_lowest=True)
    dist = df['_money_bin'].value_counts().sort_index()
    for bin_label, count in dist.items():
        pct = count / len(df) * 100
        logger.info(f"  {bin_label}: {count} ({pct:.1f}%)")
    df.drop('_money_bin', axis=1, inplace=True)

    logger.info(f"Mean money score: {df['money_score'].mean():.1f}")
    logger.info(f"Max money score: {df['money_score'].max()}")

    # Log component distributions
    logger.info("Component score averages:")
    logger.info(f"  ad_volume: {df['money_ad_volume'].mean():.1f} / 15")
    logger.info(f"  always_on: {df['money_always_on'].mean():.1f} / 15")
    logger.info(f"  velocity:  {df['money_velocity'].mean():.1f} / 10")
    logger.info(f"  scale:     {df['money_scale'].mean():.1f} / 10")

    return df


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 3: MONEY SCORE (HARDENED)")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Calculate money scores')
    parser.add_argument('--csv', '-i', help='Input CSV file')
    parser.add_argument('--output', '-o', help='Output CSV path')
    args = parser.parse_args()

    # Resolve input path
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv
    else:
        csv_path = DEFAULT_INPUT

    if not csv_path.exists():
        logger.error(f"Input file not found: {csv_path}")
        logger.info(f"Run m2_conv_gate.py first to create candidate data.")
        return 1

    # Determine output path
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    else:
        output_path = DEFAULT_OUTPUT

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    print()

    # Load CSV
    logger.info(f"Loading {csv_path}...")
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
        logger.info(f"Loaded {len(df)} advertisers")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return 1

    # Score
    df = score_all(df)

    # Sort by money_score descending
    df = df.sort_values('money_score', ascending=False).reset_index(drop=True)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved scored data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"MONEY SCORE SUMMARY (HARDENED)")
    print(f"{'='*60}")
    print(f"  Total scored:     {len(df)} advertisers")
    print(f"  Mean score:       {df['money_score'].mean():.1f}")
    print(f"  Median score:     {df['money_score'].median():.1f}")
    print(f"  Max score:        {df['money_score'].max()}")
    print()
    print(f"  Component breakdown (avg):")
    print(f"    ad_volume:      {df['money_ad_volume'].mean():.1f} / 15 (capped)")
    print(f"    always_on:      {df['money_always_on'].mean():.1f} / 15")
    print(f"    velocity:       {df['money_velocity'].mean():.1f} / 10")
    print(f"    scale:          {df['money_scale'].mean():.1f} / 10")
    print(f"{'='*60}")

    # Top advertisers
    print("\nTop 10 by money score:")
    for idx, row in df.head(10).iterrows():
        name = str(row['page_name'])[:45] if 'page_name' in row else 'Unknown'
        print(f"  {row['money_score']:>2} | {name:<45} | {row['money_breakdown']}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
