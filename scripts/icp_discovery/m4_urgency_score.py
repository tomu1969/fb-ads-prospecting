"""
Module 4: Urgency Score (0-50)

Measures how time-sensitive lead response is based on destination shares,
immediacy language in ad copy, and qualification complexity.

Input: output/icp_discovery/03_money_scored.csv
Output: output/icp_discovery/04_urgency_scored.csv

Usage:
    python scripts/icp_discovery/m4_urgency_score.py
    python scripts/icp_discovery/m4_urgency_score.py --csv output/icp_discovery/03_money_scored.csv
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from tqdm import tqdm

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from constants import COMPILED_IMMEDIACY, COMPILED_QUALIFICATION

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m4_urgency_score.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '03_money_scored.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '04_urgency_scored.csv'


def count_keyword_matches(text: str, patterns: list) -> int:
    """Count how many patterns match in the text."""
    if not text or pd.isna(text):
        return 0

    text = str(text).lower()
    matches = sum(1 for p in patterns if p.search(text))
    return matches


def calculate_urgency_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate Urgency Score (0-50) based on lead response time sensitivity.

    Components:
    - Direct Share Score (0-25): MESSAGE + CALL shares (MESSAGE weighted 1.5x)
    - Form Share Score (0-10): Form leads need follow-up
    - Immediacy Language Score (0-10): Time-sensitive keywords in copy
    - Qualification Keywords Score (0-5): Complexity implies conversation need

    Returns dict with urgency_score and breakdown.
    """
    breakdown = []

    # === DIRECT SHARE SCORE (0-25) ===
    share_message = row.get('share_message', 0)
    share_call = row.get('share_call', 0)

    try:
        share_message = float(share_message) if pd.notna(share_message) else 0
        share_call = float(share_call) if pd.notna(share_call) else 0
    except (ValueError, TypeError):
        share_message = share_call = 0

    # MESSAGE weighted 1.5x because messaging is more conversational
    weighted_direct = share_message * 1.5 + share_call
    direct_score = min(25, int(weighted_direct * 25))
    breakdown.append(f"direct:{direct_score}")

    # === FORM SHARE SCORE (0-10) ===
    share_form = row.get('share_form', 0)
    try:
        share_form = float(share_form) if pd.notna(share_form) else 0
    except (ValueError, TypeError):
        share_form = 0

    form_score = min(10, int(share_form * 10))
    breakdown.append(f"form:{form_score}")

    # === IMMEDIACY LANGUAGE SCORE (0-10) ===
    ad_text = row.get('ad_texts_combined', '')
    immediacy_matches = count_keyword_matches(ad_text, COMPILED_IMMEDIACY)
    # 2 points per match, max 10
    immediacy_score = min(10, immediacy_matches * 2)
    breakdown.append(f"immediacy:{immediacy_score}")

    # === QUALIFICATION KEYWORDS SCORE (0-5) ===
    qualification_matches = count_keyword_matches(ad_text, COMPILED_QUALIFICATION)
    # 2 points per match, max 5
    qualification_score = min(5, qualification_matches * 2)
    breakdown.append(f"qualification:{qualification_score}")

    # === TOTAL ===
    total_score = direct_score + form_score + immediacy_score + qualification_score

    return {
        'urgency_score': min(50, total_score),
        'urgency_breakdown': '|'.join(breakdown),
        'urgency_direct': direct_score,
        'urgency_form': form_score,
        'urgency_immediacy': immediacy_score,
        'urgency_qualification': qualification_score,
    }


def score_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate urgency score for all advertisers.

    Returns DataFrame with urgency_score columns added.
    """
    logger.info(f"Calculating urgency scores for {len(df)} advertisers...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Scoring"):
        try:
            scores = calculate_urgency_score(row)
            results.append(scores)
        except Exception as e:
            logger.warning(f"Error scoring {row.get('page_name', idx)}: {e}")
            results.append({
                'urgency_score': 0,
                'urgency_breakdown': 'error',
                'urgency_direct': 0,
                'urgency_form': 0,
                'urgency_immediacy': 0,
                'urgency_qualification': 0,
            })

    result_df = pd.DataFrame(results)
    for col in result_df.columns:
        df[col] = result_df[col].values

    # Handle empty dataframe
    if len(df) == 0:
        logger.warning("No advertisers to score")
        return df

    # Calculate combined score for ranking
    df['combined_score'] = df['money_score'] + df['urgency_score']

    # Log distribution
    logger.info("Urgency score distribution:")
    bins = [0, 10, 20, 30, 40, 50]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-50']
    df['_urgency_bin'] = pd.cut(df['urgency_score'], bins=bins, labels=labels, include_lowest=True)
    dist = df['_urgency_bin'].value_counts().sort_index()
    for bin_label, count in dist.items():
        pct = count / len(df) * 100
        logger.info(f"  {bin_label}: {count} ({pct:.1f}%)")
    df.drop('_urgency_bin', axis=1, inplace=True)

    logger.info(f"Mean urgency score: {df['urgency_score'].mean():.1f}")
    logger.info(f"Max urgency score: {df['urgency_score'].max()}")
    logger.info(f"Mean combined score (money + urgency): {df['combined_score'].mean():.1f}")

    return df


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 4: URGENCY SCORE")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Calculate urgency scores')
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
        logger.info(f"Run m3_money_score.py first.")
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

    # Sort by combined_score descending
    df = df.sort_values('combined_score', ascending=False).reset_index(drop=True)

    # Add rank
    df['rank'] = range(1, len(df) + 1)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved scored data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"URGENCY SCORE SUMMARY")
    print(f"{'='*60}")
    print(f"  Total scored:      {len(df)} advertisers")
    print(f"  Mean urgency:      {df['urgency_score'].mean():.1f}")
    print(f"  Median urgency:    {df['urgency_score'].median():.1f}")
    print(f"  Max urgency:       {df['urgency_score'].max()}")
    print(f"  Mean combined:     {df['combined_score'].mean():.1f}")
    print(f"{'='*60}")

    # Top advertisers by combined score
    print("\nTop 15 by combined score (money + urgency):")
    print(f"{'Rank':<5} {'Money':<6} {'Urgency':<8} {'Combined':<9} {'Page Name':<45}")
    print("-" * 75)
    for _, row in df.head(15).iterrows():
        print(f"{row['rank']:<5} {row['money_score']:<6} {row['urgency_score']:<8} "
              f"{row['combined_score']:<9} {str(row['page_name'])[:45]:<45}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
