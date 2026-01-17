"""
Module 1: Advertiser (Page-Level) Aggregation

Converts ad-level data into advertiser-level behavior metrics.
Groups by page_id and calculates destination shares, velocity, always-on behavior.

Input: output/icp_discovery/00_ads_normalized.csv
Output: output/icp_discovery/01_pages_aggregated.csv

Usage:
    python scripts/icp_discovery/m1_aggregator.py
    python scripts/icp_discovery/m1_aggregator.py --csv output/icp_discovery/00_ads_normalized.csv
"""

import os
import sys
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m1_aggregator.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '00_ads_normalized.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '01_pages_aggregated.csv'

# Constants
ALWAYS_ON_THRESHOLD_DAYS = 21  # Ads running 21+ days are "always on"
RECENT_DAYS = 30  # Window for new ads velocity


def safe_json_loads(value: Any) -> list:
    """Safely parse JSON list field."""
    if pd.isna(value) or value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            result = json.loads(value)
            return result if isinstance(result, list) else []
        except:
            return []
    return []


def aggregate_page(page_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Aggregate metrics for a single page/advertiser.

    Returns dict with page-level metrics.
    """
    now = datetime.now()
    cutoff_30d = now - timedelta(days=RECENT_DAYS)

    # Basic counts
    total_ads = len(page_df)
    active_ads = page_df['is_active'].sum() if 'is_active' in page_df.columns else total_ads

    # Velocity: new ads in last 30 days
    new_ads_30d = 0
    if 'start_date' in page_df.columns:
        for _, row in page_df.iterrows():
            try:
                start = row.get('start_date')
                if pd.notna(start):
                    if isinstance(start, str):
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00').split('+')[0])
                    else:
                        start_dt = start
                    if start_dt >= cutoff_30d:
                        new_ads_30d += 1
            except:
                continue

    # Always-on share: % of ads running 21+ days
    always_on_count = 0
    if 'days_live' in page_df.columns:
        days_live = pd.to_numeric(page_df['days_live'], errors='coerce')
        always_on_count = (days_live >= ALWAYS_ON_THRESHOLD_DAYS).sum()

    always_on_share = always_on_count / total_ads if total_ads > 0 else 0

    # Creative refresh rate: distinct collations / total ads
    distinct_collations = page_df['collation_id'].nunique() if 'collation_id' in page_df.columns else total_ads
    creative_refresh_rate = distinct_collations / total_ads if total_ads > 0 else 0

    # Destination type shares
    if 'destination_type' in page_df.columns:
        dest_counts = page_df['destination_type'].value_counts()
        share_message = dest_counts.get('MESSAGE', 0) / total_ads
        share_call = dest_counts.get('CALL', 0) / total_ads
        share_form = dest_counts.get('FORM', 0) / total_ads
        share_web = dest_counts.get('WEB', 0) / total_ads
    else:
        share_message = share_call = share_form = 0
        share_web = 1.0

    # Dominant CTA type
    dominant_cta = ''
    if 'cta_type' in page_df.columns:
        cta_counts = page_df['cta_type'].value_counts()
        if len(cta_counts) > 0:
            dominant_cta = cta_counts.index[0]

    # Platform mix
    platforms_all = []
    if 'platforms' in page_df.columns:
        for platforms_str in page_df['platforms']:
            platforms_all.extend(safe_json_loads(platforms_str))

    unique_platforms = list(set(platforms_all))
    platform_count = len(unique_platforms)

    # Calculate platform shares
    platform_shares = {}
    if platforms_all:
        from collections import Counter
        platform_counter = Counter(platforms_all)
        total_platform_mentions = sum(platform_counter.values())
        for platform, count in platform_counter.items():
            key = f"platform_{platform.lower()}_share"
            platform_shares[key] = count / total_platform_mentions

    # Page metadata
    page_id = page_df['page_id'].iloc[0]
    page_name = page_df['page_name'].iloc[0] if 'page_name' in page_df.columns else ''
    page_category = page_df['page_category'].iloc[0] if 'page_category' in page_df.columns else ''

    # Page like count (max across ads)
    page_like_count = 0
    if 'page_like_count' in page_df.columns:
        page_like_count = int(pd.to_numeric(page_df['page_like_count'], errors='coerce').max() or 0)

    # Combined ad text for keyword analysis
    ad_texts = []
    for col in ['ad_text', 'title', 'link_description']:
        if col in page_df.columns:
            texts = page_df[col].dropna().astype(str).tolist()
            ad_texts.extend([t for t in texts if t and t != 'nan'])

    ad_texts_combined = ' | '.join(ad_texts[:20])[:5000]  # Limit size

    # Domains
    domains = []
    if 'domain' in page_df.columns:
        domains = page_df['domain'].dropna().unique().tolist()[:5]

    # Has carousel ads
    has_carousel = False
    if 'has_carousel' in page_df.columns:
        has_carousel = page_df['has_carousel'].any()

    # Has video ads
    has_video = False
    if 'has_video' in page_df.columns:
        has_video = page_df['has_video'].any()

    result = {
        # Identifiers
        'page_id': page_id,
        'page_name': page_name,
        'page_category': page_category,

        # Volume metrics
        'total_ads': total_ads,
        'active_ads': int(active_ads),
        'distinct_collations': distinct_collations,

        # Velocity & persistence
        'new_ads_30d': new_ads_30d,
        'always_on_share': round(always_on_share, 4),
        'creative_refresh_rate': round(creative_refresh_rate, 4),

        # Destination shares
        'share_message': round(share_message, 4),
        'share_call': round(share_call, 4),
        'share_form': round(share_form, 4),
        'share_web': round(share_web, 4),
        'dominant_cta': dominant_cta,

        # Scale
        'page_like_count': page_like_count,

        # Platform mix
        'platform_count': platform_count,
        'platforms': json.dumps(unique_platforms),

        # Content signals
        'ad_texts_combined': ad_texts_combined,
        'domains': json.dumps(domains),
        'has_carousel': has_carousel,
        'has_video': has_video,
    }

    # Add platform shares
    result.update(platform_shares)

    return result


def aggregate_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate all ads to page level.

    Returns page-level DataFrame.
    """
    logger.info(f"Aggregating {len(df)} ads by page_id...")

    if 'page_id' not in df.columns:
        logger.error("No 'page_id' column found")
        return pd.DataFrame()

    # Group by page_id
    grouped = df.groupby('page_id')
    total_pages = len(grouped)
    logger.info(f"Found {total_pages} unique advertisers")

    results = []
    for idx, (page_id, page_df) in enumerate(tqdm(grouped, total=total_pages, desc="Aggregating")):
        try:
            aggregated = aggregate_page(page_df)
            results.append(aggregated)
        except Exception as e:
            logger.warning(f"Error aggregating page {page_id}: {e}")
            continue

        if (idx + 1) % 500 == 0:
            logger.info(f"[{idx + 1}/{total_pages}] Aggregated...")

    result_df = pd.DataFrame(results)

    # Sort by active_ads descending
    if 'active_ads' in result_df.columns:
        result_df = result_df.sort_values('active_ads', ascending=False).reset_index(drop=True)

    # Log summary stats
    if len(result_df) > 0:
        logger.info(f"Aggregation complete: {len(result_df)} advertisers")
        logger.info(f"  Avg active ads: {result_df['active_ads'].mean():.1f}")
        logger.info(f"  Avg destination shares: MESSAGE={result_df['share_message'].mean():.3f}, "
                   f"CALL={result_df['share_call'].mean():.3f}, "
                   f"FORM={result_df['share_form'].mean():.3f}, "
                   f"WEB={result_df['share_web'].mean():.3f}")

    return result_df


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 1: ADVERTISER AGGREGATION")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Aggregate ads to page level')
    parser.add_argument('--csv', '-i', help='Input CSV file (normalized ad-level)')
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
        logger.info(f"Run m0_normalizer.py first to create normalized data.")
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
        logger.info(f"Loaded {len(df)} ads")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return 1

    # Aggregate
    df_aggregated = aggregate_all(df)

    if len(df_aggregated) == 0:
        logger.error("No data to save after aggregation")
        return 1

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_aggregated.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved aggregated data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"COMPLETED: {len(df_aggregated)} advertisers aggregated")
    print(f"{'='*60}")

    # Show top advertisers by ad count
    print("\nTop 10 advertisers by active ads:")
    top10 = df_aggregated.head(10)[['page_name', 'active_ads', 'share_message', 'share_form']]
    for _, row in top10.iterrows():
        print(f"  {row['page_name'][:40]:<40} | ads={row['active_ads']:>4} | msg={row['share_message']:.2f} | form={row['share_form']:.2f}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
