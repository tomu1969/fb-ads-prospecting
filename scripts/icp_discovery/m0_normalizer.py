"""
Module 0: Data Normalization

Normalizes raw Facebook Ads Library scraper output into a clean, consistent ad-level table.
Classifies destination types (MESSAGE, CALL, FORM, WEB) based on CTA and URL patterns.

Input: Raw CSV from Apify actor (ad-level with snapshot field)
Output: output/icp_discovery/00_ads_normalized.csv

Usage:
    python scripts/icp_discovery/m0_normalizer.py --csv output/fb_ads_scraped_broad.csv
    python scripts/icp_discovery/m0_normalizer.py --csv output/fb_ads.csv --limit 100
"""

import os
import sys
import argparse
import logging
import json
import ast
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from constants import (
    MESSAGE_CTA_TYPES, CALL_CTA_TYPES, FORM_CTA_TYPES,
    COMPILED_MESSAGE_URL_PATTERNS, COMPILED_CALL_URL_PATTERNS, COMPILED_FORM_URL_PATTERNS,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m0_normalizer.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'


def parse_snapshot(snapshot_str: Any) -> Dict[str, Any]:
    """Parse snapshot field from string or dict.

    Handles both JSON format and Python dict literals (single quotes, None).
    """
    if pd.isna(snapshot_str) or snapshot_str is None:
        return {}

    if isinstance(snapshot_str, dict):
        return snapshot_str

    if isinstance(snapshot_str, str):
        # Try ast.literal_eval first (handles Python dict literals with single quotes and None)
        try:
            result = ast.literal_eval(snapshot_str)
            if isinstance(result, dict):
                return result
        except (ValueError, SyntaxError, TypeError):
            pass

        # Fallback to JSON parsing
        try:
            return json.loads(snapshot_str)
        except (json.JSONDecodeError, TypeError):
            return {}

    return {}


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL."""
    if not url or pd.isna(url):
        return None

    try:
        parsed = urlparse(str(url))
        domain = parsed.netloc or parsed.path.split('/')[0]
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.lower() if domain else None
    except Exception:
        return None


def classify_destination_type(cta_type: str, link_url: str, platforms: list) -> str:
    """
    Classify destination type based on CTA and URL.

    Priority: MESSAGE > CALL > FORM > WEB

    Returns: 'MESSAGE', 'CALL', 'FORM', or 'WEB'
    """
    cta_type = str(cta_type).upper() if cta_type and not pd.isna(cta_type) else ''
    link_url = str(link_url).lower() if link_url and not pd.isna(link_url) else ''

    # Check MESSAGE (highest priority)
    if cta_type in MESSAGE_CTA_TYPES:
        return 'MESSAGE'
    for pattern in COMPILED_MESSAGE_URL_PATTERNS:
        if pattern.search(link_url):
            return 'MESSAGE'
    # Messenger-only platform indicates MESSAGE
    if platforms and platforms == ['MESSENGER']:
        return 'MESSAGE'

    # Check CALL
    if cta_type in CALL_CTA_TYPES:
        return 'CALL'
    for pattern in COMPILED_CALL_URL_PATTERNS:
        if pattern.search(link_url):
            return 'CALL'

    # Check FORM
    if cta_type in FORM_CTA_TYPES:
        return 'FORM'
    for pattern in COMPILED_FORM_URL_PATTERNS:
        if pattern.search(link_url):
            return 'FORM'

    # Default to WEB
    return 'WEB'


def parse_timestamp(ts: Any) -> Optional[datetime]:
    """Parse timestamp from various formats."""
    if pd.isna(ts) or ts is None:
        return None

    # Unix timestamp (seconds)
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts)
        except (ValueError, OSError):
            return None

    # String timestamp
    if isinstance(ts, str):
        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
        # Try parsing as unix timestamp string
        try:
            return datetime.fromtimestamp(int(ts))
        except (ValueError, OSError):
            return None

    return None


def parse_list_field(field: Any) -> list:
    """Parse a field that could be a list, JSON string, or None."""
    if pd.isna(field) or field is None:
        return []

    if isinstance(field, list):
        return field

    if isinstance(field, str):
        try:
            parsed = json.loads(field)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []

    return []


def normalize_ad(row: pd.Series) -> Dict[str, Any]:
    """
    Normalize a single ad row.

    Returns dict with normalized fields.
    """
    # Parse snapshot
    snapshot = parse_snapshot(row.get('snapshot'))

    # Extract CTA info
    cta_type = snapshot.get('cta_type', '') or row.get('cta_type', '')
    cta_text = snapshot.get('cta_text', '') or ''
    link_url = snapshot.get('link_url', '') or row.get('link_url', '')

    # Parse platforms
    platforms = parse_list_field(row.get('publisher_platform', []))

    # Classify destination
    destination_type = classify_destination_type(cta_type, link_url, platforms)

    # Extract domain
    domain = extract_domain(link_url)

    # Parse timestamps
    start_date = parse_timestamp(row.get('start_date'))
    end_date = parse_timestamp(row.get('end_date'))

    # Calculate days live
    days_live = None
    if start_date:
        end = end_date or datetime.now()
        days_live = (end - start_date).days

    # Extract ad text
    body = snapshot.get('body', {})
    ad_text = ''
    if isinstance(body, dict):
        ad_text = body.get('text', '') or body.get('markup', {}).get('__html', '')
    elif isinstance(body, str):
        ad_text = body

    title = snapshot.get('title', '') or ''
    link_description = snapshot.get('link_description', '') or ''
    caption = snapshot.get('caption', '') or ''

    # Media flags
    images = snapshot.get('images', []) or []
    videos = snapshot.get('videos', []) or []
    cards = snapshot.get('cards', []) or []

    has_image = len(images) > 0 if isinstance(images, list) else False
    has_video = len(videos) > 0 if isinstance(videos, list) else False
    has_carousel = len(cards) > 1 if isinstance(cards, list) else False

    # Page info
    page_like_count = snapshot.get('page_like_count', 0) or row.get('page_like_count', 0) or 0

    return {
        # Identifiers
        'ad_archive_id': row.get('ad_archive_id'),
        'page_id': row.get('page_id'),
        'page_name': row.get('page_name', ''),

        # Destination classification
        'destination_type': destination_type,
        'cta_type': cta_type,
        'cta_text': cta_text,
        'link_url': link_url,
        'domain': domain,

        # Timing
        'start_date': start_date.isoformat() if start_date else None,
        'end_date': end_date.isoformat() if end_date else None,
        'days_live': days_live,
        'is_active': bool(row.get('is_active', False)),

        # Content
        'ad_text': ad_text[:2000] if ad_text else '',  # Truncate long text
        'title': title[:500] if title else '',
        'link_description': link_description[:500] if link_description else '',

        # Media
        'has_image': has_image,
        'has_video': has_video,
        'has_carousel': has_carousel,
        'card_count': len(cards) if isinstance(cards, list) else 0,

        # Platform & targeting
        'platforms': json.dumps(platforms),
        'platform_count': len(platforms),
        'targeted_countries': row.get('targeted_or_reached_countries', ''),

        # Scale
        'page_like_count': int(page_like_count) if page_like_count else 0,
        'page_category': row.get('page_category', '') or '',

        # Grouping
        'collation_id': row.get('collation_id'),
        'collation_count': row.get('collation_count', 1) or 1,
    }


def normalize_all(df: pd.DataFrame, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Normalize all ads in the dataframe.

    Returns normalized DataFrame.
    """
    if limit:
        df = df.head(limit)

    logger.info(f"Normalizing {len(df)} ads...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Normalizing"):
        try:
            normalized = normalize_ad(row)
            results.append(normalized)
        except Exception as e:
            logger.warning(f"Error normalizing ad {row.get('ad_archive_id', idx)}: {e}")
            continue

        if (idx + 1) % 1000 == 0:
            logger.info(f"[{idx + 1}/{len(df)}] Processed...")

    result_df = pd.DataFrame(results)

    # Log destination type distribution
    if 'destination_type' in result_df.columns:
        dist = result_df['destination_type'].value_counts()
        logger.info("Destination type distribution:")
        for dtype, count in dist.items():
            pct = count / len(result_df) * 100
            logger.info(f"  {dtype}: {count} ({pct:.1f}%)")

    return result_df


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 0: DATA NORMALIZATION")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Normalize raw FB Ads data')
    parser.add_argument('--csv', '-i', required=True, help='Input CSV file (raw ad-level data)')
    parser.add_argument('--output', '-o', help='Output CSV path')
    parser.add_argument('--limit', type=int, help='Limit number of ads to process')
    args = parser.parse_args()

    # Resolve input path
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = BASE_DIR / args.csv

    if not csv_path.exists():
        logger.error(f"Input file not found: {csv_path}")
        return 1

    # Determine output path
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / '00_ads_normalized.csv'

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.limit:
        print(f"Limit:  {args.limit} ads")
    print()

    # Load CSV
    logger.info(f"Loading {csv_path}...")
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
        logger.info(f"Loaded {len(df)} rows")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return 1

    # Check for required columns
    if 'snapshot' not in df.columns and 'cta_type' not in df.columns:
        logger.warning("No 'snapshot' or 'cta_type' column found. Destination classification may be limited.")

    # Normalize
    df_normalized = normalize_all(df, limit=args.limit)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_normalized.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved normalized data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"COMPLETED: {len(df_normalized)} ads normalized")
    print(f"{'='*60}")

    # Show sample
    if len(df_normalized) > 0:
        print("\nSample destination types:")
        sample = df_normalized.groupby('destination_type').size()
        for dtype, count in sample.items():
            print(f"  {dtype}: {count}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
