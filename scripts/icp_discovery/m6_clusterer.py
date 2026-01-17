"""
Module 6: Behavioral Clustering

Assigns advertisers to behavioral clusters based on their destination types
and conversational patterns. Calculates total_score and flags junk risk.

Clusters (priority order):
1. message_first - MESSAGE dominant or share_message >= 0.2
2. call_first - CALL dominant or share_call >= 0.2
3. form_first - FORM dominant with follow-up language, or share_form >= 0.2
4. web_consult - Consult language + lead intent CTA
5. uncategorized - Fallback (should be < 30%)

Also calculates:
- multi_funnel flag (hash-based distinct_creatives or multiple dest types)
- junk_risk flag (content farm keywords, transactional signals)
- total_score (0-100 normalized)

Input: output/icp_discovery/05_fit_scored.csv
Output: output/icp_discovery/06_clustered.csv

Usage:
    python scripts/icp_discovery/m6_clusterer.py
    python scripts/icp_discovery/m6_clusterer.py --csv output/icp_discovery/05_fit_scored.csv
"""

import os
import sys
import argparse
import logging
import re
from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd
from tqdm import tqdm

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from constants import (
    TRANSACTIONAL_CTA_TYPES,
    TRANSACTIONAL_DOMAINS,
    LEAD_INTENT_CTAS,
    COMPILED_CONSULT,
    COMPILED_FOLLOWUP,
    COMPILED_CONTENT,
    COMPILED_QUALIFICATION_ALL,
    COMPILED_TRANSACTIONAL_PATHS,
    MESSAGE_SHARE_THRESHOLD,
    CALL_SHARE_THRESHOLD,
    FORM_SHARE_THRESHOLD,
    normalize_text,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m6_clusterer.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '05_fit_scored.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '06_clustered.csv'

# Total score normalization factor
# Max raw = 0.45*50 + 0.35*50 + 0.20*50 = 50 (fit_score now 0-50 with split model)
MAX_RAW_SCORE = 50.0


def has_consult_language(ad_text: str) -> bool:
    """Check if ad copy contains consult/qualification language."""
    if not ad_text or pd.isna(ad_text):
        return False
    text = normalize_text(ad_text)
    return any(p.search(text) for p in COMPILED_CONSULT)


def has_followup_language(ad_text: str) -> bool:
    """Check if ad copy contains follow-up language."""
    if not ad_text or pd.isna(ad_text):
        return False
    text = normalize_text(ad_text)
    return any(p.search(text) for p in COMPILED_FOLLOWUP)


def has_qualification_language(ad_text: str) -> bool:
    """Check if ad copy contains qualification/requirements language."""
    if not ad_text or pd.isna(ad_text):
        return False
    text = normalize_text(ad_text)
    return any(p.search(text) for p in COMPILED_QUALIFICATION_ALL)


def check_transactional_url(urls: str) -> bool:
    """Check if any URL in the list is transactional."""
    if not urls or pd.isna(urls):
        return False

    from urllib.parse import urlparse

    url_list = str(urls).split(' | ')
    for url in url_list:
        url = url.strip()
        if not url:
            continue
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            path = parsed.path.lower()

            # Check transactional domains
            if any(d in domain for d in TRANSACTIONAL_DOMAINS):
                return True

            # Check transactional paths
            if any(p.search(path) for p in COMPILED_TRANSACTIONAL_PATHS):
                return True
        except Exception:
            continue

    return False


def compute_distinct_creatives(ad_texts: str) -> int:
    """
    FIX 5: Hash-based distinct_creatives count.

    Count unique creative variants using text hashing.
    ad_texts is the combined ad text string from aggregation.
    """
    if not ad_texts or pd.isna(ad_texts):
        return 0

    # Split on common delimiters (ads are often joined by ' | ' or newlines)
    text_parts = re.split(r'\s*\|\s*|\n+', str(ad_texts))

    unique_hashes = set()
    for text in text_parts:
        text = text.strip()
        if text and len(text) > 20:  # Skip very short/empty
            # Use first 200 chars for hashing
            text_hash = hash(text[:200])
            unique_hashes.add(text_hash)

    return len(unique_hashes)


def check_junk_risk(row: pd.Series) -> bool:
    """
    Check if advertiser is likely a content farm or junk.

    Junk if:
    - Transactional CTA + transactional domain
    - Content/media keywords dominate (3+ matches)
    """
    dominant_cta = str(row.get('dominant_cta', '')).upper()
    ad_text = row.get('ad_texts_combined', '')
    domains = str(row.get('domains', ''))

    # Check transactional CTA + domain combo
    if dominant_cta in TRANSACTIONAL_CTA_TYPES:
        domain_match = any(d in domains.lower() for d in TRANSACTIONAL_DOMAINS)
        if domain_match:
            return True

    # Check content/media keywords
    if ad_text:
        text = normalize_text(ad_text)
        content_matches = sum(1 for p in COMPILED_CONTENT if p.search(text))
        if content_matches >= 3:
            return True

    return False


def assign_cluster(row: pd.Series) -> str:
    """
    Assign behavioral cluster using priority-based rules.

    Priority order:
    1. message_first - MESSAGE dominant or share_message >= 0.10
    2. call_first - CALL dominant or share_call >= 0.10
    3. form_first - share_form >= 0.20 AND (followup OR high share >= 0.30)
    4. web_consult - STRICT: WEB dest + lead CTA + consult + NOT transactional + (followup OR qualification)
    5. uncategorized - Fallback
    """
    dominant_dest = str(row.get('dominant_dest', '')).upper()
    dominant_cta = str(row.get('dominant_cta', '')).upper()
    share_message = float(row.get('share_message', 0) or 0)
    share_call = float(row.get('share_call', 0) or 0)
    share_form = float(row.get('share_form', 0) or 0)
    ad_text = row.get('ad_texts_combined', '')
    link_urls = row.get('link_urls', '')

    # Priority 1: MESSAGE
    if dominant_dest == 'MESSAGE' or share_message >= MESSAGE_SHARE_THRESHOLD:
        return 'message_first'

    # Priority 2: CALL
    if dominant_dest == 'CALL' or share_call >= CALL_SHARE_THRESHOLD:
        return 'call_first'

    # Priority 3: FORM - need share >= 0.20 AND (followup language OR high share >= 0.30)
    if share_form >= FORM_SHARE_THRESHOLD:
        has_followup = has_followup_language(ad_text)
        if has_followup:
            return 'form_first'
        # High form share even without explicit follow-up
        if share_form >= 0.30:
            return 'form_first'

    # Priority 4: WEB_CONSULT - STRICT requirements
    # Only assign if ALL conditions met:
    # - dominant_dest == WEB
    # - dominant_cta in LEAD_INTENT_CTAS
    # - consult_language_present == True
    # - transactional_url_match == False
    # - AND (followup OR qualification language present)
    if dominant_dest == 'WEB' and dominant_cta in LEAD_INTENT_CTAS:
        has_consult = has_consult_language(ad_text)
        is_transactional = check_transactional_url(link_urls)
        has_followup = has_followup_language(ad_text)
        has_qualification = has_qualification_language(ad_text)

        if has_consult and not is_transactional:
            if has_followup or has_qualification:
                return 'web_consult'

    # Fallback
    return 'uncategorized'


def check_multi_funnel(row: pd.Series) -> bool:
    """
    FIX 5: Check if advertiser uses multiple funnels.

    Multi-funnel if:
    - distinct_creatives >= 5 (hash-based)
    - OR multiple destination types used (message + form, call + form, etc.)
    """
    # Check distinct creatives
    ad_texts = row.get('ad_texts_combined', '')
    distinct_creatives = compute_distinct_creatives(ad_texts)
    if distinct_creatives >= 5:
        return True

    # Check multiple destination types
    share_message = float(row.get('share_message', 0) or 0)
    share_call = float(row.get('share_call', 0) or 0)
    share_form = float(row.get('share_form', 0) or 0)

    active_dest_types = sum([
        share_message > 0.05,
        share_call > 0.05,
        share_form > 0.05,
    ])

    if active_dest_types >= 2:
        return True

    return False


def calculate_total_score(row: pd.Series) -> float:
    """
    Calculate normalized total score (0-100).

    Formula: 0.45*money + 0.35*urgency + 0.20*fit
    Normalized: (raw / 50) * 100

    Score ranges: money 0-50, urgency 0-50, fit 0-50 (split model)
    """
    money_score = float(row.get('money_score', 0) or 0)
    urgency_score = float(row.get('urgency_score', 0) or 0)
    fit_score = float(row.get('fit_score', 0) or 0)

    raw_total = 0.45 * money_score + 0.35 * urgency_score + 0.20 * fit_score

    # Normalize to 0-100
    normalized = (raw_total / MAX_RAW_SCORE) * 100

    return round(normalized, 1)


def cluster_all(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Assign clusters and calculate final scores for all advertisers.

    Returns:
        - DataFrame with cluster and score columns added
        - Statistics dict
    """
    logger.info(f"Clustering {len(df)} advertisers...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Clustering"):
        try:
            cluster = assign_cluster(row)
            multi_funnel = check_multi_funnel(row)
            junk_risk = check_junk_risk(row)
            total_score = calculate_total_score(row)

            results.append({
                'behavioral_cluster': cluster,
                'multi_funnel': multi_funnel,
                'junk_risk': junk_risk,
                'total_score': total_score,
            })
        except Exception as e:
            logger.warning(f"Error clustering {row.get('page_name', idx)}: {e}")
            results.append({
                'behavioral_cluster': 'uncategorized',
                'multi_funnel': False,
                'junk_risk': False,
                'total_score': 0.0,
            })

    result_df = pd.DataFrame(results)
    for col in result_df.columns:
        df[col] = result_df[col].values

    # Handle empty dataframe
    if len(df) == 0:
        logger.warning("No advertisers to cluster")
        return df, {'total': 0}

    # Calculate statistics
    cluster_counts = df['behavioral_cluster'].value_counts().to_dict()
    total = len(df)

    stats = {
        'total': total,
        'message_first': cluster_counts.get('message_first', 0),
        'call_first': cluster_counts.get('call_first', 0),
        'form_first': cluster_counts.get('form_first', 0),
        'web_consult': cluster_counts.get('web_consult', 0),
        'uncategorized': cluster_counts.get('uncategorized', 0),
        'multi_funnel_count': int(df['multi_funnel'].sum()),
        'junk_risk_count': int(df['junk_risk'].sum()),
    }

    # Calculate percentages
    for key in ['message_first', 'call_first', 'form_first', 'web_consult', 'uncategorized']:
        stats[f'{key}_pct'] = stats[key] / total * 100 if total > 0 else 0

    # Log cluster distribution
    logger.info("Cluster distribution:")
    for cluster, count in cluster_counts.items():
        pct = count / total * 100
        logger.info(f"  {cluster}: {count} ({pct:.1f}%)")

    logger.info(f"Multi-funnel: {stats['multi_funnel_count']} ({stats['multi_funnel_count']/total*100:.1f}%)")
    logger.info(f"Junk risk: {stats['junk_risk_count']} ({stats['junk_risk_count']/total*100:.1f}%)")

    # FIX 8: Soft assertion for uncategorized
    uncategorized_pct = stats['uncategorized_pct'] / 100
    if uncategorized_pct >= 0.30:
        logger.warning(f"Uncategorized {uncategorized_pct:.1%} exceeds 30% - review cluster rules")
        # Log top 10 uncategorized for debugging
        uncategorized_df = df[df['behavioral_cluster'] == 'uncategorized'].head(10)
        for _, row in uncategorized_df.iterrows():
            name = str(row.get('page_name', 'Unknown'))[:30]
            cta = str(row.get('dominant_cta', ''))[:15]
            dest = str(row.get('dominant_dest', ''))[:10]
            logger.warning(f"  Uncategorized: {name} | CTA: {cta} | dest: {dest}")
    else:
        logger.info(f"Uncategorized constraint: {uncategorized_pct:.1%} < 30% (PASS)")

    return df, stats


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 6: BEHAVIORAL CLUSTERING")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Assign behavioral clusters')
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
        logger.info(f"Run m5_fit_score.py first.")
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

    # Cluster
    df, stats = cluster_all(df)

    # Sort by total_score descending
    df = df.sort_values('total_score', ascending=False).reset_index(drop=True)

    # Add rank
    df['rank'] = range(1, len(df) + 1)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved clustered data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"CLUSTERING SUMMARY")
    print(f"{'='*60}")
    print(f"  Total advertisers:  {stats['total']}")
    print()
    print(f"  Cluster distribution:")
    print(f"    message_first:    {stats['message_first']:>4} ({stats['message_first_pct']:.1f}%)")
    print(f"    call_first:       {stats['call_first']:>4} ({stats['call_first_pct']:.1f}%)")
    print(f"    form_first:       {stats['form_first']:>4} ({stats['form_first_pct']:.1f}%)")
    print(f"    web_consult:      {stats['web_consult']:>4} ({stats['web_consult_pct']:.1f}%)")
    print(f"    uncategorized:    {stats['uncategorized']:>4} ({stats['uncategorized_pct']:.1f}%)")
    print()
    print(f"  Flags:")
    print(f"    multi_funnel:     {stats['multi_funnel_count']}")
    print(f"    junk_risk:        {stats['junk_risk_count']}")
    print()
    print(f"  Total score stats:")
    print(f"    Mean:   {df['total_score'].mean():.1f}")
    print(f"    Median: {df['total_score'].median():.1f}")
    print(f"    Max:    {df['total_score'].max():.1f}")
    print(f"{'='*60}")

    # Top advertisers by total score
    print("\nTop 15 by total score:")
    print(f"{'Rank':<5} {'Score':<7} {'Cluster':<15} {'Junk':<5} {'Page Name':<40}")
    print("-" * 75)
    for _, row in df.head(15).iterrows():
        name = str(row['page_name'])[:40] if 'page_name' in row else 'Unknown'
        junk = 'Yes' if row['junk_risk'] else 'No'
        print(f"{row['rank']:<5} {row['total_score']:<7.1f} {row['behavioral_cluster']:<15} {junk:<5} {name:<40}")

    # Check top 20 junk risk
    top_20_junk = df.head(20)['junk_risk'].sum()
    if top_20_junk > 0:
        logger.warning(f"Top 20 advertisers have {top_20_junk} with junk_risk=True")
    else:
        logger.info("Top 20 advertisers: all junk_risk=False (PASS)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
