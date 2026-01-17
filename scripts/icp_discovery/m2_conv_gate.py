"""
Module 2: Conversational Necessity Gate (Hard Filter) - HARDENED

Eliminates advertisers whose business does not require conversation to convert leads.
Keeps advertisers with meaningful MESSAGE/CALL/FORM shares.
Drops transactional advertisers (pure e-commerce, app installs).

HARDENED RULES:
- Domain/path splitting for accurate transactional URL detection
- Lead form detection (FORM_CTAS + FOLLOWUP_PHRASES)
- ASCII text normalization before regex matching (EN/ES support)
- Aggressive exclusion/inclusion rules

Input: output/icp_discovery/01_pages_aggregated.csv
Output: output/icp_discovery/02_pages_candidate.csv

Usage:
    python scripts/icp_discovery/m2_conv_gate.py
    python scripts/icp_discovery/m2_conv_gate.py --csv output/icp_discovery/01_pages_aggregated.csv
"""

import os
import sys
import argparse
import logging
import re
from pathlib import Path
from typing import Dict, Any, Tuple
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from constants import (
    TRANSACTIONAL_CTA_TYPES,
    TRANSACTIONAL_DOMAINS,
    TRANSACTIONAL_PATHS,
    FORM_CTA_TYPES,
    LEAD_INTENT_CTAS,
    MESSAGE_CTA_TYPES,
    CALL_CTA_TYPES,
    COMPILED_TRANSACTIONAL_COPY,
    COMPILED_TRANSACTIONAL_PATHS,
    COMPILED_PRICE_DISCOUNT,
    COMPILED_FOLLOWUP,
    COMPILED_CONSULT,
    COMPILED_REGULATED_BUSINESS_NAME,
    MIN_CONVERSATION_SHARE,
    MIN_FORM_SHARE,
    normalize_text,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m2_conv_gate.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '01_pages_aggregated.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '02_pages_candidate.csv'


def check_transactional_url(url: str) -> bool:
    """
    Check if URL is transactional using domain/path splitting.

    FIX 1: Split link_url into domain and path, check separately.
    This prevents false positives like matching 'checkout' in innocent domains.
    """
    if not url or pd.isna(url):
        return False

    try:
        parsed = urlparse(str(url))
        domain = parsed.netloc.lower()
        path = parsed.path.lower()

        # Check transactional domains
        domain_match = any(d in domain for d in TRANSACTIONAL_DOMAINS)
        if domain_match:
            return True

        # Check transactional paths
        path_match = any(p.search(path) for p in COMPILED_TRANSACTIONAL_PATHS)
        return path_match
    except Exception:
        return False


def check_transactional_urls(urls: str) -> bool:
    """Check if any URL in the list is transactional."""
    if not urls or pd.isna(urls):
        return False

    # URLs are joined by ' | ' in aggregation
    url_list = str(urls).split(' | ')
    return any(check_transactional_url(u.strip()) for u in url_list)


def check_transactional_copy(ad_text: str) -> Tuple[bool, int]:
    """
    Check if ad copy is price/discount dominated (transactional).

    Returns (is_transactional, price_discount_count)
    Uses normalize_text for ASCII folding.
    """
    if not ad_text or pd.isna(ad_text):
        return False, 0

    # Normalize text for matching (ASCII fold)
    text = normalize_text(ad_text)

    # Count price/discount patterns
    price_matches = sum(1 for p in COMPILED_PRICE_DISCOUNT if p.search(text))

    # Heavy transactional if 3+ price/discount signals
    is_transactional = price_matches >= 3

    return is_transactional, price_matches


def check_transactional_cta(dominant_cta: str) -> bool:
    """Check if dominant CTA is transactional."""
    if not dominant_cta or pd.isna(dominant_cta):
        return False

    return str(dominant_cta).upper() in TRANSACTIONAL_CTA_TYPES


def has_consult_language(ad_text: str) -> bool:
    """
    Check if ad copy contains consult/qualification language.
    Uses normalize_text for ASCII folding (EN/ES support).
    """
    if not ad_text or pd.isna(ad_text):
        return False

    text = normalize_text(ad_text)
    return any(p.search(text) for p in COMPILED_CONSULT)


def has_followup_language(ad_text: str) -> bool:
    """
    Check if ad copy contains follow-up language (we'll contact you, etc).
    Uses normalize_text for ASCII folding (EN/ES support).
    """
    if not ad_text or pd.isna(ad_text):
        return False

    text = normalize_text(ad_text)
    return any(p.search(text) for p in COMPILED_FOLLOWUP)


def is_form_with_followup(row: pd.Series) -> bool:
    """
    FIX 2: Lead form detection.

    Returns True if:
    - dominant_cta is a FORM CTA AND
    - ad copy contains follow-up language
    """
    dominant_cta = str(row.get('dominant_cta', '')).upper()
    ad_text = row.get('ad_texts_combined', '')

    if dominant_cta in FORM_CTA_TYPES:
        if has_followup_language(ad_text):
            return True

    return False


def has_qualification_language(ad_text: str) -> bool:
    """
    Check if ad copy contains qualification/requirements language.
    Uses normalize_text for ASCII folding (EN/ES support).
    """
    if not ad_text or pd.isna(ad_text):
        return False

    text = normalize_text(ad_text)
    # Import qualification patterns
    from constants import COMPILED_QUALIFICATION_ALL
    return any(p.search(text) for p in COMPILED_QUALIFICATION_ALL)


def has_regulated_business_name(page_name: str) -> bool:
    """
    Check if page name indicates a regulated/high-consideration business.

    RESCUE PATH: Used to rescue advertisers that fail destination-based gate
    but have business names indicating they need conversational qualification.

    Examples: "Terra Nova Roofing", "Eric Aragon, CPA", "RE/MAX Pinnacle"

    IMPORTANT: Only checks page_name (not ad copy) to avoid false positives
    from content farms with stories about doctors, lawyers, etc.
    """
    if not page_name or pd.isna(page_name):
        return False

    name_lower = normalize_text(page_name)
    return any(p.search(name_lower) for p in COMPILED_REGULATED_BUSINESS_NAME)


def evaluate_gate(row: pd.Series) -> Tuple[bool, str]:
    """
    Evaluate if advertiser passes the conversational necessity gate.

    HARDENED LOGIC (v2):
    - MINIMUM SIGNAL REQUIREMENT: Must have measurable conversational signals
    - PASS only if:
      - share_message >= 0.10, OR
      - share_call >= 0.10, OR
      - (share_form >= 0.20 AND followup_language_present), OR
      - dominant_dest in {MESSAGE, CALL}
    - Otherwise check for WEB_CONSULT (strict requirements) or DROP

    Returns:
        Tuple of (passed: bool, reason: str)
        reason format: 'MESSAGE', 'CALL', 'FORM', 'WEB_CONSULT', 'TRANSACTIONAL_DROP', 'NO_SIGNAL_DROP'
    """
    share_message = float(row.get('share_message', 0) or 0)
    share_call = float(row.get('share_call', 0) or 0)
    share_form = float(row.get('share_form', 0) or 0)
    share_web = float(row.get('share_web', 1) or 1)
    dominant_cta = str(row.get('dominant_cta', '')).upper()
    dominant_dest = str(row.get('dominant_dest', '')).upper()
    ad_text = row.get('ad_texts_combined', '')
    link_urls = row.get('link_urls', '')

    # Pre-compute signals
    has_followup = has_followup_language(ad_text)
    has_consult = has_consult_language(ad_text)
    has_qualification = has_qualification_language(ad_text)
    is_transactional_cta = check_transactional_cta(dominant_cta)
    is_transactional_url = check_transactional_urls(link_urls)
    is_transactional_copy, price_count = check_transactional_copy(ad_text)

    # === TRANSACTIONAL DROP (check first) ===

    # Hard drop: Transactional CTA + transactional URL/copy + no consult language
    if is_transactional_cta:
        if is_transactional_url and not has_consult:
            return False, 'TRANSACTIONAL_DROP'
        if is_transactional_copy and not has_consult:
            return False, 'TRANSACTIONAL_DROP'

    # Drop: Price/discount dominated copy with no conversation signals
    if price_count >= 3 and share_message == 0 and share_call == 0 and not has_consult:
        return False, 'TRANSACTIONAL_DROP'

    # === MINIMUM SIGNAL PASS RULES ===

    # Rule 1: MESSAGE - dominant dest or meaningful share
    if dominant_dest == 'MESSAGE':
        return True, 'MESSAGE'
    if share_message >= 0.10:
        return True, 'MESSAGE'

    # Rule 2: CALL - dominant dest or meaningful share
    if dominant_dest == 'CALL':
        return True, 'CALL'
    if share_call >= 0.10:
        return True, 'CALL'

    # Rule 3: FORM - need share >= 0.20 AND followup language
    if share_form >= 0.20 and has_followup:
        return True, 'FORM'

    # Rule 3b: Very high FORM share (>= 0.40) implies lead capture even without followup
    if share_form >= 0.40:
        return True, 'FORM'

    # === WEB_CONSULT (strict requirements) ===
    # Only assign if:
    # - dominant_dest == WEB
    # - dominant_cta in LEAD_INTENT_CTAS
    # - consult_language_present == True
    # - NOT transactional URL
    # - AND (followup OR qualification language present)

    if dominant_dest == 'WEB' and dominant_cta in LEAD_INTENT_CTAS:
        if has_consult and not is_transactional_url:
            if has_followup or has_qualification:
                return True, 'WEB_CONSULT'

    # === RESCUE PATH ===
    # Rescue advertisers that fail destination-based gate but have:
    # 1. Regulated business name (realtor, CPA, roofing, etc.)
    # 2. NOT a transactional CTA (SHOP_NOW, DOWNLOAD, etc.)
    #
    # These businesses drive traffic to websites but still need
    # conversational qualification (budget, timeline, scope, etc.)

    page_name = row.get('page_name', '')
    is_regulated_business = has_regulated_business_name(page_name)

    if is_regulated_business and not is_transactional_cta:
        # Additional safeguard: must not be heavily price/discount focused
        if price_count < 3:
            return True, 'RESCUED'

    # === NO SIGNAL DROP ===
    # If we reach here, the advertiser doesn't meet minimum conversational requirements
    return False, 'NO_SIGNAL_DROP'


def apply_gate(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Apply conversational necessity gate to all advertisers.

    Returns:
        - DataFrame with conversational_gate_pass and gate_reason columns
        - Statistics dict with drop counts
    """
    logger.info(f"Applying conversational necessity gate to {len(df)} advertisers...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Filtering"):
        passed, reason = evaluate_gate(row)
        results.append({
            'conversational_gate_pass': passed,
            'gate_reason': reason,
        })

    result_df = pd.DataFrame(results)
    df['conversational_gate_pass'] = result_df['conversational_gate_pass']
    df['gate_reason'] = result_df['gate_reason']

    # Calculate statistics
    stats = {
        'total_input': len(df),
        'passed': int(df['conversational_gate_pass'].sum()),
        'dropped': int(len(df) - df['conversational_gate_pass'].sum()),
    }

    # Count by reason
    reason_counts = df['gate_reason'].value_counts().to_dict()
    stats['TRANSACTIONAL_DROP'] = reason_counts.get('TRANSACTIONAL_DROP', 0)
    stats['NO_SIGNAL_DROP'] = reason_counts.get('NO_SIGNAL_DROP', 0)
    stats['MESSAGE'] = reason_counts.get('MESSAGE', 0)
    stats['CALL'] = reason_counts.get('CALL', 0)
    stats['FORM'] = reason_counts.get('FORM', 0)
    stats['WEB_CONSULT'] = reason_counts.get('WEB_CONSULT', 0)
    stats['RESCUED'] = reason_counts.get('RESCUED', 0)

    # Log stats
    pass_rate = stats['passed'] / len(df) * 100 if len(df) > 0 else 0
    logger.info(f"Gate results: {stats['passed']} passed ({pass_rate:.1f}%), {stats['dropped']} dropped")

    # Log reason breakdown
    logger.info("Gate reason breakdown:")
    for reason, count in reason_counts.items():
        pct = count / len(df) * 100
        logger.info(f"  {reason}: {count} ({pct:.1f}%)")

    # FIX 8: Soft assertion - warn if zero drops
    if stats['dropped'] == 0:
        logger.warning("Gate dropped 0 advertisers - check if data is pre-filtered or rules too lenient")

    return df, stats


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 2: CONVERSATIONAL NECESSITY GATE (HARDENED)")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Filter for conversational necessity')
    parser.add_argument('--csv', '-i', help='Input CSV file (page-level aggregated)')
    parser.add_argument('--output', '-o', help='Output CSV path')
    parser.add_argument('--keep-all', action='store_true', help='Keep all rows (add columns only)')
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
        logger.info(f"Run m1_aggregator.py first to create aggregated data.")
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

    # Apply gate
    df, stats = apply_gate(df)

    # Filter to candidates (unless --keep-all)
    if args.keep_all:
        df_output = df
        logger.info("Keeping all rows with gate columns added")
    else:
        df_output = df[df['conversational_gate_pass'] == True].copy()
        logger.info(f"Filtered to {len(df_output)} candidates")

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_output.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved candidates to {output_path}")

    # Save gate stats to JSON for m7_report.py
    import json
    stats_path = output_path.parent / 'gate_stats.json'
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    logger.info(f"Saved gate stats to {stats_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"GATE SUMMARY")
    print(f"{'='*60}")
    print(f"  Input:              {stats['total_input']} advertisers")
    print(f"  Passed:             {stats['passed']} advertisers")
    print(f"  Dropped:            {stats['dropped']} advertisers")
    print(f"    TRANSACTIONAL:    {stats['TRANSACTIONAL_DROP']}")
    print(f"    NO_SIGNAL:        {stats['NO_SIGNAL_DROP']}")
    print(f"  Output:             {len(df_output)} advertisers")
    print()
    print(f"  Pass by reason:")
    print(f"    MESSAGE:          {stats['MESSAGE']}")
    print(f"    CALL:             {stats['CALL']}")
    print(f"    FORM:             {stats['FORM']}")
    print(f"    WEB_CONSULT:      {stats['WEB_CONSULT']}")
    print(f"    RESCUED:          {stats['RESCUED']}")
    print(f"{'='*60}")

    # Show sample of dropped advertisers
    dropped_df = df[df['conversational_gate_pass'] == False]
    if len(dropped_df) > 0:
        print("\nSample dropped advertisers:")
        for _, row in dropped_df.head(5).iterrows():
            name = str(row['page_name'])[:40] if 'page_name' in row else 'Unknown'
            cta = str(row.get('dominant_cta', ''))[:15]
            print(f"  {name:<40} | CTA: {cta:<15} | {row['gate_reason']}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
