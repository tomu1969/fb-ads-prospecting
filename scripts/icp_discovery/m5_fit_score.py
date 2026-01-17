"""
Module 5: Fit Score (0-50) - SPLIT MODEL v3

Measures conversational fit using TWO dimensions:
1) Explicit Fit (0-30): Pre-qualification language in ads (unchanged from v2)
2) Implicit Fit (0-20): Qualification load DURING conversation (new)

This split ensures verticals that qualify leads INSIDE the conversation
(not in ad copy) are not penalized.

EXPLICIT FIT (0-30) - Pre-qualification signals in ad copy:
- Question Marks (0-2): +1 if >= 1, +2 if >= 3
- Qualification Depth (0-10): EN/ES requirements/eligibility/finance patterns
- Consult / Booking Intent (0-6): Schedule, book, quote patterns
- Followup Language (0-4): "we will contact", "expect a call" patterns
- Multi-Step Language (0-4): "answer a few questions", form completion
- Complexity (0-4): Multiple CTAs, carousels, destination types

IMPLICIT FIT (0-20) - Qualification load inferred from structure:
- Conversational Entry Without Pricing (0-6): MESSAGE/CALL dest + no prices
- Generic Entry CTA (0-4): CALL_NOW/MESSAGE_PAGE + no qualifying language
- Service Breadth / Ambiguity (0-4): Generic service phrases
- Advisor / Human Mediation (0-4): advisor, specialist, expert, etc.
- Regulated / High-Consideration (0-2): legal, education, healthcare, etc.

Total fit_score = explicit_fit_score + implicit_fit_score (0-50)

Input: output/icp_discovery/04_urgency_scored.csv
Output: output/icp_discovery/05_fit_scored.csv

Usage:
    python scripts/icp_discovery/m5_fit_score.py
    python scripts/icp_discovery/m5_fit_score.py --csv output/icp_discovery/04_urgency_scored.csv
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
from constants import (
    COMPILED_QUALIFICATION_ALL,
    COMPILED_CONSULT_BOOKING,
    COMPILED_FOLLOWUP,
    COMPILED_FIT_QUALIFICATION_EXPANDED,
    COMPILED_FIT_FOLLOWUP,
    COMPILED_FIT_MULTISTEP,
    COMPILED_PRICE_DISCOUNT,
    COMPILED_ADVISOR_LANGUAGE,
    COMPILED_SERVICE_BREADTH,
    COMPILED_REGULATED_DOMAIN,
    REGULATED_PAGE_CATEGORIES,
    normalize_text,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m5_fit_score.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
DEFAULT_INPUT = OUTPUT_DIR / '04_urgency_scored.csv'
DEFAULT_OUTPUT = OUTPUT_DIR / '05_fit_scored.csv'

# Generic entry CTAs (intent is deferred to conversation)
GENERIC_ENTRY_CTAS = {'CALL_NOW', 'MESSAGE_PAGE', 'SEND_MESSAGE', 'WHATSAPP_MESSAGE', 'CONTACT_US'}


def count_pattern_matches(text: str, patterns: list) -> int:
    """Count how many patterns match in the normalized text."""
    if not text or pd.isna(text):
        return 0

    # Normalize text for matching (ASCII fold)
    normalized = normalize_text(text)
    return sum(1 for p in patterns if p.search(normalized))


def has_pattern_match(text: str, patterns: list) -> bool:
    """Check if any pattern matches in the normalized text."""
    if not text or pd.isna(text):
        return False
    normalized = normalize_text(text)
    return any(p.search(normalized) for p in patterns)


def count_question_marks(text: str) -> int:
    """Count question marks in text."""
    if not text or pd.isna(text):
        return 0
    return str(text).count('?')


# =============================================================================
# EXPLICIT FIT SCORE COMPONENTS (0-30) - Pre-qualification in ads
# =============================================================================

def calculate_question_score(ad_text: str) -> int:
    """
    Calculate question mark score (0-2).

    +1 if >= 1 question mark
    +2 if >= 3 question marks
    """
    question_count = count_question_marks(ad_text)
    if question_count >= 3:
        return 2
    elif question_count >= 1:
        return 1
    return 0


def calculate_qualification_score(ad_text: str) -> int:
    """
    Calculate expanded qualification/depth score (0-10).

    2 points per match, max 10.
    Uses expanded EN + ES patterns: requirements, eligible, qualify, approval,
    prequal, finance, licensed, insurance, estimate, etc.
    """
    matches = count_pattern_matches(ad_text, COMPILED_FIT_QUALIFICATION_EXPANDED)
    return min(10, matches * 2)


def calculate_consult_booking_score(ad_text: str) -> int:
    """
    Calculate consult/booking intent score (0-6).

    2 points per match, max 6.
    Patterns: book, schedule, consult, quote, appointment, etc.
    """
    matches = count_pattern_matches(ad_text, COMPILED_CONSULT_BOOKING)
    return min(6, matches * 2)


def calculate_followup_score(ad_text: str) -> int:
    """
    Calculate followup language score (0-4).

    2 points per match, max 4.
    Patterns: "we will contact", "expect a call", "respond within", etc.
    """
    matches = count_pattern_matches(ad_text, COMPILED_FIT_FOLLOWUP)
    return min(4, matches * 2)


def calculate_multistep_score(ad_text: str) -> int:
    """
    Calculate multi-step/questionnaire language score (0-4).

    2 points per match, max 4.
    Patterns: "answer a few questions", "fill out the form", "step 1", etc.
    """
    matches = count_pattern_matches(ad_text, COMPILED_FIT_MULTISTEP)
    return min(4, matches * 2)


def calculate_complexity_score(row: pd.Series) -> int:
    """
    Calculate funnel complexity score (0-4).

    - Multiple CTAs across ads: +2 if > 2 distinct CTA types
    - Multiple destination types: +2 if (share_message + share_call) > 0 AND share_form > 0
    """
    score = 0

    # Check distinct CTA types
    distinct_ctas = row.get('distinct_ctas', 0)
    try:
        distinct_ctas = int(distinct_ctas) if pd.notna(distinct_ctas) else 0
    except (ValueError, TypeError):
        distinct_ctas = 0

    if distinct_ctas > 2:
        score += 2

    # Check multiple destination types
    share_message = float(row.get('share_message', 0) or 0)
    share_call = float(row.get('share_call', 0) or 0)
    share_form = float(row.get('share_form', 0) or 0)

    direct_share = share_message + share_call
    if direct_share > 0 and share_form > 0:
        score += 2

    return min(4, score)


def calculate_explicit_fit_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate Explicit Fit Score (0-30) - pre-qualification in ad copy.

    Components:
    - Question Marks (0-2): +1 if >= 1, +2 if >= 3
    - Qualification Depth (0-10): EN/ES expanded patterns
    - Consult / Booking Intent (0-6): Schedule, book, quote
    - Followup Language (0-4): "we will contact" patterns
    - Multi-Step Language (0-4): "answer questions" patterns
    - Complexity (0-4): Multiple CTAs, destination types

    Returns dict with explicit_fit_score and breakdown.
    """
    ad_text = row.get('ad_texts_combined', '')
    breakdown = []

    # === QUESTION MARKS (0-2) ===
    question_score = calculate_question_score(ad_text)
    breakdown.append(f"q:{question_score}")

    # === QUALIFICATION DEPTH (0-10) ===
    qualification_score = calculate_qualification_score(ad_text)
    breakdown.append(f"qual:{qualification_score}")

    # === CONSULT/BOOKING SCORE (0-6) ===
    consult_score = calculate_consult_booking_score(ad_text)
    breakdown.append(f"cons:{consult_score}")

    # === FOLLOWUP LANGUAGE (0-4) ===
    followup_score = calculate_followup_score(ad_text)
    breakdown.append(f"fup:{followup_score}")

    # === MULTI-STEP LANGUAGE (0-4) ===
    multistep_score = calculate_multistep_score(ad_text)
    breakdown.append(f"multi:{multistep_score}")

    # === COMPLEXITY SCORE (0-4) ===
    complexity_score = calculate_complexity_score(row)
    breakdown.append(f"cmplx:{complexity_score}")

    # === TOTAL ===
    total_score = (question_score + qualification_score + consult_score +
                   followup_score + multistep_score + complexity_score)

    return {
        'explicit_fit_score': min(30, total_score),
        'explicit_fit_breakdown': '|'.join(breakdown),
        'exp_questions': question_score,
        'exp_qualification': qualification_score,
        'exp_consult': consult_score,
        'exp_followup': followup_score,
        'exp_multistep': multistep_score,
        'exp_complexity': complexity_score,
    }


# =============================================================================
# IMPLICIT FIT SCORE COMPONENTS (0-20) - Qualification load during conversation
# =============================================================================

def calculate_conv_entry_score(row: pd.Series, ad_text: str) -> int:
    """
    Component A: Conversational Entry Without Pricing (0-6).

    +6 if dominant_dest in {MESSAGE, CALL} AND no price/discount patterns.
    Rationale: qualification must happen verbally.
    """
    dominant_dest = str(row.get('dominant_dest', '')).upper()

    if dominant_dest not in {'MESSAGE', 'CALL'}:
        return 0

    # Check for price/discount patterns
    has_pricing = has_pattern_match(ad_text, COMPILED_PRICE_DISCOUNT)

    if not has_pricing:
        return 6
    return 0


def calculate_generic_cta_score(row: pd.Series, ad_text: str) -> int:
    """
    Component B: Generic Entry CTA (0-4).

    +4 if dominant_cta in {CALL_NOW, MESSAGE_PAGE, etc.} AND no qualifying language in ad.
    Rationale: intent is deferred to conversation.
    """
    dominant_cta = str(row.get('dominant_cta', '')).upper()

    if dominant_cta not in GENERIC_ENTRY_CTAS:
        return 0

    # Check for qualifying language already present
    has_qualifying = has_pattern_match(ad_text, COMPILED_QUALIFICATION_ALL)

    if not has_qualifying:
        return 4
    return 0


def calculate_service_breadth_score(ad_text: str) -> int:
    """
    Component C: Service Breadth / Ambiguity (0-4).

    +2 if multiple services or broad offerings detected
    +2 if ad copy is descriptive but non-specific ("We help with...", etc.)
    Rationale: agent must ask clarifying questions.
    """
    score = 0
    matches = count_pattern_matches(ad_text, COMPILED_SERVICE_BREADTH)

    if matches >= 2:
        score += 4
    elif matches >= 1:
        score += 2

    return min(4, score)


def calculate_advisor_score(ad_text: str) -> int:
    """
    Component D: Advisor / Human Mediation Signal (0-4).

    +4 if copy includes advisor-style language:
    EN: advisor, specialist, expert, consultant, team
    ES: asesor, especialista, equipo
    Rationale: implies human-led qualification flow.
    """
    has_advisor = has_pattern_match(ad_text, COMPILED_ADVISOR_LANGUAGE)
    return 4 if has_advisor else 0


def calculate_regulated_domain_score(row: pd.Series, ad_text: str) -> int:
    """
    Component E: Regulated / High-Consideration Proxy (0-2).

    +2 if page_category or copy implies regulated/high-stakes domains:
    legal, education, healthcare, finance, housing, insurance
    Rationale: qualification is unavoidable but deferred.
    """
    # Check page category first
    page_category = str(row.get('page_category', '')).lower().strip()
    if page_category in REGULATED_PAGE_CATEGORIES:
        return 2

    # Check ad text for regulated domain keywords
    has_regulated = has_pattern_match(ad_text, COMPILED_REGULATED_DOMAIN)
    return 2 if has_regulated else 0


def calculate_implicit_fit_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate Implicit Fit Score (0-20) - qualification load during conversation.

    Components:
    A) Conversational Entry Without Pricing (0-6)
    B) Generic Entry CTA (0-4)
    C) Service Breadth / Ambiguity (0-4)
    D) Advisor / Human Mediation (0-4)
    E) Regulated / High-Consideration (0-2)

    Returns dict with implicit_fit_score and breakdown.
    """
    ad_text = row.get('ad_texts_combined', '')
    breakdown = []

    # === A) CONVERSATIONAL ENTRY WITHOUT PRICING (0-6) ===
    conv_entry_score = calculate_conv_entry_score(row, ad_text)
    breakdown.append(f"entry:{conv_entry_score}")

    # === B) GENERIC ENTRY CTA (0-4) ===
    generic_cta_score = calculate_generic_cta_score(row, ad_text)
    breakdown.append(f"cta:{generic_cta_score}")

    # === C) SERVICE BREADTH / AMBIGUITY (0-4) ===
    service_breadth_score = calculate_service_breadth_score(ad_text)
    breakdown.append(f"svc:{service_breadth_score}")

    # === D) ADVISOR / HUMAN MEDIATION (0-4) ===
    advisor_score = calculate_advisor_score(ad_text)
    breakdown.append(f"adv:{advisor_score}")

    # === E) REGULATED / HIGH-CONSIDERATION (0-2) ===
    regulated_score = calculate_regulated_domain_score(row, ad_text)
    breakdown.append(f"reg:{regulated_score}")

    # === TOTAL ===
    total_score = (conv_entry_score + generic_cta_score + service_breadth_score +
                   advisor_score + regulated_score)

    return {
        'implicit_fit_score': min(20, total_score),
        'implicit_fit_breakdown': '|'.join(breakdown),
        'imp_conv_entry': conv_entry_score,
        'imp_generic_cta': generic_cta_score,
        'imp_service_breadth': service_breadth_score,
        'imp_advisor': advisor_score,
        'imp_regulated': regulated_score,
    }


# =============================================================================
# COMBINED FIT SCORE
# =============================================================================

def calculate_fit_score(row: pd.Series) -> Dict[str, Any]:
    """
    Calculate combined Fit Score (0-50).

    fit_score = explicit_fit_score (0-30) + implicit_fit_score (0-20)

    Returns dict with all fit score columns.
    """
    # Calculate explicit fit (pre-qualification in ads)
    explicit = calculate_explicit_fit_score(row)

    # Calculate implicit fit (qualification load during conversation)
    implicit = calculate_implicit_fit_score(row)

    # Combined score
    combined_score = explicit['explicit_fit_score'] + implicit['implicit_fit_score']

    return {
        # Combined
        'fit_score': min(50, combined_score),
        # Explicit components
        'explicit_fit_score': explicit['explicit_fit_score'],
        'explicit_fit_breakdown': explicit['explicit_fit_breakdown'],
        'exp_questions': explicit['exp_questions'],
        'exp_qualification': explicit['exp_qualification'],
        'exp_consult': explicit['exp_consult'],
        'exp_followup': explicit['exp_followup'],
        'exp_multistep': explicit['exp_multistep'],
        'exp_complexity': explicit['exp_complexity'],
        # Implicit components
        'implicit_fit_score': implicit['implicit_fit_score'],
        'implicit_fit_breakdown': implicit['implicit_fit_breakdown'],
        'imp_conv_entry': implicit['imp_conv_entry'],
        'imp_generic_cta': implicit['imp_generic_cta'],
        'imp_service_breadth': implicit['imp_service_breadth'],
        'imp_advisor': implicit['imp_advisor'],
        'imp_regulated': implicit['imp_regulated'],
    }


def score_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate fit scores for all advertisers.

    Returns DataFrame with fit_score columns added.
    """
    logger.info(f"Calculating fit scores for {len(df)} advertisers...")

    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Scoring"):
        try:
            scores = calculate_fit_score(row)
            results.append(scores)
        except Exception as e:
            logger.warning(f"Error scoring {row.get('page_name', idx)}: {e}")
            results.append({
                'fit_score': 0,
                'explicit_fit_score': 0,
                'explicit_fit_breakdown': 'error',
                'exp_questions': 0, 'exp_qualification': 0, 'exp_consult': 0,
                'exp_followup': 0, 'exp_multistep': 0, 'exp_complexity': 0,
                'implicit_fit_score': 0,
                'implicit_fit_breakdown': 'error',
                'imp_conv_entry': 0, 'imp_generic_cta': 0, 'imp_service_breadth': 0,
                'imp_advisor': 0, 'imp_regulated': 0,
            })

    result_df = pd.DataFrame(results)
    for col in result_df.columns:
        df[col] = result_df[col].values

    # Handle empty dataframe
    if len(df) == 0:
        logger.warning("No advertisers to score")
        return df

    # Log combined distribution
    logger.info("Combined fit score distribution:")
    bins = [0, 10, 20, 30, 40, 50]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-50']
    df['_fit_bin'] = pd.cut(df['fit_score'], bins=bins, labels=labels, include_lowest=True)
    dist = df['_fit_bin'].value_counts().sort_index()
    for bin_label, count in dist.items():
        pct = count / len(df) * 100
        logger.info(f"  {bin_label}: {count} ({pct:.1f}%)")
    df.drop('_fit_bin', axis=1, inplace=True)

    logger.info(f"Mean combined fit score: {df['fit_score'].mean():.1f}")
    logger.info(f"Max combined fit score: {df['fit_score'].max()}")

    # Log explicit component averages
    logger.info("Explicit fit component averages (0-30):")
    logger.info(f"  questions:     {df['exp_questions'].mean():.1f} / 2")
    logger.info(f"  qualification: {df['exp_qualification'].mean():.1f} / 10")
    logger.info(f"  consult:       {df['exp_consult'].mean():.1f} / 6")
    logger.info(f"  followup:      {df['exp_followup'].mean():.1f} / 4")
    logger.info(f"  multistep:     {df['exp_multistep'].mean():.1f} / 4")
    logger.info(f"  complexity:    {df['exp_complexity'].mean():.1f} / 4")

    # Log implicit component averages
    logger.info("Implicit fit component averages (0-20):")
    logger.info(f"  conv_entry:    {df['imp_conv_entry'].mean():.1f} / 6")
    logger.info(f"  generic_cta:   {df['imp_generic_cta'].mean():.1f} / 4")
    logger.info(f"  service_breadth: {df['imp_service_breadth'].mean():.1f} / 4")
    logger.info(f"  advisor:       {df['imp_advisor'].mean():.1f} / 4")
    logger.info(f"  regulated:     {df['imp_regulated'].mean():.1f} / 2")

    return df


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 5: FIT SCORE (SPLIT MODEL v3)")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Calculate fit scores')
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
        logger.info(f"Run m4_urgency_score.py first.")
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

    # Sort by fit_score descending
    df = df.sort_values('fit_score', ascending=False).reset_index(drop=True)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved scored data to {output_path}")

    # Summary
    print(f"\n{'='*60}")
    print(f"FIT SCORE SUMMARY (SPLIT MODEL v3)")
    print(f"{'='*60}")
    print(f"  Total scored:     {len(df)} advertisers")
    print()
    print(f"  Combined Fit Score (0-50):")
    print(f"    Mean:   {df['fit_score'].mean():.1f}")
    print(f"    Median: {df['fit_score'].median():.1f}")
    print(f"    Max:    {df['fit_score'].max()}")
    print()
    print(f"  Explicit Fit (0-30) - pre-qualification in ads:")
    print(f"    Mean:   {df['explicit_fit_score'].mean():.1f}")
    print(f"    Median: {df['explicit_fit_score'].median():.1f}")
    print()
    print(f"  Implicit Fit (0-20) - qualification load in conversation:")
    print(f"    Mean:   {df['implicit_fit_score'].mean():.1f}")
    print(f"    Median: {df['implicit_fit_score'].median():.1f}")
    print(f"{'='*60}")

    # Top advertisers
    print("\nTop 15 by combined fit score:")
    print(f"{'Score':<6} {'Exp':<5} {'Imp':<5} {'Name':<40}")
    print("-" * 60)
    for _, row in df.head(15).iterrows():
        name = str(row['page_name'])[:38] if 'page_name' in row else 'Unknown'
        print(f"{row['fit_score']:<6} {row['explicit_fit_score']:<5} {row['implicit_fit_score']:<5} {name:<40}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
