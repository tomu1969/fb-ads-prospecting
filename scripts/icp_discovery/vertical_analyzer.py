#!/usr/bin/env python3
"""
Vertical Deep Dive Analyzer

Classifies:
- Real Estate advertisers by intent (buyer leads, seller leads, rental, etc.)
- Home Services advertisers by trade (roofing, HVAC, plumbing, etc.)

Usage:
    python scripts/icp_discovery/vertical_analyzer.py
"""

import re
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / 'output' / 'icp_exploration'
ICP_OUTPUT_DIR = PROJECT_ROOT / 'output' / 'icp_discovery'

# ============================================================================
# CLASSIFICATION PATTERNS
# ============================================================================

RE_INTENT_PATTERNS = {
    'seller_leads': [
        r'\bsell(ing)?\s*(your|my|a)?\s*home\b',
        r'\bselling\s*your\s*(house|property)\b',
        r'\bhome\s*valuation\b',
        r'\bwhat\'?s?\s*(your|my)\s*home\s*worth\b',
        r'\bfree\s*(home\s*)?valuation\b',
        r'\bmarket\s*value\b',
        r'\blist(ing)?\s*(your|my)\s*home\b',
        r'\bthinking\s*(of|about)\s*selling\b',
        r'\bready\s*to\s*sell\b',
        r'\bget\s*(top|highest)\s*(dollar|price)\b',
        r'\bcash\s*(offer|buyer|for\s*your)\b',
        r'\bwe\s*buy\s*houses\b',
        r'\bsell\s*fast\b',
        r'\binstant\s*offer\b',
        r'\bibuyer\b',
        r'\bhouse\s*offer\b',  # "House Offer 365" type names
        r'\bbuy\s*your\s*house\b',
    ],
    'buyer_leads': [
        r'\bbuy(ing)?\s*(a|your)?\s*(new\s*)?(home|house)\b',
        r'\bhome\s*buyer\b',
        r'\bfirst.?time\s*buyer\b',
        r'\bfind(ing)?\s*(your|a|my)\s*(dream\s*)?(home|house)\b',
        r'\bsearch(ing)?\s*(for)?\s*(homes|houses|properties)\b',
        r'\bdream\s*home\b',
        r'\bhouse\s*hunting\b',
        r'\bnew\s*listing\b',
        r'\bjust\s*listed\b',
        r'\bopen\s*house\b',
        r'\bprice\s*reduced\b',
        r'\bhome\s*for\s*sale\b',
        r'\bstart\s*(your)?\s*search\b',
        r'\bready\s*to\s*buy\b',
    ],
    'rental': [
        r'\b(for\s*)?rent(al|ing|er)?\b',
        r'\blease\b',
        r'\bapartment\b',
        r'\btenant\b',
        r'\blandlord\b',
        r'\brenter\b',
        r'\bleasing\s*now\b',
        r'\bavailable\s*(for\s*)?rent\b',
        r'\bmonthly\s*rent\b',
        r'\bno\s*deposit\b',
        r'\bpet\s*friendly\b',
        r'\bstudio\b',
        r'\b1\s*bed(room)?\b',
        r'\b2\s*bed(room)?\b',
    ],
    'agent_recruiting': [
        r'\bjoin\s*(our|my|the)\s*team\b',
        r'\breal\s*estate\s*career\b',
        r'\bbecome\s*(an?\s*)?(agent|realtor)\b',
        r'\brecruit(ing|ment)?\b',
        r'\bhiring\s*(agents|realtors)\b',
        r'\bbrokerage\b',
        r'\bnow\s*hiring\b',
        r'\bcommission\s*split\b',
        r'\bagent\s*opportunity\b',
        r'\bbuilding\s*a\s*career\b',
        r'\bserious\s*about\s*building\s*a\s*career\b',
        r'\bpeople\s*who\s*thrive\b',
    ],
    'branding': [
        r'\bleave\s*a\s*(facebook\s*)?review\b',
        r'\btestimoni(al|es)\b',
        r'\b(your|my)\s*experience\s*with\s*me\b',
        r'\bclient\s*review\b',
        r'\b5.?star\s*review\b',
    ],
    'investor': [
        r'\binvest(or|ment|ing)?\s*(property|properties|opportunity)?\b',
        r'\broi\b',
        r'\bcash\s*flow\b',
        r'\bflip(ping)?\b',
        r'\bwholesale\b',
        r'\brental\s*income\b',
        r'\bpassive\s*income\b',
        r'\bmulti.?family\b',
        r'\bfix\s*(and|&)\s*flip\b',
        r'\bbuy\s*(and|&)\s*hold\b',
        r'\bcap\s*rate\b',
    ],
    'mortgage': [
        r'\bmortgage\b',
        r'\b(home\s*)?loan\b',
        r'\bfinancing\b',
        r'\bpre.?approv(al|ed)\b',
        r'\binterest\s*rate\b',
        r'\brefinance\b',
        r'\brefi\b',
        r'\bdown\s*payment\b',
        r'\blender\b',
        r'\bfha\b',
        r'\bva\s*loan\b',
        r'\bconventional\s*loan\b',
    ],
}

HS_TRADE_PATTERNS = {
    'roofing': [
        r'\broof(ing|er)?\b',
        r'\bshingle\b',
        r'\bgutter\b',
        r'\broof\s*repair\b',
        r'\broof\s*replacement\b',
        r'\broof\s*inspection\b',
        r'\bstorm\s*damage\b',
        r'\bleak(ing|s)?\s*roof\b',
    ],
    'hvac': [
        r'\bhvac\b',
        r'\bheating\b',
        r'\bcooling\b',
        r'\bair\s*condition(ing|er)?\b',
        r'\bfurnace\b',
        r'\ba/?c\s*(repair|service|install)\b',
        r'\bduct\s*(work|cleaning)\b',
        r'\bthermostat\b',
        r'\bheat\s*pump\b',
    ],
    'plumbing': [
        r'\bplumb(ing|er)\b',
        r'\bdrain\b',
        r'\bpipe\b',
        r'\bwater\s*heater\b',
        r'\bsewer\b',
        r'\bclog(ged)?\b',
        r'\bleaky?\s*(faucet|pipe)\b',
        r'\btoilet\b',
        r'\bfaucet\b',
        r'\bgarbage\s*disposal\b',
    ],
    'electrical': [
        r'\belectric(al|ian)?\b',
        r'\bwiring\b',
        r'\bcircuit\b',
        r'\bpanel\s*(upgrade|replacement)?\b',
        r'\boutlet\b',
        r'\blighting\s*(install|repair)\b',
        r'\bgenerator\b',
        r'\bsolar\s*(panel|install)\b',
    ],
    'pest_control': [
        r'\bpest\s*(control)?\b',
        r'\bexterminat(or|ing|ion)\b',
        r'\btermite\b',
        r'\bbug\b',
        r'\binsect\b',
        r'\brodent\b',
        r'\bmice\b',
        r'\brat\b',
        r'\bbed\s*bug\b',
        r'\bcockroach\b',
        r'\bant\s*(control|removal)\b',
    ],
    'cleaning': [
        r'\bclean(ing|er|s)?\b',
        r'\bmaid\b',
        r'\bjanitorial\b',
        r'\bpressure\s*wash\b',
        r'\bpower\s*wash\b',
        r'\bhouse\s*cleaning\b',
        r'\bdeep\s*clean\b',
        r'\bcarpet\s*clean\b',
        r'\bwindow\s*clean\b',
    ],
    'landscaping': [
        r'\blandscap(ing|er|e)\b',
        r'\blawn\s*(care|mowing|service)\b',
        r'\btree\s*(service|removal|trim)\b',
        r'\bgarden(ing|er)?\b',
        r'\birrigation\b',
        r'\bsprinkler\b',
        r'\bsod\b',
        r'\bmulch\b',
        r'\bhedge\b',
        r'\byard\s*(work|clean)\b',
    ],
    'painting': [
        r'\bpaint(ing|er)\b',
        r'\bcoating\b',
        r'\binterior\s*paint\b',
        r'\bexterior\s*paint\b',
        r'\bhouse\s*paint\b',
        r'\bstain(ing)?\b',
    ],
    'general_contractor': [
        r'\bcontractor\b',
        r'\bremodel(ing|er)?\b',
        r'\brenovation\b',
        r'\bconstruction\b',
        r'\bhandyman\b',
        r'\bhome\s*improvement\b',
        r'\bkitchen\s*(remodel|renovation)\b',
        r'\bbathroom\s*(remodel|renovation)\b',
        r'\bbasement\s*(finish|remodel)\b',
        r'\baddition\b',
    ],
    'appliance_repair': [
        r'\bappliance\s*(repair|service)\b',
        r'\bwasher\s*(repair)?\b',
        r'\bdryer\s*(repair)?\b',
        r'\brefrigerator\s*(repair)?\b',
        r'\bdishwasher\s*(repair)?\b',
        r'\boven\s*(repair)?\b',
        r'\bstove\s*(repair)?\b',
    ],
    'flooring': [
        r'\bfloor(ing|s)?\b',
        r'\bhardwood\b',
        r'\btile\b',
        r'\bcarpet\s*(install)?\b',
        r'\blaminate\b',
        r'\bvinyl\s*(plank)?\b',
    ],
    'fencing': [
        r'\bfenc(e|ing)\b',
        r'\bgate\s*(install|repair)?\b',
    ],
    'moving': [
        r'\bmov(ing|er)\b',
        r'\brelocation\b',
        r'\bpacking\s*service\b',
        r'\bstorage\b',
        r'\bjunk\s*removal\b',
        r'\bhauling\b',
    ],
    'garage_door': [
        r'\bgarage\s*door\b',
        r'\boverhead\s*door\b',
    ],
    'pool': [
        r'\bpool\s*(service|cleaning|repair|build)\b',
        r'\bswimming\s*pool\b',
        r'\bhot\s*tub\b',
        r'\bspa\s*service\b',
    ],
    'locksmith': [
        r'\blocksmith\b',
        r'\block(s)?\s*(repair|change|install)\b',
        r'\bkey\s*(cut|copy|duplicate)\b',
    ],
    'glass': [
        r'\bglass\s*(repair|replacement)\b',
        r'\bwindow\s*(repair|replacement|install)\b',
        r'\bauto\s*glass\b',
        r'\bwindshield\b',
    ],
    'windows_doors': [
        r'\bwindow(s)?\b',
        r'\bandersen\s*windows\b',
        r'\bdoor(s)?\s*(install|replace)?\b',
        r'\bpatio\s*door\b',
        r'\bsliding\s*door\b',
        r'\bwindow\s*dealer\b',
    ],
    'lighting': [
        r'\blighting\b',
        r'\bpermanent\s*light(s|ing)?\b',
        r'\bholiday\s*light(s|ing)?\b',
        r'\boutdoor\s*light(s|ing)?\b',
        r'\blandscape\s*light(s|ing)?\b',
    ],
}

# Compile all patterns
def compile_patterns(pattern_dict: dict) -> dict:
    """Compile regex patterns for faster matching."""
    compiled = {}
    for category, patterns in pattern_dict.items():
        compiled[category] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled

COMPILED_RE_INTENT = compile_patterns(RE_INTENT_PATTERNS)
COMPILED_HS_TRADE = compile_patterns(HS_TRADE_PATTERNS)


def classify_by_patterns(text: str, compiled_patterns: dict) -> tuple[str, list]:
    """
    Classify text by matching against pattern dictionary.

    Returns:
        (best_category, all_matches) - Best matching category and list of all matches
    """
    if not text or pd.isna(text):
        return 'unknown', []

    text = str(text)
    matches = {}

    for category, patterns in compiled_patterns.items():
        match_count = 0
        for pattern in patterns:
            found = pattern.findall(text)
            match_count += len(found)
        if match_count > 0:
            matches[category] = match_count

    if not matches:
        return 'unknown', []

    # Return category with most matches
    best = max(matches.items(), key=lambda x: x[1])
    return best[0], list(matches.keys())


def load_data() -> pd.DataFrame:
    """Load and merge master CSV with ad texts from aggregated pages."""
    # Load master
    master_path = OUTPUT_DIR / 'all_advertisers_master.csv'
    if not master_path.exists():
        raise FileNotFoundError(f"Master CSV not found: {master_path}")

    master = pd.read_csv(master_path)
    logger.info(f"Loaded master CSV: {len(master)} advertisers")

    # Load aggregated pages for ad texts
    agg_path = ICP_OUTPUT_DIR / '01_pages_aggregated.csv'
    if not agg_path.exists():
        raise FileNotFoundError(f"Aggregated pages not found: {agg_path}")

    agg = pd.read_csv(agg_path)
    logger.info(f"Loaded aggregated pages: {len(agg)} pages")

    # Merge on page_id
    merged = master.merge(
        agg[['page_id', 'ad_texts_combined', 'domains']],
        on='page_id',
        how='left'
    )

    return merged


def analyze_real_estate(df: pd.DataFrame) -> pd.DataFrame:
    """Classify Real Estate advertisers by intent."""
    # Filter to Real Estate sector
    re_df = df[df['sector'] == 'Real Estate'].copy()
    logger.info(f"Found {len(re_df)} Real Estate advertisers")

    if len(re_df) == 0:
        return pd.DataFrame()

    # Classify each advertiser
    results = []
    for _, row in re_df.iterrows():
        # Combine page name and ad texts for classification
        text = f"{row['page_name']} {row.get('ad_texts_combined', '')}"

        intent, all_intents = classify_by_patterns(text, COMPILED_RE_INTENT)

        results.append({
            'page_id': row['page_id'],
            'page_name': row['page_name'],
            'intent': intent,
            'all_intents': ', '.join(all_intents) if all_intents else '',
            'gate_passed': row['gate_passed'],
            'gate_reason': row['gate_reason'],
            'total_score': row['total_score'],
            'money_score': row['money_score'],
            'urgency_score': row['urgency_score'],
            'fit_score': row['fit_score'],
            'share_message': row['share_message'],
            'share_call': row['share_call'],
            'dominant_cta': row['dominant_cta'],
            'ad_texts_sample': str(row.get('ad_texts_combined', ''))[:500] if pd.notna(row.get('ad_texts_combined')) else '',
        })

    return pd.DataFrame(results)


def analyze_home_services(df: pd.DataFrame) -> pd.DataFrame:
    """Classify Home Services advertisers by trade."""
    # Filter to Home Services sector
    hs_df = df[df['sector'] == 'Home Services'].copy()
    logger.info(f"Found {len(hs_df)} Home Services advertisers")

    if len(hs_df) == 0:
        return pd.DataFrame()

    # Classify each advertiser
    results = []
    for _, row in hs_df.iterrows():
        # Combine page name and ad texts for classification
        text = f"{row['page_name']} {row.get('ad_texts_combined', '')}"

        trade, all_trades = classify_by_patterns(text, COMPILED_HS_TRADE)

        results.append({
            'page_id': row['page_id'],
            'page_name': row['page_name'],
            'trade': trade,
            'all_trades': ', '.join(all_trades) if all_trades else '',
            'gate_passed': row['gate_passed'],
            'gate_reason': row['gate_reason'],
            'total_score': row['total_score'],
            'money_score': row['money_score'],
            'urgency_score': row['urgency_score'],
            'fit_score': row['fit_score'],
            'share_message': row['share_message'],
            'share_call': row['share_call'],
            'dominant_cta': row['dominant_cta'],
            'ad_texts_sample': str(row.get('ad_texts_combined', ''))[:500] if pd.notna(row.get('ad_texts_combined')) else '',
        })

    return pd.DataFrame(results)


def generate_report(re_results: pd.DataFrame, hs_results: pd.DataFrame) -> str:
    """Generate markdown report."""
    lines = [
        "# Vertical Deep Dive Analysis",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "---",
        "",
    ]

    # ========================================================================
    # REAL ESTATE SECTION
    # ========================================================================
    lines.append("## Real Estate Advertisers")
    lines.append("")

    if len(re_results) == 0:
        lines.append("*No Real Estate advertisers found.*")
    else:
        # Summary by intent
        lines.append("### Intent Distribution")
        lines.append("")

        intent_counts = re_results['intent'].value_counts()
        lines.append("| Intent | Count | % |")
        lines.append("|--------|-------|---|")
        for intent, count in intent_counts.items():
            pct = count / len(re_results) * 100
            lines.append(f"| {intent.replace('_', ' ').title()} | {count} | {pct:.1f}% |")
        lines.append("")

        # Gate status by intent
        lines.append("### Gate Status by Intent")
        lines.append("")
        lines.append("| Intent | Total | Passed | Pass Rate |")
        lines.append("|--------|-------|--------|-----------|")
        for intent in intent_counts.index:
            subset = re_results[re_results['intent'] == intent]
            passed = subset['gate_passed'].sum()
            rate = passed / len(subset) * 100 if len(subset) > 0 else 0
            lines.append(f"| {intent.replace('_', ' ').title()} | {len(subset)} | {int(passed)} | {rate:.0f}% |")
        lines.append("")

        # Detailed breakdown by intent
        for intent in intent_counts.index:
            intent_df = re_results[re_results['intent'] == intent].sort_values('total_score', ascending=False)
            lines.append(f"### {intent.replace('_', ' ').title()} ({len(intent_df)})")
            lines.append("")
            lines.append("| Advertiser | Score | Gate | CTA | Ad Sample |")
            lines.append("|------------|-------|------|-----|-----------|")

            for _, row in intent_df.head(10).iterrows():
                name = row['page_name'][:30]
                score = row['total_score']
                gate = '✅' if row['gate_passed'] else '❌'
                cta = row['dominant_cta']
                sample = row['ad_texts_sample'][:80].replace('|', ' ').replace('\n', ' ')
                lines.append(f"| {name} | {score:.0f} | {gate} | {cta} | {sample}... |")
            lines.append("")

    # ========================================================================
    # HOME SERVICES SECTION
    # ========================================================================
    lines.append("---")
    lines.append("")
    lines.append("## Home Services Advertisers")
    lines.append("")

    if len(hs_results) == 0:
        lines.append("*No Home Services advertisers found.*")
    else:
        # Summary by trade
        lines.append("### Trade Distribution")
        lines.append("")

        trade_counts = hs_results['trade'].value_counts()
        lines.append("| Trade | Count | % |")
        lines.append("|-------|-------|---|")
        for trade, count in trade_counts.items():
            pct = count / len(hs_results) * 100
            lines.append(f"| {trade.replace('_', ' ').title()} | {count} | {pct:.1f}% |")
        lines.append("")

        # Gate status by trade
        lines.append("### Gate Status by Trade")
        lines.append("")
        lines.append("| Trade | Total | Passed | Pass Rate |")
        lines.append("|-------|-------|--------|-----------|")
        for trade in trade_counts.index:
            subset = hs_results[hs_results['trade'] == trade]
            passed = subset['gate_passed'].sum()
            rate = passed / len(subset) * 100 if len(subset) > 0 else 0
            lines.append(f"| {trade.replace('_', ' ').title()} | {len(subset)} | {int(passed)} | {rate:.0f}% |")
        lines.append("")

        # Detailed breakdown by trade
        for trade in trade_counts.index:
            trade_df = hs_results[hs_results['trade'] == trade].sort_values('total_score', ascending=False)
            lines.append(f"### {trade.replace('_', ' ').title()} ({len(trade_df)})")
            lines.append("")
            lines.append("| Advertiser | Score | Gate | CTA | Ad Sample |")
            lines.append("|------------|-------|------|-----|-----------|")

            for _, row in trade_df.head(10).iterrows():
                name = row['page_name'][:30]
                score = row['total_score']
                gate = '✅' if row['gate_passed'] else '❌'
                cta = row['dominant_cta']
                sample = row['ad_texts_sample'][:80].replace('|', ' ').replace('\n', ' ')
                lines.append(f"| {name} | {score:.0f} | {gate} | {cta} | {sample}... |")
            lines.append("")

    # ========================================================================
    # SUMMARY
    # ========================================================================
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total Real Estate:** {len(re_results)}")
    if len(re_results) > 0:
        re_passed = re_results['gate_passed'].sum()
        lines.append(f"  - Passed Gate: {int(re_passed)} ({re_passed/len(re_results)*100:.0f}%)")
    lines.append(f"- **Total Home Services:** {len(hs_results)}")
    if len(hs_results) > 0:
        hs_passed = hs_results['gate_passed'].sum()
        lines.append(f"  - Passed Gate: {int(hs_passed)} ({hs_passed/len(hs_results)*100:.0f}%)")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by vertical_analyzer.py*")

    return '\n'.join(lines)


def main():
    """Run vertical deep dive analysis."""
    logger.info("=" * 60)
    logger.info("VERTICAL DEEP DIVE ANALYSIS")
    logger.info("=" * 60)

    # Load data
    df = load_data()

    # Run analyses
    re_results = analyze_real_estate(df)
    hs_results = analyze_home_services(df)

    # Save CSV outputs
    if len(re_results) > 0:
        re_path = OUTPUT_DIR / 're_by_intent.csv'
        re_results.to_csv(re_path, index=False)
        logger.info(f"Saved: {re_path}")

    if len(hs_results) > 0:
        hs_path = OUTPUT_DIR / 'hs_by_trade.csv'
        hs_results.to_csv(hs_path, index=False)
        logger.info(f"Saved: {hs_path}")

    # Generate report
    report = generate_report(re_results, hs_results)
    report_path = OUTPUT_DIR / 'vertical_deep_dive.md'
    report_path.write_text(report)
    logger.info(f"Saved: {report_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)

    if len(re_results) > 0:
        print(f"\nReal Estate ({len(re_results)} advertisers):")
        print(re_results['intent'].value_counts().to_string())

    if len(hs_results) > 0:
        print(f"\nHome Services ({len(hs_results)} advertisers):")
        print(hs_results['trade'].value_counts().to_string())

    print(f"\nOutput files:")
    print(f"  - {OUTPUT_DIR / 'vertical_deep_dive.md'}")
    print(f"  - {OUTPUT_DIR / 're_by_intent.csv'}")
    print(f"  - {OUTPUT_DIR / 'hs_by_trade.csv'}")


if __name__ == '__main__':
    main()
