"""
Module 7: Report Generator

Generates final ICP discovery outputs to output/icp_exploration/:
- classified_advertisers.csv - All advertisers with scores and clusters
- icp_leaderboard.csv - Cluster rankings by median_total_score
- icp_leaderboard.json - Same in JSON format
- icp_analysis_report.md - Human-readable analysis report

Input: output/icp_discovery/06_clustered.csv
Output: output/icp_exploration/

Usage:
    python scripts/icp_discovery/m7_report.py
    python scripts/icp_discovery/m7_report.py --csv output/icp_discovery/06_clustered.csv
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('m7_report.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
EXPORT_DIR = BASE_DIR / 'output' / 'icp_exploration'
DEFAULT_INPUT = OUTPUT_DIR / '06_clustered.csv'


def generate_classified_advertisers(df: pd.DataFrame, output_path: Path):
    """
    Generate classified_advertisers.csv with specified columns.
    """
    # Define output columns
    output_columns = [
        'page_id', 'page_name', 'page_category', 'active_ads', 'is_active',
        'ad_texts_combined', 'link_urls', 'domains',
        'conversational_gate_pass', 'gate_reason',
        'dominant_dest', 'share_message', 'share_call', 'share_form', 'share_web',
        'money_score', 'urgency_score', 'fit_score', 'total_score',
        'behavioral_cluster', 'multi_funnel', 'junk_risk', 'rank',
    ]

    # Select columns that exist
    available_cols = [c for c in output_columns if c in df.columns]

    # Create output dataframe
    df_output = df[available_cols].copy()

    # Sort by rank
    if 'rank' in df_output.columns:
        df_output = df_output.sort_values('rank')

    # Save
    df_output.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved classified_advertisers.csv: {len(df_output)} rows")

    return df_output


def generate_leaderboard(df: pd.DataFrame, output_path_csv: Path, output_path_json: Path):
    """
    Generate icp_leaderboard.csv/json with cluster statistics.

    Ranked by median_total_score DESC, NOT by avg_ad_count.
    """
    clusters = df['behavioral_cluster'].unique()

    leaderboard = []
    for cluster in clusters:
        cluster_df = df[df['behavioral_cluster'] == cluster]

        top_advertisers = cluster_df.head(5)['page_name'].tolist()
        top_advertisers_str = ' | '.join([str(n)[:30] for n in top_advertisers])

        entry = {
            'icp_cluster': cluster,
            'total_advertisers': len(cluster_df),
            'median_total_score': round(cluster_df['total_score'].median(), 1),
            'median_money': round(cluster_df['money_score'].median(), 1) if 'money_score' in cluster_df.columns else 0,
            'median_urgency': round(cluster_df['urgency_score'].median(), 1) if 'urgency_score' in cluster_df.columns else 0,
            'median_fit': round(cluster_df['fit_score'].median(), 1) if 'fit_score' in cluster_df.columns else 0,
            'avg_message_share': round(cluster_df['share_message'].mean(), 3) if 'share_message' in cluster_df.columns else 0,
            'avg_call_share': round(cluster_df['share_call'].mean(), 3) if 'share_call' in cluster_df.columns else 0,
            'avg_form_share': round(cluster_df['share_form'].mean(), 3) if 'share_form' in cluster_df.columns else 0,
            'avg_web_share': round(cluster_df['share_web'].mean(), 3) if 'share_web' in cluster_df.columns else 0,
            'top_advertisers': top_advertisers_str,
        }
        leaderboard.append(entry)

    # Sort by median_total_score DESC
    leaderboard = sorted(leaderboard, key=lambda x: x['median_total_score'], reverse=True)

    # Add rank
    for i, entry in enumerate(leaderboard, 1):
        entry['rank'] = i

    # Save CSV
    lb_df = pd.DataFrame(leaderboard)
    cols_order = ['rank', 'icp_cluster', 'total_advertisers', 'median_total_score',
                  'median_money', 'median_urgency', 'median_fit',
                  'avg_message_share', 'avg_call_share', 'avg_form_share', 'avg_web_share',
                  'top_advertisers']
    lb_df = lb_df[[c for c in cols_order if c in lb_df.columns]]
    lb_df.to_csv(output_path_csv, index=False, encoding='utf-8')
    logger.info(f"Saved icp_leaderboard.csv: {len(lb_df)} clusters")

    # Save JSON
    with open(output_path_json, 'w', encoding='utf-8') as f:
        json.dump(leaderboard, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved icp_leaderboard.json")

    return leaderboard


def load_gate_stats() -> Dict[str, int]:
    """Load gate statistics from gate_stats.json (saved by m2_conv_gate.py)."""
    stats_path = OUTPUT_DIR / 'gate_stats.json'

    default_stats = {
        'total_input': 0,
        'passed': 0,
        'dropped': 0,
        'TRANSACTIONAL_DROP': 0,
        'NO_SIGNAL_DROP': 0,
        'MESSAGE': 0,
        'CALL': 0,
        'FORM': 0,
        'WEB_CONSULT': 0,
    }

    if stats_path.exists():
        try:
            with open(stats_path, 'r') as f:
                stats = json.load(f)
            # Merge with defaults for any missing keys
            for key in default_stats:
                if key not in stats:
                    stats[key] = default_stats[key]
            return stats
        except Exception as e:
            logger.warning(f"Failed to load gate_stats.json: {e}")

    return default_stats


def calculate_percentiles(series: pd.Series) -> Dict[str, float]:
    """Calculate percentile statistics for a score series."""
    if len(series) == 0:
        return {'min': 0, 'p25': 0, 'p50': 0, 'p75': 0, 'p90': 0, 'max': 0}

    return {
        'min': round(series.min(), 1),
        'p25': round(series.quantile(0.25), 1),
        'p50': round(series.quantile(0.50), 1),
        'p75': round(series.quantile(0.75), 1),
        'p90': round(series.quantile(0.90), 1),
        'max': round(series.max(), 1),
    }


def generate_analysis_report(df: pd.DataFrame, leaderboard: list, output_path: Path, gate_stats: Dict = None):
    """
    Generate icp_analysis_report.md with comprehensive analysis.

    Includes:
    - Gate breakdown (PASS, TRANSACTIONAL_DROP, NO_SIGNAL_DROP counts)
    - Score distributions with percentiles (min/p50/p90/max)
    - Cluster medians + counts
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    total = len(df)

    # Load actual gate stats from pipeline files
    if gate_stats is None:
        gate_stats = load_gate_stats()

    # Cluster counts
    cluster_counts = df['behavioral_cluster'].value_counts().to_dict()

    # Uncategorized percentage
    uncategorized = cluster_counts.get('uncategorized', 0)
    uncategorized_pct = uncategorized / total * 100 if total > 0 else 0
    uncategorized_status = 'PASS' if uncategorized_pct < 30 else 'FAIL'

    # Junk risk in top 20
    top_20_junk = df.head(20)['junk_risk'].sum() if 'junk_risk' in df.columns else 0
    junk_status = 'PASS' if top_20_junk == 0 else 'FAIL'

    # Calculate percentiles for each score
    money_pct = calculate_percentiles(df['money_score']) if 'money_score' in df.columns else {}
    urgency_pct = calculate_percentiles(df['urgency_score']) if 'urgency_score' in df.columns else {}
    fit_pct = calculate_percentiles(df['fit_score']) if 'fit_score' in df.columns else {}
    explicit_fit_pct = calculate_percentiles(df['explicit_fit_score']) if 'explicit_fit_score' in df.columns else {}
    implicit_fit_pct = calculate_percentiles(df['implicit_fit_score']) if 'implicit_fit_score' in df.columns else {}
    total_pct = calculate_percentiles(df['total_score']) if 'total_score' in df.columns else {}

    # Pre-compute fit stats for report
    explicit_fit_mean = round(df['explicit_fit_score'].mean(), 1) if 'explicit_fit_score' in df.columns else 0
    explicit_fit_median = round(df['explicit_fit_score'].median(), 1) if 'explicit_fit_score' in df.columns else 0
    implicit_fit_mean = round(df['implicit_fit_score'].mean(), 1) if 'implicit_fit_score' in df.columns else 0
    implicit_fit_median = round(df['implicit_fit_score'].median(), 1) if 'implicit_fit_score' in df.columns else 0

    report = f"""# ICP Analysis Report

**Generated:** {timestamp}

---

## Gate Statistics

| Metric | Count | % |
|--------|-------|---|
| **Total Input** | {gate_stats['total_input']:,} | 100% |
| **Passed Gate** | {gate_stats['passed']:,} | {gate_stats['passed']/max(gate_stats['total_input'], 1)*100:.1f}% |
| **Dropped** | {gate_stats['dropped']:,} | {gate_stats['dropped']/max(gate_stats['total_input'], 1)*100:.1f}% |

### Gate Reason Breakdown

| Reason | Count |
|--------|-------|
| MESSAGE | {gate_stats['MESSAGE']} |
| CALL | {gate_stats['CALL']} |
| FORM | {gate_stats['FORM']} |
| WEB_CONSULT | {gate_stats['WEB_CONSULT']} |
| TRANSACTIONAL_DROP | {gate_stats['TRANSACTIONAL_DROP']} |
| NO_SIGNAL_DROP | {gate_stats['NO_SIGNAL_DROP']} |

---

## Score Distributions (Percentiles)

| Score | Min | P25 | P50 | P75 | P90 | Max |
|-------|-----|-----|-----|-----|-----|-----|
| **Money (0-50)** | {money_pct.get('min', 0)} | {money_pct.get('p25', 0)} | {money_pct.get('p50', 0)} | {money_pct.get('p75', 0)} | {money_pct.get('p90', 0)} | {money_pct.get('max', 0)} |
| **Urgency (0-50)** | {urgency_pct.get('min', 0)} | {urgency_pct.get('p25', 0)} | {urgency_pct.get('p50', 0)} | {urgency_pct.get('p75', 0)} | {urgency_pct.get('p90', 0)} | {urgency_pct.get('max', 0)} |
| **Fit (0-50)** | {fit_pct.get('min', 0)} | {fit_pct.get('p25', 0)} | {fit_pct.get('p50', 0)} | {fit_pct.get('p75', 0)} | {fit_pct.get('p90', 0)} | {fit_pct.get('max', 0)} |
| *- Explicit (0-30)* | {explicit_fit_pct.get('min', 0)} | {explicit_fit_pct.get('p25', 0)} | {explicit_fit_pct.get('p50', 0)} | {explicit_fit_pct.get('p75', 0)} | {explicit_fit_pct.get('p90', 0)} | {explicit_fit_pct.get('max', 0)} |
| *- Implicit (0-20)* | {implicit_fit_pct.get('min', 0)} | {implicit_fit_pct.get('p25', 0)} | {implicit_fit_pct.get('p50', 0)} | {implicit_fit_pct.get('p75', 0)} | {implicit_fit_pct.get('p90', 0)} | {implicit_fit_pct.get('max', 0)} |
| **Total (0-100)** | {total_pct.get('min', 0)} | {total_pct.get('p25', 0)} | {total_pct.get('p50', 0)} | {total_pct.get('p75', 0)} | {total_pct.get('p90', 0)} | {total_pct.get('max', 0)} |

### Tier Recommendations (Data-Driven)

- **Top Tier (>= P90)**: Total Score >= {total_pct.get('p90', 0)} → {len(df[df['total_score'] >= total_pct.get('p90', 0)])} advertisers
- **Mid Tier (P50-P90)**: Total Score {total_pct.get('p50', 0)} - {total_pct.get('p90', 0)} → {len(df[(df['total_score'] >= total_pct.get('p50', 0)) & (df['total_score'] < total_pct.get('p90', 0))])} advertisers
- **Low Tier (< P50)**: Total Score < {total_pct.get('p50', 0)} → {len(df[df['total_score'] < total_pct.get('p50', 0)])} advertisers

---

## Cluster Composition

| Cluster | Count | % | Median Score | Avg Message Share |
|---------|-------|---|--------------|-------------------|
"""

    for entry in leaderboard:
        cluster = entry['icp_cluster']
        count = entry['total_advertisers']
        pct = count / total * 100 if total > 0 else 0
        median_score = entry['median_total_score']
        msg_share = entry['avg_message_share']
        report += f"| {cluster} | {count} | {pct:.1f}% | {median_score} | {msg_share:.1%} |\n"

    report += """
---

## Top 20 Advertisers

| Rank | Name | Cluster | Total | Money | Urgency | Fit (Exp+Imp) | Gate |
|------|------|---------|-------|-------|---------|---------------|------|
"""

    for _, row in df.head(20).iterrows():
        rank = row.get('rank', '-')
        name = str(row.get('page_name', 'Unknown'))[:30]
        cluster = row.get('behavioral_cluster', '-')[:12]
        total_score = row.get('total_score', 0)
        money = row.get('money_score', 0)
        urgency = row.get('urgency_score', 0)
        explicit_fit = int(row.get('explicit_fit_score', 0) or 0)
        implicit_fit = int(row.get('implicit_fit_score', 0) or 0)
        fit_str = f"{explicit_fit}+{implicit_fit}"
        gate_reason = row.get('gate_reason', '-')
        report += f"| {rank} | {name} | {cluster} | {total_score} | {money} | {urgency} | {fit_str} | {gate_reason} |\n"

    report += f"""
---

## Validation

- **Uncategorized:** {uncategorized_pct:.1f}% ({uncategorized_status} < 30%)
- **Top 20 junk_risk:** {top_20_junk} with junk_risk=True ({junk_status})

---

## Score Distributions

### Money Score (0-50)
- Mean: {df['money_score'].mean():.1f}
- Median: {df['money_score'].median():.1f}
- Max: {df['money_score'].max()}

### Urgency Score (0-50)
- Mean: {df['urgency_score'].mean():.1f}
- Median: {df['urgency_score'].median():.1f}
- Max: {df['urgency_score'].max()}

### Fit Score (0-50) — Split Model
- **Combined:** Mean {df['fit_score'].mean():.1f}, Median {df['fit_score'].median():.1f}, Max {df['fit_score'].max()}
- **Explicit (0-30):** Mean {explicit_fit_mean}, Median {explicit_fit_median}
- **Implicit (0-20):** Mean {implicit_fit_mean}, Median {implicit_fit_median}

### Total Score (0-100)
- Mean: {df['total_score'].mean():.1f}
- Median: {df['total_score'].median():.1f}
- Max: {df['total_score'].max()}

---

## Recommendations

1. **Top Tier (Total >= 50)**: Priority outreach - {len(df[df['total_score'] >= 50])} advertisers
2. **Mid Tier (Total 25-49)**: Secondary outreach - {len(df[(df['total_score'] >= 25) & (df['total_score'] < 50)])} advertisers
3. **Low Tier (Total < 25)**: Nurture or skip - {len(df[df['total_score'] < 25])} advertisers

---

*Report generated by ICP Discovery Pipeline (Hardened)*
"""

    # Save
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"Saved icp_analysis_report.md")

    return report


def main():
    """Main function."""
    print(f"\n{'='*60}")
    print("MODULE 7: REPORT GENERATOR")
    print(f"{'='*60}")

    parser = argparse.ArgumentParser(description='Generate ICP discovery reports')
    parser.add_argument('--csv', '-i', help='Input CSV file (clustered data)')
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
        logger.info(f"Run m6_clusterer.py first.")
        return 1

    print(f"\nInput:  {csv_path}")
    print(f"Output: {EXPORT_DIR}")
    print()

    # Create export directory
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Load CSV
    logger.info(f"Loading {csv_path}...")
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
        logger.info(f"Loaded {len(df)} advertisers")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return 1

    # Generate outputs
    print("\nGenerating reports...")

    # 1. Classified advertisers
    classified_path = EXPORT_DIR / 'classified_advertisers.csv'
    generate_classified_advertisers(df, classified_path)

    # 2. Leaderboard
    leaderboard_csv = EXPORT_DIR / 'icp_leaderboard.csv'
    leaderboard_json = EXPORT_DIR / 'icp_leaderboard.json'
    leaderboard = generate_leaderboard(df, leaderboard_csv, leaderboard_json)

    # 3. Analysis report
    report_path = EXPORT_DIR / 'icp_analysis_report.md'
    generate_analysis_report(df, leaderboard, report_path)

    # Summary
    print(f"\n{'='*60}")
    print(f"REPORT GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"\nOutputs saved to {EXPORT_DIR}:")
    print(f"  - classified_advertisers.csv ({len(df)} advertisers)")
    print(f"  - icp_leaderboard.csv ({len(leaderboard)} clusters)")
    print(f"  - icp_leaderboard.json")
    print(f"  - icp_analysis_report.md")
    print(f"{'='*60}")

    # Print leaderboard summary
    print("\nICP Leaderboard (by median_total_score):")
    print(f"{'Rank':<5} {'Cluster':<18} {'Count':<7} {'Median Score':<13}")
    print("-" * 45)
    for entry in leaderboard:
        print(f"{entry['rank']:<5} {entry['icp_cluster']:<18} {entry['total_advertisers']:<7} {entry['median_total_score']:<13.1f}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
