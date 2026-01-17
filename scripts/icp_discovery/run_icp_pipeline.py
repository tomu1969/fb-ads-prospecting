#!/usr/bin/env python3
"""
ICP Discovery Pipeline Orchestrator (HARDENED)

Runs the full ICP discovery pipeline on Facebook Ads Library data:
  M0: Normalize raw ad data
  M1: Aggregate to page level
  M2: Apply conversational necessity gate (HARDENED)
  M3: Calculate money scores (HARDENED - ad_count capped)
  M4: Calculate urgency scores
  M5: Calculate fit scores (NEW)
  M6: Assign behavioral clusters (NEW)
  M7: Generate reports (NEW)

Input: Raw CSV from fb_ads_scraper.py (ad-level with snapshot data)
Output: ICP reports in output/icp_exploration/

Usage:
    # Full pipeline
    python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads_scraped_broad.csv

    # Test mode (limit to 100 ads)
    python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads.csv --limit 100

    # Start from specific module
    python scripts/icp_discovery/run_icp_pipeline.py --from m2

    # Individual modules can also be run directly:
    python scripts/icp_discovery/m0_normalizer.py --csv output/fb_ads.csv
"""

import os
import sys
import argparse
import subprocess
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('icp_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output' / 'icp_discovery'
EXPORT_DIR = BASE_DIR / 'output' / 'icp_exploration'

# Module definitions (UPDATED with m5-m7)
MODULES = [
    {
        'id': 'm0',
        'name': 'Normalizer',
        'script': 'm0_normalizer.py',
        'input': None,  # Takes raw input
        'output': '00_ads_normalized.csv',
        'description': 'Normalize raw ad data, classify destination types',
    },
    {
        'id': 'm1',
        'name': 'Aggregator',
        'script': 'm1_aggregator.py',
        'input': '00_ads_normalized.csv',
        'output': '01_pages_aggregated.csv',
        'description': 'Aggregate to page level, compute shares',
    },
    {
        'id': 'm2',
        'name': 'Conv Gate (Hardened)',
        'script': 'm2_conv_gate.py',
        'input': '01_pages_aggregated.csv',
        'output': '02_pages_candidate.csv',
        'description': 'Filter for conversational necessity (hardened)',
    },
    {
        'id': 'm3',
        'name': 'Money Score (Hardened)',
        'script': 'm3_money_score.py',
        'input': '02_pages_candidate.csv',
        'output': '03_money_scored.csv',
        'description': 'Calculate money scores (ad_count capped)',
    },
    {
        'id': 'm4',
        'name': 'Urgency Score',
        'script': 'm4_urgency_score.py',
        'input': '03_money_scored.csv',
        'output': '04_urgency_scored.csv',
        'description': 'Calculate urgency scores (0-50)',
    },
    {
        'id': 'm5',
        'name': 'Fit Score',
        'script': 'm5_fit_score.py',
        'input': '04_urgency_scored.csv',
        'output': '05_fit_scored.csv',
        'description': 'Calculate fit scores (0-30, EN/ES patterns)',
    },
    {
        'id': 'm6',
        'name': 'Clusterer',
        'script': 'm6_clusterer.py',
        'input': '05_fit_scored.csv',
        'output': '06_clustered.csv',
        'description': 'Assign behavioral clusters, total score',
    },
    {
        'id': 'm7',
        'name': 'Report Generator',
        'script': 'm7_report.py',
        'input': '06_clustered.csv',
        'output': None,  # Outputs to icp_exploration/
        'description': 'Generate final reports to icp_exploration/',
    },
]


def print_banner():
    """Print welcome banner."""
    print("\n" + "=" * 70)
    print("           ICP DISCOVERY PIPELINE (HARDENED)")
    print("           Behavior-Based ICP Identification")
    print("=" * 70)


def print_pipeline_status():
    """Print status of pipeline outputs."""
    print("\nPipeline outputs:")
    for module in MODULES:
        if module['output']:
            output_file = OUTPUT_DIR / module['output']
            status = "EXISTS" if output_file.exists() else "MISSING"
            size = ""
            if output_file.exists():
                try:
                    row_count = sum(1 for _ in open(output_file)) - 1
                    size = f" ({row_count:,} rows)"
                except:
                    size = ""
            print(f"  [{module['id']}] {module['output']:<30} {status}{size}")
        else:
            # Check icp_exploration outputs
            export_exists = (EXPORT_DIR / 'classified_advertisers.csv').exists()
            status = "EXISTS" if export_exists else "MISSING"
            print(f"  [{module['id']}] icp_exploration/<files>           {status}")


def run_module(module: dict, input_file: str = None, limit: int = None) -> bool:
    """
    Run a single module.

    Returns True if successful.
    """
    script_path = SCRIPT_DIR / module['script']
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False

    # Build command
    cmd = [sys.executable, str(script_path)]

    # Determine input file
    if module['input']:
        csv_path = OUTPUT_DIR / module['input']
        if not csv_path.exists():
            logger.error(f"Input file not found: {csv_path}")
            return False
        cmd.extend(['--csv', str(csv_path)])
    elif input_file:
        cmd.extend(['--csv', input_file])

    # Add limit for m0 only
    if limit and module['id'] == 'm0':
        cmd.extend(['--limit', str(limit)])

    logger.info(f"Running: {' '.join(cmd)}")

    # Run module
    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=False,
        )
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Error running module: {e}")
        return False


def generate_summary_report() -> str:
    """Generate summary report after pipeline completion."""
    final_output = OUTPUT_DIR / '06_clustered.csv'
    if not final_output.exists():
        return "No final output found."

    df = pd.read_csv(final_output)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Cluster distribution
    cluster_counts = df['behavioral_cluster'].value_counts()

    report = f"""
{'='*70}
ICP DISCOVERY PIPELINE - SUMMARY REPORT
{'='*70}

Generated: {timestamp}
Total ICPs identified: {len(df):,}

CLUSTER DISTRIBUTION:
"""
    for cluster, count in cluster_counts.items():
        pct = count / len(df) * 100
        report += f"  {cluster:<18}: {count:>5} ({pct:>5.1f}%)\n"

    report += f"""
SCORE STATISTICS:
  Money Score:   mean={df['money_score'].mean():.1f}, median={df['money_score'].median():.1f}
  Urgency Score: mean={df['urgency_score'].mean():.1f}, median={df['urgency_score'].median():.1f}
  Fit Score:     mean={df['fit_score'].mean():.1f}, median={df['fit_score'].median():.1f}
  Total Score:   mean={df['total_score'].mean():.1f}, median={df['total_score'].median():.1f}

VALIDATION:
"""
    # Check uncategorized
    uncategorized_pct = cluster_counts.get('uncategorized', 0) / len(df) * 100
    uncat_status = 'PASS' if uncategorized_pct < 30 else 'FAIL'
    report += f"  Uncategorized < 30%: {uncategorized_pct:.1f}% [{uncat_status}]\n"

    # Check top 20 junk risk
    top_20_junk = df.head(20)['junk_risk'].sum() if 'junk_risk' in df.columns else 0
    junk_status = 'PASS' if top_20_junk == 0 else 'FAIL'
    report += f"  Top 20 junk_risk=0:  {top_20_junk} [{junk_status}]\n"

    # Check conversational clusters exist
    conv_clusters = ['message_first', 'call_first', 'form_first']
    has_conv = any(c in cluster_counts.index for c in conv_clusters)
    conv_status = 'PASS' if has_conv else 'FAIL'
    report += f"  Conv clusters exist: [{conv_status}]\n"

    report += f"""
TOP 10 ICPs BY TOTAL SCORE:
{'Rank':<5} {'Score':<7} {'Cluster':<15} {'Page Name':<40}
{'-'*70}
"""
    for _, row in df.head(10).iterrows():
        name = str(row['page_name'])[:40]
        report += f"{row['rank']:<5} {row['total_score']:<7.1f} {row['behavioral_cluster']:<15} {name:<40}\n"

    report += f"""
OUTPUT FILES:
  Pipeline data: {OUTPUT_DIR}
  Final reports: {EXPORT_DIR}
{'='*70}
"""
    return report


def main():
    """Main function."""
    print_banner()

    parser = argparse.ArgumentParser(
        description='ICP Discovery Pipeline (Hardened)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline on raw FB ads data
  python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads_scraped_broad.csv

  # Test with limited data
  python scripts/icp_discovery/run_icp_pipeline.py --input output/fb_ads.csv --limit 100

  # Continue from specific module
  python scripts/icp_discovery/run_icp_pipeline.py --from m2
        """
    )
    parser.add_argument('--input', '-i', help='Input CSV file (raw ad-level data)')
    parser.add_argument('--from', dest='from_module', choices=['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7'],
                       help='Start from specific module')
    parser.add_argument('--limit', type=int, help='Limit number of ads to process')
    parser.add_argument('--status', action='store_true', help='Show pipeline status and exit')
    args = parser.parse_args()

    # Create output directories
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Status only
    if args.status:
        print_pipeline_status()
        return 0

    # Determine starting module
    start_idx = 0
    if args.from_module:
        for idx, module in enumerate(MODULES):
            if module['id'] == args.from_module:
                start_idx = idx
                break
        logger.info(f"Starting from module {args.from_module}")
    elif not args.input:
        # No input specified, check if we can resume
        if (OUTPUT_DIR / '00_ads_normalized.csv').exists():
            print("\nNo --input specified. Use --from to resume from a module.")
            print_pipeline_status()
            return 1
        else:
            print("\nError: --input required for initial run")
            return 1

    # Validate input for m0
    if start_idx == 0:
        if not args.input:
            logger.error("--input required when starting from m0")
            return 1

        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = BASE_DIR / args.input

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return 1

        logger.info(f"Input file: {input_path}")

    # Run modules
    print("\n" + "-" * 70)
    print("RUNNING PIPELINE (HARDENED)")
    print("-" * 70)

    modules_to_run = MODULES[start_idx:]
    total_modules = len(modules_to_run)

    for idx, module in enumerate(modules_to_run, 1):
        print(f"\n[{idx}/{total_modules}] {module['id'].upper()}: {module['name']}")
        print(f"    {module['description']}")

        input_file = str(input_path) if start_idx == 0 and module['id'] == 'm0' else None

        success = run_module(module, input_file=input_file, limit=args.limit)

        if not success:
            logger.error(f"Module {module['id']} failed")
            return 1

        if module['output']:
            print(f"    -> Output: {module['output']}")
        else:
            print(f"    -> Output: icp_exploration/")

    # Generate summary report
    print("\n" + "-" * 70)
    print("PIPELINE COMPLETE")
    print("-" * 70)

    report = generate_summary_report()
    print(report)

    # Save summary report
    summary_path = EXPORT_DIR / 'pipeline_summary.txt'
    with open(summary_path, 'w') as f:
        f.write(report)
    logger.info(f"Summary saved to {summary_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
