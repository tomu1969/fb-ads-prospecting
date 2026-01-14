"""
Email Verifier - Main verification CLI.

Validates drafted emails for quality and consistency with source data.

Usage:
    python verifier.py --drafts output/email_campaign/drafts_batch2.csv
    python verifier.py --drafts drafts.csv --report
    python verifier.py --drafts drafts.csv --fix
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import asdict

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from checks import (
    CheckResult,
    check_contact_name,
    check_email_name_match,
    check_no_template_vars,
    check_domain_match,
    check_greeting_name,
    check_writing_quality,
    check_franchise_personalization,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('email_verifier.log')
    ]
)
logger = logging.getLogger(__name__)

# Default paths
BASE_DIR = Path(__file__).parent.parent.parent
DEFAULT_DRAFTS = "output/email_campaign/drafts_batch2.csv"
DEFAULT_PROSPECTS = "output/prospects_master.csv"
DEFAULT_OUTPUT = "output/email_campaign/verification_report.csv"


def load_drafts(path: str) -> pd.DataFrame:
    """Load email drafts CSV."""
    logger.info(f"Loading drafts from: {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} drafts")
    return df


def load_prospects(path: str) -> pd.DataFrame:
    """Load prospects master CSV for cross-reference."""
    logger.info(f"Loading prospects from: {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} prospects")
    return df


def verify_single_draft(draft: Dict[str, Any], prospect: Optional[Dict[str, Any]] = None) -> List[CheckResult]:
    """
    Run all verification checks on a single draft.

    Args:
        draft: Draft email dict
        prospect: Optional matching prospect for cross-reference

    Returns:
        List of CheckResult objects
    """
    results = []

    contact_name = str(draft.get('contact_name', ''))
    email = str(draft.get('primary_email', ''))
    email_body = str(draft.get('email_body', ''))
    page_name = str(draft.get('page_name', ''))
    hook_used = str(draft.get('hook_used', ''))

    # 1. Check contact name validity
    results.append(check_contact_name(contact_name))

    # 2. Check email-name match
    results.append(check_email_name_match(email, contact_name))

    # 3. Check for template variables
    results.append(check_no_template_vars(email_body))

    # 4. Check domain match
    results.append(check_domain_match(email, page_name))

    # 5. Check greeting name
    results.append(check_greeting_name(email_body, contact_name))

    # 6. Check writing quality (awkward/redundant phrasing)
    results.append(check_writing_quality(email_body, hook_used))

    # 7. Check franchise personalization (agent-specific vs generic franchise info)
    results.append(check_franchise_personalization(email_body, hook_used, page_name, contact_name))

    return results


def verify_all_drafts(
    drafts_df: pd.DataFrame,
    prospects_df: Optional[pd.DataFrame] = None
) -> List[Dict[str, Any]]:
    """
    Verify all drafts and return detailed results.

    Args:
        drafts_df: DataFrame of email drafts
        prospects_df: Optional DataFrame of prospects for cross-reference

    Returns:
        List of result dicts with check details
    """
    all_results = []

    # Create prospect lookup if available
    prospect_lookup = {}
    if prospects_df is not None and 'page_name' in prospects_df.columns:
        prospect_lookup = prospects_df.set_index('page_name').to_dict('index')

    for idx, draft in drafts_df.iterrows():
        page_name = draft.get('page_name', '')
        prospect = prospect_lookup.get(page_name)

        # Run all checks
        check_results = verify_single_draft(draft.to_dict(), prospect)

        # Add results
        for result in check_results:
            if result.status != "pass":  # Only include non-passing results
                all_results.append({
                    'page_name': page_name,
                    'contact_name': draft.get('contact_name', ''),
                    'email': draft.get('primary_email', ''),
                    **asdict(result)
                })

    return all_results


def print_summary(results: List[Dict[str, Any]], total_drafts: int):
    """Print verification summary to console."""
    print("\n" + "=" * 60)
    print("EMAIL VERIFICATION SUMMARY")
    print("=" * 60)

    # Count by severity
    critical = sum(1 for r in results if r['severity'] == 'critical')
    high = sum(1 for r in results if r['severity'] == 'high')
    medium = sum(1 for r in results if r['severity'] == 'medium')
    low = sum(1 for r in results if r['severity'] == 'low')

    # Count by check type
    check_counts = {}
    for r in results:
        check = r['check_name']
        check_counts[check] = check_counts.get(check, 0) + 1

    # Unique drafts with issues
    drafts_with_issues = len(set(r['page_name'] for r in results))

    print(f"\nTotal drafts checked: {total_drafts}")
    print(f"Drafts with issues:   {drafts_with_issues}")
    print(f"Total issues found:   {len(results)}")

    print(f"\nBy severity:")
    print(f"  Critical: {critical}")
    print(f"  High:     {high}")
    print(f"  Medium:   {medium}")
    print(f"  Low:      {low}")

    print(f"\nBy check type:")
    for check, count in sorted(check_counts.items(), key=lambda x: -x[1]):
        print(f"  {check}: {count}")

    # Show critical issues
    if critical > 0:
        print(f"\n{'='*60}")
        print("CRITICAL ISSUES (must fix before sending):")
        print("=" * 60)
        for r in results:
            if r['severity'] == 'critical':
                print(f"\n  Company: {r['page_name']}")
                print(f"  Check:   {r['check_name']}")
                print(f"  Issue:   {r['issue_detail']}")
                print(f"  Fix:     {r['suggested_fix']}")

    # Show high severity issues
    if high > 0:
        print(f"\n{'='*60}")
        print("HIGH SEVERITY ISSUES:")
        print("=" * 60)
        for r in results:
            if r['severity'] == 'high':
                print(f"\n  Company: {r['page_name']}")
                print(f"  Email:   {r['email']}")
                print(f"  Check:   {r['check_name']}")
                print(f"  Issue:   {r['issue_detail']}")


def save_report(results: List[Dict[str, Any]], output_path: str):
    """Save verification report to CSV."""
    if not results:
        logger.info("No issues found - skipping report generation")
        return

    df = pd.DataFrame(results)

    # Order columns
    cols = ['page_name', 'contact_name', 'email', 'check_name', 'status',
            'severity', 'issue_detail', 'suggested_fix']
    df = df[[c for c in cols if c in df.columns]]

    # Sort by severity
    severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    df['_severity_order'] = df['severity'].map(severity_order)
    df = df.sort_values('_severity_order').drop('_severity_order', axis=1)

    df.to_csv(output_path, index=False)
    logger.info(f"Report saved to: {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Verify email drafts for quality and consistency'
    )

    parser.add_argument(
        '--drafts', '-d',
        type=str,
        default=DEFAULT_DRAFTS,
        help=f'Path to email drafts CSV (default: {DEFAULT_DRAFTS})'
    )

    parser.add_argument(
        '--prospects', '-p',
        type=str,
        default=DEFAULT_PROSPECTS,
        help=f'Path to prospects CSV for cross-reference (default: {DEFAULT_PROSPECTS})'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default=DEFAULT_OUTPUT,
        help=f'Path for verification report (default: {DEFAULT_OUTPUT})'
    )

    parser.add_argument(
        '--report', '-r',
        action='store_true',
        help='Generate detailed CSV report'
    )

    parser.add_argument(
        '--fix',
        action='store_true',
        help='Auto-fix issues where possible (not implemented)'
    )

    args = parser.parse_args()

    print("""
    ╔═══════════════════════════════════════════════╗
    ║           EMAIL VERIFICATION AGENT            ║
    ║   Quality checks for personalized emails      ║
    ╚═══════════════════════════════════════════════╝
    """)

    # Resolve paths
    drafts_path = Path(args.drafts)
    if not drafts_path.is_absolute():
        drafts_path = BASE_DIR / args.drafts

    prospects_path = Path(args.prospects)
    if not prospects_path.is_absolute():
        prospects_path = BASE_DIR / args.prospects

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = BASE_DIR / args.output

    # Load data
    if not drafts_path.exists():
        logger.error(f"Drafts file not found: {drafts_path}")
        sys.exit(1)

    drafts_df = load_drafts(str(drafts_path))

    prospects_df = None
    if prospects_path.exists():
        prospects_df = load_prospects(str(prospects_path))
    else:
        logger.warning(f"Prospects file not found: {prospects_path}")

    # Run verification
    logger.info("Running verification checks...")
    results = verify_all_drafts(drafts_df, prospects_df)

    # Print summary
    print_summary(results, len(drafts_df))

    # Save report if requested
    if args.report or results:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_report(results, str(output_path))

    # Exit with error if critical issues found
    critical_count = sum(1 for r in results if r['severity'] == 'critical')
    if critical_count > 0:
        print(f"\n[WARNING] {critical_count} critical issues found - fix before sending!")
        sys.exit(1)

    print("\n[OK] Verification complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
