#!/usr/bin/env python3
"""
Email Pipeline - Complete end-to-end email campaign workflow.

Orchestrates:
1. Email Drafting (email_drafter)
2. Email Verification (email_verifier)
3. Email Fixing (email_fixer)
4. Re-verification
5. Email Sending (gmail_sender)

Usage:
    # Full pipeline (draft → verify → fix → send)
    python scripts/email_pipeline.py --input output/to_email.csv --all

    # Draft and verify only (no sending)
    python scripts/email_pipeline.py --input output/to_email.csv --no-send

    # Verify and fix existing drafts
    python scripts/email_pipeline.py --drafts output/email_campaign/drafts.csv --verify-only

    # Dry run (show what would happen)
    python scripts/email_pipeline.py --input output/to_email.csv --dry-run
"""

import argparse
import asyncio
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('email_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
OUTPUT_DIR = BASE_DIR / "output" / "email_campaign"


def run_command(cmd: list, description: str, dry_run: bool = False) -> tuple:
    """
    Run a command and capture output.

    Returns:
        Tuple of (success, stdout, stderr)
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would run: {' '.join(cmd)}")
        return True, "", ""

    logger.info(f"Running: {description}")
    logger.debug(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR)
        )

        if result.returncode == 0:
            return True, result.stdout, result.stderr
        else:
            logger.error(f"Command failed with exit code {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
            return False, result.stdout, result.stderr

    except Exception as e:
        logger.error(f"Error running command: {e}")
        return False, "", str(e)


def draft_emails(
    input_path: str,
    output_path: str,
    limit: Optional[int] = None,
    dry_run: bool = False
) -> bool:
    """Run the email drafter module."""
    cmd = [
        "python", str(SCRIPTS_DIR / "email_drafter" / "drafter.py"),
        "--input", input_path,
        "--output", output_path,
    ]

    if limit:
        cmd.extend(["--limit", str(limit)])
    else:
        cmd.append("--all")

    if dry_run:
        cmd.append("--dry-run")

    success, stdout, stderr = run_command(cmd, "Email Drafter", dry_run=False)
    print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)
    return success


def verify_emails(
    drafts_path: str,
    output_path: Optional[str] = None,
    dry_run: bool = False
) -> tuple:
    """
    Run the email verification agent.

    Returns:
        Tuple of (success, critical_count, high_count)
    """
    cmd = [
        "python", str(SCRIPTS_DIR / "email_verifier" / "verifier.py"),
        "--drafts", drafts_path,
        "--report"
    ]

    if output_path:
        cmd.extend(["--output", output_path])

    success, stdout, stderr = run_command(cmd, "Email Verifier", dry_run=dry_run)
    print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)

    # Parse issue counts from output
    critical_count = 0
    high_count = 0

    for line in stdout.split('\n'):
        if 'Critical:' in line:
            try:
                critical_count = int(line.split(':')[1].strip())
            except (ValueError, IndexError):
                pass
        elif 'High:' in line:
            try:
                high_count = int(line.split(':')[1].strip())
            except (ValueError, IndexError):
                pass

    return success or critical_count == 0, critical_count, high_count


def fix_emails(
    drafts_path: str,
    output_path: Optional[str] = None,
    dry_run: bool = False
) -> tuple:
    """
    Run the email fixer agent.

    Returns:
        Tuple of (success, fixed_count)
    """
    cmd = [
        "python", str(SCRIPTS_DIR / "email_verifier" / "fixer.py"),
        "--drafts", drafts_path,
    ]

    if output_path:
        cmd.extend(["--output", output_path])

    if dry_run:
        cmd.append("--dry-run")

    success, stdout, stderr = run_command(cmd, "Email Fixer", dry_run=False)
    print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)

    # Parse fixed count from output
    fixed_count = 0
    for line in stdout.split('\n'):
        if 'Drafts fixed:' in line:
            try:
                fixed_count = int(line.split(':')[1].strip())
            except (ValueError, IndexError):
                pass

    return success, fixed_count


def send_emails(
    drafts_path: str,
    limit: Optional[int] = None,
    dry_run: bool = False
) -> bool:
    """Run the Gmail sender module."""
    cmd = [
        "python", str(SCRIPTS_DIR / "gmail_sender" / "gmail_sender.py"),
        "--csv", drafts_path,
    ]

    if limit:
        cmd.extend(["--limit", str(limit)])

    if dry_run:
        cmd.append("--dry-run")

    success, stdout, stderr = run_command(cmd, "Gmail Sender", dry_run=False)
    print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)

    return success


def main():
    """Main entry point for email pipeline."""
    parser = argparse.ArgumentParser(
        description='Complete email campaign pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline with new prospects
  python scripts/email_pipeline.py --input output/to_email.csv --all

  # Draft and verify only (no sending)
  python scripts/email_pipeline.py --input output/to_email.csv --no-send

  # Verify and fix existing drafts
  python scripts/email_pipeline.py --drafts output/email_campaign/drafts.csv --verify-only

  # Send previously fixed drafts
  python scripts/email_pipeline.py --drafts output/email_campaign/drafts_fixed.csv --send-only
        """
    )

    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--input', '-i',
        type=str,
        help='Input CSV of prospects to draft emails for'
    )
    input_group.add_argument(
        '--drafts', '-d',
        type=str,
        help='Existing drafts CSV to verify/fix/send'
    )

    # Processing options
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Process all contacts (default: test mode with 3)'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limit number of contacts to process'
    )

    # Pipeline control
    parser.add_argument(
        '--no-send',
        action='store_true',
        help='Draft and verify only, do not send emails'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify and fix existing drafts'
    )
    parser.add_argument(
        '--send-only',
        action='store_true',
        help='Only send existing (fixed) drafts'
    )
    parser.add_argument(
        '--skip-fix',
        action='store_true',
        help='Skip the auto-fix step'
    )

    # Safety
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would happen without making changes'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Send even if there are critical issues (not recommended)'
    )

    # Output
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=str(OUTPUT_DIR),
        help=f'Output directory (default: {OUTPUT_DIR})'
    )

    args = parser.parse_args()

    # Banner
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║               EMAIL CAMPAIGN PIPELINE                     ║
    ║   Draft → Verify → Fix → Verify → Send                    ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Track pipeline results
    results = {
        'drafted': 0,
        'issues_before': 0,
        'fixed': 0,
        'issues_after': 0,
        'sent': 0
    }

    # ─────────────────────────────────────────────────────────────
    # STEP 1: DRAFT EMAILS (if not using existing drafts)
    # ─────────────────────────────────────────────────────────────
    if args.input and not args.verify_only and not args.send_only:
        print("\n" + "=" * 60)
        print("STEP 1: DRAFTING EMAILS")
        print("=" * 60)

        drafts_path = output_dir / f"drafts_{timestamp}.csv"

        limit = args.limit if args.limit else (None if args.all else 3)

        success = draft_emails(
            input_path=args.input,
            output_path=str(drafts_path),
            limit=limit,
            dry_run=args.dry_run
        )

        if not success:
            logger.error("Email drafting failed")
            sys.exit(1)

        # Count drafted
        if drafts_path.exists():
            df = pd.read_csv(drafts_path)
            results['drafted'] = len(df)
            logger.info(f"Drafted {results['drafted']} emails")

    elif args.drafts:
        drafts_path = Path(args.drafts)
        if not drafts_path.is_absolute():
            drafts_path = BASE_DIR / args.drafts

        if not drafts_path.exists():
            logger.error(f"Drafts file not found: {drafts_path}")
            sys.exit(1)

        df = pd.read_csv(drafts_path)
        results['drafted'] = len(df)
        logger.info(f"Using existing drafts: {drafts_path} ({results['drafted']} emails)")

    # ─────────────────────────────────────────────────────────────
    # STEP 2: VERIFY EMAILS
    # ─────────────────────────────────────────────────────────────
    if not args.send_only:
        print("\n" + "=" * 60)
        print("STEP 2: VERIFYING EMAILS")
        print("=" * 60)

        verify_report = output_dir / f"verification_report_{timestamp}.csv"

        success, critical, high = verify_emails(
            drafts_path=str(drafts_path),
            output_path=str(verify_report),
            dry_run=args.dry_run
        )

        results['issues_before'] = critical + high
        logger.info(f"Found {critical} critical and {high} high severity issues")

        # ─────────────────────────────────────────────────────────────
        # STEP 3: FIX EMAILS
        # ─────────────────────────────────────────────────────────────
        if not args.skip_fix and results['issues_before'] > 0:
            print("\n" + "=" * 60)
            print("STEP 3: FIXING EMAILS")
            print("=" * 60)

            fixed_path = output_dir / f"drafts_{timestamp}_fixed.csv"

            success, fixed_count = fix_emails(
                drafts_path=str(drafts_path),
                output_path=str(fixed_path),
                dry_run=args.dry_run
            )

            results['fixed'] = fixed_count

            if success and fixed_path.exists():
                drafts_path = fixed_path
                logger.info(f"Fixed {fixed_count} drafts")

            # ─────────────────────────────────────────────────────────────
            # STEP 4: RE-VERIFY EMAILS
            # ─────────────────────────────────────────────────────────────
            print("\n" + "=" * 60)
            print("STEP 4: RE-VERIFYING EMAILS")
            print("=" * 60)

            verify_report_after = output_dir / f"verification_report_{timestamp}_after.csv"

            success, critical_after, high_after = verify_emails(
                drafts_path=str(drafts_path),
                output_path=str(verify_report_after),
                dry_run=args.dry_run
            )

            results['issues_after'] = critical_after + high_after
            logger.info(f"After fix: {critical_after} critical and {high_after} high severity issues")

            # Check if we should proceed
            if critical_after > 0 and not args.force:
                logger.warning(f"{critical_after} critical issues remain - not sending")
                logger.info("Use --force to send anyway, or fix manually")
                print(f"\n[BLOCKED] Critical issues prevent sending. Review: {verify_report_after}")
                print(f"Fixed drafts saved to: {drafts_path}")
                sys.exit(1)

    # ─────────────────────────────────────────────────────────────
    # STEP 5: SEND EMAILS
    # ─────────────────────────────────────────────────────────────
    if not args.no_send and not args.verify_only:
        print("\n" + "=" * 60)
        print("STEP 5: SENDING EMAILS")
        print("=" * 60)

        if args.dry_run:
            logger.info("[DRY RUN] Would send emails from: " + str(drafts_path))
        else:
            limit = args.limit if args.limit else (None if args.all else 3)

            success = send_emails(
                drafts_path=str(drafts_path),
                limit=limit,
                dry_run=args.dry_run
            )

            if success:
                logger.info("Emails sent successfully")
            else:
                logger.error("Some emails failed to send")

    # ─────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"  Emails drafted:        {results['drafted']}")
    print(f"  Issues before fix:     {results['issues_before']}")
    print(f"  Emails fixed:          {results['fixed']}")
    print(f"  Issues after fix:      {results['issues_after']}")
    print(f"  Final drafts:          {drafts_path}")
    print("=" * 60)

    if args.no_send or args.verify_only:
        print("\n[OK] Pipeline complete (no emails sent)")
    elif args.dry_run:
        print("\n[DRY RUN] Pipeline complete - no changes made")
    else:
        print("\n[OK] Pipeline complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
