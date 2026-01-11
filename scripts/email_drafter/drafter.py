"""Email Drafter - Main orchestrator module.

Standalone module for drafting hyper-personalized cold emails.

Usage:
    python drafter.py                           # Test mode (3 contacts)
    python drafter.py --all                     # All contacts
    python drafter.py --input custom.csv        # Custom input file
    python drafter.py --limit 10                # Process 10 contacts
    python drafter.py --sender "Your Name"      # Custom sender name
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm

# Add parent directory to path for imports when running as script
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))

from researcher import research_prospect
from analyzer import analyze_and_select_hook
from composer import compose_email

# Default paths
DEFAULT_INPUT = "processed/03d_final.csv"
ALT_INPUT = "output/prospects_final.csv"
DEFAULT_OUTPUT = "output/email_drafts.csv"
DEFAULT_SENDER = "Tomas"
DEFAULT_LIMIT = 3


def parse_args(args: List[str] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Args:
        args: List of arguments (for testing). Uses sys.argv if None.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Draft hyper-personalized cold emails for prospects."
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        default=None,
        help=f"Input CSV file (default: {DEFAULT_INPUT} or {ALT_INPUT})"
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV file (default: {DEFAULT_OUTPUT})"
    )

    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help="Process all contacts (default: test mode with 3 contacts)"
    )

    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Number of contacts to process (default: {DEFAULT_LIMIT})"
    )

    parser.add_argument(
        '--sender', '-s',
        type=str,
        default=DEFAULT_SENDER,
        help=f"Sender name for emails (default: {DEFAULT_SENDER})"
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be processed without making API calls"
    )

    return parser.parse_args(args)


def find_input_file(specified_path: Optional[str] = None) -> str:
    """
    Find the input CSV file.

    Args:
        specified_path: User-specified path, or None to auto-detect

    Returns:
        Path to input file

    Raises:
        FileNotFoundError: If no valid input file found
    """
    if specified_path:
        if os.path.exists(specified_path):
            return specified_path
        raise FileNotFoundError(f"Input file not found: {specified_path}")

    # Try default paths
    script_dir = Path(__file__).parent.parent.parent  # Go up to project root
    for default in [DEFAULT_INPUT, ALT_INPUT]:
        path = script_dir / default
        if path.exists():
            return str(path)

    raise FileNotFoundError(
        f"No input file found. Tried: {DEFAULT_INPUT}, {ALT_INPUT}. "
        "Use --input to specify a file."
    )


def load_prospects(input_path: str) -> List[Dict[str, Any]]:
    """
    Load prospects from CSV file.

    Args:
        input_path: Path to input CSV

    Returns:
        List of prospect dicts
    """
    print(f"\n[Drafter] Loading prospects from: {input_path}")

    df = pd.read_csv(input_path)

    # Ensure required columns exist
    required_cols = ['page_name', 'contact_name', 'primary_email']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Optional columns with defaults
    optional_cols = {
        'website_url': '',
        'ad_texts': '',
        'linkedin_url': '',
        'instagram_handle': '',
        'twitter_handle': '',
        'contact_position': ''
    }

    for col, default in optional_cols.items():
        if col not in df.columns:
            df[col] = default

    # Filter rows with valid email
    df = df[df['primary_email'].notna() & (df['primary_email'] != '')]

    # Convert to list of dicts
    prospects = df.to_dict('records')

    print(f"[Drafter] Loaded {len(prospects)} prospects with valid emails")

    return prospects


def save_results(results: List[Dict[str, Any]], output_path: str) -> None:
    """
    Save draft results to CSV.

    Args:
        results: List of result dicts
        output_path: Path to output CSV
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

    # Define output columns
    output_cols = [
        'page_name',
        'contact_name',
        'primary_email',
        'subject_line',
        'email_body',
        'hook_used',
        'hook_source',
        'hook_type',
        'analyzer_reasoning',
        'exa_sources',
        'confidence_score',
        'draft_timestamp'
    ]

    # Create DataFrame with columns in order
    df = pd.DataFrame(results)

    # Add any missing columns
    for col in output_cols:
        if col not in df.columns:
            df[col] = ''

    # Reorder columns
    df = df[[col for col in output_cols if col in df.columns]]

    df.to_csv(output_path, index=False)
    print(f"\n[Drafter] Results saved to: {output_path}")


async def process_prospect(
    prospect: Dict[str, Any],
    sender_name: str = DEFAULT_SENDER
) -> Dict[str, Any]:
    """
    Process a single prospect through the full pipeline.

    Pipeline: Research -> Analyze -> Compose

    Args:
        prospect: Prospect dict with contact info
        sender_name: Name to sign emails with

    Returns:
        Result dict with email draft and metadata
    """
    result = {
        'page_name': prospect.get('page_name', ''),
        'contact_name': prospect.get('contact_name', ''),
        'primary_email': prospect.get('primary_email', ''),
        'subject_line': '',
        'email_body': '',
        'hook_used': '',
        'hook_source': '',
        'hook_type': '',
        'analyzer_reasoning': '',
        'exa_sources': '',
        'confidence_score': 0,
        'draft_timestamp': datetime.now().isoformat(),
        'error': False
    }

    try:
        # Parse ad_texts (may be stored as string list)
        ad_texts = prospect.get('ad_texts', '')
        if isinstance(ad_texts, str) and ad_texts:
            ad_texts = [ad_texts]
        elif not ad_texts:
            ad_texts = []

        # 1. RESEARCH PHASE
        research = await research_prospect(
            contact_name=prospect.get('contact_name', ''),
            company_name=prospect.get('page_name', ''),
            website_url=prospect.get('website_url'),
            linkedin_url=prospect.get('linkedin_url'),
            instagram_handle=prospect.get('instagram_handle'),
            twitter_handle=prospect.get('twitter_handle'),
            ad_texts=ad_texts
        )

        result['exa_sources'] = ','.join(research.get('sources', []))

        # 2. ANALYSIS PHASE
        hook = await analyze_and_select_hook(research)

        result['hook_used'] = hook.get('chosen_hook', '')
        result['hook_source'] = hook.get('hook_source', '')
        result['hook_type'] = hook.get('hook_type', '')
        result['analyzer_reasoning'] = hook.get('reasoning', '')
        result['confidence_score'] = hook.get('confidence', 0)

        # 3. COMPOSITION PHASE
        email = await compose_email(
            contact=prospect,
            hook=hook,
            sender_name=sender_name
        )

        result['subject_line'] = email.get('subject_line', '')
        result['email_body'] = email.get('email_body', '')

    except Exception as e:
        print(f"    [Error] Failed to process {prospect.get('page_name')}: {e}")
        result['error'] = True
        result['analyzer_reasoning'] = f"Error: {str(e)}"

    return result


async def process_batch(
    prospects: List[Dict[str, Any]],
    limit: Optional[int] = None,
    sender_name: str = DEFAULT_SENDER
) -> List[Dict[str, Any]]:
    """
    Process a batch of prospects.

    Args:
        prospects: List of prospect dicts
        limit: Max number to process (None for all)
        sender_name: Name to sign emails with

    Returns:
        List of result dicts
    """
    # Apply limit
    if limit is not None:
        prospects = prospects[:limit]

    results = []

    print(f"\n[Drafter] Processing {len(prospects)} prospects...")

    for i, prospect in enumerate(tqdm(prospects, desc="Drafting emails")):
        print(f"\n[{i+1}/{len(prospects)}] {prospect.get('page_name', 'Unknown')}")

        try:
            result = await process_prospect(prospect, sender_name)
            results.append(result)
        except Exception as e:
            print(f"    [Error] {e}")
            # Add error result
            results.append({
                'page_name': prospect.get('page_name', ''),
                'contact_name': prospect.get('contact_name', ''),
                'primary_email': prospect.get('primary_email', ''),
                'subject_line': '',
                'email_body': '',
                'hook_used': '',
                'hook_source': '',
                'hook_type': '',
                'analyzer_reasoning': f'Error: {str(e)}',
                'exa_sources': '',
                'confidence_score': 0,
                'draft_timestamp': datetime.now().isoformat(),
                'error': True
            })

    return results


def print_summary(results: List[Dict[str, Any]]) -> None:
    """Print a summary of the batch results."""
    total = len(results)
    successful = sum(1 for r in results if not r.get('error') and r.get('confidence_score', 0) > 0)
    high_confidence = sum(1 for r in results if r.get('confidence_score', 0) >= 70)

    print(f"\n{'='*50}")
    print("DRAFT SUMMARY")
    print(f"{'='*50}")
    print(f"Total processed: {total}")
    print(f"Successful drafts: {successful}")
    print(f"High confidence (>=70): {high_confidence}")

    if successful > 0:
        avg_confidence = sum(r.get('confidence_score', 0) for r in results if not r.get('error')) / successful
        print(f"Average confidence: {avg_confidence:.1f}")

    # Hook source breakdown
    hook_sources = {}
    for r in results:
        source = r.get('hook_source', 'unknown')
        hook_sources[source] = hook_sources.get(source, 0) + 1

    print(f"\nHook sources:")
    for source, count in sorted(hook_sources.items(), key=lambda x: -x[1]):
        print(f"  {source}: {count}")


async def main():
    """Main entry point."""
    args = parse_args()

    print("""
    ╔═══════════════════════════════════════════╗
    ║         EMAIL DRAFTER MODULE              ║
    ║   Hyper-personalized cold email drafts    ║
    ╚═══════════════════════════════════════════╝
    """)

    try:
        # Find input file
        input_path = find_input_file(args.input)

        # Load prospects
        prospects = load_prospects(input_path)

        if not prospects:
            print("[Drafter] No prospects to process.")
            return

        # Determine limit
        if args.all:
            limit = None
            print(f"[Drafter] Processing ALL {len(prospects)} prospects")
        else:
            limit = args.limit
            print(f"[Drafter] Test mode: processing {limit} prospects")

        # Dry run mode
        if args.dry_run:
            print("\n[DRY RUN] Would process:")
            for p in prospects[:limit]:
                print(f"  - {p.get('page_name')} ({p.get('primary_email')})")
            return

        # Process batch
        results = await process_batch(prospects, limit=limit, sender_name=args.sender)

        # Save results
        save_results(results, args.output)

        # Print summary
        print_summary(results)

        print(f"\n[Drafter] Done! Check {args.output} for email drafts.")

    except FileNotFoundError as e:
        print(f"\n[Error] {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[Error] {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
