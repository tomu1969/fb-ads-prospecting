"""Overnight graph enrichment orchestrator.

Runs three enrichment tasks sequentially:
1. Relationship strength scoring (free, ~30 min)
2. Industry classification (~$0.50, ~1-2 hrs)
3. Contact gap filling (~$10, ~2-3 hrs)

Usage:
    python -m scripts.contact_intel.overnight_enrichment

Progress is logged to overnight_enrichment.log
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Configure logging
LOG_FILE = Path('overnight_enrichment.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
logger = logging.getLogger(__name__)


def run_task_1_strength():
    """Task 1: Relationship strength scoring."""
    logger.info("=" * 60)
    logger.info("TASK 1: RELATIONSHIP STRENGTH SCORING")
    logger.info("=" * 60)

    from scripts.contact_intel.relationship_strength import run_strength_scoring

    start = datetime.now()
    stats = run_strength_scoring()
    duration = datetime.now() - start

    logger.info(f"Task 1 completed in {duration}")
    logger.info(f"Results: {stats}")

    return stats


def run_task_2_industry():
    """Task 2: Industry classification."""
    logger.info("=" * 60)
    logger.info("TASK 2: INDUSTRY CLASSIFICATION")
    logger.info("=" * 60)

    from scripts.contact_intel.industry_classifier import run_classification

    start = datetime.now()
    stats = run_classification()
    duration = datetime.now() - start

    logger.info(f"Task 2 completed in {duration}")
    logger.info(f"Results: {stats}")

    return stats


def run_task_3_domain_linking():
    """Task 3: Domain-based company linking (free)."""
    logger.info("=" * 60)
    logger.info("TASK 3: DOMAIN COMPANY LINKING (FREE)")
    logger.info("=" * 60)

    from scripts.contact_intel.domain_company_linker import run_domain_linking

    start = datetime.now()
    stats = run_domain_linking()
    duration = datetime.now() - start

    logger.info(f"Task 3 completed in {duration}")
    logger.info(f"Results: {stats}")

    return stats


def run_task_4_gap_fill():
    """Task 4: Contact gap filling with Apollo (optional, costly)."""
    logger.info("=" * 60)
    logger.info("TASK 4: CONTACT GAP FILLING (APOLLO)")
    logger.info("=" * 60)

    from scripts.contact_intel.contact_gap_filler import run_gap_filling

    start = datetime.now()
    stats = run_gap_filling()
    duration = datetime.now() - start

    logger.info(f"Task 4 completed in {duration}")
    logger.info(f"Results: {stats}")

    return stats


def run_all():
    """Run all enrichment tasks."""
    overall_start = datetime.now()

    logger.info("=" * 60)
    logger.info("OVERNIGHT ENRICHMENT STARTED")
    logger.info(f"Start time: {overall_start}")
    logger.info("=" * 60)

    results = {}

    # Task 1: Relationship strength (free, fast)
    try:
        results['task_1_strength'] = run_task_1_strength()
    except Exception as e:
        logger.error(f"Task 1 failed: {e}")
        results['task_1_strength'] = {'error': str(e)}

    # Task 2: Industry classification (cheap, medium)
    try:
        results['task_2_industry'] = run_task_2_industry()
    except Exception as e:
        logger.error(f"Task 2 failed: {e}")
        results['task_2_industry'] = {'error': str(e)}

    # Task 3: Domain linking (free, fast)
    try:
        results['task_3_domain_linking'] = run_task_3_domain_linking()
    except Exception as e:
        logger.error(f"Task 3 failed: {e}")
        results['task_3_domain_linking'] = {'error': str(e)}

    # Task 4: Apollo gap filling (optional, skip by default due to cost)
    # Uncomment to run:
    # try:
    #     results['task_4_gap_fill'] = run_task_4_gap_fill()
    # except Exception as e:
    #     logger.error(f"Task 4 failed: {e}")
    #     results['task_4_gap_fill'] = {'error': str(e)}

    # Summary
    overall_duration = datetime.now() - overall_start

    logger.info("=" * 60)
    logger.info("OVERNIGHT ENRICHMENT COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total duration: {overall_duration}")

    # Calculate totals
    total_cost = 0
    for task_result in results.values():
        if isinstance(task_result, dict):
            total_cost += task_result.get('cost_usd', 0)

    logger.info(f"Estimated total cost: ${total_cost:.2f}")

    # Print summary
    print("\n" + "=" * 60)
    print("OVERNIGHT ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"Total duration: {overall_duration}")
    print(f"Estimated cost: ${total_cost:.2f}")
    print(f"Log file: {LOG_FILE.absolute()}")

    print("\nTask Results:")
    for task_name, task_result in results.items():
        status = "OK" if 'error' not in task_result else "FAILED"
        print(f"  {task_name}: {status}")
        if 'error' in task_result:
            print(f"    Error: {task_result['error']}")

    return results


def show_status():
    """Show status of all enrichment tasks."""
    print("\n" + "=" * 60)
    print("OVERNIGHT ENRICHMENT STATUS")
    print("=" * 60)

    # Import and run status for each task
    print("\n--- Task 1: Relationship Strength ---")
    from scripts.contact_intel.relationship_strength import show_status as show_strength_status
    show_strength_status()

    print("\n--- Task 2: Industry Classification ---")
    from scripts.contact_intel.industry_classifier import show_status as show_industry_status
    show_industry_status()

    print("\n--- Task 3: Domain Company Linking ---")
    from scripts.contact_intel.domain_company_linker import show_status as show_domain_status
    show_domain_status()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Overnight graph enrichment orchestrator'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show enrichment status')
    parser.add_argument('--run', action='store_true',
                        help='Run all enrichment tasks')
    parser.add_argument('--task', type=int, choices=[1, 2, 3, 4],
                        help='Run only a specific task (1=strength, 2=industry, 3=domain, 4=apollo)')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.task:
        if args.task == 1:
            run_task_1_strength()
        elif args.task == 2:
            run_task_2_industry()
        elif args.task == 3:
            run_task_3_domain_linking()
        elif args.task == 4:
            run_task_4_gap_fill()
        return

    if args.run:
        run_all()
        return

    # Default: show help
    parser.print_help()
    print("\nQuick start:")
    print("  python -m scripts.contact_intel.overnight_enrichment --status  # Check current state")
    print("  python -m scripts.contact_intel.overnight_enrichment --run     # Run all tasks overnight")


if __name__ == '__main__':
    main()
