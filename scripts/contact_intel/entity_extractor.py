"""Entity Extractor - Extract company, role, topics from emails via Groq.

Usage:
    python scripts/contact_intel/entity_extractor.py --status
    python scripts/contact_intel/entity_extractor.py --budget 50
    python scripts/contact_intel/entity_extractor.py --resume
    python scripts/contact_intel/entity_extractor.py --sync
"""

import argparse
import logging
from typing import Set

from scripts.contact_intel.body_fetcher import get_contact_emails_with_body
from scripts.contact_intel.contact_prioritizer import get_contact_stats, get_prioritized_contacts
from scripts.contact_intel.extraction_db import (
    complete_run,
    get_extracted_emails,
    init_db,
    save_extraction,
    start_extraction_run,
    update_run_stats,
)
from scripts.contact_intel.groq_client import GroqClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('entity_extractor.log'),
    ]
)
logger = logging.getLogger(__name__)

MY_EMAILS: Set[str] = {
    'tu@jaguarcapital.co',
    'tomas@tujaguarcapital.com',
    'tomasuribe@lahaus.com',
}


def show_status():
    """Show extraction status and eligible contacts."""
    init_db()
    already_extracted = get_extracted_emails()
    stats = get_contact_stats(MY_EMAILS)

    print("\n" + "=" * 50)
    print("ENTITY EXTRACTION STATUS")
    print("=" * 50)
    print(f"\nEligible contacts: {stats['total']}")
    print(f"  Tier 1 (target industry): {stats['tier_1_target_industry']}")
    print(f"  Tier 2 (active 3+): {stats['tier_2_active']}")
    print(f"  Tier 3 (any replied): {stats['tier_3_replied']}")
    print(f"\nBy industry: {stats['by_industry']}")
    print(f"\nAlready extracted: {len(already_extracted)}")
    print(f"Remaining: {stats['total'] - len(already_extracted)}")

    remaining = stats['total'] - len(already_extracted)
    est_cost = min(remaining, 2500) * 0.02
    print(f"\nEstimated cost for 2500 contacts: ${est_cost:.2f}")


def run_extraction(budget: float = 50.0, resume: bool = False):
    """Run entity extraction with budget limit."""
    init_db()

    already_extracted = get_extracted_emails() if resume else set()
    logger.info(f"Starting extraction (budget=${budget}, resume={resume})")
    logger.info(f"Already extracted: {len(already_extracted)} contacts")

    contacts = get_prioritized_contacts(
        my_emails=MY_EMAILS,
        limit=5000,
        already_extracted=already_extracted,
    )

    if not contacts:
        logger.info("No contacts to extract")
        return

    logger.info(f"Contacts to process: {len(contacts)}")

    client = GroqClient()
    run_id = start_extraction_run()
    total_cost = 0.0
    total_contacts = 0
    total_tokens = 0

    try:
        for i, contact in enumerate(contacts):
            if total_cost >= budget:
                logger.info(f"Budget exhausted: ${total_cost:.2f} >= ${budget}")
                break

            email = contact['email']
            name = contact['name']

            emails = get_contact_emails_with_body(email, limit=3)
            if not emails:
                logger.debug(f"No emails found for {email}")
                continue

            result = client.extract_contact_info(email=email, name=name, emails=emails)

            # Partial save immediately
            save_extraction(
                email=email,
                name=name,
                company=result.company,
                role=result.role,
                topics=result.topics,
                confidence=result.confidence,
                source_emails=[e.get('subject', '') for e in emails],
                model=client.model,
            )

            total_cost += result.cost_usd
            total_tokens += result.input_tokens + result.output_tokens
            total_contacts += 1

            update_run_stats(run_id, result.input_tokens + result.output_tokens, result.cost_usd, 1)

            if (i + 1) % 10 == 0:
                logger.info(f"[{i + 1}/{len(contacts)}] Processed {total_contacts}, cost=${total_cost:.2f}")

    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise
    finally:
        complete_run(run_id)
        logger.info(f"\nExtraction complete: {total_contacts} contacts, ${total_cost:.2f}, {total_tokens:,} tokens")


def sync_to_neo4j():
    """Sync extractions to Neo4j graph."""
    from scripts.contact_intel.extraction_sync import sync_extractions_to_neo4j
    sync_extractions_to_neo4j()


def main():
    parser = argparse.ArgumentParser(description='Extract entities from emails via Groq')
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--budget', type=float, default=50.0, help='Max USD to spend')
    parser.add_argument('--resume', action='store_true', help='Resume previous run')
    parser.add_argument('--sync', action='store_true', help='Sync to Neo4j')
    parser.add_argument('--email', type=str, help='Extract single contact')

    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.sync:
        sync_to_neo4j()
    elif args.email:
        init_db()
        client = GroqClient()
        emails = get_contact_emails_with_body(args.email, limit=3)
        if emails:
            result = client.extract_contact_info(args.email, args.email.split('@')[0], emails)
            print(f"Company: {result.company}\nRole: {result.role}\nTopics: {result.topics}\nConfidence: {result.confidence}\nCost: ${result.cost_usd:.4f}")
        else:
            print(f"No emails found for {args.email}")
    else:
        run_extraction(budget=args.budget, resume=args.resume)


if __name__ == "__main__":
    main()
