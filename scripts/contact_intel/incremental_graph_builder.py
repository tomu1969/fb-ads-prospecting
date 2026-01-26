"""Incremental graph builder for contact intelligence.

Monitors the emails database and builds the Neo4j graph incrementally
as new emails are synced. Can run alongside gmail_sync.py.

Usage:
    # Run continuously, building graph as emails arrive
    python -m scripts.contact_intel.incremental_graph_builder

    # Process once and exit
    python -m scripts.contact_intel.incremental_graph_builder --once

    # Show status
    python -m scripts.contact_intel.incremental_graph_builder --status
"""

import argparse
import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv

load_dotenv()

from scripts.contact_intel.config import DATA_DIR, ensure_data_dir
from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# State file to track processed emails
STATE_FILE = DATA_DIR / "graph_build_state.json"


def get_processed_message_ids() -> Set[str]:
    """Load set of already-processed message IDs."""
    if not STATE_FILE.exists():
        return set()

    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
            return set(state.get("processed_ids", []))
    except (json.JSONDecodeError, IOError):
        return set()


def save_processed_message_ids(processed_ids: Set[str], stats: Dict):
    """Save processed message IDs and stats."""
    ensure_data_dir()
    state = {
        "processed_ids": list(processed_ids),
        "last_updated": datetime.now().isoformat(),
        "stats": stats,
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_unprocessed_emails(db_path: Path, processed_ids: Set[str], limit: int = 1000) -> List[Dict]:
    """Get emails that haven't been processed into the graph yet."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all emails not in processed set
    cursor.execute("""
        SELECT message_id, thread_id, from_email, from_name,
               to_emails, cc_emails, subject, date, account
        FROM emails
        ORDER BY date ASC
    """)

    emails = []
    for row in cursor:
        if row['message_id'] not in processed_ids:
            emails.append(dict(row))
            if len(emails) >= limit:
                break

    conn.close()
    return emails


def get_total_email_count(db_path: Path) -> int:
    """Get total number of emails in database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM emails")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def parse_email_list(email_str: Optional[str]) -> List[Dict]:
    """Parse JSON email list string into list of dicts."""
    if not email_str:
        return []
    try:
        emails = json.loads(email_str)
        # Ensure each item has email and name keys
        result = []
        for item in emails:
            if isinstance(item, str):
                result.append({"email": item, "name": None})
            elif isinstance(item, dict):
                result.append({
                    "email": item.get("email", item.get("address", "")),
                    "name": item.get("name")
                })
        return result
    except (json.JSONDecodeError, TypeError):
        return []


def process_emails_to_graph(gb: GraphBuilder, emails: List[Dict]) -> int:
    """Process emails into the graph.

    Returns number of emails processed.
    """
    processed = 0

    for email in emails:
        try:
            # Parse email data into graph format
            from_email = email['from_email']
            from_name = email.get('from_name')

            # Parse recipients
            to_list = parse_email_list(email.get('to_emails'))
            cc_list = parse_email_list(email.get('cc_emails'))

            # Parse date
            date_str = email.get('date')
            if date_str:
                try:
                    email_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except ValueError:
                    email_date = datetime.now()
            else:
                email_date = datetime.now()

            # Build graph-compatible format
            graph_email = {
                'from': {'email': from_email, 'name': from_name},
                'to': to_list,
                'cc': cc_list,
                'date': email_date,
                'subject': email.get('subject', ''),
            }

            # Process into graph
            gb.process_email(graph_email)
            processed += 1

        except Exception as e:
            logger.warning(f"Error processing email {email.get('message_id')}: {e}")
            continue

    return processed


def run_incremental_build(once: bool = False, batch_size: int = 500, poll_interval: int = 30):
    """Run the incremental graph builder.

    Args:
        once: If True, process once and exit. Otherwise, poll continuously.
        batch_size: Number of emails to process per batch.
        poll_interval: Seconds between polls for new emails.
    """
    db_path = DATA_DIR / "emails.db"

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.info("Run gmail_sync.py first to sync emails.")
        return

    if not neo4j_available():
        logger.error("Neo4j not available. Check connection settings.")
        return

    # Connect to Neo4j
    gb = GraphBuilder()
    gb.connect()

    # Load processed state
    processed_ids = get_processed_message_ids()
    logger.info(f"Already processed: {len(processed_ids):,} emails")

    total_processed = 0

    try:
        while True:
            # Get total emails in database
            total_in_db = get_total_email_count(db_path)

            # Get unprocessed emails
            emails = get_unprocessed_emails(db_path, processed_ids, limit=batch_size)

            if emails:
                logger.info(f"Processing {len(emails)} new emails (DB has {total_in_db:,} total)")

                # Process into graph
                count = process_emails_to_graph(gb, emails)

                # Update processed set
                for email in emails:
                    processed_ids.add(email['message_id'])

                total_processed += count

                # Get graph stats
                stats = gb.get_stats()

                # Save state
                save_processed_message_ids(processed_ids, stats)

                logger.info(
                    f"Processed {count} emails. "
                    f"Graph: {stats['person_nodes']:,} people, "
                    f"{stats['knows_relationships']:,} KNOWS edges"
                )
            else:
                if once:
                    break
                logger.info(f"No new emails. DB has {total_in_db:,}, processed {len(processed_ids):,}. Waiting...")

            if once:
                break

            # Wait before next poll
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    finally:
        gb.close()

        # Final stats
        logger.info(f"\n{'='*50}")
        logger.info(f"Total processed this session: {total_processed:,}")
        logger.info(f"Total processed overall: {len(processed_ids):,}")


def show_status():
    """Show current build status."""
    db_path = DATA_DIR / "emails.db"

    # Database stats
    if db_path.exists():
        total_in_db = get_total_email_count(db_path)
        print(f"Emails in database: {total_in_db:,}")
    else:
        print("Database not found")
        return

    # Processed stats
    processed_ids = get_processed_message_ids()
    print(f"Emails processed to graph: {len(processed_ids):,}")
    print(f"Emails pending: {total_in_db - len(processed_ids):,}")

    # Graph stats
    if neo4j_available():
        gb = GraphBuilder()
        gb.connect()
        stats = gb.get_stats()
        gb.close()

        print(f"\nGraph statistics:")
        print(f"  Person nodes: {stats['person_nodes']:,}")
        print(f"  Company nodes: {stats['company_nodes']:,}")
        print(f"  KNOWS relationships: {stats['knows_relationships']:,}")
        print(f"  CC_TOGETHER edges: {stats['cc_together_relationships']:,}")
    else:
        print("\nNeo4j not available")


def main():
    parser = argparse.ArgumentParser(
        description='Incremental graph builder for contact intelligence'
    )
    parser.add_argument('--once', action='store_true',
                        help='Process once and exit (default: poll continuously)')
    parser.add_argument('--status', action='store_true',
                        help='Show current build status')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Emails per batch (default: 500)')
    parser.add_argument('--poll-interval', type=int, default=30,
                        help='Seconds between polls (default: 30)')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    run_incremental_build(
        once=args.once,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
    )


if __name__ == '__main__':
    main()
