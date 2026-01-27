"""LinkedIn Connections CSV sync module.

Parses LinkedIn's exported Connections.csv and integrates with the contact graph.
"""

import argparse
import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Default path for LinkedIn Connections CSV export
DEFAULT_CSV_PATH = Path('data/contact_intel/linkedin_connections.csv')


def parse_linkedin_csv(csv_path: Path) -> List[Dict]:
    """Parse LinkedIn Connections.csv export file.

    Args:
        csv_path: Path to the LinkedIn Connections.csv file

    Returns:
        List of dicts with keys: first_name, last_name, email, company, position, connected_on
        Empty values are returned as None.

    Raises:
        FileNotFoundError: If the CSV file doesn't exist
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"LinkedIn CSV not found: {csv_path}")

    connections = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            connection = {
                'first_name': row.get('First Name') or None,
                'last_name': row.get('Last Name') or None,
                'email': row.get('Email Address') or None,
                'company': row.get('Company') or None,
                'position': row.get('Position') or None,
                'connected_on': row.get('Connected On') or None,
            }
            connections.append(connection)

    logger.info(f"Parsed {len(connections)} LinkedIn connections from {csv_path}")
    return connections


def create_linkedin_connection(
    gb,  # GraphBuilder instance
    my_email: str,
    connection: Dict,
) -> bool:
    """Create LINKEDIN_CONNECTED edge between me and a connection.

    Args:
        gb: GraphBuilder instance with active connection
        my_email: Your email address
        connection: Dict with first_name, last_name, email, company, position, connected_on

    Returns:
        True if edge created, False if skipped (no email)
    """
    email = connection.get('email')
    if not email:
        logger.debug(f"Skipping connection without email: {connection.get('first_name')} {connection.get('last_name')}")
        return False

    first_name = connection.get('first_name', '')
    last_name = connection.get('last_name', '')
    full_name = f"{first_name} {last_name}".strip()
    company = connection.get('company')
    position = connection.get('position')
    connected_on = connection.get('connected_on')

    with gb.driver.session() as session:
        # Create/update Person node for the connection
        session.run("""
            MERGE (p:Person {primary_email: $email})
            SET p.name = COALESCE(p.name, $name),
                p.linkedin_company = $company,
                p.linkedin_position = $position,
                p.updated_at = datetime()
        """, email=email, name=full_name, company=company, position=position)

        # Create LINKEDIN_CONNECTED edge
        session.run("""
            MATCH (me:Person {primary_email: $my_email})
            MATCH (them:Person {primary_email: $email})
            MERGE (me)-[r:LINKEDIN_CONNECTED]->(them)
            SET r.degree = 1,
                r.connected_on = $connected_on,
                r.created_at = datetime()
        """, my_email=my_email, email=email, connected_on=connected_on)

    logger.debug(f"Created LINKEDIN_CONNECTED: {my_email} -> {email}")
    return True


def sync_linkedin_connections(
    csv_path: Optional[Path] = None,
    my_email: str = 'tu@jaguarcapital.co',
) -> Dict:
    """Sync LinkedIn connections to Neo4j graph.

    Args:
        csv_path: Path to LinkedIn Connections.csv (default: data/contact_intel/linkedin_connections.csv)
        my_email: Your email address

    Returns:
        Stats dict with total, synced, skipped, errors counts
    """
    if csv_path is None:
        csv_path = DEFAULT_CSV_PATH

    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    # Parse CSV
    connections = parse_linkedin_csv(csv_path)

    # Connect to Neo4j
    gb = GraphBuilder()
    gb.connect()

    # Ensure schema exists
    gb.setup_linkedin_schema()

    stats = {
        'total': len(connections),
        'synced': 0,
        'skipped': 0,
        'errors': 0,
    }

    try:
        for conn in connections:
            try:
                if create_linkedin_connection(gb, my_email, conn):
                    stats['synced'] += 1
                else:
                    stats['skipped'] += 1
            except Exception as e:
                logger.error(f"Error syncing {conn.get('email')}: {e}")
                stats['errors'] += 1
    finally:
        gb.close()

    logger.info(f"LinkedIn sync complete: {stats}")
    return stats


def show_status():
    """Show LinkedIn sync status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count LINKEDIN_CONNECTED edges
            result = session.run("""
                MATCH ()-[r:LINKEDIN_CONNECTED]->()
                RETURN count(r) as count
            """)
            linkedin_count = result.single()['count']

            # Count unique LinkedIn connections
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_CONNECTED]->(p)
                RETURN count(DISTINCT p) as count
            """)
            my_connections = result.single()['count']

        print("\n" + "=" * 50)
        print("LINKEDIN SYNC STATUS")
        print("=" * 50)
        print(f"Total LINKEDIN_CONNECTED edges: {linkedin_count}")
        print(f"Your LinkedIn connections: {my_connections}")

        # Check if CSV exists
        if DEFAULT_CSV_PATH.exists():
            connections = parse_linkedin_csv(DEFAULT_CSV_PATH)
            print(f"\nLinkedIn CSV: {DEFAULT_CSV_PATH}")
            print(f"Connections in CSV: {len(connections)}")
            with_email = sum(1 for c in connections if c.get('email'))
            print(f"With email address: {with_email}")
        else:
            print(f"\nLinkedIn CSV not found: {DEFAULT_CSV_PATH}")
            print("Export your connections from LinkedIn:")
            print("  Settings → Data Privacy → Get a copy of your data → Connections")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Sync LinkedIn connections to contact graph'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show sync status')
    parser.add_argument('--sync', action='store_true',
                        help='Sync LinkedIn connections to Neo4j')
    parser.add_argument('--csv', type=str,
                        help='Path to LinkedIn Connections.csv')
    parser.add_argument('--my-email', type=str, default='tu@jaguarcapital.co',
                        help='Your email address')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.sync:
        csv_path = Path(args.csv) if args.csv else None
        stats = sync_linkedin_connections(csv_path=csv_path, my_email=args.my_email)
        print("\n" + "=" * 50)
        print("LINKEDIN SYNC RESULTS")
        print("=" * 50)
        print(f"Total connections: {stats.get('total', 0)}")
        print(f"Synced to graph:   {stats.get('synced', 0)}")
        print(f"Skipped (no email): {stats.get('skipped', 0)}")
        print(f"Errors:            {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
