"""Neo4j graph builder for contact intelligence.

Builds and updates the contact intelligence graph in Neo4j.
Handles Person nodes, KNOWS relationships, and CC_TOGETHER edges.

Usage:
    # Setup schema (run once)
    python scripts/contact_intel/graph_builder.py --setup

    # Test connection
    python scripts/contact_intel/graph_builder.py --test

    # Show graph statistics
    python scripts/contact_intel/graph_builder.py --status
"""

import argparse
import logging
import os
from datetime import datetime
from itertools import combinations
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

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

# Neo4j connection settings
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')


def neo4j_available() -> bool:
    """Check if Neo4j is available and credentials are configured."""
    if not NEO4J_PASSWORD:
        return False
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        driver.verify_connectivity()
        driver.close()
        return True
    except Exception as e:
        logger.debug(f"Neo4j not available: {e}")
        return False


class GraphBuilder:
    """Builds and updates the contact intelligence graph in Neo4j."""

    def __init__(self):
        """Initialize GraphBuilder (does not connect automatically)."""
        self.driver = None

    def connect(self):
        """Connect to Neo4j database.

        Raises:
            Exception: If connection fails or credentials not configured.
        """
        from neo4j import GraphDatabase

        if not NEO4J_PASSWORD:
            raise ValueError(
                "NEO4J_PASSWORD not set. Add to .env:\n"
                "NEO4J_URI=bolt://localhost:7687\n"
                "NEO4J_USER=neo4j\n"
                "NEO4J_PASSWORD=your_password"
            )

        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        self.driver.verify_connectivity()
        logger.info(f"Connected to Neo4j at {NEO4J_URI}")

    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            logger.debug("Neo4j connection closed")

    def setup_schema(self):
        """Create indexes and constraints for the graph schema.

        Creates:
        - Unique constraint on Person.primary_email
        - Index on Person.name for search
        - Index on Company.domain
        """
        with self.driver.session() as session:
            # Unique constraint on Person primary_email
            session.run("""
                CREATE CONSTRAINT person_email IF NOT EXISTS
                FOR (p:Person) REQUIRE p.primary_email IS UNIQUE
            """)

            # Index on Person name for search
            session.run("""
                CREATE INDEX person_name IF NOT EXISTS
                FOR (p:Person) ON (p.name)
            """)

            # Index on Company domain
            session.run("""
                CREATE INDEX company_domain IF NOT EXISTS
                FOR (c:Company) ON (c.domain)
            """)

            # Index on alternate emails
            session.run("""
                CREATE INDEX person_alternate_emails IF NOT EXISTS
                FOR (p:Person) ON (p.alternate_emails)
            """)

            # Location indexes for geographic queries
            session.run("""
                CREATE INDEX person_city IF NOT EXISTS
                FOR (p:Person) ON (p.city)
            """)
            session.run("""
                CREATE INDEX person_country IF NOT EXISTS
                FOR (p:Person) ON (p.country)
            """)
            session.run("""
                CREATE INDEX person_state IF NOT EXISTS
                FOR (p:Person) ON (p.state)
            """)

        logger.info("Schema setup complete (constraints and indexes created)")

    def setup_linkedin_schema(self):
        """Create indexes for LinkedIn integration.

        Creates:
        - Index on Person.linkedin_url for lookup
        """
        with self.driver.session() as session:
            session.run("""
                CREATE INDEX person_linkedin_url IF NOT EXISTS
                FOR (p:Person) ON (p.linkedin_url)
            """)

        logger.info("LinkedIn schema setup complete")

    def create_or_update_person(
        self,
        email: str,
        name: Optional[str] = None,
        company: Optional[str] = None,
        **properties
    ) -> str:
        """Create or update a Person node.

        Uses MERGE to ensure idempotency - same email always maps to same node.

        Args:
            email: Primary email address (unique identifier)
            name: Person's name
            company: Company name
            **properties: Additional properties (linkedin_url, role, etc.)

        Returns:
            The Person node's element ID
        """
        # Build properties dict
        props = {'primary_email': email}
        if name:
            props['name'] = name
        if company:
            props['company'] = company
        props.update(properties)

        # Remove None values
        props = {k: v for k, v in props.items() if v is not None}

        with self.driver.session() as session:
            result = session.run("""
                MERGE (p:Person {primary_email: $email})
                SET p += $props
                SET p.updated_at = datetime()
                RETURN elementId(p) as id
            """, email=email, props=props)

            record = result.single()
            return record['id'] if record else None

    def find_person_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Find Person node by primary email address.

        Args:
            email: Email address to search for

        Returns:
            Dict of person properties, or None if not found
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Person {primary_email: $email})
                RETURN p
            """, email=email)

            record = result.single()
            if record:
                return dict(record['p'])
            return None

    def find_person_by_any_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Find Person node by primary or alternate email.

        Args:
            email: Email address to search for (primary or alternate)

        Returns:
            Dict of person properties, or None if not found
        """
        with self.driver.session() as session:
            # First try primary email
            result = session.run("""
                MATCH (p:Person {primary_email: $email})
                RETURN p
            """, email=email)
            record = result.single()
            if record:
                return dict(record['p'])

            # Then try alternate emails
            result = session.run("""
                MATCH (p:Person)
                WHERE $email IN p.alternate_emails
                RETURN p
            """, email=email)
            record = result.single()
            if record:
                return dict(record['p'])

            return None

    def add_alternate_email(self, primary_email: str, alternate_email: str):
        """Add an alternate email address to an existing Person.

        Args:
            primary_email: The person's primary email
            alternate_email: The alternate email to add
        """
        with self.driver.session() as session:
            session.run("""
                MATCH (p:Person {primary_email: $primary_email})
                SET p.alternate_emails = CASE
                    WHEN p.alternate_emails IS NULL THEN [$alternate_email]
                    WHEN NOT $alternate_email IN p.alternate_emails
                        THEN p.alternate_emails + $alternate_email
                    ELSE p.alternate_emails
                END
            """, primary_email=primary_email, alternate_email=alternate_email)

    def create_knows_relationship(
        self,
        from_email: str,
        to_email: str,
        email_date: datetime,
        **properties
    ):
        """Create or update KNOWS relationship between two people.

        MERGE ensures idempotency. Updates:
        - email_count: Incremented on each call
        - first_contact: Set only on creation
        - last_contact: Updated if email_date is more recent

        Args:
            from_email: Sender's email
            to_email: Recipient's email
            email_date: Date of the email
            **properties: Additional relationship properties
        """
        date_str = email_date.isoformat()

        with self.driver.session() as session:
            session.run("""
                MATCH (from:Person {primary_email: $from_email})
                MATCH (to:Person {primary_email: $to_email})
                MERGE (from)-[r:KNOWS]->(to)
                ON CREATE SET
                    r.email_count = 1,
                    r.first_contact = $date_str,
                    r.last_contact = $date_str,
                    r.created_at = datetime()
                ON MATCH SET
                    r.email_count = r.email_count + 1,
                    r.last_contact = CASE
                        WHEN r.last_contact < $date_str THEN $date_str
                        ELSE r.last_contact
                    END,
                    r.first_contact = CASE
                        WHEN r.first_contact > $date_str THEN $date_str
                        ELSE r.first_contact
                    END
                SET r += $props
            """, from_email=from_email, to_email=to_email, date_str=date_str, props=properties)

    def get_relationship(self, from_email: str, to_email: str) -> Optional[Dict[str, Any]]:
        """Get KNOWS relationship between two people.

        Args:
            from_email: Sender's email
            to_email: Recipient's email

        Returns:
            Dict of relationship properties, or None if not found
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (from:Person {primary_email: $from_email})-[r:KNOWS]->(to:Person {primary_email: $to_email})
                RETURN r
            """, from_email=from_email, to_email=to_email)

            record = result.single()
            if record:
                return dict(record['r'])
            return None

    def create_cc_together_relationship(
        self,
        email1: str,
        email2: str,
        email_date: datetime
    ):
        """Create CC_TOGETHER relationship (people CC'd on same email).

        This is a symmetric relationship - order doesn't matter.
        Uses MERGE to ensure only one edge exists between any two people.

        Args:
            email1: First person's email
            email2: Second person's email
            email_date: Date of the email they were CC'd on
        """
        # Normalize order to ensure consistent relationship direction
        if email1 > email2:
            email1, email2 = email2, email1

        date_str = email_date.isoformat()

        with self.driver.session() as session:
            session.run("""
                MATCH (p1:Person {primary_email: $email1})
                MATCH (p2:Person {primary_email: $email2})
                MERGE (p1)-[r:CC_TOGETHER]-(p2)
                ON CREATE SET
                    r.cc_count = 1,
                    r.first_seen = $date_str,
                    r.last_seen = $date_str
                ON MATCH SET
                    r.cc_count = r.cc_count + 1,
                    r.last_seen = CASE
                        WHEN r.last_seen < $date_str THEN $date_str
                        ELSE r.last_seen
                    END
            """, email1=email1, email2=email2, date_str=date_str)

    def process_email(self, email_message: Dict[str, Any]):
        """Process a single email and update the graph.

        Creates/updates:
        - Person nodes for sender and all recipients
        - KNOWS edges from sender to each recipient (to + cc)
        - CC_TOGETHER edges between all people CC'd together

        Args:
            email_message: Dict with keys:
                - from: {email, name}
                - to: [{email, name}, ...]
                - cc: [{email, name}, ...] (optional)
                - date: datetime
                - subject: str (optional)
        """
        sender = email_message['from']
        to_list = email_message.get('to', [])
        cc_list = email_message.get('cc', [])
        email_date = email_message['date']
        subject = email_message.get('subject', '')

        # Create sender node
        self.create_or_update_person(
            email=sender['email'],
            name=sender.get('name')
        )

        # Create recipient nodes and KNOWS relationships
        all_recipients = to_list + cc_list
        for recipient in all_recipients:
            # Create recipient node
            self.create_or_update_person(
                email=recipient['email'],
                name=recipient.get('name')
            )

            # Create KNOWS relationship from sender to recipient
            self.create_knows_relationship(
                from_email=sender['email'],
                to_email=recipient['email'],
                email_date=email_date,
                last_subject=subject
            )

        # Create CC_TOGETHER relationships between all CC'd people
        # (and also include people in 'to' since they're all on the same email)
        if len(all_recipients) > 1:
            recipient_emails = [r['email'] for r in all_recipients]
            for email1, email2 in combinations(recipient_emails, 2):
                self.create_cc_together_relationship(
                    email1=email1,
                    email2=email2,
                    email_date=email_date
                )

    def get_stats(self) -> Dict[str, int]:
        """Get graph statistics.

        Returns:
            Dict with counts of nodes and relationships
        """
        with self.driver.session() as session:
            # Count Person nodes
            result = session.run("MATCH (p:Person) RETURN count(p) as count")
            person_count = result.single()['count']

            # Count KNOWS relationships
            result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r) as count")
            knows_count = result.single()['count']

            # Count CC_TOGETHER relationships
            result = session.run("MATCH ()-[r:CC_TOGETHER]-() RETURN count(r) as count")
            cc_count = result.single()['count']

            # Count Company nodes
            result = session.run("MATCH (c:Company) RETURN count(c) as count")
            company_count = result.single()['count']

            return {
                'person_nodes': person_count,
                'company_nodes': company_count,
                'knows_relationships': knows_count,
                'cc_together_relationships': cc_count // 2,  # Divide by 2 since undirected
            }


def main():
    """CLI entry point for graph builder operations."""
    parser = argparse.ArgumentParser(
        description='Neo4j graph builder for contact intelligence'
    )
    parser.add_argument('--setup', action='store_true',
                        help='Setup schema (constraints and indexes)')
    parser.add_argument('--test', action='store_true',
                        help='Test Neo4j connection')
    parser.add_argument('--status', action='store_true',
                        help='Show graph statistics')

    args = parser.parse_args()

    if args.test:
        logger.info("Testing Neo4j connection...")
        if neo4j_available():
            logger.info("Neo4j is available and connected!")
            gb = GraphBuilder()
            gb.connect()
            gb.close()
        else:
            logger.error("Neo4j is not available.")
            logger.info("\nTo start Neo4j with Docker:")
            logger.info("  docker run -d --name neo4j \\")
            logger.info("    -p 7474:7474 -p 7687:7687 \\")
            logger.info("    -e NEO4J_AUTH=neo4j/contact_intel_2025 \\")
            logger.info("    neo4j:latest")
            logger.info("\nThen add to .env:")
            logger.info("  NEO4J_URI=bolt://localhost:7687")
            logger.info("  NEO4J_USER=neo4j")
            logger.info("  NEO4J_PASSWORD=contact_intel_2025")
        return

    if args.setup:
        logger.info("Setting up Neo4j schema...")
        gb = GraphBuilder()
        gb.connect()
        gb.setup_schema()
        gb.close()
        logger.info("Schema setup complete!")
        return

    if args.status:
        logger.info("Fetching graph statistics...")
        gb = GraphBuilder()
        gb.connect()
        stats = gb.get_stats()
        gb.close()

        logger.info("\n=== Graph Statistics ===")
        logger.info(f"  Person nodes:           {stats['person_nodes']:,}")
        logger.info(f"  Company nodes:          {stats['company_nodes']:,}")
        logger.info(f"  KNOWS relationships:    {stats['knows_relationships']:,}")
        logger.info(f"  CC_TOGETHER edges:      {stats['cc_together_relationships']:,}")
        return

    # Default: show help
    parser.print_help()


if __name__ == '__main__':
    main()
