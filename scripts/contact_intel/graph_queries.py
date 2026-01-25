"""Cypher query library for contact intelligence graph.

Provides structured queries for finding intro paths to prospects through
your network. Works with Neo4j graph built by graph_builder.py.

Usage:
    from scripts.contact_intel.graph_queries import GraphQueries, PathResult
    from scripts.contact_intel.graph_builder import GraphBuilder

    gb = GraphBuilder()
    gb.connect()
    queries = GraphQueries(gb.driver)

    # Find direct connection
    result = queries.find_direct_connection("me@example.com", "target@example.com")

    # Find one-hop paths
    results = queries.find_one_hop_paths("me@example.com", "target@example.com")

    gb.close()
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path for CLI execution
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

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


@dataclass
class PathResult:
    """Result of a path query.

    Represents a single path from you to a prospect, optionally
    through a connector (intermediary).

    Attributes:
        prospect_email: Email of the prospect we're trying to reach
        prospect_name: Name of the prospect (if known)
        path_type: Type of path - direct, one_hop, company_connection, cc_together, cold
        connector_email: Email of the person who can make the intro (None for direct)
        connector_name: Name of the connector (None for direct)
        connector_strength: Strength of your relationship with connector (1-10)
        email_count: Number of emails you've exchanged with connector (or prospect for direct)
        last_contact: Date of last email with connector (or prospect for direct)
        shared_cc_count: Number of times connector was CC'd with prospect
    """
    prospect_email: str
    prospect_name: Optional[str]
    path_type: str  # direct, one_hop, company_connection, cc_together, cold
    connector_email: Optional[str]
    connector_name: Optional[str]
    connector_strength: int  # 1-10
    email_count: int
    last_contact: Optional[datetime]
    shared_cc_count: int  # Times CC'd together


class GraphQueries:
    """Cypher query library for contact intelligence graph.

    Provides structured queries for finding intro paths to prospects.
    All methods return PathResult objects for consistent handling.
    """

    def __init__(self, driver):
        """Initialize with Neo4j driver.

        Args:
            driver: Neo4j driver instance (from GraphBuilder.driver)
        """
        self.driver = driver

    def find_direct_connection(
        self,
        my_email: str,
        target_email: str
    ) -> Optional[PathResult]:
        """Find direct KNOWS relationship to target.

        Checks if you have emailed the target directly.

        Args:
            my_email: Your email address
            target_email: Target prospect's email

        Returns:
            PathResult with path_type='direct' if found, None otherwise
        """
        query = '''
        MATCH (me:Person {primary_email: $my_email})-[r:KNOWS]->(target:Person)
        WHERE target.primary_email = $target_email
           OR $target_email IN coalesce(target.alternate_emails, [])
        RETURN target.primary_email as email,
               target.name as name,
               r.email_count as email_count,
               r.last_contact as last_contact
        '''

        with self.driver.session() as session:
            result = session.run(query, my_email=my_email, target_email=target_email)
            record = result.single()

            if record:
                last_contact = None
                if record['last_contact']:
                    try:
                        last_contact = datetime.fromisoformat(record['last_contact'])
                    except (ValueError, TypeError):
                        pass

                return PathResult(
                    prospect_email=record['email'],
                    prospect_name=record['name'],
                    path_type='direct',
                    connector_email=None,
                    connector_name=None,
                    connector_strength=0,  # Not applicable for direct
                    email_count=record['email_count'] or 0,
                    last_contact=last_contact,
                    shared_cc_count=0
                )

        return None

    def find_one_hop_paths(
        self,
        my_email: str,
        target_email: str,
        limit: int = 5
    ) -> List[PathResult]:
        """Find friend-of-friend paths to target.

        Finds people you know who also know the target.
        Results are ranked by your email count with the connector.

        Args:
            my_email: Your email address
            target_email: Target prospect's email
            limit: Maximum number of paths to return

        Returns:
            List of PathResult with path_type='one_hop', sorted by connector strength
        """
        query = '''
        MATCH (me:Person {primary_email: $my_email})-[r1:KNOWS]->(connector)-[r2:KNOWS]->(target:Person)
        WHERE (target.primary_email = $target_email
               OR $target_email IN coalesce(target.alternate_emails, []))
          AND connector <> me
          AND connector <> target
        RETURN connector.primary_email as connector_email,
               connector.name as connector_name,
               target.primary_email as target_email,
               target.name as target_name,
               r1.email_count as my_emails_with_connector,
               r1.last_contact as last_contact_with_connector
        ORDER BY r1.email_count DESC
        LIMIT $limit
        '''

        results = []
        with self.driver.session() as session:
            result = session.run(
                query,
                my_email=my_email,
                target_email=target_email,
                limit=limit
            )

            for record in result:
                last_contact = None
                if record['last_contact_with_connector']:
                    try:
                        last_contact = datetime.fromisoformat(
                            record['last_contact_with_connector']
                        )
                    except (ValueError, TypeError):
                        pass

                # Calculate connector strength
                email_count = record['my_emails_with_connector'] or 0
                connector_strength = self._calculate_strength(email_count, last_contact)

                results.append(PathResult(
                    prospect_email=record['target_email'],
                    prospect_name=record['target_name'],
                    path_type='one_hop',
                    connector_email=record['connector_email'],
                    connector_name=record['connector_name'],
                    connector_strength=connector_strength,
                    email_count=email_count,
                    last_contact=last_contact,
                    shared_cc_count=0
                ))

        return results

    def find_company_connections(
        self,
        my_email: str,
        target_domain: str,
        limit: int = 5
    ) -> List[PathResult]:
        """Find people you know at a specific company domain.

        Useful when you want to reach someone at a company where
        you already have connections.

        Args:
            my_email: Your email address
            target_domain: Company domain (e.g., "acme.com")
            limit: Maximum number of results

        Returns:
            List of PathResult with path_type='company_connection'
        """
        # Ensure domain format
        if not target_domain.startswith('@'):
            domain_suffix = f'@{target_domain}'
        else:
            domain_suffix = target_domain

        query = '''
        MATCH (me:Person {primary_email: $my_email})-[r:KNOWS]->(person:Person)
        WHERE person.primary_email ENDS WITH $domain_suffix
        RETURN person.primary_email as email,
               person.name as name,
               r.email_count as email_count,
               r.last_contact as last_contact
        ORDER BY r.email_count DESC
        LIMIT $limit
        '''

        results = []
        with self.driver.session() as session:
            result = session.run(
                query,
                my_email=my_email,
                domain_suffix=domain_suffix,
                limit=limit
            )

            for record in result:
                last_contact = None
                if record['last_contact']:
                    try:
                        last_contact = datetime.fromisoformat(record['last_contact'])
                    except (ValueError, TypeError):
                        pass

                email_count = record['email_count'] or 0
                connector_strength = self._calculate_strength(email_count, last_contact)

                results.append(PathResult(
                    prospect_email=record['email'],
                    prospect_name=record['name'],
                    path_type='company_connection',
                    connector_email=None,  # They are the connection directly
                    connector_name=None,
                    connector_strength=connector_strength,
                    email_count=email_count,
                    last_contact=last_contact,
                    shared_cc_count=0
                ))

        return results

    def find_cc_together_connections(
        self,
        my_email: str,
        target_email: str,
        limit: int = 5
    ) -> List[PathResult]:
        """Find people who have been CC'd together with the target.

        This can reveal people in the same circles as the target,
        even if you don't have direct one-hop paths.

        Path: me -> (KNOWS) -> connector -> (CC_TOGETHER) -> target

        Args:
            my_email: Your email address
            target_email: Target prospect's email
            limit: Maximum number of results

        Returns:
            List of PathResult with path_type='cc_together'
        """
        query = '''
        MATCH (me:Person {primary_email: $my_email})-[r1:KNOWS]->(connector)-[r2:CC_TOGETHER]-(target:Person)
        WHERE (target.primary_email = $target_email
               OR $target_email IN coalesce(target.alternate_emails, []))
          AND connector <> me
          AND connector <> target
        RETURN connector.primary_email as connector_email,
               connector.name as connector_name,
               target.primary_email as target_email,
               target.name as target_name,
               r1.email_count as my_emails_with_connector,
               r1.last_contact as last_contact_with_connector,
               r2.cc_count as shared_cc_count
        ORDER BY r2.cc_count DESC, r1.email_count DESC
        LIMIT $limit
        '''

        results = []
        with self.driver.session() as session:
            result = session.run(
                query,
                my_email=my_email,
                target_email=target_email,
                limit=limit
            )

            for record in result:
                last_contact = None
                if record['last_contact_with_connector']:
                    try:
                        last_contact = datetime.fromisoformat(
                            record['last_contact_with_connector']
                        )
                    except (ValueError, TypeError):
                        pass

                email_count = record['my_emails_with_connector'] or 0
                connector_strength = self._calculate_strength(email_count, last_contact)

                results.append(PathResult(
                    prospect_email=record['target_email'],
                    prospect_name=record['target_name'],
                    path_type='cc_together',
                    connector_email=record['connector_email'],
                    connector_name=record['connector_name'],
                    connector_strength=connector_strength,
                    email_count=email_count,
                    last_contact=last_contact,
                    shared_cc_count=record['shared_cc_count'] or 0
                ))

        return results

    def get_relationship_strength(
        self,
        from_email: str,
        to_email: str
    ) -> int:
        """Calculate relationship strength between two people.

        Strength is calculated based on:
        - Number of emails exchanged
        - Recency of last contact

        Args:
            from_email: First person's email
            to_email: Second person's email

        Returns:
            Strength score from 0-10 (0 = no relationship)
        """
        query = '''
        MATCH (from:Person {primary_email: $from_email})-[r:KNOWS]->(to:Person {primary_email: $to_email})
        RETURN r.email_count as email_count, r.last_contact as last_contact
        '''

        with self.driver.session() as session:
            result = session.run(query, from_email=from_email, to_email=to_email)
            record = result.single()

            if not record:
                return 0

            email_count = record['email_count'] or 0
            last_contact = None
            if record['last_contact']:
                try:
                    last_contact = datetime.fromisoformat(record['last_contact'])
                except (ValueError, TypeError):
                    pass

            return self._calculate_strength(email_count, last_contact)

    def _calculate_strength(
        self,
        email_count: int,
        last_contact: Optional[datetime]
    ) -> int:
        """Calculate relationship strength score (1-10).

        Formula:
        - Base score from email count (1-7):
          - 1-5 emails: 1-2
          - 6-20 emails: 3-4
          - 21-50 emails: 5-6
          - 50+ emails: 7
        - Recency bonus (0-3):
          - Last 30 days: +3
          - Last 90 days: +2
          - Last 180 days: +1
          - Older: +0

        Args:
            email_count: Number of emails exchanged
            last_contact: Date of last email

        Returns:
            Strength score from 1-10
        """
        if email_count == 0:
            return 0

        # Base score from email count
        if email_count <= 5:
            base_score = min(2, email_count)
        elif email_count <= 20:
            base_score = 3 + (email_count - 5) // 8
        elif email_count <= 50:
            base_score = 5 + (email_count - 20) // 15
        else:
            base_score = 7

        # Recency bonus
        recency_bonus = 0
        if last_contact:
            # Handle timezone-aware datetime comparison
            now = datetime.now()
            if last_contact.tzinfo is not None:
                # Convert to naive datetime for comparison
                last_contact = last_contact.replace(tzinfo=None)
            days_since = (now - last_contact).days
            if days_since <= 30:
                recency_bonus = 3
            elif days_since <= 90:
                recency_bonus = 2
            elif days_since <= 180:
                recency_bonus = 1

        return min(10, base_score + recency_bonus)

    def find_all_paths(
        self,
        my_email: str,
        target_email: str
    ) -> List[PathResult]:
        """Find all possible paths to a target, prioritized.

        Checks in order:
        1. Direct connection
        2. One-hop paths
        3. CC-together connections
        4. Company connections (if we know target's domain)

        Args:
            my_email: Your email address
            target_email: Target prospect's email

        Returns:
            List of PathResult, sorted by path quality
        """
        results = []

        # 1. Check direct connection
        direct = self.find_direct_connection(my_email, target_email)
        if direct:
            results.append(direct)

        # 2. Find one-hop paths
        one_hop = self.find_one_hop_paths(my_email, target_email)
        results.extend(one_hop)

        # 3. Find CC-together connections
        cc_together = self.find_cc_together_connections(my_email, target_email)
        results.extend(cc_together)

        # 4. Check company connections if domain known
        if '@' in target_email:
            domain = target_email.split('@')[1]
            company = self.find_company_connections(my_email, domain)
            results.extend(company)

        return results


def main():
    """CLI entry point for graph queries testing."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Test graph queries for contact intelligence'
    )
    parser.add_argument('--my-email', default='tu@jaguarcapital.co',
                        help='Your email address')
    parser.add_argument('--target', required=True,
                        help='Target email to find paths to')
    parser.add_argument('--company', help='Find connections at company domain')

    args = parser.parse_args()

    from scripts.contact_intel.graph_builder import GraphBuilder

    gb = GraphBuilder()
    gb.connect()
    queries = GraphQueries(gb.driver)

    try:
        if args.company:
            logger.info(f"Finding connections at {args.company}...")
            results = queries.find_company_connections(args.my_email, args.company)
        else:
            logger.info(f"Finding all paths to {args.target}...")
            results = queries.find_all_paths(args.my_email, args.target)

        if not results:
            logger.info("No paths found.")
        else:
            logger.info(f"\nFound {len(results)} path(s):\n")
            for i, r in enumerate(results, 1):
                logger.info(f"[{i}] {r.path_type.upper()}")
                logger.info(f"    Target: {r.prospect_name} <{r.prospect_email}>")
                if r.connector_email:
                    logger.info(f"    Via: {r.connector_name} <{r.connector_email}>")
                    logger.info(f"    Connector strength: {r.connector_strength}/10")
                logger.info(f"    Email count: {r.email_count}")
                if r.last_contact:
                    logger.info(f"    Last contact: {r.last_contact.strftime('%Y-%m-%d')}")
                if r.shared_cc_count:
                    logger.info(f"    Shared CC: {r.shared_cc_count} times")
                logger.info("")

    finally:
        gb.close()


if __name__ == '__main__':
    main()
