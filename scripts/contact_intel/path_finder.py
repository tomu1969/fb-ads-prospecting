"""Find entry paths to prospects through your network.

Uses the contact intelligence graph to find the best way to reach
prospects - either direct connections, introductions via mutual contacts,
or company connections.

Usage:
    # Query single prospect
    python scripts/contact_intel/path_finder.py --query "chad@example.com"

    # Search by company
    python scripts/contact_intel/path_finder.py --company "nfx.com"

    # Process CSV of prospects
    python scripts/contact_intel/path_finder.py --input prospects.csv --output entry_paths.csv
"""

import argparse
import csv
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path for CLI execution
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

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


@dataclass
class EntryPath:
    """Entry path to a prospect.

    Represents the best way to reach a prospect, including
    connector details if an introduction is needed.

    Attributes:
        prospect_name: Name of the prospect
        prospect_email: Email of the prospect
        prospect_company: Company of the prospect (if known)
        path_type: Type of path - direct, one_hop, company_connection, cc_together, cold
        path_strength: Overall path quality score (0-100)
        connector_name: Name of the person who can intro (None for direct/cold)
        connector_email: Email of the connector
        connector_strength: Your relationship strength with connector (1-10)
        last_contact_date: Date of last email with connector (or prospect for direct)
        email_count: Emails exchanged with connector (or prospect for direct)
        suggested_opener: AI-generated intro request (future feature)
    """
    prospect_name: str
    prospect_email: str
    prospect_company: Optional[str]
    path_type: str  # direct, one_hop, company_connection, cc_together, cold
    path_strength: int  # 1-100
    connector_name: Optional[str]
    connector_email: Optional[str]
    connector_strength: int  # 1-10
    last_contact_date: Optional[str]
    email_count: int
    suggested_opener: Optional[str]


class PathFinder:
    """Find entry paths to prospects through your network.

    Searches the contact intelligence graph to find the best way
    to reach prospects - prioritizing direct connections, then
    strong mutual connections, then company connections.
    """

    def __init__(self, my_email: str = "tu@jaguarcapital.co"):
        """Initialize PathFinder.

        Args:
            my_email: Your email address (used as the starting point)
        """
        self.my_email = my_email
        self.gb = None
        self.queries = None

    def connect(self):
        """Connect to Neo4j and initialize queries."""
        from scripts.contact_intel.graph_builder import GraphBuilder
        from scripts.contact_intel.graph_queries import GraphQueries

        self.gb = GraphBuilder()
        self.gb.connect()
        self.queries = GraphQueries(self.gb.driver)
        logger.debug(f"Connected as {self.my_email}")

    def close(self):
        """Close Neo4j connection."""
        if self.gb:
            self.gb.close()
            self.gb = None
            self.queries = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def find_path(
        self,
        prospect_email: str,
        prospect_name: str = None,
        prospect_company: str = None
    ) -> EntryPath:
        """Find best entry path to a prospect.

        Checks in order of priority:
        1. Direct connection (you've emailed them)
        2. One-hop path (friend of friend)
        3. Company connection (you know someone at their company)
        4. CC-together connection (weak signal)
        5. Cold (no path found)

        Args:
            prospect_email: Email of the prospect to reach
            prospect_name: Name of the prospect (optional)
            prospect_company: Company of the prospect (optional)

        Returns:
            EntryPath with the best available path
        """
        if not self.queries:
            self.connect()

        # 1. Check direct connection
        direct = self.queries.find_direct_connection(self.my_email, prospect_email)
        if direct:
            return self._path_result_to_entry_path(
                direct,
                prospect_name=prospect_name,
                prospect_company=prospect_company
            )

        # 2. Check one-hop paths
        one_hop = self.queries.find_one_hop_paths(self.my_email, prospect_email, limit=1)
        if one_hop:
            return self._path_result_to_entry_path(
                one_hop[0],
                prospect_name=prospect_name,
                prospect_company=prospect_company
            )

        # 3. Check company connections (if we can determine company)
        domain = prospect_email.split('@')[1] if '@' in prospect_email else None
        if domain:
            company_conn = self.queries.find_company_connections(
                self.my_email, domain, limit=1
            )
            if company_conn:
                # Return the company connection as the suggested path
                entry = self._path_result_to_entry_path(
                    company_conn[0],
                    prospect_name=prospect_name,
                    prospect_company=prospect_company
                )
                # Update path type to reflect this is a company connection suggestion
                entry.path_type = 'company_connection'
                entry.connector_email = company_conn[0].prospect_email
                entry.connector_name = company_conn[0].prospect_name
                entry.connector_strength = company_conn[0].connector_strength
                return entry

        # 4. Check CC-together connections
        cc_together = self.queries.find_cc_together_connections(
            self.my_email, prospect_email, limit=1
        )
        if cc_together:
            return self._path_result_to_entry_path(
                cc_together[0],
                prospect_name=prospect_name,
                prospect_company=prospect_company
            )

        # 5. No path found - return cold
        return EntryPath(
            prospect_name=prospect_name or "Unknown",
            prospect_email=prospect_email,
            prospect_company=prospect_company,
            path_type='cold',
            path_strength=0,
            connector_name=None,
            connector_email=None,
            connector_strength=0,
            last_contact_date=None,
            email_count=0,
            suggested_opener=None
        )

    def _path_result_to_entry_path(
        self,
        path_result,
        prospect_name: str = None,
        prospect_company: str = None
    ) -> EntryPath:
        """Convert PathResult to EntryPath with scoring.

        Args:
            path_result: PathResult from GraphQueries
            prospect_name: Override name if provided
            prospect_company: Company name if known

        Returns:
            EntryPath with calculated path_strength
        """
        # Calculate path strength (0-100)
        path_strength = self._calculate_path_strength(path_result)

        # Format last contact date
        last_contact_str = None
        if path_result.last_contact:
            last_contact_str = path_result.last_contact.strftime('%Y-%m-%d')

        return EntryPath(
            prospect_name=prospect_name or path_result.prospect_name or "Unknown",
            prospect_email=path_result.prospect_email,
            prospect_company=prospect_company,
            path_type=path_result.path_type,
            path_strength=path_strength,
            connector_name=path_result.connector_name,
            connector_email=path_result.connector_email,
            connector_strength=path_result.connector_strength,
            last_contact_date=last_contact_str,
            email_count=path_result.email_count,
            suggested_opener=None
        )

    def _calculate_path_strength(self, path_result) -> int:
        """Calculate overall path strength (0-100).

        Scoring factors:
        - Path type multiplier:
          - direct: 100% of base
          - one_hop: 60% of base
          - company_connection: 40% of base
          - cc_together: 30% of base
        - Base score from connector strength and email count

        Args:
            path_result: PathResult to score

        Returns:
            Score from 0-100
        """
        # Type multipliers
        type_multipliers = {
            'direct': 1.0,
            'one_hop': 0.6,
            'company_connection': 0.4,
            'cc_together': 0.3,
            'cold': 0.0
        }

        multiplier = type_multipliers.get(path_result.path_type, 0.0)

        if path_result.path_type == 'direct':
            # For direct: base on email count and recency
            # Max 100 if 50+ emails and recent
            email_score = min(50, path_result.email_count) * 2  # Max 100
            base_score = email_score
        else:
            # For indirect: base on connector strength
            connector_score = path_result.connector_strength * 10  # 1-10 -> 10-100
            base_score = connector_score

        return int(base_score * multiplier)

    def find_paths_for_prospects(self, prospects: List[Dict]) -> List[EntryPath]:
        """Find paths for a list of prospects.

        Args:
            prospects: List of dicts with 'email' and optionally 'name', 'company'

        Returns:
            List of EntryPath for each prospect
        """
        results = []
        total = len(prospects)

        for i, prospect in enumerate(prospects, 1):
            email = prospect.get('email') or prospect.get('prospect_email')
            name = prospect.get('name') or prospect.get('prospect_name')
            company = prospect.get('company') or prospect.get('prospect_company')

            if not email:
                logger.warning(f"[{i}/{total}] Skipping prospect without email")
                continue

            logger.info(f"[{i}/{total}] Finding path to {email}...")
            entry_path = self.find_path(email, name, company)
            results.append(entry_path)

            logger.debug(f"  -> {entry_path.path_type} (strength: {entry_path.path_strength})")

        return results

    def process_csv(self, input_path: str, output_path: str):
        """Process prospects CSV and output entry paths.

        Args:
            input_path: Path to input CSV with prospects
            output_path: Path to output CSV with entry paths
        """
        # Read input CSV
        prospects = []
        with open(input_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                prospects.append(row)

        logger.info(f"Loaded {len(prospects)} prospects from {input_path}")

        # Find paths
        entry_paths = self.find_paths_for_prospects(prospects)

        # Write output CSV
        output_columns = [
            'prospect_name', 'prospect_email', 'prospect_company',
            'path_type', 'path_strength',
            'connector_name', 'connector_email', 'connector_strength',
            'last_contact_date', 'email_count', 'suggested_opener'
        ]

        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()

            for entry_path in entry_paths:
                writer.writerow(asdict(entry_path))

        logger.info(f"Wrote {len(entry_paths)} entry paths to {output_path}")

        # Summary
        by_type = {}
        for ep in entry_paths:
            by_type[ep.path_type] = by_type.get(ep.path_type, 0) + 1

        logger.info("\nPath type summary:")
        for path_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
            logger.info(f"  {path_type}: {count}")

    def find_company_connections(self, domain: str) -> List[EntryPath]:
        """Find all your connections at a company.

        Args:
            domain: Company domain (e.g., "acme.com")

        Returns:
            List of EntryPath for each connection at that company
        """
        if not self.queries:
            self.connect()

        results = self.queries.find_company_connections(self.my_email, domain)

        return [
            self._path_result_to_entry_path(r)
            for r in results
        ]


def main():
    """CLI entry point for path finder."""
    parser = argparse.ArgumentParser(
        description='Find entry paths to prospects through your network'
    )
    parser.add_argument('--my-email', default='tu@jaguarcapital.co',
                        help='Your email address')

    # Query modes
    query_group = parser.add_mutually_exclusive_group()
    query_group.add_argument('--query', '-q',
                             help='Email of prospect to find path to')
    query_group.add_argument('--company', '-c',
                             help='Find connections at company domain')
    query_group.add_argument('--input', '-i',
                             help='Input CSV with prospects')

    parser.add_argument('--output', '-o',
                        help='Output CSV path (required with --input)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    with PathFinder(my_email=args.my_email) as pf:
        if args.query:
            # Single prospect query
            entry_path = pf.find_path(args.query)

            print(f"\n=== Entry Path to {entry_path.prospect_email} ===\n")
            print(f"Path type: {entry_path.path_type.upper()}")
            print(f"Path strength: {entry_path.path_strength}/100")

            if entry_path.path_type == 'direct':
                print(f"\nYou have emailed them {entry_path.email_count} times")
                if entry_path.last_contact_date:
                    print(f"Last contact: {entry_path.last_contact_date}")
            elif entry_path.connector_email:
                print(f"\nConnector: {entry_path.connector_name or 'Unknown'}")
                print(f"Connector email: {entry_path.connector_email}")
                print(f"Connector strength: {entry_path.connector_strength}/10")
                print(f"Your emails with connector: {entry_path.email_count}")
                if entry_path.last_contact_date:
                    print(f"Last contact with connector: {entry_path.last_contact_date}")
            elif entry_path.path_type == 'cold':
                print("\nNo warm path found. This would be a cold outreach.")

        elif args.company:
            # Company connections
            connections = pf.find_company_connections(args.company)

            print(f"\n=== Your Connections at {args.company} ===\n")

            if not connections:
                print("No connections found at this company.")
            else:
                for i, conn in enumerate(connections, 1):
                    print(f"[{i}] {conn.prospect_name or 'Unknown'}")
                    print(f"    Email: {conn.prospect_email}")
                    print(f"    Emails exchanged: {conn.email_count}")
                    if conn.last_contact_date:
                        print(f"    Last contact: {conn.last_contact_date}")
                    print()

        elif args.input:
            # CSV processing
            if not args.output:
                output_path = args.input.replace('.csv', '_paths.csv')
            else:
                output_path = args.output

            pf.process_csv(args.input, output_path)

        else:
            parser.print_help()


if __name__ == '__main__':
    main()
