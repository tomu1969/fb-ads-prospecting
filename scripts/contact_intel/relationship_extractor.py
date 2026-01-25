"""Relationship extraction from emails.

Detects:
- Introduction patterns in email body
- CC-based introductions (first time CC'd together)
- Relationship strength scoring

Usage:
    python scripts/contact_intel/relationship_extractor.py --scan          # Scan emails for intros
    python scripts/contact_intel/relationship_extractor.py --update-graph  # Update Neo4j with INTRODUCED edges
    python scripts/contact_intel/relationship_extractor.py --stats         # Show extraction stats
"""

import argparse
import json
import logging
import math
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('relationship_extractor.log'),
    ]
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "contact_intel"

# =============================================================================
# Introduction Detection Patterns
# =============================================================================

INTRO_PATTERNS = [
    r"i'd like to introduce you to",
    r"i want to introduce",
    r"connecting you with",
    r"you should meet",
    r"i think you two should connect",
    r"meet my friend",
    r"putting you in touch with",
    r"looping in",
    r"adding .* to this thread",
    r"let me introduce",
    r"pleased to introduce",
    r"introducing you to",
]

FORWARDED_INTRO_PATTERNS = [
    r"forwarding this intro",
    r"passing along this introduction",
    r"fwd:.*intro",
    r"re:.*introduction",
]

# Compile patterns for efficiency
COMPILED_INTRO_PATTERNS = [re.compile(p, re.IGNORECASE) for p in INTRO_PATTERNS]
COMPILED_FORWARDED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in FORWARDED_INTRO_PATTERNS]

# Pattern to extract name after introduction phrase
# Matches: "introduce you to John Smith", "introduce my friend Sarah"
NAME_EXTRACTION_PATTERNS = [
    r"introduce (?:you to|my friend|my colleague|)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"connecting you with\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"you should meet\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"looping in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"adding\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+to this thread",
    r"meet my friend\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"putting you in touch with\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"pleased to introduce\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"introducing you to\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    r"let me introduce\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
]

COMPILED_NAME_PATTERNS = [re.compile(p, re.IGNORECASE) for p in NAME_EXTRACTION_PATTERNS]


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class Introduction:
    """Represents a detected introduction between people."""
    introducer_email: str
    introduced_email: str
    introduced_to_email: str
    introduced_name: Optional[str]
    date: datetime
    context: str  # Subject or snippet
    confidence: float  # 0-1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "introducer_email": self.introducer_email,
            "introduced_email": self.introduced_email,
            "introduced_to_email": self.introduced_to_email,
            "introduced_name": self.introduced_name,
            "date": self.date.isoformat() if self.date else None,
            "context": self.context,
            "confidence": self.confidence,
        }


# =============================================================================
# Relationship Extractor
# =============================================================================

class RelationshipExtractor:
    """Extracts relationship information from emails.

    Detects introduction patterns, CC-based introductions, and calculates
    relationship strength scores.
    """

    def __init__(self, db_path: str):
        """Initialize the extractor.

        Args:
            db_path: Path to SQLite database with emails.
        """
        self.db_path = db_path
        self._introductions: List[Introduction] = []
        self._conn: Optional[sqlite3.Connection] = None

    def init_db(self):
        """Initialize or verify database schema for relationship tracking.

        Creates additional tables for tracking CC pairs if they don't exist.
        """
        conn = self._get_connection()

        # Table to track which email pairs have been CC'd together
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cc_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email1 TEXT NOT NULL,
                email2 TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL,
                last_seen TIMESTAMP NOT NULL,
                cc_count INTEGER DEFAULT 1,
                UNIQUE(email1, email2)
            )
        """)

        # Table to store detected introductions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS introductions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                introducer_email TEXT NOT NULL,
                introduced_email TEXT NOT NULL,
                introduced_to_email TEXT NOT NULL,
                introduced_name TEXT,
                date TIMESTAMP NOT NULL,
                context TEXT,
                confidence REAL,
                source TEXT,  -- 'body_pattern' or 'cc_detection'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(introducer_email, introduced_email, introduced_to_email, date)
            )
        """)

        # Index for efficient lookups
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cc_pairs_emails
            ON cc_pairs(email1, email2)
        """)

        conn.commit()
        logger.debug("Database schema initialized for relationship extraction")

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection, creating if needed."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    # =========================================================================
    # Intro Detection in Text
    # =========================================================================

    def detect_intro_in_text(self, text: str) -> Tuple[bool, Optional[str]]:
        """Detect introduction patterns in text.

        Args:
            text: Email body or subject text.

        Returns:
            Tuple of (is_intro, introduced_name).
            introduced_name may be None even if is_intro is True.
        """
        if not text:
            return (False, None)

        # Check main intro patterns
        for pattern in COMPILED_INTRO_PATTERNS:
            if pattern.search(text):
                # Try to extract the introduced person's name
                name = self._extract_introduced_name(text)
                return (True, name)

        # Check forwarded intro patterns
        for pattern in COMPILED_FORWARDED_PATTERNS:
            if pattern.search(text):
                # Forwarded intros may not have extractable names
                return (True, None)

        return (False, None)

    def _extract_introduced_name(self, text: str) -> Optional[str]:
        """Extract the name of the person being introduced.

        Args:
            text: Text containing introduction phrase.

        Returns:
            Name if found, None otherwise.
        """
        for pattern in COMPILED_NAME_PATTERNS:
            match = pattern.search(text)
            if match:
                name = match.group(1).strip()
                # Clean up the name - remove trailing punctuation
                name = re.sub(r'[,\.\!]+$', '', name)
                if name and len(name) > 1:
                    return name

        # Fallback: try to find capitalized words after intro phrases
        for intro_pattern in INTRO_PATTERNS:
            compiled = re.compile(intro_pattern + r'\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', re.IGNORECASE)
            match = compiled.search(text)
            if match:
                name = match.group(1).strip()
                name = re.sub(r'[,\.\!]+$', '', name)
                if name and len(name) > 1:
                    return name

        return None

    # =========================================================================
    # CC-Based Introduction Detection
    # =========================================================================

    def detect_cc_introduction(
        self,
        from_email: str,
        to_emails: List[str],
        cc_emails: List[str],
        date: datetime,
    ) -> List[Introduction]:
        """Detect if this email represents a CC-based introduction.

        An introduction is detected when:
        - Person A emails Person B
        - Person C is CC'd
        - Person C has never been on an email with Person B before

        Args:
            from_email: Sender's email.
            to_emails: List of To recipients.
            cc_emails: List of CC recipients.
            date: Email date.

        Returns:
            List of detected Introduction objects.
        """
        introductions = []

        if not cc_emails:
            return introductions

        conn = self._get_connection()

        # For each CC'd person, check if they've been in contact with To recipients before
        for cc_email in cc_emails:
            cc_email_lower = cc_email.lower()

            for to_email in to_emails:
                to_email_lower = to_email.lower()

                # Normalize email pair (alphabetical order)
                email1, email2 = sorted([cc_email_lower, to_email_lower])

                # Check if this pair has been CC'd together before
                cursor = conn.execute("""
                    SELECT cc_count FROM cc_pairs
                    WHERE email1 = ? AND email2 = ?
                """, (email1, email2))

                row = cursor.fetchone()

                if row is None:
                    # First time these two are on the same email - potential introduction
                    intro = Introduction(
                        introducer_email=from_email.lower(),
                        introduced_email=cc_email_lower,
                        introduced_to_email=to_email_lower,
                        introduced_name=None,  # Could try to extract from email
                        date=date,
                        context=f"CC introduction from {from_email}",
                        confidence=0.7,  # Medium confidence for CC detection
                    )
                    introductions.append(intro)

        return introductions

    def _record_email_interaction(
        self,
        from_email: str,
        to_emails: List[str],
        cc_emails: List[str],
        date: datetime,
    ):
        """Record email interaction in cc_pairs table.

        Tracks all pairs of people who appear on the same email.

        Args:
            from_email: Sender email.
            to_emails: To recipients.
            cc_emails: CC recipients.
            date: Email date.
        """
        conn = self._get_connection()

        # All participants in this email
        all_emails = [from_email.lower()] + [e.lower() for e in to_emails] + [e.lower() for e in cc_emails]
        all_emails = list(set(all_emails))  # Remove duplicates

        # Record each pair
        for email1, email2 in combinations(all_emails, 2):
            # Normalize order
            email1, email2 = sorted([email1, email2])
            date_str = date.isoformat()

            conn.execute("""
                INSERT INTO cc_pairs (email1, email2, first_seen, last_seen, cc_count)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(email1, email2) DO UPDATE SET
                    cc_count = cc_count + 1,
                    last_seen = MAX(last_seen, excluded.last_seen)
            """, (email1, email2, date_str, date_str))

        conn.commit()

    # =========================================================================
    # Relationship Strength Calculation
    # =========================================================================

    def calculate_relationship_strength(
        self,
        email_count: int,
        last_contact: datetime,
        first_contact: datetime,
    ) -> int:
        """Calculate relationship strength on a 1-10 scale.

        Factors:
        - Email frequency (more emails = stronger)
        - Recency (recent contact = stronger)
        - Duration (longer relationship = bonus)

        Args:
            email_count: Number of emails exchanged.
            last_contact: Date of most recent email.
            first_contact: Date of first email.

        Returns:
            Strength score from 1 to 10.
        """
        now = datetime.now()

        # Frequency score (0-4 points)
        # 1-5 emails: 1 point, 6-15: 2 points, 16-30: 3 points, 30+: 4 points
        if email_count <= 5:
            freq_score = 1
        elif email_count <= 15:
            freq_score = 2
        elif email_count <= 30:
            freq_score = 3
        else:
            freq_score = 4

        # Recency score (0-4 points)
        # Last contact within: 7 days: 4, 30 days: 3, 90 days: 2, 180 days: 1, older: 0
        days_since_contact = (now - last_contact).days

        if days_since_contact <= 7:
            recency_score = 4
        elif days_since_contact <= 30:
            recency_score = 3
        elif days_since_contact <= 90:
            recency_score = 2
        elif days_since_contact <= 180:
            recency_score = 1
        else:
            recency_score = 0

        # Duration bonus (0-2 points)
        # Relationship > 6 months: 1 point, > 1 year: 2 points
        relationship_days = (last_contact - first_contact).days

        if relationship_days >= 365:
            duration_bonus = 2
        elif relationship_days >= 180:
            duration_bonus = 1
        else:
            duration_bonus = 0

        # Calculate total (max 10)
        total = freq_score + recency_score + duration_bonus

        # Ensure in range 1-10
        return max(1, min(10, total))

    # =========================================================================
    # Batch Processing
    # =========================================================================

    def extract_introductions_from_db(self, emails_db_path: str = None) -> List[Introduction]:
        """Scan all emails in DB and extract introductions.

        Args:
            emails_db_path: Path to emails.db (defaults to standard location).

        Returns:
            List of detected Introduction objects.
        """
        if emails_db_path is None:
            emails_db_path = str(DATA_DIR / "emails.db")

        if not Path(emails_db_path).exists():
            logger.warning(f"Emails database not found: {emails_db_path}")
            return []

        # Connect to emails database
        emails_conn = sqlite3.connect(emails_db_path)
        emails_conn.row_factory = sqlite3.Row

        # Initialize our tracking database
        self.init_db()

        cursor = emails_conn.execute("""
            SELECT id, from_email, from_name, to_emails, cc_emails, subject, date
            FROM emails
            ORDER BY date ASC
        """)

        introductions = []
        processed = 0

        for row in cursor:
            processed += 1

            # Parse JSON lists
            try:
                to_emails = json.loads(row['to_emails']) if row['to_emails'] else []
                cc_emails = json.loads(row['cc_emails']) if row['cc_emails'] else []
            except json.JSONDecodeError:
                to_emails = []
                cc_emails = []

            # Parse date
            email_date = None
            if row['date']:
                try:
                    email_date = datetime.fromisoformat(row['date'].replace('Z', '+00:00'))
                except ValueError:
                    email_date = datetime.now()

            if email_date is None:
                email_date = datetime.now()

            from_email = row['from_email']
            subject = row['subject'] or ""

            # Check for body-based introduction patterns
            # Note: We don't have email body in the current schema
            # This would need to be added to the emails table
            # For now, we check the subject line
            is_intro, introduced_name = self.detect_intro_in_text(subject)

            if is_intro and cc_emails:
                # Body/subject mentions introduction + there are CC'd people
                for cc_email in cc_emails:
                    intro = Introduction(
                        introducer_email=from_email.lower(),
                        introduced_email=cc_email.lower(),
                        introduced_to_email=to_emails[0].lower() if to_emails else from_email.lower(),
                        introduced_name=introduced_name,
                        date=email_date,
                        context=subject,
                        confidence=0.85,
                    )
                    introductions.append(intro)

            # Check for CC-based introductions
            cc_intros = self.detect_cc_introduction(
                from_email=from_email,
                to_emails=to_emails,
                cc_emails=cc_emails,
                date=email_date,
            )
            introductions.extend(cc_intros)

            # Record this interaction
            self._record_email_interaction(
                from_email=from_email,
                to_emails=to_emails,
                cc_emails=cc_emails,
                date=email_date,
            )

            if processed % 100 == 0:
                logger.info(f"[{processed}] Processed emails, found {len(introductions)} introductions")

        emails_conn.close()

        self._introductions = introductions
        logger.info(f"Extraction complete: {processed} emails, {len(introductions)} introductions detected")

        return introductions

    def save_introductions_to_db(self, introductions: List[Introduction] = None):
        """Save detected introductions to database.

        Args:
            introductions: List of introductions to save. Uses self._introductions if None.
        """
        if introductions is None:
            introductions = self._introductions

        conn = self._get_connection()

        saved = 0
        for intro in introductions:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO introductions
                    (introducer_email, introduced_email, introduced_to_email,
                     introduced_name, date, context, confidence, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    intro.introducer_email,
                    intro.introduced_email,
                    intro.introduced_to_email,
                    intro.introduced_name,
                    intro.date.isoformat(),
                    intro.context,
                    intro.confidence,
                    'body_pattern' if intro.confidence > 0.8 else 'cc_detection',
                ))
                saved += 1
            except sqlite3.IntegrityError:
                pass  # Duplicate

        conn.commit()
        logger.info(f"Saved {saved} introductions to database")

    # =========================================================================
    # Graph Updates
    # =========================================================================

    def update_graph_with_introductions(self, graph_builder):
        """Add INTRODUCED relationships to Neo4j graph.

        Args:
            graph_builder: GraphBuilder instance with active connection.
        """
        introductions = self._introductions

        if not introductions:
            # Load from database if not in memory
            conn = self._get_connection()
            cursor = conn.execute("""
                SELECT introducer_email, introduced_email, introduced_to_email,
                       introduced_name, date, context, confidence
                FROM introductions
            """)

            introductions = []
            for row in cursor:
                intro = Introduction(
                    introducer_email=row['introducer_email'],
                    introduced_email=row['introduced_email'],
                    introduced_to_email=row['introduced_to_email'],
                    introduced_name=row['introduced_name'],
                    date=datetime.fromisoformat(row['date']) if row['date'] else datetime.now(),
                    context=row['context'],
                    confidence=row['confidence'],
                )
                introductions.append(intro)

        logger.info(f"Updating graph with {len(introductions)} introductions")

        for intro in introductions:
            try:
                # Ensure all three people exist in the graph
                graph_builder.create_or_update_person(email=intro.introducer_email)
                graph_builder.create_or_update_person(
                    email=intro.introduced_email,
                    name=intro.introduced_name,
                )
                graph_builder.create_or_update_person(email=intro.introduced_to_email)

                # Create INTRODUCED relationship
                self._create_introduced_relationship(
                    graph_builder,
                    introducer=intro.introducer_email,
                    introduced=intro.introduced_email,
                    introduced_to=intro.introduced_to_email,
                    date=intro.date,
                    context=intro.context,
                    confidence=intro.confidence,
                )

            except Exception as e:
                logger.error(f"Error updating graph for intro: {e}")

    def _create_introduced_relationship(
        self,
        graph_builder,
        introducer: str,
        introduced: str,
        introduced_to: str,
        date: datetime,
        context: str,
        confidence: float,
    ):
        """Create INTRODUCED relationship in Neo4j.

        Creates two edges:
        - (introducer)-[:INTRODUCED]->(introduced)
        - (introduced)-[:INTRODUCED_TO]->(introduced_to)

        Args:
            graph_builder: GraphBuilder with active connection.
            introducer: Email of person making introduction.
            introduced: Email of person being introduced.
            introduced_to: Email of person they're being introduced to.
            date: Date of introduction.
            context: Context/subject of introduction.
            confidence: Confidence score (0-1).
        """
        # Using raw Neo4j session from graph_builder
        if not hasattr(graph_builder, 'driver') or graph_builder.driver is None:
            logger.warning("Graph builder not connected")
            return

        date_str = date.isoformat()

        with graph_builder.driver.session() as session:
            # Create INTRODUCED relationship
            session.run("""
                MATCH (introducer:Person {primary_email: $introducer})
                MATCH (introduced:Person {primary_email: $introduced})
                MERGE (introducer)-[r:INTRODUCED]->(introduced)
                ON CREATE SET
                    r.date = $date,
                    r.context = $context,
                    r.confidence = $confidence,
                    r.created_at = datetime()
                ON MATCH SET
                    r.date = CASE WHEN r.date > $date THEN r.date ELSE $date END
            """, introducer=introducer, introduced=introduced, date=date_str,
                context=context, confidence=confidence)

            # Create INTRODUCED_TO relationship (the connection made)
            session.run("""
                MATCH (introduced:Person {primary_email: $introduced})
                MATCH (introduced_to:Person {primary_email: $introduced_to})
                MERGE (introduced)-[r:INTRODUCED_TO]->(introduced_to)
                ON CREATE SET
                    r.via = $introducer,
                    r.date = $date,
                    r.context = $context,
                    r.confidence = $confidence,
                    r.created_at = datetime()
            """, introduced=introduced, introduced_to=introduced_to, introducer=introducer,
                date=date_str, context=context, confidence=confidence)

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics.

        Returns:
            Dict with statistics about detected relationships.
        """
        conn = self._get_connection()

        stats = {}

        # Introduction counts
        cursor = conn.execute("SELECT COUNT(*) FROM introductions")
        row = cursor.fetchone()
        stats['total_introductions'] = row[0] if row else 0

        # By source
        cursor = conn.execute("""
            SELECT source, COUNT(*) as count
            FROM introductions
            GROUP BY source
        """)
        stats['by_source'] = dict(cursor.fetchall())

        # CC pairs count
        cursor = conn.execute("SELECT COUNT(*) FROM cc_pairs")
        row = cursor.fetchone()
        stats['unique_cc_pairs'] = row[0] if row else 0

        # Top introducers
        cursor = conn.execute("""
            SELECT introducer_email, COUNT(*) as count
            FROM introductions
            GROUP BY introducer_email
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_introducers'] = [
            {"email": row[0], "count": row[1]}
            for row in cursor.fetchall()
        ]

        return stats


# =============================================================================
# CLI Interface
# =============================================================================

def parse_args(args=None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract relationship information from emails.'
    )
    parser.add_argument(
        '--scan',
        action='store_true',
        help='Scan emails for introductions',
    )
    parser.add_argument(
        '--update-graph',
        action='store_true',
        help='Update Neo4j with INTRODUCED edges',
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show extraction statistics',
    )
    parser.add_argument(
        '--emails-db',
        type=str,
        default=str(DATA_DIR / "emails.db"),
        help='Path to emails database',
    )
    parser.add_argument(
        '--output-db',
        type=str,
        default=str(DATA_DIR / "relationships.db"),
        help='Path to output relationships database',
    )
    return parser.parse_args(args)


def main():
    """Main CLI entry point."""
    args = parse_args()

    extractor = RelationshipExtractor(args.output_db)
    extractor.init_db()

    if args.scan:
        logger.info("Scanning emails for introductions...")
        intros = extractor.extract_introductions_from_db(args.emails_db)
        extractor.save_introductions_to_db(intros)

        print(f"\nScan Complete")
        print("=" * 50)
        print(f"  Introductions detected: {len(intros)}")

        if intros:
            print("\nSample introductions:")
            for intro in intros[:5]:
                print(f"  - {intro.introducer_email} introduced {intro.introduced_email}")
                print(f"    to {intro.introduced_to_email}")
                print(f"    Context: {intro.context[:50]}...")
                print()

    elif args.update_graph:
        logger.info("Updating Neo4j graph with introductions...")

        from .graph_builder import GraphBuilder, neo4j_available

        if not neo4j_available():
            logger.error("Neo4j is not available. Start Neo4j and set credentials in .env")
            return

        graph = GraphBuilder()
        graph.connect()

        extractor.update_graph_with_introductions(graph)

        graph.close()
        logger.info("Graph update complete")

    elif args.stats:
        logger.info("Fetching extraction statistics...")
        stats = extractor.get_stats()

        print("\nRelationship Extraction Statistics")
        print("=" * 50)
        print(f"  Total introductions: {stats.get('total_introductions', 0):,}")
        print(f"  Unique CC pairs: {stats.get('unique_cc_pairs', 0):,}")
        print(f"\nBy source:")
        for source, count in stats.get('by_source', {}).items():
            print(f"    {source}: {count}")

        if stats.get('top_introducers'):
            print(f"\nTop introducers:")
            for i, intro in enumerate(stats['top_introducers'][:5], 1):
                print(f"    {i}. {intro['email']}: {intro['count']} introductions")

    else:
        parse_args(['--help'])

    extractor.close()


if __name__ == '__main__':
    main()
