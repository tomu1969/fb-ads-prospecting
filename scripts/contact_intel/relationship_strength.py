"""Relationship strength scoring for KNOWS edges.

Analyzes email patterns to calculate relationship strength scores.
Updates KNOWS edges with strength_score, is_bidirectional, days_since_contact.

Usage:
    python -m scripts.contact_intel.relationship_strength
    python -m scripts.contact_intel.relationship_strength --status
"""

import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('relationship_strength.log'),
    ]
)
logger = logging.getLogger(__name__)

# My email address
MY_EMAIL = 'tu@jaguarcapital.co'


def calculate_strength_score(
    email_count: int,
    days_since_contact: int,
    is_bidirectional: bool,
) -> int:
    """Calculate relationship strength score (0-100).

    Formula:
    - base_score: min(email_count * 5, 40) - up to 40 points for volume
    - recency_score: max(0, 30 - (days/30)) - up to 30 points for recency
    - bidirectional_bonus: 30 if mutual exchange - 30 points

    Args:
        email_count: Total emails exchanged
        days_since_contact: Days since last email
        is_bidirectional: True if both parties have sent emails

    Returns:
        Strength score 0-100
    """
    # Volume score (0-40): 5 points per email, max 40
    base_score = min(email_count * 5, 40)

    # Recency score (0-30): Decays over 30 months (900 days)
    # Full 30 points if contacted today, 0 after 900 days
    recency_score = max(0, 30 - (days_since_contact / 30))

    # Bidirectional bonus (0 or 30)
    bidirectional_bonus = 30 if is_bidirectional else 0

    return int(base_score + recency_score + bidirectional_bonus)


def get_all_knows_edges(gb: GraphBuilder) -> List[Dict]:
    """Get all KNOWS edges with metadata.

    Returns:
        List of dicts with from_email, to_email, email_count, last_contact
    """
    with gb.driver.session() as session:
        result = session.run("""
            MATCH (a:Person)-[r:KNOWS]->(b:Person)
            RETURN a.primary_email as from_email,
                   b.primary_email as to_email,
                   r.email_count as email_count,
                   r.last_contact as last_contact,
                   r.first_contact as first_contact
        """)
        return [dict(record) for record in result]


def check_bidirectional(gb: GraphBuilder, email1: str, email2: str) -> bool:
    """Check if relationship is bidirectional (both have sent emails)."""
    with gb.driver.session() as session:
        result = session.run("""
            MATCH (a:Person {primary_email: $email1})-[:KNOWS]->(b:Person {primary_email: $email2})
            MATCH (b)-[:KNOWS]->(a)
            RETURN count(*) > 0 as is_bidirectional
        """, email1=email1, email2=email2)
        record = result.single()
        return record['is_bidirectional'] if record else False


def update_edge_strength(
    gb: GraphBuilder,
    from_email: str,
    to_email: str,
    strength_score: int,
    is_bidirectional: bool,
    days_since_contact: int,
):
    """Update KNOWS edge with strength metadata."""
    with gb.driver.session() as session:
        session.run("""
            MATCH (a:Person {primary_email: $from_email})-[r:KNOWS]->(b:Person {primary_email: $to_email})
            SET r.strength_score = $strength_score,
                r.is_bidirectional = $is_bidirectional,
                r.days_since_contact = $days_since_contact,
                r.strength_updated_at = datetime()
        """, from_email=from_email, to_email=to_email,
            strength_score=strength_score, is_bidirectional=is_bidirectional,
            days_since_contact=days_since_contact)


def run_strength_scoring() -> Dict:
    """Run relationship strength scoring on all KNOWS edges.

    Returns:
        Stats dict with total, updated, errors counts
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    gb = GraphBuilder()
    gb.connect()

    stats = {
        'total': 0,
        'updated': 0,
        'errors': 0,
        'avg_strength': 0,
        'bidirectional_count': 0,
    }

    try:
        # Get all KNOWS edges
        edges = get_all_knows_edges(gb)
        stats['total'] = len(edges)
        logger.info(f"Found {len(edges)} KNOWS edges to process")

        # Track bidirectional pairs we've already checked
        checked_pairs = set()
        strength_scores = []

        now = datetime.now(timezone.utc)

        for i, edge in enumerate(edges):
            try:
                from_email = edge['from_email']
                to_email = edge['to_email']
                email_count = edge['email_count'] or 1

                # Parse last_contact date
                last_contact = edge['last_contact']
                if last_contact:
                    if isinstance(last_contact, str):
                        # Handle ISO format string
                        last_dt = datetime.fromisoformat(last_contact.replace('Z', '+00:00'))
                    else:
                        last_dt = last_contact
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    days_since = (now - last_dt).days
                else:
                    days_since = 365  # Default to 1 year if no date

                # Check bidirectional (cache to avoid duplicate queries)
                pair_key = tuple(sorted([from_email, to_email]))
                if pair_key not in checked_pairs:
                    is_bidirectional = check_bidirectional(gb, from_email, to_email)
                    checked_pairs.add(pair_key)
                else:
                    # We already checked this pair from the other direction
                    is_bidirectional = True  # If we're seeing it again, it's bidirectional

                # Calculate strength
                strength = calculate_strength_score(email_count, days_since, is_bidirectional)
                strength_scores.append(strength)

                if is_bidirectional:
                    stats['bidirectional_count'] += 1

                # Update edge
                update_edge_strength(gb, from_email, to_email, strength, is_bidirectional, days_since)
                stats['updated'] += 1

                if (i + 1) % 500 == 0:
                    logger.info(f"Progress: {i + 1}/{len(edges)} edges processed")

            except Exception as e:
                logger.error(f"Error processing edge {from_email} -> {to_email}: {e}")
                stats['errors'] += 1

        if strength_scores:
            stats['avg_strength'] = sum(strength_scores) / len(strength_scores)

    finally:
        gb.close()

    logger.info(f"Strength scoring complete: {stats}")
    return stats


def show_status():
    """Show relationship strength status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count edges with strength scores
            result = session.run("""
                MATCH ()-[r:KNOWS]->()
                WHERE r.strength_score IS NOT NULL
                RETURN count(r) as scored,
                       avg(r.strength_score) as avg_strength,
                       max(r.strength_score) as max_strength,
                       min(r.strength_score) as min_strength
            """)
            scored_stats = result.single()

            # Count total edges
            result = session.run("""
                MATCH ()-[r:KNOWS]->()
                RETURN count(r) as total
            """)
            total = result.single()['total']

            # Count bidirectional
            result = session.run("""
                MATCH ()-[r:KNOWS]->()
                WHERE r.is_bidirectional = true
                RETURN count(r) as bidirectional
            """)
            bidirectional = result.single()['bidirectional']

            # Strength distribution
            result = session.run("""
                MATCH ()-[r:KNOWS]->()
                WHERE r.strength_score IS NOT NULL
                RETURN
                    sum(CASE WHEN r.strength_score >= 70 THEN 1 ELSE 0 END) as strong,
                    sum(CASE WHEN r.strength_score >= 40 AND r.strength_score < 70 THEN 1 ELSE 0 END) as medium,
                    sum(CASE WHEN r.strength_score < 40 THEN 1 ELSE 0 END) as weak
            """)
            dist = result.single()

        print("\n" + "=" * 50)
        print("RELATIONSHIP STRENGTH STATUS")
        print("=" * 50)
        print(f"Total KNOWS edges:        {total:,}")
        print(f"With strength scores:     {scored_stats['scored']:,}")
        print(f"Bidirectional:            {bidirectional:,}")
        if scored_stats['scored'] > 0:
            print(f"\nStrength Statistics:")
            print(f"  Average:                {scored_stats['avg_strength']:.1f}")
            print(f"  Max:                    {scored_stats['max_strength']}")
            print(f"  Min:                    {scored_stats['min_strength']}")
        if scored_stats['scored'] > 0:
            print(f"\nDistribution:")
            print(f"  Strong (70-100):        {dist['strong']:,}")
            print(f"  Medium (40-69):         {dist['medium']:,}")
            print(f"  Weak (0-39):            {dist['weak']:,}")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate relationship strength scores for KNOWS edges'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show strength scoring status')
    parser.add_argument('--run', action='store_true',
                        help='Run strength scoring')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.run:
        stats = run_strength_scoring()
        print("\n" + "=" * 50)
        print("RELATIONSHIP STRENGTH RESULTS")
        print("=" * 50)
        print(f"Total edges:              {stats.get('total', 0):,}")
        print(f"Updated:                  {stats.get('updated', 0):,}")
        print(f"Bidirectional:            {stats.get('bidirectional_count', 0):,}")
        print(f"Average strength:         {stats.get('avg_strength', 0):.1f}")
        print(f"Errors:                   {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
