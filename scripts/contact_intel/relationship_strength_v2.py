"""Improved relationship strength scoring (V2).

Addresses limitations of V1:
- Penalizes group emails (many recipients)
- Rewards actual reciprocity (back-and-forth)
- Detects newsletters (one-way, no replies)
- Uses logarithmic scaling for email count
- Exponential decay for recency

Usage:
    python -m scripts.contact_intel.relationship_strength_v2 --run
    python -m scripts.contact_intel.relationship_strength_v2 --status
    python -m scripts.contact_intel.relationship_strength_v2 --check "email@example.com"
"""

import argparse
import json
import logging
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scripts.contact_intel.config import DATA_DIR
from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('relationship_strength_v2.log'),
    ]
)
logger = logging.getLogger(__name__)

# Database path
EMAILS_DB = DATA_DIR / "emails.db"

# My email addresses (to identify sent vs received)
MY_EMAILS = {
    'tu@jaguarcapital.co',
    'tu@jaguar.la',
    'tomasuribe@gmail.com',
}


def get_email_stats_from_sqlite() -> Dict[str, Dict]:
    """Extract email statistics from SQLite database.

    Returns:
        Dict mapping email -> {
            'emails_sent': count of emails I sent to them,
            'emails_received': count of emails they sent to me,
            'total_recipients_when_sent': sum of recipients when I emailed them,
            'total_recipients_when_received': sum of recipients when they emailed me,
            'replies_received': count of their emails that were replies to mine,
            'first_contact': earliest email date,
            'last_contact': most recent email date,
        }
    """
    if not EMAILS_DB.exists():
        logger.error(f"Email database not found: {EMAILS_DB}")
        return {}

    conn = sqlite3.connect(EMAILS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all emails
    cursor.execute("""
        SELECT from_email, from_name, to_emails, cc_emails, date, in_reply_to, message_id
        FROM emails
        WHERE from_email IS NOT NULL
        ORDER BY date
    """)

    stats = defaultdict(lambda: {
        'emails_sent': 0,
        'emails_received': 0,
        'total_recipients_when_sent': 0,
        'total_recipients_when_received': 0,
        'replies_received': 0,
        'first_contact': None,
        'last_contact': None,
        'name': None,
    })

    # Track message IDs for reply detection
    my_message_ids = set()

    for row in cursor:
        from_email = row['from_email'].lower() if row['from_email'] else None
        if not from_email:
            continue

        from_name = row['from_name']
        email_date = row['date']
        in_reply_to = row['in_reply_to']
        message_id = row['message_id']

        # Parse recipients
        try:
            to_list = json.loads(row['to_emails']) if row['to_emails'] else []
            cc_list = json.loads(row['cc_emails']) if row['cc_emails'] else []
        except json.JSONDecodeError:
            to_list = []
            cc_list = []

        # Extract just email addresses from recipient lists
        to_emails = set()
        for r in to_list:
            if isinstance(r, (list, tuple)) and len(r) >= 2:
                to_emails.add(r[1].lower())
            elif isinstance(r, str):
                to_emails.add(r.lower())

        cc_emails = set()
        for r in cc_list:
            if isinstance(r, (list, tuple)) and len(r) >= 2:
                cc_emails.add(r[1].lower())
            elif isinstance(r, str):
                cc_emails.add(r.lower())

        all_recipients = to_emails | cc_emails
        recipient_count = len(all_recipients)

        # Check if this is from me
        is_from_me = from_email in MY_EMAILS

        if is_from_me:
            # I sent this email
            if message_id:
                my_message_ids.add(message_id)

            for recipient in all_recipients:
                if recipient not in MY_EMAILS:
                    stats[recipient]['emails_sent'] += 1
                    stats[recipient]['total_recipients_when_sent'] += recipient_count

                    # Update contact dates
                    if stats[recipient]['first_contact'] is None:
                        stats[recipient]['first_contact'] = email_date
                    stats[recipient]['last_contact'] = email_date
        else:
            # I received this email
            stats[from_email]['emails_received'] += 1
            stats[from_email]['total_recipients_when_received'] += recipient_count

            if from_name and not stats[from_email]['name']:
                stats[from_email]['name'] = from_name

            # Check if this is a reply to my email
            if in_reply_to and in_reply_to in my_message_ids:
                stats[from_email]['replies_received'] += 1

            # Update contact dates
            if stats[from_email]['first_contact'] is None:
                stats[from_email]['first_contact'] = email_date
            stats[from_email]['last_contact'] = email_date

    conn.close()

    logger.info(f"Extracted stats for {len(stats)} contacts from SQLite")
    return dict(stats)


def calculate_strength_v2(
    emails_sent: int,
    emails_received: int,
    avg_recipients_sent: float,
    avg_recipients_received: float,
    days_since_contact: int,
    reply_rate: float,
) -> Tuple[int, Dict]:
    """Calculate relationship strength score V2.

    Args:
        emails_sent: Number of emails I sent to them
        emails_received: Number of emails they sent to me
        avg_recipients_sent: Average recipients when I emailed them
        avg_recipients_received: Average recipients when they emailed me
        days_since_contact: Days since last email
        reply_rate: Fraction of my emails they replied to (0-1)

    Returns:
        Tuple of (score, breakdown_dict)
    """
    total_emails = emails_sent + emails_received

    # 1. Volume score (0-35) - logarithmic scaling
    # 1 email = 5, 10 emails = 18, 50 emails = 27, 200 emails = 35
    if total_emails > 0:
        volume_score = min(35, 5 + 10 * math.log10(total_emails + 1))
    else:
        volume_score = 0

    # 2. Recency score (0-25) - exponential decay
    # Full 25 for <30 days, ~20 at 90 days, ~12 at 1 year, ~5 at 2 years
    if days_since_contact is not None:
        recency_score = 25 * math.exp(-days_since_contact / 365)
    else:
        recency_score = 0

    # 3. Reciprocity score (0-25) - reward actual back-and-forth
    # Max when sent == received, lower when one-sided
    if emails_sent > 0 and emails_received > 0:
        balance = min(emails_sent, emails_received) / max(emails_sent, emails_received)
        reciprocity_score = 25 * balance
    elif emails_sent > 0 or emails_received > 0:
        # One-sided relationship
        reciprocity_score = 5  # Small credit for any contact
    else:
        reciprocity_score = 0

    # 4. Reply bonus (0-15) - did they actually reply to my emails?
    reply_bonus = 15 * reply_rate

    # Calculate base score
    base_score = volume_score + recency_score + reciprocity_score + reply_bonus

    # 5. Group email penalty (multiplier 0.5 - 1.0)
    # Use average of sent and received recipient counts
    avg_recipients = (avg_recipients_sent + avg_recipients_received) / 2 if (avg_recipients_sent + avg_recipients_received) > 0 else 1

    if avg_recipients > 20:
        group_multiplier = 0.5
    elif avg_recipients > 10:
        group_multiplier = 0.7
    elif avg_recipients > 5:
        group_multiplier = 0.85
    else:
        group_multiplier = 1.0

    # 6. Newsletter penalty
    # If they send me emails but never reply to mine, likely newsletter
    newsletter_penalty = 1.0
    if emails_received >= 3 and emails_sent >= 2 and reply_rate == 0:
        newsletter_penalty = 0.7  # 30% penalty
    elif emails_received >= 5 and emails_sent == 0:
        # Pure incoming, no outgoing - could be newsletter
        newsletter_penalty = 0.5

    final_score = int(base_score * group_multiplier * newsletter_penalty)
    final_score = max(0, min(100, final_score))  # Clamp to 0-100

    breakdown = {
        'volume': round(volume_score, 1),
        'recency': round(recency_score, 1),
        'reciprocity': round(reciprocity_score, 1),
        'reply_bonus': round(reply_bonus, 1),
        'group_multiplier': group_multiplier,
        'newsletter_penalty': newsletter_penalty,
        'avg_recipients': round(avg_recipients, 1),
    }

    return final_score, breakdown


def run_strength_scoring_v2() -> Dict:
    """Run V2 strength scoring on all contacts.

    Returns:
        Stats dict
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    # Get email stats from SQLite
    email_stats = get_email_stats_from_sqlite()
    if not email_stats:
        return {'error': 'No email stats found'}

    gb = GraphBuilder()
    gb.connect()

    stats = {
        'total': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0,
        'avg_score': 0,
        'score_distribution': {
            'strong_70_100': 0,
            'medium_40_69': 0,
            'weak_10_39': 0,
            'minimal_0_9': 0,
        }
    }

    now = datetime.now(timezone.utc)
    scores = []

    try:
        with gb.driver.session() as session:
            # Get all KNOWS edges from me
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person)
                RETURN p.primary_email as email, r.email_count as old_count
            """)
            edges = [dict(record) for record in result]

        stats['total'] = len(edges)
        logger.info(f"Processing {len(edges)} KNOWS edges")

        for i, edge in enumerate(edges):
            contact_email = edge['email']

            if contact_email not in email_stats:
                stats['skipped'] += 1
                continue

            try:
                es = email_stats[contact_email]

                # Calculate averages
                avg_recipients_sent = es['total_recipients_when_sent'] / es['emails_sent'] if es['emails_sent'] > 0 else 0
                avg_recipients_received = es['total_recipients_when_received'] / es['emails_received'] if es['emails_received'] > 0 else 0

                # Calculate days since contact
                if es['last_contact']:
                    try:
                        last_dt = datetime.fromisoformat(es['last_contact'].replace('Z', '+00:00'))
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=timezone.utc)
                        days_since = (now - last_dt).days
                    except:
                        days_since = 365
                else:
                    days_since = 365

                # Calculate reply rate
                reply_rate = es['replies_received'] / es['emails_sent'] if es['emails_sent'] > 0 else 0
                reply_rate = min(1.0, reply_rate)  # Cap at 1.0

                # Calculate V2 score
                score, breakdown = calculate_strength_v2(
                    emails_sent=es['emails_sent'],
                    emails_received=es['emails_received'],
                    avg_recipients_sent=avg_recipients_sent,
                    avg_recipients_received=avg_recipients_received,
                    days_since_contact=days_since,
                    reply_rate=reply_rate,
                )

                scores.append(score)

                # Update distribution
                if score >= 70:
                    stats['score_distribution']['strong_70_100'] += 1
                elif score >= 40:
                    stats['score_distribution']['medium_40_69'] += 1
                elif score >= 10:
                    stats['score_distribution']['weak_10_39'] += 1
                else:
                    stats['score_distribution']['minimal_0_9'] += 1

                # Update Neo4j
                with gb.driver.session() as session:
                    session.run("""
                        MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person {primary_email: $email})
                        SET r.strength_score_v2 = $score,
                            r.emails_sent = $emails_sent,
                            r.emails_received = $emails_received,
                            r.avg_recipients = $avg_recipients,
                            r.reply_rate = $reply_rate,
                            r.group_multiplier = $group_mult,
                            r.score_breakdown = $breakdown,
                            r.v2_updated_at = datetime()
                    """,
                        email=contact_email,
                        score=score,
                        emails_sent=es['emails_sent'],
                        emails_received=es['emails_received'],
                        avg_recipients=round((avg_recipients_sent + avg_recipients_received) / 2, 1),
                        reply_rate=round(reply_rate, 2),
                        group_mult=breakdown['group_multiplier'],
                        breakdown=json.dumps(breakdown),
                    )

                stats['updated'] += 1

                if (i + 1) % 1000 == 0:
                    logger.info(f"Progress: {i + 1}/{len(edges)}")

            except Exception as e:
                logger.error(f"Error processing {contact_email}: {e}")
                stats['errors'] += 1

        if scores:
            stats['avg_score'] = sum(scores) / len(scores)

    finally:
        gb.close()

    logger.info(f"V2 scoring complete: {stats}")
    return stats


def check_contact(email: str):
    """Show detailed scoring breakdown for a specific contact."""
    email_stats = get_email_stats_from_sqlite()

    if email not in email_stats:
        print(f"Contact not found: {email}")
        return

    es = email_stats[email]

    # Calculate metrics
    avg_recipients_sent = es['total_recipients_when_sent'] / es['emails_sent'] if es['emails_sent'] > 0 else 0
    avg_recipients_received = es['total_recipients_when_received'] / es['emails_received'] if es['emails_received'] > 0 else 0

    now = datetime.now(timezone.utc)
    if es['last_contact']:
        try:
            last_dt = datetime.fromisoformat(es['last_contact'].replace('Z', '+00:00'))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            days_since = (now - last_dt).days
        except:
            days_since = 365
    else:
        days_since = 365

    reply_rate = es['replies_received'] / es['emails_sent'] if es['emails_sent'] > 0 else 0
    reply_rate = min(1.0, reply_rate)

    score, breakdown = calculate_strength_v2(
        emails_sent=es['emails_sent'],
        emails_received=es['emails_received'],
        avg_recipients_sent=avg_recipients_sent,
        avg_recipients_received=avg_recipients_received,
        days_since_contact=days_since,
        reply_rate=reply_rate,
    )

    print(f"\n{'='*50}")
    print(f"CONTACT: {email}")
    print(f"Name: {es['name'] or 'Unknown'}")
    print(f"{'='*50}")
    print(f"\nRaw Stats:")
    print(f"  Emails I sent to them:     {es['emails_sent']}")
    print(f"  Emails they sent to me:    {es['emails_received']}")
    print(f"  Avg recipients (sent):     {avg_recipients_sent:.1f}")
    print(f"  Avg recipients (received): {avg_recipients_received:.1f}")
    print(f"  Their replies to me:       {es['replies_received']}")
    print(f"  Reply rate:                {reply_rate:.1%}")
    print(f"  Days since last contact:   {days_since}")
    print(f"  First contact:             {es['first_contact'][:10] if es['first_contact'] else 'N/A'}")
    print(f"  Last contact:              {es['last_contact'][:10] if es['last_contact'] else 'N/A'}")

    print(f"\nScore Breakdown:")
    print(f"  Volume (0-35):             {breakdown['volume']}")
    print(f"  Recency (0-25):            {breakdown['recency']}")
    print(f"  Reciprocity (0-25):        {breakdown['reciprocity']}")
    print(f"  Reply bonus (0-15):        {breakdown['reply_bonus']}")
    print(f"  ─────────────────────────")
    print(f"  Base score:                {breakdown['volume'] + breakdown['recency'] + breakdown['reciprocity'] + breakdown['reply_bonus']:.1f}")
    print(f"  Group multiplier:          {breakdown['group_multiplier']}x")
    print(f"  Newsletter penalty:        {breakdown['newsletter_penalty']}x")
    print(f"  ─────────────────────────")
    print(f"  FINAL SCORE:               {score}/100")


def show_status():
    """Show V2 scoring status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count edges with V2 scores
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person)
                WHERE r.strength_score_v2 IS NOT NULL
                RETURN
                    count(r) as scored,
                    avg(r.strength_score_v2) as avg_v2,
                    avg(r.strength_score) as avg_v1,
                    sum(CASE WHEN r.strength_score_v2 >= 70 THEN 1 ELSE 0 END) as strong,
                    sum(CASE WHEN r.strength_score_v2 >= 40 AND r.strength_score_v2 < 70 THEN 1 ELSE 0 END) as medium,
                    sum(CASE WHEN r.strength_score_v2 >= 10 AND r.strength_score_v2 < 40 THEN 1 ELSE 0 END) as weak,
                    sum(CASE WHEN r.strength_score_v2 < 10 THEN 1 ELSE 0 END) as minimal
            """)
            stats = result.single()

            # Get total edges
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person)
                RETURN count(r) as total
            """)
            total = result.single()['total']

            # Top contacts by V2 score
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person)
                WHERE r.strength_score_v2 IS NOT NULL
                RETURN p.name as name, p.primary_email as email,
                       r.strength_score_v2 as v2, r.strength_score as v1,
                       r.emails_sent as sent, r.emails_received as received,
                       r.reply_rate as reply_rate, r.group_multiplier as group_mult
                ORDER BY r.strength_score_v2 DESC
                LIMIT 15
            """)
            top_contacts = [dict(r) for r in result]

            # Biggest score changes
            result = session.run("""
                MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]-(p:Person)
                WHERE r.strength_score_v2 IS NOT NULL AND r.strength_score IS NOT NULL
                WITH p, r, r.strength_score - r.strength_score_v2 as drop
                WHERE drop > 20
                RETURN p.name as name, p.primary_email as email,
                       r.strength_score as v1, r.strength_score_v2 as v2,
                       drop, r.group_multiplier as group_mult, r.reply_rate as reply_rate
                ORDER BY drop DESC
                LIMIT 10
            """)
            biggest_drops = [dict(r) for r in result]

        print("\n" + "=" * 60)
        print("RELATIONSHIP STRENGTH V2 STATUS")
        print("=" * 60)
        print(f"Total KNOWS edges:        {total:,}")
        print(f"With V2 scores:           {stats['scored']:,}")
        print(f"\nAverage Scores:")
        print(f"  V1 (old):               {stats['avg_v1']:.1f}")
        print(f"  V2 (new):               {stats['avg_v2']:.1f}")
        print(f"\nV2 Distribution:")
        print(f"  Strong (70-100):        {stats['strong']:,}")
        print(f"  Medium (40-69):         {stats['medium']:,}")
        print(f"  Weak (10-39):           {stats['weak']:,}")
        print(f"  Minimal (0-9):          {stats['minimal']:,}")

        print(f"\nTop 15 Contacts (V2):")
        print(f"{'Name':<25} {'V2':<5} {'V1':<5} {'Sent':<6} {'Recv':<6} {'Reply%':<8} {'GroupX'}")
        print("-" * 75)
        for c in top_contacts:
            name = (c['name'] or c['email'].split('@')[0])[:24]
            reply = f"{c['reply_rate']*100:.0f}%" if c['reply_rate'] else "-"
            group = f"{c['group_mult']:.1f}" if c['group_mult'] else "-"
            print(f"{name:<25} {c['v2']:<5} {c['v1'] or '-':<5} {c['sent'] or 0:<6} {c['received'] or 0:<6} {reply:<8} {group}")

        if biggest_drops:
            print(f"\nBiggest Score Drops (V1 → V2):")
            print(f"{'Name':<25} {'V1':<5} {'V2':<5} {'Drop':<6} {'GroupX':<8} {'Reply%'}")
            print("-" * 65)
            for c in biggest_drops:
                name = (c['name'] or c['email'].split('@')[0])[:24]
                reply = f"{c['reply_rate']*100:.0f}%" if c['reply_rate'] else "-"
                group = f"{c['group_mult']:.1f}" if c['group_mult'] else "-"
                print(f"{name:<25} {c['v1']:<5} {c['v2']:<5} {c['drop']:<6} {group:<8} {reply}")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Improved relationship strength scoring (V2)'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show V2 scoring status')
    parser.add_argument('--run', action='store_true',
                        help='Run V2 scoring')
    parser.add_argument('--check', type=str, metavar='EMAIL',
                        help='Check detailed scoring for a specific contact')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.check:
        check_contact(args.check)
        return

    if args.run:
        stats = run_strength_scoring_v2()
        print("\n" + "=" * 50)
        print("V2 SCORING RESULTS")
        print("=" * 50)
        print(f"Total edges:              {stats.get('total', 0):,}")
        print(f"Updated:                  {stats.get('updated', 0):,}")
        print(f"Skipped (no stats):       {stats.get('skipped', 0):,}")
        print(f"Average V2 score:         {stats.get('avg_score', 0):.1f}")
        print(f"\nDistribution:")
        dist = stats.get('score_distribution', {})
        print(f"  Strong (70-100):        {dist.get('strong_70_100', 0):,}")
        print(f"  Medium (40-69):         {dist.get('medium_40_69', 0):,}")
        print(f"  Weak (10-39):           {dist.get('weak_10_39', 0):,}")
        print(f"  Minimal (0-9):          {dist.get('minimal_0_9', 0):,}")
        print(f"Errors:                   {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
