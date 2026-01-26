"""Sync LLM extractions to Neo4j graph.

Handles:
- Company nodes with normalized names
- Topic nodes
- WORKS_AT relationships (Person -> Company)
- DISCUSSED relationships (Person -> Topic)
"""

import json
import logging
import sqlite3
from typing import Dict

from scripts.contact_intel.config import DATA_DIR
from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

logger = logging.getLogger(__name__)

EXTRACTIONS_DB = DATA_DIR / "extractions.db"


def _normalize_company_name(name: str) -> str:
    """Normalize company name for matching.

    Removes common suffixes like Inc, LLC, Corp, etc.
    Converts to lowercase and strips whitespace.

    Args:
        name: Company name to normalize

    Returns:
        Normalized company name, or empty string if None/empty
    """
    if not name:
        return ''
    normalized = name.lower().strip()
    for suffix in [' inc', ' inc.', ' llc', ' ltd', ' corp', ' corporation', ' co', ' co.']:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    return normalized.strip()


def sync_extractions_to_neo4j():
    """Sync all extractions from SQLite to Neo4j.

    Creates/updates:
    - Company nodes with normalized_name and name
    - Topic nodes with name
    - WORKS_AT edges (Person -> Company) with role and confidence
    - DISCUSSED edges (Person -> Topic)
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return

    conn = sqlite3.connect(EXTRACTIONS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT email, name, company, role, topics, confidence
        FROM contact_extractions
        WHERE extracted_at IS NOT NULL
    """)

    extractions = cursor.fetchall()
    conn.close()

    logger.info(f"Syncing {len(extractions)} extractions to Neo4j")

    gb = GraphBuilder()
    gb.connect()

    try:
        companies_created = 0
        topics_created = 0
        works_at_created = 0
        discussed_created = 0

        for row in extractions:
            email = row['email']
            company = row['company']
            role = row['role']
            topics_json = row['topics']
            confidence = row['confidence'] or 0.0

            try:
                topics = json.loads(topics_json) if topics_json else []
            except json.JSONDecodeError:
                topics = []

            if company:
                normalized = _normalize_company_name(company)

                with gb.driver.session() as session:
                    session.run("""
                        MERGE (c:Company {normalized_name: $normalized})
                        ON CREATE SET c.name = $name, c.created_at = datetime()
                    """, normalized=normalized, name=company)
                    companies_created += 1

                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (c:Company {normalized_name: $normalized})
                        MERGE (p)-[r:WORKS_AT]->(c)
                        ON CREATE SET r.role = $role, r.confidence = $confidence, r.created_at = datetime()
                        ON MATCH SET r.role = $role, r.confidence = $confidence, r.updated_at = datetime()
                    """, email=email, normalized=normalized, role=role, confidence=confidence)
                    works_at_created += 1

            for topic in topics:
                if not topic:
                    continue

                topic_normalized = topic.lower().strip()

                with gb.driver.session() as session:
                    session.run("""
                        MERGE (t:Topic {name: $name})
                        ON CREATE SET t.created_at = datetime()
                    """, name=topic_normalized)
                    topics_created += 1

                    session.run("""
                        MATCH (p:Person {primary_email: $email})
                        MATCH (t:Topic {name: $name})
                        MERGE (p)-[r:DISCUSSED]->(t)
                        ON CREATE SET r.created_at = datetime()
                        ON MATCH SET r.updated_at = datetime()
                    """, email=email, name=topic_normalized)
                    discussed_created += 1

        logger.info(f"Sync complete: {companies_created} companies, {topics_created} topics, {works_at_created} WORKS_AT, {discussed_created} DISCUSSED")

    finally:
        gb.close()


def get_sync_stats() -> Dict:
    """Get stats about synced data in Neo4j.

    Returns:
        Dict with counts of companies, topics, and relationships.
        Returns {'error': 'Neo4j not available'} if Neo4j is not configured.
    """
    if not neo4j_available():
        return {'error': 'Neo4j not available'}

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            companies = session.run("MATCH (c:Company) RETURN count(c) as c").single()['c']
            topics = session.run("MATCH (t:Topic) RETURN count(t) as c").single()['c']
            works_at = session.run("MATCH ()-[r:WORKS_AT]->() RETURN count(r) as c").single()['c']
            discussed = session.run("MATCH ()-[r:DISCUSSED]->() RETURN count(r) as c").single()['c']

        return {
            'companies': companies,
            'topics': topics,
            'works_at_edges': works_at,
            'discussed_edges': discussed,
        }
    finally:
        gb.close()
