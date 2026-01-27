# Phase 4: LinkedIn Integration - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Import LinkedIn connections into the contact graph, add LINKEDIN_CONNECTED edges with degree/mutual count, merge with existing Person nodes, and improve warm intro path scoring.

**Architecture:** Parse LinkedIn's exported CSV (Connections.csv from data export), create LINKEDIN_CONNECTED relationships in Neo4j with connection degree and mutual count. Merge LinkedIn profiles with existing email-derived Person nodes using email matching. Update graph queries to weight paths through LinkedIn connections.

**Tech Stack:** Python, Neo4j, pandas, pytest

---

## Prerequisites

Before starting, the user needs to export their LinkedIn data:
1. Go to LinkedIn → Settings → Data Privacy → Get a copy of your data
2. Select "Connections"
3. Download and extract - file will be `Connections.csv`
4. Place at `data/contact_intel/linkedin_connections.csv`

The CSV format from LinkedIn:
```csv
First Name,Last Name,Email Address,Company,Position,Connected On
John,Smith,john@acme.com,Acme Corp,CEO,15 Jan 2024
```

---

## Task 1: Create LinkedIn Sync Module - CSV Parser

**Files:**
- Create: `scripts/contact_intel/linkedin_sync.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# scripts/contact_intel/tests/test_linkedin_sync.py
"""Tests for LinkedIn connections sync."""

import tempfile
from pathlib import Path

import pytest


class TestLinkedInCSVParser:
    """Tests for parsing LinkedIn Connections.csv export."""

    def test_parse_connections_csv(self):
        """Should parse LinkedIn export CSV into connection records."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        # Create test CSV
        csv_content = """First Name,Last Name,Email Address,Company,Position,Connected On
John,Smith,john@acme.com,Acme Corp,CEO,15 Jan 2024
Jane,Doe,jane@startup.io,TechStartup,CTO,20 Feb 2024
Bob,Wilson,,Big Corp,Manager,01 Mar 2024"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            connections = parse_linkedin_csv(csv_path)

            assert len(connections) == 3
            assert connections[0]['first_name'] == 'John'
            assert connections[0]['last_name'] == 'Smith'
            assert connections[0]['email'] == 'john@acme.com'
            assert connections[0]['company'] == 'Acme Corp'
            assert connections[0]['position'] == 'CEO'
            assert connections[0]['connected_on'] == '15 Jan 2024'

            # Bob has no email - should still be parsed
            assert connections[2]['email'] is None
        finally:
            csv_path.unlink()

    def test_parse_empty_csv(self):
        """Should handle empty CSV gracefully."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        csv_content = """First Name,Last Name,Email Address,Company,Position,Connected On"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            connections = parse_linkedin_csv(csv_path)
            assert connections == []
        finally:
            csv_path.unlink()

    def test_parse_missing_file(self):
        """Should raise FileNotFoundError for missing CSV."""
        from scripts.contact_intel.linkedin_sync import parse_linkedin_csv

        with pytest.raises(FileNotFoundError):
            parse_linkedin_csv(Path('/nonexistent/file.csv'))
```

**Step 2: Run test to verify it fails**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInCSVParser -v
```

Expected: FAIL with "ModuleNotFoundError: No module named 'scripts.contact_intel.linkedin_sync'"

**Step 3: Write minimal implementation**

```python
# scripts/contact_intel/linkedin_sync.py
"""LinkedIn connections sync for contact intelligence graph.

Imports LinkedIn connections from exported CSV and creates
LINKEDIN_CONNECTED relationships in Neo4j.

Usage:
    python -m scripts.contact_intel.linkedin_sync --status
    python -m scripts.contact_intel.linkedin_sync --sync
    python -m scripts.contact_intel.linkedin_sync --csv path/to/Connections.csv
"""

import argparse
import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Default path for LinkedIn export
DEFAULT_CSV_PATH = Path('data/contact_intel/linkedin_connections.csv')


def parse_linkedin_csv(csv_path: Path) -> List[Dict]:
    """Parse LinkedIn Connections.csv export.

    Args:
        csv_path: Path to LinkedIn export CSV

    Returns:
        List of connection dicts with keys:
        - first_name, last_name, email, company, position, connected_on

    Raises:
        FileNotFoundError: If CSV file doesn't exist
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"LinkedIn CSV not found: {csv_path}")

    connections = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            connection = {
                'first_name': row.get('First Name', '').strip() or None,
                'last_name': row.get('Last Name', '').strip() or None,
                'email': row.get('Email Address', '').strip() or None,
                'company': row.get('Company', '').strip() or None,
                'position': row.get('Position', '').strip() or None,
                'connected_on': row.get('Connected On', '').strip() or None,
            }
            connections.append(connection)

    logger.info(f"Parsed {len(connections)} connections from {csv_path}")
    return connections
```

**Step 4: Run test to verify it passes**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInCSVParser -v
```

Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add scripts/contact_intel/linkedin_sync.py scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "feat(contact-intel): add LinkedIn CSV parser for Phase 4

- Parse LinkedIn Connections.csv export format
- Extract first_name, last_name, email, company, position, connected_on
- Handle missing emails gracefully

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Add Neo4j Schema for LINKEDIN_CONNECTED

**Files:**
- Modify: `scripts/contact_intel/graph_builder.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# Add to scripts/contact_intel/tests/test_linkedin_sync.py

class TestLinkedInSchema:
    """Tests for LinkedIn schema in Neo4j."""

    def test_setup_linkedin_schema(self):
        """Should create index on linkedin_url."""
        from unittest.mock import MagicMock, patch

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        with patch('scripts.contact_intel.graph_builder.GraphDatabase') as mock_gdb:
            mock_gdb.driver.return_value = mock_driver

            from scripts.contact_intel.graph_builder import GraphBuilder
            gb = GraphBuilder()
            gb.driver = mock_driver
            gb.setup_linkedin_schema()

        # Should have created linkedin_url index
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('linkedin_url' in c for c in calls)
```

**Step 2: Run test to verify it fails**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInSchema -v
```

Expected: FAIL with "AttributeError: 'GraphBuilder' object has no attribute 'setup_linkedin_schema'"

**Step 3: Write minimal implementation**

```python
# Add to scripts/contact_intel/graph_builder.py, inside GraphBuilder class

    def setup_linkedin_schema(self):
        """Create indexes for LinkedIn integration.

        Creates:
        - Index on Person.linkedin_url for lookup
        """
        with self.driver.session() as session:
            # Index on linkedin_url for matching
            session.run("""
                CREATE INDEX person_linkedin_url IF NOT EXISTS
                FOR (p:Person) ON (p.linkedin_url)
            """)

        logger.info("LinkedIn schema setup complete")
```

**Step 4: Run test to verify it passes**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInSchema -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/graph_builder.py scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "feat(contact-intel): add LinkedIn schema to graph builder

- Add setup_linkedin_schema() method
- Create index on Person.linkedin_url

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Create LINKEDIN_CONNECTED Edges

**Files:**
- Modify: `scripts/contact_intel/linkedin_sync.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# Add to scripts/contact_intel/tests/test_linkedin_sync.py

class TestLinkedInSync:
    """Tests for syncing LinkedIn connections to Neo4j."""

    def test_create_linkedin_connected_edge(self):
        """Should create LINKEDIN_CONNECTED relationship."""
        from unittest.mock import MagicMock, patch

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@acme.com',
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        # Should have run MERGE for LINKEDIN_CONNECTED
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('LINKEDIN_CONNECTED' in c for c in calls)

    def test_skip_connection_without_email(self):
        """Should skip connections without email address."""
        from unittest.mock import MagicMock

        mock_gb = MagicMock()

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': None,  # No email
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        result = create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        assert result is False
        mock_gb.driver.session.assert_not_called()
```

**Step 2: Run test to verify it fails**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInSync -v
```

Expected: FAIL with "ImportError: cannot import name 'create_linkedin_connection'"

**Step 3: Write minimal implementation**

```python
# Add to scripts/contact_intel/linkedin_sync.py

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
```

**Step 4: Run test to verify it passes**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInSync -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/linkedin_sync.py scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "feat(contact-intel): add LINKEDIN_CONNECTED edge creation

- Create Person node for connection if not exists
- Create LINKEDIN_CONNECTED edge with degree=1, connected_on
- Skip connections without email address

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Merge LinkedIn Profiles with Email-Derived Persons

**Files:**
- Modify: `scripts/contact_intel/linkedin_sync.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# Add to scripts/contact_intel/tests/test_linkedin_sync.py

class TestLinkedInMerge:
    """Tests for merging LinkedIn profiles with existing Person nodes."""

    def test_merge_updates_existing_person(self):
        """Should update existing Person node with LinkedIn data."""
        from unittest.mock import MagicMock, patch, call

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate existing person found by email
        mock_result = MagicMock()
        mock_result.single.return_value = {'count': 1}
        mock_session.run.return_value = mock_result

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@acme.com',
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        # Should use MERGE to update existing node
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('MERGE' in c and 'primary_email' in c for c in calls)

    def test_merge_preserves_email_derived_name(self):
        """Should not overwrite name if already set from email signature."""
        from unittest.mock import MagicMock

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        connection = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@acme.com',
            'company': 'Acme Corp',
            'position': 'CEO',
            'connected_on': '15 Jan 2024',
        }

        from scripts.contact_intel.linkedin_sync import create_linkedin_connection
        create_linkedin_connection(mock_gb, 'tu@jaguarcapital.co', connection)

        # Should use COALESCE to preserve existing name
        calls = [str(c) for c in mock_session.run.call_args_list]
        assert any('COALESCE' in c for c in calls)
```

**Step 2: Run test to verify it passes (already implemented in Task 3)**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInMerge -v
```

Expected: PASS (COALESCE already in Task 3 implementation)

**Step 3: Commit (if any changes)**

```bash
git add scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "test(contact-intel): add merge behavior tests for LinkedIn sync

- Test that existing Person nodes are updated via MERGE
- Test that COALESCE preserves email-derived names

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Full Sync Function with Stats

**Files:**
- Modify: `scripts/contact_intel/linkedin_sync.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# Add to scripts/contact_intel/tests/test_linkedin_sync.py

class TestFullSync:
    """Tests for full LinkedIn sync process."""

    def test_sync_linkedin_connections(self):
        """Should sync all connections and return stats."""
        from unittest.mock import MagicMock, patch
        import tempfile
        from pathlib import Path

        csv_content = """First Name,Last Name,Email Address,Company,Position,Connected On
John,Smith,john@acme.com,Acme Corp,CEO,15 Jan 2024
Jane,Doe,jane@startup.io,TechStartup,CTO,20 Feb 2024
Bob,Wilson,,Big Corp,Manager,01 Mar 2024"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_gb = MagicMock()
        mock_gb.driver = mock_driver

        try:
            with patch('scripts.contact_intel.linkedin_sync.GraphBuilder', return_value=mock_gb):
                with patch('scripts.contact_intel.linkedin_sync.neo4j_available', return_value=True):
                    from scripts.contact_intel.linkedin_sync import sync_linkedin_connections

                    stats = sync_linkedin_connections(
                        csv_path=csv_path,
                        my_email='tu@jaguarcapital.co'
                    )

            assert stats['total'] == 3
            assert stats['synced'] == 2  # 2 with email
            assert stats['skipped'] == 1  # 1 without email
        finally:
            csv_path.unlink()
```

**Step 2: Run test to verify it fails**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestFullSync -v
```

Expected: FAIL with "ImportError: cannot import name 'sync_linkedin_connections'"

**Step 3: Write minimal implementation**

```python
# Add to scripts/contact_intel/linkedin_sync.py

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available


def sync_linkedin_connections(
    csv_path: Optional[Path] = None,
    my_email: str = 'tu@jaguarcapital.co',
) -> Dict:
    """Sync LinkedIn connections to Neo4j graph.

    Args:
        csv_path: Path to LinkedIn Connections.csv (default: data/contact_intel/linkedin_connections.csv)
        my_email: Your email address

    Returns:
        Stats dict with total, synced, skipped counts
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
```

**Step 4: Run test to verify it passes**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestFullSync -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/linkedin_sync.py scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "feat(contact-intel): add full LinkedIn sync with stats

- sync_linkedin_connections() parses CSV and creates edges
- Returns stats: total, synced, skipped, errors
- Sets up schema on first run

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Update Graph Queries for LinkedIn Paths

**Files:**
- Modify: `scripts/contact_intel/graph_query.py`
- Test: `scripts/contact_intel/tests/test_linkedin_sync.py`

**Step 1: Write the failing test**

```python
# Add to scripts/contact_intel/tests/test_linkedin_sync.py

class TestLinkedInQueries:
    """Tests for LinkedIn-aware graph queries."""

    def test_schema_includes_linkedin_connected(self):
        """GRAPH_SCHEMA should document LINKEDIN_CONNECTED relationship."""
        from scripts.contact_intel.graph_query import GRAPH_SCHEMA

        assert 'LINKEDIN_CONNECTED' in GRAPH_SCHEMA

    def test_system_prompt_includes_linkedin_example(self):
        """SYSTEM_PROMPT should include LinkedIn query examples."""
        from scripts.contact_intel.graph_query import SYSTEM_PROMPT

        assert 'LINKEDIN_CONNECTED' in SYSTEM_PROMPT or 'linkedin' in SYSTEM_PROMPT.lower()
```

**Step 2: Run test to verify it fails**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInQueries -v
```

Expected: FAIL with AssertionError (LINKEDIN_CONNECTED not in schema)

**Step 3: Write minimal implementation**

Update `scripts/contact_intel/graph_query.py`:

```python
# Update GRAPH_SCHEMA (add after CC_TOGETHER section):

- (Person)-[:LINKEDIN_CONNECTED {degree, connected_on}]->(Person)
  1st degree LinkedIn connections

# Update SYSTEM_PROMPT (add example):

EXAMPLE - "LinkedIn connections at Google" or "who am I LinkedIn-connected to at Google":
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_CONNECTED]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*google.*'
RETURN DISTINCT p.name, p.primary_email, c.name
LIMIT 25

EXAMPLE - "warm intro via LinkedIn" (prefer LinkedIn-connected paths):
MATCH path = (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_CONNECTED|KNOWS*1..2]-(target:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*target_company.*'
RETURN DISTINCT target.name, target.primary_email, c.name,
       [r IN relationships(path) | type(r)] as path_types,
       length(path) as hops
ORDER BY hops
LIMIT 25
```

**Step 4: Run test to verify it passes**

```bash
pytest scripts/contact_intel/tests/test_linkedin_sync.py::TestLinkedInQueries -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add scripts/contact_intel/graph_query.py scripts/contact_intel/tests/test_linkedin_sync.py
git commit -m "feat(contact-intel): add LinkedIn support to graph queries

- Add LINKEDIN_CONNECTED to GRAPH_SCHEMA
- Add LinkedIn query examples to SYSTEM_PROMPT
- Support combined LINKEDIN_CONNECTED|KNOWS paths

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: CLI Interface

**Files:**
- Modify: `scripts/contact_intel/linkedin_sync.py`

**Step 1: Add CLI main function**

```python
# Add to scripts/contact_intel/linkedin_sync.py

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
```

**Step 2: Test manually**

```bash
python -m scripts.contact_intel.linkedin_sync --status
python -m scripts.contact_intel.linkedin_sync --help
```

**Step 3: Commit**

```bash
git add scripts/contact_intel/linkedin_sync.py
git commit -m "feat(contact-intel): add CLI for LinkedIn sync

- --status: Show sync status and CSV info
- --sync: Sync connections to Neo4j
- --csv: Custom path to Connections.csv

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Update CLAUDE.md and Documentation

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add LinkedIn sync commands**

Add to CLAUDE.md Quick Commands section:

```markdown
# === CONTACT INTELLIGENCE GRAPH ===

# Query your contact network (natural language → Neo4j)
python -m scripts.contact_intel.graph_query "who do I know at Google?"
python -m scripts.contact_intel.graph_query "LinkedIn connections at Compass"
python -m scripts.contact_intel.graph_query "warm intro path to target via LinkedIn"

# Sync LinkedIn connections
python -m scripts.contact_intel.linkedin_sync --status
python -m scripts.contact_intel.linkedin_sync --sync
python -m scripts.contact_intel.linkedin_sync --csv path/to/Connections.csv
```

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add LinkedIn sync commands to CLAUDE.md

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

| Task | Description | Tests |
|------|-------------|-------|
| 1 | CSV parser for LinkedIn export | 3 |
| 2 | Neo4j schema for linkedin_url index | 1 |
| 3 | Create LINKEDIN_CONNECTED edges | 2 |
| 4 | Merge with existing Person nodes | 2 |
| 5 | Full sync function with stats | 1 |
| 6 | Update graph queries for LinkedIn paths | 2 |
| 7 | CLI interface | Manual |
| 8 | Documentation | - |

**Total: 8 tasks, ~11 tests**

After completing all tasks:
1. Export LinkedIn connections from LinkedIn settings
2. Place at `data/contact_intel/linkedin_connections.csv`
3. Run `python -m scripts.contact_intel.linkedin_sync --sync`
4. Query with `python -m scripts.contact_intel.graph_query "LinkedIn connections at Google"`
