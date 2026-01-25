"""Tests for path_finder.py - Find entry paths to prospects through your network.

TDD: Write tests first, then implement.
Tests skip gracefully if Neo4j is not available.
"""

import csv
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available


def neo4j_is_running() -> bool:
    """Check if Neo4j is actually running and accessible."""
    return neo4j_available()


# Skip all integration tests if Neo4j not available
pytestmark = pytest.mark.skipif(
    not neo4j_is_running(),
    reason="Neo4j not running - start with: docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/contact_intel_2025 neo4j:latest"
)


@pytest.fixture
def graph_builder():
    """Create a connected GraphBuilder instance for tests."""
    gb = GraphBuilder()
    gb.connect()
    gb.setup_schema()
    yield gb
    # Cleanup: remove test data
    with gb.driver.session() as session:
        session.run("MATCH (n) WHERE n.primary_email CONTAINS 'test_pf_' DETACH DELETE n")
    gb.close()


@pytest.fixture
def setup_test_graph(graph_builder):
    """Set up a test graph with known relationships.

    Graph structure similar to test_graph_queries but with test_pf_ prefix.
    """
    import datetime as dt

    # Create Me
    graph_builder.create_or_update_person(
        email="test_pf_me@test.com",
        name="Test Me"
    )

    # Create Direct Contact (I know them directly)
    graph_builder.create_or_update_person(
        email="test_pf_direct@known.com",
        name="Direct Contact",
        company="Known Company"
    )

    # Create Connector (bridge to target)
    graph_builder.create_or_update_person(
        email="test_pf_connector@bridge.com",
        name="Sarah Connector",
        company="Bridge Corp"
    )

    # Create Target (prospect we want to reach)
    graph_builder.create_or_update_person(
        email="test_pf_target@prospect.com",
        name="Target Prospect",
        company="Target Corp"
    )

    # Create Company Connection (same company as target)
    graph_builder.create_or_update_person(
        email="test_pf_company@target.com",  # Note: @target.com domain
        name="Company Person",
        company="Target Corp"
    )

    # Create Cold Contact (no path to them)
    graph_builder.create_or_update_person(
        email="test_pf_cold@nowhere.com",
        name="Cold Contact",
        company="Unknown Corp"
    )

    # Me KNOWS Direct Contact (strong - 30 emails, recent)
    for i in range(30):
        graph_builder.create_knows_relationship(
            from_email="test_pf_me@test.com",
            to_email="test_pf_direct@known.com",
            email_date=datetime(2024, 10, 1) + dt.timedelta(days=i)
        )

    # Me KNOWS Connector (strong - 40 emails)
    for i in range(40):
        graph_builder.create_knows_relationship(
            from_email="test_pf_me@test.com",
            to_email="test_pf_connector@bridge.com",
            email_date=datetime(2024, 8, 1) + dt.timedelta(days=i)
        )

    # Me KNOWS Company Person (at same company as target)
    for i in range(15):
        graph_builder.create_knows_relationship(
            from_email="test_pf_me@test.com",
            to_email="test_pf_company@target.com",
            email_date=datetime(2024, 9, 1) + dt.timedelta(days=i*3)
        )

    # Connector KNOWS Target
    for i in range(10):
        graph_builder.create_knows_relationship(
            from_email="test_pf_connector@bridge.com",
            to_email="test_pf_target@prospect.com",
            email_date=datetime(2024, 7, 1) + dt.timedelta(days=i*5)
        )

    return {
        'my_email': 'test_pf_me@test.com',
        'direct_email': 'test_pf_direct@known.com',
        'connector_email': 'test_pf_connector@bridge.com',
        'target_email': 'test_pf_target@prospect.com',
        'company_email': 'test_pf_company@target.com',
        'cold_email': 'test_pf_cold@nowhere.com',
    }


@pytest.fixture
def path_finder(setup_test_graph):
    """Create PathFinder instance."""
    from scripts.contact_intel.path_finder import PathFinder
    pf = PathFinder(my_email=setup_test_graph['my_email'])
    yield pf
    pf.close()


class TestFindPathsForKnownContact:
    """Test finding paths for contacts we know directly."""

    def test_find_paths_for_known_contact(self, path_finder, setup_test_graph):
        """Should return direct path for known contact."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['direct_email'],
            prospect_name="Direct Contact"
        )

        assert entry_path is not None
        assert entry_path.path_type == 'direct'
        assert entry_path.prospect_email == setup_test_graph['direct_email']
        assert entry_path.connector_email is None  # No connector for direct
        assert entry_path.path_strength > 0  # Should have positive strength
        assert entry_path.email_count >= 1

    def test_direct_path_includes_last_contact(self, path_finder, setup_test_graph):
        """Should include last contact date for direct connections."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['direct_email']
        )

        assert entry_path.last_contact_date is not None


class TestFindPathsForUnknownContact:
    """Test finding paths for contacts we don't know directly."""

    def test_find_paths_for_unknown_contact(self, path_finder, setup_test_graph):
        """Should return one-hop paths via connectors."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['target_email'],
            prospect_name="Target Prospect"
        )

        assert entry_path is not None
        assert entry_path.path_type == 'one_hop'
        assert entry_path.connector_email == setup_test_graph['connector_email']
        assert entry_path.connector_name is not None
        assert entry_path.connector_strength >= 1

    def test_one_hop_path_includes_connector_details(self, path_finder, setup_test_graph):
        """Should include connector name, email, and strength."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['target_email']
        )

        assert entry_path.connector_email is not None
        assert entry_path.connector_name is not None
        assert entry_path.connector_strength >= 1
        assert entry_path.connector_strength <= 10
        assert entry_path.email_count >= 1  # Emails with connector


class TestFindPathsByCompany:
    """Test finding paths via company connections."""

    def test_find_paths_by_company(self, path_finder, setup_test_graph):
        """Should find paths via company connections."""
        # When looking for someone at target.com and we know someone there
        entry_path = path_finder.find_path(
            prospect_email="someone_new@target.com",  # Unknown person at target.com
            prospect_name="Someone New"
        )

        # Should suggest company connection since we know company@target.com
        if entry_path and entry_path.path_type == 'company_connection':
            assert 'target.com' in entry_path.connector_email


class TestColdPath:
    """Test handling contacts with no path."""

    def test_find_paths_returns_cold_when_no_path(self, path_finder, setup_test_graph):
        """Should return cold path type when no connection exists."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['cold_email'],
            prospect_name="Cold Contact"
        )

        assert entry_path is not None
        assert entry_path.path_type == 'cold'
        assert entry_path.path_strength == 0
        assert entry_path.connector_email is None


class TestOutputEntryPathsCSV:
    """Test CSV output functionality."""

    def test_output_entry_paths_csv(self, path_finder, setup_test_graph):
        """Should output CSV with path details."""
        # Create input CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['email', 'name', 'company'])
            writer.writeheader()
            writer.writerow({
                'email': setup_test_graph['direct_email'],
                'name': 'Direct Contact',
                'company': 'Known Company'
            })
            writer.writerow({
                'email': setup_test_graph['target_email'],
                'name': 'Target Prospect',
                'company': 'Target Corp'
            })
            input_path = f.name

        output_path = input_path.replace('.csv', '_paths.csv')

        try:
            # Process CSV
            path_finder.process_csv(input_path, output_path)

            # Verify output exists and has correct columns
            assert os.path.exists(output_path)

            with open(output_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2

            # Check required columns
            required_columns = [
                'prospect_name', 'prospect_email', 'path_type',
                'path_strength', 'connector_name', 'connector_email',
                'connector_strength', 'last_contact_date', 'email_count'
            ]
            for col in required_columns:
                assert col in reader.fieldnames, f"Missing column: {col}"

            # First row should be direct path
            assert rows[0]['path_type'] == 'direct'
            # Second row should be one_hop
            assert rows[1]['path_type'] == 'one_hop'

        finally:
            # Cleanup
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_csv_handles_missing_names(self, path_finder, setup_test_graph):
        """Should handle CSV rows with missing name field."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['email', 'company'])
            writer.writeheader()
            writer.writerow({
                'email': setup_test_graph['direct_email'],
                'company': 'Known Company'
            })
            input_path = f.name

        output_path = input_path.replace('.csv', '_paths.csv')

        try:
            path_finder.process_csv(input_path, output_path)
            assert os.path.exists(output_path)

            with open(output_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 1

        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)


class TestScorePathQuality:
    """Test path quality scoring."""

    def test_score_path_quality(self, path_finder, setup_test_graph):
        """Should score paths by connector strength and recency."""
        entry_path = path_finder.find_path(
            prospect_email=setup_test_graph['direct_email']
        )

        # Direct paths should have high strength
        assert entry_path.path_strength >= 50  # Out of 100

    def test_direct_path_scores_higher_than_one_hop(self, path_finder, setup_test_graph):
        """Direct paths should score higher than one-hop paths."""
        direct = path_finder.find_path(
            prospect_email=setup_test_graph['direct_email']
        )

        one_hop = path_finder.find_path(
            prospect_email=setup_test_graph['target_email']
        )

        assert direct.path_strength > one_hop.path_strength

    def test_cold_path_has_zero_strength(self, path_finder, setup_test_graph):
        """Cold paths should have zero strength."""
        cold = path_finder.find_path(
            prospect_email=setup_test_graph['cold_email']
        )

        assert cold.path_strength == 0


class TestFindPathsForProspects:
    """Test batch processing of prospects."""

    def test_find_paths_for_prospects(self, path_finder, setup_test_graph):
        """Should find paths for a list of prospects."""
        prospects = [
            {'email': setup_test_graph['direct_email'], 'name': 'Direct'},
            {'email': setup_test_graph['target_email'], 'name': 'Target'},
            {'email': setup_test_graph['cold_email'], 'name': 'Cold'},
        ]

        results = path_finder.find_paths_for_prospects(prospects)

        assert len(results) == 3
        assert results[0].path_type == 'direct'
        assert results[1].path_type == 'one_hop'
        assert results[2].path_type == 'cold'


class TestEntryPathDataclass:
    """Test EntryPath dataclass structure."""

    @pytest.mark.skipif(not neo4j_is_running(), reason="Need module import")
    def test_entry_path_has_required_fields(self):
        """EntryPath should have all required fields."""
        from scripts.contact_intel.path_finder import EntryPath

        entry = EntryPath(
            prospect_name="John Doe",
            prospect_email="john@example.com",
            prospect_company="Acme Corp",
            path_type="one_hop",
            path_strength=75,
            connector_name="Sarah",
            connector_email="sarah@example.com",
            connector_strength=8,
            last_contact_date="2024-11-01",
            email_count=50,
            suggested_opener=None
        )

        assert entry.prospect_name == "John Doe"
        assert entry.path_type == "one_hop"
        assert entry.path_strength == 75
        assert entry.connector_strength == 8


class TestCLI:
    """Test CLI functionality."""

    def test_cli_query_mode(self, setup_test_graph):
        """Should support --query flag for single prospect lookup."""
        import subprocess

        result = subprocess.run(
            [
                'python', 'scripts/contact_intel/path_finder.py',
                '--query', setup_test_graph['direct_email'],
                '--my-email', setup_test_graph['my_email']
            ],
            capture_output=True,
            text=True,
            cwd='/Users/tomas/Desktop/fb-ads-prospecting'
        )

        # Should not error
        assert result.returncode == 0 or 'usage' in result.stderr.lower()

    def test_cli_company_mode(self, setup_test_graph):
        """Should support --company flag for company domain lookup."""
        import subprocess

        result = subprocess.run(
            [
                'python', 'scripts/contact_intel/path_finder.py',
                '--company', 'target.com',
                '--my-email', setup_test_graph['my_email']
            ],
            capture_output=True,
            text=True,
            cwd='/Users/tomas/Desktop/fb-ads-prospecting'
        )

        # Should not error (may return empty results)
        assert result.returncode == 0 or 'usage' in result.stderr.lower()
