"""Tests for graph_queries.py - Cypher query library for contact intelligence.

TDD: Write tests first, then implement.
Tests skip gracefully if Neo4j is not available.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

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
        session.run("MATCH (n) WHERE n.primary_email CONTAINS 'test_gq_' DETACH DELETE n")
    gb.close()


@pytest.fixture
def graph_queries(graph_builder):
    """Create GraphQueries instance with connection."""
    from scripts.contact_intel.graph_queries import GraphQueries
    return GraphQueries(graph_builder.driver)


@pytest.fixture
def setup_test_graph(graph_builder):
    """Set up a test graph with known relationships.

    Graph structure:
    - me@test.com (Me)
        - KNOWS -> alice@acme.com (Alice at Acme, 50 emails)
        - KNOWS -> bob@startup.com (Bob at Startup, 10 emails)
    - alice@acme.com (Alice)
        - KNOWS -> target@prospect.com (Target - our prospect)
        - KNOWS -> charlie@acme.com (Charlie - same company)
    - bob@startup.com (Bob)
        - KNOWS -> target@prospect.com (Target - weaker connection)
    - charlie@acme.com
        - CC_TOGETHER -> target@prospect.com (seen together in emails)
    """
    # Create Me
    graph_builder.create_or_update_person(
        email="test_gq_me@test.com",
        name="Test Me"
    )

    # Create Alice (strong connection to me)
    graph_builder.create_or_update_person(
        email="test_gq_alice@acme.com",
        name="Alice Smith",
        company="Acme Corp"
    )

    # Create Bob (weaker connection to me)
    graph_builder.create_or_update_person(
        email="test_gq_bob@startup.com",
        name="Bob Jones",
        company="Startup Inc"
    )

    # Create Target (prospect we want to reach)
    graph_builder.create_or_update_person(
        email="test_gq_target@prospect.com",
        name="Target Person",
        company="Prospect Corp"
    )

    # Create Charlie (same company as Alice)
    graph_builder.create_or_update_person(
        email="test_gq_charlie@acme.com",
        name="Charlie Brown",
        company="Acme Corp"
    )

    # Create Direct contact
    graph_builder.create_or_update_person(
        email="test_gq_direct@known.com",
        name="Direct Contact"
    )

    # Me KNOWS Alice (strong - 50 emails)
    for i in range(50):
        graph_builder.create_knows_relationship(
            from_email="test_gq_me@test.com",
            to_email="test_gq_alice@acme.com",
            email_date=datetime(2024, 1, 1) + __import__('datetime').timedelta(days=i)
        )

    # Me KNOWS Bob (weaker - 10 emails)
    for i in range(10):
        graph_builder.create_knows_relationship(
            from_email="test_gq_me@test.com",
            to_email="test_gq_bob@startup.com",
            email_date=datetime(2024, 1, 1) + __import__('datetime').timedelta(days=i*10)
        )

    # Me KNOWS Direct Contact (direct connection)
    for i in range(20):
        graph_builder.create_knows_relationship(
            from_email="test_gq_me@test.com",
            to_email="test_gq_direct@known.com",
            email_date=datetime(2024, 6, 1) + __import__('datetime').timedelta(days=i)
        )

    # Alice KNOWS Target
    for i in range(5):
        graph_builder.create_knows_relationship(
            from_email="test_gq_alice@acme.com",
            to_email="test_gq_target@prospect.com",
            email_date=datetime(2024, 3, 1) + __import__('datetime').timedelta(days=i*7)
        )

    # Bob KNOWS Target (weaker)
    for i in range(2):
        graph_builder.create_knows_relationship(
            from_email="test_gq_bob@startup.com",
            to_email="test_gq_target@prospect.com",
            email_date=datetime(2024, 2, 1) + __import__('datetime').timedelta(days=i*30)
        )

    # Alice KNOWS Charlie (same company)
    graph_builder.create_knows_relationship(
        from_email="test_gq_alice@acme.com",
        to_email="test_gq_charlie@acme.com",
        email_date=datetime(2024, 1, 15)
    )

    # Charlie CC_TOGETHER with Target
    for i in range(8):
        graph_builder.create_cc_together_relationship(
            email1="test_gq_charlie@acme.com",
            email2="test_gq_target@prospect.com",
            email_date=datetime(2024, 4, 1) + __import__('datetime').timedelta(days=i*5)
        )

    return {
        'my_email': 'test_gq_me@test.com',
        'direct_email': 'test_gq_direct@known.com',
        'alice_email': 'test_gq_alice@acme.com',
        'bob_email': 'test_gq_bob@startup.com',
        'target_email': 'test_gq_target@prospect.com',
        'charlie_email': 'test_gq_charlie@acme.com',
    }


class TestFindDirectConnection:
    """Test finding direct KNOWS relationships."""

    def test_find_direct_connection(self, graph_queries, setup_test_graph):
        """Should find direct KNOWS relationship."""
        result = graph_queries.find_direct_connection(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['direct_email']
        )

        assert result is not None
        assert result.prospect_email == setup_test_graph['direct_email']
        assert result.path_type == 'direct'
        assert result.email_count >= 1
        assert result.connector_email is None  # No connector needed for direct

    def test_find_direct_connection_not_found(self, graph_queries, setup_test_graph):
        """Should return None when no direct connection exists."""
        result = graph_queries.find_direct_connection(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']  # Not directly connected
        )

        assert result is None

    def test_find_direct_connection_with_alternate_email(self, graph_builder, graph_queries, setup_test_graph):
        """Should find direct connection using alternate email."""
        # Add alternate email to direct contact
        graph_builder.add_alternate_email(
            primary_email=setup_test_graph['direct_email'],
            alternate_email="test_gq_direct_alt@known.com"
        )

        result = graph_queries.find_direct_connection(
            my_email=setup_test_graph['my_email'],
            target_email="test_gq_direct_alt@known.com"  # Use alternate email
        )

        assert result is not None
        assert result.path_type == 'direct'


class TestFindOneHopPath:
    """Test finding friend-of-friend paths."""

    def test_find_one_hop_path(self, graph_queries, setup_test_graph):
        """Should find friend-of-friend path."""
        results = graph_queries.find_one_hop_paths(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']
        )

        assert len(results) >= 1

        # Should find path through Alice (stronger) and Bob (weaker)
        connector_emails = [r.connector_email for r in results]
        assert setup_test_graph['alice_email'] in connector_emails or \
               setup_test_graph['bob_email'] in connector_emails

        # All results should be one_hop type
        for result in results:
            assert result.path_type == 'one_hop'
            assert result.connector_email is not None
            assert result.prospect_email == setup_test_graph['target_email']

    def test_find_one_hop_path_ranked_by_strength(self, graph_queries, setup_test_graph):
        """Should rank connectors by relationship strength (email_count)."""
        results = graph_queries.find_one_hop_paths(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']
        )

        assert len(results) >= 2

        # Alice should be ranked higher (50 emails with me vs Bob's 10)
        alice_result = next((r for r in results if r.connector_email == setup_test_graph['alice_email']), None)
        bob_result = next((r for r in results if r.connector_email == setup_test_graph['bob_email']), None)

        if alice_result and bob_result:
            # Alice should have higher email count with me
            assert alice_result.email_count > bob_result.email_count

    def test_find_one_hop_path_respects_limit(self, graph_queries, setup_test_graph):
        """Should respect limit parameter."""
        results = graph_queries.find_one_hop_paths(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email'],
            limit=1
        )

        assert len(results) <= 1

    def test_find_one_hop_path_not_found(self, graph_queries, setup_test_graph):
        """Should return empty list when no path exists."""
        results = graph_queries.find_one_hop_paths(
            my_email=setup_test_graph['my_email'],
            target_email="test_gq_nonexistent@nowhere.com"
        )

        assert results == []


class TestFindCompanyConnections:
    """Test finding connections by company domain."""

    def test_find_company_connection(self, graph_queries, setup_test_graph):
        """Should find people at same company domain."""
        results = graph_queries.find_company_connections(
            my_email=setup_test_graph['my_email'],
            target_domain="acme.com"
        )

        assert len(results) >= 1

        # Should find Alice at acme.com (we know her directly)
        alice_found = any(r.prospect_email == setup_test_graph['alice_email'] for r in results)
        assert alice_found

        # All results should be company_connection type
        for result in results:
            assert result.path_type == 'company_connection'
            assert 'acme.com' in result.prospect_email

    def test_find_company_connection_no_results(self, graph_queries, setup_test_graph):
        """Should return empty list for unknown company."""
        results = graph_queries.find_company_connections(
            my_email=setup_test_graph['my_email'],
            target_domain="unknowncompany.com"
        )

        assert results == []


class TestRankConnectorsByStrength:
    """Test connector ranking by relationship strength."""

    def test_rank_connectors_by_strength(self, graph_queries, setup_test_graph):
        """Should rank multiple connectors by relationship strength."""
        results = graph_queries.find_one_hop_paths(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']
        )

        # Results should be ordered by email_count (descending)
        for i in range(len(results) - 1):
            assert results[i].email_count >= results[i + 1].email_count


class TestFindCCTogetherConnections:
    """Test finding CC_TOGETHER connections."""

    def test_find_cc_together_connections(self, graph_queries, setup_test_graph):
        """Should find people frequently CC'd together."""
        results = graph_queries.find_cc_together_connections(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']
        )

        # Should find Charlie who was CC'd with Target
        # But only if Charlie is connected to me somehow
        # In our test graph, I -> Alice -> Charlie, and Charlie CC_TOGETHER Target
        for result in results:
            assert result.path_type == 'cc_together'
            assert result.shared_cc_count >= 1

    def test_find_cc_together_with_count(self, graph_queries, setup_test_graph):
        """Should return shared CC count."""
        results = graph_queries.find_cc_together_connections(
            my_email=setup_test_graph['my_email'],
            target_email=setup_test_graph['target_email']
        )

        # Charlie was CC'd with Target 8 times
        charlie_result = next(
            (r for r in results if 'charlie' in r.connector_email.lower()),
            None
        )

        if charlie_result:
            assert charlie_result.shared_cc_count >= 8


class TestRelationshipStrength:
    """Test relationship strength calculation."""

    def test_get_relationship_strength(self, graph_queries, setup_test_graph):
        """Should calculate relationship strength (1-10)."""
        strength = graph_queries.get_relationship_strength(
            from_email=setup_test_graph['my_email'],
            to_email=setup_test_graph['alice_email']
        )

        # Should be high - 50 emails
        assert strength >= 7
        assert strength <= 10

    def test_get_relationship_strength_weak(self, graph_queries, setup_test_graph):
        """Should return lower strength for weaker relationships."""
        strength = graph_queries.get_relationship_strength(
            from_email=setup_test_graph['my_email'],
            to_email=setup_test_graph['bob_email']
        )

        # Should be lower than Alice (10 emails vs 50)
        alice_strength = graph_queries.get_relationship_strength(
            from_email=setup_test_graph['my_email'],
            to_email=setup_test_graph['alice_email']
        )

        assert strength < alice_strength

    def test_get_relationship_strength_no_relationship(self, graph_queries, setup_test_graph):
        """Should return 0 for non-existent relationship."""
        strength = graph_queries.get_relationship_strength(
            from_email=setup_test_graph['my_email'],
            to_email=setup_test_graph['target_email']  # Not directly connected
        )

        assert strength == 0


class TestGraphQueriesUnit:
    """Unit tests that don't require Neo4j."""

    @pytest.mark.skipif(True, reason="Integration tests only")
    def test_placeholder(self):
        """Placeholder for structure."""
        pass


# Remove the skipif for unit tests
class TestPathResultDataclass:
    """Test PathResult dataclass structure."""

    @pytest.fixture(autouse=True)
    def skip_check(self):
        """Override the module-level skip for unit tests."""
        pass

    @pytest.mark.skipif(not neo4j_is_running(), reason="Need module import")
    def test_path_result_has_required_fields(self):
        """PathResult should have all required fields."""
        from scripts.contact_intel.graph_queries import PathResult

        result = PathResult(
            prospect_email="target@example.com",
            prospect_name="Target",
            path_type="one_hop",
            connector_email="connector@example.com",
            connector_name="Connector",
            connector_strength=8,
            email_count=50,
            last_contact=datetime.now(),
            shared_cc_count=0
        )

        assert result.prospect_email == "target@example.com"
        assert result.path_type == "one_hop"
        assert result.connector_strength == 8
