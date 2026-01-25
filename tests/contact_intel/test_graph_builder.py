"""Tests for Neo4j graph builder - Contact Intelligence System.

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


# Skip all tests if Neo4j not available
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
        session.run("MATCH (n) WHERE n.email CONTAINS 'test' OR n.primary_email CONTAINS 'test' DETACH DELETE n")
    gb.close()


class TestNeo4jConnection:
    """Test Neo4j connection functionality."""

    def test_connect_to_neo4j(self):
        """Should connect to Neo4j with credentials from .env."""
        gb = GraphBuilder()
        gb.connect()
        assert gb.driver is not None
        gb.close()

    def test_verify_connectivity(self):
        """Should verify connection is working."""
        gb = GraphBuilder()
        gb.connect()
        # Should not raise
        gb.driver.verify_connectivity()
        gb.close()


class TestPersonNode:
    """Test Person node CRUD operations."""

    def test_create_person_node(self, graph_builder):
        """Should create Person node with email as unique constraint."""
        person_id = graph_builder.create_or_update_person(
            email="test_john@example.com",
            name="John Doe",
            company="Acme Corp"
        )
        assert person_id is not None

        # Verify node exists
        person = graph_builder.find_person_by_email("test_john@example.com")
        assert person is not None
        assert person['name'] == "John Doe"
        assert person['company'] == "Acme Corp"
        assert person['primary_email'] == "test_john@example.com"

    def test_create_person_idempotent(self, graph_builder):
        """Should not duplicate Person if email already exists."""
        # Create first person
        graph_builder.create_or_update_person(
            email="test_jane@example.com",
            name="Jane Doe"
        )

        # Try to create again with same email
        graph_builder.create_or_update_person(
            email="test_jane@example.com",
            name="Jane Updated",
            company="New Corp"
        )

        # Should only have one node, with updated properties
        with graph_builder.driver.session() as session:
            result = session.run(
                "MATCH (p:Person {primary_email: $email}) RETURN count(p) as count",
                email="test_jane@example.com"
            )
            record = result.single()
            assert record['count'] == 1

        # Properties should be updated
        person = graph_builder.find_person_by_email("test_jane@example.com")
        assert person['name'] == "Jane Updated"
        assert person['company'] == "New Corp"

    def test_find_person_by_email(self, graph_builder):
        """Should find Person node by email address."""
        # Create person
        graph_builder.create_or_update_person(
            email="test_findme@example.com",
            name="Find Me"
        )

        # Find by email
        person = graph_builder.find_person_by_email("test_findme@example.com")
        assert person is not None
        assert person['name'] == "Find Me"

        # Non-existent email returns None
        not_found = graph_builder.find_person_by_email("test_nonexistent@example.com")
        assert not_found is None

    def test_create_person_with_additional_properties(self, graph_builder):
        """Should store additional custom properties on Person node."""
        graph_builder.create_or_update_person(
            email="test_props@example.com",
            name="Props Test",
            linkedin_url="https://linkedin.com/in/propstest",
            role="Software Engineer"
        )

        person = graph_builder.find_person_by_email("test_props@example.com")
        assert person['linkedin_url'] == "https://linkedin.com/in/propstest"
        assert person['role'] == "Software Engineer"


class TestKnowsRelationship:
    """Test KNOWS relationship operations."""

    def test_create_knows_relationship(self, graph_builder):
        """Should create KNOWS edge between two Person nodes."""
        # Create two people
        graph_builder.create_or_update_person(email="test_alice@example.com", name="Alice")
        graph_builder.create_or_update_person(email="test_bob@example.com", name="Bob")

        # Create relationship
        email_date = datetime(2024, 1, 15, 10, 30)
        graph_builder.create_knows_relationship(
            from_email="test_alice@example.com",
            to_email="test_bob@example.com",
            email_date=email_date,
            subject="Hello Bob!"
        )

        # Verify relationship exists
        rel = graph_builder.get_relationship("test_alice@example.com", "test_bob@example.com")
        assert rel is not None
        assert rel['email_count'] == 1
        assert rel['first_contact'] is not None
        assert rel['last_contact'] is not None

    def test_update_relationship_strength(self, graph_builder):
        """Should increment email_count on existing KNOWS relationship."""
        # Create people and initial relationship
        graph_builder.create_or_update_person(email="test_sender@example.com", name="Sender")
        graph_builder.create_or_update_person(email="test_receiver@example.com", name="Receiver")

        # First email
        graph_builder.create_knows_relationship(
            from_email="test_sender@example.com",
            to_email="test_receiver@example.com",
            email_date=datetime(2024, 1, 1)
        )

        # Second email
        graph_builder.create_knows_relationship(
            from_email="test_sender@example.com",
            to_email="test_receiver@example.com",
            email_date=datetime(2024, 1, 15)
        )

        # Third email
        graph_builder.create_knows_relationship(
            from_email="test_sender@example.com",
            to_email="test_receiver@example.com",
            email_date=datetime(2024, 2, 1)
        )

        # Check count incremented
        rel = graph_builder.get_relationship("test_sender@example.com", "test_receiver@example.com")
        assert rel['email_count'] == 3

    def test_relationship_tracks_first_and_last_contact(self, graph_builder):
        """Should track first_contact and last_contact dates."""
        graph_builder.create_or_update_person(email="test_a@example.com", name="A")
        graph_builder.create_or_update_person(email="test_b@example.com", name="B")

        # First email
        first_date = datetime(2024, 1, 1, 9, 0)
        graph_builder.create_knows_relationship(
            from_email="test_a@example.com",
            to_email="test_b@example.com",
            email_date=first_date
        )

        # Later email
        last_date = datetime(2024, 6, 15, 14, 30)
        graph_builder.create_knows_relationship(
            from_email="test_a@example.com",
            to_email="test_b@example.com",
            email_date=last_date
        )

        rel = graph_builder.get_relationship("test_a@example.com", "test_b@example.com")
        # First contact should be the earlier date
        assert rel['first_contact'] == first_date.isoformat()
        # Last contact should be the later date
        assert rel['last_contact'] == last_date.isoformat()


class TestCCTogetherRelationship:
    """Test CC_TOGETHER relationship operations."""

    def test_create_cc_together_relationship(self, graph_builder):
        """Should create CC_TOGETHER edge between two people CC'd on same email."""
        # Create people
        graph_builder.create_or_update_person(email="test_cc1@example.com", name="CC Person 1")
        graph_builder.create_or_update_person(email="test_cc2@example.com", name="CC Person 2")

        # Create CC_TOGETHER relationship
        email_date = datetime(2024, 3, 1)
        graph_builder.create_cc_together_relationship(
            email1="test_cc1@example.com",
            email2="test_cc2@example.com",
            email_date=email_date
        )

        # Verify relationship exists (check both directions since it's symmetric)
        with graph_builder.driver.session() as session:
            result = session.run("""
                MATCH (a:Person {primary_email: $email1})-[r:CC_TOGETHER]-(b:Person {primary_email: $email2})
                RETURN r.cc_count as count
            """, email1="test_cc1@example.com", email2="test_cc2@example.com")
            record = result.single()
            assert record is not None
            assert record['count'] >= 1


class TestEmailProcessing:
    """Test full email processing workflow."""

    def test_process_email_creates_nodes_and_relationships(self, graph_builder):
        """Should create Person nodes and KNOWS edges from email."""
        email_message = {
            'from': {'email': 'test_from@example.com', 'name': 'From Person'},
            'to': [
                {'email': 'test_to1@example.com', 'name': 'To Person 1'},
                {'email': 'test_to2@example.com', 'name': 'To Person 2'}
            ],
            'cc': [
                {'email': 'test_cc@example.com', 'name': 'CC Person'}
            ],
            'date': datetime(2024, 5, 1, 10, 0),
            'subject': 'Test Email'
        }

        graph_builder.process_email(email_message)

        # Verify sender exists
        sender = graph_builder.find_person_by_email("test_from@example.com")
        assert sender is not None
        assert sender['name'] == "From Person"

        # Verify recipients exist
        to1 = graph_builder.find_person_by_email("test_to1@example.com")
        assert to1 is not None

        # Verify KNOWS relationships from sender to recipients
        rel_to1 = graph_builder.get_relationship("test_from@example.com", "test_to1@example.com")
        assert rel_to1 is not None

        rel_to2 = graph_builder.get_relationship("test_from@example.com", "test_to2@example.com")
        assert rel_to2 is not None

        rel_cc = graph_builder.get_relationship("test_from@example.com", "test_cc@example.com")
        assert rel_cc is not None

    def test_process_email_creates_cc_together(self, graph_builder):
        """Should create CC_TOGETHER edges between all CC'd people."""
        email_message = {
            'from': {'email': 'test_sender@example.com', 'name': 'Sender'},
            'to': [{'email': 'test_recipient@example.com', 'name': 'Recipient'}],
            'cc': [
                {'email': 'test_cc_a@example.com', 'name': 'CC A'},
                {'email': 'test_cc_b@example.com', 'name': 'CC B'},
                {'email': 'test_cc_c@example.com', 'name': 'CC C'}
            ],
            'date': datetime(2024, 5, 1),
            'subject': 'Group Email'
        }

        graph_builder.process_email(email_message)

        # CC_TOGETHER should exist between all pairs of CC'd people
        with graph_builder.driver.session() as session:
            # Check A-B
            result = session.run("""
                MATCH (a:Person {primary_email: 'test_cc_a@example.com'})-[:CC_TOGETHER]-(b:Person {primary_email: 'test_cc_b@example.com'})
                RETURN count(*) as count
            """)
            assert result.single()['count'] >= 1

            # Check A-C
            result = session.run("""
                MATCH (a:Person {primary_email: 'test_cc_a@example.com'})-[:CC_TOGETHER]-(b:Person {primary_email: 'test_cc_c@example.com'})
                RETURN count(*) as count
            """)
            assert result.single()['count'] >= 1

            # Check B-C
            result = session.run("""
                MATCH (a:Person {primary_email: 'test_cc_b@example.com'})-[:CC_TOGETHER]-(b:Person {primary_email: 'test_cc_c@example.com'})
                RETURN count(*) as count
            """)
            assert result.single()['count'] >= 1


class TestMultipleEmails:
    """Test handling Person with multiple email addresses."""

    def test_merge_person_with_multiple_emails(self, graph_builder):
        """Should handle Person with multiple email addresses."""
        # First, create person with primary email
        graph_builder.create_or_update_person(
            email="test_primary@example.com",
            name="Multi Email Person"
        )

        # Add alternate email
        graph_builder.add_alternate_email(
            primary_email="test_primary@example.com",
            alternate_email="test_alternate@example.com"
        )

        # Should be able to find by either email
        person = graph_builder.find_person_by_email("test_primary@example.com")
        assert person is not None

        # Alternate email lookup should return same person
        person_alt = graph_builder.find_person_by_any_email("test_alternate@example.com")
        assert person_alt is not None
        assert person_alt['primary_email'] == "test_primary@example.com"


class TestSchemaSetup:
    """Test schema and constraint creation."""

    def test_setup_schema_creates_constraints(self, graph_builder):
        """Should create unique constraint on Person email."""
        # Schema already set up in fixture, verify it works
        # Try to create duplicate should merge, not fail
        graph_builder.create_or_update_person(email="test_schema@example.com", name="First")
        graph_builder.create_or_update_person(email="test_schema@example.com", name="Second")

        # Should only have one node
        with graph_builder.driver.session() as session:
            result = session.run(
                "MATCH (p:Person {primary_email: 'test_schema@example.com'}) RETURN count(p) as count"
            )
            assert result.single()['count'] == 1


# Note: Unit tests that don't require Neo4j are in test_graph_builder_unit.py
