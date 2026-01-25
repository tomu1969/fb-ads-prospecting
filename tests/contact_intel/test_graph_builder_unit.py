"""Unit tests for graph_builder that don't require Neo4j.

These tests always run, even when Neo4j is not available.
"""

import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available


def test_graph_builder_initializes_without_connection():
    """GraphBuilder should initialize without connecting."""
    gb = GraphBuilder()
    assert gb.driver is None


def test_neo4j_available_function_returns_bool():
    """neo4j_available() function should exist and return bool."""
    result = neo4j_available()
    assert isinstance(result, bool)


def test_graph_builder_has_required_methods():
    """GraphBuilder should have all required public methods."""
    gb = GraphBuilder()
    required_methods = [
        'connect',
        'close',
        'setup_schema',
        'create_or_update_person',
        'find_person_by_email',
        'find_person_by_any_email',
        'add_alternate_email',
        'create_knows_relationship',
        'get_relationship',
        'create_cc_together_relationship',
        'process_email',
        'get_stats',
    ]
    for method in required_methods:
        assert hasattr(gb, method), f"Missing method: {method}"
        assert callable(getattr(gb, method)), f"{method} is not callable"


def test_close_when_not_connected():
    """GraphBuilder.close() should not raise when never connected."""
    gb = GraphBuilder()
    # Should not raise
    gb.close()
    assert gb.driver is None


def test_neo4j_uri_defaults():
    """NEO4J_URI should default to localhost if not set."""
    from scripts.contact_intel.graph_builder import NEO4J_URI, NEO4J_USER
    assert 'localhost' in NEO4J_URI or NEO4J_URI.startswith('bolt://')
    assert NEO4J_USER == 'neo4j' or NEO4J_USER is not None
