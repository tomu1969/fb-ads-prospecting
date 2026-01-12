"""Tests for Apollo enricher module."""
import pytest
from unittest.mock import patch, MagicMock
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestSearchApollo:
    """Tests for search_apollo function."""

    @patch('scripts.apollo_enricher.requests.post')
    def test_search_apollo_success(self, mock_post):
        """Test successful Apollo search returns emails and contacts."""
        from scripts.apollo_enricher import search_apollo

        # Mock search response
        search_response = MagicMock()
        search_response.ok = True
        search_response.json.return_value = {
            "people": [
                {
                    "id": "person_1",
                    "first_name": "John",
                    "has_email": True,
                    "organization": {"name": "Test Company"}
                },
                {
                    "id": "person_2",
                    "first_name": "Jane",
                    "has_email": True,
                    "organization": {"name": "Test Company"}
                }
            ]
        }

        # Mock match responses (one per person)
        match_response_1 = MagicMock()
        match_response_1.ok = True
        match_response_1.json.return_value = {
            "person": {
                "email": "john.doe@testcompany.com",
                "name": "John Doe",
                "title": "CEO",
                "linkedin_url": "https://linkedin.com/in/johndoe"
            }
        }

        match_response_2 = MagicMock()
        match_response_2.ok = True
        match_response_2.json.return_value = {
            "person": {
                "email": "jane.smith@testcompany.com",
                "name": "Jane Smith",
                "title": "Marketing Director",
                "linkedin_url": None
            }
        }

        mock_post.side_effect = [search_response, match_response_1, match_response_2]

        result = search_apollo("testcompany.com", api_key="test_key")

        assert len(result["emails"]) == 2
        assert "john.doe@testcompany.com" in result["emails"]
        assert "jane.smith@testcompany.com" in result["emails"]
        assert len(result["contacts"]) == 2
        assert result["contacts"][0]["name"] == "John Doe"
        assert result["contacts"][0]["title"] == "CEO"

    @patch('scripts.apollo_enricher.requests.post')
    def test_search_apollo_no_people(self, mock_post):
        """Test Apollo search with no results."""
        from scripts.apollo_enricher import search_apollo

        search_response = MagicMock()
        search_response.ok = True
        search_response.json.return_value = {"people": []}

        mock_post.return_value = search_response

        result = search_apollo("unknowndomain.com", api_key="test_key")

        assert result["emails"] == []
        assert result["contacts"] == []

    @patch('scripts.apollo_enricher.requests.post')
    def test_search_apollo_api_error(self, mock_post):
        """Test Apollo search handles API errors gracefully."""
        from scripts.apollo_enricher import search_apollo

        search_response = MagicMock()
        search_response.ok = False
        search_response.status_code = 401

        mock_post.return_value = search_response

        result = search_apollo("testcompany.com", api_key="invalid_key")

        assert result["emails"] == []
        assert result["contacts"] == []

    def test_search_apollo_no_api_key(self):
        """Test Apollo search without API key."""
        from scripts.apollo_enricher import search_apollo

        result = search_apollo("testcompany.com", api_key=None)

        assert result["emails"] == []
        assert result["contacts"] == []

    @patch('scripts.apollo_enricher.requests.post')
    def test_search_apollo_timeout(self, mock_post):
        """Test Apollo search handles timeout."""
        from scripts.apollo_enricher import search_apollo
        import requests

        mock_post.side_effect = requests.exceptions.Timeout()

        result = search_apollo("testcompany.com", api_key="test_key")

        assert result["emails"] == []
        assert result["contacts"] == []


class TestEnrichWithApollo:
    """Tests for enrich_with_apollo function."""

    @patch('scripts.apollo_enricher.search_apollo')
    @patch('scripts.apollo_enricher.verify_with_hunter')
    def test_enrich_with_apollo_valid_email(self, mock_hunter, mock_search):
        """Test enrichment with valid email found."""
        from scripts.apollo_enricher import enrich_with_apollo

        mock_search.return_value = {
            "emails": ["john@company.com"],
            "contacts": [{
                "name": "John Doe",
                "title": "CEO",
                "email": "john@company.com",
                "linkedin_url": "https://linkedin.com/in/johndoe"
            }]
        }

        mock_hunter.return_value = {
            "status": "valid",
            "score": 95
        }

        result = enrich_with_apollo("Test Company", "https://company.com")

        assert result["email"] == "john@company.com"
        assert result["name"] == "John Doe"
        assert result["position"] == "CEO"
        assert result["hunter_status"] == "valid"
        assert result["hunter_score"] == 95
        assert result["stage_found"] == "apollo"

    @patch('scripts.apollo_enricher.search_apollo')
    @patch('scripts.apollo_enricher.verify_with_hunter')
    def test_enrich_with_apollo_invalid_email(self, mock_hunter, mock_search):
        """Test enrichment when email fails Hunter verification."""
        from scripts.apollo_enricher import enrich_with_apollo

        mock_search.return_value = {
            "emails": ["fake@company.com"],
            "contacts": [{
                "name": "Fake Person",
                "title": "Unknown",
                "email": "fake@company.com",
                "linkedin_url": None
            }]
        }

        mock_hunter.return_value = {
            "status": "invalid",
            "score": 0
        }

        result = enrich_with_apollo("Test Company", "https://company.com")

        assert result["email"] == "fake@company.com"
        assert result["hunter_status"] == "invalid"
        assert result["hunter_score"] == 0

    @patch('scripts.apollo_enricher.search_apollo')
    def test_enrich_with_apollo_no_results(self, mock_search):
        """Test enrichment when Apollo finds nothing."""
        from scripts.apollo_enricher import enrich_with_apollo

        mock_search.return_value = {"emails": [], "contacts": []}

        result = enrich_with_apollo("Unknown Company", "https://unknown.com")

        assert result["email"] is None
        assert result["hunter_status"] is None
        assert result["stage_found"] == "apollo"

    @patch('scripts.apollo_enricher.search_apollo')
    @patch('scripts.apollo_enricher.verify_with_hunter')
    def test_enrich_with_apollo_multiple_emails_picks_best(self, mock_hunter, mock_search):
        """Test enrichment picks best verified email from multiple."""
        from scripts.apollo_enricher import enrich_with_apollo

        mock_search.return_value = {
            "emails": ["invalid@company.com", "valid@company.com"],
            "contacts": [
                {"name": "Invalid", "title": "Unknown", "email": "invalid@company.com", "linkedin_url": None},
                {"name": "Valid Person", "title": "Manager", "email": "valid@company.com", "linkedin_url": None}
            ]
        }

        # First email invalid, second valid
        mock_hunter.side_effect = [
            {"status": "invalid", "score": 0},
            {"status": "valid", "score": 90}
        ]

        result = enrich_with_apollo("Test Company", "https://company.com")

        assert result["email"] == "valid@company.com"
        assert result["hunter_status"] == "valid"
        assert result["name"] == "Valid Person"


class TestExtractDomain:
    """Tests for extract_domain helper."""

    def test_extract_domain_simple(self):
        """Test domain extraction from simple URL."""
        from scripts.apollo_enricher import extract_domain

        assert extract_domain("https://example.com") == "example.com"
        assert extract_domain("http://example.com/page") == "example.com"

    def test_extract_domain_with_www(self):
        """Test domain extraction removes www."""
        from scripts.apollo_enricher import extract_domain

        assert extract_domain("https://www.example.com") == "example.com"

    def test_extract_domain_subdomain(self):
        """Test domain extraction with subdomain."""
        from scripts.apollo_enricher import extract_domain

        assert extract_domain("https://blog.example.com") == "blog.example.com"

    def test_extract_domain_invalid(self):
        """Test domain extraction with invalid input."""
        from scripts.apollo_enricher import extract_domain

        assert extract_domain(None) is None
        assert extract_domain("") is None


class TestSearchApolloAlternatives:
    """Tests for search_apollo_alternatives function (for bounce recovery)."""

    @patch('scripts.apollo_enricher.search_apollo')
    def test_search_alternatives_excludes_original(self, mock_search):
        """Test that original bounced email is excluded from alternatives."""
        from scripts.apollo_enricher import search_apollo_alternatives

        mock_search.return_value = {
            "emails": ["original@company.com", "alternative@company.com"],
            "contacts": [
                {"name": "Original", "title": "CEO", "email": "original@company.com", "linkedin_url": None},
                {"name": "Alternative", "title": "Manager", "email": "alternative@company.com", "linkedin_url": None}
            ]
        }

        result = search_apollo_alternatives("company.com", exclude_email="original@company.com")

        assert len(result) == 1
        assert result[0]["email"] == "alternative@company.com"
        assert result[0]["source"] == "apollo"

    @patch('scripts.apollo_enricher.search_apollo')
    def test_search_alternatives_no_results(self, mock_search):
        """Test alternatives search with no results."""
        from scripts.apollo_enricher import search_apollo_alternatives

        mock_search.return_value = {"emails": [], "contacts": []}

        result = search_apollo_alternatives("unknown.com")

        assert result == []


# Integration test - only runs if APOLLO_API_KEY is set
@pytest.mark.skipif(
    not os.getenv('APOLLO_API_KEY'),
    reason="APOLLO_API_KEY not set"
)
class TestApolloIntegration:
    """Integration tests with real Apollo API."""

    def test_real_apollo_search(self):
        """Test real Apollo API call."""
        from scripts.apollo_enricher import search_apollo

        api_key = os.getenv('APOLLO_API_KEY')
        result = search_apollo("hubspot.com", api_key=api_key)

        # HubSpot should have results in Apollo's database
        assert isinstance(result["emails"], list)
        assert isinstance(result["contacts"], list)
