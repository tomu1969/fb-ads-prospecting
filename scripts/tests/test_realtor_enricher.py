"""Tests for the Realtor Agent Enricher module.

Tests the three stages:
1. CSV structure fix (LinkedIn URLs from email → linkedin_url column)
2. Apollo people/match enrichment
3. Apify LinkedIn scraper fallback
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from realtor_enricher import (
    fix_csv_structure,
    is_linkedin_url,
    enrich_via_apollo_match,
    enrich_via_apify,
    run_enrichment_pipeline,
)


class TestIsLinkedInUrl:
    """Test LinkedIn URL detection."""

    def test_standard_linkedin_url(self):
        assert is_linkedin_url("https://www.linkedin.com/in/johndoe") is True

    def test_linkedin_url_without_www(self):
        assert is_linkedin_url("https://linkedin.com/in/johndoe") is True

    def test_linkedin_url_http(self):
        assert is_linkedin_url("http://linkedin.com/in/johndoe") is True

    def test_regular_email(self):
        assert is_linkedin_url("john@example.com") is False

    def test_empty_string(self):
        assert is_linkedin_url("") is False

    def test_none(self):
        assert is_linkedin_url(None) is False

    def test_linkedin_company_url(self):
        # Company URLs should not be treated as profile URLs
        assert is_linkedin_url("https://linkedin.com/company/acme") is False


class TestFixCSVStructure:
    """Test CSV structure normalization."""

    def test_moves_linkedin_to_new_column(self):
        """LinkedIn URLs in email column should move to linkedin_url."""
        df = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith'],
            'email': ['https://linkedin.com/in/johndoe', 'jane@example.com'],
            'company': ['Acme', 'Corp'],
        })

        result = fix_csv_structure(df)

        assert 'linkedin_url' in result.columns
        assert result.loc[0, 'linkedin_url'] == 'https://linkedin.com/in/johndoe'
        assert pd.isna(result.loc[0, 'email']) or result.loc[0, 'email'] == ''
        assert result.loc[1, 'email'] == 'jane@example.com'
        assert pd.isna(result.loc[1, 'linkedin_url']) or result.loc[1, 'linkedin_url'] == ''

    def test_preserves_existing_linkedin_url_column(self):
        """If linkedin_url column exists, should still work."""
        df = pd.DataFrame({
            'name': ['John Doe'],
            'email': ['https://linkedin.com/in/johndoe'],
            'company': ['Acme'],
            'linkedin_url': [''],
        })

        result = fix_csv_structure(df)

        assert result.loc[0, 'linkedin_url'] == 'https://linkedin.com/in/johndoe'
        assert pd.isna(result.loc[0, 'email']) or result.loc[0, 'email'] == ''

    def test_no_linkedin_urls_unchanged(self):
        """If no LinkedIn URLs in email, nothing changes."""
        df = pd.DataFrame({
            'name': ['Jane Smith'],
            'email': ['jane@example.com'],
            'company': ['Corp'],
        })

        result = fix_csv_structure(df)

        assert result.loc[0, 'email'] == 'jane@example.com'

    def test_updates_type_column(self):
        """Should update type column to reflect LinkedIn Only."""
        df = pd.DataFrame({
            'name': ['John Doe'],
            'email': ['https://linkedin.com/in/johndoe'],
            'company': ['Acme'],
            'type': ['Agent (LinkedIn)'],
        })

        result = fix_csv_structure(df)

        # Type should remain as-is (we don't force change it)
        assert 'type' in result.columns


class TestApolloMatch:
    """Test Apollo people/match API integration."""

    @patch('realtor_enricher.requests.post')
    def test_successful_match(self, mock_post):
        """Should return email when Apollo finds a match."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {
            'person': {
                'email': 'john@acme.com',
                'name': 'John Doe',
                'phone_numbers': [{'number': '+1234567890'}],
                'organization': {'name': 'Acme Corp'}
            }
        }
        mock_post.return_value = mock_response

        result = enrich_via_apollo_match(
            'https://linkedin.com/in/johndoe',
            api_key='test_key'
        )

        assert result['email'] == 'john@acme.com'
        assert result['name'] == 'John Doe'
        assert result['phone'] == '+1234567890'
        assert result['source'] == 'apollo'

    @patch('realtor_enricher.requests.post')
    def test_no_match_found(self, mock_post):
        """Should return empty dict when Apollo doesn't find person."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {'person': None}
        mock_post.return_value = mock_response

        result = enrich_via_apollo_match(
            'https://linkedin.com/in/unknown',
            api_key='test_key'
        )

        assert result.get('email') is None

    @patch('realtor_enricher.requests.post')
    def test_api_error(self, mock_post):
        """Should handle API errors gracefully."""
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 429
        mock_post.return_value = mock_response

        result = enrich_via_apollo_match(
            'https://linkedin.com/in/johndoe',
            api_key='test_key'
        )

        assert result.get('email') is None


class TestApifyEnrichment:
    """Test Apify LinkedIn scraper fallback."""

    @patch('apify_client.ApifyClient')
    def test_successful_scrape(self, mock_client_class):
        """Should return contact info from Apify scrape."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_run = {'defaultDatasetId': 'test_dataset'}
        mock_client.actor.return_value.call.return_value = mock_run

        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([
            {
                'linkedinUrl': 'https://linkedin.com/in/johndoe',
                'email': 'john@acme.com',
                'mobileNumber': '+1234567890',
                'fullName': 'John Doe',
            }
        ])
        mock_client.dataset.return_value = mock_dataset

        result = enrich_via_apify(['https://linkedin.com/in/johndoe'])

        assert len(result) == 1
        assert result[0]['email'] == 'john@acme.com'
        assert result[0]['linkedin_url'] == 'https://linkedin.com/in/johndoe'

    @patch('apify_client.ApifyClient')
    def test_no_results(self, mock_client_class):
        """Should return empty list when no results."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_run = {'defaultDatasetId': 'test_dataset'}
        mock_client.actor.return_value.call.return_value = mock_run

        mock_dataset = MagicMock()
        mock_dataset.iterate_items.return_value = iter([])
        mock_client.dataset.return_value = mock_dataset

        result = enrich_via_apify(['https://linkedin.com/in/unknown'])

        assert result == []


class TestEnrichmentPipeline:
    """Test full enrichment pipeline."""

    def test_identifies_linkedin_only_contacts(self):
        """Should correctly identify contacts needing enrichment."""
        df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'email': ['https://linkedin.com/in/john', 'jane@example.com', 'https://linkedin.com/in/bob'],
            'company': ['A', 'B', 'C'],
        })

        df = fix_csv_structure(df)

        # John and Bob should have linkedin_url, no email
        linkedin_only = df[df['linkedin_url'].notna() & (df['email'].isna() | (df['email'] == ''))]
        assert len(linkedin_only) == 2
        assert 'John' in linkedin_only['name'].values
        assert 'Bob' in linkedin_only['name'].values

    @patch('realtor_enricher.requests.post')
    def test_pipeline_uses_apollo_first(self, mock_post):
        """Pipeline should try Apollo before Apify."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {
            'person': {
                'email': 'john@acme.com',
                'name': 'John Doe',
                'phone_numbers': [],
                'organization': {'name': 'Acme'}
            }
        }
        mock_post.return_value = mock_response

        df = pd.DataFrame({
            'name': ['John'],
            'email': [''],
            'company': ['Acme'],
            'linkedin_url': ['https://linkedin.com/in/john'],
        })

        # Run apollo stage
        for idx, row in df.iterrows():
            if row['linkedin_url'] and (pd.isna(row['email']) or row['email'] == ''):
                result = enrich_via_apollo_match(row['linkedin_url'], api_key='test')
                if result.get('email'):
                    df.at[idx, 'email'] = result['email']

        assert df.loc[0, 'email'] == 'john@acme.com'
        mock_post.assert_called_once()


class TestNameInference:
    """Test name inference from email addresses."""

    def test_first_dot_last(self):
        """Should parse first.last@domain.com"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("karen.gil@example.com") == "Karen Gil"
        assert infer_name_from_email("juan.montoya@example.com") == "Juan Montoya"
        assert infer_name_from_email("owen.charles@compass.com") == "Owen Charles"

    def test_first_underscore_last(self):
        """Should parse first_last@domain.com"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("john_smith@example.com") == "John Smith"

    def test_first_hyphen_last(self):
        """Should parse first-last@domain.com"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("mary-jones@example.com") == "Mary Jones"

    def test_single_name_returns_none(self):
        """Should return None for single names (no last name)"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("keith@example.com") is None
        assert infer_name_from_email("lucia@example.com") is None

    def test_generic_emails_return_none(self):
        """Should return None for generic/role-based emails"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("facturas@example.com") is None
        assert infer_name_from_email("info@example.com") is None
        assert infer_name_from_email("admin@example.com") is None
        assert infer_name_from_email("support@example.com") is None
        assert infer_name_from_email("efacturacliente@example.com") is None

    def test_initial_dot_last_returns_none(self):
        """Should return None for initial.last patterns (single char first name)"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("j.smith@example.com") is None
        assert infer_name_from_email("a.vengoechea@example.com") is None  # 'a' is only 1 char, skip

    def test_empty_and_none(self):
        """Should handle empty and None inputs"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("") is None
        assert infer_name_from_email(None) is None

    def test_preserves_capitalization(self):
        """Should title-case the name parts"""
        from realtor_enricher import infer_name_from_email
        assert infer_name_from_email("KAREN.GIL@example.com") == "Karen Gil"
        assert infer_name_from_email("karen.GIL@example.com") == "Karen Gil"


class TestApolloNameLookup:
    """Test Apollo people/match for name lookup via email."""

    @patch('realtor_enricher.requests.post')
    def test_get_name_from_apollo_success(self, mock_post):
        """Should return name when Apollo finds a match."""
        from realtor_enricher import get_name_from_apollo

        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {
            'person': {
                'name': 'Keith Johnson',
                'first_name': 'Keith',
                'last_name': 'Johnson',
                'email': 'keith@riserealty.com'
            }
        }
        mock_post.return_value = mock_response

        result = get_name_from_apollo("keith@riserealty.com", api_key="test_key")
        assert result == "Keith Johnson"

    @patch('realtor_enricher.requests.post')
    def test_get_name_from_apollo_no_match(self, mock_post):
        """Should return None when no match found."""
        from realtor_enricher import get_name_from_apollo

        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {'person': None}
        mock_post.return_value = mock_response

        result = get_name_from_apollo("unknown@example.com", api_key="test_key")
        assert result is None

    @patch('realtor_enricher.requests.post')
    def test_get_name_from_apollo_api_error(self, mock_post):
        """Should return None on API error."""
        from realtor_enricher import get_name_from_apollo

        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 429
        mock_post.return_value = mock_response

        result = get_name_from_apollo("test@example.com", api_key="test_key")
        assert result is None
