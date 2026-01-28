"""Tests for location enricher — Slices 1-3: ccTLD, Company Geo, Apollo API."""

from unittest.mock import MagicMock, call, patch

import pytest


# ============================================================
# Slice 1: ccTLD → Country mapping
# ============================================================


class TestCctldToCountry:
    """Tests for cctld_to_country lookup."""

    def test_co_returns_colombia(self):
        """'.co' should map to 'Colombia'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('co') == 'Colombia'

    def test_mx_returns_mexico(self):
        """'.mx' should map to 'Mexico'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('mx') == 'Mexico'

    def test_edu_returns_usa(self):
        """'.edu' should map to 'United States'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('edu') == 'United States'

    def test_com_returns_none(self):
        """'.com' is generic and should return None."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('com') is None

    def test_net_returns_none(self):
        """'.net' is generic and should return None."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('net') is None

    def test_org_returns_none(self):
        """'.org' is generic and should return None."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('org') is None

    def test_br_returns_brazil(self):
        """'.br' should map to 'Brazil'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('br') == 'Brazil'

    def test_uk_returns_united_kingdom(self):
        """'.uk' should map to 'United Kingdom'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('uk') == 'United Kingdom'

    def test_unknown_tld_returns_none(self):
        """Unknown TLD should return None."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        assert cctld_to_country('xyz') is None


class TestExtractCountryFromEmail:
    """Tests for extract_country_from_email function."""

    def test_com_co_returns_colombia(self):
        """'john@company.com.co' should return 'Colombia' (compound TLD)."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('john@company.com.co') == 'Colombia'

    def test_gmail_com_returns_none(self):
        """'bob@gmail.com' should return None (generic TLD)."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('bob@gmail.com') is None

    def test_co_uk_returns_uk(self):
        """'jane@company.co.uk' should return 'United Kingdom'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('jane@company.co.uk') == 'United Kingdom'

    def test_com_mx_returns_mexico(self):
        """'carlos@empresa.com.mx' should return 'Mexico'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('carlos@empresa.com.mx') == 'Mexico'

    def test_com_br_returns_brazil(self):
        """'ana@empresa.com.br' should return 'Brazil'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('ana@empresa.com.br') == 'Brazil'

    def test_edu_returns_usa(self):
        """'student@mit.edu' should return 'United States'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('student@mit.edu') == 'United States'

    def test_gov_returns_usa(self):
        """'employee@agency.gov' should return 'United States'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('employee@agency.gov') == 'United States'

    def test_plain_co_returns_colombia(self):
        """'maria@startup.co' should return 'Colombia'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('maria@startup.co') == 'Colombia'

    def test_com_ar_returns_argentina(self):
        """'pablo@empresa.com.ar' should return 'Argentina'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('pablo@empresa.com.ar') == 'Argentina'

    def test_empty_email_returns_none(self):
        """Empty or None email should return None."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('') is None
        assert extract_country_from_email(None) is None

    def test_invalid_email_returns_none(self):
        """Email without @ should return None."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('not-an-email') is None

    def test_synthetic_li_email_returns_none(self):
        """Synthetic li:// emails should return None."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('li://john-smith-123') is None


class TestEduCoNotUsa:
    """Ensure .edu.co is treated as Colombia, not USA."""

    def test_edu_co_returns_colombia(self):
        """'.edu.co' should map to 'Colombia', not 'United States'."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('student@universidad.edu.co') == 'Colombia'

    def test_edu_co_compound_tld_priority(self):
        """Compound TLD '.edu.co' should take priority over simple '.edu'."""
        from scripts.contact_intel.location_enricher import cctld_to_country

        # edu.co explicitly maps to Colombia
        assert cctld_to_country('edu.co') == 'Colombia'
        # edu alone maps to USA
        assert cctld_to_country('edu') == 'United States'

    def test_gov_co_returns_colombia(self):
        """'.gov.co' should map to 'Colombia' (gov + co compound)."""
        from scripts.contact_intel.location_enricher import extract_country_from_email

        assert extract_country_from_email('oficina@mintic.gov.co') == 'Colombia'


class TestEnrichCctldNodes:
    """Tests for enrich_cctld function — mock Neo4j session."""

    def test_enriches_matching_emails(self):
        """Should SET country and location_source on Person nodes with ccTLD emails."""
        from scripts.contact_intel.location_enricher import enrich_cctld

        mock_session = MagicMock()

        # Simulate the first query returning emails without country
        mock_record_1 = {'email': 'juan@empresa.com.co'}
        mock_record_2 = {'email': 'bob@gmail.com'}
        mock_record_3 = {'email': 'ana@empresa.com.mx'}

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            mock_record_1, mock_record_2, mock_record_3
        ]))

        # First call returns the email list, subsequent calls are SET operations
        mock_session.run.side_effect = [mock_result, MagicMock(), MagicMock()]

        count = enrich_cctld(mock_session)

        # Should have enriched 2 emails (com.co → Colombia, com.mx → Mexico)
        # bob@gmail.com has .com which returns None
        assert count == 2

    def test_skips_generic_tlds(self):
        """Should NOT set country for .com, .net, .org emails."""
        from scripts.contact_intel.location_enricher import enrich_cctld

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'user@company.com'},
            {'email': 'user@org.net'},
            {'email': 'user@nonprofit.org'},
        ]))

        mock_session.run.side_effect = [mock_result]

        count = enrich_cctld(mock_session)

        assert count == 0
        # Only the initial query should have been run (no SET calls)
        assert mock_session.run.call_count == 1

    def test_cypher_query_structure(self):
        """Should use the correct Cypher query to find emails without country."""
        from scripts.contact_intel.location_enricher import enrich_cctld

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_result

        enrich_cctld(mock_session)

        # Verify the initial query
        first_call = mock_session.run.call_args_list[0]
        query = first_call[0][0]

        assert 'Person' in query
        assert 'country IS NULL' in query
        assert 'primary_email IS NOT NULL' in query
        assert "STARTS WITH 'li://'" in query

    def test_set_query_uses_correct_params(self):
        """Should SET with correct country and location_source='cctld'."""
        from scripts.contact_intel.location_enricher import enrich_cctld

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'carlos@empresa.com.mx'},
        ]))

        mock_session.run.side_effect = [mock_result, MagicMock()]

        enrich_cctld(mock_session)

        # Second call should be the SET operation
        set_call = mock_session.run.call_args_list[1]
        query = set_call[0][0]
        params = set_call[1] if len(set_call) > 1 else set_call[0][1] if len(set_call[0]) > 1 else {}

        # Check the query has SET
        assert 'SET' in query
        assert 'country' in query
        assert 'location_source' in query

        # Check the params have correct values
        # Params may be passed as kwargs or as second positional arg
        all_params = {}
        if len(set_call[0]) > 1:
            all_params = set_call[0][1]
        elif set_call[1]:
            all_params = set_call[1]

        assert all_params.get('email') == 'carlos@empresa.com.mx'
        assert all_params.get('country') == 'Mexico'

    def test_returns_zero_when_no_emails(self):
        """Should return 0 when no Person nodes need enrichment."""
        from scripts.contact_intel.location_enricher import enrich_cctld

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_result

        count = enrich_cctld(mock_session)

        assert count == 0


# ============================================================
# Slice 2: Company Name → City/Country geo extraction
# ============================================================


class TestExtractGeoFromCompany:
    """Tests for extract_geo_from_company function."""

    def test_extract_geo_medellin(self):
        """'Empresas Públicas de Medellín' → Medellín, Colombia."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Empresas Públicas de Medellín')
        assert result['city'] == 'Medellín'
        assert result['country'] == 'Colombia'

    def test_extract_geo_bogota(self):
        """'Banco de Bogotá' → Bogotá, Colombia."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Banco de Bogotá')
        assert result['city'] == 'Bogotá'
        assert result['country'] == 'Colombia'

    def test_extract_geo_miami(self):
        """'CBRE Miami' → Miami, Florida, United States."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('CBRE Miami')
        assert result['city'] == 'Miami'
        assert result['state'] == 'Florida'
        assert result['country'] == 'United States'

    def test_extract_geo_country_only(self):
        """'Bancolombia' → country Colombia (has 'colombia' in name)."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Bancolombia')
        assert result['country'] == 'Colombia'

    def test_extract_geo_no_match(self):
        """'Google' → empty dict (no geo pattern)."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Google')
        assert result == {}

    def test_case_insensitive(self):
        """'CBRE MIAMI' (uppercase) → Miami, Florida, United States."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('CBRE MIAMI')
        assert result['city'] == 'Miami'
        assert result['state'] == 'Florida'
        assert result['country'] == 'United States'

    def test_empty_company_returns_empty(self):
        """Empty or None company name should return empty dict."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        assert extract_geo_from_company('') == {}
        assert extract_geo_from_company(None) == {}

    def test_medellin_without_accent(self):
        """'Medellin' without accent should still match."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Real Estate Medellin')
        assert result['city'] == 'Medellín'
        assert result['country'] == 'Colombia'

    def test_bogota_without_accent(self):
        """'Bogota' without accent should still match."""
        from scripts.contact_intel.location_enricher import extract_geo_from_company

        result = extract_geo_from_company('Inversiones Bogota')
        assert result['city'] == 'Bogotá'
        assert result['country'] == 'Colombia'


class TestEnrichCompanyGeoNodes:
    """Tests for enrich_company_geo function — mock Neo4j session."""

    def test_enrich_company_geo_nodes(self):
        """Should SET city/state/country on synthetic LinkedIn nodes."""
        from scripts.contact_intel.location_enricher import enrich_company_geo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'li://juan-gomez-123', 'company': 'CBRE Miami'},
            {'email': 'li-name://ana-lopez', 'company': 'Google'},
        ]))

        mock_session.run.side_effect = [mock_result, MagicMock()]

        count = enrich_company_geo(mock_session)

        # Only CBRE Miami should match (Google has no geo)
        assert count == 1

        # Verify the SET query was called with correct params
        set_call = mock_session.run.call_args_list[1]
        query = set_call[0][0]
        params = set_call[1]

        assert 'SET' in query
        assert 'location_source' in query
        assert params['email'] == 'li://juan-gomez-123'
        assert params['city'] == 'Miami'
        assert params['state'] == 'Florida'
        assert params['country'] == 'United States'

    def test_no_overwrite_existing(self):
        """Node with existing city should NOT be overwritten by the Cypher query.

        The Cypher uses CASE WHEN p.city IS NULL THEN $city ELSE p.city END,
        so existing values are preserved. We verify the query structure enforces this.
        """
        from scripts.contact_intel.location_enricher import SET_COMPANY_GEO_QUERY

        # The SET query should use CASE WHEN ... IS NULL for each field
        assert 'CASE WHEN p.city IS NULL THEN $city ELSE p.city END' in SET_COMPANY_GEO_QUERY
        assert 'CASE WHEN p.state IS NULL THEN $state ELSE p.state END' in SET_COMPANY_GEO_QUERY
        assert 'CASE WHEN p.country IS NULL THEN $country ELSE p.country END' in SET_COMPANY_GEO_QUERY
        assert 'CASE WHEN p.location_source IS NULL THEN' in SET_COMPANY_GEO_QUERY

    def test_returns_zero_when_no_synthetic_nodes(self):
        """Should return 0 when no synthetic nodes need enrichment."""
        from scripts.contact_intel.location_enricher import enrich_company_geo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_result

        count = enrich_company_geo(mock_session)

        assert count == 0

    def test_find_query_targets_synthetic_nodes(self):
        """The FIND query should only target li:// and li-name:// nodes."""
        from scripts.contact_intel.location_enricher import FIND_COMPANY_GEO_QUERY

        assert "STARTS WITH 'li://'" in FIND_COMPANY_GEO_QUERY
        assert "STARTS WITH 'li-name://'" in FIND_COMPANY_GEO_QUERY
        assert 'linkedin_company IS NOT NULL' in FIND_COMPANY_GEO_QUERY
        assert 'city IS NULL' in FIND_COMPANY_GEO_QUERY


# ============================================================
# Slice 3: Apollo API → Full Location enrichment
# ============================================================


class TestApolloLocationLookup:
    """Tests for get_location_from_apollo function."""

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_apollo_location_lookup(self, mock_post):
        """Mock successful API response with city/state/country -> correct extraction."""
        from scripts.contact_intel.location_enricher import get_location_from_apollo

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'person': {
                'city': 'Miami',
                'state': 'Florida',
                'country': 'United States',
            }
        }
        mock_post.return_value = mock_response

        result = get_location_from_apollo(
            'https://www.linkedin.com/in/john-smith-123',
            api_key='test-key',
        )

        assert result == {'city': 'Miami', 'state': 'Florida', 'country': 'United States'}
        mock_post.assert_called_once_with(
            'https://api.apollo.io/api/v1/people/match',
            json={'linkedin_url': 'https://www.linkedin.com/in/john-smith-123'},
            headers={'x-api-key': 'test-key', 'Content-Type': 'application/json'},
            timeout=10,
        )

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_apollo_missing_location(self, mock_post):
        """API returns no location fields -> returns empty dict."""
        from scripts.contact_intel.location_enricher import get_location_from_apollo

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'person': {
                'first_name': 'John',
                'last_name': 'Smith',
            }
        }
        mock_post.return_value = mock_response

        result = get_location_from_apollo(
            'https://www.linkedin.com/in/john-smith-123',
            api_key='test-key',
        )

        assert result == {}

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_apollo_api_error(self, mock_post):
        """requests raises exception -> returns empty dict, logs error."""
        from scripts.contact_intel.location_enricher import get_location_from_apollo

        mock_post.side_effect = Exception('Connection timeout')

        result = get_location_from_apollo(
            'https://www.linkedin.com/in/john-smith-123',
            api_key='test-key',
        )

        assert result == {}

    @patch('scripts.contact_intel.location_enricher.time.sleep')
    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_apollo_rate_limit(self, mock_post, mock_sleep):
        """Verify time.sleep(0.5) is called between requests in enrich_apollo."""
        from scripts.contact_intel.location_enricher import enrich_apollo

        mock_session = MagicMock()

        # Return 2 nodes to enrich
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'li://juan-123', 'url': 'https://linkedin.com/in/juan-123', 'company': 'Acme'},
            {'email': 'li://ana-456', 'url': 'https://linkedin.com/in/ana-456', 'company': 'Corp'},
        ]))
        mock_session.run.side_effect = [mock_result, MagicMock(), MagicMock()]

        # Apollo returns location for both
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'person': {'city': 'Miami', 'state': 'Florida', 'country': 'United States'}
        }
        mock_post.return_value = mock_response

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            enrich_apollo(mock_session, dry_run=False)

        # sleep should be called between API calls (after each call)
        assert mock_sleep.call_count >= 1
        mock_sleep.assert_called_with(0.5)


class TestEnrichApollo:
    """Tests for enrich_apollo function — mock Neo4j session + API."""

    @patch('scripts.contact_intel.location_enricher.time.sleep')
    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_enrich_apollo_enriches_nodes(self, mock_post, mock_sleep):
        """Mock session + API, verify SET params."""
        from scripts.contact_intel.location_enricher import enrich_apollo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'li://juan-123', 'url': 'https://linkedin.com/in/juan-123', 'company': 'Acme'},
        ]))
        mock_session.run.side_effect = [mock_result, MagicMock()]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'person': {'city': 'Bogota', 'state': 'Cundinamarca', 'country': 'Colombia'}
        }
        mock_post.return_value = mock_response

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            stats = enrich_apollo(mock_session, dry_run=False)

        assert stats['enriched'] == 1
        assert stats['api_calls'] == 1

        # Verify the SET query was called with correct params
        set_call = mock_session.run.call_args_list[1]
        params = set_call[1]
        assert params['email'] == 'li://juan-123'
        assert params['city'] == 'Bogota'
        assert params['state'] == 'Cundinamarca'
        assert params['country'] == 'Colombia'

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_enrich_apollo_skips_already_enriched(self, mock_post):
        """Nodes with existing city are skipped (via Cypher WHERE city IS NULL)."""
        from scripts.contact_intel.location_enricher import (
            FIND_APOLLO_TARGETS_QUERY,
            enrich_apollo,
        )

        # The Cypher query itself filters out nodes with city IS NOT NULL
        assert 'city IS NULL' in FIND_APOLLO_TARGETS_QUERY

        mock_session = MagicMock()

        # Return empty — all nodes already have city
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_result

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            stats = enrich_apollo(mock_session, dry_run=False)

        assert stats['enriched'] == 0
        assert stats['api_calls'] == 0
        # No API calls made
        mock_post.assert_not_called()

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_enrich_apollo_dry_run(self, mock_post):
        """Verify no API calls in dry_run mode."""
        from scripts.contact_intel.location_enricher import enrich_apollo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'li://juan-123', 'url': 'https://linkedin.com/in/juan-123', 'company': 'Acme'},
        ]))
        mock_session.run.side_effect = [mock_result]

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            stats = enrich_apollo(mock_session, dry_run=True)

        # No API calls should be made in dry_run mode
        mock_post.assert_not_called()
        assert stats['api_calls'] == 0
        assert stats['skipped'] == 1

    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_enrich_apollo_industry_filter(self, mock_post):
        """Verify Cypher includes industry filter when specified."""
        from scripts.contact_intel.location_enricher import enrich_apollo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_result

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            enrich_apollo(mock_session, industry_filter='real_estate', dry_run=False)

        # Verify the FIND query was called with industry param
        find_call = mock_session.run.call_args_list[0]
        params = find_call[1]
        assert params['industry'] == 'real_estate'

    @patch('scripts.contact_intel.location_enricher.time.sleep')
    @patch('scripts.contact_intel.location_enricher.requests.post')
    def test_enrich_apollo_returns_stats(self, mock_post, mock_sleep):
        """Returns dict with enriched, skipped, errors, api_calls, estimated_cost."""
        from scripts.contact_intel.location_enricher import enrich_apollo

        mock_session = MagicMock()

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([
            {'email': 'li://juan-123', 'url': 'https://linkedin.com/in/juan-123', 'company': 'Acme'},
            {'email': 'li://ana-456', 'url': 'https://linkedin.com/in/ana-456', 'company': 'Corp'},
        ]))
        mock_session.run.side_effect = [mock_result, MagicMock(), MagicMock()]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'person': {'city': 'Lima', 'state': 'Lima', 'country': 'Peru'}
        }
        mock_post.return_value = mock_response

        with patch.dict('os.environ', {'APOLLO_API_KEY': 'test-key'}):
            stats = enrich_apollo(mock_session, dry_run=False)

        assert 'enriched' in stats
        assert 'skipped' in stats
        assert 'errors' in stats
        assert 'api_calls' in stats
        assert 'estimated_cost' in stats
        assert stats['enriched'] == 2
        assert stats['api_calls'] == 2
        assert stats['estimated_cost'] == pytest.approx(0.04)  # 2 * $0.02
