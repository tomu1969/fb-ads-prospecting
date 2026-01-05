"""
Unit tests for fb_ads_scraper.py
"""

import sys
import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from fb_ads_scraper import (
    build_search_url,
    filter_duplicates,
    update_history,
    convert_to_pipeline_format,
    load_history,
    save_history,
    apply_housing_workaround,
    LOCATION_PRESETS,
    HOUSING_KEYWORDS,
)


class TestBuildSearchUrl:
    """Tests for build_search_url function."""

    def test_basic_query(self):
        """Test basic search URL building."""
        url = build_search_url(query="real estate", country="US", ad_type="all")
        # URL encoding can use %20 or + for spaces
        assert "q=real%20estate" in url or "q=real+estate" in url
        assert "country=US" in url
        assert "ad_type=all" in url

    def test_with_date_range(self):
        """Test URL with date parameters."""
        url = build_search_url(
            query="test",
            country="US",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        assert "start_date_min=2024-01-01" in url
        assert "start_date_max=2024-12-31" in url

    def test_special_characters(self):
        """Test URL encoding of special characters."""
        url = build_search_url(query="real estate & mortgage", country="US")
        # URL encoding can use %20 or + for spaces
        assert "real%20estate" in url or "real+estate" in url
        # & should be encoded as %26
        assert "%26" in url
        assert "mortgage" in url

    def test_different_countries(self):
        """Test different country codes."""
        url_us = build_search_url(query="test", country="US")
        url_gb = build_search_url(query="test", country="GB")

        assert "country=US" in url_us
        assert "country=GB" in url_gb

    def test_ad_types(self):
        """Test different ad types."""
        for ad_type in ['all', 'political_and_issue_ads', 'housing_ads', 'employment_ads', 'credit_ads']:
            url = build_search_url(query="test", country="US", ad_type=ad_type)
            assert f"ad_type={ad_type}" in url

    def test_empty_query(self):
        """Test URL building without keywords (browsing by filters)."""
        url = build_search_url(query='', country='US', ad_type='housing_ads')
        assert 'country=US' in url
        assert 'ad_type=housing_ads' in url
        assert 'q=' not in url  # No query parameter when empty

    def test_empty_query_default(self):
        """Test URL building with default empty query."""
        url = build_search_url(country='GB')
        assert 'country=GB' in url
        assert 'q=' not in url


class TestLocationPresets:
    """Tests for location presets."""

    def test_us_preset(self):
        """Test US preset has correct values."""
        preset = LOCATION_PRESETS['us']
        assert preset['country'] == 'US'
        assert preset['name'] == 'United States'

    def test_uk_preset(self):
        """Test UK preset has correct values."""
        preset = LOCATION_PRESETS['uk']
        assert preset['country'] == 'GB'
        assert preset['name'] == 'United Kingdom'

    def test_canada_preset(self):
        """Test Canada preset has correct values."""
        preset = LOCATION_PRESETS['canada']
        assert preset['country'] == 'CA'
        assert preset['name'] == 'Canada'

    def test_all_presets_have_required_keys(self):
        """Test all presets have required keys."""
        required_keys = ['country', 'name']
        for key, preset in LOCATION_PRESETS.items():
            for req_key in required_keys:
                assert req_key in preset, f"Preset '{key}' missing key '{req_key}'"


class TestFilterDuplicates:
    """Tests for filter_duplicates function."""

    def test_no_duplicates(self):
        """Test when no duplicates exist."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
            {'page_id': '456', 'page_name': 'Company B'},
        ]
        history = {'advertisers': {}}

        filtered, count = filter_duplicates(ads, history)

        assert len(filtered) == 2
        assert count == 0

    def test_with_duplicates(self):
        """Test when some duplicates exist."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
            {'page_id': '456', 'page_name': 'Company B'},
            {'page_id': '789', 'page_name': 'Company C'},
        ]
        history = {
            'advertisers': {
                '123': {'page_name': 'Company A', 'first_scraped': '2024-01-01'}
            }
        }

        filtered, count = filter_duplicates(ads, history)

        assert len(filtered) == 2
        assert count == 1
        # 123 should be filtered out
        page_ids = [str(ad['page_id']) for ad in filtered]
        assert '123' not in page_ids
        assert '456' in page_ids
        assert '789' in page_ids

    def test_all_duplicates(self):
        """Test when all ads are duplicates."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
            {'page_id': '456', 'page_name': 'Company B'},
        ]
        history = {
            'advertisers': {
                '123': {'page_name': 'Company A'},
                '456': {'page_name': 'Company B'},
            }
        }

        filtered, count = filter_duplicates(ads, history)

        assert len(filtered) == 0
        assert count == 2

    def test_force_flag(self):
        """Test force flag bypasses deduplication."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
        ]
        history = {
            'advertisers': {
                '123': {'page_name': 'Company A'},
            }
        }

        filtered, count = filter_duplicates(ads, history, force=True)

        assert len(filtered) == 1
        assert count == 0

    def test_alternate_key_names(self):
        """Test handling of alternate field names (pageId vs page_id)."""
        ads = [
            {'pageId': '123', 'pageName': 'Company A'},
        ]
        history = {'advertisers': {'123': {}}}

        filtered, count = filter_duplicates(ads, history)

        assert len(filtered) == 0
        assert count == 1


class TestUpdateHistory:
    """Tests for update_history function."""

    def test_add_new_advertisers(self):
        """Test adding new advertisers to history."""
        history = {'advertisers': {}, 'searches': []}
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
            {'page_id': '456', 'page_name': 'Company B'},
        ]
        config = {'query': 'real estate'}

        updated = update_history(history, ads, config, duplicates=0)

        assert '123' in updated['advertisers']
        assert '456' in updated['advertisers']
        assert updated['advertisers']['123']['page_name'] == 'Company A'
        assert updated['advertisers']['123']['scrape_count'] == 1
        assert len(updated['searches']) == 1

    def test_update_existing_advertiser(self):
        """Test updating scrape count for existing advertiser."""
        history = {
            'advertisers': {
                '123': {
                    'page_name': 'Company A',
                    'first_scraped': '2024-01-01',
                    'last_scraped': '2024-01-01',
                    'scrape_count': 1
                }
            },
            'searches': []
        }
        ads = [{'page_id': '123', 'page_name': 'Company A'}]
        config = {'query': 'test'}

        updated = update_history(history, ads, config, duplicates=0)

        assert updated['advertisers']['123']['scrape_count'] == 2
        assert updated['advertisers']['123']['first_scraped'] == '2024-01-01'

    def test_search_log(self):
        """Test search is logged correctly."""
        history = {'advertisers': {}, 'searches': []}
        ads = [{'page_id': '123', 'page_name': 'Company A'}]
        config = {'query': 'miami real estate'}

        updated = update_history(history, ads, config, duplicates=5)

        assert len(updated['searches']) == 1
        assert updated['searches'][0]['query'] == 'miami real estate'
        assert updated['searches'][0]['new_advertisers'] == 1
        assert updated['searches'][0]['duplicates_skipped'] == 5

    def test_search_log_empty_query(self):
        """Test search logging with empty query (browse by filters)."""
        history = {'advertisers': {}, 'searches': []}
        ads = [{'page_id': '123', 'page_name': 'Company A'}]
        config = {'query': '', 'location_name': 'Miami, US'}

        updated = update_history(history, ads, config, duplicates=0)

        assert len(updated['searches']) == 1
        assert updated['searches'][0]['query'] == '(browse by filters)'
        assert updated['searches'][0]['location'] == 'Miami, US'

    def test_search_log_with_location(self):
        """Test search logging includes location info."""
        history = {'advertisers': {}, 'searches': []}
        ads = [{'page_id': '123', 'page_name': 'Company A'}]
        config = {'query': 'real estate', 'location_name': 'Florida, US', 'country': 'US'}

        updated = update_history(history, ads, config, duplicates=0)

        assert updated['searches'][0]['location'] == 'Florida, US'


class TestConvertToPipelineFormat:
    """Tests for convert_to_pipeline_format function."""

    def test_basic_conversion(self):
        """Test basic conversion to pipeline format."""
        ads = [
            {
                'page_id': '123',
                'page_name': 'Test Company',
                'ad_creative_body': 'Check out our services!',
                'platforms': ['facebook', 'instagram'],
                'is_active': True,
                'ad_delivery_start_time': '2024-01-15',
            }
        ]

        df = convert_to_pipeline_format(ads)

        assert len(df) == 1
        assert df.iloc[0]['page_name'] == 'Test Company'
        assert df.iloc[0]['ad_count'] == 1
        assert df.iloc[0]['is_active'] == True

    def test_required_columns(self):
        """Test all required columns are present."""
        ads = [{'page_id': '123', 'page_name': 'Test'}]

        df = convert_to_pipeline_format(ads)

        required_cols = ['page_name', 'ad_count', 'ad_texts', 'platforms', 'is_active', 'first_ad_date']
        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

    def test_multiple_ads_same_advertiser(self):
        """Test grouping multiple ads from same advertiser."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A', 'ad_creative_body': 'Ad 1'},
            {'page_id': '123', 'page_name': 'Company A', 'ad_creative_body': 'Ad 2'},
            {'page_id': '123', 'page_name': 'Company A', 'ad_creative_body': 'Ad 3'},
        ]

        df = convert_to_pipeline_format(ads)

        assert len(df) == 1  # Should be grouped
        assert df.iloc[0]['ad_count'] == 3
        ad_texts = json.loads(df.iloc[0]['ad_texts'])
        assert len(ad_texts) == 3

    def test_multiple_advertisers(self):
        """Test multiple different advertisers."""
        ads = [
            {'page_id': '123', 'page_name': 'Company A'},
            {'page_id': '456', 'page_name': 'Company B'},
        ]

        df = convert_to_pipeline_format(ads)

        assert len(df) == 2

    def test_alternate_field_names(self):
        """Test handling of alternate field names."""
        ads = [
            {
                'pageId': '123',
                'pageName': 'Test Company',
                'adCreativeBody': 'Ad text here',
                'publisherPlatform': 'facebook',
                'isActive': True,
            }
        ]

        df = convert_to_pipeline_format(ads)

        assert len(df) == 1
        assert df.iloc[0]['page_name'] == 'Test Company'

    def test_platforms_json_format(self):
        """Test platforms are stored as JSON."""
        ads = [
            {'page_id': '123', 'page_name': 'Test', 'platforms': ['facebook', 'instagram']},
        ]

        df = convert_to_pipeline_format(ads)

        platforms = json.loads(df.iloc[0]['platforms'])
        assert 'facebook' in platforms
        assert 'instagram' in platforms

    def test_ad_texts_limit(self):
        """Test ad texts are limited to 10."""
        ads = [{'page_id': '123', 'page_name': 'Test', 'ad_creative_body': f'Ad {i}'} for i in range(15)]

        df = convert_to_pipeline_format(ads)

        ad_texts = json.loads(df.iloc[0]['ad_texts'])
        assert len(ad_texts) <= 10


class TestHistoryFileOperations:
    """Tests for history file operations."""

    def test_load_empty_history(self, tmp_path):
        """Test loading when no history file exists."""
        import fb_ads_scraper
        original_file = fb_ads_scraper.HISTORY_FILE
        fb_ads_scraper.HISTORY_FILE = tmp_path / 'nonexistent.json'

        history = load_history()

        assert history == {'advertisers': {}, 'searches': []}

        fb_ads_scraper.HISTORY_FILE = original_file

    def test_save_and_load_history(self, tmp_path):
        """Test saving and loading history."""
        import fb_ads_scraper
        original_file = fb_ads_scraper.HISTORY_FILE
        original_config = fb_ads_scraper.CONFIG_DIR

        fb_ads_scraper.CONFIG_DIR = tmp_path
        fb_ads_scraper.HISTORY_FILE = tmp_path / 'test_history.json'

        history = {
            'advertisers': {'123': {'page_name': 'Test'}},
            'searches': [{'query': 'test', 'date': '2024-01-01'}]
        }

        save_history(history)
        loaded = load_history()

        assert loaded == history

        fb_ads_scraper.HISTORY_FILE = original_file
        fb_ads_scraper.CONFIG_DIR = original_config


class TestHousingWorkaround:
    """Tests for housing ads workaround."""

    def test_non_housing_unchanged(self):
        """Test that non-housing ad types are not modified."""
        config = {'ad_type': 'all', 'query': 'test'}
        result = apply_housing_workaround(config)
        assert result['ad_type'] == 'all'
        assert result['query'] == 'test'

    def test_housing_converted_to_all(self):
        """Test housing_ads is converted to all."""
        config = {'ad_type': 'housing_ads', 'query': 'miami'}
        result = apply_housing_workaround(config)
        assert result['ad_type'] == 'all'
        assert result['_original_ad_type'] == 'housing_ads'

    def test_housing_adds_keyword_when_empty(self):
        """Test housing workaround adds keyword when query is empty."""
        config = {'ad_type': 'housing_ads', 'query': ''}
        result = apply_housing_workaround(config)
        assert result['ad_type'] == 'all'
        assert 'real estate' in result['query']

    def test_housing_adds_keyword_when_missing(self):
        """Test housing workaround prepends keyword when no housing terms present."""
        config = {'ad_type': 'housing_ads', 'query': 'miami florida'}
        result = apply_housing_workaround(config)
        assert result['query'].startswith('real estate')
        assert 'miami florida' in result['query']

    def test_housing_preserves_existing_keyword(self):
        """Test housing workaround doesn't add keyword if one exists."""
        config = {'ad_type': 'housing_ads', 'query': 'real estate miami'}
        result = apply_housing_workaround(config)
        # Should not duplicate 'real estate'
        assert result['query'] == 'real estate miami'

    def test_housing_keywords_list(self):
        """Test HOUSING_KEYWORDS contains expected terms."""
        assert 'real estate' in HOUSING_KEYWORDS
        assert 'realtor' in HOUSING_KEYWORDS
        assert 'mortgage' in HOUSING_KEYWORDS


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
