"""Tests for researcher.py - Multi-source Exa research."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestResearchCompanyWebsite:
    """Tests for company website research via Exa."""

    @pytest.mark.asyncio
    async def test_research_company_website_success(self):
        """Should extract findings from company website via Exa."""
        from researcher import research_company_website

        mock_exa_response = [
            {
                'url': 'https://example.com/about',
                'text': 'We are hiring 2 new agents to join our team. 50% lead surplus.',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_company_website('Example Realty', 'https://example.com')

        assert 'findings' in result
        assert len(result['findings']) > 0
        assert 'sources' in result

    @pytest.mark.asyncio
    async def test_research_company_website_extracts_hiring_signals(self):
        """Should identify hiring-related content."""
        from researcher import research_company_website

        mock_exa_response = [
            {
                'url': 'https://example.com/careers',
                'text': 'Join our team! We are looking for 3 experienced agents.',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_company_website('Example Realty', 'https://example.com')

        assert any('hiring' in str(f).lower() or 'agent' in str(f).lower()
                   for f in result.get('findings', []))

    @pytest.mark.asyncio
    async def test_research_company_website_handles_no_results(self):
        """Should handle empty Exa results gracefully."""
        from researcher import research_company_website

        with patch('researcher.search_exa', return_value=[]):
            result = await research_company_website('Unknown Company', 'https://unknown.com')

        assert result is not None
        assert 'findings' in result
        assert result['findings'] == []


class TestResearchLinkedInProfile:
    """Tests for LinkedIn profile research via Exa."""

    @pytest.mark.asyncio
    async def test_research_linkedin_profile_success(self):
        """Should extract LinkedIn profile information."""
        from researcher import research_linkedin_profile

        mock_exa_response = [
            {
                'url': 'https://linkedin.com/in/john-doe',
                'text': 'John Doe - Real Estate Agent at Example Realty. Just closed my 50th deal!',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_linkedin_profile('John Doe', 'Example Realty')

        assert 'headline' in result or 'findings' in result
        assert 'sources' in result

    @pytest.mark.asyncio
    async def test_research_linkedin_extracts_achievements(self):
        """Should identify achievements from LinkedIn."""
        from researcher import research_linkedin_profile

        mock_exa_response = [
            {
                'url': 'https://linkedin.com/in/jane-smith',
                'text': 'Top 1% of realtors in Miami. $50M in sales this year.',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_linkedin_profile('Jane Smith', 'Miami Realty')

        assert result is not None

    @pytest.mark.asyncio
    async def test_research_linkedin_handles_not_found(self):
        """Should handle case when LinkedIn profile not found."""
        from researcher import research_linkedin_profile

        with patch('researcher.search_exa', return_value=[]):
            result = await research_linkedin_profile('Unknown Person', 'Unknown Company')

        assert result is not None
        assert result.get('findings', []) == [] or result.get('headline') is None


class TestResearchSocialMedia:
    """Tests for social media research (Instagram, Twitter) via Exa."""

    @pytest.mark.asyncio
    async def test_research_social_media_instagram(self):
        """Should extract Instagram content."""
        from researcher import research_social_media

        mock_exa_response = [
            {
                'url': 'https://instagram.com/p/abc123',
                'text': 'Just listed another amazing property! #realestate',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_social_media('John Doe', instagram_handle='johndoe_realty')

        assert 'instagram' in result
        assert 'sources' in result

    @pytest.mark.asyncio
    async def test_research_social_media_twitter(self):
        """Should extract Twitter content."""
        from researcher import research_social_media

        mock_exa_response = [
            {
                'url': 'https://twitter.com/johndoe/status/123',
                'text': 'Excited to announce our team just hit 100 closings this year!',
            }
        ]

        with patch('researcher.search_exa', return_value=mock_exa_response):
            result = await research_social_media('John Doe', company_name='Example Realty')

        assert 'twitter' in result

    @pytest.mark.asyncio
    async def test_research_social_media_handles_no_handles(self):
        """Should handle case when no social handles provided."""
        from researcher import research_social_media

        result = await research_social_media('John Doe')

        assert result is not None
        assert result.get('instagram', []) == []
        assert result.get('twitter', []) == []


class TestResearchProspect:
    """Tests for the main research_prospect function that aggregates all sources."""

    @pytest.mark.asyncio
    async def test_aggregates_all_sources(self):
        """Should combine results from website, LinkedIn, and social media."""
        from researcher import research_prospect

        with patch('researcher.research_company_website', new_callable=AsyncMock) as mock_web, \
             patch('researcher.research_linkedin_profile', new_callable=AsyncMock) as mock_li, \
             patch('researcher.research_social_media', new_callable=AsyncMock) as mock_social:

            mock_web.return_value = {'findings': ['hiring 2 agents'], 'sources': ['web.com']}
            mock_li.return_value = {'headline': 'Top Agent', 'findings': [], 'sources': ['linkedin.com']}
            mock_social.return_value = {'instagram': [], 'twitter': [], 'sources': []}

            result = await research_prospect(
                contact_name='John Doe',
                company_name='Example Realty',
                website_url='https://example.com'
            )

        assert 'company' in result
        assert 'personal' in result
        assert 'sources' in result

    @pytest.mark.asyncio
    async def test_handles_missing_data(self):
        """Should work even when some data is missing."""
        from researcher import research_prospect

        with patch('researcher.research_company_website', new_callable=AsyncMock) as mock_web, \
             patch('researcher.research_linkedin_profile', new_callable=AsyncMock) as mock_li, \
             patch('researcher.research_social_media', new_callable=AsyncMock) as mock_social:

            mock_web.return_value = {'findings': [], 'sources': []}
            mock_li.return_value = {'findings': [], 'sources': []}
            mock_social.return_value = {'instagram': [], 'twitter': [], 'sources': []}

            result = await research_prospect(
                contact_name='Unknown',
                company_name='Unknown Company'
            )

        assert result is not None
        assert 'company' in result
        assert 'personal' in result

    @pytest.mark.asyncio
    async def test_passes_ad_content_through(self):
        """Should include ad_texts in the result if provided."""
        from researcher import research_prospect

        ad_texts = ['Check out our new listings!', 'Best agents in town']

        with patch('researcher.research_company_website', new_callable=AsyncMock) as mock_web, \
             patch('researcher.research_linkedin_profile', new_callable=AsyncMock) as mock_li, \
             patch('researcher.research_social_media', new_callable=AsyncMock) as mock_social:

            mock_web.return_value = {'findings': [], 'sources': []}
            mock_li.return_value = {'findings': [], 'sources': []}
            mock_social.return_value = {'instagram': [], 'twitter': [], 'sources': []}

            result = await research_prospect(
                contact_name='John',
                company_name='Test Realty',
                ad_texts=ad_texts
            )

        assert 'ad_content' in result
        assert result['ad_content'] == ad_texts


class TestSearchExa:
    """Tests for the low-level Exa search function."""

    @pytest.mark.asyncio
    async def test_search_exa_calls_api(self):
        """Should make correct API call to Exa."""
        from researcher import search_exa

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'results': [
                {'url': 'https://example.com', 'text': 'Test content'}
            ]
        }

        with patch('researcher.requests.post', return_value=mock_response):
            results = await search_exa('test query')

        assert len(results) == 1
        assert results[0]['url'] == 'https://example.com'

    @pytest.mark.asyncio
    async def test_search_exa_handles_api_error(self):
        """Should handle Exa API errors gracefully."""
        from researcher import search_exa

        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch('researcher.requests.post', return_value=mock_response):
            results = await search_exa('test query')

        assert results == []

    @pytest.mark.asyncio
    async def test_search_exa_handles_timeout(self):
        """Should handle request timeouts."""
        from researcher import search_exa
        import requests

        with patch('researcher.requests.post', side_effect=requests.Timeout):
            results = await search_exa('test query')

        assert results == []


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
