"""Tests for analyzer.py - LLM-powered hook selection."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import json
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestAnalyzeAndSelectHook:
    """Tests for the main hook selection function."""

    @pytest.mark.asyncio
    async def test_returns_valid_structure(self):
        """Should return dict with all required fields."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['Hiring 2 new agents']},
            'personal': {
                'linkedin': {'headline': 'Top Agent', 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['Check out our new listings!'],
            'sources': ['example.com']
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'Hiring 2 new agents',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'When hiring, lead response times suffer',
            'confidence': 85,
            'reasoning': 'Clear hiring signal indicates growth and capacity issues'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        assert 'chosen_hook' in result
        assert 'hook_source' in result
        assert 'hook_type' in result
        assert 'problem_framing' in result
        assert 'confidence' in result
        assert 'reasoning' in result

    @pytest.mark.asyncio
    async def test_prioritizes_unique_hooks(self):
        """Should prefer unique/specific hooks over generic ones."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['50% lead surplus banner']},
            'personal': {
                'linkedin': {'headline': 'Real Estate Agent', 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['We sell homes'],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': '50% lead surplus banner',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Lead surplus means overwhelmed response capacity',
            'confidence': 90,
            'reasoning': 'Very specific and unique detail only this company has'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        # The hook should be the unique website finding, not generic ad
        assert '50% lead surplus' in result['chosen_hook']

    @pytest.mark.asyncio
    async def test_valid_hook_source_values(self):
        """Should return hook_source from allowed values."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {'headline': None, 'findings': ['Closed 50th deal']},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'Just closed 50th deal',
            'hook_source': 'linkedin',
            'hook_type': 'achievement',
            'problem_framing': 'Success leads to more leads to handle',
            'confidence': 80,
            'reasoning': 'Achievement shows success and likely overflow'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        valid_sources = ['ad', 'website', 'linkedin', 'instagram', 'twitter']
        assert result['hook_source'] in valid_sources

    @pytest.mark.asyncio
    async def test_valid_hook_type_values(self):
        """Should return hook_type from allowed values."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': ['Learning to swim at 40'], 'twitter': []}
            },
            'ad_content': ['My journey learning to swim at 40'],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'Learning to swim at 40',
            'hook_source': 'ad',
            'hook_type': 'story',
            'problem_framing': 'Vulnerable storytelling drives high engagement and leads',
            'confidence': 95,
            'reasoning': 'Personal vulnerability is highly unique and memorable'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        valid_types = ['story', 'hiring', 'achievement', 'offer', 'milestone', 'personal']
        assert result['hook_type'] in valid_types

    @pytest.mark.asyncio
    async def test_confidence_score_range(self):
        """Should return confidence score between 0 and 100."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['Join our team']},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'Join our team',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Hiring indicates growth and capacity needs',
            'confidence': 75,
            'reasoning': 'Decent hook but somewhat generic'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        assert 0 <= result['confidence'] <= 100

    @pytest.mark.asyncio
    async def test_handles_empty_research(self):
        """Should handle case when no research data is available."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': '',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'problem_framing': '',
            'confidence': 0,
            'reasoning': 'No data available for personalization'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        assert result is not None
        assert result['confidence'] <= 20  # Low confidence for empty data

    @pytest.mark.asyncio
    async def test_handles_api_error(self):
        """Should handle OpenAI API errors gracefully."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['Test']},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, side_effect=Exception("API Error")):
            result = await analyze_and_select_hook(research_data)

        # Should return a fallback result, not crash
        assert result is not None
        assert 'chosen_hook' in result
        assert result['confidence'] == 0

    @pytest.mark.asyncio
    async def test_handles_malformed_json_response(self):
        """Should handle when LLM returns invalid JSON."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['Test data']},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "This is not valid JSON"

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        # Should return a fallback result
        assert result is not None
        assert 'chosen_hook' in result


class TestHookPrioritization:
    """Tests for hook prioritization logic."""

    @pytest.mark.asyncio
    async def test_prefers_ad_content_when_unique(self):
        """Should prefer unique ad content over generic website info."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': ['Contact us for more info']},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['I learned to swim at 40 and it changed my life'],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'learned to swim at 40',
            'hook_source': 'ad',
            'hook_type': 'story',
            'problem_framing': 'Vulnerable storytelling drives engagement',
            'confidence': 95,
            'reasoning': 'Personal story is highly unique'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        assert result['hook_source'] == 'ad'
        assert result['hook_type'] == 'story'

    @pytest.mark.asyncio
    async def test_uses_linkedin_achievement(self):
        """Should pick LinkedIn achievement when it's the strongest hook."""
        from analyzer import analyze_and_select_hook

        research_data = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {
                    'headline': 'Top 1% Realtor | $50M in sales',
                    'findings': ['Top 1%', '$50M in sales']
                },
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['Looking for your dream home?'],
            'sources': []
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            'chosen_hook': 'Top 1% Realtor with $50M in sales',
            'hook_source': 'linkedin',
            'hook_type': 'achievement',
            'problem_framing': 'High performance means high lead volume',
            'confidence': 88,
            'reasoning': 'Specific achievement with numbers'
        })

        with patch('analyzer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await analyze_and_select_hook(research_data)

        assert result['hook_source'] == 'linkedin'
        assert result['hook_type'] == 'achievement'


class TestBuildAnalyzerPrompt:
    """Tests for the prompt building function."""

    def test_build_prompt_includes_all_data(self):
        """Should include all research data in the prompt."""
        from analyzer import build_analyzer_prompt

        research_data = {
            'company': {'website_findings': ['Hiring notice']},
            'personal': {
                'linkedin': {'headline': 'Top Agent', 'findings': ['50 deals']},
                'social_media': {
                    'instagram': ['Beach listing photo'],
                    'twitter': ['Market update']
                }
            },
            'ad_content': ['Best homes in Miami'],
            'sources': ['example.com']
        }

        prompt = build_analyzer_prompt(research_data)

        assert 'Hiring notice' in prompt
        assert 'Top Agent' in prompt
        assert '50 deals' in prompt
        assert 'Beach listing photo' in prompt
        assert 'Market update' in prompt
        assert 'Best homes in Miami' in prompt

    def test_build_prompt_handles_missing_sections(self):
        """Should handle missing data sections gracefully."""
        from analyzer import build_analyzer_prompt

        research_data = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': [],
            'sources': []
        }

        prompt = build_analyzer_prompt(research_data)

        # Should not crash and should produce a valid prompt
        assert isinstance(prompt, str)
        assert len(prompt) > 100  # Has some structure


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
