"""Tests for drafter.py - Main orchestrator and integration tests."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import pandas as pd
import tempfile
import os
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestProcessProspect:
    """Tests for processing a single prospect."""

    @pytest.mark.asyncio
    async def test_full_pipeline_success(self):
        """Should run research -> analyze -> compose for a prospect."""
        from drafter import process_prospect

        prospect = {
            'page_name': 'Example Realty',
            'contact_name': 'John Doe',
            'primary_email': 'john@example.com',
            'website_url': 'https://example.com',
            'ad_texts': 'Check out our listings!'
        }

        mock_research = {
            'company': {'website_findings': ['Hiring 2 agents']},
            'personal': {
                'linkedin': {'headline': 'Top Agent', 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['Check out our listings!'],
            'sources': ['example.com']
        }

        mock_hook = {
            'chosen_hook': 'Hiring 2 agents',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Hiring means capacity issues',
            'confidence': 85,
            'reasoning': 'Clear hiring signal'
        }

        mock_email = {
            'subject_line': 'About your team growth',
            'email_body': 'Hi John...',
            'hook_used': 'Hiring 2 agents',
            'hook_source': 'website'
        }

        with patch('drafter.research_prospect', new_callable=AsyncMock, return_value=mock_research), \
             patch('drafter.analyze_and_select_hook', new_callable=AsyncMock, return_value=mock_hook), \
             patch('drafter.compose_email', new_callable=AsyncMock, return_value=mock_email):

            result = await process_prospect(prospect)

        assert result is not None
        assert result['subject_line'] == 'About your team growth'
        assert result['hook_used'] == 'Hiring 2 agents'
        assert result['confidence_score'] == 85

    @pytest.mark.asyncio
    async def test_handles_missing_website(self):
        """Should handle prospects without website URL."""
        from drafter import process_prospect

        prospect = {
            'page_name': 'Test Realty',
            'contact_name': 'Jane',
            'primary_email': 'jane@test.com',
            'website_url': None,
            'ad_texts': 'Best homes in town'
        }

        mock_research = {
            'company': {'website_findings': []},
            'personal': {
                'linkedin': {'headline': None, 'findings': []},
                'social_media': {'instagram': [], 'twitter': []}
            },
            'ad_content': ['Best homes in town'],
            'sources': []
        }

        mock_hook = {
            'chosen_hook': 'Best homes in town',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'problem_framing': 'Popular offerings attract leads',
            'confidence': 60,
            'reasoning': 'Only ad content available'
        }

        mock_email = {
            'subject_line': 'Your listings ad',
            'email_body': 'Hi Jane...',
            'hook_used': 'Best homes in town',
            'hook_source': 'ad'
        }

        with patch('drafter.research_prospect', new_callable=AsyncMock, return_value=mock_research), \
             patch('drafter.analyze_and_select_hook', new_callable=AsyncMock, return_value=mock_hook), \
             patch('drafter.compose_email', new_callable=AsyncMock, return_value=mock_email):

            result = await process_prospect(prospect)

        assert result is not None
        assert result['hook_source'] == 'ad'

    @pytest.mark.asyncio
    async def test_handles_research_failure(self):
        """Should handle when research fails."""
        from drafter import process_prospect

        prospect = {
            'page_name': 'Fail Test',
            'contact_name': 'Error User',
            'primary_email': 'error@test.com',
            'website_url': 'https://error.com',
            'ad_texts': None
        }

        with patch('drafter.research_prospect', new_callable=AsyncMock,
                   side_effect=Exception("Research failed")):

            result = await process_prospect(prospect)

        # Should return a result with error flag
        assert result is not None
        assert result.get('error') is True or result.get('confidence_score', 0) == 0


class TestLoadProspects:
    """Tests for loading prospects from CSV."""

    def test_loads_csv_successfully(self):
        """Should load prospects from CSV file."""
        from drafter import load_prospects

        # Create temp CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('page_name,contact_name,primary_email,website_url,ad_texts\n')
            f.write('Test Co,John Doe,john@test.com,https://test.com,Test ad\n')
            f.write('Another Co,Jane Doe,jane@another.com,https://another.com,Another ad\n')
            temp_path = f.name

        try:
            prospects = load_prospects(temp_path)

            assert len(prospects) == 2
            assert prospects[0]['page_name'] == 'Test Co'
            assert prospects[1]['contact_name'] == 'Jane Doe'
        finally:
            os.unlink(temp_path)

    def test_handles_missing_columns(self):
        """Should handle CSV with missing optional columns."""
        from drafter import load_prospects

        # Create temp CSV with minimal columns
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('page_name,contact_name,primary_email\n')
            f.write('Test Co,John,john@test.com\n')
            temp_path = f.name

        try:
            prospects = load_prospects(temp_path)

            assert len(prospects) == 1
            assert prospects[0].get('website_url') is None or prospects[0].get('website_url') == ''
        finally:
            os.unlink(temp_path)

    def test_filters_invalid_emails(self):
        """Should skip rows without valid email."""
        from drafter import load_prospects

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('page_name,contact_name,primary_email\n')
            f.write('Valid Co,John,john@test.com\n')
            f.write('No Email Co,Jane,\n')
            f.write('Another Valid,Bob,bob@example.com\n')
            temp_path = f.name

        try:
            prospects = load_prospects(temp_path)

            # Should only load rows with valid email
            assert len(prospects) == 2
        finally:
            os.unlink(temp_path)


class TestSaveResults:
    """Tests for saving draft results."""

    def test_saves_csv_successfully(self):
        """Should save results to CSV file."""
        from drafter import save_results

        results = [
            {
                'page_name': 'Test Co',
                'contact_name': 'John',
                'primary_email': 'john@test.com',
                'subject_line': 'Test subject',
                'email_body': 'Test body',
                'hook_used': 'Test hook',
                'hook_source': 'website',
                'hook_type': 'hiring',
                'confidence_score': 85
            }
        ]

        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            save_results(results, temp_path)

            # Read back and verify
            df = pd.read_csv(temp_path)
            assert len(df) == 1
            assert df.iloc[0]['subject_line'] == 'Test subject'
            assert df.iloc[0]['hook_source'] == 'website'
        finally:
            os.unlink(temp_path)

    def test_includes_all_columns(self):
        """Should include all required output columns."""
        from drafter import save_results

        results = [
            {
                'page_name': 'Test',
                'contact_name': 'User',
                'primary_email': 'test@test.com',
                'subject_line': 'Subject',
                'email_body': 'Body',
                'hook_used': 'Hook',
                'hook_source': 'ad',
                'hook_type': 'story',
                'analyzer_reasoning': 'Reasoning',
                'exa_sources': 'source1.com,source2.com',
                'confidence_score': 90,
                'draft_timestamp': '2024-01-01T12:00:00'
            }
        ]

        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            save_results(results, temp_path)

            df = pd.read_csv(temp_path)
            required_cols = ['page_name', 'contact_name', 'primary_email',
                           'subject_line', 'email_body', 'hook_used',
                           'hook_source', 'hook_type', 'confidence_score']

            for col in required_cols:
                assert col in df.columns
        finally:
            os.unlink(temp_path)


class TestBatchProcessing:
    """Tests for batch processing multiple prospects."""

    @pytest.mark.asyncio
    async def test_processes_multiple_prospects(self):
        """Should process a batch of prospects."""
        from drafter import process_batch

        prospects = [
            {'page_name': 'Co1', 'contact_name': 'User1', 'primary_email': 'u1@test.com'},
            {'page_name': 'Co2', 'contact_name': 'User2', 'primary_email': 'u2@test.com'},
        ]

        mock_result = {
            'subject_line': 'Test',
            'email_body': 'Body',
            'hook_used': 'Hook',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'confidence_score': 80
        }

        with patch('drafter.process_prospect', new_callable=AsyncMock, return_value=mock_result):
            results = await process_batch(prospects)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_respects_limit_parameter(self):
        """Should only process up to the specified limit."""
        from drafter import process_batch

        prospects = [
            {'page_name': f'Co{i}', 'contact_name': f'User{i}', 'primary_email': f'u{i}@test.com'}
            for i in range(10)
        ]

        mock_result = {
            'subject_line': 'Test',
            'email_body': 'Body',
            'hook_used': 'Hook',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'confidence_score': 80
        }

        with patch('drafter.process_prospect', new_callable=AsyncMock, return_value=mock_result):
            results = await process_batch(prospects, limit=3)

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_continues_on_individual_failure(self):
        """Should continue processing even if one prospect fails."""
        from drafter import process_batch

        prospects = [
            {'page_name': 'Co1', 'contact_name': 'User1', 'primary_email': 'u1@test.com'},
            {'page_name': 'Co2', 'contact_name': 'User2', 'primary_email': 'u2@test.com'},
            {'page_name': 'Co3', 'contact_name': 'User3', 'primary_email': 'u3@test.com'},
        ]

        call_count = 0

        async def mock_process(p):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Processing failed")
            return {
                'subject_line': 'Test',
                'email_body': 'Body',
                'hook_used': 'Hook',
                'hook_source': 'ad',
                'hook_type': 'offer',
                'confidence_score': 80
            }

        with patch('drafter.process_prospect', side_effect=mock_process):
            results = await process_batch(prospects)

        # Should have results for the prospects that didn't fail
        # (2 success + 1 error result)
        assert len(results) >= 2


class TestCLI:
    """Tests for command-line interface."""

    def test_parses_input_argument(self):
        """Should parse --input argument."""
        from drafter import parse_args

        args = parse_args(['--input', 'test.csv'])
        assert args.input == 'test.csv'

    def test_parses_limit_argument(self):
        """Should parse --limit argument."""
        from drafter import parse_args

        args = parse_args(['--limit', '5'])
        assert args.limit == 5

    def test_parses_all_flag(self):
        """Should parse --all flag."""
        from drafter import parse_args

        args = parse_args(['--all'])
        assert args.all is True

    def test_default_values(self):
        """Should have sensible defaults."""
        from drafter import parse_args

        args = parse_args([])

        # Default limit should be small for testing
        assert args.limit == 3 or args.all is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
