"""Tests for entity extractor main module."""

from unittest.mock import MagicMock, patch

import pytest


class TestEntityExtractor:
    """Tests for entity extractor."""

    def test_show_status_runs_without_error(self, capsys):
        """Should show status without crashing."""
        from scripts.contact_intel.entity_extractor import show_status

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_contact_stats', return_value={
                    'total': 100,
                    'tier_1_target_industry': 20,
                    'tier_2_active': 50,
                    'tier_3_replied': 30,
                    'by_industry': {'real_estate': 10, 'finance': 5},
                }):
                    show_status()

        captured = capsys.readouterr()
        assert 'Eligible contacts: 100' in captured.out
        assert 'Tier 1' in captured.out

    def test_run_extraction_respects_budget(self):
        """Should stop when budget is exhausted."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=10.0  # $10 per extraction
        )

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts', return_value=[
                    {'email': 'a@test.com', 'name': 'A', 'tier': 1, 'industry': 'tech'},
                    {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                    {'email': 'c@test.com', 'name': 'C', 'tier': 1, 'industry': 'tech'},
                ]):
                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction'):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            # Budget of $15 should only process 1 contact ($10 each)
                                            run_extraction(budget=15.0, resume=False)

        # Should have stopped after 1-2 contacts due to budget
        assert mock_client.return_value.extract_contact_info.call_count <= 2

    def test_run_extraction_saves_after_each_contact(self):
        """Should save extraction immediately after each contact (partial save)."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=0.01
        )

        save_calls = []

        def track_save(*args, **kwargs):
            save_calls.append(args)

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value=set()):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts', return_value=[
                    {'email': 'a@test.com', 'name': 'A', 'tier': 1, 'industry': 'tech'},
                    {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                ]):
                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction', side_effect=track_save):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            run_extraction(budget=50.0, resume=False)

        # Should have saved after each of the 2 contacts
        assert len(save_calls) == 2

    def test_run_extraction_resumes_from_extracted(self):
        """Should skip already extracted contacts on resume."""
        from scripts.contact_intel.entity_extractor import run_extraction
        from scripts.contact_intel.groq_client import ExtractionResult

        mock_result = ExtractionResult(
            company='Test', role='Dev', topics=['tech'],
            confidence=0.9, input_tokens=500, output_tokens=50, cost_usd=0.01
        )

        with patch('scripts.contact_intel.entity_extractor.init_db'):
            # a@test.com already extracted
            with patch('scripts.contact_intel.entity_extractor.get_extracted_emails', return_value={'a@test.com'}):
                with patch('scripts.contact_intel.entity_extractor.get_prioritized_contacts') as mock_prioritize:
                    mock_prioritize.return_value = [
                        {'email': 'b@test.com', 'name': 'B', 'tier': 1, 'industry': 'tech'},
                    ]

                    with patch('scripts.contact_intel.entity_extractor.get_contact_emails_with_body', return_value=[{'subject': 'Test', 'date': '2024-01-01', 'body': 'Hello'}]):
                        with patch('scripts.contact_intel.entity_extractor.GroqClient') as mock_client:
                            mock_client.return_value.extract_contact_info.return_value = mock_result
                            mock_client.return_value.model = 'test-model'

                            with patch('scripts.contact_intel.entity_extractor.save_extraction'):
                                with patch('scripts.contact_intel.entity_extractor.start_extraction_run', return_value=1):
                                    with patch('scripts.contact_intel.entity_extractor.update_run_stats'):
                                        with patch('scripts.contact_intel.entity_extractor.complete_run'):
                                            run_extraction(budget=50.0, resume=True)

        # Should have passed already_extracted to prioritizer
        call_args = mock_prioritize.call_args
        assert 'a@test.com' in call_args.kwargs.get('already_extracted', set())
