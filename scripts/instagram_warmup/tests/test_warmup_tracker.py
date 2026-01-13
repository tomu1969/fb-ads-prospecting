"""
Tests for WarmupTracker - State management for Instagram warm-up automation.

TDD: Write tests first, then implement warmup_tracker.py
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta


class TestWarmupState:
    """Test WarmupState dataclass."""

    def test_warmup_state_creation(self):
        """Test creating a WarmupState with required fields."""
        from scripts.instagram_warmup.warmup_tracker import WarmupState

        state = WarmupState(
            instagram_handle='johndoe',
            page_name='John Doe Realty',
            contact_name='John'
        )

        assert state.instagram_handle == 'johndoe'
        assert state.page_name == 'John Doe Realty'
        assert state.contact_name == 'John'
        assert state.current_phase == 0
        assert state.status == 'pending'

    def test_warmup_state_defaults(self):
        """Test default values are set correctly."""
        from scripts.instagram_warmup.warmup_tracker import WarmupState

        state = WarmupState(
            instagram_handle='test',
            page_name='Test Co',
            contact_name='Test'
        )

        assert state.warmup_start_date is None
        assert state.followed_at is None
        assert state.likes_completed == 0
        assert state.comments_completed == 0
        assert state.posts_liked == []
        assert state.posts_commented == []
        assert state.last_error is None
        assert state.retry_count == 0

    def test_warmup_state_to_dict(self):
        """Test converting state to dictionary for CSV storage."""
        from scripts.instagram_warmup.warmup_tracker import WarmupState

        state = WarmupState(
            instagram_handle='test',
            page_name='Test Co',
            contact_name='Test',
            current_phase=2,
            status='in_progress'
        )

        data = state.to_dict()

        assert isinstance(data, dict)
        assert data['instagram_handle'] == 'test'
        assert data['current_phase'] == 2
        assert data['status'] == 'in_progress'

    def test_warmup_state_from_dict(self):
        """Test creating state from dictionary (CSV row)."""
        from scripts.instagram_warmup.warmup_tracker import WarmupState

        data = {
            'instagram_handle': 'test',
            'page_name': 'Test Co',
            'contact_name': 'Test',
            'current_phase': '3',  # String from CSV
            'status': 'in_progress',
            'likes_completed': '2',
            'posts_liked': '["url1", "url2"]'  # JSON string from CSV
        }

        state = WarmupState.from_dict(data)

        assert state.instagram_handle == 'test'
        assert state.current_phase == 3
        assert state.likes_completed == 2
        assert state.posts_liked == ['url1', 'url2']


class TestWarmupTracker:
    """Test WarmupTracker class."""

    @pytest.fixture
    def temp_csv_path(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            yield Path(f.name)
        # Cleanup
        if os.path.exists(f.name):
            os.unlink(f.name)

    def test_tracker_creates_file_if_missing(self, temp_csv_path):
        """Test that tracker creates CSV file if it doesn't exist."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        # Delete the file first
        if temp_csv_path.exists():
            temp_csv_path.unlink()

        tracker = WarmupTracker(temp_csv_path)

        assert temp_csv_path.exists()

    def test_tracker_loads_existing_file(self, temp_csv_path):
        """Test loading existing CSV with prospect data."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        # Write some data first
        with open(temp_csv_path, 'w') as f:
            f.write('instagram_handle,page_name,contact_name,current_phase,status,likes_completed,comments_completed,posts_liked,posts_commented,warmup_start_date,followed_at,last_error,retry_count\n')
            f.write('johndoe,John Realty,John,2,in_progress,1,0,[],[],,,,0\n')

        tracker = WarmupTracker(temp_csv_path)

        assert len(tracker.prospects) == 1
        assert 'johndoe' in tracker.prospects

    def test_add_prospect(self, temp_csv_path):
        """Test adding a new prospect to the tracker."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect(
            instagram_handle='newprospect',
            page_name='New Prospect Co',
            contact_name='New'
        )

        assert 'newprospect' in tracker.prospects
        assert tracker.prospects['newprospect'].status == 'pending'

    def test_add_prospect_normalizes_handle(self, temp_csv_path):
        """Test that handles are normalized (lowercase, no @)."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect(
            instagram_handle='@TestHandle',
            page_name='Test',
            contact_name='Test'
        )

        assert 'testhandle' in tracker.prospects
        assert '@TestHandle' not in tracker.prospects

    def test_add_prospect_skips_duplicate(self, temp_csv_path):
        """Test that duplicate handles are skipped."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test1', 'T1')
        tracker.add_prospect('test', 'Test2', 'T2')  # Duplicate

        assert len(tracker.prospects) == 1
        assert tracker.prospects['test'].page_name == 'Test1'

    def test_update_status(self, temp_csv_path):
        """Test updating prospect status."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.update_status('test', 'in_progress')

        assert tracker.prospects['test'].status == 'in_progress'

    def test_advance_phase(self, temp_csv_path):
        """Test advancing a prospect to the next phase."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.advance_phase('test')

        assert tracker.prospects['test'].current_phase == 1
        assert tracker.prospects['test'].status == 'in_progress'

    def test_mark_followed(self, temp_csv_path):
        """Test marking a prospect as followed."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.mark_followed('test')

        assert tracker.prospects['test'].followed_at is not None

    def test_add_liked_post(self, temp_csv_path):
        """Test adding a liked post."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.add_liked_post('test', 'https://instagram.com/p/ABC123')

        assert 'https://instagram.com/p/ABC123' in tracker.prospects['test'].posts_liked
        assert tracker.prospects['test'].likes_completed == 1

    def test_add_comment(self, temp_csv_path):
        """Test adding a comment."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.add_comment('test', 'https://instagram.com/p/ABC123', 'Great post!')

        assert 'https://instagram.com/p/ABC123' in tracker.prospects['test'].posts_commented
        assert tracker.prospects['test'].comments_completed == 1

    def test_get_prospects_by_status(self, temp_csv_path):
        """Test filtering prospects by status."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test1', 'Test1', 'T1')
        tracker.add_prospect('test2', 'Test2', 'T2')
        tracker.add_prospect('test3', 'Test3', 'T3')

        tracker.update_status('test2', 'in_progress')
        tracker.update_status('test3', 'ready_for_dm')

        pending = tracker.get_prospects_by_status('pending')
        in_progress = tracker.get_prospects_by_status('in_progress')
        ready = tracker.get_prospects_by_status('ready_for_dm')

        assert len(pending) == 1
        assert len(in_progress) == 1
        assert len(ready) == 1

    def test_get_prospects_by_phase(self, temp_csv_path):
        """Test filtering prospects by phase."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test1', 'Test1', 'T1')
        tracker.add_prospect('test2', 'Test2', 'T2')

        tracker.advance_phase('test1')  # Phase 1
        tracker.advance_phase('test1')  # Phase 2
        tracker.advance_phase('test2')  # Phase 1

        phase_1 = tracker.get_prospects_by_phase(1)
        phase_2 = tracker.get_prospects_by_phase(2)

        assert len(phase_1) == 1
        assert len(phase_2) == 1

    def test_save_and_reload(self, temp_csv_path):
        """Test that data persists after save and reload."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        # Create and populate
        tracker1 = WarmupTracker(temp_csv_path)
        tracker1.add_prospect('test', 'Test Co', 'Test')
        tracker1.advance_phase('test')
        tracker1.mark_followed('test')
        tracker1.add_liked_post('test', 'https://post1')
        tracker1.save()

        # Reload
        tracker2 = WarmupTracker(temp_csv_path)

        assert 'test' in tracker2.prospects
        assert tracker2.prospects['test'].current_phase == 1
        assert tracker2.prospects['test'].followed_at is not None
        assert tracker2.prospects['test'].likes_completed == 1

    def test_set_error(self, temp_csv_path):
        """Test setting an error on a prospect."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test', 'Test', 'T')

        tracker.set_error('test', 'API rate limit exceeded')

        assert tracker.prospects['test'].last_error == 'API rate limit exceeded'
        assert tracker.prospects['test'].retry_count == 1

    def test_get_ready_for_dm(self, temp_csv_path):
        """Test getting all prospects ready for DM."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        tracker = WarmupTracker(temp_csv_path)
        tracker.add_prospect('test1', 'Test1', 'T1')
        tracker.add_prospect('test2', 'Test2', 'T2')

        tracker.update_status('test1', 'ready_for_dm')

        ready = tracker.get_ready_for_dm()

        assert len(ready) == 1
        assert ready[0].instagram_handle == 'test1'

    def test_import_from_prospects_csv(self, temp_csv_path):
        """Test importing prospects from a prospects CSV file."""
        from scripts.instagram_warmup.warmup_tracker import WarmupTracker

        # Create a mock prospects CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('page_name,contact_name,instagram_handles\n')
            f.write('REEP Equity,Tim Ellis,"[\'reepequity\', \'timellis\']"\n')
            f.write('Test Co,John Doe,johndoe\n')
            prospects_path = Path(f.name)

        try:
            tracker = WarmupTracker(temp_csv_path)
            count = tracker.import_from_csv(prospects_path)

            assert count >= 2  # At least 2 handles imported
            assert 'reepequity' in tracker.prospects or 'timellis' in tracker.prospects
        finally:
            prospects_path.unlink()
