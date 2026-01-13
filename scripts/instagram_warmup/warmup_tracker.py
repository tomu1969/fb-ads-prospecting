"""
WarmupTracker - State management for Instagram warm-up automation.

Tracks warm-up progress per prospect:
- Current phase (day 1-7)
- Actions completed (follow, likes, comments)
- Status (pending, in_progress, ready_for_dm, dm_sent, failed)
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WarmupState:
    """State for a single prospect in warm-up."""

    # Required fields
    instagram_handle: str
    page_name: str
    contact_name: str

    # Phase tracking
    current_phase: int = 0
    status: str = 'pending'  # pending, in_progress, ready_for_dm, dm_sent, failed
    warmup_start_date: Optional[datetime] = None

    # Action tracking
    followed_at: Optional[datetime] = None
    likes_completed: int = 0
    posts_liked: List[str] = field(default_factory=list)
    comments_completed: int = 0
    posts_commented: List[str] = field(default_factory=list)

    # Error tracking
    last_error: Optional[str] = None
    retry_count: int = 0

    def to_dict(self) -> Dict:
        """Convert to dictionary for CSV storage."""
        data = asdict(self)
        # Convert datetime to ISO string
        if self.warmup_start_date:
            data['warmup_start_date'] = self.warmup_start_date.isoformat()
        if self.followed_at:
            data['followed_at'] = self.followed_at.isoformat()
        # Convert lists to JSON strings
        data['posts_liked'] = json.dumps(self.posts_liked)
        data['posts_commented'] = json.dumps(self.posts_commented)
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'WarmupState':
        """Create from dictionary (CSV row)."""
        # Parse numeric fields
        current_phase = int(data.get('current_phase', 0) or 0)
        likes_completed = int(data.get('likes_completed', 0) or 0)
        comments_completed = int(data.get('comments_completed', 0) or 0)
        retry_count = int(data.get('retry_count', 0) or 0)

        # Parse datetime fields
        warmup_start_date = None
        if data.get('warmup_start_date'):
            try:
                warmup_start_date = datetime.fromisoformat(str(data['warmup_start_date']))
            except (ValueError, TypeError):
                pass

        followed_at = None
        if data.get('followed_at'):
            try:
                followed_at = datetime.fromisoformat(str(data['followed_at']))
            except (ValueError, TypeError):
                pass

        # Parse JSON list fields
        posts_liked = []
        if data.get('posts_liked'):
            try:
                posts_liked = json.loads(str(data['posts_liked']))
            except (json.JSONDecodeError, TypeError):
                pass

        posts_commented = []
        if data.get('posts_commented'):
            try:
                posts_commented = json.loads(str(data['posts_commented']))
            except (json.JSONDecodeError, TypeError):
                pass

        return cls(
            instagram_handle=str(data.get('instagram_handle', '')),
            page_name=str(data.get('page_name', '')),
            contact_name=str(data.get('contact_name', '')),
            current_phase=current_phase,
            status=str(data.get('status', 'pending')),
            warmup_start_date=warmup_start_date,
            followed_at=followed_at,
            likes_completed=likes_completed,
            posts_liked=posts_liked,
            comments_completed=comments_completed,
            posts_commented=posts_commented,
            last_error=data.get('last_error') if data.get('last_error') else None,
            retry_count=retry_count
        )


class WarmupTracker:
    """Manages warm-up state for all prospects."""

    CSV_COLUMNS = [
        'instagram_handle', 'page_name', 'contact_name',
        'current_phase', 'status', 'warmup_start_date', 'followed_at',
        'likes_completed', 'posts_liked', 'comments_completed', 'posts_commented',
        'last_error', 'retry_count'
    ]

    def __init__(self, csv_path: Path):
        """Initialize tracker with CSV file path."""
        self.csv_path = Path(csv_path)
        self.prospects: Dict[str, WarmupState] = {}
        self._load()

    def _load(self):
        """Load state from CSV file."""
        if not self.csv_path.exists():
            # Create empty CSV with headers
            self._create_empty_csv()
            return

        try:
            df = pd.read_csv(self.csv_path, encoding='utf-8')
            for _, row in df.iterrows():
                state = WarmupState.from_dict(row.to_dict())
                handle = self._normalize_handle(state.instagram_handle)
                self.prospects[handle] = state
            logger.info(f"Loaded {len(self.prospects)} prospects from {self.csv_path}")
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            self.prospects = {}

    def _create_empty_csv(self):
        """Create empty CSV with headers."""
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(columns=self.CSV_COLUMNS)
        df.to_csv(self.csv_path, index=False, encoding='utf-8')
        logger.info(f"Created empty state file: {self.csv_path}")

    def _normalize_handle(self, handle: str) -> str:
        """Normalize Instagram handle (lowercase, no @)."""
        if not handle:
            return ''
        handle = str(handle).strip().lower()
        if handle.startswith('@'):
            handle = handle[1:]
        return handle

    def save(self):
        """Save state to CSV file."""
        if not self.prospects:
            self._create_empty_csv()
            return

        rows = [state.to_dict() for state in self.prospects.values()]
        df = pd.DataFrame(rows, columns=self.CSV_COLUMNS)
        df.to_csv(self.csv_path, index=False, encoding='utf-8')
        logger.info(f"Saved {len(self.prospects)} prospects to {self.csv_path}")

    def add_prospect(self, instagram_handle: str, page_name: str, contact_name: str) -> bool:
        """Add a new prospect. Returns False if already exists."""
        handle = self._normalize_handle(instagram_handle)
        if not handle:
            return False

        if handle in self.prospects:
            logger.debug(f"Prospect {handle} already exists, skipping")
            return False

        self.prospects[handle] = WarmupState(
            instagram_handle=handle,
            page_name=page_name,
            contact_name=contact_name
        )
        logger.debug(f"Added prospect: {handle}")
        return True

    def update_status(self, instagram_handle: str, status: str):
        """Update prospect status."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            self.prospects[handle].status = status
            logger.debug(f"Updated {handle} status to {status}")

    def advance_phase(self, instagram_handle: str):
        """Advance prospect to next phase."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            prospect = self.prospects[handle]
            prospect.current_phase += 1
            if prospect.status == 'pending':
                prospect.status = 'in_progress'
                prospect.warmup_start_date = datetime.now()
            logger.debug(f"Advanced {handle} to phase {prospect.current_phase}")

    def mark_followed(self, instagram_handle: str):
        """Mark prospect as followed."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            self.prospects[handle].followed_at = datetime.now()
            logger.debug(f"Marked {handle} as followed")

    def add_liked_post(self, instagram_handle: str, post_url: str):
        """Record a liked post."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            prospect = self.prospects[handle]
            if post_url not in prospect.posts_liked:
                prospect.posts_liked.append(post_url)
                prospect.likes_completed += 1
                logger.debug(f"Added liked post for {handle}: {post_url}")

    def add_comment(self, instagram_handle: str, post_url: str, comment_text: str):
        """Record a comment."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            prospect = self.prospects[handle]
            if post_url not in prospect.posts_commented:
                prospect.posts_commented.append(post_url)
                prospect.comments_completed += 1
                logger.debug(f"Added comment for {handle} on {post_url}")

    def set_error(self, instagram_handle: str, error: str):
        """Set error on prospect."""
        handle = self._normalize_handle(instagram_handle)
        if handle in self.prospects:
            prospect = self.prospects[handle]
            prospect.last_error = error
            prospect.retry_count += 1
            logger.warning(f"Error for {handle}: {error}")

    def get_prospects_by_status(self, status: str) -> List[WarmupState]:
        """Get all prospects with given status."""
        return [p for p in self.prospects.values() if p.status == status]

    def get_prospects_by_phase(self, phase: int) -> List[WarmupState]:
        """Get all prospects at given phase."""
        return [p for p in self.prospects.values() if p.current_phase == phase]

    def get_ready_for_dm(self) -> List[WarmupState]:
        """Get all prospects ready for DM."""
        return self.get_prospects_by_status('ready_for_dm')

    def import_from_csv(self, prospects_csv: Path) -> int:
        """Import prospects from a CSV file with instagram_handles column."""
        try:
            df = pd.read_csv(prospects_csv, encoding='utf-8')
        except Exception as e:
            logger.error(f"Error reading prospects CSV: {e}")
            return 0

        if 'instagram_handles' not in df.columns:
            logger.error("CSV missing 'instagram_handles' column")
            return 0

        count = 0
        for _, row in df.iterrows():
            handles_str = row.get('instagram_handles', '')
            handles = self._parse_handles(handles_str)

            page_name = str(row.get('page_name', ''))
            contact_name = str(row.get('contact_name', ''))

            for handle in handles:
                if self.add_prospect(handle, page_name, contact_name):
                    count += 1

        self.save()
        logger.info(f"Imported {count} prospects from {prospects_csv}")
        return count

    def _parse_handles(self, handles_str) -> List[str]:
        """Parse Instagram handles from CSV column."""
        if pd.isna(handles_str) or not handles_str:
            return []

        handles_str = str(handles_str).strip()
        if not handles_str or handles_str == '[]':
            return []

        # Handle list-like strings: ['handle1', 'handle2']
        if handles_str.startswith('['):
            try:
                import ast
                handles_list = ast.literal_eval(handles_str)
                if isinstance(handles_list, list):
                    return [self._normalize_handle(h) for h in handles_list if h]
            except:
                pass

        # Split by comma
        handles = [h.strip().strip("'\"") for h in handles_str.split(',')]
        return [self._normalize_handle(h) for h in handles if h]
