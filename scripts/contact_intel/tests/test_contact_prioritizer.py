"""Tests for contact prioritization module."""

import sqlite3
from unittest.mock import patch

import pytest


class TestContactPrioritizer:
    """Tests for contact prioritization logic."""

    def test_is_internal_domain(self):
        """Should identify internal company domains."""
        from scripts.contact_intel.contact_prioritizer import _is_internal

        assert _is_internal('john@jaguarcapital.co') is True
        assert _is_internal('jane@lahaus.com') is True
        assert _is_internal('bob@external.com') is False

    def test_is_automated_email(self):
        """Should identify automated/noreply emails."""
        from scripts.contact_intel.contact_prioritizer import _is_automated

        assert _is_automated('noreply@company.com') is True
        assert _is_automated('no-reply@service.io') is True
        assert _is_automated('notifications@app.com') is True
        assert _is_automated('john@company.com') is False

    def test_is_target_industry_real_estate(self):
        """Should identify real estate industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@compass.com')
        assert is_match is True
        assert industry == 'real_estate'

        is_match, industry = _is_target_industry('jane@cbre.com')
        assert is_match is True

        is_match, industry = _is_target_industry('bob@acmerealty.com')
        assert is_match is True

    def test_is_target_industry_finance(self):
        """Should identify finance/VC industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@acmecapital.com')
        assert is_match is True
        assert industry == 'finance'

        is_match, industry = _is_target_industry('jane@xyzpartners.com')
        assert is_match is True

        is_match, industry = _is_target_industry('bob@kaszek.com')
        assert is_match is True

    def test_is_target_industry_tech(self):
        """Should identify tech industry domains."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@google.com')
        assert is_match is True
        assert industry == 'tech'

        is_match, industry = _is_target_industry('jane@techstartup.io')
        assert is_match is True

    def test_is_target_industry_no_match(self):
        """Should return False for non-target industries."""
        from scripts.contact_intel.contact_prioritizer import _is_target_industry

        is_match, industry = _is_target_industry('john@randomcompany.com')
        assert is_match is False
        assert industry == ''

    def test_get_prioritized_contacts_excludes_internal(self, mock_emails_db, my_emails):
        """Should exclude internal domain contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'tu@jaguarcapital.co' not in emails

    def test_get_prioritized_contacts_excludes_automated(self, mock_emails_db, my_emails):
        """Should exclude noreply/automated contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'noreply@notifications.com' not in emails

    def test_get_prioritized_contacts_excludes_one_way_outbound(self, mock_emails_db, my_emails):
        """Should exclude contacts we emailed but never replied."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        # cold@example.com only received email, never sent one
        assert 'cold@example.com' not in emails

    def test_get_prioritized_contacts_includes_replied(self, mock_emails_db, my_emails):
        """Should include contacts who replied to us."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        emails = [c['email'] for c in contacts]
        assert 'john@realty.com' in emails
        assert 'jane@techstartup.io' in emails

    def test_get_prioritized_contacts_assigns_tiers(self, mock_emails_db, my_emails):
        """Should assign priority tiers correctly."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100)

        # john@realty.com is target industry (real estate) + replied = tier 1
        john = next((c for c in contacts if c['email'] == 'john@realty.com'), None)
        assert john is not None
        assert john['tier'] == 1
        assert john['industry'] == 'real_estate'

    def test_get_prioritized_contacts_respects_limit(self, mock_emails_db, my_emails):
        """Should respect the limit parameter."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=1)

        assert len(contacts) <= 1

    def test_get_prioritized_contacts_skips_already_extracted(self, mock_emails_db, my_emails):
        """Should skip already extracted contacts."""
        from scripts.contact_intel.contact_prioritizer import get_prioritized_contacts

        already_extracted = {'john@realty.com'}

        with patch('scripts.contact_intel.contact_prioritizer.EMAILS_DB', mock_emails_db):
            contacts = get_prioritized_contacts(my_emails, limit=100, already_extracted=already_extracted)

        emails = [c['email'] for c in contacts]
        assert 'john@realty.com' not in emails
