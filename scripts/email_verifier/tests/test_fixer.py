"""
Tests for email fixer functions.

Run with: pytest scripts/email_verifier/tests/test_fixer.py -v
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from fixer import (
    is_invalid_name,
    extract_name_from_email,
    fix_greeting,
    fix_template_variables,
    fix_draft,
)


class TestIsInvalidName:
    """Tests for invalid name detection."""
    
    def test_none_string(self):
        assert is_invalid_name("None") is True
    
    def test_none_none(self):
        assert is_invalid_name("None None") is True
    
    def test_nan(self):
        assert is_invalid_name("nan") is True
    
    def test_empty(self):
        assert is_invalid_name("") is True
    
    def test_valid_name(self):
        assert is_invalid_name("John Smith") is False
    
    def test_meet_our_team(self):
        assert is_invalid_name("Meet Our Team") is True


class TestExtractNameFromEmail:
    """Tests for extracting names from email addresses."""
    
    def test_simple_name(self):
        assert extract_name_from_email("john@company.com") == "John"
    
    def test_firstname_lastname(self):
        assert extract_name_from_email("john.smith@company.com") == "John"
    
    def test_firstname_underscore_lastname(self):
        assert extract_name_from_email("john_smith@company.com") == "John"
    
    def test_generic_info(self):
        assert extract_name_from_email("info@company.com") is None
    
    def test_generic_support(self):
        assert extract_name_from_email("support@company.com") is None
    
    def test_generic_contact(self):
        assert extract_name_from_email("contact@company.com") is None
    
    def test_short_prefix(self):
        # Too short to be a name
        result = extract_name_from_email("jd@company.com")
        assert result is None
    
    def test_jeremy(self):
        assert extract_name_from_email("jeremy@company.com") == "Jeremy"
    
    def test_madison(self):
        assert extract_name_from_email("madison@company.com") == "Madison"
    
    def test_victoria(self):
        assert extract_name_from_email("victoria@company.com") == "Victoria"
    
    def test_editor(self):
        # 'editor' is generic
        assert extract_name_from_email("editor@news.com") is None


class TestFixGreeting:
    """Tests for greeting fixes."""
    
    def test_fix_hi_none(self):
        body = "Hi None,\n\nI noticed your company..."
        fixed, changed = fix_greeting(body, None)
        assert "Hi there," in fixed
        assert changed is True
    
    def test_fix_hi_nan(self):
        body = "Hi nan,\n\nI noticed your company..."
        fixed, changed = fix_greeting(body, None)
        assert "Hi there," in fixed
        assert changed is True
    
    def test_fix_with_new_name(self):
        body = "Hi None,\n\nI noticed your company..."
        fixed, changed = fix_greeting(body, "John")
        assert "Hi John," in fixed
        assert changed is True
    
    def test_no_change_needed(self):
        body = "Hi John,\n\nI noticed your company..."
        fixed, changed = fix_greeting(body, "John")
        assert "Hi John," in fixed
        # Should not change if already correct
    
    def test_update_wrong_name(self):
        body = "Hi Sarah,\n\nI noticed your company..."
        fixed, changed = fix_greeting(body, "John")
        assert "Hi John," in fixed
        assert changed is True


class TestFixTemplateVariables:
    """Tests for template variable removal."""
    
    def test_remove_product_brand(self):
        body = "I noticed in your Facebook ad that you're running ads with \"{{product.brand}}.\"\n\nThis is great."
        fixed, changed, vars_found = fix_template_variables(body)
        assert "{{product.brand}}" not in fixed
        assert changed is True
        assert "{{product.brand}}" in vars_found
    
    def test_no_template_vars(self):
        body = "I noticed your company is doing great work."
        fixed, changed, vars_found = fix_template_variables(body)
        assert fixed == body
        assert changed is False
        assert vars_found == []
    
    def test_multiple_vars(self):
        body = "Hello {{name}},\n\nWelcome to {{company}}.\n\nBest regards."
        fixed, changed, vars_found = fix_template_variables(body)
        assert "{{name}}" not in fixed
        assert "{{company}}" not in fixed
        assert changed is True
        assert len(vars_found) == 2


class TestFixDraft:
    """Tests for complete draft fixing."""
    
    def test_fix_none_contact(self):
        draft = {
            'page_name': 'Test Company',
            'contact_name': 'None None',
            'primary_email': 'john@test.com',
            'email_body': 'Hi None,\n\nI noticed your company...'
        }
        fixed, fixes = fix_draft(draft)
        assert fixed['contact_name'] == 'John'
        assert 'Hi John,' in fixed['email_body']
        assert len(fixes) >= 1
    
    def test_fix_template_var(self):
        draft = {
            'page_name': 'Test Company',
            'contact_name': 'John Smith',
            'primary_email': 'john@test.com',
            'email_body': 'Hi John,\n\nI saw your {{product.brand}} ads.\n\nBest.'
        }
        fixed, fixes = fix_draft(draft)
        assert '{{product.brand}}' not in fixed['email_body']
        assert len(fixes) >= 1
    
    def test_fix_email_name_mismatch(self):
        draft = {
            'page_name': 'Test Company',
            'contact_name': 'Brent Hutto',
            'primary_email': 'jeremy@test.com',
            'email_body': 'Hi Brent,\n\nI noticed your company...'
        }
        fixed, fixes = fix_draft(draft)
        # Should extract Jeremy from email
        assert fixed['contact_name'] == 'Jeremy'
        assert 'Hi Jeremy,' in fixed['email_body']
    
    def test_no_fix_needed(self):
        draft = {
            'page_name': 'Test Company',
            'contact_name': 'John Smith',
            'primary_email': 'john@test.com',
            'email_body': 'Hi John,\n\nI noticed your company...'
        }
        fixed, fixes = fix_draft(draft)
        assert len(fixes) == 0
        assert fixed['contact_name'] == 'John Smith'

    def test_unfixable_mismatch_uses_generic(self):
        """When we can't extract name from email, use generic greeting."""
        draft = {
            'page_name': 'Test Company',
            'contact_name': 'Bill Scott',
            'primary_email': 'mboulos@weichert.com',  # Can't extract "Mboulos" as a name
            'email_body': 'Hi Bill,\n\nI noticed your company...'
        }
        fixed, fixes = fix_draft(draft)
        # Should use generic greeting instead of wrong name
        assert fixed['contact_name'] == ''
        assert 'Hi there,' in fixed['email_body']
        assert 'Hi Bill,' not in fixed['email_body']

    def test_media_domain_email(self):
        """Emails to media domains should still be fixed with generic greeting."""
        draft = {
            'page_name': 'John Wyche',
            'contact_name': 'Anna Rumer',
            'primary_email': 'editor@popculture.com',
            'email_body': 'Hi Anna,\n\nI noticed your company...'
        }
        fixed, fixes = fix_draft(draft)
        # editor@ is generic, so greeting should become generic
        assert 'Hi there,' in fixed['email_body'] or 'Hi Anna,' in fixed['email_body']
