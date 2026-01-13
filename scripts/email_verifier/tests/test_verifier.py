"""
Tests for email verification checks.

Run with: pytest scripts/email_verifier/tests/ -v
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from checks import (
    CheckResult,
    check_contact_name,
    check_email_name_match,
    check_no_template_vars,
    check_domain_match,
    check_greeting_name,
)


class TestCheckContactName:
    """Tests for contact name validation."""

    def test_valid_name(self):
        """Valid person names should pass."""
        result = check_contact_name("John Smith")
        assert result.status == "pass"

    def test_valid_single_name(self):
        """Single names should pass."""
        result = check_contact_name("Tomas")
        assert result.status == "pass"

    def test_none_string_fails(self):
        """'None' as string should fail."""
        result = check_contact_name("None")
        assert result.status == "fail"
        assert result.severity == "critical"

    def test_none_none_fails(self):
        """'None None' should fail."""
        result = check_contact_name("None None")
        assert result.status == "fail"
        assert result.severity == "critical"

    def test_empty_string_passes(self):
        """Empty string should pass (will use generic greeting)."""
        result = check_contact_name("")
        assert result.status == "pass"

    def test_nan_passes(self):
        """NaN value should pass (will use generic greeting)."""
        result = check_contact_name("nan")
        assert result.status == "pass"

    def test_generic_name_fails(self):
        """Generic names like 'Meet Our Team' should fail."""
        result = check_contact_name("Meet Our Team")
        assert result.status == "fail"
        assert "meet our team" in result.issue_detail.lower()

    def test_social_media_fails(self):
        """'Social Media' should fail."""
        result = check_contact_name("Social Media")
        assert result.status == "fail"

    def test_short_name_warns(self):
        """Very short names should warn."""
        result = check_contact_name("Jo")
        assert result.status in ["pass", "warning"]


class TestCheckEmailNameMatch:
    """Tests for email-name consistency."""

    def test_matching_names(self):
        """Email and contact name match should pass."""
        result = check_email_name_match("john@company.com", "John Smith")
        assert result.status == "pass"

    def test_mismatched_names(self):
        """Different names should fail."""
        result = check_email_name_match("jeremy@company.com", "Brent Hutto")
        assert result.status == "fail"
        assert result.severity == "high"
        assert "jeremy" in result.issue_detail.lower()

    def test_generic_email_passes(self):
        """Generic emails (info@, support@) should pass regardless of name."""
        result = check_email_name_match("info@company.com", "John Smith")
        assert result.status == "pass"

    def test_contact_email_passes(self):
        """contact@ emails should pass."""
        result = check_email_name_match("contact@company.com", "Sarah Jones")
        assert result.status == "pass"

    def test_sales_email_passes(self):
        """sales@ emails should pass."""
        result = check_email_name_match("sales@company.com", "Mike Johnson")
        assert result.status == "pass"

    def test_none_contact_skips(self):
        """None contact name should skip the check."""
        result = check_email_name_match("john@company.com", "None")
        assert result.status in ["pass", "warning"]

    def test_case_insensitive_match(self):
        """Name matching should be case insensitive."""
        result = check_email_name_match("JOHN@company.com", "john Smith")
        assert result.status == "pass"


class TestCheckNoTemplateVars:
    """Tests for template variable detection."""

    def test_clean_email_passes(self):
        """Email without template vars should pass."""
        body = "Hi John, I noticed your company is doing great work."
        result = check_no_template_vars(body)
        assert result.status == "pass"

    def test_product_brand_fails(self):
        """{{product.brand}} should fail."""
        body = "I noticed your ads with {{product.brand}}."
        result = check_no_template_vars(body)
        assert result.status == "fail"
        assert result.severity == "critical"
        assert "{{product.brand}}" in result.issue_detail

    def test_any_template_var_fails(self):
        """Any {{...}} pattern should fail."""
        body = "Hello {{first_name}}, welcome to {{company_name}}."
        result = check_no_template_vars(body)
        assert result.status == "fail"

    def test_curly_braces_in_text_pass(self):
        """Regular curly braces without template syntax should pass."""
        body = "The revenue increased by {large amount}."
        result = check_no_template_vars(body)
        assert result.status == "pass"


class TestCheckDomainMatch:
    """Tests for email domain verification."""

    def test_matching_domain(self):
        """Email domain matching company should pass."""
        result = check_domain_match("john@reepequity.com", "REEP Equity")
        assert result.status == "pass"

    def test_unrelated_domain_fails(self):
        """Completely unrelated domain should fail."""
        result = check_domain_match("editor@popculture.com", "John Wyche")
        assert result.status == "fail"
        assert result.severity == "high"

    def test_generic_domain_warns(self):
        """Generic domains like gmail should warn."""
        result = check_domain_match("john@gmail.com", "John's Real Estate")
        assert result.status == "warning"

    def test_media_domain_fails(self):
        """Media domains for non-media companies should fail."""
        result = check_domain_match("dsanchez@housingwire.com", "Adwerx Enterprise")
        assert result.status == "fail"

    def test_partial_match_passes(self):
        """Partial domain match should pass."""
        result = check_domain_match("brian@greatlakes-film.com", "Great Lakes Film & Graphics")
        assert result.status == "pass"


class TestCheckGreetingName:
    """Tests for greeting name extraction and validation."""

    def test_extract_greeting_name(self):
        """Should extract name from 'Hi Name,' pattern."""
        body = "Hi John,\n\nI noticed your company..."
        result = check_greeting_name(body, "John Smith")
        assert result.status == "pass"

    def test_hi_none_fails(self):
        """'Hi None,' should fail."""
        body = "Hi None,\n\nI noticed your company..."
        result = check_greeting_name(body, "None None")
        assert result.status == "fail"
        assert result.severity == "critical"

    def test_greeting_mismatch_fails(self):
        """Greeting name not matching contact should fail."""
        body = "Hi Sarah,\n\nI noticed your company..."
        result = check_greeting_name(body, "John Smith")
        assert result.status == "fail"
        assert "sarah" in result.issue_detail.lower() or "john" in result.issue_detail.lower()

    def test_no_greeting_passes(self):
        """Email without personal greeting should pass."""
        body = "Hello,\n\nI noticed your company..."
        result = check_greeting_name(body, "John Smith")
        assert result.status == "pass"


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_create_pass_result(self):
        """Should create passing result."""
        result = CheckResult(
            check_name="test_check",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )
        assert result.status == "pass"
        assert result.check_name == "test_check"

    def test_create_fail_result(self):
        """Should create failing result with details."""
        result = CheckResult(
            check_name="contact_name",
            status="fail",
            severity="critical",
            issue_detail="Contact name is 'None'",
            suggested_fix="Remove or replace with valid name"
        )
        assert result.status == "fail"
        assert result.severity == "critical"
        assert "None" in result.issue_detail
