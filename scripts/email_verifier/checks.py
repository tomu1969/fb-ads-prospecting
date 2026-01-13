"""
Email verification check functions.

Each check returns a CheckResult with status, severity, and details.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class CheckResult:
    """Result of a verification check."""
    check_name: str
    status: str  # 'pass', 'fail', 'warning'
    severity: str  # 'critical', 'high', 'medium', 'low'
    issue_detail: str
    suggested_fix: str


# Invalid contact name patterns
INVALID_NAMES = {
    'none', 'none none', 'nan', 'n/a', 'null', 'undefined', 'unknown',
    'meet our team', 'social media', 'featured video', 'your information',
    'contact', 'info', 'support', 'team', 'admin', 'owner', 'manager',
    'contact us', 'get in touch', 'learn more', 'click here'
}

# Generic email prefixes (these don't represent a specific person)
GENERIC_EMAIL_PREFIXES = {
    'info', 'contact', 'support', 'sales', 'hello', 'hi', 'team',
    'admin', 'office', 'general', 'mail', 'enquiries', 'inquiries',
    'help', 'service', 'services', 'marketing', 'press', 'media',
    'careers', 'jobs', 'hr', 'billing', 'accounts', 'orders', 'orderdesk'
}

# Known media/news domains that are likely wrong for B2B outreach
MEDIA_DOMAINS = {
    'popculture.com', 'housingwire.com', 'inman.com', 'realtor.com',
    'zillow.com', 'redfin.com', 'wsj.com', 'nytimes.com', 'bloomberg.com',
    'cnn.com', 'forbes.com', 'businessinsider.com'
}

# Generic email domains
GENERIC_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com'
}


def check_contact_name(name: str) -> CheckResult:
    """
    Validate that contact name is a real person name.

    Args:
        name: Contact name to validate

    Returns:
        CheckResult with pass/fail status
    """
    name_str = str(name).strip() if name else ''
    name_lower = name_str.lower()

    # Empty contact name is acceptable (will use generic greeting)
    if not name_str or name_lower in ('', 'nan', '[generic]'):
        return CheckResult(
            check_name="contact_name",
            status="pass",
            severity="low",
            issue_detail="Empty contact name (using generic greeting)",
            suggested_fix=""
        )

    # Check for invalid placeholder names that should NOT be used
    if name_lower in ('none', 'none none'):
        return CheckResult(
            check_name="contact_name",
            status="fail",
            severity="critical",
            issue_detail=f"Contact name is '{name}' (invalid placeholder)",
            suggested_fix="Remove or replace with valid name"
        )

    if name_lower in INVALID_NAMES:
        return CheckResult(
            check_name="contact_name",
            status="fail",
            severity="critical",
            issue_detail=f"Invalid contact name: '{name}'",
            suggested_fix="Remove contact name or find valid name"
        )

    # Check for generic phrases
    for invalid in INVALID_NAMES:
        if invalid in name_lower:
            return CheckResult(
                check_name="contact_name",
                status="fail",
                severity="high",
                issue_detail=f"Contact name appears to be generic: '{name}'",
                suggested_fix="Find actual person name for contact"
            )

    # Check minimum length
    if len(name.strip()) < 2:
        return CheckResult(
            check_name="contact_name",
            status="warning",
            severity="medium",
            issue_detail=f"Contact name is very short: '{name}'",
            suggested_fix="Verify this is a valid name"
        )

    return CheckResult(
        check_name="contact_name",
        status="pass",
        severity="low",
        issue_detail="",
        suggested_fix=""
    )


def check_email_name_match(email: str, contact_name: str) -> CheckResult:
    """
    Check if email address matches contact name.

    Extracts first name from email and compares with contact_name.
    Generic emails (info@, support@) are allowed regardless of name.

    Args:
        email: Email address
        contact_name: Contact name being addressed

    Returns:
        CheckResult with pass/fail status
    """
    if not email or not contact_name:
        return CheckResult(
            check_name="email_name_match",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )

    # Skip check for invalid contact names
    contact_lower = str(contact_name).lower().strip()
    if contact_lower in INVALID_NAMES or contact_lower in ('none', 'none none', 'nan'):
        return CheckResult(
            check_name="email_name_match",
            status="pass",
            severity="low",
            issue_detail="Skipped - contact name is invalid",
            suggested_fix=""
        )

    # Extract email prefix
    email_prefix = email.split('@')[0].lower() if '@' in email else ''

    # Skip check for generic email prefixes
    if email_prefix in GENERIC_EMAIL_PREFIXES:
        return CheckResult(
            check_name="email_name_match",
            status="pass",
            severity="low",
            issue_detail=f"Generic email prefix '{email_prefix}' - no name match required",
            suggested_fix=""
        )

    # Extract first name from contact
    first_name = contact_name.split()[0].lower() if contact_name else ''

    # Check if first name appears in email prefix
    # Handle patterns like: john, johnsmith, john.smith, jsmith
    if first_name and len(first_name) >= 2:
        if first_name in email_prefix or email_prefix.startswith(first_name[0]):
            return CheckResult(
                check_name="email_name_match",
                status="pass",
                severity="low",
                issue_detail="",
                suggested_fix=""
            )

    # Names don't match - this is a problem
    return CheckResult(
        check_name="email_name_match",
        status="fail",
        severity="high",
        issue_detail=f"Email '{email_prefix}@...' doesn't match contact name '{contact_name}'",
        suggested_fix=f"Verify email belongs to {contact_name}, or update contact name"
    )


def check_no_template_vars(email_body: str) -> CheckResult:
    """
    Check that no template variables remain in email body.

    Looks for {{...}} patterns that should have been replaced.

    Args:
        email_body: Full email body text

    Returns:
        CheckResult with pass/fail status
    """
    if not email_body:
        return CheckResult(
            check_name="template_vars",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )

    # Find all {{...}} patterns
    template_vars = re.findall(r'\{\{[^}]+\}\}', email_body)

    if template_vars:
        return CheckResult(
            check_name="template_vars",
            status="fail",
            severity="critical",
            issue_detail=f"Unreplaced template variables: {', '.join(template_vars)}",
            suggested_fix="Replace template variables with actual values or remove"
        )

    return CheckResult(
        check_name="template_vars",
        status="pass",
        severity="low",
        issue_detail="",
        suggested_fix=""
    )


def check_domain_match(email: str, company_name: str) -> CheckResult:
    """
    Check if email domain is related to company.

    Flags emails from unrelated domains (media sites, etc.)

    Args:
        email: Email address
        company_name: Company name (page_name)

    Returns:
        CheckResult with pass/fail status
    """
    if not email or not company_name:
        return CheckResult(
            check_name="domain_match",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )

    # Extract domain
    domain = email.split('@')[1].lower() if '@' in email else ''

    if not domain:
        return CheckResult(
            check_name="domain_match",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )

    # Check for generic email domains
    if domain in GENERIC_DOMAINS:
        return CheckResult(
            check_name="domain_match",
            status="warning",
            severity="medium",
            issue_detail=f"Generic email domain: {domain}",
            suggested_fix="Consider finding company email address"
        )

    # Check for media domains (usually wrong)
    if domain in MEDIA_DOMAINS:
        return CheckResult(
            check_name="domain_match",
            status="fail",
            severity="high",
            issue_detail=f"Email domain '{domain}' appears to be a media site, not '{company_name}'",
            suggested_fix="Find correct company email address"
        )

    # Normalize company name for comparison
    company_lower = company_name.lower()
    # Remove common suffixes
    for suffix in ['llc', 'inc', 'corp', 'co', 'real estate', 'realty', 'realtor', 'group', 'team', '& associates']:
        company_lower = company_lower.replace(suffix, '')
    company_lower = re.sub(r'[^a-z0-9]', '', company_lower)

    # Normalize domain for comparison
    domain_base = domain.split('.')[0]  # Get first part before TLD
    domain_normalized = re.sub(r'[^a-z0-9]', '', domain_base)

    # Check for any overlap
    if len(company_lower) >= 3 and len(domain_normalized) >= 3:
        # Check if significant part of company name appears in domain or vice versa
        if (company_lower[:4] in domain_normalized or
            domain_normalized[:4] in company_lower or
            any(word in domain_normalized for word in company_lower.split() if len(word) >= 4)):
            return CheckResult(
                check_name="domain_match",
                status="pass",
                severity="low",
                issue_detail="",
                suggested_fix=""
            )

    # No obvious match - could be a problem
    return CheckResult(
        check_name="domain_match",
        status="fail",
        severity="high",
        issue_detail=f"Email domain '{domain}' doesn't appear related to company '{company_name}'",
        suggested_fix="Verify email belongs to this company"
    )


def check_greeting_name(email_body: str, contact_name: str) -> CheckResult:
    """
    Check that the greeting name in the email is valid and matches contact.

    Extracts name from patterns like "Hi John," or "Hello Sarah,"

    Args:
        email_body: Full email body text
        contact_name: Expected contact name

    Returns:
        CheckResult with pass/fail status
    """
    if not email_body:
        return CheckResult(
            check_name="greeting_name",
            status="pass",
            severity="low",
            issue_detail="",
            suggested_fix=""
        )

    # Check for generic greeting patterns first - these are always acceptable
    generic_patterns = [r'^Hi\s+there\s*,', r'^Hello\s+there\s*,', r'^Hello\s*,']
    for pattern in generic_patterns:
        if re.search(pattern, email_body.strip(), re.IGNORECASE):
            return CheckResult(
                check_name="greeting_name",
                status="pass",
                severity="low",
                issue_detail="Generic greeting used",
                suggested_fix=""
            )

    # Extract greeting name from personal greeting patterns
    greeting_patterns = [
        r'^Hi\s+([A-Za-z]+)',
        r'^Hello\s+([A-Za-z]+)',
        r'^Dear\s+([A-Za-z]+)',
        r'^Hey\s+([A-Za-z]+)',
    ]

    greeting_name = None
    for pattern in greeting_patterns:
        match = re.search(pattern, email_body.strip(), re.IGNORECASE)
        if match:
            greeting_name = match.group(1)
            break

    # No personal greeting found - that's fine
    if not greeting_name:
        return CheckResult(
            check_name="greeting_name",
            status="pass",
            severity="low",
            issue_detail="No personal greeting found",
            suggested_fix=""
        )

    # Check for "Hi None" pattern
    if greeting_name.lower() == 'none':
        return CheckResult(
            check_name="greeting_name",
            status="fail",
            severity="critical",
            issue_detail=f"Email starts with 'Hi None' - invalid greeting",
            suggested_fix="Replace with valid name or use generic greeting"
        )

    # If contact name is empty/nan, any personal greeting is suspicious
    contact_str = str(contact_name).strip() if contact_name else ''
    contact_lower = contact_str.lower()
    if not contact_str or contact_lower in ('nan', '', '[generic]'):
        # Contact is empty, but we have a personal greeting - could be a problem
        # But if the greeting looks reasonable, it's probably fine
        return CheckResult(
            check_name="greeting_name",
            status="pass",
            severity="low",
            issue_detail=f"Personal greeting '{greeting_name}' with empty contact name",
            suggested_fix=""
        )

    # Check if greeting name matches contact first name
    contact_first = contact_str.split()[0].lower() if contact_str else ''
    if contact_first and greeting_name.lower() != contact_first:
        return CheckResult(
            check_name="greeting_name",
            status="fail",
            severity="high",
            issue_detail=f"Greeting uses '{greeting_name}' but contact is '{contact_name}'",
            suggested_fix=f"Change greeting to match contact name"
        )

    return CheckResult(
        check_name="greeting_name",
        status="pass",
        severity="low",
        issue_detail="",
        suggested_fix=""
    )
