"""Tests for Contact Intelligence data models.

TDD: These tests are written first, before implementation.
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError


class TestPersonModel:
    """Tests for the Person model."""

    def test_person_model_validation(self):
        """Should validate Person with required fields (name, emails)."""
        from scripts.contact_intel.models import Person

        # Valid person with required fields
        person = Person(name="John Doe", emails=["john@example.com"])
        assert person.name == "John Doe"
        assert person.emails == ["john@example.com"]

        # Person with all optional fields
        person_full = Person(
            name="Jane Smith",
            emails=["jane@company.com", "jane.smith@gmail.com"],
            linkedin_url="https://linkedin.com/in/janesmith",
            company="Acme Corp",
            role="CEO",
            first_seen=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_seen=datetime(2024, 12, 31, tzinfo=timezone.utc),
        )
        assert person_full.company == "Acme Corp"
        assert person_full.linkedin_url == "https://linkedin.com/in/janesmith"

    def test_person_requires_name(self):
        """Should fail validation without name."""
        from scripts.contact_intel.models import Person

        with pytest.raises(ValidationError):
            Person(emails=["test@example.com"])

    def test_person_empty_emails_allowed(self):
        """Should allow empty emails list (default)."""
        from scripts.contact_intel.models import Person

        person = Person(name="Unknown Contact")
        assert person.emails == []

    def test_person_merge_emails(self):
        """Should merge email lists when combining Person records."""
        from scripts.contact_intel.models import Person

        person1 = Person(
            name="John Doe",
            emails=["john@work.com"],
            first_seen=datetime(2024, 6, 1, tzinfo=timezone.utc),
            last_seen=datetime(2024, 8, 1, tzinfo=timezone.utc),
        )
        person2 = Person(
            name="John D.",
            emails=["john@personal.com", "john@work.com"],  # Overlapping email
            first_seen=datetime(2024, 1, 1, tzinfo=timezone.utc),  # Earlier
            last_seen=datetime(2024, 12, 1, tzinfo=timezone.utc),  # Later
            company="New Company",
        )

        merged = person1.merge_with(person2)

        # Should combine unique emails
        assert set(merged.emails) == {"john@work.com", "john@personal.com"}
        # Should keep earliest first_seen
        assert merged.first_seen == datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Should keep latest last_seen
        assert merged.last_seen == datetime(2024, 12, 1, tzinfo=timezone.utc)
        # Should keep primary person's name
        assert merged.name == "John Doe"
        # Should adopt non-null fields from other
        assert merged.company == "New Company"

    def test_person_merge_preserves_existing_fields(self):
        """Merge should not overwrite existing non-null fields."""
        from scripts.contact_intel.models import Person

        person1 = Person(
            name="John Doe",
            emails=["john@example.com"],
            company="Original Corp",
            role="Developer",
        )
        person2 = Person(
            name="Johnny",
            emails=["johnny@other.com"],
            company="Other Corp",  # Should NOT overwrite
            linkedin_url="https://linkedin.com/in/john",  # Should be adopted
        )

        merged = person1.merge_with(person2)
        assert merged.company == "Original Corp"  # Preserved
        assert merged.linkedin_url == "https://linkedin.com/in/john"  # Adopted


class TestEmailMessageModel:
    """Tests for the EmailMessage model."""

    def test_email_message_model(self):
        """Should parse email headers into EmailMessage model."""
        from scripts.contact_intel.models import EmailMessage

        email = EmailMessage(
            message_id="<abc123@mail.gmail.com>",
            thread_id="thread123",
            from_email="sender@example.com",
            from_name="Sender Name",
            to_emails=["recipient@example.com"],
            cc_emails=["cc1@example.com", "cc2@example.com"],
            subject="Test Subject",
            date=datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc),
            account="personal",
        )

        assert email.message_id == "<abc123@mail.gmail.com>"
        assert email.from_email == "sender@example.com"
        assert email.from_name == "Sender Name"
        assert len(email.to_emails) == 1
        assert len(email.cc_emails) == 2
        assert email.account == "personal"

    def test_email_message_requires_message_id(self):
        """Should require message_id."""
        from scripts.contact_intel.models import EmailMessage

        with pytest.raises(ValidationError):
            EmailMessage(
                from_email="sender@example.com",
                date=datetime.now(timezone.utc),
                account="test",
            )

    def test_email_message_requires_account(self):
        """Should require account field."""
        from scripts.contact_intel.models import EmailMessage

        with pytest.raises(ValidationError):
            EmailMessage(
                message_id="<abc@mail.com>",
                from_email="sender@example.com",
                date=datetime.now(timezone.utc),
            )

    def test_email_message_defaults(self):
        """Should have sensible defaults for optional fields."""
        from scripts.contact_intel.models import EmailMessage

        email = EmailMessage(
            message_id="<abc@mail.com>",
            from_email="sender@example.com",
            date=datetime.now(timezone.utc),
            account="test",
        )

        assert email.to_emails == []
        assert email.cc_emails == []
        assert email.bcc_emails == []
        assert email.thread_id is None
        assert email.subject is None
        assert email.in_reply_to is None


class TestRelationshipModel:
    """Tests for the Relationship model."""

    def test_relationship_model(self):
        """Should create Relationship with strength, dates, counts."""
        from scripts.contact_intel.models import Relationship

        rel = Relationship(
            from_person_id="person_1",
            to_person_id="person_2",
            relationship_type="KNOWS",
            strength=7,
            email_count=15,
            first_contact=datetime(2023, 1, 1, tzinfo=timezone.utc),
            last_contact=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )

        assert rel.from_person_id == "person_1"
        assert rel.to_person_id == "person_2"
        assert rel.relationship_type == "KNOWS"
        assert rel.strength == 7
        assert rel.email_count == 15

    def test_relationship_types(self):
        """Should accept various relationship types."""
        from scripts.contact_intel.models import Relationship

        types = ["KNOWS", "INTRODUCED", "CC_TOGETHER", "LINKEDIN_CONNECTED", "WORKS_AT"]

        for rel_type in types:
            rel = Relationship(
                from_person_id="a",
                to_person_id="b",
                relationship_type=rel_type,
            )
            assert rel.relationship_type == rel_type

    def test_relationship_defaults(self):
        """Should have sensible defaults."""
        from scripts.contact_intel.models import Relationship

        rel = Relationship(
            from_person_id="a",
            to_person_id="b",
            relationship_type="KNOWS",
        )

        assert rel.strength == 1
        assert rel.email_count == 0
        assert rel.first_contact is None
        assert rel.last_contact is None
        assert rel.introduced_by is None

    def test_relationship_with_introduction(self):
        """Should track who made the introduction."""
        from scripts.contact_intel.models import Relationship

        rel = Relationship(
            from_person_id="alice",
            to_person_id="bob",
            relationship_type="INTRODUCED",
            introduced_by="charlie",
        )

        assert rel.introduced_by == "charlie"


class TestCompanyModel:
    """Tests for the Company model."""

    def test_company_model(self):
        """Should create Company with basic attributes."""
        from scripts.contact_intel.models import Company

        company = Company(
            name="Acme Corp",
            domain="acme.com",
            industry="Technology",
        )

        assert company.name == "Acme Corp"
        assert company.domain == "acme.com"
        assert company.industry == "Technology"

    def test_company_extraction_from_email(self):
        """Should extract company domain from email address."""
        from scripts.contact_intel.models import Company

        # Test the helper method
        domain = Company.extract_domain_from_email("john@acme.com")
        assert domain == "acme.com"

        # Should handle subdomains
        domain = Company.extract_domain_from_email("jane@mail.company.co.uk")
        assert domain == "mail.company.co.uk"

        # Should return None for personal email providers
        domain = Company.extract_domain_from_email("user@gmail.com")
        assert domain is None

        domain = Company.extract_domain_from_email("user@yahoo.com")
        assert domain is None

        domain = Company.extract_domain_from_email("user@hotmail.com")
        assert domain is None

    def test_company_requires_name(self):
        """Should require name field."""
        from scripts.contact_intel.models import Company

        with pytest.raises(ValidationError):
            Company(domain="example.com")


class TestGmailAccountModel:
    """Tests for the GmailAccount model."""

    def test_gmail_account_oauth(self):
        """Should create OAuth account config."""
        from scripts.contact_intel.models import GmailAccount, AuthType

        account = GmailAccount(
            name="personal",
            email="user@gmail.com",
            auth_type=AuthType.OAUTH,
            token_path="/path/to/token.json",
        )

        assert account.name == "personal"
        assert account.auth_type == AuthType.OAUTH
        assert account.token_path == "/path/to/token.json"

    def test_gmail_account_imap(self):
        """Should create IMAP account config with app password."""
        from scripts.contact_intel.models import GmailAccount, AuthType

        account = GmailAccount(
            name="work",
            email="user@company.com",
            auth_type=AuthType.IMAP,
            app_password="xxxx-xxxx-xxxx-xxxx",
        )

        assert account.auth_type == AuthType.IMAP
        assert account.app_password == "xxxx-xxxx-xxxx-xxxx"

    def test_auth_type_enum(self):
        """Should have OAUTH and IMAP auth types."""
        from scripts.contact_intel.models import AuthType

        assert AuthType.OAUTH.value == "oauth"
        assert AuthType.IMAP.value == "imap"
