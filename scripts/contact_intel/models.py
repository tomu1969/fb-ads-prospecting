"""Data models for Contact Intelligence System.

Pydantic models for representing people, companies, relationships,
email messages, and Gmail account configurations.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class AuthType(str, Enum):
    """Authentication type for Gmail accounts."""
    OAUTH = "oauth"
    IMAP = "imap"


class EmailMessage(BaseModel):
    """Represents a single email message."""
    message_id: str
    thread_id: Optional[str] = None
    from_email: str
    from_name: Optional[str] = None
    to_emails: List[str] = Field(default_factory=list)
    cc_emails: List[str] = Field(default_factory=list)
    bcc_emails: List[str] = Field(default_factory=list)
    subject: Optional[str] = None
    date: datetime
    in_reply_to: Optional[str] = None
    account: str  # Which account this came from


class Person(BaseModel):
    """Represents a contact/person in the graph."""
    id: Optional[str] = None  # Neo4j node ID
    name: str
    emails: List[str] = Field(default_factory=list)
    linkedin_url: Optional[str] = None
    company: Optional[str] = None
    role: Optional[str] = None
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    def merge_with(self, other: "Person") -> "Person":
        """Merge another Person record into this one.

        Strategy:
        - Combine emails (unique)
        - Keep earliest first_seen
        - Keep latest last_seen
        - Keep this person's name
        - Adopt non-null fields from other if this has None

        Args:
            other: Another Person record to merge with this one

        Returns:
            A new Person with merged data
        """
        # Combine unique emails
        merged_emails = list(set(self.emails) | set(other.emails))

        # Get earliest first_seen
        first_seen = self.first_seen
        if other.first_seen:
            if first_seen is None or other.first_seen < first_seen:
                first_seen = other.first_seen

        # Get latest last_seen
        last_seen = self.last_seen
        if other.last_seen:
            if last_seen is None or other.last_seen > last_seen:
                last_seen = other.last_seen

        return Person(
            id=self.id or other.id,
            name=self.name,  # Keep primary person's name
            emails=merged_emails,
            linkedin_url=self.linkedin_url or other.linkedin_url,
            company=self.company or other.company,
            role=self.role or other.role,
            first_seen=first_seen,
            last_seen=last_seen,
        )


class Company(BaseModel):
    """Represents a company."""
    name: str
    domain: Optional[str] = None
    industry: Optional[str] = None

    @staticmethod
    def extract_domain_from_email(email: str) -> Optional[str]:
        """Extract company domain from email address.

        Args:
            email: Email address like "john@acme.com"

        Returns:
            Domain like "acme.com", or None if it's a personal email provider
        """
        from scripts.contact_intel.config import is_personal_email_domain

        if not email or "@" not in email:
            return None

        domain = email.split("@")[1].lower()

        # Return None for personal email providers
        if is_personal_email_domain(domain):
            return None

        return domain


class Relationship(BaseModel):
    """Represents a relationship between two people."""
    from_person_id: str
    to_person_id: str
    relationship_type: str  # KNOWS, INTRODUCED, CC_TOGETHER, LINKEDIN_CONNECTED, WORKS_AT
    strength: int = 1  # 1-10
    email_count: int = 0
    first_contact: Optional[datetime] = None
    last_contact: Optional[datetime] = None
    introduced_by: Optional[str] = None


class GmailAccount(BaseModel):
    """Configuration for a Gmail account."""
    name: str
    email: Optional[str] = None
    auth_type: AuthType
    token_path: Optional[str] = None  # For OAuth
    app_password: Optional[str] = None  # For IMAP
