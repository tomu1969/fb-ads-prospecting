"""Email Verifier Module - Professional email verification with catch-all detection."""

from .verifier import (
    verify_email,
    verify_emails_bulk,
    VerificationResult,
    VerificationStatus
)
from .scorer import calculate_send_score, EmailScore

__all__ = [
    'verify_email',
    'verify_emails_bulk',
    'VerificationResult',
    'VerificationStatus',
    'calculate_send_score',
    'EmailScore'
]
