"""Email Scorer - Multi-factor scoring system for email deliverability.

Combines multiple signals to determine if an email is safe to send:
- API verification result (50%)
- Hunter.io confidence (20%)
- Domain type/catch-all status (15%)
- Email pattern (generic vs named) (15%)
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Import verification types (handle circular import)
try:
    from .verifier import VerificationStatus, VerificationResult, is_generic_email
except ImportError:
    from scripts.email_verifier.verifier import VerificationStatus, VerificationResult, is_generic_email


class SendRecommendation(Enum):
    """Sending recommendation based on score."""
    SAFE = 'safe'           # Score 70+: Safe to send
    CAUTION = 'caution'     # Score 50-69: Send with caution
    RISKY = 'risky'         # Score 30-49: High risk
    DO_NOT_SEND = 'do_not_send'  # Score <30: Do not send


@dataclass
class EmailScore:
    """Result of email scoring."""
    email: str
    total_score: int                    # 0-100 composite score
    recommendation: SendRecommendation
    api_score: int = 0                  # 0-50 points
    hunter_score: int = 0               # 0-20 points
    domain_score: int = 0               # 0-15 points
    pattern_score: int = 0              # 0-15 points
    breakdown: Dict[str, Any] = None

    @property
    def safe_to_send(self) -> bool:
        """Check if email meets minimum score threshold."""
        return self.recommendation == SendRecommendation.SAFE


def calculate_send_score(
    email: str,
    verification_result: Optional[VerificationResult] = None,
    hunter_confidence: Optional[int] = None,
    is_catch_all_domain: Optional[bool] = None
) -> EmailScore:
    """
    Calculate composite send score for an email.

    Scoring weights:
    - API Verification: 50 points max
    - Hunter Confidence: 20 points max
    - Domain Type: 15 points max
    - Email Pattern: 15 points max

    Args:
        email: Email address to score.
        verification_result: Result from MillionVerifier API.
        hunter_confidence: Hunter.io confidence score (0-100).
        is_catch_all_domain: Override for catch-all domain detection.

    Returns:
        EmailScore with total and breakdown.
    """
    breakdown = {}
    api_score = 0
    hunter_score = 0
    domain_score = 0
    pattern_score = 0

    # 1. API Verification Score (50 points max)
    if verification_result:
        if verification_result.status == VerificationStatus.OK:
            api_score = 50
            breakdown['api'] = 'OK - mailbox verified'
        elif verification_result.status == VerificationStatus.CATCH_ALL:
            api_score = 20
            breakdown['api'] = 'Catch-all domain (risky)'
        elif verification_result.status == VerificationStatus.UNKNOWN:
            api_score = 10
            breakdown['api'] = 'Unknown - could not verify'
        elif verification_result.status == VerificationStatus.INVALID:
            api_score = 0
            breakdown['api'] = 'Invalid - will bounce'
        else:
            api_score = 5
            breakdown['api'] = 'Error during verification'
    else:
        api_score = 0
        breakdown['api'] = 'Not verified'

    # 2. Hunter Confidence Score (20 points max)
    if hunter_confidence is not None and hunter_confidence > 0:
        # Scale 0-100 confidence to 0-20 points
        hunter_score = int(hunter_confidence * 0.2)
        breakdown['hunter'] = f'{hunter_confidence}% confidence'
    else:
        hunter_score = 0
        breakdown['hunter'] = 'No Hunter data'

    # 3. Domain Type Score (15 points max)
    # Check if domain is catch-all
    if is_catch_all_domain is not None:
        is_catch_all = is_catch_all_domain
    elif verification_result:
        is_catch_all = verification_result.is_catch_all
    else:
        is_catch_all = False

    if not is_catch_all:
        domain_score = 15
        breakdown['domain'] = 'Normal domain'
    else:
        domain_score = 0
        breakdown['domain'] = 'Catch-all domain (accepts all)'

    # 4. Email Pattern Score (15 points max)
    if is_generic_email(email):
        pattern_score = 5
        breakdown['pattern'] = 'Generic email (info@, sales@, etc.)'
    else:
        pattern_score = 15
        breakdown['pattern'] = 'Named contact'

    # Calculate total
    total_score = api_score + hunter_score + domain_score + pattern_score

    # Determine recommendation
    # Special case: INVALID emails should never be sent regardless of score
    if verification_result and verification_result.status == VerificationStatus.INVALID:
        recommendation = SendRecommendation.DO_NOT_SEND
    elif total_score >= 70:
        recommendation = SendRecommendation.SAFE
    elif total_score >= 50:
        recommendation = SendRecommendation.CAUTION
    elif total_score >= 30:
        recommendation = SendRecommendation.RISKY
    else:
        recommendation = SendRecommendation.DO_NOT_SEND

    return EmailScore(
        email=email,
        total_score=total_score,
        recommendation=recommendation,
        api_score=api_score,
        hunter_score=hunter_score,
        domain_score=domain_score,
        pattern_score=pattern_score,
        breakdown=breakdown
    )


def score_for_sending(
    email: str,
    verification_result: Optional[VerificationResult] = None,
    hunter_confidence: Optional[int] = None,
    min_score: int = 70
) -> tuple[bool, EmailScore]:
    """
    Quick check if email meets minimum score for sending.

    Args:
        email: Email address to check.
        verification_result: API verification result.
        hunter_confidence: Hunter.io confidence.
        min_score: Minimum score threshold (default 70).

    Returns:
        Tuple of (should_send: bool, score: EmailScore)
    """
    score = calculate_send_score(
        email=email,
        verification_result=verification_result,
        hunter_confidence=hunter_confidence
    )

    should_send = score.total_score >= min_score
    return should_send, score


def print_score_breakdown(score: EmailScore) -> None:
    """Print detailed score breakdown."""
    print(f"\nEmail: {score.email}")
    print(f"Total Score: {score.total_score}/100")
    print(f"Recommendation: {score.recommendation.value.upper()}")
    print("\nBreakdown:")
    print(f"  API Verification:  {score.api_score}/50  - {score.breakdown.get('api', 'N/A')}")
    print(f"  Hunter Confidence: {score.hunter_score}/20  - {score.breakdown.get('hunter', 'N/A')}")
    print(f"  Domain Type:       {score.domain_score}/15  - {score.breakdown.get('domain', 'N/A')}")
    print(f"  Email Pattern:     {score.pattern_score}/15  - {score.breakdown.get('pattern', 'N/A')}")
    print(f"\nSafe to Send: {'YES' if score.safe_to_send else 'NO'}")
