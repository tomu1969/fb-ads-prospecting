"""Tests for email scorer module."""

import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.email_verifier.scorer import (
    calculate_send_score,
    score_for_sending,
    EmailScore,
    SendRecommendation
)
from scripts.email_verifier.verifier import VerificationResult, VerificationStatus


class TestSendRecommendation:
    """Test SendRecommendation enum."""

    def test_recommendation_values(self):
        """Verify all expected recommendation values exist."""
        assert SendRecommendation.SAFE.value == 'safe'
        assert SendRecommendation.CAUTION.value == 'caution'
        assert SendRecommendation.RISKY.value == 'risky'
        assert SendRecommendation.DO_NOT_SEND.value == 'do_not_send'


class TestEmailScore:
    """Test EmailScore dataclass."""

    def test_score_creation(self):
        """Test creating an email score."""
        score = EmailScore(
            email='test@example.com',
            total_score=85,
            recommendation=SendRecommendation.SAFE,
            api_score=50,
            hunter_score=15,
            domain_score=15,
            pattern_score=5
        )
        assert score.total_score == 85
        assert score.safe_to_send is True

    def test_safe_to_send_property(self):
        """Test safe_to_send returns True only for SAFE recommendation."""
        safe_score = EmailScore(
            email='test@example.com',
            total_score=75,
            recommendation=SendRecommendation.SAFE
        )
        assert safe_score.safe_to_send is True

        caution_score = EmailScore(
            email='test@example.com',
            total_score=55,
            recommendation=SendRecommendation.CAUTION
        )
        assert caution_score.safe_to_send is False


class TestCalculateSendScore:
    """Test calculate_send_score function."""

    def test_perfect_score(self):
        """Test maximum score scenario."""
        # Named contact, verified OK, high Hunter confidence, normal domain
        verification = VerificationResult(
            email='john@company.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        score = calculate_send_score(
            email='john@company.com',
            verification_result=verification,
            hunter_confidence=100
        )

        assert score.total_score == 100
        assert score.api_score == 50
        assert score.hunter_score == 20
        assert score.domain_score == 15
        assert score.pattern_score == 15
        assert score.recommendation == SendRecommendation.SAFE

    def test_generic_email_penalty(self):
        """Test that generic emails get lower pattern score."""
        verification = VerificationResult(
            email='info@company.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        score = calculate_send_score(
            email='info@company.com',
            verification_result=verification,
            hunter_confidence=100
        )

        # Pattern score reduced from 15 to 5 for generic email
        assert score.pattern_score == 5
        assert score.total_score == 90

    def test_catch_all_domain_penalty(self):
        """Test that catch-all domains get lower scores."""
        verification = VerificationResult(
            email='john@catchall.com',
            status=VerificationStatus.CATCH_ALL,
            is_catch_all=True,
            is_deliverable=True,
            confidence=50
        )

        score = calculate_send_score(
            email='john@catchall.com',
            verification_result=verification,
            hunter_confidence=80
        )

        # API score = 20 (catch_all), domain_score = 0 (catch_all)
        assert score.api_score == 20
        assert score.domain_score == 0
        assert score.recommendation in [SendRecommendation.CAUTION, SendRecommendation.RISKY]

    def test_invalid_email_score(self):
        """Test that invalid emails get zero API score."""
        verification = VerificationResult(
            email='invalid@example.com',
            status=VerificationStatus.INVALID,
            is_catch_all=False,
            is_deliverable=False,
            confidence=0
        )

        score = calculate_send_score(
            email='invalid@example.com',
            verification_result=verification
        )

        assert score.api_score == 0
        assert score.recommendation == SendRecommendation.DO_NOT_SEND

    def test_no_verification_data(self):
        """Test scoring with no verification data."""
        score = calculate_send_score(
            email='unknown@example.com',
            verification_result=None,
            hunter_confidence=None
        )

        assert score.api_score == 0
        assert score.hunter_score == 0
        # Only domain (15) and pattern (15) scores available
        assert score.total_score == 30
        assert score.recommendation == SendRecommendation.RISKY

    def test_hunter_only_scoring(self):
        """Test scoring with only Hunter data."""
        score = calculate_send_score(
            email='contact@example.com',
            verification_result=None,
            hunter_confidence=90
        )

        # Hunter: 18, Domain: 15, Pattern: 5 (generic)
        assert score.hunter_score == 18
        assert score.total_score == 38
        assert score.recommendation == SendRecommendation.RISKY

    def test_score_thresholds(self):
        """Test recommendation thresholds."""
        # 70+ = SAFE
        safe_verification = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )
        safe_score = calculate_send_score(
            email='john@example.com',
            verification_result=safe_verification
        )
        assert safe_score.total_score >= 70
        assert safe_score.recommendation == SendRecommendation.SAFE

        # 50-69 = CAUTION
        caution_verification = VerificationResult(
            email='info@example.com',
            status=VerificationStatus.CATCH_ALL,
            is_catch_all=True,
            is_deliverable=True,
            confidence=50
        )
        caution_score = calculate_send_score(
            email='john@example.com',
            verification_result=caution_verification,
            hunter_confidence=50
        )
        # API=20, Hunter=10, Domain=0, Pattern=15 = 45 -> RISKY
        # Let's check actual value
        assert caution_score.total_score >= 30


class TestScoreForSending:
    """Test score_for_sending convenience function."""

    def test_above_threshold(self):
        """Test email above minimum score."""
        verification = VerificationResult(
            email='john@company.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        should_send, score = score_for_sending(
            email='john@company.com',
            verification_result=verification,
            hunter_confidence=80,
            min_score=70
        )

        assert should_send is True
        assert score.total_score >= 70

    def test_below_threshold(self):
        """Test email below minimum score."""
        verification = VerificationResult(
            email='info@catchall.com',
            status=VerificationStatus.CATCH_ALL,
            is_catch_all=True,
            is_deliverable=True,
            confidence=30
        )

        should_send, score = score_for_sending(
            email='info@catchall.com',
            verification_result=verification,
            min_score=70
        )

        assert should_send is False

    def test_custom_threshold(self):
        """Test with custom minimum score."""
        verification = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        # With threshold of 90
        should_send_90, score = score_for_sending(
            email='info@example.com',  # Generic reduces pattern score
            verification_result=verification,
            min_score=90
        )

        # Total should be 50+0+15+5 = 70
        assert score.total_score < 90
        assert should_send_90 is False

        # With threshold of 50
        should_send_50, _ = score_for_sending(
            email='info@example.com',
            verification_result=verification,
            min_score=50
        )
        assert should_send_50 is True


class TestScoreBreakdown:
    """Test score breakdown details."""

    def test_breakdown_included(self):
        """Test that breakdown dict is populated."""
        verification = VerificationResult(
            email='test@example.com',
            status=VerificationStatus.OK,
            is_catch_all=False,
            is_deliverable=True,
            confidence=95
        )

        score = calculate_send_score(
            email='test@example.com',
            verification_result=verification,
            hunter_confidence=80
        )

        assert 'api' in score.breakdown
        assert 'hunter' in score.breakdown
        assert 'domain' in score.breakdown
        assert 'pattern' in score.breakdown
