"""
Unit tests for ICP Discovery Pipeline modules (HARDENED).

Run with: pytest scripts/icp_discovery/tests/ -v
"""

import sys
from pathlib import Path

import pytest
import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from constants import (
    MESSAGE_CTA_TYPES, CALL_CTA_TYPES, FORM_CTA_TYPES,
    TRANSACTIONAL_CTA_TYPES, AD_VOLUME_THRESHOLDS,
    normalize_text,
)
from m0_normalizer import classify_destination_type, extract_domain, parse_timestamp
from m2_conv_gate import evaluate_gate, check_transactional_cta, check_transactional_url
from m3_money_score import calculate_money_score, calculate_ad_volume_score, calculate_velocity_score
from m4_urgency_score import calculate_urgency_score, count_keyword_matches


class TestTextNormalization:
    """Tests for ASCII text normalization."""

    def test_ascii_fold_spanish(self):
        """Spanish accented characters should be normalized."""
        assert normalize_text('evaluación') == 'evaluacion'
        assert normalize_text('asesoría') == 'asesoria'
        assert normalize_text('te contactarán') == 'te contactaran'

    def test_lowercase(self):
        """Text should be lowercased."""
        assert normalize_text('HELLO WORLD') == 'hello world'

    def test_empty(self):
        """Empty/None text should return empty string."""
        assert normalize_text('') == ''
        assert normalize_text(None) == ''


class TestDestinationClassification:
    """Tests for destination type classification."""

    def test_message_cta_types(self):
        """MESSAGE CTA types should classify as MESSAGE."""
        for cta in MESSAGE_CTA_TYPES:
            result = classify_destination_type(cta, '', [])
            assert result == 'MESSAGE', f"Expected MESSAGE for CTA {cta}"

    def test_call_cta_types(self):
        """CALL CTA types should classify as CALL."""
        for cta in CALL_CTA_TYPES:
            result = classify_destination_type(cta, '', [])
            assert result == 'CALL', f"Expected CALL for CTA {cta}"

    def test_form_cta_types(self):
        """FORM CTA types should classify as FORM (except CONTACT_US which is MESSAGE)."""
        for cta in FORM_CTA_TYPES:
            result = classify_destination_type(cta, '', [])
            # CONTACT_US is in both MESSAGE and FORM, MESSAGE takes priority
            if cta == 'CONTACT_US':
                assert result == 'MESSAGE', f"Expected MESSAGE for CTA {cta}"
            else:
                assert result == 'FORM', f"Expected FORM for CTA {cta}"

    def test_message_url_patterns(self):
        """Messenger/WhatsApp URLs should classify as MESSAGE."""
        urls = [
            'https://m.me/123456',
            'https://wa.me/1234567890',
            'https://api.whatsapp.com/send?phone=123',
        ]
        for url in urls:
            result = classify_destination_type('LEARN_MORE', url, [])
            assert result == 'MESSAGE', f"Expected MESSAGE for URL {url}"

    def test_call_url_patterns(self):
        """Tel: URLs should classify as CALL."""
        result = classify_destination_type('LEARN_MORE', 'tel:+1234567890', [])
        assert result == 'CALL'

    def test_form_url_patterns(self):
        """Form URLs should classify as FORM."""
        urls = [
            'https://example.com/contact-us',
            'https://calendly.com/schedule',
            'https://forms.gle/abc123',
        ]
        for url in urls:
            result = classify_destination_type('LEARN_MORE', url, [])
            assert result == 'FORM', f"Expected FORM for URL {url}"

    def test_default_web(self):
        """Generic URLs should classify as WEB."""
        result = classify_destination_type('LEARN_MORE', 'https://example.com', [])
        assert result == 'WEB'

    def test_messenger_platform(self):
        """Messenger-only platform should classify as MESSAGE."""
        result = classify_destination_type('', '', ['MESSENGER'])
        assert result == 'MESSAGE'


class TestDomainExtraction:
    """Tests for domain extraction."""

    def test_standard_url(self):
        """Standard URL should extract domain."""
        assert extract_domain('https://www.example.com/page') == 'example.com'

    def test_www_removal(self):
        """www prefix should be removed."""
        assert extract_domain('https://www.test.com') == 'test.com'

    def test_empty_url(self):
        """Empty URL should return None."""
        assert extract_domain('') is None
        assert extract_domain(None) is None


class TestTimestampParsing:
    """Tests for timestamp parsing."""

    def test_unix_timestamp(self):
        """Unix timestamp should parse correctly."""
        result = parse_timestamp(1704067200)  # 2024-01-01 00:00:00 UTC
        assert result is not None
        # Check year is 2023 or 2024 (timezone dependent)
        assert result.year in (2023, 2024)

    def test_iso_string(self):
        """ISO date string should parse correctly."""
        result = parse_timestamp('2024-01-15')
        assert result is not None
        assert result.day == 15

    def test_invalid(self):
        """Invalid timestamp should return None."""
        assert parse_timestamp('invalid') is None
        assert parse_timestamp(None) is None


class TestTransactionalURLDetection:
    """Tests for transactional URL detection (FIX 1)."""

    def test_transactional_domain(self):
        """Transactional domains should be detected."""
        assert check_transactional_url('https://shopify.com/mystore') is True
        assert check_transactional_url('https://play.google.com/store/apps') is True
        assert check_transactional_url('https://amazon.com/product/123') is True

    def test_transactional_path(self):
        """Transactional paths should be detected."""
        assert check_transactional_url('https://example.com/checkout') is True
        assert check_transactional_url('https://example.com/cart') is True
        assert check_transactional_url('https://example.com/products/item') is True

    def test_non_transactional(self):
        """Non-transactional URLs should not be flagged."""
        assert check_transactional_url('https://example.com/about') is False
        assert check_transactional_url('https://example.com/contact') is False
        assert check_transactional_url('https://mybusiness.com') is False


class TestConversationalGate:
    """Tests for conversational necessity gate (HARDENED)."""

    def test_high_message_share_passes(self):
        """High MESSAGE share should pass gate."""
        row = pd.Series({
            'share_message': 0.15,
            'share_call': 0,
            'share_form': 0,
            'share_web': 0.85,
            'dominant_cta': 'MESSAGE_PAGE',
            'dominant_dest': 'MESSAGE',
            'ad_texts_combined': '',
            'link_urls': '',
            'page_name': 'Test Business',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'MESSAGE'

    def test_high_call_share_passes(self):
        """High CALL share should pass gate."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0.15,
            'share_form': 0,
            'share_web': 0.85,
            'dominant_cta': 'CALL_NOW',
            'dominant_dest': 'CALL',
            'ad_texts_combined': '',
            'link_urls': '',
            'page_name': 'Test Business',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'CALL'

    def test_transactional_cta_with_no_signal_dropped(self):
        """Transactional CTA with no conversation share should be dropped."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'SHOP_NOW',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Buy now! 50% off! Free shipping!',
            'link_urls': 'https://shopify.com/mystore/checkout',
            'page_name': 'My Shop',
        })
        passed, reason = evaluate_gate(row)
        assert passed is False
        assert 'DROP' in reason

    def test_transactional_cta_check(self):
        """Transactional CTAs should be correctly identified."""
        for cta in TRANSACTIONAL_CTA_TYPES:
            assert check_transactional_cta(cta) is True

        assert check_transactional_cta('MESSAGE_PAGE') is False
        assert check_transactional_cta('CALL_NOW') is False


class TestRescuePath:
    """Tests for rescue path - regulated businesses with WEB destinations."""

    def test_realtor_rescued(self):
        """Real estate agents with LEARN_MORE should be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'LEARN_MORE',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Looking to buy or sell? We can help.',
            'link_urls': 'https://example.com',
            'page_name': 'Colorado Property Group of RE/MAX Pinnacle',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'RESCUED'

    def test_cpa_rescued(self):
        """CPAs with LEARN_MORE should be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'LEARN_MORE',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Tax planning for business owners.',
            'link_urls': 'https://example.com',
            'page_name': 'Eric Aragon, CPA',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'RESCUED'

    def test_roofing_company_rescued(self):
        """Roofing companies with LEARN_MORE should be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'LEARN_MORE',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Need a new roof? Contact us.',
            'link_urls': 'https://example.com',
            'page_name': 'Terra Nova Roofing',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'RESCUED'

    def test_mortgage_company_rescued(self):
        """Mortgage companies should be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'LEARN_MORE',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'We close on time. Every time.',
            'link_urls': 'https://example.com',
            'page_name': 'Bay Capital Mortgage Corporation',
        })
        passed, reason = evaluate_gate(row)
        assert passed is True
        assert reason == 'RESCUED'

    def test_content_farm_not_rescued(self):
        """Content farms should NOT be rescued (no regulated name pattern)."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'DOWNLOAD',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Read the latest romance novel!',
            'link_urls': 'https://example.com',
            'page_name': 'Metronovel-Wuxia-mx1',
        })
        passed, reason = evaluate_gate(row)
        assert passed is False

    def test_ecommerce_not_rescued(self):
        """E-commerce with SHOP_NOW should NOT be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'SHOP_NOW',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Best deals on home goods!',
            'link_urls': 'https://example.com',
            'page_name': 'The Home Depot',  # Has "home" but SHOP_NOW blocks rescue
        })
        passed, reason = evaluate_gate(row)
        assert passed is False

    def test_generic_business_not_rescued(self):
        """Generic business without regulated name should NOT be rescued."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'share_web': 1.0,
            'dominant_cta': 'LEARN_MORE',
            'dominant_dest': 'WEB',
            'ad_texts_combined': 'Check out our cool stuff!',
            'link_urls': 'https://example.com',
            'page_name': 'Cool Stuff Store',
        })
        passed, reason = evaluate_gate(row)
        assert passed is False
        assert reason == 'NO_SIGNAL_DROP'

    def test_regulated_name_detection(self):
        """Test has_regulated_business_name function directly."""
        from m2_conv_gate import has_regulated_business_name

        # Should match
        assert has_regulated_business_name('Colorado Property Group of RE/MAX Pinnacle') is True
        assert has_regulated_business_name('Eric Aragon, CPA') is True
        assert has_regulated_business_name('Terra Nova Roofing') is True
        assert has_regulated_business_name('Bay Capital Mortgage Corporation') is True
        assert has_regulated_business_name('Glass Doctor of Livonia, MI') is True
        assert has_regulated_business_name('Sean Dittmer Sells Hi-Desert Real Estate') is True
        assert has_regulated_business_name('University of Pittsburgh School of Business') is True

        # Should NOT match
        assert has_regulated_business_name('Metronovel-Wuxia-mx1') is False
        assert has_regulated_business_name('CafeDrama-JM1-25') is False
        assert has_regulated_business_name('Cool Stuff Store') is False


class TestMoneyScore:
    """Tests for money scoring (HARDENED)."""

    def test_ad_volume_score_capped(self):
        """Ad volume score should be capped at 15 (log scale)."""
        # 100+ ads should be near cap due to log scale
        score_100 = calculate_ad_volume_score(100)
        score_200 = calculate_ad_volume_score(200)
        assert score_100 >= 13  # Log scale: log2(101)*2.25 ≈ 14.9
        assert score_100 <= 15  # Capped at 15
        assert score_200 == score_100  # Both capped at 100 input
        # Lower values
        assert calculate_ad_volume_score(10) > 0
        assert calculate_ad_volume_score(0) == 0

    def test_velocity_score_log_scale(self):
        """Velocity score should use log scale."""
        # High velocity should be dampened by log scale
        score_10 = calculate_velocity_score(10)
        score_100 = calculate_velocity_score(100)
        # Log scale means 10x more ads doesn't give 10x score
        assert score_100 < score_10 * 3
        assert calculate_velocity_score(0) == 0

    def test_money_score_calculation(self):
        """Money score should be calculated correctly."""
        row = pd.Series({
            'active_ads': 50,
            'always_on_share': 0.5,
            'new_ads_30d': 5,
            'page_like_count': 10000,
        })
        result = calculate_money_score(row)

        assert 'money_score' in result
        assert result['money_score'] >= 0
        assert result['money_score'] <= 50
        assert 'money_breakdown' in result

    def test_money_score_max(self):
        """Maximum values should hit near-max score (log scale dampens)."""
        row = pd.Series({
            'active_ads': 200,
            'always_on_share': 1.0,
            'new_ads_30d': 20,
            'page_like_count': 500000,
        })
        result = calculate_money_score(row)
        # Log scale dampens extreme values, so max is typically 45-50
        assert result['money_score'] >= 45
        assert result['money_score'] <= 50

    def test_money_score_zero(self):
        """Zero/missing values should score zero."""
        row = pd.Series({
            'active_ads': 0,
            'always_on_share': 0,
            'new_ads_30d': 0,
            'page_like_count': 0,
        })
        result = calculate_money_score(row)
        assert result['money_score'] == 0


class TestUrgencyScore:
    """Tests for urgency scoring (NO ad_count - FIX 7)."""

    def test_urgency_score_calculation(self):
        """Urgency score should be calculated correctly."""
        row = pd.Series({
            'share_message': 0.3,
            'share_call': 0.1,
            'share_form': 0.2,
            'ad_texts_combined': 'Call now for a free consultation! Book today!',
        })
        result = calculate_urgency_score(row)

        assert 'urgency_score' in result
        assert result['urgency_score'] >= 0
        assert result['urgency_score'] <= 50
        assert 'urgency_breakdown' in result

    def test_urgency_immediacy_keywords(self):
        """Immediacy keywords should boost urgency score."""
        row_with_keywords = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'ad_texts_combined': 'Call NOW! Today only! Urgent! Limited time!',
        })
        row_without = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'ad_texts_combined': 'Learn more about our services.',
        })

        score_with = calculate_urgency_score(row_with_keywords)
        score_without = calculate_urgency_score(row_without)

        assert score_with['urgency_score'] > score_without['urgency_score']

    def test_urgency_score_zero(self):
        """Zero conversation share and no keywords should score low."""
        row = pd.Series({
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'ad_texts_combined': '',
        })
        result = calculate_urgency_score(row)
        assert result['urgency_score'] == 0


class TestKeywordMatching:
    """Tests for keyword pattern matching."""

    def test_keyword_count(self):
        """Keyword count should be correct."""
        from constants import COMPILED_IMMEDIACY

        text = "Call now! Book today! Available now!"
        count = count_keyword_matches(text, COMPILED_IMMEDIACY)
        assert count >= 2  # Should match 'now' multiple times and 'today'

    def test_empty_text(self):
        """Empty text should return zero matches."""
        from constants import COMPILED_IMMEDIACY

        assert count_keyword_matches('', COMPILED_IMMEDIACY) == 0
        assert count_keyword_matches(None, COMPILED_IMMEDIACY) == 0


class TestFitScore:
    """Tests for fit scoring (SPLIT MODEL v3 - explicit + implicit)."""

    def test_explicit_fit_score_calculation(self):
        """Explicit fit score should be calculated correctly."""
        from m5_fit_score import calculate_explicit_fit_score

        row = pd.Series({
            'ad_texts_combined': 'Do you qualify? Check requirements. Book your free consultation today!',
            'distinct_ctas': 3,
            'share_message': 0.3,
            'share_call': 0.1,
            'share_form': 0.2,
        })
        result = calculate_explicit_fit_score(row)

        assert 'explicit_fit_score' in result
        assert result['explicit_fit_score'] >= 0
        assert result['explicit_fit_score'] <= 30
        assert 'explicit_fit_breakdown' in result
        # Should have points for questions and qualification language
        assert result['exp_questions'] >= 1  # Has "?"
        assert result['exp_qualification'] >= 2  # Has "qualify" and "requirements"

    def test_explicit_fit_question_marks(self):
        """Question marks should boost explicit fit score."""
        from m5_fit_score import calculate_question_score

        assert calculate_question_score('') == 0
        assert calculate_question_score('No questions here.') == 0
        assert calculate_question_score('Is this a question?') == 1
        assert calculate_question_score('One? Two? Three?') == 2

    def test_explicit_fit_complexity(self):
        """Complexity score should detect multiple CTAs and destination types."""
        from m5_fit_score import calculate_complexity_score

        # Multiple CTAs only
        row1 = pd.Series({
            'distinct_ctas': 3,
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
        })
        assert calculate_complexity_score(row1) >= 2

        # Multiple destination types
        row2 = pd.Series({
            'distinct_ctas': 1,
            'share_message': 0.3,
            'share_call': 0,
            'share_form': 0.2,
        })
        assert calculate_complexity_score(row2) >= 2

    def test_implicit_fit_score_calculation(self):
        """Implicit fit score should be calculated correctly."""
        from m5_fit_score import calculate_implicit_fit_score

        row = pd.Series({
            'ad_texts_combined': 'Speak with our specialist team. We help with all your needs.',
            'dominant_dest': 'MESSAGE',
            'dominant_cta': 'MESSAGE_PAGE',
            'page_category': 'real estate agent',
        })
        result = calculate_implicit_fit_score(row)

        assert 'implicit_fit_score' in result
        assert result['implicit_fit_score'] >= 0
        assert result['implicit_fit_score'] <= 20
        assert 'implicit_fit_breakdown' in result
        # Should have points for advisor language and service breadth
        assert result['imp_advisor'] >= 0
        assert result['imp_service_breadth'] >= 0

    def test_implicit_fit_conv_entry_without_pricing(self):
        """Conversational entry without pricing should get full points."""
        from m5_fit_score import calculate_conv_entry_score

        # MESSAGE dest, no pricing - should get 6 points
        row1 = pd.Series({'dominant_dest': 'MESSAGE'})
        assert calculate_conv_entry_score(row1, 'Contact us for a consultation') == 6

        # MESSAGE dest with pricing - should get 0 points
        row2 = pd.Series({'dominant_dest': 'MESSAGE'})
        assert calculate_conv_entry_score(row2, 'Only $99! Sale ends today!') == 0

        # Non-MESSAGE dest - should get 0 points
        row3 = pd.Series({'dominant_dest': 'WEB'})
        assert calculate_conv_entry_score(row3, 'Contact us') == 0

    def test_implicit_fit_generic_cta(self):
        """Generic entry CTA without qualification should get points."""
        from m5_fit_score import calculate_generic_cta_score

        # Generic CTA, no qualifying language - should get 4 points
        row1 = pd.Series({'dominant_cta': 'MESSAGE_PAGE'})
        assert calculate_generic_cta_score(row1, 'Learn more about us') == 4

        # Generic CTA with qualifying language - should get 0 points
        row2 = pd.Series({'dominant_cta': 'CALL_NOW'})
        assert calculate_generic_cta_score(row2, 'Check if you qualify today!') == 0

        # Non-generic CTA - should get 0 points
        row3 = pd.Series({'dominant_cta': 'SHOP_NOW'})
        assert calculate_generic_cta_score(row3, 'Shop our collection') == 0

    def test_implicit_fit_advisor_language(self):
        """Advisor language should be detected."""
        from m5_fit_score import calculate_advisor_score

        # English advisor language
        assert calculate_advisor_score('Speak with our advisor') == 4
        assert calculate_advisor_score('Talk to a specialist') == 4
        assert calculate_advisor_score('Our expert team') == 4

        # Spanish advisor language
        assert calculate_advisor_score('Habla con nuestro asesor') == 4
        assert calculate_advisor_score('Consulta con un especialista') == 4

        # No advisor language
        assert calculate_advisor_score('Buy our products') == 0

    def test_implicit_fit_service_breadth(self):
        """Service breadth/ambiguity should be detected."""
        from m5_fit_score import calculate_service_breadth_score

        # Generic service phrases
        assert calculate_service_breadth_score('We help with all your needs') >= 2
        assert calculate_service_breadth_score('Our services include many options') >= 2
        assert calculate_service_breadth_score('Customized solutions for you') >= 2

        # Specific non-generic text
        assert calculate_service_breadth_score('Buy our widget today') == 0

    def test_implicit_fit_regulated_domain(self):
        """Regulated domain should be detected."""
        from m5_fit_score import calculate_regulated_domain_score

        # By page category
        row1 = pd.Series({'page_category': 'real estate agent'})
        assert calculate_regulated_domain_score(row1, '') == 2

        row2 = pd.Series({'page_category': 'lawyer & law firm'})
        assert calculate_regulated_domain_score(row2, '') == 2

        # By ad text
        row3 = pd.Series({'page_category': ''})
        assert calculate_regulated_domain_score(row3, 'Licensed real estate agent') == 2
        assert calculate_regulated_domain_score(row3, 'Mortgage financing available') == 2

        # Non-regulated
        row4 = pd.Series({'page_category': 'restaurant'})
        assert calculate_regulated_domain_score(row4, 'Best pizza in town') == 0

    def test_combined_fit_score(self):
        """Combined fit score should be explicit + implicit."""
        from m5_fit_score import calculate_fit_score

        row = pd.Series({
            'ad_texts_combined': 'Speak with our expert advisor! Do you qualify? Schedule today!',
            'distinct_ctas': 2,
            'share_message': 0.3,
            'share_call': 0.1,
            'share_form': 0.2,
            'dominant_dest': 'MESSAGE',
            'dominant_cta': 'MESSAGE_PAGE',
            'page_category': 'real estate agent',
        })
        result = calculate_fit_score(row)

        assert 'fit_score' in result
        assert 'explicit_fit_score' in result
        assert 'implicit_fit_score' in result
        assert result['fit_score'] == result['explicit_fit_score'] + result['implicit_fit_score']
        assert result['fit_score'] <= 50

    def test_implicit_fit_captures_in_conversation_qualification(self):
        """
        CORE TEST: Verticals that qualify inside conversation should score high on implicit fit.

        This tests the main bug fix - businesses that use MESSAGE/CALL entry points
        without explicit qualification language should still get high fit scores
        because qualification happens DURING the conversation.
        """
        from m5_fit_score import calculate_fit_score

        # Real estate agent: MESSAGE entry, no pricing, advisor language, regulated domain
        # This is a classic "qualify in conversation" scenario
        row_re_agent = pd.Series({
            'ad_texts_combined': 'Looking to buy or sell? Talk to our team today.',
            'distinct_ctas': 1,
            'share_message': 0.8,
            'share_call': 0.1,
            'share_form': 0.1,
            'dominant_dest': 'MESSAGE',
            'dominant_cta': 'MESSAGE_PAGE',
            'page_category': 'real estate agent',
        })
        result_re = calculate_fit_score(row_re_agent)

        # Should have HIGH implicit fit (qualification happens in conversation)
        assert result_re['implicit_fit_score'] >= 10, \
            f"Real estate agent should have high implicit fit, got {result_re['implicit_fit_score']}"

        # May have LOW explicit fit (no pre-qualification language)
        # But combined should still be reasonable
        assert result_re['fit_score'] >= 10, \
            f"Real estate agent should have reasonable combined fit, got {result_re['fit_score']}"

        # Compare to e-commerce (should have low implicit fit)
        row_ecom = pd.Series({
            'ad_texts_combined': 'Shop now! 20% off all items. Free shipping on orders over $50.',
            'distinct_ctas': 1,
            'share_message': 0,
            'share_call': 0,
            'share_form': 0,
            'dominant_dest': 'WEB',
            'dominant_cta': 'SHOP_NOW',
            'page_category': 'e-commerce website',
        })
        result_ecom = calculate_fit_score(row_ecom)

        # E-commerce should have LOW implicit fit (no qualification needed)
        assert result_ecom['implicit_fit_score'] <= 4, \
            f"E-commerce should have low implicit fit, got {result_ecom['implicit_fit_score']}"

        # Real estate should beat e-commerce on implicit fit
        assert result_re['implicit_fit_score'] > result_ecom['implicit_fit_score'], \
            "Real estate should have higher implicit fit than e-commerce"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
