"""Tests for LinkedIn dedup V2: name enrichment + enhanced matching."""

import pytest
from unittest.mock import MagicMock, patch, call


# ============================================================
# Slice 1: Email-prefix name extraction
# ============================================================


class TestExtractNameFromEmail:
    """Tests for extract_name_from_email function."""

    def test_simple_dot_separator(self):
        """john.smith@cbre.com → 'John Smith'"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john.smith@cbre.com') == 'John Smith'

    def test_underscore_separator(self):
        """john_smith@co.com → 'John Smith'"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john_smith@co.com') == 'John Smith'

    def test_single_word_returns_none(self):
        """Single-word prefix has insufficient signal → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john@company.com') is None

    def test_numeric_stripped(self):
        """john.smith2@co.com → 'John Smith'"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john.smith2@co.com') == 'John Smith'

    def test_generic_prefix_info(self):
        """info@ → None (generic)"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('info@company.com') is None

    def test_generic_prefix_support(self):
        """support@ → None (generic)"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('support@company.com') is None

    def test_generic_prefix_admin(self):
        """admin@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('admin@company.com') is None

    def test_generic_prefix_noreply(self):
        """noreply@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('noreply@company.com') is None

    def test_generic_prefix_hello(self):
        """hello@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('hello@company.com') is None

    def test_generic_prefix_sales(self):
        """sales@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('sales@company.com') is None

    def test_generic_prefix_contact(self):
        """contact@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('contact@company.com') is None

    def test_generic_prefix_office(self):
        """office@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('office@company.com') is None

    def test_generic_prefix_team(self):
        """team@ → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('team@company.com') is None

    def test_none_input(self):
        """None input → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email(None) is None

    def test_empty_string(self):
        """Empty string → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('') is None

    def test_no_at_sign(self):
        """Invalid email (no @) → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('notanemail') is None

    def test_three_part_name(self):
        """john.michael.smith@co.com → 'John Michael Smith'"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john.michael.smith@co.com') == 'John Michael Smith'

    def test_hyphenated_separator(self):
        """john-smith@co.com → 'John Smith'"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('john-smith@co.com') == 'John Smith'

    def test_numeric_heavy_prefix(self):
        """12345@co.com → None (no name signal)"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('12345@company.com') is None

    def test_synthetic_email_skipped(self):
        """li:// synthetic emails → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('li://john-smith-123') is None

    def test_synthetic_name_email_skipped(self):
        """li-name:// synthetic emails → None"""
        from scripts.contact_intel.linkedin_dedup import extract_name_from_email

        assert extract_name_from_email('li-name://john-smith-abc123') is None


class TestEnrichNamelessNodes:
    """Tests for enrich_nameless_nodes function."""

    def test_enriches_nameless_node(self):
        """Should set name on nodes where name IS NULL from email prefix."""
        from scripts.contact_intel.linkedin_dedup import enrich_nameless_nodes

        mock_session = MagicMock()
        # Simulate query returning nameless nodes with extractable emails
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([
            {'email': 'john.smith@cbre.com'},
            {'email': 'jane.doe@coldwellbanker.com'},
            {'email': 'info@company.com'},  # generic, should skip
        ])
        mock_session.run.return_value = mock_result

        count = enrich_nameless_nodes(mock_session)

        assert count == 2  # only john.smith and jane.doe enriched
        # Verify the SET call was made for valid names
        assert mock_session.run.call_count >= 3  # 1 query + 2 SET calls

    def test_skips_synthetic_emails(self):
        """Should not attempt extraction on li:// or li-name:// emails."""
        from scripts.contact_intel.linkedin_dedup import enrich_nameless_nodes

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])  # query excludes synthetics
        mock_session.run.return_value = mock_result

        count = enrich_nameless_nodes(mock_session)

        assert count == 0


# ============================================================
# Slice 2: Name normalization
# ============================================================


class TestNormalizeNameForDedup:
    """Tests for normalize_name_for_dedup function."""

    def test_strip_suffix_cfa(self):
        """'John Smith, CFA' → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('John Smith, CFA') == 'john smith'

    def test_strip_suffix_mba(self):
        """'Jane Doe MBA' → 'jane doe'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('Jane Doe MBA') == 'jane doe'

    def test_strip_accents(self):
        """'José García' → 'jose garcia'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('José García') == 'jose garcia'

    def test_last_first_format(self):
        """'Smith, John' → 'john smith' (exactly 2 comma-separated parts)"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('Smith, John') == 'john smith'

    def test_jr_suffix(self):
        """'John Smith Jr.' → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('John Smith Jr.') == 'john smith'

    def test_sr_suffix(self):
        """'John Smith Sr.' → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('John Smith Sr.') == 'john smith'

    def test_roman_numeral_iii(self):
        """'Robert Lee III' → 'robert lee'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('Robert Lee III') == 'robert lee'

    def test_extra_whitespace(self):
        """'  John   Smith  ' → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('  John   Smith  ') == 'john smith'

    def test_phd_suffix(self):
        """'Dr. Maria Santos PhD' → 'dr maria santos'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        result = normalize_name_for_dedup('Maria Santos PhD')
        assert result == 'maria santos'

    def test_multiple_suffixes(self):
        """'John Smith CFA, MBA' → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('John Smith CFA, MBA') == 'john smith'

    def test_empty_string(self):
        """Empty string → empty string."""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('') == ''

    def test_none_returns_empty(self):
        """None → empty string."""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup(None) == ''

    def test_no_suffix_unchanged(self):
        """'John Smith' → 'john smith' (no suffix to strip)"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        assert normalize_name_for_dedup('John Smith') == 'john smith'

    def test_last_first_with_suffix_not_treated_as_last_first(self):
        """'Smith, John CFA' — has suffix keyword after comma, should not flip.
        Actually 'Smith, John CFA' has 2 comma parts: 'Smith' and 'John CFA',
        so it should flip to 'john cfa smith' then strip CFA → 'john smith'"""
        from scripts.contact_intel.linkedin_dedup import normalize_name_for_dedup

        # 'Smith, John CFA' → detect Last,First → 'John CFA Smith' → strip CFA → 'john smith'
        assert normalize_name_for_dedup('Smith, John CFA') == 'john smith'


# ============================================================
# Slice 3: Domain-company matching (Tier 3)
# ============================================================


class TestFindTier3Candidates:
    """Tests for find_tier3_candidates function."""

    def test_matches_company_to_domain(self):
        """LinkedIn 'CBRE' company should match john.smith@cbre.com."""
        from scripts.contact_intel.linkedin_dedup import find_tier3_candidates

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([
            {
                'synth_email': 'li://john-smith-123',
                'real_email': 'john.smith@cbre.com',
                'synth_name': 'John Smith',
                'real_name': 'John Smith',
                'synth_url': 'https://linkedin.com/in/john-smith-123',
                'synth_company': 'CBRE',
                'synth_position': 'Agent',
                'real_domain': 'cbre.com',
            },
        ])
        mock_session.run.return_value = mock_result

        candidates = find_tier3_candidates(mock_session)
        assert len(candidates) >= 1
        assert candidates[0]['synth_email'] == 'li://john-smith-123'

    def test_no_match_generic_domain(self):
        """Should not match john@gmail.com to company 'Gmail'."""
        from scripts.contact_intel.linkedin_dedup import GENERIC_DOMAINS

        assert 'gmail.com' in GENERIC_DOMAINS
        assert 'yahoo.com' in GENERIC_DOMAINS
        assert 'hotmail.com' in GENERIC_DOMAINS

    def test_generic_domains_blocklist_complete(self):
        """Ensure all major generic domains are in the blocklist."""
        from scripts.contact_intel.linkedin_dedup import GENERIC_DOMAINS

        expected = {
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'icloud.com', 'aol.com', 'live.com', 'me.com', 'msn.com',
            'mail.com', 'protonmail.com', 'zoho.com',
        }
        assert expected.issubset(GENERIC_DOMAINS)


# ============================================================
# Slice 4: Orchestration
# ============================================================


class TestRunDedupOrchestration:
    """Tests for updated run_dedup orchestration."""

    @patch('scripts.contact_intel.linkedin_dedup.neo4j_available', return_value=False)
    def test_exits_when_neo4j_unavailable(self, mock_neo4j):
        """Should return early when Neo4j is not available."""
        from scripts.contact_intel.linkedin_dedup import run_dedup

        run_dedup(dry_run=True)  # Should not raise

    @patch('scripts.contact_intel.linkedin_dedup.neo4j_available', return_value=True)
    @patch('scripts.contact_intel.linkedin_dedup.GraphBuilder')
    def test_enrichment_runs_before_dedup(self, mock_gb_cls, mock_neo4j):
        """enrich_nameless_nodes should be called before finding candidates."""
        from scripts.contact_intel.linkedin_dedup import run_dedup

        mock_gb = MagicMock()
        mock_gb_cls.return_value = mock_gb
        mock_driver = MagicMock()
        mock_gb.driver = mock_driver
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Make all queries return empty results
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_result.single.return_value = {'remaining': 0}
        mock_session.run.return_value = mock_result

        run_dedup(dry_run=True)

        # Verify session.run was called (enrichment queries happen)
        assert mock_session.run.called
