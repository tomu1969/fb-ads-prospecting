"""Tests for composer.py - Email generation."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestComposeEmail:
    """Tests for the main email composition function."""

    @pytest.mark.asyncio
    async def test_returns_valid_structure(self):
        """Should return dict with all required fields."""
        from composer import compose_email

        contact = {
            'contact_name': 'John Doe',
            'page_name': 'Example Realty',
            'primary_email': 'john@example.com'
        }

        hook = {
            'chosen_hook': 'Hiring 2 new agents',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Hiring means lead overflow is imminent'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Quick question about your team growth

Hi John,

I noticed you're hiring 2 new agents at Example Realty.

When a team is growing rapidly, lead response times often suffer while you wait for new hires to get up to speed.

We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        assert 'subject_line' in result
        assert 'email_body' in result
        assert 'hook_used' in result
        assert 'hook_source' in result

    @pytest.mark.asyncio
    async def test_includes_hook_in_email(self):
        """Should include the selected hook in the email body."""
        from composer import compose_email

        contact = {
            'contact_name': 'Jane Smith',
            'page_name': 'Miami Luxury Homes',
            'primary_email': 'jane@miami.com'
        }

        hook = {
            'chosen_hook': '50% lead surplus banner',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Lead surplus means response time issues'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: About your 50% lead surplus

Hi Jane,

I was on your site and saw the banner regarding your "50% lead surplus."

When a team has a surplus of leads, response times suffer and advertisement ROI drops.

We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        # Hook should be referenced in the body
        assert '50% lead surplus' in result['email_body'] or 'lead surplus' in result['email_body']

    @pytest.mark.asyncio
    async def test_includes_standard_offer(self):
        """Should include the standard offer CTA."""
        from composer import compose_email

        contact = {
            'contact_name': 'Bob Agent',
            'page_name': 'Premier Realty',
            'primary_email': 'bob@premier.com'
        }

        hook = {
            'chosen_hook': 'Just closed 50th deal',
            'hook_source': 'linkedin',
            'hook_type': 'achievement',
            'problem_framing': 'Success brings more leads to handle'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Congrats on your 50th deal!

Hi Bob,

Saw on LinkedIn that you just closed your 50th deal - impressive milestone!

That level of success usually brings a flood of new leads that can overwhelm response times.

We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        # Should include the standard offer
        assert 'help' in result['email_body'].lower()
        assert 'realtor' in result['email_body'].lower()

    @pytest.mark.asyncio
    async def test_follows_template_structure(self):
        """Should follow the hook -> problem -> offer structure."""
        from composer import compose_email

        contact = {
            'contact_name': 'Alice',
            'page_name': 'Coastal Homes',
            'primary_email': 'alice@coastal.com'
        }

        hook = {
            'chosen_hook': 'Learning to swim at 40',
            'hook_source': 'ad',
            'hook_type': 'story',
            'problem_framing': 'Vulnerable storytelling creates engagement floods'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Your swim story resonated with me

Hi Alice,

I caught your ad about learning to swim at 40 - incredibly vulnerable stuff to share in a business ad.

This high-engagement storytelling can cause a flood of leads and fan comments, leading to slower response times.

We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        body = result['email_body']
        # Should have greeting
        assert 'Hi' in body or 'Hello' in body
        # Should have sign-off
        assert 'Thanks' in body or 'Best' in body

    @pytest.mark.asyncio
    async def test_under_150_words(self):
        """Should keep email under 150 words."""
        from composer import compose_email

        contact = {
            'contact_name': 'Test User',
            'page_name': 'Test Realty',
            'primary_email': 'test@test.com'
        }

        hook = {
            'chosen_hook': 'Test hook',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Test problem'
        }

        # A short email response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Quick question

Hi Test,

I saw your test hook on the website.

This often leads to lead overflow challenges.

We help 100+ realtors handle that exact overflow instantly without adding headcount. Would you be open to seeing how they do it?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        word_count = len(result['email_body'].split())
        assert word_count < 150

    @pytest.mark.asyncio
    async def test_handles_missing_contact_name(self):
        """Should handle case when contact name is missing."""
        from composer import compose_email

        contact = {
            'contact_name': None,
            'page_name': 'Test Realty',
            'primary_email': 'test@test.com'
        }

        hook = {
            'chosen_hook': 'Test hook',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Test problem'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Quick question

Hi there,

I saw your test hook on the website.

We help 100+ realtors handle lead overflow instantly. Would you be open to seeing how?

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        assert result is not None
        assert 'email_body' in result

    @pytest.mark.asyncio
    async def test_uses_custom_sender_name(self):
        """Should use the provided sender name."""
        from composer import compose_email

        contact = {
            'contact_name': 'John',
            'page_name': 'Test Realty',
            'primary_email': 'john@test.com'
        }

        hook = {
            'chosen_hook': 'Test hook',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'problem_framing': 'Test problem'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Quick question

Hi John,

Test email body.

Thanks,
Sarah"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook, sender_name='Sarah')

        # The prompt should have included Sarah as sender
        assert result is not None

    @pytest.mark.asyncio
    async def test_handles_api_error(self):
        """Should handle OpenAI API errors gracefully."""
        from composer import compose_email

        contact = {
            'contact_name': 'John',
            'page_name': 'Test Realty',
            'primary_email': 'john@test.com'
        }

        hook = {
            'chosen_hook': 'Test hook',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Test problem'
        }

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, side_effect=Exception("API Error")):
            result = await compose_email(contact, hook)

        # Should return a fallback result, not crash
        assert result is not None
        assert 'email_body' in result
        assert 'error' in result.get('email_body', '').lower() or result.get('email_body') == ''

    @pytest.mark.asyncio
    async def test_preserves_hook_metadata(self):
        """Should include hook metadata in the result."""
        from composer import compose_email

        contact = {
            'contact_name': 'Test',
            'page_name': 'Test Co',
            'primary_email': 'test@test.com'
        }

        hook = {
            'chosen_hook': 'Specific hook detail',
            'hook_source': 'linkedin',
            'hook_type': 'achievement',
            'problem_framing': 'Test framing'
        }

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """Subject: Test

Hi Test,

Test body with specific hook detail.

Thanks,
Tomas"""

        with patch('composer.openai_client.chat.completions.create',
                   new_callable=AsyncMock, return_value=mock_response):
            result = await compose_email(contact, hook)

        assert result['hook_used'] == 'Specific hook detail'
        assert result['hook_source'] == 'linkedin'


class TestBuildComposerPrompt:
    """Tests for the prompt building function."""

    def test_includes_contact_info(self):
        """Should include contact name and company in prompt."""
        from composer import build_composer_prompt

        contact = {
            'contact_name': 'John Doe',
            'page_name': 'Example Realty'
        }

        hook = {
            'chosen_hook': 'Test hook',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Test framing'
        }

        prompt = build_composer_prompt(contact, hook, 'Tomas')

        assert 'John' in prompt  # First name
        assert 'Example Realty' in prompt

    def test_includes_hook_and_framing(self):
        """Should include the hook and problem framing."""
        from composer import build_composer_prompt

        contact = {
            'contact_name': 'Jane',
            'page_name': 'Test Co'
        }

        hook = {
            'chosen_hook': '50% lead surplus',
            'hook_source': 'website',
            'hook_type': 'hiring',
            'problem_framing': 'Lead surplus causes response delays'
        }

        prompt = build_composer_prompt(contact, hook, 'Tomas')

        assert '50% lead surplus' in prompt
        assert 'Lead surplus causes response delays' in prompt

    def test_includes_sender_name(self):
        """Should include the sender name."""
        from composer import build_composer_prompt

        contact = {'contact_name': 'Test', 'page_name': 'Test Co'}
        hook = {
            'chosen_hook': 'Test',
            'hook_source': 'ad',
            'hook_type': 'offer',
            'problem_framing': 'Test'
        }

        prompt = build_composer_prompt(contact, hook, 'CustomName')

        assert 'CustomName' in prompt


class TestParseEmailResponse:
    """Tests for parsing the LLM email response."""

    def test_extracts_subject_and_body(self):
        """Should correctly split subject and body."""
        from composer import parse_email_response

        response = """Subject: Quick question about your team

Hi John,

This is the email body.

Thanks,
Tomas"""

        subject, body = parse_email_response(response)

        assert 'Quick question' in subject
        assert 'Hi John' in body
        assert 'email body' in body

    def test_handles_no_subject_prefix(self):
        """Should handle response without 'Subject:' prefix."""
        from composer import parse_email_response

        response = """Hi John,

This is the email body.

Thanks,
Tomas"""

        subject, body = parse_email_response(response)

        # Should still work, perhaps with empty or auto-generated subject
        assert body is not None
        assert 'Hi John' in body

    def test_handles_multiline_subject(self):
        """Should handle subject that might span formatting."""
        from composer import parse_email_response

        response = """Subject: About your hiring needs

Hi Jane,

Body text here.

Best,
Sender"""

        subject, body = parse_email_response(response)

        assert 'hiring' in subject.lower()
        assert 'Hi Jane' in body


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
