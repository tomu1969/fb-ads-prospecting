"""Test script comparing old vs new email approach.

Old approach: Templated pitch with personalized hook glued on
New approach: Genuinely personalized email focused on THEIR situation
"""

import asyncio
from groq import AsyncGroq
from dotenv import load_dotenv
import os

load_dotenv()

client = AsyncGroq(api_key=os.getenv('GROQ_API_KEY'))

# Test prospects
PROSPECTS = [
    {
        "name": "Tim Ellis",
        "company": "REEP Equity",
        "email": "tim@reepequity.com",
        "hook": "Named one of the Top 5 Fastest Growing Businesses by the San Antonio Business Journal two years in a row",
        "hook_source": "website",
        "context": "Real estate investment company focused on multifamily properties in San Antonio"
    },
    {
        "name": "Ross Howatt",
        "company": "The Newcomer Group",
        "email": "ross@thenewcomergroup.com",
        "hook": "#1 real estate team for high-end properties in St. Augustine for the last five years",
        "hook_source": "Facebook ad",
        "context": "Luxury real estate team in St. Augustine, FL"
    },
    {
        "name": "Luiz Pizzamiglio",
        "company": "Red Door Realty & Associates",
        "email": "luiz@reddoorrealtyandassociates.com",
        "hook": "The Houston Chronicle has recognized Red Door as a Top Workplace for 10 consecutive years",
        "hook_source": "website",
        "context": "Large real estate brokerage in Houston with multiple agents"
    }
]

# Old approach template (what we've been using)
OLD_TEMPLATE = """Hi {first_name},

I noticed {hook_source} that {hook}.

{forced_bridge}

We help 100+ businesses running Facebook and Instagram ads close more deals by responding to leads instantly and nurturing them automatically until they're ready to buy. Would you be open to seeing how they do it?

Thanks,
Tomas
Cofounder, LaHaus AI"""

OLD_BRIDGES = {
    "Tim Ellis": "This recognition indicates rapid business growth, likely resulting in a higher volume of leads that need instant response and effective nurturing to convert into deals.",
    "Ross Howatt": "Being the top real estate team suggests a high volume of high-end property leads, necessitating efficient lead response and automated nurturing to maintain your position.",
    "Luiz Pizzamiglio": "A workplace recognized for 10 years suggests a large, growing team generating significant lead volume that requires instant response to convert effectively."
}


async def generate_new_email(prospect: dict) -> str:
    """Generate email using new approach - genuinely personalized, no template."""

    prompt = f"""Write a cold email to a real estate professional. Your goal is NOT to pitch - it's to start a conversation.

RECIPIENT:
- Name: {prospect['name']}
- Company: {prospect['company']}
- Notable: {prospect['hook']} (found on {prospect['hook_source']})
- Context: {prospect['context']}

WHAT WE DO (for your context only - don't pitch this directly):
LaHaus AI helps real estate teams respond to leads in under 60 seconds using AI. We've helped teams like [specific names would go here] increase their conversion rates.

RULES FOR THE EMAIL:
1. NO templated pitch. The entire email should feel written just for them.
2. Lead with genuine curiosity about THEIR business, not your product
3. Reference their specific achievement naturally
4. Briefly mention what we do in context of their situation (1 sentence max)
5. Ask a question that shows you understand their world
6. The CTA should be soft - asking if something resonates, not asking for a meeting
7. Keep it under 80 words total
8. Sound like a human, not a sales robot
9. Don't use phrases like "I came across", "I noticed", "Would you be open to"
10. Include "Hi [Name]," greeting and "Tomas" sign-off

GOOD EXAMPLE (follow this structure):
Subject: Congrats on the growth

Hi Tim,

Two years in a row as one of San Antonio's fastest growing businesses - that's not easy to pull off in real estate.

Curious: with that kind of growth, is your team keeping up with inbound leads, or are some slipping through? We help a few fast-growing teams with that exact problem.

Either way, congrats on the momentum.

Tomas

OUTPUT FORMAT:
Subject: [short, specific, no clickbait]

[Email body - casual, curious, human]

Tomas"""

    response = await client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        max_tokens=300
    )

    return response.choices[0].message.content


def generate_old_email(prospect: dict) -> str:
    """Generate email using old approach - templated pitch."""
    first_name = prospect['name'].split()[0] if prospect['name'] else "there"
    hook_source = f"on your {prospect['hook_source']}" if "website" in prospect['hook_source'] else f"in your {prospect['hook_source']}"

    return OLD_TEMPLATE.format(
        first_name=first_name,
        hook_source=hook_source,
        hook=prospect['hook'].lower(),
        forced_bridge=OLD_BRIDGES.get(prospect['name'], "This success likely means more leads requiring faster response.")
    )


async def main():
    print("=" * 80)
    print("COLD EMAIL A/B TEST: Old Approach vs New Approach")
    print("=" * 80)

    for prospect in PROSPECTS:
        print(f"\n{'#' * 80}")
        print(f"# PROSPECT: {prospect['name']} @ {prospect['company']}")
        print(f"# Hook: {prospect['hook'][:60]}...")
        print("#" * 80)

        # Generate old email
        old_email = generate_old_email(prospect)

        # Generate new email
        new_email = await generate_new_email(prospect)

        print(f"\n{'─' * 40}")
        print("OLD APPROACH (templated pitch)")
        print("─" * 40)
        print(old_email)

        print(f"\n{'─' * 40}")
        print("NEW APPROACH (genuinely personalized)")
        print("─" * 40)
        print(new_email)

        print()


if __name__ == "__main__":
    asyncio.run(main())
