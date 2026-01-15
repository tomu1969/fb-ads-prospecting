# Cold Email A/B Comparison: Old vs New Approach

## Summary

| Aspect | Old Approach | New Approach |
|--------|--------------|--------------|
| Pitch | 100% identical across all emails | Unique per recipient |
| Structure | Hook → Forced bridge → Template pitch | Observation → Genuine question → Soft context |
| CTA | "Would you be open to seeing how they do it?" | Question about their situation |
| Tone | Sales robot | Curious peer |
| Word count | ~120 words | ~60-70 words |

---

## Example 1: Tim Ellis @ REEP Equity

**Context:** Named Top 5 Fastest Growing Business in San Antonio (2 years)

### OLD (What We Sent)

```
Subject: Congrats on the Top 5 Recognition!

Hi Tim,

I was on your website and noticed that REEP Equity was named one of the
"Top 5 Fastest Growing Businesses" by the San Antonio Business Journal
two years in a row.

This recognition indicates rapid business growth, likely resulting in a
higher volume of leads that need instant response and effective nurturing
to convert into deals.

We help 100+ businesses running Facebook and Instagram ads close more deals
by responding to leads instantly and nurturing them automatically until
they're ready to buy. Would you be open to seeing how they do it?

Thanks,
Tomas
Cofounder, LaHaus AI
```

**Problems:**
- "This recognition indicates..." sounds like AI wrote it
- Forces a connection between growth award → lead response problem
- Generic pitch identical to every other email
- "Would you be open to seeing how they do it?" is classic spam

### NEW (Proposed)

```
Subject: San Antonio's fastest growing

Hi Tim,

Two years in a row as one of San Antonio's fastest-growing businesses —
that's hard to pull off in real estate.

Quick question: at that growth rate, is your team keeping up with
inbound inquiries, or are some falling through the cracks?

We've been helping a few fast-scaling teams automate their initial
lead response (under 60 seconds). Happy to share what's worked if
it's relevant.

Either way, congrats on the momentum.

Tomas
```

**Why it's better:**
- Genuine compliment without forcing a connection
- Asks a real question (doesn't assume they have a problem)
- Mentions what we do in context, not as a pitch
- "Happy to share if relevant" vs "Would you be open to"
- Ends with warmth, not a sales push

---

## Example 2: Ross Howatt @ The Newcomer Group

**Context:** #1 luxury real estate team in St. Augustine for 5 years

### OLD (What We Sent)

```
Subject: St. Augustine's #1 Real Estate Team

Hi Ross,

I noticed in your Facebook ad that you've been the "#1 real estate team
for high-end properties in St. Augustine for the last five years."

Being the top real estate team suggests a high volume of high-end property
leads, necessitating efficient lead response and automated nurturing to
maintain your position and handle the demand effectively.

We help 100+ businesses running Facebook and Instagram ads close more deals
by responding to leads instantly and nurturing them automatically until
they're ready to buy. Would you be open to seeing how they do it?

Thanks,
Tomas
Cofounder, LaHaus AI
```

**Problems:**
- "Being the top real estate team suggests..." is robotic assumption
- "necessitating efficient lead response" — nobody talks like this
- Same template pitch as every other email
- No acknowledgment of luxury market specifics

### NEW (Proposed)

```
Subject: 5 years at #1

Hi Ross,

Five years as St. Augustine's top luxury team is impressive — especially
in a market where relationships matter as much as the properties.

Curious: do you find that high-end buyers expect faster response times
than typical clients? I've heard that from a few luxury teams and wondered
if it's universal.

We work with some teams on instant lead response, but I'm genuinely
curious how you've approached it at your scale.

Tomas
```

**Why it's better:**
- Acknowledges the luxury market context specifically
- Asks a genuine question (not a leading one)
- Shows industry awareness ("relationships matter")
- Positions us as curious, not pushy
- No identical pitch paragraph

---

## Example 3: Luiz Pizzamiglio @ Red Door Realty

**Context:** Top Workplace 10 consecutive years (Houston Chronicle)

### OLD (What We Sent)

```
Subject: Celebrating Red Door's Top Workplace Status

Hi Luiz,

I noticed on your website that Red Door Realty has been recognized by
the Houston Chronicle as a Top Workplace for 10 consecutive years.

A workplace recognized for 10 years suggests a large, growing team
generating significant lead volume that requires instant response to
convert effectively.

We help 100+ businesses running Facebook and Instagram ads close more deals
by responding to leads instantly and nurturing them automatically until
they're ready to buy. Would you be open to seeing how they do it?

Thanks,
Tomas
Cofounder, LaHaus AI
```

**Problems:**
- Forces connection: Top Workplace award → lead response need (???)
- The logical leap makes no sense
- Same template pitch
- Doesn't acknowledge what Top Workplace actually means (culture, retention)

### NEW (Proposed)

```
Subject: 10 years as a Top Workplace

Hi Luiz,

Ten years as a Top Workplace is rare — especially in real estate where
agent retention is notoriously tough. You're clearly doing something right.

Out of curiosity: with a larger team, do you find it harder to maintain
consistent response times to leads? Or has the team size actually helped?

We've been working with some brokerages on that specific problem, but
I'm always curious how different teams approach it.

Tomas
```

**Why it's better:**
- Actually understands what the award means (retention, culture)
- Connects to our topic through a genuine question, not an assumption
- Acknowledges larger teams have different dynamics
- "I'm always curious" vs "Would you be open to"

---

## Key Differences

| Element | Old | New |
|---------|-----|-----|
| **Opening** | "I noticed..." | Direct observation |
| **Bridge** | "This suggests/indicates..." | Question |
| **Pitch** | Template (identical) | Contextual mention (unique) |
| **CTA** | "Would you be open to..." | Genuine curiosity |
| **Tone** | Selling | Exploring |
| **Assumption** | You have a problem | Do you have this problem? |

---

## Implementation Recommendation

1. **Remove STANDARD_OFFER** from composer.py entirely
2. **Train LLM** to generate unique value mentions per recipient
3. **Change CTA** from pitch to question
4. **Add qualifying question** — don't assume pain, ask about it
5. **Shorter emails** — 60-70 words max

---

*Created: 2026-01-14*
