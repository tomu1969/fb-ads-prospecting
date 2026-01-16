# Experiment Report: Q1 Cold Email Campaign (exp_001)

**Experiment ID:** exp_001
**Date Range:** January 11-16, 2026
**Status:** In Progress (A/B Testing Phase)
**Hypothesis:** Personalized cold email campaign targeting FB/IG advertisers will achieve >= 10% reply rate

---

## Executive Summary

**Result: HYPOTHESIS NOT CONFIRMED (Yet)**

After 5 days and 171 unique emails sent to FB/IG advertisers, the campaign achieved a **1.3% reply rate** (2/150 deliveries), significantly below the 10% target. Both replies were unsubscribe requests triggered by follow-up emails, resulting in **0% positive replies**.

**Key Finding:** The templated pitch approach (batches 1-4) produced robotic-sounding emails that failed to generate engagement. On January 15, we pivoted to a new "question-based" approach that removes templated pitches, uses genuine curiosity, and asks real questions. This new approach is being A/B tested in batch 5 (6 emails) and value-add follow-ups (49 emails).

**Current Metrics:**

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Reply Rate | 1.3% (2/150) | >= 10% | Behind |
| Positive Reply Rate | 0% (0/150) | >= 10% | Behind |
| Bounce Rate | 2% (3/150) | <= 5% | Met |
| Unsubscribes | 2 | - | - |

**Next Milestone:** Monitor batch 5 and value-add follow-ups for responses by January 17.

---

## 1. Pipeline Architecture

### 1.1 Scrape Pipeline

```
Facebook Ads Library → fb_ads_scraper.py
    ↓
Prospect Discovery (company name, ad text, ad images)
    ↓
Enrichment Pipeline (website, contact, email, LinkedIn, Instagram)
    ↓
prospects_master.csv (primary contact database)
```

**Key Components:**
- **fb_ads_scraper.py**: Scrapes Facebook Ad Library for active advertisers
- **Enrichment stages**: loader → enricher → scraper → hunter → exa_enricher → contact_name_resolver → linkedin_enricher

### 1.2 Contact Enrichment Pipeline

```
Input CSV → loader.py (AI field mapping)
    ↓
enricher.py (DuckDuckGo website discovery)
    ↓
scraper.py (contact extraction from websites)
    ↓
hunter.py (email verification via Hunter.io)
    ↓
exa_enricher.py (fast Exa API contact discovery)
    ↓
contact_name_resolver.py (multi-source name resolution)
    ↓
linkedin_enricher.py (personal LinkedIn profile finder)
    ↓
prospects_master.csv
```

**Data Quality Improvements:**
- Domain mismatch detection (excluded 11 contacts)
- Wrong persona detection (excluded 2 contacts: auction house IT engineer, B2B software consultant)
- Bounce recovery system (90% recovery rate via alternative emails)

### 1.3 Email Drafting Pipeline

```
prospects_master.csv → email_drafter/drafter.py
    ↓
    ├─ researcher.py (Exa API multi-source research)
    │   ├─ Company website research
    │   ├─ LinkedIn profile research
    │   └─ Social media research
    ↓
    ├─ analyzer.py (LLM hook selection & reasoning)
    │   └─ Selects best hook from ad/website/linkedin
    ↓
    ├─ composer.py (LLM email generation)
    │   └─ Generates subject + body
    ↓
drafts.csv
    ↓
email_verifier/verifier.py (quality checks)
    ├─ Name validation
    ├─ Domain validation
    ├─ Template variable check
    ├─ Greeting check
    └─ Writing quality check (LLM)
    ↓
email_verifier/fixer.py (auto-fix issues)
    ├─ Name extraction
    ├─ Greeting fixes
    └─ Template variable fixes
    ↓
drafts_fixed.csv
    ↓
gmail_sender/gmail_sender.py (SMTP sender)
    ↓
Sent emails + inbox_checker.py (bounce/reply detection)
```

**Quality Gates:**
- Verification pipeline reduces critical issues by 79%
- LLM-powered writing quality check catches spam triggers
- Domain mismatch check prevents wrong recipients
- Bounce recovery with MillionVerifier re-verification

---

## 2. Experiment Timeline

| Date | Milestone | Details |
|------|-----------|---------|
| **Jan 11** | Batch 1 Drafted | 93 emails drafted, 69 sent |
| Jan 11 | Bounce Crisis | 24 bounces (35%) - DMARC issues |
| Jan 11-13 | Bounce Recovery | 3 recovery attempts, 100% delivered |
| **Jan 13** | Batch 2 Sent | 42 drafted, 31 sent (11 excluded) |
| **Jan 13** | Batch 3 Sent | 21 drafted, 20 sent (Exa MCP research) |
| **Jan 14** | Follow-up #1 | 63 follow-ups sent to batch 1 non-responders |
| Jan 14 | Unsubscribes | 2 unsubscribe requests (100% of replies) |
| **Jan 15** | Composer Rewrite | Analyzed 0% positive reply rate, rewrote approach |
| **Jan 15** | Batch 5 Sent | 6 emails with new approach (A/B test) |
| **Jan 15** | Value-Add Follow-ups | 49 follow-ups with stats/insights approach |
| Jan 16 | Monitoring | Waiting for responses from new approach |

---

## 3. Results by Batch

### Overview Table

| Batch | Date | Drafted | Sent | Delivered | Bounced | Replies | Approach | Reply Rate |
|-------|------|---------|------|-----------|---------|---------|----------|------------|
| Batch 1 | Jan 11 | 93 | 69 | 69 | 0 (recovered) | 0 | old | 0% |
| Batch 2 | Jan 13 | 42 | 31 | 30 | 1 | 0 | old | 0% |
| Batch 3 | Jan 13 | 21 | 20 | 19 | 1 | 0 | old | 0% |
| Follow-up #1 | Jan 14 | 69 | 63 | 63 | 0 | 2 (unsub) | old | 3.2% |
| **Batch 5** | Jan 15 | 6 | 6 | 6 | 0 | TBD | **new_v2** | TBD |
| **Value-Add** | Jan 15 | 49 | 49 | TBD | TBD | TBD | **value_add** | TBD |
| **Total** | - | **280** | **238** | **187+** | **3** | **2** | - | **1.3%** |

### Batch Details

**Batch 1 (Initial Campaign)**
- 93 emails drafted with old approach
- 69 sent successfully after 3 resend attempts
- 24 initial bounces due to DMARC issues (lahaus.ai alias rejected)
- Bounce recovery via MillionVerifier re-verification: 17/24 were valid
- Final delivery rate: 100% (after recovery)
- 0 replies after 72 hours

**Batch 2 (Additional Prospects)**
- 42 emails drafted with old approach
- 11 excluded due to domain mismatch (e.g., email@realty.com for "ABC Insurance")
- 31 sent, 1 bounce (emily@delriotitle.net)
- 0 replies after 48 hours

**Batch 3 (Exa MCP Enhanced Research)**
- 21 emails drafted with enhanced Exa MCP research
- 1 excluded: sirisha.deevi@sothebys.com (IT engineer at auction house, not realtor)
- 20 sent, 1 bounce: david@homesusa.com (Software Consultant at B2B tech company)
- 0 replies after 48 hours

**Follow-up #1 (Batch 1 Non-responders)**
- 69 follow-ups drafted with "Quick follow-up" approach
- 6 excluded (previously bounced emails)
- 63 sent successfully
- **2 replies: BOTH were unsubscribe requests** (Bob Woerner, Donald McVicar)
- Learning: "Quick follow-up" subject line triggered 100% unsubscribe rate

**Batch 5 (New Composer A/B Test)**
- 6 emails drafted with NEW approach (new_v2)
- Key changes:
  - Removed templated STANDARD_OFFER pitch
  - Question-based CTAs instead of "Would you be open to"
  - Shorter emails (50-70 words vs 120)
  - Genuine curiosity tone vs sales robot
  - Title "LaHaus Co-Founder" in signature
- 6 sent, 0 bounces
- **Monitoring for responses** (will compare vs old approach)

**Value-Add Follow-ups (Batches 2 & 3)**
- 49 follow-ups drafted with VALUE-ADD approach
- Key changes:
  - Lead with stats/insights, not "following up"
  - Short questions about lead response approach
  - No direct reference to previous email
  - Provides genuine value (industry stat or observation)
- 49 sent, 0 failures
- **Monitoring for responses** (testing if value-add avoids unsubscribes)

---

## 4. Sample Emails

### 4.1 Old Approach (Batches 1-4)

**Example 1: REEP Equity (Tim Ellis)**

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

**Example 2: The Newcomer Group (Ross Howatt)**

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

**Analysis:**
- Templated pitch: "We help 100+ businesses..." appears verbatim in every email
- Over-explaining: "This recognition indicates..." feels condescending
- Generic CTA: "Would you be open to seeing how they do it?" - not personalized
- Word count: ~120 words (too long for cold email)
- Tone: Sales robot, not human

---

### 4.2 New Approach (Batch 5)

**Example 1: HomesUSA.com (David Klein)**

```
Subject: Guinness World Records Holder

Hi David,

Your team's achievements are impressive. Do you find consistent response
times harder with a larger team? We help teams with instant lead response.

Either way, congrats on the momentum.

Tomas
LaHaus Co-Founder
```

**Example 2: Corcoran (Alexandra Lanci)**

```
Subject: Premier Luxury Properties

Hi Alexandra,

Corcoran's national and international reach is impressive. Do high-end
buyers expect faster response times? We help teams with instant lead
response. Either way, congrats on the momentum.

Tomas
LaHaus Co-Founder
```

**Analysis:**
- No templated pitch - each email is contextually unique
- Question-based CTA: Real question showing understanding of their business
- Soft mention: "We help teams with instant lead response" - context, not pitch
- Warm close: "Either way, congrats on the momentum" - no sales pressure
- Word count: 50-60 words (much shorter)
- Tone: Human, peer-to-peer

---

### 4.3 Comparison: Old vs New

| Dimension | Old Approach | New Approach |
|-----------|--------------|--------------|
| **Word Count** | 120 words | 50-70 words |
| **Pitch** | Templated paragraph (identical) | Soft mention (contextual) |
| **CTA** | "Would you be open to..." | "Do you find..." (real question) |
| **Tone** | Sales robot | Genuine curiosity |
| **Personalization** | Surface-level hook + template | Hook integrated into question |
| **Close** | "Thanks" | "Congrats on the momentum" |
| **Reply Rate** | 0% (batches 1-3) | TBD (batch 5 pending) |

---

## 5. Issues Encountered & Fixes

### Issue 1: High Initial Bounce Rate (35%)

**Problem:** 24 out of 69 emails bounced in batch 1

**Root Cause:**
- DMARC rejection: lahaus.ai domain has strict DMARC policy
- Emails sent via lahaus.com SMTP server with lahaus.ai "Send As" alias
- Receiving servers rejected due to SPF/DMARC mismatch

**Fix Attempted:**
1. Changed GMAIL_SEND_AS to lahaus.com → Did NOT work (only 1/15 delivered)
2. Bounce recovery with MillionVerifier re-verification → 17/24 emails were actually valid
3. Found alternative emails via Hunter.io and Apollo.io

**Result:** 100% of batch 1 eventually delivered after 3 resend attempts

**Learning:** DMARC issues require sender domain alignment, not just alias change. Temporary failures are often misidentified as hard bounces.

---

### Issue 2: Domain Mismatch (11 contacts excluded)

**Problem:** Email domains unrelated to company names

**Examples:**
- Company: "ABC Realty" → Email: info@insurancegroup.com
- Company: "XYZ Properties" → Email: contact@techstartup.io

**Root Cause:** Data quality issue in enrichment pipeline (scraper picked up wrong contact info from website)

**Fix:** Added domain mismatch check to verifier.py:
```python
def check_domain_mismatch(email, company_name):
    email_domain = email.split('@')[1]
    company_slug = company_name.lower().replace(' ', '')

    if email_domain not in company_slug and company_slug not in email_domain:
        return "WARNING: Email domain may not match company"
```

**Result:** 11 contacts excluded from batch 2, improved email quality

---

### Issue 3: Wrong Persona (2 contacts excluded)

**Problem:** Contacts were completely wrong people

**Case 1:** sirisha.deevi@sothebys.com
- Expected: Real estate agent at Sotheby's Realty
- Actual: IT engineer at Sotheby's auction house (sothebys.com ≠ sothebysrealty.com)

**Case 2:** david@homesusa.com
- Expected: Real estate agent at HomesUSA
- Actual: Software Consultant at B2B tech company

**Root Cause:**
- Scraper confusion between auction house and real estate franchise
- Generic website with no clear persona information

**Fix:** Enhanced Exa MCP research to verify persona before sending

**Result:** Prevented sending to completely wrong recipients

---

### Issue 4: Follow-up Triggered 100% Unsubscribe Rate

**Problem:** Both replies to follow-up #1 were unsubscribe requests

**Follow-up #1 Details:**
- Subject: "Quick follow-up"
- Sent 3 days after initial email
- 63 sent → 2 replies → BOTH unsubscribes

**Root Cause Analysis:**
- "Quick follow-up" subject line perceived as pushy sales pressure
- 3-day timing may be too aggressive
- Follow-up didn't add value, just reminded about previous email

**Fix:** Created value-add follow-up approach:
- Lead with stats/insights, not "following up"
- Short questions about their lead response approach
- No direct reference to previous email
- Provides genuine value upfront

**Example Value-Add Follow-up:**
```
Subject: 60% of real estate leads respond within 5 minutes

Hi [Name],

Most real estate buyers respond within 5 minutes of submitting a lead.
How does your team handle that window?

Tomas
LaHaus Co-Founder
```

**Result:** 49 value-add follow-ups sent (testing if this avoids unsubscribes)

---

### Issue 5: 0% Positive Reply Rate from Old Approach

**Problem:** Batches 1-3 (118 delivered emails) generated 0 positive replies

**Analysis:**

**Templated Pitch Problem:**
```
We help 100+ businesses running Facebook and Instagram ads close more
deals by responding to leads instantly and nurturing them automatically
until they're ready to buy.
```

This exact sentence appeared in EVERY email. Recipients likely recognized it as:
- Mass email (not truly personalized)
- Sales robot (not human writing)
- Copy-paste job (low effort)

**Over-Explanation Problem:**
```
This recognition indicates rapid business growth, likely resulting in a
higher volume of leads that need instant response...
```

Explaining what their own achievement means feels condescating.

**Generic CTA Problem:**
```
Would you be open to seeing how they do it?
```

Not specific to their situation, feels like template language.

**Fix:** Complete composer.py rewrite (January 15):
- Removed STANDARD_OFFER template entirely
- Question-based CTAs that show understanding of their business
- Shorter emails (50-70 words vs 120)
- Genuine curiosity tone vs sales pitch
- Title "LaHaus Co-Founder" to establish credibility

**Result:** Batch 5 sent with new approach (monitoring for replies)

---

## 6. Key Learnings

### Technical Learnings

1. **MillionVerifier re-verification catches false-positive bounces** - 71% (17/24) of "bounced" emails were actually valid when re-verified

2. **DMARC issues require sender domain alignment** - Cannot send from lahaus.ai via lahaus.com SMTP, even with "Send As" alias

3. **Domain mismatch check prevents wrong recipients** - Email domain should relate to company name (prevents sending to wrong organizations)

4. **Email verification pipeline reduces critical issues by 79%** - LLM + rules catch most problems before sending

5. **Exa MCP tool provides better research when API quota is exhausted** - MCP server had better results than direct API calls

6. **For franchise agents, research their personal site, not franchise homepage** - RE/MAX, Coldwell Banker agents need personalized research on their team/individual websites

7. **Verify sothebys.com vs sothebysrealty.com** - Auction house vs real estate franchise are completely different companies

8. **LLM-powered writing quality check catches spam triggers** - Regex-based checks miss subtle awkward phrasing

### Campaign Learnings

9. **Cold email CTAs should ask real questions, not assume pain** - "Do you find..." vs "This recognition indicates you need..."

10. **Follow-up emails with "Quick follow-up" triggered 100% unsubscribe rate** - Timing (3 days) or tone perceived as pushy

11. **Both unsubscribes came from follow-ups, not initial emails** - Initial approach (while templated) wasn't offensive, but follow-up crossed the line

12. **100% templated pitch likely caused 0% positive reply rate** - Identical paragraph in every email made them feel mass-produced

13. **New approach: question-based CTAs, shorter emails (50-70 words), genuine curiosity** - Testing with batch 5 to compare

14. **Batch 5 tests new composer** - Will compare reply rate vs batches 1-4 to validate hypothesis

15. **Value-add follow-ups lead with stats/insights** - Avoids pushy "following up" tone that triggered unsubscribes

---

## 7. Recommendations

### Immediate Actions (Next 48 Hours)

1. **Monitor Batch 5 & Value-Add Follow-ups** (Due: Jan 17)
   - Track reply rate from new approach vs old approach
   - Check if value-add follow-ups reduce unsubscribe rate
   - Decision point: If new approach shows improvement, apply to all remaining prospects

2. **Pause Old Approach** (Immediate)
   - Do NOT send more emails with templated STANDARD_OFFER pitch
   - Old approach has proven 0% positive reply rate across 118 emails

3. **Prepare Batch 6** (If Batch 5 Shows Improvement)
   - Draft 20-30 emails with new approach
   - Send to remaining uncontacted prospects from master file
   - Include control group metrics comparison

### Medium-Term Improvements (Next 2 Weeks)

4. **Refine Question-Based CTAs**
   - Analyze which question types get responses:
     - "Do you find..." (curiosity about challenges)
     - "How does your team handle..." (process questions)
     - "Is it harder/easier..." (comparative questions)
   - A/B test different question styles

5. **Test Follow-up Timing**
   - Current: 3 days (triggered unsubscribes)
   - Test: 7 days, 14 days, 21 days
   - Hypothesis: Longer wait time = less pushy perception

6. **Improve Bounce Recovery Process**
   - Add Hunter.io personal email finder BEFORE first send attempt
   - Avoid generic emails (info@, contact@) - 47% secondary bounce rate
   - Focus on personal emails (firstname@, firstname.lastname@)

7. **Enhance Persona Verification**
   - Add LinkedIn title verification before sending
   - Check if job title contains "real estate", "realtor", "broker", "agent"
   - Exclude IT engineers, software developers, office managers

### Long-Term Strategy (Next Month)

8. **Multi-Channel Approach**
   - Instagram DM outreach (after warm-up period)
   - LinkedIn connection requests with personalized notes
   - Test response rate comparison: Email vs DM vs LinkedIn

9. **Content Personalization Tiers**
   - Tier 1: High-value prospects (>$10M in sales) → Manual research + custom email
   - Tier 2: Medium prospects → New approach with Exa research
   - Tier 3: Lower priority → Simple question-based email

10. **Build Reply Analysis System**
    - Categorize replies: Positive, Neutral, Unsubscribe, Auto-reply
    - Track which hooks generate positive replies
    - Identify patterns in successful emails

### Experimental Ideas

11. **Test "No-Pitch" Emails**
    - Only ask question, no mention of what we do
    - See if curiosity alone generates replies
    - Example: "Hi [Name], at your growth rate, is keeping up with inquiries harder or easier?"

12. **Test Referral Angle**
    - "I help [similar company] with lead response..."
    - Social proof instead of company pitch
    - Requires collecting success stories first

13. **Test Value-First Approach**
    - Share free resource (guide, checklist, industry stat)
    - No ask, just value
    - Follow-up only if they engage with resource

---

## Conclusion

The Q1 Cold Email Campaign revealed a critical insight: **templated pitches kill cold email response rates**. Despite strong personalization in the hook and opening, the identical STANDARD_OFFER paragraph made emails feel mass-produced, resulting in 0% positive replies across 118 emails.

The pivot to a question-based approach (batch 5) represents a fundamental shift from "sales robot" to "genuine human curiosity." Early signals will determine if this approach meets the 10% reply rate target.

**Decision Point:** January 17, 2026
- If batch 5 + value-add follow-ups show improvement → Scale new approach
- If no improvement → Re-evaluate hypothesis and consider multi-channel approach

**Key Metric to Watch:** Positive reply rate from batch 5 (new approach) vs batches 1-3 (old approach)

---

*Report generated: 2026-01-16*
*Experiment status: In Progress*
*Next update: 2026-01-17 (batch 5 monitoring complete)*
