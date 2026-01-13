# Experiment 001: Q1 Cold Email Campaign - FB Ads Prospects

## Hypothesis
Personalized cold email campaign targeting businesses running Facebook/Instagram ads will achieve a reply rate of at least 10%.

## Goal
Test email outreach effectiveness for LaHaus AI lead response service across real estate and related B2B verticals.

## Success Criteria
- **Primary**: Reply rate >= 10%
- **Secondary**: Final bounce rate <= 5%

## Campaign Summary

### Batch 1: Initial Campaign (Jan 11, 2026)
| Metric | Value |
|--------|-------|
| Drafted | 93 |
| Sent | 69 |
| Initial bounces | 24 (35%) |
| After recovery | 69 delivered |

**Resend History:**
1. Jan 11 19:04 - 24 recovered contacts resent → 15 DMARC bounces
2. Jan 11 19:14 - 15 resent (DMARC fix attempt) → 14 still bouncing
3. Jan 13 09:55 - 14 resent (2nd recovery) → all delivered

### Batch 2: Additional Prospects (Jan 13, 2026)
| Metric | Value |
|--------|-------|
| Drafted | 42 |
| Verified | 58 issues → 12 after fix (79% fixed) |
| Excluded | 11 (domain mismatch) |
| Sent | 31 |
| Bounced | 1 (3.2%) |
| Delivered | 30 |

## Cumulative Metrics

| Metric | Value |
|--------|-------|
| **Total Unique Contacts** | ~100 |
| **Total Send Attempts** | 153 |
| **Successful Deliveries** | 99 |
| **Delivery Rate** | 99% |
| **Replies** | 0 (monitoring) |
| **Reply Rate** | 0% (TBD) |

## Issues Encountered & Fixes

### Issue 1: High Initial Bounce Rate (35%)
- **Cause:** Temporary failures incorrectly marked as permanent bounces
- **Fix:** Added re-verification (Strategy 0) to bounce recovery
- **Result:** 17/24 "bounced" emails were actually valid

### Issue 2: DMARC Rejection
- **Cause:** `lahaus.ai` strict DMARC policy conflicts with `lahaus.com` SMTP
- **Attempted Fix:** Changed GMAIL_SEND_AS to lahaus.com
- **Result:** Did NOT work - required bounce recovery with alternative addresses

### Issue 3: Domain Mismatch
- **Cause:** Scraped emails from unrelated domains (popculture.com, housingwire.com)
- **Fix:** Email verifier now detects and excludes domain mismatches
- **Result:** 11 contacts excluded, improved campaign quality

## Key Learnings

1. **MillionVerifier re-verification** catches false-positive bounces
2. **DMARC alignment** requires matching sender domain, not just alias
3. **Domain mismatch detection** prevents sending to wrong recipients
4. **Email verification pipeline** reduces critical issues by 79%

## Timeline

| Date | Event |
|------|-------|
| Jan 11 12:00 | Batch 1 drafted (93 emails) |
| Jan 11 13:00 | Batch 1 sent (69 emails) |
| Jan 11 17:00 | 24 bounces discovered |
| Jan 11 19:04 | First resend (bounce recovery) |
| Jan 11 19:14 | Second resend (DMARC fix attempt) |
| Jan 13 09:55 | Third resend (successful recovery) |
| Jan 13 10:39 | Batch 2 drafted (42 emails) |
| Jan 13 11:57 | Batch 2 sent (31 emails) |
| **Jan 15** | **Check responses (48h)** |
| Jan 17-18 | Send follow-up to non-responders |
| Jan 20 | Final analysis |

## Next Steps

1. **Jan 15 (~12:00)**: Check inbox for replies
   ```bash
   python scripts/gmail_sender/inbox_checker.py --hours 48
   ```

2. **Based on results**:
   - If reply rate >= 5%: Document learnings, consider scaling
   - If reply rate < 5%: Send follow-up to non-responders

3. **Follow-up strategy** (if reply rate < 5%):
   - Use `followup_1.txt` template (brief reminder + conversion stat)
   - Wait 48h, then send `followup_2.txt` (soft breakup) to remaining non-responders
   - Alternative: `followup_value.txt` (leads with stat) for higher-value prospects

4. **Final analysis**: Generate results.md with learnings and recommendations

## Follow-up Templates

### Template 1: Quick Follow-up (`followup_1.txt`)
```
Subject: Quick follow-up

Hi {first_name},

I reached out a few days ago about helping {company_name} respond to leads faster.

Didn't want this to get buried — we've helped teams cut response time from hours
to under 60 seconds, which typically doubles conversion rates.

Worth a quick look?
```

### Template 2: Soft Breakup (`followup_2.txt`)
```
Subject: One last thing

Hi {first_name},

Following up one more time — I know your inbox is busy.

If instant lead response isn't a priority right now, no worries. But if you're
losing deals to slow follow-up, I'd love to show you how we fix that in under a week.

Either way, I'll stop filling your inbox after this.
```

### Template 3: Value Add (`followup_value.txt`)
```
Subject: Thought this might help

Hi {first_name},

Quick follow-up with something useful — businesses running FB/IG ads see 391%
higher conversion when they respond to leads in under 5 minutes vs. 30+ minutes.

We automate that instant response so your team can focus on closing, not chasing.
```

## Files Reference

| File | Description |
|------|-------------|
| `output/email_campaign/drafts_v2.csv` | Batch 1 drafts with send status |
| `output/email_campaign/drafts_batch2_ready_to_send.csv` | Batch 2 sent emails |
| `output/email_campaign/campaign_log.md` | Detailed campaign log |
| `config/bounced_contacts.csv` | Bounced contacts tracking |
| `experiments/active/exp_001_*/state.json` | Experiment state |
| `scripts/email_drafter/templates/followup_1.txt` | Follow-up template (quick reminder) |
| `scripts/email_drafter/templates/followup_2.txt` | Follow-up template (soft breakup) |
| `scripts/email_drafter/templates/followup_value.txt` | Follow-up template (value add) |
