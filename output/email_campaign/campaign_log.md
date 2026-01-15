# Experiment: Q1 Cold Email Campaign (exp_001)

## Hypothesis & Goals

**Hypothesis:** Personalized cold email campaign targeting FB/IG advertisers will achieve >= 10% reply rate

| Goal | Metric | Target | Operator |
|------|--------|--------|----------|
| Primary | Reply Rate | 10% | >= |
| Secondary | Bounce Rate | 5% | <= |

---

## Current Status

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Reply Rate | **1.4%** (2/144) | >= 10% | :x: Behind |
| Positive Reply Rate | **0%** | >= 10% | :x: Behind |
| Unsubscribes | **2** | - | :warning: |
| Bounce Rate | **2%** | <= 5% | :white_check_mark: Met |
| Unique Emails Sent | 165 | - | - |
| Successful Deliveries | 144 | - | - |
| Total Touches | 228 | - | (includes follow-ups) |

**Assessment:** Behind on primary goal. 2 replies received but both were unsubscribe requests.
**Days Elapsed:** 3 (Jan 11-14)
**Next Milestone:** Wait until Jan 16 for more follow-up responses before deciding on follow-up #2

---

## Batches Summary

| Batch | Date | Drafted | Sent | Delivered | Bounced | Replies |
|-------|------|---------|------|-----------|---------|---------|
| Batch 1 | Jan 11 | 93 | 69 | 69 | 0 (recovered) | 0 |
| Batch 2 | Jan 13 | 42 | 31 | 30 | 1 | 0 |
| Batch 3 | Jan 13 | 21 | 20 | 19 | 1 | 0 |
| **Follow-up #1** | Jan 14 | 69 | 63 | 63 | 0 | 2 (unsubscribes) |
| **Total** | - | **225** | **183** | **181** | **2** | **2** |

### Batch Details

**Batch 1 (Initial Campaign)**
- 93 drafted, 69 sent initially
- 24 bounces → recovered via MillionVerifier re-verification
- DMARC issues required 3 resend attempts
- Final: 69 delivered

**Batch 2 (Additional Prospects)**
- 42 drafted, 11 excluded (domain mismatch)
- 31 sent, 1 bounce (emily@delriotitle.net)
- Final: 30 delivered

**Batch 3 (Exa MCP Research)**
- 21 drafted with enhanced research
- 1 excluded (sirisha.deevi@sothebys.com - wrong persona)
- 20 sent, 1 bounce (david@homesusa.com - wrong persona)
- Final: 19 delivered

**Follow-up #1 (Batch 1 Non-responders)**
- 69 drafted for batch 1 non-responders
- 6 excluded (bounced emails)
- 63 sent, 63 delivered
- 2 replies: both unsubscribe requests (Bob Woerner, Donald McVicar)
- Added to do_not_contact.csv

---

## Timeline

| Timestamp | Event | Details |
|-----------|-------|---------|
| 2026-01-11 12:00 | batch_1_drafted | 93 emails drafted |
| 2026-01-11 13:00 | batch_1_sent | 69 emails sent |
| 2026-01-11 17:00 | bounces_detected | 24 bounces discovered |
| 2026-01-11 18:59 | bounce_recovery_1 | 24 contacts recovered |
| 2026-01-11 19:04 | resend_1 | 24 resent from lahaus.ai |
| 2026-01-11 19:05 | dmarc_failure | 15 bounces - DMARC issue |
| 2026-01-11 19:13 | dmarc_fix_attempt | Changed sender to lahaus.com |
| 2026-01-11 19:14 | resend_2 | 15 resent from lahaus.com |
| 2026-01-11 19:25 | dmarc_fix_failed | Only 1 delivered, 14 still bouncing |
| 2026-01-13 09:26 | bounce_recovery_2 | 14 contacts recovered (11 re-verified, 3 new) |
| 2026-01-13 09:55 | resend_3 | 14 resent - all delivered |
| 2026-01-13 10:39 | batch_2_drafted | 42 new emails drafted |
| 2026-01-13 11:30 | batch_2_verified | 58 issues found, 46 fixed (79%) |
| 2026-01-13 11:52 | batch_2_excluded | 11 contacts excluded (domain mismatch) |
| 2026-01-13 11:57 | batch_2_sent | 31 emails sent |
| 2026-01-13 11:58 | batch_2_bounce | 1 bounce: emily@delriotitle.net |
| 2026-01-13 14:00 | batch_3_drafted | 21 new emails drafted with Exa MCP research |
| 2026-01-13 14:01 | batch_3_verified | 3 name mismatches fixed, ready to send |
| 2026-01-13 16:15 | batch_3_contact_removed | Removed sirisha.deevi@sothebys.com - IT engineer at auction house |
| 2026-01-13 16:48 | batch_3_sent | 20 emails sent - all successful |
| 2026-01-13 16:55 | inbox_check | 1 bounce (david@homesusa.com), 1 auto-reply, 0 human replies |
| 2026-01-14 10:00 | inbox_check_2 | 0 human replies, 1 auto-reply (support@champion.com) |
| 2026-01-14 10:05 | followup_1_drafted | 69 follow-up emails drafted for Batch 1 non-responders |
| 2026-01-14 11:16 | followup_1_sent | 63 follow-ups sent (6 bounced emails excluded) |
| 2026-01-14 17:30 | inbox_check_4 | 2 replies - both unsubscribes (Bob Woerner, Donald McVicar). Added to do_not_contact.csv |

---

## Issues Encountered & Fixes

### Issue 1: High Initial Bounce Rate (35%)
- **Problem:** 24 out of 69 emails bounced initially
- **Cause:** Temporary failures marked as bounces
- **Fix:** Added re-verification to bounce recovery (MillionVerifier)
- **Result:** 17/24 'bounced' emails were actually valid

### Issue 2: DMARC Rejection (15 bounces)
- **Problem:** Emails sent from lahaus.ai alias rejected by receiving servers
- **Cause:** lahaus.ai strict DMARC policy, sent via lahaus.com SMTP
- **Fix Attempted:** Changed GMAIL_SEND_AS to lahaus.com
- **Result:** Fix did NOT work - needed bounce recovery with alternative addresses

### Issue 3: Domain Mismatch (11 contacts)
- **Problem:** Email domains unrelated to company names
- **Cause:** Data quality issue in enrichment pipeline
- **Fix:** Added domain mismatch check to verification pipeline, excluded contacts
- **Result:** Improved email quality, reduced bounce risk

### Issue 4: Wrong Person (sirisha.deevi@sothebys.com)
- **Problem:** Contact was IT engineer at auction house, not realtor
- **Cause:** sothebys.com (auction house) vs sothebysrealty.com (real estate franchise)
- **Fix:** Removed from batch 3 after Exa research confirmed wrong persona
- **Result:** Prevented sending to completely wrong recipient

### Issue 5: Follow-up Emails Triggered Unsubscribes (100% of replies)
- **Problem:** Both replies to follow-up #1 were unsubscribe requests
- **Cause:** "Quick follow-up" subject line may feel too pushy; 3-day timing too aggressive
- **Fix:** Consider softer follow-up approach, longer wait time, or value-add content
- **Result:** 2 unsubscribes, 0 positive replies from follow-up #1

---

## Learnings

1. **MillionVerifier re-verification catches false-positive bounces** - Many "bounced" emails are actually valid
2. **DMARC issues require sender domain alignment, not just alias change** - Can't send from lahaus.ai via lahaus.com SMTP
3. **Domain mismatch check prevents sending to wrong recipients** - Email domain should relate to company name
4. **Email verification pipeline reduces critical issues by 79%** - LLM + rules catch most problems
5. **Exa MCP tool provides better research when script-based Exa API quota is exhausted**
6. **For franchise agents (RE/MAX, Coldwell Banker, etc.), research should find agent's personal/team website, not franchise homepage**
7. **Verify sothebys.com vs sothebysrealty.com** - auction house vs real estate franchise are different companies
8. **LLM-powered writing quality check catches spam triggers and awkward phrasing that regex misses**
9. **Cold email CTAs should focus on the problem being solved, not product claims**
10. **Follow-up emails with "Quick follow-up" subject triggered 100% unsubscribe rate** - Consider value-add approach instead of reminder
11. **Both unsubscribes came from follow-up #1, not initial emails** - Timing (3 days) or tone may be the issue

---

## Next Steps

- [x] ~~Monitor for replies to Follow-up #1~~ - 2 unsubscribes received
- [ ] Wait until Jan 16 for more responses before deciding on follow-up #2
- [ ] Reconsider follow-up strategy: value-add content vs. reminder approach
- [ ] Analyze if initial email messaging needs improvement (0 positive replies total)
- [ ] Consider A/B testing subject lines on new prospects

---

## Artifacts

| Artifact | Path |
|----------|------|
| Batch 1 Drafts | output/email_campaign/drafts_v2.csv |
| Batch 2 Drafts | output/email_campaign/drafts_batch2_ready_to_send.csv |
| Batch 3 Drafts | output/email_campaign/drafts_batch3.csv |
| Follow-up #1 | output/email_campaign/followup_1_batch1.csv |
| State File | experiments/active/exp_001_q1_email_sequence/state.json |
| Bounced Contacts | config/bounced_contacts.csv |

---

*Last updated: 2026-01-14 17:35*
