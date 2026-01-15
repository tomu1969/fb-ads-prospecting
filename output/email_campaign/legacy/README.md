# Archived Email Campaign Files

This folder contains intermediate and duplicate email campaign files from the Q1 2026 campaign (exp_001).

## Archived on 2026-01-14

### Duplicate Draft Files
- `drafts_20260113_110425_fixed.csv` - Duplicate of batch2_fixed
- `drafts_20260113_110432_fixed.csv` - Duplicate of batch2_fixed
- `drafts_batch2_fixed.csv` - Superseded by drafts_batch2_fixed_v2.csv
- `drafts_recovered_v2.csv` - Early version of batch 4 drafts

### Intermediate Verification Reports
- `verification_report_20260113_110425.csv` - Batch 2 verification (before)
- `verification_report_20260113_110425_after.csv` - Batch 2 verification (after fixes)
- `verification_report_20260113_110432.csv` - Batch 2 verification duplicate (before)
- `verification_report_20260113_110432_after.csv` - Batch 2 verification duplicate (after)
- `verification_report.csv` - Batch 4 verification report

### Intermediate Files
- `recovered_contacts.csv` - Superseded by recovered_batch2.csv
- `resend_2_drafts.csv` - Small intermediate resend file (batch 1)
- `resend_input.csv` - Intermediate batch 1 resend input
- `to_draft_batch2.csv` - Batch 2 input file before drafting

## Active Campaign Files (NOT archived)

These files are actively referenced in `experiments/active/exp_001_q1_email_sequence/state.json`:

**Batch 1:**
- `drafts_v2.csv` (93 drafts)
- `drafts_resend.csv` (resend 1)
- `drafts_resend_bounced.csv` (resend 2)
- `resend_drafts.csv` (resend 3)

**Batch 2:**
- `drafts_batch2.csv` (original)
- `drafts_batch2_fixed_v2.csv` (fixed version)
- `drafts_batch2_ready_to_send.csv` (sent)
- `excluded_contacts.csv` (11 exclusions)

**Batch 3:**
- `drafts_batch3.csv` (21 drafts)

**Batch 4:**
- `to_draft_recovered.csv` (input)
- `drafts_recovered.csv` (45 drafts)
- `drafts_recovered_fixed.csv` (sent)
- `recovered_batch2.csv` (recovery output)

**Tracking:**
- `campaign_log.md`
- `old_vs_new_comparison.md`

**Follow-ups:**
- `followup_1_batch1.csv` (63 follow-ups)

## Recovery

These files are NOT in git (in .gitignore). To restore:
```bash
cp output/email_campaign/legacy/[filename] output/email_campaign/[filename]
```
