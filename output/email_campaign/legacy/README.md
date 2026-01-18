# Archived Email Campaign Files

This folder contains intermediate and duplicate email campaign files from the Q1 2026 campaign (exp_001).

## Archived on 2026-01-18

### Superseded Campaign Files
- `drafts_v2.csv` - Early batch 1 version (superseded by active batches)
- `drafts_resend.csv` - Batch 1 resend attempt (superseded)
- `drafts_resend_bounced.csv` - Batch 1 bounced contacts resend
- `resend_drafts.csv` - Another batch 1 resend iteration
- `drafts_recovered.csv` - Batch 4 recovery drafts (pre-fixes)
- `drafts_recovered_fixed.csv` - Batch 4 recovery (final version sent)
- `to_draft_recovered.csv` - Batch 4 recovery input
- `recovered_batch2.csv` - Additional recovery batch
- `drafts_test_new_approach.csv` - Test batch for new email approach
- `drafts_fresh_batch.csv` - Small test batch (5 contacts)
- `excluded_contacts.csv` - Contacts excluded from batch 2
- `old_vs_new_comparison.md` - Analysis of email template changes
- `verification_report.csv` - Email verification report (duplicate)

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

## Active Campaign Files (Still in parent directory)

These files are currently active and referenced in `experiments/active/exp_001_q1_email_sequence/state.json`:

**Active Batches:**
- `drafts_batch2.csv` - Batch 2 original (49 contacts)
- `drafts_batch2_fixed_v2.csv` - Batch 2 fixed version
- `drafts_batch2_ready_to_send.csv` - Batch 2 sent version (36 sent)
- `drafts_batch3.csv` - Batch 3 (21 contacts)

**Follow-ups:**
- `followup_1_batch1.csv` - Batch 1 follow-ups (63 contacts)
- `followup_value_batches_2_3.csv` - Batches 2-3 follow-ups

**Tracking:**
- `campaign_log.md` - Master campaign log
- `experiment_report.md` - Experiment analysis

## Recovery

These files are NOT in git (in .gitignore). To restore:
```bash
cp output/email_campaign/legacy/[filename] output/email_campaign/[filename]
```
