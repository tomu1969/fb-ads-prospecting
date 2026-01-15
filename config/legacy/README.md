# Archived Config Files

This folder contains archived configuration files that are no longer actively used but preserved for reference.

## Archived on 2026-01-14

### Bounced Contacts Files
- `bounced_contacts_2.csv` - Intermediate bounce tracking file (15 contacts)
- `bounced_contacts_3.csv` - Earlier bounce tracking snapshot (4 contacts)

**Current file**: `config/bounced_contacts.csv` (68 contacts) is actively maintained and referenced by experiment exp_001.

## Recovery

These files are preserved in git history. To restore:
```bash
git mv config/legacy/[filename] config/[filename]
```
