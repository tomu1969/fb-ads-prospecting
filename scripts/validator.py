#!/usr/bin/env python3
"""
Module 5: Pipeline Validator
Validates final output against source data and checks enrichment quality.
Run after exporter.py to verify data quality.
"""

import ast
import pandas as pd
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
SOURCE_FILE = BASE_DIR / "processed" / "01_loaded.csv"
ENRICHED_FILE = BASE_DIR / "processed" / "02_enriched.csv"
HUNTER_FILE = BASE_DIR / "processed" / "03b_hunter.csv"
FINAL_FILE = BASE_DIR / "processed" / "03d_final.csv"
HUBSPOT_FILE = BASE_DIR / "output" / "hubspot_contacts.csv"


def parse_list_field(value):
    """Parse a list field from string representation."""
    if isinstance(value, list):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == '':
        return []
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return [value] if value else []


def load_data():
    """Load all pipeline data files."""
    data = {}

    if SOURCE_FILE.exists():
        data['source'] = pd.read_csv(SOURCE_FILE)
        print(f"Loaded source: {len(data['source'])} rows")

    if ENRICHED_FILE.exists():
        data['enriched'] = pd.read_csv(ENRICHED_FILE)
        print(f"Loaded enriched: {len(data['enriched'])} rows")

    if HUNTER_FILE.exists():
        data['hunter'] = pd.read_csv(HUNTER_FILE)
        print(f"Loaded hunter: {len(data['hunter'])} rows")

    if FINAL_FILE.exists():
        data['final'] = pd.read_csv(FINAL_FILE)
        print(f"Loaded final: {len(data['final'])} rows")
    elif HUNTER_FILE.exists():
        # Fallback to hunter if agent enricher wasn't run
        data['final'] = data.get('hunter')
        print("Using hunter output as final (agent enricher not run)")

    if HUBSPOT_FILE.exists():
        data['hubspot'] = pd.read_csv(HUBSPOT_FILE)
        print(f"Loaded hubspot: {len(data['hubspot'])} rows")

    return data


def check_contact_completeness(df):
    """Check for missing contact names, emails, and phones."""
    issues = {
        'complete': [],           # Has email + contact + phone
        'email_contact': [],      # Has email + contact (no phone)
        'email_only': [],         # Has email only
        'no_email': [],           # No valid email
    }

    for _, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')

        # Check email
        primary_email = row.get('primary_email', '')
        has_email = bool(primary_email) and '@' in str(primary_email)

        # Check contact name
        contact_name = str(row.get('contact_name', '')).strip()
        bad_names = ['none', 'none none', 'nan', 'null', 'n/a', '']
        has_contact = contact_name.lower() not in bad_names

        # Check phone
        phones = parse_list_field(row.get('phones', []))
        has_phone = len(phones) > 0

        if not has_email:
            issues['no_email'].append(page_name)
        elif has_email and has_contact and has_phone:
            issues['complete'].append(page_name)
        elif has_email and has_contact:
            issues['email_contact'].append(page_name)
        else:
            issues['email_only'].append(page_name)

    return issues


def check_enrichment_sources(df):
    """Check enrichment sources and stages."""
    sources = defaultdict(int)
    stages = defaultdict(list)

    for _, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')
        stage = row.get('enrichment_stage', 'unknown')
        sources[stage] += 1
        stages[stage].append(page_name)

    return sources, stages


def check_email_verification(df):
    """Check email verification status."""
    status_counts = defaultdict(int)
    details = defaultdict(list)

    for _, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')
        primary_email = row.get('primary_email', '')
        verified = str(row.get('email_verified', ''))

        if not primary_email or '@' not in str(primary_email):
            status_counts['no_email'] += 1
            details['no_email'].append(page_name)
        elif verified.lower() in ['valid', 'accept_all']:
            status_counts['verified'] += 1
        elif verified == 'manual':
            status_counts['manual'] += 1
        elif verified.lower() in ['invalid', 'unknown', 'risky']:
            status_counts['unverified'] += 1
            details['unverified'].append(f"{page_name} ({verified})")
        else:
            status_counts['not_checked'] += 1
            details['not_checked'].append(page_name)

    return status_counts, details


def check_phone_coverage(df):
    """Check phone number coverage."""
    stats = {
        'with_phone': 0,
        'no_phone': 0,
        'multiple_phones': 0,
    }
    no_phone_list = []

    for _, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')
        phones = parse_list_field(row.get('phones', []))

        if len(phones) == 0:
            stats['no_phone'] += 1
            no_phone_list.append(page_name)
        elif len(phones) == 1:
            stats['with_phone'] += 1
        else:
            stats['multiple_phones'] += 1
            stats['with_phone'] += 1

    return stats, no_phone_list


def check_website_coverage(df):
    """Check website enrichment coverage."""
    stats = {
        'with_website': 0,
        'no_website': 0,
        'low_confidence': 0,
    }
    issues = []

    for _, row in df.iterrows():
        page_name = row.get('page_name', 'Unknown')
        website = row.get('website_url', '')
        confidence = row.get('search_confidence', 0)

        if pd.isna(website) or not website:
            stats['no_website'] += 1
            issues.append(page_name)
        else:
            stats['with_website'] += 1
            if confidence and float(confidence) < 0.5:
                stats['low_confidence'] += 1

    return stats, issues


def check_hubspot_export(data):
    """Validate HubSpot export file."""
    issues = []

    if 'hubspot' not in data:
        return ["HubSpot export file not found"]

    hubspot_df = data['hubspot']

    # Check required columns
    required_columns = ['email', 'firstname', 'company']
    for col in required_columns:
        if col not in hubspot_df.columns:
            issues.append(f"Missing required column: {col}")

    # Check all rows have email
    if 'email' in hubspot_df.columns:
        empty_emails = hubspot_df['email'].isna().sum()
        if empty_emails > 0:
            issues.append(f"{empty_emails} rows have empty email")

        invalid_emails = (~hubspot_df['email'].str.contains('@', na=True)).sum()
        if invalid_emails > 0:
            issues.append(f"{invalid_emails} rows have invalid email format")

    return issues


def generate_report(data):
    """Generate comprehensive validation report."""
    print("\n" + "=" * 70)
    print("PIPELINE VALIDATION REPORT")
    print("=" * 70)

    final_df = data.get('final')
    if final_df is None:
        print("ERROR: No final data to validate")
        return False

    total = len(final_df)

    # 1. Contact Completeness Check
    print("\n1. CONTACT COMPLETENESS")
    print("-" * 50)
    issues = check_contact_completeness(final_df)

    complete_pct = 100 * len(issues['complete']) / total if total else 0
    email_contact_pct = 100 * len(issues['email_contact']) / total if total else 0
    email_only_pct = 100 * len(issues['email_only']) / total if total else 0
    no_email_pct = 100 * len(issues['no_email']) / total if total else 0

    print(f"   Complete (email+contact+phone): {len(issues['complete'])} ({complete_pct:.1f}%)")
    print(f"   Email + Contact (no phone):     {len(issues['email_contact'])} ({email_contact_pct:.1f}%)")
    print(f"   Email only:                     {len(issues['email_only'])} ({email_only_pct:.1f}%)")
    print(f"   No valid email:                 {len(issues['no_email'])} ({no_email_pct:.1f}%)")

    if issues['no_email']:
        print(f"\n   Prospects without valid email:")
        for name in issues['no_email'][:5]:
            print(f"      - {name}")
        if len(issues['no_email']) > 5:
            print(f"      ... and {len(issues['no_email']) - 5} more")

    # 2. Email Verification Status
    print("\n2. EMAIL VERIFICATION STATUS")
    print("-" * 50)
    email_stats, email_details = check_email_verification(final_df)

    verified_pct = 100 * email_stats['verified'] / total if total else 0
    manual_pct = 100 * email_stats['manual'] / total if total else 0

    print(f"   Verified (valid/accept_all):    {email_stats['verified']} ({verified_pct:.1f}%)")
    print(f"   Manual entries:                 {email_stats['manual']} ({manual_pct:.1f}%)")
    print(f"   Unverified/risky:               {email_stats['unverified']}")
    print(f"   Not checked:                    {email_stats['not_checked']}")
    print(f"   No email:                       {email_stats['no_email']}")

    # 3. Phone Coverage
    print("\n3. PHONE NUMBER COVERAGE")
    print("-" * 50)
    phone_stats, no_phone_list = check_phone_coverage(final_df)

    phone_pct = 100 * phone_stats['with_phone'] / total if total else 0
    print(f"   With phone number:              {phone_stats['with_phone']} ({phone_pct:.1f}%)")
    print(f"   Multiple phones:                {phone_stats['multiple_phones']}")
    print(f"   No phone:                       {phone_stats['no_phone']}")

    if no_phone_list and len(no_phone_list) <= 10:
        print(f"\n   Prospects without phone:")
        for name in no_phone_list:
            print(f"      - {name}")

    # 4. Website Enrichment
    print("\n4. WEBSITE ENRICHMENT")
    print("-" * 50)
    website_stats, no_website = check_website_coverage(final_df)

    website_pct = 100 * website_stats['with_website'] / total if total else 0
    print(f"   With website:                   {website_stats['with_website']} ({website_pct:.1f}%)")
    print(f"   Low confidence (<50%):          {website_stats['low_confidence']}")
    print(f"   No website:                     {website_stats['no_website']}")

    if no_website and len(no_website) <= 5:
        print(f"\n   Prospects without website:")
        for name in no_website:
            print(f"      - {name}")

    # 5. Enrichment Sources
    print("\n5. ENRICHMENT SOURCES")
    print("-" * 50)
    if 'enrichment_stage' in final_df.columns:
        sources, _ = check_enrichment_sources(final_df)
        for source, count in sorted(sources.items(), key=lambda x: -x[1]):
            pct = 100 * count / total if total else 0
            print(f"   {source}: {count} ({pct:.1f}%)")
    else:
        print("   No enrichment stage data available")

    # 6. HubSpot Export Validation
    print("\n6. HUBSPOT EXPORT VALIDATION")
    print("-" * 50)
    hubspot_issues = check_hubspot_export(data)

    if hubspot_issues:
        print(f"   Found {len(hubspot_issues)} issues:")
        for issue in hubspot_issues:
            print(f"      - {issue}")
    else:
        hubspot_count = len(data.get('hubspot', []))
        print(f"   HubSpot export valid: {hubspot_count} contacts ready for import")

    # 7. Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    # Calculate overall quality score
    with_email = total - len(issues['no_email'])
    quality_score = (
        (len(issues['complete']) * 1.0) +
        (len(issues['email_contact']) * 0.7) +
        (len(issues['email_only']) * 0.4)
    ) / total * 100 if total else 0

    print(f"   Total prospects:                {total}")
    print(f"   Ready for HubSpot (has email):  {with_email}")
    print(f"   Quality score:                  {quality_score:.1f}%")

    # Issues summary
    critical_issues = len(issues['no_email']) + len(hubspot_issues)

    if critical_issues == 0:
        print("\n   STATUS: ALL CHECKS PASSED")
    else:
        print(f"\n   STATUS: {critical_issues} CRITICAL ISSUES")
        if issues['no_email']:
            print(f"   - {len(issues['no_email'])} prospects without email (won't be in HubSpot)")
        for issue in hubspot_issues:
            print(f"   - {issue}")

    print("\n   Recommendations:")
    if issues['no_email']:
        print("   - Manual research needed for prospects without email")
    if phone_stats['no_phone'] > total * 0.3:
        print("   - Consider additional phone enrichment sources")
    if email_stats['not_checked'] > 0:
        print("   - Run email verification on unchecked emails")
    if quality_score < 50:
        print("   - Low quality score - review enrichment pipeline")

    print("=" * 70 + "\n")

    return critical_issues == 0


def main():
    print("=== Pipeline Validator ===\n")

    data = load_data()

    if not data:
        print("ERROR: No data files found. Run the pipeline first.")
        return False

    success = generate_report(data)

    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
