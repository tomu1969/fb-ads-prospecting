"""Module 4: Exporter - Export enriched contacts to HubSpot-compatible format"""

import ast
from pathlib import Path
import pandas as pd


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


def safe_str(value, default=''):
    """Convert value to string, handling NaN and None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    return str(value)


def split_name(name):
    """Split full name into first and last name."""
    if pd.isna(name) or not name:
        return '', ''
    name_str = str(name).strip()
    # Handle common bad values
    if name_str.lower() in ['none', 'none none', 'nan', 'null', 'n/a', '']:
        return '', ''
    parts = name_str.split(' ', 1)
    return parts[0], parts[1] if len(parts) > 1 else ''


def get_first_phone(phones):
    """Extract first phone number from phones list/string."""
    phone_list = parse_list_field(phones)
    return phone_list[0] if phone_list else ''


def is_valid_name(name):
    """Check if a name value is valid (not empty/null/placeholder)."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return False
    name_str = str(name).strip().lower()
    return name_str not in ['', 'none', 'nan', 'null', 'n/a', 'none none']


def get_matched_name(row):
    """Get the name that corresponds to the email/phone source.

    Priority:
    1. If primary_email matches pipeline_email, use pipeline_name
    2. If Hunter found a name, use hunter_contact_name
    3. Fall back to scraper_contact_name or contact_name
    """
    primary_email = str(row.get('primary_email', '')).strip()

    # Check if email came from pipeline enrichment
    pipeline_email = str(row.get('pipeline_email', '')).strip()
    if pipeline_email and primary_email and primary_email == pipeline_email:
        pipeline_name = row.get('pipeline_name', '')
        if is_valid_name(pipeline_name):
            return str(pipeline_name).strip()

    # Check if Hunter found a name (associated with primary_email from Hunter)
    hunter_name = row.get('hunter_contact_name', '')
    if is_valid_name(hunter_name):
        return str(hunter_name).strip()

    # Fall back to scraper name (original name from website scraping)
    scraper_name = row.get('scraper_contact_name', '')
    if is_valid_name(scraper_name):
        return str(scraper_name).strip()

    # Last resort: contact_name (which may be Hunter or scraper depending on data)
    contact_name = row.get('contact_name', '')
    if is_valid_name(contact_name):
        return str(contact_name).strip()

    return ''


def format_platforms(platforms):
    """Format platforms list as semicolon-separated string."""
    platform_list = parse_list_field(platforms)
    return ';'.join(platform_list) if platform_list else ''


def export_hubspot(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export HubSpot-compatible CSV with mapped columns.

    HubSpot column mapping:
    - email: primary_email (unique identifier)
    - firstname, lastname: parsed from matched_name (name that corresponds to email source)
    - company: page_name
    - jobtitle: contact_position
    - website: website_url
    - phone: first phone from phones array
    - Custom properties: fb_ad_count, fb_page_likes, ad_platforms, etc.
    """
    output_path = output_dir / 'hubspot_contacts.csv'

    hubspot_df = pd.DataFrame()

    # Standard HubSpot properties
    hubspot_df['email'] = df['primary_email'].apply(safe_str)

    # Get matched name (name that corresponds to the email source) and split into first/last
    matched_names = df.apply(get_matched_name, axis=1)
    names = matched_names.apply(split_name)
    hubspot_df['firstname'] = [n[0] for n in names]
    hubspot_df['lastname'] = [n[1] for n in names]

    hubspot_df['company'] = df['page_name'].apply(safe_str)
    hubspot_df['jobtitle'] = df.get('contact_position', pd.Series([''] * len(df))).apply(safe_str)
    hubspot_df['website'] = df['website_url'].apply(safe_str)
    hubspot_df['phone'] = df['phones'].apply(get_first_phone)

    # LinkedIn (custom property)
    if 'linkedin_url' in df.columns:
        hubspot_df['linkedin_url'] = df['linkedin_url'].apply(safe_str)

    # Custom properties for FB ads data
    hubspot_df['fb_ad_count'] = df['ad_count'].fillna(0).astype(int)
    hubspot_df['fb_page_likes'] = df['total_page_likes'].fillna(0).astype(int)
    hubspot_df['ad_platforms'] = df['platforms'].apply(format_platforms)

    if 'first_ad_date' in df.columns:
        hubspot_df['first_ad_date'] = df['first_ad_date'].apply(safe_str)

    if 'services' in df.columns:
        # Convert services list to string
        hubspot_df['services'] = df['services'].apply(lambda x: ';'.join(parse_list_field(x)) if x else '')

    # Email verification status
    hubspot_df['email_verified'] = df['email_verified'].apply(
        lambda x: 'true' if x in [True, 'valid', 'accept_all'] else 'false' if pd.notna(x) else ''
    )

    # Enrichment source (for tracking)
    if 'enrichment_stage' in df.columns:
        hubspot_df['enrichment_source'] = df['enrichment_stage'].apply(safe_str)

    # Only export rows with valid email
    hubspot_df = hubspot_df[hubspot_df['email'].str.contains('@', na=False)]

    hubspot_df.to_csv(output_path, index=False, encoding='utf-8')
    return output_path


def export_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export full data as CSV (all columns), including matched_name."""
    output_path = output_dir / 'prospects_final.csv'

    # Add matched_name column (name that corresponds to email source)
    df = df.copy()
    df['matched_name'] = df.apply(get_matched_name, axis=1)

    df.to_csv(output_path, index=False, encoding='utf-8')
    return output_path


def export_excel(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export full data as Excel, including matched_name."""
    output_path = output_dir / 'prospects_final.xlsx'

    # Add matched_name column (name that corresponds to email source)
    df = df.copy()
    df['matched_name'] = df.apply(get_matched_name, axis=1)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Prospects')

        worksheet = writer.sheets['Prospects']
        for idx, col in enumerate(df.columns, 1):
            max_len = max(
                df[col].astype(str).str.len().max(),
                len(col)
            )
            # Handle column indices > 26
            if idx <= 26:
                col_letter = chr(64 + idx)
            else:
                col_letter = 'A'  # Fallback for many columns
            worksheet.column_dimensions[col_letter].width = min(max_len + 2, 50)

    return output_path


def export_imessage(df: pd.DataFrame, output_dir: Path) -> dict:
    """Export iMessage-compatible CSVs split by with/without names.

    Creates two files:
    - imessage_with_names.csv: Contacts that have a matched name
    - imessage_without_names.csv: Contacts without a matched name

    Columns: Phone, First Name, Last Name, Email, Company
    Only includes rows with valid phone numbers.
    """
    # Add matched_name if not present
    df = df.copy()
    if 'matched_name' not in df.columns:
        df['matched_name'] = df.apply(get_matched_name, axis=1)

    # Create iMessage-compatible format
    imessage_df = pd.DataFrame()
    imessage_df['Phone'] = df['phones'].apply(get_first_phone)
    names = df['matched_name'].apply(split_name)
    imessage_df['First Name'] = [n[0] for n in names]
    imessage_df['Last Name'] = [n[1] for n in names]
    imessage_df['Email'] = df['primary_email'].apply(safe_str)
    imessage_df['Company'] = df['page_name'].apply(safe_str)

    # Filter to only rows with phone numbers
    imessage_df = imessage_df[imessage_df['Phone'] != '']

    # Split by whether they have a name
    has_name = (imessage_df['First Name'] != '') | (imessage_df['Last Name'] != '')
    df_with_names = imessage_df[has_name]
    df_without_names = imessage_df[~has_name]

    # Save files
    path_with = output_dir / 'imessage_with_names.csv'
    path_without = output_dir / 'imessage_without_names.csv'

    df_with_names.to_csv(path_with, index=False, encoding='utf-8')
    df_without_names.to_csv(path_without, index=False, encoding='utf-8')

    return {
        'iMessage (with names)': path_with,
        'iMessage (without names)': path_without,
        'counts': {
            'with_names': len(df_with_names),
            'without_names': len(df_without_names)
        }
    }


def generate_summary_report(df: pd.DataFrame, output_paths: dict) -> None:
    """Print summary report of exported data."""
    total = len(df)

    # Count verified emails
    with_verified_email = 0
    if 'email_verified' in df.columns:
        with_verified_email = df['email_verified'].apply(
            lambda x: x in [True, 'valid', 'accept_all', 'manual']
        ).sum()

    # Count contacts with any email
    with_email = df['primary_email'].apply(
        lambda x: bool(x) and '@' in str(x)
    ).sum() if 'primary_email' in df.columns else 0

    # Count contacts with phone
    with_phone = df['phones'].apply(
        lambda x: bool(parse_list_field(x))
    ).sum() if 'phones' in df.columns else 0

    # Count contacts with name
    with_contact = df['contact_name'].apply(
        lambda x: bool(x) and str(x).lower() not in ['none', 'nan', 'none none', '']
    ).sum() if 'contact_name' in df.columns else 0

    print("\n" + "=" * 50)
    print("PIPELINE EXPORT SUMMARY")
    print("=" * 50)
    print(f"Total prospects:           {total}")
    print(f"With verified email:       {with_verified_email} ({100*with_verified_email/total:.1f}%)" if total else "")
    print(f"With any email:            {with_email} ({100*with_email/total:.1f}%)" if total else "")
    print(f"With phone number:         {with_phone} ({100*with_phone/total:.1f}%)" if total else "")
    print(f"With contact name:         {with_contact} ({100*with_contact/total:.1f}%)" if total else "")
    print("-" * 50)
    print("Output files:")
    for name, path in output_paths.items():
        print(f"  {name}: {path}")
    print("=" * 50 + "\n")


def export_all(df: pd.DataFrame, output_dir: str = 'output') -> dict:
    """Export all formats: HubSpot CSV, full CSV, Excel, and iMessage CSVs."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_paths = {
        'HubSpot': export_hubspot(df, output_path),
        'CSV': export_csv(df, output_path),
        'Excel': export_excel(df, output_path),
    }

    # Export iMessage-compatible CSVs
    imessage_result = export_imessage(df, output_path)
    output_paths['iMessage (with names)'] = imessage_result['iMessage (with names)']
    output_paths['iMessage (without names)'] = imessage_result['iMessage (without names)']

    generate_summary_report(df, output_paths)

    # Print iMessage-specific counts
    counts = imessage_result['counts']
    print(f"iMessage contacts with names: {counts['with_names']}")
    print(f"iMessage contacts without names: {counts['without_names']}")
    print("=" * 50 + "\n")

    return output_paths


if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent

    # Try primary input (from Agent Enricher), then fallback
    input_path = base_dir / 'processed' / '03d_final.csv'
    fallback_path = base_dir / 'processed' / '03b_hunter.csv'

    output_dir = base_dir / 'output'

    if input_path.exists():
        print(f"Loading: {input_path}")
        df = pd.read_csv(input_path, encoding='utf-8')
        export_all(df, str(output_dir))
    elif fallback_path.exists():
        print(f"Using fallback: {fallback_path}")
        df = pd.read_csv(fallback_path, encoding='utf-8')
        export_all(df, str(output_dir))
    else:
        print(f"Input file not found: {input_path}")
        print("Run previous pipeline modules first.")
