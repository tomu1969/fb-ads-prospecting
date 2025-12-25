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


def format_platforms(platforms):
    """Format platforms list as semicolon-separated string."""
    platform_list = parse_list_field(platforms)
    return ';'.join(platform_list) if platform_list else ''


def export_hubspot(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export HubSpot-compatible CSV with mapped columns.

    HubSpot column mapping:
    - email: primary_email (unique identifier)
    - firstname, lastname: parsed from contact_name
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

    # Parse contact_name into first/last
    names = df['contact_name'].apply(split_name)
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
    """Export full data as CSV (all columns)."""
    output_path = output_dir / 'prospects_final.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    return output_path


def export_excel(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export full data as Excel."""
    output_path = output_dir / 'prospects_final.xlsx'

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
    """Export all formats: HubSpot CSV, full CSV, and Excel."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_paths = {
        'HubSpot': export_hubspot(df, output_path),
        'CSV': export_csv(df, output_path),
        'Excel': export_excel(df, output_path),
    }

    generate_summary_report(df, output_paths)

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
