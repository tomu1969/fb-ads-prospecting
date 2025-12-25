"""
Module 4: Email Composer
Generates personalized outreach emails using OpenAI GPT-4o-mini
"""

import pandas as pd
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm
import ast
import os
import re
import json

# Load from local .env first, then fallback to parent ai_pbx .env
load_dotenv()
if not os.getenv("OPENAI_API_KEY"):
    parent_env = Path(__file__).parent.parent.parent / ".env"
    if parent_env.exists():
        load_dotenv(parent_env)

# Primary input is from Agent Enricher (Module 3.6), fallback to Hunter output
INPUT_PATH = Path(__file__).parent.parent / "processed" / "03d_final.csv"
INPUT_FALLBACK = Path(__file__).parent.parent / "processed" / "03b_hunter.csv"
OUTPUT_PATH = Path(__file__).parent.parent / "processed" / "04_emails.csv"

MODEL = "gpt-4o-mini"
MAX_TOKENS = 500
TEMPERATURE = 0.7


def parse_list_column(value):
    if pd.isna(value) or value == "":
        return []
    if isinstance(value, list):
        return value
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return [value] if value else []


def build_prospect_context(row):
    ad_texts = parse_list_column(row.get("ad_texts", []))
    ad_summary = "; ".join(ad_texts[:3]) if ad_texts else "No ad copy available"
    if len(ad_summary) > 300:
        ad_summary = ad_summary[:297] + "..."

    services = parse_list_column(row.get("services", []))
    services_str = ", ".join(services) if services else "Real estate services"

    # Parse platforms and format nicely
    platforms = parse_list_column(row.get("platforms", []))
    # Filter to main platforms and format
    main_platforms = [p for p in platforms if p in ["FACEBOOK", "INSTAGRAM"]]
    if not main_platforms:
        main_platforms = platforms[:2] if platforms else ["Meta"]
    platforms_str = " and ".join([p.capitalize() for p in main_platforms])

    # Proper pluralization
    ad_count = int(row.get("ad_count", 0))
    ad_word = "ad" if ad_count == 1 else "ads"
    ad_count_str = f"{ad_count} {ad_word}"

    # Clean contact name - remove bad values
    contact_name = row.get("contact_name") or ""
    bad_names = ["none", "none none", "nan", "null", "n/a", "team", ""]
    if str(contact_name).lower().strip() in bad_names:
        contact_name = ""

    return {
        "contact_name": contact_name,
        "contact_position": row.get("contact_position") or "",
        "page_name": row.get("page_name", ""),
        "ad_count": ad_count,
        "ad_count_str": ad_count_str,
        "platforms": platforms_str,
        "first_ad_date": row.get("first_ad_date", ""),
        "page_likes": row.get("total_page_likes", 0),
        "services": services_str,
        "ad_summary": ad_summary,
        "company_description": row.get("company_description") or "",
    }


def generate_email(client, context):
    prompt = f"""You are writing a cold outreach email for LaHaus AI.

ABOUT LAHAUS AI:
- AI assistant that answers real estate leads in SECONDS (not hours)
- Automatically qualifies leads and schedules appointments
- 60+ realtors already using LaHaus AI to increase their ad ROI
- Perfect for agents running Facebook/Instagram ads who lose leads to slow response times

PROSPECT INFO:
- Contact: {context['contact_name']}, {context['contact_position']}
- Company: {context['page_name']}
- Running {context['ad_count_str']} on {context['platforms']} (since {context['first_ad_date']})
- Page Likes: {context['page_likes']}
- Their Services: {context['services']}
- Their Ad Copy: {context['ad_summary']}

Write a short, personalized email (max 120 words) that:
1. Addresses them by first name (extract from contact_name). IMPORTANT: If contact_name is empty, "None", "nan", "Team", or unclear, use "Hi there" instead - NEVER write "Hi None" or "Hi nan"
2. MUST mention exactly: "{context['ad_count_str']} on {context['platforms']}" - this shows you researched them
3. Points out the problem: leads from ads go cold if not answered in seconds
4. Solution: LaHaus AI responds instantly, qualifies, and books appointments
5. Social proof: 60+ realtors already using it
6. CTA: Quick call or demo this week

Tone: Friendly, direct, not salesy. Like a fellow realtor sharing a tool that works.

Sign off with EXACTLY:
Tomás Uribe
Cofounder, LaHaus AI

Respond in JSON:
{{
    "subject": "short punchy subject - mention ad count and platform",
    "body": "email body - MUST include the exact ad count and platforms",
    "personalization_points": ["what you personalized"]
}}"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_TOKENS,
        temperature=TEMPERATURE,
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    return result.get("subject", ""), result.get("body", ""), result.get("personalization_points", [])


def validate_email_content(subject, body):
    errors = []

    placeholder_patterns = [r"\{[^}]+\}", r"\[[A-Z_]+\]", r"<[^>]+>"]
    for pattern in placeholder_patterns:
        if re.search(pattern, subject) or re.search(pattern, body):
            errors.append(f"Contains placeholder: {pattern}")

    if len(body) < 50:
        errors.append(f"Body too short: {len(body)} chars (min 50)")

    if not subject:
        errors.append("Subject is empty")

    return len(errors) == 0, errors


def compose_all(df):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    subjects = []
    bodies = []
    personalization = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Composing emails"):
        try:
            context = build_prospect_context(row)
            subject, body, points = generate_email(client, context)

            is_valid, errors = validate_email_content(subject, body)
            if not is_valid:
                print(f"Warning: Row {idx} validation failed: {errors}")

            subjects.append(subject)
            bodies.append(body)
            personalization.append(points)

        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            subjects.append("")
            bodies.append("")
            personalization.append([])

    df["email_subject"] = subjects
    df["email_body"] = bodies
    df["personalization_points"] = personalization

    return df


if __name__ == "__main__":
    import sys

    run_all = "--all" in sys.argv
    print(f"=== Email Composer Module {'Full Run' if run_all else 'Test'} ===\n")

    # Try primary input (from Module 3.6), then fallback to Hunter output
    if INPUT_PATH.exists():
        print(f"Loading: {INPUT_PATH}")
        df = pd.read_csv(INPUT_PATH)
        test_df = df.copy() if run_all else df.head(3).copy()
    elif INPUT_FALLBACK.exists():
        print(f"Module 3.6 output not found, using fallback: {INPUT_FALLBACK}")
        df = pd.read_csv(INPUT_FALLBACK)
        test_df = df.copy() if run_all else df.head(3).copy()
    else:
        print("No input file found. Creating mock data for testing...")
        test_df = pd.DataFrame({
            "page_name": ["Sunset Realty Group", "Miami Luxury Homes", "First Choice Realtors"],
            "ad_count": [12, 8, 5],
            "total_page_likes": [2500, 15000, 850],
            "ad_texts": [
                '["Find your dream home today!", "Luxury living awaits"]',
                '["Exclusive waterfront properties", "VIP buyer services"]',
                '["First-time buyer specialists", "Free home valuation"]',
            ],
            "platforms": ['["FACEBOOK", "INSTAGRAM"]', '["FACEBOOK"]', '["INSTAGRAM"]'],
            "is_active": [True, True, False],
            "first_ad_date": ["2024-06-15", "2024-03-20", "2024-09-01"],
            "website_url": ["https://sunsetrealty.com", "https://miamiluxury.com", "https://firstchoice.com"],
            "search_confidence": [0.9, 0.85, 0.7],
            "linkedin_url": ["", "https://linkedin.com/company/miami-luxury", ""],
            "contact_name": ["Sarah Johnson", "Carlos Rodriguez", ""],
            "contact_position": ["Lead Agent", "Broker", ""],
            "emails": ['["sarah@sunsetrealty.com"]', '["carlos@miamiluxury.com"]', "[]"],
            "phones": ['["305-555-0123"]', '["786-555-0456"]', "[]"],
            "company_description": [
                "Full-service real estate agency",
                "Luxury waterfront property specialists",
                "Helping families find homes since 2010",
            ],
            "services": [
                '["Buyer representation", "Seller listing", "Property management"]',
                '["Luxury sales", "Investment properties", "Relocation services"]',
                '["First-time buyers", "VA loans", "FHA specialists"]',
            ],
            "social_links": ["{}", "{}", "{}"],
        })

    print(f"Processing {len(test_df)} rows...\n")

    result_df = compose_all(test_df)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved to: {OUTPUT_PATH}")

    print("\n=== Sample Output ===")
    for idx, row in result_df.iterrows():
        print(f"\n--- {row['page_name']} ---")
        print(f"Subject: {row['email_subject']}")
        print(f"Body: {row['email_body'][:200]}..." if len(str(row['email_body'])) > 200 else f"Body: {row['email_body']}")
        print(f"Personalization: {row['personalization_points']}")
