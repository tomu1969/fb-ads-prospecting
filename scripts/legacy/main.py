"""
FB Ads Library Prospecting Pipeline - Main Orchestrator

Usage:
    python scripts/main.py              # Test mode (3 rows)
    python scripts/main.py --full       # Full pipeline (all records)
    python scripts/main.py --from-step 3  # Resume from step 3
"""

import argparse
import os
import sys
from pathlib import Path

# Load environment before importing modules
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "processed"
OUTPUT_DIR = BASE_DIR / "output"

# Load local .env first, then parent ai_pbx .env as fallback
load_dotenv(BASE_DIR / ".env")
if not os.getenv("OPENAI_API_KEY"):
    parent_env = BASE_DIR.parent / ".env"
    if parent_env.exists():
        load_dotenv(parent_env, override=True)

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from loader import load_and_process
from enricher import enrich_all
from scraper import scrape_all
from apollo_enricher import enrich_missing_emails
from composer import compose_all
from exporter import export_all


def step_load():
    print("\n" + "=" * 60)
    print("STEP 1: Loading and processing Excel data")
    print("=" * 60)
    df = load_and_process()
    print(f"Loaded {len(df)} records")
    return df


def step_enrich(df, test_limit=None):
    print("\n" + "=" * 60)
    print("STEP 2: Enriching with website URLs")
    print("=" * 60)

    if test_limit:
        df = df.head(test_limit).copy()
        print(f"Test mode: processing {test_limit} rows")

    df = enrich_all(df)

    output_path = PROCESSED_DIR / "02_enriched.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved to {output_path}")

    websites_found = (df["website_url"] != "").sum()
    print(f"Websites found: {websites_found}/{len(df)}")

    return df


def step_scrape(df):
    print("\n" + "=" * 60)
    print("STEP 3: Scraping contact information")
    print("=" * 60)

    df = scrape_all(df)

    output_path = PROCESSED_DIR / "03_contacts.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved to {output_path}")

    contacts_found = (df["contact_name"] != "").sum()
    emails_found = df["emails"].apply(lambda x: x != "[]" and x != "").sum()
    print(f"Contacts found: {contacts_found}/{len(df)}")
    print(f"Emails found: {emails_found}/{len(df)}")

    return df


def step_apollo_enrich(df):
    print("\n" + "=" * 60)
    print("STEP 3.5: Apollo.io email enrichment")
    print("=" * 60)

    if not os.getenv("APOLLO_API_KEY"):
        print("APOLLO_API_KEY not set - skipping Apollo enrichment")
        return df

    df = enrich_missing_emails(df)

    # Save updated contacts
    output_path = PROCESSED_DIR / "03_contacts.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Updated {output_path}")

    # Report final email count
    emails_found = df["emails"].apply(lambda x: x != "[]" and x != "").sum()
    print(f"Total emails after Apollo: {emails_found}/{len(df)}")

    return df


def step_compose(df):
    print("\n" + "=" * 60)
    print("STEP 4: Composing personalized emails")
    print("=" * 60)

    df = compose_all(df)

    output_path = PROCESSED_DIR / "04_emails.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved to {output_path}")

    emails_composed = (df["email_body"].notna() & (df["email_body"] != "")).sum()
    print(f"Emails composed: {emails_composed}/{len(df)}")

    return df


def step_export(df):
    print("\n" + "=" * 60)
    print("STEP 5: Exporting final files")
    print("=" * 60)

    output_paths = export_all(df, str(OUTPUT_DIR))
    return output_paths


def load_intermediate(step):
    files = {
        2: PROCESSED_DIR / "01_loaded.csv",
        3: PROCESSED_DIR / "02_enriched.csv",
        4: PROCESSED_DIR / "03_contacts.csv",
        5: PROCESSED_DIR / "04_emails.csv",
    }
    path = files.get(step)
    if path and path.exists():
        print(f"Loading from {path}")
        return pd.read_csv(path, encoding="utf-8")
    return None


def run_pipeline(test_mode=True, from_step=1):
    print("\n" + "#" * 60)
    print("# FB Ads Library Prospecting Pipeline")
    print(f"# Mode: {'TEST (3 rows)' if test_mode else 'FULL'}")
    print(f"# Starting from step: {from_step}")
    print("#" * 60)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    test_limit = 3 if test_mode else None
    df = None

    if from_step > 1:
        df = load_intermediate(from_step)
        if df is None:
            print(f"Error: Cannot resume from step {from_step}, intermediate file not found")
            return

    if from_step <= 1:
        df = step_load()
        if test_limit:
            df = df.head(test_limit).copy()
            df.to_csv(PROCESSED_DIR / "01_loaded.csv", index=False, encoding="utf-8")

    if from_step <= 2:
        if df is None:
            df = load_intermediate(2)
        df = step_enrich(df, test_limit=None if from_step > 1 else test_limit)

    if from_step <= 3:
        if df is None:
            df = load_intermediate(3)
        df = step_scrape(df)
        # Apollo enrichment runs after scraping
        df = step_apollo_enrich(df)

    if from_step <= 4:
        if df is None:
            df = load_intermediate(4)
        df = step_compose(df)

    if from_step <= 5:
        if df is None:
            df = load_intermediate(5)
        step_export(df)

    print("\n" + "#" * 60)
    print("# Pipeline Complete!")
    print("#" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FB Ads Library Prospecting Pipeline")
    parser.add_argument("--test", action="store_true", help="Test mode with 3 rows (default)")
    parser.add_argument("--full", action="store_true", help="Run full pipeline with all records")
    parser.add_argument("--from-step", type=int, default=1, choices=[1, 2, 3, 4, 5],
                        help="Resume from step N (1=load, 2=enrich, 3=scrape, 4=compose, 5=export)")

    args = parser.parse_args()

    test_mode = not args.full
    run_pipeline(test_mode=test_mode, from_step=args.from_step)
