"""
Sector Classifier: Classify advertisers into business sectors using Exa API

Uses Exa to search for business info and OpenAI to classify into sectors.
Designed to reclassify "Other" advertisers into more specific categories.

Usage:
    python scripts/icp_discovery/sector_classifier.py
    python scripts/icp_discovery/sector_classifier.py --limit 10
    python scripts/icp_discovery/sector_classifier.py --dry-run
"""

import os
import sys
import json
import argparse
import logging
import time
import requests
from pathlib import Path
from typing import Optional, Dict, List
from dotenv import load_dotenv

import pandas as pd
from tqdm import tqdm

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sector_classifier.log')
    ]
)
logger = logging.getLogger(__name__)

# API Keys
EXA_API_KEY = os.getenv('EXA_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
MASTER_CSV = BASE_DIR / 'output' / 'icp_exploration' / 'all_advertisers_master.csv'
OUTPUT_CSV = BASE_DIR / 'output' / 'icp_exploration' / 'all_advertisers_master.csv'

# Sector taxonomy
SECTORS = [
    "Real Estate",
    "Legal Services",
    "Healthcare",
    "Finance & Accounting",
    "Insurance",
    "Education",
    "Home Services",
    "Auto & Sales",
    "Professional Services",
    "Beauty & Wellness",
    "Construction & Excavation",
    "Furniture & Retail",
    "Food & Restaurant",
    "Fitness & Gym",
    "Technology & Software",
    "Manufacturing",
    "Transportation & Logistics",
    "Entertainment & Media",
    "Nonprofit & Community",
    "Other",
]

SECTOR_DESCRIPTIONS = """
- Real Estate: Realtors, brokers, property management, mortgage, title/escrow
- Legal Services: Attorneys, law firms, legal aid
- Healthcare: Doctors, dentists, clinics, therapy, mental health, medical
- Finance & Accounting: CPAs, accountants, bookkeeping, tax prep, financial advisors
- Insurance: Insurance agents, brokers, agencies
- Education: Schools, colleges, training centers, CDL schools, academies
- Home Services: Roofing, plumbing, HVAC, electrical, pest control, cleaning, landscaping, painting, junk removal
- Auto & Sales: Car dealerships, auto sales, truck/trailer sales, vehicle dealers
- Professional Services: Marketing, consulting, photography, design, media, staffing
- Beauty & Wellness: Salons, spas, beauty parlors, aesthetics, skincare
- Construction & Excavation: General contractors, excavation, demolition, site work
- Furniture & Retail: Furniture stores, home goods, retail shops
- Food & Restaurant: Restaurants, catering, food service, cafes
- Fitness & Gym: Gyms, fitness centers, personal training, fitness coaching
- Technology & Software: Software companies, tech startups, SaaS, IT services
- Manufacturing: Factories, production, industrial
- Transportation & Logistics: Trucking, shipping, logistics, delivery
- Entertainment & Media: Content creators, entertainment, media production
- Nonprofit & Community: Charities, community organizations, churches
- Other: Does not fit any above category
"""


def search_exa(query: str, num_results: int = 3) -> List[Dict]:
    """Search Exa for business information."""
    if not EXA_API_KEY:
        logger.warning("No EXA_API_KEY configured")
        return []

    try:
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-api-key": EXA_API_KEY
        }

        payload = {
            "query": query,
            "numResults": num_results,
            "type": "auto",
            "contents": {
                "text": {"maxCharacters": 2000}
            }
        }

        response = requests.post(
            "https://api.exa.ai/search",
            headers=headers,
            json=payload,
            timeout=15
        )

        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            logger.warning(f"Exa API error: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Exa search error: {e}")
        return []


def classify_with_patterns(business_name: str, business_info: str) -> Optional[str]:
    """Pattern-based classification as fast fallback."""
    import re

    combined = f"{business_name} {business_info}".lower()

    PATTERN_MAP = {
        'Real Estate': [r'\brealt[yo]r?\b', r'\breal\s*estate\b', r'\bmortgage\b', r'\btitle\b', r'\bescrow\b', r'\bproperty\b'],
        'Legal Services': [r'\battorney\b', r'\blaw\s*firm\b', r'\blegal\b', r'\blawyer\b', r'\b,\s*pc\b', r'\besq\b'],
        'Healthcare': [r'\bmedical\b', r'\bdental\b', r'\bdentist\b', r'\bclinic\b', r'\bdoctor\b', r'\bdr\.\s', r'\btherapy\b', r'\bhealth\b'],
        'Finance & Accounting': [r'\bcpa\b', r'\baccountant\b', r'\bbookkeep\b', r'\btax\s*(prep|service)?\b', r'\bfinancial\b'],
        'Insurance': [r'\binsurance\b', r'\ballstate\b', r'\bstate\s*farm\b'],
        'Education': [r'\bschool\b', r'\bacademy\b', r'\buniversity\b', r'\bcollege\b', r'\btraining\b', r'\bcdl\b'],
        'Home Services': [r'\broofing\b', r'\bplumbing\b', r'\bhvac\b', r'\belectrical\b', r'\bpest\b', r'\bexterminat\b', r'\bcleaning\b', r'\bjunk\b', r'\blandscap\b', r'\bpainting\b', r'\bcoating\b', r'\brepair\b'],
        'Auto & Sales': [r'\bauto\s*(sales|deal)?\b', r'\bcar\s*(sales|dealer)?\b', r'\btrailer\s*sales\b', r'\bdealership\b'],
        'Beauty & Wellness': [r'\bsalon\b', r'\bspa\b', r'\bbeauty\b', r'\baesthetics?\b', r'\bhair\b', r'\bnail\b', r'\bbarber\b'],
        'Construction & Excavation': [r'\bexcavat\b', r'\bconstructi?o?n?\b', r'\bdemolition\b', r'\bcontractor\b', r'\bbuilder\b', r'\bsite\s*work\b'],
        'Furniture & Retail': [r'\bfurniture\b', r'\bmattress\b', r'\bhome\s*goods\b', r'\bappliance\b'],
        'Food & Restaurant': [r'\brestaurant\b', r'\bcatering\b', r'\bcafe\b', r'\bfood\b', r'\bdining\b', r'\bpizza\b'],
        'Fitness & Gym': [r'\bfitness\b', r'\bgym\b', r'\bpersonal\s*train\b', r'\bworkout\b', r'\bcrossfit\b'],
        'Technology & Software': [r'\bsoftware\b', r'\btech\b', r'\bsaas\b', r'\bapp\b', r'\bplatform\b', r'\bdigital\b'],
        'Professional Services': [r'\bmarketing\b', r'\bconsulting\b', r'\bphotograph\b', r'\bdesign\b', r'\bagency\b', r'\bstudio\b', r'\bstaffing\b', r'\brecruit\b'],
        'Entertainment & Media': [r'\bmedia\b', r'\bproduction\b', r'\bentertainment\b', r'\bvideo\b', r'\bcontent\b'],
        'Nonprofit & Community': [r'\bnonprofit\b', r'\bcharity\b', r'\bchurch\b', r'\bfoundation\b', r'\bcommunity\b'],
    }

    for sector, patterns in PATTERN_MAP.items():
        for p in patterns:
            if re.search(p, combined, re.IGNORECASE):
                return sector

    return None


def classify_with_openai(business_name: str, business_info: str, retries: int = 3) -> str:
    """Use OpenAI to classify business into a sector with retry logic."""
    if not OPENAI_API_KEY:
        logger.warning("No OPENAI_API_KEY configured")
        return "Other"

    for attempt in range(retries):
        try:
            headers = {
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            }

            prompt = f"""Classify this business into ONE of these sectors:

{SECTOR_DESCRIPTIONS}

Business Name: {business_name}
Business Info: {business_info[:1500]}

Respond with ONLY the sector name, nothing else. If uncertain, respond "Other"."""

            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are a business classifier. Respond with only the sector name."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0,
                "max_tokens": 50
            }

            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                sector = data['choices'][0]['message']['content'].strip()
                # Validate sector is in our list
                if sector in SECTORS:
                    return sector
                # Try partial match
                for s in SECTORS:
                    if s.lower() in sector.lower() or sector.lower() in s.lower():
                        return s
                return "Other"
            elif response.status_code == 429:
                wait_time = (2 ** attempt) * 2  # Exponential backoff
                logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait_time)
            else:
                logger.warning(f"OpenAI API error: {response.status_code}")
                return "Other"
        except Exception as e:
            logger.error(f"OpenAI classification error: {e}")
            if attempt < retries - 1:
                time.sleep(2)

    return "Other"


def classify_business(page_name: str, page_category: str = "", use_exa: bool = True) -> Dict:
    """Classify a business using pattern matching, Exa search, and OpenAI.

    Strategy:
    1. Try pattern-based classification on name alone (fast, no API)
    2. If no match and use_exa=True, search Exa for more info
    3. Try pattern-based classification on Exa results
    4. If still no match, use OpenAI classification
    """
    result = {
        'exa_sector': 'Other',
        'exa_confidence': 'low',
        'exa_source': '',
        'exa_snippet': '',
        'classification_method': 'none',
    }

    # Step 1: Try pattern-based classification on name + category
    sector = classify_with_patterns(page_name, page_category or "")
    if sector:
        result['exa_sector'] = sector
        result['exa_confidence'] = 'high'
        result['classification_method'] = 'pattern_name'
        logger.info(f"  Pattern match (name): {sector}")
        return result

    if not use_exa:
        return result

    # Step 2: Search Exa for more info
    query = f'"{page_name}" company business'
    exa_results = search_exa(query, num_results=3)

    if not exa_results:
        # Try simpler query
        query = f'{page_name} business'
        exa_results = search_exa(query, num_results=2)

    if not exa_results:
        logger.info(f"  No Exa results for: {page_name}")
        return result

    # Combine text from results
    combined_text = ""
    source_url = ""
    for r in exa_results:
        text = r.get('text', '')
        url = r.get('url', '')
        title = r.get('title', '')
        combined_text += f"\n{title}: {text}"
        if not source_url:
            source_url = url

    result['exa_source'] = source_url
    result['exa_snippet'] = combined_text[:500]

    # Step 3: Try pattern-based classification on Exa results
    sector = classify_with_patterns(page_name, combined_text)
    if sector:
        result['exa_sector'] = sector
        result['exa_confidence'] = 'high'
        result['classification_method'] = 'pattern_exa'
        logger.info(f"  Pattern match (Exa): {sector}")
        return result

    # Step 4: Use OpenAI classification
    sector = classify_with_openai(page_name, combined_text)
    result['exa_sector'] = sector
    result['classification_method'] = 'openai'

    # Determine confidence
    if sector != "Other" and len(combined_text) > 200:
        result['exa_confidence'] = 'high'
        logger.info(f"  OpenAI classification: {sector}")
    elif sector != "Other":
        result['exa_confidence'] = 'medium'
        logger.info(f"  OpenAI classification (medium): {sector}")
    else:
        result['exa_confidence'] = 'low'
        logger.info(f"  No classification found")

    return result


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Classify advertisers into sectors using Exa')
    parser.add_argument('--limit', type=int, help='Limit number of advertisers to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--all', action='store_true', help='Process all advertisers, not just "Other"')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("SECTOR CLASSIFIER (Exa + OpenAI)")
    print(f"{'='*60}\n")

    # Load master CSV
    if not MASTER_CSV.exists():
        logger.error(f"Master CSV not found: {MASTER_CSV}")
        return 1

    df = pd.read_csv(MASTER_CSV)
    logger.info(f"Loaded {len(df)} advertisers from master CSV")

    # Filter to "Other" sector (or all if --all)
    if args.all:
        to_classify = df.copy()
    else:
        to_classify = df[df['sector'] == 'Other'].copy()

    # Further filter to those that passed gate (more valuable)
    to_classify = to_classify[to_classify['gate_passed'] == True]

    logger.info(f"Advertisers to classify: {len(to_classify)}")

    if args.limit:
        to_classify = to_classify.head(args.limit)
        logger.info(f"Limited to {len(to_classify)} advertisers")

    if args.dry_run:
        print("\nDRY RUN - Would classify these advertisers:")
        for _, row in to_classify.iterrows():
            print(f"  - {row['page_name']}")
        return 0

    # Process each advertiser
    results = []
    for idx, row in tqdm(to_classify.iterrows(), total=len(to_classify), desc="Classifying"):
        page_name = row['page_name']
        page_category = row.get('page_category', '')

        logger.info(f"Classifying: {page_name}")

        classification = classify_business(page_name, page_category)

        results.append({
            'page_id': row['page_id'],
            'page_name': page_name,
            'old_sector': row['sector'],
            'new_sector': classification['exa_sector'],
            'confidence': classification['exa_confidence'],
            'method': classification.get('classification_method', 'unknown'),
            'source': classification['exa_source'],
            'snippet': classification['exa_snippet'][:200],
        })

        # Log result
        if classification['exa_sector'] != row['sector']:
            logger.info(f"  RECLASSIFIED: {row['sector']} -> {classification['exa_sector']}")

        # Rate limiting (longer for API calls)
        if classification.get('classification_method') in ['pattern_exa', 'openai']:
            time.sleep(1.5)  # Longer delay for API calls
        else:
            time.sleep(0.3)  # Short delay for pattern-only

    # Create results DataFrame
    results_df = pd.DataFrame(results)

    # Save classification results
    results_path = BASE_DIR / 'output' / 'icp_exploration' / 'sector_classifications.csv'
    results_df.to_csv(results_path, index=False)
    logger.info(f"Saved classification results to: {results_path}")

    # Update master CSV with new sectors
    for _, result in results_df.iterrows():
        if result['new_sector'] != 'Other' and result['confidence'] in ['high', 'medium']:
            mask = df['page_id'] == result['page_id']
            df.loc[mask, 'sector'] = result['new_sector']

    # Save updated master
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info(f"Updated master CSV: {OUTPUT_CSV}")

    # Summary
    print(f"\n{'='*60}")
    print("CLASSIFICATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total processed: {len(results_df)}")

    reclassified = results_df[results_df['old_sector'] != results_df['new_sector']]
    print(f"Reclassified: {len(reclassified)}")

    print(f"\nNew sector distribution:")
    for sector, count in results_df['new_sector'].value_counts().items():
        print(f"  {sector}: {count}")

    print(f"\nReclassifications (high/medium confidence):")
    for _, row in reclassified[reclassified['confidence'].isin(['high', 'medium'])].iterrows():
        print(f"  {row['page_name'][:40]}: {row['old_sector']} -> {row['new_sector']}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
