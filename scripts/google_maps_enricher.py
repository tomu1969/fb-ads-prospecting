"""
Google Maps Enricher - Module 3.10 - Lead Volume & Local Presence Signals

Enriches contacts with Google Business Profile data as a proxy for lead volume
and local market presence. High review counts correlate with high lead volume.

Input: processed/03f_linkedin.csv (from Module 3.9)
Output: processed/03g_gmaps.csv

Data extracted:
- gmaps_rating: Star rating (1.0-5.0)
- gmaps_review_count: Total reviews (lead volume proxy)
- gmaps_place_id: Google Place ID (deduplication)
- gmaps_business_status: OPERATIONAL, CLOSED, etc.
- gmaps_url: Google Maps URL
- gmaps_phone: Listed phone number
- gmaps_address: Business address

Usage:
    python scripts/google_maps_enricher.py           # Test mode (3 contacts)
    python scripts/google_maps_enricher.py --all     # Process all contacts
    python scripts/google_maps_enricher.py --csv output/prospects.csv  # Standalone
"""

import os
import sys
import re
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from difflib import SequenceMatcher
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('google_maps_enricher.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent

# Pipeline input/output paths
INPUT_BASE = "03f_linkedin.csv"
OUTPUT_BASE = "03g_gmaps.csv"

# Apify configuration (check both common env var names)
APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')
GOOGLE_MAPS_ACTOR = "compass/crawler-google-places"  # Primary actor

# Try to import Apify client
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    ApifyClient = None
    logger.warning("apify-client not installed. Install with: pip install apify-client")


def get_apify_client() -> Optional[Any]:
    """Get Apify client if available."""
    if not APIFY_AVAILABLE:
        return None
    if not APIFY_API_TOKEN:
        return None
    try:
        return ApifyClient(APIFY_API_TOKEN)
    except Exception as e:
        logger.error(f"Failed to create Apify client: {e}")
        return None


def extract_domain(url: str) -> str:
    """Extract domain from URL for matching."""
    if not url:
        return ""
    try:
        parsed = urlparse(url if url.startswith('http') else f'https://{url}')
        domain = parsed.netloc.lower()
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity ratio between two names (0.0 to 1.0)."""
    if not name1 or not name2:
        return 0.0
    return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()


def clean_company_name_for_search(name: str) -> str:
    """Clean company name for Google Maps search."""
    if not name:
        return name

    # Remove common suffixes that don't help search
    suffixes_to_remove = [
        r'\s*,?\s*(llc|inc|corp|co|ltd|group|team)\.?\s*$',
        r'\s*-\s*(real estate|realty|properties).*$',
    ]

    cleaned = name
    for pattern in suffixes_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    return cleaned.strip()


def search_google_maps_apify(
    company_name: str,
    location: str = None,
    limit: int = 3
) -> List[Dict]:
    """
    Search Google Maps using Apify actor.

    Args:
        company_name: Business name to search
        location: Optional location to narrow search
        limit: Max results to return

    Returns:
        List of place results
    """
    client = get_apify_client()
    if not client:
        logger.warning("Apify client not available")
        return []

    # Build search query
    search_query = clean_company_name_for_search(company_name)
    if location:
        search_query = f"{search_query} {location}"

    try:
        # Run the Google Maps actor
        run_input = {
            "searchStringsArray": [search_query],
            "maxCrawledPlacesPerSearch": limit,
            "language": "en",
            "maxReviews": 0,  # Don't fetch individual reviews, just counts
            "maxImages": 0,   # Don't fetch images
            "scrapeDirectories": False,
        }

        logger.debug(f"Searching Google Maps for: {search_query}")

        run = client.actor(GOOGLE_MAPS_ACTOR).call(run_input=run_input)

        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # Debug: log first result structure
            if not results:
                logger.debug(f"Sample result keys: {list(item.keys())[:20]}")

            results.append({
                "name": item.get("title", ""),
                "rating": item.get("totalScore"),
                "review_count": item.get("reviewsCount") or item.get("reviews") or 0,
                "place_id": item.get("placeId", ""),
                "website": item.get("website", ""),
                "phone": item.get("phone", ""),
                "address": item.get("address", ""),
                "business_status": "CLOSED" if item.get("permanentlyClosed") else "OPERATIONAL",
                "categories": item.get("categories", []),
                "url": item.get("url", ""),
            })

        return results

    except Exception as e:
        logger.error(f"Apify Google Maps search error: {e}")
        return []


def validate_match(
    result: Dict,
    company_name: str,
    website_url: str = None
) -> Dict:
    """
    Validate if a Google Maps result matches our company.

    Returns result dict with 'match_confidence' score (0.0 to 1.0).
    """
    confidence = 0.0
    match_reasons = []

    result_name = result.get("name", "")
    result_website = result.get("website", "")

    # 1. Name similarity check (max 0.5)
    name_sim = name_similarity(company_name, result_name)
    if name_sim >= 0.9:
        confidence += 0.5
        match_reasons.append(f"name_exact:{name_sim:.2f}")
    elif name_sim >= 0.7:
        confidence += 0.35
        match_reasons.append(f"name_similar:{name_sim:.2f}")
    elif name_sim >= 0.5:
        confidence += 0.2
        match_reasons.append(f"name_partial:{name_sim:.2f}")

    # 2. Website domain match (max 0.4)
    if website_url and result_website:
        our_domain = extract_domain(website_url)
        their_domain = extract_domain(result_website)

        if our_domain and their_domain:
            if our_domain == their_domain:
                confidence += 0.4
                match_reasons.append("website_exact")
            elif our_domain in their_domain or their_domain in our_domain:
                confidence += 0.25
                match_reasons.append("website_partial")

    # 3. Has reviews (indicates real business) (max 0.1)
    review_count = result.get("review_count") or 0
    if review_count > 0:
        confidence += 0.1
        match_reasons.append("has_reviews")

    result["match_confidence"] = min(confidence, 1.0)
    result["match_reasons"] = "|".join(match_reasons)

    return result


def find_best_match(
    results: List[Dict],
    company_name: str,
    website_url: str = None,
    min_confidence: float = 0.5
) -> Optional[Dict]:
    """
    Find the best matching result from Google Maps search.

    Args:
        results: List of search results
        company_name: Company name to match
        website_url: Company website for validation
        min_confidence: Minimum confidence threshold

    Returns:
        Best matching result or None
    """
    if not results:
        return None

    # Validate and score each result
    scored_results = []
    for result in results:
        validated = validate_match(result, company_name, website_url)
        if validated["match_confidence"] >= min_confidence:
            scored_results.append(validated)

    if not scored_results:
        return None

    # Sort by confidence descending
    scored_results.sort(key=lambda x: x["match_confidence"], reverse=True)

    best = scored_results[0]
    logger.debug(f"Best match for '{company_name}': {best['name']} (confidence: {best['match_confidence']:.2f})")

    return best


def enrich_with_google_maps(
    row: Dict,
    delay: float = 1.0
) -> Dict:
    """
    Enrich a single contact with Google Maps data.

    Args:
        row: Contact row as dict
        delay: Delay before API call (for rate limiting)

    Returns:
        Dict with gmaps_* fields
    """
    company_name = str(row.get("page_name", "")).strip()
    website_url = str(row.get("website_url", "")).strip()

    # Default empty result
    empty_result = {
        "gmaps_rating": None,
        "gmaps_review_count": None,
        "gmaps_place_id": "",
        "gmaps_business_status": "",
        "gmaps_url": "",
        "gmaps_phone": "",
        "gmaps_address": "",
        "gmaps_match_confidence": 0.0,
    }

    if not company_name:
        return empty_result

    # Rate limiting
    time.sleep(delay)

    # Search Google Maps
    results = search_google_maps_apify(company_name, limit=5)

    if not results:
        logger.debug(f"No results for: {company_name}")
        return empty_result

    # Find best match
    best_match = find_best_match(results, company_name, website_url, min_confidence=0.5)

    if not best_match:
        logger.debug(f"No confident match for: {company_name}")
        return empty_result

    # Extract data from best match - ensure proper types
    return {
        "gmaps_rating": best_match.get("rating") if best_match.get("rating") is not None else None,
        "gmaps_review_count": int(best_match.get("review_count") or 0),
        "gmaps_place_id": str(best_match.get("place_id") or ""),
        "gmaps_business_status": str(best_match.get("business_status") or ""),
        "gmaps_url": str(best_match.get("url") or ""),
        "gmaps_phone": str(best_match.get("phone") or ""),
        "gmaps_address": str(best_match.get("address") or ""),
        "gmaps_match_confidence": float(best_match.get("match_confidence") or 0.0),
    }


def enrich_google_maps(
    csv_path: Path,
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    delay: float = 1.0
) -> Dict:
    """
    Enrich CSV with Google Maps business data.

    Args:
        csv_path: Input CSV path
        output_path: Output CSV path
        limit: Max contacts to process
        dry_run: Preview without API calls
        delay: Delay between API calls

    Returns:
        Stats dictionary
    """
    # Load CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return {"error": str(e)}

    if "page_name" not in df.columns:
        logger.error("CSV must have 'page_name' column")
        return {"error": "Missing required column: page_name"}

    # Initialize gmaps columns if not exist
    gmaps_columns = [
        "gmaps_rating", "gmaps_review_count", "gmaps_place_id",
        "gmaps_business_status", "gmaps_url", "gmaps_phone",
        "gmaps_address", "gmaps_match_confidence"
    ]
    for col in gmaps_columns:
        if col not in df.columns:
            df[col] = None if col in ["gmaps_rating", "gmaps_review_count", "gmaps_match_confidence"] else ""

    # Stats
    stats = {
        "total": len(df),
        "processed": 0,
        "found": 0,
        "skipped": 0,
        "errors": 0,
        "already_had": 0,
    }

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if already has Google Maps data
        if pd.notna(row.get("gmaps_place_id")) and str(row.get("gmaps_place_id")).strip():
            stats["already_had"] += 1
            continue

        company_name = str(row.get("page_name", "")).strip()
        if not company_name or company_name.lower() in ["nan", "none", ""]:
            stats["skipped"] += 1
            continue

        rows_to_process.append((idx, row.to_dict()))

    # Apply limit
    if limit:
        rows_to_process = rows_to_process[:limit]

    logger.info(f"Processing {len(rows_to_process)} contacts for Google Maps data")

    if dry_run:
        logger.info("DRY RUN - No API calls will be made")
        for idx, row in rows_to_process[:10]:
            logger.info(f"  Would search: \"{row.get('page_name')}\"")
        return stats

    # Process each contact
    for idx, row in tqdm(rows_to_process, desc="Enriching with Google Maps"):
        try:
            gmaps_data = enrich_with_google_maps(row, delay=delay)

            # Update dataframe
            for key, value in gmaps_data.items():
                df.at[idx, key] = value

            if gmaps_data.get("gmaps_place_id"):
                stats["found"] += 1

            stats["processed"] += 1

        except Exception as e:
            logger.error(f"Error processing {row.get('page_name')}: {e}")
            stats["errors"] += 1

    # Save output
    if output_path is None:
        output_path = csv_path.parent / f"{csv_path.stem}_gmaps{csv_path.suffix}"

    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved to {output_path}")

    return stats


def print_summary(stats: Dict):
    """Print enrichment summary."""
    print("\n" + "=" * 60)
    print("GOOGLE MAPS ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"Total contacts:       {stats.get('total', 0)}")
    print(f"Already had data:     {stats.get('already_had', 0)}")
    print(f"Processed:            {stats.get('processed', 0)}")
    print(f"Found on Google Maps: {stats.get('found', 0)}")
    print(f"Skipped (no name):    {stats.get('skipped', 0)}")
    print(f"Errors:               {stats.get('errors', 0)}")
    print("=" * 60)


def main():
    """Main function with pipeline and standalone mode support."""

    print(f"\n{'='*60}")
    print("MODULE 3.10: GOOGLE MAPS ENRICHER")
    print(f"{'='*60}")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Enrich contacts with Google Maps business data',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--csv', type=str,
                       help='Input CSV file (standalone mode)')
    parser.add_argument('--output', type=str,
                       help='Output CSV path')
    parser.add_argument('--all', action='store_true',
                       help='Process all contacts (default: test mode with 3)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of contacts to process')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making API calls')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between API calls in seconds (default: 1.0)')

    args = parser.parse_args()

    # Determine input file
    run_id = get_run_id_from_env()

    if args.csv:
        # Standalone mode
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv
        standalone_mode = True
    else:
        # Pipeline mode
        if run_id:
            input_name = get_versioned_filename(INPUT_BASE, run_id)
            csv_path = BASE_DIR / "processed" / input_name
        else:
            csv_path = BASE_DIR / "processed" / INPUT_BASE

        # Try latest symlink if versioned doesn't exist
        if not csv_path.exists():
            latest_path = BASE_DIR / "processed" / INPUT_BASE
            if latest_path.exists() or latest_path.is_symlink():
                csv_path = latest_path

        # Fallback: try 03f_linkedin.csv without version prefix
        if not csv_path.exists():
            for fallback in ["03f_linkedin.csv", "03e_names.csv", "03d_final.csv"]:
                fallback_path = BASE_DIR / "processed" / fallback
                if fallback_path.exists():
                    csv_path = fallback_path
                    logger.info(f"Using fallback input: {fallback_path}")
                    break

        standalone_mode = False

    if not csv_path.exists():
        print(f"ERROR: Input file not found: {csv_path}")
        if not standalone_mode:
            print("Make sure Module 3.9 (LinkedIn Enricher) has run first.")
        return 1

    # Determine output file
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    elif standalone_mode:
        output_path = csv_path.parent / f"{csv_path.stem}_gmaps{csv_path.suffix}"
    else:
        if run_id:
            output_name = get_versioned_filename(OUTPUT_BASE, run_id)
            output_path = BASE_DIR / "processed" / output_name
        else:
            output_path = BASE_DIR / "processed" / OUTPUT_BASE

    # Determine limit
    if args.limit:
        limit = args.limit
    elif args.all:
        limit = None
    else:
        limit = 3
        print("Test mode: Processing first 3 contacts")
        print("(Use --all to process all contacts)")

    # Check API token
    if not APIFY_API_TOKEN and not args.dry_run:
        print("ERROR: APIFY_API_TOKEN not found in environment")
        print("Add it to your .env file")
        return 1

    if not APIFY_AVAILABLE and not args.dry_run:
        print("ERROR: apify-client not installed")
        print("Install with: pip install apify-client")
        return 1

    print(f"\nInput:  {csv_path}")
    print(f"Output: {output_path}")
    if args.dry_run:
        print("Mode: DRY RUN")

    # Run enrichment
    stats = enrich_google_maps(
        csv_path=csv_path,
        output_path=output_path,
        limit=limit,
        dry_run=args.dry_run,
        delay=args.delay
    )

    # Create latest symlink in pipeline mode
    if not standalone_mode and run_id and output_path.exists():
        latest_path = create_latest_symlink(output_path, OUTPUT_BASE)
        if latest_path:
            print(f"Latest symlink: {latest_path}")

    print_summary(stats)
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
