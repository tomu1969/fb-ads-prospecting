#!/usr/bin/env python3
"""
Repliers MLS API Scraper - Scrape sold/closed transactions with agent data from MLS.

Usage:
    # Sold listings (2025 transactions)
    python scripts/repliers_mls_scraper.py --city Miami --type sale --sold --limit 500
    python scripts/repliers_mls_scraper.py --city Miami --type sale --sold --all

    # Active listings
    python scripts/repliers_mls_scraper.py --city Miami --type sale --limit 100
    python scripts/repliers_mls_scraper.py --city Miami --type lease --all

    # With date filtering (post-hoc filter for 2025)
    python scripts/repliers_mls_scraper.py --city Miami --type sale --sold --min-sold-date 2025-01-01 --all
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

API_BASE_URL = "https://api.repliers.io"


class RepliersScraper:
    """Scraper for Repliers MLS API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'REPLIERS-API-KEY': api_key
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def search_listings(
        self,
        city: str,
        listing_type: str = "sale",
        status: str = "A",
        last_status: Optional[str] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_bedrooms: Optional[int] = None,
        max_bedrooms: Optional[int] = None,
        property_type: Optional[str] = None,
        page: int = 1,
        results_per_page: int = 100,
        min_sold_date: Optional[str] = None,
        max_sold_date: Optional[str] = None,
    ) -> dict:
        """
        Search MLS listings with filters.

        Args:
            city: City name (e.g., "Miami")
            listing_type: "lease" for rentals, "sale" for purchases
            status: "A" for active, "U" for unavailable/sold
            last_status: "Sld" for sold listings (used with status=U)
            min_price: Minimum price
            max_price: Maximum price
            min_bedrooms: Minimum bedrooms
            max_bedrooms: Maximum bedrooms
            property_type: Property type filter
            page: Page number (1-indexed)
            results_per_page: Results per page (max 100)
            min_sold_date: Minimum sold date (YYYY-MM-DD) - API may not support this
            max_sold_date: Maximum sold date (YYYY-MM-DD) - API may not support this

        Returns:
            API response dict with listings and pagination info
        """
        params = {
            'city': city,
            'type': listing_type,
            'status': status,
            'pageNum': page,  # API uses pageNum, not page
            'resultsPerPage': min(results_per_page, 100),
        }

        # Add lastStatus for sold listings
        if last_status:
            params['lastStatus'] = last_status

        # Add optional filters
        if min_price:
            params['minPrice'] = min_price
        if max_price:
            params['maxPrice'] = max_price
        if min_bedrooms:
            params['minBedrooms'] = min_bedrooms
        if max_bedrooms:
            params['maxBedrooms'] = max_bedrooms
        if property_type:
            params['propertyType'] = property_type

        # Date filters (API may or may not support these)
        if min_sold_date:
            params['minSoldDate'] = min_sold_date
        if max_sold_date:
            params['maxSoldDate'] = max_sold_date

        url = f"{API_BASE_URL}/listings"

        logger.debug(f"Request: GET {url} params={params}")
        response = self.session.get(url, params=params)  # Use GET, not POST

        if response.status_code == 401:
            raise ValueError("Invalid API key. Check your REPLIERS_API_KEY in .env")
        elif response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text}")

        return response.json()

    def get_all_listings(
        self,
        city: str,
        listing_type: str = "sale",
        limit: Optional[int] = None,
        **kwargs
    ) -> list:
        """
        Get all listings with pagination.

        Args:
            city: City name
            listing_type: "lease" or "sale"
            limit: Maximum total listings to retrieve (None for all)
            **kwargs: Additional filters passed to search_listings

        Returns:
            List of all listings
        """
        all_listings = []
        page = 1

        while True:
            logger.info(f"Fetching page {page}...")

            result = self.search_listings(
                city=city,
                listing_type=listing_type,
                page=page,
                **kwargs
            )

            listings = result.get('listings', [])
            total_count = result.get('count', 0)
            num_pages = result.get('numPages', 1)

            if page == 1:
                logger.info(f"Total listings available: {total_count} across {num_pages} pages")

            if not listings:
                break

            all_listings.extend(listings)
            logger.info(f"  Page {page}/{num_pages}: Got {len(listings)} listings (total: {len(all_listings)})")

            # Check if we've hit the limit
            if limit and len(all_listings) >= limit:
                all_listings = all_listings[:limit]
                logger.info(f"Reached limit of {limit} listings")
                break

            # Check if we've fetched all pages
            if page >= num_pages:
                break

            page += 1

        return all_listings

    def get_agent(self, agent_id: str) -> dict:
        """Get agent details by ID."""
        url = f"{API_BASE_URL}/agents/{agent_id}"
        response = self.session.get(url)

        if response.status_code != 200:
            logger.warning(f"Failed to get agent {agent_id}: {response.status_code}")
            return {}

        return response.json()

    def extract_listing_data(self, listing: dict, is_sold: bool = False) -> dict:
        """
        Extract relevant fields from a listing.

        Args:
            listing: Raw listing dict from API
            is_sold: Whether this is a sold listing (extracts soldPrice, soldDate)
        """
        address = listing.get('address', {})
        details = listing.get('details', {})

        # Build full address
        street_parts = [
            address.get('streetDirectionPrefix', ''),
            address.get('streetNumber', ''),
            address.get('streetName', ''),
            address.get('streetSuffix', ''),
        ]
        street_address = ' '.join(p for p in street_parts if p).strip()
        if address.get('unitNumber'):
            street_address += f" #{address.get('unitNumber')}"

        # Basic listing info
        data = {
            'mls_number': listing.get('mlsNumber'),
            'status': listing.get('status'),
            'last_status': listing.get('lastStatus'),
            'listing_type': listing.get('type'),
            'property_type': listing.get('class'),
            'list_price': listing.get('listPrice'),

            # Address
            'address': street_address,
            'city': address.get('city'),
            'state': address.get('state'),
            'zip_code': address.get('zip'),
            'neighborhood': address.get('neighborhood'),
            'area': address.get('area'),

            # Property details
            'bedrooms': details.get('numBedrooms'),
            'bathrooms': details.get('numBathrooms'),
            'sqft': details.get('sqft'),
            'year_built': details.get('yearBuilt'),
            'style': details.get('style'),
            'furnished': details.get('furnished'),

            # Dates
            'list_date': listing.get('listDate'),
            'days_on_market': listing.get('daysOnMarket'),

            # Description
            'description': details.get('description'),

            # Photos
            'photo_count': listing.get('photoCount', 0),
            'virtual_tour': details.get('virtualTourUrl'),

            # Location
            'latitude': listing.get('map', {}).get('latitude'),
            'longitude': listing.get('map', {}).get('longitude'),
        }

        # Sold-specific fields
        if is_sold:
            # Try multiple possible field names for sold price
            sold_price = (
                listing.get('soldPrice') or
                listing.get('closePrice') or
                listing.get('lastPrice') or
                details.get('soldPrice') or
                details.get('closePrice')
            )
            data['sold_price'] = sold_price

            # Try multiple possible field names for sold date
            sold_date = (
                listing.get('soldDate') or
                listing.get('closeDate') or
                listing.get('unavailableDate') or
                details.get('soldDate') or
                details.get('closeDate')
            )
            data['sold_date'] = sold_date

            # Add timestamps info
            timestamps = listing.get('timestamps', {})
            if timestamps:
                data['sold_date'] = data['sold_date'] or timestamps.get('soldDate') or timestamps.get('unavailableDate')
                data['list_date'] = data['list_date'] or timestamps.get('listDate')

        # Agent info (from agents array)
        agents = listing.get('agents', [])
        if agents:
            primary_agent = agents[0]
            phones = primary_agent.get('phones', [])
            brokerage = primary_agent.get('brokerage', {})

            data.update({
                'agent_name': primary_agent.get('name'),
                'agent_email': primary_agent.get('email'),
                'agent_phone': phones[0] if phones else None,
                'agent_phone2': phones[1] if len(phones) > 1 else None,
                'agent_id': primary_agent.get('agentId'),
                'brokerage': brokerage.get('name') if brokerage else None,
            })

            # If multiple agents, capture secondary
            if len(agents) > 1:
                data['agent2_name'] = agents[1].get('name')
                data['agent2_email'] = agents[1].get('email')
                agent2_phones = agents[1].get('phones', [])
                data['agent2_phone'] = agent2_phones[0] if agent2_phones else None
        else:
            # Fallback to office data
            office = listing.get('office', {})
            data.update({
                'agent_name': None,
                'agent_email': None,
                'agent_phone': None,
                'agent_id': None,
                'brokerage': office.get('brokerageName'),
            })

        return data


def save_sample_response(listings: list, output_dir: str = "output/repliers"):
    """Save a sample raw API response for debugging."""
    if listings:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        sample_path = f"{output_dir}/sample_response.json"
        with open(sample_path, 'w') as f:
            json.dump(listings[:3], f, indent=2, default=str)
        logger.info(f"Saved sample response to: {sample_path}")


def main():
    parser = argparse.ArgumentParser(description='Scrape MLS listings from Repliers API')
    parser.add_argument('--city', type=str, default='Miami', help='City to search')
    parser.add_argument('--type', type=str, default='sale', choices=['lease', 'sale'],
                        help='Listing type: lease (rentals) or sale')
    parser.add_argument('--sold', action='store_true',
                        help='Fetch sold/closed listings instead of active')
    parser.add_argument('--limit', type=int, help='Max listings to retrieve')
    parser.add_argument('--all', action='store_true', help='Get all listings (no limit)')
    parser.add_argument('--min-price', type=int, help='Minimum price')
    parser.add_argument('--max-price', type=int, help='Maximum price')
    parser.add_argument('--min-beds', type=int, help='Minimum bedrooms')
    parser.add_argument('--min-sold-date', type=str,
                        help='Minimum sold date (YYYY-MM-DD) for post-hoc filtering')
    parser.add_argument('--max-sold-date', type=str,
                        help='Maximum sold date (YYYY-MM-DD) for post-hoc filtering')
    parser.add_argument('--output', type=str, help='Output CSV path')
    parser.add_argument('--test', action='store_true', help='Test mode: fetch 10 listings')
    parser.add_argument('--api-key', type=str, help='API key (overrides env variable)')
    parser.add_argument('--save-raw', action='store_true',
                        help='Save sample raw API response for debugging')

    args = parser.parse_args()

    # Get API key (command line takes precedence)
    api_key = args.api_key or os.getenv('REPLIERS_API_KEY')
    if not api_key:
        logger.error("REPLIERS_API_KEY not found in environment")
        logger.error("Add to .env: REPLIERS_API_KEY=your_key_here")
        sys.exit(1)

    # Initialize scraper
    scraper = RepliersScraper(api_key)

    # Set limit
    if args.test:
        limit = 10
    elif args.all:
        limit = None
    else:
        limit = args.limit or 100

    # Build status parameters for sold listings
    status = "U" if args.sold else "A"
    last_status = "Sld" if args.sold else None
    listing_desc = "sold" if args.sold else "active"

    logger.info(f"Scraping {listing_desc} {args.type} listings in {args.city}...")
    if limit:
        logger.info(f"Limit: {limit} listings")
    else:
        logger.info("Fetching ALL listings (no limit)")

    # Fetch listings
    try:
        listings = scraper.get_all_listings(
            city=args.city,
            listing_type=args.type,
            status=status,
            last_status=last_status,
            limit=limit,
            min_price=args.min_price,
            max_price=args.max_price,
            min_bedrooms=args.min_beds,
        )
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to fetch listings: {e}")
        sys.exit(1)

    if not listings:
        logger.warning("No listings found")
        sys.exit(0)

    # Save raw sample if requested
    if args.save_raw:
        save_sample_response(listings)

    logger.info(f"Processing {len(listings)} listings...")

    # Extract data
    data = [scraper.extract_listing_data(l, is_sold=args.sold) for l in listings]
    df = pd.DataFrame(data)

    # Post-hoc date filtering (if API doesn't support date params)
    original_count = len(df)
    if args.sold and args.min_sold_date:
        if 'sold_date' in df.columns and df['sold_date'].notna().any():
            # Try to parse and filter by date
            try:
                df['sold_date_parsed'] = pd.to_datetime(df['sold_date'], errors='coerce')
                min_date = pd.to_datetime(args.min_sold_date)
                df = df[df['sold_date_parsed'] >= min_date]
                df = df.drop(columns=['sold_date_parsed'])
                logger.info(f"Filtered to sold_date >= {args.min_sold_date}: {original_count} -> {len(df)} listings")
            except Exception as e:
                logger.warning(f"Could not filter by sold date: {e}")
        else:
            logger.warning("sold_date column is empty or missing - cannot filter by date")

    if args.sold and args.max_sold_date:
        if 'sold_date' in df.columns and df['sold_date'].notna().any():
            try:
                df['sold_date_parsed'] = pd.to_datetime(df['sold_date'], errors='coerce')
                max_date = pd.to_datetime(args.max_sold_date)
                df = df[df['sold_date_parsed'] <= max_date]
                df = df.drop(columns=['sold_date_parsed'])
                logger.info(f"Filtered to sold_date <= {args.max_sold_date}: {len(df)} listings remain")
            except Exception as e:
                logger.warning(f"Could not filter by max sold date: {e}")

    # Generate output path
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sold_suffix = "_sold" if args.sold else ""
        output_dir = "output/repliers"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        output_path = f"{output_dir}/mls_{args.city.lower().replace(' ', '_')}_{args.type}{sold_suffix}_{timestamp}.csv"

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} listings to: {output_path}")

    # Print summary
    print("\n=== Summary ===")
    print(f"Total listings: {len(df)}")
    print(f"With agent name: {df['agent_name'].notna().sum()}")
    print(f"With agent email: {df['agent_email'].notna().sum()}")
    print(f"With agent phone: {df['agent_phone'].notna().sum()}")
    print(f"Unique agents: {df['agent_name'].nunique()}")
    print(f"Unique brokerages: {df['brokerage'].nunique()}")

    if args.sold:
        print(f"\n=== Sold Listings Info ===")
        print(f"With sold_price: {df['sold_price'].notna().sum()}")
        print(f"With sold_date: {df['sold_date'].notna().sum()}")
        if df['sold_price'].notna().any():
            print(f"Price range: ${df['sold_price'].min():,.0f} - ${df['sold_price'].max():,.0f}")
            print(f"Average price: ${df['sold_price'].mean():,.0f}")
        if df['sold_date'].notna().any():
            print(f"Date range: {df['sold_date'].min()} to {df['sold_date'].max()}")

    if df['agent_name'].notna().any():
        print("\n=== Top Agents by Listings ===")
        top_agents = df['agent_name'].value_counts().head(20)
        for agent, count in top_agents.items():
            print(f"  {count:3d} listings: {agent}")

    return df


if __name__ == '__main__':
    main()
