#!/usr/bin/env python3
"""
Convert Repliers top agents data to master file format.

This script takes the aggregated top agents CSV and converts it to the
master file schema with source tracking columns.

Usage:
    python scripts/repliers_to_master.py --input output/repliers/top_agents_2025.csv
    python scripts/repliers_to_master.py --input output/repliers/top_agents_2025.csv --output processed/repliers_prospects.csv
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_to_master.log')
    ]
)
logger = logging.getLogger(__name__)


def convert_to_master_format(input_path: str, output_path: str = None) -> pd.DataFrame:
    """
    Convert Repliers top agents data to master file format.

    Args:
        input_path: Path to top agents CSV
        output_path: Optional output path (if not provided, uses processed/ folder)

    Returns:
        Converted DataFrame
    """
    logger.info(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} agents")

    # Create master format DataFrame
    master = pd.DataFrame()

    # Map fields (per plan)
    master['contact_name'] = df['agent_name']
    master['primary_email'] = df['agent_email']
    master['primary_phone'] = df['agent_phone']
    master['company_name'] = df['brokerage']
    master['page_name'] = df['brokerage']  # Use brokerage as page_name

    # Add Repliers-specific fields (new columns for master)
    master['repliers_tx_count'] = df['transaction_count']
    master['repliers_total_volume'] = df['total_volume']
    master['repliers_avg_price'] = df['avg_price']
    master['repliers_median_price'] = df['median_price']
    master['repliers_highest_sale'] = df['highest_sale']
    master['repliers_lowest_sale'] = df['lowest_sale']
    master['repliers_cities'] = df['cities']
    master['repliers_sample_addresses'] = df['sample_addresses']
    master['repliers_agent_id'] = df['agent_id']

    # Add source tracking columns
    master['source'] = 'repliers'
    master['scrape_type'] = 'sold_property_scrape_2025'
    master['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

    # Add placeholder fields for standard master schema
    master['website'] = ''
    master['instagram_handle'] = ''
    master['linkedin_profile'] = ''
    master['linkedin_headline'] = ''
    master['lead_score'] = None
    master['lead_tier'] = None

    # Generate output path
    if output_path is None:
        Path('processed').mkdir(exist_ok=True)
        output_path = 'processed/repliers_prospects.csv'

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save
    master.to_csv(output_path, index=False)
    logger.info(f"Saved {len(master)} prospects to: {output_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Total prospects: {len(master)}")
    print(f"With email: {master['primary_email'].notna().sum()}")
    print(f"With phone: {master['primary_phone'].notna().sum()}")
    print(f"With brokerage: {master['company_name'].notna().sum()}")
    print(f"\nTotal transaction volume: ${master['repliers_total_volume'].sum():,.0f}")
    print(f"Total transactions: {master['repliers_tx_count'].sum()}")
    print(f"\nOutput: {output_path}")

    return master


def main():
    parser = argparse.ArgumentParser(description='Convert Repliers data to master format')
    parser.add_argument('--input', type=str, required=True,
                        help='Input top agents CSV from aggregator')
    parser.add_argument('--output', type=str,
                        help='Output path (default: processed/repliers_prospects.csv)')

    args = parser.parse_args()

    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    convert_to_master_format(args.input, args.output)


if __name__ == '__main__':
    main()
