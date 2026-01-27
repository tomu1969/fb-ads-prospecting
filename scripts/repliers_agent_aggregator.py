#!/usr/bin/env python3
"""
Repliers Agent Aggregator - Aggregate sold transactions by agent and identify top performers.

This script takes sold transaction data from the Repliers MLS scraper and:
1. Aggregates transactions by agent
2. Calculates statistics (transaction count, total volume, avg price)
3. Filters to high-volume agents (10+ transactions)
4. Exports a prospect list sorted by total volume

Usage:
    # Aggregate from single file
    python scripts/repliers_agent_aggregator.py --input output/repliers/mls_miami_sale_sold_*.csv

    # Aggregate from multiple files (glob pattern)
    python scripts/repliers_agent_aggregator.py --input "output/repliers/mls_*_sale_sold_*.csv"

    # Aggregate all sold listings in repliers folder
    python scripts/repliers_agent_aggregator.py --input-dir output/repliers --pattern "*_sold_*.csv"

    # Filter to 2025 transactions only
    python scripts/repliers_agent_aggregator.py --input output/repliers/*.csv --year 2025

    # Custom transaction threshold
    python scripts/repliers_agent_aggregator.py --input output/repliers/*.csv --min-transactions 5
"""

import argparse
import glob
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('repliers_aggregator.log')
    ]
)
logger = logging.getLogger(__name__)


def load_transaction_files(
    input_files: Optional[List[str]] = None,
    input_dir: Optional[str] = None,
    pattern: str = "*_sold_*.csv"
) -> pd.DataFrame:
    """
    Load and merge transaction data from multiple CSV files.

    Args:
        input_files: List of CSV file paths (can include glob patterns)
        input_dir: Directory to search for files
        pattern: Glob pattern to match files in input_dir

    Returns:
        Combined DataFrame of all transactions
    """
    files_to_load = []

    # Collect files from input_files (resolve glob patterns)
    if input_files:
        for file_pattern in input_files:
            matched = glob.glob(file_pattern)
            if matched:
                files_to_load.extend(matched)
            else:
                logger.warning(f"No files matched pattern: {file_pattern}")

    # Collect files from input_dir
    if input_dir:
        dir_pattern = os.path.join(input_dir, pattern)
        matched = glob.glob(dir_pattern)
        files_to_load.extend(matched)

    # Deduplicate
    files_to_load = list(set(files_to_load))

    if not files_to_load:
        raise ValueError("No input files found. Check your --input or --input-dir arguments.")

    logger.info(f"Loading {len(files_to_load)} files...")

    dfs = []
    for f in files_to_load:
        try:
            df = pd.read_csv(f)
            df['source_file'] = os.path.basename(f)
            logger.info(f"  Loaded {len(df)} rows from {os.path.basename(f)}")
            dfs.append(df)
        except Exception as e:
            logger.error(f"  Failed to load {f}: {e}")

    if not dfs:
        raise ValueError("No valid data loaded from input files.")

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows loaded: {len(combined)}")

    return combined


def filter_by_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filter transactions to a specific year based on sold_date.

    Args:
        df: Transaction DataFrame
        year: Year to filter (e.g., 2025)

    Returns:
        Filtered DataFrame
    """
    original_count = len(df)

    # Check if sold_date column exists
    if 'sold_date' not in df.columns:
        logger.warning("No sold_date column found - cannot filter by year")
        return df

    # Parse dates - handle timezone-aware strings
    df['sold_date_parsed'] = pd.to_datetime(df['sold_date'], errors='coerce', utc=True)

    # Check how many valid dates we have
    valid_dates = df['sold_date_parsed'].notna().sum()
    if valid_dates == 0:
        logger.warning("No valid sold_date values found - cannot filter by year")
        df = df.drop(columns=['sold_date_parsed'])
        return df

    logger.info(f"Valid sold dates: {valid_dates}/{len(df)}")

    # Filter to year
    df = df[df['sold_date_parsed'].dt.year == year]
    df = df.drop(columns=['sold_date_parsed'])

    logger.info(f"Filtered to year {year}: {original_count} -> {len(df)} transactions")

    return df


def deduplicate_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate transactions based on MLS number.

    Args:
        df: Transaction DataFrame

    Returns:
        Deduplicated DataFrame
    """
    original_count = len(df)

    if 'mls_number' in df.columns:
        df = df.drop_duplicates(subset=['mls_number'], keep='first')
        logger.info(f"Deduplicated by mls_number: {original_count} -> {len(df)} transactions")
    else:
        logger.warning("No mls_number column - using all columns for deduplication")
        df = df.drop_duplicates()
        logger.info(f"Deduplicated: {original_count} -> {len(df)} transactions")

    return df


def aggregate_by_agent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate transaction data by agent.

    Args:
        df: Transaction DataFrame with agent_name, sold_price, address, etc.

    Returns:
        DataFrame with one row per agent and aggregated statistics
    """
    # Filter to transactions with agent info
    df_with_agent = df[df['agent_name'].notna() & (df['agent_name'] != '')].copy()
    logger.info(f"Transactions with agent name: {len(df_with_agent)}/{len(df)}")

    if len(df_with_agent) == 0:
        raise ValueError("No transactions with agent information found.")

    # Determine price column
    price_col = 'sold_price' if 'sold_price' in df_with_agent.columns else 'list_price'
    logger.info(f"Using {price_col} for volume calculations")

    # Ensure price is numeric
    df_with_agent[price_col] = pd.to_numeric(df_with_agent[price_col], errors='coerce')

    # Group by agent and aggregate
    def aggregate_agent(group):
        """Aggregate function for each agent group."""
        prices = group[price_col].dropna()

        # Get unique cities
        cities = group['city'].dropna().unique().tolist()

        # Get sample addresses (up to 5)
        addresses = group['address'].dropna().unique().tolist()[:5]

        # Get first non-null contact info
        email = group['agent_email'].dropna().iloc[0] if group['agent_email'].notna().any() else None
        phone = group['agent_phone'].dropna().iloc[0] if group['agent_phone'].notna().any() else None
        brokerage = group['brokerage'].dropna().iloc[0] if group['brokerage'].notna().any() else None
        agent_id = group['agent_id'].dropna().iloc[0] if 'agent_id' in group.columns and group['agent_id'].notna().any() else None

        return pd.Series({
            'agent_email': email,
            'agent_phone': phone,
            'agent_id': agent_id,
            'brokerage': brokerage,
            'transaction_count': len(group),
            'total_volume': prices.sum() if len(prices) > 0 else 0,
            'avg_price': prices.mean() if len(prices) > 0 else 0,
            'median_price': prices.median() if len(prices) > 0 else 0,
            'highest_sale': prices.max() if len(prices) > 0 else 0,
            'lowest_sale': prices.min() if len(prices) > 0 else 0,
            'cities': ', '.join(cities) if cities else '',
            'sample_addresses': ' | '.join(addresses) if addresses else '',
            'listings_with_price': len(prices),
        })

    logger.info("Aggregating by agent...")
    agent_stats = df_with_agent.groupby('agent_name').apply(aggregate_agent, include_groups=False).reset_index()

    # Rename agent_name to match expected output
    agent_stats = agent_stats.rename(columns={'agent_name': 'agent_name'})

    logger.info(f"Unique agents: {len(agent_stats)}")

    return agent_stats


def filter_top_agents(
    agent_stats: pd.DataFrame,
    min_transactions: int = 10
) -> pd.DataFrame:
    """
    Filter to agents with minimum transaction count.

    Args:
        agent_stats: Aggregated agent DataFrame
        min_transactions: Minimum number of transactions required

    Returns:
        Filtered DataFrame sorted by total_volume descending
    """
    original_count = len(agent_stats)

    # Filter by transaction count
    top_agents = agent_stats[agent_stats['transaction_count'] >= min_transactions].copy()

    # Sort by total volume descending
    top_agents = top_agents.sort_values('total_volume', ascending=False)

    # Reset index
    top_agents = top_agents.reset_index(drop=True)

    logger.info(f"Agents with {min_transactions}+ transactions: {len(top_agents)}/{original_count}")

    return top_agents


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format output DataFrame with proper column order and formatting.

    Args:
        df: Agent stats DataFrame

    Returns:
        Formatted DataFrame
    """
    # Define column order
    columns = [
        'agent_name',
        'agent_email',
        'agent_phone',
        'brokerage',
        'transaction_count',
        'total_volume',
        'avg_price',
        'median_price',
        'highest_sale',
        'lowest_sale',
        'cities',
        'sample_addresses',
        'agent_id',
    ]

    # Only include columns that exist
    columns = [c for c in columns if c in df.columns]

    df = df[columns].copy()

    # Format currency columns
    for col in ['total_volume', 'avg_price', 'median_price', 'highest_sale', 'lowest_sale']:
        if col in df.columns:
            df[col] = df[col].round(0).astype(int)

    return df


def export_transactions_by_agent(
    df: pd.DataFrame,
    top_agents: pd.DataFrame,
    output_path: str
):
    """
    Export detailed transactions for each top agent.

    Args:
        df: Original transaction DataFrame
        top_agents: Filtered top agents DataFrame
        output_path: Path for output CSV
    """
    # Filter transactions to only top agents
    top_agent_names = set(top_agents['agent_name'].tolist())
    df_top = df[df['agent_name'].isin(top_agent_names)].copy()

    # Sort by agent name, then sold_date
    if 'sold_date' in df_top.columns:
        df_top = df_top.sort_values(['agent_name', 'sold_date'], ascending=[True, False])
    else:
        df_top = df_top.sort_values('agent_name')

    df_top.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df_top)} transactions for top agents to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Aggregate Repliers MLS sold transactions by agent')
    parser.add_argument('--input', type=str, nargs='+',
                        help='Input CSV file(s) or glob pattern(s)')
    parser.add_argument('--input-dir', type=str,
                        help='Directory containing input CSV files')
    parser.add_argument('--pattern', type=str, default='*_sold_*.csv',
                        help='Glob pattern for files in input-dir (default: *_sold_*.csv)')
    parser.add_argument('--year', type=int,
                        help='Filter to specific year (e.g., 2025)')
    parser.add_argument('--min-transactions', type=int, default=10,
                        help='Minimum transactions to qualify as top agent (default: 10)')
    parser.add_argument('--output', type=str,
                        help='Output path for top agents CSV')
    parser.add_argument('--output-dir', type=str, default='output/repliers',
                        help='Output directory (default: output/repliers)')
    parser.add_argument('--export-details', action='store_true',
                        help='Also export detailed transactions for top agents')
    parser.add_argument('--all-agents', action='store_true',
                        help='Export all agents, not just top performers')

    args = parser.parse_args()

    # Validate inputs
    if not args.input and not args.input_dir:
        parser.error("Must provide --input or --input-dir")

    # Load data
    try:
        df = load_transaction_files(
            input_files=args.input,
            input_dir=args.input_dir,
            pattern=args.pattern
        )
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Deduplicate
    df = deduplicate_transactions(df)

    # Filter by year if specified
    if args.year:
        df = filter_by_year(df, args.year)
        if len(df) == 0:
            logger.error(f"No transactions found for year {args.year}")
            sys.exit(1)

    # Aggregate by agent
    try:
        agent_stats = aggregate_by_agent(df)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Filter to top agents
    if args.all_agents:
        top_agents = agent_stats.sort_values('total_volume', ascending=False).reset_index(drop=True)
        logger.info(f"Exporting all {len(top_agents)} agents")
    else:
        top_agents = filter_top_agents(agent_stats, min_transactions=args.min_transactions)
        if len(top_agents) == 0:
            logger.warning(f"No agents found with {args.min_transactions}+ transactions")
            logger.info("Try reducing --min-transactions threshold")

            # Show distribution
            print("\n=== Transaction Count Distribution ===")
            print(agent_stats['transaction_count'].describe())
            print(f"\nAgents with 5+ transactions: {len(agent_stats[agent_stats['transaction_count'] >= 5])}")
            print(f"Agents with 3+ transactions: {len(agent_stats[agent_stats['transaction_count'] >= 3])}")
            print(f"Agents with 2+ transactions: {len(agent_stats[agent_stats['transaction_count'] >= 2])}")
            sys.exit(0)

    # Format output
    top_agents = format_output(top_agents)

    # Generate output paths
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    year_suffix = f"_{args.year}" if args.year else ""
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.output:
        output_path = args.output
    else:
        output_path = f"{args.output_dir}/top_agents{year_suffix}_{timestamp}.csv"

    # Save top agents
    top_agents.to_csv(output_path, index=False)
    logger.info(f"Saved {len(top_agents)} top agents to: {output_path}")

    # Export detailed transactions if requested
    if args.export_details:
        details_path = output_path.replace('.csv', '_transactions.csv')
        export_transactions_by_agent(df, top_agents, details_path)

    # Print summary
    print("\n" + "=" * 60)
    print("AGGREGATION SUMMARY")
    print("=" * 60)
    print(f"Total transactions processed: {len(df)}")
    print(f"Unique agents: {len(agent_stats)}")
    print(f"Top agents ({args.min_transactions}+ tx): {len(top_agents)}")

    if len(top_agents) > 0:
        print(f"\nTotal volume (top agents): ${top_agents['total_volume'].sum():,.0f}")
        print(f"Total transactions (top agents): {top_agents['transaction_count'].sum()}")

        print("\n" + "=" * 60)
        print("TOP 20 AGENTS BY VOLUME")
        print("=" * 60)
        for i, row in top_agents.head(20).iterrows():
            print(f"{i+1:2d}. {row['agent_name'][:35]:35s} | {row['transaction_count']:3d} tx | ${row['total_volume']:>12,.0f}")

    print(f"\nOutput: {output_path}")

    return top_agents


if __name__ == '__main__':
    main()
