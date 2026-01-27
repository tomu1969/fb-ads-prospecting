"""Industry classification for Company nodes.

Uses GPT-4o-mini to classify companies into standard industries.
Processes in batches of 10 for efficiency.

Usage:
    python -m scripts.contact_intel.industry_classifier
    python -m scripts.contact_intel.industry_classifier --status
"""

import argparse
import json
import logging
import os
import time
from typing import Dict, List, Optional

from dotenv import load_dotenv

from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('industry_classifier.log'),
    ]
)
logger = logging.getLogger(__name__)

# Standard industries
INDUSTRIES = [
    "Real Estate",
    "Finance",
    "Technology",
    "Healthcare",
    "Legal",
    "Marketing",
    "Consulting",
    "Construction",
    "Education",
    "Retail",
    "Manufacturing",
    "Media",
    "Hospitality",
    "Transportation",
    "Energy",
    "Non-Profit",
    "Government",
    "Other",
]

SYSTEM_PROMPT = """You classify companies into industries. Return valid JSON only."""

USER_PROMPT_TEMPLATE = """Classify these companies into industries.

Companies:
{companies_list}

Available industries:
{industries}

Return JSON array with one object per company:
[
  {{"name": "Company Name", "industry": "Industry Name"}},
  ...
]

Rules:
- Use ONLY industries from the list above
- If unsure, use "Other"
- Match company name exactly as given
"""

# Batch size for API calls
BATCH_SIZE = 10

# Rate limiting
REQUEST_DELAY = 0.2  # seconds between requests


def get_openai_client():
    """Get OpenAI client."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("Install openai: pip install openai")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    return OpenAI(api_key=api_key)


def classify_batch(client, companies: List[str]) -> Dict[str, str]:
    """Classify a batch of companies.

    Args:
        client: OpenAI client
        companies: List of company names

    Returns:
        Dict mapping company name to industry
    """
    companies_list = "\n".join(f"- {c}" for c in companies)
    industries_list = ", ".join(INDUSTRIES)

    prompt = USER_PROMPT_TEMPLATE.format(
        companies_list=companies_list,
        industries=industries_list,
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )

        content = response.choices[0].message.content
        # Handle both array and object responses
        data = json.loads(content)
        if isinstance(data, dict) and 'classifications' in data:
            data = data['classifications']
        elif isinstance(data, dict) and 'companies' in data:
            data = data['companies']
        elif isinstance(data, dict):
            # Try to extract array from any key
            for key, value in data.items():
                if isinstance(value, list):
                    data = value
                    break

        if not isinstance(data, list):
            logger.warning(f"Unexpected response format: {content[:200]}")
            return {}

        # Build mapping
        result = {}
        for item in data:
            if isinstance(item, dict):
                name = item.get('name', '')
                industry = item.get('industry', 'Other')
                if name and industry in INDUSTRIES:
                    result[name] = industry
                elif name:
                    result[name] = 'Other'

        return result

    except Exception as e:
        logger.error(f"API error: {e}")
        return {}


def get_unclassified_companies(gb: GraphBuilder) -> List[Dict]:
    """Get companies without industry classification."""
    with gb.driver.session() as session:
        result = session.run("""
            MATCH (c:Company)
            WHERE c.industry IS NULL
            RETURN c.name as name
            ORDER BY c.name
        """)
        return [dict(record) for record in result]


def get_all_companies(gb: GraphBuilder) -> List[Dict]:
    """Get all companies."""
    with gb.driver.session() as session:
        result = session.run("""
            MATCH (c:Company)
            RETURN c.name as name, c.industry as industry
            ORDER BY c.name
        """)
        return [dict(record) for record in result]


def update_company_industry(gb: GraphBuilder, name: str, industry: str):
    """Update company with industry classification."""
    with gb.driver.session() as session:
        session.run("""
            MATCH (c:Company {name: $name})
            SET c.industry = $industry,
                c.industry_classified_at = datetime()
        """, name=name, industry=industry)


def run_classification(limit: Optional[int] = None) -> Dict:
    """Run industry classification on unclassified companies.

    Args:
        limit: Optional limit on companies to process

    Returns:
        Stats dict
    """
    if not neo4j_available():
        logger.error("Neo4j not available")
        return {'error': 'Neo4j not available'}

    gb = GraphBuilder()
    gb.connect()

    stats = {
        'total': 0,
        'classified': 0,
        'errors': 0,
        'api_calls': 0,
        'cost_usd': 0.0,
    }

    try:
        # Get unclassified companies
        companies = get_unclassified_companies(gb)
        if limit:
            companies = companies[:limit]

        stats['total'] = len(companies)
        logger.info(f"Found {len(companies)} unclassified companies")

        if not companies:
            logger.info("No companies to classify")
            return stats

        # Get OpenAI client
        client = get_openai_client()

        # Process in batches
        company_names = [c['name'] for c in companies]
        batches = [company_names[i:i + BATCH_SIZE] for i in range(0, len(company_names), BATCH_SIZE)]

        for batch_num, batch in enumerate(batches):
            logger.info(f"Processing batch {batch_num + 1}/{len(batches)} ({len(batch)} companies)")

            # Rate limiting
            time.sleep(REQUEST_DELAY)

            # Classify batch
            classifications = classify_batch(client, batch)
            stats['api_calls'] += 1

            # Estimate cost: ~200 tokens input, ~100 tokens output per batch
            stats['cost_usd'] += (300 / 1_000_000) * 0.15  # rough estimate

            # Update graph
            for name in batch:
                industry = classifications.get(name, 'Other')
                try:
                    update_company_industry(gb, name, industry)
                    stats['classified'] += 1
                    logger.debug(f"Classified {name} -> {industry}")
                except Exception as e:
                    logger.error(f"Error updating {name}: {e}")
                    stats['errors'] += 1

            # Progress log
            progress = (batch_num + 1) * BATCH_SIZE
            logger.info(f"Progress: {min(progress, len(company_names))}/{len(company_names)} companies")

    finally:
        gb.close()

    logger.info(f"Classification complete: {stats}")
    return stats


def show_status():
    """Show industry classification status."""
    if not neo4j_available():
        print("Neo4j not available")
        return

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            # Count classified vs unclassified
            result = session.run("""
                MATCH (c:Company)
                RETURN
                    count(c) as total,
                    sum(CASE WHEN c.industry IS NOT NULL THEN 1 ELSE 0 END) as classified,
                    sum(CASE WHEN c.industry IS NULL THEN 1 ELSE 0 END) as unclassified
            """)
            counts = result.single()

            # Industry distribution
            result = session.run("""
                MATCH (c:Company)
                WHERE c.industry IS NOT NULL
                RETURN c.industry as industry, count(*) as count
                ORDER BY count DESC
            """)
            distribution = [dict(record) for record in result]

        print("\n" + "=" * 50)
        print("INDUSTRY CLASSIFICATION STATUS")
        print("=" * 50)
        print(f"Total companies:          {counts['total']:,}")
        print(f"Classified:               {counts['classified']:,}")
        print(f"Unclassified:             {counts['unclassified']:,}")

        if distribution:
            print(f"\nIndustry Distribution:")
            for row in distribution[:15]:  # Top 15
                print(f"  {row['industry']:20} {row['count']:,}")

    finally:
        gb.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Classify companies into industries using LLM'
    )
    parser.add_argument('--status', action='store_true',
                        help='Show classification status')
    parser.add_argument('--run', action='store_true',
                        help='Run classification')
    parser.add_argument('--limit', type=int,
                        help='Limit number of companies to process')

    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.run:
        stats = run_classification(limit=args.limit)
        print("\n" + "=" * 50)
        print("INDUSTRY CLASSIFICATION RESULTS")
        print("=" * 50)
        print(f"Total companies:          {stats.get('total', 0):,}")
        print(f"Classified:               {stats.get('classified', 0):,}")
        print(f"API calls:                {stats.get('api_calls', 0)}")
        print(f"Estimated cost:           ${stats.get('cost_usd', 0):.4f}")
        print(f"Errors:                   {stats.get('errors', 0)}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()
