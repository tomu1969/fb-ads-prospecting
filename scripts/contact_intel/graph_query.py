"""Natural language query interface for the contact graph.

Translates natural language questions to Cypher using GPT-4o-mini,
executes against Neo4j, and returns results.

Usage:
    python -m scripts.contact_intel.graph_query "Who do I know at CBRE?"
    python -m scripts.contact_intel.graph_query "Find warm intros to real estate investors"
    python -m scripts.contact_intel.graph_query --interactive
"""

import argparse
import json
import logging
import os
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Graph schema for LLM context
GRAPH_SCHEMA = """
Neo4j Graph Schema:

NODES:
- Person: {name, primary_email, domain}
- Company: {name, normalized_name}
- Topic: {name}

RELATIONSHIPS:
- (Person)-[:KNOWS {email_count, last_contact}]->(Person)
  People who have emailed each other

- (Person)-[:CC_TOGETHER {count}]->(Person)
  People CC'd together on emails

- (Person)-[:LINKEDIN_CONNECTED {degree, connected_on}]->(Person)
  1st degree LinkedIn connections

- (Person)-[:WORKS_AT {role, confidence}]->(Company)
  Person works at a company (extracted from emails)

- (Person)-[:DISCUSSED]->(Topic)
  Topics a person has discussed in emails

KEY CONTEXT:
- My email is: tu@jaguarcapital.co (starting point for "who do I know" queries)
- Use case-insensitive regex: =~ '(?i).*pattern.*'
- ALWAYS use DISTINCT to avoid duplicates

INDUSTRY KEYWORDS (use OR patterns for industry searches):
- Real Estate/Realtors: CBRE, JLL, Compass, Cushman, Colliers, Sotheby, Remax, Century21,
  Coldwell, Zillow, realty, realtor, property, properties, broker, estate, homes
- Finance: Goldman, Morgan, JPMorgan, Blackstone, KKR, bank, capital, invest, fund,
  asset, wealth, partners, ventures, equity, securities
- Tech: Google, Microsoft, Amazon, Meta, Apple, Salesforce, tech, software, ai, data, saas

EXAMPLE QUERIES:
1. "realtors I know" -> search companies matching real estate keywords
2. "connect with CEO" -> find paths through KNOWS to people at that company
3. "warm intro to X" -> MATCH path through 1-2 KNOWS hops
"""

SYSTEM_PROMPT = """You are a Cypher query generator for a contact intelligence graph.
Given a natural language question, generate a valid Neo4j Cypher query.

Return ONLY the Cypher query, no explanation or markdown formatting.

CRITICAL RULES:
1. Use case-insensitive regex: =~ '(?i).*pattern.*'
2. ALWAYS use DISTINCT to avoid duplicate rows
3. ALWAYS RETURN name, primary_email, and company/role when relevant
4. Limit to 25 results unless user asks for more
5. For "who do I know" -> start from tu@jaguarcapital.co via KNOWS
6. For warm intros -> use -[:KNOWS*1..2]- paths
7. IMPORTANT - Specific company vs industry search:
   - If user mentions a SPECIFIC company (e.g., "at Compass", "at CBRE", "in Goldman") -> filter ONLY by that company name
   - If user asks for a GENERIC industry (e.g., "realtors", "finance people") WITHOUT a specific company -> use OR patterns with industry keywords
8. NEVER mix specific company + industry keywords. If they say "realtors at Compass" -> ONLY search for Compass.

EXAMPLE - "which realtors do I know" (GENERIC - no specific company):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*(cbre|jll|compass|cushman|colliers|realty|realtor|broker|property|estate|homes|sotheby|remax|century21|coldwell).*'
RETURN DISTINCT p.name, p.primary_email, c.name
LIMIT 25

EXAMPLE - "which realtors do I know at Compass" (SPECIFIC company - Compass only):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*compass.*'
RETURN DISTINCT p.name, p.primary_email, c.name
LIMIT 25

EXAMPLE - "who do I know at Goldman Sachs" (SPECIFIC company):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*goldman.*'
RETURN DISTINCT p.name, p.primary_email, c.name
LIMIT 25

EXAMPLE - "how to connect with someone at Compass":
MATCH path = (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS*1..2]-(target:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*compass.*'
RETURN DISTINCT target.name, target.primary_email, c.name, length(path) as hops
ORDER BY hops
LIMIT 25

EXAMPLE - "tech companies I've emailed" or "people in tech" (INDUSTRY via TOPICS):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS]->(p:Person)-[:DISCUSSED]->(t:Topic)
WHERE t.name IN ['ai', 'startup', 'data', 'proptech', 'technology', 'software', 'saas', 'developer']
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, 'Unknown') as company, collect(DISTINCT t.name) as topics
LIMIT 25

EXAMPLE - "finance people" or "investors I know" (INDUSTRY via TOPICS):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:KNOWS]->(p:Person)-[:DISCUSSED]->(t:Topic)
WHERE t.name IN ['investment', 'finance', 'finanzas', 'fundraising', 'financial services', 'venture capital', 'private equity']
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, 'Unknown') as company, collect(DISTINCT t.name) as topics
LIMIT 25

IMPORTANT: For industry/sector queries (tech, finance, retail), use TOPICS via [:DISCUSSED] - NOT company name patterns.
Only use company name patterns when user asks about a SPECIFIC company (e.g., "people at Google").

EXAMPLE - "LinkedIn connections at Google" or "who am I LinkedIn-connected to at Google":
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_CONNECTED]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*google.*'
RETURN DISTINCT p.name, p.primary_email, c.name
LIMIT 25

EXAMPLE - "warm intro via LinkedIn" (prefer LinkedIn-connected paths):
MATCH path = (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_CONNECTED|KNOWS*1..2]-(target:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*target_company.*'
RETURN DISTINCT target.name, target.primary_email, c.name,
       [r IN relationships(path) | type(r)] as path_types,
       length(path) as hops
ORDER BY hops
LIMIT 25
"""


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


def generate_cypher(question: str) -> str:
    """Generate Cypher query from natural language question."""
    client = get_openai_client()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Graph Schema:\n{GRAPH_SCHEMA}\n\nQuestion: {question}"},
        ],
        temperature=0.1,
        max_tokens=500,
    )

    cypher = response.choices[0].message.content.strip()

    # Clean up any markdown formatting
    if cypher.startswith("```"):
        lines = cypher.split("\n")
        cypher = "\n".join(lines[1:-1])

    return cypher


def execute_cypher(cypher: str) -> List[Dict]:
    """Execute Cypher query against Neo4j."""
    from scripts.contact_intel.graph_builder import GraphBuilder, neo4j_available

    if not neo4j_available():
        raise RuntimeError("Neo4j not available")

    gb = GraphBuilder()
    gb.connect()

    try:
        with gb.driver.session() as session:
            result = session.run(cypher)
            records = [dict(record) for record in result]
            return records
    finally:
        gb.close()


def format_results(records: List[Dict]) -> str:
    """Format query results for display."""
    if not records:
        return "No results found."

    # Get column names from first record
    columns = list(records[0].keys())

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for record in records:
        for col in columns:
            val = str(record.get(col, ""))[:50]  # Truncate long values
            widths[col] = max(widths[col], len(val))

    # Build table
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)

    rows = []
    for record in records:
        row = " | ".join(str(record.get(col, ""))[:50].ljust(widths[col]) for col in columns)
        rows.append(row)

    return f"{header}\n{separator}\n" + "\n".join(rows)


def query(question: str, show_cypher: bool = False) -> str:
    """Process a natural language query and return results."""
    logger.info(f"Question: {question}")

    # Generate Cypher
    cypher = generate_cypher(question)
    logger.info(f"Generated Cypher: {cypher}")

    if show_cypher:
        print(f"\nCypher:\n{cypher}\n")

    # Execute query
    try:
        records = execute_cypher(cypher)
        logger.info(f"Got {len(records)} results")
    except Exception as e:
        return f"Query error: {e}\n\nGenerated Cypher:\n{cypher}"

    # Format results
    return format_results(records)


def interactive_mode():
    """Run interactive query session."""
    print("Contact Graph Query (type 'exit' to quit, 'cypher' to toggle showing Cypher)")
    print("-" * 60)

    show_cypher = False

    while True:
        try:
            question = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue

        if question.lower() == "exit":
            print("Goodbye!")
            break

        if question.lower() == "cypher":
            show_cypher = not show_cypher
            print(f"Show Cypher: {show_cypher}")
            continue

        result = query(question, show_cypher=show_cypher)
        print(f"\n{result}")


def main():
    parser = argparse.ArgumentParser(
        description="Natural language query interface for contact graph"
    )
    parser.add_argument(
        "question",
        nargs="?",
        help="Natural language question to query the graph"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Run in interactive mode"
    )
    parser.add_argument(
        "--cypher", "-c",
        action="store_true",
        help="Show generated Cypher query"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format='%(message)s',
    )

    if args.interactive:
        interactive_mode()
    elif args.question:
        result = query(args.question, show_cypher=args.cypher)
        print(result)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
