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

- (Person)-[:WORKS_AT {role, confidence}]->(Company)
  Person works at a company (extracted from emails)

- (Person)-[:DISCUSSED]->(Topic)
  Topics a person has discussed in emails

NOTES:
- My email is: tu@jaguarcapital.co (use this as the starting point for "who do I know" queries)
- Use case-insensitive regex for name matching: =~ '(?i).*pattern.*'
- Company names may have variations (e.g., "CBRE", "CBRE Group", "cbre.com")
- For warm intros, traverse KNOWS relationships 1-2 hops
"""

SYSTEM_PROMPT = """You are a Cypher query generator for a contact intelligence graph.
Given a natural language question, generate a valid Neo4j Cypher query.

Return ONLY the Cypher query, no explanation or markdown formatting.

Important rules:
1. Use case-insensitive regex for text matching: =~ '(?i).*pattern.*'
2. Always RETURN relevant fields (name, email, company, etc.)
3. Limit results to 25 unless user asks for more
4. For "who do I know" questions, start from tu@jaguarcapital.co
5. For warm intros, use variable-length paths: -[:KNOWS*1..2]-
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
