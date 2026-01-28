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
- Person: {name, primary_email, domain, city, state, country, location_source}
  Location properties:
    - city: Person's city (e.g., "Miami", "Bogotá")
    - state: State/province (e.g., "Florida", "Antioquia")
    - country: Country (e.g., "United States", "Colombia")
    - location_source: How location was determined ('cctld', 'company_geo', 'apollo')
- Company: {name, normalized_name, industry}
- Topic: {name}

RELATIONSHIPS:
- (Person)-[:KNOWS]->(Person)
  People who have emailed each other.
  Properties:
    - strength_score_v2: Relationship strength 0-100 (USE THIS for ranking)
    - emails_sent: Emails I sent to them
    - emails_received: Emails they sent to me
    - reply_rate: How often they reply to my emails (0.0-1.0)
    - last_contact: Date of last email

- (Person)-[:CC_TOGETHER {count}]->(Person)
  People CC'd together on emails

- (Person)-[:LINKEDIN_CONNECTED {degree, connected_on}]->(Person)
  1st degree LinkedIn connections

- (Person)-[:LINKEDIN_MESSAGED]->(Person)
  People who have exchanged LinkedIn messages.
  Properties:
    - total_messages: Total messages in conversation
    - my_messages: Messages I sent
    - their_messages: Messages they sent
    - first_message: Date of first message
    - last_message: Date of last message
    - conversation_count: Number of separate conversations

- (Person)-[:ENDORSED {skill, date}]->(Person)
  LinkedIn skill endorsements

- (Person)-[:WORKS_AT {role, confidence}]->(Company)
  Person works at a company (extracted from emails)

- (Person)-[:DISCUSSED]->(Topic)
  Topics a person has discussed in emails

KEY CONTEXT:
- My email is: tu@jaguarcapital.co (starting point for "who do I know" queries)
- Use case-insensitive regex: =~ '(?i).*pattern.*'
- ALWAYS use DISTINCT to avoid duplicates
- ALWAYS order by strength_score_v2 DESC for relationship-based queries
- Company.industry can be: Real Estate, Finance, Technology, Healthcare, Legal, Marketing, Consulting, Construction, Education, Retail, Manufacturing, Media, Hospitality, Transportation, Energy, Non-Profit, Government, Other

STRENGTH SCORE INTERPRETATION:
- 70-100: Strong relationship (frequent, balanced, recent contact)
- 40-69: Medium relationship
- 10-39: Weak relationship
- 0-9: Minimal (likely newsletter or one-off)

EXAMPLE QUERIES:
1. "realtors I know" -> search by industry = 'Real Estate', order by strength
2. "strongest contacts at X" -> filter by company, order by strength_score_v2 DESC
3. "warm intro to X" -> MATCH path through 1-2 KNOWS hops, order by strength
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
7. ALWAYS ORDER BY r.strength_score_v2 DESC for relationship-based queries
8. IMPORTANT - Specific company vs industry search:
   - If user mentions a SPECIFIC company (e.g., "at Compass", "at CBRE", "in Goldman") -> filter ONLY by that company name
   - If user asks for a GENERIC industry (e.g., "realtors", "finance people") WITHOUT a specific company -> use OR patterns with industry keywords
9. NEVER mix specific company + industry keywords. If they say "realtors at Compass" -> ONLY search for Compass.

EXAMPLE - "which realtors do I know" (GENERIC - no specific company):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*(cbre|jll|compass|cushman|colliers|realty|realtor|broker|property|estate|homes|sotheby|remax|century21|coldwell).*'
RETURN DISTINCT p.name, p.primary_email, c.name, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "which realtors do I know at Compass" (SPECIFIC company - Compass only):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*compass.*'
RETURN DISTINCT p.name, p.primary_email, c.name, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "who do I know at Goldman Sachs" (SPECIFIC company):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*goldman.*'
RETURN DISTINCT p.name, p.primary_email, c.name, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "my strongest contacts" or "closest relationships":
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)
WHERE r.strength_score_v2 >= 70
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, 'Unknown') as company, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "how to connect with someone at Compass" (warm intro via strongest paths):
MATCH path = (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r1:KNOWS]->(intermediary:Person)-[r2:KNOWS]->(target:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*compass.*'
RETURN DISTINCT target.name as target, target.primary_email, c.name as company,
       intermediary.name as intro_via, r1.strength_score_v2 as intro_strength,
       length(path) as hops
ORDER BY r1.strength_score_v2 DESC, hops
LIMIT 25

EXAMPLE - "tech companies I've emailed" or "people in tech" (INDUSTRY via TOPICS):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)-[:DISCUSSED]->(t:Topic)
WHERE t.name IN ['ai', 'startup', 'data', 'proptech', 'technology', 'software', 'saas', 'developer']
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, 'Unknown') as company, r.strength_score_v2 as strength, collect(DISTINCT t.name) as topics
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "finance people" or "investors I know" (INDUSTRY via TOPICS):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)-[:DISCUSSED]->(t:Topic)
WHERE t.name IN ['investment', 'finance', 'finanzas', 'fundraising', 'financial services', 'venture capital', 'private equity']
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, 'Unknown') as company, r.strength_score_v2 as strength, collect(DISTINCT t.name) as topics
ORDER BY r.strength_score_v2 DESC
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

EXAMPLE - "LinkedIn connections I've messaged" or "warm LinkedIn contacts":
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:LINKEDIN_MESSAGED]->(p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name, p.primary_email, coalesce(c.name, p.linkedin_company, 'Unknown') as company,
       r.total_messages, r.my_messages, r.their_messages, r.last_message
ORDER BY r.total_messages DESC
LIMIT 25

EXAMPLE - "warm intro path via LinkedIn messages" (strongest path = messaged + connected):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[:LINKEDIN_MESSAGED]->(intermediary:Person)-[:LINKEDIN_CONNECTED]->(target:Person)-[:WORKS_AT]->(c:Company)
WHERE c.name =~ '(?i).*target_company.*'
RETURN DISTINCT target.name as target, target.primary_email, c.name as company,
       intermediary.name as intro_via
LIMIT 25

EXAMPLE - "realtors in Miami" or "real estate contacts in Miami" (LOCATION + INDUSTRY):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)
WHERE p.city =~ '(?i)miami'
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Real Estate'
RETURN DISTINCT p.name, p.primary_email, c.name, p.city, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "contacts in Colombia" or "people from Colombia" (LOCATION filter):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)
WHERE p.country = 'Colombia'
RETURN DISTINCT p.name, p.primary_email, p.city, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

EXAMPLE - "contacts in Florida" or "people in Florida" (STATE filter):
MATCH (me:Person {primary_email: 'tu@jaguarcapital.co'})-[r:KNOWS]->(p:Person)
WHERE p.state =~ '(?i)florida'
RETURN DISTINCT p.name, p.primary_email, p.city, r.strength_score_v2 as strength
ORDER BY r.strength_score_v2 DESC
LIMIT 25

IMPORTANT: For location queries (city, state, country), use Person node properties (p.city, p.state, p.country).
Use case-insensitive regex for city/state matching: =~ '(?i)pattern'
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
