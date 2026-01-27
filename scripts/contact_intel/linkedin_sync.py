"""LinkedIn Connections CSV sync module.

Parses LinkedIn's exported Connections.csv and integrates with the contact graph.
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def parse_linkedin_csv(csv_path: Path) -> List[Dict]:
    """Parse LinkedIn Connections.csv export file.

    Args:
        csv_path: Path to the LinkedIn Connections.csv file

    Returns:
        List of dicts with keys: first_name, last_name, email, company, position, connected_on
        Empty values are returned as None.

    Raises:
        FileNotFoundError: If the CSV file doesn't exist
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"LinkedIn CSV not found: {csv_path}")

    connections = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            connection = {
                'first_name': row.get('First Name') or None,
                'last_name': row.get('Last Name') or None,
                'email': row.get('Email Address') or None,
                'company': row.get('Company') or None,
                'position': row.get('Position') or None,
                'connected_on': row.get('Connected On') or None,
            }
            connections.append(connection)

    logger.info(f"Parsed {len(connections)} LinkedIn connections from {csv_path}")
    return connections
