#!/usr/bin/env python3
"""
HubSpot BANT Qualification Properties Creator

Creates 4 custom deal properties in HubSpot for BANT qualification framework:
- Need (bant_need): Customer's need and how our solution helps
- Budget (bant_budget): Monthly budget including performance marketing + solution cost
- Authority (bant_authority): Decision maker(s) and decision process
- Timing (bant_timing): Expected decision date

Usage:
    python scripts/hubspot_bant_properties.py

After running this script, manually configure required fields in HubSpot UI:
1. Settings → Objects → Deals → Pipelines
2. Select "LaHaus AI" pipeline
3. Click "Edit stages"
4. On "Demo Presentado" stage, click "Configure required properties"
5. Add all 4 BANT properties as required
"""

import os
import sys
import logging
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hubspot_bant_properties.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

HUBSPOT_API_KEY = os.getenv('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    logger.error("HUBSPOT_API_KEY not found in .env file")
    sys.exit(1)

# HubSpot API base URL
BASE_URL = "https://api.hubapi.com/crm/v3/properties/deals"

# Headers for API requests
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json"
}

# BANT properties to create
BANT_PROPERTIES = [
    {
        "groupName": "dealinformation",
        "name": "bant_need",
        "label": "Need (BANT)",
        "type": "string",
        "fieldType": "textarea",
        "description": "What's the customer's need? How can our solution help?"
    },
    {
        "groupName": "dealinformation",
        "name": "bant_budget",
        "label": "Budget (BANT)",
        "type": "string",
        "fieldType": "text",
        "description": "Monthly budget including performance marketing spend + solution cost"
    },
    {
        "groupName": "dealinformation",
        "name": "bant_authority",
        "label": "Authority (BANT)",
        "type": "string",
        "fieldType": "textarea",
        "description": "Who is the decision maker? What's the decision process?"
    },
    {
        "groupName": "dealinformation",
        "name": "bant_timing",
        "label": "Timing (BANT)",
        "type": "date",
        "fieldType": "date",
        "description": "Expected decision date"
    }
]


def create_property(property_config: dict) -> bool:
    """Create a single deal property in HubSpot."""
    try:
        response = requests.post(BASE_URL, headers=HEADERS, json=property_config)

        if response.status_code == 201:
            logger.info(f"✓ Created property: {property_config['name']} ({property_config['label']})")
            return True
        elif response.status_code == 409:
            logger.warning(f"⚠ Property already exists: {property_config['name']}")
            return True
        else:
            logger.error(f"✗ Failed to create {property_config['name']}: {response.status_code}")
            logger.error(f"  Response: {response.text}")
            return False

    except requests.RequestException as e:
        logger.error(f"✗ Request failed for {property_config['name']}: {e}")
        return False


def verify_properties() -> dict:
    """Verify that all BANT properties exist in HubSpot."""
    results = {}

    for prop in BANT_PROPERTIES:
        try:
            url = f"{BASE_URL}/{prop['name']}"
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 200:
                results[prop['name']] = True
                logger.info(f"✓ Verified: {prop['name']}")
            else:
                results[prop['name']] = False
                logger.warning(f"✗ Not found: {prop['name']}")

        except requests.RequestException as e:
            results[prop['name']] = False
            logger.error(f"✗ Verification failed for {prop['name']}: {e}")

    return results


def main():
    """Main function to create and verify BANT properties."""
    logger.info("=" * 60)
    logger.info("HubSpot BANT Properties Creator")
    logger.info("=" * 60)

    # Create properties
    logger.info("\n[1/2] Creating BANT properties...")
    created_count = 0
    for prop in BANT_PROPERTIES:
        if create_property(prop):
            created_count += 1

    logger.info(f"\nCreated/verified: {created_count}/{len(BANT_PROPERTIES)} properties")

    # Verify properties
    logger.info("\n[2/2] Verifying properties exist...")
    verification = verify_properties()

    all_verified = all(verification.values())

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    if all_verified:
        logger.info("✓ All BANT properties created successfully!")
        logger.info("\n⚠ NEXT STEP (Manual Configuration Required):")
        logger.info("  HubSpot API does NOT support stage-specific required fields.")
        logger.info("  You must configure this manually in HubSpot UI:")
        logger.info("")
        logger.info("  1. Go to Settings → Objects → Deals → Pipelines")
        logger.info("  2. Select 'LaHaus AI' pipeline")
        logger.info("  3. Click 'Edit stages'")
        logger.info("  4. On 'Demo Presentado' stage, click 'Configure required properties'")
        logger.info("  5. Add all 4 BANT properties as required:")
        logger.info("     - Need (BANT)")
        logger.info("     - Budget (BANT)")
        logger.info("     - Authority (BANT)")
        logger.info("     - Timing (BANT)")
        logger.info("")
        logger.info("  After configuration, deals cannot move from 'Demo agendado' to")
        logger.info("  'Demo presentado' without filling all BANT fields.")
    else:
        logger.error("✗ Some properties failed to create:")
        for name, success in verification.items():
            status = "✓" if success else "✗"
            logger.info(f"  {status} {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
