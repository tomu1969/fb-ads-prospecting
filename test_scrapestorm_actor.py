#!/usr/bin/env python3
"""
Test script for alternative Facebook Ads Library Scraper actors.

Tests multiple actors to find one that properly handles housing ads category.

Usage:
    python test_scrapestorm_actor.py
"""

import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_API_KEY')

if not APIFY_API_TOKEN:
    print("ERROR: APIFY_API_TOKEN not set in environment")
    sys.exit(1)

try:
    from apify_client import ApifyClient
except ImportError:
    print("ERROR: apify-client not installed. Run: pip install apify-client")
    sys.exit(1)

client = ApifyClient(APIFY_API_TOKEN)


def test_actor(actor_id: str, description: str, run_input: dict) -> dict:
    """Run a test with the given input and print results."""
    print(f"\n{'='*60}")
    print(f"ACTOR: {actor_id}")
    print(f"TEST: {description}")
    print(f"{'='*60}")
    print(f"Input: {json.dumps(run_input, indent=2)}")
    print()

    try:
        run = client.actor(actor_id).call(run_input=run_input, timeout_secs=180)
        status = run.get('status')
        print(f"Status: {status}")

        if status == 'SUCCEEDED':
            dataset_id = run.get('defaultDatasetId')
            if dataset_id:
                items = list(client.dataset(dataset_id).iterate_items())
                print(f"Results: {len(items)} ads found")

                if items:
                    print("\nSample ad fields:")
                    sample = items[0]
                    for key in list(sample.keys())[:10]:
                        val = str(sample.get(key, ''))[:60]
                        print(f"  - {key}: {val}...")

                return {'success': True, 'count': len(items), 'items': items}
            else:
                print("No dataset returned")
                return {'success': False, 'count': 0, 'error': 'No dataset'}
        else:
            print(f"Actor failed with status: {status}")
            return {'success': False, 'count': 0, 'error': status}

    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: {error_msg}")
        return {'success': False, 'count': 0, 'error': error_msg}


def test_official_apify_actor():
    """Test the official apify/facebook-ads-scraper actor."""
    actor_id = 'apify/facebook-ads-scraper'
    print(f"\n{'#'*60}")
    print(f"# Testing: {actor_id}")
    print(f"{'#'*60}")

    results = {}

    # Test 1: All ads with URL (official actor uses startUrls as array of objects)
    results['all_ads'] = test_actor(
        actor_id,
        "All ads - miami (baseline)",
        {
            "startUrls": [
                {"url": "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&q=miami"}
            ],
            "maxAds": 5
        }
    )

    # Test 2: Housing ads via URL
    results['housing_url'] = test_actor(
        actor_id,
        "Housing ads via URL",
        {
            "startUrls": [
                {"url": "https://www.facebook.com/ads/library/?active_status=active&ad_type=housing_ads&country=US&q=miami"}
            ],
            "maxAds": 5
        }
    )

    # Test 3: Housing ads - no query (browse)
    results['housing_browse'] = test_actor(
        actor_id,
        "Housing ads - browse by filter (no keyword)",
        {
            "startUrls": [
                {"url": "https://www.facebook.com/ads/library/?active_status=active&ad_type=housing_ads&country=US"}
            ],
            "maxAds": 5
        }
    )

    return actor_id, results


def test_curious_coder_actor():
    """Test the curious_coder/facebook-ads-library-scraper actor."""
    actor_id = 'curious_coder/facebook-ads-library-scraper'
    print(f"\n{'#'*60}")
    print(f"# Testing: {actor_id}")
    print(f"{'#'*60}")

    results = {}

    # This actor uses URLs, not parameters
    # Test 1: All ads
    results['all_ads'] = test_actor(
        actor_id,
        "All ads - miami (baseline)",
        {
            "urls": [
                "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&q=miami"
            ],
            "maxResults": 5
        }
    )

    # Test 2: Housing ads via URL
    results['housing_url'] = test_actor(
        actor_id,
        "Housing ads via URL",
        {
            "urls": [
                "https://www.facebook.com/ads/library/?active_status=active&ad_type=housing_ads&country=US&q=miami"
            ],
            "maxResults": 5
        }
    )

    # Test 3: Housing ads - no keyword
    results['housing_browse'] = test_actor(
        actor_id,
        "Housing ads - browse (no keyword)",
        {
            "urls": [
                "https://www.facebook.com/ads/library/?active_status=active&ad_type=housing_ads&country=US"
            ],
            "maxResults": 5
        }
    )

    return actor_id, results


def main():
    print("Testing Facebook Ads Library Scrapers for Housing Ads Support")
    print(f"API Token: {APIFY_API_TOKEN[:10]}...")

    all_results = {}

    # Test official Apify actor
    try:
        actor_id, results = test_official_apify_actor()
        all_results[actor_id] = results
    except Exception as e:
        print(f"Failed to test apify/facebook-ads-scraper: {e}")

    # Test curious_coder actor
    try:
        actor_id, results = test_curious_coder_actor()
        all_results[actor_id] = results
    except Exception as e:
        print(f"Failed to test curious_coder: {e}")

    # Summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)

    working_actors = []

    for actor_id, results in all_results.items():
        print(f"\n{actor_id}:")
        housing_works = False
        for test_name, result in results.items():
            status = "PASS" if result['success'] and result['count'] > 0 else "FAIL"
            print(f"  {test_name}: {status} ({result['count']} ads)")
            if 'housing' in test_name.lower() and result['count'] > 0:
                housing_works = True

        if housing_works:
            working_actors.append(actor_id)

    print("\n" + "-"*60)
    if working_actors:
        print(f"WORKING ACTORS FOR HOUSING: {working_actors}")
        print(f"RECOMMENDATION: Use '{working_actors[0]}'")
        return 0
    else:
        print("NO ACTORS FOUND THAT SUPPORT HOUSING ADS")
        print("Consider using ad_type=all and filtering by keywords")
        return 1


if __name__ == '__main__':
    sys.exit(main())
