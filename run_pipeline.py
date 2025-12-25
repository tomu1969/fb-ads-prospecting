#!/usr/bin/env python3
"""
FB Ads Library Prospecting Pipeline
Runs all modules in sequence with progress tracking and validation.

Usage:
    python run_pipeline.py           # Test mode (3 rows)
    python run_pipeline.py --all     # Full run (all rows)
    python run_pipeline.py --from 3  # Resume from module 3
"""

import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / "scripts"

MODULES = [
    {"num": 1, "name": "Loader", "script": "loader.py", "supports_all": False},
    {"num": 2, "name": "Enricher", "script": "enricher.py", "supports_all": True},
    {"num": 3, "name": "Scraper", "script": "scraper.py", "supports_all": True},
    {"num": 3.5, "name": "Hunter", "script": "hunter.py", "supports_all": True},
    {"num": 3.6, "name": "Agent Enricher", "script": "contact_enricher_pipeline.py", "supports_all": True},
    # Composer removed - email drafts will be done in HubSpot
    {"num": 4, "name": "Exporter", "script": "exporter.py", "supports_all": False},
    {"num": 5, "name": "Validator", "script": "validator.py", "supports_all": False},
]


def run_module(module, run_all=False):
    """Run a single module and return success status."""
    script_path = SCRIPTS_DIR / module["script"]

    if not script_path.exists():
        print(f"   ERROR: Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path)]
    if run_all and module["supports_all"]:
        cmd.append("--all")

    print(f"\n{'='*60}")
    print(f"MODULE {module['num']}: {module['name'].upper()}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(cmd)}\n")

    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=False,
            text=True
        )
        elapsed = time.time() - start_time

        if result.returncode == 0:
            print(f"\n   Completed in {elapsed:.1f}s")
            return True
        else:
            # Validator returns 1 when issues found (expected)
            if module["name"] == "Validator":
                print(f"\n   Completed in {elapsed:.1f}s (issues found)")
                return True
            print(f"\n   FAILED with exit code {result.returncode}")
            return False

    except Exception as e:
        print(f"\n   ERROR: {e}")
        return False


def main():
    run_all = "--all" in sys.argv

    # Check for --from flag
    start_from = 1
    if "--from" in sys.argv:
        idx = sys.argv.index("--from")
        if idx + 1 < len(sys.argv):
            try:
                start_from = float(sys.argv[idx + 1])
            except ValueError:
                print("ERROR: --from requires a module number (e.g., --from 3)")
                return 1

    mode = "FULL RUN" if run_all else "TEST MODE (3 rows)"
    print(f"\n{'#'*60}")
    print(f"# FB ADS LIBRARY PROSPECTING PIPELINE")
    print(f"# Mode: {mode}")
    if start_from > 1:
        print(f"# Starting from: Module {start_from}")
    print(f"{'#'*60}")

    total_start = time.time()
    failed_modules = []

    for module in MODULES:
        if module["num"] < start_from:
            print(f"\n   Skipping Module {module['num']}: {module['name']}")
            continue

        success = run_module(module, run_all)
        if not success and module["name"] != "Validator":
            failed_modules.append(module["name"])
            print(f"\n   Pipeline stopped due to failure in {module['name']}")
            break

    total_elapsed = time.time() - total_start

    print(f"\n{'#'*60}")
    print(f"# PIPELINE COMPLETE")
    print(f"# Total time: {total_elapsed/60:.1f} minutes")

    if failed_modules:
        print(f"# Status: FAILED")
        print(f"# Failed modules: {', '.join(failed_modules)}")
        print(f"{'#'*60}\n")
        return 1
    else:
        print(f"# Status: SUCCESS")
        print(f"{'#'*60}\n")
        return 0


if __name__ == "__main__":
    exit(main())
