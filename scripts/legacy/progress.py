#!/usr/bin/env python3
"""
Pipeline Progress Tracker
Run in a separate terminal: python scripts/progress.py
Updates every 2 seconds showing real-time pipeline status.
"""

import time
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "processed"
OUTPUT_DIR = BASE_DIR / "output"

# Pipeline stages with expected input/output
STAGES = [
    {"name": "Loader", "input": None, "output": "01_loaded.csv", "expected": 59},
    {"name": "Enricher", "input": "01_loaded.csv", "output": "02_enriched.csv", "expected": 59},
    {"name": "Scraper", "input": "02_enriched.csv", "output": "03_contacts.csv", "expected": None},
    {"name": "Hunter", "input": "03_contacts.csv", "output": "03b_hunter.csv", "expected": None},
    {"name": "Composer", "input": "03b_hunter.csv", "output": "04_emails.csv", "expected": None},
    {"name": "Exporter", "input": "04_emails.csv", "output": "prospects_final.csv", "expected": None, "output_dir": OUTPUT_DIR},
]


def count_rows(filepath):
    """Count data rows in CSV (excluding header)."""
    if not filepath.exists():
        return 0
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return max(0, len(lines) - 1)  # Subtract header
    except Exception:
        return 0


def get_file_mtime(filepath):
    """Get file modification time."""
    if not filepath.exists():
        return None
    return datetime.fromtimestamp(filepath.stat().st_mtime)


def format_time(dt):
    """Format datetime for display."""
    if dt is None:
        return "N/A"
    return dt.strftime("%H:%M:%S")


def progress_bar(current, total, width=20):
    """Create ASCII progress bar."""
    if total == 0:
        return "[" + " " * width + "]"
    filled = int(width * current / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}]"


def clear_screen():
    """Clear terminal screen."""
    os.system('clear' if os.name != 'nt' else 'cls')


def print_status():
    """Print current pipeline status."""
    clear_screen()

    print("=" * 60)
    print("  FB ADS LIBRARY PROSPECTING - PIPELINE STATUS")
    print("=" * 60)
    print(f"  Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)

    # Track which stage is currently active
    current_stage = None
    prev_rows = 59  # Expected input from loader

    for i, stage in enumerate(STAGES):
        output_dir = stage.get("output_dir", PROCESSED_DIR)
        output_path = output_dir / stage["output"]

        rows = count_rows(output_path)
        mtime = get_file_mtime(output_path)
        expected = stage.get("expected") or prev_rows

        # Determine stage status
        if rows == 0:
            status = "⏳ PENDING"
            status_color = "\033[90m"  # Gray
        elif rows < expected and rows > 0:
            status = "🔄 RUNNING"
            status_color = "\033[93m"  # Yellow
            current_stage = stage["name"]
        else:
            status = "✅ DONE"
            status_color = "\033[92m"  # Green

        # Build progress display
        pbar = progress_bar(rows, expected)
        pct = (rows / expected * 100) if expected > 0 else 0

        print(f"\n  {i+1}. {stage['name']}")
        print(f"     {status_color}{status}\033[0m  {pbar} {rows}/{expected} ({pct:.0f}%)")
        print(f"     Output: {stage['output']}")
        if mtime:
            print(f"     Last updated: {format_time(mtime)}")

        # Update expected for next stage
        if rows > 0:
            prev_rows = rows

    print("\n" + "-" * 60)

    # Summary
    output_csv = OUTPUT_DIR / "prospects_final.csv"
    output_json = OUTPUT_DIR / "email_drafts.json"

    if output_csv.exists() and output_json.exists():
        final_rows = count_rows(output_csv)
        print(f"\n  🎉 PIPELINE COMPLETE!")
        print(f"     Final prospects: {final_rows}")
        print(f"     Output files:")
        print(f"       - output/prospects_final.csv")
        print(f"       - output/email_drafts.json")
    elif current_stage:
        print(f"\n  ⏳ Currently running: {current_stage}")
        print(f"     Keep this window open to monitor progress...")
    else:
        print(f"\n  📋 Pipeline ready to run")
        print(f"     Run: python scripts/enricher.py --all")

    print("\n" + "=" * 60)
    print("  Press Ctrl+C to exit")
    print("=" * 60)


def main():
    """Main loop - update status every 2 seconds."""
    print("Starting pipeline monitor...")

    try:
        while True:
            print_status()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")


if __name__ == "__main__":
    main()
