#!/usr/bin/env python3
"""
FB Ads Library Prospecting Pipeline
Runs all modules in sequence with progress tracking and validation.

Usage:
    python run_pipeline.py                           # Interactive file & mode selection
    python run_pipeline.py --all                     # Interactive file selection + full run
    python run_pipeline.py --test                    # Interactive file selection + test mode (3 rows)
    python run_pipeline.py --input path/to/file.csv  # Use custom input file (will prompt for mode)
    python run_pipeline.py --input path/to/file.csv --all  # Custom input + full run
    python run_pipeline.py --from 3                  # Resume from module 3
    python run_pipeline.py --fast                    # Fast Mode: Skip AI enricher (~10 min)
    python run_pipeline.py --speed-full              # Full Mode: Include AI enricher (~25-40 min)

Speed Modes:
    --fast         Uses Hunter.io + website scraping only. ~10 min, 60-70% email coverage.
    --speed-full   Includes AI-powered email discovery. ~25-40 min, 85-95% coverage.

Note: If --input is not provided, the script will prompt you to select a file
      from the input/ directory or enter a custom file path.
      If --all or --test is not provided, the script will prompt you to choose
      between test mode (3 rows) or full run (all rows).
"""

import os
import subprocess
import sys
import time
import shutil
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Import run ID utilities
sys.path.insert(0, str(Path(__file__).parent / "scripts"))
from utils.run_id import get_run_id, set_run_id_in_env
from utils.enrichment_config import (
    ENRICHMENT_TYPES, get_default_config, save_config_to_env,
    estimate_cost_and_time, get_cost_breakdown
)
from utils.strategy_suggester import suggest_enrichment_strategy, format_suggestions_for_display

BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / "scripts"

MODULES = [
    {"num": 1, "name": "Loader", "script": "loader.py", "supports_all": False},
    {"num": 2, "name": "Enricher", "script": "enricher.py", "supports_all": True},
    {"num": 3, "name": "Scraper", "script": "scraper.py", "supports_all": True},
    {"num": 3.5, "name": "Hunter", "script": "hunter.py", "supports_all": True},
    {"num": 3.6, "name": "Agent Enricher", "script": "contact_enricher_pipeline.py", "supports_all": True},
    {"num": 3.7, "name": "Instagram Enricher", "script": "instagram_enricher.py", "supports_all": True},
    # Composer removed - email drafts will be done in HubSpot
    {"num": 4, "name": "Exporter", "script": "exporter.py", "supports_all": False},
    {"num": 5, "name": "Validator", "script": "validator.py", "supports_all": False},
]


def run_module(module, run_all=False, input_file=None, run_id=None, enrichment_config=None):
    """Run a single module and return success status."""
    from utils.enrichment_config import should_run_module
    
    # Map module names to their script names for enrichment config checking
    module_name_to_script = {
        "Enricher": "enricher",
        "Scraper": "scraper",
        "Hunter": "hunter",
        "Agent Enricher": "contact_enricher",
        "Instagram Enricher": "instagram_enricher",
    }
    
    script_path = SCRIPTS_DIR / module["script"]

    if not script_path.exists():
        print(f"   ERROR: Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path)]
    if run_all and module["supports_all"]:
        cmd.append("--all")
    
    # Pass --input parameter to loader module
    if module["name"] == "Loader" and input_file:
        cmd.extend(["--input", str(input_file)])
        # In non-interactive mode, use existing field mapping automatically
        if run_all or "--skip-enrichment-selection" in sys.argv:
            cmd.append("--all")  # Tells loader to skip interactive prompts

    # Check if module should run based on enrichment config
    script_name = module_name_to_script.get(module["name"])
    should_run = True

    # Fast Mode: Skip Agent Enricher (contact_enricher) entirely
    if module["name"] == "Agent Enricher" and os.environ.get('SKIP_CONTACT_ENRICHER') == 'true':
        print(f"\n{'='*60}")
        print(f"MODULE {module['num']}: {module['name'].upper()}")
        print(f"{'='*60}")
        print(f"SKIPPED: Fast Mode enabled")

        # Create 03d_final.csv by copying 03b_hunter.csv for pipeline continuity
        from utils.run_id import get_versioned_filename, create_latest_symlink
        processed_dir = BASE_DIR / "processed"
        if run_id:
            hunter_file = processed_dir / get_versioned_filename("03b_hunter.csv", run_id)
            final_file = processed_dir / get_versioned_filename("03d_final.csv", run_id)
        else:
            hunter_file = processed_dir / "03b_hunter.csv"
            final_file = processed_dir / "03d_final.csv"

        if hunter_file.exists() or hunter_file.is_symlink():
            import shutil
            actual_hunter = hunter_file.resolve() if hunter_file.is_symlink() else hunter_file
            shutil.copy(actual_hunter, final_file)
            create_latest_symlink(str(final_file), "03d_final.csv")
            print(f"   Created {final_file.name} from Hunter output")

        print(f"{'='*60}\n")
        return True  # Return success to continue pipeline

    if script_name and enrichment_config:
        should_run = should_run_module(script_name, enrichment_config)
        if not should_run:
            print(f"\n{'='*60}")
            print(f"MODULE {module['num']}: {module['name'].upper()}")
            print(f"{'='*60}")
            print(f"SKIPPED: Enrichment type not selected in configuration")
            print(f"   This module enriches: {', '.join([ENRICHMENT_TYPES[et]['name'] for et in ['websites', 'emails', 'phones', 'contact_names', 'instagram_handles'] if script_name in ENRICHMENT_TYPES.get(et, {}).get('modules', [])])}")
            print(f"   Running module to create output files for pipeline continuity...")
            print(f"{'='*60}\n")
    
    print(f"\n{'='*60}")
    print(f"MODULE {module['num']}: {module['name'].upper()}")
    print(f"{'='*60}")
    if should_run:
        print(f"Running: {' '.join(cmd)}\n")
    else:
        print(f"Running (skip mode): {' '.join(cmd)}\n")

    start_time = time.time()

    # Set run ID in environment for module to read
    env = os.environ.copy()
    if run_id:
        env['PIPELINE_RUN_ID'] = run_id

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=False,
            text=True,
            env=env
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


def check_api_keys():
    """Check if required API keys are set."""
    missing = []
    if not os.getenv('OPENAI_API_KEY'):
        missing.append('OPENAI_API_KEY')
    if not os.getenv('HUNTER_API_KEY'):
        missing.append('HUNTER_API_KEY')
    
    if missing:
        print(f"\n⚠️  WARNING: Missing API keys: {', '.join(missing)}")
        print("   Some modules may fail. Set them in .env file or environment variables.")
        return False
    return True


def check_disk_space(min_gb=1):
    """Check available disk space."""
    try:
        stat = shutil.disk_usage(BASE_DIR)
        free_gb = stat.free / (1024**3)
        if free_gb < min_gb:
            print(f"\n⚠️  WARNING: Low disk space: {free_gb:.1f} GB available (need at least {min_gb} GB)")
            return False
        return True
    except Exception:
        # Can't check on some systems, skip
        return True


def get_row_count(input_file, run_all=None):
    """Get row count from input file.

    Args:
        input_file: Path to input file
        run_all: If True, return full count. If False, return min(count, 3).
                 If None, return full count (for display before user chooses mode).
    """
    if not input_file or not input_file.exists():
        return None

    try:
        import pandas as pd
        if input_file.suffix.lower() == '.csv':
            df = pd.read_csv(input_file)
        else:
            df = pd.read_excel(input_file)
        row_count = len(df)

        # Only limit to 3 if explicitly in test mode (run_all=False)
        if run_all is False:
            row_count = min(row_count, 3)  # Test mode

        return row_count
    except Exception:
        # Fallback: approximate count
        try:
            with open(input_file, 'rb') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header
            if run_all is False:
                row_count = min(row_count, 3)
            return row_count if row_count > 0 else None
        except Exception:
            return None


def estimate_runtime_and_cost(input_file, run_all, enrichment_config=None, parallel_factor=1.5):
    """
    Estimate runtime and cost for the pipeline run.
    
    Args:
        input_file: Path to input file
        run_all: Whether running full dataset
        enrichment_config: Dict of enrichment type -> enabled status
        parallel_factor: Speedup from parallel processing (default 1.5 = modest speedup)
                        Note: Actual parallelization varies by module, this is conservative
    """
    row_count = get_row_count(input_file, run_all)
    
    if row_count is None:
        return None, None
    
    # Use enrichment config if provided, otherwise default to all enabled
    if enrichment_config is None:
        enrichment_config = get_default_config()
    
    estimated_cost, estimated_minutes = estimate_cost_and_time(
        row_count, enrichment_config, parallel_factor
    )
    
    return estimated_minutes, estimated_cost


def interactive_file_selection() -> Path:
    """Interactive file selection from input directory or custom path."""
    input_dir = BASE_DIR / "input"
    supported_extensions = {'.csv', '.xlsx', '.xls', '.json', '.tsv'}
    
    # Get all supported files from input directory
    available_files = []
    if input_dir.exists():
        for file_path in sorted(input_dir.iterdir()):
            if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
                # Skip temporary Excel files
                if not file_path.name.startswith('~$'):
                    available_files.append(file_path)
    
    # Default file
    default_file = BASE_DIR / "input" / "FB Ad library scraping.xlsx"
    
    print("\n" + "=" * 60)
    print("SELECT INPUT FILE")
    print("=" * 60)
    
    if available_files:
        print("\nAvailable files in input/ directory:")
        for idx, file_path in enumerate(available_files, 1):
            file_size = file_path.stat().st_size / 1024  # Size in KB
            print(f"  [{idx}] {file_path.name} ({file_size:.1f} KB)")
    
    # Add options
    menu_start = len(available_files) + 1
    print(f"\n  [{menu_start}] Enter custom file path")
    if default_file.exists():
        print(f"  [{menu_start + 1}] Use default: {default_file.name}")
        default_option = menu_start + 1
    else:
        default_option = None
    
    # Get user selection
    while True:
        try:
            max_choice = menu_start + (1 if default_option else 0)
            if available_files:
                choice = input(f"\nEnter choice (1-{max_choice}): ").strip()
            else:
                # No files in directory, only custom path option
                choice = input(f"\nEnter choice (1 for custom path): ").strip()
            
            # Try to parse as number
            try:
                choice_num = int(choice)
                
                # Check if it's a file selection
                if 1 <= choice_num <= len(available_files):
                    selected_file = available_files[choice_num - 1]
                    if selected_file.exists():
                        print(f"✓ Selected: {selected_file.name}")
                        return selected_file
                    else:
                        print(f"✗ File not found: {selected_file}")
                        continue
                
                # Check if it's custom path option
                elif choice_num == menu_start:
                    custom_path = input("Enter file path (absolute or relative): ").strip()
                    if not custom_path:
                        print("✗ No path entered. Please try again.")
                        continue
                    
                    # Handle quoted paths (for paths with spaces)
                    if custom_path.startswith('"') and custom_path.endswith('"'):
                        custom_path = custom_path[1:-1]
                    elif custom_path.startswith("'") and custom_path.endswith("'"):
                        custom_path = custom_path[1:-1]
                    
                    selected_file = Path(custom_path)
                    if not selected_file.is_absolute():
                        selected_file = BASE_DIR / selected_file
                    
                    if selected_file.exists():
                        print(f"✓ Selected: {selected_file}")
                        return selected_file
                    else:
                        print(f"✗ File not found: {selected_file}")
                        print("   Please check the path and try again.")
                        continue
                
                # Check if it's default option
                elif default_option and choice_num == default_option:
                    if default_file.exists():
                        print(f"✓ Using default: {default_file.name}")
                        return default_file
                    else:
                        print(f"✗ Default file not found: {default_file}")
                        continue
                
                else:
                    print(f"✗ Invalid choice. Please enter a number between 1 and {max_choice}.")
                    continue
            
            except ValueError:
                # Not a number, treat as direct file path
                custom_path = choice
                if not custom_path:
                    print("✗ No path entered. Please try again.")
                    continue
                
                # Handle quoted paths
                if custom_path.startswith('"') and custom_path.endswith('"'):
                    custom_path = custom_path[1:-1]
                elif custom_path.startswith("'") and custom_path.endswith("'"):
                    custom_path = custom_path[1:-1]
                
                selected_file = Path(custom_path)
                if not selected_file.is_absolute():
                    selected_file = BASE_DIR / selected_file
                
                if selected_file.exists():
                    print(f"✓ Selected: {selected_file}")
                    return selected_file
                else:
                    print(f"✗ File not found: {selected_file}")
                    print("   Please check the path and try again.")
                    continue
        
        except KeyboardInterrupt:
            print("\n\nCancelled by user.")
            sys.exit(1)
        except Exception as e:
            print(f"✗ Error: {e}")
            print("   Please try again.")


def interactive_enrichment_selection_with_ai(input_file: Path, run_all: bool, initial_email_depth: str = 'thorough') -> dict:
    """
    AI-first enrichment selection: Analyze input, show recommendations, let user customize.

    Args:
        input_file: Path to input file
        run_all: Whether running full dataset (ignored - we show full count for planning)
        initial_email_depth: Starting email depth ('basic' or 'thorough')

    Returns:
        Dict mapping enrichment types to enabled status (includes email_depth)
    """
    from utils.enrichment_config import EMAIL_DEPTH_OPTIONS

    # Always get FULL row count for enrichment planning (user picks test/full mode later)
    row_count = get_row_count(input_file, run_all=None)  # None = get full count
    if row_count is None:
        row_count = 100  # Fallback estimate

    print("\n" + "=" * 60)
    print("ENRICHMENT CONFIGURATION")
    print("=" * 60)
    print(f"\nAnalyzing input data for {row_count} contact(s)...")

    # Start with default config for AI analysis
    default_config = get_default_config()
    default_config['email_depth'] = initial_email_depth
    
    # Get AI recommendations first
    recommended_config = default_config
    ai_suggestions = None
    
    if os.getenv('OPENAI_API_KEY') and "--skip-ai-suggestions" not in sys.argv:
        print("🤖 Generating AI-powered recommendations...")
        try:
            suggestions = suggest_enrichment_strategy(input_file, default_config, row_count)
            
            if 'error' not in suggestions and 'recommended_config' in suggestions:
                ai_suggestions = suggestions
                recommended_config = suggestions['recommended_config']
                print("✓ AI analysis complete")
            else:
                print(f"⚠️  Could not generate AI suggestions: {suggestions.get('error', 'Unknown error')}")
                print("   Using default configuration (all enrichments enabled)")
        except Exception as e:
            print(f"⚠️  Error generating AI suggestions: {e}")
            print("   Using default configuration (all enrichments enabled)")
    else:
        print("⚠️  OpenAI API key not found. Using default configuration.")
        print("   (Set OPENAI_API_KEY in .env for AI-powered recommendations)")
    
    # Show AI recommendations if available
    if ai_suggestions:
        print(format_suggestions_for_display(ai_suggestions))
    
    # Start with recommended config
    config = recommended_config.copy()
    
    # Calculate estimates for recommended config
    estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=1.5)
    breakdown = get_cost_breakdown(row_count, config)
    
    # Show recommended configuration and allow customization
    while True:
        print("\n" + "=" * 60)
        print("RECOMMENDED ENRICHMENT CONFIGURATION")
        print("=" * 60)
        print(f"\nProcessing {row_count} contact(s) with current configuration:\n")
        
        for idx, (enrichment_type, info) in enumerate(ENRICHMENT_TYPES.items(), 1):
            enabled = config.get(enrichment_type, False)
            checkbox = "[✓]" if enabled else "[ ]"

            # Get cost/time for this enrichment type
            type_breakdown = breakdown.get(enrichment_type, {'cost': 0.0, 'time_minutes': 0.0})
            cost_str = f"${type_breakdown['cost']:.2f}" if type_breakdown['cost'] > 0 else "Free"
            time_str = f"{type_breakdown['time_minutes']:.1f} min" if type_breakdown['time_minutes'] > 0.1 else "<0.1 min"

            # Show if this was recommended by AI
            recommended_by_ai = ""
            if ai_suggestions and recommended_config.get(enrichment_type) == enabled:
                recommended_by_ai = " (AI recommended)" if enabled else " (AI skipped)"

            print(f"  [{idx}] {checkbox} {info['name']}{recommended_by_ai}")
            print(f"      {info['description']}")
            print(f"      Cost: {cost_str} | Time: {time_str}")

            # Show email depth option if this is emails and enabled
            if enrichment_type == 'emails' and enabled:
                email_depth = config.get('email_depth', 'thorough')
                depth_info = EMAIL_DEPTH_OPTIONS.get(email_depth, {})
                depth_name = depth_info.get('name', email_depth)
                print(f"      Depth: {depth_name}")
                print(f"      (Press 'd' to toggle depth)")
        
        # Show totals
        print("\n" + "-" * 60)
        print("TOTALS:")
        print(f"  Estimated cost: ${estimated_cost:.2f}")
        print(f"  Estimated time: {estimated_minutes:.1f} minutes ({estimated_minutes/60:.1f} hours)")
        print("-" * 60)
        
        print("\nOptions:")
        print("  [Enter] Accept and continue")
        print("  [1-5] Toggle enrichment type")
        print("  [d] Toggle email depth (Basic ↔ Thorough)")
        print("  [a] Enable all enrichments")
        print("  [n] Disable all enrichments")
        print("  [r] Reset to AI recommendations")
        print("  [q] Quit")
        
        try:
            choice = input("\nEnter choice: ").strip().lower()
            
            if choice == '':
                # User pressed Enter, confirm and return
                enabled_count = sum(1 for v in config.values() if v)
                if enabled_count == 0:
                    print("\n⚠️  No enrichments selected. At least one is required.")
                    continue
                
                print(f"\n✓ Configuration accepted: {enabled_count} enrichment type(s) enabled")
                return config
            
            elif choice == 'q':
                print("\nCancelled by user.")
                sys.exit(1)
            
            elif choice == 'a':
                current_depth = config.get('email_depth', 'thorough')  # Preserve current depth
                config = get_default_config()
                config['email_depth'] = current_depth
                estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=1.5)
                breakdown = get_cost_breakdown(row_count, config)
                print("✓ All enrichments enabled")
                continue
            
            elif choice == 'n':
                current_depth = config.get('email_depth', 'thorough')  # Save before reset
                config = {k: False for k in ENRICHMENT_TYPES.keys()}
                config['email_depth'] = current_depth  # Preserve email_depth
                estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=1.5)
                breakdown = get_cost_breakdown(row_count, config)
                print("✓ All enrichments disabled")
                continue

            elif choice == 'd':
                # Toggle email depth between basic and thorough
                current_depth = config.get('email_depth', 'thorough')
                new_depth = 'basic' if current_depth == 'thorough' else 'thorough'
                config['email_depth'] = new_depth
                depth_info = EMAIL_DEPTH_OPTIONS.get(new_depth, {})
                estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=1.5)
                breakdown = get_cost_breakdown(row_count, config)
                print(f"✓ Email depth set to: {depth_info.get('name', new_depth)}")
                continue

            elif choice == 'r' and ai_suggestions:
                config = recommended_config.copy()
                estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=3.0)
                breakdown = get_cost_breakdown(row_count, config)
                print("✓ Reset to AI recommendations")
                continue
            
            else:
                try:
                    choice_num = int(choice)
                    if 1 <= choice_num <= len(ENRICHMENT_TYPES):
                        enrichment_type = list(ENRICHMENT_TYPES.keys())[choice_num - 1]
                        config[enrichment_type] = not config.get(enrichment_type, False)
                        estimated_cost, estimated_minutes = estimate_cost_and_time(row_count, config, parallel_factor=1.5)
                        breakdown = get_cost_breakdown(row_count, config)
                        status = "enabled" if config[enrichment_type] else "disabled"
                        print(f"✓ {ENRICHMENT_TYPES[enrichment_type]['name']} {status}")
                        continue
                    else:
                        print(f"✗ Invalid choice. Please enter 1-{len(ENRICHMENT_TYPES)}, 'a', 'n', 'r', or press Enter.")
                        continue
                except ValueError:
                    print(f"✗ Invalid choice. Please enter 1-{len(ENRICHMENT_TYPES)}, 'a', 'n', 'r', or press Enter.")
                    continue
        
        except KeyboardInterrupt:
            print("\n\nCancelled by user.")
            sys.exit(1)
        except Exception as e:
            print(f"✗ Error: {e}")
            print("   Please try again.")


# NOTE: interactive_speed_mode_selection() has been removed.
# Speed mode (email_depth) is now integrated into interactive_enrichment_selection_with_ai()
# Use 'd' to toggle between Basic (Hunter only) and Thorough (Hunter + AI agents)


def interactive_run_mode_selection(input_file: Path, enrichment_config: dict = None) -> bool:
    """Interactive run mode selection (test mode vs full run)."""
    # Get estimates for both modes (will be updated after enrichment selection)
    test_estimated_minutes, test_estimated_cost = estimate_runtime_and_cost(
        input_file, run_all=False, enrichment_config=enrichment_config
    )
    full_estimated_minutes, full_estimated_cost = estimate_runtime_and_cost(
        input_file, run_all=True, enrichment_config=enrichment_config
    )
    
    print("\n" + "=" * 60)
    print("SELECT RUN MODE")
    print("=" * 60)
    
    print("\n[1] Test mode (3 rows)")
    if test_estimated_minutes:
        print(f"    Estimated runtime: {test_estimated_minutes:.1f} minutes")
        if test_estimated_cost:
            print(f"    Estimated cost: ${test_estimated_cost:.2f}")
    print("    Faster, for testing pipeline functionality")
    
    print("\n[2] Full run (all rows)")
    if full_estimated_minutes:
        print(f"    Estimated runtime: {full_estimated_minutes:.1f} minutes ({full_estimated_minutes/60:.1f} hours)")
        if full_estimated_cost:
            print(f"    Estimated cost: ${full_estimated_cost:.2f}")
    print("    Processes entire dataset")
    
    # Get user selection
    while True:
        try:
            choice = input("\nEnter choice (1-2): ").strip()
            
            if choice == '1':
                print("✓ Selected: Test mode (3 rows)")
                return False  # Test mode
            elif choice == '2':
                print("✓ Selected: Full run (all rows)")
                if full_estimated_minutes and full_estimated_minutes > 60:
                    print(f"\n⚠️  This will take approximately {full_estimated_minutes/60:.1f} hours.")
                    confirm = input("   Continue? (y/n): ").strip().lower()
                    if confirm != 'y':
                        print("   Cancelled. Please select test mode or exit.")
                        continue
                return True  # Full run
            else:
                print("✗ Invalid choice. Please enter 1 or 2.")
                continue
        
        except KeyboardInterrupt:
            print("\n\nCancelled by user.")
            sys.exit(1)
        except Exception as e:
            print(f"✗ Error: {e}")
            print("   Please try again.")


def preflight_checks(input_file, run_all, enrichment_config=None):
    """Run pre-flight checks before starting pipeline."""
    print(f"\n{'='*60}")
    print("PRE-FLIGHT CHECKS")
    print(f"{'='*60}")
    
    checks_passed = True
    
    # Check API keys
    if not check_api_keys():
        checks_passed = False
    
    # Check disk space
    if not check_disk_space():
        checks_passed = False
    
    # Estimate runtime and cost
    estimated_minutes, estimated_cost = estimate_runtime_and_cost(
        input_file, run_all, enrichment_config=enrichment_config
    )
    if estimated_minutes:
        print(f"\n📊 ESTIMATES:")
        print(f"   Estimated runtime: {estimated_minutes:.1f} minutes ({estimated_minutes/60:.1f} hours)")
        if estimated_cost:
            print(f"   Estimated cost: ${estimated_cost:.2f} (OpenAI + Hunter.io)")
    
    # Confirm for full runs
    if run_all and estimated_minutes and estimated_minutes > 60:
        print(f"\n⚠️  This is a FULL RUN that will take approximately {estimated_minutes/60:.1f} hours.")
        print("   Press Ctrl+C to cancel, or wait 5 seconds to continue...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n   Cancelled by user.")
            return False
    
    print(f"{'='*60}\n")
    return checks_passed


def main():
    # Check for explicit run mode flags
    if "--all" in sys.argv or "--full" in sys.argv:
        run_all = True  # Explicit full run
    elif "--test" in sys.argv:
        run_all = False  # Explicit test mode
    else:
        run_all = None  # Will be determined interactively

    # Check for --input flag OR positional argument for input file
    input_file = None
    if "--input" in sys.argv:
        idx = sys.argv.index("--input")
        if idx + 1 < len(sys.argv):
            input_path_str = sys.argv[idx + 1]
            # Handle quoted paths (for paths with spaces)
            if input_path_str.startswith('"') and input_path_str.endswith('"'):
                input_path_str = input_path_str[1:-1]
            elif input_path_str.startswith("'") and input_path_str.endswith("'"):
                input_path_str = input_path_str[1:-1]

            input_file = Path(input_path_str)
            if not input_file.is_absolute():
                input_file = BASE_DIR / input_file
            if not input_file.exists():
                print(f"ERROR: Input file not found: {input_file}")
                return 1
    else:
        # Check for positional argument (first arg that's not a flag)
        for arg in sys.argv[1:]:
            if not arg.startswith('-') and (arg.endswith('.csv') or arg.endswith('.xlsx')):
                input_path_str = arg
                input_file = Path(input_path_str)
                if not input_file.is_absolute():
                    input_file = BASE_DIR / input_file
                if input_file.exists():
                    break
                else:
                    print(f"ERROR: Input file not found: {input_file}")
                    return 1

        # No input file provided, use interactive selection
        if input_file is None:
            input_file = interactive_file_selection()

    # Handle --fast flag (sets email_depth to 'basic')
    # Speed mode is now controlled via email_depth in config, not a separate selection
    initial_email_depth = 'thorough'  # Default
    if "--fast" in sys.argv:
        initial_email_depth = 'basic'
        os.environ['PIPELINE_SPEED_MODE'] = 'fast'  # For backward compatibility
        os.environ['SKIP_CONTACT_ENRICHER'] = 'true'  # For backward compatibility
        print("Using Fast Mode (Hunter Only) - email_depth=basic")
    elif "--speed-full" in sys.argv:
        initial_email_depth = 'thorough'
        os.environ['PIPELINE_SPEED_MODE'] = 'full'
        print("Using Full Enrichment Mode - email_depth=thorough")

    # AI-first enrichment selection (analyze first, then let user customize)
    # Speed mode is now integrated into enrichment selection via email_depth option
    enrichment_config = None
    if "--skip-enrichment-selection" not in sys.argv:
        enrichment_config = interactive_enrichment_selection_with_ai(
            input_file,
            run_all if run_all is not None else False,
            initial_email_depth=initial_email_depth
        )
        save_config_to_env(enrichment_config)

        # Set env vars for backward compatibility with modules that check them
        if enrichment_config.get('email_depth') == 'basic':
            os.environ['SKIP_CONTACT_ENRICHER'] = 'true'
            os.environ['PIPELINE_SPEED_MODE'] = 'fast'
        else:
            os.environ['SKIP_CONTACT_ENRICHER'] = 'false'
            os.environ['PIPELINE_SPEED_MODE'] = 'full'
    else:
        # Load from env if available, otherwise default to all
        from utils.enrichment_config import load_config_from_env, get_default_config
        enrichment_config = load_config_from_env() or get_default_config()
        enrichment_config['email_depth'] = initial_email_depth
        save_config_to_env(enrichment_config)
    
    # Interactive run mode selection if not explicitly set
    if run_all is None:
        run_all = interactive_run_mode_selection(input_file, enrichment_config)

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

    # Run pre-flight checks
    if not preflight_checks(input_file, run_all, enrichment_config):
        return 1

    # Generate run ID at pipeline start
    run_id = get_run_id(input_file=input_file)
    set_run_id_in_env(run_id)
    
    mode = "FULL RUN" if run_all else "TEST MODE (3 rows)"
    print(f"\n{'#'*60}")
    print(f"# FB ADS LIBRARY PROSPECTING PIPELINE")
    print(f"# Mode: {mode}")
    print(f"# Run ID: {run_id}")
    if input_file:
        print(f"# Input file: {input_file}")
    if start_from > 1:
        print(f"# Starting from: Module {start_from}")
    print(f"{'#'*60}")

    # === LOGGING: Show enrichment configuration ===
    print(f"\n{'='*60}")
    print("ENRICHMENT CONFIGURATION")
    print(f"{'='*60}")
    print("\nEnrichment types:")
    from utils.enrichment_config import EMAIL_DEPTH_OPTIONS
    for etype, enabled in (enrichment_config or {}).items():
        if etype == 'email_depth':
            depth_info = EMAIL_DEPTH_OPTIONS.get(enabled, {})
            print(f"  email_depth: {depth_info.get('name', enabled)}")
        else:
            status = "✓ ENABLED" if enabled else "✗ disabled"
            print(f"  {etype}: {status}")

    print("\nModules to run:")
    from utils.enrichment_config import should_run_module, MODULE_TO_ENRICHMENT
    for module in MODULES:
        module_key = {
            "Enricher": "enricher",
            "Scraper": "scraper",
            "Hunter": "hunter",
            "Agent Enricher": "contact_enricher",
            "Instagram Enricher": "instagram_enricher",
        }.get(module["name"])

        if module_key:
            will_run = should_run_module(module_key, enrichment_config)
            enriches = MODULE_TO_ENRICHMENT.get(module_key, [])
            enriches_str = f" ({', '.join(enriches)})" if enriches else ""
            status = "→ WILL RUN" if will_run else "→ SKIP"
            print(f"  {module['num']} {module['name']}{enriches_str}: {status}")
        else:
            print(f"  {module['num']} {module['name']}: → WILL RUN")

    print(f"\nEnvironment:")
    print(f"  SKIP_CONTACT_ENRICHER: {os.environ.get('SKIP_CONTACT_ENRICHER', 'not set')}")
    print(f"  PIPELINE_SPEED_MODE: {os.environ.get('PIPELINE_SPEED_MODE', 'not set')}")
    print(f"{'='*60}\n")

    total_start = time.time()
    failed_modules = []
    modules_run = []
    modules_skipped = []

    for module in MODULES:
        if module["num"] < start_from:
            print(f"\n   Skipping Module {module['num']}: {module['name']}")
            continue

        success = run_module(module, run_all, input_file, run_id, enrichment_config)
        if not success and module["name"] != "Validator":
            failed_modules.append(module["name"])
            print(f"\n   Pipeline stopped due to failure in {module['name']}")
            break

    total_elapsed = time.time() - total_start

    # === LOGGING: Pipeline summary with field coverage ===
    print(f"\n{'='*60}")
    print("PIPELINE SUMMARY")
    print(f"{'='*60}")

    # Try to read the final output and show field coverage
    try:
        import pandas as pd
        from utils.run_id import get_versioned_filename

        # Find the final output file
        if run_id:
            final_file = BASE_DIR / "processed" / get_versioned_filename("03d_final.csv", run_id)
        else:
            final_file = BASE_DIR / "processed" / "03d_final.csv"

        if final_file.exists() or (final_file.is_symlink() and final_file.resolve().exists()):
            df = pd.read_csv(final_file)
            total_rows = len(df)

            print(f"\nOutput: {final_file.name} ({total_rows} rows)")
            print(f"\nField coverage:")

            # Key fields to check
            key_fields = [
                ('contact_name', 'Contact names'),
                ('primary_email', 'Primary emails'),
                ('hunter_contact_name', 'Hunter names'),
                ('pipeline_name', 'Pipeline names'),
                ('instagram_handles', 'Instagram handles'),
                ('phones', 'Phone numbers'),
            ]

            warnings = []
            for field, label in key_fields:
                if field in df.columns:
                    # Count non-empty values
                    def is_filled(val):
                        if pd.isna(val):
                            return False
                        val_str = str(val).strip().lower()
                        return val_str not in ['', 'nan', 'none', 'none none', 'null', '[]']

                    filled = df[field].apply(is_filled).sum()
                    pct = 100 * filled / total_rows if total_rows > 0 else 0
                    status = ""
                    if pct < 30:
                        status = " ← LOW"
                        warnings.append(f"{label}: only {pct:.0f}%")
                    print(f"  {label}: {filled}/{total_rows} ({pct:.0f}%){status}")
                else:
                    print(f"  {label}: MISSING COLUMN ← Module likely skipped")
                    warnings.append(f"{label} column missing")

            # Check which modules actually added columns
            print(f"\nModule output verification:")
            hunter_cols = ['hunter_contact_name', 'hunter_emails', 'primary_email']
            hunter_added = any(c in df.columns for c in hunter_cols)
            print(f"  Hunter module: {'✓ Columns present' if hunter_added else '✗ No columns added - SKIPPED?'}")

            pipeline_cols = ['pipeline_name', 'pipeline_email', 'enrichment_stage']
            pipeline_added = any(c in df.columns for c in pipeline_cols)
            print(f"  Contact Enricher: {'✓ Columns present' if pipeline_added else '✗ No columns added - SKIPPED?'}")

            if warnings:
                print(f"\n⚠️  Warnings:")
                for w in warnings:
                    print(f"    - {w}")

    except Exception as e:
        print(f"\n  Could not read final output: {e}")

    print(f"{'='*60}")

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
