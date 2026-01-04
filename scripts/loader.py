"""
Module 1: Smart Input Adapter
Accepts any input file format, uses OpenAI to understand structure, maps fields to pipeline schema through interactive verification.

Input: Any CSV, Excel, JSON, or TSV file
Output: processed/01_loaded.csv (standardized pipeline format)

Usage:
    python scripts/loader.py                    # Use default input file
    python scripts/loader.py --input path/to/file.csv
    python scripts/loader.py --input path/to/file.csv --mapping config/field_mappings/mapping.json
"""

import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT_FILE = BASE_DIR / "input" / "FB Ad library scraping.xlsx"

# Import run ID utilities
import sys
sys.path.insert(0, str(Path(__file__).parent))
from utils.run_id import get_run_id_from_env, get_versioned_filename, create_latest_symlink

def get_output_file():
    """Get versioned output file path based on run ID."""
    base_name = "01_loaded.csv"
    run_id = get_run_id_from_env()
    
    if run_id:
        versioned_name = get_versioned_filename(base_name, run_id)
        output_file = BASE_DIR / "processed" / versioned_name
    else:
        # Fallback to default if no run ID
        output_file = BASE_DIR / "processed" / base_name
    
    return output_file, base_name

OUTPUT_FILE = None  # Will be set dynamically
MAPPINGS_DIR = BASE_DIR / "config" / "field_mappings"

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Pipeline required schema
REQUIRED_SCHEMA = {
    "page_name": {
        "required": True,
        "description": "Company or business name",
        "type": "string"
    },
    "ad_count": {
        "required": False,
        "description": "Number of ads/entries",
        "type": "integer",
        "default": 1
    },
    "total_page_likes": {
        "required": False,
        "description": "Social media metrics (likes, followers)",
        "type": "integer",
        "default": 0
    },
    "ad_texts": {
        "required": False,
        "description": "Marketing text or descriptions",
        "type": "list",
        "default": [""]
    },
    "platforms": {
        "required": False,
        "description": "Platforms where content appears",
        "type": "list",
        "default": ["UNKNOWN"]
    },
    "is_active": {
        "required": False,
        "description": "Active status",
        "type": "boolean",
        "default": True
    },
    "first_ad_date": {
        "required": False,
        "description": "Date of first appearance",
        "type": "date",
        "default": datetime.now().strftime("%Y-%m-%d")
    },
    "primary_email": {
        "required": False,
        "description": "Contact email address",
        "type": "string",
        "default": ""
    },
    "phones": {
        "required": False,
        "description": "Phone numbers (can be multiple)",
        "type": "list",
        "default": []
    },
    "contact_name": {
        "required": False,
        "description": "Contact person name",
        "type": "string",
        "default": ""
    },
    "contact_position": {
        "required": False,
        "description": "Contact job title/position",
        "type": "string",
        "default": ""
    }
}


def detect_file_format(file_path: Path) -> str:
    """Detect file format from extension."""
    ext = file_path.suffix.lower()
    format_map = {
        '.csv': 'csv',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.json': 'json',
        '.tsv': 'tsv'
    }
    return format_map.get(ext, 'unknown')


def load_file(file_path: Path) -> pd.DataFrame:
    """Load file based on detected format."""
    format_type = detect_file_format(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    try:
        if format_type == 'csv':
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    return pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not decode CSV file with common encodings")
        
        elif format_type == 'excel':
            return pd.read_excel(file_path)
        
        elif format_type == 'json':
            return pd.read_json(file_path)
        
        elif format_type == 'tsv':
            return pd.read_csv(file_path, sep='\t', encoding='utf-8')
        
        else:
            raise ValueError(f"Unsupported file format: {format_type}")
    
    except Exception as e:
        raise ValueError(f"Error loading file: {str(e)}")


def analyze_schema_with_openai(df: pd.DataFrame) -> Dict[str, Any]:
    """Use OpenAI to analyze file structure and suggest field mappings."""
    print("\n🤖 Analyzing file structure with AI...")
    
    # Prepare sample data for AI
    column_names = list(df.columns)
    sample_rows = df.head(3).to_dict('records')
    
    # Create preview text
    preview_text = f"Columns: {', '.join(column_names)}\n\n"
    preview_text += "Sample data (first 3 rows):\n"
    for i, row in enumerate(sample_rows, 1):
        preview_text += f"\nRow {i}:\n"
        for col, val in row.items():
            preview_text += f"  {col}: {str(val)[:100]}\n"
    
    prompt = f"""Analyze this dataset and map fields to a real estate prospecting pipeline schema.

INPUT FILE PREVIEW:
{preview_text}

REQUIRED PIPELINE SCHEMA:
1. page_name (REQUIRED): Company or business name
2. ad_count (optional): Number of ads/entries
3. total_page_likes (optional): Social media metrics (likes, followers, etc.)
4. ad_texts (optional): Marketing text or descriptions
5. platforms (optional): Platforms where content appears (e.g., Facebook, Instagram)
6. is_active (optional): Active status (boolean)
7. first_ad_date (optional): Date of first appearance

TASK:
For each pipeline field, suggest the best matching column from the input file.
If no good match exists, return null.

Return ONLY a valid JSON object with this exact structure:
{{
  "page_name": "exact_column_name_from_input or null",
  "ad_count": "exact_column_name_from_input or null",
  "total_page_likes": "exact_column_name_from_input or null",
  "ad_texts": "exact_column_name_from_input or null",
  "platforms": "exact_column_name_from_input or null",
  "is_active": "exact_column_name_from_input or null",
  "first_ad_date": "exact_column_name_from_input or null",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation of mapping choices"
}}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at analyzing data schemas and mapping fields. Always return valid JSON only, no additional text."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        content = response.choices[0].message.content.strip()
        
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        mapping = json.loads(content)
        return mapping
    
    except json.JSONDecodeError as e:
        print(f"⚠️  Error parsing AI response: {e}")
        print(f"Response was: {content[:200]}")
        return {}
    except Exception as e:
        print(f"⚠️  Error calling OpenAI: {e}")
        return {}


def display_preview(df: pd.DataFrame, file_path: Path):
    """Display file preview to user."""
    print("\n" + "=" * 70)
    print("FILE PREVIEW")
    print("=" * 70)
    print(f"File: {file_path.name}")
    print(f"Format: {detect_file_format(file_path)}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nColumn names: {', '.join(df.columns.tolist())}")
    print(f"\nFirst 3 rows:")
    print(df.head(3).to_string(max_colwidth=30))
    print("=" * 70)


def display_mapping_table(ai_suggestions: Dict[str, Any], available_columns: List[str]) -> None:
    """Display all AI-suggested mappings in a table format."""
    print("\n" + "=" * 70)
    print("FIELD MAPPING PREVIEW")
    print("=" * 70)
    
    # Required field
    required_field = "page_name"
    req_suggestion = ai_suggestions.get(required_field, "")
    print(f"\n{'Pipeline Field':<25} {'AI Suggestion':<30} {'Status'}")
    print("-" * 70)
    print(f"{required_field + ' (REQ)':<25} {req_suggestion:<30} {'[required]'}")
    
    # Optional fields
    optional_fields = [
        ("ad_count", "Number of ads/entries"),
        ("total_page_likes", "Social media metrics"),
        ("ad_texts", "Marketing text/descriptions"),
        ("platforms", "Platforms (FB, IG, etc.)"),
        ("is_active", "Active status"),
        ("first_ad_date", "Date of first appearance"),
        ("primary_email", "Contact email"),
        ("phones", "Phone numbers"),
        ("contact_name", "Contact name"),
        ("contact_position", "Contact position/job title")
    ]
    
    print("\nOptional Fields:")
    for field_name, description in optional_fields:
        suggestion = ai_suggestions.get(field_name, "") or ""
        # Handle array suggestions for phones
        if isinstance(suggestion, list):
            suggestion = ", ".join(suggestion)
        if suggestion and (suggestion in available_columns or any(col in available_columns for col in str(suggestion).split(", "))):
            status = "[suggested]"
        else:
            status = "[no match]"
        print(f"  {field_name:<23} {str(suggestion)[:28]:<30} {status}")
    
    print("\n" + "=" * 70)


def get_quick_action() -> str:
    """Get quick action from user."""
    print("\n" + "-" * 70)
    print("QUICK ACTIONS")
    print("-" * 70)
    print("  (a) Accept all AI suggestions")
    print("  (r) Review and edit mappings")
    print("  (s) Skip all optional fields (only map required)")
    print("  (q) Quick mode (accept high-confidence, skip low-confidence)")
    print("-" * 70)
    
    while True:
        action = input("\nChoose action (a/r/s/q): ").strip().lower()
        if action in ['a', 'r', 's', 'q']:
            return action
        print("  Invalid choice. Please enter 'a', 'r', 's', or 'q'.")


def interactive_field_mapping(df: pd.DataFrame, ai_suggestions: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Interactive CLI to verify and correct AI field mappings."""
    print("\n" + "=" * 70)
    print("SMART INPUT ADAPTER - Field Mapping")
    print("=" * 70)
    
    if ai_suggestions.get("confidence"):
        print(f"\nAI Confidence: {ai_suggestions.get('confidence', 0) * 100:.0f}%")
    if ai_suggestions.get("reasoning"):
        print(f"AI Reasoning: {ai_suggestions.get('reasoning', 'N/A')}")
    
    mapping = {}
    available_columns = df.columns.tolist()
    
    # Show mapping table
    display_mapping_table(ai_suggestions, available_columns)
    
    # Get quick action
    action = get_quick_action()
    
    # Handle required field first
    ai_suggestion = ai_suggestions.get("page_name")
    print(f"\n{'='*70}")
    print("REQUIRED FIELD")
    print("=" * 70)
    print(f"Company/Business Name (REQUIRED)")
    if ai_suggestion:
        print(f"AI suggests: '{ai_suggestion}'")
    
    while True:
        user_input = input(f"Enter column name: ").strip()
        if user_input in available_columns:
            mapping["page_name"] = user_input
            print(f"✓ Mapped: {user_input} → page_name")
            break
        elif user_input == "" and ai_suggestion and ai_suggestion in available_columns:
            mapping["page_name"] = ai_suggestion
            print(f"✓ Using AI suggestion: {ai_suggestion} → page_name")
            break
        else:
            print(f"✗ Column '{user_input}' not found. Please try again.")
    
    # Handle optional fields based on action
    optional_fields = [
        ("ad_count", "Number of ads/entries"),
        ("total_page_likes", "Social media metrics (likes, followers)"),
        ("ad_texts", "Marketing text or descriptions"),
        ("platforms", "Platforms (Facebook, Instagram, etc.)"),
        ("is_active", "Active status"),
        ("first_ad_date", "Date of first appearance")
    ]
    
    confidence = ai_suggestions.get("confidence", 0)
    
    if action == 'a':  # Accept all AI suggestions
        print(f"\n{'='*70}")
        print("ACCEPTING ALL AI SUGGESTIONS")
        print("=" * 70)
        for field_name, description in optional_fields:
            ai_suggestion = ai_suggestions.get(field_name)
            if ai_suggestion and ai_suggestion in available_columns:
                mapping[field_name] = ai_suggestion
                print(f"✓ Mapped: {ai_suggestion} → {field_name} (AI suggestion)")
            else:
                mapping[field_name] = None
                print(f"⊘ Skipped: {field_name} (no good match)")
    
    elif action == 's':  # Skip all optional fields
        print(f"\n{'='*70}")
        print("SKIPPING ALL OPTIONAL FIELDS")
        print("=" * 70)
        for field_name, description in optional_fields:
            mapping[field_name] = None
            print(f"⊘ Skipped: {field_name}")
    
    elif action == 'q':  # Quick mode (high confidence only)
        print(f"\n{'='*70}")
        print("QUICK MODE (High-confidence suggestions only)")
        print("=" * 70)
        for field_name, description in optional_fields:
            ai_suggestion = ai_suggestions.get(field_name)
            # Accept if high confidence (>80%) and suggestion exists
            if confidence > 0.8 and ai_suggestion and ai_suggestion in available_columns:
                mapping[field_name] = ai_suggestion
                print(f"✓ Mapped: {ai_suggestion} → {field_name} (high confidence)")
            else:
                mapping[field_name] = None
                print(f"⊘ Skipped: {field_name} (low confidence or no match)")
    
    else:  # action == 'r' - Review mode (streamlined)
        print(f"\n{'='*70}")
        print("REVIEW MODE - Optional Fields")
        print("=" * 70)
        print("(Press Enter to skip, or enter column name)")
        for field_name, description in optional_fields:
            ai_suggestion = ai_suggestions.get(field_name)
            print(f"\n{field_name.replace('_', ' ').title()}: {description}")
            if ai_suggestion:
                print(f"  AI suggests: '{ai_suggestion}'")
            
            user_input = input(f"  Column name (Enter to skip): ").strip()
            
            if user_input == "":
                # Auto-skip (no double prompt)
                mapping[field_name] = None
                print(f"  ⊘ Skipped: {field_name}")
            elif user_input in available_columns:
                mapping[field_name] = user_input
                print(f"  ✓ Mapped: {user_input} → {field_name}")
            else:
                print(f"  ✗ Column '{user_input}' not found. Skipping.")
                mapping[field_name] = None
    
    return mapping


def is_invalid_company_name(name):
    """Check if company name is invalid (too short, special chars only, etc.)."""
    if pd.isna(name) or not name:
        return True
    name_str = str(name).strip()
    # Single character or just special characters
    if len(name_str) <= 1:
        return True
    # Only special characters (no alphanumeric)
    if not any(c.isalnum() for c in name_str):
        return True
    return False


def transform_to_pipeline_schema(df: pd.DataFrame, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    """Transform user's file to pipeline schema using field mappings."""
    print("\n" + "=" * 70)
    print("TRANSFORMING DATA TO PIPELINE SCHEMA")
    print("=" * 70)
    
    result_df = pd.DataFrame()
    
    # Required field: page_name
    if mapping.get("page_name"):
        result_df["page_name"] = df[mapping["page_name"]].astype(str)
        # Normalize: remove emojis, strip whitespace
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE
        )
        result_df["page_name"] = (
            result_df["page_name"]
            .apply(lambda x: emoji_pattern.sub("", x))
            .str.strip()
            .str.normalize("NFKC")
        )
        
        # Filter out invalid company names and log them
        invalid_mask = result_df["page_name"].apply(is_invalid_company_name)
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            invalid_names = result_df[invalid_mask]["page_name"].unique().tolist()
            print(f"\n⚠️  Found {invalid_count} row(s) with invalid company names:")
            for name in invalid_names[:10]:  # Show first 10
                count = (result_df["page_name"] == name).sum()
                print(f"   - '{name}' ({count} row(s))")
            if len(invalid_names) > 10:
                print(f"   ... and {len(invalid_names) - 10} more invalid names")
            print(f"\n   Filtering out invalid rows to prevent processing errors...")
            
            # Save invalid rows to a separate file for reference
            invalid_df = df[invalid_mask].copy()
            if len(invalid_df) > 0:
                run_id = get_run_id_from_env()
                if run_id:
                    invalid_file = BASE_DIR / "processed" / f"{run_id}_invalid_company_names.csv"
                else:
                    invalid_file = BASE_DIR / "processed" / "invalid_company_names.csv"
                invalid_file.parent.mkdir(parents=True, exist_ok=True)
                invalid_df.to_csv(invalid_file, index=False)
                print(f"   Saved invalid rows to: {invalid_file}")
            
            # Filter out invalid rows
            result_df = result_df[~invalid_mask].copy()
            df = df[~invalid_mask].copy()
            print(f"   ✓ Kept {len(result_df)} valid rows (removed {invalid_count} invalid)")
    else:
        raise ValueError("page_name mapping is required but not provided")
    
    # Optional fields with transformations
    field_configs = {
        "ad_count": {"type": "int", "default": 1},
        "total_page_likes": {"type": "int", "default": 0},
        "ad_texts": {"type": "list", "default": [""]},
        "platforms": {"type": "list", "default": ["UNKNOWN"]},
        "is_active": {"type": "bool", "default": True},
        "first_ad_date": {"type": "date", "default": datetime.now().strftime("%Y-%m-%d")},
        "primary_email": {"type": "string", "default": ""},
        "contact_name": {"type": "string", "default": ""},
        "contact_position": {"type": "string", "default": ""}
    }
    
    for field_name, config in field_configs.items():
        source_col = mapping.get(field_name)
        
        if source_col and source_col in df.columns:
            if config["type"] == "int":
                result_df[field_name] = pd.to_numeric(df[source_col], errors='coerce').fillna(config["default"]).astype(int)
            
            elif config["type"] == "list":
                # Convert to list format
                def to_list(val):
                    if pd.isna(val):
                        return config["default"]
                    if isinstance(val, list):
                        return val
                    if isinstance(val, str):
                        # Try to parse as JSON list
                        try:
                            parsed = json.loads(val)
                            if isinstance(parsed, list):
                                return parsed
                        except:
                            pass
                        # Split by common delimiters
                        if ';' in val:
                            return [x.strip() for x in val.split(';') if x.strip()]
                        if ',' in val:
                            return [x.strip() for x in val.split(',') if x.strip()]
                        return [val] if val.strip() else config["default"]
                    return [str(val)]
                
                result_df[field_name] = df[source_col].apply(to_list)
            
            elif config["type"] == "bool":
                result_df[field_name] = df[source_col].astype(bool)
            
            elif config["type"] == "date":
                result_df[field_name] = pd.to_datetime(df[source_col], errors='coerce')
                result_df[field_name] = result_df[field_name].dt.strftime("%Y-%m-%d")
                result_df[field_name] = result_df[field_name].fillna(config["default"])
            
            else:
                result_df[field_name] = df[source_col].astype(str)
        else:
            # Apply default
            if config["type"] == "list":
                result_df[field_name] = [config["default"]] * len(df)
            else:
                result_df[field_name] = config["default"]
    
    # Handle phones separately (can map multiple columns)
    phones_mapping = mapping.get("phones")
    if phones_mapping:
        # Handle array of phone columns or single column
        phone_columns = []
        if isinstance(phones_mapping, list):
            phone_columns = phones_mapping
        elif isinstance(phones_mapping, str):
            # Check if it's a comma-separated list
            if ',' in phones_mapping:
                phone_columns = [col.strip() for col in phones_mapping.split(',')]
            else:
                phone_columns = [phones_mapping]
        
        # Combine all phone columns into a single phones list
        def combine_phones(row):
            phones = []
            for col in phone_columns:
                if col in df.columns:
                    val = row[col]
                    if pd.notna(val) and str(val).strip():
                        phones.append(str(val).strip())
            return phones if phones else []
        
        result_df["phones"] = df.apply(combine_phones, axis=1)
    else:
        result_df["phones"] = [[]] * len(df)
    
    # Convert list columns to string format for CSV
    for col in ["ad_texts", "platforms", "phones"]:
        if col in result_df.columns:
            result_df[col] = result_df[col].apply(str)
    
    # Deduplicate by page_name (if multiple rows per company)
    if len(result_df) > len(result_df["page_name"].unique()):
        print(f"\n⚠️  Found duplicate company names. Deduplicating...")
        original_count = len(result_df)
        
        def merge_lists(series):
            """Merge list values from multiple rows."""
            all_items = []
            for val in series:
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, list):
                            all_items.extend(parsed)
                        else:
                            all_items.append(parsed)
                    except:
                        # Try eval as fallback
                        try:
                            parsed = eval(val)
                            if isinstance(parsed, list):
                                all_items.extend(parsed)
                            else:
                                all_items.append(val)
                        except:
                            all_items.append(val)
                elif isinstance(val, list):
                    all_items.extend(val)
                else:
                    all_items.append(val)
            return list(set([str(item) for item in all_items if item]))
        
        result_df = result_df.groupby("page_name", as_index=False).agg({
            "ad_count": "sum",
            "total_page_likes": "max",
            "ad_texts": merge_lists,
            "platforms": merge_lists,
            "is_active": "any",
            "first_ad_date": "min"
        })
        result_df["ad_texts"] = result_df["ad_texts"].apply(str)
        result_df["platforms"] = result_df["platforms"].apply(str)
        print(f"   Reduced from {original_count} to {len(result_df)} unique companies")
    
    return result_df


def save_mapping(mapping: Dict[str, Optional[str]], source_file: Path, file_format: str) -> Path:
    """Save field mapping configuration for reuse."""
    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create safe filename from source file
    safe_name = re.sub(r'[^\w\-_\.]', '_', source_file.stem)
    mapping_file = MAPPINGS_DIR / f"{safe_name}.json"
    
    config = {
        "source_file": source_file.name,
        "source_path": str(source_file),
        "file_format": file_format,
        "created_date": datetime.now().isoformat(),
        "pipeline_version": "3.7",
        "field_mappings": mapping
    }
    
    with open(mapping_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    return mapping_file


def load_mapping(source_file: Path) -> Optional[Dict[str, Any]]:
    """Load existing field mapping if available."""
    safe_name = re.sub(r'[^\w\-_\.]', '_', source_file.stem)
    mapping_file = MAPPINGS_DIR / f"{safe_name}.json"
    
    if mapping_file.exists():
        try:
            with open(mapping_file, 'r') as f:
                config = json.load(f)
            return config
        except Exception as e:
            print(f"⚠️  Error loading mapping: {e}")
            return None
    
    return None


def check_fb_ads_format(df: pd.DataFrame) -> bool:
    """Check if file matches FB Ads Library format (backwards compatibility)."""
    required_cols = ['page_name', 'page_likes', 'text', 'ad_category', 'is_active', 'start_date']
    
    # Normalize column names for comparison (strip whitespace, lowercase)
    df_cols_normalized = {col.strip().lower(): col for col in df.columns}
    required_cols_normalized = [col.strip().lower() for col in required_cols]
    
    # Check if all required columns are present
    missing_cols = [col for col in required_cols_normalized if col not in df_cols_normalized]
    
    # Debug output
    print(f"\n[FB Format Detection] Checking for FB Ads Library format...")
    print(f"  File columns ({len(df.columns)}): {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
    print(f"  Required columns: {required_cols}")
    if missing_cols:
        print(f"  Missing columns: {missing_cols}")
        print(f"  Result: NOT FB Ads Library format")
        return False
    else:
        print(f"  All required columns found!")
        print(f"  Result: FB Ads Library format detected")
        return True


def load_fb_ads_format(input_path: Path) -> pd.DataFrame:
    """Load FB Ads Library format using legacy logic."""
    import importlib.util
    legacy_path = BASE_DIR / "scripts" / "legacy" / "loader_fb_ads.py"
    spec = importlib.util.spec_from_file_location("loader_fb_ads", legacy_path)
    loader_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader_module)
    output_file, _ = get_output_file()
    return loader_module.load_and_process(input_path, output_file)


def main():
    """Main function for smart input adapter."""
    print("\n" + "=" * 70)
    print("MODULE 1: SMART INPUT ADAPTER")
    print("=" * 70)
    
    # Parse command line arguments
    input_path = DEFAULT_INPUT_FILE
    mapping_file = None
    
    if "--input" in sys.argv:
        idx = sys.argv.index("--input")
        if idx + 1 < len(sys.argv):
            input_path = Path(sys.argv[idx + 1])
            if not input_path.is_absolute():
                input_path = BASE_DIR / input_path
    
    if "--mapping" in sys.argv:
        idx = sys.argv.index("--mapping")
        if idx + 1 < len(sys.argv):
            mapping_file = Path(sys.argv[idx + 1])
            if not mapping_file.is_absolute():
                mapping_file = BASE_DIR / mapping_file
    
    print(f"\nInput file: {input_path}")
    
    # Load file
    try:
        df = load_file(input_path)
        print(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return 1
    
    # Check for FB Ads Library format (backwards compatibility)
    is_fb_format = check_fb_ads_format(df)
    if is_fb_format:
        print("\n✓ Detected FB Ads Library format - using legacy loader")
        try:
            result_df = load_fb_ads_format(input_path)
            print(f"\n✓ Processed {len(result_df)} unique advertisers")
            
            # Get versioned output file
            output_file, base_name = get_output_file()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"✓ Output saved to: {output_file}")
            
            # Create latest symlink
            latest_path = create_latest_symlink(output_file, base_name)
            if latest_path:
                print(f"✓ Latest symlink: {latest_path}")
            
            return 0
        except Exception as e:
            print(f"✗ Error in legacy loader: {e}")
            return 1
    else:
        print("\n✓ File format: Generic input (not FB Ads Library format)")
        print("  Proceeding with smart adapter and AI-powered field mapping...")
    
    # Display preview
    display_preview(df, input_path)
    
    # Check for existing mapping
    existing_mapping = load_mapping(input_path)
    use_existing = False
    
    if existing_mapping:
        print(f"\n✓ Found existing mapping from {existing_mapping['created_date']}")
        # Skip prompt in non-interactive mode (--all flag means batch processing)
        if '--use-existing-mapping' in sys.argv or '--all' in sys.argv:
            response = 'y'
            print("✓ Using existing mapping (non-interactive mode)")
        else:
            response = input("Use existing mapping? (y/n/edit): ").strip().lower()
        if response == 'y':
            use_existing = True
            mapping = existing_mapping.get("field_mappings", {})
            if '--use-existing-mapping' not in sys.argv and '--all' not in sys.argv:
                print("✓ Using existing mapping")
        elif response == 'edit':
            # Load mapping but allow edits
            mapping = existing_mapping.get("field_mappings", {})
            # Will go through interactive verification
            use_existing = False
        else:
            use_existing = False
    
    # Get field mappings
    if not use_existing:
        # Use OpenAI to analyze
        ai_suggestions = analyze_schema_with_openai(df)
        
        # Interactive verification
        mapping = interactive_field_mapping(df, ai_suggestions)
        
        # Ask to save mapping
        save_response = input("\n💾 Save this mapping for future use? (y/n): ").strip().lower()
        if save_response == 'y':
            mapping_path = save_mapping(mapping, input_path, detect_file_format(input_path))
            print(f"✓ Mapping saved to: {mapping_path}")
    
    # Transform to pipeline schema
    try:
        result_df = transform_to_pipeline_schema(df, mapping)
        
        # Validate
        if "page_name" not in result_df.columns:
            raise ValueError("Required field 'page_name' missing after transformation")
        
        if len(result_df) == 0:
            raise ValueError("No rows after transformation")
        
        # Generate input validation report
        run_id = get_run_id_from_env()
        if run_id:
            report_file = BASE_DIR / "processed" / f"{run_id}_input_validation_report.txt"
        else:
            report_file = BASE_DIR / "processed" / "input_validation_report.txt"
        
        report_file.parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("INPUT DATA VALIDATION REPORT\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Source file: {input_path}\n")
            f.write(f"Total rows loaded: {len(df)}\n")
            f.write(f"Rows after filtering: {len(result_df)}\n")
            f.write(f"Rows filtered out: {len(df) - len(result_df)}\n\n")
            
            # Invalid company names
            invalid_mask = result_df["page_name"].apply(is_invalid_company_name) if len(result_df) > 0 else pd.Series([], dtype=bool)
            invalid_count = invalid_mask.sum()
            if invalid_count > 0:
                f.write(f"Invalid company names found: {invalid_count}\n")
                invalid_names = result_df[invalid_mask]["page_name"].unique().tolist()
                for name in invalid_names:
                    count = (result_df["page_name"] == name).sum()
                    f.write(f"  - '{name}' ({count} row(s))\n")
            else:
                f.write("Invalid company names: 0\n")
            
            f.write("\n")
            
            # Missing required fields
            required_fields = ["page_name"]
            f.write("Required fields check:\n")
            for field in required_fields:
                if field in result_df.columns:
                    missing = result_df[field].isna().sum() + (result_df[field] == '').sum()
                    f.write(f"  - {field}: {missing} missing\n")
                else:
                    f.write(f"  - {field}: MISSING COLUMN\n")
            
            f.write("\n")
            
            # Data quality score
            total_valid = len(result_df) - invalid_count
            quality_score = (total_valid / len(result_df) * 100) if len(result_df) > 0 else 0
            f.write(f"Data Quality Score: {quality_score:.1f}%\n")
            f.write(f"  - Valid rows: {total_valid}/{len(result_df)}\n")
            f.write(f"  - Invalid rows: {invalid_count}/{len(result_df)}\n")
        
        print(f"✓ Input validation report saved to: {report_file}")
        
        # Save output to versioned file
        output_file, base_name = get_output_file()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_file, index=False, encoding="utf-8")
        
        print("\n" + "=" * 70)
        print("TRANSFORMATION COMPLETE")
        print("=" * 70)
        print(f"✓ Processed {len(result_df)} unique companies")
        print(f"✓ Output saved to: {output_file}")
        
        # Create latest symlink
        latest_path = create_latest_symlink(output_file, base_name)
        if latest_path:
            print(f"✓ Latest symlink: {latest_path}")
        print(f"\nOutput columns: {', '.join(result_df.columns.tolist())}")
        print(f"\nSample row:")
        print(result_df.iloc[0].to_string())
        
        return 0
    
    except Exception as e:
        print(f"\n✗ Error during transformation: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

