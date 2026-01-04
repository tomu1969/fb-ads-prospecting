"""Run ID and versioned filename utilities for pipeline file management."""

import os
import re
from datetime import datetime
from pathlib import Path


def extract_input_reference(input_file_path):
    """
    Extract sanitized input reference from input file path.
    
    Args:
        input_file_path: Path object or string to input file
        
    Returns:
        Sanitized filename without extension (e.g., 'hubspot_leads' from 'hubspot_leads.csv')
    """
    if isinstance(input_file_path, str):
        input_file_path = Path(input_file_path)
    
    # Get filename without extension
    filename = input_file_path.stem
    
    # Sanitize: replace spaces and special chars with underscores, lowercase
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)
    sanitized = re.sub(r'_+', '_', sanitized)  # Collapse multiple underscores
    sanitized = sanitized.lower().strip('_')
    
    # Handle empty or very short names
    if not sanitized or len(sanitized) < 2:
        sanitized = 'input'
    
    return sanitized


def get_run_id(timestamp=None, input_file=None):
    """
    Generate run ID from timestamp and input file reference.
    
    Args:
        timestamp: datetime object (defaults to now)
        input_file: Path to input file (optional)
        
    Returns:
        Run ID string in format: {timestamp}_{input_ref}
        Example: '20240115_143045_hubspot_leads'
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    # Format timestamp: YYYYMMDD_HHMMSS
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    # Extract input reference
    if input_file:
        input_ref = extract_input_reference(input_file)
        run_id = f"{timestamp_str}_{input_ref}"
    else:
        # Fallback if no input file
        run_id = timestamp_str
    
    return run_id


def get_versioned_filename(base_name, run_id):
    """
    Generate versioned filename from base name and run ID.
    
    Args:
        base_name: Base filename (e.g., '01_loaded.csv' or 'prospects_final.csv')
        run_id: Run ID string (e.g., '20240115_143045_hubspot_leads')
        
    Returns:
        Versioned filename (e.g., '20240115_143045_hubspot_leads_01_loaded.csv')
    """
    # Extract extension if present
    if '.' in base_name:
        name_parts = base_name.rsplit('.', 1)
        base = name_parts[0]
        ext = '.' + name_parts[1]
    else:
        base = base_name
        ext = ''
    
    # Combine: {run_id}_{base}{ext}
    versioned = f"{run_id}_{base}{ext}"
    
    return versioned


def create_latest_symlink(versioned_path, latest_name, base_dir=None):
    """
    Create or update symlink from latest_name to versioned_path.
    
    Args:
        versioned_path: Path to versioned file (target of symlink)
        latest_name: Name for symlink (e.g., '01_loaded.csv')
        base_dir: Base directory for symlink (defaults to versioned_path's parent)
        
    Returns:
        Path to created symlink
    """
    if isinstance(versioned_path, str):
        versioned_path = Path(versioned_path)
    
    if base_dir is None:
        base_dir = versioned_path.parent
    elif isinstance(base_dir, str):
        base_dir = Path(base_dir)
    
    latest_path = base_dir / latest_name
    
    # Remove existing symlink or file if it exists
    if latest_path.exists() or latest_path.is_symlink():
        try:
            if latest_path.is_symlink():
                latest_path.unlink()
            else:
                # If it's a regular file, we might want to keep it or remove it
                # For now, we'll remove it to ensure symlink works
                latest_path.unlink()
        except Exception as e:
            print(f"Warning: Could not remove existing {latest_path}: {e}")
    
    # Create symlink
    try:
        # Use relative path for symlink (works better across systems)
        try:
            relative_target = os.path.relpath(versioned_path, base_dir)
            latest_path.symlink_to(relative_target)
        except (OSError, ValueError):
            # Fallback to absolute path if relative doesn't work
            latest_path.symlink_to(versioned_path)
        
        return latest_path
    except OSError as e:
        # On Windows or if symlinks aren't supported, create a copy instead
        print(f"Warning: Could not create symlink (symlinks may not be supported): {e}")
        print(f"  Creating copy instead: {latest_path}")
        try:
            import shutil
            shutil.copy2(versioned_path, latest_path)
            return latest_path
        except Exception as copy_error:
            print(f"Error creating copy: {copy_error}")
            return None


def get_run_id_from_env():
    """
    Get run ID from environment variable.
    
    Returns:
        Run ID string or None if not set
    """
    return os.environ.get('PIPELINE_RUN_ID')


def set_run_id_in_env(run_id):
    """
    Set run ID in environment variable.
    
    Args:
        run_id: Run ID string to set
    """
    os.environ['PIPELINE_RUN_ID'] = run_id

