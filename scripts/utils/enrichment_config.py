"""Enrichment configuration utilities for pipeline module selection."""

import os
import json
from typing import Dict, List, Optional
from pathlib import Path

# Enrichment types and their associated modules
# NOTE: Time estimates based on actual measured performance with parallelization
ENRICHMENT_TYPES = {
    'websites': {
        'name': 'Website URLs',
        'description': 'Find company websites via DuckDuckGo search',
        'modules': ['enricher'],
        'cost_per_contact': 0.0,  # Free (DuckDuckGo)
        'time_per_contact': 2.0,  # seconds (measured: ~2 min for 59 contacts with 10x parallel)
    },
    'emails': {
        'name': 'Email Addresses',
        'description': 'Hunter.io lookup + email verification',
        'modules': ['hunter', 'contact_enricher'],
        'cost_per_contact': 0.0,  # Free with Hunter tier
        'time_per_contact': 1.3,  # seconds (measured: ~1.3 min for 59 contacts)
    },
    'phones': {
        'name': 'Phone Numbers',
        'description': 'Scrape from company websites',
        'modules': ['scraper', 'hunter'],
        'cost_per_contact': 0.0,  # Free (scraping)
        'time_per_contact': 0.8,  # seconds (measured: ~46s for 59 contacts with 10x parallel)
    },
    'contact_names': {
        'name': 'Contact Names',
        'description': 'Extract from websites (runs with phones)',
        'modules': ['scraper', 'hunter'],
        'cost_per_contact': 0.0,  # Free (scraping)
        'time_per_contact': 0.0,  # seconds (included with phones - same scrape)
    },
    'instagram_handles': {
        'name': 'Instagram Handles',
        'description': 'AI-powered search (Groq) - Fast mode',
        'modules': ['instagram_enricher'],
        'cost_per_contact': 0.0002,  # Groq API calls (~$0.01 for 59 contacts)
        'time_per_contact': 0.5,  # seconds (optimized fast mode: cache + scrape + Groq)
    },
}

# Module to enrichment type mapping
MODULE_TO_ENRICHMENT = {
    'enricher': ['websites'],
    'scraper': ['phones', 'contact_names'],
    'hunter': ['emails'],  # Hunter only runs for email enrichment
    'contact_enricher': ['emails'],
    'instagram_enricher': ['instagram_handles'],
}


def get_default_config() -> Dict[str, bool]:
    """Get default enrichment configuration (all enabled)."""
    return {enrichment_type: True for enrichment_type in ENRICHMENT_TYPES.keys()}


def load_config_from_env() -> Optional[Dict[str, bool]]:
    """
    Load enrichment configuration from environment variable.
    
    Returns:
        Dict mapping enrichment types to enabled status, or None if not set
    """
    config_str = os.environ.get('ENRICHMENT_CONFIG')
    if not config_str:
        return None
    
    try:
        return json.loads(config_str)
    except json.JSONDecodeError:
        return None


def save_config_to_env(config: Dict[str, bool]):
    """
    Save enrichment configuration to environment variable.
    
    Args:
        config: Dict mapping enrichment types to enabled status
    """
    os.environ['ENRICHMENT_CONFIG'] = json.dumps(config)


def load_config_from_file(file_path: Path) -> Optional[Dict[str, bool]]:
    """
    Load enrichment configuration from JSON file.
    
    Args:
        file_path: Path to JSON config file
        
    Returns:
        Dict mapping enrichment types to enabled status, or None if file doesn't exist
    """
    if not file_path.exists():
        return None
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def save_config_to_file(config: Dict[str, bool], file_path: Path):
    """
    Save enrichment configuration to JSON file.
    
    Args:
        config: Dict mapping enrichment types to enabled status
        file_path: Path to JSON config file
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=2)


def should_run_module(module_name: str, config: Optional[Dict[str, bool]] = None) -> bool:
    """
    Check if a module should run based on enrichment configuration.
    
    Args:
        module_name: Name of the module (e.g., 'enricher', 'hunter')
        config: Enrichment configuration dict (defaults to loading from env)
        
    Returns:
        True if module should run, False otherwise
    """
    if config is None:
        config = load_config_from_env()
    
    # If no config, default to running all modules (backward compatibility)
    if config is None:
        return True
    
    # Get enrichment types required by this module
    required_types = MODULE_TO_ENRICHMENT.get(module_name, [])
    
    # If module has no enrichment types, run it (e.g., loader, exporter, validator)
    if not required_types:
        return True
    
    # Module should run if any of its required enrichment types are enabled
    return any(config.get(enrichment_type, False) for enrichment_type in required_types)


def get_enabled_enrichments(config: Optional[Dict[str, bool]] = None) -> List[str]:
    """
    Get list of enabled enrichment types.
    
    Args:
        config: Enrichment configuration dict (defaults to loading from env)
        
    Returns:
        List of enabled enrichment type names
    """
    if config is None:
        config = load_config_from_env()
    
    if config is None:
        return list(ENRICHMENT_TYPES.keys())
    
    return [enrichment_type for enrichment_type, enabled in config.items() if enabled]


def estimate_cost_and_time(
    row_count: int,
    config: Optional[Dict[str, bool]] = None,
    parallel_factor: float = 1.0
) -> tuple[float, float]:
    """
    Estimate total cost and time based on enrichment configuration.
    
    Args:
        row_count: Number of contacts to process
        config: Enrichment configuration dict (defaults to loading from env)
        parallel_factor: Speedup factor from parallel processing (default 1.0 = no speedup)
        
    Returns:
        Tuple of (estimated_cost, estimated_time_minutes)
    """
    if config is None:
        config = load_config_from_env()
    
    if config is None:
        config = get_default_config()
    
    total_cost = 0.0
    # Track modules to avoid double-counting time when multiple enrichments share a module
    module_times = {
        'enricher': 0.0,
        'scraper': 0.0,
        'hunter': 0.0,
        'contact_enricher': 0.0,
        'instagram_enricher': 0.0,
    }
    
    for enrichment_type, enabled in config.items():
        if not enabled:
            continue
        
        enrichment_info = ENRICHMENT_TYPES.get(enrichment_type, {})
        cost_per = enrichment_info.get('cost_per_contact', 0.0)
        time_per = enrichment_info.get('time_per_contact', 0.0)
        modules = enrichment_info.get('modules', [])
        
        # Special handling for emails (only 30% need agent enrichment)
        if enrichment_type == 'emails':
            # Check if Fast Mode is enabled (skip contact_enricher)
            fast_mode = os.environ.get('SKIP_CONTACT_ENRICHER') == 'true' or \
                        os.environ.get('PIPELINE_SPEED_MODE') == 'fast'

            if fast_mode:
                # Fast Mode: Only Hunter, no agent enricher
                module_times['hunter'] = max(module_times['hunter'], row_count * 1.0)
                # No cost for agent enricher in fast mode
            else:
                # Full Mode: Hunter + Agent enricher for 30% of contacts
                contacts_needing_enrichment = int(row_count * 0.3)
                total_cost += contacts_needing_enrichment * cost_per
                # Time is average across all contacts (some fast via Hunter, some slow via Agent)
                # Distribute time across modules: Hunter gets 1s, Agent enricher gets the rest
                for module in modules:
                    if module == 'hunter':
                        module_times['hunter'] = max(module_times['hunter'], row_count * 1.0)
                    elif module == 'contact_enricher':
                        # Agent enricher only runs for 30% of contacts, but takes longer (~30s with 10x parallelism)
                        module_times['contact_enricher'] = max(module_times['contact_enricher'], contacts_needing_enrichment * 30.0)
        else:
            total_cost += row_count * cost_per
            # Distribute time to modules (use max to avoid double-counting when multiple enrichments share a module)
            for module in modules:
                if module in module_times:
                    # Only count time once per module, use max to handle multiple enrichments sharing a module
                    module_times[module] = max(module_times[module], row_count * time_per)
    
    # Sum up module times (each module counted once)
    total_time = sum(module_times.values())
    
    # Apply parallel processing speedup
    total_time = total_time / parallel_factor
    
    return total_cost, total_time / 60.0  # Convert seconds to minutes


def get_cost_breakdown(
    row_count: int,
    config: Optional[Dict[str, bool]] = None
) -> Dict[str, Dict[str, float]]:
    """
    Get detailed cost and time breakdown by enrichment type.
    
    Args:
        row_count: Number of contacts to process
        config: Enrichment configuration dict (defaults to loading from env)
        
    Returns:
        Dict mapping enrichment type to {'cost': float, 'time_minutes': float}
    """
    if config is None:
        config = load_config_from_env()
    
    if config is None:
        config = get_default_config()
    
    breakdown = {}
    
    for enrichment_type, enabled in config.items():
        if not enabled:
            continue
        
        enrichment_info = ENRICHMENT_TYPES.get(enrichment_type, {})
        cost_per = enrichment_info.get('cost_per_contact', 0.0)
        time_per = enrichment_info.get('time_per_contact', 0.0)
        
        if enrichment_type == 'emails':
            contacts_needing_enrichment = int(row_count * 0.3)
            cost = contacts_needing_enrichment * cost_per
            time_minutes = (row_count * time_per) / 60.0
        else:
            cost = row_count * cost_per
            time_minutes = (row_count * time_per) / 60.0
        
        breakdown[enrichment_type] = {
            'cost': cost,
            'time_minutes': time_minutes,
        }
    
    return breakdown

