"""Redis caching utilities for Instagram handle enrichment."""

import os
import json
import hashlib
from typing import Optional, List
from pathlib import Path

# Try to import redis, but make it optional
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


def get_redis_client():
    """Get Redis client if available, otherwise return None."""
    if not REDIS_AVAILABLE:
        return None
    
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    try:
        client = redis.from_url(redis_url, decode_responses=True)
        # Test connection
        client.ping()
        return client
    except Exception:
        return None


def generate_cache_key(company_name: str, website_url: str = "") -> str:
    """Generate cache key for Instagram handles."""
    # Create hash of company name and website for consistent keys
    key_data = f"{company_name.lower().strip()}:{website_url.lower().strip()}"
    key_hash = hashlib.md5(key_data.encode()).hexdigest()[:16]
    return f"ig_handle:{key_hash}"


def get_cached_handles(company_name: str, website_url: str = "") -> Optional[List[str]]:
    """Get cached Instagram handles for a company."""
    client = get_redis_client()
    if not client:
        return None
    
    try:
        cache_key = generate_cache_key(company_name, website_url)
        cached_data = client.get(cache_key)
        if cached_data:
            handles = json.loads(cached_data)
            return handles if isinstance(handles, list) else None
    except Exception:
        pass
    
    return None


def cache_handles(company_name: str, website_url: str, handles: List[str], ttl_days: int = 30):
    """Cache Instagram handles for a company."""
    client = get_redis_client()
    if not client:
        return False
    
    try:
        cache_key = generate_cache_key(company_name, website_url)
        ttl_seconds = ttl_days * 24 * 60 * 60
        client.setex(cache_key, ttl_seconds, json.dumps(handles))
        return True
    except Exception:
        return False


def is_redis_available() -> bool:
    """Check if Redis is available."""
    return get_redis_client() is not None

