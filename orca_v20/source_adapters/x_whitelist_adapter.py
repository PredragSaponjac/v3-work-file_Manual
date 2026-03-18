"""
ORCA v20 — Whitelisted X (Twitter) Ingestion Adapter.

Tier 3 — optional enrichment only. NEVER dominates thesis.

Strict whitelist from config — no broad search/firehose crawling.
If X API credentials are absent, degrades gracefully.
Does NOT block the rest of ORCA if X is unavailable.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.x_whitelist")

SOURCE_TIER = 3
SOURCE_TYPE = "X_WHITELIST"

# Default whitelist — domain experts only
DEFAULT_WHITELIST = {
    "weather": [
        "ryanhallyall",
        "RyanMaue",
        "WeatherProf",
    ],
    "oil_energy": [
        "OilStockTrader",
        "DB_WTI",
        "tradingcrudeoil",
    ],
}


def _get_x_credentials() -> Optional[Dict[str, str]]:
    """Get X API credentials from environment. Never hardcoded."""
    api_key = os.environ.get("X_API_KEY", "")
    api_secret = os.environ.get("X_API_SECRET", "")
    access_token = os.environ.get("X_ACCESS_TOKEN", "")
    access_secret = os.environ.get("X_ACCESS_SECRET", "")

    if not all([api_key, api_secret, access_token, access_secret]):
        return None

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "access_token": access_token,
        "access_secret": access_secret,
    }


def _flatten_whitelist(whitelist: Optional[Dict[str, List[str]]] = None) -> List[str]:
    """Flatten category-grouped whitelist to a flat list of handles."""
    wl = whitelist or DEFAULT_WHITELIST
    handles = []
    for category_handles in wl.values():
        handles.extend(category_handles)
    return handles


def fetch_user_timeline(
    handle: str,
    creds: Dict[str, str],
    max_results: int = 5,
    timeout: int = 15,
) -> List[Dict[str, Any]]:
    """
    Fetch recent posts from a whitelisted X account.

    Uses X API v2 user timeline endpoint.
    Returns list of post dicts.
    """
    try:
        import tweepy
    except ImportError:
        logger.warning("[x_whitelist] tweepy not installed")
        return []

    try:
        # Use OAuth 1.0a for user lookup (v1.1) then v2 for timeline
        auth = tweepy.OAuth1UserHandler(
            consumer_key=creds["api_key"],
            consumer_secret=creds["api_secret"],
            access_token=creds["access_token"],
            access_token_secret=creds["access_secret"],
        )
        api_v1 = tweepy.API(auth, timeout=timeout)

        # Resolve user ID via v1.1 (reliable with OAuth1)
        try:
            user = api_v1.get_user(screen_name=handle)
            user_id = user.id
        except Exception:
            logger.debug(f"[x_whitelist] User @{handle} not found")
            return []

        # Fetch recent tweets via v1.1 user_timeline (OAuth1 works natively)
        statuses = api_v1.user_timeline(
            user_id=user_id,
            count=max_results,
            tweet_mode="extended",
        )

        if not statuses:
            return []

        posts = []
        for status in statuses:
            posts.append({
                "handle": handle,
                "post_id": str(status.id),
                "post_timestamp": status.created_at.isoformat() if status.created_at else "",
                "text": (status.full_text or status.text or "")[:500],
                "engagement_likes": status.favorite_count or 0,
                "engagement_retweets": status.retweet_count or 0,
                "engagement_replies": 0,  # not available in v1.1
                "ingest_method": "api_v1_timeline",
                "source_timestamp": datetime.now(timezone.utc).isoformat(),
                "source_type": SOURCE_TYPE,
                "source_tier": SOURCE_TIER,
            })

        return posts

    except Exception as e:
        logger.warning(f"[x_whitelist] Failed to fetch @{handle}: {e}")
        return []


def fetch_all(
    whitelist: Optional[Dict[str, List[str]]] = None,
    max_per_user: int = 5,
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    Public API — fetch recent posts from all whitelisted X accounts.

    Returns {
        "source": "x_whitelist",
        "tier": 3,
        "status": "healthy" | "degraded" | "down" | "no_credentials",
        "items": [...],
        "freshness_utc": str,
    }
    """
    creds = _get_x_credentials()
    if not creds:
        logger.info("[x_whitelist] No X API credentials — source unavailable (graceful degradation)")
        return {
            "source": "x_whitelist",
            "tier": SOURCE_TIER,
            "status": "no_credentials",
            "items": [],
            "freshness_utc": datetime.now(timezone.utc).isoformat(),
            "count": 0,
        }

    handles = _flatten_whitelist(whitelist)
    all_items = []
    errors = 0

    for handle in handles:
        try:
            posts = fetch_user_timeline(handle, creds, max_results=max_per_user, timeout=timeout)
            all_items.extend(posts)
        except Exception as e:
            logger.warning(f"[x_whitelist] @{handle} failed: {e}")
            errors += 1

    if errors == len(handles):
        status = "down"
    elif errors > 0:
        status = "degraded"
    else:
        status = "healthy" if all_items else "degraded"

    logger.info(f"[x_whitelist] Fetched {len(all_items)} posts from {len(handles)} accounts")

    return {
        "source": "x_whitelist",
        "tier": SOURCE_TIER,
        "status": status,
        "items": all_items,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(all_items),
    }
