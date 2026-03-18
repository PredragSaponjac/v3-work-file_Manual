"""
ORCA v20 — GDELT Source Adapter.

Tier 2 — broad discovery / enrichment.

Uses GDELT DOC 2.0 API for keyword/topic/entity queries.
Discovery and enrichment only — never sole primary confirmation.
No API key required (public API).
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

logger = logging.getLogger("orca_v20.sources.gdelt")

# GDELT DOC 2.0 API (free, no key needed)
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
SOURCE_TIER = 2
SOURCE_TYPE = "GDELT_DOC"

# Default queries for ORCA's areas of interest
DEFAULT_QUERIES = [
    "oil price surge",
    "natural gas supply disruption",
    "hurricane energy infrastructure",
    "sanctions energy",
    "airline fuel costs",
    "shipping disruption",
    "commodity supply shock",
]


def search_articles(
    query: str,
    mode: str = "artlist",
    max_records: int = 10,
    timespan: str = "3d",
    source_lang: str = "english",
    timeout: int = 15,
) -> List[Dict[str, Any]]:
    """
    Search GDELT DOC 2.0 API for articles matching a query.

    Returns list of article dicts with:
        article_url, title, source, tone, language,
        source_timestamp, matched_query, source_type, source_tier
    """
    try:
        import requests
    except ImportError:
        logger.warning("[gdelt] requests not installed")
        return []

    params = {
        "query": query,
        "mode": mode,
        "maxrecords": str(max_records),
        "timespan": timespan,
        "format": "json",
        "sourcelang": source_lang,
    }

    # Retry once on 429 with backoff
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            resp = requests.get(GDELT_DOC_API, params=params, timeout=timeout, headers={
                "User-Agent": "ORCA-v20/1.0",
            })
            if resp.status_code == 429 and attempt < max_attempts - 1:
                logger.info(f"[gdelt] 429 rate-limited on '{query}', backing off 5s...")
                time.sleep(5)
                continue
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(3)
                continue
            logger.warning(f"[gdelt] Search failed for '{query}': {e}")
            return []

    articles_raw = data.get("articles", [])
    articles = []

    seen_urls = set()
    for art in articles_raw:
        url = art.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        articles.append({
            "article_url": url,
            "title": art.get("title", "")[:300],
            "source": art.get("domain", ""),
            "tone": art.get("tone", None),
            "language": art.get("language", source_lang),
            "seendate": art.get("seendate", ""),
            "source_timestamp": datetime.now(timezone.utc).isoformat(),
            "matched_query": query,
            "source_type": SOURCE_TYPE,
            "source_tier": SOURCE_TIER,
        })

    logger.info(f"[gdelt] '{query}' → {len(articles)} articles")
    return articles


def fetch_all(
    queries: Optional[List[str]] = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    Public API — run all default queries against GDELT.

    Returns {
        "source": "gdelt",
        "tier": 2,
        "status": "healthy" | "degraded" | "down",
        "items": [...],
        "freshness_utc": str,
    }
    """
    queries = queries or DEFAULT_QUERIES
    all_items = []
    errors = 0

    # Deduplicate across queries by URL
    seen_urls = set()

    for idx, query in enumerate(queries):
        try:
            articles = search_articles(query, timeout=timeout)
            for art in articles:
                url = art.get("article_url", "")
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_items.append(art)
        except Exception as e:
            logger.warning(f"[gdelt] Query '{query}' failed: {e}")
            errors += 1

        # Rate-limit courtesy: 1.5s between queries to avoid 429 bursts
        if idx < len(queries) - 1:
            time.sleep(1.5)

    if errors == len(queries):
        status = "down"
    elif errors > 0:
        status = "degraded"
    else:
        status = "healthy" if all_items else "degraded"

    return {
        "source": "gdelt",
        "tier": SOURCE_TIER,
        "status": status,
        "items": all_items,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(all_items),
    }
