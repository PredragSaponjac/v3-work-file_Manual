"""
ORCA v20 — Google Trends Source Adapter.

Tier 2 — attention/acceleration data, not proof by itself.

Two extraction paths:
    1. RSS feed (primary): https://trends.google.com/trending/rss?geo=US
    2. Trending Now page (optional deeper pull)

Does NOT depend on pytrends.
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.google_trends")

RSS_URL = "https://trends.google.com/trending/rss?geo=US"
SOURCE_TIER = 2
SOURCE_TYPE_RSS = "GOOGLE_TRENDS_RSS"
SOURCE_TYPE_PAGE = "GOOGLE_TRENDS_PAGE"

# Namespace used in Google Trends RSS
_HT_NS = "{https://trends.google.com/trending/rss}"


def fetch_trends_rss(url: str = RSS_URL, timeout: int = 15) -> List[Dict[str, Any]]:
    """
    Fetch and parse Google Trends RSS feed.

    Returns list of trend dicts with:
        trend_title, traffic_estimate_raw, started_at,
        related_news_links, source_timestamp, extraction_method,
        source_type, source_tier
    """
    try:
        import requests
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "ORCA-v20/1.0 (research bot)"
        })
        resp.raise_for_status()
    except ImportError:
        logger.warning("[google_trends] requests not installed")
        return []
    except Exception as e:
        logger.warning(f"[google_trends] RSS fetch failed: {e}")
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.warning(f"[google_trends] XML parse failed: {e}")
        return []

    items = root.findall(".//item")
    trends = []

    for item in items:
        title = _text(item, "title")
        if not title:
            continue

        # Traffic estimate
        traffic_raw = _text(item, f"{_HT_NS}approx_traffic") or ""

        # Publication date
        pub_date = _text(item, "pubDate") or ""

        # Related news links
        news_links = []
        for news_item in item.findall(f"{_HT_NS}news_item"):
            news_title = _text(news_item, f"{_HT_NS}news_item_title")
            news_url = _text(news_item, f"{_HT_NS}news_item_url")
            news_source = _text(news_item, f"{_HT_NS}news_item_source")
            if news_url:
                news_links.append({
                    "title": news_title or "",
                    "url": news_url,
                    "source": news_source or "",
                })

        trends.append({
            "trend_title": title,
            "traffic_estimate_raw": traffic_raw,
            "started_at": pub_date,
            "related_news_links": news_links,
            "source_timestamp": datetime.now(timezone.utc).isoformat(),
            "extraction_method": "rss",
            "source_type": SOURCE_TYPE_RSS,
            "source_tier": SOURCE_TIER,
        })

    logger.info(f"[google_trends] Fetched {len(trends)} trends from RSS")
    return trends


def _text(element, tag: str) -> Optional[str]:
    """Safely extract text from an XML element."""
    el = element.find(tag)
    return el.text.strip() if el is not None and el.text else None


def fetch_all(timeout: int = 15) -> Dict[str, Any]:
    """
    Public API — fetch all available Google Trends data.

    Returns {
        "source": "google_trends",
        "tier": 2,
        "status": "healthy" | "degraded" | "down",
        "items": [...],
        "freshness_utc": str,
    }
    """
    items = fetch_trends_rss(timeout=timeout)

    status = "healthy" if items else "degraded"

    return {
        "source": "google_trends",
        "tier": SOURCE_TIER,
        "status": status,
        "items": items,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
    }
