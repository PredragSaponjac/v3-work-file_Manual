"""
ORCA v20 — NWS / weather.gov Source Adapter.

Tier 1 — official US government source.

Ingests:
    - Active weather alerts (api.weather.gov/alerts/active)
    - Severe weather / freeze / hurricane / storm products
    - Headline + severity for catalyst-relevant weather events

No API key required (public API, rate-limited by User-Agent).
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.nws")

ALERTS_URL = "https://api.weather.gov/alerts/active"
SOURCE_TIER = 1
SOURCE_TYPE = "NWS_ALERT"

# Severity levels that may impact markets
MARKET_RELEVANT_SEVERITIES = {"Extreme", "Severe"}

# Event types relevant to ORCA's commodity/energy/agriculture thesis
MARKET_RELEVANT_EVENTS = {
    "Hurricane", "Tropical Storm", "Storm Surge",
    "Tornado", "Severe Thunderstorm",
    "Blizzard", "Ice Storm", "Winter Storm", "Freeze",
    "Extreme Cold", "Extreme Heat", "Heat",
    "Flood", "Flash Flood", "Coastal Flood",
    "Fire Weather", "Red Flag",
    "Tsunami",
    "Volcanic Ash",
}

# Regions of interest for energy/commodity markets
DEFAULT_REGIONS = [
    "Gulf Coast",   # oil/gas/refinery
    "TX", "LA",     # energy corridor
    "OK", "ND",     # oil production
    "CA", "FL",     # agriculture/insurance
]


def fetch_active_alerts(
    severity: Optional[List[str]] = None,
    status: str = "actual",
    timeout: int = 15,
) -> List[Dict[str, Any]]:
    """
    Fetch active weather alerts from NWS API.

    Returns list of alert dicts with:
        product_type, zone_region, severity, effective_time,
        expires_time, headline, summary, source_timestamp, source_tier
    """
    try:
        import requests
    except ImportError:
        logger.warning("[nws] requests not installed")
        return []

    params = {"status": status}
    if severity:
        params["severity"] = ",".join(severity)

    try:
        resp = requests.get(
            ALERTS_URL,
            params=params,
            headers={
                "User-Agent": "ORCA-v20/1.0 (research bot, contact: orca@example.com)",
                "Accept": "application/geo+json",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[nws] Alert fetch failed: {e}")
        return []

    features = data.get("features", [])
    alerts = []

    for feature in features:
        props = feature.get("properties", {})

        event = props.get("event", "")
        sev = props.get("severity", "")
        headline = props.get("headline", "")
        description = props.get("description", "")
        area_desc = props.get("areaDesc", "")

        # Filter for market-relevant events
        is_relevant = (
            sev in MARKET_RELEVANT_SEVERITIES
            or any(evt in event for evt in MARKET_RELEVANT_EVENTS)
        )

        if not is_relevant:
            continue

        alerts.append({
            "product_type": event,
            "zone_region": area_desc[:200] if area_desc else "",
            "severity": sev,
            "effective_time": props.get("effective", ""),
            "expires_time": props.get("expires", ""),
            "headline": headline[:300] if headline else "",
            "summary": description[:500] if description else "",
            "source_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_type": SOURCE_TYPE,
            "source_tier": SOURCE_TIER,
            "urgency": props.get("urgency", ""),
            "certainty": props.get("certainty", ""),
            "sender": props.get("senderName", ""),
        })

    logger.info(f"[nws] Fetched {len(alerts)} market-relevant alerts (of {len(features)} total)")
    return alerts


def fetch_all(timeout: int = 15) -> Dict[str, Any]:
    """
    Public API — fetch all available NWS data.

    Returns {
        "source": "nws",
        "tier": 1,
        "status": "healthy" | "degraded" | "down",
        "items": [...],
        "freshness_utc": str,
    }
    """
    try:
        items = fetch_active_alerts(timeout=timeout)
        status = "healthy"
    except Exception as e:
        logger.error(f"[nws] Fetch failed: {e}")
        items = []
        status = "down"

    return {
        "source": "nws",
        "tier": SOURCE_TIER,
        "status": status,
        "items": items,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
    }
