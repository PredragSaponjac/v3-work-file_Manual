"""
ORCA v20 — EIA (Energy Information Administration) Source Adapter.

Tier 1 — official US government energy data.

Supports:
    - Petroleum inventories (weekly)
    - Natural gas storage (weekly)
    - Relevant energy release series

API key from environment variable EIA_API_KEY only.
NO hardcoded secrets. Degrades gracefully if key is absent.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.eia")

EIA_API_BASE = "https://api.eia.gov/v2"
SOURCE_TIER = 1
SOURCE_TYPE = "EIA_API"

# Key petroleum and gas series
DEFAULT_SERIES = {
    # Weekly petroleum status report
    "petroleum_crude_stocks": {
        "route": "/petroleum/sum/sndw/data/",
        "params": {"frequency": "weekly", "data[0]": "value",
                   "facets[product][]": "EPC0", "facets[process][]": "SAE",
                   "sort[0][column]": "period", "sort[0][direction]": "desc",
                   "length": "4"},
        "label": "US Crude Oil Stocks (Weekly)",
        "units": "thousand barrels",
    },
    # Weekly nat gas storage
    "natgas_storage": {
        "route": "/natural-gas/stor/wkly/data/",
        "params": {"frequency": "weekly", "data[0]": "value",
                   "facets[process][]": "SAE",
                   "sort[0][column]": "period", "sort[0][direction]": "desc",
                   "length": "4"},
        "label": "US Natural Gas Storage (Weekly)",
        "units": "billion cubic feet",
    },
    # Weekly petroleum product supplied (demand proxy)
    "petroleum_product_supplied": {
        "route": "/petroleum/sum/sndw/data/",
        "params": {"frequency": "weekly", "data[0]": "value",
                   "facets[product][]": "EPPM", "facets[process][]": "VPP",
                   "sort[0][column]": "period", "sort[0][direction]": "desc",
                   "length": "4"},
        "label": "US Petroleum Products Supplied (Weekly)",
        "units": "thousand barrels per day",
    },
}


def _get_api_key() -> Optional[str]:
    """Get EIA API key from environment. Never hardcoded."""
    return os.environ.get("EIA_API_KEY", "")


def fetch_series(series_id: str, timeout: int = 15) -> List[Dict[str, Any]]:
    """
    Fetch a specific EIA data series.

    Returns list of data point dicts with:
        series_id, reported_value, period, units,
        release_timestamp, source_timestamp, api_key_present
    """
    api_key = _get_api_key()
    if not api_key:
        logger.info(f"[eia] No EIA_API_KEY — skipping {series_id}")
        return []

    series_config = DEFAULT_SERIES.get(series_id)
    if not series_config:
        logger.warning(f"[eia] Unknown series: {series_id}")
        return []

    try:
        import requests
    except ImportError:
        logger.warning("[eia] requests not installed")
        return []

    url = f"{EIA_API_BASE}{series_config['route']}"
    params = dict(series_config["params"])
    params["api_key"] = api_key

    try:
        resp = requests.get(url, params=params, timeout=timeout, headers={
            "User-Agent": "ORCA-v20/1.0",
        })
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[eia] API call failed for {series_id}: {e}")
        return []

    response_data = data.get("response", {}).get("data", [])
    results = []

    for point in response_data:
        results.append({
            "series_id": series_id,
            "series_label": series_config["label"],
            "reported_value": point.get("value"),
            "period": point.get("period", ""),
            "units": series_config["units"],
            "release_timestamp": point.get("period", ""),
            "source_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_type": SOURCE_TYPE,
            "source_tier": SOURCE_TIER,
            "api_key_present": True,
        })

    logger.info(f"[eia] Fetched {len(results)} data points for {series_id}")
    return results


def fetch_all(timeout: int = 15) -> Dict[str, Any]:
    """
    Public API — fetch all default EIA series.

    Returns {
        "source": "eia",
        "tier": 1,
        "status": "healthy" | "degraded" | "down" | "no_key",
        "items": [...],
        "freshness_utc": str,
        "api_key_present": bool,
    }
    """
    api_key = _get_api_key()
    if not api_key:
        logger.info("[eia] No EIA_API_KEY set — source unavailable (graceful degradation)")
        return {
            "source": "eia",
            "tier": SOURCE_TIER,
            "status": "no_key",
            "items": [],
            "freshness_utc": datetime.now(timezone.utc).isoformat(),
            "count": 0,
            "api_key_present": False,
        }

    all_items = []
    errors = 0

    for series_id in DEFAULT_SERIES:
        try:
            items = fetch_series(series_id, timeout=timeout)
            all_items.extend(items)
        except Exception as e:
            logger.warning(f"[eia] Series {series_id} failed: {e}")
            errors += 1

    if errors == len(DEFAULT_SERIES):
        status = "down"
    elif errors > 0:
        status = "degraded"
    else:
        status = "healthy"

    return {
        "source": "eia",
        "tier": SOURCE_TIER,
        "status": status,
        "items": all_items,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(all_items),
        "api_key_present": True,
    }
