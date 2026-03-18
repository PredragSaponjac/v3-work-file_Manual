"""
ORCA v20 — NAVCEN / NAIS Adapter (Graceful Stub).

NAIS (Nationwide Automatic Identification System) real-time access
requires USCG authorization and is not available for commercial or
trading use. The web-based VIVS lookup exists but has no REST API.

This stub returns status: "unavailable" so the orchestrator can
report it cleanly. Can be upgraded if access is ever obtained.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("orca_v20.sources.navcen")

SOURCE_TIER = 1  # Would be Tier 1 if available (official government)
SOURCE_TYPE = "NAVCEN_NAIS"


def fetch_all(timeout: int = 15) -> Dict[str, Any]:
    """
    NAVCEN/NAIS stub — returns unavailable status.

    NAIS real-time data requires USCG authorization.
    This adapter exists as a placeholder for future integration.
    """
    logger.debug("[navcen] NAIS real-time access not available (requires USCG auth)")

    return {
        "source": "navcen_nais",
        "tier": SOURCE_TIER,
        "status": "unavailable",
        "items": [],
        "count": 0,
        "note": "NAIS real-time access requires USCG authorization. Not available for commercial use.",
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
    }
