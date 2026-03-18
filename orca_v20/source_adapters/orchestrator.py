"""
ORCA v20 — Source Orchestrator.

Centralized ingestion layer that runs all enabled source adapters,
tracks health, and feeds results into RetrievalState for downstream
consumption by the evidence gate and CIO stages.

Source trust hierarchy:
    Tier 1 (official/primary): NWS, EIA, NOAA PORTS
    Tier 2 (broad discovery):  GDELT, Google Trends, AISstream
    Tier 3 (optional enrichment): X whitelist

Source-tier weighting for evidence packs:
    Tier 1 items → relevance_boost = 1.5x
    Tier 2 items → relevance_boost = 1.0x
    Tier 3 items → relevance_boost = 0.6x
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from orca_v20.config import FLAGS
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.sources.orchestrator")

# Tier-based relevance weights for evidence scoring
TIER_WEIGHTS = {1: 1.5, 2: 1.0, 3: 0.6}


def ingest_all_sources(ctx: RunContext) -> Dict[str, Any]:
    """
    Run all enabled source adapters and return consolidated results.

    Updates RetrievalState with source health and cached items.
    Degrades gracefully — a failing source never blocks the pipeline.

    Returns {
        "sources": {source_name: fetch_result, ...},
        "total_items": int,
        "source_health": {source_name: status, ...},
    }
    """
    results = {}
    health = {}

    # ── Tier 1: Official sources (always try first) ──

    if getattr(FLAGS, "enable_nws", True):
        try:
            from orca_v20.source_adapters.nws_adapter import fetch_all as nws_fetch
            results["nws"] = nws_fetch()
            health["nws"] = results["nws"]["status"]
            logger.info(f"[sources] NWS: {results['nws']['count']} items ({results['nws']['status']})")
        except Exception as e:
            logger.warning(f"[sources] NWS adapter failed: {e}")
            health["nws"] = "down"
            results["nws"] = _empty_result("nws", 1, "down")
    else:
        health["nws"] = "disabled"

    if getattr(FLAGS, "enable_eia", True):
        try:
            from orca_v20.source_adapters.eia_adapter import fetch_all as eia_fetch
            results["eia"] = eia_fetch()
            health["eia"] = results["eia"]["status"]
            logger.info(f"[sources] EIA: {results['eia']['count']} items ({results['eia']['status']})")
        except Exception as e:
            logger.warning(f"[sources] EIA adapter failed: {e}")
            health["eia"] = "down"
            results["eia"] = _empty_result("eia", 1, "down")
    else:
        health["eia"] = "disabled"

    if getattr(FLAGS, "enable_noaa_ports", True):
        try:
            from orca_v20.source_adapters.noaa_ports_adapter import fetch_all as noaa_ports_fetch
            results["noaa_ports"] = noaa_ports_fetch()
            health["noaa_ports"] = results["noaa_ports"]["status"]
            logger.info(f"[sources] NOAA PORTS: {results['noaa_ports']['count']} items ({results['noaa_ports']['status']})")
        except Exception as e:
            logger.warning(f"[sources] NOAA PORTS adapter failed: {e}")
            health["noaa_ports"] = "down"
            results["noaa_ports"] = _empty_result("noaa_ports", 1, "down")
    else:
        health["noaa_ports"] = "disabled"

    # ── Tier 2: Discovery sources ──

    if getattr(FLAGS, "enable_google_trends", True):
        try:
            from orca_v20.source_adapters.google_trends import fetch_all as trends_fetch
            results["google_trends"] = trends_fetch()
            health["google_trends"] = results["google_trends"]["status"]
            logger.info(f"[sources] Google Trends: {results['google_trends']['count']} items ({results['google_trends']['status']})")
        except Exception as e:
            logger.warning(f"[sources] Google Trends adapter failed: {e}")
            health["google_trends"] = "down"
            results["google_trends"] = _empty_result("google_trends", 2, "down")
    else:
        health["google_trends"] = "disabled"

    if getattr(FLAGS, "enable_gdelt", True):
        try:
            from orca_v20.source_adapters.gdelt_adapter import fetch_all as gdelt_fetch
            results["gdelt"] = gdelt_fetch()
            health["gdelt"] = results["gdelt"]["status"]
            logger.info(f"[sources] GDELT: {results['gdelt']['count']} items ({results['gdelt']['status']})")
        except Exception as e:
            logger.warning(f"[sources] GDELT adapter failed: {e}")
            health["gdelt"] = "down"
            results["gdelt"] = _empty_result("gdelt", 2, "down")
    else:
        health["gdelt"] = "disabled"

    if getattr(FLAGS, "enable_aisstream", True):
        try:
            from orca_v20.source_adapters.aisstream_adapter import fetch_all as ais_fetch
            results["aisstream"] = ais_fetch()
            health["aisstream"] = results["aisstream"]["status"]
            logger.info(f"[sources] AISstream: {results['aisstream']['count']} items ({results['aisstream']['status']})")
        except Exception as e:
            logger.warning(f"[sources] AISstream adapter failed: {e}")
            health["aisstream"] = "down"
            results["aisstream"] = _empty_result("aisstream", 2, "down")
    else:
        health["aisstream"] = "disabled"

    # ── Tier 3: Optional enrichment ──

    if getattr(FLAGS, "enable_x_whitelist", True):
        try:
            from orca_v20.source_adapters.x_whitelist_adapter import fetch_all as x_fetch
            results["x_whitelist"] = x_fetch()
            health["x_whitelist"] = results["x_whitelist"]["status"]
            logger.info(f"[sources] X whitelist: {results['x_whitelist']['count']} items ({results['x_whitelist']['status']})")
        except Exception as e:
            logger.warning(f"[sources] X whitelist adapter failed: {e}")
            health["x_whitelist"] = "down"
            results["x_whitelist"] = _empty_result("x_whitelist", 3, "down")
    else:
        health["x_whitelist"] = "disabled"

    # ── Stubs (disabled by default) ──

    if getattr(FLAGS, "enable_navcen", False):
        try:
            from orca_v20.source_adapters.navcen_adapter import fetch_all as navcen_fetch
            results["navcen"] = navcen_fetch()
            health["navcen"] = results["navcen"]["status"]
            logger.info(f"[sources] NAVCEN: {results['navcen']['status']}")
        except Exception as e:
            logger.warning(f"[sources] NAVCEN adapter failed: {e}")
            health["navcen"] = "down"
            results["navcen"] = _empty_result("navcen", 1, "down")
    else:
        health["navcen"] = "disabled"

    # Tally
    total = sum(r.get("count", 0) for r in results.values())
    tier1_count = sum(r.get("count", 0) for r in results.values() if r.get("tier") == 1)
    tier2_count = sum(r.get("count", 0) for r in results.values() if r.get("tier") == 2)
    tier3_count = sum(r.get("count", 0) for r in results.values() if r.get("tier") == 3)

    logger.info(
        f"[sources] Total: {total} items "
        f"(T1={tier1_count}, T2={tier2_count}, T3={tier3_count})"
    )

    return {
        "sources": results,
        "total_items": total,
        "tier_counts": {"tier_1": tier1_count, "tier_2": tier2_count, "tier_3": tier3_count},
        "source_health": health,
        "fetched_utc": datetime.now(timezone.utc).isoformat(),
    }


def update_retrieval_state(retrieval_state, source_results: Dict[str, Any]) -> None:
    """
    Push source results into the RetrievalState for downstream consumption.

    Sets:
        - "v20_sources" → full results dict
        - "v20_source_health" → health summary
        - Per-source health marks
    """
    retrieval_state.set("v20_sources", source_results)
    retrieval_state.set("v20_source_health", source_results.get("source_health", {}))

    for source_name, status in source_results.get("source_health", {}).items():
        retrieval_state.mark_source_health(source_name, status)


def get_items_for_ticker(
    source_results: Dict[str, Any],
    ticker: str,
    keywords: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Filter source items relevant to a specific ticker or keyword set.

    Used by evidence gate to build per-idea evidence packs with
    tier-weighted relevance.
    """
    matching = []
    search_terms = [ticker.lower()]
    if keywords:
        search_terms.extend(k.lower() for k in keywords)

    for source_name, result in source_results.get("sources", {}).items():
        tier = result.get("tier", 3)
        weight = TIER_WEIGHTS.get(tier, 0.6)

        for item in result.get("items", []):
            # Search across common text fields
            searchable = " ".join([
                str(item.get("title", "")),
                str(item.get("headline", "")),
                str(item.get("trend_title", "")),
                str(item.get("text", "")),
                str(item.get("summary", "")),
                str(item.get("series_label", "")),
                str(item.get("matched_query", "")),
            ]).lower()

            if any(term in searchable for term in search_terms):
                item_copy = dict(item)
                item_copy["_matched_source"] = source_name
                item_copy["_tier_weight"] = weight
                matching.append(item_copy)

    return matching


def _empty_result(source: str, tier: int, status: str) -> Dict[str, Any]:
    """Create an empty result for a failed/disabled source."""
    return {
        "source": source,
        "tier": tier,
        "status": status,
        "items": [],
        "count": 0,
        "freshness_utc": datetime.now(timezone.utc).isoformat(),
    }
