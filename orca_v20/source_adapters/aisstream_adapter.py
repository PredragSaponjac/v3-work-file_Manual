"""
ORCA v20 — AISstream Vessel Tracking Adapter (Snapshot Mode).

Tier 2 — free AIS data, valuable for energy/logistics intelligence.

Connects to AISstream WebSocket, subscribes to configured bounding boxes,
collects vessel positions for a short duration (default 60s), disconnects,
then derives regional signals (tanker congestion, slow vessels, cluster density).

Does NOT store raw vessel positions — only interpretable signals.

Requires API key: AISSTREAM_API_KEY env var.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.aisstream")

AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"
SOURCE_TIER = 2
SOURCE_TYPE = "AISSTREAM"

# ─────────────────────────────────────────────────────────────────────
# Default bounding boxes — energy-relevant maritime regions
# Format: [[lat_min, lon_min], [lat_max, lon_max]]
# ─────────────────────────────────────────────────────────────────────

DEFAULT_BOUNDING_BOXES = {
    "hormuz": {
        "label": "Strait of Hormuz",
        "bbox": [[24.5, 55.5], [27.5, 57.0]],
    },
    "persian_gulf": {
        "label": "Persian Gulf (wider)",
        "bbox": [[24.0, 49.0], [30.0, 55.0]],
    },
    "houston_galveston": {
        "label": "Houston / Galveston",
        "bbox": [[28.8, -95.5], [29.9, -94.0]],
    },
    "corpus_christi": {
        "label": "Corpus Christi",
        "bbox": [[27.4, -97.3], [27.9, -96.8]],
    },
    "calcasieu_cameron": {
        "label": "Calcasieu / Cameron LNG",
        "bbox": [[29.6, -93.5], [29.9, -93.2]],
    },
    "sabine_pass": {
        "label": "Sabine Pass",
        "bbox": [[29.6, -93.8], [29.9, -93.5]],
    },
}

# ─────────────────────────────────────────────────────────────────────
# Configurable severity thresholds
# ─────────────────────────────────────────────────────────────────────

TANKER_COUNT_ELEVATED = 10   # tankers in region → elevated
TANKER_COUNT_HIGH = 20       # tankers in region → high
SLOW_VESSEL_ELEVATED = 3     # slow/stopped vessels → elevated
SLOW_VESSEL_HIGH = 6         # slow/stopped vessels → high
SLOW_SPEED_KT = 1.0          # SOG below this = "slow/stopped"

DEFAULT_SNAPSHOT_DURATION_SEC = 60

# ─────────────────────────────────────────────────────────────────────
# AIS vessel type classification
# ─────────────────────────────────────────────────────────────────────

def _classify_vessel(vessel_type: int) -> str:
    """
    Map AIS vessel type code to a human-readable category.

    AIS vessel type codes (ITU-R M.1371):
      80-89: Tanker
      70-79: Cargo
      60-69: Passenger
      30-39: Fishing / Towing / Dredging
      40-49: High-speed craft
      50-59: Special craft / SAR
    """
    if vessel_type is None:
        return "UNKNOWN"
    vt = int(vessel_type)
    if 80 <= vt <= 89:
        if vt == 81:
            return "TANKER_HAZARDOUS_A"
        elif vt == 82:
            return "TANKER_HAZARDOUS_B"
        elif vt == 84:
            return "LNG_CARRIER"  # Often coded as 84
        return "TANKER"
    elif 70 <= vt <= 79:
        return "CARGO"
    elif 60 <= vt <= 69:
        return "PASSENGER"
    elif 30 <= vt <= 39:
        return "SERVICE"  # Fishing, towing, dredging
    elif 40 <= vt <= 49:
        return "HIGH_SPEED"
    elif 50 <= vt <= 59:
        return "SPECIAL"  # SAR, law enforcement, medical
    else:
        return "OTHER"


def _is_energy_vessel(category: str) -> bool:
    """Check if vessel category is energy-relevant (tankers, LNG carriers)."""
    return category in ("TANKER", "TANKER_HAZARDOUS_A", "TANKER_HAZARDOUS_B", "LNG_CARRIER")


# ─────────────────────────────────────────────────────────────────────
# WebSocket snapshot — collect positions for a fixed duration
# ─────────────────────────────────────────────────────────────────────

def _snapshot_positions(
    api_key: str,
    bboxes: Dict[str, Dict],
    duration_sec: int = DEFAULT_SNAPSHOT_DURATION_SEC,
    timeout: int = 90,
) -> Dict[str, List[Dict]]:
    """
    Connect to AISstream WebSocket, subscribe to bounding boxes,
    collect vessel positions for `duration_sec`, then disconnect.

    Returns dict keyed by region name, each containing a list of
    vessel position dicts.
    """
    try:
        import asyncio
        import json
        import websockets
    except ImportError as e:
        logger.warning(f"[aisstream] Missing dependency: {e}")
        return {}

    # Build subscription message
    bbox_list = [v["bbox"] for v in bboxes.values()]
    subscribe_msg = json.dumps({
        "APIKey": api_key,
        "BoundingBoxes": bbox_list,
        "FiltersShipMMSI": [],
        "FilterMessageTypes": ["PositionReport"],
    })

    # Region lookup by bounding box
    region_map = {}
    for region_key, region_data in bboxes.items():
        bb = region_data["bbox"]
        region_map[region_key] = {
            "label": region_data["label"],
            "lat_min": bb[0][0], "lon_min": bb[0][1],
            "lat_max": bb[1][0], "lon_max": bb[1][1],
        }

    positions_by_region: Dict[str, List[Dict]] = {k: [] for k in bboxes}
    seen_mmsi = set()  # Deduplicate by MMSI per snapshot

    async def _collect():
        try:
            async with websockets.connect(
                AISSTREAM_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                await ws.send(subscribe_msg)
                logger.debug(f"[aisstream] Subscribed to {len(bbox_list)} bounding boxes")

                start = time.monotonic()
                while (time.monotonic() - start) < duration_sec:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.debug("[aisstream] Connection closed during snapshot")
                        break

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Extract position data
                    meta = msg.get("MetaData", {})
                    position = msg.get("Message", {}).get("PositionReport", {})
                    if not position:
                        continue

                    mmsi = meta.get("MMSI", "")
                    if mmsi in seen_mmsi:
                        continue
                    seen_mmsi.add(mmsi)

                    lat = position.get("Latitude", 0)
                    lon = position.get("Longitude", 0)
                    sog = position.get("Sog", 0)  # Speed over ground in 1/10 knot
                    cog = position.get("Cog", 0)  # Course over ground
                    vessel_type = meta.get("ShipType", None)

                    # Classify vessel
                    category = _classify_vessel(vessel_type)

                    # Determine which region this position falls in
                    for region_key, rm in region_map.items():
                        if (rm["lat_min"] <= lat <= rm["lat_max"] and
                                rm["lon_min"] <= lon <= rm["lon_max"]):
                            positions_by_region[region_key].append({
                                "mmsi": mmsi,
                                "lat": lat,
                                "lon": lon,
                                "sog_kt": sog / 10.0,  # AIS SOG is in 1/10 knot
                                "cog": cog,
                                "vessel_type": vessel_type,
                                "category": category,
                                "ship_name": meta.get("ShipName", "").strip(),
                            })
                            break  # Assign to first matching region

                logger.debug(f"[aisstream] Snapshot complete: {len(seen_mmsi)} unique vessels in {duration_sec}s")

        except Exception as e:
            logger.warning(f"[aisstream] WebSocket error: {e}")

    # Run async collection in sync context
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.wait_for(_collect(), timeout=timeout))
    except asyncio.TimeoutError:
        logger.warning(f"[aisstream] Snapshot timed out after {timeout}s")
    except Exception as e:
        logger.warning(f"[aisstream] Snapshot failed: {e}")
    finally:
        loop.close()

    return positions_by_region


# ─────────────────────────────────────────────────────────────────────
# Derive marine signals from snapshot positions
# ─────────────────────────────────────────────────────────────────────

def _derive_marine_signals(
    positions_by_region: Dict[str, List[Dict]],
    bboxes: Dict[str, Dict],
    snapshot_duration_sec: int,
) -> List[Dict]:
    """
    Analyze collected vessel positions and produce derived marine signals.

    Produces one signal per region that has any vessels.
    Signal severity based on configurable thresholds.
    """
    signals = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for region_key, vessels in positions_by_region.items():
        if not vessels:
            continue

        region_label = bboxes.get(region_key, {}).get("label", region_key)

        # Count by category
        total = len(vessels)
        tankers = [v for v in vessels if _is_energy_vessel(v["category"])]
        tanker_count = len(tankers)

        # Slow/stopped vessels (SOG < threshold)
        slow = [v for v in vessels if v["sog_kt"] < SLOW_SPEED_KT]
        slow_count = len(slow)
        slow_tankers = [v for v in slow if _is_energy_vessel(v["category"])]

        # Average speed
        speeds = [v["sog_kt"] for v in vessels if v["sog_kt"] > 0]
        avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0

        # Determine severity
        severity = "normal"
        if tanker_count >= TANKER_COUNT_HIGH or slow_count >= SLOW_VESSEL_HIGH:
            severity = "high"
        elif tanker_count >= TANKER_COUNT_ELEVATED or slow_count >= SLOW_VESSEL_ELEVATED:
            severity = "elevated"

        # Build signal
        signal_type = "MARINE_TRAFFIC"
        if severity != "normal" and tanker_count > 0:
            signal_type = "TANKER_CONGESTION"

        # Headline
        parts = []
        if tanker_count > 0:
            parts.append(f"{tanker_count} tanker{'s' if tanker_count != 1 else ''}")
        if slow_count > 0:
            parts.append(f"{slow_count} slow/stopped")
        if not parts:
            parts.append(f"{total} vessel{'s' if total != 1 else ''}")

        severity_label = f" — {severity}" if severity != "normal" else ""
        headline = f"{region_label}: {', '.join(parts)}{severity_label}"

        # Summary
        summary_parts = [
            f"AIS snapshot ({snapshot_duration_sec}s) detected {total} vessels"
            f" in {region_label} region.",
        ]
        if tanker_count > 0:
            summary_parts.append(f"{tanker_count} energy-relevant vessels (tankers/LNG).")
        if slow_count > 0:
            summary_parts.append(
                f"{slow_count} vessel{'s' if slow_count != 1 else ''} "
                f"slow/stopped (SOG < {SLOW_SPEED_KT}kt)."
            )
        if severity == "high":
            summary_parts.append("Elevated congestion risk — potential logistics disruption.")
        elif severity == "elevated":
            summary_parts.append("Above-normal traffic — monitoring.")

        signals.append({
            "signal_type": signal_type,
            "region": region_key,
            "region_label": region_label,
            "vessel_count": total,
            "tanker_count": tanker_count,
            "slow_vessels": slow_count,
            "slow_tankers": len(slow_tankers),
            "avg_speed_kt": avg_speed,
            "snapshot_duration_sec": snapshot_duration_sec,
            "severity": severity,
            "observed_at": now_iso,
            "source_timestamp": now_iso,
            "source_type": SOURCE_TYPE,
            "source_tier": SOURCE_TIER,
            "ingest_method": "AISSTREAM",
            "headline": headline,
            "summary": " ".join(summary_parts),
        })

    return signals


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

def fetch_all(timeout: int = 90) -> Dict[str, Any]:
    """
    Fetch maritime vessel data via AISstream snapshot mode.

    Connect → subscribe → collect for ~60s → disconnect → derive signals.

    Returns standardized result dict with:
      - Derived signal items (regional traffic summaries, congestion alerts)
      - No raw vessel positions stored

    If no API key is configured, returns status: "no_key".
    """
    from orca_v20.config import SOURCES

    # Get API key
    api_key_env = getattr(SOURCES, "aisstream_api_key_env", "AISSTREAM_API_KEY")
    api_key = os.environ.get(api_key_env, "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    if not api_key:
        logger.info("[aisstream] No API key configured — skipping")
        return {
            "source": "aisstream",
            "tier": SOURCE_TIER,
            "status": "no_key",
            "items": [],
            "count": 0,
            "freshness_utc": now_iso,
        }

    # Get config
    duration = getattr(SOURCES, "aisstream_snapshot_duration_sec", DEFAULT_SNAPSHOT_DURATION_SEC)
    bboxes = getattr(SOURCES, "marine_bounding_boxes", DEFAULT_BOUNDING_BOXES)

    # Normalize bboxes if they're raw lists (config might store just coords)
    normalized = {}
    for key, val in bboxes.items():
        if isinstance(val, dict) and "bbox" in val:
            normalized[key] = val
        elif isinstance(val, list):
            # Config stores [[lat_min, lon_min], [lat_max, lon_max]]
            normalized[key] = {"label": key.replace("_", " ").title(), "bbox": val}
        else:
            normalized[key] = {"label": key.replace("_", " ").title(), "bbox": val}
    bboxes = normalized

    logger.info(f"[aisstream] Starting snapshot: {len(bboxes)} regions, {duration}s duration")

    # Collect positions
    positions = _snapshot_positions(api_key, bboxes, duration_sec=duration, timeout=timeout)

    if not positions or all(len(v) == 0 for v in positions.values()):
        logger.info("[aisstream] No vessel positions collected")
        return {
            "source": "aisstream",
            "tier": SOURCE_TIER,
            "status": "degraded",
            "items": [],
            "count": 0,
            "freshness_utc": now_iso,
        }

    # Derive signals
    signals = _derive_marine_signals(positions, bboxes, duration)

    total_vessels = sum(len(v) for v in positions.values())
    regions_with_data = sum(1 for v in positions.values() if len(v) > 0)

    logger.info(
        f"[aisstream] Snapshot complete: {total_vessels} vessels across "
        f"{regions_with_data} regions, {len(signals)} signal(s)"
    )

    elevated = [s for s in signals if s["severity"] != "normal"]
    if elevated:
        logger.info(
            f"[aisstream] {len(elevated)} elevated signal(s): "
            f"{', '.join(s['region'] + '=' + s['severity'] for s in elevated)}"
        )

    return {
        "source": "aisstream",
        "tier": SOURCE_TIER,
        "status": "healthy",
        "items": signals,
        "count": len(signals),
        "total_vessels_seen": total_vessels,
        "regions_with_data": regions_with_data,
        "snapshot_duration_sec": duration,
        "freshness_utc": now_iso,
    }
