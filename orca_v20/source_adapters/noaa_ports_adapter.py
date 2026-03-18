"""
ORCA v20 — NOAA CO-OPS / PORTS Maritime Conditions Adapter.

Tier 1 — official US government maritime data.

Ingests:
    - Water levels at energy-relevant Gulf Coast ports
    - Wind speed/direction
    - Air pressure
    - Water temperature (where available)

Derives:
    - PORT_HIGH_WIND — wind >25kt at an energy port
    - PORT_SURGE — water level anomaly (>0.5m above predicted)
    - PORT_LOW_PRESSURE — pressure <1005mb (storm indicator)
    - PORT_CLOSURE_RISK — combination of high wind + surge

No API key required (public API).
Rate limiting: respect NOAA by sleeping between station calls.
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("orca_v20.sources.noaa_ports")

NOAA_API_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
SOURCE_TIER = 1
SOURCE_TYPE = "NOAA_PORTS"

# ─────────────────────────────────────────────────────────────────────
# Default Gulf Coast energy port stations
# ─────────────────────────────────────────────────────────────────────

DEFAULT_STATIONS = {
    "houston_galveston": [
        {"id": "8771341", "name": "Galveston Bay Entrance"},
        {"id": "8770475", "name": "Port Arthur"},
    ],
    "corpus_christi": [
        {"id": "8775241", "name": "Aransas Pass"},
    ],
    "louisiana": [
        {"id": "8768094", "name": "Calcasieu Pass"},
        {"id": "8764227", "name": "LAWMA Pilottown"},
    ],
}

# Products to fetch per station
DEFAULT_PRODUCTS = ["water_level", "wind", "air_pressure"]

# ─────────────────────────────────────────────────────────────────────
# Derived signal thresholds
# ─────────────────────────────────────────────────────────────────────

WIND_HIGH_KT = 25          # knots — operational disruption threshold
WIND_EXTREME_KT = 40       # knots — severe / closure-level
PRESSURE_LOW_MB = 1005     # millibars — storm approach indicator
WATER_LEVEL_SURGE_M = 0.5  # meters above normal — surge threshold
WATER_LEVEL_HIGH_M = 1.0   # meters above normal — significant surge


# ─────────────────────────────────────────────────────────────────────
# Station data fetching
# ─────────────────────────────────────────────────────────────────────

def _fetch_product(station_id: str, product: str, timeout: int = 15) -> Optional[Dict]:
    """
    Fetch latest data for one product at one station.

    Uses NOAA CO-OPS datagetter API with the latest 6 hours of data.
    Returns the most recent observation or None.
    """
    try:
        import requests
    except ImportError:
        logger.warning("[noaa_ports] requests not installed")
        return None

    now = datetime.now(timezone.utc)
    begin = now - timedelta(hours=6)

    params = {
        "product": product,
        "begin_date": begin.strftime("%Y%m%d %H:%M"),
        "end_date": now.strftime("%Y%m%d %H:%M"),
        "station": station_id,
        "datum": "MLLW",
        "units": "metric",
        "time_zone": "gmt",
        "format": "json",
        "application": "ORCA_v20",
    }

    try:
        resp = requests.get(NOAA_API_BASE, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        # NOAA returns errors in JSON body sometimes
        if "error" in data:
            logger.debug(f"[noaa_ports] {station_id}/{product}: {data['error'].get('message', 'API error')}")
            return None

        observations = data.get("data", [])
        if not observations:
            return None

        # Return most recent observation
        latest = observations[-1]
        return {
            "product": product,
            "time": latest.get("t", ""),
            "value": latest.get("v", ""),
            "flags": latest.get("f", ""),
            # Wind has direction + speed + gusts
            "direction": latest.get("d", ""),
            "speed": latest.get("s", ""),
            "gust": latest.get("g", ""),
        }

    except Exception as e:
        logger.debug(f"[noaa_ports] {station_id}/{product} fetch failed: {e}")
        return None


def _fetch_station(station_id: str, station_name: str, region: str,
                   products: List[str], timeout: int = 15) -> Dict[str, Any]:
    """
    Fetch all products for one station. Returns a consolidated reading.
    """
    reading = {
        "station_id": station_id,
        "station_name": station_name,
        "region": region,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "water_level_m": None,
        "wind_speed_kt": None,
        "wind_direction": None,
        "wind_gust_kt": None,
        "air_pressure_mb": None,
        "water_temp_c": None,
        "anomaly_flags": [],
        "products_fetched": 0,
        "products_failed": 0,
    }

    for product in products:
        result = _fetch_product(station_id, product, timeout=timeout)

        if result is None:
            reading["products_failed"] += 1
            continue

        reading["products_fetched"] += 1

        if product == "water_level":
            try:
                reading["water_level_m"] = float(result["value"])
            except (ValueError, TypeError):
                pass

        elif product == "wind":
            try:
                # NOAA wind speed is in m/s, convert to knots
                speed_ms = float(result.get("speed", 0))
                reading["wind_speed_kt"] = round(speed_ms * 1.94384, 1)
                reading["wind_direction"] = result.get("direction", "")
                gust = result.get("gust", "")
                if gust:
                    reading["wind_gust_kt"] = round(float(gust) * 1.94384, 1)
            except (ValueError, TypeError):
                pass

        elif product == "air_pressure":
            try:
                reading["air_pressure_mb"] = float(result["value"])
            except (ValueError, TypeError):
                pass

        elif product == "water_temperature":
            try:
                reading["water_temp_c"] = float(result["value"])
            except (ValueError, TypeError):
                pass

        # Throttle slightly between products to be respectful
        time.sleep(0.3)

    return reading


# ─────────────────────────────────────────────────────────────────────
# Derived signal generation
# ─────────────────────────────────────────────────────────────────────

def _derive_signals(readings: List[Dict]) -> List[Dict]:
    """
    Analyze station readings and produce derived maritime signals.

    Each signal is an interpretable event-like item suitable for
    evidence packs and thesis enrichment.
    """
    signals = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for r in readings:
        station = f"{r['station_name']} ({r['region']})"
        flags = []

        # ── High wind ──
        wind = r.get("wind_speed_kt")
        if wind and wind >= WIND_HIGH_KT:
            severity = "high" if wind >= WIND_EXTREME_KT else "elevated"
            flags.append("HIGH_WIND")
            signals.append({
                "signal_type": "PORT_HIGH_WIND",
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                "region": r["region"],
                "wind_speed_kt": wind,
                "wind_direction": r.get("wind_direction", ""),
                "severity": severity,
                "observed_at": r["observed_at"],
                "source_timestamp": now_iso,
                "source_type": "noaa_ports_signal",
                "source_tier": SOURCE_TIER,
                "ingest_method": "NOAA_PORTS",
                "headline": f"{station}: wind {wind:.0f}kt {r.get('wind_direction', '')} — {severity} disruption risk",
                "summary": (
                    f"NOAA reports {wind:.0f}kt winds at {station}. "
                    f"{'Exceeds severe threshold — port operations likely disrupted.' if severity == 'high' else 'Above operational threshold — potential delays.'}"
                ),
            })

        # ── Low pressure ──
        pressure = r.get("air_pressure_mb")
        if pressure and pressure < PRESSURE_LOW_MB:
            flags.append("LOW_PRESSURE")
            signals.append({
                "signal_type": "PORT_LOW_PRESSURE",
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                "region": r["region"],
                "pressure_mb": pressure,
                "severity": "elevated",
                "observed_at": r["observed_at"],
                "source_timestamp": now_iso,
                "source_type": "noaa_ports_signal",
                "source_tier": SOURCE_TIER,
                "ingest_method": "NOAA_PORTS",
                "headline": f"{station}: pressure {pressure:.1f}mb — storm approach indicator",
                "summary": (
                    f"Air pressure at {station} dropped to {pressure:.1f}mb, "
                    f"below the {PRESSURE_LOW_MB}mb storm threshold. "
                    f"May indicate approaching weather system."
                ),
            })

        # ── Water level surge ──
        wl = r.get("water_level_m")
        if wl is not None and wl > WATER_LEVEL_SURGE_M:
            severity = "high" if wl > WATER_LEVEL_HIGH_M else "elevated"
            flags.append("SURGE")
            signals.append({
                "signal_type": "PORT_SURGE",
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                "region": r["region"],
                "water_level_m": wl,
                "severity": severity,
                "observed_at": r["observed_at"],
                "source_timestamp": now_iso,
                "source_type": "noaa_ports_signal",
                "source_tier": SOURCE_TIER,
                "ingest_method": "NOAA_PORTS",
                "headline": f"{station}: water level +{wl:.2f}m — {severity} surge",
                "summary": (
                    f"Water level at {station} is {wl:.2f}m above MLLW datum. "
                    f"{'Significant surge — vessel draft restrictions likely.' if severity == 'high' else 'Elevated levels — monitoring.'}"
                ),
            })

        # ── Composite: port closure risk ──
        if "HIGH_WIND" in flags and "SURGE" in flags:
            signals.append({
                "signal_type": "PORT_CLOSURE_RISK",
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                "region": r["region"],
                "wind_speed_kt": wind,
                "water_level_m": wl,
                "severity": "high",
                "observed_at": r["observed_at"],
                "source_timestamp": now_iso,
                "source_type": "noaa_ports_signal",
                "source_tier": SOURCE_TIER,
                "ingest_method": "NOAA_PORTS",
                "headline": f"{station}: HIGH WIND + SURGE — port closure risk",
                "summary": (
                    f"Combined high wind ({wind:.0f}kt) and water surge (+{wl:.2f}m) at {station}. "
                    f"Elevated risk of port closure or vessel traffic restrictions. "
                    f"Energy logistics disruption possible."
                ),
            })

        # Store flags back on reading
        r["anomaly_flags"] = flags

    return signals


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

def fetch_all(timeout: int = 15) -> Dict[str, Any]:
    """
    Fetch maritime conditions from all configured NOAA PORTS stations.

    Returns standardized result dict with:
      - Raw station readings (one item per station)
      - Derived signal items (anomalies, closure risks)

    Both raw readings and derived signals are included in the items list.
    """
    from orca_v20.config import SOURCES

    stations_config = getattr(SOURCES, "noaa_ports_stations", DEFAULT_STATIONS)
    products = getattr(SOURCES, "noaa_ports_products", DEFAULT_PRODUCTS)

    readings = []
    stations_ok = 0
    stations_failed = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for region, station_list in stations_config.items():
        for station in station_list:
            sid = station["id"]
            sname = station["name"]
            logger.debug(f"[noaa_ports] Fetching {sname} ({sid})...")

            reading = _fetch_station(sid, sname, region, products, timeout=timeout)

            if reading["products_fetched"] > 0:
                stations_ok += 1
            else:
                stations_failed += 1

            # Build headline + summary for the raw station item
            parts = []
            if reading["wind_speed_kt"] is not None:
                parts.append(f"wind {reading['wind_speed_kt']:.0f}kt {reading['wind_direction'] or ''}")
            if reading["water_level_m"] is not None:
                parts.append(f"water +{reading['water_level_m']:.2f}m")
            if reading["air_pressure_mb"] is not None:
                parts.append(f"pressure {reading['air_pressure_mb']:.0f}mb")

            reading["headline"] = f"{sname}: {', '.join(parts)}" if parts else f"{sname}: no data"
            reading["summary"] = f"NOAA PORTS reading at {sname} ({region}): {', '.join(parts)}"
            reading["source_timestamp"] = now_iso
            reading["source_type"] = SOURCE_TYPE
            reading["source_tier"] = SOURCE_TIER
            reading["ingest_method"] = "NOAA_PORTS"

            readings.append(reading)

            # Throttle between stations
            time.sleep(0.5)

    # Derive anomaly signals
    derived = _derive_signals(readings)

    # Combine raw readings + derived signals
    all_items = readings + derived

    # Determine health
    total_stations = stations_ok + stations_failed
    if total_stations == 0:
        status = "down"
    elif stations_failed == 0:
        status = "healthy"
    elif stations_ok > 0:
        status = "degraded"
    else:
        status = "down"

    if derived:
        logger.info(f"[noaa_ports] {len(derived)} derived signal(s): "
                     f"{', '.join(s['signal_type'] for s in derived)}")

    return {
        "source": "noaa_ports",
        "tier": SOURCE_TIER,
        "status": status,
        "items": all_items,
        "count": len(all_items),
        "stations_ok": stations_ok,
        "stations_failed": stations_failed,
        "derived_signals": len(derived),
        "freshness_utc": now_iso,
    }
