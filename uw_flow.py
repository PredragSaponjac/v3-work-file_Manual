#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Unusual Whales Flow Client (3-Tier Speed-Adaptive)
==============================================================
Fetches options flow, OI change, dark pool, greek exposure, IV data,
historical contract data, and multi-day flow/volume trends from the
Unusual Whales API.

Three memory tiers:
  DEEP:    Contract-level history (OI/vol/IV/bid-ask over N sessions)
  MEDIUM:  Multi-day flow alerts, daily options volume, dark pool, lit flow
  SHALLOW: Greek exposure, IV rank, flow per strike/expiry (point-in-time)

Lookback depth is speed-adaptive per catalyst:
  FAST   (1-5d repricing): short lookback
  MEDIUM (1-3wk repricing): moderate lookback
  SLOW   (3-6wk repricing): deep lookback

Rate limits: 20,000 req/day · 120 req/min
Auth: Bearer token via UW_API_KEY env var

Endpoints used:
  /api/option-trades/flow-alerts      — multi-day flow alerts (ticker-filtered)
  /api/stock/{ticker}/options-volume   — daily options volume time series
  /api/stock/{ticker}/oi_change        — open interest changes (point-in-time)
  /api/stock/{ticker}/flow_per_expiry  — flow breakdown by expiry
  /api/stock/{ticker}/flow_per_strike  — flow breakdown by strike
  /api/stock/{ticker}/greek_exposure   — net greek exposure
  /api/stock/{ticker}/iv_rank          — IV rank / percentile
  /api/stock/{ticker}/volatility_stats — IV stats
  /api/stock/{ticker}/option-contracts — all option contracts for a ticker
  /api/option-contract/{id}/historic   — daily OI/vol/IV history per contract
  /api/darkpool/{ticker}               — dark pool prints (with date lookback)
  /api/lit-flow/{ticker}               — lit flow (with date lookback)
  /api/option-trade/flow_alerts        — market-wide unusual flow (Stage 1)
"""

import os
import re
import time
import json
import requests
from datetime import datetime, timedelta, date as date_type
from pathlib import Path
from typing import Optional, Dict, List, Any
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://api.unusualwhales.com"
UW_API_KEY = os.environ.get("UW_API_KEY", "")

# Rate limiting — 120 req/min = 2 req/sec
_last_request_time = 0.0
MIN_REQUEST_INTERVAL = 0.55  # slightly conservative

# Cache directory to avoid re-fetching within same run
CACHE_DIR = Path(__file__).parent / "uw_cache"
CACHE_TTL = 300  # 5 min cache

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()
    UW_API_KEY = os.environ.get("UW_API_KEY", UW_API_KEY)


# ---------------------------------------------------------------------------
# Trading Session Helper
# ---------------------------------------------------------------------------
# Major US market holidays for 2025-2027 (update annually)
_US_MARKET_HOLIDAYS = {
    # 2025
    date_type(2025, 1, 1), date_type(2025, 1, 20), date_type(2025, 2, 17),
    date_type(2025, 4, 18), date_type(2025, 5, 26), date_type(2025, 6, 19),
    date_type(2025, 7, 4), date_type(2025, 9, 1), date_type(2025, 11, 27),
    date_type(2025, 12, 25),
    # 2026
    date_type(2026, 1, 1), date_type(2026, 1, 19), date_type(2026, 2, 16),
    date_type(2026, 4, 3), date_type(2026, 5, 25), date_type(2026, 6, 19),
    date_type(2026, 7, 3), date_type(2026, 9, 7), date_type(2026, 11, 26),
    date_type(2026, 12, 25),
    # 2027
    date_type(2027, 1, 1), date_type(2027, 1, 18), date_type(2027, 2, 15),
    date_type(2027, 3, 26), date_type(2027, 5, 31), date_type(2027, 6, 18),
    date_type(2027, 7, 5), date_type(2027, 9, 6), date_type(2027, 11, 25),
    date_type(2027, 12, 24),
}


def _trading_sessions_ago(n: int) -> str:
    """
    Return ISO datetime string for N trading sessions ago.
    Accounts for weekends and major US market holidays.
    5 trading sessions ≠ 5 calendar days.
    """
    current = date_type.today()
    sessions_back = 0
    while sessions_back < n:
        current -= timedelta(days=1)
        if current.weekday() < 5 and current not in _US_MARKET_HOLIDAYS:
            sessions_back += 1
    return current.isoformat() + "T00:00:00"


def _trading_dates_list(n: int) -> List[str]:
    """
    Return list of the last N trading-day date strings (YYYY-MM-DD).
    Most recent first. Skips weekends and major US holidays.
    """
    dates = []
    current = date_type.today()
    while len(dates) < n:
        current -= timedelta(days=1)
        if current.weekday() < 5 and current not in _US_MARKET_HOLIDAYS:
            dates.append(current.isoformat())
    return dates


# ---------------------------------------------------------------------------
# Catalyst Speed Classifier
# ---------------------------------------------------------------------------
# Lookback profiles: trading sessions per data tier
LOOKBACK_PROFILES = {
    "fast": {
        "flow_alerts": 5, "options_volume": 5, "contract_history": 10,
        "darkpool": 3, "lit_flow": 3, "contract_max_dte": 30,
    },
    "medium": {
        "flow_alerts": 10, "options_volume": 10, "contract_history": 15,
        "darkpool": 5, "lit_flow": 5, "contract_max_dte": 45,
    },
    "slow": {
        # flow_alerts hard-capped at 14 — UW API max lookback is 14 days
        "flow_alerts": 14, "options_volume": 15, "contract_history": 20,
        "darkpool": 10, "lit_flow": 10, "contract_max_dte": 75,
    },
}


def classify_catalyst_speed(idea: dict) -> dict:
    """
    Classify catalyst speed from R1 output fields.
    Returns {"speed": str, "lookback": dict} with trading-session counts
    for each data tier.

    Speed classification (from repricing_window, catalyst_status, hard_date_status):
      FAST   (1-5 day repricing): weather, war, rates surprise, headline shock
      MEDIUM (1-3 week repricing): commodity squeeze, policy shift, supply disruption
      SLOW   (3-6 week repricing): regulatory timeline, sector rerating, hard-date event
    """
    repricing = (idea.get("repricing_window", "") or "").lower()
    status = (idea.get("catalyst_status", "") or "").lower()
    hard_date = (idea.get("hard_date_status", "") or "").lower()

    # Parse number + unit from repricing window (e.g. "1-3 days", "2-4 weeks")
    speed = "medium"  # default

    # Try to extract numeric range + unit
    num_match = re.search(r'(\d+)\s*[-–to]+\s*(\d+)\s*(day|week|month|hour)', repricing)
    single_match = re.search(r'(\d+)\s*(day|week|month|hour)', repricing)

    if num_match:
        hi = int(num_match.group(2))
        unit = num_match.group(3)
        if unit.startswith("hour"):
            speed = "fast"  # any hours-scale = fast
        elif unit.startswith("day"):
            speed = "fast" if hi <= 5 else "medium"
        elif unit.startswith("week"):
            speed = "medium" if hi <= 3 else "slow"
        elif unit.startswith("month"):
            speed = "slow"
    elif single_match:
        val = int(single_match.group(1))
        unit = single_match.group(2)
        if unit.startswith("hour"):
            speed = "fast"  # any hours-scale = fast
        elif unit.startswith("day"):
            speed = "fast" if val <= 5 else "medium"
        elif unit.startswith("week"):
            speed = "medium" if val <= 3 else "slow"
        elif unit.startswith("month"):
            speed = "slow"
    else:
        # Keyword fallback for non-numeric descriptions
        fast_keywords = ["immediate", "intraday", "within days", "this week",
                         "breaking", "hours"]
        slow_keywords = ["month", "quarter", "long", "gradual", "regulatory timeline"]
        for kw in fast_keywords:
            if kw in repricing:
                speed = "fast"
                break
        if speed == "medium":
            for kw in slow_keywords:
                if kw in repricing:
                    speed = "slow"
                    break

    # Secondary signals (only if still medium / unresolved)
    if speed == "medium":
        if any(s in status for s in ["triggered", "breaking", "acute"]):
            speed = "fast"
        elif "hard date" in hard_date and any(kw in repricing for kw in ["3", "4", "5", "6"]):
            speed = "slow"

    return {"speed": speed, "lookback": LOOKBACK_PROFILES[speed]}


# ---------------------------------------------------------------------------
# HTTP Layer
# ---------------------------------------------------------------------------
def _rate_limit():
    """Enforce rate limit: ~2 req/sec."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _get(endpoint: str, params: dict = None, timeout: int = 30,
         max_retries: int = 2) -> Optional[dict]:
    """Make authenticated GET request to UW API with retry on transient errors.

    Retries on 429 (rate limit) and 5xx (server errors) with capped backoff.
    Non-retryable errors (4xx except 429) return None immediately.
    """
    api_key = os.environ.get("UW_API_KEY", UW_API_KEY)
    if not api_key:
        return None

    url = f"{BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    last_status = None
    for attempt in range(max_retries + 1):
        _rate_limit()
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            last_status = resp.status_code

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code == 429:
                wait = 10 + attempt * 5  # 10s, 15s, 20s
                print(f"  ⚠ UW rate limit hit on {endpoint} — "
                      f"waiting {wait}s (attempt {attempt + 1}/{max_retries + 1})...")
                time.sleep(wait)
                continue

            elif resp.status_code == 404:
                return None  # not found — don't retry

            elif 500 <= resp.status_code < 600:
                wait = 2 + attempt * 3  # 2s, 5s, 8s
                print(f"  ⚠ UW API {resp.status_code} on {endpoint} — "
                      f"retrying in {wait}s (attempt {attempt + 1}/{max_retries + 1})...")
                time.sleep(wait)
                continue

            else:
                # Other 4xx — non-retryable
                print(f"  ⚠ UW API {resp.status_code} on {endpoint}: "
                      f"{resp.text[:200]}")
                return None

        except requests.exceptions.Timeout:
            wait = 3 + attempt * 3  # 3s, 6s, 9s
            print(f"  ⚠ UW API timeout on {endpoint} — "
                  f"retrying in {wait}s (attempt {attempt + 1}/{max_retries + 1})...")
            time.sleep(wait)
            continue

        except Exception as e:
            print(f"  ⚠ UW API error on {endpoint}: {e}")
            return None

    # All retries exhausted
    print(f"  ✗ UW API failed on {endpoint} after {max_retries + 1} attempts "
          f"(last status: {last_status})")
    return None


def _cached_get(endpoint: str, params: dict = None) -> Optional[dict]:
    """GET with file-based cache to avoid redundant calls within a run."""
    CACHE_DIR.mkdir(exist_ok=True)
    cache_key = endpoint.replace("/", "_") + (
        "_" + "_".join(f"{k}{v}" for k, v in sorted((params or {}).items()))
        if params else ""
    )
    cache_file = CACHE_DIR / f"{cache_key}.json"

    # Check cache freshness
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_TTL:
            try:
                return json.loads(cache_file.read_text())
            except Exception:
                pass

    data = _get(endpoint, params)
    if data is not None:
        try:
            cache_file.write_text(json.dumps(data, default=str))
        except Exception:
            pass
    return data


def _extract_data(data, as_list=True):
    """Extract data payload from UW API response (handles 'data' key)."""
    if not data:
        return [] if as_list else {}
    if isinstance(data, dict):
        inner = data.get("data", data)
    else:
        inner = data
    if as_list:
        return inner if isinstance(inner, list) else []
    return inner if isinstance(inner, dict) else {}


# ---------------------------------------------------------------------------
# TIER 3 (SHALLOW): Point-in-Time Snapshot Endpoints
# ---------------------------------------------------------------------------

def fetch_oi_change(ticker: str) -> List[dict]:
    """Open interest changes for a ticker (yesterday → today)."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/oi_change"))


def fetch_flow_per_expiry(ticker: str) -> List[dict]:
    """Flow breakdown by expiry date — where positioning is concentrated."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/flow_per_expiry"))


def fetch_flow_per_strike(ticker: str) -> List[dict]:
    """Flow breakdown by strike — directional concentration."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/flow_per_strike"))


def fetch_greek_exposure(ticker: str) -> dict:
    """Net greek exposure (delta, gamma, vanna, charm)."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/greek_exposure"), as_list=False)


def fetch_iv_rank(ticker: str) -> dict:
    """IV rank and percentile for the ticker."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/iv_rank"), as_list=False)


def fetch_volatility_stats(ticker: str) -> dict:
    """Volatility statistics (IV, RV, ranks, highs/lows)."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/volatility_stats"), as_list=False)


# ---------------------------------------------------------------------------
# TIER 2 (MEDIUM): Multi-Day Historical Endpoints
# ---------------------------------------------------------------------------

def fetch_flow_alerts_historical(ticker: str, days: int = 10,
                                  limit: int = 200) -> List[dict]:
    """
    Fetch unusual flow alerts for a ticker over the last N trading sessions.
    Uses the market-wide endpoint with ticker_symbol filter + newer_than.
    Adapter: tries documented route, falls back to alternate.

    Max lookback: 14 days (API limit). Capped in LOOKBACK_PROFILES.
    """
    cutoff = _trading_sessions_ago(min(days, 14))
    params = {
        "ticker_symbol": ticker,
        "newer_than": cutoff,
        "limit": str(limit),
    }

    # Try primary documented route
    data = _cached_get("/api/option-trades/flow-alerts", params=params)
    if data is None:
        # Fallback to alternate route format
        data = _cached_get("/api/option-trade/flow_alerts", params=params)

    return _extract_data(data)


def fetch_options_volume_history(ticker: str, days: int = 10) -> List[dict]:
    """
    Fetch daily options volume history for a ticker.
    Single call with limit=N returns N dated rows.
    Fields include: date, call_volume, put_volume, call_premium, put_premium,
    ask_volume, bid_volume, net_premium, open_interest.
    """
    data = _cached_get(f"/api/stock/{ticker}/options-volume", params={
        "limit": str(days),
    })
    return _extract_data(data)


def fetch_darkpool_historical(ticker: str, days: int = 5,
                               limit_per_day: int = 50) -> List[dict]:
    """
    Fetch dark pool prints for a ticker over the last N trading sessions.
    API uses 'date' param per day (newer_than not supported).
    Calls once per trading day and merges results.
    """
    all_prints = []
    for dt in _trading_dates_list(days):
        data = _cached_get(f"/api/darkpool/{ticker}", params={
            "date": dt, "limit": str(limit_per_day),
        })
        all_prints.extend(_extract_data(data))
    return all_prints


def fetch_lit_flow_historical(ticker: str, days: int = 5,
                               limit_per_day: int = 50) -> List[dict]:
    """
    Fetch lit flow for a ticker over the last N trading sessions.
    API uses 'date' param per day (newer_than not supported).
    Uses /api/lit-flow/{ticker} endpoint.
    """
    all_entries = []
    for dt in _trading_dates_list(days):
        data = _cached_get(f"/api/lit-flow/{ticker}", params={
            "date": dt, "limit": str(limit_per_day),
        })
        all_entries.extend(_extract_data(data))
    return all_entries


# Keep old fetch for backward compatibility (used by format_market_flow_summary)
def fetch_flow_alerts(ticker: str, limit: int = 50) -> List[dict]:
    """Legacy: fetch flow alerts for a ticker (no date filtering)."""
    data = _cached_get(f"/api/stock/{ticker}/flow_alerts")
    alerts = _extract_data(data)
    return alerts[:limit]


def fetch_darkpool(ticker: str, limit: int = 30) -> List[dict]:
    """Legacy: fetch recent dark pool prints (no date filtering)."""
    data = _cached_get(f"/api/darkpool/{ticker}")
    prints = _extract_data(data)
    return prints[:limit]


# ---------------------------------------------------------------------------
# TIER 1 (DEEP): Historical Contract Data
# ---------------------------------------------------------------------------

def _parse_option_symbol(symbol: str) -> dict:
    """
    Parse OCC option symbol like SPY260313C00670000 into components.
    Format: TICKER + YYMMDD + C/P + 8-digit strike (price * 1000).
    """
    m = re.match(r'^([A-Z]+)(\d{6})([CP])(\d{8})$', symbol)
    if not m:
        return {}
    ticker = m.group(1)
    date_str = m.group(2)  # YYMMDD
    cp = "CALL" if m.group(3) == "C" else "PUT"
    strike = int(m.group(4)) / 1000.0

    try:
        yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
        expiry = f"20{yy:02d}-{mm:02d}-{dd:02d}"
    except (ValueError, IndexError):
        expiry = date_str

    return {"ticker": ticker, "expiry": expiry, "call_put": cp, "strike": strike}


def fetch_option_contracts(ticker: str) -> List[dict]:
    """Fetch all option contracts for a ticker."""
    return _extract_data(_cached_get(f"/api/stock/{ticker}/option-contracts"))


def fetch_contract_history(contract_id: str, limit: int = 20) -> List[dict]:
    """
    Fetch historical data for a specific option contract.
    Returns daily snapshots: date, volume, open_interest, avg_price,
    implied_volatility, bid_volume, ask_volume, sweep_volume, etc.
    """
    data = _cached_get(
        f"/api/option-contract/{contract_id}/historic",
        params={"limit": str(limit)},
    )
    if not data:
        return []
    if isinstance(data, dict):
        history = data.get("chains", data.get("data", data))
    else:
        history = data
    return history if isinstance(history, list) else []


def fetch_contract_flow(contract_id: str, limit: int = 100) -> List[dict]:
    """
    Fetch trade-level flow for a specific option contract.
    Returns individual fills showing execution intent:
      - side: ask/bid/mid/multi-leg
      - trade_type: sweep, block, split, etc.
      - premium, volume, price
      - tied_to_stock, multi_leg flags
      - nbbo_bid, nbbo_ask (for fill aggressiveness)

    This is the execution-intent layer that complements contract history
    (OI/vol/IV). History tells you WHAT happened to the position;
    flow tells you HOW it was executed (aggressive sweeps = conviction).
    """
    data = _cached_get(
        f"/api/option-contract/{contract_id}/flow",
        params={"limit": str(limit)},
    )
    return _extract_data(data)


def _aggregate_contract_flow(flow_rows: List[dict]) -> dict:
    """
    Pre-aggregate contract-level flow into execution-intent summary.
    Returns dict with ask/bid/mid split, sweep/block counts,
    multi-leg flags, and fill aggressiveness.
    """
    if not flow_rows:
        return {}

    total_ask = 0
    total_bid = 0
    total_mid = 0
    total_multi = 0
    total_premium = 0
    sweep_count = 0
    block_count = 0
    multi_leg_count = 0
    tied_to_stock_count = 0
    fills = len(flow_rows)
    fill_vs_mid_values = []

    for row in flow_rows:
        # Side volumes — field names vary by API response format
        ask_v = _safe_float(row.get("ask_vol", row.get("ask_volume", 0)))
        bid_v = _safe_float(row.get("bid_vol", row.get("bid_volume", 0)))
        mid_v = _safe_float(row.get("mid_vol", row.get("mid_volume", 0)))
        multi_v = _safe_float(row.get("multi_vol", row.get("multi_volume", 0)))
        vol = _safe_float(row.get("volume", row.get("size", 0)))

        # If no side breakdown, infer from trade side field
        if ask_v == 0 and bid_v == 0 and mid_v == 0 and vol > 0:
            side = str(row.get("side", row.get("trade_side", ""))).lower()
            if side in ("ask", "above_ask", "a"):
                ask_v = vol
            elif side in ("bid", "below_bid", "b"):
                bid_v = vol
            elif side in ("mid", "m"):
                mid_v = vol

        total_ask += ask_v
        total_bid += bid_v
        total_mid += mid_v
        total_multi += multi_v

        prem = _safe_float(row.get("premium", row.get("total_premium", 0)))
        total_premium += prem

        # Trade type detection
        trade_type = str(row.get("trade_type", row.get("type",
                         row.get("urgency", "")))).lower()
        if "sweep" in trade_type:
            sweep_count += 1
        if "block" in trade_type:
            block_count += 1
        if row.get("has_sweep"):
            sweep_count += 1

        # Structure flags
        if row.get("multi_leg") or row.get("is_multi_leg"):
            multi_leg_count += 1
        if row.get("tied_to_stock"):
            tied_to_stock_count += 1

        # Fill aggressiveness: how close to ask vs bid
        price = _safe_float(row.get("price", row.get("last_price", 0)))
        nbbo_bid = _safe_float(row.get("nbbo_bid", row.get("bid", 0)))
        nbbo_ask = _safe_float(row.get("nbbo_ask", row.get("ask", 0)))
        if price > 0 and nbbo_ask > nbbo_bid > 0:
            width = max(nbbo_ask - nbbo_bid, 0.0001)
            mid_price = (nbbo_bid + nbbo_ask) / 2
            fill_vs_mid = (price - mid_price) / width
            fill_vs_mid_values.append(fill_vs_mid)

    total_sided = total_ask + total_bid + total_mid + total_multi
    result = {
        "fills": fills,
        "total_premium": total_premium,
        "sweep_count": sweep_count,
        "block_count": block_count,
        "multi_leg_count": multi_leg_count,
        "tied_to_stock_count": tied_to_stock_count,
    }

    # Side percentages
    if total_sided > 0:
        result["ask_pct"] = round(total_ask / total_sided * 100, 1)
        result["bid_pct"] = round(total_bid / total_sided * 100, 1)
        result["mid_pct"] = round(total_mid / total_sided * 100, 1)
        result["multi_pct"] = round(total_multi / total_sided * 100, 1)
    else:
        result["ask_pct"] = result["bid_pct"] = 0
        result["mid_pct"] = result["multi_pct"] = 0

    # Fill aggressiveness
    if fill_vs_mid_values:
        avg_fill = sum(fill_vs_mid_values) / len(fill_vs_mid_values)
        result["avg_fill_vs_mid"] = round(avg_fill, 3)
        if avg_fill >= 0.15:
            result["fill_signal"] = "AGGRESSIVE_BUY"
        elif avg_fill <= -0.15:
            result["fill_signal"] = "AGGRESSIVE_SELL"
        else:
            result["fill_signal"] = "NEUTRAL"
    else:
        result["avg_fill_vs_mid"] = None
        result["fill_signal"] = "UNKNOWN"

    # Execution intent label
    if result["ask_pct"] >= 55 and sweep_count >= 2:
        result["execution_intent"] = "ACCUMULATION_SWEEP"
    elif result["ask_pct"] >= 55:
        result["execution_intent"] = "ACCUMULATION"
    elif result["bid_pct"] >= 55:
        result["execution_intent"] = "DISTRIBUTION"
    elif multi_leg_count >= fills * 0.3:
        result["execution_intent"] = "STRUCTURED"
    elif tied_to_stock_count >= fills * 0.3:
        result["execution_intent"] = "HEDGED"
    else:
        result["execution_intent"] = "MIXED"

    return result


def build_historical_flow_context(ticker: str, direction: str = "",
                                   max_contracts: int = 6,
                                   history_days: int = 15,
                                   max_dte: int = 45) -> dict:
    """
    Fetch historical contract data with smart filtering.

    Filters before selecting top contracts:
    1. Expiry within max_dte (speed-adaptive: fast=30, medium=45, slow=75)
    2. Near-the-money: strike within ±15% of current price (if available)
    3. Call/put aligned with direction (prioritized, not excluded)
    Then rank by activity score and take top N.

    Args:
        ticker: Stock ticker
        direction: "Bullish" or "Bearish" — prioritizes calls/puts accordingly
        max_contracts: Max contracts to pull history for (API budget)
        history_days: Number of daily snapshots per contract
        max_dte: Maximum days to expiry for contract filtering
    """
    print(f"  📈 Fetching historical contract data for {ticker}...")

    contracts = fetch_option_contracts(ticker)
    if not contracts:
        print(f"    ⚠ No option contracts found for {ticker}")
        return {"ticker": ticker, "contracts_found": 0,
                "top_contracts": [], "has_history": False}

    print(f"    ✓ {len(contracts)} contracts found")

    today = date_type.today()

    # --- Smart filtering ---
    def _activity_score(c):
        vol = 0
        oi = 0
        try:
            vol = float(c.get("volume", 0) or 0)
        except (ValueError, TypeError):
            pass
        try:
            oi = float(c.get("open_interest", 0) or 0)
        except (ValueError, TypeError):
            pass
        return vol * 2 + oi

    filtered = []
    for c in contracts:
        if _activity_score(c) <= 0:
            continue

        # Parse contract details
        symbol = (c.get("option_symbol") or c.get("option_id")
                  or c.get("id") or c.get("symbol") or "")
        parsed = _parse_option_symbol(symbol)

        # Filter by DTE
        expiry_str = c.get("expiry", c.get("expires_at", parsed.get("expiry", "")))
        if expiry_str:
            try:
                exp_date = date_type.fromisoformat(expiry_str[:10])
                dte = (exp_date - today).days
                if dte < 0 or dte > max_dte:
                    continue
            except (ValueError, TypeError):
                pass

        # Direction priority boost (don't exclude, just boost score)
        cp = c.get("call_put", c.get("option_type", parsed.get("call_put", "")))
        direction_boost = 1.0
        if direction:
            dir_lower = direction.lower()
            if dir_lower == "bullish" and cp and cp.upper() == "CALL":
                direction_boost = 1.5
            elif dir_lower == "bearish" and cp and cp.upper() == "PUT":
                direction_boost = 1.5

        c["_score"] = _activity_score(c) * direction_boost
        c["_symbol"] = symbol
        c["_parsed"] = parsed
        filtered.append(c)

    # Sort by boosted score, take top N
    filtered.sort(key=lambda c: c["_score"], reverse=True)
    top = filtered[:max_contracts]

    if not top:
        # Fallback: use unfiltered top if filtering removed everything
        active = [c for c in contracts if _activity_score(c) > 0]
        active.sort(key=_activity_score, reverse=True)
        top = active[:max_contracts]
        for c in top:
            c["_symbol"] = (c.get("option_symbol") or c.get("option_id")
                            or c.get("id") or c.get("symbol") or "")
            c["_parsed"] = _parse_option_symbol(c["_symbol"])

    print(f"    ✓ {len(top)} contracts selected (max_dte={max_dte}, "
          f"history={history_days}d)")

    results = []
    for c in top:
        symbol = c.get("_symbol", "")
        parsed = c.get("_parsed", {})
        if not symbol:
            continue

        history = fetch_contract_history(symbol, limit=history_days)
        # Execution-intent layer: how was this contract traded?
        flow_rows = fetch_contract_flow(symbol, limit=100)
        flow_agg = _aggregate_contract_flow(flow_rows)

        strike = c.get("strike", parsed.get("strike", "?"))
        expiry = c.get("expiry", c.get("expires_at", parsed.get("expiry", "?")))
        call_put = c.get("call_put", c.get("option_type", parsed.get("call_put", "?")))
        oi = c.get("open_interest", 0)
        vol = c.get("volume", 0)
        prev_oi = c.get("prev_oi", None)

        results.append({
            "option_symbol": symbol,
            "strike": strike,
            "expiry": expiry,
            "call_put": call_put,
            "current_oi": oi,
            "prev_oi": prev_oi,
            "current_volume": vol,
            "history": history,
            "history_days": len(history),
            "contract_flow": flow_agg,
        })

        flow_label = ""
        if flow_agg:
            intent = flow_agg.get("execution_intent", "?")
            fills = flow_agg.get("fills", 0)
            flow_label = f" | flow: {fills} fills, {intent}"

        if history:
            print(f"    ✓ {symbol}: {len(history)}-day history "
                  f"({call_put} ${strike} exp {expiry}){flow_label}")

    has_history = any(r["history"] for r in results)
    print(f"    📊 {len(results)} contracts with history: {has_history}")

    return {
        "ticker": ticker,
        "contracts_found": len(contracts),
        "top_contracts": results,
        "has_history": has_history,
    }


# ---------------------------------------------------------------------------
# Market-Wide Endpoints (Stage 1 — unchanged)
# ---------------------------------------------------------------------------

def fetch_market_flow_alerts(limit: int = 100) -> List[dict]:
    """Market-wide unusual flow alerts for the R1 catalyst hunter."""
    data = _cached_get("/api/option-trade/flow_alerts")
    alerts = _extract_data(data)
    return alerts[:limit]


# ---------------------------------------------------------------------------
# Composite Data Builder — 3-tier speed-adaptive
# ---------------------------------------------------------------------------

def build_ticker_flow_context(ticker: str, include_history: bool = True,
                               lookback: dict = None, direction: str = "") -> dict:
    """
    Fetch ALL flow data for a single ticker using 3-tier memory model.

    Args:
        ticker: Stock symbol
        include_history: Whether to fetch deep contract history
        lookback: Speed-adaptive lookback config from classify_catalyst_speed()
        direction: "Bullish" or "Bearish" — for smart contract filtering

    Returns dict with all flow data across three tiers.
    """
    if lookback is None:
        lookback = LOOKBACK_PROFILES["medium"]

    print(f"  📡 Fetching UW flow data for {ticker} "
          f"(flow:{lookback['flow_alerts']}d history:{lookback.get('contract_history',15)}d "
          f"DP:{lookback['darkpool']}d)...")

    # TIER 2 (MEDIUM): Multi-day historical
    flow_alerts = fetch_flow_alerts_historical(
        ticker, days=lookback["flow_alerts"])
    options_volume = fetch_options_volume_history(
        ticker, days=lookback["options_volume"])
    darkpool = fetch_darkpool_historical(
        ticker, days=lookback["darkpool"])
    lit_flow = fetch_lit_flow_historical(
        ticker, days=lookback["lit_flow"])

    # TIER 3 (SHALLOW): Point-in-time snapshots
    oi_change = fetch_oi_change(ticker)
    flow_expiry = fetch_flow_per_expiry(ticker)
    flow_strike = fetch_flow_per_strike(ticker)
    greeks = fetch_greek_exposure(ticker)
    iv = fetch_iv_rank(ticker)
    vol_stats = fetch_volatility_stats(ticker)

    # TIER 1 (DEEP): Contract-level history
    historical = {}
    if include_history:
        try:
            historical = build_historical_flow_context(
                ticker, direction=direction,
                max_contracts=6,
                history_days=lookback.get("contract_history", 15),
                max_dte=lookback.get("contract_max_dte", 45),
            )
        except Exception as e:
            print(f"    ⚠ Historical contract fetch failed: {e}")
            historical = {"ticker": ticker, "contracts_found": 0,
                          "top_contracts": [], "has_history": False}

    has_data = bool(flow_alerts or oi_change or darkpool or greeks
                    or options_volume or lit_flow
                    or historical.get("has_history"))

    counts = []
    if flow_alerts:
        counts.append(f"{len(flow_alerts)} flow alerts")
    if options_volume:
        counts.append(f"{len(options_volume)}d vol history")
    if oi_change:
        counts.append(f"{len(oi_change)} OI changes")
    if darkpool:
        counts.append(f"{len(darkpool)} DP prints")
    if lit_flow:
        counts.append(f"{len(lit_flow)} lit flow")
    if greeks:
        counts.append("greeks")
    if iv:
        counts.append("IV rank")
    if vol_stats:
        counts.append("vol stats")
    if historical.get("has_history"):
        n_hist = len([c for c in historical.get("top_contracts", [])
                      if c.get("history")])
        counts.append(f"{n_hist} contract histories")

    if counts:
        print(f"    ✓ {', '.join(counts)}")
    else:
        print(f"    ⚠ No UW data available for {ticker}")

    return {
        "ticker": ticker,
        "flow_alerts": flow_alerts,
        "options_volume": options_volume,
        "oi_change": oi_change,
        "flow_per_expiry": flow_expiry,
        "flow_per_strike": flow_strike,
        "greek_exposure": greeks,
        "iv_rank": iv,
        "volatility_stats": vol_stats,
        "darkpool": darkpool,
        "lit_flow": lit_flow,
        "historical_contracts": historical,
        "fetch_time": datetime.now().isoformat(),
        "data_available": has_data,
    }


# ---------------------------------------------------------------------------
# Pre-Aggregation Helpers — summarize before prompting
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0):
    """Safely convert to float."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _aggregate_flow_alerts(alerts: List[dict]) -> str:
    """Pre-aggregate multi-day flow alerts into a compact summary."""
    if not alerts:
        return ""

    # Group by date
    by_date = defaultdict(list)
    total_premium = 0
    call_count = 0
    put_count = 0
    sweep_count = 0
    block_count = 0
    ask_side_prem = 0
    bid_side_prem = 0
    largest_alerts = []

    for a in alerts:
        # UW flow alerts use created_at for timestamp
        ts = a.get("created_at", a.get("timestamp", a.get("date", "")))
        day = ts[:10] if ts else "unknown"
        by_date[day].append(a)

        prem = _safe_float(a.get("total_premium", a.get("premium", 0)))
        total_premium += prem

        # Determine call/put from option_chain (e.g. AAPL260313C00255000)
        chain = str(a.get("option_chain", ""))
        cp = ""
        if chain:
            for i, ch in enumerate(chain):
                if ch in ("C", "P") and i > 5:
                    cp = "CALL" if ch == "C" else "PUT"
                    break
        if not cp:
            cp = str(a.get("call_put", "")).upper()
        if cp == "CALL":
            call_count += 1
        elif cp == "PUT":
            put_count += 1

        # Sweep from boolean flag, block from alert_rule
        if a.get("has_sweep"):
            sweep_count += 1
        rule = str(a.get("alert_rule", "")).lower()
        if "block" in rule:
            block_count += 1

        # Ask/bid side premium
        ask_side_prem += _safe_float(a.get("total_ask_side_prem", 0))
        bid_side_prem += _safe_float(a.get("total_bid_side_prem", 0))

        largest_alerts.append((prem, a))

    total_alerts = len(alerts)
    n_days = len(by_date)

    lines = [f"--- MULTI-DAY FLOW ALERTS ({n_days} sessions, {total_alerts} alerts) ---"]

    # Daily count trend
    sorted_dates = sorted(d for d in by_date.keys() if d != "unknown")
    daily_counts = [f"{d[-5:]}={len(by_date[d])}" for d in sorted_dates[-7:]]
    if len(sorted_dates) >= 3:
        first_half = sum(len(by_date[d]) for d in sorted_dates[:len(sorted_dates)//2])
        second_half = sum(len(by_date[d]) for d in sorted_dates[len(sorted_dates)//2:])
        trend = "building" if second_half > first_half * 1.2 else (
            "fading" if first_half > second_half * 1.2 else "steady")
    else:
        trend = "limited data"
    lines.append(f"  Daily alert count: {', '.join(daily_counts)} ({trend})")

    # Largest premium alerts
    largest_alerts.sort(key=lambda x: x[0], reverse=True)
    top3 = []
    for prem, a in largest_alerts[:3]:
        chain = str(a.get("option_chain", ""))
        cp_label = "?"
        for i, ch in enumerate(chain):
            if ch in ("C", "P") and i > 5:
                cp_label = "CALL" if ch == "C" else "PUT"
                break
        strike = a.get("strike", "?")
        expiry = a.get("expiry", "?")
        rule = a.get("alert_rule", "?")
        top3.append(f"${prem/1e6:.1f}M {rule} {cp_label} ${strike} exp {expiry}")
    if top3:
        lines.append(f"  Largest: {' | '.join(top3)}")

    # Composition
    if total_alerts > 0:
        call_pct = call_count / total_alerts * 100
        put_pct = put_count / total_alerts * 100
        lines.append(f"  Composition: {call_pct:.0f}% calls, {put_pct:.0f}% puts"
                      f" | {sweep_count} sweeps, {block_count} blocks")

    # Ask/bid side premium concentration
    total_sided = ask_side_prem + bid_side_prem
    if total_sided > 0:
        ask_pct = ask_side_prem / total_sided * 100
        if ask_pct >= 55:
            side = f"{ask_pct:.0f}% ask-side (accumulation bias)"
        elif ask_pct <= 45:
            side = f"{100-ask_pct:.0f}% bid-side (distribution bias)"
        else:
            side = "balanced"
        lines.append(f"  Side concentration: {side}")

    lines.append(f"  Total premium: ${total_premium/1e6:.1f}M")

    return "\n".join(lines)


def _aggregate_options_volume(volume_rows: List[dict]) -> str:
    """Pre-aggregate daily options volume into a compact trend summary."""
    if not volume_rows:
        return ""

    # Sort by date (oldest first for trajectory)
    sorted_rows = sorted(volume_rows,
                         key=lambda r: r.get("date", ""), reverse=False)

    n_days = len(sorted_rows)
    lines = [f"--- DAILY OPTIONS VOLUME TREND ({n_days} sessions) ---"]

    # Extract trajectories
    call_vols = []
    put_vols = []
    ask_vols = []
    bid_vols = []
    net_prems = []

    for r in sorted_rows:
        call_vols.append(_safe_float(r.get("call_volume", 0)))
        put_vols.append(_safe_float(r.get("put_volume", 0)))
        # UW returns call_volume_ask_side + put_volume_ask_side (not top-level ask_volume)
        call_ask = _safe_float(r.get("call_volume_ask_side", 0))
        put_ask = _safe_float(r.get("put_volume_ask_side", 0))
        call_bid = _safe_float(r.get("call_volume_bid_side", 0))
        put_bid = _safe_float(r.get("put_volume_bid_side", 0))
        ask_vols.append(call_ask + put_ask)
        bid_vols.append(call_bid + put_bid)
        # Net premium = net_call_premium + net_put_premium
        net_call = _safe_float(r.get("net_call_premium", 0))
        net_put = _safe_float(r.get("net_put_premium", 0))
        net_prems.append(net_call + net_put)

    # Call volume trajectory (show first, mid, last)
    if call_vols and any(v > 0 for v in call_vols):
        cv_start = call_vols[0]
        cv_end = call_vols[-1]
        cv_ratio = cv_end / cv_start if cv_start > 0 else 0
        trajectory = " → ".join(f"{v:,.0f}" for v in call_vols[-5:])
        if cv_ratio > 1.5:
            label = f"({cv_ratio:.1f}x increase)"
        elif cv_ratio < 0.67:
            label = f"({cv_ratio:.1f}x, declining)"
        else:
            label = "(stable)"
        lines.append(f"  Call volume: {trajectory} {label}")

    if put_vols and any(v > 0 for v in put_vols):
        trajectory = " → ".join(f"{v:,.0f}" for v in put_vols[-5:])
        lines.append(f"  Put volume: {trajectory}")

    # Call/put ratio trajectory
    ratios = []
    for cv, pv in zip(call_vols, put_vols):
        if pv > 0:
            ratios.append(cv / pv)
    if len(ratios) >= 2:
        ratio_traj = " → ".join(f"{r:.1f}" for r in ratios[-5:])
        if ratios[-1] > ratios[0] * 1.3:
            label = "(bullish shift)"
        elif ratios[-1] < ratios[0] * 0.7:
            label = "(bearish shift)"
        else:
            label = ""
        lines.append(f"  Call/put ratio: {ratio_traj} {label}")

    # Ask-side share trajectory
    ask_shares = []
    for av, bv in zip(ask_vols, bid_vols):
        total = av + bv
        if total > 0:
            ask_shares.append(av / total * 100)
    if len(ask_shares) >= 2:
        share_traj = " → ".join(f"{s:.0f}%" for s in ask_shares[-5:])
        if ask_shares[-1] > 55:
            label = "(accumulation accelerating)"
        elif ask_shares[-1] < 45:
            label = "(distribution)"
        else:
            label = ""
        lines.append(f"  Ask-side share: {share_traj} {label}")

    # Net premium direction
    if net_prems:
        pos_days = sum(1 for p in net_prems if p > 0)
        lines.append(f"  Net premium: positive {pos_days} of {n_days} sessions")

    return "\n".join(lines)


def _aggregate_darkpool(prints: List[dict]) -> str:
    """Pre-aggregate dark pool prints into structural summary."""
    if not prints:
        return ""

    # Group by date
    by_date = defaultdict(lambda: {"notional": 0, "count": 0, "prices": []})
    total_notional = 0
    all_prices = []

    for p in prints:
        ts = p.get("timestamp", p.get("executed_at", p.get("date", "")))
        day = ts[:10] if ts else "unknown"
        notional = _safe_float(p.get("notional", 0))
        price = _safe_float(p.get("price", 0))
        size = _safe_float(p.get("size", p.get("volume", 0)))

        if notional == 0 and price > 0 and size > 0:
            notional = price * size

        by_date[day]["notional"] += notional
        by_date[day]["count"] += 1
        if price > 0:
            by_date[day]["prices"].append(price)
            all_prices.append(price)
        total_notional += notional

    n_days = len(by_date)
    n_prints = len(prints)

    lines = [f"--- DARK POOL STRUCTURE ({n_days} sessions, {n_prints} prints) ---"]
    lines.append(f"  Total notional: ${total_notional/1e6:.1f}M")

    # Daily trend
    sorted_dates = sorted(by_date.keys())
    daily_parts = []
    for d in sorted_dates[-7:]:
        daily_parts.append(f"{d[-5:]} ${by_date[d]['notional']/1e6:.1f}M")
    if daily_parts:
        lines.append(f"  Daily: {', '.join(daily_parts)}")

    # Price clustering
    if all_prices:
        from statistics import mean, stdev
        avg_price = mean(all_prices)
        if len(all_prices) >= 3:
            sd = stdev(all_prices)
            cluster_low = avg_price - sd
            cluster_high = avg_price + sd
            in_cluster = sum(1 for p in all_prices if cluster_low <= p <= cluster_high)
            cluster_pct = in_cluster / len(all_prices) * 100
            lines.append(f"  Price clustering: {cluster_pct:.0f}% of prints in "
                          f"${cluster_low:.2f}-${cluster_high:.2f} range")

        # Most repeated price levels
        from collections import Counter
        price_rounded = [round(p, 2) for p in all_prices]
        top_levels = Counter(price_rounded).most_common(3)
        repeated = [f"${p} ({c}x)" for p, c in top_levels if c >= 2]
        if repeated:
            lines.append(f"  Repeated levels: {', '.join(repeated)}")

    return "\n".join(lines)


def _aggregate_lit_flow(lit_data: List[dict]) -> str:
    """Pre-aggregate lit flow (equity trades) into compact summary."""
    if not lit_data:
        return ""

    # Group by date — each entry is a raw trade with price, size, premium, executed_at
    by_date = defaultdict(lambda: {
        "notional": 0, "count": 0, "buy_notional": 0, "sell_notional": 0
    })

    for entry in lit_data:
        ts = entry.get("executed_at", "")
        day = ts[:10] if ts else "unknown"
        price = _safe_float(entry.get("price", 0))
        size = _safe_float(entry.get("size", 0))
        notional = _safe_float(entry.get("premium", 0))
        if notional == 0 and price > 0 and size > 0:
            notional = price * size

        by_date[day]["notional"] += notional
        by_date[day]["count"] += 1

        # Estimate buy/sell side: trade at/above ask = buy, at/below bid = sell
        ask = _safe_float(entry.get("nbbo_ask", 0))
        bid = _safe_float(entry.get("nbbo_bid", 0))
        if ask > 0 and bid > 0 and price > 0:
            mid = (ask + bid) / 2
            if price >= mid:
                by_date[day]["buy_notional"] += notional
            else:
                by_date[day]["sell_notional"] += notional

    sorted_dates = sorted(d for d in by_date.keys() if d != "unknown")
    n_days = len(sorted_dates)
    n_trades = len(lit_data)

    lines = [f"--- LIT FLOW SUMMARY ({n_days} sessions, {n_trades} trades) ---"]

    # Daily notional trend
    if sorted_dates:
        total_notional = sum(by_date[d]["notional"] for d in sorted_dates)
        lines.append(f"  Total notional: ${total_notional/1e6:.1f}M")
        daily_parts = []
        for d in sorted_dates[-7:]:
            daily_parts.append(f"{d[-5:]} ${by_date[d]['notional']/1e6:.1f}M")
        lines.append(f"  Daily: {', '.join(daily_parts)}")

        # Buy/sell balance
        total_buy = sum(by_date[d]["buy_notional"] for d in sorted_dates)
        total_sell = sum(by_date[d]["sell_notional"] for d in sorted_dates)
        total_sided = total_buy + total_sell
        if total_sided > 0:
            buy_pct = total_buy / total_sided * 100
            if buy_pct >= 55:
                lines.append(f"  Buy-side: {buy_pct:.0f}% (demand bias)")
            elif buy_pct <= 45:
                lines.append(f"  Sell-side: {100-buy_pct:.0f}% (supply bias)")
            else:
                lines.append(f"  Side: balanced ({buy_pct:.0f}% buy)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Contract Analytics — per-contract and cross-contract summaries
# ---------------------------------------------------------------------------

def _analyze_contract(contract: dict) -> dict:
    """
    Extract analytics from a single contract's history.
    Returns dict with OI trend, trade side, volume stats, relevance score.
    """
    history = contract.get("history", [])
    sym = contract.get("option_symbol", "?")
    cp = contract.get("call_put", "?")
    strike = contract.get("strike", "?")
    expiry = contract.get("expiry", "?")

    result = {
        "symbol": sym, "call_put": cp, "strike": strike, "expiry": expiry,
        "oi_trend": "UNKNOWN", "oi_change_pct": 0, "oi_first": 0, "oi_last": 0,
        "ask_pct": 50, "side_signal": "BALANCED",
        "avg_vol": 0, "max_vol": 0, "vol_days": 0,
        "sweep_vol": 0, "relevance_score": 0,
    }

    if len(history) < 2:
        return result

    # OI trend
    oi_values = []
    vol_values = []
    for h in history:
        oi_val = h.get("open_interest")
        if oi_val is not None:
            try:
                oi_values.append(float(oi_val))
            except (ValueError, TypeError):
                pass
        vol_val = h.get("volume")
        if vol_val is not None:
            try:
                vol_values.append(float(vol_val))
            except (ValueError, TypeError):
                pass

    if len(oi_values) >= 2:
        oi_first = oi_values[-1]  # oldest
        oi_last = oi_values[0]   # newest
        oi_change = oi_last - oi_first
        if oi_first > 0:
            oi_pct = (oi_change / oi_first) * 100
        elif oi_last > 0:
            oi_pct = 9999  # new position built from zero — flag as "NEW"
        else:
            oi_pct = 0

        result["oi_first"] = oi_first
        result["oi_last"] = oi_last
        result["oi_change_pct"] = oi_pct

        if len(oi_values) >= 3:
            rising = sum(1 for i in range(len(oi_values) - 1)
                         if oi_values[i] > oi_values[i + 1])
            if rising >= len(oi_values) * 0.7:
                result["oi_trend"] = "BUILDING"
            elif rising <= len(oi_values) * 0.3:
                result["oi_trend"] = "UNWINDING"
            else:
                result["oi_trend"] = "MIXED"
        else:
            result["oi_trend"] = "BUILDING" if oi_change > 0 else "UNWINDING"

    # Trade side
    total_bid = sum(_safe_float(h.get("bid_volume", 0)) for h in history)
    total_ask = sum(_safe_float(h.get("ask_volume", 0)) for h in history)
    total_sided = total_bid + total_ask
    if total_sided > 0:
        ask_pct = (total_ask / total_sided) * 100
        result["ask_pct"] = ask_pct
        if ask_pct >= 55:
            result["side_signal"] = "ASK-HEAVY"
        elif ask_pct <= 45:
            result["side_signal"] = "BID-HEAVY"

    # Volume stats
    if vol_values:
        result["avg_vol"] = sum(vol_values) / len(vol_values)
        result["max_vol"] = max(vol_values)
        result["vol_days"] = len(vol_values)

    # Sweep volume
    result["sweep_vol"] = sum(_safe_float(h.get("sweep_volume", 0)) for h in history)

    # Contract flow (execution intent) — from /api/option-contract/{id}/flow
    flow_agg = contract.get("contract_flow", {})
    result["execution_intent"] = flow_agg.get("execution_intent", "UNKNOWN")
    result["flow_fills"] = flow_agg.get("fills", 0)
    result["flow_sweep_count"] = flow_agg.get("sweep_count", 0)
    result["flow_ask_pct"] = flow_agg.get("ask_pct", 0)
    result["fill_signal"] = flow_agg.get("fill_signal", "UNKNOWN")

    # Relevance score (for ranking which contracts to show in detail)
    # Factors: OI build strength, ask-side dominance, volume, sweep urgency,
    #          execution intent from contract flow
    score = 0
    if result["oi_trend"] == "BUILDING":
        oi_pct_capped = min(abs(result["oi_change_pct"]), 500)
        if result["oi_change_pct"] >= 9999:
            score += 50  # new position from zero — strong signal
        else:
            score += 30 + oi_pct_capped / 10  # up to +80
    elif result["oi_trend"] == "UNWINDING":
        score -= 10
    if result["ask_pct"] >= 55:
        score += 15 + (result["ask_pct"] - 55)  # bonus for strong ask dominance
    if result["avg_vol"] > 0:
        score += min(result["avg_vol"] / 100, 20)  # volume contribution, capped
    if result["sweep_vol"] > 0:
        score += 10  # urgency signal

    # Execution intent bonus: sweeps + ask-heavy flow = high conviction
    if flow_agg:
        intent = flow_agg.get("execution_intent", "")
        if intent == "ACCUMULATION_SWEEP":
            score += 20  # strongest conviction signal
        elif intent == "ACCUMULATION":
            score += 12
        elif intent == "DISTRIBUTION":
            score -= 8
        elif intent in ("STRUCTURED", "HEDGED"):
            score -= 5  # likely not directional spec

        if flow_agg.get("fill_signal") == "AGGRESSIVE_BUY":
            score += 8
        elif flow_agg.get("fill_signal") == "AGGRESSIVE_SELL":
            score -= 5

    result["relevance_score"] = score

    return result


def _build_contract_summary(contracts_with_hist: list, n_days) -> tuple:
    """
    Build a compact cross-contract summary block.
    Gives the model the conclusion before the raw detail.
    Returns (summary_text, analyses_list).
    """
    if not contracts_with_hist:
        return "", []

    # Analyze all contracts
    analyses = []
    for c in contracts_with_hist:
        analyses.append(_analyze_contract(c))

    n = len(analyses)
    building = [a for a in analyses if a["oi_trend"] == "BUILDING"]
    unwinding = [a for a in analyses if a["oi_trend"] == "UNWINDING"]
    ask_heavy = [a for a in analyses if a["ask_pct"] >= 55]
    bid_heavy = [a for a in analyses if a["ask_pct"] <= 45]

    lines = [f"--- CONTRACT SUMMARY ({n} lines tracked, {n_days} sessions) ---"]

    # OI build status
    lines.append(f"  Rising OI: {len(building)}/{n} contracts")
    if unwinding:
        lines.append(f"  Weakening / unwind: {len(unwinding)}/{n} contracts")

    # Ask-heavy status (from history bid/ask volume)
    if ask_heavy:
        lines.append(f"  Ask-heavy (accumulation): {len(ask_heavy)}/{n} contracts")
    if bid_heavy:
        lines.append(f"  Bid-heavy (distribution): {len(bid_heavy)}/{n} contracts")

    # Execution intent from contract flow (trade tape)
    accum_sweep = [a for a in analyses
                   if a.get("execution_intent") == "ACCUMULATION_SWEEP"]
    accum = [a for a in analyses
             if a.get("execution_intent") == "ACCUMULATION"]
    distrib = [a for a in analyses
               if a.get("execution_intent") == "DISTRIBUTION"]
    structured = [a for a in analyses
                  if a.get("execution_intent") in ("STRUCTURED", "HEDGED")]

    flow_parts = []
    if accum_sweep:
        flow_parts.append(f"{len(accum_sweep)} sweep-accumulation")
    if accum:
        flow_parts.append(f"{len(accum)} accumulation")
    if distrib:
        flow_parts.append(f"{len(distrib)} distribution")
    if structured:
        flow_parts.append(f"{len(structured)} structured/hedged")
    if flow_parts:
        lines.append(f"  Execution intent: {', '.join(flow_parts)}")

    # Strongest build
    if building:
        best = max(building, key=lambda a: a["relevance_score"])
        sym_short = best["symbol"][-15:] if len(best["symbol"]) > 15 else best["symbol"]
        oi_pct = best["oi_change_pct"]
        side = f", {best['side_signal'].lower()}" if best["side_signal"] != "BALANCED" else ""
        intent = best.get("execution_intent", "")
        intent_tag = f", {intent.lower().replace('_', '-')}" if intent not in ("UNKNOWN", "MIXED", "") else ""
        oi_label = "NEW (from zero)" if oi_pct >= 9999 else f"{oi_pct:+.0f}%"
        lines.append(f"  Strongest build: {sym_short} — OI {oi_label}{side}{intent_tag}")

    # Detect likely roll / migration
    # If one contract is unwinding while another at a nearby strike/later expiry is building
    if building and unwinding:
        for u in unwinding:
            for b in building:
                u_strike = _safe_float(u.get("strike", 0))
                b_strike = _safe_float(b.get("strike", 0))
                if u_strike > 0 and b_strike > 0:
                    strike_diff = abs(b_strike - u_strike) / u_strike
                    if strike_diff < 0.15:  # within 15%
                        u_short = f"{u['call_put']} ${u['strike']} {u['expiry'][-5:]}"
                        b_short = f"{b['call_put']} ${b['strike']} {b['expiry'][-5:]}"
                        lines.append(f"  Likely roll: {u_short} → {b_short}")
                        break
            else:
                continue
            break

    # Overall assessment (OI build + execution intent combined)
    build_ratio = len(building) / n if n > 0 else 0
    ask_ratio = len(ask_heavy) / n if n > 0 else 0
    conviction_flow = len(accum_sweep) + len(accum)
    if build_ratio >= 0.6 and (ask_ratio >= 0.4 or conviction_flow >= 2):
        overall = "supportive build, not churn"
        if accum_sweep:
            overall += " (sweep-confirmed)"
    elif build_ratio >= 0.4:
        overall = "mixed build — some lines supportive, some fading"
    elif len(unwinding) > len(building):
        overall = "net unwind — positioning weakening"
    elif distrib and len(distrib) >= n * 0.4:
        overall = "distribution-dominant — likely selling"
    else:
        overall = "inconclusive — check raw detail"
    lines.append(f"  Overall contract picture: {overall}")

    return "\n".join(lines), analyses


# ---------------------------------------------------------------------------
# Prompt Formatter — pre-aggregated, compact, high-signal
# ---------------------------------------------------------------------------

def format_flow_for_prompt(flow_data: dict, max_chars: int = 15000) -> str:
    """
    Format ticker flow data into a text block for AI prompt injection.
    Uses pre-aggregated summaries for medium-memory tiers.
    Keeps raw day-by-day detail only for deep contract history.

    Target output: 10-13K chars.
    """
    ticker = flow_data.get("ticker", "?")
    if not flow_data.get("data_available"):
        return f"=== UW FLOW DATA: {ticker} ===\nData unavailable — verify in R2\n"

    sections = [f"=== UW FLOW DATA: {ticker} ==="]

    # --- TIER 2: Pre-aggregated medium-memory summaries ---

    # Multi-day flow alerts (pre-aggregated)
    flow_summary = _aggregate_flow_alerts(flow_data.get("flow_alerts", []))
    if flow_summary:
        sections.append(f"\n{flow_summary}")

    # Daily options volume trend (pre-aggregated)
    vol_summary = _aggregate_options_volume(flow_data.get("options_volume", []))
    if vol_summary:
        sections.append(f"\n{vol_summary}")

    # Dark pool structure (pre-aggregated)
    dp_summary = _aggregate_darkpool(flow_data.get("darkpool", []))
    if dp_summary:
        sections.append(f"\n{dp_summary}")

    # Lit flow summary (pre-aggregated)
    lit_summary = _aggregate_lit_flow(flow_data.get("lit_flow", []))
    if lit_summary:
        sections.append(f"\n{lit_summary}")

    # --- TIER 3: Point-in-time snapshots ---

    # OI Change
    oi = flow_data.get("oi_change", [])
    if oi:
        sections.append(f"\n--- OI CHANGE ({len(oi)} entries) ---")
        for o in oi[:20]:
            parts = []
            for key in ["call_put", "strike", "expiry", "oi_change",
                        "prev_oi", "current_oi", "volume"]:
                val = o.get(key)
                if val is not None and val != "":
                    parts.append(f"{key}={val}")
            if parts:
                sections.append("  " + " | ".join(parts))

    # Flow Per Expiry
    fpe = flow_data.get("flow_per_expiry", [])
    if fpe:
        sections.append(f"\n--- FLOW PER EXPIRY ({len(fpe)} expiries) ---")
        for f in fpe[:10]:
            parts = []
            for key in ["expiry", "call_premium", "put_premium", "call_volume",
                        "put_volume", "net_premium"]:
                val = f.get(key)
                if val is not None and val != "":
                    parts.append(f"{key}={val}")
            if parts:
                sections.append("  " + " | ".join(parts))

    # Flow Per Strike
    fps = flow_data.get("flow_per_strike", [])
    if fps:
        sections.append(f"\n--- FLOW PER STRIKE (top {min(len(fps), 15)}) ---")
        for f in fps[:15]:
            parts = []
            for key in ["strike", "call_premium", "put_premium", "call_volume",
                        "put_volume", "net_premium"]:
                val = f.get(key)
                if val is not None and val != "":
                    parts.append(f"{key}={val}")
            if parts:
                sections.append("  " + " | ".join(parts))

    # Greek Exposure
    greeks = flow_data.get("greek_exposure", {})
    if greeks and isinstance(greeks, dict):
        sections.append("\n--- GREEK EXPOSURE ---")
        for key in ["net_delta", "net_gamma", "net_vanna", "net_charm",
                     "call_delta", "put_delta", "call_gamma", "put_gamma"]:
            val = greeks.get(key)
            if val is not None:
                sections.append(f"  {key}: {val}")

    # IV Rank
    iv = flow_data.get("iv_rank", {})
    if iv and isinstance(iv, dict):
        sections.append("\n--- IV RANK ---")
        for key in ["iv_rank", "iv_percentile", "iv_current", "iv_high",
                     "iv_low", "hv_20", "hv_50"]:
            val = iv.get(key)
            if val is not None:
                sections.append(f"  {key}: {val}")

    # Volatility Stats (was fetched but never shown — now exposed)
    vol_stats = flow_data.get("volatility_stats", {})
    if vol_stats and isinstance(vol_stats, dict):
        sections.append("\n--- VOLATILITY STATS ---")
        for key in ["iv", "iv_high", "iv_low", "iv_rank",
                     "rv", "rv_high", "rv_low"]:
            val = vol_stats.get(key)
            if val is not None:
                sections.append(f"  {key}: {val}")

    # --- TIER 1: Deep contract history ---
    # 1. Contract summary block (always shown — compact conclusion first)
    # 2. Raw day-by-day detail (dynamically compressed by prompt budget)
    hist = flow_data.get("historical_contracts", {})
    top_contracts = hist.get("top_contracts", [])
    if top_contracts:
        contracts_with_hist = [c for c in top_contracts if c.get("history")]
        if contracts_with_hist:
            n_days = contracts_with_hist[0].get("history_days", "?")

            # Step 1: Build summary block (always shown)
            summary_text, analyses = _build_contract_summary(
                contracts_with_hist, n_days
            )
            if summary_text:
                sections.append(f"\n{summary_text}")

            # Step 2: Rank contracts by relevance for raw detail
            ranked = sorted(
                zip(analyses, contracts_with_hist),
                key=lambda x: x[0]["relevance_score"],
                reverse=True,
            )

            # Step 3: Determine contracts + rows to show
            # Target: contract raw detail ≈ 7-9K chars
            # This gives ~50-60% attention when medium sections are ~1-2K
            # ~200 chars/row avg, ~120 char header per contract
            avg_rows = int(n_days) if isinstance(n_days, int) else 15
            contract_budget = 8500  # target chars for raw contract section
            chars_per_row = 200
            header_per_contract = 120

            # Start with 3-4 contracts, trim rows to fit
            max_raw = min(4, len(ranked))
            rows_per_contract = avg_rows  # start with all rows

            while max_raw >= 2:
                est = max_raw * (rows_per_contract * chars_per_row
                                 + header_per_contract)
                if est <= contract_budget:
                    break
                # Try fewer rows first (min 7), then fewer contracts
                if rows_per_contract > 7:
                    rows_per_contract -= 1
                else:
                    max_raw -= 1
                    rows_per_contract = avg_rows  # reset rows for fewer contracts

            raw_contracts = ranked[:max_raw]
            omitted = len(ranked) - max_raw

            rows_label = (f", {rows_per_contract}d shown"
                          if rows_per_contract < avg_rows else f", {n_days}d")
            sections.append(
                f"\n--- CONTRACT DETAIL (top {max_raw} by relevance"
                f"{f', {omitted} summary-only' if omitted > 0 else ''}"
                f"{rows_label}) ---"
            )

            for analysis, c in raw_contracts:
                cp = c.get("call_put", "?")
                strike = c.get("strike", "?")
                expiry = c.get("expiry", "?")
                sym = c.get("option_symbol", "?")

                # Per-contract analytics header
                oi_trend = analysis["oi_trend"]
                oi_pct = analysis["oi_change_pct"]
                oi_label = "NEW" if oi_pct >= 9999 else f"{oi_pct:+.0f}%"
                side = analysis["side_signal"]
                intent = analysis.get("execution_intent", "")
                intent_tag = (f" | Exec: {intent.replace('_', ' ').title()}"
                              if intent not in ("UNKNOWN", "MIXED", "") else "")

                sections.append(
                    f"\n  CONTRACT: {sym} | {cp} ${strike} exp {expiry} "
                    f"| OI: {oi_trend} ({oi_label}) | Side: {side}{intent_tag}"
                )

                # Contract flow detail (if available)
                flow_agg = c.get("contract_flow", {})
                if flow_agg and flow_agg.get("fills", 0) > 0:
                    fa = flow_agg
                    fill_sig = fa.get("fill_signal", "")
                    fill_tag = (f" | fills: {fill_sig.lower().replace('_', ' ')}"
                                if fill_sig not in ("UNKNOWN", "") else "")
                    sections.append(
                        f"  Flow tape: {fa['fills']} fills | "
                        f"ask {fa.get('ask_pct', 0):.0f}% / "
                        f"bid {fa.get('bid_pct', 0):.0f}% / "
                        f"mid {fa.get('mid_pct', 0):.0f}% | "
                        f"sweeps: {fa.get('sweep_count', 0)} | "
                        f"blocks: {fa.get('block_count', 0)}"
                        f"{fill_tag}"
                    )

                sections.append("  Day-by-day (newest first):")

                for day in c["history"][:rows_per_contract]:
                    day_parts = []
                    core_fields = [
                        ("date", None),
                        ("volume", 0), ("open_interest", 0),
                        ("last_price", 2), ("implied_volatility", 4),
                        ("nbbo_bid", 2), ("nbbo_ask", 2),
                        ("bid_volume", 0), ("ask_volume", 0),
                        ("total_premium", 0), ("trades", 0),
                    ]
                    for key, precision in core_fields:
                        val = day.get(key)
                        if val is None or val == "":
                            continue
                        if precision is not None:
                            try:
                                val = round(float(val), precision)
                                val = int(val) if precision == 0 else val
                            except (ValueError, TypeError):
                                pass
                        day_parts.append(f"{key}={val}")
                    if day_parts:
                        sections.append("    " + " | ".join(day_parts))

            if omitted > 0:
                sections.append(
                    f"\n  Note: {omitted} additional contract(s) tracked in "
                    f"summary above but raw detail omitted for prompt balance."
                )

    sections.append(f"\n=== END UW FLOW DATA: {ticker} ===")

    text = "\n".join(sections)
    if len(text) > max_chars:
        text = text[:max_chars - 50] + "\n... [truncated for length]"
    return text


def format_market_flow_summary(max_alerts: int = 30) -> str:
    """
    Build a market-wide flow summary for the R1 catalyst hunter.
    Shows the most unusual activity across all tickers.
    """
    alerts = fetch_market_flow_alerts(limit=max_alerts)
    if not alerts:
        return ""

    lines = [f"=== MARKET-WIDE UNUSUAL FLOW ({len(alerts)} alerts) ==="]
    for a in alerts:
        parts = []
        for key in ["ticker", "call_put", "strike", "expiry", "premium",
                     "volume", "open_interest", "trade_type", "sentiment"]:
            val = a.get(key)
            if val is not None and val != "":
                parts.append(f"{key}={val}")
        if parts:
            lines.append("  " + " | ".join(parts))

    lines.append("=== END MARKET FLOW ===")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Utility: clear cache
# ---------------------------------------------------------------------------
def clear_cache():
    """Remove all cached UW data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.json"):
            f.unlink()
        print("  🗑 UW cache cleared")


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    no_history = "--no-history" in sys.argv
    speed = "medium"
    for arg in sys.argv:
        if arg.startswith("--speed="):
            speed = arg.split("=")[1]

    print(f"Testing UW flow fetch for {ticker} (speed={speed})...")

    # Test trading sessions helper
    print(f"\n5 trading sessions ago: {_trading_sessions_ago(5)}")
    print(f"10 trading sessions ago: {_trading_sessions_ago(10)}")

    # Test speed classifier
    test_idea = {"repricing_window": "1-3 weeks", "catalyst_status": "Active"}
    config = classify_catalyst_speed(test_idea)
    print(f"\nSpeed config for '1-3 weeks': {config}")

    lookback = LOOKBACK_PROFILES.get(speed, LOOKBACK_PROFILES["medium"])
    flow = build_ticker_flow_context(
        ticker, include_history=not no_history,
        lookback=lookback, direction="Bullish"
    )
    print(f"\nData available: {flow['data_available']}")
    print(f"Flow alerts: {len(flow['flow_alerts'])}")
    print(f"Options volume rows: {len(flow['options_volume'])}")
    print(f"OI changes: {len(flow['oi_change'])}")
    print(f"Dark pool: {len(flow['darkpool'])}")
    print(f"Lit flow: {len(flow['lit_flow'])}")

    hist = flow.get("historical_contracts", {})
    print(f"Contracts found: {hist.get('contracts_found', 0)}")
    top = hist.get("top_contracts", [])
    with_hist = [c for c in top if c.get("history")]
    print(f"Contracts with history: {len(with_hist)}")

    text = format_flow_for_prompt(flow)
    print(f"\nFormatted text ({len(text)} chars):")
    print(text[:5000])
