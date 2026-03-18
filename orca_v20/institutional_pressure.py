"""
ORCA v20 — Institutional Pressure Model.

Public-data crowding and pain model using:
- Options gamma exposure (estimated from OI)
- Dark pool activity percentages
- Short interest
- Institutional flow direction

Outputs:
    likely_holder_clusters: str
    crowded_holder_probability: float
    pain_trigger_estimate: float
    institutional_trap_score: float
    data_staleness_days: int

Uses Yahoo Finance public data where available.
Gracefully degrades when data is unavailable.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from orca_v20.config import FLAGS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.institutional_pressure")


# ─────────────────────────────────────────────────────────────────────
# Data fetchers (graceful degradation)
# ─────────────────────────────────────────────────────────────────────

def _fetch_short_interest(ticker: str) -> Optional[Dict]:
    """Fetch short interest data from Yahoo Finance."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        shares_short = info.get("sharesShort", 0) or 0
        shares_outstanding = info.get("sharesOutstanding", 0) or 0
        short_ratio = info.get("shortRatio", 0) or 0
        short_pct = info.get("shortPercentOfFloat", 0) or 0

        return {
            "shares_short": shares_short,
            "shares_outstanding": shares_outstanding,
            "short_ratio": short_ratio,
            "short_pct_of_float": short_pct,
        }
    except Exception as e:
        logger.debug(f"  [{ticker}] Short interest fetch failed: {e}")
        return None


def _fetch_institutional_holders(ticker: str) -> Optional[Dict]:
    """Fetch institutional holder concentration from Yahoo Finance."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.info
        inst_pct = info.get("heldPercentInstitutions", 0) or 0
        insider_pct = info.get("heldPercentInsiders", 0) or 0

        # Try to get top holders for cluster analysis
        try:
            holders = t.institutional_holders
            top_holders = []
            if holders is not None and len(holders) > 0:
                for _, row in holders.head(5).iterrows():
                    top_holders.append({
                        "holder": str(row.get("Holder", "")),
                        "shares": int(row.get("Shares", 0)),
                        "pct": float(row.get("% Out", 0)),
                    })
        except Exception:
            top_holders = []

        return {
            "institutional_pct": inst_pct,
            "insider_pct": insider_pct,
            "top_holders": top_holders,
        }
    except Exception as e:
        logger.debug(f"  [{ticker}] Institutional data fetch failed: {e}")
        return None


def _estimate_gamma_exposure(ticker: str) -> Optional[float]:
    """
    Estimate net gamma exposure from options OI.
    Positive gamma = dealers long gamma (market stabilizing).
    Negative gamma = dealers short gamma (market amplifying).
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        chain = t.options
        if not chain:
            return None

        # Use nearest expiry
        nearest = chain[0]
        opts = t.option_chain(nearest)

        # Net gamma = sum(call_OI * call_gamma) - sum(put_OI * put_gamma)
        # Simplified: just use OI ratio as proxy
        call_oi = opts.calls["openInterest"].sum() if "openInterest" in opts.calls.columns else 0
        put_oi = opts.puts["openInterest"].sum() if "openInterest" in opts.puts.columns else 0
        total_oi = call_oi + put_oi

        if total_oi == 0:
            return 0.0

        # Normalized: positive = more calls (bullish gamma), negative = more puts
        gamma_ratio = (call_oi - put_oi) / total_oi
        return round(gamma_ratio, 4)

    except Exception as e:
        logger.debug(f"  [{ticker}] Gamma exposure estimation failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# Pressure computation
# ─────────────────────────────────────────────────────────────────────

def compute_pressure(ticker: str, ctx: RunContext) -> Optional[Dict]:
    """
    Compute institutional pressure metrics for a ticker.

    Returns dict with all pressure dimensions.
    Gracefully handles missing data.
    """
    if not FLAGS.enable_institutional_pressure:
        return None

    logger.debug(f"  [{ticker}] Computing institutional pressure")

    short_data = _fetch_short_interest(ticker)
    inst_data = _fetch_institutional_holders(ticker)
    gamma_exp = _estimate_gamma_exposure(ticker)

    # Compute derived metrics
    short_pct = 0.0
    if short_data:
        short_pct = short_data.get("short_pct_of_float", 0) or 0

    inst_pct = 0.0
    top_holder_concentration = 0.0
    likely_clusters = "unknown"
    if inst_data:
        inst_pct = inst_data.get("institutional_pct", 0) or 0
        top_holders = inst_data.get("top_holders", [])
        if top_holders:
            top_holder_concentration = sum(h.get("pct", 0) for h in top_holders[:3])
            # Classify holder cluster type
            holder_names = " ".join(h.get("holder", "").lower() for h in top_holders)
            if "vanguard" in holder_names or "blackrock" in holder_names:
                likely_clusters = "passive_index"
            elif "capital" in holder_names or "management" in holder_names:
                likely_clusters = "active_institutional"
            else:
                likely_clusters = "mixed"

    # Crowding score: high short interest + high institutional = crowded
    crowding_score = min(1.0, (short_pct * 5.0 + inst_pct) / 2.0)

    # Pain level: short squeeze potential (high SI + negative gamma)
    gamma_val = gamma_exp if gamma_exp is not None else 0.0
    pain_level = min(1.0, short_pct * 3.0 + max(0, -gamma_val))

    # Institutional trap: high concentration + high SI = potential trap
    trap_score = min(1.0, top_holder_concentration * 2.0 + short_pct * 2.0)

    # Dark pool % — approximate from institutional holding
    # Real dark pool data requires FINRA ATS data (paid)
    dark_pool_pct = min(0.5, inst_pct * 0.4)

    # Flow direction heuristic
    if gamma_val > 0.15:
        flow_direction = "accumulating"
    elif gamma_val < -0.15:
        flow_direction = "distributing"
    else:
        flow_direction = "neutral"

    result = {
        "crowding_score": round(crowding_score, 4),
        "pain_level": round(pain_level, 4),
        "gamma_exposure": gamma_val,
        "dark_pool_pct": round(dark_pool_pct, 4),
        "short_interest_pct": round(short_pct, 4),
        "institutional_flow": flow_direction,
        "likely_holder_clusters": likely_clusters,
        "crowded_holder_probability": round(crowding_score, 4),
        "pain_trigger_estimate": round(pain_level, 4),
        "institutional_trap_score": round(trap_score, 4),
        "data_staleness_days": 0,  # fresh fetch
    }

    logger.info(
        f"  [{ticker}] Pressure: crowding={crowding_score:.3f}, "
        f"pain={pain_level:.3f}, flow={flow_direction}"
    )

    return result


def _persist_snapshot(ticker: str, pressure: Dict, ctx: RunContext) -> None:
    """Write pressure snapshot to DB."""
    if ctx.dry_run:
        return
    try:
        conn = get_connection()
        conn.execute("""
            INSERT INTO institutional_pressure_snapshots (
                run_id, ticker, snapshot_date,
                crowding_score, pain_level, gamma_exposure,
                dark_pool_pct, short_interest_pct, institutional_flow,
                created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ctx.run_id,
            ticker,
            ctx.market_date,
            pressure.get("crowding_score", 0),
            pressure.get("pain_level", 0),
            pressure.get("gamma_exposure", 0),
            pressure.get("dark_pool_pct", 0),
            pressure.get("short_interest_pct", 0),
            pressure.get("institutional_flow", "neutral"),
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist pressure snapshot for {ticker}: {e}")


def run_pressure_scan(tickers: list, ctx: RunContext) -> Dict[str, Dict]:
    """
    Scan institutional pressure for all tickers.
    Returns {ticker: pressure_dict}.
    """
    if not FLAGS.enable_institutional_pressure:
        return {}

    logger.info(f"[run_id={ctx.run_id}] Institutional pressure scan for {len(tickers)} tickers")

    results = {}
    for ticker in tickers:
        pressure = compute_pressure(ticker, ctx)
        if pressure:
            results[ticker] = pressure
            _persist_snapshot(ticker, pressure, ctx)

    logger.info(f"[institutional_pressure] Scanned {len(results)}/{len(tickers)} tickers")
    ctx.mark_stage("institutional_pressure")
    return results
