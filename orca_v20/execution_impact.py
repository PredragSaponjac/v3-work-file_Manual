"""
ORCA v20 — Execution Impact Model (Phase 4 hardened).

Almgren-Chriss inspired slippage estimation with:
    - Temporary impact: f(order_size / ADV)
    - Permanent impact: g(order_size / total_OI)
    - Bid-ask crossing cost
    - Confidence score (data quality)
    - Capacity-constrained and illiquid flags
    - Sanity bounds / clipping
    - Reason codes for interpretability

Populates:
    trade.estimated_slippage_pct
    trade.liquidity_score
"""

import logging
import math
from typing import Optional

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.run_context import RunContext
from orca_v20.schemas import StructuredTrade

logger = logging.getLogger("orca_v20.execution_impact")


# ─────────────────────────────────────────────────────────────────────
# Constants / sanity bounds
# ─────────────────────────────────────────────────────────────────────

MAX_SLIPPAGE_PCT = 15.0       # hard cap — anything over 15% is unreliable
MIN_SLIPPAGE_PCT = 0.01       # floor — there's always *some* slippage
DEFAULT_SLIPPAGE_PCT = 2.0    # when no data is available
DEFAULT_LIQUIDITY = 0.3       # when no data is available
ILLIQUID_OI_THRESHOLD = 100   # OI < 100 = illiquid
ILLIQUID_SPREAD_THRESHOLD = 0.10  # spread > 10% = illiquid
CAPACITY_OI_THRESHOLD = 50    # OI < 50 for this strike = capacity constrained


# ─────────────────────────────────────────────────────────────────────
# Market data fetchers
# ─────────────────────────────────────────────────────────────────────

def _fetch_option_liquidity(ticker: str, strike: float, expiry: str) -> Optional[dict]:
    """
    Fetch option-level liquidity data: OI, volume, bid-ask spread.
    Uses yfinance (graceful fallback).
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        chain_dates = t.options
        if not chain_dates:
            return None

        # Find closest expiry
        target_expiry = expiry or ""
        selected = chain_dates[0]  # default to nearest
        for d in chain_dates:
            if d == target_expiry or d >= target_expiry:
                selected = d
                break

        opts = t.option_chain(selected)

        # Search calls and puts for matching strike
        for df_name, df in [("calls", opts.calls), ("puts", opts.puts)]:
            if strike and "strike" in df.columns:
                matches = df[abs(df["strike"] - strike) < 0.5]
                if len(matches) > 0:
                    row = matches.iloc[0]
                    bid = float(row.get("bid", 0) or 0)
                    ask = float(row.get("ask", 0) or 0)
                    oi = int(row.get("openInterest", 0) or 0)
                    vol = int(row.get("volume", 0) or 0)
                    mid = (bid + ask) / 2 if (bid + ask) > 0 else 1.0
                    spread_pct = (ask - bid) / mid if mid > 0 else 0.0

                    return {
                        "open_interest": oi,
                        "volume": vol,
                        "bid": bid,
                        "ask": ask,
                        "spread_pct": spread_pct,
                        "mid_price": mid,
                    }

        return None

    except Exception as e:
        logger.debug(f"  [{ticker}] Option liquidity fetch failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# Impact models
# ─────────────────────────────────────────────────────────────────────

def _temporary_impact(order_contracts: int, daily_volume: int) -> float:
    """
    Temporary impact: η * sqrt(order_size / ADV).
    η (eta) calibrated to ~0.5% for typical options.
    """
    if daily_volume <= 0:
        return 0.02  # 2% assumed if no volume data

    participation_rate = order_contracts / max(daily_volume, 1)
    eta = 0.005  # 50 bps base
    return eta * math.sqrt(participation_rate)


def _permanent_impact(order_contracts: int, open_interest: int) -> float:
    """
    Permanent impact: γ * (order_size / OI).
    γ calibrated to ~0.1% for typical options.
    """
    if open_interest <= 0:
        return 0.01  # 1% assumed if no OI data

    oi_fraction = order_contracts / max(open_interest, 1)
    gamma = 0.001  # 10 bps base
    return gamma * oi_fraction


def _compute_liquidity_score(oi: int, volume: int, spread_pct: float) -> float:
    """
    Compute a 0-1 liquidity score.
    Factors: OI, volume, bid-ask spread.
    """
    min_oi = THRESHOLDS.min_liquidity_oi
    oi_score = min(1.0, oi / max(min_oi, 1))
    vol_score = min(1.0, volume / max(min_oi * 0.5, 1))

    if spread_pct <= 0.01:
        spread_score = 1.0
    elif spread_pct <= 0.03:
        spread_score = 0.7
    elif spread_pct <= 0.05:
        spread_score = 0.4
    elif spread_pct <= 0.10:
        spread_score = 0.2
    else:
        spread_score = 0.1

    return round(oi_score * 0.4 + vol_score * 0.3 + spread_score * 0.3, 4)


# ─────────────────────────────────────────────────────────────────────
# Main estimation (hardened)
# ─────────────────────────────────────────────────────────────────────

def estimate_impact(trade: StructuredTrade, ctx: RunContext) -> StructuredTrade:
    """
    Estimate execution slippage and liquidity score.

    Populates:
        trade.estimated_slippage_pct
        trade.liquidity_score

    Also logs structured impact report with:
        confidence, capacity_constrained, illiquid, reason_codes
    """
    if not FLAGS.enable_execution_impact:
        return trade

    contracts = trade.contracts or 1
    strike = trade.strike_1 or 0
    expiry = trade.expiry or ""

    # Fetch liquidity data
    liq_data = _fetch_option_liquidity(trade.ticker, strike, expiry)

    if liq_data:
        oi = liq_data["open_interest"]
        vol = liq_data["volume"]
        spread_pct = liq_data["spread_pct"]

        # Compute impact components
        temp_impact = _temporary_impact(contracts, vol)
        perm_impact = _permanent_impact(contracts, oi)
        crossing_cost = spread_pct / 2  # half-spread as crossing cost

        raw_slippage = (temp_impact + perm_impact + crossing_cost) * 100

        # Sanity clipping
        clipped_slippage = max(MIN_SLIPPAGE_PCT, min(MAX_SLIPPAGE_PCT, raw_slippage))
        was_clipped = abs(raw_slippage - clipped_slippage) > 0.001

        liquidity_score = _compute_liquidity_score(oi, vol, spread_pct)

        # Flags
        illiquid = (oi < ILLIQUID_OI_THRESHOLD or spread_pct > ILLIQUID_SPREAD_THRESHOLD)
        capacity_constrained = (oi < CAPACITY_OI_THRESHOLD)

        # Confidence in the estimate (0-1)
        # High OI + high volume + narrow spread = high confidence
        data_confidence = min(1.0, (
            min(1.0, oi / 500) * 0.4 +
            min(1.0, vol / 200) * 0.3 +
            max(0, 1.0 - spread_pct * 10) * 0.3
        ))

        # Reason codes
        reason_codes = []
        if was_clipped:
            reason_codes.append(f"CLIPPED_FROM_{raw_slippage:.1f}")
        if illiquid:
            reason_codes.append("ILLIQUID")
        if capacity_constrained:
            reason_codes.append("CAPACITY_CONSTRAINED")
        if spread_pct > 0.05:
            reason_codes.append("WIDE_SPREAD")
        if data_confidence < 0.3:
            reason_codes.append("LOW_DATA_CONFIDENCE")
        if not reason_codes:
            reason_codes.append("NORMAL")

        trade.estimated_slippage_pct = round(clipped_slippage, 4)
        trade.liquidity_score = liquidity_score

        logger.info(
            f"  [{trade.ticker}] Execution impact: slippage={clipped_slippage:.2f}% "
            f"(temp={temp_impact:.4f}, perm={perm_impact:.6f}, cross={crossing_cost:.4f}) "
            f"| liq={liquidity_score:.3f} | conf={data_confidence:.2f} "
            f"| OI={oi} vol={vol} spread={spread_pct:.2%} "
            f"| flags={reason_codes}"
        )
    else:
        # No data — conservative defaults
        trade.estimated_slippage_pct = DEFAULT_SLIPPAGE_PCT
        trade.liquidity_score = DEFAULT_LIQUIDITY

        logger.info(
            f"  [{trade.ticker}] Execution impact: NO DATA — "
            f"defaults (slippage={DEFAULT_SLIPPAGE_PCT}%, liq={DEFAULT_LIQUIDITY}) "
            f"| flags=[NO_OPTION_DATA]"
        )

    return trade


def assess_all(trades: list, ctx: RunContext) -> list:
    """Assess execution impact for all trades."""
    if not FLAGS.enable_execution_impact:
        return trades

    logger.info(f"[run_id={ctx.run_id}] Execution impact for {len(trades)} trades")
    result = [estimate_impact(t, ctx) for t in trades]
    ctx.mark_stage("execution_impact")
    return result
