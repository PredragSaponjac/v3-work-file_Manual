"""
ORCA v20 — Position Sizing Engine.

Kelly criterion + ergodic adjustment + liquidity constraints.

Sizing pipeline:
    1. Compute raw Kelly fraction from estimated win_rate and payoff ratio
    2. Apply ergodic discount (quarter-Kelly by default)
    3. Apply liquidity haircut based on OI
    4. Cap at max_single_position_pct
    5. Convert to contracts
"""

import logging
import math
import os
from typing import Dict, Optional

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.run_context import RunContext
from orca_v20.schemas import StructuredTrade

logger = logging.getLogger("orca_v20.sizing")

_DEFAULT_PORTFOLIO_VALUE = 50000.0


def _resolve_portfolio_value(ctx: RunContext) -> float:
    """
    Resolve portfolio value in priority order:
        1. RunContext.portfolio_value (if set)
        2. ORCA_PORTFOLIO_VALUE env var
        3. Default fallback
    """
    # 1. RunContext override
    if ctx.portfolio_value is not None and ctx.portfolio_value > 0:
        return ctx.portfolio_value

    # 2. Environment variable
    env_val = os.environ.get("ORCA_PORTFOLIO_VALUE")
    if env_val:
        try:
            val = float(env_val)
            if val > 0:
                return val
        except (ValueError, TypeError):
            logger.warning(f"Invalid ORCA_PORTFOLIO_VALUE={env_val}, using default")

    # 3. Default
    return _DEFAULT_PORTFOLIO_VALUE


# ─────────────────────────────────────────────────────────────────────
# Win rate / payoff estimation from trade structure
# ─────────────────────────────────────────────────────────────────────

def _estimate_win_rate(trade: StructuredTrade) -> float:
    """
    Estimate base win rate from confidence + trade type.

    Confidence mapping (1-10):
        10 → 0.70, 8 → 0.60, 6 → 0.50, 4 → 0.40

    Credit strategies (sell spreads) get a boost.
    Debit strategies get a penalty.
    """
    base = 0.35 + (trade.confidence / 10.0) * 0.35  # range: 0.35 - 0.70

    # Strategy adjustment
    expression = trade.trade_expression_type.value
    if "SELL" in expression and "SPREAD" in expression:
        base += 0.05  # credit spreads have higher base win rate
    elif "BUY" in expression and ("CALL" in expression or "PUT" in expression):
        base -= 0.05  # naked long options have lower win rate

    return max(0.20, min(0.80, base))


def _estimate_payoff_ratio(trade: StructuredTrade) -> float:
    """
    Estimate avg_win / avg_loss from risk_reward or max_gain/max_loss.
    """
    if trade.risk_reward and trade.risk_reward > 0:
        return trade.risk_reward

    if trade.max_gain and trade.max_loss and trade.max_loss > 0:
        return abs(trade.max_gain / trade.max_loss)

    # Default: assume 2:1 for directional, 0.5:1 for credit
    expression = trade.trade_expression_type.value
    if "SELL" in expression:
        return 0.5  # credit spreads: small win, big potential loss
    return 2.0  # debit strategies: lose premium, win multiples


# ─────────────────────────────────────────────────────────────────────
# Kelly computation
# ─────────────────────────────────────────────────────────────────────

def _kelly_fraction(win_rate: float, payoff_ratio: float) -> float:
    """
    Compute Kelly fraction: f* = (p * W - (1-p)) / W

    Where:
        p = win probability
        W = win/loss ratio (payoff)

    Returns fraction of bankroll to risk.
    """
    if payoff_ratio <= 0:
        return 0.0

    f = (win_rate * payoff_ratio - (1 - win_rate)) / payoff_ratio

    # Kelly can be negative (don't trade) or very large (concentrated)
    return max(0.0, f)


def _liquidity_haircut(trade: StructuredTrade) -> float:
    """
    Reduce size if liquidity is poor.

    Returns multiplier in [0.0, 1.0].
    1.0 = full size (liquid)
    0.0 = no trade (illiquid)
    """
    liq_score = trade.liquidity_score
    if liq_score is None:
        return 0.8  # unknown liquidity → conservative

    # Scale: score 0-1 maps to haircut
    if liq_score >= 0.8:
        return 1.0
    elif liq_score >= 0.5:
        return 0.8
    elif liq_score >= 0.3:
        return 0.5
    else:
        return 0.25


# ─────────────────────────────────────────────────────────────────────
# Main sizing
# ─────────────────────────────────────────────────────────────────────

def compute_size(trade: StructuredTrade, ctx: RunContext) -> StructuredTrade:
    """
    Compute position size using Kelly + ergodic + liquidity.

    Populates:
        trade.kelly_size_pct
        trade.adjusted_size_pct
        trade.contracts
    """
    if not FLAGS.enable_sizing:
        return trade

    win_rate = _estimate_win_rate(trade)
    payoff = _estimate_payoff_ratio(trade)

    # Raw Kelly
    raw_kelly = _kelly_fraction(win_rate, payoff)

    # Ergodic adjustment (quarter-Kelly by default)
    ergodic_kelly = raw_kelly * THRESHOLDS.kelly_fraction

    # Cap at max single position
    capped = min(ergodic_kelly, THRESHOLDS.max_single_position_pct)

    # Liquidity haircut
    haircut = _liquidity_haircut(trade)
    adjusted = capped * haircut

    # A5: Resolve portfolio value from RunContext → env → default
    portfolio_value = _resolve_portfolio_value(ctx)
    entry = trade.entry_price or 1.0
    max_risk = portfolio_value * adjusted
    contracts = max(1, int(max_risk / (entry * 100))) if entry > 0 else 1

    trade.kelly_size_pct = round(raw_kelly * 100, 2)
    trade.adjusted_size_pct = round(adjusted * 100, 2)
    trade.contracts = contracts

    logger.info(
        f"  [{trade.ticker}] Sizing: Kelly={raw_kelly:.3f} → "
        f"ergodic={ergodic_kelly:.3f} → adjusted={adjusted:.3f} "
        f"({contracts} contracts)"
    )

    return trade


def size_all(trades: list, ctx: RunContext) -> list:
    """Size all trades."""
    if not FLAGS.enable_sizing:
        return trades

    logger.info(f"[run_id={ctx.run_id}] Sizing {len(trades)} trades")
    result = [compute_size(t, ctx) for t in trades]
    ctx.mark_stage("sizing")
    return result
