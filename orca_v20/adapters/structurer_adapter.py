"""
Adapter: Stage 4/5 — Trade Structuring.

Wraps trade_structurer.structure_all() and maps output
to v20 StructuredTrade objects.

CRITICAL: idea_direction is PRESERVED from the original IdeaCandidate.
trade_expression_type is set SEPARATELY based on structuring logic.
The legacy v3 bug of overwriting direction with strategy does NOT propagate.
"""

import logging
import sys
import os
from typing import List, Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.run_context import RunContext
from orca_v20.schemas import (
    IdeaCandidate,
    StructuredTrade,
    TradeExpressionType,
)

logger = logging.getLogger("orca_v20.adapters.structurer")


def _parse_expression_type(raw: str) -> TradeExpressionType:
    """Map v3 strategy_type string to v20 TradeExpressionType."""
    raw_upper = (raw or "").strip().upper().replace(" ", "_")
    mapping = {
        "BUY_CALL": TradeExpressionType.BUY_CALL,
        "BUY_PUT": TradeExpressionType.BUY_PUT,
        "SELL_CALL": TradeExpressionType.SELL_CALL,
        "SELL_PUT": TradeExpressionType.SELL_PUT,
        "BUY_CALL_SPREAD": TradeExpressionType.BUY_CALL_SPREAD,
        "BUY_PUT_SPREAD": TradeExpressionType.BUY_PUT_SPREAD,
        "SELL_CALL_SPREAD": TradeExpressionType.SELL_CALL_SPREAD,
        "SELL_PUT_SPREAD": TradeExpressionType.SELL_PUT_SPREAD,
        "SELL_PUT_VERTICAL": TradeExpressionType.SELL_PUT_SPREAD,
        "SELL_CALL_VERTICAL": TradeExpressionType.SELL_CALL_SPREAD,
        "BUY_STRADDLE": TradeExpressionType.BUY_STRADDLE,
        "SELL_STRADDLE": TradeExpressionType.SELL_STRADDLE,
        "BUY_STRANGLE": TradeExpressionType.BUY_STRANGLE,
        "SELL_STRANGLE": TradeExpressionType.SELL_STRANGLE,
        "IRON_CONDOR": TradeExpressionType.IRON_CONDOR,
        "IRON_BUTTERFLY": TradeExpressionType.IRON_BUTTERFLY,
        "CALENDAR_SPREAD": TradeExpressionType.CALENDAR_SPREAD,
        "DIAGONAL_SPREAD": TradeExpressionType.DIAGONAL_SPREAD,
        "UNDERLYING": TradeExpressionType.CUSTOM,
    }
    return mapping.get(raw_upper, TradeExpressionType.CUSTOM)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def structure_ideas(ideas: List[IdeaCandidate], ctx: RunContext) -> List[StructuredTrade]:
    """
    Run v3 trade_structurer.structure_all() on the raw v3 dicts,
    then map results to v20 StructuredTrade objects.

    The v3 structurer mutates dicts in-place and overwrites direction.
    We capture strategy_type BEFORE that overwrite, and map it
    to trade_expression_type while preserving idea_direction.
    """
    if not ideas:
        return []

    # Build v3 dict copies for structurer (it mutates in-place)
    v3_dicts = []
    idea_by_ticker = {}  # ticker -> IdeaCandidate (for matching)
    idea_by_index = {}   # index -> IdeaCandidate (fallback)
    for i, idea in enumerate(ideas):
        v3_dict = dict(idea._v3_raw) if idea._v3_raw else {
            "ticker": idea.ticker,
            "direction": idea.idea_direction.value.capitalize(),
            "repricing_window": idea.repricing_window,
        }
        v3_dicts.append(v3_dict)
        idea_by_ticker[idea.ticker.upper()] = idea
        idea_by_index[i] = idea

    # Call v3 structurer
    try:
        from trade_structurer import structure_all
        structured_dicts = structure_all(v3_dicts)
        logger.info(f"[Stage 4] v3 structurer returned {len(structured_dicts)} trades")
    except ImportError:
        logger.warning("[Stage 4] trade_structurer.py not available -- skipping structuring")
        structured_dicts = v3_dicts
    except Exception as e:
        logger.error(f"[Stage 4] Trade structuring failed: {e}")
        ctx.add_error("stage4_structure", str(e))
        structured_dicts = v3_dicts

    # Detect partial structurer failures
    if len(structured_dicts) < len(v3_dicts):
        logger.warning(
            f"[Stage 4] Structurer returned {len(structured_dicts)}/{len(v3_dicts)} "
            f"trades — {len(v3_dicts) - len(structured_dicts)} ideas lost silently"
        )

    # Map to v20 StructuredTrade objects — match by ticker, fall back to index
    trades = []
    for i, sd in enumerate(structured_dicts):
        sd_ticker = (sd.get("ticker") or "").upper()
        idea = idea_by_ticker.get(sd_ticker) if sd_ticker else None
        if idea is None:
            idea = idea_by_index.get(i)
        if idea is None:
            logger.warning(f"[Stage 4] Could not match structured trade #{i} (ticker={sd_ticker}) to any idea — skipping")
            continue

        # Read strategy_type (v3 adds this key during structuring)
        strategy_type = sd.get("strategy_type", "")

        trade = StructuredTrade(
            idea_id=idea.idea_id,
            thesis_id=idea.thesis_id or "",
            run_id=ctx.run_id,
            ticker=idea.ticker,

            # CRITICAL: preserve original direction from thesis
            idea_direction=idea.idea_direction,

            # Expression type from structurer -- separate field
            trade_expression_type=_parse_expression_type(strategy_type),

            strategy_label=strategy_type.replace("_", " ").title() if strategy_type else "",
            strike_1=_safe_float(sd.get("strike")),
            strike_2=_safe_float(sd.get("strike_2")),
            expiry=sd.get("expiry"),
            dte=sd.get("dte"),

            entry_price=_safe_float(sd.get("entry_price")),
            target_price=_safe_float(sd.get("target_price") or sd.get("profit_target")),
            stop_price=_safe_float(sd.get("stop_price") or sd.get("stop_alert")),
            max_loss=_safe_float(sd.get("max_loss")),
            max_gain=_safe_float(sd.get("max_gain")),
            risk_reward=_safe_float(sd.get("risk_reward")),

            iv_at_entry=_safe_float(sd.get("iv_at_entry")),
            iv_hv_ratio=_safe_float(sd.get("iv_hv_ratio")),
            delta=_safe_float(sd.get("delta")),
            theta=_safe_float(sd.get("theta")),

            confidence=idea.confidence,
            confidence_raw=idea.confidence_raw,
            urgency=idea.urgency,
            urgency_raw=idea.urgency_raw,
            consensus_tag=idea.consensus_tag,
            expected_horizon=idea.expected_horizon.value if hasattr(idea.expected_horizon, 'value') else str(idea.expected_horizon),

            report_framing=sd.get("report_framing"),
            report_label=sd.get("report_label"),
        )

        # Update v3 raw dict on the idea for downstream logger/report
        idea._v3_raw = sd

        trades.append(trade)
        logger.info(
            f"  [{trade.ticker}] {trade.idea_direction.value} -> "
            f"{trade.trade_expression_type.value} "
            f"| Strike: {trade.strike_1} | Exp: {trade.expiry}"
        )

    return trades


def _vol_aware_adjust(trade: StructuredTrade, ctx: RunContext) -> StructuredTrade:
    """
    C2: Vol-aware structuring adjustment.

    If IV is extremely elevated (IV/HV > 1.5), discourage naked premium buying.
    Prefer defined-risk spreads or premium-selling structures.
    If near a major event (OPEX/FOMC) and IV is high, prefer calendars or spreads.
    """
    iv_hv = trade.iv_hv_ratio
    if iv_hv is None or iv_hv <= 0:
        return trade  # no IV data, no adjustment

    expression = trade.trade_expression_type

    # High IV regime: avoid naked long premium
    if iv_hv > 1.5:
        naked_long = expression in (
            TradeExpressionType.BUY_CALL,
            TradeExpressionType.BUY_PUT,
        )
        if naked_long:
            # FIX #7: Only upgrade to spread if strike_2 is available.
            # If strike_2 is NULL, a spread record would be invalid (missing
            # the second leg).  Keep the original naked type instead.
            if trade.strike_2 is not None:
                # Upgrade to spread
                if trade.idea_direction.value == "BULLISH":
                    trade.trade_expression_type = TradeExpressionType.BUY_CALL_SPREAD
                    trade.strategy_label = "Bull Call Spread (vol-adjusted)"
                else:
                    trade.trade_expression_type = TradeExpressionType.BUY_PUT_SPREAD
                    trade.strategy_label = "Bear Put Spread (vol-adjusted)"
                logger.info(
                    f"  [{trade.ticker}] Vol-adjusted: {expression.value} → "
                    f"{trade.trade_expression_type.value} (IV/HV={iv_hv:.2f})"
                )
            else:
                logger.warning(
                    f"  [{trade.ticker}] Vol-adjust would upgrade {expression.value} to spread "
                    f"but strike_2 is NULL — keeping naked type (IV/HV={iv_hv:.2f})"
                )

    # Very high IV (>2.0) + near OPEX/FOMC: prefer credit structures
    is_event_near = getattr(ctx, "is_opex_week", False) or getattr(ctx, "is_fomc_week", False)
    if iv_hv > 2.0 and is_event_near:
        debit_types = (
            TradeExpressionType.BUY_CALL_SPREAD,
            TradeExpressionType.BUY_PUT_SPREAD,
            TradeExpressionType.BUY_CALL,
            TradeExpressionType.BUY_PUT,
        )
        if expression in debit_types:
            # FIX #7: Guard — credit spreads also require strike_2
            if trade.strike_2 is not None:
                if trade.idea_direction.value == "BULLISH":
                    trade.trade_expression_type = TradeExpressionType.SELL_PUT_SPREAD
                    trade.strategy_label = "Bull Put Spread (event-vol-adjusted)"
                else:
                    trade.trade_expression_type = TradeExpressionType.SELL_CALL_SPREAD
                    trade.strategy_label = "Bear Call Spread (event-vol-adjusted)"
                logger.info(
                    f"  [{trade.ticker}] Event-vol-adjusted → "
                    f"{trade.trade_expression_type.value} (IV/HV={iv_hv:.2f}, event_near)"
                )
            else:
                logger.warning(
                    f"  [{trade.ticker}] Event-vol-adjust would upgrade to credit spread "
                    f"but strike_2 is NULL — keeping {trade.trade_expression_type.value} "
                    f"(IV/HV={iv_hv:.2f}, event_near)"
                )

    return trade


def run_stage4(ideas: List[IdeaCandidate], ctx: RunContext) -> List[StructuredTrade]:
    """
    Execute Stage 4/5: Trade structuring for all confirmed ideas.
    """
    # ── Defensive hard cap (belt-and-suspenders) ──
    import orca_v20.config as _cfg
    if _cfg.BUDGET_MODE and len(ideas) > _cfg.THRESHOLDS.budget_max_structured:
        cap = _cfg.THRESHOLDS.budget_max_structured
        ideas = list(ideas)
        ideas.sort(key=lambda x: x.confidence, reverse=True)
        skipped = ideas[cap:]
        ideas = ideas[:cap]
        logger.warning(
            f"[budget][DEFENSIVE] Stage 4 adapter hard cap: structuring {cap}, "
            f"dropping {len(skipped)} ({[i.ticker for i in skipped]})"
        )

    logger.info(f"[run_id={ctx.run_id}] Stage 4: Structuring {len(ideas)} ideas")

    trades = structure_ideas(ideas, ctx)

    # C2: Vol-aware post-processing
    trades = [_vol_aware_adjust(t, ctx) for t in trades]

    logger.info(f"[run_id={ctx.run_id}] Stage 4 complete: {len(trades)} trades structured")
    ctx.mark_stage("stage4_structure")
    return trades
