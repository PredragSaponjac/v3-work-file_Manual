"""
Adapter: Stage 6 — Trade Logger.

Writes structured trades to orca_v20.db (etp_records table).
Also calls v3 trade_logger.log_trades() for v3 DB compatibility.
Does NOT modify v3 DB schema -- calls existing v3 functions as-is.

dry_run=True -> no DB writes at all (v20 or v3).
"""

import logging
import sys
import os
from datetime import datetime, timezone
from typing import List

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.config import FLAGS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import IdeaCandidate, StructuredTrade

logger = logging.getLogger("orca_v20.adapters.logger")


def _log_trade_v20(trade: StructuredTrade, ctx: RunContext) -> bool:
    """Write a single StructuredTrade to orca_v20.db -> etp_records."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO etp_records (
                run_id, idea_id, thesis_id, ticker,
                idea_direction, trade_expression, strategy_label,
                strike_1, strike_2, expiry, dte,
                entry_price, target_price, stop_price,
                max_loss, max_gain, risk_reward,
                iv_at_entry, iv_hv_ratio, delta, theta,
                kelly_size_pct, adjusted_size_pct, contracts,
                slippage_pct, liquidity_score,
                confidence, confidence_raw, urgency, urgency_raw,
                consensus_tag,
                report_framing, report_label,
                expected_horizon,
                status, created_utc
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?,
                ?, ?,
                ?,
                ?, ?
            )
        """, (
            trade.run_id, trade.idea_id, trade.thesis_id, trade.ticker,
            trade.idea_direction.value, trade.trade_expression_type.value, trade.strategy_label,
            trade.strike_1, trade.strike_2, trade.expiry, trade.dte,
            trade.entry_price, trade.target_price, trade.stop_price,
            trade.max_loss, trade.max_gain, trade.risk_reward,
            trade.iv_at_entry, trade.iv_hv_ratio, trade.delta, trade.theta,
            trade.kelly_size_pct, trade.adjusted_size_pct, trade.contracts,
            trade.estimated_slippage_pct, trade.liquidity_score,
            trade.confidence, trade.confidence_raw,
            trade.urgency, trade.urgency_raw,
            trade.consensus_tag.value,
            trade.report_framing, trade.report_label,
            getattr(trade, 'expected_horizon', 'UNKNOWN') or 'UNKNOWN',
            "OPEN", datetime.now(timezone.utc).isoformat(),
        ))

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"[{trade.ticker}] v20 DB write failed: {e}")
        return False


def _log_trades_v3(ideas: List[IdeaCandidate]) -> int:
    """
    Call v3 trade_logger.log_trades() for backward compatibility.
    Passes the raw v3 dicts -- no schema changes.
    """
    try:
        from trade_logger import log_trades, snapshot_all_positions

        v3_dicts = [idea._v3_raw for idea in ideas if idea._v3_raw]
        if not v3_dicts:
            return 0

        n_logged = log_trades(v3_dicts)
        snapshot_all_positions()
        return n_logged

    except ImportError:
        logger.warning("[Stage 6] v3 trade_logger not available -- v3 logging skipped")
        return 0
    except Exception as e:
        logger.error(f"[Stage 6] v3 trade logging failed (non-critical): {e}")
        return 0


def _sync_sheet() -> None:
    """Call v3 sheet_sync for Google Sheet update."""
    try:
        from sheet_sync import sync_trades_to_sheet
        sync_trades_to_sheet()
        logger.info("  Google Sheet synced")
    except Exception as e:
        logger.warning(f"  Sheet sync failed (non-critical): {e}")


def run_stage6(
    trades: List[StructuredTrade],
    ideas: List[IdeaCandidate],
    ctx: RunContext,
) -> int:
    """
    Execute Stage 6: Log trades to both v20 DB and v3 DB.

    dry_run=True -> no writes at all.
    Returns count of successfully logged trades (v20 DB).
    """
    logger.info(f"[run_id={ctx.run_id}] Stage 6: Logging {len(trades)} trades")

    if ctx.dry_run:
        logger.info("[Stage 6] DRY RUN -- skipping all DB writes")
        ctx.mark_stage("stage6_logger")
        return 0

    # Write to v20 DB
    v20_count = 0
    for trade in trades:
        if _log_trade_v20(trade, ctx):
            v20_count += 1
            logger.info(f"  [{trade.ticker}] Logged to orca_v20.db")

    # Write to v3 DB (only if mirroring enabled)
    v3_count = 0
    if FLAGS.mirror_to_v3_trade_log:
        v3_count = _log_trades_v3(ideas)
        logger.info(f"  v3 logger: {v3_count} trades logged to orca_v3_trades.db")
    else:
        logger.info("  v3 trade log mirroring: OFF (FLAGS.mirror_to_v3_trade_log)")

    # Sheet sync: handled by v20 publisher (Stage 7), NOT legacy v3 sheet_sync.
    # Legacy _sync_sheet() was clearing the sheet and rebuilding from v3 DB,
    # which overwrites v20 data. v20 publisher now owns all sheet writes.
    logger.info("  Google Sheet sync: deferred to Stage 7 (v20 publisher)")

    logger.info(
        f"[run_id={ctx.run_id}] Stage 6 complete: "
        f"{v20_count} v20 + {v3_count} v3 logged"
    )
    ctx.mark_stage("stage6_logger")
    return v20_count
