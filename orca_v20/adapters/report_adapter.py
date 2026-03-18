"""
Adapter: Stage 7 — Reporting + Publishing (Operator Activation).

Two publishing paths:
    1. v20 native publisher (Telegram filtered, X filtered, Google Sheet)
    2. Legacy v3 executive_report (fallback if v3 raw data available)

v20 publisher is the PRIMARY path. Legacy is fallback only.
"""

import logging
import sys
import os
from typing import Dict, List, Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.config import FLAGS
from orca_v20.run_context import RunContext
from orca_v20.schemas import IdeaCandidate, StructuredTrade

logger = logging.getLogger("orca_v20.adapters.report")


def run_stage7(
    trades: List[StructuredTrade],
    ideas: List[IdeaCandidate],
    ctx: RunContext,
    trace: Optional[Dict] = None,
    execution_metas: Optional[Dict[str, Dict]] = None,
) -> bool:
    """
    Execute Stage 7: Generate reports + publish via all enabled channels.

    Publishing channels (independently controllable):
        FLAGS.publish_telegram  → Telegram alerts
        FLAGS.publish_x         → X (Twitter) filtered posts
        FLAGS.mirror_to_google_sheet → Google Sheet sync
        FLAGS.publish_reports   → master switch for all publishing

    Gated by:
      - ctx.dry_run → skip everything
      - FLAGS.publish_reports → master switch
    """
    logger.info(f"[run_id={ctx.run_id}] Stage 7: Report generation for {len(trades)} trades")

    if ctx.dry_run:
        logger.info("[Stage 7] DRY RUN -- skipping report + notifications")
        ctx.mark_stage("stage7_report")
        return True

    if not FLAGS.publish_reports:
        logger.info("[Stage 7] Report publishing: OFF (FLAGS.publish_reports)")
        ctx.mark_stage("stage7_report")
        return True

    if not trades:
        logger.info("[Stage 7] No trades -- skipping report")
        ctx.mark_stage("stage7_report")
        return True

    # ── v20 native publisher (PRIMARY) ──
    try:
        from orca_v20.publisher import publish_trades

        pub_summary = publish_trades(
            trades, ideas, ctx,
            trace=trace or {},
            execution_metas=execution_metas,
        )
        logger.info(
            f"[Stage 7] v20 publisher: "
            f"TG={pub_summary['telegram_sent']}, "
            f"X={pub_summary['x_posted']}/{pub_summary['x_filtered']} filtered, "
            f"Sheet={pub_summary['sheet_rows']} rows"
        )

    except Exception as e:
        logger.error(f"[Stage 7] v20 publisher failed: {e}")
        ctx.add_error("stage7_v20_publisher", str(e))

        # ── Legacy fallback ──
        v3_survivors = [idea._v3_raw for idea in ideas if idea._v3_raw]
        if v3_survivors:
            try:
                from executive_report import (
                    generate_executive_reports,
                    send_executive_reports_telegram,
                    send_executive_report_x,
                )
                reports = generate_executive_reports(v3_survivors)
                if reports:
                    if FLAGS.publish_telegram:
                        send_executive_reports_telegram(reports)
                    if FLAGS.publish_x:
                        for report in reports:
                            try:
                                send_executive_report_x(report)
                            except Exception as xe:
                                logger.warning(f"  X post failed: {xe}")
            except ImportError:
                logger.warning("[Stage 7] Legacy executive_report not available")
            except Exception as le:
                logger.error(f"[Stage 7] Legacy fallback also failed: {le}")

    ctx.mark_stage("stage7_report")
    return True
