"""
ORCA v20 — Thesis Momentum Engine.

Computes cross-time belief/confidence slopes from daily snapshots.
Identifies strengthening and weakening theses.
Supports WATCH → ACTIONABLE promotion logic based on cross-run strengthening.

Uses simple linear regression (no external deps beyond stdlib).
"""

import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import ThesisStatus

logger = logging.getLogger("orca_v20.thesis_momentum")


# ─────────────────────────────────────────────────────────────────────
# Linear regression (pure stdlib)
# ─────────────────────────────────────────────────────────────────────

def _linreg(xs: List[float], ys: List[float]) -> Dict:
    """
    Simple OLS linear regression.
    Returns {slope, intercept, r_squared, n}.
    """
    n = len(xs)
    if n < 2:
        return {"slope": 0.0, "intercept": ys[0] if ys else 0.0, "r_squared": 0.0, "n": n}

    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_x2 = sum(x * x for x in xs)
    sum_y2 = sum(y * y for y in ys)

    denom = n * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-12:
        return {"slope": 0.0, "intercept": sum_y / n, "r_squared": 0.0, "n": n}

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    # R²
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    y_mean = sum_y / n
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    r_squared = 1.0 - (ss_res / ss_tot) if abs(ss_tot) > 1e-12 else 0.0

    return {"slope": slope, "intercept": intercept, "r_squared": max(0.0, r_squared), "n": n}


def _trend_label(slope: float, r_squared: float) -> str:
    """Map slope + fit quality to a trend label."""
    if r_squared < 0.15:
        return "NOISY"
    if slope > 0.3:
        return "STRENGTHENING"
    elif slope < -0.3:
        return "WEAKENING"
    else:
        return "STABLE"


# ─────────────────────────────────────────────────────────────────────
# Core functions
# ─────────────────────────────────────────────────────────────────────

def compute_momentum(thesis_id: str, ctx: RunContext) -> Optional[Dict]:
    """
    Compute confidence slope and momentum metrics for a thesis.

    Returns dict with:
        belief_slope: float         — confidence trend over time
        confidence_slope: float     — alias (same as belief_slope)
        why_now_slope: float        — catalyst urgency trend (approximated)
        thesis_momentum_score: float — composite [-1, 1]
        r_squared: float            — fit quality
        days_tracked: int
        trend_label: str            — "STRENGTHENING", "STABLE", "WEAKENING", "NOISY"
    """
    if not FLAGS.enable_thesis_momentum:
        return None

    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT snapshot_date, confidence, catalyst_status
            FROM thesis_daily_snapshots
            WHERE thesis_id = ?
            ORDER BY snapshot_date ASC
        """, (thesis_id,)).fetchall()
        conn.close()

        if len(rows) < 2:
            return {
                "belief_slope": 0.0,
                "confidence_slope": 0.0,
                "why_now_slope": 0.0,
                "thesis_momentum_score": 0.0,
                "r_squared": 0.0,
                "days_tracked": len(rows),
                "trend_label": "INSUFFICIENT_DATA",
            }

        # X = day index (0, 1, 2, ...), Y = confidence
        xs = list(range(len(rows)))
        ys = [float(r["confidence"] or 0) for r in rows]

        reg = _linreg([float(x) for x in xs], ys)

        # Catalyst status progression score
        # PENDING=0, DEVELOPING=1, CONFIRMED=2 → approximate "why_now" trend
        status_map = {"PENDING": 0.0, "DEVELOPING": 1.0, "CONFIRMED": 2.0,
                       "INVALIDATED": -1.0, "EXPIRED": -1.0}
        status_ys = [status_map.get(r["catalyst_status"], 0.0) for r in rows]
        status_reg = _linreg([float(x) for x in xs], status_ys)

        # Composite momentum score: weighted blend of confidence trend + status trend
        # Normalized to [-1, 1] range
        raw_score = (reg["slope"] * 0.7 + status_reg["slope"] * 0.3)
        momentum_score = max(-1.0, min(1.0, raw_score / 2.0))

        trend = _trend_label(reg["slope"], reg["r_squared"])

        result = {
            "belief_slope": reg["slope"],
            "confidence_slope": reg["slope"],
            "why_now_slope": status_reg["slope"],
            "thesis_momentum_score": momentum_score,
            "r_squared": reg["r_squared"],
            "days_tracked": len(rows),
            "trend_label": trend,
        }

        logger.debug(
            f"  Momentum for {thesis_id}: slope={reg['slope']:.3f}, "
            f"R²={reg['r_squared']:.3f}, trend={trend}"
        )
        return result

    except Exception as e:
        logger.error(f"Failed to compute momentum for {thesis_id}: {e}")
        return None


def _check_promotion(thesis_id: str, momentum: Dict, ctx: RunContext) -> None:
    """
    WATCH → ACTIONABLE promotion logic.

    If a DRAFT thesis shows strengthening momentum (slope > 0, R² > 0.3,
    tracked ≥ 3 days), promote it to ACTIVE.
    """
    if momentum["trend_label"] != "STRENGTHENING":
        return
    if momentum["days_tracked"] < 3:
        return
    if momentum["r_squared"] < 0.3:
        return

    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT status FROM theses WHERE thesis_id = ?", (thesis_id,)
        ).fetchone()

        if row and row["status"] == ThesisStatus.DRAFT.value:
            conn.execute("""
                UPDATE theses
                SET status = ?, confidence_slope = ?, last_updated_utc = ?
                WHERE thesis_id = ?
            """, (
                ThesisStatus.ACTIVE.value,
                momentum["confidence_slope"],
                datetime.now(timezone.utc).isoformat(),
                thesis_id,
            ))
            conn.commit()
            logger.info(f"  Thesis {thesis_id} PROMOTED: DRAFT → ACTIVE "
                        f"(slope={momentum['confidence_slope']:.3f})")
        else:
            # Just update slope on existing active thesis
            conn.execute("""
                UPDATE theses SET confidence_slope = ? WHERE thesis_id = ?
            """, (momentum["confidence_slope"], thesis_id))
            conn.commit()

        conn.close()
    except Exception as e:
        logger.error(f"Failed promotion check for {thesis_id}: {e}")


def run_momentum_update(ctx: RunContext) -> Dict[str, Dict]:
    """
    Update momentum for all active/draft theses.
    Returns {thesis_id: momentum_dict}.
    """
    if not FLAGS.enable_thesis_momentum:
        return {}

    logger.info(f"[run_id={ctx.run_id}] Momentum update starting")

    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT thesis_id FROM theses WHERE status IN ('ACTIVE','DRAFT')"
        ).fetchall()
        conn.close()

        results = {}
        for row in rows:
            tid = row["thesis_id"]
            momentum = compute_momentum(tid, ctx)
            if momentum:
                results[tid] = momentum
                _check_promotion(tid, momentum, ctx)

        logger.info(f"[thesis_momentum] Updated {len(results)} theses")
        return results

    except Exception as e:
        logger.error(f"Momentum update failed: {e}")
        ctx.add_error("thesis_momentum", str(e))
        return {}
