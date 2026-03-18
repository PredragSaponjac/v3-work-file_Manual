"""
ORCA v20 — Horizon Utilities.

Dedicated module for thesis horizon parsing, trading-day math,
timing quality assessment, and catalyst integrity checks.

All horizon logic lives here — not in thesis_store or elsewhere.

Trading-day semantics:
    All horizon math uses trading days (~252/yr), not calendar days.
    calendar_to_trading_days() is an approximation (ignores market holidays).
    Sufficient for horizon classification but not exact for backtesting.
"""

import logging
import re
from typing import Optional

from orca_v20.config import THRESHOLDS
from orca_v20.schemas import ThesisHorizon, TimingQuality

logger = logging.getLogger("orca_v20.horizon")


# ─────────────────────────────────────────────────────────────────────
# Horizon parsing
# ─────────────────────────────────────────────────────────────────────

# Regex patterns for repricing_window → ThesisHorizon conversion
_RANGE_RE = re.compile(r'(\d+)\s*(?:-|–|to)\s*(\d+)\s*(days?|weeks?)', re.IGNORECASE)
_SINGLE_RE = re.compile(r'(\d+)\s*(days?|weeks?)', re.IGNORECASE)


def parse_horizon_from_window(repricing_window: str) -> ThesisHorizon:
    """
    Convert free-text repricing_window to structured ThesisHorizon enum.

    Uses upper bound of ranges (conservative: "1-3 days" → 3 days → THREE_DAY).
    Converts weeks to calendar days first, then classifies.

    Examples:
        "intraday" / "same day"     → INTRADAY
        "1 day"                     → ONE_DAY
        "1-3 days"                  → THREE_DAY  (upper bound = 3)
        "3-5 days"                  → FIVE_DAY   (upper bound = 5)
        "1-2 weeks"                 → SEVEN_TO_TEN_DAY (upper bound = 14 cal days)
        "2-4 weeks"                 → TWO_TO_FOUR_WEEK (upper bound = 28 cal days)
        "" / "soon" / unrecognized  → UNKNOWN
    """
    if not repricing_window:
        return ThesisHorizon.UNKNOWN

    text = repricing_window.strip().lower()

    # Special keywords
    if text in ("intraday", "same day", "same-day", "today"):
        return ThesisHorizon.INTRADAY

    # Try range pattern first: "1-3 days", "2 to 4 weeks", etc.
    m = _RANGE_RE.search(text)
    if m:
        upper = int(m.group(2))
        unit = m.group(3).lower()
        if unit.startswith("week"):
            cal_days = upper * 7
        else:
            cal_days = upper
        return _classify_calendar_days(cal_days)

    # Try single number: "5 days", "1 week", etc.
    m = _SINGLE_RE.search(text)
    if m:
        num = int(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("week"):
            cal_days = num * 7
        else:
            cal_days = num
        return _classify_calendar_days(cal_days)

    return ThesisHorizon.UNKNOWN


def _classify_calendar_days(cal_days: int) -> ThesisHorizon:
    """Classify calendar days into ThesisHorizon buckets."""
    if cal_days <= 0:
        return ThesisHorizon.UNKNOWN
    elif cal_days <= 1:
        return ThesisHorizon.ONE_DAY
    elif cal_days <= 3:
        return ThesisHorizon.THREE_DAY
    elif cal_days <= 5:
        return ThesisHorizon.FIVE_DAY
    elif cal_days <= 14:
        return ThesisHorizon.SEVEN_TO_TEN_DAY
    else:
        return ThesisHorizon.TWO_TO_FOUR_WEEK


# ─────────────────────────────────────────────────────────────────────
# Trading-day math
# ─────────────────────────────────────────────────────────────────────

def calendar_to_trading_days(calendar_days: int) -> int:
    """
    Convert calendar days to approximate trading days.

    Approximation: trading_days ≈ calendar_days * 5/7.
    Ignores market holidays — sufficient for horizon classification
    but not exact for backtesting.

    Always returns at least 1 for positive input.
    """
    if calendar_days <= 0:
        return 0
    return max(1, int(calendar_days * 5 / 7))


# ─────────────────────────────────────────────────────────────────────
# Timing quality assessment
# ─────────────────────────────────────────────────────────────────────

def compute_timing_quality(
    directional_return: float,
    mfe: float,
    trading_age: int,
    horizon_days: int,
    status: str,
    expected_horizon: str,
) -> str:
    """
    Assess timing quality of a thesis based on horizon-aware thresholds.

    Uses per-horizon thresholds from THRESHOLDS.horizon_auto_thresholds.

    Args:
        directional_return: Current directional return (positive = thesis direction)
        mfe: Max favorable excursion (best directional return seen)
        trading_age: Thesis age in trading days
        horizon_days: Expected horizon in trading days
        status: Thesis status string (e.g., "CLOSED_INVALIDATED")
        expected_horizon: ThesisHorizon value string (e.g., "7_10D")

    Returns:
        TimingQuality value string.
    """
    # Invalidated before playout
    if status == "CLOSED_INVALIDATED":
        return TimingQuality.INVALIDATED_BEFORE_PLAYOUT.value

    # Grace period: too early to judge
    grace = int(horizon_days * THRESHOLDS.horizon_grace_multiplier)
    if trading_age < grace:
        return TimingQuality.TOO_EARLY_TO_JUDGE.value

    # Get per-horizon thresholds
    thresholds = THRESHOLDS.horizon_auto_thresholds.get(
        expected_horizon,
        THRESHOLDS.horizon_auto_thresholds.get("UNKNOWN", (0.15, -0.10)),
    )
    win_pct, loss_pct = thresholds

    # Correct and timely: return hit win threshold within horizon
    if directional_return >= win_pct and trading_age <= horizon_days:
        return TimingQuality.CORRECT_AND_TIMELY.value

    # Correct but slow: return hit win threshold, but after horizon
    if directional_return >= win_pct and trading_age > horizon_days:
        return TimingQuality.CORRECT_BUT_SLOW.value

    # Correct thesis, poor timing: MFE hit win threshold but final return negative
    if mfe >= win_pct and directional_return < 0:
        return TimingQuality.CORRECT_THESIS_POOR_TIMING.value

    # Failed thesis: return breached loss threshold
    if directional_return <= loss_pct:
        return TimingQuality.FAILED_THESIS.value

    # Default: still too early to judge conclusively
    return TimingQuality.TOO_EARLY_TO_JUDGE.value


# ─────────────────────────────────────────────────────────────────────
# Catalyst integrity
# ─────────────────────────────────────────────────────────────────────

def is_catalyst_intact(thesis_status: str, catalyst_status: str = "") -> bool:
    """
    Conservative catalyst integrity check.

    Returns True by default. Only returns False when thesis status is
    explicitly CLOSED_INVALIDATED or catalyst_status is INVALIDATED/EXPIRED.
    """
    if thesis_status == "CLOSED_INVALIDATED":
        return False
    if catalyst_status.upper() in ("INVALIDATED", "EXPIRED"):
        return False
    return True
