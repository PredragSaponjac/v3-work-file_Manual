"""
ORCA v20 — Quantitative Gate (Phase 4 hardened).

Checks:
    1. Correlation to SPY — reject if too correlated (pure beta, no alpha)
    2. Historical analog win rate — from memory_store
    3. Minimum analog count — need enough historical precedent

Gate statuses:
    PASS             — correlation low + analogs healthy
    PASS_LOW_CONFIDENCE — correlation unavailable or analogs insufficient
    UNPROVEN         — no data to evaluate (both corr + analogs missing)
    FAIL             — correlation too high or analog win rate too low

Writes results to quant_proof_records table.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import GateStatus, IdeaCandidate

logger = logging.getLogger("orca_v20.quant_gate")


def _compute_spy_correlation(ticker: str) -> Optional[float]:
    """
    Compute 60-day rolling correlation of ticker returns to SPY.
    Uses yfinance for data (graceful fallback).
    """
    try:
        import yfinance as yf
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # Download separately to avoid MultiIndex column issues
            ticker_data = yf.download(ticker, period="3mo", progress=False)
            spy_data = yf.download("SPY", period="3mo", progress=False)

        if ticker_data.empty or spy_data.empty or len(ticker_data) < 20 or len(spy_data) < 20:
            return None

        # Get close prices — handle both flat and MultiIndex columns
        def _get_close(df, sym):
            if "Close" in df.columns:
                col = df["Close"]
                if hasattr(col, "columns"):  # MultiIndex
                    return col[sym] if sym in col.columns else col.iloc[:, 0]
                return col
            return None

        ticker_close = _get_close(ticker_data, ticker)
        spy_close = _get_close(spy_data, "SPY")

        if ticker_close is None or spy_close is None:
            return None

        # Compute returns
        ticker_ret = ticker_close.pct_change(fill_method=None).dropna()
        spy_ret = spy_close.pct_change(fill_method=None).dropna()

        # Align dates
        common_idx = ticker_ret.index.intersection(spy_ret.index)
        if len(common_idx) < 20:
            return None

        t_vals = [float(ticker_ret.loc[i]) for i in common_idx]
        s_vals = [float(spy_ret.loc[i]) for i in common_idx]

        n = len(t_vals)
        mean_t = sum(t_vals) / n
        mean_s = sum(s_vals) / n

        cov = sum((t - mean_t) * (s - mean_s) for t, s in zip(t_vals, s_vals)) / (n - 1)
        std_t = (sum((t - mean_t) ** 2 for t in t_vals) / (n - 1)) ** 0.5
        std_s = (sum((s - mean_s) ** 2 for s in s_vals) / (n - 1)) ** 0.5

        if std_t == 0 or std_s == 0:
            return 0.0

        return round(cov / (std_t * std_s), 4)

    except Exception as e:
        logger.debug(f"  [{ticker}] SPY correlation failed: {e}")
        return None


def evaluate(idea: IdeaCandidate, ctx: RunContext) -> Tuple[bool, Dict]:
    """
    Run quantitative checks on an idea.

    Returns (passed: bool, details_dict).
    passed=True when gate_status is PASS or PASS_LOW_CONFIDENCE or UNPROVEN.
    passed=False only when gate_status is FAIL.
    """
    if not FLAGS.enable_quant_gate:
        return True, {"gate_status": "DISABLED", "reason": "quant_gate disabled"}

    details = {
        "ticker": idea.ticker,
        "correlation_to_spy": None,
        "analog_count": 0,
        "analog_win_rate": 0.0,
        "gate_status": GateStatus.UNPROVEN.value,
        "gate_passed": True,
        "failure_reasons": [],
        "reason_codes": [],
    }

    failures = []
    evidence_present = False

    # 1. SPY correlation check
    corr = _compute_spy_correlation(idea.ticker)
    details["correlation_to_spy"] = corr

    if corr is not None:
        evidence_present = True
        if abs(corr) > THRESHOLDS.max_correlation_to_spy:
            failures.append(
                f"correlation_too_high: |{corr:.3f}| > {THRESHOLDS.max_correlation_to_spy}"
            )
            details["reason_codes"].append("HIGH_SPY_CORRELATION")
            logger.info(f"  [{idea.ticker}] Quant gate: SPY corr={corr:.3f} (too high)")
        else:
            details["reason_codes"].append("SPY_CORRELATION_OK")
    else:
        details["reason_codes"].append("SPY_CORRELATION_UNAVAILABLE")

    # 2. Analog win rate from memory store
    try:
        from orca_v20.memory_store import get_analog_stats
        stats = get_analog_stats(
            ticker=idea.ticker,
            catalyst_type=idea.catalyst[:50] if idea.catalyst else "",
            setup_summary=idea.thesis[:200] if idea.thesis else "",
        )
        details["analog_count"] = stats["count"]
        details["analog_win_rate"] = stats["win_rate"]

        if stats["count"] >= THRESHOLDS.min_analog_count:
            evidence_present = True
            if stats["win_rate"] < THRESHOLDS.min_analog_win_rate:
                failures.append(
                    f"low_analog_win_rate: {stats['win_rate']:.2f} < "
                    f"{THRESHOLDS.min_analog_win_rate}"
                )
                details["reason_codes"].append("LOW_ANALOG_WIN_RATE")
            else:
                details["reason_codes"].append("ANALOG_WIN_RATE_OK")
        else:
            details["reason_codes"].append("INSUFFICIENT_ANALOGS")
    except Exception as e:
        logger.debug(f"  [{idea.ticker}] Analog retrieval failed: {e}")
        details["reason_codes"].append("ANALOG_RETRIEVAL_FAILED")

    # Determine gate status
    if failures:
        gate_status = GateStatus.FAIL
        passed = False
    elif not evidence_present:
        gate_status = GateStatus.UNPROVEN
        passed = True  # soft pass — don't block on missing data
    elif corr is None or details["analog_count"] < THRESHOLDS.min_analog_count:
        gate_status = GateStatus.PASS_LOW_CONFIDENCE
        passed = True
    else:
        gate_status = GateStatus.PASS
        passed = True

    details["gate_status"] = gate_status.value
    details["gate_passed"] = passed
    details["failure_reasons"] = failures

    logger.info(
        f"  [{idea.ticker}] Quant gate: {gate_status.value} "
        f"(corr={corr}, analogs={details['analog_count']}, "
        f"reasons={details['reason_codes']})"
    )

    # Persist
    _persist_quant_record(idea, details, ctx)

    return passed, details


def _persist_quant_record(idea: IdeaCandidate, details: Dict, ctx: RunContext) -> None:
    """Write quant proof record to DB."""
    if ctx.dry_run:
        return
    try:
        conn = get_connection()
        conn.execute("""
            INSERT INTO quant_proof_records (
                run_id, thesis_id, ticker,
                analog_count, analog_win_rate, correlation_to_spy,
                factor_residual, causal_p_value,
                gate_passed, gate_reason, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ctx.run_id,
            idea.thesis_id or "",
            idea.ticker,
            details.get("analog_count", 0),
            details.get("analog_win_rate", 0.0),
            details.get("correlation_to_spy"),
            None,  # populated by factor_gate
            None,  # populated by causal_gate
            int(details.get("gate_passed", True)),
            f"{details.get('gate_status', 'UNKNOWN')}: {'; '.join(details.get('reason_codes', []))}",
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist quant record for {idea.ticker}: {e}")
