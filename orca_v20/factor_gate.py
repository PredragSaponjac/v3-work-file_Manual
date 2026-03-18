"""
ORCA v20 — Factor Gate (Phase 4 hardened).

Single-factor CAPM proxy residualization: ensures the thesis has alpha
beyond market beta exposure.

Method: single_factor_capm_proxy (explicitly labeled — NOT full FF5).
Full Fama-French 5-factor residualization requires Ken French data library
and is deferred to Phase 5.

Gate statuses:
    PASS             — positive alpha, not pure beta play
    PASS_LOW_CONFIDENCE — data available but short history or borderline
    UNPROVEN         — return data unavailable
    FAIL             — negative alpha or pure beta play
"""

import logging
import math
from typing import Dict, List, Optional, Tuple

from orca_v20.config import FLAGS
from orca_v20.run_context import RunContext
from orca_v20.schemas import GateStatus, IdeaCandidate

logger = logging.getLogger("orca_v20.factor_gate")


# ─────────────────────────────────────────────────────────────────────
# Lightweight OLS (no numpy/scipy dependency)
# ─────────────────────────────────────────────────────────────────────

def _ols_alpha_beta(ys: List[float], xs: List[float]) -> Dict:
    """
    Simple OLS: y = alpha + beta * x + epsilon.
    Returns {alpha, beta, r_squared, residual_std, n}.
    """
    n = len(ys)
    if n < 10:
        return {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0, "residual_std": 0.0, "n": n}

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    cov_xy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / n
    var_x = sum((x - mean_x) ** 2 for x in xs) / n

    if abs(var_x) < 1e-12:
        return {"alpha": mean_y, "beta": 0.0, "r_squared": 0.0, "residual_std": 0.0, "n": n}

    beta = cov_xy / var_x
    alpha = mean_y - beta * mean_x

    residuals = [y - (alpha + beta * x) for x, y in zip(xs, ys)]
    ss_res = sum(r * r for r in residuals)
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    r_squared = 1.0 - (ss_res / ss_tot) if abs(ss_tot) > 1e-12 else 0.0
    residual_std = math.sqrt(ss_res / max(n - 2, 1))

    return {
        "alpha": round(alpha, 6),
        "beta": round(beta, 4),
        "r_squared": round(max(0, r_squared), 4),
        "residual_std": round(residual_std, 6),
        "n": n,
    }


def _fetch_returns(ticker: str, benchmark: str = "SPY", period: str = "6mo") -> Optional[Dict]:
    """
    Fetch daily returns for ticker and benchmark.
    Downloads separately to avoid MultiIndex column issues.
    """
    try:
        import yfinance as yf
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ticker_data = yf.download(ticker, period=period, progress=False)
            bench_data = yf.download(benchmark, period=period, progress=False)

        if ticker_data.empty or bench_data.empty:
            return None

        # Get close — handle MultiIndex
        def _get_close(df, sym):
            if "Close" in df.columns:
                col = df["Close"]
                if hasattr(col, "columns"):
                    return col[sym] if sym in col.columns else col.iloc[:, 0]
                return col
            return None

        ticker_close = _get_close(ticker_data, ticker)
        bench_close = _get_close(bench_data, benchmark)

        if ticker_close is None or bench_close is None:
            return None

        ticker_ret = ticker_close.pct_change(fill_method=None).dropna()
        bench_ret = bench_close.pct_change(fill_method=None).dropna()

        common_idx = ticker_ret.index.intersection(bench_ret.index)
        if len(common_idx) < 20:
            return None

        return {
            "ticker_returns": [float(ticker_ret.loc[i]) for i in common_idx],
            "benchmark_returns": [float(bench_ret.loc[i]) for i in common_idx],
            "n_days": len(common_idx),
        }
    except Exception as e:
        logger.debug(f"  [{ticker}] Returns fetch failed: {e}")
        return None


def evaluate(idea: IdeaCandidate, ctx: RunContext) -> Tuple[bool, Dict]:
    """
    Run factor residualization on an idea.

    Method: single_factor_capm_proxy (explicitly labeled).
    1. Regress ticker returns on SPY (single-factor CAPM proxy)
    2. Check if alpha (residual) is positive and meaningful
    3. High R² with no alpha = pure beta play (weak thesis)

    Returns (passed: bool, details_dict).
    """
    if not FLAGS.enable_factor_gate:
        return True, {"gate_status": "DISABLED", "reason": "factor_gate disabled"}

    details = {
        "ticker": idea.ticker,
        "method": "single_factor_capm_proxy",
        "factor_model": "CAPM (SPY benchmark)",
        "full_ff5_available": False,
        "alpha": None,
        "beta": None,
        "r_squared": None,
        "residual_std": None,
        "annual_alpha": None,
        "gate_status": GateStatus.UNPROVEN.value,
        "gate_passed": True,
        "failure_reasons": [],
        "reason_codes": [],
    }

    returns_data = _fetch_returns(idea.ticker)

    if returns_data is None:
        details["gate_status"] = GateStatus.UNPROVEN.value
        details["reason_codes"] = ["RETURN_DATA_UNAVAILABLE"]
        logger.info(f"  [{idea.ticker}] Factor gate: UNPROVEN — data unavailable")
        return True, details

    # Run single-factor regression (CAPM proxy)
    reg = _ols_alpha_beta(
        returns_data["ticker_returns"],
        returns_data["benchmark_returns"],
    )

    details.update({
        "alpha": reg["alpha"],
        "beta": reg["beta"],
        "r_squared": reg["r_squared"],
        "residual_std": reg["residual_std"],
        "n_observations": reg["n"],
    })

    failures = []
    reason_codes = []

    # Check 1: Alpha should not be significantly negative
    annual_alpha = reg["alpha"] * 252
    details["annual_alpha"] = round(annual_alpha, 4)

    if annual_alpha < -0.10:  # worse than -10% annualized alpha
        failures.append(f"negative_alpha: {annual_alpha:.2%} annualized")
        reason_codes.append("NEGATIVE_ALPHA")

    # Check 2: If R² is very high (>0.90), this is pure beta — weak thesis
    if reg["r_squared"] > 0.90 and abs(annual_alpha) < 0.05:
        failures.append(
            f"pure_beta_play: R²={reg['r_squared']:.3f}, alpha={annual_alpha:.2%}"
        )
        reason_codes.append("PURE_BETA_PLAY")

    # Check 3: Beta alignment warnings (informational, not blocking)
    if idea.idea_direction.value == "BEARISH" and reg["beta"] > 1.5:
        details["beta_warning"] = "high_beta_bearish"
        reason_codes.append("HIGH_BETA_BEARISH_WARNING")
    elif idea.idea_direction.value == "BULLISH" and reg["beta"] < 0.3:
        details["beta_warning"] = "low_beta_bullish"
        reason_codes.append("LOW_BETA_BULLISH_WARNING")

    # Determine gate status
    if failures:
        gate_status = GateStatus.FAIL
        passed = False
    elif reg["n"] < 30:
        gate_status = GateStatus.PASS_LOW_CONFIDENCE
        passed = True
        reason_codes.append("SHORT_HISTORY")
    elif annual_alpha > 0.05:
        gate_status = GateStatus.PASS
        passed = True
        reason_codes.append("POSITIVE_ALPHA")
    else:
        gate_status = GateStatus.PASS_LOW_CONFIDENCE
        passed = True
        reason_codes.append("MARGINAL_ALPHA")

    details["gate_status"] = gate_status.value
    details["gate_passed"] = passed
    details["failure_reasons"] = failures
    details["reason_codes"] = reason_codes

    logger.info(
        f"  [{idea.ticker}] Factor gate: {gate_status.value} "
        f"(alpha={annual_alpha:.2%}, beta={reg['beta']:.2f}, "
        f"R²={reg['r_squared']:.3f}, method=single_factor_capm_proxy, "
        f"reasons={reason_codes})"
    )

    return passed, details
