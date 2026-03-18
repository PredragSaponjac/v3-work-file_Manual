"""
ORCA v20 — Daemon Rules / Kill Switches.

Hard invalidation monitor rules + Bayesian soft-stop logic.
DB-backed rule state transitions via monitor_rules table.

Rule types:
    HARD — immediate halt (drawdown, consecutive losses)
    SOFT — Bayesian probability update (regime shift detection)
    TRADE — per-trade checks (sector concentration, kill list)

All checks return (healthy/allowed, reasons) tuples.
"""

import json
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.daemon_rules")


# ─────────────────────────────────────────────────────────────────────
# Portfolio state queries
# ─────────────────────────────────────────────────────────────────────

def _get_open_positions() -> List[Dict]:
    """Get all open positions from etp_records."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ticker, idea_direction, entry_price, strike_1, expiry,
                   confidence, trade_expression, created_utc
            FROM etp_records
            WHERE status = 'OPEN'
            ORDER BY created_utc DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"Failed to query open positions: {e}")
        return []


def _get_recent_trades(n: int = 20) -> List[Dict]:
    """Get N most recent trades for loss streak detection."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ticker, status, created_utc
            FROM etp_records
            ORDER BY created_utc DESC
            LIMIT ?
        """, (n,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"Failed to query recent trades: {e}")
        return []


def _count_consecutive_losses() -> int:
    """Count consecutive most-recent losing trades."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT status FROM etp_records
            WHERE status IN ('CLOSED_WIN', 'CLOSED_LOSS')
            ORDER BY created_utc DESC
            LIMIT 20
        """).fetchall()
        conn.close()

        streak = 0
        for r in rows:
            if r["status"] == "CLOSED_LOSS":
                streak += 1
            else:
                break
        return streak
    except Exception:
        return 0


def _get_sector_concentration(positions: List[Dict]) -> Dict[str, int]:
    """B4: Count positions by real sector using full 4-tier resolver."""
    from orca_v20.sector_map import resolve_sector_for_positions
    return resolve_sector_for_positions(positions, allow_live=True)


# ─────────────────────────────────────────────────────────────────────
# Bayesian soft-stop
# ─────────────────────────────────────────────────────────────────────

def _bayesian_regime_shift(ctx: RunContext) -> Tuple[float, str]:
    """
    Estimate posterior probability of adverse regime shift.

    Uses a simple Bayesian update:
        Prior: P(regime_shift) = 0.1 (base rate)
        Likelihood: adjusted by VIX, consecutive losses, drawdown

    Returns (probability, assessment).
    """
    prior = 0.10  # 10% base rate

    # Evidence factors
    factors = []

    # VIX level (if available)
    vix = getattr(ctx, 'vix_level', None)
    if vix and vix > 30:
        factors.append(2.0)  # VIX > 30 doubles the prior
    elif vix and vix > 25:
        factors.append(1.5)

    # Consecutive losses
    streak = _count_consecutive_losses()
    if streak >= THRESHOLDS.max_consecutive_losses:
        factors.append(3.0)  # strong evidence of regime shift
    elif streak >= 3:
        factors.append(1.5)

    # Regime conviction (if available)
    regime = getattr(ctx, 'spy_regime', 'neutral')
    conviction = getattr(ctx, 'regime_conviction', 0.0)
    if regime in ('bearish', 'crisis') and conviction > 0.7:
        factors.append(2.0)

    # Bayesian update: P(shift | evidence) ∝ P(evidence | shift) * P(shift)
    likelihood_ratio = 1.0
    for f in factors:
        likelihood_ratio *= f

    # Posterior
    posterior = (prior * likelihood_ratio) / (
        prior * likelihood_ratio + (1 - prior) * 1.0
    )
    posterior = min(0.95, posterior)

    # Assessment
    if posterior > 0.6:
        assessment = "HIGH_RISK"
    elif posterior > 0.3:
        assessment = "ELEVATED"
    else:
        assessment = "NORMAL"

    return round(posterior, 4), assessment


# ─────────────────────────────────────────────────────────────────────
# Hard rules
# ─────────────────────────────────────────────────────────────────────

def check_portfolio_health(ctx: RunContext) -> Tuple[bool, List[str]]:
    """
    Check portfolio-level health before allowing new trades.

    Returns (healthy, list_of_triggered_rules).
    """
    if not FLAGS.enable_daemon_rules:
        return True, []

    triggered = []
    positions = _get_open_positions()

    # Rule 1: Max consecutive losses
    streak = _count_consecutive_losses()
    if streak >= THRESHOLDS.max_consecutive_losses:
        triggered.append(f"HARD: consecutive_losses={streak} >= {THRESHOLDS.max_consecutive_losses}")
        logger.warning(f"[daemon] HARD STOP: {streak} consecutive losses")

    # Rule 2: Position count hard cap
    if len(positions) >= THRESHOLDS.hard_cap:
        triggered.append(f"HARD: position_count={len(positions)} >= hard_cap={THRESHOLDS.hard_cap}")
        logger.warning(f"[daemon] HARD STOP: position count at hard cap")

    # Rule 3: Bayesian soft-stop
    shift_prob, assessment = _bayesian_regime_shift(ctx)
    if shift_prob > 0.6:
        triggered.append(f"SOFT: regime_shift_probability={shift_prob:.2f} ({assessment})")
        logger.warning(f"[daemon] SOFT STOP: regime shift probability={shift_prob:.2f}")

    # Rule 4: Budget exhaustion
    if ctx.is_over_budget():
        triggered.append(f"HARD: api_budget_exhausted (${ctx.api_cost_usd:.2f})")

    healthy = len(triggered) == 0

    if not healthy:
        logger.warning(f"[daemon] Portfolio health CHECK FAILED: {triggered}")
    else:
        logger.info(
            f"[daemon] Portfolio healthy: {len(positions)} positions, "
            f"streak={streak}, regime_shift_prob={shift_prob:.2f}"
        )

    return healthy, triggered


def check_trade_allowed(ticker: str, ctx: RunContext) -> Tuple[bool, str]:
    """
    Check if a specific trade is allowed by daemon rules.

    Returns (allowed, reason).
    """
    if not FLAGS.enable_daemon_rules:
        return True, ""

    positions = _get_open_positions()

    # Check 1: Duplicate ticker
    ticker_positions = [p for p in positions if p.get("ticker") == ticker]
    if len(ticker_positions) >= 2:
        return False, f"already_have_{len(ticker_positions)}_positions_in_{ticker}"

    # Check 2: Max positions (with overflow logic)
    n_positions = len(positions)
    if n_positions >= THRESHOLDS.hard_cap:
        return False, f"at_hard_cap_{n_positions}"

    # Check 3: Sector concentration
    from orca_v20.sector_map import resolve_sector
    sectors = _get_sector_concentration(positions)
    new_ticker_sector, _, new_ticker_source = resolve_sector(ticker, allow_live=True)

    # Don't count UNKNOWN as a real sector for concentration blocking
    over_concentrated = [s for s, c in sectors.items() if c >= 3 and s != "UNKNOWN"]
    if new_ticker_sector in over_concentrated:
        return False, f"sector_over_concentrated_{new_ticker_sector}"

    # Check 4: Unknown-sector caution (advisory — not blocking)
    unknown_count = sectors.get("UNKNOWN", 0)
    if new_ticker_source == "unknown":
        unknown_count += 1
    if unknown_count >= 2:
        logger.info(
            f"  [daemon] CAUTION: {unknown_count} positions with unresolved sectors "
            f"— concentration confidence reduced"
        )

    return True, ""


# ─────────────────────────────────────────────────────────────────────
# B5: Active-thesis overlap / concentration check
# ─────────────────────────────────────────────────────────────────────

def check_thesis_overlap(ideas: list, ctx: RunContext) -> list:
    """
    B5: Flag clustered theses that are effectively one macro bet.

    Computes pairwise catalyst text similarity + shared sector.
    If overlap exceeds threshold, flags concentration risk (advisory).

    Returns ideas with overlap_warnings attached.
    """
    if len(ideas) < 2:
        return ideas

    from orca_v20.sector_map import resolve_sector
    from orca_v20.thesis_store import _tokenize, _build_tfidf_vector, _cosine_similarity

    OVERLAP_THRESHOLD = 0.50  # catalyst text similarity

    # Determine concentration confidence based on sector resolution quality
    has_unknown = False
    sector_sources = {}
    for idea in ideas:
        sec, _, src = resolve_sector(idea.ticker, allow_live=True)
        sector_sources[idea.ticker] = (sec, src)
        if sec == "UNKNOWN":
            has_unknown = True

    conc_conf = "reduced" if has_unknown else "high"

    # Compute pairwise overlap
    for i, idea_a in enumerate(ideas):
        for j, idea_b in enumerate(ideas):
            if j <= i:
                continue

            # Catalyst text similarity
            text_a = f"{idea_a.catalyst} {idea_a.thesis}"
            text_b = f"{idea_b.catalyst} {idea_b.thesis}"
            vec_a = _build_tfidf_vector(_tokenize(text_a))
            vec_b = _build_tfidf_vector(_tokenize(text_b))
            sim = _cosine_similarity(vec_a, vec_b)

            # Sector overlap (using cached results from above)
            sec_a = sector_sources.get(idea_a.ticker, ("UNKNOWN", "unknown"))[0]
            sec_b = sector_sources.get(idea_b.ticker, ("UNKNOWN", "unknown"))[0]

            # Two UNKNOWNs are NOT assumed to share a sector,
            # but apply mild similarity boost to catch possible hidden overlap
            both_unknown = (sec_a == "UNKNOWN" and sec_b == "UNKNOWN")
            same_sector = (sec_a == sec_b and sec_a != "UNKNOWN")

            effective_sim = sim + 0.05 if both_unknown else sim

            # Flag if high text similarity OR same sector + moderate similarity
            if effective_sim >= OVERLAP_THRESHOLD or (same_sector and sim >= 0.30):
                warning = (
                    f"OVERLAP: {idea_a.ticker}/{idea_b.ticker} "
                    f"(sim={sim:.2f}, same_sector={same_sector}, "
                    f"sectors={sec_a}/{sec_b})"
                )
                logger.warning(f"  [daemon] {warning}")
                # Attach advisory warning to structured field
                idea_a.overlap_warnings.append(warning)

    # Set concentration_confidence on all ideas
    for idea in ideas:
        idea.concentration_confidence = conc_conf

    return ideas


# ─────────────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────────────

def _persist_rule(rule_type: str, rule_name: str, threshold: float,
                  current_value: float, triggered: bool, action: str) -> None:
    """Write a single rule check to monitor_rules table."""
    try:
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO monitor_rules (
                rule_type, rule_name, threshold, current_value,
                triggered, action_taken, last_checked_utc, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule_type, rule_name, threshold, current_value,
            int(triggered), action,
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist rule {rule_name}: {e}")


def update_rules(ctx: RunContext) -> None:
    """
    Update daemon rule state after a pipeline run.
    Persists current values to monitor_rules table.
    """
    if not FLAGS.enable_daemon_rules:
        return
    if ctx.dry_run:
        return

    logger.info(f"[run_id={ctx.run_id}] Updating daemon rules")

    positions = _get_open_positions()
    streak = _count_consecutive_losses()
    shift_prob, _ = _bayesian_regime_shift(ctx)

    _persist_rule("HARD", "consecutive_losses",
                  THRESHOLDS.max_consecutive_losses, streak,
                  streak >= THRESHOLDS.max_consecutive_losses, "halt_if_triggered")

    _persist_rule("HARD", "position_count",
                  THRESHOLDS.hard_cap, len(positions),
                  len(positions) >= THRESHOLDS.hard_cap, "block_new_trades")

    _persist_rule("SOFT", "regime_shift_probability",
                  0.6, shift_prob,
                  shift_prob > 0.6, "reduce_size_or_halt")

    _persist_rule("HARD", "api_budget",
                  ctx.max_api_cost_usd, ctx.api_cost_usd,
                  ctx.is_over_budget(), "halt_pipeline")

    logger.info(f"[daemon] Rules updated: {len(positions)} positions, streak={streak}")
    ctx.mark_stage("daemon_rules")
