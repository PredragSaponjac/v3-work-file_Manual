"""
ORCA v20 — Overnight Learning Engine (Operator Activation).

3-layer overnight processing with hard budget controls:

LAYER 1 (always runs, no/low model cost):
    - thesis momentum update
    - monitor rule checks
    - open-thesis status refresh
    - rules-based replay
    - false-negative detection
    - queue generation for deeper replay

LAYER 2 (runs while budget remains):
    - LLM replay summaries for top misses
    - LLM training example generation
    - clarification passes for ambiguous outcomes

LAYER 3 (premium escalation only):
    - biggest losers deep dive
    - biggest missed winners
    - highest-confidence wrong calls
    - most important open theses with rising stakes

Budget policy:
    - Hard overnight ceiling: $50 (configurable)
    - Warm-up period: 14 days, no per-thesis cap
    - On budget exhaustion: persist DEFERRED_BUDGET, resume next night
    - NEVER erase state, NEVER delete queued work
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from orca_v20.config import FLAGS, THRESHOLDS, PATHS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.overnight")


# ─────────────────────────────────────────────────────────────────────
# Replay job statuses
# ─────────────────────────────────────────────────────────────────────

class ReplayJobStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DEFERRED_BUDGET = "DEFERRED_BUDGET"
    SKIPPED_NO_CREDIT = "SKIPPED_NO_CREDIT"
    FAILED = "FAILED"


# ─────────────────────────────────────────────────────────────────────
# Budget tracker (per-night, persistent)
# ─────────────────────────────────────────────────────────────────────

class OvernightBudgetTracker:
    """
    Tracks overnight API spend with hard ceiling.

    Persists budget state to DB so interrupted runs can resume.
    All state preserved on budget exhaustion — only premium tasks deferred.
    """

    def __init__(self, hard_limit: float, soft_limit: float = 0.0):
        self.hard_limit = hard_limit
        self.soft_limit = soft_limit if soft_limit > 0 else hard_limit
        self.total_spent = 0.0
        self.cost_by_provider: Dict[str, float] = {}
        self.cost_by_role: Dict[str, float] = {}
        self.items_processed = 0
        self.items_deferred = 0
        self.items_escalated = 0
        self.night_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._exhausted = False

    def add_cost(self, amount: float, provider: str = "", role: str = "") -> None:
        self.total_spent += amount
        if provider:
            self.cost_by_provider[provider] = self.cost_by_provider.get(provider, 0) + amount
        if role:
            self.cost_by_role[role] = self.cost_by_role.get(role, 0) + amount
        if self.total_spent >= self.hard_limit:
            self._exhausted = True

    def is_exhausted(self) -> bool:
        return self._exhausted

    def budget_remaining(self) -> float:
        return max(0.0, self.hard_limit - self.total_spent)

    def can_afford(self, estimated_cost: float) -> bool:
        """Check if we can afford a task. Conservative: check against remaining."""
        return self.budget_remaining() >= estimated_cost

    def summary(self) -> Dict:
        return {
            "night_date": self.night_date,
            "hard_limit": self.hard_limit,
            "total_cost": round(self.total_spent, 4),
            "budget_remaining": round(self.budget_remaining(), 4),
            "cost_by_provider": {k: round(v, 4) for k, v in self.cost_by_provider.items()},
            "cost_by_role": {k: round(v, 4) for k, v in self.cost_by_role.items()},
            "items_processed": self.items_processed,
            "items_deferred": self.items_deferred,
            "items_escalated": self.items_escalated,
            "exhausted": self._exhausted,
        }


def _persist_budget_state(budget: OvernightBudgetTracker) -> None:
    """Save budget state to DB for resume capability."""
    try:
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO overnight_budget_log (
                night_date, total_spent, hard_limit,
                items_processed, items_deferred, items_escalated,
                cost_by_provider_json, cost_by_role_json,
                exhausted, updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            budget.night_date,
            budget.total_spent,
            budget.hard_limit,
            budget.items_processed,
            budget.items_deferred,
            budget.items_escalated,
            json.dumps(budget.cost_by_provider),
            json.dumps(budget.cost_by_role),
            int(budget.is_exhausted()),
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[budget] Failed to persist budget state: {e}")


# ─────────────────────────────────────────────────────────────────────
# Replay job queue (persistent)
# ─────────────────────────────────────────────────────────────────────

def _enqueue_replay_job(thesis_id: str, ticker: str, layer: int,
                         priority: int, reason: str) -> None:
    """Add a replay job to the queue."""
    try:
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO replay_job_queue (
                thesis_id, ticker, layer, priority, reason,
                status, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            thesis_id, ticker, layer, priority, reason,
            ReplayJobStatus.PENDING,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"[queue] Failed to enqueue {thesis_id}: {e}")


def _update_replay_job(thesis_id: str, status: str,
                        result_json: str = "", layer: int = None) -> None:
    """Update status of a replay job.

    FIX: Added layer filter. Without it, a thesis with both L2 and L3 jobs
    could have the wrong layer's job updated (whichever row comes first).
    """
    try:
        conn = get_connection()
        if layer is not None:
            conn.execute("""
                UPDATE replay_job_queue
                SET status = ?, result_json = ?, updated_utc = ?
                WHERE thesis_id = ? AND layer = ? AND status IN (?, ?)
            """, (
                status, result_json,
                datetime.now(timezone.utc).isoformat(),
                thesis_id, layer,
                ReplayJobStatus.PENDING, ReplayJobStatus.RUNNING,
            ))
        else:
            conn.execute("""
                UPDATE replay_job_queue
                SET status = ?, result_json = ?, updated_utc = ?
                WHERE thesis_id = ? AND status IN (?, ?)
            """, (
                status, result_json,
                datetime.now(timezone.utc).isoformat(),
                thesis_id,
                ReplayJobStatus.PENDING, ReplayJobStatus.RUNNING,
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"[queue] Failed to update {thesis_id}: {e}")


def _get_pending_jobs(layer: int, include_deferred: bool = True) -> List[Dict]:
    """Get pending jobs for a specific layer.

    FIX: Added include_deferred flag. The post-loop cleanup should only
    fetch PENDING (not DEFERRED_BUDGET) to avoid double-counting jobs
    that were already deferred inside the loop.
    """
    try:
        conn = get_connection()
        if include_deferred:
            rows = conn.execute("""
                SELECT thesis_id, ticker, layer, priority, reason
                FROM replay_job_queue
                WHERE (status = ? OR status = ?)
                AND layer = ?
                ORDER BY priority DESC, created_utc ASC
            """, (ReplayJobStatus.PENDING, ReplayJobStatus.DEFERRED_BUDGET, layer)).fetchall()
        else:
            rows = conn.execute("""
                SELECT thesis_id, ticker, layer, priority, reason
                FROM replay_job_queue
                WHERE status = ?
                AND layer = ?
                ORDER BY priority DESC, created_utc ASC
            """, (ReplayJobStatus.PENDING, layer)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"[queue] Failed to get pending L{layer} jobs: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# False-negative detection (Layer 1)
# ─────────────────────────────────────────────────────────────────────

def _detect_false_negatives(lookback_days: int = 7) -> List[Dict]:
    """
    Find rejected/WATCH ideas that later moved materially.
    These are false negatives — ideas we passed on that would have worked.
    """
    try:
        import yfinance as yf
        import warnings

        conn = get_connection()
        # Find theses that were rejected or stayed WATCH
        rows = conn.execute("""
            SELECT thesis_id, ticker, idea_direction, catalyst,
                   initial_confidence, created_utc, expected_horizon
            FROM theses
            WHERE status IN ('DRAFT', 'CLOSED_INVALIDATED', 'CLOSED_EXPIRED')
            AND created_utc >= datetime('now', ?)
            ORDER BY initial_confidence DESC
            LIMIT 20
        """, (f"-{lookback_days} days",)).fetchall()
        conn.close()

        from orca_v20.horizon import calendar_to_trading_days

        false_negatives = []
        for row in rows:
            r = dict(row)
            ticker = r["ticker"]
            start_date = r["created_utc"][:10]

            # Horizon-aware: skip if thesis hasn't had time to play out
            expected_horizon = r.get("expected_horizon") or "UNKNOWN"
            horizon_days = THRESHOLDS.horizon_days_map.get(expected_horizon, 10)
            try:
                from datetime import datetime as _dt
                created = _dt.fromisoformat(r["created_utc"].replace("Z", "+00:00"))
                cal_days = (_dt.now(timezone.utc) - created).days
                thesis_age_td = calendar_to_trading_days(cal_days)
            except Exception:
                thesis_age_td = lookback_days  # fallback: assume enough time

            if thesis_age_td < horizon_days:
                logger.debug(
                    f"[false_neg] {ticker}: too early to judge "
                    f"(age={thesis_age_td}td < horizon={horizon_days}td)"
                )
                continue

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    data = yf.download(ticker, start=start_date, progress=False)
                if data.empty:
                    continue
                close = data["Close"]
                if hasattr(close, "columns"):
                    close = close.iloc[:, 0]
                prices = close.dropna().tolist()
                if len(prices) < 2:
                    continue

                pct = (prices[-1] - prices[0]) / prices[0] * 100

                # Material move in the thesis direction = false negative
                direction = r["idea_direction"]
                is_false_neg = (
                    (direction == "BULLISH" and pct > 8)
                    or (direction == "BEARISH" and pct < -8)
                )

                if is_false_neg:
                    r["pct_move"] = round(pct, 2)
                    r["false_negative_type"] = "REJECTED_WINNER"
                    false_negatives.append(r)
                    logger.info(
                        f"[false_neg] {ticker}: rejected {direction} thesis "
                        f"moved {pct:.1f}% — FALSE NEGATIVE"
                    )
            except Exception:
                continue

        return false_negatives

    except Exception as e:
        logger.debug(f"[false_neg] Detection failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# Layer 1: Always run, no/low model cost
# ─────────────────────────────────────────────────────────────────────

def run_layer_1(ctx: RunContext, budget: OvernightBudgetTracker) -> Dict:
    """
    Layer 1: cheap deterministic processing.
    - thesis momentum update
    - monitor rule checks
    - auto-label active theses (BEFORE replay)
    - rules-based replay
    - false-negative detection
    - queue generation for deeper replay
    """
    logger.info("[overnight L1] Starting cheap/deterministic layer")
    results = {
        "momentum_updated": False,
        "rules_updated": False,
        "auto_labeled": 0,
        "replays_rules": 0,
        "false_negatives": 0,
        "queue_generated": 0,
    }

    # 0. FIX: finalize_outcomes MUST run before auto-label and replay.
    #    Without this, thesis_forward_outcomes table has stale/missing data,
    #    and replay's horizon-aware timing quality is computed on incomplete info.
    try:
        from orca_v20.thesis_store import finalize_outcomes
        fin_count = finalize_outcomes(ctx)
        results["finalized_outcomes"] = fin_count
        logger.info(f"[overnight L1] Finalized {fin_count} thesis outcomes")
    except Exception as e:
        logger.error(f"[overnight L1] finalize_outcomes failed: {e}")

    # 1. Auto-label active theses FIRST — closes theses so momentum captures
    #    the final pre-closure state, and closed theses enter the replay queue.
    try:
        from orca_v20.thesis_store import auto_label_active_theses
        labeled = auto_label_active_theses(ctx)
        results["auto_labeled"] = labeled
        logger.info(f"[overnight L1] Auto-labeled {labeled} theses")
    except Exception as e:
        logger.error(f"[overnight L1] Auto-label failed: {e}")

    # 2. Thesis momentum update (runs AFTER auto-label so it captures
    #    the final confidence state including any just-closed theses)
    try:
        from orca_v20.thesis_momentum import run_momentum_update
        run_momentum_update(ctx)
        results["momentum_updated"] = True
        logger.info("[overnight L1] Momentum update complete")
    except Exception as e:
        logger.error(f"[overnight L1] Momentum update failed: {e}")

    # 3. Monitor rule checks
    try:
        from orca_v20.daemon_rules import check_portfolio_health, update_rules
        update_rules(ctx)
        results["rules_updated"] = True
        logger.info("[overnight L1] Monitor rules updated")
    except Exception as e:
        logger.error(f"[overnight L1] Rule update failed: {e}")

    # 3b. Compute forward outcomes (multi-window tracking with MFE/MAE)
    try:
        from orca_v20.thesis_store import compute_forward_outcomes
        fwd_count = compute_forward_outcomes(ctx)
        results["forward_outcomes_computed"] = fwd_count
        logger.info(f"[overnight L1] Computed {fwd_count} forward outcome windows")
    except Exception as e:
        logger.error(f"[overnight L1] Forward outcomes failed: {e}")

    # 4. Rules-based replay on recently closed theses (after auto-label)
    try:
        from orca_v20.replay_engine import run_nightly_replay
        cost_before_replay = ctx.api_cost_usd
        replay_results = run_nightly_replay(ctx)
        results["replays_rules"] = len(replay_results)

        # Track L1 replay LLM cost in the budget tracker
        l1_replay_cost = ctx.api_cost_usd - cost_before_replay
        if l1_replay_cost > 0:
            budget.add_cost(l1_replay_cost, role="replay_l1")
            logger.info(f"[overnight L1] Replay LLM cost: ${l1_replay_cost:.4f}")

        # Queue significant misses for Layer 2/3
        for r in replay_results:
            if abs(r.get("confidence_delta", 0)) >= 3:
                _enqueue_replay_job(
                    r["thesis_id"], r["ticker"],
                    layer=2, priority=abs(r["confidence_delta"]),
                    reason=f"confidence_delta={r['confidence_delta']}"
                )
                results["queue_generated"] += 1

            # Biggest losers → Layer 3
            if r.get("realized_outcome") == "LOSS" and abs(r.get("confidence_delta", 0)) >= 4:
                _enqueue_replay_job(
                    r["thesis_id"], r["ticker"],
                    layer=3, priority=10,
                    reason="biggest_loser"
                )

    except Exception as e:
        logger.error(f"[overnight L1] Replay failed: {e}")

    # 5. False-negative detection
    try:
        false_negs = _detect_false_negatives(THRESHOLDS.overnight_false_negative_lookback)
        results["false_negatives"] = len(false_negs)

        for fn in false_negs:
            _enqueue_replay_job(
                fn["thesis_id"], fn["ticker"],
                layer=2, priority=5,
                reason=f"false_negative_{fn['pct_move']:.0f}pct"
            )
            results["queue_generated"] += 1

    except Exception as e:
        logger.error(f"[overnight L1] False-negative detection failed: {e}")

    budget.items_processed += results["replays_rules"]
    logger.info(f"[overnight L1] Complete: {results}")
    return results


# ─────────────────────────────────────────────────────────────────────
# Layer 2: LLM replay (runs while budget remains)
# ─────────────────────────────────────────────────────────────────────

LAYER_2_ESTIMATED_COST = 0.50  # ~$0.50 per LLM replay summary

def run_layer_2(ctx: RunContext, budget: OvernightBudgetTracker) -> Dict:
    """
    Layer 2: LLM-powered replay summaries for top misses.
    Stops when budget is exhausted. Defers remaining items.
    """
    logger.info("[overnight L2] Starting LLM replay layer")
    results = {"processed": 0, "deferred": 0, "training_examples": 0}

    jobs = _get_pending_jobs(layer=2)
    if not jobs:
        logger.info("[overnight L2] No pending L2 jobs")
        return results

    logger.info(f"[overnight L2] {len(jobs)} pending jobs")

    from orca_v20.replay_engine import replay_thesis

    for job in jobs:
        # Budget check
        if not budget.can_afford(LAYER_2_ESTIMATED_COST):
            logger.info(f"[overnight L2] Budget exhausted — deferring remaining {len(jobs) - results['processed']} jobs")
            _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
            results["deferred"] += 1
            budget.items_deferred += 1
            continue

        # Run premium replay
        _update_replay_job(job["thesis_id"], ReplayJobStatus.RUNNING, layer=job.get("layer"))

        try:
            conn = get_connection()
            thesis_row = conn.execute(
                "SELECT * FROM theses WHERE thesis_id = ?",
                (job["thesis_id"],)
            ).fetchone()
            conn.close()

            if thesis_row:
                thesis = dict(thesis_row)
                cost_before = ctx.api_cost_usd
                result = replay_thesis(thesis, ctx)
                cost_after = ctx.api_cost_usd
                cost_used = cost_after - cost_before

                budget.add_cost(cost_used, role="replay_analyst")
                budget.items_processed += 1
                results["processed"] += 1
                results["training_examples"] += result.get("training_examples_generated", 0)
                budget.items_escalated += 1

                _update_replay_job(job["thesis_id"], ReplayJobStatus.COMPLETED,
                                    json.dumps(result.get("counterfactual_verdict", "")),
                                    layer=job.get("layer"))

        except Exception as e:
            logger.error(f"[overnight L2] {job['ticker']} failed: {e}")
            _update_replay_job(job["thesis_id"], ReplayJobStatus.FAILED, layer=job.get("layer"))

    # Defer all remaining pending L2 jobs
    remaining = _get_pending_jobs(layer=2, include_deferred=False)
    for job in remaining:
        _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
        results["deferred"] += 1
        budget.items_deferred += 1

    logger.info(f"[overnight L2] Complete: {results}")
    return results


# ─────────────────────────────────────────────────────────────────────
# Layer 3: Premium escalation (biggest misses only)
# ─────────────────────────────────────────────────────────────────────

LAYER_3_ESTIMATED_COST = 1.50  # ~$1.50 per deep analysis

def run_layer_3(ctx: RunContext, budget: OvernightBudgetTracker) -> Dict:
    """
    Layer 3: Premium deep analysis for biggest misses.
    Uses expensive models (claude-opus). Strictly budget-gated.
    """
    logger.info("[overnight L3] Starting premium escalation layer")
    results = {"processed": 0, "deferred": 0}

    jobs = _get_pending_jobs(layer=3)
    if not jobs:
        logger.info("[overnight L3] No pending L3 jobs")
        return results

    logger.info(f"[overnight L3] {len(jobs)} pending jobs")

    for job in jobs:
        if not budget.can_afford(LAYER_3_ESTIMATED_COST):
            logger.info(f"[overnight L3] Budget exhausted — deferring {job['ticker']}")
            _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
            results["deferred"] += 1
            budget.items_deferred += 1
            continue

        _update_replay_job(job["thesis_id"], ReplayJobStatus.RUNNING, layer=job.get("layer"))

        try:
            # Premium analysis using the more expensive model
            from orca_v20.router import call_model

            conn = get_connection()
            thesis_row = conn.execute(
                "SELECT * FROM theses WHERE thesis_id = ?",
                (job["thesis_id"],)
            ).fetchone()
            conn.close()

            if not thesis_row:
                _update_replay_job(job["thesis_id"], ReplayJobStatus.FAILED, layer=job.get("layer"))
                continue

            thesis = dict(thesis_row)

            prompt = f"""Deep post-mortem analysis of a {'major loss' if job['reason'] == 'biggest_loser' else 'missed opportunity'}.

THESIS:
- Ticker: {thesis['ticker']}
- Direction: {thesis.get('idea_direction', 'UNKNOWN')}
- Catalyst: {thesis.get('catalyst', '')}
- Original confidence: {thesis.get('initial_confidence', 0)}
- Final status: {thesis.get('status', '')}
- Reason: {job['reason']}

Provide a DEEP analysis:
1. What systemic pattern does this represent?
2. What structural change to the pipeline would catch this next time?
3. What specific signals should be added to the evidence gate?
4. Rate the severity of this miss (1-10).

Format as JSON with keys: pattern, pipeline_change, new_signals, severity."""

            cost_before = ctx.api_cost_usd
            response = call_model(
                role="review_pass",  # Uses claude-opus for premium analysis
                system_prompt="You are a senior portfolio risk analyst conducting post-mortem on trading thesis failures.",
                user_prompt=prompt,
                ctx=ctx,
                temperature=0.3,
                max_tokens=2000,
            )
            cost_after = ctx.api_cost_usd
            budget.add_cost(cost_after - cost_before, provider="anthropic", role="premium_replay")
            budget.items_processed += 1
            budget.items_escalated += 1
            results["processed"] += 1

            _update_replay_job(job["thesis_id"], ReplayJobStatus.COMPLETED,
                                response.content[:500])

        except Exception as e:
            logger.error(f"[overnight L3] {job['ticker']} failed: {e}")
            _update_replay_job(job["thesis_id"], ReplayJobStatus.FAILED, layer=job.get("layer"))

    # Defer remaining
    remaining = _get_pending_jobs(layer=3, include_deferred=False)
    for job in remaining:
        _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
        results["deferred"] += 1
        budget.items_deferred += 1

    logger.info(f"[overnight L3] Complete: {results}")
    return results


# ─────────────────────────────────────────────────────────────────────
# Public API — orchestrated overnight run
# ─────────────────────────────────────────────────────────────────────

def run_overnight(ctx: RunContext, deep_review: bool = False) -> Dict:
    """
    Execute the full overnight learning cycle.

    3 layers, budget-gated. Preserves all state on exhaustion.
    Returns comprehensive summary dict.
    """
    logger.info("=" * 60)
    logger.info(f"ORCA v20 Overnight Learning — {ctx.market_date}")
    logger.info(f"  Budget: ${THRESHOLDS.overnight_hard_budget_usd:.2f} hard ceiling")
    logger.info(f"  Deep review: {deep_review}")
    logger.info("=" * 60)

    budget = OvernightBudgetTracker(
        hard_limit=THRESHOLDS.overnight_hard_budget_usd,
        soft_limit=THRESHOLDS.overnight_soft_budget_usd,
    )

    # Layer 1: always runs (cheap)
    l1_results = run_layer_1(ctx, budget)
    _persist_budget_state(budget)

    # Layer 2: LLM replay (budget-gated)
    l2_results = {"processed": 0, "deferred": 0, "training_examples": 0}
    if not budget.is_exhausted():
        l2_results = run_layer_2(ctx, budget)
        _persist_budget_state(budget)
    else:
        # Defer ALL L2 jobs
        jobs = _get_pending_jobs(layer=2)
        for job in jobs:
            _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
            l2_results["deferred"] += 1

    # Layer 3: premium escalation (budget-gated, deep_review expands scope)
    l3_results = {"processed": 0, "deferred": 0}
    if not budget.is_exhausted():
        l3_results = run_layer_3(ctx, budget)
        _persist_budget_state(budget)
    else:
        jobs = _get_pending_jobs(layer=3)
        for job in jobs:
            _update_replay_job(job["thesis_id"], ReplayJobStatus.DEFERRED_BUDGET, layer=job.get("layer"))
            l3_results["deferred"] += 1

    # Final budget persist
    _persist_budget_state(budget)

    summary = {
        "overnight_date": ctx.market_date,
        "layer_1": l1_results,
        "layer_2": l2_results,
        "layer_3": l3_results,
        "budget": budget.summary(),
        "state_preserved": True,  # explicit guarantee
    }

    # Publish replay summary to Telegram
    try:
        from orca_v20.publisher import publish_replay_summary
        # Gather all replay results for the summary message
        replay_results_for_msg = []
        if l1_results.get("replays_rules", 0) > 0:
            replay_results_for_msg.append({"replay_mode": "RULES_ONLY", "count": l1_results["replays_rules"]})
        if l2_results.get("processed", 0) > 0:
            replay_results_for_msg.append({"replay_mode": "PREMIUM_ESCALATED", "count": l2_results["processed"]})
        if l2_results.get("deferred", 0) + l3_results.get("deferred", 0) > 0:
            replay_results_for_msg.append({
                "replay_mode": "DEFERRED_BUDGET",
                "count": l2_results["deferred"] + l3_results["deferred"],
            })
        publish_replay_summary(replay_results_for_msg, budget.summary(), ctx)
    except Exception as e:
        logger.warning(f"[overnight] Failed to publish replay summary: {e}")

    logger.info("=" * 60)
    logger.info(f"Overnight complete — cost: ${budget.total_spent:.2f} / ${budget.hard_limit:.2f}")
    logger.info(f"  L1: {l1_results}")
    logger.info(f"  L2: {l2_results}")
    logger.info(f"  L3: {l3_results}")
    logger.info(f"  State preserved: YES")
    logger.info("=" * 60)

    return summary
