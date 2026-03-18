"""
ORCA v20 — Nightly Counterfactual Replay Engine (Phase 5).

Two-layer replay:
    Layer 1: RULES_ONLY — deterministic analysis using DB traces, price data,
             gate outputs, and heuristic scoring. Cheap, runs on every closed thesis.
    Layer 2: PREMIUM_ESCALATED — LLM-powered deep analysis for highest-value
             misses only. Optional, expensive, triggered when rules-only replay
             detects significant miss (confidence_delta >= 3).

Outputs:
    - replay_runs table records
    - training_examples table records (for future fine-tuning)
    - memory_cases updates (for analog retrieval)

Prerequisites: finalize_outcomes() must have run first (Phase 4).
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.replay_engine")


# ─────────────────────────────────────────────────────────────────────
# DB queries
# ─────────────────────────────────────────────────────────────────────

def _get_closed_theses(lookback_days: int = 14) -> List[Dict]:
    """Query theses closed in the last N days that haven't been replayed yet."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT t.thesis_id, t.ticker, t.catalyst, t.thesis_text,
                   t.idea_direction, t.status, t.invalidated_reason,
                   t.created_run_id, t.created_utc, t.last_updated_utc,
                   t.initial_confidence, t.current_confidence,
                   t.times_seen, t.invalidation_trigger,
                   t.expected_horizon  -- FIX: was missing; replay_thesis needs it for horizon-aware timing
            FROM theses t
            WHERE t.status LIKE 'CLOSED_%'
            AND t.last_updated_utc >= datetime('now', ?)
            AND t.thesis_id NOT IN (
                SELECT thesis_id FROM replay_runs
            )
            ORDER BY t.last_updated_utc DESC
        """, (f"-{lookback_days} days",)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"Failed to query closed theses: {e}")
        return []


def _get_trade_records(thesis_id: str) -> List[Dict]:
    """Get all trade records linked to a thesis."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT * FROM etp_records WHERE thesis_id = ?
        """, (thesis_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"Failed to get trades for {thesis_id}: {e}")
        return []


def _get_snapshots(thesis_id: str) -> List[Dict]:
    """Get daily confidence snapshots for a thesis."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT * FROM thesis_daily_snapshots
            WHERE thesis_id = ? ORDER BY snapshot_date
        """, (thesis_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"Failed to get snapshots for {thesis_id}: {e}")
        return []


def _fetch_hindsight_price(ticker: str, start_date: str) -> Optional[Dict]:
    """Fetch price data from thesis creation to now for hindsight analysis."""
    try:
        import yfinance as yf
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = yf.download(ticker, start=start_date, progress=False)
        if data.empty:
            return None
        close = data["Close"]
        if hasattr(close, "columns"):
            close = close.iloc[:, 0]
        prices = close.dropna().tolist()
        if len(prices) < 2:
            return None
        return {
            "start_price": round(prices[0], 2),
            "end_price": round(prices[-1], 2),
            "high": round(max(prices), 2),
            "low": round(min(prices), 2),
            "pct_change": round((prices[-1] - prices[0]) / prices[0] * 100, 2),
            "n_days": len(prices),
        }
    except Exception as e:
        logger.debug(f"[{ticker}] Hindsight price fetch failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# Layer 1: Rules-based replay (cheap, deterministic)
# ─────────────────────────────────────────────────────────────────────

def _rules_replay(thesis: Dict, trades: List[Dict], snapshots: List[Dict],
                  price_data: Optional[Dict]) -> Dict:
    """
    Deterministic replay using DB data + price history.
    No LLM calls. Always runs.
    """
    thesis_id = thesis["thesis_id"]
    ticker = thesis["ticker"]
    direction = thesis.get("idea_direction", "BULLISH")
    status = thesis["status"]

    # Determine realized outcome
    if status == "CLOSED_WIN":
        realized_outcome = "WIN"
    elif status == "CLOSED_LOSS":
        realized_outcome = "LOSS"
    elif status == "CLOSED_EXPIRED":
        realized_outcome = "EXPIRED"
    else:
        realized_outcome = "UNKNOWN"

    # Original verdict = the direction we bet on
    original_verdict = f"{direction} via {thesis.get('catalyst', 'unknown catalyst')}"

    # Analyze price movement for hindsight verdict
    missed_signals = []
    missed_contradictions = []
    confidence_delta = 0
    counterfactual_verdict = ""

    if price_data:
        pct = price_data["pct_change"]

        # Did price move in expected direction?
        if direction == "BULLISH" and pct < -5:
            counterfactual_verdict = f"Price declined {pct:.1f}% — bearish outcome contradicted bullish thesis"
            confidence_delta = -3
            missed_contradictions.append(f"Stock dropped {pct:.1f}% despite bullish thesis")
        elif direction == "BEARISH" and pct > 5:
            counterfactual_verdict = f"Price rose {pct:.1f}% — bullish outcome contradicted bearish thesis"
            confidence_delta = -3
            missed_contradictions.append(f"Stock rose {pct:.1f}% despite bearish thesis")
        elif direction == "BULLISH" and pct > 10:
            counterfactual_verdict = f"Strong move +{pct:.1f}% validates thesis — sizing may have been too conservative"
            confidence_delta = 2
            missed_signals.append("Thesis was correct but potentially undersized")
        elif direction == "BEARISH" and pct < -10:
            counterfactual_verdict = f"Strong move {pct:.1f}% validates thesis — sizing may have been too conservative"
            confidence_delta = 2
            missed_signals.append("Thesis was correct but potentially undersized")
        else:
            counterfactual_verdict = f"Price moved {pct:.1f}% — outcome inconclusive"
            confidence_delta = 0

        # Check if we missed a big intraday swing
        if price_data["high"] / price_data["start_price"] > 1.15:
            missed_signals.append(
                f"Intraday high reached +{((price_data['high']/price_data['start_price'])-1)*100:.1f}% — potential early exit missed"
            )
        if price_data["low"] / price_data["start_price"] < 0.85:
            missed_signals.append(
                f"Intraday low reached {((price_data['low']/price_data['start_price'])-1)*100:.1f}% — potential early exit missed"
            )
    else:
        counterfactual_verdict = "No price data available for hindsight analysis"

    # Confidence evolution from snapshots
    if len(snapshots) >= 2:
        conf_series = [s["confidence"] for s in snapshots]
        if conf_series[-1] < conf_series[0] - 2:
            missed_signals.append(
                f"Confidence degraded from {conf_series[0]} to {conf_series[-1]} — thesis was weakening"
            )

    # Trade-level analysis
    pnl_actual = None
    if trades:
        # Check for actual PnL if available
        for t in trades:
            if t.get("max_loss") and t.get("max_gain"):
                if realized_outcome == "WIN":
                    pnl_actual = t["max_gain"]
                elif realized_outcome == "LOSS":
                    pnl_actual = -abs(t["max_loss"])

    # MFE / MAE computation
    mfe_pct = 0.0
    mae_pct = 0.0
    if price_data:
        entry = price_data["start_price"]
        if entry > 0:
            if direction == "BULLISH":
                mfe_pct = round((price_data["high"] - entry) / entry, 4)
                mae_pct = round((price_data["low"] - entry) / entry, 4)
            else:
                mfe_pct = round((entry - price_data["low"]) / entry, 4)
                mae_pct = round((entry - price_data["high"]) / entry, 4)

    # Horizon-aware timing quality
    expected_horizon = thesis.get("expected_horizon") or "UNKNOWN"
    timing_quality = ""
    try:
        from orca_v20.horizon import compute_timing_quality, calendar_to_trading_days
        horizon_days = THRESHOLDS.horizon_days_map.get(expected_horizon, 10)
        # Compute trading age from created_utc
        trading_age = 0
        created_utc = thesis.get("created_utc", "")
        if created_utc and price_data:
            try:
                from datetime import datetime as _dt
                created = _dt.fromisoformat(created_utc.replace("Z", "+00:00"))
                cal_days = price_data.get("n_days", 0)
                trading_age = max(1, cal_days)  # n_days is already trading days from yfinance
            except Exception:
                trading_age = price_data.get("n_days", 0)

        directional_return = 0.0
        if price_data:
            pct_raw = price_data["pct_change"] / 100.0
            directional_return = pct_raw if direction == "BULLISH" else -pct_raw

        timing_quality = compute_timing_quality(
            directional_return, mfe_pct, trading_age,
            horizon_days, status, expected_horizon,
        )
    except Exception as e:
        logger.debug(f"[replay] Timing quality failed for {thesis_id}: {e}")

    # Agent miss report (rules-based)
    agent_miss_parts = []
    if realized_outcome == "LOSS" and confidence_delta < -2:
        agent_miss_parts.append("Thesis direction was wrong — original confidence may have been inflated")
    if realized_outcome == "EXPIRED":
        agent_miss_parts.append("Thesis expired without resolution — timing was off or catalyst fizzled")
    if missed_contradictions:
        agent_miss_parts.append(f"Missed contradictions: {'; '.join(missed_contradictions)}")
    agent_miss_report = " | ".join(agent_miss_parts) if agent_miss_parts else "No critical agent misses detected"

    # Enrich with multi-window forward outcomes from thesis_forward_outcomes
    # (computed by overnight L1 compute_forward_outcomes). These provide richer
    # MFE/MAE data at windows [1, 3, 5, 10, 20] than the single-snapshot
    # computation above.
    forward_outcomes = []
    try:
        from orca_v20.thesis_store import get_horizon_outcomes
        forward_outcomes = get_horizon_outcomes(thesis_id)
        if forward_outcomes:
            # Use the best multi-window MFE/MAE if available
            best_window = max(forward_outcomes, key=lambda x: x.get("window_days", 0))
            if best_window.get("mfe_pct") is not None:
                mfe_pct = best_window["mfe_pct"]
            if best_window.get("mae_pct") is not None:
                mae_pct = best_window["mae_pct"]
            if best_window.get("timing_quality"):
                timing_quality = best_window["timing_quality"]
    except Exception as e:
        logger.debug(f"[replay] Forward outcomes lookup failed for {thesis_id}: {e}")

    return {
        "thesis_id": thesis_id,
        "ticker": ticker,
        "replay_mode": "RULES_ONLY",
        "original_verdict": original_verdict,
        "realized_outcome": realized_outcome,
        "counterfactual_verdict": counterfactual_verdict,
        "what_we_missed": "; ".join(missed_signals + missed_contradictions) or "Nothing significant",
        "missed_signal_candidates": missed_signals,
        "missed_contradiction_candidates": missed_contradictions,
        "agent_miss_report": agent_miss_report,
        "confidence_delta": confidence_delta,
        "pnl_actual": pnl_actual,
        "pnl_counterfactual": None,
        "price_data": price_data,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "expected_horizon": expected_horizon,
        "timing_quality": timing_quality,
        "forward_outcomes": forward_outcomes,
    }


# ─────────────────────────────────────────────────────────────────────
# Layer 2: Premium LLM escalation (expensive, selective)
# ─────────────────────────────────────────────────────────────────────

def _should_escalate(rules_result: Dict) -> bool:
    """Decide if this replay warrants premium LLM analysis."""
    # Escalate on significant misses only
    if abs(rules_result.get("confidence_delta", 0)) >= 3:
        return True
    if rules_result.get("realized_outcome") == "LOSS" and len(rules_result.get("missed_contradiction_candidates", [])) > 0:
        return True
    return False


def _premium_replay(thesis: Dict, rules_result: Dict, ctx: RunContext) -> Dict:
    """
    LLM-powered deep analysis for highest-value misses.
    Uses replay_analyst role (claude-sonnet by default).
    """
    try:
        from orca_v20.router import call_model

        prompt = f"""Analyze this closed trading thesis with hindsight data.

THESIS:
- Ticker: {thesis['ticker']}
- Direction: {thesis.get('idea_direction', 'UNKNOWN')}
- Catalyst: {thesis.get('catalyst', 'unknown')}
- Thesis: {thesis.get('thesis_text', '')}
- Invalidation trigger: {thesis.get('invalidation_trigger', '')}

OUTCOME:
- Status: {thesis['status']}
- Realized outcome: {rules_result['realized_outcome']}
- Price data: {json.dumps(rules_result.get('price_data', {}))}

RULES-BASED ANALYSIS:
- Confidence delta: {rules_result['confidence_delta']}
- Missed signals: {json.dumps(rules_result['missed_signal_candidates'])}
- Missed contradictions: {json.dumps(rules_result['missed_contradiction_candidates'])}

Provide:
1. COUNTERFACTUAL_VERDICT: What should we have done differently?
2. MISSED_SIGNALS: What signals existed that we failed to detect?
3. MISSED_CONTRADICTIONS: What evidence contradicted our thesis that we ignored?
4. KEY_LESSON: One sentence lesson for future similar setups.
5. CONFIDENCE_ADJUSTMENT: Should our models be more or less confident on similar setups? (+/- integer)

Format as JSON with these 5 keys."""

        response = call_model(
            role="replay_analyst",
            system_prompt="You are a trading thesis post-mortem analyst. Be brutally honest about what went wrong or right.",
            user_prompt=prompt,
            ctx=ctx,
            temperature=0.3,
            max_tokens=2000,
        )

        # Try to parse JSON from response
        try:
            # Find JSON in response
            text = response.content
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                parsed = json.loads(text[start:end])
                rules_result["counterfactual_verdict"] = parsed.get("COUNTERFACTUAL_VERDICT", rules_result["counterfactual_verdict"])
                rules_result["missed_signal_candidates"] = parsed.get("MISSED_SIGNALS", rules_result["missed_signal_candidates"])
                rules_result["missed_contradiction_candidates"] = parsed.get("MISSED_CONTRADICTIONS", rules_result["missed_contradiction_candidates"])
                rules_result["agent_miss_report"] = parsed.get("KEY_LESSON", rules_result["agent_miss_report"])
                rules_result["confidence_delta"] = parsed.get("CONFIDENCE_ADJUSTMENT", rules_result["confidence_delta"])
        except json.JSONDecodeError:
            logger.warning(f"[replay] Failed to parse premium replay JSON — using rules-only result")

        rules_result["replay_mode"] = "PREMIUM_ESCALATED"
        return rules_result

    except Exception as e:
        logger.warning(f"[replay] Premium escalation failed: {e} — using rules-only result")
        return rules_result


# ─────────────────────────────────────────────────────────────────────
# Training example generation
# ─────────────────────────────────────────────────────────────────────

def _generate_training_examples(thesis: Dict, replay_result: Dict,
                                 replay_id: str, ctx: RunContext) -> List[Dict]:
    """
    Generate training examples from a replay for future fine-tuning.
    Each example = (input_prompt, expected_output, outcome_label).
    """
    examples = []
    now = datetime.now(timezone.utc).isoformat()
    ticker = thesis["ticker"]
    direction = thesis.get("idea_direction", "BULLISH")
    outcome = replay_result["realized_outcome"]

    # Build multi-window outcome summary for training context
    fwd_summary = ""
    fwd_outcomes = replay_result.get("forward_outcomes", [])
    if fwd_outcomes:
        parts = []
        for fo in fwd_outcomes:
            w = fo.get("window_days", "?")
            ret = fo.get("forward_return_pct", 0)
            mfe = fo.get("mfe_pct", 0)
            mae = fo.get("mae_pct", 0)
            label = fo.get("horizon_outcome_label", "")
            parts.append(f"{w}D: ret={ret:.1%} mfe={mfe:.1%} mae={mae:.1%} [{label}]")
        fwd_summary = "\nForward outcomes: " + " | ".join(parts)

    # Example 1: Catalyst assessment (was the catalyst read correct?)
    examples.append({
        "example_id": uuid.uuid4().hex[:12],
        "input_prompt": (
            f"Assess this catalyst for {ticker}: {thesis.get('catalyst', '')}\n"
            f"Thesis: {thesis.get('thesis_text', '')}\n"
            f"Direction: {direction}\n"
            f"Expected horizon: {replay_result.get('expected_horizon', 'UNKNOWN')}\n"
            f"Timing quality: {replay_result.get('timing_quality', '')}"
            f"{fwd_summary}"
        ),
        "expected_output": (
            f"Outcome: {outcome}. "
            f"{replay_result.get('counterfactual_verdict', '')}"
        ),
        "outcome_label": outcome,
        "source_run_id": thesis.get("created_run_id", ""),
        "source_thesis_id": thesis["thesis_id"],
        "replay_id": replay_id,
        "generated_utc": now,
    })

    # Example 2: Risk assessment (if loss, what should invalidation have been?)
    if outcome == "LOSS" and replay_result.get("missed_contradiction_candidates"):
        examples.append({
            "example_id": uuid.uuid4().hex[:12],
            "input_prompt": (
                f"What are the key risks for a {direction} thesis on {ticker}?\n"
                f"Catalyst: {thesis.get('catalyst', '')}\n"
                f"Original invalidation: {thesis.get('invalidation_trigger', '')}"
            ),
            "expected_output": (
                f"Missed contradictions: {'; '.join(replay_result['missed_contradiction_candidates'])}. "
                f"Agent report: {replay_result.get('agent_miss_report', '')}"
            ),
            "outcome_label": "LOSS_MISSED_RISK",
            "source_run_id": thesis.get("created_run_id", ""),
            "source_thesis_id": thesis["thesis_id"],
            "replay_id": replay_id,
            "generated_utc": now,
        })

    return examples


def _persist_training_examples(examples: List[Dict], ctx: RunContext) -> int:
    """Write training examples to DB."""
    if ctx.dry_run or not examples:
        return 0

    try:
        conn = get_connection()
        for ex in examples:
            conn.execute("""
                INSERT OR IGNORE INTO training_examples (
                    example_id, input_prompt, expected_output, outcome_label,
                    source_run_id, source_thesis_id, replay_id, generated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ex["example_id"], ex["input_prompt"], ex["expected_output"],
                ex["outcome_label"], ex["source_run_id"], ex["source_thesis_id"],
                ex["replay_id"], ex["generated_utc"],
            ))
        conn.commit()
        conn.close()
        return len(examples)
    except Exception as e:
        logger.error(f"[replay] Failed to persist training examples: {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────
# Replay persistence
# ─────────────────────────────────────────────────────────────────────

def _persist_replay(replay_id: str, thesis: Dict, result: Dict,
                    n_examples: int, ctx: RunContext) -> bool:
    """Write replay result to replay_runs table."""
    if ctx.dry_run:
        return True

    try:
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        conn.execute("""
            INSERT OR IGNORE INTO replay_runs (
                replay_id, thesis_id, replay_date, original_run_id,
                replay_mode, original_verdict, realized_outcome,
                hindsight_verdict, counterfactual_verdict,
                what_we_missed, missed_signal_candidates,
                missed_contradiction_candidates, agent_miss_report,
                confidence_delta, pnl_actual, pnl_counterfactual,
                lessons_json, training_examples_generated, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            replay_id,
            thesis["thesis_id"],
            ctx.market_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            thesis.get("created_run_id", ""),
            result["replay_mode"],
            result["original_verdict"],
            result["realized_outcome"],
            result.get("hindsight_verdict", result.get("counterfactual_verdict", "")),  # FIX: was duplicating counterfactual
            result.get("counterfactual_verdict", ""),
            result.get("what_we_missed", ""),
            json.dumps(result.get("missed_signal_candidates", [])),
            json.dumps(result.get("missed_contradiction_candidates", [])),
            result.get("agent_miss_report", ""),
            result.get("confidence_delta", 0),
            result.get("pnl_actual"),
            result.get("pnl_counterfactual"),
            json.dumps([]),
            n_examples,
            now,
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"[replay] Failed to persist replay: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

def replay_thesis(thesis: Dict, ctx: RunContext) -> Optional[Dict]:
    """
    Replay a single closed thesis.

    Layer 1: Rules-based analysis (always runs, cheap).
    Layer 2: Premium LLM escalation (only if significant miss detected).

    Returns full replay result dict or None on failure.
    """
    thesis_id = thesis["thesis_id"]
    ticker = thesis["ticker"]
    replay_id = uuid.uuid4().hex[:12]

    logger.info(f"[replay] {ticker} — replaying thesis {thesis_id}")

    # Gather context from DB
    trades = _get_trade_records(thesis_id)
    snapshots = _get_snapshots(thesis_id)

    # Fetch hindsight price data
    start_date = thesis.get("created_utc", "")[:10]
    price_data = _fetch_hindsight_price(ticker, start_date) if start_date else None

    # Layer 1: Rules-based replay (always runs)
    result = _rules_replay(thesis, trades, snapshots, price_data)

    # Layer 2: Premium escalation (selective)
    if _should_escalate(result):
        logger.info(f"  [{ticker}] Escalating to premium replay (confidence_delta={result['confidence_delta']})")
        result = _premium_replay(thesis, result, ctx)
    else:
        logger.info(f"  [{ticker}] Rules-only replay sufficient")

    # Generate training examples
    examples = _generate_training_examples(thesis, result, replay_id, ctx)
    result["training_examples_generated"] = len(examples)
    result["replay_id"] = replay_id

    # Persist — replay_run FIRST (training_examples has FK to replay_runs)
    _persist_replay(replay_id, thesis, result, len(examples), ctx)
    n_persisted = _persist_training_examples(examples, ctx)

    logger.info(
        f"  [{ticker}] Replay complete: {result['replay_mode']}, "
        f"outcome={result['realized_outcome']}, delta={result['confidence_delta']}, "
        f"examples={n_persisted}"
    )

    # FIX: Populate memory_cases table so analog retrieval has data.
    # store_case was never called after replay — memory_cases stayed empty.
    try:
        from orca_v20.memory_store import store_case
        store_case(
            thesis_id=thesis_id,
            ticker=ticker,
            catalyst_type=thesis.get("catalyst", ""),
            setup_summary=thesis.get("thesis_text", "")[:500],
            outcome=result["realized_outcome"],
            pnl_pct=result.get("pnl_actual", 0.0) or 0.0,
            key_lesson=result.get("what_we_missed", ""),
            ctx=ctx,
        )
    except Exception as e:
        logger.debug(f"  [{ticker}] Memory store failed (non-fatal): {e}")

    return result


def run_nightly_replay(ctx: RunContext) -> List[Dict]:
    """
    Run replay on all recently closed theses.

    Returns list of replay results. Called from pipeline scheduler
    when enable_replay_engine=True.
    """
    if not FLAGS.enable_replay_engine:
        logger.info("[replay_engine] Disabled (FLAGS.enable_replay_engine=False)")
        return []

    closed = _get_closed_theses(lookback_days=14)
    if not closed:
        logger.info("[replay_engine] No unreplayed closed theses found")
        return []

    logger.info(f"[replay_engine] Found {len(closed)} closed theses for replay")

    results = []
    for thesis in closed:
        result = replay_thesis(thesis, ctx)
        if result:
            results.append(result)

    rules_count = sum(1 for r in results if r.get("replay_mode") == "RULES_ONLY")
    premium_count = sum(1 for r in results if r.get("replay_mode") == "PREMIUM_ESCALATED")
    total_examples = sum(r.get("training_examples_generated", 0) for r in results)

    logger.info(
        f"[replay_engine] Replay complete: {len(results)} theses "
        f"({rules_count} rules-only, {premium_count} premium), "
        f"{total_examples} training examples generated"
    )
    ctx.mark_stage("replay_engine")
    return results
