#!/usr/bin/env python3
"""
ORCA v20 — Main Pipeline Entrypoint.

Usage:
    python pipeline_v20.py                     # standard run
    python pipeline_v20.py --dry-run           # no writes, no notifications
    python pipeline_v20.py --mode deep         # deep research mode
    python pipeline_v20.py --no-uw             # skip Unusual Whales
    python pipeline_v20.py --verbose           # debug logging

This is the ONLY entrypoint for v20. It:
    1. Creates a RunContext with a unique run_id
    2. Bootstraps orca_v20.db
    3. Runs all stages through v20 adapters
    4. Writes run trace for audit

NO v3 files are modified. All writes go to orca_v20.db.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Ensure project root is on sys.path so both v3 and v20 modules are importable
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env file (Telegram, X, EIA, Google Sheets, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass  # dotenv optional — env vars must be set externally

# ── v20 imports ──────────────────────────────────────────────────────
import orca_v20.config as _cfg
from orca_v20.config import FLAGS, PATHS, THRESHOLDS
# NOTE: Do NOT import BUDGET_MODE at module level — it binds to the pre-reload
# value (False). Always access via _cfg.BUDGET_MODE so importlib.reload() works.
from orca_v20.db_bootstrap import bootstrap_db, get_connection
from orca_v20.run_context import ResearchMode, RunContext, SourceMode
from orca_v20.retrieval_state import RetrievalState

# Adapters (wrap legacy v3)
from orca_v20.adapters.stage1_hunter_adapter import run_stage1
from orca_v20.adapters.flow_adapter import run_stage2
from orca_v20.adapters.catalyst_adapter import run_stage3
from orca_v20.adapters.structurer_adapter import run_stage4
from orca_v20.adapters.logger_adapter import run_stage6
from orca_v20.adapters.report_adapter import run_stage7

# New v20 modules
from orca_v20.evidence_gate import run_evidence_gate
from orca_v20.thesis_store import run_thesis_matching, finalize_outcomes, auto_label_active_theses
from orca_v20.thesis_momentum import run_momentum_update
from orca_v20.daemon_rules import check_portfolio_health, check_thesis_overlap, update_rules
from orca_v20.red_team_gate import run_red_team
from orca_v20.sizing import size_all
from orca_v20.execution_impact import assess_all

# Phase 3 modules
from orca_v20.institutional_pressure import run_pressure_scan
from orca_v20.simulation.elite_agents import simulate_elite
from orca_v20.simulation.follower_crowd import simulate_crowd
from orca_v20.simulation.aggregation import aggregate
from orca_v20 import quant_gate, causal_gate, factor_gate


# ─────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────

def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    # Quiet noisy libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("orca_v20.pipeline")


# ─────────────────────────────────────────────────────────────────────
# Temporal context builder
# ─────────────────────────────────────────────────────────────────────

def _third_friday(year: int, month: int) -> datetime:
    """Compute the third Friday of a given year/month (monthly OPEX)."""
    from calendar import monthcalendar
    cal = monthcalendar(year, month)
    # Friday is index 4 in each week row
    fridays = [week[4] for week in cal if week[4] != 0]
    day = fridays[2]  # third Friday (0-indexed)
    return datetime(year, month, day, tzinfo=timezone.utc)


def _next_opex(as_of: datetime) -> datetime:
    """Return the next monthly OPEX (3rd Friday). If this month's already passed, use next month."""
    opex = _third_friday(as_of.year, as_of.month)
    if as_of.date() > opex.date():
        # Move to next month
        if as_of.month == 12:
            opex = _third_friday(as_of.year + 1, 1)
        else:
            opex = _third_friday(as_of.year, as_of.month + 1)
    return opex


# 2026 FOMC meeting dates (announcement days, Wed)
_FOMC_2026 = [
    (1, 28), (3, 18), (5, 6), (6, 17),
    (7, 29), (9, 16), (10, 28), (12, 9),
]


def _is_fomc_week(as_of: datetime) -> bool:
    """True if as_of falls in the same ISO week as any FOMC meeting date in 2026."""
    as_of_iso = as_of.isocalendar()
    for month, day in _FOMC_2026:
        fomc_date = datetime(2026, month, day, tzinfo=timezone.utc)
        fomc_iso = fomc_date.isocalendar()
        if as_of_iso[:2] == fomc_iso[:2]:  # same (year, week)
            return True
    return False


def _is_earnings_season(as_of: datetime) -> bool:
    """Approximate: weeks 3-5 of Jan/Apr/Jul/Oct are earnings season."""
    month = as_of.month
    day = as_of.day
    if month in (1, 4, 7, 10) and 15 <= day <= 31:
        return True
    if month in (2, 5, 8, 11) and 1 <= day <= 7:
        return True
    return False


def build_temporal_context(ctx: RunContext) -> None:
    """Populate temporal fields on the RunContext."""
    now = ctx.as_of_utc
    ctx.market_date = now.strftime("%Y-%m-%d")
    ctx.day_of_week = now.strftime("%A")

    # OPEX computation
    try:
        opex = _next_opex(now)
        ctx.days_to_opex = (opex.date() - now.date()).days
        ctx.is_opex_week = ctx.days_to_opex <= 5
    except Exception as e:
        logger.warning(f"OPEX computation failed: {e}")
        ctx.days_to_opex = None
        ctx.is_opex_week = False

    # FOMC week check
    ctx.is_fomc_week = _is_fomc_week(now)

    # Earnings season check
    ctx.is_earnings_season = _is_earnings_season(now)


# ─────────────────────────────────────────────────────────────────────
# Regime loader (reads JSON from regime_runner.py output)
# ─────────────────────────────────────────────────────────────────────

def load_regime(ctx: RunContext) -> None:
    """Load regime prediction from JSON file (if available)."""
    regime_path = PATHS.regime_json
    if not os.path.exists(regime_path):
        logger.warning("No regime_prediction.json found — skipping regime context")
        return

    try:
        with open(regime_path, "r") as f:
            data = json.load(f)
        ctx.spy_regime = data.get("bias", "neutral")
        ctx.regime_conviction = data.get("bias_confidence", 0.0)
        logger.info(f"Regime loaded: {ctx.spy_regime} (conviction={ctx.regime_conviction})")
    except Exception as e:
        logger.error(f"Failed to load regime: {e}")
        ctx.add_error("regime_loader", str(e))


# ─────────────────────────────────────────────────────────────────────
# Run trace persistence
# ─────────────────────────────────────────────────────────────────────

def save_run_trace(ctx: RunContext, success: bool, trace_counts: dict) -> None:
    """Write the run trace to orca_v20.db."""
    if ctx.dry_run:
        logger.info("[trace] DRY RUN — skipping trace write")
        return

    conn = None
    try:
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO run_traces (
                run_id, started_utc, completed_utc, market_date,
                research_mode, source_mode,
                ideas_generated, ideas_after_flow, ideas_after_confirm,
                ideas_after_gates, trades_structured, trades_logged,
                total_api_cost_usd, errors_json, warnings_json,
                dry_run, success
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ctx.run_id,
            ctx.as_of_utc.isoformat(),
            datetime.now(timezone.utc).isoformat(),
            ctx.market_date,
            ctx.research_mode.value,
            ctx.source_mode.value,
            trace_counts.get("ideas_generated", 0),
            trace_counts.get("ideas_after_flow", 0),
            trace_counts.get("ideas_after_confirm", 0),
            trace_counts.get("ideas_after_gates", 0),
            trace_counts.get("trades_structured", 0),
            trace_counts.get("trades_logged", 0),
            ctx.api_cost_usd,
            json.dumps(ctx.errors),
            json.dumps([]),
            int(ctx.dry_run),
            int(success),
        ))
        conn.commit()
        logger.info(f"[trace] Run trace saved: {ctx.run_id}")
    except Exception as e:
        logger.error(f"[trace] Failed to save run trace: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────

def run_pipeline(ctx: RunContext) -> bool:
    """
    Execute the full ORCA v20 pipeline.

    Returns True on success.
    """
    trace = {}

    logger.info("=" * 60)
    logger.info(f"ORCA v20 Pipeline — run_id={ctx.run_id}")
    logger.info(f"  market_date={ctx.market_date}")
    logger.info(f"  research_mode={ctx.research_mode.value}")
    logger.info(f"  source_mode={ctx.source_mode.value}")
    logger.info(f"  dry_run={ctx.dry_run}")
    logger.info("=" * 60)

    # ── Pre-flight: daemon health check ──
    healthy, triggered = check_portfolio_health(ctx)
    if not healthy:
        logger.error(f"Portfolio health check FAILED: {triggered}")
        logger.error("Pipeline halted by daemon rules.")
        save_run_trace(ctx, success=False, trace_counts=trace)
        return False

    # ── Source Layer: ingest all enabled source adapters ──
    try:
        from orca_v20.source_adapters.orchestrator import ingest_all_sources, update_retrieval_state
        source_results = ingest_all_sources(ctx)
        ctx.source_results = source_results  # A1: make available to evidence gate
        retrieval = RetrievalState(ctx)
        update_retrieval_state(retrieval, source_results)
        logger.info(
            f"Source layer → {source_results['total_items']} items "
            f"(T1={source_results['tier_counts']['tier_1']}, "
            f"T2={source_results['tier_counts']['tier_2']}, "
            f"T3={source_results['tier_counts']['tier_3']})"
        )
    except Exception as e:
        logger.warning(f"Source layer failed (non-blocking): {e}")
        ctx.add_error("source_layer", str(e))

    # ── Stage 1: Catalyst Hunting ──
    ideas = run_stage1(ctx)
    trace["ideas_generated"] = len(ideas)
    logger.info(f"Stage 1 → {len(ideas)} ideas generated")

    if not ideas:
        logger.info("No ideas generated — pipeline complete (nothing to do)")
        save_run_trace(ctx, success=True, trace_counts=trace)
        return True

    # ── Budget shortlist cap: Stage 2 ──
    stage2_total = len(ideas)
    stage2_skipped = 0
    if _cfg.BUDGET_MODE and len(ideas) > THRESHOLDS.budget_max_stage2_candidates:
        ideas.sort(key=lambda x: x.confidence, reverse=True)
        dropped = ideas[THRESHOLDS.budget_max_stage2_candidates:]
        ideas = ideas[:THRESHOLDS.budget_max_stage2_candidates]
        stage2_skipped = len(dropped)
        logger.info(
            f"[budget] Stage 2 cap ENFORCED: {stage2_total} → {len(ideas)} "
            f"(skipped {stage2_skipped}: {[i.ticker for i in dropped]})"
        )
    trace["stage2_cap"] = THRESHOLDS.budget_max_stage2_candidates if _cfg.BUDGET_MODE else None
    trace["stage2_input_count"] = stage2_total
    trace["stage2_skipped_count"] = stage2_skipped
    trace["stage2_processed_count"] = len(ideas)

    # ── Stage 2: Flow Enrichment ──
    ideas = run_stage2(ideas, ctx)
    trace["ideas_after_flow"] = len(ideas)

    # ── Budget shortlist cap: Stage 3 ──
    stage3_total = len(ideas)
    stage3_skipped = 0
    if _cfg.BUDGET_MODE and len(ideas) > THRESHOLDS.budget_max_stage3_candidates:
        ideas.sort(key=lambda x: x.confidence, reverse=True)
        dropped = ideas[THRESHOLDS.budget_max_stage3_candidates:]
        ideas = ideas[:THRESHOLDS.budget_max_stage3_candidates]
        stage3_skipped = len(dropped)
        logger.info(
            f"[budget] Stage 3 cap ENFORCED: {stage3_total} → {len(ideas)} "
            f"(skipped {stage3_skipped}: {[i.ticker for i in dropped]})"
        )
    trace["stage3_cap"] = THRESHOLDS.budget_max_stage3_candidates if _cfg.BUDGET_MODE else None
    trace["stage3_input_count"] = stage3_total
    trace["stage3_skipped_count"] = stage3_skipped
    trace["stage3_processed_count"] = len(ideas)

    # ── Stage 3: Catalyst Confirmation ──
    ideas, filtered = run_stage3(ideas, ctx)
    trace["ideas_after_confirm"] = len(ideas)
    logger.info(f"Stage 3 → {len(ideas)} confirmed, {len(filtered)} filtered")

    if not ideas:
        logger.info("All ideas filtered out — pipeline complete")
        save_run_trace(ctx, success=True, trace_counts=trace)
        return True

    # ── Evidence Gate (v20) — run BEFORE thesis matching so that
    #    rejected ideas don't inflate times_seen counters on theses ──
    ideas, evidence_failed = run_evidence_gate(ideas, ctx)

    # ── Thesis Matching (v20) — only match ideas that passed evidence ──
    ideas = run_thesis_matching(ideas, ctx)
    trace["ideas_after_evidence_gate"] = len(ideas)  # FIX: separate key, don't overwrite

    if not ideas:
        logger.info("All ideas failed evidence gate — pipeline complete")
        save_run_trace(ctx, success=True, trace_counts=trace)
        return True

    # ── Red-Team Gate (v20) ──
    ideas_before_rt = len(ideas)
    ideas = run_red_team(ideas, ctx)
    trace["ideas_after_red_team"] = len(ideas)
    logger.info(f"Red-team gate → {len(ideas)}/{ideas_before_rt} survived")

    if not ideas:
        logger.info("All ideas rejected by red-team gate — pipeline complete")
        save_run_trace(ctx, success=True, trace_counts=trace)
        return True

    # ── Thesis Overlap Check (advisory, non-blocking) ──
    ideas = check_thesis_overlap(ideas, ctx)
    overlap_flagged = sum(1 for i in ideas if i.overlap_warnings)
    if overlap_flagged:
        logger.info(f"Thesis overlap: {overlap_flagged} idea(s) flagged (advisory)")

    # ── Institutional Pressure Scan (v20) ──
    tickers_for_pressure = list(set(i.ticker for i in ideas))
    pressure_data = run_pressure_scan(tickers_for_pressure, ctx)

    # ── Elite Simulation + Crowd (v20) ──
    # Phase 4: shortlist before simulation to control cost.
    # Only top N ideas (by confidence) enter the full 15-agent sim.
    # This saves ~$0.60/idea in Sonnet calls without degrading thesis quality.
    sim_max = THRESHOLDS.elite_simulation_max_ideas
    sim_min_conf = THRESHOLDS.elite_shortlist_min_confidence
    sim_candidates = [i for i in ideas if i.confidence >= sim_min_conf]
    sim_candidates.sort(key=lambda x: x.confidence, reverse=True)

    if len(sim_candidates) > sim_max:
        skipped = sim_candidates[sim_max:]
        sim_candidates = sim_candidates[:sim_max]
        logger.info(
            f"[elite_sim] Shortlisted {len(sim_candidates)}/{len(ideas)} ideas "
            f"(max={sim_max}, min_conf={sim_min_conf}). "
            f"Skipped: {[i.ticker for i in skipped]}"
        )

    for idea in sim_candidates:
        elite_votes = simulate_elite(idea, ctx)
        if elite_votes:
            crowd_sent = simulate_crowd(elite_votes)
            verdict = aggregate(idea, elite_votes, crowd_sent, ctx)
            if verdict.final_verdict == "REJECT":
                logger.info(f"  [{idea.ticker}] REJECTED by simulation — removing")
                ideas = [i for i in ideas if i.idea_id != idea.idea_id]

    # ── Quant / Causal / Factor Gates (v20) ──
    gate_survivors = []
    for idea in ideas:
        q_pass, q_details = quant_gate.evaluate(idea, ctx)
        c_pass, c_details = causal_gate.evaluate(idea, ctx)
        f_pass, f_details = factor_gate.evaluate(idea, ctx)

        if q_pass and c_pass and f_pass:
            gate_survivors.append(idea)
        else:
            failed_gates = []
            if not q_pass: failed_gates.append("quant")
            if not c_pass: failed_gates.append("causal")
            if not f_pass: failed_gates.append("factor")
            logger.info(f"  [{idea.ticker}] Failed gates: {', '.join(failed_gates)}")

    ideas = gate_survivors
    trace["ideas_after_gates"] = len(ideas)

    if not ideas:
        logger.info("All ideas failed advanced gates — pipeline complete")
        save_run_trace(ctx, success=True, trace_counts=trace)
        return True

    # ── Budget shortlist cap: Stage 4 (structuring) ──
    stage4_total = len(ideas)
    stage4_skipped = 0
    if _cfg.BUDGET_MODE and len(ideas) > THRESHOLDS.budget_max_structured:
        ideas.sort(key=lambda x: x.confidence, reverse=True)
        dropped = ideas[THRESHOLDS.budget_max_structured:]
        ideas = ideas[:THRESHOLDS.budget_max_structured]
        stage4_skipped = len(dropped)
        logger.info(
            f"[budget] Stage 4 cap ENFORCED: {stage4_total} → {len(ideas)} "
            f"(skipped {stage4_skipped}: {[i.ticker for i in dropped]})"
        )
    trace["stage4_cap"] = THRESHOLDS.budget_max_structured if _cfg.BUDGET_MODE else None
    trace["stage4_input_count"] = stage4_total
    trace["stage4_skipped_count"] = stage4_skipped
    trace["stage4_processed_count"] = len(ideas)

    # ── Stage 4: Trade Structuring ──
    trades = run_stage4(ideas, ctx)
    trace["trades_structured"] = len(trades)

    # ── Sizing + Execution Impact (v20) ──
    trades = size_all(trades, ctx)
    trades = assess_all(trades, ctx)

    # ── FIX #8: Filter ideas to only those that were successfully structured.
    #    Stage 4 can drop ideas (no options chain, structurer failure, etc.)
    #    but the `ideas` list still contains all pre-Stage-4 ideas.  Passing
    #    unstructured ideas to the v3 logger creates ghost rows with NULL
    #    strikes/expiry.  Only keep ideas whose ticker matched a trade.
    structured_tickers = {t.ticker.upper() for t in trades}
    structured_ideas = [i for i in ideas if i.ticker.upper() in structured_tickers]
    if len(structured_ideas) < len(ideas):
        dropped_tickers = [i.ticker for i in ideas if i.ticker.upper() not in structured_tickers]
        logger.info(
            f"[FIX #8] Filtered {len(ideas) - len(structured_ideas)} unstructured idea(s) "
            f"before logger: {dropped_tickers}"
        )

    # ── Stage 6: Logging ──
    logged_count = run_stage6(trades, structured_ideas, ctx)
    trace["trades_logged"] = logged_count

    # ── Stage 7: Reporting + Publishing ──
    run_stage7(trades, structured_ideas, ctx, trace=trace)

    # ── B1: Automated thesis outcome labeling via price watcher ──
    auto_labeled = auto_label_active_theses(ctx)
    if auto_labeled:
        logger.info(f"  Auto-labeled {auto_labeled} thesis outcomes")

    # ── Thesis Outcome Finalization (v20, Phase 4) ──
    finalize_outcomes(ctx)

    # ── Nightly Replay (v20, Phase 5) ──
    # Budget mode: replay runs ONLY in overnight, never in intraday
    if not _cfg.BUDGET_MODE and FLAGS.enable_replay_engine:
        from orca_v20.replay_engine import run_nightly_replay
        replay_results = run_nightly_replay(ctx)
        if replay_results:
            logger.info(f"  Replay: {len(replay_results)} theses replayed")
    elif _cfg.BUDGET_MODE:
        logger.info("[budget] Replay skipped — runs only in overnight teacher loop")

    # ── Thesis Momentum Update (v20) ──
    run_momentum_update(ctx)

    # ── Daemon Rules Update ──
    update_rules(ctx)

    # ── Save run trace ──
    save_run_trace(ctx, success=True, trace_counts=trace)

    # ── Router session summary ──
    from orca_v20.router import log_session_summary, get_session_summary, get_role_costs
    log_session_summary()

    # ── Budget mode: write compact artifacts for overnight review ──
    if _cfg.BUDGET_MODE:
        _write_budget_artifacts(ctx, trace)

    logger.info("=" * 60)
    logger.info(f"Pipeline complete — run_id={ctx.run_id}")
    logger.info(f"  Ideas: {trace['ideas_generated']} → {trace.get('ideas_after_gates', 0)} after gates")
    logger.info(f"  Trades: {trace.get('trades_structured', 0)} structured, {trace.get('trades_logged', 0)} logged")
    logger.info(f"  API cost: ${ctx.api_cost_usd:.4f}")
    logger.info(f"  Errors: {len(ctx.errors)}")
    if _cfg.BUDGET_MODE:
        logger.info(f"  Budget mode: YES (artifacts in {PATHS.artifacts_dir})")
    logger.info("=" * 60)

    return True


# ─────────────────────────────────────────────────────────────────────
# Budget artifact writer
# ─────────────────────────────────────────────────────────────────────

def _write_budget_artifacts(ctx: RunContext, trace: dict) -> None:
    """Write compact run artifacts for budget sprint overnight review."""
    from orca_v20.router import get_session_summary
    artifacts_dir = PATHS.artifacts_dir
    os.makedirs(artifacts_dir, exist_ok=True)

    run_ts = ctx.as_of_utc.strftime("%Y%m%dT%H%M%S")
    prefix = f"{artifacts_dir}/{ctx.market_date}_{run_ts}"

    # 1. Compact run summary
    summary = {
        "run_id": ctx.run_id,
        "market_date": ctx.market_date,
        "timestamp_utc": ctx.as_of_utc.isoformat(),
        "budget_mode": True,
        "total_cost_usd": round(ctx.api_cost_usd, 4),
        "trace": trace,
        "errors": ctx.errors,
        "router_summary": get_session_summary(),
    }
    with open(f"{prefix}_run_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # 2. Cost summary (includes stage cap enforcement counts)
    cost = {
        "run_id": ctx.run_id,
        "total_cost_usd": round(ctx.api_cost_usd, 4),
        "max_budget": THRESHOLDS.max_api_cost_per_run,
        "role_costs": get_session_summary().get("role_costs", {}),
        "stage_caps": {
            "stage2": {
                "cap": trace.get("stage2_cap"),
                "input": trace.get("stage2_input_count", 0),
                "processed": trace.get("stage2_processed_count", 0),
                "skipped": trace.get("stage2_skipped_count", 0),
            },
            "stage3": {
                "cap": trace.get("stage3_cap"),
                "input": trace.get("stage3_input_count", 0),
                "processed": trace.get("stage3_processed_count", 0),
                "skipped": trace.get("stage3_skipped_count", 0),
            },
            "stage4": {
                "cap": trace.get("stage4_cap"),
                "input": trace.get("stage4_input_count", 0),
                "processed": trace.get("stage4_processed_count", 0),
                "skipped": trace.get("stage4_skipped_count", 0),
            },
        },
    }
    with open(f"{prefix}_cost_summary.json", "w") as f:
        json.dump(cost, f, indent=2, default=str)

    logger.info(f"[budget] Artifacts written to {artifacts_dir}/")


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ORCA v20 Pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip all writes and notifications")
    parser.add_argument("--mode", choices=["fast", "standard", "deep"],
                        default="standard", help="Research depth mode")
    parser.add_argument("--no-uw", action="store_true",
                        help="Skip Unusual Whales data source")
    parser.add_argument("--minimal", action="store_true",
                        help="Minimal sources (scanner + news only)")
    parser.add_argument("--verbose", action="store_true",
                        help="Debug-level logging")
    parser.add_argument("--budget", action="store_true",
                        help="Budget sprint mode (cheap models, no publishing, max data collection)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    # ── Budget mode activation ──
    if args.budget:
        os.environ["ORCA_BUDGET_MODE"] = "1"
        # Reload config module so BUDGET_MODE, THRESHOLDS, ROUTING pick up overrides.
        # We access via _cfg (module ref) throughout run_pipeline() to avoid stale bindings.
        import importlib
        importlib.reload(_cfg)
        from orca_v20.config import ROUTING
        # FIX: Re-bind module-level THRESHOLDS/FLAGS/PATHS after reload so budget
        # caps ($3) aren't stale from the pre-reload values ($20).
        global THRESHOLDS, FLAGS, PATHS
        THRESHOLDS = _cfg.THRESHOLDS
        FLAGS = _cfg.FLAGS
        PATHS = _cfg.PATHS
        logger.info("=" * 60)
        logger.info("*** BUDGET SPRINT MODE ACTIVE ***")
        logger.info(f"  BUDGET_MODE={_cfg.BUDGET_MODE}")
        logger.info(f"  Models: {ROUTING.hunter_primary}/{ROUTING.hunter_secondary}/{ROUTING.hunter_tertiary}")
        logger.info(f"  Publishing: ALL DISABLED")
        logger.info(f"  Max cost/run: ${_cfg.THRESHOLDS.max_api_cost_per_run}")
        logger.info(f"  Stage caps: S2={_cfg.THRESHOLDS.budget_max_stage2_candidates}, "
                     f"S3={_cfg.THRESHOLDS.budget_max_stage3_candidates}, "
                     f"Struct={_cfg.THRESHOLDS.budget_max_structured}")
        logger.info(f"  Replay in intraday: DISABLED")
        logger.info("=" * 60)

    # Build RunContext
    ctx = RunContext(
        research_mode=ResearchMode(args.mode),
        source_mode=(
            SourceMode.MINIMAL if args.minimal
            else SourceMode.NO_UW if args.no_uw
            else SourceMode.FULL
        ),
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_positions=THRESHOLDS.max_positions,
        overflow_slots=THRESHOLDS.overflow_slots,
        hard_cap=THRESHOLDS.hard_cap,
        max_api_cost_usd=THRESHOLDS.max_api_cost_per_run,
    )

    # Temporal context
    build_temporal_context(ctx)

    # Portfolio value (A5: resolve from env/config/default)
    from orca_v20.sizing import _resolve_portfolio_value
    ctx.portfolio_value = _resolve_portfolio_value(ctx)
    logger.info(f"Portfolio value: ${ctx.portfolio_value:,.0f}")

    # Load regime
    load_regime(ctx)

    # Bootstrap DB
    bootstrap_db()

    # Run pipeline
    success = run_pipeline(ctx)

    # Exit code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
