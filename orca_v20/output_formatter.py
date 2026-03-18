"""
ORCA v20 — Output Formatter (Phase 5).

Two output modes:
    PRODUCTION — concise, high-signal, operator-readable
    DEBUG      — full traces, gate statuses, internal diagnostics

Production output emphasizes:
    thesis, why now, catalyst state, flow interaction, confidence,
    structure, sizing, invalidation, monitor status, institutional pressure

Debug output adds:
    gate statuses, raw metrics, simulation diagnostics, fallback/retry notes,
    internal trace IDs, reason codes, raw scores
"""

import json
import logging
from typing import Dict, List, Optional

from orca_v20.schemas import IdeaCandidate, StructuredTrade

logger = logging.getLogger("orca_v20.output")


# ─────────────────────────────────────────────────────────────────────
# Production output (concise, operator-facing)
# ─────────────────────────────────────────────────────────────────────

def format_trade_production(trade: StructuredTrade, idea: IdeaCandidate,
                             gate_results: Optional[Dict] = None,
                             pressure_data: Optional[Dict] = None) -> str:
    """
    Concise production output for a single trade.
    Designed for Telegram / operator dashboard.
    """
    lines = []
    lines.append(f"{'='*50}")
    lines.append(f"  {trade.ticker} — {trade.idea_direction.value}")
    lines.append(f"{'='*50}")

    # Thesis
    lines.append(f"  Thesis: {idea.thesis[:200]}")

    # Why now
    if idea.catalyst:
        lines.append(f"  Why now: {idea.catalyst[:150]}")

    # Catalyst state
    if idea.catalyst_action:
        lines.append(f"  Catalyst: {idea.catalyst_action.value} (CDS: {idea.cds_score or 'N/A'})")

    # Flow interaction
    if idea.tape_read:
        lines.append(f"  Flow: {idea.tape_read}")

    # Confidence
    lines.append(f"  Confidence: {trade.confidence}/10 | Urgency: {trade.urgency}/10")
    if trade.consensus_tag:
        lines.append(f"  Consensus: {trade.consensus_tag.value}")

    # Structure
    lines.append(f"  Strategy: {trade.strategy_label}")
    strike_str = f"${trade.strike_1}"
    if trade.strike_2:
        strike_str += f" / ${trade.strike_2}"
    lines.append(f"  Strikes: {strike_str} | Exp: {trade.expiry} ({trade.dte}d)")

    # Pricing
    if trade.entry_price:
        lines.append(f"  Entry: ${trade.entry_price:.2f} | Target: ${trade.target_price:.2f} | Stop: ${trade.stop_price:.2f}")
    if trade.risk_reward:
        lines.append(f"  R/R: {trade.risk_reward:.1f}x | Max loss: ${trade.max_loss:.0f} | Max gain: ${trade.max_gain:.0f}")

    # Sizing
    if trade.contracts:
        lines.append(f"  Size: {trade.contracts} contracts ({trade.adjusted_size_pct or 0:.1%} of portfolio)")

    # Execution impact
    if trade.estimated_slippage_pct:
        lines.append(f"  Slippage est: {trade.estimated_slippage_pct:.2f}% | Liquidity: {trade.liquidity_score or 0:.2f}")

    # Invalidation
    if idea.invalidation:
        lines.append(f"  Invalidation: {idea.invalidation[:150]}")

    # Institutional pressure
    if pressure_data and pressure_data.get(trade.ticker):
        p = pressure_data[trade.ticker]
        parts = []
        if "crowding_score" in p:
            parts.append(f"crowd={p['crowding_score']:.2f}")
        if "short_interest_pct" in p:
            parts.append(f"SI={p['short_interest_pct']:.1f}%")
        if parts:
            lines.append(f"  Inst. pressure: {', '.join(parts)}")

    lines.append("")
    return "\n".join(lines)


def format_run_production(trades: List[StructuredTrade],
                           ideas: List[IdeaCandidate],
                           ctx_summary: Dict,
                           gate_results: Optional[Dict] = None,
                           pressure_data: Optional[Dict] = None) -> str:
    """Format full run output in production mode."""
    lines = []
    lines.append(f"ORCA v20 — {ctx_summary.get('market_date', 'N/A')}")
    lines.append(f"Run: {ctx_summary.get('run_id', 'N/A')} | Cost: ${ctx_summary.get('api_cost_usd', 0):.2f}")
    lines.append("")

    if not trades:
        lines.append("No trades generated this run.")
        return "\n".join(lines)

    lines.append(f"{len(trades)} trade(s) generated:")
    lines.append("")

    # Match trades to ideas
    idea_map = {i.idea_id: i for i in ideas}
    for trade in trades:
        idea = idea_map.get(trade.idea_id, IdeaCandidate())
        lines.append(format_trade_production(trade, idea, gate_results, pressure_data))

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# Debug output (full diagnostics)
# ─────────────────────────────────────────────────────────────────────

def format_trade_debug(trade: StructuredTrade, idea: IdeaCandidate,
                        gate_results: Optional[Dict] = None,
                        sim_verdict: Optional[Dict] = None,
                        pressure_data: Optional[Dict] = None) -> str:
    """
    Full debug output with all internal diagnostics.
    For operator troubleshooting and audit trail.
    """
    lines = []
    lines.append(f"{'#'*60}")
    lines.append(f"  DEBUG: {trade.ticker} — {trade.idea_direction.value}")
    lines.append(f"{'#'*60}")

    # IDs
    lines.append(f"  idea_id: {trade.idea_id}")
    lines.append(f"  thesis_id: {trade.thesis_id}")
    lines.append(f"  run_id: {trade.run_id}")

    # Production fields (included in debug too)
    lines.append(f"  thesis: {idea.thesis}")
    lines.append(f"  catalyst: {idea.catalyst}")
    lines.append(f"  catalyst_action: {idea.catalyst_action}")
    lines.append(f"  cds_score: {idea.cds_score}")
    lines.append(f"  tape_read: {idea.tape_read}")
    lines.append(f"  confidence: {trade.confidence}/10 (raw: {trade.confidence_raw})")
    lines.append(f"  urgency: {trade.urgency}/10 (raw: {trade.urgency_raw})")
    lines.append(f"  consensus_tag: {trade.consensus_tag.value}")
    lines.append(f"  model_sources: {idea.model_sources}")

    # Thesis matching
    lines.append(f"  matched_existing_thesis: {idea.matched_existing_thesis_id}")
    lines.append(f"  match_confidence: {idea.match_confidence:.3f}")
    lines.append(f"  thesis_status: {idea.thesis_status.value}")

    # Structure
    lines.append(f"  strategy: {trade.strategy_label}")
    lines.append(f"  expression_type: {trade.trade_expression_type.value}")
    lines.append(f"  strikes: {trade.strike_1} / {trade.strike_2}")
    lines.append(f"  expiry: {trade.expiry} (DTE={trade.dte})")
    lines.append(f"  entry: {trade.entry_price} | target: {trade.target_price} | stop: {trade.stop_price}")
    lines.append(f"  max_loss: {trade.max_loss} | max_gain: {trade.max_gain} | R/R: {trade.risk_reward}")

    # Greeks
    lines.append(f"  iv_at_entry: {trade.iv_at_entry}")
    lines.append(f"  iv_hv_ratio: {trade.iv_hv_ratio}")
    lines.append(f"  delta: {trade.delta} | theta: {trade.theta}")

    # Sizing
    lines.append(f"  kelly_size_pct: {trade.kelly_size_pct}")
    lines.append(f"  adjusted_size_pct: {trade.adjusted_size_pct}")
    lines.append(f"  contracts: {trade.contracts}")

    # Execution impact
    lines.append(f"  slippage_pct: {trade.estimated_slippage_pct}")
    lines.append(f"  liquidity_score: {trade.liquidity_score}")

    # Gate results
    if gate_results:
        lines.append("")
        lines.append("  --- GATE RESULTS ---")
        for gate_name, details in gate_results.items():
            lines.append(f"  {gate_name}:")
            if isinstance(details, dict):
                for k, v in details.items():
                    lines.append(f"    {k}: {v}")
            else:
                lines.append(f"    {details}")

    # Simulation verdict
    if sim_verdict:
        lines.append("")
        lines.append("  --- SIMULATION VERDICT ---")
        if isinstance(sim_verdict, dict):
            for k, v in sim_verdict.items():
                lines.append(f"    {k}: {v}")

    # Institutional pressure
    if pressure_data and pressure_data.get(trade.ticker):
        lines.append("")
        lines.append("  --- INSTITUTIONAL PRESSURE ---")
        for k, v in pressure_data[trade.ticker].items():
            lines.append(f"    {k}: {v}")

    # Invalidation
    lines.append(f"  invalidation: {idea.invalidation}")
    lines.append(f"  second_order: {idea.second_order}")
    lines.append(f"  crowding_risk: {idea.crowding_risk}")

    lines.append("")
    return "\n".join(lines)


def format_run_debug(trades: List[StructuredTrade],
                      ideas: List[IdeaCandidate],
                      ctx_summary: Dict,
                      gate_results: Optional[Dict] = None,
                      sim_verdicts: Optional[Dict] = None,
                      pressure_data: Optional[Dict] = None,
                      router_stats: Optional[Dict] = None) -> str:
    """Format full run output in debug mode."""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"ORCA v20 DEBUG OUTPUT — {ctx_summary.get('market_date', 'N/A')}")
    lines.append(f"{'='*60}")
    lines.append(f"  run_id: {ctx_summary.get('run_id')}")
    lines.append(f"  research_mode: {ctx_summary.get('research_mode')}")
    lines.append(f"  source_mode: {ctx_summary.get('source_mode')}")
    lines.append(f"  dry_run: {ctx_summary.get('dry_run')}")
    lines.append(f"  api_cost: ${ctx_summary.get('api_cost_usd', 0):.4f}")
    lines.append(f"  stages_completed: {ctx_summary.get('stages_completed', [])}")
    lines.append(f"  errors: {ctx_summary.get('errors_count', 0)}")
    lines.append("")

    # Router stats
    if router_stats:
        lines.append("--- ROUTER STATS ---")
        if "role_costs" in router_stats:
            for role, cost in router_stats["role_costs"].items():
                lines.append(f"  {role}: ${cost:.4f}")
        if "provider_health" in router_stats:
            for provider, health in router_stats["provider_health"].items():
                lines.append(f"  {provider}: {health}")
        lines.append("")

    if not trades:
        lines.append("No trades generated this run.")
        return "\n".join(lines)

    lines.append(f"{len(trades)} trade(s) generated:")
    lines.append("")

    idea_map = {i.idea_id: i for i in ideas}
    for trade in trades:
        idea = idea_map.get(trade.idea_id, IdeaCandidate())
        ticker_gates = gate_results.get(trade.ticker, {}) if gate_results else None
        ticker_sim = sim_verdicts.get(trade.ticker, None) if sim_verdicts else None
        lines.append(format_trade_debug(trade, idea, ticker_gates, ticker_sim, pressure_data))

    return "\n".join(lines)
