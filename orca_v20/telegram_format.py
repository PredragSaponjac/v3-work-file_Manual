"""
ORCA v20 — Telegram Message Formatting.

Two publish surfaces, one analyst voice:
    1. ORCA Research  — conviction / caveated conviction (LONG or SHORT)
    2. ORCA Alert     — invalidation / kill / thesis broken

Middle states (WATCHLIST, DOWNGRADE, HOLD, MONITOR) do NOT get
standalone Telegram reports or X posts. They appear ONLY in:
    - Google Sheet rows
    - Daily summary under "Watchlist" section (ticker + confidence only)

Design principle: if it isn't actionable or dead, it's not a message.
Non-actionable names should never appear as if they are trade reports.

This is a FORMATTING MODULE ONLY:
    - Does not touch trading logic, gating, or model orchestration.
    - Does not generate content — only arranges existing data.
"""

import html
import logging
from typing import Any, Dict, List, Optional

from orca_v20.executive_report import GOOGLE_SHEET_URL
from orca_v20.schemas import (
    CatalystAction,
    ConsensusTag,
    IdeaCandidate,
    IdeaDirection,
    StructuredTrade,
    ThesisStatus,
)
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.telegram_format")

TELEGRAM_MAX_CHARS = 4000

# ─────────────────────────────────────────────────────────────────────
# Canonical vocabulary — the only public-facing labels we use
# ─────────────────────────────────────────────────────────────────────

_STATUS_MAP = {
    "conviction_call": "Conviction",
    "bullish_with_caveats": "Conviction, but mixed confirmation",
    "bearish_with_caveats": "Conviction, but mixed confirmation",
    "watchlist": "Watchlist",
    "downgrade_note": "Thesis weakening",
    "invalidated": "Thesis broken",
}

_ACTION_MAP = {
    "conviction_call": "candidate is actionable",
    "bullish_with_caveats": "candidate is actionable",
    "bearish_with_caveats": "candidate is actionable",
    "watchlist": "monitoring only",
    "downgrade_note": "monitoring only",
    "invalidated": "exit / thesis invalidated",
}

# Normalize internal health enums → clean public language
_HEALTH_MAP = {
    "STRONG": "Rising",
    "RISING": "Rising",
    "IMPROVING": "Rising",
    "HEALTHY": "Stable",
    "STABLE": "Stable",
    "NEUTRAL": "Stable",
    "WEAKENING": "Fading",
    "FADING": "Fading",
    "DETERIORATING": "Fading",
    "DEAD": "Fading",
    "MIXED": "Mixed",
}

_CONSENSUS_MAP = {
    ConsensusTag.UNANIMOUS: "Strong",
    ConsensusTag.MAJORITY: "Mixed",
    ConsensusTag.SPLIT: "Limited",
    ConsensusTag.SINGLE: "Single-model",
}

# Normalize internal action_bucket for sheet rows
ACTION_BUCKET_LABELS = {
    "CONFIRM": "Conviction",
    "DOWNGRADE": "Thesis weakening",
    "KILL": "Thesis broken",
    "HOLD": "Watchlist",
    "UNKNOWN": "Pending",
}


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """HTML-escape user-generated text for Telegram HTML mode."""
    if not text:
        return ""
    return html.escape(str(text), quote=False)


def _direction_label(idea: IdeaCandidate) -> str:
    if idea.idea_direction == IdeaDirection.BULLISH:
        return "Bullish"
    elif idea.idea_direction == IdeaDirection.BEARISH:
        return "Bearish"
    return "Neutral"


def _catalyst_health_label(idea: IdeaCandidate) -> str:
    """Convert internal catalyst health to canonical public label."""
    health = idea.catalyst_health
    if health and isinstance(health, dict):
        raw = health.get("catalyst_health", "")
    else:
        raw = str(health) if health else ""

    raw_upper = raw.upper().strip()
    return _HEALTH_MAP.get(raw_upper, raw.capitalize() if raw else "Unknown")


def _consensus_label(idea: IdeaCandidate) -> str:
    return _CONSENSUS_MAP.get(idea.consensus_tag, "Single-model")


def _resolve_framing(trade: StructuredTrade, idea: IdeaCandidate) -> str:
    """Resolve the canonical framing string from trade + idea state."""
    framing = trade.report_framing or ""
    if framing:
        return framing

    action = idea.catalyst_action
    if action == CatalystAction.KILL:
        return "invalidated"
    elif action == CatalystAction.DOWNGRADE:
        return "downgrade_note"
    elif action == CatalystAction.HOLD:
        return "watchlist"
    elif trade.confidence >= 8:
        return "conviction_call"
    elif trade.confidence >= 6:
        d = "bullish" if idea.idea_direction == IdeaDirection.BULLISH else "bearish"
        return f"{d}_with_caveats"
    return "watchlist"


def _public_status(framing: str) -> str:
    return _STATUS_MAP.get(framing, "Watchlist")


def _public_action(framing: str) -> str:
    return _ACTION_MAP.get(framing, "monitoring only")


def _surface_for_framing(framing: str) -> str:
    """
    Return which Telegram surface a framing belongs to.

    Publish policy:
        research  → standalone ORCA Research report (Telegram + X)
        alert     → standalone ORCA Alert (Telegram only)
        silent    → NO standalone message (sheet + daily summary only)
    """
    if framing in ("conviction_call", "bullish_with_caveats", "bearish_with_caveats"):
        return "research"
    elif framing == "invalidated":
        return "alert"
    # Middle states: watchlist, downgrade_note, hold — summary/sheet only
    return "silent"


def _sheet_link_html() -> str:
    return f'<a href="{GOOGLE_SHEET_URL}">Full trade log</a>'


def _build_trade_line(trade: StructuredTrade) -> str:
    """Build a single-line trade structure summary (V3 style)."""
    if not trade.strategy_label:
        return ""
    parts = [trade.strategy_label]
    if trade.strike_1:
        s = f"${trade.strike_1:.0f}"
        if trade.strike_2:
            s += f"/${trade.strike_2:.0f}"
        parts.append(s)
    if trade.entry_price:
        cr_dr = "credit" if "SELL" in trade.trade_expression_type.value else "debit"
        parts.append(f"${trade.entry_price:.2f} {cr_dr}")
    if trade.expiry:
        parts.append(f"Exp {trade.expiry}")
    return " | ".join(parts)


def _build_targets_line(trade: StructuredTrade) -> str:
    """Build target/stop/R:R line."""
    parts = []
    if trade.target_price:
        parts.append(f"Target ${trade.target_price:.2f}")
    if trade.stop_price:
        parts.append(f"Stop ${trade.stop_price:.2f}")
    if trade.risk_reward:
        parts.append(f"R/R {trade.risk_reward:.1f}x")
    return " | ".join(parts)


def _truncate_to_limit(msg: str, limit: int = TELEGRAM_MAX_CHARS) -> str:
    """
    Deterministic truncation preserving header, action, footer.

    Priority (kept first):
        1. Title + headline (first 6 lines)
        2. Footer (last 8 lines: action, sheet link, disclosure)
        3. Middle narrative (trimmed progressively)
    """
    if len(msg) <= limit:
        return msg

    lines = msg.split("\n")

    if len(lines) <= 16:
        return msg[:limit - 20] + "\n\n..."

    head = lines[:6]
    tail = lines[-8:]
    middle = lines[6:-8]

    while middle and len("\n".join(head + middle + tail)) > limit - 5:
        middle.pop()

    if middle:
        middle.append("...")

    result = "\n".join(head + middle + tail)
    if len(result) > limit:
        result = result[:limit - 20] + "\n\n..."
    return result


# ─────────────────────────────────────────────────────────────────────
# Surface 1: ORCA Research
#
# Full research note — conviction or caveated conviction.
# When an LLM executive report is available, use it as the body
# (V3 style: the report IS the message, not a field assembly).
# When no LLM report, assemble a clean narrative fallback.
# ─────────────────────────────────────────────────────────────────────

def format_research(trade: StructuredTrade, idea: IdeaCandidate,
                    report: Optional[Dict] = None) -> str:
    """
    Full research note — reads like institutional research, not a status card.

    When an LLM-generated executive report exists, it flows directly into
    the message body (matching V3's descriptive quality). Otherwise falls
    back to a structured-but-narrative assembly from pipeline fields.

    Target: 1,600–2,500 chars. Max: 4,000.
    """
    framing = _resolve_framing(trade, idea)
    ticker = _esc(trade.ticker)

    headline = ""
    report_body = ""
    if report:
        headline = report.get("headline", "")
        report_body = report.get("report", "")

    lines = []

    # ── Title ──
    lines.append(f"ORCA Research — ${ticker}")
    lines.append("")

    # ── Headline ──
    if headline:
        lines.append(_esc(headline))
    else:
        direction = _direction_label(idea).lower()
        status = _public_status(framing).lower()
        lines.append(f"${ticker} {direction} — {status}")
    lines.append("")

    # ── Meta line ──
    meta = f"Direction: {_direction_label(idea)} | Confidence: {trade.confidence}/10"
    health = _catalyst_health_label(idea)
    meta += f" | Catalyst health: {health}"
    consensus = _consensus_label(idea)
    if consensus != "Single-model":
        meta += f" | Consensus: {consensus}"
    lines.append(meta)
    lines.append("")

    # ── Body ──
    if report_body:
        # LLM-generated report: use it directly (V3 pattern).
        # This gives us the full narrative flow — catalyst, why this name,
        # what moved vs what hasn't, tape, timing, risk, conclusion.
        lines.append(_esc(report_body))
    else:
        # Fallback: assemble narrative from structured fields.
        # Written as prose, not labeled dashboard blocks.
        if idea.catalyst:
            lines.append(_esc(idea.catalyst[:500]))
            lines.append("")

        if idea.thesis:
            lines.append(_esc(idea.thesis[:500]))
            lines.append("")

    # ── Trade structure (conviction only) ──
    if framing in ("conviction_call", "bullish_with_caveats", "bearish_with_caveats"):
        trade_line = _build_trade_line(trade)
        if trade_line:
            lines.append("")
            lines.append(f"Structure: {trade_line}")
            targets = _build_targets_line(trade)
            if targets:
                lines.append(targets)

    # ── Invalidation ──
    if idea.invalidation:
        lines.append("")
        lines.append(f"Invalidation: {_esc(idea.invalidation[:300])}")

    # ── Action ──
    lines.append("")
    lines.append(f"Action: {_public_action(framing)}")

    # ── Footer ──
    lines.append("")
    lines.append(_sheet_link_html())
    lines.append("")
    lines.append("Disclosure: Research only. Not investment advice.")

    msg = "\n".join(lines)
    return _truncate_to_limit(msg)


# ─────────────────────────────────────────────────────────────────────
# Surface 2: ORCA Monitor — DISABLED
#
# Policy: middle states (watchlist/downgrade/hold) do NOT get
# standalone Telegram reports. They appear in sheet + daily summary.
# format_monitor() kept as a no-op stub for backward compatibility.
# ─────────────────────────────────────────────────────────────────────

def format_monitor(trade: StructuredTrade, idea: IdeaCandidate) -> None:
    """
    DISABLED — middle states no longer get standalone Telegram messages.

    Watchlist / downgrade / hold names appear ONLY in:
        - Google Sheet rows
        - Daily summary "Watchlist" section (ticker + confidence)

    Returns None. Callers should check for None before sending.
    """
    return None


# ─────────────────────────────────────────────────────────────────────
# Surface 3: ORCA Alert
#
# Kill / invalidation. Clear, compact, urgent without being dramatic.
# ─────────────────────────────────────────────────────────────────────

def format_alert(trade: StructuredTrade, idea: IdeaCandidate,
                 has_open_position: bool = False) -> str:
    """
    Alert — thesis broken.

    Position-aware action wording:
        has_open_position=True  → "Action: exit — thesis invalidated"
        has_open_position=False → "Action: stand aside — thesis invalidated"

    Urgent and clear, not dramatic. Should feel like an analyst pulling
    the plug with a brief explanation, not a panic alarm.

    Target: 400–900 chars.
    """
    ticker = _esc(trade.ticker)

    lines = []

    # ── Title ──
    lines.append(f"ORCA Alert — ${ticker}")
    lines.append("")

    # ── Headline ──
    lines.append(f"${ticker} — thesis broken")
    lines.append("")

    # ── Meta ──
    meta = f"Direction: {_direction_label(idea)} | Confidence: {trade.confidence}/10"
    meta += f" | Catalyst health: {_catalyst_health_label(idea)}"
    lines.append(meta)
    lines.append("")

    # ── What happened (narrative, not labeled blocks) ──
    reason = ""
    health = idea.catalyst_health
    if health and isinstance(health, dict):
        kill_reason = health.get("kill_reason", "")
        if kill_reason:
            reason = kill_reason
    if not reason and idea.thesis:
        reason = idea.thesis[:250]

    if reason:
        lines.append(_esc(reason[:400]))
        lines.append("")

    # ── What specifically broke ──
    if idea.invalidation:
        lines.append(f"What failed: {_esc(idea.invalidation[:250])}")
        lines.append("")

    # ── Action (position-aware) ──
    if has_open_position:
        lines.append("Action: exit — thesis invalidated")
    else:
        lines.append("Action: stand aside — thesis invalidated")

    # ── Footer ──
    lines.append("")
    lines.append(_sheet_link_html())

    msg = "\n".join(lines)
    return _truncate_to_limit(msg)


# ─────────────────────────────────────────────────────────────────────
# Auto-router: pick the right surface from trade + idea
# ─────────────────────────────────────────────────────────────────────

def format_telegram_message(trade: StructuredTrade, idea: IdeaCandidate,
                            report: Optional[Dict] = None,
                            has_open_position: Optional[bool] = None) -> Optional[str]:
    """
    Route to the correct Telegram surface based on framing.

    Args:
        has_open_position: Explicit override for position awareness.
            If None (default), derived from idea.thesis_status == ACTIVE.

    Returns:
        str  — ready-to-send Telegram HTML message (research or alert)
        None — trade is a middle state (watchlist/downgrade/hold),
               no standalone message. Caller should skip Telegram + X.
    """
    framing = _resolve_framing(trade, idea)
    surface = _surface_for_framing(framing)

    if surface == "research":
        return format_research(trade, idea, report=report)
    elif surface == "alert":
        # Derive position state from thesis lifecycle if not explicitly given
        if has_open_position is None:
            has_open_position = idea.thesis_status == ThesisStatus.ACTIVE
        return format_alert(trade, idea, has_open_position=has_open_position)
    # "silent" — no standalone publish for middle states
    return None


# ─────────────────────────────────────────────────────────────────────
# Daily Run Summary
#
# Sectioned output: Actionable / Monitoring / Invalidated.
# Clean enough to forward internally.
# ─────────────────────────────────────────────────────────────────────

def format_daily_summary(ctx: RunContext, trades: List[StructuredTrade],
                         ideas: List[IdeaCandidate], trace: Dict) -> str:
    """
    Daily run summary — sectioned, scan-friendly, no ambiguous labels.
    """
    idea_map = {i.idea_id: i for i in ideas}

    actionable = []
    monitoring = []
    invalidated = []

    for t in trades:
        idea = idea_map.get(t.idea_id, IdeaCandidate())
        framing = _resolve_framing(t, idea)
        surface = _surface_for_framing(framing)

        if surface == "research":
            direction = "Bullish" if t.idea_direction.value == "BULLISH" else "Bearish"
            label = _public_status(framing)
            actionable.append(f"${t.ticker} — {direction} | {label} | c={t.confidence}")
        elif surface == "alert":
            invalidated.append(f"${t.ticker} — thesis broken | c={t.confidence}")
        else:
            # Middle states: ticker + confidence only — no directional framing
            monitoring.append(f"${t.ticker} | c={t.confidence}")

    lines = []
    lines.append(f"<b>ORCA v20 Daily Summary — {ctx.market_date}</b>")
    lines.append(f"Run: {ctx.run_id}")
    lines.append("")

    lines.append(f"Ideas generated: {trace.get('ideas_generated', 0)}")
    lines.append(f"After gates: {trace.get('ideas_after_gates', 0)}")
    lines.append(f"Actionable: {len(actionable)}")
    lines.append(f"Watchlist: {len(monitoring)}")
    lines.append(f"Invalidated: {len(invalidated)}")
    lines.append(f"API cost: ${ctx.api_cost_usd:.2f}")
    errors = len(ctx.errors) if ctx.errors else 0
    if errors:
        lines.append(f"Errors: {errors}")
    lines.append("")

    # ── Sections ──
    if actionable:
        lines.append("<b>Actionable:</b>")
        for entry in actionable:
            lines.append(f"  {entry}")
        lines.append("")

    if monitoring:
        lines.append("<b>Watchlist:</b>")
        for entry in monitoring:
            lines.append(f"  {entry}")
        lines.append("")

    if invalidated:
        lines.append("<b>Invalidated:</b>")
        for entry in invalidated:
            lines.append(f"  {entry}")
        lines.append("")

    if not actionable and not monitoring and not invalidated:
        lines.append("No pipeline survivors today.")
        lines.append("")

    # ── Thesis momentum ──
    try:
        from orca_v20.thesis_store import get_confidence_trajectory
        trajectories = get_confidence_trajectory(ctx)
        rising = sum(1 for t in trajectories if t["trajectory"] == "RISING")
        stable = sum(1 for t in trajectories if t["trajectory"] == "STABLE")
        falling = sum(1 for t in trajectories if t["trajectory"] == "FALLING")
        if trajectories:
            lines.append(f"<b>Thesis Momentum:</b> {rising} rising, {stable} stable, {falling} falling")
            lines.append("")
    except Exception:
        pass

    # ── Footer ──
    lines.append(_sheet_link_html())

    msg = "\n".join(lines)
    return _truncate_to_limit(msg)


# ─────────────────────────────────────────────────────────────────────
# Overnight Replay Summary
# ─────────────────────────────────────────────────────────────────────

def format_replay_summary(replay_results: List[Dict],
                          budget_summary: Optional[Dict] = None,
                          ctx: Optional[RunContext] = None) -> str:
    """Nightly replay summary — compact, operator-grade."""
    if not replay_results:
        return "ORCA v20 Nightly Replay: no theses to replay tonight."

    rules_only = sum(1 for r in replay_results if r.get("replay_mode") == "RULES_ONLY")
    premium = sum(1 for r in replay_results if r.get("replay_mode") == "PREMIUM_ESCALATED")
    deferred = sum(1 for r in replay_results if r.get("replay_mode") == "DEFERRED_BUDGET")
    examples = sum(r.get("training_examples_generated", 0) for r in replay_results)

    lines = []
    lines.append("<b>ORCA v20 Nightly Replay</b>")
    lines.append("")
    lines.append(f"Theses replayed: {len(replay_results)} (rules: {rules_only}, premium: {premium}, deferred: {deferred})")
    if examples:
        lines.append(f"Training examples generated: {examples}")

    # Losses
    losses = [r for r in replay_results if r.get("realized_outcome") == "LOSS"]
    if losses:
        lines.append("")
        lines.append("<b>Losses reviewed:</b>")
        for r in losses[:3]:
            lines.append(f"  {r['ticker']}: {_esc(r.get('counterfactual_verdict', '')[:120])}")

    # Thesis momentum
    if ctx:
        try:
            from orca_v20.thesis_store import get_confidence_trajectory
            trajectories = get_confidence_trajectory(ctx)
            rising = sum(1 for t in trajectories if t["trajectory"] == "RISING")
            stable = sum(1 for t in trajectories if t["trajectory"] == "STABLE")
            falling = sum(1 for t in trajectories if t["trajectory"] == "FALLING")
            if trajectories:
                lines.append("")
                lines.append(f"<b>Thesis Momentum:</b> {rising} rising, {stable} stable, {falling} falling")
        except Exception:
            pass

    # Budget
    if budget_summary:
        lines.append("")
        lines.append(f"Overnight cost: ${budget_summary.get('total_cost', 0):.2f} | Budget remaining: ${budget_summary.get('budget_remaining', 0):.2f}")

    # Footer
    lines.append("")
    lines.append(_sheet_link_html())

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# Sheet row label normalization
# ─────────────────────────────────────────────────────────────────────

def normalize_action_bucket(raw: str) -> str:
    """
    Normalize internal action enum to clean public label for sheet rows.
    Ensures sheet column matches Telegram/X vocabulary.
    """
    return ACTION_BUCKET_LABELS.get(raw.upper().strip(), raw)
