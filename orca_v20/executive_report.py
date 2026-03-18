"""
ORCA v20 — Executive Report Writer.

Converts approved pipeline survivors into polished, institutional-quality reports
for Telegram and X. Uses Claude Sonnet via the v20 model router.

Adapted from V3's Executive Report Writer v2.0 — same professional tone,
same 9-section structure, same 1,800-2,500 char target.

This is a COMMUNICATION LAYER ONLY:
  - Never upgrades or downgrades a trade
  - Never invents missing facts
  - Only explains the already-approved result
  - Non-blocking: if report fails, the trade alert still goes out
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from orca_v20.config import FLAGS, MODELS, ROUTING
from orca_v20.schemas import (
    CatalystAction,
    ConsensusTag,
    IdeaCandidate,
    IdeaDirection,
    StructuredTrade,
)

logger = logging.getLogger("orca_v20.executive_report")

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────
REPORT_TOP_N = 5                     # Max reports per pipeline run
REPORT_MIN_CONFIDENCE = 1            # All survivors get reports
REPORT_MAX_TOKENS = 2000

GOOGLE_SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID", "1nKlWFb3nUn94bfMqm_WBo_DZLnMUrdCiDprAKpJb1F4"
)
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"


# ─────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT (ORCA Executive Report Writer v2.0 — adapted for v20)
# ─────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are ORCA Executive Report Writer.

Your job is to convert a fully approved ORCA trade idea into a professional, public-facing report for X or Telegram.

You do NOT generate new ideas.
You do NOT repair weak ideas.
You do NOT invent missing facts.
You do NOT exaggerate conviction.

This is a communication layer only.

TONE AND VOICE
- Write like institutional research translated into clean public language
- Professional, dense with signal, plain English
- Adopt a slightly skeptical tone — present the thesis clearly but acknowledge where it could be wrong
- No hype, no emojis, no marketing language, no promotional phrasing
- No chest-thumping, no exaggerated certainty
- If something is mixed, say so
- If something is unconfirmed, say so briefly
- Do not mention internal system names, scoring formulas, pipeline stages, or internal architecture
- Convert technical logic into readable explanation

TRUTH RULES
- If flow is mixed, say it is mixed
- If dark-pool read is unclear, say it is unclear
- If catalyst health is fading or colliding, say so directly
- If expected payoff range is not well-grounded, do not invent one
- Never convert uncertainty into conviction
- Never let options flow, dark-pool, or prediction-market language overshadow the real catalyst
- Never describe tape as primary thesis origin
- If a claim is unsupported by the inputs, soften it rather than invent support

MISSING-INPUT RULES
- If a required section has no usable input, say so briefly and move on
- Only use facts, numbers, and judgments explicitly present in the provided inputs
- Do not infer missing options, OI, dark-pool, catalyst-health, or timing details
- If options / OI / dark-pool input is missing, say the tape read is unavailable
- If catalyst confirmation input is missing, say catalyst-health confirmation is unavailable
- Missing evidence must reduce specificity, not trigger invention

REPORT FRAMING
The input includes a REPORT FRAMING block set by the pipeline. This is the final editorial posture:
- conviction_call → headline uses direction (e.g. "$TICKER bullish — ...")
- bullish_with_caveats / bearish_with_caveats → headline acknowledges mixed confirmation
- watchlist → headline says "watchlist" not bullish/bearish (e.g. "$TICKER watchlist — catalyst real, confirmation weakening")
- downgrade_note → headline says "thesis weakening" (e.g. "$TICKER — thesis weakening, monitoring")
- invalidated → headline says "thesis broken" (e.g. "$TICKER — thesis broken")
IMPORTANT: Never use the word "update" in headlines. These are new research reports, not updates to previous reports.
Do NOT override the framing with the raw R1 direction. If framing says "watchlist", do not write a bullish headline.
The conclusion section must match the framing: conviction_call → "Why ORCA likes it", watchlist → "Why ORCA is cautious", downgrade_note → "Why ORCA is stepping back".

REPORT STRUCTURE
Write one cohesive report in this exact order, using these exact section labels:

1) Headline
- $TICKER [framing posture] — one-line setup summary
- Use $cashtag notation
- Match the framing: conviction_call uses direction, watchlist says "watchlist", downgrade says "thesis weakening"

2) Catalyst:
- What is happening now
- Why it matters for this company
- Be specific about the driver — name the commodity, event, or data point

3) Why this name:
- Why this is the cleanest or cleaner single-name expression
- Tie to actual business exposure (revenue mix, operating leverage, geographic concentration)
- Do not overstate company-specific claims unless explicitly supported by inputs
- Keep it precise — 2-3 sentences max

4) What already moved vs what has not:
- Clearly separate what already repriced from what has not
- Be concrete and numerical where possible
- Avoid vague phrases like "lagged meaningfully" unless quantified
- If exact numbers are not in the inputs, describe the lag directionally but honestly

5) Tape:
- Tape is confirmation, not thesis origin — make this clear
- Use restrained language. Prefer phrasings like:
  "Positioning is supportive, not euphoric"
  "Options flow suggests fresh interest rather than obvious unwind"
  "The tape is not contradicting the thesis"
- Do not overclaim what options flow or dark pools prove
- Keep this to 2-3 sentences max

6) Catalyst health:
- State whether the catalyst is still active, strengthening, fading, or mixed
- Use plain language, not internal model language
- 2-3 sentences max

7) Timing:
- Explain why the window is days / 1-2 weeks / 1-3 weeks
- Tie timing to how the catalyst resolves (data release, event date, forecast cycle)
- Keep tactical and specific

8) Risk:
- One clear sentence on what breaks the thesis
- Mention the strongest counterargument
- Do not pad with generic "volatility" or "market risk" filler

9) Conclusion (framing-dependent):
- If framing is conviction_call: section title "Why ORCA likes it:" — explain visible catalyst, clean expression, measurable lag, tape confirmation
- If framing is watchlist: section title "Why ORCA is cautious:" — explain that the catalyst is real but confirmation is incomplete or weakening
- If framing is downgrade_note: section title "Why ORCA is stepping back:" — explain what changed since the original thesis
- If framing is bullish/bearish_with_caveats: section title "Why ORCA likes it, with caveats:" — explain the thesis but flag what is not confirming
- One tight paragraph max
- Do not repeat a full description of the ORCA system
- Do not use promotional language

LENGTH DISCIPLINE
- Target 1,800-2,500 characters for the report body (excluding header/footer)
- Every paragraph must earn its place
- If the draft is too long, compress wording rather than dropping sections
- Prioritize: catalyst, cleanest expression, underreaction, tape, catalyst health, invalidation

DO NOT
- do not use tables
- do not use markdown formatting including bold, italics, headers, lists, or code blocks
- do not use emojis
- do not use hype or promotional language
- do not describe ORCA using marketing phrases like "most intelligent AI" or similar
- do not include a full system description in every report
- do not bury the invalidation at the end without stating it clearly
- do not output a thread unless explicitly asked
- do not omit the catalyst-health portion
- do not omit the tape / positioning portion
- do not make the report longer just because more input fields were supplied

OUTPUT FORMAT EXACTLY

Headline:
[one line with $cashtag]

Report:
[full report body]"""


# ─────────────────────────────────────────────────────────────────────
# REPORT INPUT ASSEMBLY (v20 schemas → structured text block)
# ─────────────────────────────────────────────────────────────────────

def _build_report_input(trade: StructuredTrade, idea: IdeaCandidate) -> str:
    """
    Assemble all available pipeline data from v20 typed schemas into a
    structured input block for the report writer prompt.
    """
    sections = []

    # ── Report Framing ──
    framing = trade.report_framing or ""
    label = trade.report_label or ""
    if not framing:
        # Derive framing from idea state
        if idea.catalyst_action == CatalystAction.KILL:
            framing = "invalidated"
            label = "thesis broken"
        elif idea.catalyst_action == CatalystAction.DOWNGRADE:
            framing = "downgrade_note"
            label = "thesis weakening"
        elif idea.catalyst_action == CatalystAction.HOLD:
            framing = "watchlist"
            label = "watchlist — monitoring"
        elif trade.confidence >= 8:
            framing = "conviction_call"
            label = "Bullish" if idea.idea_direction == IdeaDirection.BULLISH else "Bearish"
        elif trade.confidence >= 6:
            d = "bullish" if idea.idea_direction == IdeaDirection.BULLISH else "bearish"
            framing = f"{d}_with_caveats"
            label = f"{d} with caveats"
        else:
            framing = "watchlist"
            label = "watchlist"

    sections.append("=== REPORT FRAMING (set by pipeline — do not override) ===")
    sections.append(f"Final framing: {framing}")
    sections.append(f"Headline posture: {label}")
    sections.append("Use this framing for the headline and conclusion tone.")
    sections.append("Do NOT default to raw R1 direction if framing is more cautious.")
    sections.append("")

    # ── Final Idea Card ──
    sections.append("=== FINAL APPROVED IDEA CARD ===")
    sections.append(f"Ticker: {trade.ticker}")
    sections.append(f"Direction: {idea.idea_direction.value}")
    sections.append(f"Confidence: {trade.confidence}/10")
    sections.append(f"Urgency: {trade.urgency}/10")

    if idea.catalyst:
        sections.append(f"Catalyst: {idea.catalyst}")
    if idea.thesis:
        sections.append(f"Thesis: {idea.thesis}")
    if idea.repricing_window:
        sections.append(f"Repricing window: {idea.repricing_window}")
    if idea.invalidation:
        sections.append(f"Main invalidation: {idea.invalidation}")
    if idea.second_order:
        sections.append(f"Second-order effects: {idea.second_order}")
    if idea.crowding_risk:
        sections.append(f"Crowding risk: {idea.crowding_risk}")

    # Evidence items
    if idea.evidence:
        sections.append(f"Evidence: {'; '.join(idea.evidence[:5])}")

    # ── Options / OI / Dark-Pool Read ──
    flow = idea.flow_details
    if flow and isinstance(flow, dict):
        sections.append("")
        sections.append("=== OPTIONS / OI / DARK-POOL READ ===")

        tape = flow.get("tape_read", idea.tape_read or "")
        if tape:
            sections.append(f"Tape read: {tape}")

        options_read = flow.get("options_read", "")
        if options_read and options_read != "Data unavailable":
            sections.append(f"Options read: {options_read}")

        darkpool = flow.get("darkpool_read", "")
        if darkpool and darkpool != "Data unavailable":
            sections.append(f"Dark-pool read: {darkpool}")

        gamma = flow.get("dealer_gamma_context", "")
        if gamma and gamma != "Data unavailable":
            sections.append(f"Dealer gamma context: {gamma}")

        downgrades = flow.get("downgrade_factors", "")
        if downgrades and downgrades != "None identified":
            sections.append(f"Downgrade factors: {downgrades}")
    elif idea.tape_read:
        sections.append("")
        sections.append("=== OPTIONS / OI / DARK-POOL READ ===")
        sections.append(f"Tape read: {idea.tape_read}")

    # ── Catalyst Confirmation ──
    cat_health = idea.catalyst_health
    if cat_health and isinstance(cat_health, dict):
        sections.append("")
        sections.append("=== CATALYST CONFIRMATION ===")

        health_val = cat_health.get("catalyst_health", "")
        if health_val:
            sections.append(f"Catalyst health: {health_val}")

        action_val = cat_health.get("action", "")
        if not action_val and idea.catalyst_action:
            action_val = idea.catalyst_action.value
        if action_val:
            sections.append(f"Recommended action: {action_val}")

        cds_val = cat_health.get("effective_cds", idea.cds_score)
        if cds_val:
            sections.append(f"Catalyst Durability Score: {cds_val}/100")

        kill_reason = cat_health.get("kill_reason", "")
        if kill_reason:
            sections.append(f"Concerns: {kill_reason}")
    elif idea.catalyst_action:
        sections.append("")
        sections.append("=== CATALYST CONFIRMATION ===")
        sections.append(f"Action: {idea.catalyst_action.value}")
        if idea.cds_score:
            sections.append(f"CDS: {idea.cds_score}/100")

    # ── Trade Structure ──
    if trade.strategy_label:
        sections.append("")
        sections.append("=== TRADE STRUCTURE ===")
        sections.append(f"Strategy: {trade.strategy_label}")

        if trade.strike_1:
            strike_str = f"${trade.strike_1:.2f}"
            if trade.strike_2:
                strike_str += f" / ${trade.strike_2:.2f}"
            sections.append(f"Strikes: {strike_str}")

        if trade.expiry:
            sections.append(f"Expiry: {trade.expiry}")

        if trade.entry_price:
            cr_dr = "credit" if "SELL" in trade.trade_expression_type.value else "debit"
            sections.append(f"Entry: ${trade.entry_price:.2f} {cr_dr}")

        if trade.target_price:
            sections.append(f"Target: ${trade.target_price:.2f}")
        if trade.stop_price:
            sections.append(f"Stop: ${trade.stop_price:.2f}")
        if trade.risk_reward:
            sections.append(f"Risk/Reward: {trade.risk_reward:.1f}x")

    # ── Multi-model consensus ──
    if idea.consensus_tag != ConsensusTag.SINGLE:
        sections.append("")
        sections.append("=== MULTI-MODEL CONSENSUS ===")
        sections.append(f"Consensus: {idea.consensus_tag.value}")
        if idea.model_sources:
            sections.append(f"Models: {', '.join(idea.model_sources)}")

    # ── Output target ──
    sections.append("")
    sections.append("=== OUTPUT TARGET ===")
    sections.append("Target: Telegram and X (same report for both)")

    return "\n".join(sections)


# ─────────────────────────────────────────────────────────────────────
# REPORT GENERATION (LLM call via v20 router)
# ─────────────────────────────────────────────────────────────────────

def generate_report(trade: StructuredTrade, idea: IdeaCandidate) -> Optional[Dict]:
    """
    Generate a single executive report for one trade/idea pair.

    Uses the model routed by ROUTING.report_writer (default: claude-sonnet).

    Returns dict with headline, report body, cost, elapsed — or None on failure.
    """
    ticker = trade.ticker

    # Resolve model from router
    model_spec = ROUTING.get_model("report_writer")

    if model_spec.provider != "anthropic":
        logger.warning(f"[exec_report] report_writer routed to {model_spec.provider} "
                       f"— only anthropic supported for now, skipping {ticker}")
        return None

    try:
        import anthropic
    except ImportError:
        logger.warning(f"[exec_report] anthropic package not installed — cannot generate report for {ticker}")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning(f"[exec_report] ANTHROPIC_API_KEY not set — cannot generate report for {ticker}")
        return None

    # Build structured input
    report_input = _build_report_input(trade, idea)

    logger.info(f"[exec_report] Generating executive report for {ticker}...")
    t0 = time.time()

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=model_spec.model_id,
            max_tokens=REPORT_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": report_input}],
        )

        raw_text = response.content[0].text if response.content else ""
        elapsed = time.time() - t0

        # Calculate cost
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cost = (input_tokens * model_spec.cost_per_1k_input / 1000) + \
               (output_tokens * model_spec.cost_per_1k_output / 1000)

        # Parse headline and report body
        headline = ""
        report_body = raw_text

        if "Headline:" in raw_text and "Report:" in raw_text:
            parts = raw_text.split("Report:", 1)
            headline_part = parts[0]
            report_body = parts[1].strip() if len(parts) > 1 else raw_text
            headline = headline_part.replace("Headline:", "").strip()
            headline = headline.split("\n")[0].strip()

        logger.info(f"[exec_report] Report generated for {ticker} "
                    f"({elapsed:.1f}s, ${cost:.4f}, {len(report_body)} chars)")

        return {
            "ticker": ticker,
            "headline": headline,
            "report": report_body,
            "raw_output": raw_text,
            "cost": cost,
            "elapsed": elapsed,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }

    except Exception as e:
        logger.error(f"[exec_report] Report generation failed for {ticker}: {e}")
        return None


def generate_executive_reports(
    trades: List[StructuredTrade],
    ideas: List[IdeaCandidate],
) -> List[Dict]:
    """
    Generate executive reports for qualifying trade/idea pairs.

    Respects REPORT_TOP_N limit — generates for the top N trades by confidence.

    Returns list of report dicts (may be empty).
    """
    if not FLAGS.publish_reports:
        logger.info("[exec_report] Executive reports disabled (publish_reports = False)")
        return []

    if not trades:
        return []

    idea_map = {i.idea_id: i for i in ideas}

    # Filter to qualifying trades
    # Skip KILL/DOWNGRADE — they get short alerts, not full LLM reports
    qualifying = []
    for t in trades:
        if t.confidence < REPORT_MIN_CONFIDENCE:
            continue
        framing = t.report_framing or ""
        if framing in ("downgrade_note", "invalidated"):
            logger.info(f"[exec_report] Skipping {t.ticker}: framing={framing} "
                        f"(short alert only)")
            continue
        idea = idea_map.get(t.idea_id, IdeaCandidate())
        action = getattr(idea, 'catalyst_action', None)
        if action and action.value in ("KILL", "DOWNGRADE"):
            logger.info(f"[exec_report] Skipping {t.ticker}: action={action.value} "
                        f"(short alert, no LLM report)")
            continue
        qualifying.append((t, idea))

    if not qualifying:
        logger.info(f"[exec_report] No trades qualify for executive report "
                    f"(min confidence: {REPORT_MIN_CONFIDENCE})")
        return []

    # Sort by confidence desc
    qualifying.sort(key=lambda pair: pair[0].confidence, reverse=True)
    to_report = qualifying[:REPORT_TOP_N]

    logger.info(f"[exec_report] Generating {len(to_report)} executive report(s) "
                f"(of {len(trades)} trades)")

    reports = []
    total_cost = 0.0

    for trade, idea in to_report:
        result = generate_report(trade, idea)
        if result:
            # Attach trade/idea references for downstream formatting
            result["trade"] = trade
            result["idea"] = idea
            reports.append(result)
            total_cost += result.get("cost", 0)

    if reports:
        logger.info(f"[exec_report] {len(reports)} report(s) generated "
                    f"(total cost: ${total_cost:.4f})")
    else:
        logger.warning("[exec_report] No reports generated successfully")

    return reports


# ─────────────────────────────────────────────────────────────────────
# FORMATTING HELPERS
# ─────────────────────────────────────────────────────────────────────

def _build_trade_block(trade: StructuredTrade) -> str:
    """Build a clean multi-line trade structure block."""
    if not trade.strategy_label:
        return ""

    cr_dr = "credit" if "SELL" in trade.trade_expression_type.value else "debit"

    # Line 1: strategy + strikes + expiry
    line1_parts = [trade.strategy_label]
    if trade.strike_1:
        strike_str = f"${trade.strike_1:.2f}"
        if trade.strike_2:
            strike_str += f" / ${trade.strike_2:.2f}"
        line1_parts.append(f"Strike: {strike_str}")
    if trade.expiry:
        line1_parts.append(f"Expiry: {trade.expiry}")
    if trade.entry_price:
        line1_parts.append(f"Entry: ${trade.entry_price:.2f} {cr_dr}")

    lines = [" | ".join(line1_parts)]

    # Line 2: target / stop / R:R / sizing (if available)
    line2_parts = []
    if trade.target_price:
        line2_parts.append(f"Target: ${trade.target_price:.2f}")
    if trade.stop_price:
        line2_parts.append(f"Stop: ${trade.stop_price:.2f}")
    if trade.risk_reward:
        line2_parts.append(f"R/R: {trade.risk_reward:.1f}x")
    if trade.contracts:
        size_pct = f" ({trade.adjusted_size_pct * 100:.1f}%)" if trade.adjusted_size_pct else ""
        line2_parts.append(f"Size: {trade.contracts}x{size_pct}")

    if line2_parts:
        lines.append(" | ".join(line2_parts))

    return "\n".join(lines)


def _consensus_indicator(idea: IdeaCandidate) -> str:
    """
    Build multi-model consensus indicator for report footer.
    dot = 1/3 models, dotdot = 2/3, dotdotdot = 3/3 UNANIMOUS
    """
    n = len(idea.model_sources) if idea.model_sources else 1
    if n >= 3:
        return "***"
    elif n == 2:
        return "**"
    else:
        return "*"


# ─────────────────────────────────────────────────────────────────────
# TELEGRAM FORMATTING
# ─────────────────────────────────────────────────────────────────────

def format_telegram_report(report: Dict) -> str:
    """
    Format a full executive report for Telegram.
    Institutional style, HTML parse mode, up to 4,000 chars.
    """
    ticker = report.get("ticker", "?")
    headline = report.get("headline", "")
    body = report.get("report", "")
    trade = report.get("trade")
    idea = report.get("idea")

    if not body:
        return ""

    # Build Telegram message (HTML)
    msg = f"ORCA Research — ${ticker}\n\n"
    if headline:
        msg += f"{headline}\n\n"
    msg += body

    # Trade structure — only show for conviction/caveats framing, NOT watchlist/downgrade
    framing = (trade.report_framing or "") if trade else ""
    show_trade = framing in ("conviction_call", "bullish_with_caveats",
                             "bearish_with_caveats", "")
    if trade and show_trade:
        trade_block = _build_trade_block(trade)
        if trade_block:
            msg += f"\n\nStructure:\n{trade_block}"

    # Footer: thesis_id, sheet link, disclaimer, consensus
    footer = ""
    if trade and trade.thesis_id:
        footer += f"\n\nThesis: {trade.thesis_id[:12]}"
    if trade and trade.run_id:
        footer += f" | Run: {trade.run_id[:12]}"
    footer += f'\n<a href="{GOOGLE_SHEET_URL}">Full trade log</a>'
    footer += "\n\nDisclosure: Research only. Not financial advice."
    if idea:
        footer += f"\n{_consensus_indicator(idea)}"

    msg += footer

    # Telegram limit is 4096
    if len(msg) > 4000:
        # Trim the body to fit
        overhead = len(msg) - len(body)
        max_body = 4000 - overhead - 10
        body = body[:max_body] + "..."
        # Rebuild
        msg = f"ORCA Research — ${ticker}\n\n"
        if headline:
            msg += f"{headline}\n\n"
        msg += body
        if trade and show_trade:
            trade_block = _build_trade_block(trade)
            if trade_block:
                msg += f"\n\nStructure:\n{trade_block}"
        msg += footer

    return msg


# ─────────────────────────────────────────────────────────────────────
# X (TWITTER) FORMATTING
# ─────────────────────────────────────────────────────────────────────

def format_x_report(report: Dict) -> str:
    """
    Format a full executive report for X (Twitter).
    Same institutional style, up to 4,000 chars (not 280).
    """
    ticker = report.get("ticker", "?")
    headline = report.get("headline", "")
    body = report.get("report", "")
    trade = report.get("trade")
    idea = report.get("idea")

    if not body:
        return ""

    # Build X post
    msg = f"ORCA Research — ${ticker}\n\n"
    if headline:
        # Ensure $cashtag in headline
        if ticker in headline and f"${ticker}" not in headline:
            headline = headline.replace(ticker, f"${ticker}")
        msg += f"{headline}\n\n"

    msg += body

    # Trade structure — only show for conviction/caveats framing, NOT watchlist/downgrade
    framing = (trade.report_framing or "") if trade else ""
    show_trade = framing in ("conviction_call", "bullish_with_caveats",
                             "bearish_with_caveats", "")
    if trade and show_trade:
        trade_block = _build_trade_block(trade)
        if trade_block:
            msg += f"\n\nTrade: {trade_block}"

    msg += f"\n\nTrade log: {GOOGLE_SHEET_URL}\nNot financial advice."
    if idea:
        msg += f"\n{_consensus_indicator(idea)}"

    # Enforce X character limit (4,000 for long posts)
    if len(msg) > 4000:
        overhead = len(msg) - len(body)
        max_body = 4000 - overhead - 10
        body = body[:max_body] + "..."
        # Rebuild
        msg = f"ORCA Research — ${ticker}\n\n"
        if headline:
            msg += f"{headline}\n\n"
        msg += body
        if trade and show_trade:
            trade_block = _build_trade_block(trade)
            if trade_block:
                msg += f"\n\nTrade: {trade_block}"
        msg += f"\n\nTrade log: {GOOGLE_SHEET_URL}\nNot financial advice."
        if idea:
            msg += f"\n{_consensus_indicator(idea)}"

    return msg
