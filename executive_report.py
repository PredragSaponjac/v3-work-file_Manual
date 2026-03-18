#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Executive Report Writer (Stage 7)
=============================================
Converts approved pipeline survivors into polished, public-facing reports
for Telegram and X. Uses the ORCA Executive Report Writer v2.0 prompt.

This is a COMMUNICATION LAYER ONLY:
  - Never upgrades or downgrades a trade
  - Never invents missing facts
  - Only explains the already-approved result
  - Non-blocking: if report fails, the trade alert still goes out

Usage:
  # Called from pipeline.py after trade logging + sheet sync
  from executive_report import generate_executive_reports
  reports = generate_executive_reports(survivors)

  # Standalone test
  python executive_report.py
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SEND_EXEC_REPORT = True
SEND_EXEC_REPORT_TOP_N = 5          # Max reports per pipeline run
SEND_EXEC_REPORT_MIN_CONFIDENCE = 1  # All survivors get reports (pipeline already filters)

# Google Sheet link — appended to every report
GOOGLE_SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID", "1nKlWFb3nUn94bfMqm_WBo_DZLnMUrdCiDprAKpJb1F4"
)
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"

# Confidence mapping: text → numeric for comparison
CONFIDENCE_MAP = {
    "low": 5, "medium": 7, "high": 8, "very high": 9,
}

# Model for report generation — use Claude Sonnet for cost efficiency
# (the prompt is purely editorial, doesn't need Opus reasoning)
REPORT_MODEL = "claude-sonnet-4-6"
REPORT_MAX_TOKENS = 2000


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT (ORCA Executive Report Writer v2.0)
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# REPORT INPUT ASSEMBLY
# ─────────────────────────────────────────────────────────────────────────────
def _build_report_input(survivor: Dict) -> str:
    """
    Assemble all available pipeline data into a structured input block
    for the report writer prompt. Uses only fields that exist — the prompt
    handles missing sections gracefully per its MISSING-INPUT RULES.
    """
    sections = []

    # ── Report Framing (set upstream by pipeline, not by this writer) ──
    framing = survivor.get("report_framing", "")
    label = survivor.get("report_label", "")
    if framing:
        sections.append("=== REPORT FRAMING (set by pipeline — do not override) ===")
        sections.append(f"Final framing: {framing}")
        sections.append(f"Headline posture: {label}")
        sections.append("Use this framing for the headline and conclusion tone.")
        sections.append("Do NOT default to raw R1 direction if framing is more cautious.")
        sections.append("")

    # ── Final Idea Card ──
    sections.append("=== FINAL APPROVED IDEA CARD ===")
    sections.append(f"Ticker: {survivor.get('ticker', '?')}")
    sections.append(f"Direction: {survivor.get('direction', '?')}")
    sections.append(f"Confidence: {survivor.get('confidence', '?')}")

    catalyst = survivor.get("catalyst", "")
    if catalyst:
        sections.append(f"Catalyst: {catalyst}")

    thesis = survivor.get("thesis", "")
    if thesis:
        sections.append(f"Thesis: {thesis}")

    why_cleanest = survivor.get("why_cleanest_expression", "")
    if why_cleanest:
        sections.append(f"Why cleanest expression: {why_cleanest}")

    underreaction = survivor.get("underreaction_detail", "")
    if underreaction:
        sections.append(f"What already moved vs what has not: {underreaction}")

    repricing = survivor.get("repricing_window", "")
    if repricing:
        sections.append(f"Repricing window: {repricing}")

    invalidation = survivor.get("invalidation", "")
    if invalidation:
        sections.append(f"Main invalidation: {invalidation}")

    source_quality = survivor.get("source_quality", "")
    if source_quality:
        sections.append(f"Source quality: {source_quality}")

    # ── Options / OI / Dark-Pool Read (Stage 2) ──
    flow = survivor.get("flow_details", {})
    if flow and isinstance(flow, dict):
        sections.append("")
        sections.append("=== OPTIONS / OI / DARK-POOL READ ===")

        tape = flow.get("tape_read", survivor.get("tape_read", ""))
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
    elif survivor.get("tape_read"):
        sections.append("")
        sections.append("=== OPTIONS / OI / DARK-POOL READ ===")
        sections.append(f"Tape read: {survivor['tape_read']}")

    # ── Catalyst Confirmation (Stage 3) ──
    conf = survivor.get("confirmation_details", {})
    if conf and isinstance(conf, dict):
        sections.append("")
        sections.append("=== CATALYST CONFIRMATION ===")

        health = conf.get("catalyst_health", survivor.get("catalyst_health", ""))
        if health:
            sections.append(f"Catalyst health: {health}")

        action = conf.get("action", survivor.get("catalyst_action", ""))
        if action:
            sections.append(f"Recommended action: {action}")

        cds = conf.get("effective_cds", survivor.get("cds_score", ""))
        if cds:
            sections.append(f"Catalyst Durability Score: {cds}/100")

        kill_reason = conf.get("kill_reason", "")
        if kill_reason:
            sections.append(f"Concerns: {kill_reason}")
    elif survivor.get("catalyst_health"):
        sections.append("")
        sections.append("=== CATALYST CONFIRMATION ===")
        sections.append(f"Catalyst health: {survivor['catalyst_health']}")
        if survivor.get("catalyst_action"):
            sections.append(f"Action: {survivor['catalyst_action']}")
        if survivor.get("cds_score"):
            sections.append(f"CDS: {survivor['cds_score']}/100")

    # ── Trade Structure (if available) ──
    strategy = survivor.get("strategy_type", "")
    if strategy and strategy not in ("UNDERLYING", "Bullish", "Bearish"):
        sections.append("")
        sections.append("=== TRADE STRUCTURE ===")
        sections.append(f"Strategy: {strategy.replace('_', ' ').title()}")
        if survivor.get("strike"):
            strike_str = f"${survivor['strike']:.0f}" if survivor['strike'] % 1 == 0 else f"${survivor['strike']:.2f}"
            if survivor.get("strike_2"):
                s2 = survivor['strike_2']
                strike_str += f" / ${s2:.0f}" if s2 % 1 == 0 else f" / ${s2:.2f}"
            sections.append(f"Strikes: {strike_str}")
        if survivor.get("expiry"):
            sections.append(f"Expiry: {survivor['expiry']}")
        if survivor.get("entry_price"):
            cr_dr = "credit" if "SELL" in strategy else "debit"
            sections.append(f"Entry: ${survivor['entry_price']:.2f} {cr_dr}")

    # ── Output target ──
    sections.append("")
    sections.append("=== OUTPUT TARGET ===")
    sections.append("Target: Telegram and X (same report for both)")

    return "\n".join(sections)


def _parse_confidence(conf_raw) -> int:
    """Convert confidence value to integer for threshold comparison.

    Handles: int/float, "8", "Medium", "Medium (reason text...)", etc.
    """
    if isinstance(conf_raw, (int, float)):
        return int(conf_raw)
    if isinstance(conf_raw, str):
        conf_lower = conf_raw.strip().lower()
        # Try numeric first
        try:
            return int(float(conf_raw))
        except (ValueError, TypeError):
            pass
        # Try exact text mapping
        mapped = CONFIDENCE_MAP.get(conf_lower)
        if mapped is not None:
            return mapped
        # Handle "Medium (reason text...)" — extract first word before parenthesis
        first_word = conf_lower.split("(")[0].strip().split()[0] if conf_lower else ""
        mapped = CONFIDENCE_MAP.get(first_word)
        if mapped is not None:
            return mapped
        # Try matching any key as prefix: "very high" in "very high (reason...)"
        for key, val in CONFIDENCE_MAP.items():
            if conf_lower.startswith(key):
                return val
    return 0


def _should_generate_report(survivor: Dict) -> bool:
    """
    Check if this survivor qualifies for an executive report.
    Filters out weak ideas and those overloaded with missing data.
    """
    # Check confidence threshold
    conf = _parse_confidence(survivor.get("confidence", 0))
    if conf < SEND_EXEC_REPORT_MIN_CONFIDENCE:
        return False

    # Skip if tape is entirely unavailable AND catalyst health is unavailable
    tape = survivor.get("tape_read", "")
    health = survivor.get("catalyst_health", "")
    if ("UNAVAILABLE" in tape.upper() and "UNAVAILABLE" in health.upper()):
        return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# REPORT GENERATION (Claude API call)
# ─────────────────────────────────────────────────────────────────────────────
def generate_report(survivor: Dict) -> Optional[Dict]:
    """
    Generate a single executive report for one survivor.

    Returns:
        {
            "ticker": str,
            "headline": str,
            "report": str,
            "cost": float,
            "elapsed": float,
        }
        or None on failure.
    """
    ticker = survivor.get("ticker", "?")

    try:
        import anthropic
    except ImportError:
        print(f"    ⚠ anthropic package not installed — cannot generate report for {ticker}")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(f"    ⚠ ANTHROPIC_API_KEY not set — cannot generate report for {ticker}")
        return None

    # Build the user message with all available data
    report_input = _build_report_input(survivor)

    print(f"    📝 Generating executive report for {ticker}...")
    t0 = time.time()

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=REPORT_MODEL,
            max_tokens=REPORT_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": report_input}],
        )

        raw_text = response.content[0].text if response.content else ""
        elapsed = time.time() - t0

        # Calculate cost (Sonnet pricing)
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cost = (input_tokens * 3.0 / 1_000_000) + (output_tokens * 15.0 / 1_000_000)

        # Parse headline and report body
        headline = ""
        report_body = raw_text

        if "Headline:" in raw_text and "Report:" in raw_text:
            parts = raw_text.split("Report:", 1)
            headline_part = parts[0]
            report_body = parts[1].strip() if len(parts) > 1 else raw_text

            # Extract headline text
            headline = headline_part.replace("Headline:", "").strip()
            # Clean up any trailing newlines
            headline = headline.split("\n")[0].strip()

        print(f"    ✅ Report generated for {ticker} ({elapsed:.1f}s, ${cost:.4f})")

        # Pass through trade structure for formatted output
        strategy = survivor.get("strategy_type", "")
        strike = survivor.get("strike")
        strike_2 = survivor.get("strike_2")
        expiry = survivor.get("expiry", "")
        entry_price = survivor.get("entry_price")
        direction = survivor.get("direction", "")

        # Build human-readable strategy label
        strategy_label = ""
        if strategy:
            _MAP = {
                "BUY_CALL": "Long Call", "BUY_PUT": "Long Put",
                "BUY_CALL_SPREAD": "Call Debit Spread",
                "BUY_PUT_SPREAD": "Put Debit Spread",
                "SELL_PUT_SPREAD": "Put Credit Spread",
                "SELL_CALL_SPREAD": "Call Credit Spread",
            }
            strategy_label = _MAP.get(strategy, strategy.replace("_", " ").title())

        return {
            "ticker": ticker,
            "headline": headline,
            "report": report_body,
            "raw_output": raw_text,
            "cost": cost,
            "elapsed": elapsed,
            # Trade structure passthrough
            "strategy_type": strategy,
            "strategy_label": strategy_label,
            "strike": strike,
            "strike_2": strike_2,
            "expiry": expiry,
            "entry_price": entry_price,
            "direction": direction,
            # Framing passthrough
            "report_framing": survivor.get("report_framing", ""),
            "report_label": survivor.get("report_label", ""),
            # Multi-model consensus passthrough
            "model_consensus": survivor.get("model_consensus", 1),
            "consensus_tag": survivor.get("consensus_tag", ""),
            "model_sources": survivor.get("model_sources", []),
        }

    except Exception as e:
        print(f"    ❌ Report generation failed for {ticker}: {e}")
        return None


def generate_executive_reports(survivors: List[Dict]) -> List[Dict]:
    """
    Generate executive reports for qualifying survivors.

    Respects SEND_EXEC_REPORT_TOP_N limit — only generates for the
    top N survivors (by confidence, then CDS score).

    Returns list of report dicts (may be empty if none qualify or all fail).
    """
    if not SEND_EXEC_REPORT:
        print("  ℹ Executive reports disabled (SEND_EXEC_REPORT = False)")
        return []

    if not survivors:
        return []

    # Filter to qualifying survivors
    qualifying = [s for s in survivors if _should_generate_report(s)]

    if not qualifying:
        print("  ℹ No survivors qualify for executive report "
              f"(min confidence: {SEND_EXEC_REPORT_MIN_CONFIDENCE})")
        return []

    # Sort by confidence (desc), then CDS (desc) — top ideas first
    qualifying.sort(
        key=lambda s: (
            _parse_confidence(s.get("confidence", 0)),
            s.get("cds_score", 0),
        ),
        reverse=True,
    )

    # Limit to top N
    to_report = qualifying[:SEND_EXEC_REPORT_TOP_N]

    print(f"\n{'=' * 60}")
    print(f"  STAGE 7: Executive Report Writer")
    print(f"  Generating {len(to_report)} report(s) "
          f"(of {len(survivors)} survivors, {len(qualifying)} qualifying)")
    print(f"{'=' * 60}")

    reports = []
    total_cost = 0.0

    for survivor in to_report:
        result = generate_report(survivor)
        if result:
            reports.append(result)
            total_cost += result.get("cost", 0)

    if reports:
        print(f"\n  📊 {len(reports)} executive report(s) generated "
              f"(cost: ${total_cost:.4f})")
    else:
        print(f"\n  ⚠ No reports generated successfully")

    return reports


# ─────────────────────────────────────────────────────────────────────────────
# TRADE STRUCTURE FORMATTING
# ─────────────────────────────────────────────────────────────────────────────
def _build_trade_block(report: Dict) -> str:
    """Build a clean one-line trade structure string."""
    strategy_label = report.get("strategy_label", "")
    strike = report.get("strike")
    strike_2 = report.get("strike_2")
    expiry = report.get("expiry", "")
    entry_price = report.get("entry_price")
    strategy = report.get("strategy_type", "")

    if not strategy_label or not strike:
        return ""

    cr_dr = "credit" if "SELL" in strategy else "debit"
    strike_str = f"${strike:.2f}" if strike else ""
    if strike_2:
        strike_str += f" / ${strike_2:.2f}"

    parts = [strategy_label]
    parts.append(f"Strike: {strike_str}")
    if expiry:
        parts.append(f"Expiry: {expiry}")
    if entry_price:
        parts.append(f"Entry: ${entry_price:.2f} {cr_dr}")

    return " | ".join(parts)


def _consensus_indicator(report: Dict) -> str:
    """
    Build multi-model consensus indicator for report footer.
    • = 1/3 models (single model found it)
    •• = 2/3 models agreed
    ••• = 3/3 UNANIMOUS (all models independently found it)
    """
    n = report.get("model_consensus", 1)
    if n >= 3:
        return "•••"
    elif n == 2:
        return "••"
    else:
        return "•"


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
def send_executive_reports_telegram(reports: List[Dict]):
    """
    Send generated reports to Telegram as separate messages.
    Each report is a standalone message — separate from the quick alert.
    Clean, institutional format — no promotional elements.
    """
    if not reports:
        return

    try:
        from notify import send_telegram
    except ImportError:
        print("  ⚠ notify.py not available — cannot send reports")
        return

    for report in reports:
        ticker = report.get("ticker", "?")
        headline = report.get("headline", "")
        body = report.get("report", "")

        if not body:
            continue

        # ── Build Telegram message (HTML) ──
        msg = f"ORCA Research — ${ticker}\n\n"
        if headline:
            msg += f"{headline}\n\n"
        msg += body

        # Trade structure
        trade_block = _build_trade_block(report)
        if trade_block:
            msg += f"\n\nStructure:\n{trade_block}"

        # Sheet link + disclaimer + consensus indicator
        msg += f'\n\n<a href="{GOOGLE_SHEET_URL}">Full trade log</a>'
        msg += "\n\nDisclosure: Research only. Not financial advice."
        msg += f"\n{_consensus_indicator(report)}"

        try:
            send_telegram(msg, parse_mode="HTML")
            print(f"  📨 Executive report sent for {ticker}")
        except Exception as e:
            print(f"  ⚠ Failed to send report for {ticker}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# X (TWITTER) OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
def format_for_x(report: Dict) -> str:
    """
    Format a report for X (Twitter).
    Clean, institutional format — no promotional elements.
    Returns the ready-to-post text.
    """
    ticker = report.get("ticker", "?")
    headline = report.get("headline", "")
    body = report.get("report", "")

    if not body:
        return ""

    # ── Build X post ──
    msg = f"ORCA Research — ${ticker}\n\n"
    if headline:
        # Ensure $cashtag in headline
        if ticker in headline and f"${ticker}" not in headline:
            headline = headline.replace(ticker, f"${ticker}")
        msg += f"{headline}\n\n"

    msg += body

    # Trade structure + footer
    trade_block = _build_trade_block(report)
    if trade_block:
        msg += f"\n\nTrade: {trade_block}"

    msg += f"\n\nTrade log: {GOOGLE_SHEET_URL}\nNot financial advice."
    msg += f"\n{_consensus_indicator(report)}"

    # Enforce X character limit
    if len(msg) > 4000:
        dots = _consensus_indicator(report)
        footer = ""
        if trade_block:
            footer += f"\n\nTrade: {trade_block}"
        footer += f"\n\nTrade log: {GOOGLE_SHEET_URL}\nNot financial advice."
        footer += f"\n{dots}"

        header = f"ORCA Research — ${ticker}\n\n"
        if headline:
            header += f"{headline}\n\n"

        available = 4000 - len(header) - len(footer) - 20
        body = body[:available] + "..."
        msg = header + body + footer

    return msg


def send_executive_report_x(report: Dict) -> bool:
    """
    Post a report to X (Twitter) using tweepy.
    Requires X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET in .env.
    Returns True on success.
    """
    x_text = format_for_x(report)
    if not x_text:
        return False

    api_key = os.environ.get("X_API_KEY", "")
    api_secret = os.environ.get("X_API_SECRET", "")
    access_token = os.environ.get("X_ACCESS_TOKEN", "")
    access_secret = os.environ.get("X_ACCESS_SECRET", "")

    if not all([api_key, api_secret, access_token, access_secret]):
        print(f"  ⚠ X/Twitter API credentials not set — saving X post to file")
        # Save to file so user can copy-paste
        x_file = Path(__file__).parent / "orca_results" / f"x_post_{report['ticker']}.txt"
        x_file.parent.mkdir(exist_ok=True)
        x_file.write_text(x_text, encoding="utf-8")
        print(f"  📄 X post saved: {x_file} ({len(x_text)} chars)")
        return False

    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret,
        )
        response = client.create_tweet(text=x_text)
        tweet_id = response.data["id"]
        print(f"  📨 X post published for ${report['ticker']}: "
              f"https://x.com/i/status/{tweet_id}")
        return True
    except ImportError:
        print("  ⚠ tweepy not installed — pip install tweepy")
        return False
    except Exception as e:
        print(f"  ⚠ X post failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# CLI — standalone test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Test with a sample survivor
    test_survivor = {
        "ticker": "EQT",
        "direction": "Bullish",
        "confidence": "8",
        "catalyst": "Natural gas prices surging on cold weather forecasts — EQT is the largest US natural gas producer",
        "thesis": "EQT has the highest natural gas revenue exposure among large-cap E&Ps. Cold snap forecasts drive Henry Hub above $3.50. Market has not fully repriced EQT despite peer moves in AR, RRC.",
        "why_cleanest_expression": "Highest pure-play natgas exposure among liquid names. AR already moved +8%. EQT lagging by 3-4%.",
        "underreaction_detail": "AR +8%, RRC +5% on same natgas catalyst. EQT only +1.5%. Market treating EQT like a diversified E&P when it is 95% natgas.",
        "repricing_window": "1-2 weeks",
        "invalidation": "Henry Hub drops below $3.00, warm weather forecast revision, or EQT-specific operational issue",
        "tape_read": "SUPPORTIVE",
        "catalyst_health": "STRENGTHENING",
        "catalyst_action": "CONFIRM",
        "cds_score": 72,
        "strategy_type": "BUY_CALL",
        "strike": 62.5,
        "expiry": "2026-04-17",
        "entry_price": 4.34,
        "flow_details": {
            "tape_read": "SUPPORTIVE",
            "options_read": "Fresh call buying at $62.50-$65.00 strikes, 2-3x normal volume. OI building on weekly cycle.",
            "darkpool_read": "Moderate block activity, net positive. Two $2M+ prints on bid side.",
            "dealer_gamma_context": "Dealers short gamma above $62. Move through $63 could accelerate.",
        },
        "confirmation_details": {
            "catalyst_health": "STRENGTHENING",
            "action": "CONFIRM",
            "effective_cds": 72,
            "kill_reason": "",
        },
    }

    print("=" * 60)
    print("  EXECUTIVE REPORT WRITER — Test Run")
    print("=" * 60)

    # Build and show input
    report_input = _build_report_input(test_survivor)
    print("\n📋 Report Input:")
    print(report_input)

    # Generate report
    result = generate_report(test_survivor)
    if result:
        print(f"\n{'=' * 60}")
        print(f"📰 HEADLINE: {result['headline']}")
        print(f"{'=' * 60}")
        print(result["report"])
        print(f"\n💰 Cost: ${result['cost']:.4f} | Time: {result['elapsed']:.1f}s")

        # Show X format
        x_text = format_for_x(result)
        print(f"\n{'=' * 60}")
        print(f"📱 X FORMAT ({len(x_text)} chars):")
        print(f"{'=' * 60}")
        print(x_text)
    else:
        print("\n❌ Report generation failed")
