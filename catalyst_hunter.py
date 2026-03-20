#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 Stage 1 — Delayed-Reaction Catalyst Hunter (R1 v5.0 + UW Aware)

Runs the R1 catalyst hunt using Claude + extended thinking + live news.
Now also injects market-wide Unusual Whales flow alerts for richer context.

The R1 prompt is loaded from prompts/r1_system_v5_0.txt (the full V5.0
prompt with UW flow interpretation rules attached).

Usage:
  python catalyst_hunter.py                # Run Stage 1 hunt
  python catalyst_hunter.py --dry-run      # Print prompt, don't call API
  python catalyst_hunter.py --seed "oil"   # Seeded catalyst mode
"""

import os
import sys
import json
import time
import datetime
import requests
from pathlib import Path
from typing import Optional, Dict, List

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STAGE1_MODEL = os.environ.get("STAGE1_MODEL", "claude-opus-4-6")
STAGE1_MAX_TOKENS = 32000

RESULTS_DIR = Path("orca_results")
RESULTS_DIR.mkdir(exist_ok=True)

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
# PROMPT LOADER — R1 v5.0 + Flow Module v5.7 (attached per the methodology)
# ─────────────────────────────────────────────────────────────────────────────
def _load_r1_prompt() -> str:
    """Load the R1 v5.0 system prompt + attach Flow Module as per methodology."""
    prompts_dir = Path(__file__).parent / "prompts"

    r1_file = prompts_dir / "r1_system_v5_0.txt"
    flow_file = prompts_dir / "flow_module_v5_7.txt"

    if not r1_file.exists():
        raise FileNotFoundError(f"R1 prompt not found: {r1_file}")

    r1_prompt = r1_file.read_text(encoding="utf-8")

    # Per methodology: "If Unusual Whales flow is available, also apply
    # ORCA_FLOW_MODULE during the R1 evaluation"
    if flow_file.exists():
        flow_prompt = flow_file.read_text(encoding="utf-8")
        r1_prompt += (
            "\n\n" + "=" * 60 + "\n"
            "ATTACHED: OPTIONS & DARK POOL FLOW INTERPRETATION RULES (v5.7)\n"
            "Apply these rules when interpreting any Unusual Whales flow data provided.\n"
            + "=" * 60 + "\n\n"
            + flow_prompt
        )

    return r1_prompt


# ─────────────────────────────────────────────────────────────────────────────
# CONTEXT BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
def build_news_context() -> str:
    """Scrape news feeds using analyst.py's NewsScraper if available."""
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from analyst import NewsScraper
        scraper = NewsScraper()
        news = scraper.scrape_all()
        news_text = scraper.format_for_prompt(news)
        signal_count = sum(len(v) for v in news.values())
        print(f"  📡 Scraped {signal_count} signals from news feeds")
        return news_text
    except Exception as e:
        print(f"  ⚠ News scraper import failed: {e}")
        return ""


def build_regime_context() -> str:
    """Load regime model data if available."""
    try:
        regime_file = Path("regime_prediction.json")
        if regime_file.exists():
            rd = json.loads(regime_file.read_text())
            if "error" not in rd:
                signal = rd.get("signal", "NEUTRAL")
                bias = rd.get("bias", "NEUTRAL")
                hedge = rd.get("hedge", "MODERATE")
                tail_2 = rd.get("tail_prob_2pct_5d", 0) or 0
                return (
                    f"\n=== MARKET REGIME CONTEXT ===\n"
                    f"Signal: {signal} | Bias: {bias} | Hedge: {hedge}\n"
                    f"Tail risk (5d): {tail_2:.0f}% chance of 2%+ drop\n"
                )
    except Exception:
        pass
    return ""


def build_kalshi_context() -> str:
    """Load Kalshi macro probabilities if available."""
    try:
        from analyst import fetch_kalshi_macro_events
        kalshi = fetch_kalshi_macro_events()
        if kalshi:
            print(f"  🎰 Kalshi macro events loaded")
            return f"\n{kalshi}\n"
    except Exception:
        pass
    return ""


def build_uw_market_context() -> str:
    """Fetch market-wide UW flow alerts for the R1 catalyst hunter."""
    try:
        from uw_flow import format_market_flow_summary
        flow_text = format_market_flow_summary(max_alerts=40)
        if flow_text:
            print(f"  📊 Unusual Whales market flow loaded")
            return f"\n{flow_text}\n"
    except Exception as e:
        print(f"  ⚠ UW market flow failed: {e}")
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1 API CALL
# ─────────────────────────────────────────────────────────────────────────────
def run_catalyst_hunt(seed: str = "", dry_run: bool = False) -> Optional[str]:
    """
    Run the Stage 1 Catalyst Hunter (R1 v5.0 + UW flow aware).

    Args:
        seed: Optional catalyst seed for Seeded Mode
        dry_run: Print prompt without calling API

    Returns:
        Catalyst hunter output text, or None on failure
    """
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║  🎯 ORCA V3 STAGE 1 — Catalyst Hunter (R1 v5.0 + UW)        ║
║  Model: {STAGE1_MODEL:<52s} ║
║  Thinking: adaptive (effort=high)                            ║
║  Mode: {'Seeded — ' + seed[:35] if seed else 'Open Hunt':<52s} ║
╚═══════════════════════════════════════════════════════════════╝""")

    # Gather intelligence
    print("\n  📡 Gathering intelligence...")
    news_text = build_news_context()
    regime_text = build_regime_context()
    kalshi_text = build_kalshi_context()
    uw_market_text = build_uw_market_context()

    # Build user message
    today = datetime.date.today().isoformat()
    if seed:
        mode_instruction = f"""
OPERATING MODE: Seeded Catalyst Mode
SEED CATALYST: {seed}
Start from this catalyst/theme and identify the best delayed-reaction single-name expressions.
Prioritize depth of mapping over breadth of search."""
    else:
        mode_instruction = """
OPERATING MODE: Open Hunt Mode
Search broadly for active or emerging catalysts with delayed single-name repricing.
Use the intelligence below to find catalysts the market hasn't fully priced."""

    user_message = f"""Date: {today}
Market: US equities (single-name focus)
{mode_instruction}

=== LIVE INTELLIGENCE (scraped today) ===
{news_text if news_text else "[No news data available — use your knowledge of current events]"}
{regime_text}
{kalshi_text}
{uw_market_text}

Now hunt. Find the delayed reactions. Be ruthless — reject everything that doesn't clearly qualify.
Apply the Flow Module v5.7 rules if UW flow data was provided above."""

    # Load the full R1 + Flow Module prompt
    try:
        system_prompt = _load_r1_prompt()
    except FileNotFoundError as e:
        print(f"  ❌ {e}")
        return None

    if dry_run:
        print(f"\n{'═' * 60}")
        print("DRY RUN — prompt summary:")
        print(f"{'═' * 60}")
        print(f"System prompt: {len(system_prompt):,} chars")
        print(f"User message: {len(user_message):,} chars")
        print(f"News context: {len(news_text):,} chars")
        print(f"UW flow context: {len(uw_market_text):,} chars")
        return None

    # ── MANUAL MODE: bypass API, save prompt for human ──
    if os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes"):
        from orca_v20.manual_bridge import manual_llm_call
        return manual_llm_call(
            role="hunter_primary",
            system_prompt=system_prompt,
            user_prompt=user_message,
            model_hint=f"anthropic/{STAGE1_MODEL}",
        )

    api_key = os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
    if not api_key:
        print("  ❌ ANTHROPIC_API_KEY not set")
        return None

    print(f"\n  🧠 Calling {STAGE1_MODEL} with adaptive thinking (effort=high)...")
    print(f"  📡 Using Anthropic SDK + streaming (prevents connection drops)")

    t0 = time.time()
    max_retries = 3

    # Use official Anthropic SDK with streaming.
    # Streaming sends SSE events continuously, which keeps the TCP connection
    # alive instead of sitting idle for minutes. This is the canonical fix for
    # RemoteDisconnected errors on GitHub Actions shared runners.
    try:
        import anthropic
    except ImportError:
        print("  ❌ anthropic SDK not installed — pip install anthropic")
        return None

    client = anthropic.Anthropic(
        api_key=api_key,
        max_retries=max_retries,       # SDK handles retry with exponential backoff
        timeout=900.0,                  # 15 min timeout for long thinking + web search
    )

    for attempt in range(1, max_retries + 1):
        try:
            print(f"  📡 Attempt {attempt}/{max_retries}...")

            # Stream the response — this keeps the connection alive with SSE events
            result_text = ""
            thinking_text = ""

            with client.messages.stream(
                model=STAGE1_MODEL,
                max_tokens=STAGE1_MAX_TOKENS,
                system=system_prompt,
                thinking={"type": "adaptive"},
                output_config={"effort": "high"},
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": 5,
                }],
                messages=[{"role": "user", "content": user_message}],
            ) as stream:
                # Consume stream — the connection stays alive while events flow
                response = stream.get_final_message()

            # Extract content from final message
            for block in response.content:
                if block.type == "thinking":
                    thinking_text = block.thinking or ""
                elif block.type == "text":
                    result_text += block.text or ""

            usage = response.usage
            input_tokens = usage.input_tokens
            output_tokens = usage.output_tokens
            cost = (input_tokens / 1_000_000) * 15 + (output_tokens / 1_000_000) * 75
            elapsed = time.time() - t0

            print(f"\n  ✅ Stage 1 complete in {elapsed:.1f}s")
            print(f"  📊 Tokens: {input_tokens:,} in / {output_tokens:,} out")
            print(f"  💰 Cost: ${cost:.4f}")

            if thinking_text:
                think_preview = thinking_text[:300].replace("\n", " ")
                print(f"  💭 Thinking preview: {think_preview}...")

            return result_text

        except anthropic.APIStatusError as e:
            if e.status_code in (429, 502, 503, 529):
                wait_secs = 60 * attempt
                print(f"  ⚠ Attempt {attempt}/{max_retries}: HTTP {e.status_code} "
                      f"— waiting {wait_secs}s...")
                if attempt < max_retries:
                    time.sleep(wait_secs)
                    continue
            print(f"  ❌ API error: {e.status_code} — {str(e)[:300]}")
            return None
        except anthropic.APIConnectionError as e:
            wait_secs = 30 * attempt
            print(f"  ⚠ Attempt {attempt}/{max_retries}: Connection error "
                  f"— waiting {wait_secs}s... ({e})")
            if attempt < max_retries:
                time.sleep(wait_secs)
                continue
            print(f"  ❌ All {max_retries} attempts failed with connection errors")
            return None
        except anthropic.APITimeoutError:
            print(f"  ⚠ Attempt {attempt}/{max_retries}: Timeout — retrying...")
            if attempt < max_retries:
                time.sleep(60)
                continue
            return None
        except Exception as e:
            print(f"  ❌ Unexpected error: {type(e).__name__}: {e}")
            return None

    return None


# ─────────────────────────────────────────────────────────────────────────────
# PARSER — Extract structured ideas from R1 v5.0 output
# ─────────────────────────────────────────────────────────────────────────────
def parse_catalyst_ideas(raw_text: str) -> List[Dict]:
    """Parse R1 v5.0 output into structured idea dicts."""
    if not raw_text:
        return []

    raw_stripped = raw_text.strip()
    for check_line in raw_stripped.split("\n"):
        cleaned = check_line.strip().replace("**", "").strip()
        if cleaned in ("No valid ORCA candidates found.",
                        "No valid ORCA candidates found"):
            if "IDEA #" not in raw_text and "IDEA#" not in raw_text:
                return []

    ideas = []
    current_idea = {}
    current_field = None

    # V4.8 field map — expanded
    field_map = {
        "Ticker:": "ticker",
        "Company:": "company",
        "Direction:": "direction",
        "Live / Emerging Catalyst:": "catalyst",
        "Catalyst Status:": "catalyst_status",
        "Catalyst Event Date:": "catalyst_event_date",
        "Market Visibility Date:": "market_visibility_date",
        "Hard-Date Status:": "hard_date_status",
        "Next Earnings Date:": "next_earnings_date",
        "Source quality:": "source_quality",
        "Why this company is the cleanest expression:": "cleanest_expression",
        "What already moved:": "what_moved",
        "What has already repriced in this stock": "what_repriced",
        "What has not repriced enough:": "what_lagged",
        "Expected tactical payoff range": "payoff_range",
        "Thesis": "thesis",
        "Transmission type and why lag exists:": "transmission_type",
        "Why the market may still be late:": "why_late",
        "Best reason this may already be priced:": "already_priced_risk",
        "Evidence of delayed or incomplete pricing:": "evidence",
        "Extension / continuation read:": "extension_read",
        "Options / OI / dark-pool read:": "flow_read",
        "Historical transmission check:": "historical_check",
        "Public positioning confirmation": "positioning_confirmation",
        "Practical tradability:": "practical_tradability",
        "Expected repricing window:": "repricing_window",
        "Why this is actionable now:": "actionable_now",
        "Main invalidation:": "invalidation",
        "Crowding risk:": "crowding_risk",
        "Confidence:": "confidence",
        "Key uncertainties for R2 verification:": "r2_uncertainties",
        "Known idiosyncratic risks:": "idiosyncratic_risks",
        "Source support caveat:": "source_caveat",
        "Correlation Warning:": "correlation_warning",
        "Shared root catalyst:": "shared_catalyst",
        "Why these are correlated": "correlation_reason",
        "Second-order angle:": "second_order",
        "If first-order, why it still qualifies:": "first_order_justification",
    }

    for line in raw_text.split("\n"):
        line = line.strip()
        if not line:
            current_field = None
            continue

        line_clean = line.replace("**", "").strip()

        if line_clean.startswith("IDEA #") or line_clean.startswith("IDEA#"):
            if current_idea and current_idea.get("ticker"):
                ideas.append(current_idea)
            current_idea = {"raw_text": ""}
            current_field = None
            continue

        matched = False
        for prefix, field in field_map.items():
            if line_clean.startswith(prefix):
                value = line_clean[len(prefix):].strip()
                if field == "thesis" and ":" in line_clean:
                    value = line_clean.split(":", 1)[1].strip()
                current_idea[field] = value
                current_field = field
                matched = True
                break

        if not matched and current_field:
            if current_field in current_idea:
                current_idea[current_field] += " " + line_clean
            else:
                current_idea[current_field] = line_clean

        if current_idea is not None:
            current_idea["raw_text"] = current_idea.get("raw_text", "") + line + "\n"

    if current_idea and current_idea.get("ticker"):
        ideas.append(current_idea)

    return ideas


def format_catalyst_seeds_for_analyst(ideas: List[Dict]) -> str:
    """Format parsed ideas as catalyst seeds for downstream stages."""
    if not ideas:
        return ""

    lines = [
        "=== STAGE 1 CATALYST SEEDS (R1 v5.0 + UW flow) ===",
        f"The Catalyst Hunter identified {len(ideas)} high-conviction delayed-reaction ideas.",
        "",
    ]

    for i, idea in enumerate(ideas, 1):
        ticker = idea.get("ticker", "?").strip()
        direction = idea.get("direction", "?").strip()
        catalyst = idea.get("catalyst", "?").strip()
        thesis = idea.get("thesis", "?").strip()
        what_moved = idea.get("what_moved", "").strip()
        what_lagged = idea.get("what_lagged", "").strip()
        confidence = idea.get("confidence", "?").strip()
        repricing = idea.get("repricing_window", "?").strip()
        invalidation = idea.get("invalidation", "").strip()
        crowding = idea.get("crowding_risk", "?").strip()
        flow_read = idea.get("flow_read", "").strip()

        lines.append(f"SEED #{i}: {ticker} — {direction}")
        lines.append(f"  Catalyst: {catalyst}")
        lines.append(f"  Thesis: {thesis}")
        if what_moved:
            lines.append(f"  What already moved: {what_moved}")
        if what_lagged:
            lines.append(f"  What still lags: {what_lagged}")
        if flow_read:
            lines.append(f"  Flow read: {flow_read}")
        lines.append(f"  Confidence: {confidence} | Repricing: {repricing} | Crowding: {crowding}")
        if invalidation:
            lines.append(f"  Invalidation: {invalidation}")
        lines.append("")

    lines.append("=== END CATALYST SEEDS ===")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-MODEL STAGE 1 — GPT + Gemini run the SAME R1 prompt independently
# ─────────────────────────────────────────────────────────────────────────────

def _build_hunt_messages(seed: str = "") -> tuple:
    """
    Build the system prompt + user message for catalyst hunting.
    Shared by all three models (Claude, GPT, Gemini).
    Returns (system_prompt, user_message).
    """
    # Gather intelligence (shared context)
    print("  📡 Gathering intelligence...")
    news_text = build_news_context()
    regime_text = build_regime_context()
    kalshi_text = build_kalshi_context()
    uw_market_text = build_uw_market_context()

    today = datetime.date.today().isoformat()
    if seed:
        mode_instruction = f"""
OPERATING MODE: Seeded Catalyst Mode
SEED CATALYST: {seed}
Start from this catalyst/theme and identify the best delayed-reaction single-name expressions.
Prioritize depth of mapping over breadth of search."""
    else:
        mode_instruction = """
OPERATING MODE: Open Hunt Mode
Search broadly for active or emerging catalysts with delayed single-name repricing.
Use the intelligence below to find catalysts the market hasn't fully priced."""

    user_message = f"""Date: {today}
Market: US equities (single-name focus)
{mode_instruction}

=== LIVE INTELLIGENCE (scraped today) ===
{news_text if news_text else "[No news data available — use your knowledge of current events]"}
{regime_text}
{kalshi_text}
{uw_market_text}

Now hunt. Find the delayed reactions. Be ruthless — reject everything that doesn't clearly qualify.
Apply the Flow Module v5.7 rules if UW flow data was provided above."""

    try:
        system_prompt = _load_r1_prompt()
    except FileNotFoundError as e:
        print(f"  ❌ {e}")
        return None, None

    return system_prompt, user_message


# ═══════════════════════════════════════════════════════════════════════════════
# TWO-STAGE GPT ARCHITECTURE: Generator → Adjudicator
#
# Stage A (Generator): Loose, exploratory, low reasoning + web search.
#   Finds 5-10 raw candidate theses. No rejection, just pattern matching.
#
# Stage B (Adjudicator): Strict, structured output, medium reasoning.
#   Scores each candidate with bull/bear ledger. ACCEPT / WATCHLIST / REJECT.
#
# This separates divergent thinking (exploration) from convergent thinking
# (judgment), preventing the over-rejection that plagued single-pass GPT.
# ═══════════════════════════════════════════════════════════════════════════════

# ── Generator schema: loose candidate output ──
_GPT_GENERATOR_SCHEMA = {
    "type": "json_schema",
    "name": "orca_generator_output",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string"},
                        "company": {"type": "string"},
                        "direction": {"type": "string"},
                        "catalyst": {"type": "string"},
                        "why_underreacted": {"type": "string"},
                        "what_moved": {"type": "string"},
                        "what_lagged": {"type": "string"},
                        "thesis_sketch": {"type": "string"},
                        "repricing_window": {"type": "string"},
                        "key_uncertainty": {"type": "string"},
                    },
                    "required": [
                        "ticker", "company", "direction", "catalyst",
                        "why_underreacted", "what_moved", "what_lagged",
                        "thesis_sketch", "repricing_window", "key_uncertainty",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["candidates"],
        "additionalProperties": False,
    },
}

# ── Adjudicator schema: strict scoring + decision ──
_GPT_ADJUDICATOR_SCHEMA = {
    "type": "json_schema",
    "name": "orca_adjudicator_output",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "scored_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string"},
                        "company": {"type": "string"},
                        "direction": {"type": "string"},
                        "decision": {"type": "string"},
                        "bull_score": {"type": "number"},
                        "bear_score": {"type": "number"},
                        "catalyst_confirmed": {"type": "boolean"},
                        "underreaction_evidence_count": {"type": "number"},
                        "hard_disqualifier": {"type": "string"},
                        "what_changes_my_mind": {"type": "string"},
                        "catalyst": {"type": "string"},
                        "catalyst_status": {"type": "string"},
                        "cleanest_expression": {"type": "string"},
                        "what_moved": {"type": "string"},
                        "what_lagged": {"type": "string"},
                        "thesis": {"type": "string"},
                        "transmission_type": {"type": "string"},
                        "why_late": {"type": "string"},
                        "already_priced_risk": {"type": "string"},
                        "evidence": {"type": "string"},
                        "flow_read": {"type": "string"},
                        "historical_check": {"type": "string"},
                        "practical_tradability": {"type": "string"},
                        "repricing_window": {"type": "string"},
                        "why_actionable_now": {"type": "string"},
                        "invalidation": {"type": "string"},
                        "crowding_risk": {"type": "string"},
                        "confidence": {"type": "string"},
                    },
                    "required": [
                        "ticker", "company", "direction", "decision",
                        "bull_score", "bear_score", "catalyst_confirmed",
                        "underreaction_evidence_count", "hard_disqualifier",
                        "what_changes_my_mind", "catalyst", "catalyst_status",
                        "cleanest_expression", "what_moved", "what_lagged",
                        "thesis", "transmission_type", "why_late",
                        "already_priced_risk", "evidence", "flow_read",
                        "historical_check", "practical_tradability",
                        "repricing_window", "why_actionable_now",
                        "invalidation", "crowding_risk", "confidence",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["scored_candidates"],
        "additionalProperties": False,
    },
}

# ── Generator prompt: exploratory, no rejection ──
_GPT_GENERATOR_PROMPT = """You are an exploratory market analyst. Your job is to find 5-10 potential
delayed-reaction trading candidates. Be BROAD and CREATIVE — this is brainstorming, not final selection.

For each candidate, identify:
- A real, active or recent catalyst
- What already moved (related asset, peer, commodity)
- What hasn't moved enough (the candidate stock)
- A brief thesis sketch of why the gap exists

DO NOT reject ideas at this stage. Even if you're unsure about some details, include the candidate.
Uncertainty will be evaluated in the next stage.

Think about:
- Macro shock → specific company consequence
- Geopolitical event → defense/energy/freight/supply-chain impact
- Weather event → commodity/utility/producer consequence
- Regulation → winner/loser before full repricing
- Cross-market: a related asset moved sharply, but the cleanest single-name expression is stale

Look for non-obvious second-order plays, not just headline trades.
Universe: single U.S. listed stocks / ADRs. No ETFs, no baskets.
Repricing window: days to 3 weeks preferred, up to 6 weeks if catalyst is active."""

# ── Adjudicator prompt: strict but fair evaluation ──
_GPT_ADJUDICATOR_PROMPT = """You are a trade adjudicator for an options trading system that hunts
delayed-reaction catalysts. You will receive candidate ideas from a Generator.
Your job is to score each one honestly, then classify it.

DOMAIN CONTEXT:
We trade U.S. single-name equities/ADRs via options. We look for catalysts that moved
a related asset but the cleanest single-name expression hasn't repriced yet. The downstream
pipeline has evidence gates, red-team checks, and risk filters — your job is NOT to be the
final filter. Your job is to separate signal from noise.

For EACH candidate, you MUST:
1. Score bull_score (1-10): How strong is the directional case? 7+ means you'd put money on it.
2. Score bear_score (1-10): How strong is the counter-argument? 7+ means the thesis is broken.
3. Confirm catalyst: Is there a REAL catalyst that happened or is actively unfolding? (true/false)
4. Count underreaction evidence: How many observations suggest the stock hasn't fully repriced?
   Counts: peer moved but this didn't, sector ETF diverged, options IV didn't spike, etc.
5. Check for HARD disqualifiers ONLY. A hard disqualifier is:
   - Earnings within 5 trading days (catalyst will be swamped)
   - Stock already moved 5%+ in the catalyst direction (already priced)
   - The candidate is an ETF, not a single name
   - Fundamental thesis is factually wrong (e.g., company doesn't operate in that sector)
   "Weak evidence" or "uncertain timing" are NOT hard disqualifiers — set to "none".
6. State what_changes_my_mind: What single fact would flip your decision?

Decision rules:
- ACCEPT: bull_score >= 6, bear_score <= 6, catalyst_confirmed=true, hard_disqualifier="none"
- WATCHLIST: bull_score 5-6, OR bear_score 6-7, catalyst_confirmed=true, hard_disqualifier="none"
- REJECT: hard_disqualifier present (not "none"), OR bull_score < 5, OR catalyst not confirmed

IMPORTANT: Most candidates from the Generator should be ACCEPT or WATCHLIST. If you're
rejecting more than half, you're being too strict. The downstream pipeline handles risk filtering.
Fill in ALL output fields thoroughly for ACCEPT and WATCHLIST candidates.
For REJECT, fill scores/disqualifier — other fields can be brief."""


def _gpt_responses_call(headers, model, instructions, input_text, schema,
                         reasoning_effort, web_search=False, timeout=300):
    """Helper: make a single Responses API call with structured output."""
    payload = {
        "model": model,
        "instructions": instructions,
        "input": input_text,
        "reasoning": {"effort": reasoning_effort},
        "text": {"format": schema},
        "max_output_tokens": 12000,
    }
    if web_search:
        payload["tools"] = [{
            "type": "web_search",
            "user_location": {
                "type": "approximate",
                "country": "US",
                "city": "Houston",
                "region": "Texas",
            },
        }]

    resp = requests.post(
        "https://api.openai.com/v1/responses",
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    return resp


def _extract_responses_output(resp_json):
    """Extract text content from Responses API output."""
    for item in resp_json.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content.get("text", "")
    return None


def run_catalyst_hunt_gpt(system_prompt: str, user_message: str) -> tuple:
    """
    Two-stage GPT catalyst hunt via Responses API.

    Stage A — Generator (low reasoning + web search):
      Explores broadly, outputs 5-10 raw candidate theses.
      No rejection criteria — pure divergent thinking.

    Stage B — Adjudicator (medium reasoning + structured output):
      Scores each candidate with forced bull/bear ledger.
      Decision: ACCEPT / WATCHLIST / REJECT.

    Returns (raw_json_text | None, parsed_ideas_list).
    Same interface as before — merge function doesn't know or care
    that GPT used two passes internally.
    """
    # ── MANUAL MODE: bypass API, save prompt for human ──
    if os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes"):
        from orca_v20.manual_bridge import manual_llm_call
        raw = manual_llm_call(
            role="hunter_secondary",
            system_prompt=system_prompt,
            user_prompt=user_message,
            model_hint="openai/gpt-5.4",
        )
        # Try to parse ideas from the manual response
        try:
            parsed = json.loads(raw) if raw.strip().startswith("{") or raw.strip().startswith("[") else {}
            ideas = parsed.get("ideas", parsed.get("candidates", []))
            if isinstance(ideas, list):
                return raw, ideas
        except Exception:
            pass
        return raw, []

    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        print("  ⚠ OPENAI_API_KEY not set — skipping GPT hunt")
        return None, []

    print(f"\n  🤖 GPT-5.4 Two-Stage (Generator → Adjudicator): Starting...")
    t0 = time.time()
    total_cost = 0.0

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_key}",
    }

    # ════════════════════════════════════════════════════════════════════════
    # STAGE A — GENERATOR (low reasoning + web search, broad exploration)
    # ════════════════════════════════════════════════════════════════════════
    print(f"    ⚡ Stage A (Generator): Exploring catalysts...")

    generator_input = f"""{user_message}

Search the web for the latest market-moving catalysts if needed.
Output 5-10 candidate delayed-reaction setups as structured JSON."""

    candidates = []
    for model_name in ["gpt-5.4", "o3"]:
        try:
            resp = _gpt_responses_call(
                headers, model_name, _GPT_GENERATOR_PROMPT, generator_input,
                _GPT_GENERATOR_SCHEMA, reasoning_effort="low",
                web_search=True, timeout=180,
            )

            if resp.status_code == 200:
                data = resp.json()
                raw = _extract_responses_output(data)
                if raw:
                    parsed = json.loads(raw)
                    candidates = parsed.get("candidates", [])

                    usage = data.get("usage", {})
                    inp = usage.get("input_tokens", 0)
                    out = usage.get("output_tokens", 0)
                    cost_a = inp / 1_000_000 * 2.5 + out / 1_000_000 * 15.0
                    total_cost += cost_a
                    elapsed_a = time.time() - t0

                    print(f"    ✅ Generator: {len(candidates)} candidates "
                          f"({model_name}, {elapsed_a:.1f}s, ${cost_a:.4f})")
                    for c in candidates:
                        print(f"       → {c.get('ticker', '?')} ({c.get('direction', '?')}) "
                              f"— {c.get('catalyst', '?')[:60]}")
                    break
                else:
                    print(f"    ⚠ {model_name}: No output text")
                    continue
            elif resp.status_code == 404:
                print(f"    ↳ {model_name} not available, trying next...")
                continue
            else:
                print(f"    ↳ {model_name} error {resp.status_code}: {resp.text[:200]}")
                continue
        except requests.exceptions.Timeout:
            print(f"    ↳ {model_name} timeout, trying next...")
            continue
        except Exception as e:
            print(f"    ↳ {model_name} error: {e}")
            continue

    if not candidates:
        print(f"    ❌ Generator produced no candidates")
        return None, []

    # ════════════════════════════════════════════════════════════════════════
    # STAGE B — ADJUDICATOR (medium reasoning + structured scoring)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n    ⚖️  Stage B (Adjudicator): Scoring {len(candidates)} candidates...")

    # Format candidates for adjudicator
    candidate_text = json.dumps(candidates, indent=2)
    adjudicator_input = f"""Here are {len(candidates)} candidate trading ideas from the Generator.
Evaluate each one rigorously using the scoring rules.

MARKET CONTEXT:
{user_message}

CANDIDATES TO EVALUATE:
{candidate_text}

Score each candidate. Be strict. Force the bull/bear numbers."""

    # Adjudicator uses its own prompt — NOT the full R1 prompt.
    # The R1 prompt has aggressive rejection language that causes over-rejection.
    adjudicator_instructions = _GPT_ADJUDICATOR_PROMPT

    scored = []
    for model_name in ["gpt-5.4", "o3"]:
        try:
            t_adj = time.time()
            resp = _gpt_responses_call(
                headers, model_name, adjudicator_instructions, adjudicator_input,
                _GPT_ADJUDICATOR_SCHEMA, reasoning_effort="medium",
                web_search=False, timeout=300,
            )

            if resp.status_code == 200:
                data = resp.json()
                raw = _extract_responses_output(data)
                if raw:
                    parsed = json.loads(raw)
                    scored = parsed.get("scored_candidates", [])

                    usage = data.get("usage", {})
                    inp = usage.get("input_tokens", 0)
                    out = usage.get("output_tokens", 0)
                    cost_b = inp / 1_000_000 * 2.5 + out / 1_000_000 * 15.0
                    total_cost += cost_b
                    elapsed_b = time.time() - t_adj

                    print(f"    ✅ Adjudicator: scored {len(scored)} candidates "
                          f"({model_name}, {elapsed_b:.1f}s, ${cost_b:.4f})")

                    for s in scored:
                        decision = s.get("decision", "?")
                        bull = s.get("bull_score", 0)
                        bear = s.get("bear_score", 0)
                        disq = s.get("hard_disqualifier", "none")
                        emoji = {"ACCEPT": "✅", "WATCHLIST": "⚠️", "REJECT": "❌"}.get(
                            decision, "?")
                        print(f"       {emoji} {s.get('ticker', '?')} — {decision} "
                              f"(bull:{bull} bear:{bear}) disq: {disq[:50]}")
                    break
                else:
                    print(f"    ⚠ {model_name}: No output text")
                    continue
            elif resp.status_code == 404:
                print(f"    ↳ {model_name} not available, trying next...")
                continue
            else:
                print(f"    ↳ {model_name} error {resp.status_code}: {resp.text[:200]}")
                continue
        except requests.exceptions.Timeout:
            print(f"    ↳ {model_name} timeout, trying next...")
            continue
        except Exception as e:
            print(f"    ↳ {model_name} error: {e}")
            continue

    if not scored:
        print(f"    ❌ Adjudicator produced no scores — returning Generator candidates raw")
        # Fallback: return generator candidates as-is with minimal fields
        fallback_ideas = []
        for c in candidates:
            fallback_ideas.append({
                "ticker": c.get("ticker", ""),
                "company": c.get("company", ""),
                "direction": c.get("direction", ""),
                "catalyst": c.get("catalyst", ""),
                "thesis": c.get("thesis_sketch", ""),
                "what_moved": c.get("what_moved", ""),
                "what_lagged": c.get("what_lagged", ""),
                "confidence": "Low",
                "repricing_window": c.get("repricing_window", ""),
                "invalidation": c.get("key_uncertainty", ""),
                "crowding_risk": "Medium",
            })
        return json.dumps({"candidates": candidates}), fallback_ideas

    # ════════════════════════════════════════════════════════════════════════
    # FILTER: Only ACCEPT (and optionally WATCHLIST) survive
    # ════════════════════════════════════════════════════════════════════════
    # Normalize decision strings (GPT may return "Accept", "accept", "ACCEPTED")
    for s in scored:
        s["decision"] = (s.get("decision") or "").upper().strip()
        if s["decision"] not in ("ACCEPT", "WATCHLIST", "REJECT"):
            s["decision"] = "REJECT"  # unknown → conservative default

    accepted = [s for s in scored if s["decision"] == "ACCEPT"]
    watchlist = [s for s in scored if s["decision"] == "WATCHLIST"]
    rejected = [s for s in scored if s["decision"] == "REJECT"]

    elapsed_total = time.time() - t0
    print(f"\n    📊 GPT Two-Stage Results ({elapsed_total:.1f}s total, ${total_cost:.4f}):")
    print(f"       ACCEPT: {len(accepted)} | WATCHLIST: {len(watchlist)} | REJECT: {len(rejected)}")

    # Include ACCEPT + WATCHLIST (pipeline's evidence gate & red-team will filter further)
    survivors = accepted + watchlist

    if not survivors:
        print(f"    ⚠ No ideas survived adjudication")
        return json.dumps({"scored": scored}), []

    # Format survivors as standard idea dicts for the merge function
    ideas = []
    for s in survivors:
        ideas.append({
            "ticker": s.get("ticker", ""),
            "company": s.get("company", ""),
            "direction": s.get("direction", ""),
            "catalyst": s.get("catalyst", ""),
            "catalyst_status": s.get("catalyst_status", ""),
            "cleanest_expression": s.get("cleanest_expression", ""),
            "what_moved": s.get("what_moved", ""),
            "what_repriced": "",
            "what_lagged": s.get("what_lagged", ""),
            "payoff_range": "",
            "thesis": s.get("thesis", ""),
            "transmission_type": s.get("transmission_type", ""),
            "why_late": s.get("why_late", ""),
            "already_priced_risk": s.get("already_priced_risk", ""),
            "evidence": s.get("evidence", ""),
            "flow_read": s.get("flow_read", ""),
            "historical_check": s.get("historical_check", ""),
            "practical_tradability": s.get("practical_tradability", ""),
            "repricing_window": s.get("repricing_window", ""),
            "why_actionable_now": s.get("why_actionable_now", ""),
            "invalidation": s.get("invalidation", ""),
            "crowding_risk": s.get("crowding_risk", ""),
            "confidence": s.get("confidence", ""),
            # Two-stage metadata
            "_gpt_decision": s.get("decision", ""),
            "_gpt_bull_score": s.get("bull_score", 0),
            "_gpt_bear_score": s.get("bear_score", 0),
            "_gpt_cost_usd": total_cost,
        })

    print(f"    🎯 GPT contributing {len(ideas)} idea(s) to merge")
    return json.dumps({"scored": scored}), ideas


def run_catalyst_hunt_gemini(system_prompt: str, user_message: str) -> Optional[str]:
    """
    Run R1 catalyst hunt using Gemini 3.1-pro-preview with Deep Think HIGH.
    Same prompt as Claude — independent idea generation.
    """
    # ── MANUAL MODE: bypass API, save prompt for human ──
    if os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes"):
        from orca_v20.manual_bridge import manual_llm_call
        return manual_llm_call(
            role="hunter_tertiary",
            system_prompt=system_prompt,
            user_prompt=user_message,
            model_hint="google/gemini-3.1-pro-preview",
        )

    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not gemini_key:
        print("  ⚠ GEMINI_API_KEY not set — skipping Gemini hunt")
        return None

    print(f"\n  💎 Gemini-3.1-pro-preview: Running catalyst hunt...")
    t0 = time.time()

    # Gemini combines system + user into a single user message
    combined_msg = f"{system_prompt}\n\n{user_message}"
    model = "gemini-3.1-pro-preview"

    try:
        api_url = (f"https://generativelanguage.googleapis.com/v1beta/"
                   f"models/{model}:generateContent?key={gemini_key}")

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": combined_msg}]
                }
            ],
            "tools": [
                {"google_search": {}}
            ],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 16000,
                "thinkingConfig": {
                    "thinkingLevel": "HIGH"
                }
            }
        }

        resp = requests.post(api_url, json=payload, timeout=600)

        # Fallback: remove Deep Think if not supported
        if resp.status_code in (400, 404):
            if "thinking" in resp.text.lower():
                print("    ↳ Deep Think not available, retrying without...")
                payload["generationConfig"].pop("thinkingConfig", None)
                resp = requests.post(api_url, json=payload, timeout=300)

        # Fallback: try base model
        if resp.status_code in (400, 404):
            model = "gemini-3.1-pro"
            print(f"    ↳ Trying {model}...")
            api_url = (f"https://generativelanguage.googleapis.com/v1beta/"
                       f"models/{model}:generateContent?key={gemini_key}")
            payload["generationConfig"].pop("thinkingConfig", None)
            resp = requests.post(api_url, json=payload, timeout=300)

        if resp.status_code != 200:
            print(f"    ❌ Gemini API Error {resp.status_code}: {resp.text[:300]}")
            return None

        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            print(f"    ❌ Gemini returned no candidates")
            return None

        content = ""
        for part in candidates[0].get("content", {}).get("parts", []):
            if "text" in part:
                content += part["text"]

        usage = data.get("usageMetadata", {})
        prompt_tokens = usage.get("promptTokenCount", 0)
        completion_tokens = usage.get("candidatesTokenCount", 0)
        thinking_tokens = usage.get("thoughtsTokenCount", 0)
        cost = prompt_tokens / 1_000_000 * 1.25 + completion_tokens / 1_000_000 * 10.0
        elapsed = time.time() - t0

        think_note = f", {thinking_tokens:,} thinking" if thinking_tokens else ""
        print(f"    ✅ Gemini hunt complete ({elapsed:.1f}s, ${cost:.4f}, "
              f"{prompt_tokens:,}+{completion_tokens:,}{think_note} tokens)")

        return content

    except requests.exceptions.Timeout:
        print(f"    ❌ Gemini timeout after {time.time() - t0:.0f}s")
        return None
    except Exception as e:
        print(f"    ❌ Gemini error: {e}")
        return None


def merge_multi_model_ideas(claude_ideas: List[Dict],
                             gpt_ideas: List[Dict],
                             gemini_ideas: List[Dict]) -> List[Dict]:
    """
    Merge ideas from all three models. Union approach:
    - Every unique ticker goes through to Stage 2 (flow is the real filter)
    - If 2+ models found the same ticker, note it (higher base confidence)
    - Prefer Claude's idea card if ticker overlaps (richest field set)
    """
    # Per-model dedup: if a model returned the same ticker twice, keep first only
    def _dedup_by_ticker(ideas: List[Dict]) -> List[Dict]:
        seen = set()
        out = []
        for idea in ideas:
            t = (idea.get("ticker", "") or "").upper().strip()
            if t and t not in seen:
                seen.add(t)
                out.append(idea)
        return out

    claude_ideas = _dedup_by_ticker(claude_ideas)
    gpt_ideas = _dedup_by_ticker(gpt_ideas)
    gemini_ideas = _dedup_by_ticker(gemini_ideas)

    # Index by ticker
    by_ticker = {}  # ticker → {"idea": dict, "sources": [model_names]}

    # Claude ideas first (preferred source)
    for idea in claude_ideas:
        ticker = (idea.get("ticker", "") or "").upper().strip()
        if not ticker:
            continue
        by_ticker[ticker] = {"idea": idea, "sources": ["Claude"]}

    # GPT ideas
    for idea in gpt_ideas:
        ticker = (idea.get("ticker", "") or "").upper().strip()
        if not ticker:
            continue
        if ticker in by_ticker:
            by_ticker[ticker]["sources"].append("GPT")
            # Don't replace Claude's richer card — just note agreement
        else:
            idea["_source"] = "GPT"
            by_ticker[ticker] = {"idea": idea, "sources": ["GPT"]}

    # Gemini ideas
    for idea in gemini_ideas:
        ticker = (idea.get("ticker", "") or "").upper().strip()
        if not ticker:
            continue
        if ticker in by_ticker:
            by_ticker[ticker]["sources"].append("Gemini")
        else:
            idea["_source"] = "Gemini"
            by_ticker[ticker] = {"idea": idea, "sources": ["Gemini"]}

    # Build merged list with consensus metadata
    merged = []
    for ticker, entry in by_ticker.items():
        idea = entry["idea"]
        sources = entry["sources"]
        idea["model_sources"] = sources
        idea["model_consensus"] = len(sources)

        # Tag for logging
        if len(sources) == 3:
            idea["consensus_tag"] = "3/3 UNANIMOUS"
        elif len(sources) == 2:
            idea["consensus_tag"] = f"2/3 ({'+'.join(sources)})"
        else:
            idea["consensus_tag"] = f"1/3 ({sources[0]} only)"

        merged.append(idea)

    # Sort: 3/3 first, then 2/3, then 1/3 — within same consensus, keep original order
    merged.sort(key=lambda x: x.get("model_consensus", 1), reverse=True)

    return merged


def run_multi_model_hunt(seed: str = "", dry_run: bool = False) -> tuple:
    """
    Run Stage 1 catalyst hunt across Claude + GPT + Gemini in parallel.
    All three get the same R1 v5.0 prompt + live intelligence.
    Returns (raw_text_from_claude, merged_ideas_list).
    """
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║  🎯 ORCA V3 STAGE 1 — Multi-Model Catalyst Hunt              ║
║  Models: Claude Opus 4.6 + GPT 5.4 + Gemini 3.1 Pro          ║
║  Mode: {'Seeded — ' + seed[:35] if seed else 'Open Hunt':<54s} ║
╚═══════════════════════════════════════════════════════════════╝""")

    # Build shared prompt + context (done once, shared by all models)
    system_prompt, user_message = _build_hunt_messages(seed=seed)
    if system_prompt is None:
        return None, []

    if dry_run:
        print(f"\n  DRY RUN — system: {len(system_prompt):,} chars, "
              f"user: {len(user_message):,} chars")
        return None, []

    # ── Run all 3 models ──
    # Claude runs first (primary), GPT + Gemini in sequence
    # (Could be parallelized with threads, but sequential is simpler + safer on rate limits)

    # Claude (primary — uses existing run_catalyst_hunt)
    claude_text = run_catalyst_hunt(seed=seed, dry_run=dry_run)
    claude_ideas = parse_catalyst_ideas(claude_text) if claude_text else []
    print(f"\n  📊 Claude: {len(claude_ideas)} ideas")

    # GPT (Responses API — returns pre-parsed structured ideas)
    gpt_text, gpt_ideas = run_catalyst_hunt_gpt(system_prompt, user_message)
    print(f"  📊 GPT: {len(gpt_ideas)} ideas")

    # Gemini
    gemini_text = run_catalyst_hunt_gemini(system_prompt, user_message)
    gemini_ideas = parse_catalyst_ideas(gemini_text) if gemini_text else []
    print(f"  📊 Gemini: {len(gemini_ideas)} ideas")

    # ── Merge ──
    merged = merge_multi_model_ideas(claude_ideas, gpt_ideas, gemini_ideas)

    print(f"\n{'=' * 60}")
    print(f"  MULTI-MODEL MERGE RESULTS")
    print(f"{'=' * 60}")
    print(f"  Claude: {len(claude_ideas)} | GPT: {len(gpt_ideas)} "
          f"| Gemini: {len(gemini_ideas)} → Merged: {len(merged)} unique tickers")

    for i, idea in enumerate(merged, 1):
        ticker = idea.get("ticker", "?")
        direction = idea.get("direction", "?")
        consensus = idea.get("consensus_tag", "?")
        conf = idea.get("confidence", "?")
        print(f"    #{i}: {ticker} ({direction}) — {consensus} — Conf: {conf}")

    return claude_text, merged


# ─────────────────────────────────────────────────────────────────────────────
# SAVE / LOAD
# ─────────────────────────────────────────────────────────────────────────────
def save_results(raw_text: str, ideas: List[Dict], seed: str = ""):
    """Save Stage 1 results to files."""
    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")

    raw_file = RESULTS_DIR / f"stage1_hunt_{ts}.md"
    with open(raw_file, "w", encoding="utf-8") as f:
        mode = f"Seeded: {seed}" if seed else "Open Hunt"
        f.write(f"# 🎯 ORCA V3 Stage 1 — Catalyst Hunter R1 v5.0 ({mode})\n")
        f.write(f"# Date: {datetime.date.today()}\n\n")
        f.write(raw_text)
    print(f"  📄 Saved raw output: {raw_file}")

    json_file = RESULTS_DIR / "stage1_catalysts.json"
    catalyst_data = {
        "date": datetime.date.today().isoformat(),
        "mode": "seeded" if seed else "open_hunt",
        "seed": seed,
        "n_ideas": len(ideas),
        "ideas": ideas,
        "formatted_seeds": format_catalyst_seeds_for_analyst(ideas),
    }
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(catalyst_data, f, indent=2, default=str)
    print(f"  📊 Saved structured data: {json_file}")
    return raw_file, json_file


def load_catalyst_seeds() -> str:
    """Load latest Stage 1 seeds for injection into later stages."""
    json_file = RESULTS_DIR / "stage1_catalysts.json"
    if not json_file.exists():
        return ""
    try:
        data = json.loads(json_file.read_text())
        if data.get("date") != datetime.date.today().isoformat():
            return ""
        n_ideas = data.get("n_ideas", 0)
        if n_ideas == 0:
            return ""
        print(f"  ✅ Loaded {n_ideas} Stage 1 catalyst seeds")
        return data.get("formatted_seeds", "")
    except Exception as e:
        print(f"  ⚠ Failed to load Stage 1 seeds: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="ORCA V3 Stage 1 — Catalyst Hunter")
    parser.add_argument("--seed", type=str, default="",
                        help="Catalyst seed for Seeded Mode")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompt without calling API")
    args = parser.parse_args()

    raw_text = run_catalyst_hunt(seed=args.seed, dry_run=args.dry_run)

    if args.dry_run:
        return

    if not raw_text:
        print("\n  ❌ Stage 1 produced no output")
        return

    print(f"\n{'═' * 60}")
    print(raw_text)
    print(f"{'═' * 60}")

    ideas = parse_catalyst_ideas(raw_text)
    print(f"\n  🎯 Parsed {len(ideas)} catalyst ideas")

    for i, idea in enumerate(ideas, 1):
        ticker = idea.get("ticker", "?")
        direction = idea.get("direction", "?")
        confidence = idea.get("confidence", "?")
        catalyst = idea.get("catalyst", "?")[:60]
        print(f"    #{i}: {ticker} ({direction}) — Conf: {confidence} — {catalyst}")

    save_results(raw_text, ideas, seed=args.seed)


if __name__ == "__main__":
    main()
