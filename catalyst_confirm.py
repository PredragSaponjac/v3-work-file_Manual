#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Catalyst Confirmation Layer V2 (Stage 3)
====================================================
Evaluates whether the catalyst behind an ORCA idea is:
  STRENGTHENING / STABLE / FADING / COLLIDING

Uses CDS (Catalyst Decay Score) 0-100 with weighted dimensions.
Outputs: CONFIRM / DOWNGRADE / KILL / HOLD FOR R2

This module does NOT generate new trades — it only evaluates catalyst health.
"""

import os
import json
import time
import re
import requests
from pathlib import Path
from typing import Optional, Dict, List

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CONFIRM_MODEL = os.environ.get("CONFIRM_MODEL", "claude-opus-4-6")
CONFIRM_MAX_TOKENS = 16000
CONFIRM_THINKING_BUDGET = 8000

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()

# ---------------------------------------------------------------------------
# Catalyst Confirmation V2 System Prompt
# ---------------------------------------------------------------------------
_CONFIRM_PROMPT_FILE = Path(__file__).parent / "prompts" / "catalyst_confirmation_v2.txt"


def _load_confirm_prompt() -> str:
    """Load the Catalyst Confirmation Layer V2 prompt."""
    if _CONFIRM_PROMPT_FILE.exists():
        return _CONFIRM_PROMPT_FILE.read_text(encoding="utf-8")

    fallback = Path(os.path.expanduser("~")) / "Downloads" / "orca_catalyst_confirmation_layer_v2_updated.txt"
    if fallback.exists():
        return fallback.read_text(encoding="utf-8")

    raise FileNotFoundError(
        f"Catalyst Confirmation prompt not found at {_CONFIRM_PROMPT_FILE} or {fallback}. "
        "Copy to prompts/catalyst_confirmation_v2.txt"
    )


# ---------------------------------------------------------------------------
# Catalyst Confirmation AI Call
# ---------------------------------------------------------------------------
def confirm_catalyst(idea: dict, flow_result: dict = None,
                     prediction_market_data: str = "",
                     cross_asset_data: str = "",
                     competing_catalysts: str = "") -> Dict:
    """
    Run the Catalyst Confirmation Layer on an ORCA idea.

    Args:
        idea: Parsed ORCA idea dict (ticker, direction, catalyst, thesis, etc.)
        flow_result: Output from flow_reader.read_flow() (tape read + details)
        prediction_market_data: Formatted prediction market context
        cross_asset_data: Formatted cross-asset confirmation data
        competing_catalysts: Known competing catalysts if any

    Returns:
        {
            "ticker": str,
            "catalyst_health": "STRENGTHENING" | "STABLE" | "FADING" | "COLLIDING" | "DATA UNAVAILABLE",
            "action": "CONFIRM" | "DOWNGRADE" | "KILL" | "HOLD FOR R2",
            "primary_cds": int (0-100),
            "effective_cds": int (0-100),
            "dim1_score": int,
            "dim2_score": int,
            "dim3_score": int,
            "disagreement_flag": str,
            "kill_reason": str,
            "raw_output": str,
            "cost": float,
        }
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
    ticker = idea.get("ticker", "?")

    if not api_key:
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping catalyst confirmation")
        return _empty_result(ticker)

    system_prompt = _load_confirm_prompt()

    # Build the idea card for input
    idea_card = _build_idea_card(idea)

    # Build flow summary from flow_reader output
    flow_summary = ""
    if flow_result and flow_result.get("tape_read"):
        flow_summary = (
            f"\n=== OPTIONS / POSITIONING DATA (from Flow Module) ===\n"
            f"Tape Read: [{flow_result['tape_read']}]\n"
        )
        if flow_result.get("options_read"):
            flow_summary += f"Options read: {flow_result['options_read']}\n"
        if flow_result.get("darkpool_read"):
            flow_summary += f"Dark-pool read: {flow_result['darkpool_read']}\n"
        if flow_result.get("dealer_gamma_context"):
            flow_summary += f"Dealer gamma context: {flow_result['dealer_gamma_context']}\n"
        if flow_result.get("downgrade_factors"):
            flow_summary += f"Downgrade factors: {flow_result['downgrade_factors']}\n"

    # Build user message
    user_message = f"""Evaluate the catalyst health for this ORCA idea.

=== ORIGINAL ORCA IDEA CARD ===
{idea_card}

{flow_summary}

{f'=== PREDICTION MARKET DATA ==={chr(10)}{prediction_market_data}' if prediction_market_data else ''}

{f'=== CROSS-ASSET CONFIRMATION ==={chr(10)}{cross_asset_data}' if cross_asset_data else ''}

{f'=== COMPETING CATALYSTS ==={chr(10)}{competing_catalysts}' if competing_catalysts else ''}

Now evaluate. Follow the exact output format. Assign CDS scores and the composite health state."""

    # ── MANUAL MODE: bypass API, save prompt for human ──
    if os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes"):
        from orca_v20.manual_bridge import manual_llm_call
        raw_text = manual_llm_call(
            role="catalyst_confirm",
            system_prompt=system_prompt,
            user_prompt=user_message,
            model_hint=f"anthropic/{CONFIRM_MODEL}",
        )
        health = _extract_health_state(raw_text)
        action = _extract_action(raw_text)
        primary_cds = _extract_score(raw_text, "Primary CDS:")
        effective_cds = _extract_score(raw_text, "Effective CDS:")
        if effective_cds == 0 and primary_cds > 0:
            effective_cds = primary_cds
        return {
            "ticker": ticker,
            "catalyst_health": health,
            "action": action,
            "primary_cds": primary_cds,
            "effective_cds": effective_cds,
            "dim1_score": _extract_score(raw_text, "Dim 1 Score:"),
            "dim2_score": _extract_score(raw_text, "Dim 2 Score:"),
            "dim3_score": _extract_score(raw_text, "Dim 3 Score:"),
            "disagreement_flag": _extract_field(raw_text, "Disagreement flag:"),
            "kill_reason": _extract_field(raw_text, "Primary downgrade / kill reason:"),
            "raw_output": raw_text,
            "cost": 0.0,
        }

    print(f"  🔬 Confirming catalyst for {ticker}...")
    t0 = time.time()

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": CONFIRM_MODEL,
                "max_tokens": CONFIRM_MAX_TOKENS,
                "system": system_prompt,
                "thinking": {
                    "type": "enabled",
                    "budget_tokens": CONFIRM_THINKING_BUDGET,
                },
                "tools": [
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": 5,
                    }
                ],
                "messages": [{"role": "user", "content": user_message}],
            },
            timeout=240,
        )

        if resp.status_code != 200:
            print(f"  ❌ Catalyst confirm API error {resp.status_code}: {resp.text[:200]}")
            return _empty_result(ticker)

        data = resp.json()
        usage = data.get("usage", {})
        cost = (usage.get("input_tokens", 0) / 1_000_000) * 15 + \
               (usage.get("output_tokens", 0) / 1_000_000) * 75
        elapsed = time.time() - t0

        # Extract text
        raw_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                raw_text += block.get("text", "")

        # Parse structured fields
        health = _extract_health_state(raw_text)
        action = _extract_action(raw_text)
        primary_cds = _extract_score(raw_text, "Primary CDS:")
        effective_cds = _extract_score(raw_text, "Effective CDS:")
        dim1 = _extract_score(raw_text, "Dim 1 Score:")
        dim2 = _extract_score(raw_text, "Dim 2 Score:")
        dim3 = _extract_score(raw_text, "Dim 3 Score:")
        disagreement = _extract_field(raw_text, "Disagreement flag:")
        kill_reason = _extract_field(raw_text, "Primary downgrade / kill reason:")

        # Use primary if effective not available
        if effective_cds == 0 and primary_cds > 0:
            effective_cds = primary_cds

        print(f"  ✅ Catalyst: {health} → {action} (CDS: {effective_cds}) ({elapsed:.1f}s, ${cost:.4f})")

        return {
            "ticker": ticker,
            "catalyst_health": health,
            "action": action,
            "primary_cds": primary_cds,
            "effective_cds": effective_cds,
            "dim1_score": dim1,
            "dim2_score": dim2,
            "dim3_score": dim3,
            "disagreement_flag": disagreement,
            "kill_reason": kill_reason,
            "raw_output": raw_text,
            "cost": cost,
        }

    except Exception as e:
        print(f"  ❌ Catalyst confirm error: {e}")
        return _empty_result(ticker)


def _empty_result(ticker: str) -> Dict:
    return {
        "ticker": ticker,
        "catalyst_health": "DATA UNAVAILABLE — VERIFY IN R2",
        "action": "HOLD FOR R2",
        "primary_cds": 0,
        "effective_cds": 0,
        "dim1_score": 0,
        "dim2_score": 0,
        "dim3_score": 0,
        "disagreement_flag": "N/A",
        "kill_reason": "",
        "raw_output": "",
        "cost": 0.0,
    }


def _build_idea_card(idea: dict) -> str:
    """Format an ORCA idea dict into the text card format."""
    fields = [
        ("Ticker", idea.get("ticker", "?")),
        ("Company", idea.get("company", "")),
        ("Direction", idea.get("direction", "?")),
        ("Live / Emerging Catalyst", idea.get("catalyst", "")),
        ("Catalyst Status", idea.get("catalyst_status", "")),
        ("Why this company is the cleanest expression", idea.get("cleanest_expression", "")),
        ("What already moved", idea.get("what_moved", "")),
        ("What has not repriced enough", idea.get("what_lagged", "")),
        ("Thesis", idea.get("thesis", "")),
        ("Evidence of delayed or incomplete pricing", idea.get("evidence", "")),
        ("Expected repricing window", idea.get("repricing_window", "")),
        ("Main invalidation", idea.get("invalidation", "")),
        ("Confidence", idea.get("confidence", "")),
        ("Crowding risk", idea.get("crowding_risk", "")),
    ]
    lines = []
    for name, val in fields:
        if val:
            lines.append(f"{name}: {val}")
    return "\n".join(lines)


def _extract_health_state(text: str) -> str:
    """Extract the catalyst health state from output."""
    for state in ["STRENGTHENING", "STABLE", "FADING", "COLLIDING",
                   "DATA UNAVAILABLE — VERIFY IN R2", "DATA UNAVAILABLE"]:
        # Look for "Catalyst health state: STATE"
        if re.search(rf"Catalyst health state:\s*{re.escape(state)}", text, re.IGNORECASE):
            return state
    # Fallback: look anywhere
    text_upper = text.upper()
    for state in ["STRENGTHENING", "COLLIDING", "FADING", "STABLE"]:
        if f"CATALYST HEALTH STATE: {state}" in text_upper:
            return state
    return "DATA UNAVAILABLE — VERIFY IN R2"


def _extract_action(text: str) -> str:
    """Extract recommended action."""
    for action in ["CONFIRM", "DOWNGRADE", "KILL", "HOLD FOR R2"]:
        pattern = rf"Recommended action:\s*{re.escape(action)}"
        if re.search(pattern, text, re.IGNORECASE):
            return action
    return "HOLD FOR R2"


def _extract_score(text: str, field: str) -> int:
    """Extract a numeric score from a field."""
    match = re.search(rf"{re.escape(field)}\s*(\d+)", text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return 0


def _extract_field(text: str, field: str) -> str:
    """Extract a text field value."""
    pattern = re.escape(field) + r"\s*(.*?)(?=\n\w|\n---|\Z)"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()[:300]
    return ""


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Catalyst Confirmation — test mode")
    test_idea = {
        "ticker": "AAPL",
        "company": "Apple Inc.",
        "direction": "Bullish",
        "catalyst": "iPhone AI integration driving upgrade cycle",
        "catalyst_status": "Active",
        "thesis": "Apple's AI features are driving the strongest upgrade cycle in 3 years. "
                  "Revenue beat expectations but stock only partially repriced the forward guidance.",
        "confidence": "Medium",
    }
    test_flow = {
        "ticker": "AAPL",
        "tape_read": "SUPPORTIVE",
        "options_read": "Multi-session call build at 230-235 strikes, expiry aligned with earnings",
        "darkpool_read": "Clustered accumulation near 225 level",
    }

    result = confirm_catalyst(test_idea, flow_result=test_flow)
    print(f"\nHealth: {result['catalyst_health']}")
    print(f"Action: {result['action']}")
    print(f"CDS: {result['effective_cds']}")
