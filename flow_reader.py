#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Flow Reader (Stage 2)
=================================
Wraps the ORCA Flow Module v5.7 prompt.
Takes raw UW flow data for a ticker, sends to AI for interpretation.

Output: one of [SUPPORTIVE] [NEUTRAL / MIXED] [CONTRADICTORY] [DATA UNAVAILABLE]

This module is CONFIRMATORY ONLY — it cannot originate trade ideas.
"""

import os
import json
import time
import requests
from pathlib import Path
from typing import Optional, Dict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FLOW_MODEL = os.environ.get("FLOW_MODEL", "claude-opus-4-6")
FLOW_MAX_TOKENS = 8000
FLOW_THINKING_BUDGET = 4000

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
# Flow Module v5.7 System Prompt — loaded from file or embedded
# ---------------------------------------------------------------------------
_FLOW_PROMPT_FILE = Path(__file__).parent / "prompts" / "flow_module_v5_7.txt"


def _load_flow_prompt() -> str:
    """Load the Flow Module v5.7 prompt."""
    if _FLOW_PROMPT_FILE.exists():
        return _FLOW_PROMPT_FILE.read_text(encoding="utf-8")

    # Fallback: load from Downloads if available
    fallback = Path(os.path.expanduser("~")) / "Downloads" / "orca_flow_module_v5_7_uw_native.txt"
    if fallback.exists():
        return fallback.read_text(encoding="utf-8")

    raise FileNotFoundError(
        f"Flow Module prompt not found at {_FLOW_PROMPT_FILE} or {fallback}. "
        "Copy orca_flow_module_v5_7_uw_native.txt to prompts/flow_module_v5_7.txt"
    )


# ---------------------------------------------------------------------------
# Flow Reader AI Call
# ---------------------------------------------------------------------------
def read_flow(ticker: str, direction: str, flow_text: str,
              catalyst_summary: str = "") -> Dict:
    """
    Send UW flow data to AI for interpretation using Flow Module v5.7.

    Args:
        ticker: Stock ticker
        direction: "Bullish" or "Bearish" (from R1 idea)
        flow_text: Formatted UW flow data (from uw_flow.format_flow_for_prompt)
        catalyst_summary: Brief catalyst description for context

    Returns:
        {
            "ticker": str,
            "tape_read": "SUPPORTIVE" | "NEUTRAL / MIXED" | "CONTRADICTORY" | "DATA UNAVAILABLE",
            "options_read": str,
            "darkpool_read": str,
            "dealer_gamma_context": str,
            "downgrade_factors": str,
            "raw_output": str,
            "cost": float,
        }
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)

    if not flow_text or "Data unavailable" in flow_text:
        return _empty_result(ticker, "DATA UNAVAILABLE — VERIFY IN R2")

    system_prompt = _load_flow_prompt()

    user_message = f"""Ticker: {ticker}
Direction: {direction}
{f'Catalyst context: {catalyst_summary}' if catalyst_summary else ''}

Evaluate the following Unusual Whales flow data using the v5.7 interpretation rules.
Determine whether the tape supports, is mixed on, or contradicts a {direction.lower()} thesis for {ticker}.

{flow_text}

Now interpret this data. Follow the exact output format from the flow module rules."""

    # ── MANUAL MODE: bypass API, save prompt for human ──
    if os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes"):
        from orca_v20.manual_bridge import manual_llm_call
        raw = manual_llm_call(
            role="flow_reader",
            system_prompt=system_prompt,
            user_prompt=user_message,
            model_hint=f"anthropic/{FLOW_MODEL}",
        )
        tape_read = _parse_tape_read(raw)
        return {
            "ticker": ticker,
            "tape_read": tape_read,
            "options_read": "",
            "darkpool_read": "",
            "dealer_gamma_context": "",
            "downgrade_factors": "",
            "raw_output": raw,
            "cost": 0.0,
        }

    if not api_key:
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping flow read")
        return _empty_result(ticker, "DATA UNAVAILABLE — VERIFY IN R2")

    print(f"  🔍 Reading flow for {ticker} ({direction})...")
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
                "model": FLOW_MODEL,
                "max_tokens": FLOW_MAX_TOKENS,
                "system": system_prompt,
                "thinking": {
                    "type": "enabled",
                    "budget_tokens": FLOW_THINKING_BUDGET,
                },
                "tools": [
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": 3,
                    }
                ],
                "messages": [{"role": "user", "content": user_message}],
            },
            timeout=180,
        )

        if resp.status_code != 200:
            print(f"  ❌ Flow reader API error {resp.status_code}: {resp.text[:200]}")
            return _empty_result(ticker, "DATA UNAVAILABLE — VERIFY IN R2")

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

        # Parse the tape read label
        tape_read = _parse_tape_read(raw_text)
        options_read = _extract_field(raw_text, "Options read:")
        darkpool_read = _extract_field(raw_text, "Dark-pool read:")
        gamma_ctx = _extract_field(raw_text, "Dealer gamma / timing context:")
        downgrades = _extract_field(raw_text, "Downgrade factors:")

        print(f"  ✅ Flow read: [{tape_read}] ({elapsed:.1f}s, ${cost:.4f})")

        return {
            "ticker": ticker,
            "tape_read": tape_read,
            "options_read": options_read,
            "darkpool_read": darkpool_read,
            "dealer_gamma_context": gamma_ctx,
            "downgrade_factors": downgrades,
            "raw_output": raw_text,
            "cost": cost,
        }

    except Exception as e:
        print(f"  ❌ Flow reader error: {e}")
        return _empty_result(ticker, "DATA UNAVAILABLE — VERIFY IN R2")


def _empty_result(ticker: str, tape_read: str) -> Dict:
    return {
        "ticker": ticker,
        "tape_read": tape_read,
        "options_read": "Data unavailable",
        "darkpool_read": "Data unavailable",
        "dealer_gamma_context": "Data unavailable",
        "downgrade_factors": "",
        "raw_output": "",
        "cost": 0.0,
    }


def _parse_tape_read(text: str) -> str:
    """Extract the final tape-read classification from AI output."""
    import re
    # Look for the bracketed label
    for label in ["SUPPORTIVE", "NEUTRAL / MIXED", "CONTRADICTORY",
                   "DATA UNAVAILABLE — VERIFY IN R2", "DATA UNAVAILABLE"]:
        if f"[{label}]" in text:
            return label
    # Fallback: look for unbracketed
    text_upper = text.upper()
    if "SUPPORTIVE" in text_upper and "CONTRADICTORY" not in text_upper:
        return "SUPPORTIVE"
    if "CONTRADICTORY" in text_upper:
        return "CONTRADICTORY"
    if "NEUTRAL" in text_upper or "MIXED" in text_upper:
        return "NEUTRAL / MIXED"
    return "DATA UNAVAILABLE — VERIFY IN R2"


def _extract_field(text: str, field_name: str) -> str:
    """Extract a field value from structured AI output."""
    import re
    pattern = re.escape(field_name) + r"\s*(.*?)(?=\n\w|\n---|\Z)"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()[:500]
    return ""


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Flow Reader — test mode")
    # Quick test with dummy data
    test_flow = """=== UW FLOW DATA: AAPL ===
--- UNUSUAL FLOW ALERTS (2 alerts) ---
  ticker=AAPL | call_put=CALL | strike=230 | expiry=2026-03-20 | premium=1500000 | volume=5000 | trade_type=SWEEP
  ticker=AAPL | call_put=CALL | strike=235 | expiry=2026-03-20 | premium=800000 | volume=2000 | trade_type=BLOCK
--- OI CHANGE (1 entries) ---
  call_put=CALL | strike=230 | expiry=2026-03-20 | oi_change=+3200 | prev_oi=8000 | current_oi=11200
=== END UW FLOW DATA: AAPL ==="""

    result = read_flow("AAPL", "Bullish", test_flow, "iPhone AI upgrade catalyst")
    print(f"\nTape read: [{result['tape_read']}]")
    print(f"Options: {result['options_read'][:200]}")
    print(f"Cost: ${result['cost']:.4f}")
