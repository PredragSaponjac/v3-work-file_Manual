#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Full Pipeline Orchestrator
======================================
Wires all stages together:

  Stage 1: R1 Catalyst Hunter (finds delayed-reaction ideas)
  Stage 2: Flow Module (interprets UW options flow per ticker)
  Stage 3: Catalyst Confirmation V2 (evaluates catalyst health, CDS score)
  Stage 4: (future) Multi-model adjudication
  Stage 5: (future) Risk Monitor for live positions
  Stage 6: Trade Structuring (options trade construction)
  Stage 7: Executive Report Writer (polished Telegram/X report)

Pipeline flow:
  1. Gather news + UW market flow
  2. Run R1 catalyst hunt → IDEA cards
  3. For each IDEA: fetch detailed UW flow → run Flow Reader AI
  4. For each IDEA: run Catalyst Confirmation AI → CONFIRM/KILL/DOWNGRADE
  5. Only surviving ideas proceed to output
  6. Structure options trades → log to DB → sync to Google Sheet
  7. Send quick Telegram alert → generate executive report → send report

Usage:
  python pipeline.py                    # Full pipeline
  python pipeline.py --seed "oil"       # Seeded catalyst mode
  python pipeline.py --stage1-only      # Only run Stage 1
  python pipeline.py --dry-run          # No API calls
"""

import os
import sys
import json
import time
import datetime
from pathlib import Path
from typing import List, Dict

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
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
# REPORT FRAMING — decides headline posture BEFORE report writer sees it
# ─────────────────────────────────────────────────────────────────────────────
# Framing values:
#   conviction_call     — strong thesis, supportive tape, healthy catalyst
#   bullish_with_caveats — bullish but tape or catalyst has concerns
#   bearish_with_caveats — bearish but tape or catalyst has concerns
#   watchlist           — thesis exists but confirmation is incomplete or weakening
#   downgrade_note      — was promising, now deteriorating
#   invalidated         — thesis broken, should not have been published

FRAMING_LABELS = {
    "conviction_call":      "{dir}",
    "bullish_with_caveats": "{dir}, but confirmation is mixed",
    "bearish_with_caveats": "{dir}, but confirmation is mixed",
    "watchlist":            "watchlist",
    "downgrade_note":       "thesis weakening",
    "invalidated":          "thesis broken",
}


def determine_report_framing(direction: str, tape_read: str,
                              catalyst_action: str, catalyst_health: str,
                              cds_score: int = None) -> dict:
    """
    Decide the public-facing framing for the executive report.

    This reconciles R1's original direction with Stage 2 tape and
    Stage 3 catalyst confirmation. The report writer should never
    have to reconcile a bullish headline with a fading catalyst —
    that decision is made here.

    Returns:
        {"framing": str, "label": str}
    """
    direction = (direction or "").lower()
    tape = (tape_read or "").lower()
    action = (catalyst_action or "").lower()
    health = (catalyst_health or "").lower()

    # ── Invalidated: thesis is dead ──
    if action == "kill" or health in ("colliding", "invalidated", "dead"):
        return _framing_result("invalidated", direction)

    # ── Downgraded by Stage 3 ──
    if action == "downgrade":
        if "mixed" in tape or "neutral" in tape or health == "fading":
            return _framing_result("watchlist", direction)
        return _framing_result("downgrade_note", direction)

    # ── Confirmed or held — check tape + health alignment ──
    healthy = health in ("strengthening", "stable", "active", "strong")
    tape_supportive = "supportive" in tape
    tape_bad = "contradictory" in tape or "opposing" in tape
    tape_mixed = "mixed" in tape or "neutral" in tape

    if direction in ("bullish", "bearish"):
        if tape_supportive and healthy:
            return _framing_result("conviction_call", direction)
        if tape_bad:
            dir_caveat = f"{direction}_with_caveats"
            return _framing_result(dir_caveat, direction)
        if tape_mixed or not healthy:
            # Tape isn't confirming OR catalyst isn't healthy
            # If both are weak, it's a watchlist
            if tape_mixed and not healthy:
                return _framing_result("watchlist", direction)
            # One is weak — caveats
            dir_caveat = f"{direction}_with_caveats"
            return _framing_result(dir_caveat, direction)
        # Tape OK but not explicitly supportive, catalyst healthy
        return _framing_result("conviction_call", direction)

    # Fallback for unknown direction
    return _framing_result("watchlist", direction)


def _framing_result(framing: str, direction: str) -> dict:
    """Build framing result dict with human-readable label."""
    dir_label = direction.capitalize() if direction else "Neutral"
    template = FRAMING_LABELS.get(framing, "watchlist")
    label = template.format(dir=dir_label)
    return {"framing": framing, "label": label}


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(seed: str = "", stage1_only: bool = False,
                 dry_run: bool = False) -> Dict:
    """
    Run the full ORCA V3 pipeline.

    Returns:
        {
            "date": str,
            "stage1_ideas": [...],
            "stage2_flow": {...},
            "stage3_confirmations": {...},
            "survivors": [...],
            "total_cost": float,
        }
    """
    today = datetime.date.today().isoformat()
    total_cost = 0.0
    t0 = time.time()

    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║  ⚡ ORCA V3 — Full Pipeline                                   ║
║  Date: {today}                                              ║
║  Mode: {'Seeded — ' + seed[:40] if seed else 'Open Hunt':<54s} ║
╚═══════════════════════════════════════════════════════════════╝
""")

    # ─── STAGE 1: Multi-Model Catalyst Hunt ──────────────────
    print("=" * 60)
    print("  STAGE 1: Multi-Model Catalyst Hunt (Claude+GPT+Gemini)")
    print("=" * 60)

    from catalyst_hunter import (
        run_multi_model_hunt, run_catalyst_hunt, parse_catalyst_ideas, save_results
    )

    # Multi-model: all 3 models hunt independently, merge by ticker
    raw_text, ideas = run_multi_model_hunt(seed=seed, dry_run=dry_run)

    if dry_run:
        print("\n  [DRY RUN — stopping here]")
        return {"date": today, "dry_run": True}

    if not raw_text and not ideas:
        print("\n  ❌ Stage 1 produced no output — pipeline stops")
        _send_no_ideas(today, seed)
        return {"date": today, "stage1_ideas": [], "survivors": []}

    # ── NO-CANDIDATE PIPELINE RULE (per Methodology Note) ──
    # If ALL models returned no ideas, stop immediately.
    if not ideas:
        # Check if Claude's raw text explicitly says no candidates
        no_candidate_phrases = [
            "no valid orca candidates found",
            "no valid candidates found",
            "no valid orca candidates",
        ]
        raw_lower = (raw_text or "").lower()
        if any(phrase in raw_lower for phrase in no_candidate_phrases):
            print("\n  📭 All models returned: No valid ORCA candidates found")
            print("     → Pipeline stops per NO-CANDIDATE PIPELINE RULE")
            print("     → This is a valid output, not a failure")
        else:
            print("\n  No valid candidates from any model — pipeline stops")
        _send_no_ideas(today, seed)
        return {"date": today, "stage1_ideas": [], "survivors": []}

    # Save: Claude raw text + merged ideas from all models
    save_results(raw_text or "(multi-model merge — see JSON)", ideas, seed=seed)

    print(f"\n  🎯 Stage 1: {len(ideas)} unique tickers from multi-model hunt")
    for i, idea in enumerate(ideas, 1):
        consensus = idea.get("consensus_tag", "")
        print(f"    #{i}: {idea.get('ticker', '?')} ({idea.get('direction', '?')}) "
              f"— Conf: {idea.get('confidence', '?')} — {consensus}")

    if stage1_only:
        print("\n  [--stage1-only flag — stopping after Stage 1]")
        return {"date": today, "stage1_ideas": ideas}

    # ─── STAGE 2: Flow Reader (per ticker) ────────────────────
    print(f"\n{'=' * 60}")
    print("  STAGE 2: Flow Module (UW flow interpretation)")
    print("=" * 60)

    from uw_flow import build_ticker_flow_context, format_flow_for_prompt, classify_catalyst_speed
    from flow_reader import read_flow

    flow_results = {}
    for idea in ideas:
        ticker = idea.get("ticker", "")
        direction = idea.get("direction", "Bullish")
        catalyst = idea.get("catalyst", "")

        if not ticker:
            continue

        # Classify catalyst speed → determines lookback depths
        speed_config = classify_catalyst_speed(idea)
        lookback = speed_config["lookback"]
        print(f"  ⏱ {ticker}: catalyst speed = {speed_config['speed']} "
              f"(flow: {lookback['flow_alerts']}d, history: {lookback['contract_history']}d, "
              f"DP: {lookback['darkpool']}d)")

        # Fetch UW data with speed-adaptive lookback
        flow_data = build_ticker_flow_context(
            ticker, include_history=True,
            lookback=lookback, direction=direction
        )

        if flow_data.get("data_available"):
            flow_text = format_flow_for_prompt(flow_data)
            result = read_flow(ticker, direction, flow_text,
                               catalyst_summary=catalyst[:200])
            flow_results[ticker] = result
            total_cost += result.get("cost", 0)
        else:
            flow_results[ticker] = {
                "ticker": ticker,
                "tape_read": "DATA UNAVAILABLE — VERIFY IN R2",
                "cost": 0,
            }
            print(f"  ⚠ {ticker}: No UW flow data available")

    # ─── STAGE 3: Catalyst Confirmation V2 ────────────────────
    print(f"\n{'=' * 60}")
    print("  STAGE 3: Catalyst Confirmation Layer V2")
    print("=" * 60)

    from catalyst_confirm import confirm_catalyst

    confirmations = {}
    for idea in ideas:
        ticker = idea.get("ticker", "")
        if not ticker:
            continue

        flow_result = flow_results.get(ticker, {})

        # Build prediction market context if available (Kalshi)
        prediction_data = ""
        try:
            from catalyst_hunter import build_kalshi_context
            prediction_data = build_kalshi_context()
        except Exception:
            pass

        result = confirm_catalyst(
            idea=idea,
            flow_result=flow_result,
            prediction_market_data=prediction_data,
        )
        confirmations[ticker] = result
        total_cost += result.get("cost", 0)

    # ─── FILTER: Only survivors proceed ───────────────────────
    print(f"\n{'=' * 60}")
    print("  FILTERING — Kill/Downgrade decisions")
    print("=" * 60)

    survivors = []
    for idea in ideas:
        ticker = idea.get("ticker", "")
        conf = confirmations.get(ticker, {})
        flow = flow_results.get(ticker, {})

        action = conf.get("action", "HOLD FOR R2")
        health = conf.get("catalyst_health", "?")
        cds = conf.get("effective_cds", 0)
        tape = flow.get("tape_read", "?")

        if action == "KILL":
            kill_reason = conf.get("kill_reason", "Catalyst unhealthy")
            print(f"  ❌ {ticker}: KILLED — {kill_reason}")
            continue

        if action == "DOWNGRADE" and cds < 30:
            print(f"  ⚠ {ticker}: DOWNGRADED + CDS={cds} < 30 — removed")
            continue

        if tape == "CONTRADICTORY" and action != "CONFIRM":
            print(f"  ⚠ {ticker}: Flow CONTRADICTORY + not CONFIRMED — removed")
            continue

        # Enrich idea with Stage 2 + 3 results
        idea["tape_read"] = tape
        idea["catalyst_health"] = health
        idea["catalyst_action"] = action
        idea["cds_score"] = cds
        idea["flow_details"] = flow
        idea["confirmation_details"] = conf

        # Determine report framing — reconciles direction + tape + catalyst
        framing = determine_report_framing(
            direction=idea.get("direction", ""),
            tape_read=tape,
            catalyst_action=action,
            catalyst_health=health,
            cds_score=cds,
        )
        idea["report_framing"] = framing["framing"]
        idea["report_label"] = framing["label"]

        # Final gate: if framing says "invalidated", do not publish
        if framing["framing"] == "invalidated":
            print(f"  ❌ {ticker}: INVALIDATED by framing (Health: {health}, "
                  f"Action: {action}) — removed")
            continue

        status_icon = "✅" if action == "CONFIRM" else "🟡"
        print(f"  {status_icon} {ticker}: {action} | Health: {health} | CDS: {cds} "
              f"| Tape: [{tape}] | Framing: {framing['framing']}")
        survivors.append(idea)

    # ─── TRADE STRUCTURING — Options trade construction ─────
    if survivors:
        try:
            from trade_structurer import structure_all
            survivors = structure_all(survivors)
        except ImportError:
            print("  ⚠ trade_structurer.py not available — logging as underlying")
        except Exception as e:
            print(f"  ⚠ Trade structuring failed (non-critical): {e}")
            import traceback
            traceback.print_exc()

    # ─── OUTPUT ───────────────────────────────────────────────
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Ideas found: {len(ideas)} → Survivors: {len(survivors)}")
    print(f"  Total cost: ${total_cost:.4f}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"{'=' * 60}")

    # Save full results
    results = {
        "date": today,
        "mode": "seeded" if seed else "open_hunt",
        "seed": seed,
        "stage1_ideas": ideas,
        "stage2_flow": {k: {kk: vv for kk, vv in v.items() if kk != "raw_output"}
                        for k, v in flow_results.items()},
        "stage3_confirmations": {k: {kk: vv for kk, vv in v.items() if kk != "raw_output"}
                                  for k, v in confirmations.items()},
        "survivors": [{k: v for k, v in s.items()
                       if k not in ("raw_text", "flow_details", "confirmation_details")}
                      for s in survivors],
        "n_survivors": len(survivors),
        "total_cost": round(total_cost, 4),
        "elapsed_seconds": round(elapsed, 1),
    }

    results_file = RESULTS_DIR / f"pipeline_{today}.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  📊 Saved: {results_file}")

    # ─── LOG TRADES TO DB ────────────────────────────────────
    # Survivors get logged to SQLite → enables monitoring + sheet sync
    if survivors:
        try:
            from trade_logger import log_trades, snapshot_all_positions
            n_logged = log_trades(survivors)
            print(f"  📝 {n_logged} new trades logged to DB")
            # Snapshot current prices for all open positions
            snapshot_all_positions()
        except Exception as e:
            print(f"  ⚠ Trade logging failed (non-critical): {e}")

    # ─── SYNC TO GOOGLE SHEET ────────────────────────────────
    # Incremental: updates existing trades + appends new ones
    try:
        from sheet_sync import sync_trades_to_sheet
        sync_trades_to_sheet()
    except Exception as e:
        print(f"  ⚠ Sheet sync failed (non-critical): {e}")

    # ─── TELEGRAM: No-candidates notification ─────────────────
    # If ideas were found but none survived filtering, notify user
    if not survivors:
        _send_no_ideas(today, seed)

    # ─── STAGE 7: Executive Report Writer ───────────────────
    # Polished, public-facing report — the single Telegram output per trade
    # Never upgrades or downgrades a trade — pure explanation layer
    if survivors:
        try:
            from executive_report import (
                generate_executive_reports, send_executive_reports_telegram,
                send_executive_report_x
            )
            reports = generate_executive_reports(survivors)
            if reports:
                send_executive_reports_telegram(reports)
                # Post ALL reports to X/Twitter
                for report in reports:
                    try:
                        send_executive_report_x(report)
                    except Exception as xe:
                        print(f"  ⚠ X post failed for {report.get('ticker', '?')} "
                              f"(non-critical): {xe}")
                total_cost += sum(r.get("cost", 0) for r in reports)
        except ImportError:
            print("  ⚠ executive_report.py not available — skipping reports")
        except Exception as e:
            print(f"  ⚠ Executive report failed (non-critical): {e}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
def _send_no_ideas(date_str: str, seed: str):
    """Send 'no ideas' notification."""
    try:
        from notify import send_telegram
        mode = f"Seeded: {seed}" if seed else "Open Hunt"
        msg = (
            f"⚡ <b>ORCA V3 — No Candidates</b>\n"
            f"📅 {date_str} ({mode})\n\n"
            f"No valid delayed-reaction ideas survived the pipeline today.\n"
            f"<i>⚠️ Not financial advice.</i>"
        )
        send_telegram(msg, parse_mode="HTML")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="ORCA V3 — Full Pipeline")
    parser.add_argument("--seed", type=str, default="",
                        help="Catalyst seed for Seeded Mode")
    parser.add_argument("--stage1-only", action="store_true",
                        help="Only run Stage 1 (catalyst hunt)")
    parser.add_argument("--dry-run", action="store_true",
                        help="No API calls")
    args = parser.parse_args()

    results = run_pipeline(
        seed=args.seed,
        stage1_only=args.stage1_only,
        dry_run=args.dry_run,
    )

    n_survivors = results.get("n_survivors", len(results.get("survivors", [])))
    print(f"\n  Final: {n_survivors} surviving ideas")


if __name__ == "__main__":
    main()
