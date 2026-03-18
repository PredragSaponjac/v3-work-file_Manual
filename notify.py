#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Telegram Notifier
==============================
Sends trade alerts, pipeline results, and monitoring alerts to Telegram.
Uses the same Telegram Bot API pattern as ORCA V2.

Prefix: ⚡ ORCA V3  (distinct from V2's ⚡ ORCA)
"""

import os
import requests
from pathlib import Path

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MAX_MSG_LEN = 4000  # Telegram limit is 4096, leave margin


def send_telegram(text: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Telegram. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠ Telegram credentials not set — skipping")
        return False

    # Truncate if too long
    if len(text) > MAX_MSG_LEN:
        text = text[:MAX_MSG_LEN - 20] + "\n\n...truncated"

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }, timeout=30)

        if resp.status_code == 200:
            print("  📨 Telegram message sent")
            return True
        else:
            print(f"  ⚠ Telegram error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  ⚠ Telegram send failed: {e}")
        return False


def format_new_trade_alert(trade: dict) -> str:
    """Format a new trade entry for Telegram notification."""
    ticker = trade.get("ticker", "?")
    direction = trade.get("direction", "?")
    catalyst = trade.get("catalyst", "")[:120]
    thesis = trade.get("thesis", "")[:150]
    confidence = trade.get("confidence", "?")
    repricing = trade.get("repricing_window", "?")
    entry_price = trade.get("underlying_at_entry")
    tape_read = trade.get("tape_read", "?")
    catalyst_health = trade.get("catalyst_health", "?")
    cds_score = trade.get("cds_score", "?")
    target = trade.get("target_price", 0)
    stop = trade.get("stop_price", 0)

    d_icon = "📈" if direction == "Bullish" else "📉"

    entry_str = f"${entry_price:.2f}" if entry_price else "?"
    target_str = f"${target:.2f}" if target else "?"
    stop_str = f"${stop:.2f}" if stop else "?"

    msg = (
        f"⚡ <b>ORCA V3 — New Trade</b>\n"
        f"{'─' * 28}\n"
        f"{d_icon} <b>${ticker}</b> — {direction}\n"
        f"🎯 {catalyst}\n"
        f"💡 {thesis}\n"
        f"📊 Conf: {confidence} | Window: {repricing}\n"
        f"💲 Entry: {entry_str} | Target: {target_str} | Stop: {stop_str}\n"
        f"🎛 Tape: [{tape_read}] | Catalyst: {catalyst_health} (CDS: {cds_score})\n"
        f"{'─' * 28}\n"
        f"<i>⚠️ Not financial advice. DYOR.</i>"
    )
    return msg


def format_monitor_alert(alert_type: str, trade: dict, current_price: float,
                         pnl_pct: float, recheck_text: str = "") -> str:
    """Format a monitoring alert (TP/SL hit, thesis recheck)."""
    ticker = trade.get("ticker", "?")
    direction = trade.get("direction", "?")
    entry_price = trade.get("entry_price", 0)
    target = trade.get("target_price", 0)
    stop = trade.get("stop_price", 0)
    catalyst = trade.get("catalyst", "")[:100]
    days_held = trade.get("days_held", 0)

    # Alert-specific formatting
    if alert_type == "STOP_HIT":
        icon = "🔴"
        title = "STOP HIT — Auto Close"
        detail = f"Hard stop breached at ${stop:.2f}"
    elif alert_type == "TARGET_HIT":
        icon = "🟢"
        title = "TARGET HIT — Auto Close"
        detail = f"Target reached at ${target:.2f}"
    elif alert_type == "HALFWAY_TO_SL":
        icon = "🟠"
        title = "HALFWAY TO STOP — Thesis Recheck"
        midpoint = (entry_price + stop) / 2 if entry_price and stop else 0
        detail = f"Midpoint ${midpoint:.2f} breached"
    elif alert_type == "HALFWAY_TO_TP":
        icon = "🟡"
        title = "HALFWAY TO TARGET — Move Stop to Entry"
        midpoint = (entry_price + target) / 2 if entry_price and target else 0
        detail = f"Midpoint ${midpoint:.2f} reached"
    elif alert_type == "RECHECK_THESIS":
        icon = "🟠"
        title = "THESIS RECHECK"
        detail = f"P&L threshold triggered at {pnl_pct:+.1f}%"
    elif alert_type == "TAKE_PROFIT":
        icon = "🟢"
        title = "TAKE PROFIT ZONE"
        detail = f"P&L at {pnl_pct:+.1f}%"
    else:
        icon = "⚪"
        title = alert_type
        detail = ""

    # Build P&L bar
    bars = int(min(abs(pnl_pct) / 5, 10))
    bar_char = "🟩" if pnl_pct >= 0 else "🟥"
    pnl_bar = bar_char * bars

    msg = (
        f"⚡ <b>ORCA V3 — {title}</b>\n"
        f"{'─' * 28}\n"
        f"{icon} <b>${ticker}</b> — {direction}\n"
        f"| Entry ${entry_price:.2f} → ${current_price:.2f}\n"
        f"| P&L: {pnl_pct:+.1f}% {pnl_bar}\n"
        f"| Target: ${target:.2f} | Stop: ${stop:.2f}\n"
        f"| Day {days_held}\n"
        f"| {detail}\n"
    )

    if alert_type == "HALFWAY_TO_SL" and catalyst:
        msg += (
            f"{'─' * 28}\n"
            f"🎯 Catalyst: {catalyst}\n"
            f"⚠ <b>Manual thesis recheck recommended</b>\n"
        )

    if alert_type == "HALFWAY_TO_TP":
        msg += (
            f"{'─' * 28}\n"
            f"💡 <b>Consider moving stop to entry (${entry_price:.2f})</b>\n"
        )

    if recheck_text:
        msg += (
            f"{'─' * 28}\n"
            f"🧠 AI Recheck: {recheck_text[:500]}\n"
        )

    msg += (
        f"{'─' * 28}\n"
        f"<i>⚠️ Not financial advice. DYOR.</i>"
    )

    return msg


if __name__ == "__main__":
    send_telegram("⚡ <b>ORCA V3</b> — Test message 🏓", parse_mode="HTML")
