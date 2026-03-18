#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔═══════════════════════════════════════════════════════════════╗
║  ORCA V3 — Trade Logger + Position Monitor                   ║
║  Logs pipeline survivors → SQLite, snapshots prices,         ║
║  checks TP/SL alerts with halfway logic (same as DD Report)  ║
╚═══════════════════════════════════════════════════════════════╝

V3 ideas have: ticker, direction (Bullish/Bearish), confidence,
catalyst, thesis, tape_read, catalyst_health, cds_score, etc.

Usage:
  python trade_logger.py                     # Log new trades + snapshot + alerts
  python trade_logger.py --alerts-only       # Snapshot + check alerts only
  python trade_logger.py --close TICKER      # Manually close a position
"""

import os
import sys
import json
import re
import time
import sqlite3
import datetime
from pathlib import Path
from typing import Optional, Dict, List

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()

DB_PATH = Path(__file__).parent / "orca_v3_trades.db"
RESULTS_DIR = Path("orca_results")

# ── THRESHOLDS (same concept as V2) ──
MIN_CONFIDENCE = 7            # Skip ideas below this
MAX_POSITIONS = 10            # Max normal open positions
OVERFLOW_POSITIONS = 5        # Extra slots for 9+ confidence
OVERFLOW_MIN_CONFIDENCE = 9
MAX_TOTAL = MAX_POSITIONS + OVERFLOW_POSITIONS  # Hard cap = 15
MIN_HOLD_DAYS = 5             # Min days before rotation eligible

# ── ALERT THRESHOLDS (ported from V2 — P&L % based, per strategy type) ──
ALERT_THRESHOLDS = {
    # Naked options: thesis revision only — no hard TP/SL, no auto-close
    "BUY_CALL":        (15.0, -15.0),
    "BUY_PUT":         (15.0, -15.0),
    # Debit spreads: hard take profit at +65%, hard stop at -40%
    "BUY_CALL_SPREAD": (65.0, -40.0),
    "BUY_PUT_SPREAD":  (65.0, -40.0),
    # Credit spreads: hard take profit at +65%, hard stop at -40%
    "SELL_PUT_SPREAD":  (65.0, -40.0),
    "SELL_CALL_SPREAD": (65.0, -40.0),
}
DEFAULT_THRESHOLDS = (30.0, -30.0)  # Fallback for unknown types

# Naked options: alert-only, never auto-close
REVISION_ONLY_TYPES = {"BUY_CALL", "BUY_PUT"}

# Spreads get an early thesis revision trigger BEFORE the hard stop
SPREAD_REVISION_LOSS = -30.0

# Naked option trail-stop tiers (no auto-close, just smart advice)
NAKED_PROFIT_TIERS = [
    (50.0, "💰", "STRONG PROFIT",   "Consider tightening trail stop. Lock in gains — SL above +30% level."),
    (30.0, "🎯", "TRAIL STOP",      "Option up 30%+. Consider trailing SL above entry (e.g. +15% level)."),
    (15.0, "🛡️", "BREAKEVEN STOP",  "Move mental SL to entry price — breakeven protection. Let it ride."),
]
NAKED_LOSS_TIERS = [
    (-30.0, "🔴", "DEEP DRAWDOWN",  "Significant loss — strongly consider exit unless thesis has clear near-term catalyst."),
    (-15.0, "🟡", "THESIS CHECK",   "Drawdown exceeds -15%. Review if thesis is intact. Consider SL at -30%."),
]

# ── P&L SIGNAL BANDS (for Google Sheet display) ──
SIGNAL_BANDS = {
    "take_profit": 50.0,   # P&L >= 50% → TAKE PROFIT
    "trail_stop":  20.0,   # P&L 20-50% → TRAIL STOP
    "recheck":    -20.0,   # P&L -20% to -40% → RECHECK THESIS
    "stop_hit":   -40.0,   # P&L <= -40% → CLOSE (stop hit)
}


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    """Initialize the trade log database."""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_date TEXT,
            call_timestamp TEXT,
            ticker TEXT,
            direction TEXT,
            strategy_type TEXT,
            strike REAL,
            strike_2 REAL,
            expiry TEXT,
            dte INTEGER,
            entry_price REAL,
            underlying_at_entry REAL,
            spread_width REAL,
            quality_ratio REAL,
            iv_at_entry REAL,
            iv_hv_ratio REAL,
            target_price REAL,
            stop_price REAL,
            confidence TEXT,
            catalyst TEXT,
            catalyst_status TEXT,
            thesis TEXT,
            repricing_window TEXT,
            tape_read TEXT,
            catalyst_health TEXT,
            cds_score REAL,
            catalyst_action TEXT,
            source_quality TEXT,
            invalidation TEXT,
            status TEXT DEFAULT 'OPEN',
            close_date TEXT,
            close_price REAL,
            close_reason TEXT,
            pnl_percent REAL,
            pnl_dollar REAL,
            high_watermark REAL,
            high_watermark_date TEXT,
            low_watermark REAL,
            low_watermark_date TEXT,
            notes TEXT DEFAULT '',
            eod_action TEXT,
            eod_comment TEXT,
            eod_date TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER,
            snapshot_date TEXT,
            snapshot_timestamp TEXT,
            price REAL,
            pnl_percent REAL,
            days_held INTEGER,
            FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
        )
    """)

    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)",
        "CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(call_date)",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_trade ON price_snapshots(trade_id)",
    ]:
        c.execute(idx_sql)

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# PRICE FETCHER
# ─────────────────────────────────────────────────────────────────────────────
def get_current_price(ticker: str) -> Optional[float]:
    """Get current stock price via yfinance."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        data = t.history(period="1d")
        if not data.empty:
            return float(data["Close"].iloc[-1])
        price = t.fast_info.get("lastPrice") or t.fast_info.get("regularMarketPrice")
        if price:
            return float(price)
    except Exception as e:
        print(f"  ⚠ Price fetch failed for {ticker}: {e}")
    return None


def get_option_price(ticker: str, strike: float, strike_2: float,
                     expiry: str, strategy_type: str) -> Optional[float]:
    """
    Get current option/spread price via yfinance.
    For spreads: returns net value (long_mid - short_mid for debit, short_mid - long_mid for credit).
    For singles: returns mid price.
    Conservative pricing: BUY = (mid+ask)/2, SELL = (mid+bid)/2.
    Returns None if option data unavailable.
    """
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        chain = tk.option_chain(expiry)
    except Exception as e:
        print(f"    ⚠ Option chain fetch failed for {ticker} {expiry}: {e}")
        return None

    is_call = "CALL" in (strategy_type or "").upper()
    is_put = "PUT" in (strategy_type or "").upper()
    is_spread = "SPREAD" in (strategy_type or "").upper()
    is_credit = "SELL" in (strategy_type or "").upper()

    # Pick the right side
    if is_put:
        opts_df = chain.puts
    else:
        opts_df = chain.calls

    if opts_df.empty:
        return None

    def _get_mid(df, s):
        row = df.iloc[(df["strike"] - s).abs().argsort()[:1]]
        bid = float(row["bid"].values[0]) if "bid" in df.columns else 0
        ask = float(row["ask"].values[0]) if "ask" in df.columns else 0
        return (bid + ask) / 2, bid, ask

    try:
        if is_spread and strike_2:
            # For spreads, determine which is long and which is short
            if is_credit:
                # Credit spread: short = closer to money, long = further
                # SELL_PUT_SPREAD: short is higher strike, long is lower
                # SELL_CALL_SPREAD: short is lower strike, long is higher
                if "PUT" in strategy_type:
                    short_s = max(strike, strike_2)
                    long_s = min(strike, strike_2)
                else:
                    short_s = min(strike, strike_2)
                    long_s = max(strike, strike_2)
                short_mid, short_bid, _ = _get_mid(opts_df, short_s)
                long_mid, _, long_ask = _get_mid(opts_df, long_s)
                # Credit spread value = what we'd receive closing
                sell_val = round((short_mid + short_bid) / 2, 2)
                buy_val = round((long_mid + long_ask) / 2, 2)
                return round(sell_val - buy_val, 2)
            else:
                # Debit spread: long = closer to money
                if "CALL" in strategy_type:
                    long_s = min(strike, strike_2)
                    short_s = max(strike, strike_2)
                else:
                    long_s = max(strike, strike_2)
                    short_s = min(strike, strike_2)
                long_mid, _, long_ask = _get_mid(opts_df, long_s)
                short_mid, short_bid, _ = _get_mid(opts_df, short_s)
                buy_val = round((long_mid + long_ask) / 2, 2)
                sell_val = round((short_mid + short_bid) / 2, 2)
                return round(buy_val - sell_val, 2)
        else:
            # Single leg
            mid, bid, ask = _get_mid(opts_df, strike)
            if "SELL" in (strategy_type or ""):
                return round((mid + bid) / 2, 2)
            else:
                return round((mid + ask) / 2, 2)
    except Exception as e:
        print(f"    ⚠ Option price calc failed for {ticker} {strategy_type}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# PARSE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _parse_price_from_text(text: str) -> Optional[float]:
    """Extract a dollar price from text like '$150-160' or '$155.50'."""
    if not text:
        return None
    range_match = re.search(r'\$(\d+(?:\.\d+)?)\s*[-–]\s*\$?(\d+(?:\.\d+)?)', text)
    if range_match:
        return (float(range_match.group(1)) + float(range_match.group(2))) / 2
    single_match = re.search(r'\$(\d+(?:\.\d+)?)', text)
    if single_match:
        return float(single_match.group(1))
    return None


def _parse_confidence(text: str) -> int:
    """Extract numeric confidence from text like '8/10', 'CONV: 8', or 'Medium'."""
    if not text:
        return 0
    text_str = str(text).strip()

    # 1. Numeric format: "8/10"
    m = re.search(r'(\d+)\s*/\s*10', text_str)
    if m:
        return int(m.group(1))

    # 2. Text labels from R1 v4.8 output (longest-first to avoid partial matches)
    text_lower = text_str.lower()
    TEXT_MAP = [
        ("exceptional", 10), ("extreme", 10),
        ("very high", 9),
        ("medium-high", 8), ("moderate-high", 8), ("medium high", 8),
        ("high", 8), ("strong", 8),
        ("medium-low", 5), ("moderate-low", 5), ("medium low", 5),
        ("medium", 7), ("moderate", 7),
        ("very low", 3), ("minimal", 3),
        ("low", 4), ("weak", 4),
    ]
    for label, val in TEXT_MAP:
        if label in text_lower:
            return val

    # 3. Bare number
    m = re.search(r'(\d+)', text_str)
    if m:
        val = int(m.group(1))
        return val if val <= 10 else 0
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# LOAD PIPELINE RESULTS
# ─────────────────────────────────────────────────────────────────────────────
def load_pipeline_results() -> List[Dict]:
    """Load today's pipeline results (survivors)."""
    today = datetime.date.today().isoformat()
    results_file = RESULTS_DIR / f"pipeline_{today}.json"
    if not results_file.exists():
        print(f"  ℹ No pipeline results for today ({results_file})")
        return []
    try:
        data = json.loads(results_file.read_text(encoding="utf-8"))
        survivors = data.get("survivors", [])
        print(f"  📊 Loaded {len(survivors)} survivors from pipeline results")
        return survivors
    except Exception as e:
        print(f"  ❌ Failed to load pipeline results: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# LOG NEW TRADES
# ─────────────────────────────────────────────────────────────────────────────
def log_trades(survivors: List[Dict]) -> int:
    """Log pipeline survivors as new trades. Returns count logged."""
    if not survivors:
        return 0

    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    timestamp = now.isoformat()

    # No stacking same ticker
    open_tickers = {row[0] for row in
                    c.execute("SELECT ticker FROM trades WHERE status='OPEN'").fetchall()}
    open_count = len(open_tickers)

    logged = 0
    for idea in survivors:
        ticker = idea.get("ticker", "").strip().upper()
        if not ticker or len(ticker) > 6:
            continue

        if ticker in open_tickers:
            print(f"  ⚠ {ticker}: already open — skipping")
            continue

        conf = _parse_confidence(str(idea.get("confidence", "0")))
        if conf < MIN_CONFIDENCE:
            print(f"  ⚠ {ticker}: confidence {conf} < {MIN_CONFIDENCE} — skipping")
            continue

        current_open = open_count + logged
        if current_open >= MAX_TOTAL:
            print(f"  ⚠ {ticker}: hard cap {MAX_TOTAL} reached — skipping")
            continue
        if current_open >= MAX_POSITIONS and conf < OVERFLOW_MIN_CONFIDENCE:
            print(f"  ⚠ {ticker}: overflow zone, conf {conf} < {OVERFLOW_MIN_CONFIDENCE} — skipping")
            continue

        # ── Entry price: use structured options price if available, else fetch stock price ──
        strategy_type = idea.get("strategy_type", "")
        entry_price = idea.get("entry_price")
        underlying_at_entry = idea.get("underlying_at_entry")

        if not entry_price:
            entry_price = get_current_price(ticker)
        if not underlying_at_entry:
            underlying_at_entry = get_current_price(ticker)
        if not entry_price:
            print(f"  ⚠ {ticker}: no price available — skipping")
            continue

        direction = idea.get("direction", "Bullish")

        # Parse target/stop — prefer structured values, fall back to text parsing
        target_price = idea.get("target_price") or idea.get("profit_target")
        stop_price = idea.get("stop_price") or idea.get("stop_alert")

        if not target_price:
            target_price = _parse_price_from_text(idea.get("payoff_range", ""))
        if not target_price and not stop_price:
            raw = idea.get("raw_text", "")
            if raw:
                for line in raw.split("\n"):
                    lc = line.strip().lower()
                    if "target" in lc and "$" in line and not target_price:
                        target_price = _parse_price_from_text(line)
                    if "stop" in lc and "$" in line and not stop_price:
                        stop_price = _parse_price_from_text(line)

        # Default target/stop — only for SPREADS and UNDERLYING
        # Single-leg options (BUY_CALL, BUY_PUT) use alerts only, no fixed target/stop
        is_spread = strategy_type and "SPREAD" in str(strategy_type)
        is_underlying = not strategy_type or strategy_type == "UNDERLYING"
        ref_price = underlying_at_entry or entry_price
        if not target_price:
            if is_spread:
                # Spreads: credit target = buy back at 15% of premium (keep 85%), debit target = +65% or width
                target_price = round(entry_price * 1.65, 2) if "BUY" in str(strategy_type) else round(entry_price * 0.15, 2)
            elif is_underlying:
                target_price = ref_price * (1.15 if "bullish" in direction.lower() else 0.85)
            # else: single-leg option — no target (alerts only)
        if not stop_price:
            if is_spread:
                stop_price = round(entry_price * 0.60, 2) if "BUY" in str(strategy_type) else round(entry_price * 1.40, 2)
            elif is_underlying:
                stop_price = ref_price * (0.90 if "bullish" in direction.lower() else 1.10)
            # else: single-leg option — no stop (alerts only)

        conf_raw = idea.get("confidence", "0")

        # Options fields from trade structurer
        strike = idea.get("strike")
        strike_2 = idea.get("strike_2")
        expiry = idea.get("expiry")
        dte = idea.get("dte")
        spread_width = idea.get("spread_width", 0)
        quality_ratio = idea.get("quality_ratio", 0)
        iv_at_entry = idea.get("iv_at_entry")
        iv_hv_ratio = idea.get("iv_hv_ratio")

        # Watermark initial value: use entry_price for options, underlying for stocks
        wm_init = entry_price

        c.execute("""
            INSERT INTO trades (
                call_date, call_timestamp, ticker, direction,
                strategy_type, strike, strike_2, expiry, dte,
                entry_price, underlying_at_entry, spread_width, quality_ratio,
                iv_at_entry, iv_hv_ratio,
                target_price, stop_price,
                confidence, catalyst, catalyst_status, thesis,
                repricing_window, tape_read, catalyst_health, cds_score,
                catalyst_action, source_quality, invalidation,
                status, high_watermark, high_watermark_date,
                low_watermark, low_watermark_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      'OPEN', ?, ?, ?, ?)
        """, (
            today, timestamp, ticker, direction,
            strategy_type or None, strike, strike_2, expiry, dte,
            entry_price, underlying_at_entry, spread_width, quality_ratio,
            iv_at_entry, iv_hv_ratio,
            target_price, stop_price,
            str(conf_raw), idea.get("catalyst", "")[:500],
            idea.get("catalyst_status", ""),
            idea.get("thesis", "")[:500],
            idea.get("repricing_window", ""),
            idea.get("tape_read", ""),
            idea.get("catalyst_health", ""),
            idea.get("cds_score", 0),
            idea.get("catalyst_action", ""),
            idea.get("source_quality", ""),
            idea.get("invalidation", "")[:300],
            wm_init, today,
            wm_init, today,
        ))

        open_tickers.add(ticker)
        logged += 1
        strat_info = f" [{strategy_type}]" if strategy_type else ""
        strike_info = ""
        if strike:
            strike_info = f" ${strike:.0f}" if strike % 1 == 0 else f" ${strike:.2f}"
            if strike_2:
                strike_info += f"/${strike_2:.0f}" if strike_2 % 1 == 0 else f"/${strike_2:.2f}"
            if expiry:
                strike_info += f" exp {expiry}"
        tgt_str = f"${target_price:.2f}" if target_price else "alerts"
        stp_str = f"${stop_price:.2f}" if stop_price else "alerts"
        print(f"  ✅ {ticker}: logged ({direction}{strat_info}{strike_info}, conf={conf_raw}, "
              f"entry=${entry_price:.2f}, target={tgt_str}, stop={stp_str})")

        # Telegram alert for new trade
        try:
            from notify import send_telegram, format_new_trade_alert
            trade_data = {**idea, "underlying_at_entry": entry_price,
                          "target_price": target_price, "stop_price": stop_price}
            msg = format_new_trade_alert(trade_data)
            send_telegram(msg, parse_mode="HTML")
        except Exception as e:
            print(f"  ⚠ Telegram alert failed for {ticker}: {e}")

    conn.commit()
    conn.close()
    print(f"\n  📊 Logged {logged} new trades ({open_count + logged} total open)")
    return logged


# ─────────────────────────────────────────────────────────────────────────────
# PRICE SNAPSHOTS + WATERMARKS
# ─────────────────────────────────────────────────────────────────────────────
def snapshot_all_positions() -> List[Dict]:
    """Snapshot prices for all open positions. Returns list of position dicts."""
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()

    # Check if DB has options columns
    existing_cols = {row[1] for row in c.execute("PRAGMA table_info(trades)").fetchall()}
    has_opts = "strategy_type" in existing_cols

    opts_select = ""
    if has_opts:
        opts_select = ", strategy_type, strike, strike_2, expiry"

    open_trades = c.execute(f"""
        SELECT trade_id, ticker, direction, entry_price, target_price, stop_price,
               confidence, catalyst, thesis, call_date,
               high_watermark, low_watermark, notes,
               catalyst_health, cds_score, tape_read{opts_select}
        FROM trades WHERE status = 'OPEN'
    """).fetchall()

    if not open_trades:
        print("  ℹ No open positions to snapshot")
        conn.close()
        return []

    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    timestamp = now.isoformat()
    positions = []

    for row in open_trades:
        (trade_id, ticker, direction, entry_price, target_price, stop_price,
         confidence, catalyst, thesis, call_date,
         high_wm, low_wm, notes,
         catalyst_health, cds_score, tape_read) = row[:16]

        strategy_type = row[16] if has_opts and len(row) > 16 else None
        strike = row[17] if has_opts and len(row) > 17 else None
        strike_2 = row[18] if has_opts and len(row) > 18 else None
        expiry = row[19] if has_opts and len(row) > 19 else None

        # ── Get price: option price for options trades, stock price for underlying ──
        price = None
        is_option = (strategy_type and strategy_type not in ("UNDERLYING", "Bullish", "Bearish")
                     and strike and expiry)

        if is_option:
            try:
                price = get_option_price(ticker, strike, strike_2, expiry, strategy_type)
                if price is not None:
                    print(f"    📊 {ticker}: option price ${price:.2f} "
                          f"({strategy_type} ${strike}" +
                          (f"/${strike_2}" if strike_2 else "") +
                          f" exp {expiry})")
            except Exception as e:
                print(f"    ⚠ {ticker}: option price failed ({e}), using stock")

        if price is None:
            price = get_current_price(ticker)

        if price is None:
            print(f"  ⚠ {ticker}: no price — skipping snapshot")
            continue

        # P&L calculation
        # For credit spreads: P&L is inverted (we want price to decrease toward 0)
        is_credit = "SELL" in (strategy_type or "")
        if is_credit:
            pnl_pct = ((entry_price - price) / entry_price) * 100 if entry_price else 0
        elif direction == "Bearish" and not is_option:
            pnl_pct = ((entry_price - price) / entry_price) * 100 if entry_price else 0
        else:
            pnl_pct = ((price - entry_price) / entry_price) * 100 if entry_price else 0

        try:
            entry_dt = datetime.datetime.strptime(call_date, "%Y-%m-%d")
            days_held = (now - entry_dt).days
        except (ValueError, TypeError):
            days_held = 0

        # Save snapshot
        c.execute("""
            INSERT INTO price_snapshots (trade_id, snapshot_date, snapshot_timestamp,
                                          price, pnl_percent, days_held)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (trade_id, today, timestamp, price, round(pnl_pct, 2), days_held))

        # Update trade watermarks + P&L
        c.execute("""
            UPDATE trades SET
                pnl_percent = ?,
                pnl_dollar = ?,
                high_watermark = CASE WHEN ? > COALESCE(high_watermark, 0) THEN ? ELSE high_watermark END,
                high_watermark_date = CASE WHEN ? > COALESCE(high_watermark, 0) THEN ? ELSE high_watermark_date END,
                low_watermark = CASE WHEN ? < COALESCE(low_watermark, 999999) THEN ? ELSE low_watermark END,
                low_watermark_date = CASE WHEN ? < COALESCE(low_watermark, 999999) THEN ? ELSE low_watermark_date END
            WHERE trade_id = ?
        """, (
            round(pnl_pct, 2),
            round(price - entry_price, 2) if entry_price else 0,
            price, price, price, today,
            price, price, price, today,
            trade_id,
        ))

        positions.append({
            "trade_id": trade_id, "ticker": ticker, "direction": direction,
            "entry_price": entry_price, "target_price": target_price,
            "stop_price": stop_price, "current_price": price,
            "pnl_pct": round(pnl_pct, 2), "days_held": days_held,
            "confidence": confidence, "catalyst": catalyst, "thesis": thesis,
            "notes": notes or "", "catalyst_health": catalyst_health,
            "cds_score": cds_score, "tape_read": tape_read,
        })

        icon = "🟢" if pnl_pct >= 0 else "🔴"
        print(f"  {icon} {ticker}: ${price:.2f} ({pnl_pct:+.1f}%) day {days_held}")

    conn.commit()
    conn.close()
    return positions


# ─────────────────────────────────────────────────────────────────────────────
# ALERT CHECKER — TP/SL + halfway alerts (same logic as DD Report monitor)
# ─────────────────────────────────────────────────────────────────────────────
def check_alerts(positions: List[Dict]) -> List[str]:
    """
    Check all open positions for threshold breaches (ported from V2).

    Uses P&L PERCENTAGE thresholds per strategy type — not fixed dollar prices.

    Layered system:
      1. SPREADS: Hard auto-close at +65% (TP) or -40% (SL)
                  Early thesis review at -30% (before hard stop)
      2. NAKED OPTIONS (BUY_CALL, BUY_PUT): Thesis revision only at ±15%
                  Tiered trail-stop advice — NEVER auto-close
      3. UNDERLYING: Default ±30% thresholds

    P&L direction:
      - Credit spreads (SELL_*): P&L = (entry - current) / entry (want price DOWN)
      - Debit/Long (BUY_*):     P&L = (current - entry) / entry (want price UP)
      - P&L is ALREADY computed correctly in snapshot_all_positions()
    """
    if not positions:
        return []

    from notify import send_telegram
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    alerts = []
    today = datetime.date.today().isoformat()

    for pos in positions:
        trade_id = pos["trade_id"]
        ticker = pos["ticker"]
        direction = pos["direction"]  # Strategy type: BUY_CALL_SPREAD, SELL_PUT_SPREAD, etc.
        entry = pos["entry_price"]
        price = pos["current_price"]
        pnl_pct = pos["pnl_pct"]     # Already correctly computed (inverted for credits)
        notes = pos["notes"] or ""

        if not entry or entry == 0:
            continue

        # Get thresholds for this strategy type
        profit_thresh, loss_thresh = ALERT_THRESHOLDS.get(direction, DEFAULT_THRESHOLDS)
        is_revision_type = direction in REVISION_ONLY_TYPES
        is_spread = "SPREAD" in (direction or "").upper()
        is_credit = "SELL" in (direction or "").upper()

        # ── Layered threshold check (same as V2) ──
        # 1. Hard TP/SL for spreads → PROFIT / LOSS → auto-close
        # 2. Thesis revision for naked → REVISION_PROFIT / REVISION_LOSS → advice only
        # 3. Early thesis revision for spreads at -30% before -40% hard stop
        triggered = None
        if pnl_pct >= profit_thresh:
            triggered = "REVISION_PROFIT" if is_revision_type else "PROFIT"
        elif pnl_pct <= loss_thresh:
            triggered = "REVISION_LOSS" if is_revision_type else "LOSS"
        elif is_spread and pnl_pct <= SPREAD_REVISION_LOSS:
            triggered = "REVISION_LOSS"  # Early thesis review for spreads

        if not triggered:
            print(f"    {ticker} {direction}: PnL {pnl_pct:+.1f}% "
                  f"(thresh: +{profit_thresh:.0f}%/{loss_thresh:.0f}%) — no alert")
            continue

        # ── Build alert message ──
        dir_display = (direction or "").replace("_", " ").title()
        strike_info = ""
        if pos.get("strike"):
            strike_info = f"${pos['strike']:.0f}"
            if pos.get("strike_2"):
                strike_info += f"/${pos['strike_2']:.0f}"

        if triggered == "PROFIT":
            emoji = "🟢"
            msg = (f"🟢 <b>{ticker}</b> — {dir_display}\n"
                   f"┌ Entry ${entry:.2f} → ${price:.2f}\n"
                   f"│ PnL: {pnl_pct:+.1f}%\n"
                   f"│ {strike_info}\n"
                   f"└ 💰 TAKE PROFIT zone reached (+{profit_thresh:.0f}%)")

        elif triggered == "LOSS":
            emoji = "🔴"
            msg = (f"🔴 <b>{ticker}</b> — {dir_display}\n"
                   f"┌ Entry ${entry:.2f} → ${price:.2f}\n"
                   f"│ PnL: {pnl_pct:+.1f}%\n"
                   f"│ {strike_info}\n"
                   f"└ ⚠️ STOP LOSS threshold breached ({loss_thresh:.0f}%)")

        elif triggered == "REVISION_PROFIT":
            # Tiered trail-stop advice for naked options
            tier_emoji, tier_label, tier_advice = "📋", "PROFIT REVIEW", f"Profit exceeds +{profit_thresh:.0f}%."
            for min_pnl, t_emoji, t_label, t_advice in NAKED_PROFIT_TIERS:
                if pnl_pct >= min_pnl:
                    tier_emoji, tier_label, tier_advice = t_emoji, t_label, t_advice
                    break
            emoji = tier_emoji
            msg = (f"{tier_emoji} <b>{ticker}</b> — {dir_display}\n"
                   f"┌ Entry ${entry:.2f} → ${price:.2f}\n"
                   f"│ PnL: {pnl_pct:+.1f}%\n"
                   f"│ {strike_info}\n"
                   f"└ {tier_emoji} {tier_label}: {tier_advice}")

        else:  # REVISION_LOSS
            if is_revision_type:
                # Naked option loss tiers
                tier_emoji, tier_label, tier_advice = "🟡", "THESIS CHECK", f"Drawdown exceeds {loss_thresh:.0f}%."
                for min_pnl, t_emoji, t_label, t_advice in NAKED_LOSS_TIERS:
                    if pnl_pct <= min_pnl:
                        tier_emoji, tier_label, tier_advice = t_emoji, t_label, t_advice
                        break
                emoji = tier_emoji
                msg = (f"{tier_emoji} <b>{ticker}</b> — {dir_display}\n"
                       f"┌ Entry ${entry:.2f} → ${price:.2f}\n"
                       f"│ PnL: {pnl_pct:+.1f}%\n"
                       f"│ {strike_info}\n"
                       f"└ {tier_emoji} {tier_label}: {tier_advice}")
            else:
                # Spread early revision (at -30% before -40% hard stop)
                thresh_display = SPREAD_REVISION_LOSS if (is_spread and pnl_pct > loss_thresh) else loss_thresh
                emoji = "🟡"
                msg = (f"🟡 <b>{ticker}</b> — {dir_display}\n"
                       f"┌ Entry ${entry:.2f} → ${price:.2f}\n"
                       f"│ PnL: {pnl_pct:+.1f}%\n"
                       f"│ {strike_info}\n"
                       f"└ 📋 Thesis review — drawdown exceeds {thresh_display:.0f}%")

        print(f"  🚨 {ticker}: {triggered} (PnL {pnl_pct:+.1f}%)")

        # Send Telegram
        send_telegram(msg, parse_mode="HTML")
        alerts.append(f"{ticker}: {triggered}")

        # ── Auto-close ONLY for spreads on hard PROFIT/LOSS ──
        # Naked options (BUY_CALL, BUY_PUT) are NEVER auto-closed
        if triggered in ("PROFIT", "LOSS") and not is_revision_type:
            close_reason = "STOPPED_OUT" if triggered == "LOSS" else "TARGET_HIT"
            pnl_dollar = round(entry - price, 2) if is_credit else round(price - entry, 2)
            c.execute("""
                UPDATE trades SET status = 'CLOSED', close_date = ?,
                close_price = ?, close_reason = ?,
                pnl_percent = ?, pnl_dollar = ?
                WHERE trade_id = ?
            """, (today, price, close_reason, round(pnl_pct, 2),
                  pnl_dollar, trade_id))
            print(f"    🔒 {ticker} auto-closed: {close_reason} @ ${price:.2f} | PnL: {pnl_pct:+.1f}%")

    conn.commit()
    conn.close()

    if alerts:
        print(f"\n  🚨 {len(alerts)} alerts fired")
    else:
        print(f"\n  ✅ No alerts — all positions within thresholds")
    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────
def close_trade(ticker: str, reason: str = "MANUAL") -> bool:
    """Manually close an open trade by ticker."""
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    today = datetime.date.today().isoformat()

    trade = c.execute(
        "SELECT trade_id, entry_price, direction FROM trades WHERE ticker = ? AND status = 'OPEN'",
        (ticker.upper(),)
    ).fetchone()

    if not trade:
        print(f"  ⚠ No open trade found for {ticker}")
        conn.close()
        return False

    trade_id, entry_price, direction = trade
    price = get_current_price(ticker.upper())
    if price and entry_price:
        if direction == "Bearish":
            pnl = ((entry_price - price) / entry_price) * 100
        else:
            pnl = ((price - entry_price) / entry_price) * 100
        pnl_dollar = price - entry_price
    else:
        pnl, pnl_dollar = 0, 0

    c.execute("""
        UPDATE trades SET status = 'CLOSED', close_date = ?,
        close_price = ?, close_reason = ?,
        pnl_percent = ?, pnl_dollar = ?
        WHERE trade_id = ?
    """, (today, price, reason, round(pnl, 2), round(pnl_dollar, 2), trade_id))

    conn.commit()
    conn.close()
    print(f"  ✅ {ticker} closed: {reason} (P&L: {pnl:+.1f}%)")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="ORCA V3 — Trade Logger")
    parser.add_argument("--alerts-only", action="store_true",
                        help="Only snapshot prices and check alerts")
    parser.add_argument("--close", type=str, default="",
                        help="Manually close a position by ticker")
    args = parser.parse_args()

    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║  ⚡ ORCA V3 — Trade Logger                                    ║
║  Mode: {'Alerts Only' if args.alerts_only else 'Full (log + alerts)'}                                        ║
╚═══════════════════════════════════════════════════════════════╝""")

    init_db()

    if args.close:
        close_trade(args.close)
        return

    if not args.alerts_only:
        print("\n  📥 Loading pipeline survivors...")
        survivors = load_pipeline_results()
        if survivors:
            log_trades(survivors)
        else:
            print("  ℹ No new survivors to log")

    print(f"\n  📸 Snapshotting open positions...")
    positions = snapshot_all_positions()

    if positions:
        print(f"\n  🔍 Checking alerts...")
        check_alerts(positions)

    try:
        from sheet_sync import sync_trades_to_sheet
        print(f"\n  📊 Syncing to Google Sheet...")
        sync_trades_to_sheet()
    except ImportError:
        print("  ⚠ sheet_sync.py not available — skipping")
    except Exception as e:
        print(f"  ⚠ Sheet sync failed: {e}")


if __name__ == "__main__":
    main()
