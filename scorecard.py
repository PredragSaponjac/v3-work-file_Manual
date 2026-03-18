#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║  🏆 ORCA SCORECARD — Weekly Performance Audit                 ║
║  Friday 4:15 PM CT — After market close                       ║
║                                                               ║
║  Mechanical:  PnL calc, high/low watermarks, hit rate         ║
║  Intelligent: Claude Opus 4.6 evaluates thesis validity       ║
║  Output:      Telegram report + CSV + SQLite                  ║
║                                                               ║
║  Modes:                                                       ║
║    --weekly   (default) Full position review + auto-close     ║
║    --monthly  Cumulative performance report + track record    ║
╚═══════════════════════════════════════════════════════════════╝

This is NOT a pure mechanical task. For each open position, Claude Opus 4.6
with adaptive extended thinking evaluates:
  - Is the original catalyst/thesis still alive?
  - Has the market already priced in the move?
  - Are there new developments that change the thesis?
  - Should the subscriber HOLD or CLOSE?

Auto-close batch: Positions marked CLOSE are flagged as ACTION ITEMS.
Monthly report: Tracks win rate, PnL, and Opus accuracy over time.

The subscriber makes the final decision — we provide the intelligence.
"""

import os
import sys
import json
import sqlite3
import datetime
import requests
from pathlib import Path

# Auto-load .env for local runs (GitHub Actions uses secrets)
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            key, val = k.strip(), v.strip()
            if not os.environ.get(key):
                os.environ[key] = val
from typing import List, Dict, Tuple, Optional

try:
    import yfinance as yf
except ImportError:
    yf = None

# ============================================================
# CONFIG
# ============================================================
DB_PATH = Path("orca_trade_log.db")
RESULTS_DIR = Path("orca_results")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

# Cost tracking
OPUS_INPUT_COST_PER_1K = 0.015     # $/1K input tokens (check current pricing)
OPUS_OUTPUT_COST_PER_1K = 0.075    # $/1K output tokens
MAX_SCORECARD_BUDGET = 2.00         # Max $ to spend on one scorecard run


# ============================================================
# DATABASE ACCESS
# ============================================================
def get_open_trades(conn: sqlite3.Connection) -> List[Dict]:
    """Fetch all OPEN trades with their latest snapshot and watermarks."""
    rows = conn.execute("""
        SELECT
            t.trade_id, t.call_date, t.call_timestamp, t.ticker, t.direction,
            t.strike, t.strike_2, t.strike_3, t.strike_4,
            t.expiry, t.entry_price, t.entry_bid, t.entry_ask, t.entry_mid,
            t.underlying_at_entry, t.confidence, t.urgency,
            t.thesis, t.edge, t.entry_raw, t.source_file,
            -- Watermarks
            t.option_high_watermark, t.option_high_watermark_date,
            t.option_low_watermark, t.option_low_watermark_date,
            -- Latest snapshot
            (SELECT underlying_price FROM price_snapshots
             WHERE trade_id = t.trade_id ORDER BY snapshot_date DESC LIMIT 1) as last_underlying,
            (SELECT option_mid FROM price_snapshots
             WHERE trade_id = t.trade_id ORDER BY snapshot_date DESC LIMIT 1) as last_option_mid,
            -- Snapshot count (how many data points we have)
            (SELECT COUNT(*) FROM price_snapshots
             WHERE trade_id = t.trade_id) as snapshot_count
        FROM trades t
        WHERE t.status = 'OPEN'
        ORDER BY t.call_date DESC
    """).fetchall()

    columns = [
        "trade_id", "call_date", "call_timestamp", "ticker", "direction",
        "strike", "strike_2", "strike_3", "strike_4",
        "expiry", "entry_price", "entry_bid", "entry_ask", "entry_mid",
        "underlying_at_entry", "confidence", "urgency",
        "thesis", "edge", "entry_raw", "source_file",
        "option_high_watermark", "option_high_watermark_date",
        "option_low_watermark", "option_low_watermark_date",
        "last_underlying", "last_option_mid", "snapshot_count"
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_closed_trades(conn: sqlite3.Connection, since_date: str = None) -> List[Dict]:
    """Fetch closed trades, optionally since a date."""
    query = "SELECT * FROM trades WHERE status != 'OPEN'"
    params = []
    if since_date:
        query += " AND close_date >= ?"
        params.append(since_date)
    query += " ORDER BY close_date DESC"

    rows = conn.execute(query, params).fetchall()
    columns = [desc[0] for desc in conn.execute("SELECT * FROM trades LIMIT 0").description]
    return [dict(zip(columns, row)) for row in rows]


def get_trade_snapshots(conn: sqlite3.Connection, trade_id: int) -> List[Dict]:
    """Get all price snapshots for a trade (for watermark calculation)."""
    rows = conn.execute("""
        SELECT * FROM price_snapshots
        WHERE trade_id = ?
        ORDER BY snapshot_date ASC
    """, (trade_id,)).fetchall()
    columns = [desc[0] for desc in conn.execute("SELECT * FROM price_snapshots LIMIT 0").description]
    return [dict(zip(columns, row)) for row in rows]


# ============================================================
# PRICE ENGINE — Current + Watermarks
# ============================================================
def fetch_current_prices(trades: List[Dict]) -> Dict[int, Dict]:
    """
    For each open trade, fetch:
    - Current underlying price
    - Current option price (bid/ask/mid)
    - Historical high/low of underlying since entry
    """
    if yf is None:
        return {}

    results = {}

    for trade in trades:
        trade_id = trade["trade_id"]
        ticker = trade["ticker"]
        strike = trade["strike"]
        expiry = trade["expiry"]
        direction = trade["direction"]
        call_date = trade["call_date"]

        try:
            stock = yf.Ticker(ticker)
            current = stock.fast_info.get("lastPrice") or stock.info.get("currentPrice", 0)

            result = {
                "current_underlying": current,
                "underlying_change_pct": 0,
                "option_current_mid": None,
                "option_current_bid": None,
                "option_current_ask": None,
                "underlying_high_since_entry": current,
                "underlying_low_since_entry": current,
                "days_held": 0,
                "dte_remaining": None,
            }

            # Days held
            try:
                entry_dt = datetime.datetime.strptime(call_date, "%Y-%m-%d")
                result["days_held"] = (datetime.datetime.now() - entry_dt).days
            except (ValueError, TypeError):
                pass

            # DTE remaining
            if expiry:
                try:
                    exp_dt = datetime.datetime.strptime(expiry, "%Y-%m-%d")
                    result["dte_remaining"] = (exp_dt - datetime.datetime.now()).days
                except (ValueError, TypeError):
                    pass

            # Underlying change
            if trade.get("underlying_at_entry") and trade["underlying_at_entry"] > 0:
                result["underlying_change_pct"] = (
                    (current - trade["underlying_at_entry"]) / trade["underlying_at_entry"]
                ) * 100

            # Historical high/low since entry (for watermarks)
            try:
                hist = stock.history(start=call_date, interval="1d")
                if not hist.empty:
                    result["underlying_high_since_entry"] = float(hist["High"].max())
                    result["underlying_low_since_entry"] = float(hist["Low"].min())
            except Exception:
                pass

            # Current option price
            if strike and expiry:
                try:
                    available = stock.options
                    if available:
                        closest_exp = min(available, key=lambda x: abs(
                            datetime.datetime.strptime(x, "%Y-%m-%d") -
                            datetime.datetime.strptime(expiry, "%Y-%m-%d")
                        ))
                        chain = stock.option_chain(closest_exp)
                        is_put = "PUT" in direction.upper()
                        opts = chain.puts if is_put else chain.calls

                        if not opts.empty:
                            idx = (opts["strike"] - strike).abs().idxmin()
                            row = opts.loc[idx]
                            result["option_current_bid"] = float(row.get("bid", 0))
                            result["option_current_ask"] = float(row.get("ask", 0))
                            result["option_current_mid"] = float(
                                (row.get("bid", 0) + row.get("ask", 0)) / 2
                            )
                            result["option_iv"] = float(row.get("impliedVolatility", 0))
                            result["option_volume"] = int(row.get("volume", 0)) if row.get("volume") else 0
                            result["option_oi"] = int(row.get("openInterest", 0)) if row.get("openInterest") else 0
                except Exception as e:
                    print(f"  ⚠ Option chain failed for {ticker}: {e}")

            results[trade_id] = result

        except Exception as e:
            print(f"  ⚠ Price fetch failed for {ticker}: {e}")
            results[trade_id] = {"current_underlying": 0, "error": str(e)}

    return results


# ============================================================
# POLYGON OPTIONS API — Daily OHLCV History
# ============================================================
def build_occ_symbol(ticker: str, expiry: str, direction: str, strike: float) -> Optional[str]:
    """
    Build OCC option symbol for Polygon API.

    Format: O:TICKER YYMMDD C|P STRIKE*1000 (8 digits zero-padded)
    Example: O:CRWD260314C00310000
    """
    if not all([ticker, expiry, strike]):
        return None

    try:
        exp_dt = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    date_str = exp_dt.strftime("%y%m%d")
    opt_type = "P" if "PUT" in direction.upper() else "C"
    strike_int = int(strike * 1000)
    strike_str = f"{strike_int:08d}"

    return f"O:{ticker}{date_str}{opt_type}{strike_str}"


def fetch_polygon_option_history(
    trades: List[Dict],
) -> Dict[int, Dict]:
    """
    For each open trade, fetch daily OHLCV from Polygon for the option contract.
    Returns per-trade dict with:
      - daily_bars: list of {date, open, high, low, close, volume}
      - peak_price: highest intraday price the option ever hit while we held it
      - peak_date: when the peak occurred
      - trough_price: lowest price seen
      - trough_date: when the trough occurred
      - polygon_current: latest close from Polygon
      - entry_day_note: whether entry-day high might include pre-entry movement

    For entry day: fetches hourly bars and filters to post-entry hours.
    For all other days: uses daily OHLCV highs.
    """
    if not POLYGON_API_KEY:
        print("  ⚠ POLYGON_API_KEY not set — option history skipped")
        return {}

    results = {}
    today = datetime.date.today().strftime("%Y-%m-%d")

    for trade in trades:
        trade_id = trade["trade_id"]
        ticker = trade["ticker"]
        strike = trade["strike"]
        expiry = trade.get("expiry")
        direction = trade.get("direction", "")
        call_date = trade.get("call_date")
        call_timestamp = trade.get("call_timestamp", "")

        occ = build_occ_symbol(ticker, expiry, direction, strike)
        if not occ:
            continue

        print(f"  📊 Polygon: {occ}")

        try:
            result = {
                "occ_symbol": occ,
                "daily_bars": [],
                "peak_price": None,
                "peak_date": None,
                "trough_price": None,
                "trough_date": None,
                "polygon_current": None,
                "entry_day_note": "",
            }

            # --- Fetch daily OHLCV from entry date to today ---
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{occ}"
                f"/range/1/day/{call_date}/{today}"
                f"?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
            )
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"    ⚠ Polygon {resp.status_code}: {resp.text[:100]}")
                results[trade_id] = result
                continue

            data = resp.json()
            bars = data.get("results", [])

            if not bars:
                print(f"    ⚠ No Polygon data for {occ}")
                results[trade_id] = result
                continue

            # Parse daily bars
            daily_bars = []
            for bar in bars:
                ts = bar.get("t", 0)
                bar_date = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else "?"
                daily_bars.append({
                    "date": bar_date,
                    "open": bar.get("o", 0),
                    "high": bar.get("h", 0),
                    "low": bar.get("l", 0),
                    "close": bar.get("c", 0),
                    "volume": bar.get("v", 0),
                })
            result["daily_bars"] = daily_bars

            # Latest close = current Polygon price
            if daily_bars:
                result["polygon_current"] = daily_bars[-1]["close"]

            # --- Handle entry day separately ---
            # Try to get hourly bars for entry day to find post-entry peak
            entry_hour = 0
            if call_timestamp:
                try:
                    entry_dt = datetime.datetime.fromisoformat(call_timestamp.replace("Z", "+00:00"))
                    entry_hour = entry_dt.hour
                except (ValueError, TypeError):
                    pass

            post_entry_highs = []
            post_entry_lows = []

            for i, bar in enumerate(daily_bars):
                if i == 0 and bar["date"] == call_date and entry_hour > 0:
                    # Entry day: try hourly bars for precision
                    hourly = _fetch_polygon_hourly(occ, call_date, entry_hour)
                    if hourly:
                        post_entry_highs.extend([h["high"] for h in hourly])
                        post_entry_lows.extend([h["low"] for h in hourly])
                        result["entry_day_note"] = f"entry-day filtered to post-{entry_hour}:00 UTC"
                    else:
                        # Fallback: use full day but note it
                        post_entry_highs.append(bar["high"])
                        post_entry_lows.append(bar["low"])
                        result["entry_day_note"] = "entry-day high includes full day (pre+post entry)"
                else:
                    # Subsequent days: full daily OHLC is valid
                    post_entry_highs.append(bar["high"])
                    post_entry_lows.append(bar["low"])

            # Calculate peak and trough
            if post_entry_highs:
                peak_val = max(post_entry_highs)
                peak_idx = post_entry_highs.index(peak_val)
                # Map back to date (approximate — good enough)
                if peak_idx < len(daily_bars):
                    result["peak_price"] = peak_val
                    result["peak_date"] = daily_bars[min(peak_idx, len(daily_bars) - 1)]["date"]
                else:
                    result["peak_price"] = peak_val
                    result["peak_date"] = daily_bars[-1]["date"]

            if post_entry_lows:
                trough_val = min(post_entry_lows)
                trough_idx = post_entry_lows.index(trough_val)
                if trough_idx < len(daily_bars):
                    result["trough_price"] = trough_val
                    result["trough_date"] = daily_bars[min(trough_idx, len(daily_bars) - 1)]["date"]
                else:
                    result["trough_price"] = trough_val
                    result["trough_date"] = daily_bars[-1]["date"]

            results[trade_id] = result

            peak_str = f"peak ${result['peak_price']:.2f}" if result['peak_price'] else "no peak"
            print(f"    ✅ {len(daily_bars)} bars, {peak_str}")

        except Exception as e:
            print(f"    ⚠ Polygon error for {ticker}: {e}")

    return results


def _fetch_polygon_hourly(occ: str, date: str, entry_hour_utc: int) -> List[Dict]:
    """
    Fetch hourly bars for an option on a specific date,
    returning only bars AFTER the entry hour.
    """
    try:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{occ}"
            f"/range/1/hour/{date}/{date}"
            f"?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []

        data = resp.json()
        bars = data.get("results", [])

        post_entry = []
        for bar in bars:
            ts = bar.get("t", 0)
            bar_dt = datetime.datetime.fromtimestamp(ts / 1000)
            if bar_dt.hour >= entry_hour_utc:
                post_entry.append({
                    "hour": bar_dt.hour,
                    "high": bar.get("h", 0),
                    "low": bar.get("l", 0),
                    "close": bar.get("c", 0),
                })

        return post_entry

    except Exception:
        return []


def calc_polygon_pnl(trade: Dict, polygon_data: Dict) -> Dict:
    """Calculate PnL metrics using Polygon historical data."""
    entry = trade.get("entry_price") or 0
    direction = trade.get("direction", "")
    peak = polygon_data.get("peak_price")
    trough = polygon_data.get("trough_price")
    current = polygon_data.get("polygon_current")

    result = {
        "peak_pnl_pct": None,
        "peak_pnl_dollar": None,
        "trough_pnl_pct": None,
        "current_pnl_pct": None,
        "current_pnl_dollar": None,
        "left_on_table_pct": None,  # difference between peak PnL and current PnL
    }

    if not entry or entry <= 0:
        return result

    is_sell = "SELL" in direction.upper()

    # For SELL trades: profit when option price goes DOWN
    # For BUY trades: profit when option price goes UP

    if peak is not None:
        if is_sell:
            result["peak_pnl_pct"] = ((entry - trough) / entry * 100) if trough else None
            result["peak_pnl_dollar"] = ((entry - trough) * 100) if trough else None
        else:
            result["peak_pnl_pct"] = ((peak - entry) / entry) * 100
            result["peak_pnl_dollar"] = (peak - entry) * 100

    if current is not None:
        if is_sell:
            result["current_pnl_pct"] = ((entry - current) / entry) * 100
            result["current_pnl_dollar"] = (entry - current) * 100
        else:
            result["current_pnl_pct"] = ((current - entry) / entry) * 100
            result["current_pnl_dollar"] = (current - entry) * 100

    # How much we left on the table
    if result["peak_pnl_pct"] is not None and result["current_pnl_pct"] is not None:
        result["left_on_table_pct"] = result["peak_pnl_pct"] - result["current_pnl_pct"]

    return result


def backfill_closed_trade_pnl(conn: sqlite3.Connection) -> int:
    """
    Backfill PnL for closed/expired trades missing pnl_percent.

    Uses three data sources in priority order:
    1. Polygon.io — fetch option price on/near close date
    2. Our own price_snapshots — last snapshot before close
    3. Expiry logic — if expired past expiry date, assume $0 for OTM

    Returns count of trades updated.
    """
    rows = conn.execute("""
        SELECT trade_id, ticker, direction, strike, expiry, call_date,
               close_date, close_reason, entry_price, status,
               option_high_watermark, underlying_at_entry
        FROM trades
        WHERE status != 'OPEN'
          AND pnl_percent IS NULL
          AND entry_price IS NOT NULL
          AND entry_price > 0
        ORDER BY close_date DESC
    """).fetchall()

    if not rows:
        print("  ✅ All closed trades already have PnL data")
        return 0

    print(f"  🔍 Found {len(rows)} closed trades missing PnL — backfilling...")
    updated = 0

    for row in rows:
        tid, ticker, direction, strike, expiry, call_date, \
            close_date, close_reason, entry_price, status, high_wm, underlying_entry = row

        close_price = None
        source = ""

        # ── SOURCE 1: Polygon API ──
        if POLYGON_API_KEY and ticker and strike and expiry and direction:
            occ = build_occ_symbol(ticker, expiry, direction, strike)
            if occ:
                # Determine the date range to query
                end_date = close_date or expiry or datetime.date.today().isoformat()
                start_date = call_date or end_date

                try:
                    url = (
                        f"https://api.polygon.io/v2/aggs/ticker/{occ}"
                        f"/range/1/day/{start_date}/{end_date}"
                        f"?adjusted=true&sort=desc&limit=5&apiKey={POLYGON_API_KEY}"
                    )
                    resp = requests.get(url, timeout=15)
                    if resp.status_code == 200:
                        bars = resp.json().get("results", [])
                        if bars:
                            # Use the last available bar's close price
                            close_price = bars[0].get("c", 0)
                            bar_ts = bars[0].get("t", 0)
                            bar_date = datetime.datetime.fromtimestamp(
                                bar_ts / 1000
                            ).strftime("%Y-%m-%d") if bar_ts else "?"
                            source = f"Polygon ({bar_date})"
                except Exception as e:
                    print(f"    ⚠ Polygon backfill error for {ticker}: {e}")

        # ── SOURCE 2: Our own price_snapshots ──
        if close_price is None:
            snap = conn.execute("""
                SELECT option_mid FROM price_snapshots
                WHERE trade_id = ?
                  AND option_mid IS NOT NULL AND option_mid > 0
                ORDER BY snapshot_date DESC LIMIT 1
            """, (tid,)).fetchone()
            if snap:
                close_price = snap[0]
                source = "last snapshot"

        # ── SOURCE 3: Expiry logic ──
        if close_price is None and status == "EXPIRED" and expiry:
            try:
                exp_dt = datetime.datetime.strptime(expiry, "%Y-%m-%d")
                if exp_dt < datetime.datetime.now():
                    # Option expired — assume worthless ($0) for OTM
                    # This is correct for most cases; ITM options would have
                    # been exercised/assigned, which we can't track automatically
                    close_price = 0.0
                    source = "expired (assumed $0)"
            except (ValueError, TypeError):
                pass

        # ── Calculate PnL and update ──
        if close_price is not None:
            is_sell = "SELL" in (direction or "").upper()
            if is_sell:
                pnl_pct = ((entry_price - close_price) / entry_price) * 100
                pnl_dollar = (entry_price - close_price) * 100
            else:
                pnl_pct = ((close_price - entry_price) / entry_price) * 100
                pnl_dollar = (close_price - entry_price) * 100

            conn.execute("""
                UPDATE trades
                SET close_price = ?, pnl_percent = ?, pnl_dollar = ?
                WHERE trade_id = ?
            """, (close_price, pnl_pct, pnl_dollar, tid))

            emoji = "🟢" if pnl_pct > 0 else "🔴"
            print(f"    {emoji} {ticker} {direction} → {pnl_pct:+.1f}% (${close_price:.2f} via {source})")
            updated += 1
        else:
            print(f"    ⚪ {ticker} {direction} — no price data found anywhere")

    conn.commit()
    print(f"  ✅ Backfilled PnL for {updated}/{len(rows)} trades")
    return updated


def calculate_pnl(trade: Dict, prices: Dict) -> Dict:
    """Calculate PnL metrics for a single trade."""
    entry = trade.get("entry_price", 0)
    current_mid = prices.get("option_current_mid")
    direction = trade.get("direction", "")

    pnl = {"pnl_dollar": 0, "pnl_percent": 0, "status_label": "UNKNOWN"}

    if entry and entry > 0 and current_mid is not None and current_mid > 0:
        if "SELL" in direction:
            # For premium selling, profit = entry - current
            pnl["pnl_dollar"] = (entry - current_mid) * 100  # per contract
            pnl["pnl_percent"] = ((entry - current_mid) / entry) * 100
        else:
            # For buying, profit = current - entry
            pnl["pnl_dollar"] = (current_mid - entry) * 100
            pnl["pnl_percent"] = ((current_mid - entry) / entry) * 100

        if pnl["pnl_percent"] > 50:
            pnl["status_label"] = "🟢 STRONG WIN"
        elif pnl["pnl_percent"] > 10:
            pnl["status_label"] = "🟢 WINNING"
        elif pnl["pnl_percent"] > -10:
            pnl["status_label"] = "🟡 FLAT"
        elif pnl["pnl_percent"] > -50:
            pnl["status_label"] = "🔴 LOSING"
        else:
            pnl["status_label"] = "🔴 DEEP RED"
    elif not current_mid:
        pnl["status_label"] = "⚪ NO PRICE DATA"
    else:
        pnl["status_label"] = "⚪ MISSING ENTRY"

    return pnl


def _format_strikes(trade: Dict) -> str:
    """Format strikes for display based on strategy type."""
    direction = trade.get("direction", "")
    s1 = trade.get("strike")
    s2 = trade.get("strike_2")
    s3 = trade.get("strike_3")
    s4 = trade.get("strike_4")

    if "IC" in direction and s1 and s2 and s3 and s4:
        return f"${s1}P/${s2}P — ${s3}C/${s4}C (Iron Condor)"
    elif "SPREAD" in direction and s1 and s2:
        return f"${s1}/${s2} spread"
    elif s1:
        return f"${s1}"
    else:
        return "N/A"


def _calc_peak_pnl(trade: Dict, pnl_data: Dict) -> Dict:
    """Calculate peak PnL from watermarks vs entry price."""
    entry = trade.get("entry_price") or 0
    direction = trade.get("direction", "")
    high_wm = trade.get("option_high_watermark")
    low_wm = trade.get("option_low_watermark")
    high_date = trade.get("option_high_watermark_date") or "N/A"

    result = {"peak_pct": 0, "peak_dollar": 0, "note": "no watermark data"}

    if not entry or entry <= 0 or not high_wm:
        return result

    if "SELL" in direction:
        # For sells: best PnL = entry - lowest price seen (option decayed)
        if low_wm is not None:
            best_pnl_pct = ((entry - low_wm) / entry) * 100
            best_pnl_dollar = (entry - low_wm) * 100
            result = {
                "peak_pct": best_pnl_pct,
                "peak_dollar": best_pnl_dollar,
                "note": f"option low ${low_wm:.2f} on {trade.get('option_low_watermark_date', 'N/A')}"
            }
    else:
        # For buys: best PnL = highest price seen - entry
        best_pnl_pct = ((high_wm - entry) / entry) * 100
        best_pnl_dollar = (high_wm - entry) * 100
        result = {
            "peak_pct": best_pnl_pct,
            "peak_dollar": best_pnl_dollar,
            "note": f"option high ${high_wm:.2f} on {high_date}"
        }

    return result


# ============================================================
# CLAUDE OPUS 4.6 — THESIS EVALUATOR
# ============================================================

THESIS_EVAL_SYSTEM = """You are ORCA Scorecard Analyst — a senior options strategist conducting a FRIDAY CLOSE portfolio review.

This is a DECISION-MAKING session. Every position must get a clear verdict.

For each open position, you receive:
- The original trade call (ticker, direction, strike, expiry, thesis, edge, confidence)
- EXACT timestamp of when the call was made
- Current market data (underlying price, option price, PnL, days held, DTE remaining)
- Entry pricing breakdown (bid/ask/mid at time of call, conservative entry used)
- OPTION PRICE WATERMARKS: the best and worst option prices seen across all daily snapshots since entry
- PEAK PnL: the best PnL this position reached (so we can see if we left money on the table)
- Daily snapshot count (how many data points we have for this position)

Your job is to evaluate EACH position with deep reasoning:

1. **THESIS STATUS**: Is the original catalyst/thesis still alive and valid?
   - Has the event happened yet? If not, is it still expected?
   - Has new information confirmed or contradicted the thesis?
   - Has the market already priced in the move (IV crush, gap already happened)?

2. **POSITION ASSESSMENT**: Given current PnL and market conditions:
   - Is this a winner that should be held for more upside, or locked in?
   - Is this a loser where the thesis is dead, or needs more time?
   - Is time decay (theta) working for or against us?
   - IMPORTANT: If DTE < 7 and position is losing, strongly consider CLOSE.
   - IMPORTANT: If PnL < -40%, thesis must be exceptionally strong to justify HOLD.
   - IMPORTANT: If peak PnL was +30%+ and current is +5% or less, that's a missed exit.

3. **RECOMMENDATION**: For each position, provide ONE of:
   - ✅ HOLD — thesis intact, position has room to run
   - ⏳ HOLD (NEEDS TIME) — thesis valid but catalyst hasn't fired yet
   - 🎯 TAKE PROFIT — thesis played out, lock in gains NOW
   - ⚠️ REVIEW — thesis weakened, subscriber should evaluate Monday
   - ❌ CLOSE — thesis dead, risk/reward broken, or time decay killing us

4. **KEY INSIGHT**: One sentence on what changed (or didn't) this week.

IMPORTANT RULES:
- Be AGGRESSIVE about recommending CLOSE on dead theses. Don't let losers linger.
- If a position already hit +30%+ and fell back, recommend TAKE PROFIT or CLOSE.
- If DTE < 5 and losing, almost always recommend CLOSE (theta is destroying value).
- Be direct and honest. If a call was wrong, say so — that builds trust.
- Don't sugarcoat losses but do recognize when a thesis needs more time.

OUTPUT FORMAT for each trade:
POSITION: [ticker] [direction] [strike] [expiry]
THESIS STATUS: [ALIVE / WEAKENED / DEAD / PLAYED OUT]
RECOMMENDATION: [HOLD / HOLD (NEEDS TIME) / TAKE PROFIT / REVIEW / CLOSE]
KEY INSIGHT: [one sentence]
---

After ALL positions, provide:

AUTO-CLOSE BATCH:
List ONLY the tickers you recommend closing. Format:
CLOSE: [ticker] [direction] [strike] — [one-line reason]
(If no positions to close, write "No auto-close recommendations this week.")

PORTFOLIO SUMMARY: [2-3 sentences on overall portfolio health and any macro shifts]
SELF-EVALUATION: Grade yourself A-F on your recent calls. What patterns are you seeing in your wins vs losses? What would you do differently? Be brutally honest.
LESSON OF THE WEEK: [one actionable learning from this week's results]"""


def evaluate_positions_with_claude(
    open_trades: List[Dict],
    prices: Dict[int, Dict],
    pnls: Dict[int, Dict],
    polygon_data: Dict[int, Dict] = None
) -> Tuple[str, float]:
    """
    Send all open positions to Claude Opus 4.6 with adaptive thinking
    for deep thesis evaluation.

    Returns: (evaluation_text, api_cost)
    """
    if not ANTHROPIC_API_KEY:
        return "⚠ ANTHROPIC_API_KEY not set — thesis evaluation skipped", 0.0

    if not open_trades:
        return "No open positions to evaluate.", 0.0

    # Build the position context
    positions_text = "OPEN POSITIONS FOR REVIEW:\n" + "=" * 60 + "\n\n"

    if polygon_data is None:
        polygon_data = {}

    for trade in open_trades:
        tid = trade["trade_id"]
        price_data = prices.get(tid, {})
        pnl_data = pnls.get(tid, {})
        poly = polygon_data.get(tid, {})
        poly_pnl = calc_polygon_pnl(trade, poly) if poly else {}

        # Format strikes based on strategy type
        strike_str = _format_strikes(trade)

        # Build Polygon history section
        polygon_section = ""
        if poly.get("peak_price"):
            polygon_section = f"""
  POLYGON OPTION HISTORY (daily OHLCV from entry to today):
    OCC Symbol:     {poly.get('occ_symbol', 'N/A')}
    Data Points:    {len(poly.get('daily_bars', []))} trading days
    Peak Price:     ${poly['peak_price']:.2f} on {poly.get('peak_date', '?')}
    Trough Price:   ${poly.get('trough_price') or 0:.2f} on {poly.get('trough_date', '?')}
    Polygon Close:  ${poly.get('polygon_current') or 0:.2f}
    Peak PnL:       {poly_pnl.get('peak_pnl_pct') or 0:+.1f}% (${poly_pnl.get('peak_pnl_dollar') or 0:+.0f}/ct)
    Current PnL:    {poly_pnl.get('current_pnl_pct') or 0:+.1f}% (${poly_pnl.get('current_pnl_dollar') or 0:+.0f}/ct)
    Left on Table:  {poly_pnl.get('left_on_table_pct') or 0:+.1f}% (peak vs now)
    Note:           {poly.get('entry_day_note', '')}"""
        else:
            polygon_section = "\n  POLYGON: No option history available"

        positions_text += f"""
POSITION #{tid}: {trade['ticker']} {trade['direction']}
  Called:       {trade['call_timestamp'][:19]} (exact timestamp)
  Strike:       {strike_str}
  Expiry:       {trade.get('expiry') or 'N/A'}
  Entry Price:  ${trade.get('entry_price') or 0:.2f}/contract (conservative: mid-{'bid' if 'SELL' in trade['direction'] else 'ask'} blend)
  Entry Bid/Ask/Mid: ${trade.get('entry_bid') or 0:.2f} / ${trade.get('entry_ask') or 0:.2f} / ${trade.get('entry_mid') or 0:.2f}
  Confidence:   {trade.get('confidence') or '?'}/10
  Urgency:      {trade.get('urgency') or '?'}

  ORIGINAL THESIS: {trade.get('thesis') or 'N/A'}
  ORIGINAL EDGE:   {trade.get('edge') or 'N/A'}

  CURRENT DATA (yfinance live):
    Underlying:     ${price_data.get('current_underlying') or 0:.2f} (was ${trade.get('underlying_at_entry') or 0:.2f} at entry)
    Underlying Δ:   {price_data.get('underlying_change_pct') or 0:+.1f}%
    Option Mid:     ${price_data.get('option_current_mid') or 0:.2f} (entry: ${trade.get('entry_price') or 0:.2f})
    PnL Now:        {pnl_data.get('pnl_percent') or 0:+.1f}% (${pnl_data.get('pnl_dollar') or 0:+.0f}/contract)
    Days Held:      {price_data.get('days_held') or 0}
    DTE Remaining:  {price_data.get('dte_remaining') or 'N/A'}
    Underlying Hi:  ${price_data.get('underlying_high_since_entry') or 0:.2f}
    Underlying Lo:  ${price_data.get('underlying_low_since_entry') or 0:.2f}
    Current IV:     {price_data.get('option_iv') or 0:.1%}
{polygon_section}

{"─" * 50}
"""

    # Call Claude Opus 4.6 with adaptive thinking
    print("  🧠 Calling Claude Opus 4.6 with adaptive thinking...")

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-opus-4-6",
                "max_tokens": 8000,
                "thinking": {
                    "type": "adaptive"
                },
                "system": THESIS_EVAL_SYSTEM,
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            f"Today is {datetime.date.today().strftime('%A, %B %d, %Y')}. "
                            f"Market just closed. Here are our open positions:\n\n"
                            f"{positions_text}\n\n"
                            f"Evaluate each position. Think deeply about whether each thesis "
                            f"is still valid given current market conditions. Consider macro "
                            f"shifts, sector rotation, news catalysts, and time decay.\n\n"
                            f"For positions that are underwater, be especially careful to "
                            f"distinguish between 'thesis needs more time' vs 'thesis is dead'."
                        )
                    }
                ]
            },
            timeout=300
        )

        if response.status_code != 200:
            error = response.json().get("error", {}).get("message", response.text[:200])
            return f"⚠ Claude API error ({response.status_code}): {error}", 0.0

        data = response.json()

        # Extract text response (skip thinking blocks)
        eval_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                eval_text += block["text"]

        # Calculate cost
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        # Note: thinking tokens are billed as output tokens
        cost = (
            (input_tokens / 1000) * OPUS_INPUT_COST_PER_1K +
            (output_tokens / 1000) * OPUS_OUTPUT_COST_PER_1K
        )

        print(f"  💰 Opus 4.6 cost: ${cost:.4f} ({input_tokens} in / {output_tokens} out)")
        return eval_text, cost

    except requests.exceptions.Timeout:
        return "⚠ Claude API timeout (300s) — thesis evaluation incomplete", 0.0
    except Exception as e:
        return f"⚠ Claude API error: {e}", 0.0


# ============================================================
# REPORT GENERATION
# ============================================================
def _pnl_bar(pct: float, width: int = 10) -> str:
    """Generate a visual PnL bar."""
    if pct > 0:
        filled = min(int(pct / 10), width)
        return "🟩" * filled + "⬜" * (width - filled)
    else:
        filled = min(int(abs(pct) / 10), width)
        return "🟥" * filled + "⬜" * (width - filled)


def _health_meter(avg_pnl: float) -> str:
    """Portfolio health meter based on average PnL."""
    if avg_pnl > 20:
        return "🟢🟢🟢🟢🟢 EXCELLENT"
    elif avg_pnl > 10:
        return "🟢🟢🟢🟢⚪ STRONG"
    elif avg_pnl > 0:
        return "🟢🟢🟢⚪⚪ HEALTHY"
    elif avg_pnl > -10:
        return "🟡🟡⚪⚪⚪ CAUTIOUS"
    elif avg_pnl > -20:
        return "🔴🔴⚪⚪⚪ STRESSED"
    else:
        return "🔴🔴🔴⚪⚪ CRITICAL"


def parse_auto_close(thesis_eval: str) -> List[str]:
    """Extract auto-close recommendations from Claude's evaluation."""
    lines = []
    in_close_section = False
    for line in thesis_eval.split("\n"):
        stripped = line.strip()
        if "AUTO-CLOSE BATCH" in stripped.upper():
            in_close_section = True
            continue
        if in_close_section:
            if stripped.startswith("CLOSE:"):
                lines.append(stripped)
            elif stripped.startswith("PORTFOLIO SUMMARY") or stripped.startswith("SELF-EVALUATION"):
                break
            elif stripped == "No auto-close recommendations this week.":
                break
    return lines


def parse_self_evaluation(thesis_eval: str) -> str:
    """Extract Opus self-evaluation from thesis eval."""
    lines = []
    in_section = False
    for line in thesis_eval.split("\n"):
        stripped = line.strip()
        if stripped.startswith("SELF-EVALUATION:"):
            in_section = True
            lines.append(stripped.replace("SELF-EVALUATION:", "").strip())
            continue
        if in_section:
            if stripped.startswith("LESSON OF THE WEEK"):
                break
            if stripped:
                lines.append(stripped)
    return " ".join(lines).strip()


def generate_scorecard(
    conn: sqlite3.Connection,
    open_trades: List[Dict],
    prices: Dict[int, Dict],
    pnls: Dict[int, Dict],
    thesis_eval: str,
    api_cost: float,
    polygon_data: Dict[int, Dict] = None
) -> str:
    """Generate the full weekly scorecard report with stylish formatting."""
    now = datetime.datetime.now()
    today_str = now.strftime("%b %d, %Y")
    day_str = now.strftime("%A")

    # Portfolio stats
    total_open = len(open_trades)
    closed_this_week = get_closed_trades(
        conn, since_date=(now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    )
    total_closed = len(closed_this_week)

    # Win/loss from closed trades
    winners = [t for t in closed_this_week if (t.get("pnl_percent") or 0) > 0]
    losers = [t for t in closed_this_week if (t.get("pnl_percent") or 0) <= 0]
    win_rate = (len(winners) / max(total_closed, 1)) * 100

    # Current portfolio PnL
    total_pnl_dollar = sum(pnls.get(t["trade_id"], {}).get("pnl_dollar") or 0 for t in open_trades)
    total_pnl_pct_avg = 0
    if open_trades:
        valid_pnls = [pnls.get(t["trade_id"], {}).get("pnl_percent") or 0 for t in open_trades]
        total_pnl_pct_avg = sum(valid_pnls) / len(valid_pnls)

    # Best/worst open position
    best_trade = max(open_trades, key=lambda t: pnls.get(t["trade_id"], {}).get("pnl_percent") or -999, default=None)
    worst_trade = min(open_trades, key=lambda t: pnls.get(t["trade_id"], {}).get("pnl_percent") or 999, default=None)

    # High confidence performance
    high_conf = [t for t in open_trades if (t.get("confidence") or 0) >= 7]
    low_conf = [t for t in open_trades if (t.get("confidence") or 0) < 7 and t.get("confidence")]
    high_conf_avg = sum(pnls.get(t["trade_id"], {}).get("pnl_percent") or 0 for t in high_conf) / max(len(high_conf), 1)
    low_conf_avg = sum(pnls.get(t["trade_id"], {}).get("pnl_percent") or 0 for t in low_conf) / max(len(low_conf), 1)

    # Count winning/losing open positions
    open_winning = sum(1 for t in open_trades if (pnls.get(t["trade_id"], {}).get("pnl_percent") or 0) > 0)
    open_losing = total_open - open_winning

    # Parse auto-close from Opus
    auto_close_items = parse_auto_close(thesis_eval)
    self_eval = parse_self_evaluation(thesis_eval)

    # Health meter
    health = _health_meter(total_pnl_pct_avg)

    # ──────────────── BUILD REPORT ────────────────
    report = f"""🏆 ORCA WEEKLY SCORECARD
┌─ {day_str}, {today_str}
│  Friday Close Review
└─────────────────────────────

"""

    # ── AUTO-CLOSE ACTION ITEMS (top of report) ──
    if auto_close_items:
        report += f"""🚨 ACTION ITEMS — Close These Monday Open
┌{'─' * 44}
"""
        for item in auto_close_items:
            report += f"│ ❌ {item.replace('CLOSE:', '').strip()}\n"
        report += f"""└{'─' * 44}

"""

    # ── PORTFOLIO SNAPSHOT ──
    report += f"""📊 PORTFOLIO SNAPSHOT
┌{'─' * 44}
│ Open Positions:    {total_open} ({open_winning}W / {open_losing}L)
│ Closed This Week:  {total_closed}
│ Win Rate (closed): {win_rate:.0f}%
│ Avg Open PnL:      {total_pnl_pct_avg:+.1f}%
│ Total Open PnL:    ${total_pnl_dollar:+,.0f}
│
│ Health: {health}
│ PnL:    {_pnl_bar(total_pnl_pct_avg)}
└{'─' * 44}

"""

    # ── CONFIDENCE CALIBRATION ──
    conf_check = "✅ Calibrated" if high_conf_avg > low_conf_avg else "⚠️ Miscalibrated"
    report += f"""🎯 CONFIDENCE CALIBRATION
┌{'─' * 44}
│ High (7-10): {len(high_conf)} trades → avg {high_conf_avg:+.1f}%
│ Low  (<7):   {len(low_conf)} trades → avg {low_conf_avg:+.1f}%
│ Status:      {conf_check}
└{'─' * 44}

"""

    # ── BEST / WORST ──
    if best_trade:
        bt_pnl = pnls.get(best_trade["trade_id"], {})
        bt_pct = bt_pnl.get("pnl_percent") or 0
        report += f"""🥇 BEST:  {best_trade['ticker']} {best_trade['direction']}
   {bt_pct:+.1f}% (${bt_pnl.get('pnl_dollar') or 0:+,.0f}) {_pnl_bar(bt_pct, 5)}
"""

    if worst_trade:
        wt_pnl = pnls.get(worst_trade["trade_id"], {})
        wt_pct = wt_pnl.get("pnl_percent") or 0
        report += f"""🥉 WORST: {worst_trade['ticker']} {worst_trade['direction']}
   {wt_pct:+.1f}% (${wt_pnl.get('pnl_dollar') or 0:+,.0f}) {_pnl_bar(wt_pct, 5)}
"""

    # ── POSITION-BY-POSITION BREAKDOWN ──
    report += f"""
{'━' * 46}
📋 POSITION-BY-POSITION BREAKDOWN
{'━' * 46}
"""

    if polygon_data is None:
        polygon_data = {}

    for trade in open_trades:
        tid = trade["trade_id"]
        p = prices.get(tid, {})
        pnl = pnls.get(tid, {})
        poly = polygon_data.get(tid, {})
        poly_pnl = calc_polygon_pnl(trade, poly) if poly else {}
        strike_str = _format_strikes(trade)
        pnl_pct = pnl.get("pnl_percent") or 0
        dte = p.get("dte_remaining")

        # DTE warning
        dte_warning = ""
        if dte is not None and dte <= 5:
            dte_warning = " ⚡ EXPIRY IMMINENT"
        elif dte is not None and dte <= 14:
            dte_warning = " ⏰ DTE LOW"

        # Polygon peak line
        poly_line = ""
        if poly.get("peak_price"):
            peak_p = poly['peak_price']
            peak_d = poly.get('peak_date', '?')
            ppnl = poly_pnl.get('peak_pnl_pct') or 0
            left = poly_pnl.get('left_on_table_pct') or 0
            poly_line = f"│ 📈 Peak ${peak_p:.2f} on {peak_d} ({ppnl:+.1f}%)"
            if left > 5:
                poly_line += f" ⚠️ -{left:.0f}% left on table"
            poly_line += "\n"

        report += f"""
{pnl.get('status_label') or '⚪'} #{tid} {trade['ticker']} {trade['direction']}{dte_warning}
┌ {strike_str} | exp {trade.get('expiry') or 'N/A'}
│ Entry ${trade.get('entry_price') or 0:.2f} → ${p.get('option_current_mid') or 0:.2f}
│ PnL: {pnl_pct:+.1f}% (${pnl.get('pnl_dollar') or 0:+.0f}/ct) {_pnl_bar(pnl_pct, 5)}
{poly_line}│ Stock: ${trade.get('underlying_at_entry') or 0:.2f} → ${p.get('current_underlying') or 0:.2f} ({p.get('underlying_change_pct') or 0:+.1f}%)
│ Day {p.get('days_held') or 0} | DTE {dte or 'N/A'} | Conf {trade.get('confidence') or '?'}/10
└{'─' * 40}
"""

    # ── CLAUDE'S THESIS EVALUATION ──
    report += f"""
{'━' * 46}
🧠 OPUS 4.6 THESIS EVALUATION
{'━' * 46}

{thesis_eval}
"""

    # ── SELF-EVALUATION HIGHLIGHT ──
    if self_eval:
        report += f"""
┌{'─' * 44}
│ 🪞 OPUS SELF-GRADE: {self_eval[:120]}
└{'─' * 44}
"""

    # ── FOOTER ──
    report += f"""
{'━' * 46}
💰 Scorecard Cost: ${api_cost:.4f}
⚡ ORCA — Weekly Performance Audit
"""

    return report


# ============================================================
# SNAPSHOT ALL OPEN POSITIONS
# ============================================================
def snapshot_all_open(conn: sqlite3.Connection, trades: List[Dict], prices: Dict[int, Dict]):
    """Save current price snapshots for all open trades (for future watermarks)."""
    now = datetime.datetime.now()
    for trade in trades:
        tid = trade["trade_id"]
        p = prices.get(tid, {})
        if not p or p.get("error"):
            continue

        call_date = trade.get("call_date", now.strftime("%Y-%m-%d"))
        try:
            entry_dt = datetime.datetime.strptime(call_date, "%Y-%m-%d")
            days_held = (now - entry_dt).days
        except (ValueError, TypeError):
            days_held = 0

        conn.execute("""
            INSERT INTO price_snapshots (
                trade_id, snapshot_date, snapshot_timestamp,
                underlying_price, option_bid, option_ask, option_mid,
                option_volume, option_oi, iv, delta, days_held
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tid,
            now.strftime("%Y-%m-%d"),
            now.isoformat(),
            p.get("current_underlying"),
            p.get("option_current_bid"),
            p.get("option_current_ask"),
            p.get("option_current_mid"),
            p.get("option_volume"),
            p.get("option_oi"),
            p.get("option_iv"),
            None,  # delta
            days_held,
        ))

    conn.commit()


# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(message: str):
    """Send scorecard to Telegram (chunked if needed)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠ Telegram not configured")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    # Split into chunks (Telegram 4096 char limit)
    chunks = []
    while message:
        if len(message) <= 4000:
            chunks.append(message)
            break
        # Find a good split point
        split_at = message[:4000].rfind("\n")
        if split_at < 2000:
            split_at = 4000
        chunks.append(message[:split_at])
        message = message[split_at:]

    for i, chunk in enumerate(chunks):
        try:
            r = requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "disable_web_page_preview": True,
            }, timeout=10)
            status = "✅" if r.status_code == 200 else f"❌ {r.status_code}"
            print(f"  {status} Telegram chunk {i+1}/{len(chunks)}")
        except Exception as e:
            print(f"  ❌ Telegram error: {e}")


# ============================================================
# MAIN
# ============================================================
# ============================================================
# MONTHLY PERFORMANCE REPORT
# ============================================================
def generate_monthly_report(conn: sqlite3.Connection) -> str:
    """Generate cumulative monthly performance report with track record evolution."""
    now = datetime.datetime.now()
    today_str = now.strftime("%b %d, %Y")

    # ── All-time stats ──
    total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    total_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'").fetchone()[0]
    total_closed = conn.execute("SELECT COUNT(*) FROM trades WHERE status != 'OPEN'").fetchone()[0]

    # Closed trade stats
    closed_rows = conn.execute("""
        SELECT trade_id, ticker, direction, strike, expiry, call_date, close_date,
               entry_price, close_price, pnl_percent, close_reason, confidence,
               option_high_watermark
        FROM trades WHERE status != 'OPEN'
        ORDER BY close_date DESC
    """).fetchall()

    wins = 0
    losses = 0
    total_pnl_pct = 0
    best_win = {"ticker": "N/A", "pnl": 0}
    worst_loss = {"ticker": "N/A", "pnl": 0}
    by_direction = {}
    by_month = {}
    missed_exits = 0

    for row in closed_rows:
        tid, ticker, direction, strike, expiry, call_date, close_date, \
            entry_price, close_price, pnl_pct, close_reason, confidence, high_wm = row

        pnl_pct = pnl_pct or 0
        total_pnl_pct += pnl_pct

        if pnl_pct > 0:
            wins += 1
            if pnl_pct > best_win["pnl"]:
                best_win = {"ticker": ticker, "direction": direction, "pnl": pnl_pct}
        else:
            losses += 1
            if pnl_pct < worst_loss["pnl"]:
                worst_loss = {"ticker": ticker, "direction": direction, "pnl": pnl_pct}

        # Track by direction
        dir_key = direction or "UNKNOWN"
        if dir_key not in by_direction:
            by_direction[dir_key] = {"count": 0, "wins": 0, "total_pnl": 0}
        by_direction[dir_key]["count"] += 1
        by_direction[dir_key]["total_pnl"] += pnl_pct
        if pnl_pct > 0:
            by_direction[dir_key]["wins"] += 1

        # Track by month
        month_key = (close_date or call_date or "?")[:7]  # YYYY-MM
        if month_key not in by_month:
            by_month[month_key] = {"count": 0, "wins": 0, "total_pnl": 0}
        by_month[month_key]["count"] += 1
        by_month[month_key]["total_pnl"] += pnl_pct
        if pnl_pct > 0:
            by_month[month_key]["wins"] += 1

        # Missed exits: high watermark was significantly better than close
        if high_wm and entry_price and entry_price > 0:
            peak_pnl = ((high_wm - entry_price) / entry_price) * 100
            if peak_pnl > 20 and pnl_pct < peak_pnl * 0.5:
                missed_exits += 1

    win_rate = (wins / max(total_closed, 1)) * 100
    avg_pnl = total_pnl_pct / max(total_closed, 1)

    # ── Open positions summary ──
    open_rows = conn.execute("""
        SELECT ticker, direction, strike, entry_price,
               option_high_watermark, option_low_watermark
        FROM trades WHERE status = 'OPEN'
    """).fetchall()

    # ── Scorecard history ──
    scorecards = conn.execute("""
        SELECT scorecard_date, total_open, total_closed, win_rate,
               total_pnl, api_cost
        FROM scorecards ORDER BY scorecard_date DESC LIMIT 12
    """).fetchall()

    total_scorecard_cost = sum(s[5] or 0 for s in scorecards)

    # ──────────────── BUILD REPORT ────────────────
    report = f"""📊 ORCA MONTHLY PERFORMANCE REPORT
┌─ {today_str}
│  Track Record Evolution
└─────────────────────────────

"""

    # ── ALL-TIME STATS ──
    report += f"""🏆 ALL-TIME TRACK RECORD
┌{'─' * 44}
│ Total Calls:    {total}
│ Open:           {total_open}
│ Closed:         {total_closed}
│ Wins / Losses:  {wins}W / {losses}L
│ Win Rate:       {win_rate:.1f}%
│ Avg PnL/Trade:  {avg_pnl:+.1f}%
│ Missed Exits:   {missed_exits} (peak +20%+ but closed lower)
└{'─' * 44}

"""

    # ── BEST / WORST ──
    if best_win["ticker"] != "N/A":
        report += f"""🥇 Best Win:  {best_win['ticker']} {best_win['direction']} → {best_win['pnl']:+.1f}%
"""
    if worst_loss["ticker"] != "N/A":
        report += f"""🥉 Worst Loss: {worst_loss['ticker']} {worst_loss['direction']} → {worst_loss['pnl']:+.1f}%
"""

    # ── BY DIRECTION ──
    report += f"""
📈 PERFORMANCE BY DIRECTION
┌{'─' * 44}
"""
    for dir_key in sorted(by_direction.keys()):
        d = by_direction[dir_key]
        d_wr = (d["wins"] / max(d["count"], 1)) * 100
        d_avg = d["total_pnl"] / max(d["count"], 1)
        report += f"│ {dir_key:20s} {d['count']:3d} trades  {d_wr:.0f}% WR  avg {d_avg:+.1f}%\n"
    report += f"└{'─' * 44}\n\n"

    # ── MONTHLY EVOLUTION ──
    report += f"""📅 MONTHLY EVOLUTION
┌{'─' * 44}
"""
    for month_key in sorted(by_month.keys()):
        m = by_month[month_key]
        m_wr = (m["wins"] / max(m["count"], 1)) * 100
        m_avg = m["total_pnl"] / max(m["count"], 1)
        bar = _pnl_bar(m_avg, 5)
        report += f"│ {month_key}  {m['count']:3d} closed  {m_wr:.0f}% WR  avg {m_avg:+.1f}%  {bar}\n"
    report += f"└{'─' * 44}\n\n"

    # ── SCORECARD HISTORY ──
    if scorecards:
        report += f"""📋 RECENT SCORECARDS
┌{'─' * 44}
"""
        for sc in scorecards[:8]:
            sc_date, sc_open, sc_closed, sc_wr, sc_pnl, sc_cost = sc
            report += f"│ {sc_date}  {sc_open or 0} open  {sc_wr or 0:.0f}% WR  ${sc_pnl or 0:+,.0f}\n"
        report += f"""│
│ Total Opus cost: ${total_scorecard_cost:.2f}
└{'─' * 44}
"""

    # ── OPEN PORTFOLIO SNAPSHOT ──
    if open_rows:
        report += f"""
📂 CURRENT OPEN POSITIONS ({len(open_rows)})
┌{'─' * 44}
"""
        for row in open_rows:
            ticker, direction, strike, entry_price, high_wm, low_wm = row
            peak_str = ""
            if high_wm and entry_price and entry_price > 0:
                peak_pnl = ((high_wm - entry_price) / entry_price) * 100
                peak_str = f" (peak {peak_pnl:+.0f}%)"
            report += f"│ {ticker} {direction} ${strike or 0:.0f} @ ${entry_price or 0:.2f}{peak_str}\n"
        report += f"└{'─' * 44}\n"

    # ── FOOTER ──
    report += f"""
{'━' * 46}
⚡ ORCA — Monthly Performance Report
"""

    return report


# ============================================================
# MAIN
# ============================================================
def main():
    # Parse CLI args
    mode = "weekly"
    if "--monthly" in sys.argv:
        mode = "monthly"

    if mode == "monthly":
        print("""
╔═══════════════════════════════════════════════════════════════╗
║  📊 ORCA — Monthly Performance Report                        ║
║  Track Record Evolution + Cumulative Stats                   ║
╚═══════════════════════════════════════════════════════════════╝""")
    else:
        print("""
╔═══════════════════════════════════════════════════════════════╗
║  🏆 ORCA SCORECARD — Weekly Performance Audit                 ║
║  Claude Opus 4.6 Adaptive Thinking + Mechanical PnL          ║
╚═══════════════════════════════════════════════════════════════╝""")

    # Check database exists
    if not DB_PATH.exists():
        print("  ❌ No trade log database found. Run trade_logger.py first.")
        print("     The trade logger must run daily as part of the ORCA pipeline.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    # ── MONTHLY MODE ──
    if mode == "monthly":
        print("  📊 Backfilling PnL for closed trades (Polygon + snapshots)...")
        backfill_closed_trade_pnl(conn)

        print("\n  📊 Generating monthly performance report...")
        report = generate_monthly_report(conn)

        # Save report
        RESULTS_DIR.mkdir(exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        report_path = RESULTS_DIR / f"monthly_{ts}.md"
        report_path.write_text(report, encoding="utf-8")
        print(f"  💾 Report saved: {report_path}")

        # Send to Telegram
        print("\n  📱 Sending to Telegram...")
        send_telegram(report)

        print(f"\n{'═' * 60}")
        print(report)
        print(f"\n{'═' * 60}")
        print("✅ Monthly report complete!")
        conn.close()
        return

    # ── WEEKLY MODE ──
    # Step 1: Auto-expire trades past their expiry
    today = datetime.date.today().isoformat()
    expired = conn.execute("""
        UPDATE trades
        SET status = 'EXPIRED', close_date = ?, close_reason = 'EXPIRED'
        WHERE status = 'OPEN' AND expiry IS NOT NULL AND expiry < ?
    """, (today, today)).rowcount
    conn.commit()
    if expired:
        print(f"  📅 Auto-expired {expired} trades")

    # Step 1b: Backfill PnL for any expired/closed trades missing it
    backfill_closed_trade_pnl(conn)

    # Step 2: Fetch open trades
    open_trades = get_open_trades(conn)
    print(f"  📊 Open positions: {len(open_trades)}")

    if not open_trades:
        msg = """🏆 ORCA WEEKLY SCORECARD
┌─ No open positions
│  Portfolio is empty — waiting for new signals.
└─────────────────────────────"""
        send_telegram(msg)
        conn.close()
        return

    # Step 3: Fetch current prices + watermarks
    print("\n  💹 Fetching current prices...")
    prices = fetch_current_prices(open_trades)

    # Step 4: Calculate PnL for each position
    pnls = {}
    for trade in open_trades:
        tid = trade["trade_id"]
        p = prices.get(tid, {})
        pnls[tid] = calculate_pnl(trade, p)

    # Step 5: Snapshot prices (for future watermark tracking)
    print("  📸 Saving price snapshots...")
    snapshot_all_open(conn, open_trades, prices)

    # Step 6: Fetch Polygon option history (daily OHLCV)
    print("\n  📊 Fetching Polygon option history...")
    polygon_data = fetch_polygon_option_history(open_trades)

    # Step 7: Claude Opus 4.6 thesis evaluation
    print("\n  🧠 Evaluating theses with Claude Opus 4.6...")
    thesis_eval, api_cost = evaluate_positions_with_claude(
        open_trades, prices, pnls, polygon_data
    )

    # Step 8: Generate report
    print("\n  📝 Generating scorecard...")
    report = generate_scorecard(
        conn, open_trades, prices, pnls, thesis_eval, api_cost, polygon_data
    )

    # Step 9: Save to database
    total_open = len(open_trades)
    closed_week = get_closed_trades(
        conn, since_date=(datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    )
    winners = [t for t in closed_week if (t.get("pnl_percent") or 0) > 0]
    win_rate = (len(winners) / max(len(closed_week), 1)) * 100

    conn.execute("""
        INSERT INTO scorecards (
            scorecard_date, total_open, total_closed, win_rate,
            total_pnl, avg_winner, avg_loser, thesis_eval,
            full_report, api_cost
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today, total_open, len(closed_week), win_rate,
        sum(pnls.get(t["trade_id"], {}).get("pnl_dollar") or 0 for t in open_trades),
        None, None,  # avg_winner, avg_loser calculated from closed trades
        thesis_eval[:5000],
        report[:50000],
        api_cost,
    ))
    conn.commit()

    # Step 10: Save report file
    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    report_path = RESULTS_DIR / f"scorecard_{ts}.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"  💾 Report saved: {report_path}")

    # Step 11: Send to Telegram
    print("\n  📱 Sending to Telegram...")
    send_telegram(report)

    # Print summary
    print(f"\n{'═' * 60}")
    print(report)
    print(f"\n{'═' * 60}")
    print("✅ Scorecard complete!")

    conn.close()


if __name__ == "__main__":
    main()
