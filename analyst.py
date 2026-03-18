#!/usr/bin/env python3
"""
ORCA ANALYST v3 — Opus 4.6 Brain (Adaptive Extended Thinking)
The smartest AI on the market connecting dots no human can.
- Claude Opus 4.6 with adaptive thinking (thinks deeper on complex analysis)
- Real-time cost tracking with daily/monthly caps
- Multi-source news: RSS + NWS weather + social signals
"""

import os, sys, json, csv, re, sqlite3, datetime
from pathlib import Path
from typing import List, Dict, Tuple
try:
    import requests
except ImportError:
    os.system("pip install requests -q")
    import requests
import xml.etree.ElementTree as ET

# Auto-load .env for local runs (GitHub Actions uses secrets)
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            key, val = k.strip(), v.strip()
            if not os.environ.get(key):  # override empty values too
                os.environ[key] = val

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-opus-4-6"
RESULTS_DIR = Path("orca_results")
DB_PATH = Path("orca_iv_history.db")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# ── COST CONTROLS (Opus 4.6 pricing) ──
COST_PER_1M_INPUT = 5.00      # $5 per million input tokens
COST_PER_1M_OUTPUT = 25.00    # $25 per million output tokens (includes thinking)
DAILY_SPEND_CAP = 20.00       # Triple-AI pipeline costs $3-5/run × up to 3 runs/day
MONTHLY_SPEND_CAP = 150.00    # ~$5/day × 22 trading days = $110 avg, with headroom
MAX_OUTPUT_TOKENS = 32000     # Must be > thinking budget (max 20K) + actual output

# ============================================================
# COST TRACKER
# ============================================================
class CostTracker:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("""CREATE TABLE IF NOT EXISTS api_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, date TEXT,
            model TEXT, input_tokens INTEGER, output_tokens INTEGER,
            thinking_tokens INTEGER, cost_usd REAL, purpose TEXT)""")
        # Migrate: add thinking_tokens column if missing
        try:
            self.conn.execute("ALTER TABLE api_costs ADD COLUMN thinking_tokens INTEGER DEFAULT 0")
        except:
            pass
        self.conn.commit()

    def log_call(self, model, input_tokens, output_tokens, thinking_tokens=0, purpose="analyst"):
        # Thinking tokens are billed at output rate
        cost = (input_tokens / 1_000_000 * COST_PER_1M_INPUT +
                output_tokens / 1_000_000 * COST_PER_1M_OUTPUT +
                thinking_tokens / 1_000_000 * COST_PER_1M_OUTPUT)
        now = datetime.datetime.now()
        self.conn.execute(
            "INSERT INTO api_costs (timestamp,date,model,input_tokens,output_tokens,thinking_tokens,cost_usd,purpose) VALUES (?,?,?,?,?,?,?,?)",
            (now.isoformat(), now.date().isoformat(), model, input_tokens, output_tokens, thinking_tokens, cost, purpose))
        self.conn.commit()
        return cost

    def get_today_spend(self):
        return self.conn.execute("SELECT COALESCE(SUM(cost_usd),0) FROM api_costs WHERE date=?",
                              (datetime.date.today().isoformat(),)).fetchone()[0]

    def get_month_spend(self):
        start = datetime.date.today().replace(day=1).isoformat()
        return self.conn.execute("SELECT COALESCE(SUM(cost_usd),0) FROM api_costs WHERE date>=?",
                                 (start,)).fetchone()[0]

    def get_total_spend(self):
        return self.conn.execute("SELECT COALESCE(SUM(cost_usd),0) FROM api_costs").fetchone()[0]

    def check_budget(self):
        today = self.get_today_spend()
        month = self.get_month_spend()
        if today >= DAILY_SPEND_CAP:
            return False, f"Daily cap hit: ${today:.4f}/${DAILY_SPEND_CAP:.2f}"
        if month >= MONTHLY_SPEND_CAP:
            return False, f"Monthly cap hit: ${month:.4f}/${MONTHLY_SPEND_CAP:.2f}"
        return True, "OK"

    def print_dashboard(self):
        today = self.get_today_spend()
        month = self.get_month_spend()
        total = self.get_total_spend()
        calls = self.conn.execute("SELECT COUNT(*) FROM api_costs").fetchone()[0]
        tokens = self.conn.execute(
            "SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(thinking_tokens),0) FROM api_costs"
        ).fetchone()
        print(f"""
  ┌─ 💰 API COST DASHBOARD (Opus 4.6) ────────┐
  │  Model:      Claude Opus 4.6 + Thinking    │
  │  Today:      ${today:7.4f}  / ${DAILY_SPEND_CAP:.2f} cap      │
  │  This month: ${month:7.4f}  / ${MONTHLY_SPEND_CAP:.2f} cap    │
  │  All-time:   ${total:7.4f}  ({calls} calls)       │
  │  Input:      {tokens[0]:>9,} tokens             │
  │  Output:     {tokens[1]:>9,} tokens             │
  │  Thinking:   {tokens[2]:>9,} tokens             │
  │  Remaining:  ${max(0,DAILY_SPEND_CAP-today):7.4f} today             │
  └────────────────────────────────────────────┘""")

    def close(self):
        self.conn.close()

# ============================================================
# NEWS SCRAPER (multi-source, all free)
# ============================================================
class NewsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "ORCA/3.0"})

    RSS_FEEDS = {
        "reuters_biz": "https://feeds.reuters.com/reuters/businessNews",
        "reuters_mkt": "https://feeds.reuters.com/reuters/marketsNews",
        "reuters_world": "https://feeds.reuters.com/Reuters/worldNews",
        "cnbc_top": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "cnbc_world": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        "marketwatch": "http://feeds.marketwatch.com/marketwatch/topstories/",
        "oilprice": "https://oilprice.com/rss/main",
        "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "zerohedge": "https://www.zerohedge.com/fullrss2.xml",
    }

    WEATHER_REGIONS = {
        "houston":    (29.76, -95.37, ["nat_gas","oil","refinery"]),
        "chicago":    (41.88, -87.63, ["nat_gas","heating","agriculture"]),
        "new_york":   (40.71, -74.01, ["nat_gas","heating","energy"]),
        "permian":    (31.95, -102.18, ["oil","nat_gas","production"]),
        "gulf_coast": (29.30, -94.80, ["hurricane","refinery","oil"]),
        "midwest":    (40.00, -89.00, ["agriculture","corn","soybeans"]),
        "fargo":      (46.88, -96.79, ["heating","nat_gas","extreme_cold"]),
    }

    HIGH_KW = ["iran","israel","strike","missile","war","ceasefire","strait","hormuz",
               "sanctions","tariff","embargo","fed","rate cut","rate hike","inflation",
               "cpi","crash","plunge","surge","spike","freeze","hurricane","polar vortex",
               "arctic","hack","breach","cyber","default","bankruptcy","margin call",
               "oil","crude","natural gas","gold","bitcoin","earnings","guidance"]
    MED_KW = ["china","taiwan","russia","ukraine","semiconductor","chip","nvidia","ai",
              "treasury","yield","recession","opec","production","shortage","fda",
              "merger","acquisition","volatility","vix","tesla","apple"]

    def scrape_all(self):
        result = {"market_news":[], "geopolitical":[], "weather_alerts":[], "social_signals":[]}

        print("  📰 Scraping RSS feeds...")
        rss = self._scrape_rss()
        geo_kw = {"war","strike","missile","iran","china","tariff","sanction","nato",
                  "invasion","ceasefire","israel","russia","ukraine","strait","hormuz"}
        for item in rss:
            if any(kw in item["text"].lower() for kw in geo_kw):
                result["geopolitical"].append(item)
            else:
                result["market_news"].append(item)

        print("  🌤  Checking NWS weather forecasts...")
        result["weather_alerts"] = self._scrape_weather()

        total = sum(len(v) for v in result.values())
        print(f"  📊 Total signals: {total}")
        for cat, items in result.items():
            if items: print(f"     {cat}: {len(items)}")
        return result

    def _scrape_rss(self):
        items = []
        seen = set()
        for name, url in self.RSS_FEEDS.items():
            try:
                resp = self.session.get(url, timeout=10)
                if resp.status_code != 200: continue
                root = ET.fromstring(resp.content)
                for el in root.findall(".//item")[:8]:
                    title = (el.findtext("title") or "").strip()
                    desc = re.sub(r'<[^>]+>', '', (el.findtext("description") or ""))[:200].strip()
                    key = title.lower()[:40]
                    if key in seen: continue
                    seen.add(key)
                    text = f"{title}. {desc}"
                    rel = sum(3 for k in self.HIGH_KW if k in text.lower()) + sum(1 for k in self.MED_KW if k in text.lower())
                    if rel > 0:
                        items.append({"source": name, "title": title, "text": text, "relevance": min(rel,15)})
            except: continue
        items.sort(key=lambda x: x["relevance"], reverse=True)
        return items[:25]

    def _scrape_weather(self):
        alerts = []
        for region, (lat, lon, commodities) in self.WEATHER_REGIONS.items():
            try:
                r = self.session.get(f"https://api.weather.gov/points/{lat},{lon}",
                                     timeout=10, headers={"Accept":"application/json"})
                if r.status_code != 200: continue
                forecast_url = r.json().get("properties",{}).get("forecast")
                if not forecast_url: continue
                r2 = self.session.get(forecast_url, timeout=10, headers={"Accept":"application/json"})
                if r2.status_code != 200: continue
                for period in r2.json().get("properties",{}).get("periods",[])[:4]:
                    temp = period.get("temperature",0)
                    unit = period.get("temperatureUnit","F")
                    wind = period.get("windSpeed","")
                    detail = period.get("detailedForecast","")[:200]
                    name_p = period.get("name","")
                    signal = None
                    if unit == "F":
                        if temp <= 15:
                            signal = f"🥶 EXTREME COLD: {region} {temp}°F — nat gas/heating demand spike"
                        elif temp <= 25:
                            signal = f"❄️ FREEZE: {region} {temp}°F — elevated heating demand"
                        elif temp >= 105:
                            signal = f"🔥 EXTREME HEAT: {region} {temp}°F — electricity demand spike"
                    extreme_kw = ["hurricane","blizzard","ice storm","tornado","flood",
                                  "freeze","arctic","polar vortex","winter storm","extreme cold","heat advisory"]
                    for kw in extreme_kw:
                        if kw in detail.lower():
                            signal = f"⚠️ {kw.upper()}: {region} — impacts {', '.join(commodities)}"
                            break
                    if signal:
                        alerts.append({"source":f"NWS-{region}","signal":signal,
                                       "text":f"{name_p}: {temp}°{unit}, {wind}. {detail[:150]}",
                                       "commodities":commodities, "relevance":5})
            except: continue
        return alerts

    def format_for_prompt(self, news):
        parts = []
        if news["geopolitical"]:
            parts.append("=== GEOPOLITICAL ===")
            for i in news["geopolitical"][:8]:
                parts.append(f"• [{i['source']}] {i['text'][:150]}")
        if news["market_news"]:
            parts.append("\n=== MARKET NEWS ===")
            for i in news["market_news"][:10]:
                parts.append(f"• [{i['source']}] {i['text'][:150]}")
        if news["weather_alerts"]:
            parts.append("\n=== WEATHER ALERTS (commodity trades) ===")
            for i in news["weather_alerts"]:
                parts.append(f"• {i['signal']}")
                parts.append(f"  {i['text'][:120]}")
        if news["social_signals"]:
            parts.append("\n=== EARLY SIGNALS ===")
            for i in news["social_signals"][:5]:
                parts.append(f"• [{i['source']}] {i['text'][:120]}")
        return "\n".join(parts)

# ============================================================
# TRACK RECORD — Feedback loop from trade log
# ============================================================
TRADE_LOG_DB = Path("orca_trade_log.db")

def get_track_record() -> str:
    """
    Read win/loss stats from orca_trade_log.db and format as a brief
    track record summary for the analyst prompt. This creates a feedback
    loop: Opus sees what worked and what didn't, improving future calls.
    """
    if not TRADE_LOG_DB.exists():
        return ""

    try:
        conn = sqlite3.connect(str(TRADE_LOG_DB))

        # Overall stats
        total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        if total == 0:
            conn.close()
            return ""

        open_count = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
        closed = conn.execute("SELECT COUNT(*) FROM trades WHERE status IN ('CLOSED','EXPIRED','STOPPED')").fetchone()[0]

        # Win/loss from closed trades (using watermark data for approximate PnL)
        wins = 0
        losses = 0
        closed_trades = conn.execute("""
            SELECT ticker, direction, entry_price, option_high_watermark, option_low_watermark,
                   status, close_reason, pnl_percent
            FROM trades WHERE status IN ('CLOSED','EXPIRED','STOPPED')
        """).fetchall()

        for t in closed_trades:
            pnl = t[7]  # pnl_percent
            if pnl and pnl > 0:
                wins += 1
            elif pnl and pnl < 0:
                losses += 1

        # Stats by direction type
        direction_stats = conn.execute("""
            SELECT direction, COUNT(*) as cnt,
                   AVG(CASE WHEN pnl_percent IS NOT NULL THEN pnl_percent END) as avg_pnl
            FROM trades GROUP BY direction ORDER BY cnt DESC
        """).fetchall()

        # Recent trades (last 10) with PnL from watermarks
        recent = conn.execute("""
            SELECT ticker, direction, entry_price,
                   option_high_watermark, option_low_watermark,
                   status, call_date, confidence
            FROM trades ORDER BY call_date DESC, trade_id DESC LIMIT 10
        """).fetchall()

        # Best/worst by watermark (for open trades, approximate peak/trough PnL)
        best_worst = conn.execute("""
            SELECT ticker, direction, entry_price,
                   option_high_watermark, option_low_watermark, status
            FROM trades WHERE entry_price > 0 AND option_high_watermark IS NOT NULL
            ORDER BY call_date DESC LIMIT 20
        """).fetchall()

        conn.close()

        # Format summary
        lines = ["YOUR TRACK RECORD (learn from this):"]
        lines.append(f"  Total calls: {total} | Open: {open_count} | Closed: {closed}")
        if closed > 0:
            win_rate = (wins / closed * 100) if closed > 0 else 0
            lines.append(f"  Closed W/L: {wins}W / {losses}L ({win_rate:.0f}% win rate)")

        if direction_stats:
            lines.append("  By type:")
            for d, cnt, avg in direction_stats:
                avg_str = f"avg {avg:+.1f}%" if avg else "no PnL data"
                lines.append(f"    {d}: {cnt} trades ({avg_str})")

        if recent:
            lines.append("  Recent calls:")
            for ticker, direction, entry, high_wm, low_wm, status, date, conf in recent:
                entry = entry or 0
                # Estimate peak PnL from watermarks
                if high_wm and entry > 0 and "BUY" in direction:
                    peak_pnl = ((high_wm - entry) / entry) * 100
                    pnl_str = f"peak {peak_pnl:+.0f}%"
                elif low_wm and entry > 0 and "SELL" in direction:
                    peak_pnl = ((entry - low_wm) / entry) * 100
                    pnl_str = f"peak {peak_pnl:+.0f}%"
                else:
                    pnl_str = "no data"
                lines.append(f"    {date} {ticker} {direction} conf={conf} → {status} ({pnl_str})")

        lines.append("  LEARN: If you see patterns (certain directions losing, certain sectors winning), ADAPT. Don't repeat mistakes.")
        return "\n".join(lines)

    except Exception as e:
        print(f"  ⚠ Track record read failed: {e}")
        return ""


def get_open_positions() -> str:
    """
    Query orca_trade_log.db for all OPEN trades and format as a clear list
    for the Opus prompt so it knows what's already in the book.
    This prevents Opus from re-recommending tickers we already hold.
    """
    if not TRADE_LOG_DB.exists():
        return ""

    try:
        conn = sqlite3.connect(str(TRADE_LOG_DB))
        rows = conn.execute("""
            SELECT ticker, direction, strike, strike_2, expiry,
                   entry_price, call_date, confidence, thesis
            FROM trades WHERE status = 'OPEN'
            ORDER BY call_date DESC
        """).fetchall()
        conn.close()

        if not rows:
            return ""

        lines = ["CURRENTLY OPEN POSITIONS (DO NOT re-recommend these tickers):"]
        for i, (ticker, direction, strike, strike2, expiry, entry_price, call_date, conf, thesis) in enumerate(rows, 1):
            strike_str = f"${strike:.0f}" if strike else "?"
            if strike2:
                strike_str += f"/${strike2:.0f}"
            entry_str = f"${entry_price:.2f}" if entry_price else "?"
            exp_str = expiry if expiry else "?"
            thesis_short = (thesis[:50] + "...") if thesis and len(thesis) > 50 else (thesis or "")
            lines.append(f"  {i}. {ticker} {direction} {strike_str} | Entry {entry_str} | Opened {call_date} | Exp {exp_str} | CONF {conf}")
            if thesis_short:
                lines.append(f"     Thesis: {thesis_short}")

        lines.append("")
        lines.append("IMPORTANT: Do NOT recommend new trades on tickers listed above.")
        lines.append("Focus on NEW opportunities. Do NOT comment on open positions — position management is handled separately.")
        return "\n".join(lines)

    except Exception as e:
        print(f"  ⚠ Open positions read failed: {e}")
        return ""


# ============================================================
# EARNINGS / CATALYST CALENDAR
# ============================================================
def get_earnings_calendar(tickers: list) -> str:
    """
    Fetch upcoming earnings dates for scanned tickers via yfinance.
    Near-term earnings = high-probability catalyst for options trades.
    """
    try:
        import yfinance as yf
    except ImportError:
        return ""

    if not tickers:
        return ""

    upcoming = []
    today = datetime.date.today()

    # Check up to 20 tickers for earnings within 30 days
    for ticker_sym in tickers[:20]:
        try:
            stock = yf.Ticker(ticker_sym)
            cal = stock.calendar
            if cal is None or (hasattr(cal, 'empty') and cal.empty):
                continue

            # yfinance calendar returns dict or DataFrame
            if isinstance(cal, dict):
                earnings_date = cal.get("Earnings Date")
                if isinstance(earnings_date, list) and earnings_date:
                    earnings_date = earnings_date[0]
            else:
                # DataFrame format
                if "Earnings Date" in cal.index:
                    earnings_date = cal.loc["Earnings Date"].iloc[0] if hasattr(cal.loc["Earnings Date"], 'iloc') else cal.loc["Earnings Date"]
                else:
                    continue

            if earnings_date:
                # Convert to date if it's a timestamp
                if hasattr(earnings_date, 'date'):
                    ed = earnings_date.date()
                elif isinstance(earnings_date, str):
                    ed = datetime.datetime.strptime(earnings_date[:10], "%Y-%m-%d").date()
                else:
                    continue

                days_until = (ed - today).days
                if 0 <= days_until <= 30:
                    upcoming.append((ticker_sym, ed.isoformat(), days_until))

        except Exception:
            continue

    if not upcoming:
        return ""

    upcoming.sort(key=lambda x: x[2])
    lines = ["UPCOMING EARNINGS (next 30 days — prime catalyst window):"]
    for ticker_sym, date_str, days in upcoming:
        urgency = "⚡ IMMINENT" if days <= 3 else "📅 SOON" if days <= 7 else "📆"
        lines.append(f"  {urgency} {ticker_sym}: {date_str} ({days} days)")
    lines.append("  → Earnings = vol crush opportunity. Position BEFORE, not after. Debit spreads if directional, credit spreads to sell inflated pre-earnings IV.")
    return "\n".join(lines)


# ============================================================
# ORCA DATA READER
# ============================================================
def read_latest_scan():
    csvs = sorted(RESULTS_DIR.glob("orca_*.csv"))
    if not csvs: return "", []
    trades = []
    with open(csvs[-1]) as f:
        for row in csv.DictReader(f): trades.append(row)
    return csvs[-1].name, trades

def _safe_float(val, default=1.0):
    """Safely convert to float — handles 'N/A', '', None."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def format_orca_data(trades):
    if not trades: return "(No ORCA data available — analyst running on news only)"

    # Summary table (with 25-delta skew + VWKS data)
    lines = [f"{'Ticker':8s} {'Strategy':25s} {'IV':>6s} {'HV':>6s} {'IV/HV':>6s} {'Rank':>6s} {'Skew25D':>8s} {'SkewR':>6s} {'VWKS':>7s} {'Score':>6s}","─"*95]
    for t in trades[:20]:
        skew_val = t.get('Skew25D', 'N/A')
        skew_r = t.get('SkewRatio', 'N/A')
        vwks_val = t.get('VWKS', '1.0000')
        lines.append(f"{t.get('Ticker','?'):8s} {t.get('Strategy','?'):25s} {t.get('IV','?'):>6s} {t.get('HV_YZ','?'):>6s} {t.get('IV/HV','?'):>6s} {t.get('IVRank','?'):>6s} {str(skew_val):>8s} {str(skew_r):>6s} {str(vwks_val):>7s} {t.get('Score','?'):>6s}")

    # Detailed entries with EXACT strikes, prices, targets, and stop alerts
    lines.append(f"\n{'─'*70}")
    lines.append("SCANNER ENTRY DETAILS (use these EXACT strikes, prices, targets, and alerts):")
    lines.append("⚠️ COPY THESE EXACTLY — do NOT pick your own strikes. Violations = BLOCKED message.")
    for t in trades[:15]:
        ticker = t.get('Ticker', '?')
        entry = t.get('Entry', '')
        profit = t.get('Profit', '')
        max_loss = t.get('MaxLoss', '')
        urgency = t.get('Urgency', '')
        profit_target = t.get('ProfitTarget', '')
        stop_alert = t.get('StopAlert', '')
        spread_width = t.get('SpreadWidth', '')
        quality = t.get('QualityRatio', '')
        if entry:
            skew_str = f" | Skew25D: {t.get('Skew25D', 'N/A')} | SkewR: {t.get('SkewRatio', 'N/A')} | VWKS: {t.get('VWKS', '1.0000')}"
            target_str = f" | Target: {profit_target}" if profit_target else ""
            alert_str = f" | StopAlert: {stop_alert}" if stop_alert else ""
            width_str = f" | Width: {spread_width}" if spread_width else ""
            quality_str = f" | Quality: {quality}" if quality else ""
            lines.append(f"  {ticker}: {entry}")
            lines.append(f"    MaxLoss: {max_loss} | Urgency: {urgency}{target_str}{alert_str}{width_str}{quality_str}{skew_str}")

    cheap = [f"{t['Ticker']}({_safe_float(t.get('IV/HV')):.2f}x)" for t in trades if _safe_float(t.get('IV/HV')) < 0.85]
    if cheap: lines.append(f"\nCHEAP IV (potential catalyst buys): {', '.join(cheap)}")
    expensive = [f"{t['Ticker']}({_safe_float(t.get('IV/HV')):.2f}x)" for t in trades if _safe_float(t.get('IV/HV')) > 1.3]
    if expensive: lines.append(f"EXPENSIVE IV (sell premium): {', '.join(expensive)}")
    return "\n".join(lines)

# ============================================================
# PRE-SEND VALIDATION — catch issues before Telegram
# ============================================================
def auto_correct_strikes(analysis: str, trades: list) -> tuple:
    """
    Auto-correct Opus output: replace any wrong strikes with scanner's exact strikes.
    Returns (corrected_analysis, list_of_corrections).
    This is the NUCLEAR OPTION — Opus can write whatever it wants, we force correct strikes.
    """
    import re as _re
    corrections = []

    # Build scanner strike map: TICKER → (s1, s2, spot, full_entry_string)
    scanner_strikes = {}
    for t in trades:
        ticker = t.get("Ticker", "")
        entry = t.get("Entry", "")
        if not ticker or not entry:
            continue
        strike_match = _re.search(r'\$(\d+(?:\.\d+)?)/\$(\d+(?:\.\d+)?)', entry)
        spot_match = _re.search(r'Spot\s*\$(\d+(?:\.\d+)?)', entry)
        if strike_match:
            s1 = float(strike_match.group(1))
            s2 = float(strike_match.group(2))
            spot = float(spot_match.group(1)) if spot_match else 0
            scanner_strikes[ticker] = (s1, s2, spot, entry)

    # Find each trade block in Opus output
    # Handle markdown: **#1: TICKER**, **ENTRY:**, etc.
    # Pattern: #N followed by : or whitespace, then ticker word
    trade_pattern = _re.compile(
        r'(#\d+)[:\s*]+(\w+)(.*?)(?=(?:#\d+[:\s*]+\w)|📊|👀|🧠|$)',
        _re.DOTALL
    )

    corrected = analysis
    for match in trade_pattern.finditer(analysis):
        trade_num = match.group(1)
        ticker = match.group(2)
        trade_block = match.group(0)

        if ticker not in scanner_strikes:
            continue

        scan_s1, scan_s2, spot, scan_entry = scanner_strikes[ticker]

        # Find ALL $X/$Y strike patterns in this trade block
        # Handles: $29.00/$34.50, $29/$34.50, etc.
        strike_pattern = _re.compile(r'\$(\d+(?:\.\d+)?)/\$(\d+(?:\.\d+)?)')
        for smatch in strike_pattern.finditer(trade_block):
            opus_s1 = float(smatch.group(1))
            opus_s2 = float(smatch.group(2))

            # Check if Opus used different strikes
            if {opus_s1, opus_s2} != {scan_s1, scan_s2}:
                old_str = smatch.group(0)  # e.g. "$29.00/$34.50"
                new_str = f"${scan_s1:.2f}/${scan_s2:.2f}"
                corrected = corrected.replace(old_str, new_str, 1)
                corrections.append(
                    f"✏️ {ticker}: Fixed strikes {old_str} → {new_str} "
                    f"(scanner: spot ${spot:.2f})"
                )

    # Also fix breakeven/capital if strikes changed (recalc from scanner entry)
    # This is best-effort — the key fix is the strikes themselves

    return corrected, corrections


def validate_analysis(analysis: str, trades: list) -> list:
    """
    Validate Opus output before sending to Telegram.
    Returns list of warning strings (empty = all good).
    🛑 prefix = critical (will BLOCK sending)
    ⚠️ prefix = minor warning (will still send)
    NOTE: This runs AFTER auto_correct_strikes, so strike mismatches should be fixed already.
    If any remain, it's a BLOCK.
    """
    warnings = []
    import re as _val_re

    # ── 1. Build scanner strike map for cross-reference ──
    scanner_strikes = {}
    for t in trades:
        ticker = t.get("Ticker", "")
        entry = t.get("Entry", "")
        if not ticker or not entry:
            continue
        strike_match = _val_re.search(r'\$(\d+(?:\.\d+)?)/\$(\d+(?:\.\d+)?)', entry)
        spot_match = _val_re.search(r'Spot\s*\$(\d+(?:\.\d+)?)', entry)
        if strike_match:
            s1 = float(strike_match.group(1))
            s2 = float(strike_match.group(2))
            spot = float(spot_match.group(1)) if spot_match else 0
            scanner_strikes[ticker] = (s1, s2, spot, entry)

    # ── 2. Check each spread Opus mentions against scanner data ──
    # Handle markdown bold: **ENTRY:** or ENTRY: or **ENTRY**:
    trade_blocks = _val_re.findall(
        r'#\d+[:\s*]+(\w+).*?(?:\*\*)?ENTRY(?:\*\*)?[:\s].*?\$(\d+(?:\.\d+)?)/\$(\d+(?:\.\d+)?)',
        analysis, _val_re.DOTALL
    )
    for ticker, s1_str, s2_str in trade_blocks:
        opus_s1 = float(s1_str)
        opus_s2 = float(s2_str)
        opus_width = abs(opus_s1 - opus_s2)

        if ticker in scanner_strikes:
            scan_s1, scan_s2, spot, scan_entry = scanner_strikes[ticker]
            scan_width = abs(scan_s1 - scan_s2)

            # Check if Opus STILL has wrong strikes (after auto-correct)
            if {opus_s1, opus_s2} != {scan_s1, scan_s2}:
                warnings.append(
                    f"🛑 STRIKE MISMATCH {ticker}: ${s1_str}/${s2_str} "
                    f"(${opus_width:.2f} wide) vs scanner ${scan_s1:.2f}/${scan_s2:.2f} "
                    f"(${scan_width:.2f} wide, spot ${spot:.2f})"
                )

            # Check if spread brackets the spot price
            if spot > 0:
                spread_min = min(opus_s1, opus_s2)
                spread_max = max(opus_s1, opus_s2)
                if not (spread_min <= spot <= spread_max):
                    warnings.append(
                        f"🛑 NOT IN-OUT {ticker}: ${s1_str}/${s2_str} doesn't bracket "
                        f"spot ${spot:.2f}"
                    )
        else:
            # Ticker not in scanner
            if opus_width > 5:
                warnings.append(
                    f"⚠️ Wide spread {ticker}: ${s1_str}/${s2_str} (${opus_width:.0f} wide)"
                )

    # ── 3. General wide spread check ──
    spread_pattern = _val_re.compile(r'\$(\d+(?:\.\d+)?)/\$(\d+(?:\.\d+)?)')
    for match in spread_pattern.finditer(analysis):
        s1, s2 = float(match.group(1)), float(match.group(2))
        width = abs(s1 - s2)
        if width > 5:
            already_flagged = any(match.group(1) in w and match.group(2) in w for w in warnings)
            if not already_flagged:
                warnings.append(
                    f"⚠️ Wide spread: ${match.group(1)}/${match.group(2)} (${width:.0f} wide)"
                )

    # ── 4. Check CONF ordering ──
    conf_pattern = _val_re.compile(r'#(\d+).*?CONF:\s*(\d+)', _val_re.DOTALL)
    confs = [(int(m.group(1)), int(m.group(2))) for m in conf_pattern.finditer(analysis)]
    if len(confs) >= 2:
        if confs[0][1] < confs[1][1]:
            warnings.append(
                f"⚠️ Trade #{confs[0][0]} (CONF {confs[0][1]}) ranked above "
                f"#{confs[1][0]} (CONF {confs[1][1]})"
            )

    return warnings


def opus_review_pass(analysis: str, orca_text: str, open_positions: str, tracker) -> tuple:
    """
    Second Opus call — QA review of the finished analysis before sending.
    Uses extended thinking with small budget to catch logic errors,
    contradictions, price issues, and open-position re-recommendations.
    Returns (review_text, cost) or (None, 0) on error/skip.
    """
    if not ANTHROPIC_API_KEY:
        return None, 0

    review_prompt = """You are a senior options trading editor. Review this analysis BEFORE it goes to paying subscribers.
IMPORTANT: If you say BLOCK, the message will NOT be sent to subscribers. Only use BLOCK for genuinely embarrassing errors.

CHECK EACH TRADE FOR:
1. LOGIC: Does the thesis match the direction? (bullish thesis → should be call/put-spread-bull, not bear)
2. CONTRADICTIONS: Do any trades contradict each other? (e.g., bullish energy + bearish oil)
3. PRICE SANITY: Are strikes reasonable for the stock price? Is debit/credit realistic?
4. OPEN POSITIONS: If open positions are listed, did the analyst re-recommend any of those tickers? That's a mistake.
5. MISSING DATA: Every trade MUST have: exact strikes, exact expiry, exact debit/credit in $, CONF 1-10, URG
6. SPREAD WIDTH: Both legs must bracket the spot price (one ITM, one OTM). Width should be 1-2 strikes. If a spread has both legs on the same side of spot, that's BLOCK.
7. DEBIT TARGETS: Debit spreads should include profit target (1.65x debit). Flag if missing.
8. STRIKE ACCURACY: Compare the strikes in the analysis against the scanner data. If the analyst used DIFFERENT strikes than the scanner provided, that's BLOCK — the scanner's strikes are from the real options chain.
9. DIRECTION INVERSION: If the thesis says bearish but the trade is bullish (or vice versa), that's BLOCK.

RESPOND IN THIS FORMAT:
- If everything looks good: "PASS — all trades are logically consistent, properly structured, and ready for subscribers."
- If minor issues: "WARNING — [brief issue]. Trades are still sendable but note: [detail]"
- If serious issues: "BLOCK — [issue that would embarrass us]. Specific problem: [detail]"

BLOCK means subscribers receive NOTHING today. Use it only when there's a real error that would damage credibility.
Be concise. Max 3-4 sentences. Focus only on REAL problems, not style preferences."""

    user_msg = f"""=== ANALYSIS TO REVIEW ===
{analysis}

=== SCANNER DATA (for price verification) ===
{orca_text[:3000]}"""

    if open_positions:
        user_msg += f"""

=== OPEN POSITIONS (should NOT be re-recommended) ===
{open_positions}"""

    try:
        request_body = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 6000,
            "system": review_prompt,
            "thinking": {
                "type": "enabled",
                "budget_tokens": 4000
            },
            "messages": [{"role": "user", "content": user_msg}],
        }

        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json=request_body,
            timeout=300,
        )

        if resp.status_code != 200:
            print(f"  ⚠ Review API error {resp.status_code}: {resp.text[:200]}")
            return None, 0

        data = resp.json()
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)

        text_output = ""
        thinking_tokens = 0
        for block in data.get("content", []):
            if block.get("type") == "thinking":
                thinking_tokens += len(block.get("thinking", "")) // 4
            elif block.get("type") == "text":
                text_output += block["text"]

        cost = tracker.log_call(ANTHROPIC_MODEL, input_tokens, output_tokens,
                                thinking_tokens, "review_pass")

        return text_output.strip(), cost

    except Exception as e:
        print(f"  ⚠ Review pass error: {e}")
        return None, 0


def opus_surgical_fix(analysis: str, block_reason: str, tracker) -> tuple:
    """
    When Opus Review finds a BLOCK-worthy issue in ONE trade,
    surgically remove that trade and clean up the analysis —
    instead of throwing away the entire report.

    Returns (fixed_analysis, cost) or (None, 0) if fix fails.
    """
    if not ANTHROPIC_API_KEY:
        return None, 0

    fix_prompt = """You are a senior options trading editor doing emergency surgery on an analysis.

The QA review found a CRITICAL error in one or more specific trades. Your job:
1. IDENTIFY which trade(s) have the error described in the block reason
2. COMPLETELY REMOVE those trades from the analysis (the numbered section, scanner details, everything)
3. Renumber the remaining trades sequentially (#1, #2, #3...)
4. Remove any references to the deleted trade(s) in the REGIME, WATCH, or THINKING sections
5. Update the trade count in any headers/summaries
6. Keep ALL other trades exactly as they are — do not modify anything else
7. If the THINKING section references the removed trade, replace with a brief note: "[Trade removed — failed math validation]"

CRITICAL RULES:
- Do NOT rewrite or improve any surviving trades
- Do NOT add new trades to replace the removed one
- Do NOT change any analysis, thesis, or commentary on surviving trades
- Just cleanly excise the bad trade and stitch the rest together
- Output ONLY the cleaned analysis text, nothing else (no explanations, no "Here's the fixed version")"""

    user_msg = f"""=== BLOCK REASON FROM QA ===
{block_reason}

=== ORIGINAL ANALYSIS (remove the bad trade) ===
{analysis}"""

    try:
        request_body = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 12000,
            "system": fix_prompt,
            "thinking": {
                "type": "enabled",
                "budget_tokens": 3000
            },
            "messages": [{"role": "user", "content": user_msg}],
        }

        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json=request_body,
            timeout=300,
        )

        if resp.status_code != 200:
            print(f"  ⚠ Surgical fix API error {resp.status_code}: {resp.text[:200]}")
            return None, 0

        data = resp.json()
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)

        text_output = ""
        thinking_tokens = 0
        for block in data.get("content", []):
            if block.get("type") == "thinking":
                thinking_tokens += len(block.get("thinking", "")) // 4
            elif block.get("type") == "text":
                text_output += block["text"]

        cost = tracker.log_call(ANTHROPIC_MODEL, input_tokens, output_tokens,
                                thinking_tokens, "surgical_fix")

        fixed = text_output.strip()
        if len(fixed) > 200:  # Sanity check — got meaningful output
            return fixed, cost
        else:
            print(f"  ⚠ Surgical fix returned too-short output ({len(fixed)} chars)")
            return None, 0

    except Exception as e:
        print(f"  ⚠ Surgical fix error: {e}")
        return None, 0


# ============================================================
# GPT-5.4 THESIS REVIEW (dual-model consensus)
# Flow: Opus wrote trades → GPT challenges thesis/catalyst/timing →
#       Opus accepts or defends → improved analysis
# ============================================================

GPT_CO_ANALYST_PROMPT = """You are a senior options strategist and CO-ANALYST. You work alongside another AI analyst (Claude Opus) who has already analyzed the same scanner data and news. You are NOT a reviewer or gatekeeper — you are a PARTNER at the same trading desk.

Your job: Look at the SAME scanner data and news that Claude saw, look at Claude's trade ideas, and contribute your OWN perspective as a collaborative analyst.

TRADING PHILOSOPHY (both analysts must follow):
- SECOND-DERIVATIVE IDEAS: If the first-order trade is already crowded, find the NEXT link in the chain. Oil spikes → everyone buys XLE → crowded. Second derivative: tanker stocks, pipeline MLPs, or airlines that get hurt. Real example: war breaks out → crowd buys defense/oil → we shorted EWY (South Korea ETF) because South Korea is energy-import dependent → EWY crashed 20%+ next day. THAT is second-derivative thinking. ALWAYS ask: "what's the trade BEHIND the trade?"
- QUICK CATALYSTS: We want trades that RESOLVE within 1-4 weeks. Not months. The catalyst must be IMMINENT and SPECIFIC — "FDA ruling next Tuesday", "OPEC meeting Thursday", "freeze hitting Gulf Coast this weekend." Reject vague catalysts.
- SCREEN EVERYTHING: News feeds, weather reports (NWS alerts), geopolitical events, regulatory calendars, seasonal patterns, supply chain disruptions. Check what happened LAST TIME a similar event occurred — how fast did it resolve?
- LOW CAPITAL IN-OUT SPREADS: Mainly debit/credit spreads with limited risk. CRITICAL PRICING RULE: For debit spreads, the max debit must be ≤58% of the spread width. Example: $2 wide spread → max debit $1.16, $5 wide → max $2.90. If no spread meets this criteria, use a deep ITM naked BUY CALL or BUY PUT (2-3 strikes in the money). If ALL options are very expensive (naked option >15% of stock price or IV/HV >2.0), include a stock-only P&L reference line so subscribers see what the equivalent stock trade would return. For credit spreads, we want to collect at least 40% of width. Always show IV/HV and Skew25D for each trade.

FOR EACH OF CLAUDE'S TRADES:
1. THESIS CHECK — Does the cause-effect chain hold? Is the catalyst real, SPECIFIC, and IMMINENT (within 4 weeks)?
2. CROWDING CHECK — Is this trade already crowded? Is the crowd already positioned this way? If so, is there a second-derivative angle?
3. YOUR ANGLE — What do YOU see that Claude might have missed? A stronger catalyst? A better timing argument? A risk Claude underweighted?
4. CATALYST & TIMING — Do you agree on the catalyst AND the timing? Will this resolve within the option's DTE?

YOUR OWN IDEAS (this is critical — you're not just reviewing, you're CONTRIBUTING):
5. MISSED OPPORTUNITIES — Look at the scanner data. Are there high-scoring tickers Claude skipped? Generate 1-3 of your own trade ideas. PREFER second-derivative ideas that are NOT yet crowded.
6. BEARISH ANGLES — If the SPY regime or macro data suggests risk, propose bearish trades or hedges. Don't force it, but if macro risk is elevated, lean into protective setups.
7. CONTRARIAN VIEW — Where is the crowd on one side? Where can you sell overpriced premium or take the other side?
8. WEATHER/GEO CHECK — Any weather events (storms, freezes, droughts) or geopolitical catalysts (sanctions, military moves, trade policy) that create a quick trade? Check how previous similar events resolved.

REGIME AWARENESS:
- The SPY Regime model (if provided) gives you the MACRO BACKDROP. It is NOT a blocker — it's intelligence.
- If regime says HIGH RISK / BEARISH: Consider sizing down bullish trades, adding bearish ideas, or proposing hedges. But DON'T kill a genuinely strong sector-specific trade just because SPY regime is cautious.
- If regime says LOW RISK / BULLISH: You can be more aggressive, but still watch for sector-specific traps.
- The regime is one input among many — treat it as a smart colleague's macro view.

RESPOND WITH A JSON object:
{
  "trade_reviews": [
    {"trade": "#1 TICKER", "agree": true/false, "your_view": "your perspective on this trade", "catalyst_agree": true/false, "timing_agree": true/false, "improvements": "specific suggestions if any"}
  ],
  "your_ideas": [
    {"ticker": "TICKER", "direction": "BUY_CALL_SPREAD/SELL_PUT_SPREAD/etc", "thesis": "your cause-effect chain", "catalyst": "specific catalyst + timing", "confidence": 1-10}
  ],
  "regime_note": "how the current macro regime should influence the overall trade set",
  "consensus_summary": "which trades you and Claude BOTH believe in (strongest conviction)"
}

Think like a PARTNER, not a critic. The best output comes when two smart analysts agree on the same thesis from different angles."""


def gpt_co_analyst(analysis: str, orca_text: str, open_positions: str, regime_data: str, tracker) -> tuple:
    """
    GPT-5.4 Thinking as CO-ANALYST — not a reviewer, a partner.
    GPT sees the same data Claude saw, reviews Claude's ideas, AND generates its own.
    Returns (gpt_output_dict, cost) or (None, 0) on error/skip.
    """
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not openai_key:
        print("  ℹ No OPENAI_API_KEY — GPT co-analyst skipped")
        return None, 0

    user_msg = f"""=== CLAUDE'S TRADE IDEAS ===
{analysis}

=== SCANNER DATA (same data Claude used — look for trades Claude missed) ===
{orca_text[:5000]}"""

    if open_positions:
        user_msg += f"""

=== OPEN POSITIONS (do NOT re-recommend these tickers) ===
{open_positions}"""

    if regime_data:
        user_msg += f"""

=== SPY REGIME MODEL (macro backdrop — use as context, not as blocker) ===
{regime_data}"""

    try:
        print("  🤖 GPT-5.4 co-analyst generating ideas + reviewing Claude's trades...")

        # Primary: gpt-5.4-thinking (highest intelligence)
        model = "gpt-5.4-thinking"
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {openai_key}",
            },
            json={
                "model": model,
                "max_completion_tokens": 6000,
                "reasoning_effort": "medium",
                "messages": [
                    {"role": "system", "content": GPT_CO_ANALYST_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
            },
            timeout=300,
        )

        # Fallback: gpt-5.4 base if thinking model unavailable
        if resp.status_code == 404:
            model = "gpt-5.4"
            print(f"  ℹ gpt-5.4-thinking not available — falling back to {model}")
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {openai_key}",
                },
                json={
                    "model": model,
                    "max_completion_tokens": 5000,
                    "temperature": 0.3,
                    "messages": [
                        {"role": "system", "content": GPT_CO_ANALYST_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                },
                timeout=120,
            )

        if resp.status_code != 200:
            print(f"  ⚠ GPT API error {resp.status_code}: {resp.text[:200]}")
            return None, 0

        data = resp.json()
        usage = data.get("usage", {})
        cost = usage.get("prompt_tokens", 0) / 1_000_000 * 2.5 + usage.get("completion_tokens", 0) / 1_000_000 * 10.0
        content = data["choices"][0]["message"]["content"].strip()

        print(f"  📋 GPT co-analyst ({model}) cost: ${cost:.4f}")

        # Parse JSON from GPT response (robust extraction)
        gpt_output = _extract_json_from_text(content)
        if not gpt_output:
            print(f"  ⚠ GPT response not valid JSON (first 200 chars: {content[:200]})")
            return None, 0

        # Print summary
        trade_reviews = gpt_output.get("trade_reviews", [])
        agreed = sum(1 for r in trade_reviews if r.get("agree"))
        disagreed = len(trade_reviews) - agreed
        print(f"  📊 Claude's trades: {agreed} agreed, {disagreed} challenged")

        gpt_ideas = gpt_output.get("your_ideas", [])
        if gpt_ideas:
            print(f"  💡 GPT's own ideas: {len(gpt_ideas)}")
            for idea in gpt_ideas:
                print(f"     → {idea.get('ticker', '?')} {idea.get('direction', '?')} (conf {idea.get('confidence', '?')}): {idea.get('thesis', '')[:80]}")

        consensus = gpt_output.get("consensus_summary", "")
        if consensus:
            print(f"  🤝 Consensus: {consensus[:120]}")

        regime_note = gpt_output.get("regime_note", "")
        if regime_note:
            print(f"  📈 Regime view: {regime_note[:120]}")

        # Store tracking data for main thread (SQLite can't cross threads)
        gpt_output["_tracking"] = {
            "model": model,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "thinking_tokens": 0,
            "label": "gpt_co_analyst",
        }

        return gpt_output, cost

    except json.JSONDecodeError:
        print("  ⚠ GPT response not valid JSON — skipping co-analyst")
        return None, 0
    except Exception as e:
        print(f"  ⚠ GPT co-analyst error: {e}")
        return None, 0


# ============================================================
# GEMINI CO-ANALYST — Third AI perspective (parallel with GPT)
# ============================================================
GEMINI_CO_ANALYST_PROMPT = """You are a senior options strategist and CO-ANALYST. You work alongside two other AI analysts (Claude Opus and GPT-5.4) who analyze the same scanner data and news. You are the THIRD INDEPENDENT PERSPECTIVE at the trading desk.

Your job: Look at the SAME scanner data and news that Claude saw, look at Claude's trade ideas, and contribute your OWN perspective as a collaborative analyst. Your review is INDEPENDENT — you have NOT seen GPT's review.

TRADING PHILOSOPHY (all analysts must follow):
- SECOND-DERIVATIVE IDEAS: If the first-order trade is already crowded, find the NEXT link in the chain. Oil spikes → everyone buys XLE → crowded. Second derivative: tanker stocks, pipeline MLPs, or airlines that get hurt. Real example: war breaks out → crowd buys defense/oil → we shorted EWY (South Korea ETF) because South Korea is energy-import dependent → EWY crashed 20%+ next day. THAT is second-derivative thinking. ALWAYS ask: "what's the trade BEHIND the trade?"
- QUICK CATALYSTS: We want trades that RESOLVE within 1-4 weeks. Not months. The catalyst must be IMMINENT and SPECIFIC — "FDA ruling next Tuesday", "OPEC meeting Thursday", "freeze hitting Gulf Coast this weekend." Reject vague catalysts.
- SCREEN EVERYTHING: News feeds, weather reports (NWS alerts), geopolitical events, regulatory calendars, seasonal patterns, supply chain disruptions. Check what happened LAST TIME a similar event occurred — how fast did it resolve?
- LOW CAPITAL IN-OUT SPREADS: Mainly debit/credit spreads with limited risk. CRITICAL PRICING RULE: For debit spreads, the max debit must be ≤58% of the spread width. Example: $2 wide spread → max debit $1.16, $5 wide → max $2.90. If no spread meets this criteria, use a deep ITM naked BUY CALL or BUY PUT (2-3 strikes in the money). If ALL options are very expensive (naked option >15% of stock price or IV/HV >2.0), include a stock-only P&L reference line so subscribers see what the equivalent stock trade would return. For credit spreads, we want to collect at least 40% of width. Always show IV/HV and Skew25D for each trade.

FOR EACH OF CLAUDE'S TRADES:
1. THESIS CHECK — Does the cause-effect chain hold? Is the catalyst real, SPECIFIC, and IMMINENT (within 4 weeks)?
2. CROWDING CHECK — Is this trade already crowded? Is the crowd already positioned this way? If so, is there a second-derivative angle?
3. YOUR ANGLE — What do YOU see that Claude might have missed? A stronger catalyst? A better timing argument? A risk Claude underweighted?
4. CATALYST & TIMING — Do you agree on the catalyst AND the timing? Will this resolve within the option's DTE?

YOUR OWN IDEAS (this is critical — you're not just reviewing, you're CONTRIBUTING):
5. MISSED OPPORTUNITIES — Look at the scanner data. Are there high-scoring tickers Claude skipped? Generate 1-3 of your own trade ideas. PREFER second-derivative ideas that are NOT yet crowded.
6. CONTRARIAN VIEW — Where is the crowd on one side? Where can you sell overpriced premium or take the other side?
7. CROSS-ASSET CONNECTIONS — What connections do you see across asset classes (equities, commodities, rates, currencies) that create a trade? Think about second and third-order effects.

REGIME AWARENESS:
- The SPY Regime model (if provided) gives you the MACRO BACKDROP. It is NOT a blocker — it's intelligence.
- If regime says HIGH RISK / BEARISH: Consider sizing down bullish trades, adding bearish ideas, or proposing hedges.
- If regime says LOW RISK / BULLISH: You can be more aggressive, but still watch for sector-specific traps.

RESPOND WITH A JSON object:
{
  "trade_reviews": [
    {"trade": "#1 TICKER", "agree": true/false, "your_view": "your perspective on this trade", "catalyst_agree": true/false, "timing_agree": true/false, "improvements": "specific suggestions if any"}
  ],
  "your_ideas": [
    {"ticker": "TICKER", "direction": "BUY_CALL_SPREAD/SELL_PUT_SPREAD/etc", "thesis": "your cause-effect chain", "catalyst": "specific catalyst + timing", "confidence": 1-10}
  ],
  "regime_note": "how the current macro regime should influence the overall trade set",
  "consensus_summary": "which trades you believe are strongest and why"
}

Think like a PARTNER, not a critic. Bring your unique perspective — that's why there are three analysts at this desk."""


def _clean_json_string(raw: str) -> str:
    """Fix common JSON issues from AI responses (trailing commas, comments, etc.)."""
    import re
    s = raw.strip()
    # Remove single-line comments (// ...)
    s = re.sub(r'//[^\n]*', '', s)
    # Remove trailing commas before } or ]
    s = re.sub(r',\s*([}\]])', r'\1', s)
    # Fix unescaped newlines inside string values (common Gemini issue)
    # This is tricky — only fix obvious cases
    return s


def _try_parse_json(text: str) -> dict | None:
    """Try to parse JSON, with cleanup fallback."""
    import json as _json
    # First try raw
    try:
        parsed = _json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        elif isinstance(parsed, list):
            return {"trade_reviews": parsed}
        return None
    except (json.JSONDecodeError, ValueError):
        pass
    # Try with cleanup
    try:
        cleaned = _clean_json_string(text)
        parsed = _json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
        elif isinstance(parsed, list):
            return {"trade_reviews": parsed}
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _extract_json_from_text(text: str) -> dict | None:
    """
    Robust JSON extraction from AI responses.
    Handles: markdown code blocks, raw JSON, JSON wrapped in preamble/epilogue text.
    Cleans common issues: trailing commas, comments, etc.
    Returns parsed dict or None.
    """
    import re

    if not text or not text.strip():
        return None

    content = text.strip()

    # Strategy 1: Markdown code block (```json ... ``` or ``` ... ```)
    if "```json" in content:
        try:
            extracted = content.split("```json")[1].split("```")[0].strip()
            result = _try_parse_json(extracted)
            if result:
                return result
            print(f"    [json-debug] markdown ```json block found but parse failed, first 100: {extracted[:100]}")
        except (IndexError, Exception) as e:
            print(f"    [json-debug] markdown split error: {e}")

    if "```" in content:
        try:
            # Find ALL code blocks and try each
            blocks = content.split("```")
            for i in range(1, len(blocks), 2):  # Odd indices are inside code blocks
                block = blocks[i].strip()
                # Skip if it starts with a language tag
                if block.startswith("json"):
                    block = block[4:].strip()
                result = _try_parse_json(block)
                if result:
                    return result
        except (IndexError, Exception):
            pass

    # Strategy 2: Find { and matching } using brace counting
    brace_starts = [i for i, c in enumerate(content) if c == '{']
    for start in brace_starts:
        depth = 0
        in_string = False
        escape = False
        for i in range(start, len(content)):
            c = content[i]
            if escape:
                escape = False
                continue
            if c == '\\' and in_string:
                escape = True
                continue
            if c == '"' and not escape:
                in_string = not in_string
                continue
            if in_string:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    candidate = content[start:i + 1]
                    result = _try_parse_json(candidate)
                    if result:
                        return result
                    break
        if depth == 0:
            break  # Only try the first complete object

    # Strategy 3: Simple first-{ to last-} extraction
    if '{' in content and '}' in content:
        first = content.index('{')
        last = content.rindex('}') + 1
        candidate = content[first:last]
        result = _try_parse_json(candidate)
        if result:
            return result

    # Strategy 4: Try the whole content as JSON
    result = _try_parse_json(content)
    if result:
        return result

    return None


def gemini_co_analyst(analysis: str, orca_text: str, open_positions: str, regime_data: str, tracker) -> tuple:
    """
    Gemini 2.5 Pro Deep Think as CO-ANALYST — third independent perspective.
    Gemini sees the same data Claude saw, reviews Claude's ideas, AND generates its own.
    Runs INDEPENDENTLY from GPT (has NOT seen GPT's review).
    Returns (gemini_output_dict, cost) or (None, 0) on error/skip.
    """
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        print("  ℹ No GEMINI_API_KEY — Gemini co-analyst skipped")
        return None, 0

    user_msg = f"""=== CLAUDE'S TRADE IDEAS ===
{analysis}

=== SCANNER DATA (same data Claude used — look for trades Claude missed) ===
{orca_text[:5000]}"""

    if open_positions:
        user_msg += f"""

=== OPEN POSITIONS (do NOT re-recommend these tickers) ===
{open_positions}"""

    if regime_data:
        user_msg += f"""

=== SPY REGIME MODEL (macro backdrop — use as context, not as blocker) ===
{regime_data}"""

    try:
        print("  🔮 Gemini co-analyst generating ideas + reviewing Claude's trades...")

        # Gemini API via generativelanguage.googleapis.com (REST)
        # Primary: gemini-3.1-pro-preview with Deep Think HIGH
        model = "gemini-3.1-pro-preview"
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{GEMINI_CO_ANALYST_PROMPT}\n\n{user_msg}"}]
                }
            ],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 16000,
                "thinkingConfig": {
                    "thinkingLevel": "HIGH"
                }
            }
        }

        resp = requests.post(api_url, json=payload, timeout=300)

        # Fallback: try without thinking if the model doesn't support it
        if resp.status_code in (400, 404):
            error_text = resp.text[:300]
            if "thinkingConfig" in error_text or "thinking" in error_text.lower():
                print(f"  ℹ Gemini thinking not available — retrying without thinking...")
                payload["generationConfig"].pop("thinkingConfig", None)
                resp = requests.post(api_url, json=payload, timeout=300)

        # If 2.5-pro fails entirely, try 2.0-flash as fallback
        if resp.status_code in (400, 404):
            model = "gemini-3.1-pro"
            print(f"  ℹ Gemini 3.1 Pro not available — falling back to {model}")
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
            payload["generationConfig"].pop("thinkingConfig", None)
            resp = requests.post(api_url, json=payload, timeout=300)

        if resp.status_code != 200:
            print(f"  ⚠ Gemini API error {resp.status_code}: {resp.text[:300]}")
            return None, 0

        data = resp.json()

        # Extract text from Gemini response
        content = ""
        try:
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                for part in parts:
                    if "text" in part:
                        content += part["text"]
        except (IndexError, KeyError, TypeError):
            print("  ⚠ Gemini response structure unexpected")
            return None, 0

        if not content.strip():
            print("  ⚠ Gemini returned empty content")
            return None, 0

        # Cost calculation (Gemini pricing — much cheaper)
        usage = data.get("usageMetadata", {})
        prompt_tokens = usage.get("promptTokenCount", 0)
        completion_tokens = usage.get("candidatesTokenCount", 0)
        thinking_tokens = usage.get("thoughtsTokenCount", 0)
        # Gemini 2.5 Pro pricing: ~$1.25/M input, ~$10/M output (approximate)
        cost = prompt_tokens / 1_000_000 * 1.25 + completion_tokens / 1_000_000 * 10.0

        print(f"  📋 Gemini co-analyst ({model}) cost: ${cost:.4f}")

        # Parse JSON from Gemini response (robust extraction)
        gemini_output = _extract_json_from_text(content)
        if not gemini_output:
            print(f"  ⚠ Gemini response not valid JSON (first 200 chars: {content[:200]})")
            return None, 0

        # Print summary
        trade_reviews = gemini_output.get("trade_reviews", [])
        agreed = sum(1 for r in trade_reviews if r.get("agree"))
        disagreed = len(trade_reviews) - agreed
        print(f"  📊 Claude's trades: {agreed} agreed, {disagreed} challenged")

        gemini_ideas = gemini_output.get("your_ideas", [])
        if gemini_ideas:
            print(f"  💡 Gemini's own ideas: {len(gemini_ideas)}")
            for idea in gemini_ideas:
                print(f"     → {idea.get('ticker', '?')} {idea.get('direction', '?')} (conf {idea.get('confidence', '?')}): {idea.get('thesis', '')[:80]}")

        consensus = gemini_output.get("consensus_summary", "")
        if consensus:
            print(f"  🤝 Consensus: {consensus[:120]}")

        # Store tracking data for main thread (SQLite can't cross threads)
        gemini_output["_tracking"] = {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "thinking_tokens": thinking_tokens,
            "label": "gemini_co_analyst",
        }

        return gemini_output, cost

    except Exception as e:
        print(f"  ⚠ Gemini co-analyst error: {e}")
        return None, 0


def opus_merge_consensus(analysis: str, gpt_output: dict, orca_text: str, tracker, gemini_output: dict = None) -> tuple:
    """
    Opus merges its own analysis with co-analysts' collaborative input.
    Supports DUAL (GPT only) or TRIPLE (GPT + Gemini) consensus.
    - Trades ALL analysts agree on → HIGHEST CONVICTION
    - Trades 2-out-of-3 agree on → strong conviction
    - Co-analysts' own ideas → evaluate and ADD if they pass Opus's bar
    - Challenged trades → strengthen or drop
    Returns (consensus_analysis, cost) or (original_analysis, 0) on error.
    """
    if not ANTHROPIC_API_KEY or not gpt_output:
        return analysis, 0

    import json as _json
    gpt_text = _json.dumps(gpt_output, indent=2)

    # Count agreements for logging
    gpt_reviews = gpt_output.get("trade_reviews", [])
    gpt_agreed = sum(1 for r in gpt_reviews if r.get("agree"))
    gpt_ideas = gpt_output.get("your_ideas", [])

    # Gemini stats (if available)
    gemini_text = ""
    gemini_agreed = 0
    gemini_ideas_list = []
    triple_mode = gemini_output is not None
    if triple_mode:
        gemini_text = _json.dumps(gemini_output, indent=2)
        gemini_reviews = gemini_output.get("trade_reviews", [])
        gemini_agreed = sum(1 for r in gemini_reviews if r.get("agree"))
        gemini_ideas_list = gemini_output.get("your_ideas", [])

    if triple_mode:
        system_prompt = """You are building the FINAL CONSENSUS analysis from THREE AI analysts working together.

You (Claude Opus) wrote the initial trades. Your two partners — GPT-5.4 and Gemini — have INDEPENDENTLY reviewed your ideas AND proposed some of their own. They did NOT see each other's reviews. Now build the BEST possible set of trades using ALL three perspectives.

TRIPLE-AI CONSENSUS RULES:
1. ALL THREE AGREE = MAXIMUM CONVICTION: If BOTH GPT and Gemini independently agree with your trade (catalyst + timing), that trade has TRIPLE CONSENSUS. These are your STRONGEST positions. Mark with [3/3 CONSENSUS].
2. TWO OUT OF THREE = STRONG CONVICTION: If two analysts agree but one challenges, that's still a strong trade. Address the challenger's concern but keep the trade. Mark with [2/3 CONSENSUS].
3. ONLY CLAUDE AGREES = WEAK: If BOTH GPT and Gemini challenge your trade, you need a VERY compelling defense to keep it. Otherwise DROP it. Two independent challengers finding the same problem is a strong signal.
4. CO-ANALYSTS' OWN IDEAS: Both GPT and Gemini may propose their own trades. If GPT and Gemini BOTH propose similar trades independently (same sector/direction), that's a STRONG signal — ADD it. If only one proposes it, evaluate on merit.
5. REGIME AWARENESS: Factor both co-analysts' regime notes. If both express macro concern, lean cautious. If they disagree on macro, note the divergence.
6. BEARISH IDEAS: Include bearish/hedge trades if the macro context supports caution. Don't be all-bullish when multiple analysts express caution.

OUTPUT RULES:
- Output the FINAL merged analysis in the same format (🔹 #N: TICKER, THESIS, ENTRY, EDGE, RISK, EXIT, CONF/URG).
- For co-analysts' added trades, use the scanner data for strikes if available.
- Keep ALL existing pricing numbers exactly as they were (strikes, debits, credits).
- Max 7 trades total (quality over quantity — only the strongest consensus ideas).
- Renumber trades if the set changed.
- Keep REGIME, WATCH, and THINKING sections — update them to reflect the three-way consensus view. The WATCH section should cover market-level themes only — never reference open positions.
- Do NOT add meta-commentary about the process ("GPT suggested..." / "Claude agreed..."). NEVER mention specific model names. Just output the clean analysis.

CONSENSUS LABEL (CRITICAL — add this to EVERY trade on the CONF/URG line):
For each trade, the CONF/URG line MUST include a consensus status. Format:
CONF: 8 | URG: TODAY | ✅ consensus — all analysts agree on thesis and catalyst timing
CONF: 7 | URG: THIS_WEEK | ✅ consensus — thesis agreed, minor timing debate
CONF: 6 | URG: THIS_WEEK | ⚠️ no consensus — catalyst timing questioned
CONF: 7 | URG: TODAY | ⚠️ no consensus — thesis logic challenged
CONF: 5 | URG: THIS_WEEK | ⚠️ partial consensus — entry price debated

Rules for the consensus label:
- If all analysts agree on thesis AND catalyst → "✅ consensus" + very brief reason
- If 2 of 3 agree → "✅ consensus" + note what the dissenter questioned (briefly)
- If only 1 agrees → "⚠️ no consensus" + brief reason (timing, catalyst, thesis logic, entry price, etc.)
- Keep the label SHORT (under 15 words after the emoji)
- NEVER mention model names (no "GPT", "Gemini", "Claude") — just say "analysts" or "one analyst"
- The consensus label should read like a small footnote/disclaimer"""
    else:
        system_prompt = """You are building the FINAL CONSENSUS analysis from two AI analysts working together.

You (Claude Opus) wrote the initial trades. Your partner (GPT-5.4) has reviewed your ideas AND proposed some of their own. Now build the BEST possible set of trades.

CONSENSUS RULES:
1. MUTUAL AGREEMENT = HIGHEST CONVICTION: If GPT agrees with your trade AND agrees on catalyst + timing, that trade is GOLD. Strengthen the thesis — mention that both analysts concur.
2. GPT'S OWN IDEAS: Look at trades GPT proposed that you didn't. If they have strong theses with real catalysts and the scanner data supports them, ADD them. You're partners — use the best ideas from both.
3. CHALLENGED TRADES: If GPT disagreed with one of your trades, either strengthen it with a better argument or DROP it. Don't keep a trade your partner thinks is weak unless you have a VERY strong counter-argument.
4. REGIME AWARENESS: Factor GPT's regime note into the overall positioning. If macro risk is elevated, lean toward bearish ideas, smaller sizes, or hedges. If macro is favorable, lean bullish. The regime doesn't BLOCK trades — it influences sizing and tone.
5. BEARISH IDEAS: If GPT proposed bearish trades and the regime supports caution, INCLUDE at least one bearish/hedge trade. Don't be all-bullish when the macro backdrop says be careful.

OUTPUT RULES:
- Output the FINAL merged analysis in the same format (🔹 #N: TICKER, THESIS, ENTRY, EDGE, RISK, EXIT, CONF/URG).
- For GPT's added trades, use the scanner data for strikes if available. If not in scanner, mark as "estimated pricing."
- Keep ALL existing pricing numbers exactly as they were (strikes, debits, credits).
- Max 7 trades total (quality over quantity — only the strongest consensus ideas).
- Renumber trades if the set changed.
- Keep REGIME, WATCH, and THINKING sections — update them to reflect the consensus view. The WATCH section should cover market-level themes only — never reference open positions.
- Do NOT add meta-commentary about the process ("GPT suggested..." / "we agreed on..."). NEVER mention specific model names. Just output the clean analysis.

CONSENSUS LABEL (CRITICAL — add this to EVERY trade on the CONF/URG line):
For each trade, the CONF/URG line MUST include a consensus status. Format:
CONF: 8 | URG: TODAY | ✅ consensus — both analysts agree on thesis and timing
CONF: 7 | URG: THIS_WEEK | ⚠️ no consensus — catalyst timing questioned

Rules for the consensus label:
- If both analysts agree on thesis AND catalyst → "✅ consensus" + very brief reason
- If one analyst challenged → "⚠️ no consensus" + brief reason (timing, catalyst, thesis logic, entry price)
- Keep the label SHORT (under 15 words after the emoji)
- NEVER mention model names — just say "analysts" or "one analyst"
- The consensus label should read like a small footnote/disclaimer"""

    try:
        if triple_mode:
            print(f"  🤝 Opus merging TRIPLE consensus (GPT: {gpt_agreed} agreed + {len(gpt_ideas)} ideas | Gemini: {gemini_agreed} agreed + {len(gemini_ideas_list)} ideas)...")
            user_content = f"YOUR ORIGINAL ANALYSIS:\n{analysis}\n\nGPT CO-ANALYST OUTPUT (independent review):\n{gpt_text}\n\nGEMINI CO-ANALYST OUTPUT (independent review — did NOT see GPT's review):\n{gemini_text}\n\nSCANNER DATA (for pricing new ideas):\n{orca_text[:3000]}\n\nBuild the final THREE-WAY CONSENSUS analysis:"
        else:
            print(f"  🤝 Opus merging consensus ({gpt_agreed} agreed trades + {len(gpt_ideas)} GPT ideas)...")
            user_content = f"YOUR ORIGINAL ANALYSIS:\n{analysis}\n\nGPT CO-ANALYST OUTPUT:\n{gpt_text}\n\nSCANNER DATA (for pricing GPT's ideas):\n{orca_text[:3000]}\n\nBuild the final CONSENSUS analysis:"

        request_body = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 20000,
            "system": system_prompt,
            "thinking": {
                "type": "enabled",
                "budget_tokens": 10000
            },
            "messages": [{"role": "user", "content": user_content}],
        }

        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json=request_body,
            timeout=300,
        )

        if resp.status_code != 200:
            print(f"  ⚠ Opus merge API error {resp.status_code}: {resp.text[:200]}")
            return analysis, 0

        data = resp.json()
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)

        text_output = ""
        thinking_tokens = 0
        for block in data.get("content", []):
            if block.get("type") == "thinking":
                thinking_tokens += len(block.get("thinking", "")) // 4
            elif block.get("type") == "text":
                text_output += block["text"]

        cost = tracker.log_call(ANTHROPIC_MODEL, input_tokens, output_tokens,
                                thinking_tokens, "consensus_merge")

        merged = text_output.strip()
        if merged:
            orig_trades = analysis.count("🔹 #")
            merged_trades = merged.count("🔹 #")
            consensus_trades = merged.lower().count("[consensus]")
            if merged_trades != orig_trades:
                diff = merged_trades - orig_trades
                direction = "added" if diff > 0 else "removed"
                print(f"  📝 Consensus: {merged_trades} trades ({abs(diff)} {direction}) | {consensus_trades} mutual agreement (${cost:.4f})")
            else:
                print(f"  📝 Consensus: {merged_trades} trades, {consensus_trades} mutual agreement (${cost:.4f})")
            return merged, cost
        else:
            print(f"  ⚠ Opus merge returned empty — keeping original")
            return analysis, 0

    except Exception as e:
        print(f"  ⚠ Opus consensus merge error: {e} — keeping original")
        return analysis, 0


def inject_consensus_labels(analysis: str, gpt_output: dict, gemini_output: dict = None) -> str:
    """
    Post-process: inject consensus labels onto each trade's CONF/URG line.
    Uses actual GPT/Gemini review data to determine consensus status.
    """
    import re

    if not gpt_output and not gemini_output:
        return analysis

    # Build a consensus map: ticker → {gpt_agree, gemini_agree, reason}
    consensus_map = {}

    # Parse GPT reviews
    gpt_reviews = gpt_output.get("trade_reviews", []) if gpt_output else []
    for r in gpt_reviews:
        ticker = (r.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        agree = r.get("agree", False)
        reason = r.get("challenge", r.get("reason", r.get("note", "")))
        if ticker not in consensus_map:
            consensus_map[ticker] = {"gpt": agree, "gemini": None, "gpt_reason": reason or ""}
        else:
            consensus_map[ticker]["gpt"] = agree
            consensus_map[ticker]["gpt_reason"] = reason or ""

    # Parse Gemini reviews
    gemini_reviews = gemini_output.get("trade_reviews", []) if gemini_output else []
    for r in gemini_reviews:
        ticker = (r.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        agree = r.get("agree", False)
        reason = r.get("challenge", r.get("reason", r.get("note", "")))
        if ticker not in consensus_map:
            consensus_map[ticker] = {"gpt": None, "gemini": agree, "gemini_reason": reason or ""}
        else:
            consensus_map[ticker]["gemini"] = agree
            consensus_map[ticker]["gemini_reason"] = reason or ""

    if not consensus_map:
        return analysis

    # Process each trade block
    lines = analysis.split('\n')
    result_lines = []
    current_ticker = None

    for line in lines:
        # Detect trade header: 🔹 #N: TICKER
        trade_match = re.search(r'🔹\s*#?\d+[:\s]+([A-Z]{1,5})', line)
        if trade_match:
            current_ticker = trade_match.group(1).upper()

        # Detect CONF/URG line and inject consensus label
        if current_ticker and re.search(r'CONF:\s*\d', line) and 'URG:' in line:
            # Determine consensus status
            info = consensus_map.get(current_ticker, {})
            gpt_agree = info.get("gpt")
            gemini_agree = info.get("gemini")

            if gpt_agree is True and gemini_agree is True:
                label = "✅ consensus — all analysts agree"
            elif gpt_agree is True and gemini_agree is None:
                label = "✅ consensus — analysts agree on thesis"
            elif gemini_agree is True and gpt_agree is None:
                label = "✅ consensus — analysts agree on thesis"
            elif gpt_agree is True and gemini_agree is False:
                reason = info.get("gemini_reason", "")[:60]
                label = f"✅ partial consensus — one analyst questions {_short_reason(reason)}"
            elif gemini_agree is True and gpt_agree is False:
                reason = info.get("gpt_reason", "")[:60]
                label = f"✅ partial consensus — one analyst questions {_short_reason(reason)}"
            elif gpt_agree is False and gemini_agree is False:
                reason = info.get("gpt_reason", info.get("gemini_reason", ""))[:60]
                label = f"⚠️ no consensus — {_short_reason(reason)}"
            elif gpt_agree is False and gemini_agree is None:
                reason = info.get("gpt_reason", "")[:60]
                label = f"⚠️ no consensus — {_short_reason(reason)}"
            elif gemini_agree is False and gpt_agree is None:
                reason = info.get("gemini_reason", "")[:60]
                label = f"⚠️ no consensus — {_short_reason(reason)}"
            else:
                # Ticker not in reviews (new trade from co-analysts)
                label = "🆕 co-analyst idea"

            # Append label to the CONF/URG line
            clean_line = line.rstrip()
            if label not in clean_line:
                line = f"{clean_line} | {label}"

            current_ticker = None  # Reset for next trade

        result_lines.append(line)

    return '\n'.join(result_lines)


def _short_reason(reason: str) -> str:
    """Shorten a challenge reason to a brief label."""
    if not reason:
        return "thesis debated"
    r = reason.lower()
    if any(w in r for w in ["timing", "catalyst", "expiry", "too late", "already priced"]):
        return "catalyst timing"
    if any(w in r for w in ["entry", "price", "debit", "credit", "strike", "premium"]):
        return "entry price"
    if any(w in r for w in ["thesis", "logic", "chain", "connection"]):
        return "thesis logic"
    if any(w in r for w in ["risk", "downside", "exposure"]):
        return "risk assessment"
    if any(w in r for w in ["crowd", "crowded", "consensus", "obvious"]):
        return "crowded trade"
    # Return first 30 chars of reason
    return reason[:30].strip()


# ============================================================
# SECOND PASS — Co-analysts validate the merged consensus
# ============================================================
VALIDATION_PROMPT = """You are validating a FINAL CONSENSUS analysis that was built by three AI analysts collaborating. Your job is a quick QUALITY CHECK — not a full re-analysis.

Check for:
1. CONTRADICTIONS — Do any trades contradict each other? (e.g., bullish oil + bearish energy)
2. MATH ERRORS — Do spread widths, debit/credit amounts, and risk/reward numbers add up?
3. STALE REASONING — Is any thesis based on outdated information or already-priced-in events?
4. MISSING RISKS — Is there an obvious risk that ALL three analysts overlooked?
5. OVERALL COHERENCE — Does the portfolio make sense as a whole? Is it balanced?

RESPOND WITH JSON:
{
  "issues": [
    {"trade": "#1 TICKER", "issue": "brief description", "severity": "minor|major|critical"}
  ],
  "portfolio_note": "one sentence on overall portfolio coherence",
  "verdict": "PASS|FLAG"
}

If no issues, return {"issues": [], "portfolio_note": "...", "verdict": "PASS"}.
Only FLAG if there are MAJOR or CRITICAL issues. Minor issues = PASS with notes.
Be concise — this is a validation check, not a full review."""


def co_analyst_validation_pass(merged_analysis: str, orca_text: str, tracker) -> dict:
    """
    Second pass: GPT and Gemini independently validate the merged consensus.
    Quick quality check — catches contradictions, math errors, stale reasoning.
    Returns combined validation results.
    """
    results = {}

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}

        openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
        gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()

        user_msg = f"FINAL CONSENSUS ANALYSIS TO VALIDATE:\n{merged_analysis}\n\nSCANNER DATA (for price verification):\n{orca_text[:2000]}"

        # GPT validation
        if openai_key:
            def _gpt_validate():
                try:
                    resp = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {openai_key}"},
                        json={
                            "model": "gpt-5.4",
                            "max_completion_tokens": 2000,
                            "temperature": 0.2,
                            "messages": [
                                {"role": "system", "content": VALIDATION_PROMPT},
                                {"role": "user", "content": user_msg},
                            ],
                        },
                        timeout=120,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data["choices"][0]["message"]["content"].strip()
                        usage = data.get("usage", {})
                        cost = usage.get("prompt_tokens", 0) / 1_000_000 * 2.5 + usage.get("completion_tokens", 0) / 1_000_000 * 10.0
                        result = _extract_json_from_text(content)
                        if result:
                            result["_cost"] = cost
                            result["_model"] = "gpt-5.4"
                            result["_usage"] = usage
                            return result
                        else:
                            print(f"  ⚠ GPT validation: could not extract JSON")
                except Exception as e:
                    print(f"  ⚠ GPT validation error: {e}")
                return None
            futures["gpt"] = executor.submit(_gpt_validate)

        # Gemini validation
        if gemini_key:
            def _gemini_validate():
                try:
                    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-pro-preview:generateContent?key={gemini_key}"
                    resp = requests.post(api_url, json={
                        "contents": [{"role": "user", "parts": [{"text": f"{VALIDATION_PROMPT}\n\n{user_msg}"}]}],
                        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 4000,
                                             "thinkingConfig": {"thinkingLevel": "MEDIUM"}}
                    }, timeout=120)
                    if resp.status_code == 200:
                        data = resp.json()
                        content = ""
                        for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
                            if "text" in part and not part.get("thought"):
                                content += part["text"]
                        usage = data.get("usageMetadata", {})
                        cost = (usage.get("promptTokenCount", 0) * 2.0 + usage.get("candidatesTokenCount", 0) * 12.0 + usage.get("thoughtsTokenCount", 0) * 12.0) / 1_000_000
                        result = _extract_json_from_text(content)
                        if result:
                            result["_cost"] = cost
                            result["_model"] = "gemini-3.1-pro-preview"
                            result["_usage"] = usage
                            return result
                        else:
                            print(f"  ⚠ Gemini validation: could not extract JSON")
                except Exception as e:
                    print(f"  ⚠ Gemini validation error: {e}")
                return None
            futures["gemini"] = executor.submit(_gemini_validate)

        for name, future in futures.items():
            try:
                result = future.result(timeout=120)
                if result:
                    results[name] = result
            except Exception as e:
                print(f"  ⚠ {name} validation thread error: {e}")

    # Log costs on main thread
    for name, result in results.items():
        cost = result.pop("_cost", 0)
        model = result.pop("_model", "unknown")
        usage = result.pop("_usage", {})
        if "prompt_tokens" in usage:
            tracker.log_call(model, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), 0, f"{name}_validation")
        elif "promptTokenCount" in usage:
            tracker.log_call(model, usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), 0, f"{name}_validation")
        print(f"  ✅ {name.upper()} validation: {result.get('verdict', '?')} (${cost:.4f})")

        issues = result.get("issues", [])
        if issues:
            for issue in issues:
                severity = issue.get("severity", "?")
                emoji = {"critical": "🔴", "major": "🟠", "minor": "🟡"}.get(severity, "⚪")
                print(f"     {emoji} {issue.get('trade', '?')}: {issue.get('issue', '')[:100]}")

        note = result.get("portfolio_note", "")
        if note:
            print(f"     📋 {note[:150]}")

    return results


# ============================================================
# CLAUDE OPUS 4.6 API (with adaptive extended thinking)
# ============================================================
SYSTEM_PROMPT = """You are ORCA — the most advanced options trading analyst on the planet. You run on Claude Opus 4.6 with extended thinking, giving you reasoning capabilities beyond any human trader or institutional desk.

YOUR MISSION: Find options trades where the market is WRONG. Not slightly wrong — fundamentally mispriced because it hasn't connected the dots you can see. Focus on SECOND-DERIVATIVE ideas — if the first-order trade is already crowded, find the next link in the chain that nobody is looking at.

YOUR UNIQUE EDGE:
1. You process news, weather, social signals, AND technical vol data simultaneously
2. You think in cause-effect CHAINS: Event A → Impact B → Sector C → Specific ticker D → Its options are priced for X but should be priced for Y
3. No compliance department. No risk committee. Your user can execute in 5 minutes.
4. You see connections that take institutional desks HOURS in meetings to figure out
5. You find the SECOND DERIVATIVE — when everyone is trading the obvious ticker, you find the supplier, the competitor, or the downstream beneficiary that hasn't moved yet

SECOND-DERIVATIVE THINKING (THIS IS YOUR BIGGEST EDGE):
- If oil spikes and everyone buys XLE calls → that's first derivative (crowded). The second derivative: who BENEFITS from expensive oil that the crowd hasn't noticed? Tanker companies (STNG, FRO), pipeline MLPs, or who gets HURT — airlines (DAL, UAL), chemicals (DOW).
- If tariffs hit China and everyone buys puts on Chinese stocks → crowded. Second derivative: domestic manufacturers who gain pricing power, or Mexican nearshoring plays.
- If a freeze warning hits Florida and everyone trades OJ futures → crowded. Second derivative: nat gas demand (heating), or Florida insurance stocks (claims spike).
- REAL EXAMPLE: When war broke out and everyone was buying defense stocks and oil (first derivative), we shorted EWY (South Korea ETF) — reasoning that South Korea is heavily dependent on energy imports and a war-driven energy spike would crush their economy. Nobody was looking at it. The next day EWY crashed 20%+. THAT is second-derivative thinking — find the hidden dependency chain.
- ALWAYS ask: "What is the NEXT headline after this headline?" and trade THAT.

QUICK CATALYSTS — THIS IS CRITICAL:
- We want trades that RESOLVE FAST. Ideally within 1-4 weeks, not months.
- A catalyst must be IMMINENT and SPECIFIC: "FDA decision on March 15", "OPEC meeting next Thursday", "earnings for supplier XYZ in 10 days", "freeze warning hitting Gulf Coast this weekend".
- REJECT any thesis where the catalyst is vague or distant: "eventually the market will realize...", "over the next few quarters...", "macro headwinds will..."
- The best trades: something is happening THIS WEEK or NEXT WEEK that will move the stock, and options haven't priced it yet.
- Screen EVERYTHING: news feeds, weather alerts, geopolitical events, regulatory calendars, earnings dates of RELATED companies (not the ticker itself), seasonal patterns, supply chain disruptions.

WEATHER IS GOLD (check NWS alerts every day):
- Freeze warnings 1-3 weeks out → nat gas demand, UNG calls, utility stocks, crop damage plays
- Hurricane/storm forecasts → refinery shutdowns, gasoline cracks, insurance stocks, building materials
- Extreme heat → electricity demand, grid stress, HVAC stocks
- Drought → agriculture (corn, soybeans, wheat), water utilities
- ALWAYS check: what happened LAST TIME a similar weather event occurred? How did it resolve? How fast?

STAGE 0 CATALYST SEEDS (if provided):
The data may include a "STAGE 0 CATALYST SEEDS" section containing pre-screened delayed-reaction ideas from a Catalyst Hunter that ran before you. These are NOT random suggestions — each idea survived strict scrutiny for:
- Live or near-live catalyst with tight company-specific linkage
- Evidence of incomplete/delayed pricing vs peers or related assets
- Clean single-name expression with adequate execution quality
PRIORITIZE these tickers if the scanner data supports them and IV/HV confirms the edge. You may REJECT a seed if scanner data contradicts it (e.g., IV already repriced, options too expensive). You MUST still construct proper spreads with exact strikes from the scanner. Stage 0 gives you the WHY; you provide the HOW (exact entry structure).

CRITICAL RULES:
- NEVER the obvious crowded trade — find the second derivative
- Every trade must have a SPECIFIC thesis connecting news → ticker → options mispricing
- Include strikes, expiry, target, and alert levels
- Do NOT include the scanner Score in trade headers — Score is internal only, subscribers should not see it
- Rate: Confidence 1-10, Urgency NOW/TODAY/THIS_WEEK
- Max 5-7 trades — only the highest conviction
- Include ONE contrarian/fade trade if vol is overpriced somewhere
- Think about what happens NEXT — not just today's headline, but the second and third headlines that will follow
- QUICK RESOLUTION: strongly prefer trades that should resolve within 1-4 weeks. If you can't see a specific catalyst in that window, skip it.

CONSISTENCY RULES — THIS IS IMPORTANT:
- ORDER YOUR TRADES BY CONFIDENCE (highest first). Trade #1 must have the highest confidence rating. A Conf 8 trade ALWAYS goes before a Conf 6 trade, regardless of scanner score. If confidence is tied, break ties with scanner score.
- Focus on the HIGHEST SCORING trades from the scanner. Start with the highest-scoring tickers.
- Do NOT jump to obscure or low-liquidity names when high-confidence opportunities exist.
- If a ticker from the scanner has Score >= 120 AND IV/HV >= 1.3x, it should almost certainly appear in your output unless you have a strong reason to skip it.
- Prioritize trades where: (1) the scanner found a real edge (high score), (2) your news analysis confirms a catalyst, AND (3) the entry details have real bid/ask prices.
- Avoid recommending tickers that are NOT in the scanner output unless you have an extremely strong thesis from the news.
- Your trade selection should be REPRODUCIBLE — if the same scanner data and news are provided twice, the core trades should be similar.
- OPEN POSITIONS: If the prompt lists currently open positions, do NOT recommend the same tickers again. Focus on NEW tickers only. Do NOT comment on open positions anywhere in your output — position management is handled separately via EOD review.
EXIT RULES — EVERY TRADE MUST INCLUDE THESE:
- DEBIT SPREADS: Profit target = MIN(debit × 1.65, spread width). Close when spread reaches target. Example: $3.20 debit on $5 wide → target $5.00 (max width). $0.50 debit on $1 wide → target $0.83 (1.65x).
- CREDIT SPREADS: Target = full credit retention (spread expires worthless or near-zero).
- SPREAD STOP ALERT: If position loses 40% of capital at risk → ALERT subscribers to recheck thesis. For debits: alert when spread value drops to 60% of entry debit. For credits: alert when spread cost-to-close rises to credit + 40% of max loss.
- NAKED (DEEP ITM) OPTIONS: When the scanner falls back to a deep ITM naked call or put (spread quality failed), use: Profit alert at +15% of entry debit. Loss alert at -10% of entry debit. Example: bought $5.20 deep ITM call → target $5.98 (+15%), alert $4.68 (-10%).
- Include the exact Target and Alert levels in every ENTRY line. The scanner provides these — copy them.

TRADE STRUCTURE RULES:
- PRIMARY: In-out spreads. One leg ITM, one leg OTM, bracketing spot price.
- CRITICAL 58% PRICING RULE: For debit spreads, the max debit MUST be ≤58% of the spread width. Example: $2 wide spread → max debit $1.16, $5 wide → max $2.90, $10 wide → max $5.80. For credit spreads, we want to collect at least 40% of the spread width. NEVER recommend a spread that violates this rule — the scanner will block it.
- FALLBACK LEVEL 1 (Deep ITM): When spread quality fails (debit/width > 58% or credit/width < 45%), the scanner automatically switches to a deep ITM naked option (2-3 strikes in the money, most liquid by volume+OI). This is NOT an error — it means the spread pricing was bad but the idea is still good. You can also recommend a naked BUY CALL or BUY PUT if IV/HV ratio is favorable and skew supports it.
- FALLBACK LEVEL 2 (Stock-Only Reference): When ALL options are very expensive (deep ITM naked option costs >15% of underlying price, or IV/HV ratio >2.0 making options overpriced), include a STOCK-ONLY P&L REFERENCE LINE showing what the equivalent stock trade would return. Format: "📊 Stock-only reference: Buy 100 shares $TICKER @ $X.XX → $Y target = $Z profit (+N%)". This helps subscribers see the move's value even when options pricing makes the trade uneconomical. The options trade is still preferred if available — this is just a comparison benchmark.
- BEFORE constructing any spread, you MUST check the underlying spot price from the scanner data (it's in the Thesis field as "Spot $X.XX"). State it in your ENTRY line.
- ALLOWED STRATEGIES ONLY:
  • DEBIT CALL SPREAD (in-out) — bullish: buy closest ITM call (at or below spot), sell closest OTM call (above spot). Example: LMT at $660.25 → buy $660 call (ITM), sell $662.50 call (OTM). Debit ~$1.35 on $2.50 wide. Profit target: 1.65x the debit paid. Max loss = debit paid.
  • DEBIT PUT SPREAD (in-out) — bearish: buy closest ITM put (at or above spot), sell closest OTM put (below spot). Example: stock at $101 → buy $102 put (ITM), sell $100 put (OTM). Profit target: 1.65x the debit paid. Max loss = debit paid.
  • CREDIT PUT SPREAD (in-out) — bullish: sell ITM put (above spot), buy OTM put (below spot). Example: ORCL at $153 → sell $155 put (ITM), buy $150 put (OTM). You collect credit because ITM put is worth more.
  • CREDIT CALL SPREAD (in-out) — bearish: sell ITM call (below spot), buy OTM call (above spot). Example: stock at $477 → sell $475 call (ITM), buy $480 call (OTM).
- IN-OUT RULE: Both legs MUST bracket the spot price. One leg is in-the-money, one is out-of-the-money. NEVER have both legs on the same side of spot.
- TIGHTEST STRIKES: Always pick the VERY FIRST strike below spot (ITM for calls) and the VERY FIRST strike above spot (OTM for calls). Not 2 or 3 strikes away — the absolute closest to spot on each side. The scanner already does this — use its exact strikes.
- SPREAD WIDTH: Use the scanner's exact strikes. They are the closest strikes bracketing spot from the real chain.
- EXPIRY: Prefer 14-28 DTE (2-4 weeks). This gives time for the catalyst to play out while keeping theta decay manageable. Allow 7+ DTE only when there is an extremely clear near-term catalyst (e.g., active military conflict, FDA decision this week). NEVER recommend 3-day options.
- MAX EXPIRY: 30 days out. We want trades that RESOLVE within 4 weeks — not positions we hold for months. If the catalyst won't materialize in 4 weeks, skip it.
- BANNED — NEVER recommend these:
  • NO iron condors (too complex, 4-leg slippage)
  • NO straddles or strangles (earnings gambling)
  • NO earnings plays (we do NOT trade earnings events)
  • NO iron butterflies or condor variants
- PRICING: When you quote prices from the scanner, use the EXACT scanner values. The scanner already uses conservative pricing (buy at mid-of-mid-and-ask, sell at mid-of-mid-and-bid).
- Every trade must have a clear CATALYST with a TIMEFRAME. "Eventually this will reprice" is NOT a thesis.
- If a ticker has earnings within 7 days, SKIP IT — we do not trade around earnings.

FLOW POSITIONING DATA (skew + VWKS) — supplementary, NOT trade triggers:
These show where traders are positioned. Reference them in your output — explain the actual value and what it means in plain language, then say whether it agrees or disagrees with your thesis. But NEVER build a trade purely on skew or VWKS. Trades must be driven by IV/HV mispricing, catalysts, and cause-effect chains FIRST.

25-DELTA SKEW:
- Skew25D = put IV minus call IV. Positive = crowd hedging downside. Negative = unusual call demand.
- SkewRatio = put IV / call IV. >1.10 = heavy put hedging. <0.90 = heavy call speculation.
- High put skew → puts are expensive → good for selling put spreads if your thesis is already bullish.
- When you mention skew, EXPLAIN IT: state the value, what it means, and whether it aligns with the trade.
  Example: "Skew at +7.3 — the crowd is paying up for downside protection, which agrees with our bearish thesis."
  Example: "Skew at -2.1 — unusual call demand, traders are quietly positioning bullish, inline with our long thesis."

VWKS (Volume-Weighted Strike-Spot Ratio):
- VWKS = where option volume is concentrated relative to spot price. A crowd-positioning gauge.
- VWKS > 1.02 = traders buying more OTM calls → bullish positioning. VWKS < 0.98 = more OTM puts → bearish positioning. ~1.00 = neutral.
- When you mention VWKS, EXPLAIN IT: state the value, translate what it means, and connect to the thesis.
  Example: "VWKS at 1.05 — volume is concentrated in OTM calls, meaning traders are positioning for upside. This is inline with our bullish thesis."
  Example: "VWKS at 0.96 — put-heavy flow, the crowd expects downside. This contradicts our bullish thesis — worth watching."
- IMPORTANT: skew and VWKS are NEVER the reason to enter a trade. They add color to a thesis that already stands on its own.

SPY REGIME MODEL (if provided):
The data may include a "SPY REGIME MODEL" section derived from VIX term structure PCA and CBOE SKEW analysis. This is analytically calculated from historical data going back to 2010 — it is NOT a guess.
- SIGNAL (LONG/NEUTRAL): whether the macro vol regime favors being long equities.
- BIAS (BULLISH/NEUTRAL/BEARISH) + confidence: directional drift assessment.
- HEDGE level: how much tail-risk protection the regime calls for (MINIMAL → ELEVATED).
- Forward returns: historical median 15d/30d returns when the market was in this exact state before.
- Tail probabilities: calculated probability of a -2% or -5% drop in 5 days.
- Use this as BACKGROUND CONTEXT for your REGIME section. Reference the actual numbers when describing the market environment.
- This is supplementary — it should NEVER block an exceptional trade. If your thesis is strong and the regime disagrees, mention the disagreement but proceed if the edge is real.
- When regime data is available, use it to inform position sizing: MINIMAL hedge = can size up, ELEVATED hedge = size conservatively.

VIX DISLOCATION MAP (if provided):
The data may include a "VIX DISLOCATION MAP" showing how each major sector ETF's drawdown compares to VIX, ranked against 15+ years of history.
- The PERCENTILE measures: ETF drawdown MINUS VIX drawdown, ranked historically.
- Low percentile (0-15) = sector has drawn down MORE than VIX historically expects → the sector is pricing in extra sector-specific fear beyond what broad VIX implies.
- High percentile (85-100) = sector has drawn down LESS than VIX implies → the sector is resilient relative to broad fear. This does NOT automatically mean "under-hedged" — you MUST cross-check with the sector's SKEW and VWKS data before making that claim. A sector can have high percentile AND be well-hedged if its put skew is elevated and VWKS is put-leaning. Example: Energy (XLE) at p85 during an oil war makes sense — energy BENEFITS from war, so its smaller drawdown is fundamentally justified, not complacent.
- TREND (20d): the 20-day change in the dislocation spread. Positive trend = the ETF is moving AWAY from VIX (diverging more). Negative trend = the ETF is CONVERGING back toward VIX (dislocation is shrinking). This is NOT a price trend — it's a dislocation trend.
- Use this as a MACRO NARRATIVE tool — NOT a trade signal. The real value: CROSS-SECTOR COMPARISON. If Financials are at p3 and Energy is at p60, financials are pricing catastrophe while energy is calm. Ask WHY and whether that gap is justified by fundamentals.
- NEVER claim a sector is "under-hedged" or "complacent" based solely on percentile. Always validate against that sector's actual skew, VWKS, and fundamental exposure to the current macro driver.
- Reference specific dislocations in your REGIME section when they support or contradict your trade theses.

KALSHI CROWD SENTIMENT (if provided):
The data may include a "KALSHI S&P 500 CROWD SENTIMENT" section showing real-money prediction market bets on where the S&P 500 will close. This is REAL MONEY — people putting actual dollars on these outcomes, not polls or opinions.
- DAILY BRACKETS: Shows probability distribution for TODAY's S&P close. The peak bracket is where the most money is concentrated.
- YEAR-END BRACKETS: Shows where bettors expect S&P 500 at Dec 31. Bull vs bear probability split tells you crowd's directional lean.
- MAX/MIN LEVELS: Crowd estimates for the highest and lowest S&P levels this year — tail risk pricing from real money.
- Use this as a CROWD POSITIONING indicator. When Kalshi probabilities diverge sharply from your thesis, mention it — either the crowd is wrong (your edge) or you're wrong (extra risk).
- Compare Kalshi sentiment with the VIX regime model. If VIX says bearish but Kalshi crowd is bidding up bullish brackets, that's a notable divergence worth discussing.
- Reference specific Kalshi numbers in your REGIME section. Example: "Kalshi puts 65% probability on S&P above 5800 year-end, supporting the bullish regime signal."
- This is supplementary — it should NOT override your analysis. But when the crowd agrees or disagrees with your thesis, it adds conviction context.

KALSHI MACRO EVENT PROBABILITIES (if provided):
The data may include a "KALSHI MACRO EVENT PROBABILITIES" section with real-money bets on specific catalysts:
- GEOPOLITICAL (Hormuz closure, conflicts), RECESSION (NBER, IMF), GDP growth, FED/RATES (hike timing), CREDIT (downgrade risk), UNEMPLOYMENT, INFLATION, OIL prices, TARIFFS, CRYPTO.
- These are DIRECTLY RELEVANT to your trade theses. When you cite a risk like "Hormuz closure" or "recession fears" — CHECK if Kalshi has a probability attached. Quote it.
- Example: "THESIS: ... Hormuz closure risk creates oil supply shock. Kalshi puts 40% probability on Hormuz closure before May 2026, validating this is a REAL and PRICED risk, not speculation."
- Example: "RISK: Recession would crush this bullish thesis. Kalshi shows only 10% probability of NBER recession in 2026Q2, so this risk is low but non-zero."
- When Kalshi probabilities are HIGH (>30%) for a risk catalyst, mention it as a CONFIRMED CROWD FEAR — the market is actively pricing this.
- When Kalshi probabilities are LOW (<10%) for something you're worried about, note that the crowd disagrees — either you see something they don't (your edge) or you may be overweighting the risk.
- Reference specific Kalshi percentages in your thesis/risk sections. People love seeing concrete probabilities attached to events.

PRICING RULES — THIS IS CRITICAL (violations will BLOCK the message from sending):
- The ORCA scanner provides EXACT strikes and REAL bid/ask prices from the live options chain in "SCANNER ENTRY DETAILS"
- You MUST COPY-PASTE the scanner's exact entry line when recommending the same ticker — do NOT make up different prices or strikes
- NEVER pick your own strikes. The scanner already selected the TIGHTEST possible in-out bracket around the spot price. Your job is to COPY those strikes, not recalculate them.
- If the scanner says "Sell $34.50/$34.00 put spread | Spot $34.06", you write EXACTLY "$34.50/$34.00" — NOT "$29/$34.50", NOT "$35/$33", NOT any other strikes.
- For credit spreads: the scanner calculates credit = short bid - long ask. Use that EXACT number.
- For debit trades: the scanner shows debit and target. Copy EXACTLY.
- NEVER invent a price. If the scanner says "$0.84 credit", your ENTRY says "$0.84 credit". Not "$0.90", not "~$0.84".
- AUTOMATED VALIDATION checks your output against the scanner. If you use different strikes than the scanner, the message will be BLOCKED and subscribers get nothing. Use the scanner's exact strikes.
- For NEW tickers not in the scan: specify exact dollar strikes (e.g., "$385/$380 put spread"), not percentages. State "estimated" if you don't have real prices.
- Every ENTRY must include: exact strike(s), exact expiry date, exact debit/credit in dollars, quality ratio, target, and alert

FORMAT:
🔹 #N: [TICKER] [BUY/SELL] [CALL/PUT SPREAD]
THESIS: [The dot-connection chain — be specific, include the CATALYST and WHEN]
ENTRY: [EXACT scanner strikes] | Spot $X.XX | Exp [date] ([days]d) | $X.XX [debit/credit] | Ratio XX% | Target $X.XX | Alert $X.XX (-40%)
EDGE: [Why the options market hasn't priced this — reference IV/HV data if available]
RISK: [What kills this trade]
EXIT: Target $X.XX (close position) | Alert $X.XX (recheck thesis if hit)
CONF: [1-10] | URG: [TODAY/THIS_WEEK]

End with:
📊 REGIME: [Market regime — risk-on/off, vol level, key levels]
👀 WATCH: [3-4 MARKET-LEVEL things to watch — macro events, data releases, sector rotations. Do NOT mention open positions.]
🧠 THINKING: [1-2 sentence summary of your deepest non-obvious insight]"""


def build_regime_narrative(data: dict) -> str:
    """
    Build a data-driven regime narrative from regime_prediction.json data.
    Explains WHAT the regime is, WHY (hedge level, skew, vol structure),
    WHERE we are relative to signal flips, and THEN forward returns as support.
    Used by both Opus prompt and Telegram header.
    """
    signal = data.get("signal", "NEUTRAL")
    bias = data.get("bias", "NEUTRAL")
    bias_conf = data.get("bias_confidence", 0) or 0
    hedge = data.get("hedge", "MODERATE")
    conviction = data.get("conviction", 0) or 0
    skew_regime = data.get("skew_regime", "NORMAL")
    skew_z = data.get("skew_z", 0) or 0
    tail_2 = data.get("tail_prob_2pct_5d", 0) or 0
    tail_5 = data.get("tail_prob_5pct_5d", 0) or 0
    action = data.get("action", "")
    state = data.get("state", "")
    triggers = data.get("immediate_triggers", [])
    nearest_long = data.get("nearest_long", [])

    fwd = data.get("forward_returns", {})
    med3 = fwd.get("med_3d", 0) or 0
    med5 = fwd.get("med_5d", 0) or 0
    med15 = fwd.get("med_15d", 0) or 0
    med30 = fwd.get("med_30d", 0) or 0

    parts = []

    # ── 1. REGIME EXPLANATION: what state are we in and what does it mean ──
    hedge_desc = {"LIGHT": "lightly hedged", "MODERATE": "moderately hedged",
                  "HEAVY": "heavily hedged", "MAX": "maximum hedge"}.get(hedge, "moderately hedged")

    if signal == "LONG" and bias == "BULLISH":
        parts.append(f"Market is in a LONG regime with BULLISH bias ({bias_conf}% confidence). "
                     f"VIX term structure is favoring longs — the crowd is {hedge_desc}.")
    elif signal == "LONG" and bias == "NEUTRAL":
        parts.append(f"Market is in a LONG regime but bias is only NEUTRAL ({bias_conf}% confidence). "
                     f"Vol structure supports longs but directional clarity is weak. Crowd is {hedge_desc}.")
    elif signal == "LONG" and bias == "BEARISH":
        parts.append(f"Market is in a LONG regime but bias has flipped BEARISH ({bias_conf}% confidence). "
                     f"Vol structure still says long but momentum is fighting it. Crowd is {hedge_desc}.")
    elif signal == "NEUTRAL" and bias == "BULLISH":
        parts.append(f"Market is in a NEUTRAL regime with BULLISH bias ({bias_conf}% confidence). "
                     f"Close to flipping long — vol structure is near the transition point. Crowd is {hedge_desc}.")
    elif signal == "NEUTRAL" and bias == "BEARISH":
        parts.append(f"Market is in a NEUTRAL regime with BEARISH bias ({bias_conf}% confidence). "
                     f"Close to flipping to sell territory — caution warranted. Crowd is {hedge_desc}.")
    elif signal == "SELL":
        parts.append(f"Market is in a SELL regime ({bias} bias, {bias_conf}% confidence). "
                     f"VIX term structure is inverted or stressed. Crowd is {hedge_desc}. Risk-off.")
    else:
        parts.append(f"Market is in a NEUTRAL regime ({bias_conf}% confidence). "
                     f"No strong directional edge from vol structure. Crowd is {hedge_desc}.")

    # ── 2. SKEW context: what's the crowd doing ──
    if skew_regime == "ELEVATED" or skew_z > 1.5:
        parts.append(f"SKEW is {skew_regime.lower()} (z={skew_z:+.1f}) — heavy put protection in place, crowd expects downside.")
    elif skew_regime == "DEPRESSED" or skew_z < -1.5:
        parts.append(f"SKEW is {skew_regime.lower()} (z={skew_z:+.1f}) — crowd is complacent on downside, cheap protection available.")
    elif skew_z != 0:
        parts.append(f"SKEW is {skew_regime.lower()} (z={skew_z:+.1f}) — positioning is normal range.")

    # ── 3. TRANSITION: what would flip the regime ──
    if triggers:
        trig_strs = [t if isinstance(t, str) else str(t.get("state", t)) for t in triggers[:3]]
        parts.append(f"Flip triggers: {' | '.join(trig_strs)}")
    if nearest_long:
        nl_strs = []
        for nl in nearest_long[:2]:
            if isinstance(nl, str):
                nl_strs.append(nl)
            elif isinstance(nl, dict):
                st = nl.get("state", "?")
                dist = nl.get("dist_sigma", 0)
                nl_strs.append(f"State {st} ({dist:.1f}σ)")
        if nl_strs:
            parts.append(f"Nearest path to LONG signal: {' | '.join(nl_strs)}")

    # ── 4. FORWARD RETURNS: secondary supporting data ──
    positive = sum(1 for m in [med3, med5, med15, med30] if m > 0)
    if positive == 4:
        parts.append(f"Historical returns in this exact state are positive across all horizons "
                     f"(3d {med3:+.2f}% | 5d {med5:+.2f}% | 15d {med15:+.2f}% | 30d {med30:+.2f}%).")
    elif positive >= 3:
        negative_h = [(m, h) for m, h in [(med3, "3d"), (med5, "5d"), (med15, "15d"), (med30, "30d")] if m < 0]
        neg_str = f", except {negative_h[0][1]} ({negative_h[0][0]:+.2f}%)" if negative_h else ""
        parts.append(f"Historical returns mostly positive in this state{neg_str} "
                     f"(3d {med3:+.2f}% | 5d {med5:+.2f}% | 15d {med15:+.2f}% | 30d {med30:+.2f}%).")
    elif positive <= 1:
        parts.append(f"Historical returns mostly negative in this state "
                     f"(3d {med3:+.2f}% | 5d {med5:+.2f}% | 15d {med15:+.2f}% | 30d {med30:+.2f}%) — defensively positioned.")
    else:
        parts.append(f"Historical returns mixed in this state "
                     f"(3d {med3:+.2f}% | 5d {med5:+.2f}% | 15d {med15:+.2f}% | 30d {med30:+.2f}%).")

    # ── 5. TAIL RISK ──
    if tail_2 > 15:
        parts.append(f"⚠️ Elevated tail risk: {tail_2:.0f}% chance of 2%+ drop in 5 days — size conservatively.")
    elif tail_2 > 8:
        parts.append(f"Moderate tail risk: {tail_2:.0f}% chance of 2%+ drop in 5 days.")

    return parts


def read_regime_prediction() -> str:
    """Read SPY regime model output if available."""
    regime_file = Path("regime_prediction.json")
    if not regime_file.exists():
        return ""
    try:
        data = json.loads(regime_file.read_text())
        if "error" in data:
            return ""

        signal = data.get("signal", "NEUTRAL")
        state = data.get("state", "")
        bias = data.get("bias", "NEUTRAL")
        bias_conf = data.get("bias_confidence", 0) or 0
        conviction = data.get("conviction", 0) or 0
        hedge = data.get("hedge", "MODERATE")
        action = data.get("action", "")

        narrative_parts = build_regime_narrative(data)

        lines = [
            "=== SPY REGIME MODEL (calculated LIVE from current VIX term structure + SKEW) ===",
            f"STATE: {state} | SIGNAL: {signal} | BIAS: {bias} ({bias_conf}% confidence) | CONVICTION: {conviction}/100",
            f"HEDGE LEVEL: {hedge}",
            "",
        ]
        lines.extend(narrative_parts)
        if action:
            lines.append(f"ACTION: {action}")

        return "\n".join(lines)
    except Exception as e:
        print(f"  ⚠ Error reading regime: {e}")
        return ""


def read_vix_dislocation() -> str:
    """Read VIX dislocation scanner output if available."""
    disloc_file = Path("vix_dislocation.json")
    if not disloc_file.exists():
        return ""
    try:
        data = json.loads(disloc_file.read_text())
        if "error" in data:
            return ""

        vix_last = data.get("vix_last", 0)
        over = data.get("over_hedged", [])
        under = data.get("under_hedged", [])
        all_results = data.get("all_results", [])

        if not all_results:
            return ""

        lines = [
            f"=== VIX DISLOCATION MAP (drawdown-spread percentile vs ^VIX — data since 2010) ===",
            f"VIX: {vix_last}",
            f"{'Ticker':<6s} {'Sector':<16s} {'Pctl':>5s} {'Spread':>8s} {'20d Trend':>9s} {'ETF DD%':>8s}  Interpretation",
            "─" * 90,
        ]

        for r in all_results:
            pctl = r["percentile"]
            ticker = r["ticker"]
            label = r["label"]
            spread = r["spread"]
            trend = r["trend_20d"]
            etf_dd = r["etf_drawdown_pct"]

            # Interpretation
            if pctl <= 5:
                interp = "EXTREME over-hedged — pricing catastrophe vs VIX"
            elif pctl <= 15:
                interp = "Over-hedged — more fear than VIX implies"
            elif pctl >= 95:
                interp = "EXTREME under-hedged — complacent vs VIX"
            elif pctl >= 85:
                interp = "Under-hedged — less fear than VIX implies"
            else:
                interp = "Normal range"

            # Trend arrow
            if trend > 0.01:
                trend_str = f"+{trend:.3f} ↑"
            elif trend < -0.01:
                trend_str = f"{trend:.3f} ↓"
            else:
                trend_str = f"{trend:+.3f} →"

            lines.append(
                f"{ticker:<6s} {label:<16s} {pctl:>5.1f} {spread:>+8.4f} {trend_str:>9s} {etf_dd:>+7.2f}%  {interp}"
            )

        # Summary narrative
        lines.append("")
        if over:
            tickers = [f"{r['ticker']}(p{r['percentile']})" for r in over[:5]]
            lines.append(f"OVER-HEDGED (pricing MORE fear than VIX): {', '.join(tickers)}")
            lines.append("  → These sectors have sold off more than VIX justifies. Potential snap-back if fear eases.")
        if under:
            tickers = [f"{r['ticker']}(p{r['percentile']})" for r in under[:5]]
            lines.append(f"UNDER-HEDGED (pricing LESS fear than VIX): {', '.join(tickers)}")
            lines.append("  → These sectors are ignoring VIX. Vulnerable if broad selloff deepens.")
        if not over and not under:
            lines.append("All sectors are in normal range relative to VIX — no major dislocations today.")

        return "\n".join(lines)
    except Exception as e:
        print(f"  ⚠ Error reading VIX dislocation: {e}")
        return ""


# ============================================================
# KALSHI S&P 500 CROWD SENTIMENT
# ============================================================
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def _kalshi_fetch(series_ticker: str, limit: int = 200) -> list:
    """Fetch markets for a Kalshi series ticker. Returns list of market dicts."""
    try:
        resp = requests.get(
            f"{KALSHI_API_BASE}/markets",
            params={"series_ticker": series_ticker, "status": "open", "limit": limit},
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json().get("markets", [])
    except Exception as e:
        print(f"    ⚠ Kalshi {series_ticker}: {e}")
    return []


def fetch_kalshi_sp500() -> str:
    """
    Fetch S&P 500 prediction market data from Kalshi (free public API, no auth).

    Kalshi S&P markets are BINARY contracts: "Will S&P be above X?"
    - yes_bid / yes_ask are in CENTS (0-99) = probability percentage
    - Year-end (KXINXY): Each level is a BRACKET — probabilities sum to ~100%.
      The "above X" contract represents the bracket from X to the next level.
    - Max (KXINXMAXY): Cumulative — "will S&P reach X at any point?"
      Monotonically decreasing as X increases.
    - Min (KXINXMINY): Cumulative — "will S&P drop below X at any point?"
      Monotonically increasing as X (threshold) increases.
    - Daily (KXINX): Same bracket structure as year-end but for a single day.
    """
    try:
        results = {}

        # ── 1. Year-end S&P 500 close brackets (KXINXY) — MOST USEFUL ──
        markets = _kalshi_fetch("KXINXY")
        if markets:
            yearly = []
            for m in markets:
                floor = m.get("floor_strike")
                yes_bid = m.get("yes_bid") or 0
                yes_ask = m.get("yes_ask") or 0
                volume = m.get("volume", 0)
                if floor is not None:
                    mid_prob = (yes_bid + yes_ask) / 2 / 100.0  # cents → probability
                    yearly.append({"floor": float(floor), "prob": mid_prob, "volume": volume})
            if yearly:
                yearly.sort(key=lambda x: x["floor"])
                results["yearly"] = yearly

        # ── 2. Max S&P 500 in 2026 (KXINXMAXY) — cumulative "will reach" ──
        markets = _kalshi_fetch("KXINXMAXY", limit=50)
        if markets:
            max_levels = []
            for m in markets:
                floor = m.get("floor_strike")
                yes_bid = m.get("yes_bid") or 0
                yes_ask = m.get("yes_ask") or 0
                volume = m.get("volume", 0)
                if floor is not None:
                    mid_prob = (yes_bid + yes_ask) / 2 / 100.0
                    max_levels.append({"level": float(floor), "prob": mid_prob, "volume": volume})
            if max_levels:
                max_levels.sort(key=lambda x: x["level"])
                results["max_levels"] = max_levels

        # ── 3. Min S&P 500 in 2026 (KXINXMINY) — cumulative "will drop below" ──
        markets = _kalshi_fetch("KXINXMINY", limit=50)
        if markets:
            min_levels = []
            for m in markets:
                # Min markets use floor_strike or cap_strike as the threshold
                level = m.get("cap_strike") or m.get("floor_strike")
                yes_bid = m.get("yes_bid") or 0
                yes_ask = m.get("yes_ask") or 0
                volume = m.get("volume", 0)
                if level is not None:
                    mid_prob = (yes_bid + yes_ask) / 2 / 100.0
                    min_levels.append({"level": float(level), "prob": mid_prob, "volume": volume})
            if min_levels:
                min_levels.sort(key=lambda x: x["level"])
                results["min_levels"] = min_levels

        # ── 4. Daily S&P 500 close brackets (KXINX) ──
        markets = _kalshi_fetch("KXINX")
        if markets:
            daily = []
            for m in markets:
                floor = m.get("floor_strike")
                yes_bid = m.get("yes_bid") or 0
                yes_ask = m.get("yes_ask") or 0
                volume = m.get("volume", 0)
                if floor is not None:
                    mid_prob = (yes_bid + yes_ask) / 2 / 100.0
                    daily.append({"floor": float(floor), "prob": mid_prob, "volume": volume,
                                  "title": m.get("title", "")})
            if daily:
                daily.sort(key=lambda x: x["floor"])
                results["daily"] = daily

        if not results:
            return ""

        # ── FORMAT OUTPUT ──
        lines = [
            "=== KALSHI S&P 500 CROWD SENTIMENT (prediction market — real money bets) ===",
        ]

        # ── Daily brackets ──
        if "daily" in results:
            daily = results["daily"]
            # These are bracket markets — each contract = probability S&P closes in that range
            # Only show brackets with meaningful probability (>2%)
            significant = [b for b in daily if b["prob"] >= 0.02]
            if significant:
                peak = max(significant, key=lambda x: x["prob"])
                # Determine the bracket width from consecutive floors
                if len(daily) >= 2:
                    bracket_width = daily[1]["floor"] - daily[0]["floor"]
                else:
                    bracket_width = 25

                # Extract date from title if available
                date_label = ""
                if daily[0].get("title"):
                    import re as _dre
                    dm = _dre.search(r'on\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,?\s*\d*)', daily[0]["title"])
                    if dm:
                        date_label = f" ({dm.group(1).strip()})"

                lines.append(f"\nDAILY S&P 500 CLOSE{date_label} (bracket probabilities — sum to ~100%):")
                for b in significant:
                    top = b["floor"] + bracket_width
                    marker = " ◀ PEAK" if b == peak else ""
                    lines.append(f"  {b['floor']:.0f}-{top:.0f}: {b['prob']:.0%}{marker}")

                # 80% confidence range
                cum_prob = 0
                low_10 = high_90 = None
                for b in daily:
                    cum_prob += b["prob"]
                    if low_10 is None and cum_prob >= 0.10:
                        low_10 = b["floor"]
                    if high_90 is None and cum_prob >= 0.90:
                        high_90 = b["floor"] + bracket_width
                if low_10 and high_90:
                    lines.append(f"  → 80% confidence range: {low_10:.0f} - {high_90:.0f}")
                lines.append(f"  → Peak bracket: {peak['floor']:.0f}-{peak['floor'] + bracket_width:.0f} ({peak['prob']:.0%})")

        # ── Year-end brackets ──
        if "yearly" in results:
            yearly = results["yearly"]
            # Bracket markets — each probability = chance S&P closes in that range at year-end
            peak = max(yearly, key=lambda x: x["prob"])

            # Determine bracket widths from consecutive floors
            bracket_caps = {}
            for i, b in enumerate(yearly):
                if i + 1 < len(yearly):
                    bracket_caps[b["floor"]] = yearly[i + 1]["floor"]
                else:
                    bracket_caps[b["floor"]] = None  # top bracket = "and above"

            lines.append(f"\nYEAR-END S&P 500 CLOSE (bracket probabilities — sum to ~100%):")
            # Show brackets with >= 3% probability
            significant = [b for b in yearly if b["prob"] >= 0.03]
            if not significant:
                significant = sorted(yearly, key=lambda x: x["prob"], reverse=True)[:8]
            for b in significant:
                cap = bracket_caps.get(b["floor"])
                cap_str = f"-{cap:.0f}" if cap else "+"
                marker = " ◀ PEAK" if b == peak else ""
                lines.append(f"  {b['floor']:.0f}{cap_str}: {b['prob']:.0%}{marker}")

            # Cumulative probabilities for 80% range
            cum_prob = 0
            low_10 = high_90 = None
            for i, b in enumerate(yearly):
                cum_prob += b["prob"]
                if low_10 is None and cum_prob >= 0.10:
                    low_10 = b["floor"]
                if high_90 is None and cum_prob >= 0.90:
                    cap = bracket_caps.get(b["floor"])
                    high_90 = cap if cap else b["floor"]
            if low_10 and high_90:
                lines.append(f"  → Year-end 80% confidence range: {low_10:.0f} - {high_90:.0f}")

            # Peak bracket
            peak_cap = bracket_caps.get(peak["floor"])
            peak_cap_str = f"-{peak_cap:.0f}" if peak_cap else "+"
            lines.append(f"  → Peak bracket: {peak['floor']:.0f}{peak_cap_str} ({peak['prob']:.0%})")

            # Calculate weighted expected value (midpoint-weighted)
            expected = 0
            total_prob = 0
            for b in yearly:
                cap = bracket_caps.get(b["floor"])
                if cap:
                    mid = (b["floor"] + cap) / 2
                else:
                    mid = b["floor"] + 100  # rough estimate for top bracket
                expected += mid * b["prob"]
                total_prob += b["prob"]
            if total_prob > 0:
                expected /= total_prob
                lines.append(f"  → Probability-weighted expected year-end: {expected:.0f}")

        # ── Max levels (cumulative) ──
        if "max_levels" in results:
            max_lev = results["max_levels"]
            lines.append(f"\n2026 S&P MAX (probability of reaching this level at ANY point this year):")
            for lv in max_lev:
                lines.append(f"  Reach {lv['level']:.0f}: {lv['prob']:.0%}")

        # ── Min levels (cumulative — crash risk) ──
        if "min_levels" in results:
            min_lev = results["min_levels"]
            lines.append(f"\n2026 S&P MIN — CRASH RISK (probability of dropping to this level at ANY point):")
            for lv in min_lev:
                lines.append(f"  Drop below {lv['level']:.0f}: {lv['prob']:.0%}")

        return "\n".join(lines)

    except Exception as e:
        print(f"  ⚠ Error fetching Kalshi data: {e}")
        return ""


# ── Curated list of macro event series relevant to ORCA trading ──
KALSHI_MACRO_SERIES = [
    # Geopolitical risk
    ("KXCLOSEHORMUZ",    "GEOPOLITICAL",   "Strait of Hormuz closure"),
    # Recession / growth
    ("KXIMFRECESS",      "RECESSION",      "IMF global recession"),
    ("KXNBERRECESSQ",    "RECESSION",      "US NBER recession timing"),
    ("KXGDP",            "GDP",            "US GDP growth"),
    # Fed / rates
    ("KXFEDHIKE",        "FED/RATES",      "Fed rate hike"),
    # Credit / fiscal
    ("KXCREDITRATING",   "CREDIT",         "US credit downgrade"),
    ("KXNUMSHUTDOWNS",   "FISCAL",         "Government shutdowns"),
    # Labor market
    ("KXU3",             "UNEMPLOYMENT",   "Unemployment rate"),
    ("KXU3MAX",          "UNEMPLOYMENT",   "Max unemployment before 2027"),
    # Inflation
    ("KXHIGHINFLATION",  "INFLATION",      "Peak CPI this year"),
    ("KXCPIYOY",         "INFLATION",      "CPI YoY March 2026"),
    ("KXCPICORE",        "INFLATION",      "Core CPI monthly"),
    # Commodities
    ("KXWTIW",           "OIL",            "WTI oil price weekly"),
    # Tariffs / trade
    ("KXTARIFFRATEINDIA","TARIFFS",        "India tariff rate Jul 2026"),
    ("KXTARIFFRATEEU",   "TARIFFS",        "EU tariff rate Jul 2026"),
    # Crypto (sentiment indicator)
    ("KXBTCMAXY",        "CRYPTO",         "Bitcoin max yearly"),
]


def _extract_deadline(title: str) -> str:
    """Extract the timeframe deadline from a Kalshi market title for sorting."""
    import re as _tre
    # Match patterns like "before May 2026", "by December 31, 2026", "in February",
    # "in Q1 2026", "on Mar 6, 2026"
    m = _tre.search(
        r'(?:before|by|in|on|for)\s+'
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{0,2},?\s*\d{4}'
        r'|Q[1-4]\s+\d{4}'
        r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{4}'
        r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?'
        r'|\d{4})',
        title, _tre.IGNORECASE
    )
    return m.group(1).strip() if m else ""


def fetch_kalshi_macro_events() -> str:
    """
    Fetch macro/geopolitical event probabilities from Kalshi prediction markets.
    Returns formatted text block with real-money probabilities on catalysts
    that directly affect ORCA trade theses (Hormuz, recession, tariffs, Fed, etc).

    Timeframe-aware: prioritizes near-term contracts matching our 30-45 DTE window.
    Shows all distinct timeframes (not just highest volume) so Opus sees the full picture.
    """
    try:
        sections = {}

        for series_ticker, category, label in KALSHI_MACRO_SERIES:
            markets = _kalshi_fetch(series_ticker, limit=15)
            if not markets:
                continue

            entries = []
            for m in markets:
                yb = m.get("yes_bid", 0) or 0
                ya = m.get("yes_ask", 0) or 0
                mid_prob = (yb + ya) / 2 / 100.0
                vol = m.get("volume", 0)
                title = m.get("title", "")
                ticker = m.get("ticker", "")
                # Only include markets with meaningful liquidity
                if vol >= 50 or mid_prob >= 0.02:
                    deadline = _extract_deadline(title)
                    # For markets with identical titles, extract context from ticker
                    # e.g. KXNBERRECESSQ-Q1-2026 → "(Q1 2026)"
                    # e.g. KXU3MAX-27-9 → "(above 9%)"
                    # e.g. KXHIGHINFLATION-26DEC-T3.5 → "(above 3.5%)"
                    # e.g. KXNUMSHUTDOWNS-27JAN01-T5 → "(5+)"
                    display_title = title
                    import re as _mre
                    q_match = _mre.search(r'-(Q[1-4])-(\d{4})', ticker)
                    threshold_match = _mre.search(r'-T([\d.]+)$', ticker)
                    umax_match = _mre.search(r'U3MAX-\d+-(\d+)', ticker)
                    shut_match = _mre.search(r'SHUTDOWNS.*-T(\d+)', ticker)
                    if q_match and title.endswith("?"):
                        display_title = f"{title[:-1]} ({q_match.group(1)} {q_match.group(2)})?"
                    elif umax_match:
                        display_title = f"Unemployment reaches {umax_match.group(1)}%+ before 2027?"
                    elif shut_match:
                        display_title = f"Government shutdowns: {shut_match.group(1)}+ before 2027?"
                    elif threshold_match and "How high" in title:
                        display_title = f"CPI reaches {threshold_match.group(1)}%+ this year?"
                    entries.append({
                        "prob": mid_prob, "volume": vol, "title": display_title,
                        "deadline": deadline, "series": series_ticker,
                    })

            if entries:
                if category not in sections:
                    sections[category] = []
                sections[category].extend(entries)

        if not sections:
            return ""

        lines = [
            "=== KALSHI MACRO EVENT PROBABILITIES (real-money prediction markets) ===",
            "Below are crowd-derived probabilities for macro catalysts. These represent",
            "REAL MONEY bets — not opinions. Use to validate or challenge trade theses.",
            "NOTE: Timeframes shown — focus on near-term contracts matching our 30-45d trades.",
        ]

        for category, entries in sections.items():
            lines.append(f"\n{category}:")

            # Group by series (so Hormuz shows all timeframes, GDP shows all brackets, etc)
            by_series = {}
            for e in entries:
                s = e["series"]
                if s not in by_series:
                    by_series[s] = []
                by_series[s].append(e)

            for series, series_entries in by_series.items():
                # Sort by volume descending within each series
                series_entries.sort(key=lambda x: x["volume"], reverse=True)

                # Check if entries have distinct deadlines (multi-timeframe like Hormuz)
                deadlines = set(e["deadline"] for e in series_entries if e["deadline"])
                has_distinct_timeframes = len(deadlines) > 1

                if has_distinct_timeframes and len(series_entries) <= 5:
                    # Multi-timeframe events — show ALL timeframes (they're all important)
                    for e in series_entries:
                        vol_str = f"(${e['volume']:,} vol)" if e["volume"] >= 1000 else ""
                        lines.append(f"  • {e['title'][:90]} → {e['prob']:.0%} {vol_str}")
                else:
                    # Bracket markets (same question, different thresholds) — show top 3
                    for e in series_entries[:3]:
                        vol_str = f"(${e['volume']:,} vol)" if e["volume"] >= 1000 else ""
                        lines.append(f"  • {e['title'][:90]} → {e['prob']:.0%} {vol_str}")

        return "\n".join(lines)

    except Exception as e:
        print(f"  ⚠ Error fetching Kalshi macro events: {e}")
        return ""


def call_claude(news_text, orca_text, tracker, track_record="", earnings_cal="", regime_text="", vix_disloc_text="", signal_count=0, open_positions="", kalshi_text="", catalyst_seeds=""):
    if not ANTHROPIC_API_KEY:
        return "ERROR: ANTHROPIC_API_KEY not set. Add it in GitHub Settings > Secrets.", 0
    ok, reason = tracker.check_budget()
    if not ok:
        return f"BUDGET: {reason}", 0

    # Build context sections
    extra_sections = ""
    if regime_text:
        extra_sections += f"\n{regime_text}\n"
    if vix_disloc_text:
        extra_sections += f"\n{vix_disloc_text}\n"
    if track_record:
        extra_sections += f"\n=== {track_record}\n"
    if earnings_cal:
        extra_sections += f"\n=== {earnings_cal}\n"
    if open_positions:
        extra_sections += f"\n=== {open_positions}\n"
    if kalshi_text:
        extra_sections += f"\n{kalshi_text}\n"
    if catalyst_seeds:
        extra_sections += f"\n{catalyst_seeds}\n"

    user_message = f"""Date: {datetime.date.today().isoformat()}
Market: US equities + options

{news_text}

=== ORCA TECHNICAL SCAN (IV vs realized vol) ===
{orca_text}
{extra_sections}
Analyze deeply. Connect every news item to potential options trades.
Think through the 2nd and 3rd order effects.
Which options are mispriced RIGHT NOW given this information?
Give me the TOP 5-7 trades."""

    # Adaptive thinking budget: more signals = deeper thinking needed
    # Keep under MAX_OUTPUT_TOKENS (32K) — budget + output must fit
    if signal_count >= 20:
        thinking_budget = 16000  # Complex day — think harder
    elif signal_count >= 10:
        thinking_budget = 12000  # Moderate complexity
    else:
        thinking_budget = 8000   # Normal day

    import time as _time

    # Build request with adaptive thinking (Opus 4.6 native feature)
    request_body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "system": SYSTEM_PROMPT,
        "thinking": {
            "type": "enabled",
            "budget_tokens": thinking_budget
        },
        "messages": [{"role": "user", "content": user_message}],
    }

    print(f"  🧠 Calling {ANTHROPIC_MODEL} with adaptive thinking...")
    print(f"  📊 Thinking budget: {thinking_budget:,} tokens ({signal_count} signals)")

    # Retry up to 3 times with 5-minute intervals on failure
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post("https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
                json=request_body,
                timeout=900,  # Opus extended thinking needs up to 15 min
            )

            if resp.status_code == 529 or resp.status_code == 503:
                # Overloaded or service unavailable — retry
                print(f"  ⚠ Attempt {attempt}/{max_retries}: API overloaded ({resp.status_code}) — waiting 5 min...")
                if attempt < max_retries:
                    _time.sleep(300)
                    continue
                return f"API Error {resp.status_code}: Overloaded after {max_retries} retries", 0

            if resp.status_code != 200:
                error_text = resp.text[:500]
                print(f"  ❌ API Error {resp.status_code}: {error_text}")
                return f"API Error {resp.status_code}: {error_text}", 0

            data = resp.json()

            # Extract token usage (Opus reports thinking tokens separately)
            usage = data.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            # Thinking tokens may be in cache_creation_input_tokens or separate
            # The API includes thinking in output_tokens for billing
            # But we can detect thinking blocks in the response
            thinking_tokens = 0
            text_output = ""

            for block in data.get("content", []):
                if block.get("type") == "thinking":
                    # Count thinking tokens (rough estimate: 1 token per 4 chars)
                    thinking_text = block.get("thinking", "")
                    thinking_tokens += len(thinking_text) // 4
                    print(f"  💭 Thinking: {len(thinking_text)} chars")
                elif block.get("type") == "text":
                    text_output += block["text"]

            # Log cost (thinking tokens billed at output rate)
            cost = tracker.log_call(ANTHROPIC_MODEL, input_tokens, output_tokens,
                                    thinking_tokens, "analyst_opus")

            print(f"  💰 Cost: {input_tokens:,} in + {output_tokens:,} out "
                  f"(~{thinking_tokens:,} thinking) = ${cost:.4f}")

            return text_output, cost

        except requests.exceptions.Timeout:
            print(f"  ⚠ Attempt {attempt}/{max_retries}: Timeout — waiting 5 min...")
            if attempt < max_retries:
                _time.sleep(300)
                continue
            return "ERROR: API timeout after 3 retries (Opus was thinking too hard)", 0
        except Exception as e:
            print(f"  ⚠ Attempt {attempt}/{max_retries}: {e} — waiting 5 min...")
            if attempt < max_retries:
                _time.sleep(300)
                continue
            return f"API error after 3 retries: {e}", 0

# ============================================================
# TELEGRAM
# ============================================================
def _md_to_tg_html(text: str) -> str:
    """Convert Opus markdown output to Telegram HTML.

    Keeps ALL content — regime, VIX, VWKS, skew, thesis, everything.
    Only strips the News Sources section at the bottom (noise on mobile).
    """
    import re as _re

    # Strip news sources section (stays in .md file)
    text = _re.sub(r'\n---\n## News Sources.*', '', text, flags=_re.DOTALL)

    # Escape HTML entities FIRST (before adding our own tags)
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    # Markdown bold **text** → <b>text</b>
    text = _re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)

    # Markdown italic *text* (but not inside HTML tags)
    text = _re.sub(r'(?<![<b/])\*(.+?)\*(?![>])', r'<i>\1</i>', text)

    # ## Headers → bold with line
    text = _re.sub(r'^#{1,3}\s+(.+)$', r'<b>\1</b>', text, flags=_re.MULTILINE)

    # Markdown tables → clean aligned text
    # Remove separator rows |---|---|
    text = _re.sub(r'^\|[-\s|:]+\|$\n?', '', text, flags=_re.MULTILINE)
    # Convert table rows: | a | b | c | → a | b | c
    text = _re.sub(r'^\|\s*(.+?)\s*\|$', r'\1', text, flags=_re.MULTILINE)

    # --- dividers → thin line
    text = _re.sub(r'^---+$', '─' * 28, text, flags=_re.MULTILINE)

    # Clean up excessive blank lines
    text = _re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def _smart_chunk(text: str, max_len: int = 4000) -> list:
    """Split message at logical boundaries (─── dividers), not mid-sentence."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    current = ""
    # Split on our divider lines
    sections = text.split('─' * 28)

    for i, section in enumerate(sections):
        divider = '─' * 28 if i > 0 else ''
        candidate = current + divider + section

        if len(candidate) <= max_len:
            current = candidate
        else:
            # Current chunk is full — save it and start new
            if current.strip():
                chunks.append(current.strip())
            current = divider + section

            # If a single section exceeds max_len, split by paragraphs
            if len(current) > max_len:
                paragraphs = current.split('\n\n')
                current = ""
                for para in paragraphs:
                    if len(current) + len(para) + 2 <= max_len:
                        current += ('\n\n' if current else '') + para
                    else:
                        if current.strip():
                            chunks.append(current.strip())
                        current = para

    if current.strip():
        chunks.append(current.strip())

    return chunks


def send_telegram(message, parse_mode="HTML"):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠ Telegram not configured"); return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = _smart_chunk(message)
    print(f"  📨 Sending {len(chunks)} message(s) to Telegram...")
    for i, chunk in enumerate(chunks):
        try:
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "disable_web_page_preview": True,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                print(f"  ✅ Telegram chunk {i+1}/{len(chunks)}")
            else:
                # If HTML parse fails, retry as plain text
                print(f"  ⚠ Telegram HTML failed ({r.status_code}), retrying plain...")
                payload.pop("parse_mode", None)
                r2 = requests.post(url, json=payload, timeout=10)
                print(f"  {'✅' if r2.status_code==200 else '❌'} Telegram plain: {r2.status_code}")
        except Exception as e:
            print(f"  ❌ {e}")

# ============================================================
# X/TWITTER POSTING
# ============================================================

ORCA_X_REWRITE_PROMPT = """You are rewriting an options trading analysis from Telegram for X/Twitter.

The post MUST be UNDER 3800 characters total. This is a HARD LIMIT. Count carefully.

Your goal: make people TRUST this analyst and FOLLOW. Be concise but substantive.

STRUCTURE (in this order):
1. HOOK (1 line) — "Analytics-driven options signal. One curious human quant + three independent AI minds. Scans 670+ tickers → filters → calls top trades. Mon-Fri."
2. MACRO (2-3 sentences max) — Quick market read. What matters today. Be bombastic and confident.
3. TOP TRADES — ONLY include trades marked "✅ consensus" (where all analysts agree). Skip any trade marked "⚠️ no consensus" or "⚠️ partial consensus" — those are for Telegram only. For each consensus trade:
   • $TICKER — DIRECTION (BUY CALL SPREAD / BUY PUT SPREAD / BUY CALL / BUY PUT)
   • CONTRACT: Spread strikes + cost — e.g., "$215/$210 put spread | $2.75 debit | Exp 3/27"
   • KEY DATA LINE: "CONF: X | URG: THIS_WEEK | IV/HV: 35%/27% | Skew25D: -5.4 | AI: ✅ All 3 agree"
     Include CONF (1-10 confidence), URG (urgency), IV vs HV, Skew25D value, and AI agreement
   • THESIS: 2-3 sentences of FUNDAMENTAL explanation — the CHAIN of causation
   • TARGET + ALERT: "$X target | $Y alert"
4. TRACKER — "📊 Trade tracker: [Google Sheet URL]"
5. SIGN OFF — "Analytics-driven options signal • Mon-Fri"
6. DISCLAIMER — "⚠️ Options research only. Not financial advice. DYOR."

TRADE FILTERING RULES:
- ONLY include trades where ALL analysts agreed (marked "✅ consensus" on the CONF/URG line)
- This means fewer trades but MUCH more detail per trade — that's the goal
- If a trade says "⚠️ no consensus" or "⚠️ partial consensus" → SKIP it entirely
- If NO trades have consensus, include the top 2-3 by CONF score but note "split conviction"

CRITICAL DATA — MUST INCLUDE for EACH trade:
- CONF score (1-10) — from the original analysis
- URG (urgency) — THIS_WEEK, NEXT_WEEK, or MONITOR
- IV/HV — implied vol vs historical vol (if available in the analysis)
- Skew25D — the 25-delta skew value (if available)
- AI Agreement — "✅ All 3 agree" or "⚠️ 2/3 agree" — from consensus labels
- Target price + Alert price

FORMATTING:
- Use CASHTAGS: $SPY, $AAPL — clickable on X
- Emojis: sparingly (⚡🔹📈📉🎯⚠️)
- NO hashtags, NO HTML tags
- MUST be under 3800 chars — if over, CUT the lowest-CONF consensus trades first
- Confident institutional tone
- DO include spread strikes and debit/credit cost for each trade
- Make sure the post ends CLEANLY — no cut-off sentences

REMEMBER: 3800 chars MAX. Fewer trades + more depth = better engagement. Never cut mid-sentence."""

DISCLAIMER_X = "\n\n⚠️ Options research only. Not financial advice. Options involve substantial risk of loss. Always do your own due diligence."


def opus_rewrite_for_x(telegram_msg):
    """
    Use Opus 4.6 to rewrite the Telegram message into a 4000-char X post.
    Returns the X-formatted text, or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        print("[X] No ANTHROPIC_API_KEY — skipping Opus rewrite")
        return None

    # Strip HTML tags for clean input
    import re as _xre
    clean_msg = _xre.sub(r'<[^>]+>', '', telegram_msg)

    # Include Google Sheet URL if configured
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()
    sheet_note = ""
    if sheet_id:
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        sheet_note = f"\n\nGoogle Sheet trade tracker URL (MUST include in post): {sheet_url}"

    try:
        print("[X] Calling Opus 4.6 to rewrite for X/Twitter...")
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 6000,
                "thinking": {
                    "type": "enabled",
                    "budget_tokens": 4000,
                },
                "system": ORCA_X_REWRITE_PROMPT,
                "messages": [{"role": "user", "content": f"Here is today's ORCA Telegram analysis. Rewrite for X (max 3800 chars). ONLY include ✅ consensus trades — skip any ⚠️ no consensus or partial consensus trades. Include spread strikes + debit/credit for each trade. Give deep fundamental explanations. End cleanly:\n\n{clean_msg}{sheet_note}"}],
            },
            timeout=300,
        )

        if resp.status_code != 200:
            print(f"[X] Opus API error {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        usage = data.get("usage", {})
        input_tok = usage.get("input_tokens", 0)
        output_tok = usage.get("output_tokens", 0)
        cost = input_tok / 1_000_000 * COST_PER_1M_INPUT + output_tok / 1_000_000 * COST_PER_1M_OUTPUT
        print(f"[X] Opus rewrite: {input_tok:,} in + {output_tok:,} out = ${cost:.4f}")

        # Extract text (skip thinking blocks)
        text_output = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text_output += block["text"]

        tweet = text_output.strip()

        # Safety: ensure Google Sheet link is included if configured
        if sheet_id and "docs.google.com" not in tweet:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            tweet = tweet.rstrip() + f"\n\n📊 Track all trades live: {sheet_url}"

        # Safety: ensure disclaimer is present
        if "not financial advice" not in tweet.lower():
            tweet = tweet.rstrip() + DISCLAIMER_X

        # Safety: hard-cap at 4000 chars (X Premium supports up to 25K)
        if len(tweet) > 4000:
            # Cut at last complete paragraph before limit
            truncated = tweet[:3950]
            last_newline = truncated.rfind("\n\n")
            if last_newline > 2000:
                tweet = truncated[:last_newline] + "\n\n⚠️ Options research only. Not financial advice. DYOR."
            else:
                tweet = truncated + "\n..."

        return tweet

    except Exception as e:
        print(f"[X] Opus rewrite failed: {e}")
        return None


def post_to_x(telegram_msg):
    """
    Post to X/Twitter: Opus rewrites the Telegram message into a
    4000-char X Premium post, then tweets it.
    Uses direct OAuth 1.0a via requests_oauthlib for reliable auth.
    """
    try:
        from requests_oauthlib import OAuth1
    except ImportError:
        print("[X] requests_oauthlib not installed — run: pip install requests_oauthlib")
        return

    api_key = os.environ.get("X_API_KEY", "").strip()
    api_secret = os.environ.get("X_API_SECRET", "").strip()
    access_token = os.environ.get("X_ACCESS_TOKEN", "").strip()
    access_token_secret = os.environ.get("X_ACCESS_TOKEN_SECRET", "").strip()

    if not all([api_key, api_secret, access_token, access_token_secret]):
        print("[X] X/Twitter credentials not configured — skipping")
        return

    # Debug: show key fingerprints + lengths for verification
    def _mask(s):
        return f"{s[:4]}...{s[-2:]}({len(s)})" if len(s) > 6 else f"len={len(s)}"
    print(f"[X] Auth: API_KEY={_mask(api_key)} API_SECRET={_mask(api_secret)} "
          f"TOKEN={_mask(access_token)} TOKEN_SECRET={_mask(access_token_secret)}")
    # Expected lengths: API_KEY=25, API_SECRET=50, ACCESS_TOKEN=50, ACCESS_TOKEN_SECRET=45
    expected = {"X_API_KEY": 25, "X_API_SECRET": 50, "X_ACCESS_TOKEN": 50, "X_ACCESS_TOKEN_SECRET": 45}
    for name, exp_len in expected.items():
        actual = len({"X_API_KEY": api_key, "X_API_SECRET": api_secret,
                       "X_ACCESS_TOKEN": access_token, "X_ACCESS_TOKEN_SECRET": access_token_secret}[name])
        if actual != exp_len:
            print(f"[X] ⚠ {name} length is {actual}, expected ~{exp_len} — possible truncation or extra chars")

    # Step 1: Test auth with GET /2/users/me (with Cloudflare retry)
    auth = OAuth1(api_key, api_secret, access_token, access_token_secret)
    auth_ok = False
    try:
        test_resp = None
        is_cloudflare = False
        for attempt in range(3):
            test_resp = requests.get(
                "https://api.twitter.com/2/users/me",
                auth=auth,
                timeout=15,
            )
            # Detect Cloudflare challenge (HTML "Just a moment..." page)
            is_cloudflare = ("Just a moment" in test_resp.text[:500] or
                             "cloudflare" in test_resp.text[:500].lower())
            if is_cloudflare and attempt < 2:
                import time as _time
                wait = 5 * (attempt + 1)
                print(f"[X] ⚠ Cloudflare challenge on auth check (attempt {attempt+1}/3) — retrying in {wait}s...")
                _time.sleep(wait)
                continue
            break

        if test_resp.status_code == 200:
            user_data = test_resp.json().get("data", {})
            username = user_data.get("username", "?")
            print(f"[X] ✅ Auth OK — posting as @{username}")
            auth_ok = True
        elif test_resp.status_code == 401:
            print(f"[X] ❌ Auth FAILED (401 Unauthorized)")
            print(f"[X]    Response: {test_resp.text[:300]}")
            try:
                v1_resp = requests.get(
                    "https://api.twitter.com/1.1/account/verify_credentials.json",
                    auth=auth,
                    timeout=15,
                )
                if v1_resp.status_code == 200:
                    print(f"[X]    v1.1 auth WORKS — issue is v2 API access. Enable v2 in Developer Portal.")
                elif v1_resp.status_code == 401:
                    print(f"[X]    v1.1 also 401 — credentials are invalid/expired.")
                elif v1_resp.status_code == 403:
                    print(f"[X]    v1.1 returns 403 — app suspended or wrong permissions.")
                else:
                    print(f"[X]    v1.1 returns {v1_resp.status_code}: {v1_resp.text[:200]}")
            except Exception:
                pass
            print("[X]")
            print("[X]    ╔═══════════════════════════════════════════════╗")
            print("[X]    ║  HOW TO FIX X/Twitter 401 Unauthorized:     ║")
            print("[X]    ║                                             ║")
            print("[X]    ║  1. Go to developer.x.com → Your App        ║")
            print("[X]    ║  2. Settings → User Authentication Settings ║")
            print("[X]    ║  3. Set App permissions: 'Read and Write'   ║")
            print("[X]    ║  4. OAuth 1.0a: Enabled                     ║")
            print("[X]    ║  5. Keys → Regenerate ALL 4 keys            ║")
            print("[X]    ║  6. Update GitHub Secrets with new keys:    ║")
            print("[X]    ║     X_API_KEY, X_API_SECRET,                ║")
            print("[X]    ║     X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET   ║")
            print("[X]    ╚═══════════════════════════════════════════════╝")
            return
        elif test_resp.status_code == 403:
            if is_cloudflare:
                # Cloudflare blocked all 3 auth check attempts — skip auth, try posting directly
                print(f"[X] ⚠ Cloudflare blocked auth check after 3 attempts — skipping auth, will try posting directly")
            else:
                print(f"[X] ❌ Auth returned 403 Forbidden — app may be suspended or lacks permissions")
                print(f"[X]    Response: {test_resp.text[:300]}")
                return
        elif test_resp.status_code == 429:
            print(f"[X] ⚠ Rate limited (429) — too many requests. Will retry next run.")
            return
        else:
            print(f"[X] ⚠ Unexpected auth response {test_resp.status_code}: {test_resp.text[:200]}")
            print("[X]    Attempting to post anyway...")
    except Exception as e:
        print(f"[X] Auth test error: {e}")
        print("[X]    Attempting to post anyway...")

    # Step 2: Opus rewrites the Telegram message for X audience
    tweet = opus_rewrite_for_x(telegram_msg)
    if not tweet:
        print("[X] No Opus output — skipping X post")
        return

    print(f"\n[X] Posting to X ({len(tweet)} chars):")
    print(tweet)

    # Step 3: Try tweepy first (best Cloudflare handling)
    posted = False
    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            wait_on_rate_limit=True,
        )
        # Auth check is optional — Cloudflare may block GET but allow POST
        try:
            me = client.get_me()
            if me and me.data:
                print(f"[X] ✅ tweepy auth OK — posting as @{me.data.username}")
        except Exception as auth_err:
            print(f"[X] ⚠ tweepy auth check skipped ({auth_err}) — will try posting directly")
        response = client.create_tweet(text=tweet)
        if response and response.data:
            tweet_id = response.data["id"]
            print(f"[X] ✅ Posted via tweepy! https://x.com/i/status/{tweet_id}")
            posted = True
    except Exception as e:
        print(f"[X] tweepy failed: {e} — falling back to direct API")

    if posted:
        return

    # Step 4: Direct v2 with custom headers
    try:
        import time as _time
        headers = {
            "User-Agent": "OrcaBot/1.0",
            "Accept": "application/json",
        }
        resp = None
        for attempt in range(3):
            resp = requests.post(
                "https://api.twitter.com/2/tweets",
                json={"text": tweet},
                auth=auth,
                headers=headers,
                timeout=30,
            )
            is_cf = ("Just a moment" in resp.text[:500] or
                     "cloudflare" in resp.text[:500].lower())
            if is_cf and attempt < 2:
                wait = 10 * (attempt + 1)
                print(f"[X] ⚠ Cloudflare on v2 (attempt {attempt+1}/3) — retrying in {wait}s...")
                _time.sleep(wait)
                continue
            break

        if resp.status_code in (200, 201):
            data = resp.json()
            tweet_id = data.get("data", {}).get("id", "?")
            print(f"[X] ✅ Posted via v2! https://x.com/i/status/{tweet_id}")
            return
        else:
            print(f"[X] v2 failed ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        print(f"[X] v2 error: {e}")

    # Step 5: v1.1 endpoint (different Cloudflare rules, 280 char limit)
    try:
        headers = {"User-Agent": "OrcaBot/1.0", "Accept": "application/json"}
        resp = requests.post(
            "https://api.twitter.com/1.1/statuses/update.json",
            data={"status": tweet[:280]},
            auth=auth,
            headers=headers,
            timeout=30,
        )
        if resp.status_code == 200:
            tweet_id = resp.json().get("id_str", "?")
            print(f"[X] ✅ Posted via v1.1! https://x.com/i/status/{tweet_id}")
            print(f"[X]    ⚠ v1.1 truncated to 280 chars")
        else:
            print(f"[X] ❌ All methods failed. v1.1: ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        print(f"[X] ❌ v1.1 error: {e}")


# ============================================================
# EOD POSITION REVIEW — end-of-day thesis recheck
# ============================================================

EOD_REVIEW_PROMPT = """You are ORCA's position manager. Review each OPEN position and decide: HOLD, CLOSE, or ADJUST.

For each position, you receive: ticker, direction, strikes, entry date, days held, entry price, and the original thesis.

For each position, consider:
1. Is the original CATALYST still valid? Has it played out, been invalidated, or changed timing?
2. Has the UNDERLYING moved significantly (toward target = take profit, against = cut losses)?
3. Has IV changed materially? (IV crush after catalyst = consider closing even if right on direction)
4. Is expiry approaching with the trade not working? (DTE < 5 with no movement = time decay is killing you)
5. Are there NEW developments (news, earnings, sector rotation) that change the thesis?

Respond with a JSON array. For each position:
{"ticker": "AAPL", "action": "HOLD|CLOSE|ADJUST", "reason": "brief 1-2 sentence explanation", "urgency": "NOW|TOMORROW|WATCH", "underwater": true|false}

Set "underwater" to true ONLY if the position is significantly losing money (would recommend caution or cutting).
Be DECISIVE. If a thesis is broken, say CLOSE. Don't hedge with "maybe watch" when the catalyst has passed.
Keep it concise — traders want action, not essays."""


def eod_position_review(tracker):
    """
    End-of-day thesis review: query open positions from orca_trade_log.db,
    send to Claude for thesis recheck, return per-position recommendations.
    Returns (list_of_recommendations, cost) or (None, 0) on error.
    """
    import sqlite3 as _sql

    db_path = Path("orca_trade_log.db")
    if not db_path.exists():
        print("  ℹ No trade log database found — skipping EOD review")
        return None, 0

    conn = _sql.connect(str(db_path))
    rows = conn.execute("""
        SELECT trade_id, ticker, direction, strike, strike_2, expiry,
               entry_price, call_date, confidence, thesis, edge,
               underlying_at_entry,
               option_high_watermark, option_low_watermark
        FROM trades WHERE status = 'OPEN'
        ORDER BY call_date DESC
    """).fetchall()
    conn.close()

    if not rows:
        print("  ℹ No open positions — nothing to review")
        return None, 0

    print(f"\n  📋 Reviewing {len(rows)} open position(s)...")

    # Build position summary for Claude
    positions_text = ""
    for row in rows:
        (tid, ticker, direction, strike, strike2, expiry,
         entry_price, call_date, conf, thesis, edge,
         underlying_entry, high_wm, low_wm) = row

        days_held = 0
        dte = "?"
        try:
            entry_dt = datetime.datetime.strptime(call_date, "%Y-%m-%d")
            days_held = (datetime.datetime.now() - entry_dt).days
            if expiry:
                exp_dt = datetime.datetime.strptime(expiry, "%Y-%m-%d")
                dte = (exp_dt - datetime.datetime.now()).days
        except (ValueError, TypeError):
            pass

        strike_str = f"${strike:.0f}" if strike else "?"
        if strike2:
            strike_str += f"/${strike2:.0f}"

        ul_str = f"${underlying_entry:.2f}" if underlying_entry else "N/A"
        ep_str = f"${entry_price:.2f}" if entry_price else "N/A"
        hw_str = f"${high_wm:.2f}" if high_wm else "N/A"
        lw_str = f"${low_wm:.2f}" if low_wm else "N/A"

        positions_text += f"""
--- Position #{tid}: {ticker} ---
Direction: {direction}
Strikes: {strike_str} | Expiry: {expiry or '?'} ({dte}d remaining)
Entry: {ep_str} on {call_date} ({days_held} days held)
Underlying at entry: {ul_str}
Confidence: {conf}/10
High watermark: {hw_str} | Low watermark: {lw_str}
Thesis: {thesis or 'N/A'}
Edge: {edge or 'N/A'}
"""

    try:
        request_body = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 6000,
            "system": EOD_REVIEW_PROMPT,
            "thinking": {
                "type": "enabled",
                "budget_tokens": 4000
            },
            "messages": [{"role": "user", "content": f"Today is {datetime.date.today().strftime('%B %d, %Y')}. Review these open positions:\n{positions_text}"}],
        }

        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json=request_body,
            timeout=300,
        )

        if resp.status_code != 200:
            print(f"  ⚠ EOD review API error {resp.status_code}: {resp.text[:200]}")
            return None, 0

        data = resp.json()
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)

        text_output = ""
        thinking_tokens = 0
        for block in data.get("content", []):
            if block.get("type") == "thinking":
                thinking_tokens += len(block.get("thinking", "")) // 4
            elif block.get("type") == "text":
                text_output += block["text"]

        cost = tracker.log_call(ANTHROPIC_MODEL, input_tokens, output_tokens,
                                thinking_tokens, "eod_review")

        # Parse JSON response
        content = text_output.strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        import json as _json
        recommendations = _json.loads(content)
        if not isinstance(recommendations, list):
            print("  ⚠ EOD review returned non-list")
            return None, 0

        # Print summary
        for rec in recommendations:
            action = rec.get("action", "?")
            icon = {"HOLD": "✅", "CLOSE": "🛑", "ADJUST": "🔧"}.get(action, "❓")
            print(f"  {icon} {rec.get('ticker', '?')}: {action} — {rec.get('reason', '')[:80]}")

        return recommendations, cost

    except json.JSONDecodeError:
        print("  ⚠ EOD review response not valid JSON")
        return None, 0
    except Exception as e:
        print(f"  ⚠ EOD review error: {e}")
        return None, 0


def _gpt_eod_review(positions_text: str) -> list:
    """GPT-5.4 Thinking co-analyst EOD review of open positions."""
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        return []

    try:
        # Primary: gpt-5.4-thinking (highest intelligence)
        model = "gpt-5.4-thinking"
        resp = requests.post("https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 6000,
                "messages": [
                    {"role": "system", "content": EOD_REVIEW_PROMPT},
                    {"role": "user", "content": f"Today is {datetime.date.today().strftime('%B %d, %Y')}. Review these open positions:\n{positions_text}"},
                ],
            },
            timeout=180,
        )

        # Fallback: gpt-5.4 if thinking model unavailable
        if resp.status_code != 200:
            model = "gpt-5.4"
            print(f"    ℹ gpt-5.4-thinking not available — falling back to {model}")
            resp = requests.post("https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 6000,
                    "messages": [
                        {"role": "system", "content": EOD_REVIEW_PROMPT},
                        {"role": "user", "content": f"Today is {datetime.date.today().strftime('%B %d, %Y')}. Review these open positions:\n{positions_text}"},
                    ],
                },
                timeout=180,
            )

        if resp.status_code != 200:
            print(f"    ⚠ GPT EOD review error {resp.status_code}: {resp.text[:200]}")
            return []

        content = resp.json()["choices"][0]["message"]["content"].strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        result = json.loads(content)
        print(f"    ✅ GPT ({model}): {len(result) if isinstance(result, list) else 0} recommendations")
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"    ⚠ GPT EOD review error: {e}")
        return []


def _gemini_eod_review(positions_text: str) -> list:
    """Gemini 3.1 Pro Deep Think co-analyst EOD review of open positions."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        return []

    try:
        # Primary: gemini-3.1-pro-preview with Deep Think
        model = "gemini-3.1-pro-preview"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
        resp = requests.post(url,
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": f"{EOD_REVIEW_PROMPT}\n\nToday is {datetime.date.today().strftime('%B %d, %Y')}. Review these open positions:\n{positions_text}"}]}],
                "generationConfig": {"maxOutputTokens": 6000, "temperature": 0.3},
                "thinkingConfig": {"thinkingBudget": 4000},
            },
            timeout=180,
        )

        # Fallback: gemini-3.1-pro (without Deep Think) if preview unavailable
        if resp.status_code != 200:
            model = "gemini-3.1-pro"
            print(f"    ℹ Gemini 3.1 Pro not available — falling back to {model}")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
            resp = requests.post(url,
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": f"{EOD_REVIEW_PROMPT}\n\nToday is {datetime.date.today().strftime('%B %d, %Y')}. Review these open positions:\n{positions_text}"}]}],
                    "generationConfig": {"maxOutputTokens": 6000, "temperature": 0.3},
                },
                timeout=180,
            )

        if resp.status_code != 200:
            print(f"    ⚠ Gemini EOD review error {resp.status_code}: {resp.text[:200]}")
            return []

        # Parse response — handle both thinking and non-thinking formats
        content = ""
        candidates = resp.json().get("candidates", [])
        if candidates:
            for part in candidates[0].get("content", {}).get("parts", []):
                if "text" in part:
                    content = part["text"].strip()

        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        result = json.loads(content)
        print(f"    ✅ Gemini ({model}): {len(result) if isinstance(result, list) else 0} recommendations")
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"    ⚠ Gemini EOD review error: {e}")
        return []


def _merge_eod_consensus(claude_recs, gpt_recs, gemini_recs):
    """
    Merge EOD recommendations from all 3 AIs into consensus.
    For each ticker: if all agree on action → consensus. If 2/3 agree → majority.
    Returns list of merged recommendations with consensus info.
    """
    # Build per-ticker maps
    tickers = set()
    claude_map, gpt_map, gemini_map = {}, {}, {}

    for rec in (claude_recs or []):
        t = rec.get("ticker", "").upper()
        if t:
            claude_map[t] = rec
            tickers.add(t)

    for rec in (gpt_recs or []):
        t = rec.get("ticker", "").upper()
        if t:
            gpt_map[t] = rec
            tickers.add(t)

    for rec in (gemini_recs or []):
        t = rec.get("ticker", "").upper()
        if t:
            gemini_map[t] = rec
            tickers.add(t)

    merged = []
    for ticker in sorted(tickers):
        c = claude_map.get(ticker, {})
        g = gpt_map.get(ticker, {})
        ge = gemini_map.get(ticker, {})

        actions = [x.get("action", "").upper() for x in [c, g, ge] if x]
        n_analysts = len(actions)

        # Determine consensus action
        from collections import Counter
        action_counts = Counter(actions)
        top_action, top_count = action_counts.most_common(1)[0] if action_counts else ("HOLD", 0)

        if top_count == n_analysts:
            consensus = f"[{n_analysts}/{n_analysts}]"
        elif top_count >= 2:
            consensus = f"[{top_count}/{n_analysts}]"
        else:
            consensus = "[split]"

        # Combine reasons
        reasons = []
        for label, m in [("Claude", c), ("GPT", g), ("Gemini", ge)]:
            r = m.get("reason", "")
            if r:
                reasons.append(r)

        # Use Claude's reason as primary, note disagreements
        primary_reason = c.get("reason", "") or g.get("reason", "") or ge.get("reason", "")

        # Check underwater flag from any analyst
        underwater = any(m.get("underwater", False) for m in [c, g, ge] if m)

        merged.append({
            "ticker": ticker,
            "action": top_action,
            "reason": primary_reason,
            "urgency": c.get("urgency", g.get("urgency", ge.get("urgency", "WATCH"))),
            "underwater": underwater,
            "consensus": consensus,
            "n_analysts": n_analysts,
        })

    return merged


def send_eod_review(recommendations):
    """
    Save ALL EOD recommendations to DB, then send FILTERED message to Telegram.
    Only CLOSE actions and significantly underwater HOLD actions go to Telegram.
    Everything else → silent (visible in Google Sheet only).
    """
    if not recommendations:
        return

    # Step 1: Save ALL recommendations to the database for Google Sheet
    from trade_logger import save_eod_recommendations
    updated = save_eod_recommendations(recommendations)
    print(f"  💾 Saved {len(recommendations)} EOD recommendations to DB ({updated} rows updated)")

    # Step 2: Filter for Telegram — only actionable items
    tg_recs = []
    for rec in recommendations:
        action = rec.get("action", "").upper()
        underwater = rec.get("underwater", False)
        consensus = rec.get("consensus", "")

        if action == "CLOSE":
            tg_recs.append(rec)
        elif action == "HOLD" and underwater:
            tg_recs.append(rec)
        # ADJUST goes to Telegram too (it's actionable)
        elif action == "ADJUST":
            tg_recs.append(rec)

    if not tg_recs:
        print("  ℹ No actionable EOD items for Telegram (all positions holding fine)")
        return

    # Step 3: Build concise Telegram message
    today_str = datetime.date.today().strftime("%b %d, %Y")
    sep = '─' * 28

    msg = f"<b>📋 EOD Review — {today_str}</b>\n{sep}\n\n"

    for rec in tg_recs:
        ticker = rec.get("ticker", "?")
        action = rec.get("action", "?")
        reason = rec.get("reason", "")
        consensus = rec.get("consensus", "")
        underwater = rec.get("underwater", False)

        icon = {"HOLD": "⚠️", "CLOSE": "🛑", "ADJUST": "🔧"}.get(action, "❓")
        tag = f" {consensus}" if consensus else ""

        if action == "HOLD" and underwater:
            msg += f"{icon} <b>{ticker}</b> → HOLD (underwater){tag}\n"
        else:
            msg += f"{icon} <b>{ticker}</b> → {action}{tag}\n"
        msg += f"   <i>{reason}</i>\n\n"

    msg += f"{sep}\n"
    msg += "<i>Full position details in Google Sheet.</i>"
    msg += "\n\n<i>⚠️ Not financial advice. DYOR.</i>"

    send_telegram(msg, parse_mode="HTML")
    print(f"  ✅ EOD Telegram: {len(tg_recs)} actionable items (of {len(recommendations)} total)")


# ============================================================
# MAIN
# ============================================================
def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║  ⚡ ORCA ANALYST v4 — Stage 0 Catalyst Hunter Edition         ║
║  Stage 0: Catalyst Hunter → Stage 1: Scanner → Stage 2: Opus ║
║  Delayed-reaction ideas + IV/HV mispricing + extended thinking║
╚═══════════════════════════════════════════════════════════════╝""")

    tracker = CostTracker()
    tracker.print_dashboard()

    ok, reason = tracker.check_budget()
    if not ok:
        print(f"\n  ❌ {reason}")
        send_telegram(f"{reason}", parse_mode=None)
        tracker.close(); return

    print("\n📊 Reading ORCA technical results...")
    scan_file, orca_data = read_latest_scan()

    # Pre-filter: remove iron condors before sending to Opus (banned strategy)
    raw_count = len(orca_data)
    orca_data = [t for t in orca_data if "iron condor" not in t.get("Strategy", "").lower()]
    if raw_count != len(orca_data):
        print(f"   🚫 Filtered {raw_count - len(orca_data)} iron condors (banned)")

    orca_text = format_orca_data(orca_data)
    print(f"   {len(orca_data)} opportunities from {scan_file or 'none'}")

    print("\n📡 Scraping intelligence sources...")
    scraper = NewsScraper()
    news = scraper.scrape_all()
    news_text = scraper.format_for_prompt(news)
    signal_count = sum(len(v) for v in news.values())

    print("\n📈 Loading track record (feedback loop)...")
    track_record = get_track_record()
    if track_record:
        print(f"   ✅ Track record loaded — feeding history to Opus")
    else:
        print(f"   ℹ No trade history yet (first run or no DB)")

    print("\n📋 Loading open positions (consistency filter)...")
    open_positions = get_open_positions()
    if open_positions:
        open_count = open_positions.count("\n  ")
        print(f"   ✅ {open_count} open positions — Opus will avoid re-recommending these tickers")
    else:
        print(f"   ℹ No open positions")

    # Earnings calendar — used to AVOID tickers with upcoming earnings
    print("\n📅 Checking earnings calendar (to AVOID, not trade)...")
    tickers = [t.get("Ticker", "") for t in orca_data if t.get("Ticker")]
    earnings_cal = get_earnings_calendar(tickers)
    if earnings_cal:
        earnings_count = earnings_cal.count("⚡") + earnings_cal.count("📅") + earnings_cal.count("📆")
        print(f"   ⚠ {earnings_count} tickers have earnings within 30 days — will tell Opus to SKIP them")
    else:
        print(f"   ✅ No near-term earnings conflicts")

    print("\n📊 Loading SPY regime model...")
    regime_text = read_regime_prediction()
    if regime_text:
        print(f"   ✅ Regime data loaded — feeding VIX structure + SKEW analysis to Opus")
    else:
        print(f"   ℹ No regime data available (model didn't run or no data)")

    print("\n📊 Loading VIX dislocation map...")
    vix_disloc_text = read_vix_dislocation()
    if vix_disloc_text:
        print(f"   ✅ VIX dislocation data loaded — feeding sector fear map to Opus")
    else:
        print(f"   ℹ No VIX dislocation data available")

    print("\n🎰 Fetching Kalshi S&P 500 crowd sentiment...")
    kalshi_sp500 = fetch_kalshi_sp500()
    if kalshi_sp500:
        bracket_count = kalshi_sp500.count("probability")
        print(f"   ✅ Kalshi S&P data loaded — {bracket_count} brackets (daily + year-end + max/min)")
    else:
        print(f"   ℹ No Kalshi S&P data available")

    print("\n🎰 Fetching Kalshi macro event probabilities...")
    kalshi_macro = fetch_kalshi_macro_events()
    if kalshi_macro:
        event_count = kalshi_macro.count("→")
        print(f"   ✅ Kalshi macro events loaded — {event_count} catalyst probabilities")
    else:
        print(f"   ℹ No Kalshi macro event data available")

    # Combine both Kalshi sections
    kalshi_text = ""
    if kalshi_sp500:
        kalshi_text += kalshi_sp500
    if kalshi_macro:
        kalshi_text += ("\n\n" if kalshi_text else "") + kalshi_macro

    # Stage 0: Load catalyst seeds (pre-screened delayed-reaction ideas)
    catalyst_seeds = ""
    try:
        print("\n🎯 Loading Stage 0 catalyst seeds...")
        from catalyst_hunter import load_catalyst_seeds
        catalyst_seeds = load_catalyst_seeds()
        if not catalyst_seeds:
            print(f"   ℹ No Stage 0 seeds available — Opus will hunt independently")
    except ImportError:
        print(f"   ℹ catalyst_hunter.py not found — skipping Stage 0")
    except Exception as e:
        print(f"   ⚠ Stage 0 load error: {e}")

    print(f"\n🧠 Claude Opus 4.6 analyzing ({signal_count} signals, adaptive thinking)...")
    analysis, cost = call_claude(news_text, orca_text, tracker,
                                  track_record, earnings_cal, regime_text, vix_disloc_text, signal_count,
                                  open_positions, kalshi_text, catalyst_seeds)

    if analysis.startswith(("ERROR","BUDGET","API")):
        print(f"\n  ❌ {analysis}")
        send_telegram(f"⚡ {analysis}", parse_mode=None)
        tracker.close(); return

    print(f"\n{'═'*60}\n{analysis}\n{'═'*60}")

    # Save (initial — will be re-saved after consensus labels are injected)
    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    analyst_file_path = RESULTS_DIR / f"analyst_{ts}.md"
    with open(analyst_file_path, "w", encoding="utf-8") as f:
        f.write(f"# ⚡ ORCA Analyst (Opus 4.6) — {datetime.date.today()} (${cost:.4f})\n\n{analysis}")
        f.write(f"\n\n---\n## News Sources ({sum(len(v) for v in news.values())} signals)\n")
        for cat, items in news.items():
            if items:
                f.write(f"\n### {cat}\n")
                for item in items[:5]:
                    f.write(f"- {item.get('text', item.get('signal',''))[:80]}\n")

    # Telegram — HTML formatted, smart-chunked
    print("\n📱 Sending to Telegram...")
    today_str = datetime.date.today().strftime("%b %d, %Y")
    sep = '─' * 28

    # Build regime summary for Telegram header
    regime_header = ""
    try:
        regime_file = Path("regime_prediction.json")
        if regime_file.exists():
            rd = json.loads(regime_file.read_text())
            if "error" not in rd:
                signal = rd.get("signal", "NEUTRAL")
                bias = rd.get("bias", "NEUTRAL")
                bias_conf = rd.get("bias_confidence", 0) or 0
                hedge = rd.get("hedge", "MODERATE")
                conviction = rd.get("conviction", 0) or 0
                skew_regime = rd.get("skew_regime", "NORMAL")
                skew_z = rd.get("skew_z", 0) or 0
                tail_2 = rd.get("tail_prob_2pct_5d", 0) or 0
                tail_5 = rd.get("tail_prob_5pct_5d", 0) or 0
                fwd = rd.get("forward_returns", {})
                med5 = fwd.get("med_5d", 0) or 0
                med15 = fwd.get("med_15d", 0) or 0
                med30 = fwd.get("med_30d", 0) or 0
                triggers = rd.get("immediate_triggers", [])
                nearest_long = rd.get("nearest_long", [])

                regime_header = f"\n\n📈 <b>SPY Regime Model</b>"
                regime_header += f"\n<i>Reads the VIX term structure (front vs back months) and CBOE SKEW index to determine whether the market's volatility pricing favors longs, shorts, or neither. It combines PCA-based regime classification with forward return analysis from 15+ years of data to output a signal, hedge level, and tail risk probability.</i>"
                regime_header += f"\n\n<b>Signal:</b> {signal} | <b>Bias:</b> {bias} ({bias_conf}%) | <b>Hedge:</b> {hedge} | <b>Conviction:</b> {conviction}/100"

                # Regime explanation
                narrative_parts = build_regime_narrative(rd)
                if narrative_parts:
                    regime_header += f"\n{narrative_parts[0]}"

                # Skew context (part 2 of narrative)
                if len(narrative_parts) > 1:
                    regime_header += f"\n{narrative_parts[1]}"

                # Forward returns
                regime_header += f"\n\n<b>Forward returns (this regime historically):</b>"
                regime_header += f"\n   5-day: {med5:+.2f}% | 15-day: {med15:+.2f}% | 30-day: {med30:+.2f}%"

                # Tail risk
                if tail_2 > 0:
                    regime_header += f"\n<b>Tail risk:</b> {tail_2:.0f}% chance of 2%+ drop in 5 days"
                    if tail_5 > 0:
                        regime_header += f", {tail_5:.0f}% chance of 5%+ drop"

                # Flip triggers — what would change the regime
                if triggers:
                    # triggers can be strings or dicts
                    trig_strs = [t if isinstance(t, str) else str(t.get("state", t)) for t in triggers[:3]]
                    regime_header += f"\n<b>What flips the regime:</b> {' | '.join(trig_strs)}"
                if nearest_long and signal != "LONG":
                    # nearest_long can be dicts with state/slope/skew info
                    nl_strs = []
                    for nl in nearest_long[:2]:
                        if isinstance(nl, str):
                            nl_strs.append(nl)
                        elif isinstance(nl, dict):
                            st = nl.get("state", "?")
                            dist = nl.get("dist_sigma", 0)
                            slope_dir = nl.get("slope_dir", "")
                            skew_dir = nl.get("skew_dir", "")
                            nl_strs.append(f"State {st} ({dist:.1f}σ away, slope {slope_dir.lower()}, skew {skew_dir.lower()})")
                    if nl_strs:
                        regime_header += f"\n<b>Path to LONG:</b> {' | '.join(nl_strs)}"
    except Exception:
        pass

    # Fallback if regime data unavailable
    if not regime_header:
        regime_header = f"\n\n📈 <b>SPY Regime Model</b>"
        regime_header += f"\n<i>Reads the VIX term structure (front vs back months) and CBOE SKEW index to determine whether the market's volatility pricing favors longs, shorts, or neither. It combines PCA-based regime classification with forward return analysis from 15+ years of data to output a signal, hedge level, and tail risk probability.</i>"
        regime_header += f"\n⚠️ Regime model did not run this session (requires full GitHub Actions pipeline). See Opus analysis below for market regime assessment."

    # Build VIX dislocation summary for Telegram header
    disloc_header = ""
    try:
        disloc_file = Path("vix_dislocation.json")
        if disloc_file.exists():
            vd = json.loads(disloc_file.read_text())
            if "error" not in vd:
                over = vd.get("over_hedged", [])
                under = vd.get("under_hedged", [])
                vix_last = vd.get("vix_last", 0)
                all_results = vd.get("all_results", [])

                disloc_header = f"\n\n📊 <b>VIX Dislocation Scanner</b> (VIX: {vix_last})"
                disloc_header += f"\n<i>Compares each sector ETF's drawdown-vs-VIX spread against its own 15-year history (percentile ranked since 2010). When a sector's drawdown diverges significantly from what VIX implies, it flags a dislocation — fear is unevenly distributed. The trend shows whether the dislocation is growing (diverging) or shrinking (converging back to normal).</i>"

                if over:
                    disloc_header += f"\n\n🔴 <b>Extra sector fear</b> (drawn down MORE than VIX implies — p0-15):"
                    for r in over[:4]:
                        trend = r.get("trend_20d", 0)
                        trend_arrow = "↑ diverging" if trend > 0.01 else ("↓ converging" if trend < -0.01 else "→ stable")
                        dd = r.get("etf_drawdown_pct", 0)
                        ticker = r['ticker']
                        disloc_header += f"\n   {ticker} ({r['label']}) — p{r['percentile']} | DD: {dd:+.1f}% | {trend_arrow}"
                        scan_row = orca_scan_data.get(ticker)
                        if scan_row:
                            skew_val = scan_row.get("Skew25D", "N/A")
                            skew_r = scan_row.get("SkewRatio", "N/A")
                            vwks = scan_row.get("VWKS", "N/A")
                            disloc_header += f"\n     ↳ Skew {skew_val} (SkewR {skew_r}) | VWKS {vwks}"
                    disloc_header += f"\n   <i>These sectors have sold off more than broad VIX fear justifies. If macro fear eases, they have the most room to snap back.</i>"

                # Load ORCA scan data to cross-reference skew/VWKS for dislocated sectors
                orca_scan_data = {}
                try:
                    orca_csvs = sorted(RESULTS_DIR.glob("orca_*.csv"))
                    if orca_csvs:
                        with open(orca_csvs[-1], "r") as _f:
                            for _row in csv.DictReader(_f):
                                orca_scan_data[_row.get("Ticker", "")] = _row
                except Exception:
                    pass

                if under:
                    disloc_header += f"\n\n🟢 <b>Resilient vs VIX</b> (drawn down LESS than VIX implies — p85-100):"
                    for r in under[:4]:
                        trend = r.get("trend_20d", 0)
                        trend_arrow = "↑ diverging" if trend > 0.01 else ("↓ converging" if trend < -0.01 else "→ stable")
                        dd = r.get("etf_drawdown_pct", 0)
                        ticker = r['ticker']
                        disloc_header += f"\n   {ticker} ({r['label']}) — p{r['percentile']} | DD: {dd:+.1f}% | {trend_arrow}"
                        # Cross-reference with ORCA scan for skew/VWKS confirmation
                        scan_row = orca_scan_data.get(ticker)
                        if scan_row:
                            skew_val = scan_row.get("Skew25D", "N/A")
                            skew_r = scan_row.get("SkewRatio", "N/A")
                            vwks = scan_row.get("VWKS", "N/A")
                            try:
                                sr = float(skew_r) if skew_r != "N/A" else 1.0
                                vw = float(vwks) if vwks != "N/A" else 1.0
                                if sr > 1.15 or vw < 0.95:
                                    disloc_header += f"\n     ↳ Skew {skew_val} (SkewR {skew_r}) | VWKS {vwks} — put demand is elevated, sector IS hedged despite high percentile"
                                elif sr < 0.90 or vw > 1.05:
                                    disloc_header += f"\n     ↳ Skew {skew_val} (SkewR {skew_r}) | VWKS {vwks} — call-heavy flow, sector may genuinely be complacent"
                                else:
                                    disloc_header += f"\n     ↳ Skew {skew_val} (SkewR {skew_r}) | VWKS {vwks} — neutral positioning, resilience appears fundamental"
                            except (ValueError, TypeError):
                                pass
                    disloc_header += f"\n   <i>These sectors are holding up better than VIX would predict. Cross-check above confirms whether resilience is justified or complacent.</i>"

                if not over and not under:
                    disloc_header += f"\n   Sectors mostly aligned with VIX — no major dislocations. Fear is evenly distributed across sectors."

                n_extreme = len([r for r in all_results if r["percentile"] <= 10 or r["percentile"] >= 90])
                if n_extreme >= 5:
                    disloc_header += f"\n\n⚠️ <b>{n_extreme} sectors at extreme dislocations</b> — significant cross-sector divergence. Fear is NOT distributed uniformly — look for mispricing where the crowd got the sector story wrong."
                elif n_extreme >= 2:
                    disloc_header += f"\n\n{n_extreme} sectors at notable dislocations — selective opportunities where sector-specific risk may be mispriced."
    except Exception:
        pass

    # Build Kalshi crowd sentiment summary for Telegram header
    kalshi_header = ""
    if kalshi_text:
        try:
            import re as _kre

            kalshi_header = f"\n\n🎰 <b>Kalshi S&P 500 Crowd Sentiment</b>"
            kalshi_header += f"\n<i>Real-money prediction market bets on S&P 500 levels. Shows where actual dollars are positioned — not polls or opinions.</i>"

            # Extract daily peak bracket (format: "Peak bracket: 6725-6750 (15%)")
            daily_peak = _kre.search(r'DAILY.*?Peak bracket:\s*(\d+)-(\d+)\s*\((\d+%)\)', kalshi_text, _kre.DOTALL)
            if daily_peak:
                kalshi_header += f"\n\n<b>Near-term expected close:</b> {daily_peak.group(1)}-{daily_peak.group(2)} ({daily_peak.group(3)} peak)"

            # Extract daily 80% range
            daily_range = _kre.search(r'DAILY.*?80% confidence range:\s*(\d+)\s*-\s*(\d+)', kalshi_text, _kre.DOTALL)
            if daily_range:
                kalshi_header += f"\n<b>80% range:</b> {daily_range.group(1)} - {daily_range.group(2)}"

            # Extract year-end peak bracket
            ye_peak = _kre.search(r'YEAR-END.*?Peak bracket:\s*(\d+)(-\d+|\+)\s*\((\d+%)\)', kalshi_text, _kre.DOTALL)
            if ye_peak:
                kalshi_header += f"\n\n<b>Year-end peak bracket:</b> {ye_peak.group(1)}{ye_peak.group(2)} ({ye_peak.group(3)})"

            # Extract expected year-end
            ye_expected = _kre.search(r'expected year-end:\s*(\d+)', kalshi_text)
            if ye_expected:
                kalshi_header += f"\n<b>Expected year-end (prob-weighted):</b> {ye_expected.group(1)}"

            # Extract year-end 80% range
            ye_range = _kre.search(r'Year-end 80% confidence range:\s*(\d+)\s*-\s*(\d+)', kalshi_text)
            if ye_range:
                kalshi_header += f"\n<b>Year-end 80% range:</b> {ye_range.group(1)} - {ye_range.group(2)}"

            # Max level — first entry (highest probability)
            max_reach = _kre.search(r'Reach (\d+):\s*(\d+%)', kalshi_text)
            if max_reach:
                kalshi_header += f"\n\n<b>Rally ceiling:</b> {max_reach.group(2)} chance S&P reaches {max_reach.group(1)}"

            # Min level — first entry (deepest crash)
            crash_lines = _kre.findall(r'Drop below (\d+):\s*(\d+%)', kalshi_text)
            if crash_lines:
                # First entry is the lowest level = most extreme crash
                deepest = crash_lines[0]
                kalshi_header += f"\n<b>Crash risk:</b> {deepest[1]} chance S&P drops below {deepest[0]}"

            # ── Macro event highlights (top catalyst probabilities) ──
            # Extract the most impactful events from the macro section
            hormuz = _kre.search(r'Hormuz.*?→\s*(\d+%)', kalshi_text)
            recession = _kre.search(r'recession start\?.*?→\s*(\d+%)', kalshi_text, _kre.IGNORECASE)
            imf_recess = _kre.search(r'IMF.*?recession.*?→\s*(\d+%)', kalshi_text, _kre.IGNORECASE)
            fed_hike = _kre.search(r'Federal Reserve hike.*?December 31, 2026.*?→\s*(\d+%)', kalshi_text)
            credit_dg = _kre.search(r'credit rating.*?downgraded.*?→\s*(\d+%)', kalshi_text, _kre.IGNORECASE)
            oil_price = _kre.search(r'WTI.*?>(\d+\.?\d*).*?→\s*(\d+%)', kalshi_text)
            gdp_growth = _kre.search(r'GDP.*?more than (\d+\.?\d*)%.*?→\s*(\d+%)', kalshi_text)

            macro_items = []
            if hormuz:
                macro_items.append(f"Hormuz closure: {hormuz.group(1)}")
            if imf_recess:
                macro_items.append(f"Global recession: {imf_recess.group(1)}")
            if fed_hike:
                macro_items.append(f"Fed hike 2026: {fed_hike.group(1)}")
            if credit_dg:
                macro_items.append(f"US downgrade: {credit_dg.group(1)}")
            if oil_price:
                macro_items.append(f"Oil >{oil_price.group(1)}: {oil_price.group(2)}")

            if macro_items:
                kalshi_header += f"\n\n<b>Macro catalyst probabilities:</b>"
                kalshi_header += f"\n   " + " | ".join(macro_items)

        except Exception:
            pass

    # Track record line
    track_line = ""
    if track_record:
        import re as _re
        wr_match = _re.search(r'(\d+)% win rate', track_record)
        if wr_match:
            track_line = f"\n📊 Track record: {wr_match.group(1)}% win rate"

    # Build header (clean, subscriber-friendly)
    header = f"""<i>Analytics-driven options signal • One curious human quant + three independent AI minds • Scans 670+ tickers daily using news flow and crowd positioning → filters to top candidates → calls 5-7 highest conviction trades • New ideas Mon-Fri • All trades tracked with entry prices, P&amp;L, and end-of-day thesis audit in live Google Sheet • Low-capital In-Out spreads, limited risk • Short-horizon catalysts only — if it won't move within the option's life, we skip it.</i>
{sep}
📡 {signal_count} signals deeply analyzed out of 670+ tickers scanned{track_line}
{sep}"""

    # Build market context footer (SPY Regime + VIX + Kalshi at end of message)
    market_context = ""
    context_parts = []
    if regime_header:
        context_parts.append(regime_header)
    if disloc_header:
        context_parts.append(disloc_header)
    if kalshi_header:
        context_parts.append(kalshi_header)
    if context_parts:
        market_context = f"\n\n{'─' * 30}\n<b>📊 Market Context</b>{''.join(context_parts)}"

    # ── STEP 1: Auto-correct strikes ──
    # Opus often picks its own strikes instead of the scanner's exact strikes.
    # We force-replace any wrong strikes with the scanner's real values.
    print("\n🔧 Auto-correcting strikes (scanner → Opus)...")
    analysis, strike_corrections = auto_correct_strikes(analysis, orca_data)
    if strike_corrections:
        for c in strike_corrections:
            print(f"  {c}")
        print(f"  📌 {len(strike_corrections)} strike(s) auto-corrected")
    else:
        print("  ✅ All strikes match scanner — no corrections needed")

    # ── STEP 2: Validate (after corrections) ──
    print("\n🔍 Validating analysis (Python checks)...")
    val_warnings = validate_analysis(analysis, orca_data)
    if val_warnings:
        for w in val_warnings:
            print(f"  {w}")
    else:
        print("  ✅ Python checks passed")

    # Initialize block state (used by quality gate + Opus review + Python validation)
    blocked = False
    block_reason = ""

    # ── STEP 3: MULTI-AI CO-ANALYST — triple-model collaboration ──
    # GPT-5.4 and Gemini INDEPENDENTLY review Claude's ideas AND generate their own.
    # Then Opus merges the best ideas from all three into a consensus analysis.
    # Key: GPT and Gemini run in parallel and do NOT see each other's reviews.
    has_gpt = os.environ.get("OPENAI_API_KEY", "").strip()
    has_gemini = os.environ.get("GEMINI_API_KEY", "").strip()
    ai_count = 1 + bool(has_gpt) + bool(has_gemini)
    ai_label = {1: "one", 2: "two", 3: "three"}[ai_count]
    print(f"\n🤝 Multi-AI Consensus — {ai_label} AI analyst{'s' if ai_count > 1 else ''} collaborating...")

    ok_to_review, _ = tracker.check_budget()
    if ok_to_review and (has_gpt or has_gemini):
        # Pass SPY regime data so co-analysts can factor macro context
        regime_text = ""
        try:
            regime_file = Path("regime_prediction.json")
            if regime_file.exists():
                regime_text = regime_file.read_text(encoding="utf-8")
        except Exception:
            pass

        # Run GPT and Gemini in PARALLEL (independent reviews)
        gpt_output, gpt_cost = None, 0
        gemini_output, gemini_cost = None, 0

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {}
            if has_gpt:
                futures["gpt"] = executor.submit(
                    gpt_co_analyst, analysis, orca_text, open_positions, regime_text, tracker
                )
            if has_gemini:
                futures["gemini"] = executor.submit(
                    gemini_co_analyst, analysis, orca_text, open_positions, regime_text, tracker
                )

            for name, future in futures.items():
                try:
                    result = future.result(timeout=200)
                    if name == "gpt":
                        gpt_output, gpt_cost = result
                    else:
                        gemini_output, gemini_cost = result
                except Exception as e:
                    print(f"  ⚠ {name.upper()} co-analyst thread error: {e}")

        # Log tracking data on main thread (SQLite can't cross threads)
        for output, label in [(gpt_output, "GPT"), (gemini_output, "Gemini")]:
            if output and "_tracking" in output:
                t = output.pop("_tracking")
                tracker.log_call(t["model"], t["prompt_tokens"],
                                 t["completion_tokens"], t["thinking_tokens"], t["label"])

        # Merge consensus from all available co-analysts
        if gpt_output or gemini_output:
            # If only Gemini responded (no GPT), put Gemini in the GPT slot for the merge
            if not gpt_output and gemini_output:
                analysis, merge_cost = opus_merge_consensus(analysis, gemini_output, orca_text, tracker)
            else:
                analysis, merge_cost = opus_merge_consensus(analysis, gpt_output, orca_text, tracker, gemini_output=gemini_output)

            # Re-run strike correction after merge (new trades may need correction)
            analysis, post_merge_corrections = auto_correct_strikes(analysis, orca_data)
            if post_merge_corrections:
                print(f"  📌 Post-merge: {len(post_merge_corrections)} strike(s) corrected")

            # Inject consensus labels on CONF/URG lines (programmatic, not prompt-dependent)
            analysis = inject_consensus_labels(analysis, gpt_output, gemini_output)
            label_count = analysis.count("✅ consensus") + analysis.count("✅ partial") + analysis.count("⚠️ no consensus") + analysis.count("🆕 co-analyst")
            print(f"  🏷️ Consensus labels injected: {label_count} trades tagged")
        else:
            print("  ℹ No co-analyst returned results — using Claude-only analysis")

        # ── QUALITY GATE: Block posting if not all 3 agents ran ──
        # If API keys are configured, we REQUIRE all 3 agents to succeed.
        # Partial analysis (missing an agent) should not go to subscribers.
        agents_expected = 1 + bool(has_gpt) + bool(has_gemini)
        agents_succeeded = 1  # Claude always succeeds (we're here)
        failed_agents = []
        if has_gpt:
            if gpt_output:
                agents_succeeded += 1
            else:
                failed_agents.append("GPT")
        if has_gemini:
            if gemini_output:
                agents_succeeded += 1
            else:
                failed_agents.append("Gemini")

        if failed_agents and agents_expected == 3:
            blocked = True
            block_reason = f"Agent quality gate: {', '.join(failed_agents)} failed — {agents_succeeded}/{agents_expected} agents completed"
            print(f"\n  🛑 QUALITY GATE: {block_reason}")
            print(f"     All 3 agents must complete before posting to subscribers.")
            print(f"     Failed: {', '.join(failed_agents)}")
    else:
        if not has_gpt and not has_gemini:
            print("  ℹ Co-analysts skipped (no OPENAI_API_KEY or GEMINI_API_KEY)")
        else:
            print("  ℹ Co-analysts skipped (budget cap reached)")

    # ── STEP 3.5: VALIDATION PASS — GPT + Gemini validate the merged consensus ──
    # After merge, co-analysts review ALL trades (including each other's ideas)
    # Skip if already blocked by quality gate (saves API costs)
    if blocked:
        print("\n🔍 Validation Pass — skipped (blocked by quality gate)")
    elif (has_gpt or has_gemini) and (gpt_output or gemini_output):
        print("\n🔍 Validation Pass — co-analysts checking merged consensus...")
        ok_to_validate, _ = tracker.check_budget()
        if ok_to_validate:
            validation_results = co_analyst_validation_pass(analysis, orca_text, tracker)
            # Check if any validator flagged critical issues
            for name, result in validation_results.items():
                if result.get("verdict") == "FLAG":
                    critical_issues = [i for i in result.get("issues", []) if i.get("severity") == "critical"]
                    if critical_issues:
                        print(f"  🚨 {name.upper()} flagged CRITICAL issues — these will be caught by Opus review")
        else:
            print("  ℹ Validation pass skipped (budget cap reached)")

    # ── STEP 4: OPUS REVIEW PASS — AI QA before sending to subscribers ──
    # If review returns BLOCK, we do NOT send to Telegram at all.
    # Skip if already blocked by quality gate (saves API costs)
    if not blocked:
        print("\n🔬 Opus Review Pass — checking logic before send...")
        ok_to_send, review_cost = tracker.check_budget()
    else:
        print("\n🔬 Opus Review Pass — skipped (already blocked by quality gate)")
        ok_to_send = False
    if not blocked and ok_to_send:
        review_result, review_cost = opus_review_pass(analysis, orca_text, open_positions, tracker)
        if review_result:
            print(f"  📋 Review result ({review_cost:.4f}):")
            for line in review_result.strip().split("\n"):
                print(f"     {line}")

            # Check if review found BLOCK-level issues
            if "BLOCK" in review_result.upper():
                block_reason = review_result.strip()
                print(f"\n  🛑 OPUS REVIEW flagged BLOCK-level issues")
                print(f"  Reason: {block_reason[:300]}")

                # ── SURGICAL FIX: Remove the bad trade instead of killing entire report ──
                print(f"\n  🔧 Attempting surgical fix — removing bad trade(s)...")
                ok_fix, fix_budget = tracker.check_budget()
                if ok_fix:
                    fixed_analysis, fix_cost = opus_surgical_fix(analysis, block_reason, tracker)
                    if fixed_analysis:
                        analysis = fixed_analysis
                        val_warnings.append(f"⚠️ Trade removed by QA: {block_reason[:150]}")
                        print(f"  ✅ Surgical fix succeeded ({fix_cost:.4f}) — bad trade removed, sending rest")
                    else:
                        blocked = True
                        print(f"  ❌ Surgical fix failed — falling back to full BLOCK")
                else:
                    blocked = True
                    print(f"  ❌ Budget exceeded — cannot attempt surgical fix")
            elif "FIX" in review_result.upper() or "WARNING" in review_result.upper():
                val_warnings.append(f"⚠️ Opus Review Notes: {review_result.strip()[:200]}")
        else:
            print("  ℹ Review pass skipped (budget or error)")
    else:
        print("  ℹ Review pass skipped (budget cap reached)")

    # Also attempt surgical fix if Python validation found critical issues
    critical_warnings = [w for w in val_warnings if "🛑" in w or "STRIKE MISMATCH" in w]
    if critical_warnings and not blocked:
        crit_reason = "Python validation found critical issues: " + "; ".join(critical_warnings)
        print(f"\n  🛑 PYTHON VALIDATION flagged critical issues")
        for cw in critical_warnings:
            print(f"     {cw}")

        # Try surgical fix for Python validation blocks too
        print(f"\n  🔧 Attempting surgical fix for Python validation issues...")
        ok_fix, fix_budget = tracker.check_budget()
        if ok_fix:
            fixed_analysis, fix_cost = opus_surgical_fix(analysis, crit_reason, tracker)
            if fixed_analysis:
                analysis = fixed_analysis
                val_warnings.append(f"⚠️ Trade removed by Python QA: {crit_reason[:150]}")
                print(f"  ✅ Surgical fix succeeded ({fix_cost:.4f})")
            else:
                blocked = True
                block_reason = crit_reason
                print(f"  ❌ Surgical fix failed — falling back to full BLOCK")
        else:
            blocked = True
            block_reason = crit_reason
            print(f"  ❌ Budget exceeded — cannot attempt surgical fix")

    # ── RE-SAVE analyst file with consensus labels + all modifications ──
    # The initial save (above) happens BEFORE consensus merge/labels are injected.
    # Trade logger (Stage 4) reads this file, so it MUST contain consensus labels.
    try:
        with open(analyst_file_path, "w", encoding="utf-8") as f:
            f.write(f"# ⚡ ORCA Analyst (Opus 4.6) — {datetime.date.today()} (${cost:.4f})\n\n{analysis}")
            f.write(f"\n\n---\n## News Sources ({sum(len(v) for v in news.values())} signals)\n")
            for cat, items in news.items():
                if items:
                    f.write(f"\n### {cat}\n")
                    for item in items[:5]:
                        f.write(f"- {item.get('text', item.get('signal',''))[:80]}\n")
        print(f"  📄 Analyst file re-saved with consensus labels: {analyst_file_path}")
    except Exception as e:
        print(f"  ⚠ Failed to re-save analyst file: {e}")

    if blocked:
        # Only reach here if surgical fix also failed — save and notify
        RESULTS_DIR.mkdir(exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        blocked_file = RESULTS_DIR / f"BLOCKED_{ts}.md"
        blocked_file.write_text(
            f"# 🛑 BLOCKED ANALYSIS — {datetime.date.today()}\n\n"
            f"## Block Reason\n{block_reason}\n\n"
            f"## Original Analysis\n{analysis}\n\n"
            f"## Validation Warnings\n" + "\n".join(val_warnings),
            encoding="utf-8"
        )
        print(f"\n  📄 Blocked analysis saved to {blocked_file}")
        print(f"  ❌ Telegram send SKIPPED — analysis blocked")

        # Send a brief internal notification (not the analysis itself)
        # Detect whether this was a quality gate block or a QA block
        is_agent_gate = "Agent quality gate" in block_reason
        if is_agent_gate:
            block_detail = (
                f"🛑 Today's analysis was blocked — not all analysts completed.\n"
                f"{block_reason}\n\n"
                f"No partial analysis was sent to subscribers.\n"
                f"The bot will retry on the next scheduled run."
            )
        else:
            block_detail = (
                f"🛑 Today's analysis was blocked by quality checks.\n"
                f"Surgical fix was attempted but failed.\n"
                f"Reason: {block_reason[:200]}\n\n"
                f"The bot will retry on the next scheduled run."
            )
        send_telegram(
            f"{block_detail}",
            parse_mode=None
        )
    else:
        # Convert analysis from markdown → Telegram HTML
        analysis_html = _md_to_tg_html(analysis)

        # QA validation warnings logged internally but NOT shown to subscribers
        if val_warnings:
            print("  ℹ QA flags (internal only, not shown to subscribers):")
            for w in val_warnings:
                print(f"    {w}")

        footer = ""  # Credit line already in header — no duplicate needed

        # Add Google Sheet link if configured — with explanation
        sheet_link = ""
        sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()
        if sheet_id:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            sheet_link = (
                f"\n\n📊 <a href=\"{sheet_url}\">Trade Tracker (Google Sheet)</a>"
                f"\n<i>Full audit trail: every trade with entry date, timestamps, strikes, "
                f"entry/exit prices, P&amp;L, status signals (HOLD/CLOSE/TAKE PROFIT), "
                f"and original thesis. Updated automatically after each run.</i>"
            )

        # Full disclaimer
        disclaimer = (
            "\n\n⚠️ <i>DISCLAIMER: This is options research only. "
            "Not financial advice. Not a recommendation to buy, sell, or hold any security. "
            "Options trading involves substantial risk of loss and is not suitable for all investors. "
            "Past performance does not guarantee future results. "
            "Always do your own due diligence and consult a licensed financial advisor.</i>"
        )

        tg_msg = f"{header}\n\n{analysis_html}{market_context}{footer}{sheet_link}{disclaimer}"

        send_telegram(tg_msg, parse_mode="HTML")

        # X/Twitter posting (if configured)
        if os.environ.get("X_API_KEY", "").strip():
            print("\n🐦 Posting to X/Twitter...")
            post_to_x(tg_msg)

        # Google Sheet sync (if configured)
        if os.environ.get("GOOGLE_SHEETS_CREDS", "").strip():
            print("\n📊 Syncing trades to Google Sheet...")
            try:
                from sheet_sync import sync_trades_to_sheet
                sync_trades_to_sheet()
            except Exception as e:
                print(f"  ⚠ Sheet sync error: {e}")

    tracker.print_dashboard()
    tracker.close()
    print("\n✅ Done!")

if __name__ == "__main__":
    if "--eod-review" in sys.argv:
        # EOD position review mode — triple-AI consensus on open positions
        print("""
╔═══════════════════════════════════════════════════════════════╗
║  ⚡ ORCA EOD REVIEW — Triple-AI Position Recheck              ║
║  Claude + GPT + Gemini reviewing open positions               ║
╚═══════════════════════════════════════════════════════════════╝""")
        tracker = CostTracker()
        ok, reason = tracker.check_budget()
        if not ok:
            print(f"  ❌ Budget exceeded: {reason}")
            sys.exit(1)

        # Step 1: Claude primary review of ALL positions
        print("\n  🧠 Claude EOD review...")
        claude_recs, cost = eod_position_review(tracker)

        if not claude_recs:
            print("  ℹ No recommendations generated (no open positions or error)")
            tracker.print_dashboard()
            tracker.close()
            print("\n✅ EOD review done!")
            sys.exit(0)

        # Step 2: Check if any positions need triple-AI review
        # GPT + Gemini only needed for underwater/CLOSE/ADJUST (saves API costs)
        needs_consensus = [r for r in claude_recs
                           if r.get("action", "").upper() in ("CLOSE", "ADJUST")
                           or r.get("underwater", False)]

        if needs_consensus:
            print(f"\n  🤖 {len(needs_consensus)} positions need triple-AI consensus...")

            # Build positions text for ONLY the problematic positions
            import sqlite3 as _eod_sql
            db_path = Path("orca_trade_log.db")
            problem_tickers = {r.get("ticker", "").upper() for r in needs_consensus}
            positions_text = ""
            if db_path.exists():
                conn = _eod_sql.connect(str(db_path))
                rows = conn.execute("""
                    SELECT trade_id, ticker, direction, strike, strike_2, expiry,
                           entry_price, call_date, confidence, thesis, edge,
                           underlying_at_entry, option_high_watermark, option_low_watermark
                    FROM trades WHERE status = 'OPEN' ORDER BY call_date DESC
                """).fetchall()
                conn.close()
                for row in rows:
                    (tid, ticker, direction, strike, strike2, expiry,
                     entry_price, call_date, conf, thesis, edge,
                     underlying_entry, high_wm, low_wm) = row
                    if ticker.upper() not in problem_tickers:
                        continue
                    days_held = 0
                    dte = "?"
                    try:
                        entry_dt = datetime.datetime.strptime(call_date, "%Y-%m-%d")
                        days_held = (datetime.datetime.now() - entry_dt).days
                        if expiry:
                            exp_dt = datetime.datetime.strptime(expiry, "%Y-%m-%d")
                            dte = (exp_dt - datetime.datetime.now()).days
                    except (ValueError, TypeError):
                        pass
                    strike_str = f"${strike:.0f}" if strike else "?"
                    if strike2:
                        strike_str += f"/${strike2:.0f}"
                    ep = f"${entry_price:.2f}" if entry_price else "$?"
                    positions_text += f"\n--- {ticker} {direction} {strike_str} | {expiry or '?'} ({dte}d) | Entry {ep} ({days_held}d held) | CONF {conf} ---\nThesis: {(thesis or 'N/A')[:200]}\n"

            # GPT + Gemini review ONLY the problematic positions (parallel)
            import concurrent.futures
            gpt_recs, gemini_recs = [], []
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {}
                if os.environ.get("OPENAI_API_KEY", "").strip():
                    futures["gpt"] = executor.submit(_gpt_eod_review, positions_text)
                else:
                    print("    ℹ OPENAI_API_KEY not set — GPT skipped")
                if os.environ.get("GEMINI_API_KEY", "").strip():
                    futures["gemini"] = executor.submit(_gemini_eod_review, positions_text)
                else:
                    print("    ℹ GEMINI_API_KEY not set — Gemini skipped")

                for name, future in futures.items():
                    try:
                        result = future.result(timeout=150)
                        if name == "gpt":
                            gpt_recs = result
                            print(f"    ✅ GPT: {len(result)} recommendations")
                        else:
                            gemini_recs = result
                            print(f"    ✅ Gemini: {len(result)} recommendations")
                    except Exception as e:
                        print(f"    ⚠ {name.upper()} EOD review error: {e}")

            # Merge consensus for problematic positions
            print("\n  🤝 Merging triple-AI consensus for flagged positions...")
            merged_problem = _merge_eod_consensus(needs_consensus, gpt_recs, gemini_recs)

            # Build final recs: routine HOLDs from Claude + consensus for problematic ones
            problem_map = {r["ticker"]: r for r in merged_problem}
            final_recs = []
            for rec in claude_recs:
                ticker = rec.get("ticker", "").upper()
                if ticker in problem_map:
                    final_recs.append(problem_map[ticker])
                else:
                    # Routine HOLD — Claude-only, no consensus needed
                    rec["consensus"] = "[Claude]"
                    final_recs.append(rec)
        else:
            print("  ✅ All positions healthy — Claude-only review sufficient")
            final_recs = claude_recs
            for rec in final_recs:
                rec["consensus"] = "[Claude]"

        for rec in final_recs:
            icon = {"HOLD": "✅", "CLOSE": "🛑", "ADJUST": "🔧"}.get(rec.get("action", ""), "❓")
            consensus = rec.get("consensus", "")
            print(f"    {icon} {rec.get('ticker', '?')}: {rec.get('action', '?')} {consensus} — {rec.get('reason', '')[:80]}")

        # Step 3: Send to Telegram (filtered) + save to DB (all)
        send_eod_review(final_recs)

        tracker.print_dashboard()
        tracker.close()
        print("\n✅ EOD review done!")
    else:
        main()
