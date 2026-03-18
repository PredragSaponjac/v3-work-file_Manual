#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 SHEET SYNC — Google Sheets Trade Tracker
Matches V2 visual format: dark navy header, alternating rows,
conditional P&L coloring, MAE tracking, professional legend tab.
"""

import os
import json
import base64
import sqlite3
import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = datetime.timezone(datetime.timedelta(hours=-5))

# .env auto-loader
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip()

DB_PATH = Path(__file__).parent / "orca_v3_trades.db"
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
CREDS_B64 = os.environ.get("GOOGLE_SHEETS_CREDS", "")


def _get_gspread_client():
    """Create a gspread client from base64-encoded service account credentials."""
    if not CREDS_B64:
        return None, "No GOOGLE_SHEETS_CREDS environment variable"
    if not SHEET_ID:
        return None, "No GOOGLE_SHEET_ID environment variable"

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        return None, "gspread or google-auth not installed (pip install gspread google-auth)"

    try:
        clean_b64 = CREDS_B64
        for marker in ["-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"]:
            clean_b64 = clean_b64.replace(marker, "")
        clean_b64 = "".join(clean_b64.split())

        raw_bytes = base64.b64decode(clean_b64)
        creds_json = json.loads(raw_bytes)
        print(f"  🔑 CREDS_B64 length: {len(CREDS_B64)}, cleaned: {len(clean_b64)}")
        print(f"  📋 SHEET_ID: {SHEET_ID[:10]}...")
        print(f"  ✅ Credentials decoded — project: {creds_json.get('project_id', '?')}, "
              f"email: {creds_json.get('client_email', '?')}")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
        client = gspread.authorize(credentials)
        return client, None
    except Exception as e:
        return None, f"Failed to create gspread client: {e}"


def _col_letter(idx):
    """Convert 0-based column index to letter (0=A, 25=Z, 26=AA)."""
    if idx < 26:
        return chr(65 + idx)
    return chr(64 + idx // 26) + chr(65 + idx % 26)


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN LAYOUT — clean, trader-friendly order (matching V2 style)
# ─────────────────────────────────────────────────────────────────────────────
HEADERS = [
    # ── Trade Essentials (what you need at a glance) ──
    "Date",            # A
    "Ticker",          # B
    "Direction",       # C
    "Strikes",         # D  (e.g. "$45/$47" or "$42")
    "Entry $",         # E  (option price with cr/dr)
    "Expiry",          # F
    "DTE",             # G
    "Spot $",          # H  (underlying price at entry)
    "Target $",        # I
    "Stop $",          # J
    # ── Live Performance ──
    "Status",          # K
    "P&L %",           # L
    "P&L $",           # M
    "Signal",          # N
    # ── Catalyst Intelligence ──
    "Conf",            # O  (confidence score)
    "Tape Read",       # P  (UW flow classification)
    "CDS",             # Q  (Catalyst Decay Score 0-100)
    "Cat. Health",     # R  (STRENGTHENING/STABLE/FADING/COLLIDING)
    "Catalyst",        # S
    "Thesis",          # T
    "Repricing",       # U  (repricing window)
    # ── Risk Analytics ──
    "MAE",             # V  (Maximum Adverse Excursion)
    "Notes",           # W
]

NUM_COLS = len(HEADERS)

# Column widths in pixels
COL_WIDTHS = [
    85,   # Date
    55,   # Ticker
    120,  # Direction (longer for "Call Debit Spread")
    85,   # Strikes
    75,   # Entry $
    85,   # Expiry
    40,   # DTE
    70,   # Spot $
    70,   # Target $
    70,   # Stop $
    70,   # Status
    65,   # P&L %
    70,   # P&L $
    140,  # Signal
    40,   # Conf
    120,  # Tape Read
    40,   # CDS
    110,  # Cat. Health
    250,  # Catalyst
    300,  # Thesis
    110,  # Repricing
    85,   # MAE
    200,  # Notes
]


def sync_trades_to_sheet():
    """
    Full sync: DB is source of truth.
    Matches V2's professional formatting: dark navy header, alternating rows,
    conditional P&L coloring, computed MAE, comprehensive legend tab.
    """
    if not DB_PATH.exists():
        print("  ℹ No trade log database — skipping sheet sync")
        return False

    client, error = _get_gspread_client()
    if not client:
        print(f"  ℹ Sheet sync skipped: {error}")
        return False

    try:
        sheet = client.open_by_key(SHEET_ID)
        print(f"  📊 Opening sheet by key (first 10 chars): {SHEET_ID[:10]}...")
        print(f"  ✅ Sheet opened: {sheet.title}")

        if sheet.title != "ORCA V3 Trade Tracker":
            try:
                sheet.update_title("ORCA V3 Trade Tracker")
                print("  📝 Spreadsheet renamed to 'ORCA V3 Trade Tracker'")
            except Exception:
                pass
    except Exception as e:
        print(f"  ❌ Cannot open Google Sheet: {e}")
        return False

    # Get or create "Trades" worksheet
    try:
        ws = sheet.worksheet("Trades")
    except Exception:
        try:
            ws = sheet.add_worksheet(title="Trades", rows=500, cols=NUM_COLS)
        except Exception as e:
            print(f"  ❌ Cannot create worksheet: {e}")
            return False

    # Clean up default "Sheet1" tab if present
    try:
        for tab in sheet.worksheets():
            if tab.title not in {"Trades", "Legend"}:
                try:
                    sheet.del_worksheet(tab)
                except Exception:
                    pass
    except Exception:
        pass

    # ── Query trades from DB ──
    trades, stats = _query_all_trades()
    total = stats["total"]
    open_count = stats["open_count"]
    closed = stats["closed_count"]
    winners = stats["winners"]
    win_rate = stats["win_rate"]
    avg_pnl = stats["avg_pnl"]
    has_eod = stats.get("has_eod", False)
    has_strategy = stats.get("has_strategy", False)

    now = datetime.datetime.now(ET)
    rows = []

    for t in trades:
        (tid, call_date, call_ts, ticker, direction,
         entry_price, underlying, target_price, stop_price,
         confidence, catalyst, thesis, repricing_window,
         tape_read, catalyst_health, cds_score,
         status, pnl_pct, pnl_dollar,
         high_wm, high_wm_date, low_wm, low_wm_date,
         close_date, close_reason, notes) = t[:26]

        # EOD fields (offset depends on whether options fields exist)
        base_idx = 26
        eod_action = None
        eod_comment = None
        eod_date = None
        strategy_type = None
        strike = None
        strike_2 = None
        expiry = None
        dte_val = None
        spread_width = None
        quality_ratio_val = None
        iv_at_entry = None
        iv_hv_ratio_val = None

        if has_eod:
            eod_action = t[base_idx] if len(t) > base_idx else None
            eod_comment = t[base_idx + 1] if len(t) > base_idx + 1 else None
            eod_date = t[base_idx + 2] if len(t) > base_idx + 2 else None
            base_idx += 3

        if has_strategy:
            strategy_type = t[base_idx] if len(t) > base_idx else None
            strike = t[base_idx + 1] if len(t) > base_idx + 1 else None
            strike_2 = t[base_idx + 2] if len(t) > base_idx + 2 else None
            expiry = t[base_idx + 3] if len(t) > base_idx + 3 else None
            dte_val = t[base_idx + 4] if len(t) > base_idx + 4 else None
            spread_width = t[base_idx + 5] if len(t) > base_idx + 5 else None
            quality_ratio_val = t[base_idx + 6] if len(t) > base_idx + 6 else None
            iv_at_entry = t[base_idx + 7] if len(t) > base_idx + 7 else None
            iv_hv_ratio_val = t[base_idx + 8] if len(t) > base_idx + 8 else None

        # Direction display — use strategy_type if available, else direction
        display_dir = strategy_type or direction or ""
        dir_map = {
            "BUY_CALL": "Buy Call",
            "BUY_PUT": "Buy Put",
            "SELL_CALL": "Sell Call",
            "SELL_PUT": "Sell Put",
            "BUY_CALL_SPREAD": "Buy Call Spread (debit)",
            "BUY_PUT_SPREAD": "Buy Put Spread (debit)",
            "SELL_CALL_SPREAD": "Sell Call Spread (credit)",
            "SELL_PUT_SPREAD": "Sell Put Spread (credit)",
            "UNDERLYING": "Underlying",
            "BULLISH": "Bullish",
            "BEARISH": "Bearish",
            "Bullish": "Bullish",
            "Bearish": "Bearish",
        }
        dir_display = dir_map.get(display_dir, display_dir or "")

        # Strikes display
        strikes_display = ""
        if strike:
            s1 = f"${strike:.0f}" if strike % 1 == 0 else f"${strike:.2f}"
            if strike_2:
                s2 = f"/${strike_2:.0f}" if strike_2 % 1 == 0 else f"/${strike_2:.2f}"
                strikes_display = f"{s1}{s2}"
            else:
                strikes_display = s1

        # Entry price formatting
        entry_display = ""
        if entry_price:
            st = strategy_type or direction or ""
            if "SELL" in str(st):
                entry_display = f"${entry_price:.2f} cr"
            elif "BUY" in str(st):
                entry_display = f"${entry_price:.2f} dr"
            else:
                entry_display = f"${entry_price:.2f}"

        # Expiry / DTE display
        expiry_display = expiry or ""
        dte_display = str(int(dte_val)) if dte_val else ""

        # Spot (underlying at entry)
        spot_display = f"${underlying:.2f}" if underlying else ""

        # Signal — direction-aware thresholds (matching V2 logic)
        is_naked = (strategy_type or direction) in ("BUY_CALL", "BUY_PUT")
        signal = ""
        if status == "OPEN":
            if pnl_pct is not None:
                if is_naked:
                    if pnl_pct >= 50:
                        signal = "💰 TRAIL TIGHT"
                    elif pnl_pct >= 30:
                        signal = "🎯 TRAIL STOP"
                    elif pnl_pct >= 15:
                        signal = "🛡️ SL→ENTRY"
                    elif pnl_pct <= -30:
                        signal = "🔴 DEEP LOSS"
                    elif pnl_pct <= -15:
                        signal = "🟡 THESIS CHECK"
                    else:
                        signal = "⚪ HOLD"
                else:
                    # Spreads / underlying
                    if pnl_pct >= 50:
                        signal = "🟢 TAKE PROFIT"
                    elif pnl_pct >= 20:
                        signal = "🟡 TRAIL STOP"
                    elif pnl_pct <= -40:
                        signal = "🔴 STOP HIT"
                    elif pnl_pct <= -30:
                        signal = "🟠 THESIS REVIEW"
                    elif pnl_pct <= -20:
                        signal = "🟠 RECHECK"
                    else:
                        signal = "⚪ HOLD"
            else:
                signal = "⚪ HOLD"
        elif status == "CLOSED":
            signal = "✅ CLOSED"
        elif status == "EXPIRED":
            signal = "⏰ EXPIRED"
        elif status == "STOPPED":
            signal = "🛑 STOPPED"

        # CDS display
        cds_display = ""
        if cds_score:
            try:
                cds_val = int(float(cds_score))
                cds_display = str(cds_val)
            except (ValueError, TypeError):
                cds_display = str(cds_score)

        # MAE (Maximum Adverse Excursion) — worst drawdown from entry
        mae_display = ""
        if entry_price and entry_price > 0 and low_wm is not None and low_wm > 0:
            if direction and "SELL" in str(direction):
                if high_wm is not None and high_wm > 0:
                    mae_pct = -((high_wm - entry_price) / entry_price * 100)
                    if mae_pct < 0:
                        mae_display = f"{mae_pct:.0f}%"
                        if high_wm_date:
                            try:
                                mae_dt = str(high_wm_date).split("T")[0] if "T" in str(high_wm_date) else str(high_wm_date)[:10]
                                mae_display += f" ({mae_dt[5:]})"
                            except Exception:
                                pass
            else:
                mae_pct = ((low_wm - entry_price) / entry_price * 100)
                if mae_pct < 0:
                    mae_display = f"{mae_pct:.0f}%"
                    if low_wm_date:
                        try:
                            mae_dt = str(low_wm_date).split("T")[0] if "T" in str(low_wm_date) else str(low_wm_date)[:10]
                            mae_display += f" ({mae_dt[5:]})"
                        except Exception:
                            pass

        def _trunc(val, maxlen=150):
            if not val:
                return ""
            val = str(val)
            return (val[:maxlen] + "…") if len(val) > maxlen else val

        # Notes — combine EOD action/comment if present
        notes_display = _trunc(notes, 100)
        if eod_action and eod_action != "HOLD":
            notes_display = f"[{eod_action}] {_trunc(eod_comment, 80)}"

        rows.append([
            # ── Trade Essentials ──
            call_date or "",
            ticker or "",
            dir_display,
            strikes_display,
            entry_display,
            expiry_display,
            dte_display,
            spot_display,
            f"${target_price:.2f}" if target_price else "",
            f"${stop_price:.2f}" if stop_price else "",
            # ── Live Performance ──
            status or "",
            f"{pnl_pct:+.1f}%" if pnl_pct is not None else "",
            f"${pnl_dollar:+.2f}" if pnl_dollar is not None else "",
            signal,
            # ── Catalyst Intelligence ──
            str(confidence) if confidence else "",
            tape_read or "",
            cds_display,
            catalyst_health or "",
            _trunc(catalyst, 100),
            _trunc(thesis, 180),
            repricing_window or "",
            # ── Risk Analytics ──
            mae_display,
            notes_display,
        ])

    num_cols = len(HEADERS)
    last_col = _col_letter(num_cols - 1)

    # ── Title row ──
    title = [
        "⚡ ORCA V3 Trade Tracker",
        f"Updated {now.strftime('%b %d, %Y %I:%M %p')} ET",
    ] + [""] * (num_cols - 2)

    # ── Summary row ──
    summary = [
        f"📊 {total} Trades",
        f"{open_count} Active",
        f"{closed} Closed",
        f"Win Rate {win_rate:.0f}%",
        f"Avg P&L {avg_pnl:+.1f}%",
        f"{winners}W / {closed - winners}L",
    ] + [""] * (num_cols - 6)

    # ── Write to sheet ──
    try:
        ws.clear()
        all_data = [title, summary, HEADERS] + rows
        ws.update(range_name="A1", values=all_data)

        # ── Professional formatting (matching V2 exactly) ──
        try:
            ws_id = ws.id
            requests = []

            # ── Column widths ──
            for i, w in enumerate(COL_WIDTHS):
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_id,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": w},
                        "fields": "pixelSize",
                    }
                })

            # ── Row heights ──
            # Title row: taller
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            })
            # Data rows: comfortable height
            if len(rows) > 0:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws_id, "dimension": "ROWS", "startIndex": 3, "endIndex": 3 + len(rows)},
                        "properties": {"pixelSize": 28},
                        "fields": "pixelSize",
                    }
                })

            # ── Alternating row colors for data rows ──
            for r_idx in range(len(rows)):
                row_num = r_idx + 4  # data starts at row 4 (0-indexed: 3)
                if r_idx % 2 == 1:
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": ws_id,
                                "startRowIndex": row_num - 1,
                                "endRowIndex": row_num,
                                "startColumnIndex": 0,
                                "endColumnIndex": num_cols,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.96, "green": 0.97, "blue": 0.99},
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    })

            # ── Conditional P&L colors on data rows ──
            pnl_col_idx = HEADERS.index("P&L %")
            for r_idx, row in enumerate(rows):
                pnl_val = row[pnl_col_idx]
                row_num = r_idx + 4  # 1-indexed
                if pnl_val and pnl_val.startswith("+") and pnl_val != "+0.0%":
                    # Green text for positive P&L
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": ws_id,
                                "startRowIndex": row_num - 1,
                                "endRowIndex": row_num,
                                "startColumnIndex": pnl_col_idx,
                                "endColumnIndex": pnl_col_idx + 2,  # P&L % and P&L $
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "textFormat": {"foregroundColor": {"red": 0.1, "green": 0.55, "blue": 0.15}, "bold": True},
                                }
                            },
                            "fields": "userEnteredFormat.textFormat",
                        }
                    })
                elif pnl_val and pnl_val.startswith("-"):
                    # Red text for negative P&L
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": ws_id,
                                "startRowIndex": row_num - 1,
                                "endRowIndex": row_num,
                                "startColumnIndex": pnl_col_idx,
                                "endColumnIndex": pnl_col_idx + 2,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "textFormat": {"foregroundColor": {"red": 0.8, "green": 0.1, "blue": 0.1}, "bold": True},
                                }
                            },
                            "fields": "userEnteredFormat.textFormat",
                        }
                    })

            # Execute all batch requests
            if requests:
                sheet.batch_update({"requests": requests})

            # ── Cell formatting (uses gspread format API) ──

            # Title row — dark navy, white text, large bold
            ws.format(f"A1:{last_col}1", {
                "textFormat": {"bold": True, "fontSize": 14, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "backgroundColor": {"red": 0.12, "green": 0.17, "blue": 0.30},
                "horizontalAlignment": "LEFT",
                "verticalAlignment": "MIDDLE",
            })

            # Summary row — light blue bg
            ws.format(f"A2:{last_col}2", {
                "textFormat": {"bold": True, "fontSize": 10},
                "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
                "verticalAlignment": "MIDDLE",
            })

            # Header row — medium gray, bold, centered, borders
            ws.format(f"A3:{last_col}3", {
                "textFormat": {"bold": True, "fontSize": 10},
                "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "borders": {
                    "bottom": {"style": "SOLID", "color": {"red": 0.5, "green": 0.5, "blue": 0.55}},
                },
            })

            # Data rows — default styling
            if len(rows) > 0:
                last_data_row = 3 + len(rows)
                ws.format(f"A4:{last_col}{last_data_row}", {
                    "textFormat": {"fontSize": 10},
                    "verticalAlignment": "MIDDLE",
                    "wrapStrategy": "CLIP",
                })

                # Ticker column — bold
                ws.format(f"B4:B{last_data_row}", {
                    "textFormat": {"bold": True, "fontSize": 10},
                })

                # Center-align short columns
                for col_name in ["Status", "Conf", "CDS", "Cat. Health", "MAE"]:
                    if col_name in HEADERS:
                        ci = HEADERS.index(col_name)
                        cl = _col_letter(ci)
                        ws.format(f"{cl}4:{cl}{last_data_row}", {
                            "horizontalAlignment": "CENTER",
                        })

                # Right-align money columns
                for col_name in ["Entry $", "Target $", "Stop $", "P&L %", "P&L $"]:
                    if col_name in HEADERS:
                        ci = HEADERS.index(col_name)
                        cl = _col_letter(ci)
                        ws.format(f"{cl}4:{cl}{last_data_row}", {
                            "horizontalAlignment": "RIGHT",
                        })

            # Freeze first 3 rows + first 2 columns (Date, Ticker always visible)
            ws.freeze(rows=3, cols=2)

        except Exception as fmt_err:
            print(f"  ⚠ Formatting partially failed: {fmt_err}")

        print(f"  ✅ Google Sheet synced: {total} trades ({open_count} open, {closed} closed)")
        print(f"     Win rate: {win_rate:.0f}% | Avg P&L: {avg_pnl:+.1f}%")

        # Write legend tab
        try:
            _write_legend_tab(sheet)
        except Exception as e:
            print(f"  ⚠ Legend tab failed (non-critical): {e}")

        return True

    except Exception as e:
        print(f"  ❌ Sheet write failed: {e}")
        return False


def _query_all_trades():
    """Query all trades from the database, ordered newest first."""
    if not DB_PATH.exists():
        return [], {"total": 0, "open_count": 0, "closed_count": 0,
                    "winners": 0, "win_rate": 0, "avg_pnl": 0}

    conn = sqlite3.connect(str(DB_PATH))

    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if "trade_id" not in existing_cols:
        conn.close()
        return [], {"total": 0, "open_count": 0, "closed_count": 0,
                    "winners": 0, "win_rate": 0, "avg_pnl": 0}

    has_eod = "eod_action" in existing_cols
    eod_select = ", eod_action, eod_comment, eod_date" if has_eod else ""

    # Options fields (may not exist in older DBs)
    has_strategy = "strategy_type" in existing_cols
    opts_select = ""
    if has_strategy:
        opts_select = ", strategy_type, strike, strike_2, expiry, dte, spread_width, quality_ratio, iv_at_entry, iv_hv_ratio"

    trades = conn.execute(f"""
        SELECT
            trade_id, call_date, call_timestamp, ticker, direction,
            entry_price, underlying_at_entry, target_price, stop_price,
            confidence, catalyst, thesis, repricing_window,
            tape_read, catalyst_health, cds_score,
            status, pnl_percent, pnl_dollar,
            high_watermark, high_watermark_date,
            low_watermark, low_watermark_date,
            close_date, close_reason, notes{eod_select}{opts_select}
        FROM trades
        ORDER BY call_date DESC, trade_id DESC
    """).fetchall()

    # Summary stats
    total = len(trades)
    open_count = sum(1 for t in trades if t[16] == "OPEN")
    closed_list = [t for t in trades if t[16] in ("CLOSED", "EXPIRED", "STOPPED")]
    winners = sum(1 for t in closed_list if t[17] and t[17] > 0)
    win_rate = (winners / len(closed_list) * 100) if closed_list else 0
    avg_pnl = sum(t[17] for t in closed_list if t[17] is not None) / len(closed_list) if closed_list else 0

    conn.close()

    stats = {
        "total": total,
        "open_count": open_count,
        "closed_count": len(closed_list),
        "winners": winners,
        "win_rate": win_rate,
        "avg_pnl": avg_pnl,
        "has_eod": has_eod,
        "has_strategy": has_strategy,
    }

    return trades, stats


def _write_legend_tab(spreadsheet):
    """Create/update a 'Legend' tab explaining signals and columns."""
    try:
        ws = spreadsheet.worksheet("Legend")
    except Exception:
        ws = spreadsheet.add_worksheet(title="Legend", rows=60, cols=3)

    legend_data = [
        ["📖 ORCA V3 Trade Tracker — Legend", "", ""],
        ["", "", ""],
        ["SIGNAL GUIDE", "", ""],
        ["Signal", "Trigger", "Action"],
        ["", "── SPREADS / UNDERLYING ──", ""],
        ["🟢 TAKE PROFIT", "P&L ≥ +50%", "Close position, book profits"],
        ["🟡 TRAIL STOP", "P&L +20% to +50%", "Tighten stop, let it run"],
        ["⚪ HOLD", "P&L -20% to +20%", "Hold, thesis intact"],
        ["🟠 RECHECK", "P&L -20% to -30%", "Review thesis validity"],
        ["🟠 THESIS REVIEW", "P&L -30% to -40%", "Thesis under pressure"],
        ["🔴 STOP HIT", "P&L ≤ -40%", "Close, stop loss triggered"],
        ["", "── NAKED OPTIONS ──", ""],
        ["💰 TRAIL TIGHT", "P&L ≥ +50%", "Tighten trail stop. SL at +30% min."],
        ["🎯 TRAIL STOP", "P&L +30% to +50%", "Trail SL above entry (+15% level)"],
        ["🛡️ SL→ENTRY", "P&L +15% to +30%", "Move SL to entry = breakeven. Let ride."],
        ["🟡 THESIS CHECK", "P&L -15% to -30%", "Review thesis. Consider SL at -30%."],
        ["🔴 DEEP LOSS", "P&L ≤ -30%", "Strongly consider exit."],
        ["", "", ""],
        ["CATALYST INTELLIGENCE", "", ""],
        ["Field", "Description", "Values"],
        ["Tape Read", "UW flow interpretation", "SUPPORTIVE / NEUTRAL / MIXED / CONTRADICTORY"],
        ["CDS", "Catalyst Decay Score (0-100)", "80-100 CONFIRM, 60-79 HOLD, 40-59 DOWNGRADE, <40 KILL"],
        ["Cat. Health", "Composite catalyst state", "STRENGTHENING / STABLE / FADING / COLLIDING"],
        ["", "", ""],
        ["COLUMN DESCRIPTIONS", "", ""],
        ["Column", "Description", ""],
        ["Date", "Date the trade idea was generated", ""],
        ["Ticker", "Stock/ETF symbol", ""],
        ["Direction", "Options strategy or Bullish/Bearish", "Call Debit Spread, Put Credit Spread, Buy Call, etc."],
        ["Strikes", "Option strike(s): $K1/$K2 for spreads, $K for singles", ""],
        ["Entry $", "Option entry price (dr = debit paid, cr = credit received)", ""],
        ["Expiry", "Options expiration date", ""],
        ["DTE", "Days to expiration at entry", ""],
        ["Spot $", "Underlying stock price at entry", ""],
        ["Target $", "P&L target for the options position", ""],
        ["Stop $", "Stop loss level for the options position", ""],
        ["Status", "OPEN, CLOSED, EXPIRED, or STOPPED", ""],
        ["P&L %", "Profit/loss percentage (green = profit, red = loss)", ""],
        ["P&L $", "Profit/loss in dollars", ""],
        ["Signal", "Action signal based on current P&L (see above)", ""],
        ["Conf", "R1 confidence level (1-10)", ""],
        ["Tape Read", "Unusual Whales flow classification", ""],
        ["CDS", "Catalyst Decay Score — higher = healthier catalyst", ""],
        ["Cat. Health", "Overall catalyst health state", ""],
        ["Catalyst", "The specific event driving the trade", ""],
        ["Thesis", "Core reasoning (truncated)", ""],
        ["Repricing", "Expected timeframe for market to reprice", ""],
        ["MAE", "Maximum Adverse Excursion — worst drawdown from entry (date)", ""],
        ["Notes", "System flags and EOD review comments", ""],
        ["", "", ""],
        ["STRATEGY SELECTION", "", ""],
        ["Condition", "Strategy", "Notes"],
        ["Bullish + IV/HV > 1.30", "Sell Put Spread (credit)", "IV expensive → sell premium"],
        ["Bullish + IV/HV ≤ 1.30", "Buy Call Spread (debit)", "IV normal/cheap → buy direction"],
        ["Bearish + IV/HV > 1.30", "Sell Call Spread (credit)", "IV expensive → sell premium"],
        ["Bearish + IV/HV ≤ 1.30", "Buy Put Spread (debit)", "IV normal/cheap → buy direction"],
        ["No spread passes QR", "Deep ITM single leg", "Fallback: Buy Call or Buy Put 2-3 strikes ITM"],
        ["No options available", "Underlying", "Last resort: stock price entry"],
        ["", "", ""],
        ["MAE GUIDE", "", ""],
        ["MAE Range", "Meaning", "Action"],
        ["0% to -15%", "Normal noise", "Hold — thesis intact"],
        ["-15% to -25%", "Moderate heat", "Monitor closely"],
        ["-25% to -35%", "Significant drawdown", "Recheck thesis validity"],
        ["-35%+", "Near stop territory", "Prepare to exit"],
    ]

    ws.clear()
    ws.update(range_name="A1", values=legend_data)

    try:
        ws.format("A1:C1", {
            "textFormat": {"bold": True, "fontSize": 14, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "backgroundColor": {"red": 0.12, "green": 0.17, "blue": 0.30},
        })
        ws.format("A3:C3", {
            "textFormat": {"bold": True, "fontSize": 11},
            "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
        })
        ws.format("A4:C4", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
            "horizontalAlignment": "CENTER",
        })
        ws.format("A12:C12", {
            "textFormat": {"bold": True, "fontSize": 11},
            "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
        })
        ws.format("A13:C13", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
            "horizontalAlignment": "CENTER",
        })
        ws.format("A19:C19", {
            "textFormat": {"bold": True, "fontSize": 11},
            "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
        })
        ws.format("A20:C20", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
            "horizontalAlignment": "CENTER",
        })
        ws.format("A25:C25", {
            "textFormat": {"bold": True, "fontSize": 11},
            "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
        })
        ws.format("A26:C26", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
            "horizontalAlignment": "CENTER",
        })
        ws.format("A45:C45", {
            "textFormat": {"bold": True, "fontSize": 11},
            "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 1.0},
        })
        ws.format("A46:C46", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.88},
            "horizontalAlignment": "CENTER",
        })
        ws.format("A5:A17", {"textFormat": {"bold": True}})
        ws.format("A21:A23", {"textFormat": {"bold": True}})
        ws.format("A27:A43", {"textFormat": {"bold": True}})
        ws.format("A47:A51", {"textFormat": {"bold": True}})
        ws.freeze(rows=1)

        # Set column widths for legend
        requests = []
        ws_id = ws.id
        for i, w in enumerate([180, 300, 250]):
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize",
                }
            })
        if requests:
            spreadsheet.batch_update({"requests": requests})

    except Exception:
        pass

    print("  📖 Legend tab updated")


def get_sheet_url():
    """Return the public URL for the Google Sheet."""
    if SHEET_ID:
        return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    return None


if __name__ == "__main__":
    print("⚡ ORCA V3 Sheet Sync — Syncing trades to Google Sheets...")
    success = sync_trades_to_sheet()
    if success:
        url = get_sheet_url()
        print(f"\n📊 Sheet URL: {url}")
        print("ℹ  Make sure the sheet is shared as 'Anyone with the link can VIEW'")
    else:
        print("\n⚠ Sheet sync failed or skipped")
