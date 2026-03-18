#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORCA V3 — Trade Structurer (Ported from V2 r2_trade_structuring)
=================================================================
Takes pipeline survivors and structures them as options trades:
  1. Fetch live options chain via yfinance (zero API cost)
  2. IV/HV ratio → credit spread (sell premium) or debit spread (buy direction)
  3. Strike/expiry selection with quality ratio checks
  4. Deep ITM single-leg fallback if no spread passes quality
  5. Returns enriched survivor dict with options fields

Strategy logic (same as V2):
  Bullish + IV expensive (IV/HV > 1.30) → SELL_PUT_SPREAD (credit)
  Bullish + IV normal/cheap (≤1.30)     → BUY_CALL_SPREAD (debit)
  Bearish + IV expensive (IV/HV > 1.30) → SELL_CALL_SPREAD (credit)
  Bearish + IV normal/cheap (≤1.30)     → BUY_PUT_SPREAD (debit)

Quality checks:
  Credit spread: credit >= 45% of width
  Debit spread:  debit  <= 58% of width
  Max width: max(price * 5%, $1.00)

Conservative pricing:
  BUY  = (mid + ask) / 2   (pay slightly above mid)
  SELL = (mid + bid) / 2   (receive slightly below mid)
"""

import datetime
from typing import Optional, Dict, List, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS (same as V2)
# ─────────────────────────────────────────────────────────────────────────────
IVHV_OVERPRICED = 1.30      # IV/HV ratio threshold for credit vs debit
CREDIT_MIN_QR = 0.45        # Credit must be >= 45% of width
DEBIT_MAX_QR = 0.58         # Debit must be <= 58% of width
PRICE_HISTORY_DAYS = "90d"  # yfinance history for HV calc
VOL_WINDOW = 20             # 20-day HV window


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _repricing_window_to_dte(rw: str, spread_mode: str) -> Tuple[int, int, int]:
    """Convert repricing window text to (ideal_dte, min_dte, max_dte)."""
    rw = (rw or "").lower()
    if any(x in rw for x in ["1 day", "1-2 day", "2 day", "1 trading", "monday"]):
        return (18, 7, 21)
    elif any(x in rw for x in ["1-5 day", "2-5 day", "3-5 day", "3 day", "5 day",
                                 "1-10 day", "5-10 day", "10 day", "this week"]):
        return (28, 7, 35)
    elif any(x in rw for x in ["1 week", "1-2 week", "2 week", "1-3 week"]):
        return (37, 14, 45)
    elif any(x in rw for x in ["2-3 week", "3 week", "4 week", "month", "1 month"]):
        return (40, 21, 45)
    else:
        if spread_mode == "credit":
            return (38, 7, 45)
        else:
            return (28, 7, 35)


def _pick_deep_itm_option(opts_df, price: float, opt_type: str = "call"):
    """
    Pick the most liquid deep ITM option (2-3 strikes in the money).
    Returns (strike, bid, ask, volume, oi) or None.
    """
    if opts_df.empty or "strike" not in opts_df.columns:
        return None

    strikes_sorted = sorted(opts_df["strike"].unique())

    if opt_type == "call":
        itm = [s for s in strikes_sorted if s < price]
        if len(itm) < 2:
            return None
        candidates = []
        for depth in [2, 3, 1]:
            if depth <= len(itm):
                strike = float(itm[-depth])
                candidates.append(strike)
    else:
        itm = [s for s in strikes_sorted if s > price]
        if len(itm) < 2:
            return None
        candidates = []
        for depth in [2, 3, 1]:
            if depth <= len(itm):
                strike = float(itm[depth - 1])
                candidates.append(strike)

    best = None
    best_liquidity = -1
    for strike in candidates:
        row = opts_df.iloc[(opts_df["strike"] - strike).abs().argsort()[:1]]
        vol = float(row["volume"].values[0]) if "volume" in opts_df.columns else 0
        oi = float(row["openInterest"].values[0]) if "openInterest" in opts_df.columns else 0
        bid = float(row["bid"].values[0]) if "bid" in opts_df.columns else 0
        ask = float(row["ask"].values[0]) if "ask" in opts_df.columns else 0

        if bid <= 0:
            continue

        liquidity = vol + oi
        if liquidity > best_liquidity:
            best_liquidity = liquidity
            best = (strike, bid, ask, vol, oi)

    return best


def _fetch_ticker_data(ticker: str) -> Optional[Dict]:
    """
    Fetch price, IV, HV, IV/HV ratio, and options chains via yfinance.
    Standalone — no dependency on V2's ORCA class.
    """
    try:
        import yfinance as yf
        import numpy as np
    except ImportError as e:
        print(f"    ⚠ Missing dependency: {e}")
        return None

    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period=PRICE_HISTORY_DAYS)
        if hist.empty or len(hist) < 30:
            print(f"    ⚠ {ticker}: insufficient price history")
            return None

        price = float(hist["Close"].iloc[-1])

        # ── Historical Volatility (20-day close-to-close) ──
        log_returns = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
        if len(log_returns) >= VOL_WINDOW:
            hv_20 = float(log_returns.tail(VOL_WINDOW).std() * np.sqrt(252))
        else:
            hv_20 = float(log_returns.std() * np.sqrt(252))

        # ── Options Chains ──
        try:
            expirations = tk.options
        except Exception:
            expirations = []

        chains = {}
        for exp in (expirations or [])[:8]:
            try:
                exp_date = datetime.date.fromisoformat(exp)
                dte = (exp_date - datetime.date.today()).days
                if dte < 0:
                    continue
                chain = tk.option_chain(exp)
                chains[exp] = {
                    "calls": chain.calls,
                    "puts": chain.puts,
                    "dte": dte,
                }
            except Exception:
                continue

        if not chains:
            print(f"    ⚠ {ticker}: no options chains available")
            return None

        # ── ATM IV (nearest expiration, nearest-to-spot strike) ──
        nearest_exp = min(chains.keys(), key=lambda e: chains[e]["dte"])
        nearest = chains[nearest_exp]
        atm_iv = 0.0

        for side in ["calls", "puts"]:
            df = nearest[side]
            if df.empty or "strike" not in df.columns:
                continue
            atm_row = df.iloc[(df["strike"] - price).abs().argsort()[:1]]
            if "impliedVolatility" in df.columns:
                iv_val = float(atm_row["impliedVolatility"].values[0])
                if iv_val > 0:
                    atm_iv = max(atm_iv, iv_val)

        # Normalize: yfinance returns IV as decimal (0.42 = 42%)
        if atm_iv > 5.0:
            atm_iv = atm_iv / 100.0

        # IV/HV ratio
        iv_hv_ratio = (atm_iv / hv_20) if hv_20 > 0 else 1.0

        return {
            "price": price,
            "atm_iv": atm_iv,
            "hv_20": hv_20,
            "iv_hv_ratio": iv_hv_ratio,
            "chains": chains,
        }

    except Exception as e:
        print(f"    ⚠ Failed to fetch {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN STRUCTURING FUNCTION
# ─────────────────────────────────────────────────────────────────────────────
def structure_trade(idea: Dict) -> Dict:
    """
    Structure a single pipeline survivor as an options trade.

    Takes: survivor dict with ticker, direction, repricing_window, etc.
    Returns: enriched dict with options fields added:
        - strategy_type: BUY_CALL_SPREAD, SELL_PUT_SPREAD, BUY_CALL, etc.
        - strike: primary strike
        - strike_2: second strike (for spreads)
        - expiry: expiration date string
        - dte: days to expiration
        - entry_price: option/spread entry price
        - underlying_at_entry: stock price at time of structuring
        - spread_width: width of spread (0 for single leg)
        - quality_ratio: credit/width or debit/width
        - profit_target: dollar target for the position
        - stop_alert: dollar stop level for the position
        - iv_at_entry: ATM IV at time of structuring
        - iv_hv_ratio: IV/HV ratio used for strategy selection
        - structure_notes: human-readable summary
    """
    ticker = idea.get("ticker", "").strip().upper()
    direction_raw = idea.get("direction", "").strip().lower()
    repricing_window = idea.get("repricing_window", "")

    if not ticker:
        idea["structure_notes"] = "SKIPPED — no ticker"
        return idea

    is_bullish = "bullish" in direction_raw
    is_bearish = "bearish" in direction_raw
    if not is_bullish and not is_bearish:
        idea["structure_notes"] = "SKIPPED — unclear direction"
        return idea

    print(f"  🎯 Structuring {ticker} ({direction_raw.title()})...")

    # ── Fetch live options data ──
    data = _fetch_ticker_data(ticker)
    if not data:
        idea["structure_notes"] = f"SKIPPED — no options data for {ticker}"
        idea["underlying_at_entry"] = None
        print(f"    ⏭ No options data for {ticker}")
        return idea

    price = data["price"]
    iv = data["atm_iv"]
    hv = data["hv_20"]
    iv_hv_ratio = data["iv_hv_ratio"]

    # Always store underlying price
    idea["underlying_at_entry"] = price

    # ── Determine strategy based on direction + IV regime ──
    if is_bullish and iv_hv_ratio > IVHV_OVERPRICED:
        strategy = "SELL_PUT_SPREAD"
        opt_key = "puts"
        spread_mode = "credit"
    elif is_bullish:
        strategy = "BUY_CALL_SPREAD"
        opt_key = "calls"
        spread_mode = "debit"
    elif is_bearish and iv_hv_ratio > IVHV_OVERPRICED:
        strategy = "SELL_CALL_SPREAD"
        opt_key = "calls"
        spread_mode = "credit"
    else:
        strategy = "BUY_PUT_SPREAD"
        opt_key = "puts"
        spread_mode = "debit"

    iv_display = iv * 100 if iv < 5.0 else iv
    print(f"    IV={iv_display:.1f}% HV={hv*100:.1f}% IV/HV={iv_hv_ratio:.2f} → {strategy}")

    # ── Collect candidate expirations sorted by closeness to ideal DTE ──
    ideal_dte, min_dte, max_dte = _repricing_window_to_dte(repricing_window, spread_mode)

    candidate_exps = []
    for exp, ch in data.get("chains", {}).items():
        dte = ch["dte"]
        if min_dte <= dte <= max_dte:
            candidate_exps.append((exp, dte))

    # Widen to 7-60 DTE if nothing in primary range
    if not candidate_exps:
        for exp, ch in data.get("chains", {}).items():
            dte = ch["dte"]
            if 7 <= dte <= 60:
                candidate_exps.append((exp, dte))

    if not candidate_exps:
        idea["strategy_type"] = "UNDERLYING"
        idea["entry_price"] = price
        idea["structure_notes"] = f"UNDERLYING — no suitable expiry for {ticker}"
        print(f"    ⏭ No suitable expiry — falling back to underlying")
        return idea

    candidate_exps.sort(key=lambda x: abs(x[1] - ideal_dte))
    print(f"    📅 {len(candidate_exps)} candidate expirations: "
          f"{', '.join(f'{e}({d}d)' for e, d in candidate_exps[:5])}")

    # ── Try spread at each expiration ──
    spread_found = False
    best_exp = None
    best_dte = None
    entry_price = 0
    width = 0
    quality_ratio = 0
    strike_1 = None
    strike_2 = None
    profit_target = 0
    stop_alert = 0

    for cand_exp, cand_dte in candidate_exps:
        chain = data["chains"][cand_exp]
        opts_df = chain[opt_key]
        strikes_sorted = sorted(opts_df["strike"].unique())

        short_strike = None
        long_strike = None

        if strategy == "SELL_PUT_SPREAD":
            itm = [s for s in strikes_sorted if s >= price]
            otm = [s for s in strikes_sorted if s < price]
            if itm and otm:
                short_strike = float(itm[0])
                long_strike = float(otm[-1])
        elif strategy == "SELL_CALL_SPREAD":
            itm = [s for s in strikes_sorted if s <= price]
            otm = [s for s in strikes_sorted if s > price]
            if itm and otm:
                short_strike = float(itm[-1])
                long_strike = float(otm[0])
        elif strategy == "BUY_CALL_SPREAD":
            itm = [s for s in strikes_sorted if s <= price]
            otm = [s for s in strikes_sorted if s > price]
            if itm and otm:
                long_strike = float(itm[-1])
                short_strike = float(otm[0])
        elif strategy == "BUY_PUT_SPREAD":
            itm = [s for s in strikes_sorted if s >= price]
            otm = [s for s in strikes_sorted if s < price]
            if itm and otm:
                long_strike = float(itm[0])
                short_strike = float(otm[-1])

        if short_strike is None or long_strike is None:
            continue

        exp_width = abs(short_strike - long_strike)
        max_width = max(price * 0.05, 1.0)
        if exp_width > max_width:
            continue

        # ── Spread pricing ──
        short_row = opts_df.iloc[(opts_df["strike"] - short_strike).abs().argsort()[:1]]
        long_row = opts_df.iloc[(opts_df["strike"] - long_strike).abs().argsort()[:1]]

        short_bid = float(short_row["bid"].values[0]) if "bid" in opts_df.columns else 0
        short_ask = float(short_row["ask"].values[0]) if "ask" in opts_df.columns else 0
        long_bid = float(long_row["bid"].values[0]) if "bid" in opts_df.columns else 0
        long_ask = float(long_row["ask"].values[0]) if "ask" in opts_df.columns else 0

        short_mid = (short_bid + short_ask) / 2
        long_mid = (long_bid + long_ask) / 2

        if spread_mode == "credit":
            sell_price = round((short_mid + short_bid) / 2, 2)
            buy_price = round((long_mid + long_ask) / 2, 2)
            exp_entry = round(sell_price - buy_price, 2)
            if exp_entry <= 0:
                exp_entry = round(short_mid - long_mid, 2)
            exp_qr = round(exp_entry / exp_width, 3) if exp_width > 0 else 0

            if exp_qr < CREDIT_MIN_QR:
                continue

            spread_found = True
            best_exp = cand_exp
            best_dte = cand_dte
            entry_price = exp_entry
            width = exp_width
            quality_ratio = exp_qr
            max_loss = width - entry_price
            # Target = buy back at 15% of premium (keep 85%, avoid gamma risk)
            # e.g. sold at $0.26 → buy back at $0.04 → keep $0.22
            profit_target = round(entry_price * 0.15, 2)
            stop_alert = round(entry_price + max_loss * 0.40, 2) if max_loss > 0 else round(entry_price * 1.40, 2)
            strike_1 = min(short_strike, long_strike)
            strike_2 = max(short_strike, long_strike)
            print(f"      ✅ {cand_exp}({cand_dte}d): {strategy} "
                  f"${strike_1}/{strike_2} credit ${entry_price:.2f} QR={exp_qr:.1%}")
            break

        elif spread_mode == "debit":
            buy_price = round((long_mid + long_ask) / 2, 2)
            sell_price = round((short_mid + short_bid) / 2, 2)
            exp_entry = round(buy_price - sell_price, 2)
            if exp_entry <= 0:
                exp_entry = round(long_mid - short_mid, 2)
            exp_qr = round(exp_entry / exp_width, 3) if exp_width > 0 else 1.0

            if exp_qr > DEBIT_MAX_QR:
                continue

            spread_found = True
            best_exp = cand_exp
            best_dte = cand_dte
            entry_price = exp_entry
            width = exp_width
            quality_ratio = exp_qr
            raw_target = round(entry_price * 1.65, 2)
            profit_target = min(raw_target, round(width, 2))
            stop_alert = round(entry_price * 0.60, 2)
            strike_1 = min(long_strike, short_strike)
            strike_2 = max(long_strike, short_strike)
            print(f"      ✅ {cand_exp}({cand_dte}d): {strategy} "
                  f"${strike_1}/{strike_2} debit ${entry_price:.2f} QR={exp_qr:.1%}")
            break

    # ── Deep ITM fallback ──
    if not spread_found:
        print(f"    ⚠ No spread passed quality — falling back to deep ITM")
        best_exp, best_dte = candidate_exps[0]
        chain = data["chains"][best_exp]
        opt_type_str = "call" if is_bullish else "put"
        fallback_df = chain["calls"] if opt_type_str == "call" else chain["puts"]
        itm_pick = _pick_deep_itm_option(fallback_df, price, opt_type_str)

        if not itm_pick:
            # Final fallback: underlying
            idea["strategy_type"] = "UNDERLYING"
            idea["entry_price"] = price
            idea["iv_at_entry"] = iv_display
            idea["iv_hv_ratio"] = round(iv_hv_ratio, 2)
            idea["structure_notes"] = f"UNDERLYING — no spread or ITM option passed quality"
            print(f"    ⏭ No fallback — logging as underlying at ${price:.2f}")
            return idea

        strategy = f"BUY_{opt_type_str.upper()}"
        itm_strike, itm_bid, itm_ask, itm_vol, itm_oi = itm_pick
        itm_mid = (itm_bid + itm_ask) / 2
        entry_price = round((itm_mid + itm_ask) / 2, 2)
        profit_target = None
        stop_alert = None
        strike_1 = itm_strike
        strike_2 = None
        width = 0
        quality_ratio = 0
        print(f"    🔄 Deep ITM: {strategy} ${strike_1} @ ${entry_price:.2f}")

    # ── Enrich idea dict ──
    idea["strategy_type"] = strategy
    idea["strike"] = strike_1
    idea["strike_2"] = strike_2
    idea["expiry"] = best_exp
    idea["dte"] = best_dte
    idea["entry_price"] = entry_price
    idea["spread_width"] = width
    idea["quality_ratio"] = round(quality_ratio, 3)
    idea["profit_target"] = profit_target
    idea["stop_alert"] = stop_alert
    idea["iv_at_entry"] = round(iv_display, 1)
    idea["iv_hv_ratio"] = round(iv_hv_ratio, 2)

    # Build target/stop for DB
    if profit_target is not None:
        idea["target_price"] = profit_target
    if stop_alert is not None:
        idea["stop_price"] = stop_alert

    # Human-readable summary
    s1_fmt = f"${strike_1:.0f}" if strike_1 and strike_1 % 1 == 0 else f"${strike_1:.2f}" if strike_1 else ""
    s2_fmt = f"/${strike_2:.0f}" if strike_2 and strike_2 % 1 == 0 else f"/${strike_2:.2f}" if strike_2 else ""
    cr_dr = "cr" if "SELL" in strategy else "dr"
    idea["structure_notes"] = (
        f"{strategy} {s1_fmt}{s2_fmt} exp {best_exp} ({best_dte}d) "
        f"@ ${entry_price:.2f} {cr_dr} | QR={quality_ratio:.0%} | "
        f"IV/HV={iv_hv_ratio:.2f} | Spot=${price:.2f}"
    )

    # Update direction to strategy type for downstream (sheet, logger)
    idea["direction"] = strategy

    print(f"    ✅ {idea['structure_notes']}")
    return idea


def structure_all(survivors: List[Dict]) -> List[Dict]:
    """
    Structure all pipeline survivors. Returns the enriched list.
    """
    if not survivors:
        return survivors

    print(f"\n{'=' * 60}")
    print(f"  TRADE STRUCTURER — Options Trade Construction")
    print(f"{'=' * 60}")

    structured = []
    for idea in survivors:
        enriched = structure_trade(idea)
        structured.append(enriched)

    # Summary
    strategies = {}
    for s in structured:
        st = s.get("strategy_type", "UNKNOWN")
        strategies[st] = strategies.get(st, 0) + 1

    print(f"\n  📊 Structuring complete: {len(structured)} trades")
    for st, count in strategies.items():
        print(f"    {st}: {count}")

    return structured


if __name__ == "__main__":
    # Quick test with a sample ticker
    test_idea = {
        "ticker": "EQT",
        "direction": "Bullish",
        "confidence": "8",
        "repricing_window": "1-2 weeks",
        "catalyst": "Natural gas prices rising",
        "thesis": "Test thesis",
    }
    print("🧪 Trade Structurer — Test Run")
    result = structure_trade(test_idea)
    print(f"\n📋 Result:")
    for k, v in result.items():
        if k not in ("flow_details", "confirmation_details", "raw_text"):
            print(f"  {k}: {v}")
