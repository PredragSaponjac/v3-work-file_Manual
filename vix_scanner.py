#!/usr/bin/env python3
"""
VIX Dislocation Scanner — Macro lens for ORCA Analyst.
Computes drawdown-spread percentile of key ETFs vs ^VIX.
Shows which sectors are over/under-pricing fear relative to VIX.

Output: vix_dislocation.json — consumed by analyst.py
"""
import json
import sys
import time
import numpy as np
from datetime import datetime, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("⚠ yfinance/pandas not available")
    sys.exit(0)

# ── Key ETFs covering all major sectors + styles ──
# Kept small for speed (~20 tickers + VIX = ~30s download)
SCAN_TICKERS = {
    # Broad market
    "SPY":  "S&P 500",
    "QQQ":  "Nasdaq 100",
    "IWM":  "Russell 2000",
    # Sectors
    "XLF":  "Financials",
    "XLE":  "Energy",
    "XLK":  "Technology",
    "XLV":  "Healthcare",
    "XLU":  "Utilities",
    "XLI":  "Industrials",
    "XLP":  "Staples",
    "XLY":  "Discretionary",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    # Thematic
    "XBI":  "Biotech",
    "GDX":  "Gold Miners",
    "SMH":  "Semiconductors",
    # Fixed income / credit
    "TLT":  "Long Treasuries",
    "HYG":  "High Yield",
    # International
    "EEM":  "Emerging Markets",
    "FXI":  "China",
}

VIX_TICKER = "^VIX"
LOOKBACK_START = "2010-01-01"  # 15+ years of history for robust percentiles


def download_data(tickers: list, start: str) -> dict:
    """Download daily close data for all tickers. Returns {ticker: Series}."""
    all_tickers = tickers + [VIX_TICKER]
    print(f"  Downloading {len(all_tickers)} tickers...")

    data = {}
    # Batch download for speed
    try:
        raw = yf.download(
            all_tickers,
            start=start,
            progress=False,
            ignore_tz=True,
            auto_adjust=False,
        )
        if raw is None or raw.empty:
            return data

        for t in all_tickers:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if ("Close", t) in raw.columns:
                        s = raw[("Close", t)].dropna()
                    else:
                        continue
                else:
                    s = raw["Close"].dropna()

                if len(s) > 100:
                    data[t] = s
            except Exception:
                continue

    except Exception as e:
        print(f"  ⚠ Batch download failed: {e}")
        # Fallback: individual downloads
        for t in all_tickers:
            try:
                time.sleep(0.5)
                df = yf.download(t, start=start, progress=False, ignore_tz=True, auto_adjust=False)
                if df is not None and not df.empty:
                    if isinstance(df.columns, pd.MultiIndex):
                        s = df[("Close", t)].dropna() if ("Close", t) in df.columns else None
                    else:
                        s = df["Close"].dropna() if "Close" in df.columns else None
                    if s is not None and len(s) > 100:
                        data[t] = s
            except Exception:
                continue

    print(f"  Got data for {len(data)} tickers")
    return data


def compute_log_drawdown(series: pd.Series) -> pd.Series:
    """Log drawdown: ln(price / cumulative_max)."""
    cummax = series.cummax()
    ratio = (series / cummax).replace([np.inf, -np.inf], np.nan)
    return np.log(ratio)


def compute_dislocation(data: dict) -> list:
    """
    For each ETF, compute drawdown spread vs VIX and its historical percentile.
    Returns list of dicts sorted by percentile (most extreme first).
    """
    if VIX_TICKER not in data:
        print("  ⚠ No VIX data — cannot compute dislocations")
        return []

    vix_close = data[VIX_TICKER]
    vix_dd = compute_log_drawdown(vix_close)

    results = []
    for ticker, label in SCAN_TICKERS.items():
        if ticker not in data:
            continue

        etf_close = data[ticker]

        # Align on common dates
        common = vix_dd.index.intersection(etf_close.index)
        if len(common) < 252:  # Need at least 1 year
            continue

        vix_aligned = vix_dd.loc[common]
        etf_dd = compute_log_drawdown(etf_close.loc[common])

        # Spread = ETF drawdown - VIX drawdown
        spread = (etf_dd - vix_aligned).dropna()
        if len(spread) < 252:
            continue

        current_val = float(spread.iloc[-1])

        # Percentile: where does today sit in history?
        from scipy import stats as sp_stats
        pctl = float(sp_stats.percentileofscore(spread.values, current_val))

        # 20-day trend (is it getting more extreme or reverting?)
        if len(spread) >= 20:
            val_20d_ago = float(spread.iloc[-20])
            trend_20d = current_val - val_20d_ago
        else:
            trend_20d = 0.0

        # Current ETF drawdown (how far from its own high)
        etf_dd_current = float(etf_dd.iloc[-1]) * 100  # as percentage

        results.append({
            "ticker": ticker,
            "label": label,
            "spread": round(current_val, 4),
            "percentile": round(pctl, 1),
            "trend_20d": round(trend_20d, 4),
            "etf_drawdown_pct": round(etf_dd_current, 2),
        })

    # Sort: most extreme (far from 50%) first
    results.sort(key=lambda x: abs(x["percentile"] - 50), reverse=True)
    return results


def classify_dislocations(results: list) -> dict:
    """
    Classify into over-hedged (low percentile = pricing more fear than VIX)
    and under-hedged (high percentile = pricing less fear than VIX).
    """
    over_hedged = [r for r in results if r["percentile"] <= 15]
    under_hedged = [r for r in results if r["percentile"] >= 85]
    neutral = [r for r in results if 15 < r["percentile"] < 85]

    # Sort by extremity
    over_hedged.sort(key=lambda x: x["percentile"])
    under_hedged.sort(key=lambda x: x["percentile"], reverse=True)

    return {
        "over_hedged": over_hedged,
        "under_hedged": under_hedged,
        "neutral": neutral,
    }


def main():
    print("📊 VIX Dislocation Scanner — Macro Lens")

    data = download_data(list(SCAN_TICKERS.keys()), LOOKBACK_START)

    if VIX_TICKER not in data:
        print("  ⚠ Could not download VIX data — skipping scanner")
        with open("vix_dislocation.json", "w") as f:
            json.dump({"error": "no VIX data"}, f)
        return

    results = compute_dislocation(data)
    if not results:
        print("  ⚠ No dislocation results")
        with open("vix_dislocation.json", "w") as f:
            json.dump({"error": "no results"}, f)
        return

    classified = classify_dislocations(results)

    output = {
        "date": datetime.today().strftime("%Y-%m-%d"),
        "vix_last": round(float(data[VIX_TICKER].iloc[-1]), 2),
        "total_etfs": len(results),
        "over_hedged": classified["over_hedged"],
        "under_hedged": classified["under_hedged"],
        "all_results": results,
    }

    with open("vix_dislocation.json", "w") as f:
        json.dump(output, f, indent=2)

    # Print summary
    print(f"  VIX: {output['vix_last']}")
    print(f"  Scanned: {len(results)} ETFs")
    if classified["over_hedged"]:
        tickers = [f"{r['ticker']}(p{r['percentile']})" for r in classified["over_hedged"]]
        print(f"  🔴 Over-hedged (pricing MORE fear than VIX): {', '.join(tickers)}")
    if classified["under_hedged"]:
        tickers = [f"{r['ticker']}(p{r['percentile']})" for r in classified["under_hedged"]]
        print(f"  🟢 Under-hedged (pricing LESS fear than VIX): {', '.join(tickers)}")
    print(f"  📁 Saved: vix_dislocation.json")


if __name__ == "__main__":
    main()
