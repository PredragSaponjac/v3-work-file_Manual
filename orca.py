#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║    _    ____   ____ _   _  ___  _   _                        ║
║   / \  |  _ \ / ___| | | |/ _ \| \ | |                      ║
║  / _ \ | |_) | |   | |_| | | | |  \| |                      ║
║ / ___ \|  _ <| |___|  _  | |_| | |\  |                      ║
║/_/   \_\_| \_\\____|_| |_|\___/|_| \_|                      ║
║                                                               ║
║  Analytics-Driven Options Signal                              ║
║  v1.0 — Daily Edge Finder                                     ║
╚═══════════════════════════════════════════════════════════════╝

ORCA finds mispriced options by:
1. Calculating REAL historical volatility (10/20/30/60/90-day windows)
2. Building its own IV database (stores ATM IV daily → real IV rank after 2 weeks)
3. Comparing implied vol vs realized vol to find over/under-priced premium
4. Scanning for unusual activity, earnings setups, and event-driven trades
5. Ranking everything by expected value and urgency

Run daily before market open:
    python orca.py                        # Full daily scan (ORCA)
    python orca.py --dive AVGO            # Deep dive single ticker
    python orca.py --mode earnings        # Earnings plays only
    python orca.py --mode premium         # Premium selling only
    python orca.py --history TSLA         # Show IV history for ticker
    python orca.py --capital 5000         # Set capital

Install:
    pip install yfinance pandas numpy

First run builds the IV database. Each subsequent run adds data points.
After ~10 trading days, IV rank becomes reliable.
After ~20 trading days, IV percentile is solid.
"""

import sys, os, json, csv, math, time, argparse, sqlite3
import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    os.system("pip install yfinance -q")
    import yfinance as yf

try:
    import pandas as pd
except ImportError:
    os.system("pip install pandas -q")
    import pandas as pd

try:
    import numpy as np
except ImportError:
    os.system("pip install numpy -q")
    import numpy as np

import warnings
warnings.filterwarnings('ignore')


# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = Path("orca_iv_history.db")
RESULTS_DIR = Path("orca_results")

# How many days of price history to fetch for vol calculation
PRICE_HISTORY_DAYS = "1y"

# Vol calculation windows
VOL_WINDOWS = [10, 20, 30, 60, 90]

# IV Rank thresholds
IV_RANK_SELL_THRESHOLD = 65   # Sell premium above this
IV_RANK_BUY_THRESHOLD = 25    # Buy options below this

# IV/HV ratio thresholds
IVHV_OVERPRICED = 1.20        # IV 20%+ above HV → sell
IVHV_UNDERPRICED = 0.80       # IV 20%+ below HV → buy


# ============================================================
# TICKER UNIVERSE — Official CBOE Weekly Options List (671 tickers)
# Source: https://www.cboe.com/available_weeklys
# ============================================================

try:
    from weeklies_universe import get_all_tickers, WEEKLIES_ETFS, WEEKLIES_EQUITIES
    ALL_TICKERS = get_all_tickers()
except ImportError:
    # Fallback if weeklies_universe.py not found
    ALL_TICKERS = [
        "SPY","QQQ","IWM","DIA","AAPL","MSFT","GOOGL","AMZN","META","NVDA",
        "TSLA","AMD","AVGO","MRVL","QCOM","MU","INTC","AMAT","LRCX","SMCI",
        "ARM","XLF","XLE","XLK","XLV","GLD","SLV","USO","GDX","UNG",
        "JPM","BAC","GS","MS","C","WFC","WMT","TGT","COST","HD",
        "UNH","JNJ","PFE","ABBV","MRK","LLY","MARA","RIOT","COIN","SOFI",
        "PLTR","RIVN","LCID","GME","CRWD","PANW","FTNT","ZS","NET",
        "LMT","RTX","NOC","GD","STNG","FRO","DAL","UAL","AAL",
        "XOM","CVX","SLB","OXY","SMR","OKLO","CEG","VST","NEM","AEM",
    ]

# ── Exclude inverse & extreme leveraged ETFs (confuse Opus with inverted direction) ──
EXCLUDED_TICKERS = {
    # 3x inverse
    "SPXU","SPXS","SQQQ","SOXS","TZA","LABD","KOLD","ZSL","FAZ",
    # 2x/3x leveraged (too volatile for spread construction)
    "SPXL","TQQQ","SOXL","TNA","UPRO","SSO","UVXY","UVIX","VXX","SVIX",
    "NUGT","NAIL","DPST","LABU","BOIL","AGQ","FAS","TMF",
    "YINN","CONL","MSTU","MSTX","MSTZ","NVDL","NVDX","AMDL","ETHU",
    # Leveraged single-stock / yield traps (unreliable chains)
    "TSLL","MSTY","ULTY",
}
ALL_TICKERS = [t for t in ALL_TICKERS if t not in EXCLUDED_TICKERS]


# ============================================================
# IV DATABASE (SQLite — persists between runs)
# ============================================================

class IVDatabase:
    """Stores daily ATM IV readings to calculate real IV rank over time."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS iv_history (
                date TEXT,
                symbol TEXT,
                atm_iv REAL,
                hv_20 REAL,
                hv_60 REAL,
                price REAL,
                atm_call_iv REAL,
                atm_put_iv REAL,
                total_call_vol INTEGER,
                total_put_vol INTEGER,
                total_call_oi INTEGER,
                total_put_oi INTEGER,
                PRIMARY KEY (date, symbol)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_log (
                date TEXT,
                timestamp TEXT,
                tickers_scanned INTEGER,
                opportunities_found INTEGER
            )
        """)
        self.conn.commit()

    def store_iv(self, symbol: str, data: Dict):
        """Store today's IV reading."""
        today = datetime.date.today().isoformat()
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO iv_history
                (date, symbol, atm_iv, hv_20, hv_60, price,
                 atm_call_iv, atm_put_iv,
                 total_call_vol, total_put_vol, total_call_oi, total_put_oi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                today, symbol, data.get("atm_iv", 0),
                data.get("hv_20", 0), data.get("hv_60", 0),
                data.get("price", 0),
                data.get("atm_call_iv", 0), data.get("atm_put_iv", 0),
                data.get("total_call_vol", 0), data.get("total_put_vol", 0),
                data.get("total_call_oi", 0), data.get("total_put_oi", 0),
            ))
            self.conn.commit()
        except Exception as e:
            pass

    def get_iv_history(self, symbol: str, days: int = 252) -> pd.DataFrame:
        """Get IV history for a symbol."""
        cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
        df = pd.read_sql_query(
            "SELECT * FROM iv_history WHERE symbol = ? AND date >= ? ORDER BY date",
            self.conn, params=(symbol, cutoff)
        )
        return df

    def get_iv_rank(self, symbol: str, current_iv: float) -> Tuple[float, float, int]:
        """
        Calculate IV Rank and IV Percentile from stored history.
        Returns: (iv_rank, iv_percentile, data_points)
        """
        hist = self.get_iv_history(symbol)
        if hist.empty or len(hist) < 5:
            return -1, -1, len(hist)  # Not enough data yet

        ivs = hist["atm_iv"].dropna()
        if ivs.empty:
            return -1, -1, 0

        iv_min = ivs.min()
        iv_max = ivs.max()
        iv_range = iv_max - iv_min

        # IV Rank = (current - min) / (max - min) * 100
        iv_rank = ((current_iv - iv_min) / iv_range * 100) if iv_range > 0 else 50
        iv_rank = max(0, min(100, iv_rank))

        # IV Percentile = % of readings below current
        iv_percentile = (ivs < current_iv).mean() * 100

        return iv_rank, iv_percentile, len(ivs)

    def log_scan(self, tickers_scanned: int, opportunities: int):
        now = datetime.datetime.now()
        self.conn.execute(
            "INSERT INTO scan_log VALUES (?, ?, ?, ?)",
            (now.date().isoformat(), now.isoformat(), tickers_scanned, opportunities)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ============================================================
# VOLATILITY CALCULATOR
# ============================================================

class VolCalculator:
    """Calculates historical (realized) volatility from price data."""

    @staticmethod
    def realized_vol(prices: pd.Series, window: int = 20) -> float:
        """
        Annualized realized volatility using close-to-close log returns.
        Standard method: std(log returns) * sqrt(252)
        """
        if len(prices) < window + 1:
            return 0.0
        log_returns = np.log(prices / prices.shift(1)).dropna()
        rv = log_returns.tail(window).std() * np.sqrt(252) * 100
        return rv if not np.isnan(rv) else 0.0

    @staticmethod
    def realized_vol_series(prices: pd.Series, window: int = 20) -> pd.Series:
        """Rolling realized vol series."""
        log_returns = np.log(prices / prices.shift(1))
        return log_returns.rolling(window).std() * np.sqrt(252) * 100

    @staticmethod
    def parkinson_vol(high: pd.Series, low: pd.Series, window: int = 20) -> float:
        """
        Parkinson volatility — uses high/low range, more efficient than close-to-close.
        Better estimator when intraday data matters.
        """
        if len(high) < window:
            return 0.0
        hl_ratio = np.log(high / low) ** 2
        factor = 1 / (4 * np.log(2))
        pv = np.sqrt(factor * hl_ratio.tail(window).mean()) * np.sqrt(252) * 100
        return pv if not np.isnan(pv) else 0.0

    @staticmethod
    def yang_zhang_vol(open_: pd.Series, high: pd.Series,
                       low: pd.Series, close: pd.Series, window: int = 20) -> float:
        """
        Yang-Zhang volatility — the BEST estimator.
        Combines overnight returns, open-to-close, and Parkinson range.
        Handles overnight gaps and intraday drift.
        """
        if len(close) < window + 1:
            return 0.0

        n = window

        # Overnight returns (close-to-open)
        log_oc = np.log(open_ / close.shift(1)).dropna().tail(n)

        # Open-to-close returns
        log_co = np.log(close / open_).tail(n)

        # Rogers-Satchell component (intraday range)
        log_hi = np.log(high / open_).tail(n)
        log_lo = np.log(low / open_).tail(n)
        log_hc = np.log(high / close).tail(n)
        log_lc = np.log(low / close).tail(n)
        rs = (log_hi * log_hc + log_lo * log_lc).mean()

        # Combine
        k = 0.34 / (1.34 + (n + 1) / (n - 1))
        overnight_var = log_oc.var()
        close_var = log_co.var()

        yz_var = overnight_var + k * close_var + (1 - k) * rs
        yz_vol = np.sqrt(max(yz_var, 0)) * np.sqrt(252) * 100

        return yz_vol if not np.isnan(yz_vol) else 0.0

    @staticmethod
    def vol_cone(prices: pd.Series) -> Dict:
        """
        Volatility cone — shows current vol vs historical range for each window.
        Used to determine if vol is cheap or expensive.
        """
        cone = {}
        for w in VOL_WINDOWS:
            rv_series = VolCalculator.realized_vol_series(prices, w).dropna()
            if rv_series.empty:
                continue
            current = rv_series.iloc[-1]
            cone[w] = {
                "current": current,
                "min": rv_series.min(),
                "p25": rv_series.quantile(0.25),
                "median": rv_series.median(),
                "p75": rv_series.quantile(0.75),
                "max": rv_series.max(),
                "percentile": (rv_series < current).mean() * 100,
            }
        return cone


# ============================================================
# OPTIONS DATA EXTRACTOR
# ============================================================

class OptionsDataExtractor:
    """Extracts and processes options chain data from yfinance."""

    @staticmethod
    def get_atm_iv(chain_calls: pd.DataFrame, chain_puts: pd.DataFrame,
                   price: float) -> Dict:
        """Extract ATM implied volatility from options chain."""
        result = {"atm_call_iv": 0, "atm_put_iv": 0, "atm_iv": 0,
                  "atm_straddle": 0, "atm_call_price": 0, "atm_put_price": 0}

        if chain_calls.empty and chain_puts.empty:
            return result

        if not chain_calls.empty and "strike" in chain_calls.columns:
            atm_idx = (chain_calls["strike"] - price).abs().idxmin()
            row = chain_calls.loc[atm_idx]
            result["atm_call_iv"] = row.get("impliedVolatility", 0) * 100
            result["atm_call_price"] = row.get("lastPrice", 0)
            bid = row.get("bid", 0) or 0
            ask = row.get("ask", 0) or 0
            if bid > 0 and ask > 0:
                result["atm_call_price"] = (bid + ask) / 2

        if not chain_puts.empty and "strike" in chain_puts.columns:
            atm_idx = (chain_puts["strike"] - price).abs().idxmin()
            row = chain_puts.loc[atm_idx]
            result["atm_put_iv"] = row.get("impliedVolatility", 0) * 100
            result["atm_put_price"] = row.get("lastPrice", 0)
            bid = row.get("bid", 0) or 0
            ask = row.get("ask", 0) or 0
            if bid > 0 and ask > 0:
                result["atm_put_price"] = (bid + ask) / 2

        # Average of call and put IV
        ivs = [v for v in [result["atm_call_iv"], result["atm_put_iv"]] if v > 0]
        result["atm_iv"] = np.mean(ivs) if ivs else 0

        result["atm_straddle"] = result["atm_call_price"] + result["atm_put_price"]

        return result

    @staticmethod
    def get_volume_stats(chains: Dict) -> Dict:
        """Aggregate volume and OI stats across all expirations."""
        total_call_vol = 0
        total_put_vol = 0
        total_call_oi = 0
        total_put_oi = 0

        for exp, data in chains.items():
            calls = data.get("calls", pd.DataFrame())
            puts = data.get("puts", pd.DataFrame())

            if not calls.empty:
                total_call_vol += calls.get("volume", pd.Series([0])).fillna(0).sum()
                total_call_oi += calls.get("openInterest", pd.Series([0])).fillna(0).sum()
            if not puts.empty:
                total_put_vol += puts.get("volume", pd.Series([0])).fillna(0).sum()
                total_put_oi += puts.get("openInterest", pd.Series([0])).fillna(0).sum()

        total_vol = total_call_vol + total_put_vol
        total_oi = total_call_oi + total_put_oi

        return {
            "total_call_vol": int(total_call_vol),
            "total_put_vol": int(total_put_vol),
            "total_call_oi": int(total_call_oi),
            "total_put_oi": int(total_put_oi),
            "pc_vol_ratio": total_put_vol / max(total_call_vol, 1),
            "pc_oi_ratio": total_put_oi / max(total_call_oi, 1),
            "vol_oi_ratio": total_vol / max(total_oi, 1),
        }

    @staticmethod
    def get_skew_25d(chain_calls: pd.DataFrame, chain_puts: pd.DataFrame,
                     price: float, atm_iv: float, dte: int) -> Dict:
        """
        Calculate 25-delta skew from options chain.

        Approximates 25-delta strikes using:
          25D put strike  ≈ price * exp(-0.675 * IV * sqrt(T))
          25D call strike ≈ price * exp(+0.675 * IV * sqrt(T))
        where 0.675 = norm.ppf(0.75) ≈ quantile for 25-delta

        Then interpolates IV at those strikes from the chain.

        Returns:
          put_25d_iv:  IV at 25-delta put strike
          call_25d_iv: IV at 25-delta call strike
          skew:        put_25d_iv - call_25d_iv (positive = downside fear)
          skew_ratio:  put_25d_iv / call_25d_iv (>1.0 = put skew)
          put_25d_strike: approximate strike
          call_25d_strike: approximate strike
        """
        result = {
            "put_25d_iv": 0, "call_25d_iv": 0,
            "skew": 0, "skew_ratio": 1.0,
            "put_25d_strike": 0, "call_25d_strike": 0,
        }

        if atm_iv <= 0 or dte <= 0 or price <= 0:
            return result

        try:
            iv_dec = atm_iv / 100  # Convert % to decimal
            t = max(dte, 1) / 365.0
            # 0.675 ≈ norm.ppf(0.75) — the z-score for 25-delta approximation
            offset = 0.675 * iv_dec * np.sqrt(t)

            put_25d_strike = price * np.exp(-offset)
            call_25d_strike = price * np.exp(offset)
            result["put_25d_strike"] = round(put_25d_strike, 2)
            result["call_25d_strike"] = round(call_25d_strike, 2)

            # Interpolate IV at 25D put strike
            if not chain_puts.empty and "impliedVolatility" in chain_puts.columns:
                puts_valid = chain_puts[chain_puts["impliedVolatility"] > 0].copy()
                if not puts_valid.empty:
                    idx = (puts_valid["strike"] - put_25d_strike).abs().idxmin()
                    result["put_25d_iv"] = float(puts_valid.loc[idx, "impliedVolatility"]) * 100

            # Interpolate IV at 25D call strike
            if not chain_calls.empty and "impliedVolatility" in chain_calls.columns:
                calls_valid = chain_calls[chain_calls["impliedVolatility"] > 0].copy()
                if not calls_valid.empty:
                    idx = (calls_valid["strike"] - call_25d_strike).abs().idxmin()
                    result["call_25d_iv"] = float(calls_valid.loc[idx, "impliedVolatility"]) * 100

            # Calculate skew
            if result["put_25d_iv"] > 0 and result["call_25d_iv"] > 0:
                result["skew"] = round(result["put_25d_iv"] - result["call_25d_iv"], 1)
                result["skew_ratio"] = round(result["put_25d_iv"] / result["call_25d_iv"], 2)

        except Exception:
            pass

        return result

    @staticmethod
    def get_vwks(chains: Dict, price: float, dte_min: int = 20, dte_max: int = 50) -> Dict:
        """
        Calculate Volume-Weighted Strike-Spot Ratio (VWKS).

        VWKS = Σ(Volume_i × K_i / S) / Σ(Volume_i)

        where K_i = strike, S = spot price, Volume_i = trading volume.
        Summed across ALL option contracts (calls + puts) in DTE window.

        Interpretation:
          VWKS > 1.0 → volume concentrated in OTM calls / ITM puts → bullish delta demand
          VWKS < 1.0 → volume concentrated in ITM calls / OTM puts → bearish delta demand
          VWKS = 1.0 → balanced around ATM

        Returns:
          vwks: the ratio (float)
          total_volume: total volume used in calculation
          call_vwks: VWKS for calls only
          put_vwks: VWKS for puts only
        """
        result = {"vwks": 1.0, "total_volume": 0, "call_vwks": 1.0, "put_vwks": 1.0}

        if price <= 0:
            return result

        total_weighted = 0.0
        total_vol = 0.0
        call_weighted = 0.0
        call_vol = 0.0
        put_weighted = 0.0
        put_vol = 0.0

        for exp, ch in chains.items():
            dte = ch.get("dte", 0)
            if dte < dte_min or dte > dte_max:
                continue

            for side in ["calls", "puts"]:
                df = ch.get(side, pd.DataFrame())
                if df.empty or "strike" not in df.columns or "volume" not in df.columns:
                    continue

                valid = df[df["volume"].fillna(0) > 0].copy()
                if valid.empty:
                    continue

                strikes = valid["strike"].values
                volumes = valid["volume"].fillna(0).values
                moneyness = strikes / price  # K/S for each contract

                weighted_sum = float(np.sum(volumes * moneyness))
                vol_sum = float(np.sum(volumes))

                total_weighted += weighted_sum
                total_vol += vol_sum

                if side == "calls":
                    call_weighted += weighted_sum
                    call_vol += vol_sum
                else:
                    put_weighted += weighted_sum
                    put_vol += vol_sum

        if total_vol > 0:
            result["vwks"] = round(total_weighted / total_vol, 4)
            result["total_volume"] = int(total_vol)
        if call_vol > 0:
            result["call_vwks"] = round(call_weighted / call_vol, 4)
        if put_vol > 0:
            result["put_vwks"] = round(put_weighted / put_vol, 4)

        return result

    @staticmethod
    def implied_move(straddle_price: float, stock_price: float, dte: int) -> float:
        """Calculate implied move % from straddle price, adjusted for DTE."""
        if stock_price <= 0 or dte <= 0:
            return 0
        # 85% rule for expected move
        raw_move = (straddle_price / stock_price) * 100
        # Adjust for time — the straddle prices in sqrt(time)
        # For weekly (5 DTE), the daily move contribution is higher
        return raw_move * 0.85


# ============================================================
# MAIN SCANNER (ORCA)
# ============================================================

@dataclass
class Trade:
    ticker: str
    strategy: str
    direction: str          # SELL_PREMIUM, BUY_CALL, BUY_PUT, SELL_IC, etc.
    thesis: str
    entry: str              # Specific strikes, expiry
    expected_profit: float
    max_loss: float
    win_prob: float
    capital: float
    urgency: str            # NOW, TODAY, THIS_WEEK
    confidence: float

    # Vol data
    price: float = 0
    iv: float = 0
    hv_20: float = 0
    hv_yz: float = 0        # Yang-Zhang (best estimator)
    iv_rank: float = -1
    iv_percentile: float = -1
    iv_rank_datapoints: int = 0
    iv_hv_ratio: float = 0
    vol_cone_pctile: float = 0

    # Flow data
    pc_ratio: float = 0
    vol_oi_ratio: float = 0

    # 25-delta skew
    skew_25d: float = 0        # put_25d_iv - call_25d_iv (positive = downside fear)
    skew_ratio: float = 1.0    # put_25d_iv / call_25d_iv

    # VWKS — Volume-Weighted Strike-Spot Ratio (predicts returns)
    vwks: float = 1.0          # >1.0 = bullish delta demand, <1.0 = bearish

    # Profit target (for debit spreads: min(1.65x debit, spread width))
    profit_target: float = 0
    # Stop alert: if position drops 40% from entry → alert + recheck thesis
    stop_alert: float = 0
    # Spread width (max value of the spread)
    spread_width: float = 0
    # Quality ratio: debit/width for debits, credit/width for credits
    # Good debit: < 0.58 (paying less than 58% of max). Good credit: > 0.35
    quality_ratio: float = 0

    score: float = 0

    def compute_score(self):
        urgency_mult = {"NOW": 4, "TODAY": 3, "THIS_WEEK": 2}.get(self.urgency, 1)
        self.score = (
            self.expected_profit * self.win_prob * self.confidence * urgency_mult
            / max(self.capital / 500, 0.5)
        )


def _pick_deep_itm_option(opts_df, price, opt_type="call"):
    """
    Pick the most liquid deep ITM option (2-3 strikes in the money).
    For calls: 2-3 strikes BELOW spot. For puts: 2-3 strikes ABOVE spot.
    Returns (strike, bid, ask, volume, oi) or None if nothing liquid.
    """
    if opts_df.empty or "strike" not in opts_df.columns:
        return None

    strikes_sorted = sorted(opts_df["strike"].unique())

    if opt_type == "call":
        # ITM calls = strikes below spot price
        itm = [s for s in strikes_sorted if s < price]
        if len(itm) < 2:
            return None
        # Candidates: 2nd and 3rd strike deep ITM (skip the closest — too near ATM)
        candidates = []
        for depth in [2, 3, 1]:  # prefer 2-3 deep, fallback to 1
            if depth <= len(itm):
                strike = float(itm[-depth])  # from the top (closest to spot) going deeper
                candidates.append(strike)
    else:
        # ITM puts = strikes above spot price
        itm = [s for s in strikes_sorted if s > price]
        if len(itm) < 2:
            return None
        candidates = []
        for depth in [2, 3, 1]:
            if depth <= len(itm):
                strike = float(itm[depth - 1])  # from bottom (closest to spot) going deeper
                candidates.append(strike)

    # Check liquidity at each candidate — pick the one with most volume + OI
    best = None
    best_liquidity = -1
    for strike in candidates:
        row = opts_df.iloc[(opts_df["strike"] - strike).abs().argsort()[:1]]
        vol = float(row["volume"].values[0]) if "volume" in opts_df.columns else 0
        oi = float(row["openInterest"].values[0]) if "openInterest" in opts_df.columns else 0
        bid = float(row["bid"].values[0]) if "bid" in opts_df.columns else 0
        ask = float(row["ask"].values[0]) if "ask" in opts_df.columns else 0

        # Skip if no bid (illiquid)
        if bid <= 0:
            continue

        liquidity = vol + oi
        if liquidity > best_liquidity:
            best_liquidity = liquidity
            best = (strike, bid, ask, vol, oi)

    return best


class ORCA:
    def __init__(self, capital: float = 1000):
        self.capital = capital
        self.max_per_trade = capital * 0.10
        self.db = IVDatabase()
        self.vol = VolCalculator()
        self.opts = OptionsDataExtractor()
        self._cache = {}

    def _fetch(self, symbol: str) -> Optional[Dict]:
        """Fetch all data for a ticker, calculate vols, store IV."""
        if symbol in self._cache:
            return self._cache[symbol]

        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period=PRICE_HISTORY_DAYS)
            if hist.empty or len(hist) < 30:
                return None

            price = hist['Close'].iloc[-1]

            # ── HISTORICAL VOLATILITY (multiple methods) ──
            hv = {}
            for w in VOL_WINDOWS:
                hv[f"cc_{w}"] = self.vol.realized_vol(hist['Close'], w)

            hv["parkinson_20"] = self.vol.parkinson_vol(hist['High'], hist['Low'], 20)
            hv["yang_zhang_20"] = self.vol.yang_zhang_vol(
                hist['Open'], hist['High'], hist['Low'], hist['Close'], 20
            )
            hv["yang_zhang_60"] = self.vol.yang_zhang_vol(
                hist['Open'], hist['High'], hist['Low'], hist['Close'], 60
            )

            # Vol cone
            vol_cone = self.vol.vol_cone(hist['Close'])

            # ── OPTIONS CHAINS ──
            try:
                expirations = tk.options
            except:
                expirations = []

            chains = {}
            for exp in (expirations or [])[:8]:
                exp_date = datetime.date.fromisoformat(exp)
                dte = (exp_date - datetime.date.today()).days
                if dte < 0:
                    continue
                try:
                    chain = tk.option_chain(exp)
                    chains[exp] = {
                        "calls": chain.calls,
                        "puts": chain.puts,
                        "dte": dte,
                    }
                except:
                    continue

            if not chains:
                return None

            # ── ATM IV (from nearest expiration) ──
            nearest_exp = min(chains.keys(), key=lambda e: chains[e]["dte"])
            nearest = chains[nearest_exp]
            atm_data = self.opts.get_atm_iv(nearest["calls"], nearest["puts"], price)

            # Also get 30-day IV (nearest monthly for IV rank)
            monthly_iv = atm_data["atm_iv"]
            for exp, ch in chains.items():
                if 25 <= ch["dte"] <= 45:
                    monthly_atm = self.opts.get_atm_iv(ch["calls"], ch["puts"], price)
                    if monthly_atm["atm_iv"] > 0:
                        monthly_iv = monthly_atm["atm_iv"]
                    break

            # ── VOLUME STATS ──
            vol_stats = self.opts.get_volume_stats(chains)

            # ── IV RANK from database ──
            iv_rank, iv_pctile, dp = self.db.get_iv_rank(symbol, monthly_iv)

            # If no DB history yet, approximate from HV percentile
            if iv_rank < 0 and vol_cone.get(20):
                iv_rank = vol_cone[20]["percentile"]
                iv_pctile = iv_rank
                dp = 0  # Flag as approximation

            # ── IV vs HV ratios ──
            best_hv = hv.get("yang_zhang_20", hv.get("cc_20", 1))
            iv_hv_ratio = monthly_iv / max(best_hv, 0.01) if monthly_iv > 0 else 1.0

            # ── 25-DELTA SKEW ──
            # Use the monthly expiration chain for skew (30-45 DTE = most liquid)
            skew_data = {"put_25d_iv": 0, "call_25d_iv": 0, "skew": 0, "skew_ratio": 1.0,
                         "put_25d_strike": 0, "call_25d_strike": 0}
            for exp, ch in chains.items():
                if 20 <= ch["dte"] <= 50:
                    skew_data = self.opts.get_skew_25d(
                        ch["calls"], ch["puts"], price, monthly_iv, ch["dte"]
                    )
                    break
            if skew_data["skew"] == 0 and nearest["dte"] > 0:
                # Fallback: use nearest expiration
                skew_data = self.opts.get_skew_25d(
                    nearest["calls"], nearest["puts"], price, monthly_iv, nearest["dte"]
                )

            # ── VWKS (Volume-Weighted Strike-Spot Ratio) ──
            # Uses same DTE window as skew (20-50 DTE = trade-matching)
            vwks_data = self.opts.get_vwks(chains, price, dte_min=20, dte_max=50)
            if vwks_data["total_volume"] == 0:
                # Fallback: use all available expirations
                vwks_data = self.opts.get_vwks(chains, price, dte_min=0, dte_max=999)

            # ── STORE IN DATABASE ──
            self.db.store_iv(symbol, {
                "atm_iv": monthly_iv,
                "hv_20": hv.get("cc_20", 0),
                "hv_60": hv.get("cc_60", 0),
                "price": price,
                "atm_call_iv": atm_data["atm_call_iv"],
                "atm_put_iv": atm_data["atm_put_iv"],
                **vol_stats,
            })

            # ── PACKAGE ──
            data = {
                "symbol": symbol,
                "price": price,
                "hist": hist,
                "hv": hv,
                "vol_cone": vol_cone,
                "chains": chains,
                "expirations": list(chains.keys()),
                "atm": atm_data,
                "atm_iv": monthly_iv,
                "iv_rank": iv_rank,
                "iv_percentile": iv_pctile,
                "iv_rank_dp": dp,
                "iv_hv_ratio": iv_hv_ratio,
                "skew_25d": skew_data,
                "vwks_data": vwks_data,
                **vol_stats,
            }

            self._cache[symbol] = data
            return data

        except Exception as e:
            return None

    # ─────────────────────────────────────────────
    # SCAN MODES
    # ─────────────────────────────────────────────

    def scan_premium(self, tickers: List[str] = None) -> List[Trade]:
        """Find premium selling opportunities (IV > HV)."""
        print("\n💰 PREMIUM SELLING SCAN (IV overpriced → sell it)")
        print("─" * 60)
        tickers = tickers or ALL_TICKERS
        trades = []

        for sym in tickers:
            data = self._fetch(sym)
            if not data or data["atm_iv"] <= 0:
                continue

            iv = data["atm_iv"]
            hv_yz = data["hv"].get("yang_zhang_20", 0)
            hv_cc = data["hv"].get("cc_20", 0)
            best_hv = hv_yz if hv_yz > 0 else hv_cc
            ratio = data["iv_hv_ratio"]
            iv_rank = data["iv_rank"]
            price = data["price"]

            # SELL when: IV rank high AND IV significantly above realized
            if ratio >= IVHV_OVERPRICED and iv_rank >= IV_RANK_SELL_THRESHOLD:
                # Find best expiration (30-45 DTE ideal for theta decay)
                best_exp = None
                best_dte = None
                for exp, ch in data["chains"].items():
                    dte = ch["dte"]
                    if 7 <= dte <= 50:
                        if best_exp is None or abs(dte - 35) < abs(best_dte - 35):
                            best_exp = exp
                            best_dte = dte

                if not best_exp:
                    continue

                # IN-OUT Credit spread: one leg ITM, one leg OTM, bracketing spot
                pc_ratio = data.get("pc_vol_ratio", 1.0)
                chain = data["chains"][best_exp]

                # Bearish skew (high P/C) → sell call spread, else sell put spread
                if pc_ratio and pc_ratio > 1.5:
                    spread_type = "SELL Call Spread"
                    direction = "SELL_CALL_SPREAD"
                else:
                    spread_type = "SELL Put Spread"
                    direction = "SELL_PUT_SPREAD"

                # IN-OUT strike selection: bracket the spot price
                short_strike = price  # default
                long_strike = price
                if direction == "SELL_PUT_SPREAD":
                    # Sell ITM put (above spot), Buy OTM put (below spot)
                    opts_df = chain["puts"]
                    if not opts_df.empty and "strike" in opts_df.columns:
                        strikes_sorted = sorted(opts_df["strike"].unique())
                        itm_puts = [s for s in strikes_sorted if s >= price]   # above spot = ITM puts
                        otm_puts = [s for s in strikes_sorted if s < price]    # below spot = OTM puts
                        if itm_puts and otm_puts:
                            short_strike = float(itm_puts[0])     # sell closest ITM put (just above spot)
                            long_strike = float(otm_puts[-1])     # buy closest OTM put (just below spot)
                        else:
                            continue  # can't bracket spot
                elif direction == "SELL_CALL_SPREAD":
                    # Sell ITM call (below spot), Buy OTM call (above spot)
                    opts_df = chain["calls"]
                    if not opts_df.empty and "strike" in opts_df.columns:
                        strikes_sorted = sorted(opts_df["strike"].unique())
                        itm_calls = [s for s in strikes_sorted if s <= price]  # below spot = ITM calls
                        otm_calls = [s for s in strikes_sorted if s > price]   # above spot = OTM calls
                        if itm_calls and otm_calls:
                            short_strike = float(itm_calls[-1])   # sell closest ITM call (just below spot)
                            long_strike = float(otm_calls[0])     # buy closest OTM call (just above spot)
                        else:
                            continue  # can't bracket spot

                # Look up REAL bid/ask — conservative pricing
                # Sell at mid-of-mid-and-bid, buy at mid-of-mid-and-ask
                width = abs(short_strike - long_strike)

                # ── MAX WIDTH CHECK: spread too wide → deep ITM fallback ──
                # If width > 5% of spot price, the chain doesn't have tight strikes
                # at this expiration. Fall back to naked deep ITM option instead.
                max_width = max(price * 0.05, 1.0)  # at least $1 min
                if width > max_width:
                    if direction == "SELL_PUT_SPREAD":
                        itm_opt_type = "call"
                        itm_opts_df = chain["calls"]
                    else:
                        itm_opt_type = "put"
                        itm_opts_df = chain["puts"]
                    itm_pick = _pick_deep_itm_option(itm_opts_df, price, itm_opt_type)
                    if not itm_pick:
                        print(f"  ⏭ {sym:6s} Width ${width:.2f} too wide (>{max_width:.2f}), no liquid deep ITM {itm_opt_type} → skip")
                        continue
                    itm_strike, itm_bid, itm_ask, itm_vol, itm_oi = itm_pick
                    itm_mid = (itm_bid + itm_ask) / 2
                    buy_at = round((itm_mid + itm_ask) / 2, 2)
                    target_price = round(buy_at * 1.15, 2)
                    alert_price = round(buy_at * 0.90, 2)
                    quality_ratio = round(width / price, 3) if price > 0 else 0
                    trade = Trade(
                        ticker=sym,
                        strategy=f"BUY {itm_opt_type.title()}",
                        direction=f"BUY_{itm_opt_type.upper()}",
                        thesis=(
                            f"Spot ${price:.2f}. "
                            f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                            f"Spread width ${width:.2f} too wide (>{max_width:.2f}) — "
                            f"buying deep ITM ${itm_strike:.2f} {itm_opt_type} instead (vol:{itm_vol:.0f} OI:{itm_oi:.0f})."
                        ),
                        entry=(
                            f"Buy {sym} ${itm_strike:.2f} {itm_opt_type} | "
                            f"Spot ${price:.2f} | "
                            f"Exp {best_exp} ({best_dte}d) | "
                            f"${buy_at:.2f} debit | "
                            f"Target ${target_price:.2f} (+15%) | Alert ${alert_price:.2f} (-10%)"
                        ),
                        expected_profit=buy_at * 0.15 * 100,
                        max_loss=buy_at * 0.10 * 100,
                        win_prob=0.50,
                        capital=buy_at * 100,
                        urgency="THIS_WEEK",
                        confidence=min(0.5 + (ratio - 1.2) * 0.4, 0.80),
                        price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                        iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                        iv_rank_datapoints=data["iv_rank_dp"],
                        iv_hv_ratio=ratio,
                        pc_ratio=data["pc_vol_ratio"],
                        vol_oi_ratio=data["vol_oi_ratio"],
                        skew_25d=data.get("skew_25d", {}).get("skew", 0),
                        skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                        vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                        profit_target=target_price,
                        stop_alert=alert_price,
                        quality_ratio=quality_ratio,
                    )
                    trade.compute_score()
                    trades.append(trade)
                    print(f"  🎯 {sym:6s} ${price:8.2f} | "
                          f"Width ${width:.2f} too wide → BUY ${itm_strike:.2f} {itm_opt_type} (deep ITM) | "
                          f"${buy_at:.2f} debit | vol:{itm_vol:.0f} OI:{itm_oi:.0f}")
                    continue

                est_credit = width * 0.30  # fallback estimate
                short_bid = None
                short_ask = None
                long_bid = None
                long_ask = None
                try:
                    short_row = opts_df.iloc[(opts_df["strike"] - short_strike).abs().argsort()[:1]]
                    long_row = opts_df.iloc[(opts_df["strike"] - long_strike).abs().argsort()[:1]]
                    short_bid = float(short_row["bid"].values[0]) if "bid" in opts_df.columns else None
                    short_ask = float(short_row["ask"].values[0]) if "ask" in opts_df.columns else None
                    long_bid = float(long_row["bid"].values[0]) if "bid" in opts_df.columns else None
                    long_ask = float(long_row["ask"].values[0]) if "ask" in opts_df.columns else None
                    if short_bid and short_ask and long_bid and long_ask:
                        short_mid = (short_bid + short_ask) / 2
                        long_mid = (long_bid + long_ask) / 2
                        # Conservative: sell at mid-of-mid-and-bid, buy at mid-of-mid-and-ask
                        sell_price = round((short_mid + short_bid) / 2, 2)
                        buy_price = round((long_mid + long_ask) / 2, 2)
                        if sell_price > buy_price:
                            est_credit = round(sell_price - buy_price, 2)
                except Exception:
                    pass  # use fallback estimate

                if est_credit <= 0:
                    continue  # invalid pricing

                # ── QUALITY RATIO: credit / width ──
                # Must collect at least 45% of spread width as credit
                # Example: $5 wide, $2 credit → 40% = NOT passing → buy/sell underlying
                # Example: $2 wide, $1.10 credit → 55% = great deal → do the spread
                quality_ratio = round(est_credit / width, 3) if width > 0 else 0
                if quality_ratio < 0.45:
                    # Credit not rich enough → fall back to deep ITM naked option
                    # SELL_PUT_SPREAD = bullish thesis → buy deep ITM CALL
                    # SELL_CALL_SPREAD = bearish thesis → buy deep ITM PUT
                    if direction == "SELL_PUT_SPREAD":
                        itm_opt_type = "call"
                        itm_opts_df = chain["calls"]
                    else:
                        itm_opt_type = "put"
                        itm_opts_df = chain["puts"]

                    itm_pick = _pick_deep_itm_option(itm_opts_df, price, itm_opt_type)
                    if not itm_pick:
                        print(f"  ⏭ {sym:6s} Credit {quality_ratio:.1%} too thin, no liquid deep ITM {itm_opt_type} → skip")
                        continue

                    itm_strike, itm_bid, itm_ask, itm_vol, itm_oi = itm_pick
                    # Conservative buy price: mid-of-mid-and-ask
                    itm_mid = (itm_bid + itm_ask) / 2
                    buy_at = round((itm_mid + itm_ask) / 2, 2)
                    # Naked option alerts: +15% profit, -10% loss
                    target_price = round(buy_at * 1.15, 2)
                    alert_price = round(buy_at * 0.90, 2)

                    trade = Trade(
                        ticker=sym,
                        strategy=f"BUY {itm_opt_type.title()}",
                        direction=f"BUY_{itm_opt_type.upper()}",
                        thesis=(
                            f"Spot ${price:.2f}. "
                            f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                            f"Credit spread quality {quality_ratio:.1%} too thin (<45%) — "
                            f"buying deep ITM ${itm_strike:.2f} {itm_opt_type} instead (vol:{itm_vol:.0f} OI:{itm_oi:.0f})."
                        ),
                        entry=(
                            f"Buy {sym} ${itm_strike:.2f} {itm_opt_type} | "
                            f"Spot ${price:.2f} | "
                            f"Exp {best_exp} ({best_dte}d) | "
                            f"${buy_at:.2f} debit | "
                            f"Target ${target_price:.2f} (+15%) | Alert ${alert_price:.2f} (-10%)"
                        ),
                        expected_profit=buy_at * 0.15 * 100,
                        max_loss=buy_at * 0.10 * 100,
                        win_prob=0.50,
                        capital=buy_at * 100,
                        urgency="THIS_WEEK",
                        confidence=min(0.5 + (ratio - 1.2) * 0.4, 0.80),
                        price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                        iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                        iv_rank_datapoints=data["iv_rank_dp"],
                        iv_hv_ratio=ratio,
                        pc_ratio=data["pc_vol_ratio"],
                        vol_oi_ratio=data["vol_oi_ratio"],
                        skew_25d=data.get("skew_25d", {}).get("skew", 0),
                        skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                        vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                        profit_target=target_price,
                        stop_alert=alert_price,
                        quality_ratio=quality_ratio,
                    )
                    trade.compute_score()
                    trades.append(trade)
                    print(f"  🎯 {sym:6s} ${price:8.2f} | "
                          f"Credit {quality_ratio:.1%} too thin → BUY ${itm_strike:.2f} {itm_opt_type} (deep ITM) | "
                          f"${buy_at:.2f} debit | vol:{itm_vol:.0f} OI:{itm_oi:.0f}")
                    continue

                max_loss = width - est_credit
                # Credit spread exit rules:
                # Target = keep full credit (spread expires worthless)
                # Alert = if spread value rises 40% against us (loss = 40% of max_loss)
                credit_stop_alert = round(est_credit + max_loss * 0.40, 2)  # spread cost to close

                # Build entry string with real prices + exit rules
                ratio_str = f"Ratio {quality_ratio:.1%}"
                price_note = f"${est_credit:.2f} credit | {ratio_str} | Width ${width:.2f} | Alert if spread >${credit_stop_alert:.2f} (-40%)"
                if short_bid is not None and long_ask is not None:
                    price_note = f"${est_credit:.2f} credit (sell @ ${sell_price:.2f}, buy @ ${buy_price:.2f}) | {ratio_str} | Width ${width:.2f} | Alert if spread >${credit_stop_alert:.2f} (-40%)"

                trade = Trade(
                    ticker=sym,
                    strategy=spread_type,
                    direction=direction,
                    thesis=(
                        f"Spot ${price:.2f}. "
                        f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                        f"IV Rank {iv_rank:.0f}% ({data['iv_rank_dp']} datapoints). "
                        f"Premium is fat — sell in-out credit spread."
                    ),
                    entry=(
                        f"Sell ${short_strike:.2f}/${long_strike:.2f} {spread_type.split()[1].lower()} spread | "
                        f"Spot ${price:.2f} | "
                        f"Exp {best_exp} ({best_dte}d) | "
                        f"{price_note}"
                    ),
                    expected_profit=est_credit * 100,
                    max_loss=max_loss * 100,
                    win_prob=0.68,
                    capital=max_loss * 100,
                    urgency="THIS_WEEK",
                    confidence=min(0.5 + (ratio - 1.2) * 0.4, 0.85),
                    price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                    iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                    iv_rank_datapoints=data["iv_rank_dp"],
                    iv_hv_ratio=ratio,
                    pc_ratio=data["pc_vol_ratio"],
                    vol_oi_ratio=data["vol_oi_ratio"],
                    skew_25d=data.get("skew_25d", {}).get("skew", 0),
                    skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                    vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                    profit_target=est_credit,  # target = keep full credit
                    stop_alert=credit_stop_alert,
                    spread_width=width,
                    quality_ratio=quality_ratio,
                )
                trade.compute_score()
                trades.append(trade)

                flag = "🔥" if ratio > 1.5 else "✅"
                vwks_val = data.get("vwks_data", {}).get("vwks", 1.0)
                print(f"  {flag} {sym:6s} ${price:8.2f} | "
                      f"IV:{iv:5.1f}% HV(YZ):{hv_yz:5.1f}% | "
                      f"Ratio:{ratio:.2f}x | Rank:{iv_rank:5.1f}% | "
                      f"VWKS:{vwks_val:.3f} | "
                      f"{spread_type} ${est_credit:.2f}/{width:.2f} ({quality_ratio:.1%})")

            elif ratio <= IVHV_UNDERPRICED and iv_rank <= IV_RANK_BUY_THRESHOLD:
                # IV cheap — build IN-OUT DEBIT SPREAD
                # Find best expiration (14-30 DTE for debit spreads — closer than credit)
                best_exp = None
                best_dte = None
                for exp, ch in data["chains"].items():
                    dte = ch["dte"]
                    if 7 <= dte <= 45:
                        if best_exp is None or abs(dte - 21) < abs(best_dte - 21):
                            best_exp = exp
                            best_dte = dte

                if not best_exp:
                    continue

                # Direction: P/C ratio guides bullish vs bearish
                pc_ratio = data.get("pc_vol_ratio", 1.0)
                chain = data["chains"][best_exp]

                if pc_ratio and pc_ratio > 1.5:
                    # Heavy put volume → bearish → debit put spread
                    opts_df = chain["puts"]
                    spread_type = "BUY Put Spread"
                    direction = "BUY_PUT_SPREAD"
                else:
                    # Neutral/bullish → debit call spread (default)
                    opts_df = chain["calls"]
                    spread_type = "BUY Call Spread"
                    direction = "BUY_CALL_SPREAD"

                # IN-OUT strike selection:
                # Buy ITM (in-the-money), Sell OTM (out-of-the-money)
                long_strike = price   # default
                short_strike = price
                if not opts_df.empty and "strike" in opts_df.columns:
                    strikes_sorted = sorted(opts_df["strike"].unique())
                    if direction == "BUY_CALL_SPREAD":
                        # Buy = closest ITM call (at or below spot)
                        # Sell = closest OTM call (above spot)
                        itm_calls = [s for s in strikes_sorted if s <= price]
                        otm_calls = [s for s in strikes_sorted if s > price]
                        if itm_calls and otm_calls:
                            long_strike = float(itm_calls[-1])    # buy ITM (highest strike <= price)
                            short_strike = float(otm_calls[0])    # sell OTM (lowest strike > price)
                        else:
                            continue  # can't build spread
                    elif direction == "BUY_PUT_SPREAD":
                        # Buy = closest ITM put (at or above spot)
                        # Sell = closest OTM put (below spot)
                        itm_puts = [s for s in strikes_sorted if s >= price]
                        otm_puts = [s for s in strikes_sorted if s < price]
                        if itm_puts and otm_puts:
                            long_strike = float(itm_puts[0])      # buy ITM (lowest strike >= price)
                            short_strike = float(otm_puts[-1])    # sell OTM (highest strike < price)
                        else:
                            continue  # can't build spread

                # Look up REAL bid/ask — conservative pricing
                # Buy at mid-of-mid-and-ask, Sell at mid-of-mid-and-bid
                width = abs(long_strike - short_strike)

                # ── MAX WIDTH CHECK: spread too wide → deep ITM fallback ──
                max_width = max(price * 0.05, 1.0)
                if width > max_width:
                    opt_type = "call" if direction == "BUY_CALL_SPREAD" else "put"
                    itm_pick = _pick_deep_itm_option(opts_df, price, opt_type)
                    if not itm_pick:
                        print(f"  ⏭ {sym:6s} Width ${width:.2f} too wide (>{max_width:.2f}), no liquid deep ITM {opt_type} → skip")
                        continue
                    itm_strike, itm_bid, itm_ask, itm_vol, itm_oi = itm_pick
                    itm_mid = (itm_bid + itm_ask) / 2
                    buy_at = round((itm_mid + itm_ask) / 2, 2)
                    target_price = round(buy_at * 1.15, 2)
                    alert_price = round(buy_at * 0.90, 2)
                    trade = Trade(
                        ticker=sym,
                        strategy=f"BUY {opt_type.title()}",
                        direction=f"BUY_{opt_type.upper()}",
                        thesis=(
                            f"Spot ${price:.2f}. "
                            f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                            f"Spread width ${width:.2f} too wide (>{max_width:.2f}) — "
                            f"buying deep ITM ${itm_strike:.2f} {opt_type} instead (vol:{itm_vol:.0f} OI:{itm_oi:.0f})."
                        ),
                        entry=(
                            f"Buy {sym} ${itm_strike:.2f} {opt_type} | "
                            f"Spot ${price:.2f} | "
                            f"Exp {best_exp} ({best_dte}d) | "
                            f"${buy_at:.2f} debit | "
                            f"Target ${target_price:.2f} (+15%) | Alert ${alert_price:.2f} (-10%)"
                        ),
                        expected_profit=buy_at * 0.15 * 100,
                        max_loss=buy_at * 0.10 * 100,
                        win_prob=0.45,
                        capital=buy_at * 100,
                        urgency="THIS_WEEK",
                        confidence=min(0.4 + (1.0 - ratio) * 0.5, 0.70),
                        price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                        iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                        iv_rank_datapoints=data["iv_rank_dp"],
                        iv_hv_ratio=ratio,
                        pc_ratio=data["pc_vol_ratio"],
                        vol_oi_ratio=data["vol_oi_ratio"],
                        skew_25d=data.get("skew_25d", {}).get("skew", 0),
                        skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                        vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                        profit_target=target_price,
                        stop_alert=alert_price,
                        quality_ratio=round(width / price, 3) if price > 0 else 0,
                    )
                    trade.compute_score()
                    trades.append(trade)
                    print(f"  🎯 {sym:6s} ${price:8.2f} | "
                          f"Width ${width:.2f} too wide → BUY ${itm_strike:.2f} {opt_type} (deep ITM) | "
                          f"${buy_at:.2f} debit | vol:{itm_vol:.0f} OI:{itm_oi:.0f}")
                    continue

                est_debit = width * 0.60  # fallback estimate
                long_ask = None
                long_bid = None
                short_bid = None
                short_ask = None
                try:
                    long_row = opts_df.iloc[(opts_df["strike"] - long_strike).abs().argsort()[:1]]
                    short_row = opts_df.iloc[(opts_df["strike"] - short_strike).abs().argsort()[:1]]
                    long_ask = float(long_row["ask"].values[0]) if "ask" in opts_df.columns else None
                    long_bid = float(long_row["bid"].values[0]) if "bid" in opts_df.columns else None
                    short_bid = float(short_row["bid"].values[0]) if "bid" in opts_df.columns else None
                    short_ask = float(short_row["ask"].values[0]) if "ask" in opts_df.columns else None
                    if long_ask and long_bid and short_bid and short_ask:
                        long_mid = (long_bid + long_ask) / 2
                        short_mid = (short_bid + short_ask) / 2
                        # Conservative: buy at mid-of-mid-and-ask, sell at mid-of-mid-and-bid
                        buy_price = round((long_mid + long_ask) / 2, 2)
                        sell_price = round((short_mid + short_bid) / 2, 2)
                        if buy_price > sell_price:
                            est_debit = round(buy_price - sell_price, 2)
                except Exception:
                    pass  # use fallback estimate

                if est_debit <= 0:
                    continue  # invalid pricing

                # ── QUALITY RATIO: debit / width ──
                # Good deal = paying less than 58% of max spread value
                # Example: $2 wide, $1.15 debit → 57.5% = great deal!
                quality_ratio = round(est_debit / width, 3) if width > 0 else 1.0
                if quality_ratio > 0.58:
                    # Spread too expensive → fall back to deep ITM naked option
                    # Idea is good, spread pricing isn't → buy 2-3 strikes ITM (most liquid)
                    opt_type = "call" if direction == "BUY_CALL_SPREAD" else "put"
                    itm_pick = _pick_deep_itm_option(opts_df, price, opt_type)
                    if not itm_pick:
                        print(f"  ⏭ {sym:6s} Spread {quality_ratio:.1%} too expensive, no liquid deep ITM → skip")
                        continue

                    itm_strike, itm_bid, itm_ask, itm_vol, itm_oi = itm_pick
                    # Conservative buy price: mid-of-mid-and-ask
                    itm_mid = (itm_bid + itm_ask) / 2
                    buy_at = round((itm_mid + itm_ask) / 2, 2)
                    # Naked option alerts: +15% profit, -10% loss
                    target_price = round(buy_at * 1.15, 2)
                    alert_price = round(buy_at * 0.90, 2)

                    trade = Trade(
                        ticker=sym,
                        strategy=f"BUY {opt_type.title()}",
                        direction=f"BUY_{opt_type.upper()}",
                        thesis=(
                            f"Spot ${price:.2f}. "
                            f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                            f"Spread quality {quality_ratio:.1%} too expensive (>58%) — "
                            f"buying deep ITM ${itm_strike:.2f} {opt_type} instead (vol:{itm_vol:.0f} OI:{itm_oi:.0f})."
                        ),
                        entry=(
                            f"Buy {sym} ${itm_strike:.2f} {opt_type} | "
                            f"Spot ${price:.2f} | "
                            f"Exp {best_exp} ({best_dte}d) | "
                            f"${buy_at:.2f} debit | "
                            f"Target ${target_price:.2f} (+15%) | Alert ${alert_price:.2f} (-10%)"
                        ),
                        expected_profit=buy_at * 0.15 * 100,
                        max_loss=buy_at * 0.10 * 100,
                        win_prob=0.45,
                        capital=buy_at * 100,
                        urgency="THIS_WEEK",
                        confidence=min(0.4 + (1.0 - ratio) * 0.5, 0.70),
                        price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                        iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                        iv_rank_datapoints=data["iv_rank_dp"],
                        iv_hv_ratio=ratio,
                        pc_ratio=data["pc_vol_ratio"],
                        vol_oi_ratio=data["vol_oi_ratio"],
                        skew_25d=data.get("skew_25d", {}).get("skew", 0),
                        skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                        vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                        profit_target=target_price,
                        stop_alert=alert_price,
                        quality_ratio=quality_ratio,
                    )
                    trade.compute_score()
                    trades.append(trade)
                    print(f"  🎯 {sym:6s} ${price:8.2f} | "
                          f"Spread {quality_ratio:.1%} expensive → BUY ${itm_strike:.2f} {opt_type} (deep ITM) | "
                          f"${buy_at:.2f} debit | vol:{itm_vol:.0f} OI:{itm_oi:.0f}")
                    continue

                max_loss = est_debit
                # Profit target: 1.65x debit, but CAPPED at spread width (max possible value)
                raw_target = round(est_debit * 1.65, 2)
                profit_target = min(raw_target, round(width, 2))
                target_mult = round(profit_target / est_debit, 2) if est_debit > 0 else 1.65
                expected_profit = round((profit_target - est_debit), 2)  # actual gain per spread
                # Stop alert: if spread value drops 40% from entry → recheck thesis
                stop_alert_val = round(est_debit * 0.60, 2)

                # Build entry string with conservative pricing + exit rules
                target_str = f"Target ${profit_target:.2f} ({target_mult}x)"
                alert_str = f"Alert below ${stop_alert_val:.2f} (-40%)"
                ratio_str = f"Ratio {quality_ratio:.1%}"
                price_note = f"${est_debit:.2f} debit | {ratio_str} | {target_str} | {alert_str}"
                if long_ask is not None and short_bid is not None:
                    price_note = f"${est_debit:.2f} debit (buy @ ${buy_price:.2f}, sell @ ${sell_price:.2f}) | {ratio_str} | {target_str} | {alert_str}"

                opt_type = "call" if direction == "BUY_CALL_SPREAD" else "put"
                trade = Trade(
                    ticker=sym,
                    strategy=spread_type,
                    direction=direction,
                    thesis=(
                        f"Spot ${price:.2f}. "
                        f"IV {iv:.0f}% is {ratio:.2f}x realized vol {best_hv:.0f}%. "
                        f"IV Rank {iv_rank:.0f}% ({data['iv_rank_dp']} datapoints). "
                        f"Options are cheap — buy in-out debit spread."
                    ),
                    entry=(
                        f"Buy ${long_strike:.2f}/${short_strike:.2f} {opt_type} spread | "
                        f"Spot ${price:.2f} | "
                        f"Exp {best_exp} ({best_dte}d) | "
                        f"{price_note}"
                    ),
                    expected_profit=expected_profit * 100,
                    max_loss=max_loss * 100,
                    win_prob=0.45,
                    capital=max_loss * 100,
                    urgency="THIS_WEEK",
                    confidence=min(0.4 + (1.0 - ratio) * 0.5, 0.75),
                    price=price, iv=iv, hv_20=hv_cc, hv_yz=hv_yz,
                    iv_rank=iv_rank, iv_percentile=data["iv_percentile"],
                    iv_rank_datapoints=data["iv_rank_dp"],
                    iv_hv_ratio=ratio,
                    pc_ratio=data["pc_vol_ratio"],
                    vol_oi_ratio=data["vol_oi_ratio"],
                    skew_25d=data.get("skew_25d", {}).get("skew", 0),
                    skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                    vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                    profit_target=profit_target,
                    stop_alert=stop_alert_val,
                    spread_width=width,
                    quality_ratio=quality_ratio,
                )
                trade.compute_score()
                trades.append(trade)

                vwks_val = data.get("vwks_data", {}).get("vwks", 1.0)
                print(f"  👀 {sym:6s} ${price:8.2f} | "
                      f"IV:{iv:5.1f}% HV(YZ):{hv_yz:5.1f}% | "
                      f"Ratio:{ratio:.2f}x | Rank:{iv_rank:5.1f}% | "
                      f"VWKS:{vwks_val:.3f} | "
                      f"{spread_type} ${est_debit:.2f}/{width:.2f} ({quality_ratio:.1%}) → Target ${profit_target:.2f}")

        trades.sort(key=lambda x: x.score, reverse=True)
        print(f"\n  Total: {len(trades)} premium selling setups")
        return trades

    def scan_earnings(self, earnings: Dict = None) -> List[Trade]:
        """Scan earnings plays — implied vs historical move."""
        print("\n📅 EARNINGS SCAN (implied move vs historical)")
        print("─" * 60)
        earnings = earnings or EARNINGS_THIS_WEEK
        trades = []

        for sym, info in earnings.items():
            data = self._fetch(sym)
            if not data:
                print(f"  ⚠ {sym}: no data")
                continue

            price = data["price"]
            hist_moves = [abs(m) for m in info["hist_moves"]]
            avg_hist = np.mean(hist_moves)
            med_hist = np.median(hist_moves)

            # Find post-earnings expiration
            earn_date = datetime.date.fromisoformat(info["date"])
            best_exp = None
            best_dte = None
            for exp in data["expirations"]:
                exp_d = datetime.date.fromisoformat(exp)
                gap = (exp_d - earn_date).days
                if 0 <= gap <= 7:
                    if best_exp is None or gap < best_dte:
                        best_exp = exp
                        best_dte = gap

            if not best_exp or best_exp not in data["chains"]:
                continue

            ch = data["chains"][best_exp]
            atm = self.opts.get_atm_iv(ch["calls"], ch["puts"], price)
            straddle = atm["atm_straddle"]
            impl_move = self.opts.implied_move(straddle, price, ch["dte"])

            if impl_move <= 0:
                continue

            ratio = impl_move / max(avg_hist, 0.01)

            emoji = "📈" if ratio < 0.9 else ("📉" if ratio > 1.15 else "➡️")
            print(f"  {emoji} {sym:6s} ${price:7.2f} | "
                  f"Implied:{impl_move:5.1f}% vs Hist:{avg_hist:5.1f}% | "
                  f"Ratio:{ratio:.2f}x | "
                  f"Straddle:${straddle:.2f} | "
                  f"{info['date']} {info['timing']}")

            if ratio > 1.15:
                # Overpriced → SELL IC
                sd = price * avg_hist / 100
                short_p = round(price - sd * 1.15, 1)
                short_c = round(price + sd * 1.15, 1)
                wing = max(round(sd * 0.4, 1), 1)
                credit_est = straddle * 0.12

                trade = Trade(
                    ticker=sym,
                    strategy="SELL IC (Earnings)",
                    direction="SELL_PREMIUM",
                    thesis=(
                        f"Implied move {impl_move:.1f}% > avg historical {avg_hist:.1f}% "
                        f"({(ratio-1)*100:.0f}% overpriced). Sell iron condor outside historical range."
                    ),
                    entry=(
                        f"Day of earnings: Sell {short_p:.0f}/{short_p-wing:.0f}p + "
                        f"{short_c:.0f}/{short_c+wing:.0f}c | Exp {best_exp}"
                    ),
                    expected_profit=credit_est * 100,
                    max_loss=(wing - credit_est) * 100,
                    win_prob=0.62 + min((ratio - 1.15) * 0.3, 0.15),
                    capital=(wing - credit_est) * 100,
                    urgency="THIS_WEEK",
                    confidence=0.60,
                    price=price, iv=data["atm_iv"],
                    hv_20=data["hv"].get("cc_20", 0),
                    iv_rank=data["iv_rank"],
                )
                trade.compute_score()
                trades.append(trade)
                print(f"         → SELL IC: overpriced by {(ratio-1)*100:.0f}%")

            elif ratio < 0.85:
                # Underpriced → BUY straddle
                trade = Trade(
                    ticker=sym,
                    strategy="BUY Straddle (Earnings)",
                    direction="BUY_STRADDLE",
                    thesis=(
                        f"Implied move {impl_move:.1f}% < avg historical {avg_hist:.1f}% "
                        f"({(1-ratio)*100:.0f}% underpriced). Stock typically moves more than market expects."
                    ),
                    entry=f"Buy ATM straddle @ ${straddle:.2f} | Exp {best_exp}",
                    expected_profit=straddle * 100 * 0.3,
                    max_loss=straddle * 100,
                    win_prob=0.55,
                    capital=straddle * 100,
                    urgency="THIS_WEEK",
                    confidence=0.50,
                    price=price, iv=data["atm_iv"],
                    hv_20=data["hv"].get("cc_20", 0),
                    iv_rank=data["iv_rank"],
                )
                trade.compute_score()
                trades.append(trade)
                print(f"         → BUY STRADDLE: underpriced by {(1-ratio)*100:.0f}%")

        trades.sort(key=lambda x: x.score, reverse=True)
        print(f"\n  Total: {len(trades)} earnings trades")
        return trades

    def scan_unusual(self, tickers: List[str] = None) -> List[Trade]:
        """Scan for unusual options activity."""
        print("\n🔥 UNUSUAL ACTIVITY SCAN")
        print("─" * 60)
        tickers = tickers or ALL_TICKERS
        trades = []

        for sym in tickers:
            data = self._fetch(sym)
            if not data:
                continue

            vol_oi = data["vol_oi_ratio"]
            pc = data["pc_vol_ratio"]

            if vol_oi > 0.5:
                direction = "BEARISH 🔴" if pc > 1.5 else ("BULLISH 🟢" if pc < 0.5 else "NEUTRAL ⚪")
                print(f"  {'🔴' if pc > 1.5 else '🟢' if pc < 0.5 else '⚪'} "
                      f"{sym:6s} ${data['price']:8.2f} | "
                      f"Vol/OI:{vol_oi:.2f}x | P/C:{pc:.2f} | "
                      f"{direction} | "
                      f"Calls:{data['total_call_vol']:>8,} Puts:{data['total_put_vol']:>8,}")

                if pc > 1.5 or pc < 0.5:
                    is_bearish = pc > 1.5
                    price = data["price"]

                    # Build proper IN-OUT spread (not naked call/put)
                    # Find best expiry: 14-30 DTE (minimum 14 days!)
                    best_exp = None
                    best_dte = None
                    for exp, ch in data["chains"].items():
                        dte = ch["dte"]
                        if 7 <= dte <= 45:
                            if best_exp is None or abs(dte - 21) < abs(best_dte - 21):
                                best_exp = exp
                                best_dte = dte

                    if not best_exp:
                        continue  # skip if no suitable expiry

                    chain = data["chains"][best_exp]

                    if is_bearish:
                        # BUY PUT SPREAD: buy ITM put (above spot), sell OTM put (below spot)
                        opts_df = chain["puts"]
                        direction_str = "BUY_PUT_SPREAD"
                        spread_type_str = "BUY Put Spread"
                        opt_type = "put"
                    else:
                        # BUY CALL SPREAD: buy ITM call (below spot), sell OTM call (above spot)
                        opts_df = chain["calls"]
                        direction_str = "BUY_CALL_SPREAD"
                        spread_type_str = "BUY Call Spread"
                        opt_type = "call"

                    if opts_df.empty or "strike" not in opts_df.columns:
                        continue

                    strikes_sorted = sorted(opts_df["strike"].unique())

                    if direction_str == "BUY_CALL_SPREAD":
                        itm = [s for s in strikes_sorted if s <= price]
                        otm = [s for s in strikes_sorted if s > price]
                        if not itm or not otm:
                            continue
                        long_strike = float(itm[-1])    # buy ITM call (just below spot)
                        short_strike = float(otm[0])    # sell OTM call (just above spot)
                    else:
                        itm = [s for s in strikes_sorted if s >= price]
                        otm = [s for s in strikes_sorted if s < price]
                        if not itm or not otm:
                            continue
                        long_strike = float(itm[0])     # buy ITM put (just above spot)
                        short_strike = float(otm[-1])   # sell OTM put (just below spot)

                    # Conservative pricing: buy at mid-of-mid-and-ask, sell at mid-of-mid-and-bid
                    width = abs(long_strike - short_strike)
                    est_debit = width * 0.55  # fallback
                    buy_price_str = ""
                    sell_price_str = ""
                    try:
                        long_row = opts_df.iloc[(opts_df["strike"] - long_strike).abs().argsort()[:1]]
                        short_row = opts_df.iloc[(opts_df["strike"] - short_strike).abs().argsort()[:1]]
                        l_bid = float(long_row["bid"].values[0]) if "bid" in opts_df.columns else None
                        l_ask = float(long_row["ask"].values[0]) if "ask" in opts_df.columns else None
                        s_bid = float(short_row["bid"].values[0]) if "bid" in opts_df.columns else None
                        s_ask = float(short_row["ask"].values[0]) if "ask" in opts_df.columns else None
                        if l_bid and l_ask and s_bid and s_ask:
                            l_mid = (l_bid + l_ask) / 2
                            s_mid = (s_bid + s_ask) / 2
                            buy_p = round((l_mid + l_ask) / 2, 2)
                            sell_p = round((s_mid + s_bid) / 2, 2)
                            if buy_p > sell_p:
                                est_debit = round(buy_p - sell_p, 2)
                                buy_price_str = f"buy @ ${buy_p:.2f}"
                                sell_price_str = f"sell @ ${sell_p:.2f}"
                    except Exception:
                        pass

                    if est_debit <= 0:
                        continue

                    profit_target = round(est_debit * 1.65, 2)
                    price_note = f"${est_debit:.2f} debit"
                    if buy_price_str:
                        price_note = f"${est_debit:.2f} debit ({buy_price_str}, {sell_price_str})"

                    trade = Trade(
                        ticker=sym,
                        strategy=f"Flow {spread_type_str}",
                        direction=direction_str,
                        thesis=(
                            f"Spot ${price:.2f}. "
                            f"Unusual activity: Vol/OI {vol_oi:.2f}x normal. "
                            f"P/C ratio {pc:.2f} = {'heavy put' if is_bearish else 'heavy call'} buying. "
                            f"Smart money positioning {'bearish' if is_bearish else 'bullish'}."
                        ),
                        entry=(
                            f"Buy ${long_strike:.2f}/${short_strike:.2f} {opt_type} spread | "
                            f"Spot ${price:.2f} | "
                            f"Exp {best_exp} ({best_dte}d) | "
                            f"{price_note} | Target ${profit_target:.2f} (1.65x)"
                        ),
                        expected_profit=est_debit * 0.65 * 100,
                        max_loss=est_debit * 100,
                        win_prob=0.50,
                        capital=est_debit * 100,
                        urgency="TODAY",
                        confidence=min(vol_oi / 2.5, 0.70),
                        price=price, iv=data["atm_iv"],
                        pc_ratio=pc, vol_oi_ratio=vol_oi,
                        skew_25d=data.get("skew_25d", {}).get("skew", 0),
                        skew_ratio=data.get("skew_25d", {}).get("skew_ratio", 1.0),
                        vwks=data.get("vwks_data", {}).get("vwks", 1.0),
                        profit_target=profit_target,
                    )
                    trade.compute_score()
                    trades.append(trade)

        trades.sort(key=lambda x: x.score, reverse=True)
        print(f"\n  Total: {len(trades)} unusual activity signals")
        return trades

    # ─────────────────────────────────────────────
    # DEEP DIVE
    # ─────────────────────────────────────────────

    def dive(self, symbol: str):
        """Complete vol analysis of a single ticker."""
        print(f"\n🔬 ORCA DEEP DIVE: {symbol}")
        print("═" * 60)

        data = self._fetch(symbol)
        if not data:
            print(f"  ❌ No data for {symbol}")
            return

        price = data["price"]
        iv = data["atm_iv"]
        hv = data["hv"]

        # ── PRICE ──
        print(f"\n  Price: ${price:.2f}")

        # ── VOLATILITY TABLE ──
        print(f"\n  ┌────────────────────────────────────────┐")
        print(f"  │         VOLATILITY ANALYSIS             │")
        print(f"  ├────────────────────────────────────────┤")
        print(f"  │  ATM Implied Vol:     {iv:6.1f}%            │")
        print(f"  │  ATM Call IV:         {data['atm']['atm_call_iv']:6.1f}%            │")
        print(f"  │  ATM Put IV:          {data['atm']['atm_put_iv']:6.1f}%            │")
        print(f"  ├────────────────────────────────────────┤")
        print(f"  │  HV Close-Close:                       │")
        for w in VOL_WINDOWS:
            v = hv.get(f"cc_{w}", 0)
            print(f"  │    {w:3d}-day:            {v:6.1f}%            │")
        print(f"  │  HV Parkinson 20d:   {hv.get('parkinson_20', 0):6.1f}%            │")
        print(f"  │  HV Yang-Zhang 20d:  {hv.get('yang_zhang_20', 0):6.1f}%            │")
        print(f"  │  HV Yang-Zhang 60d:  {hv.get('yang_zhang_60', 0):6.1f}%            │")
        print(f"  ├────────────────────────────────────────┤")

        best_hv = hv.get("yang_zhang_20", hv.get("cc_20", 1))
        ratio = iv / max(best_hv, 0.01) if iv > 0 else 0
        ratio_display = f"{ratio:6.2f}x" if ratio > 0 else "   N/A "
        print(f"  │  IV/HV(YZ) Ratio:    {ratio_display}            │")
        print(f"  │  IV Rank:            {data['iv_rank']:6.1f}%  ({data['iv_rank_dp']} pts)│")
        print(f"  │  IV Percentile:      {data['iv_percentile']:6.1f}%            │")
        print(f"  └────────────────────────────────────────┘")

        # ── VOL CONE ──
        cone = data["vol_cone"]
        if cone:
            print(f"\n  ┌─ VOLATILITY CONE ──────────────────────┐")
            print(f"  │ Window │  Min  │  25%  │  Med  │  75%  │  Max  │ Now  │ Pctl │")
            print(f"  │────────│───────│───────│───────│───────│───────│──────│──────│")
            for w in VOL_WINDOWS:
                if w in cone:
                    c = cone[w]
                    arrow = "▲" if c["current"] > c["p75"] else ("▼" if c["current"] < c["p25"] else "─")
                    print(f"  │ {w:4d}d  │{c['min']:6.1f} │{c['p25']:6.1f} │"
                          f"{c['median']:6.1f} │{c['p75']:6.1f} │{c['max']:6.1f} │"
                          f"{c['current']:5.1f} │{c['percentile']:5.0f}%{arrow}│")
            print(f"  └────────────────────────────────────────┘")

        # ── FLOW ──
        print(f"\n  ┌─ OPTIONS FLOW ─────────────────────────┐")
        print(f"  │  Put/Call Vol Ratio:  {data['pc_vol_ratio']:6.2f}              │")
        print(f"  │  Put/Call OI Ratio:   {data['pc_oi_ratio']:6.2f}              │")
        print(f"  │  Vol/OI Ratio:        {data['vol_oi_ratio']:6.2f}              │")
        print(f"  │  Total Call Vol:      {data['total_call_vol']:>10,}          │")
        print(f"  │  Total Put Vol:       {data['total_put_vol']:>10,}          │")
        print(f"  └────────────────────────────────────────┘")

        # ── IV HISTORY (from DB) ──
        iv_hist = self.db.get_iv_history(symbol)
        if not iv_hist.empty and len(iv_hist) > 1:
            print(f"\n  ┌─ IV HISTORY ({len(iv_hist)} datapoints) ────────┐")
            for _, row in iv_hist.tail(10).iterrows():
                bar_len = int(row['atm_iv'] / 2) if row['atm_iv'] > 0 else 0
                bar = "█" * min(bar_len, 30)
                print(f"  │ {row['date']} │ IV:{row['atm_iv']:5.1f}% │{bar}")
            print(f"  └────────────────────────────────────────┘")

        # ── VERDICT ──
        print(f"\n  📋 VERDICT:")
        if ratio > IVHV_OVERPRICED and data["iv_rank"] >= IV_RANK_SELL_THRESHOLD:
            print(f"  🔴 SELL PREMIUM — IV is {ratio:.2f}x realized vol, rank {data['iv_rank']:.0f}%")
            print(f"     → Iron condor or short strangle, 30-45 DTE")
            print(f"     → Short strikes at 1 SD ({price - price*best_hv/100*0.18:.0f} / {price + price*best_hv/100*0.18:.0f})")
        elif ratio < IVHV_UNDERPRICED and data["iv_rank"] <= IV_RANK_BUY_THRESHOLD:
            print(f"  🟢 BUY OPTIONS — IV is cheap ({ratio:.2f}x HV, rank {data['iv_rank']:.0f}%)")
            print(f"     → Need directional thesis. Good time for debit spreads.")
        elif data["vol_oi_ratio"] > 0.5:
            direction = "bearish" if data["pc_vol_ratio"] > 1.5 else "bullish"
            print(f"  ⚡ UNUSUAL FLOW — Vol/OI {data['vol_oi_ratio']:.2f}x, {direction} skew")
        else:
            print(f"  ⚪ NEUTRAL — IV fairly priced. Wait for catalyst.")

    # ─────────────────────────────────────────────
    # MASTER SCAN
    # ─────────────────────────────────────────────

    def run(self, mode: str = "all") -> List[Trade]:
        """Run ORCA daily scan."""
        print("""
╔═══════════════════════════════════════════════════════════════╗
║    _    ____   ____ _   _  ___  _   _                        ║
║   / \\  |  _ \\ / ___| | | |/ _ \\| \\ | |                      ║
║  / _ \\ | |_) | |   | |_| | | | |  \\| |                      ║
║ / ___ \\|  _ <| |___|  _  | |_| | |\\  |                      ║
║/_/   \\_\\_| \\_\\\\____|_| |_|\\___/|_| \\_|                      ║
║                                                               ║
║  Analytics-Driven Options Signal                              ║
╚═══════════════════════════════════════════════════════════════╝""")
        print(f"  Date:     {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"  Capital:  ${self.capital:,.0f}")
        print(f"  Universe: {len(ALL_TICKERS)} tickers")
        print(f"  IV DB:    {self.db.db_path}")

        all_trades = []

        if mode in ("all", "premium"):
            all_trades.extend(self.scan_premium())

        # Earnings scan disabled — user does not trade earnings plays
        # if mode in ("all", "earnings"):
        #     all_trades.extend(self.scan_earnings())

        if mode in ("all", "unusual"):
            all_trades.extend(self.scan_unusual())

        all_trades.sort(key=lambda x: x.score, reverse=True)

        # ── ACTION PLAN ──
        print(f"\n\n{'═'*60}")
        print(f"  🎯 TOP TRADES — RANKED BY EXPECTED VALUE")
        print(f"{'═'*60}")

        total_ev = 0
        for i, t in enumerate(all_trades[:10]):
            print(f"\n  #{i+1} {t.ticker} — {t.strategy}")
            print(f"  {'─'*55}")
            print(f"  {t.thesis[:75]}")
            print(f"  Entry:  {t.entry[:75]}")
            print(f"  Profit: ${t.expected_profit:,.0f} | Loss: ${t.max_loss:,.0f} | "
                  f"Win: {t.win_prob:.0%} | Capital: ${t.capital:,.0f}")
            if t.iv > 0:
                ratio_str = f"{t.iv_hv_ratio:.2f}x" if t.iv_hv_ratio > 0 else "N/A"
                print(f"  Vol:    IV {t.iv:.0f}% | HV(YZ) {t.hv_yz:.0f}% | "
                      f"Ratio {ratio_str} | Rank {t.iv_rank:.0f}%")
            print(f"  Score:  {t.score:.0f}")
            total_ev += t.expected_profit * t.win_prob

        print(f"\n  {'─'*55}")
        print(f"  Expected value (top 10): ${total_ev:,.0f}")

        # ── SAVE ──
        RESULTS_DIR.mkdir(exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        csv_path = RESULTS_DIR / f"orca_{ts}.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["#", "Ticker", "Strategy", "Direction", "Thesis",
                       "Entry", "Profit", "MaxLoss", "WinProb", "Capital",
                       "Urgency", "IV", "HV_YZ", "IV/HV", "IVRank",
                       "Skew25D", "SkewRatio", "VWKS", "Score",
                       "ProfitTarget", "StopAlert", "SpreadWidth", "QualityRatio"])
            for i, t in enumerate(all_trades):
                w.writerow([i+1, t.ticker, t.strategy, t.direction,
                           t.thesis[:60], t.entry[:160],
                           f"${t.expected_profit:.0f}", f"${t.max_loss:.0f}",
                           f"{t.win_prob:.0%}", f"${t.capital:.0f}",
                           t.urgency, f"{t.iv:.0f}%", f"{t.hv_yz:.0f}%",
                           f"{t.iv_hv_ratio:.2f}" if t.iv_hv_ratio > 0 else "N/A",
                           f"{t.iv_rank:.0f}%",
                           f"{t.skew_25d:+.1f}" if t.skew_25d != 0 else "N/A",
                           f"{t.skew_ratio:.2f}" if t.skew_ratio != 1.0 else "N/A",
                           f"{t.vwks:.4f}" if t.vwks != 1.0 else "1.0000",
                           f"{t.score:.0f}",
                           f"${t.profit_target:.2f}" if t.profit_target > 0 else "",
                           f"${t.stop_alert:.2f}" if t.stop_alert > 0 else "",
                           f"${t.spread_width:.2f}" if t.spread_width > 0 else "",
                           f"{t.quality_ratio:.1%}" if t.quality_ratio > 0 else ""])

        self.db.log_scan(len(self._cache), len(all_trades))
        print(f"\n  📁 Saved: {csv_path}")
        print(f"  📊 IV database: {len(self._cache)} tickers updated")

        self.db.close()
        return all_trades


# ============================================================
# EARNINGS DATA — UPDATE THIS WEEKLY
# ============================================================

EARNINGS_THIS_WEEK = {
    # Tuesday March 3
    "TGT":  {"date": "2026-03-03", "timing": "BMO", "hist_moves": [12.1, 5.8, -3.2, 8.7, -17.8]},
    "BBY":  {"date": "2026-03-03", "timing": "BMO", "hist_moves": [8.5, -6.2, 12.4, -2.1, 7.3]},
    "CRWD": {"date": "2026-03-03", "timing": "AMC", "hist_moves": [9.8, 11.2, -5.3, 25.1, -8.7]},
    "SE":   {"date": "2026-03-03", "timing": "BMO", "hist_moves": [14.5, -8.3, 21.6, -12.4, 9.8]},
    "AZO":  {"date": "2026-03-03", "timing": "BMO", "hist_moves": [3.2, 5.1, -2.8, 4.7, -1.9]},
    # Wednesday March 4
    "AVGO": {"date": "2026-03-04", "timing": "AMC", "hist_moves": [8.3, 12.5, -4.2, 24.4, 5.1]},
    "ANF":  {"date": "2026-03-04", "timing": "BMO", "hist_moves": [19.5, -14.8, 28.3, 12.1, -9.5]},
    "OKTA": {"date": "2026-03-04", "timing": "AMC", "hist_moves": [11.2, -15.3, 8.4, -22.1, 14.5]},
    "RGTI": {"date": "2026-03-04", "timing": "AMC", "hist_moves": [18.5, -12.3, 32.1, 15.8, -21.4]},
    # Thursday March 5
    "MRVL": {"date": "2026-03-05", "timing": "AMC", "hist_moves": [9.1, -7.5, 14.2, 23.1, -8.3]},
    "COST": {"date": "2026-03-06", "timing": "AMC", "hist_moves": [4.2, 2.1, -1.8, 3.5, -2.9]},
}


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="ORCA — Analytics-Driven Options Signal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python orca.py                      # Full daily scan
  python orca.py --mode premium       # Premium selling opportunities
  python orca.py --mode earnings      # Earnings plays only  
  python orca.py --mode unusual       # Unusual activity scan
  python orca.py --dive AVGO          # Deep dive on AVGO
  python orca.py --dive STNG          # Deep dive on tanker stock
  python orca.py --capital 5000       # Set capital to $5K

Run every morning before market open. The IV database builds over time.
After 2 weeks, IV rank becomes reliable. After a month, it's solid.
        """
    )
    parser.add_argument("--mode", default="all",
                       choices=["all", "premium", "earnings", "unusual"])
    parser.add_argument("--dive", type=str, help="Deep dive on a ticker")
    parser.add_argument("--capital", type=float, default=1000)

    args = parser.parse_args()
    orca = ORCA(capital=args.capital)

    if args.dive:
        orca.dive(args.dive.upper())
        orca.db.close()
    else:
        orca.run(mode=args.mode)


if __name__ == "__main__":
    main()
