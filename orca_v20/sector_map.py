"""
ORCA v20 — Ticker → Sector mapping + dynamic resolution.

B4: Real sector classification for concentration control.

Resolution order (4-tier fallback):
    1. Curated static map (~200 tickers)
    2. Persistent DB cache (sector_cache table)
    3. Live yfinance lookup (short-timeout, graceful)
    4. "UNKNOWN" fallback

GICS-inspired sector labels (simplified for options trading):
    ENERGY, TECH, FINANCIALS, HEALTHCARE, CONSUMER_DISC,
    CONSUMER_STAPLES, INDUSTRIALS, MATERIALS, UTILITIES,
    REAL_ESTATE, COMM_SERVICES, AEROSPACE_DEFENSE,
    TRANSPORT, ETF_BROAD, ETF_SECTOR, CRYPTO, UNKNOWN
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("orca_v20.sector_map")

# ── Sector lookup (static, covers top ~200 weeklies tickers) ──

_SECTOR_MAP = {
    # ENERGY
    "XOM": "ENERGY", "CVX": "ENERGY", "COP": "ENERGY", "EOG": "ENERGY",
    "SLB": "ENERGY", "MPC": "ENERGY", "VLO": "ENERGY", "PSX": "ENERGY",
    "PXD": "ENERGY", "OXY": "ENERGY", "DVN": "ENERGY", "HAL": "ENERGY",
    "FANG": "ENERGY", "HES": "ENERGY", "MRO": "ENERGY", "APA": "ENERGY",
    "BKR": "ENERGY", "KMI": "ENERGY", "WMB": "ENERGY", "OKE": "ENERGY",
    "EQT": "ENERGY", "PBF": "ENERGY", "LNG": "ENERGY",
    "XLE": "ETF_SECTOR", "USO": "ETF_SECTOR", "XOP": "ETF_SECTOR",
    "UNG": "ETF_SECTOR",

    # TECH
    "AAPL": "TECH", "MSFT": "TECH", "GOOGL": "TECH", "GOOG": "TECH",
    "AMZN": "TECH", "NVDA": "TECH", "META": "TECH", "TSLA": "TECH",
    "AMD": "TECH", "INTC": "TECH", "CRM": "TECH", "ORCL": "TECH",
    "ADBE": "TECH", "NFLX": "TECH", "AVGO": "TECH", "QCOM": "TECH",
    "MU": "TECH", "AMAT": "TECH", "LRCX": "TECH", "KLAC": "TECH",
    "MRVL": "TECH", "SNPS": "TECH", "CDNS": "TECH", "NOW": "TECH",
    "SHOP": "TECH", "SNOW": "TECH", "DDOG": "TECH", "NET": "TECH",
    "PANW": "TECH", "CRWD": "TECH", "ZS": "TECH", "FTNT": "TECH",
    "PLTR": "TECH", "SMCI": "TECH", "ARM": "TECH", "DELL": "TECH",

    # FINANCIALS
    "JPM": "FINANCIALS", "BAC": "FINANCIALS", "WFC": "FINANCIALS",
    "GS": "FINANCIALS", "MS": "FINANCIALS", "C": "FINANCIALS",
    "BLK": "FINANCIALS", "SCHW": "FINANCIALS", "AXP": "FINANCIALS",
    "V": "FINANCIALS", "MA": "FINANCIALS", "PYPL": "FINANCIALS",
    "SQ": "FINANCIALS", "COF": "FINANCIALS", "USB": "FINANCIALS",
    "PNC": "FINANCIALS", "XLF": "ETF_SECTOR",

    # HEALTHCARE
    "UNH": "HEALTHCARE", "JNJ": "HEALTHCARE", "LLY": "HEALTHCARE",
    "PFE": "HEALTHCARE", "ABBV": "HEALTHCARE", "MRK": "HEALTHCARE",
    "TMO": "HEALTHCARE", "ABT": "HEALTHCARE", "BMY": "HEALTHCARE",
    "AMGN": "HEALTHCARE", "GILD": "HEALTHCARE", "ISRG": "HEALTHCARE",
    "MDT": "HEALTHCARE", "REGN": "HEALTHCARE", "VRTX": "HEALTHCARE",
    "MRNA": "HEALTHCARE", "BIIB": "HEALTHCARE", "XLV": "ETF_SECTOR",

    # CONSUMER DISCRETIONARY
    "HD": "CONSUMER_DISC", "LOW": "CONSUMER_DISC", "NKE": "CONSUMER_DISC",
    "SBUX": "CONSUMER_DISC", "MCD": "CONSUMER_DISC", "TGT": "CONSUMER_DISC",
    "TJX": "CONSUMER_DISC", "ROST": "CONSUMER_DISC", "LULU": "CONSUMER_DISC",
    "CMG": "CONSUMER_DISC", "DRI": "CONSUMER_DISC", "BKNG": "CONSUMER_DISC",
    "ABNB": "CONSUMER_DISC", "GM": "CONSUMER_DISC", "F": "CONSUMER_DISC",
    "XLY": "ETF_SECTOR",

    # CONSUMER STAPLES
    "PG": "CONSUMER_STAPLES", "KO": "CONSUMER_STAPLES", "PEP": "CONSUMER_STAPLES",
    "COST": "CONSUMER_STAPLES", "WMT": "CONSUMER_STAPLES", "PM": "CONSUMER_STAPLES",
    "MO": "CONSUMER_STAPLES", "CL": "CONSUMER_STAPLES", "XLP": "ETF_SECTOR",

    # INDUSTRIALS
    "CAT": "INDUSTRIALS", "DE": "INDUSTRIALS", "UPS": "INDUSTRIALS",
    "FDX": "INDUSTRIALS", "RTX": "INDUSTRIALS", "HON": "INDUSTRIALS",
    "GE": "INDUSTRIALS", "MMM": "INDUSTRIALS", "UNP": "INDUSTRIALS",
    "CSX": "INDUSTRIALS", "NSC": "INDUSTRIALS", "XLI": "ETF_SECTOR",

    # AEROSPACE / DEFENSE
    "BA": "AEROSPACE_DEFENSE", "LMT": "AEROSPACE_DEFENSE",
    "NOC": "AEROSPACE_DEFENSE", "GD": "AEROSPACE_DEFENSE",
    "LHX": "AEROSPACE_DEFENSE",

    # TRANSPORT / SHIPPING / AIRLINES
    "DAL": "TRANSPORT", "UAL": "TRANSPORT", "AAL": "TRANSPORT",
    "LUV": "TRANSPORT", "JBLU": "TRANSPORT",
    "ZIM": "TRANSPORT", "MATX": "TRANSPORT", "GOGL": "TRANSPORT",
    "STNG": "TRANSPORT", "FLNG": "TRANSPORT", "FRO": "TRANSPORT",

    # MATERIALS
    "FCX": "MATERIALS", "NEM": "MATERIALS", "DOW": "MATERIALS",
    "APD": "MATERIALS", "ECL": "MATERIALS", "NUE": "MATERIALS",
    "CLF": "MATERIALS", "X": "MATERIALS", "AA": "MATERIALS",
    "GLD": "ETF_SECTOR", "SLV": "ETF_SECTOR", "XLB": "ETF_SECTOR",

    # UTILITIES
    "NEE": "UTILITIES", "DUK": "UTILITIES", "SO": "UTILITIES",
    "D": "UTILITIES", "AEP": "UTILITIES", "XLU": "ETF_SECTOR",

    # REAL ESTATE
    "AMT": "REAL_ESTATE", "PLD": "REAL_ESTATE", "CCI": "REAL_ESTATE",
    "SPG": "REAL_ESTATE", "O": "REAL_ESTATE", "XLRE": "ETF_SECTOR",

    # COMM SERVICES
    "DIS": "COMM_SERVICES", "CMCSA": "COMM_SERVICES", "T": "COMM_SERVICES",
    "VZ": "COMM_SERVICES", "TMUS": "COMM_SERVICES", "XLC": "ETF_SECTOR",

    # CRYPTO-ADJACENT
    "COIN": "CRYPTO", "MSTR": "CRYPTO", "MARA": "CRYPTO",
    "RIOT": "CRYPTO", "BITO": "ETF_SECTOR",

    # BROAD ETFs
    "SPY": "ETF_BROAD", "QQQ": "ETF_BROAD", "IWM": "ETF_BROAD",
    "DIA": "ETF_BROAD", "VOO": "ETF_BROAD", "VTI": "ETF_BROAD",
    "EEM": "ETF_BROAD", "EFA": "ETF_BROAD", "TLT": "ETF_BROAD",
    "HYG": "ETF_BROAD", "LQD": "ETF_BROAD", "VXX": "ETF_BROAD",
    "UVXY": "ETF_BROAD", "SQQQ": "ETF_BROAD", "TQQQ": "ETF_BROAD",
}


# ── yfinance sector string → our GICS-inspired labels ──

_YFINANCE_SECTOR_MAP = {
    "Technology": "TECH",
    "Energy": "ENERGY",
    "Financial Services": "FINANCIALS",
    "Healthcare": "HEALTHCARE",
    "Consumer Cyclical": "CONSUMER_DISC",
    "Consumer Defensive": "CONSUMER_STAPLES",
    "Industrials": "INDUSTRIALS",
    "Basic Materials": "MATERIALS",
    "Utilities": "UTILITIES",
    "Real Estate": "REAL_ESTATE",
    "Communication Services": "COMM_SERVICES",
}

# Cache TTL for dynamically resolved sectors (30 days)
_CACHE_TTL_DAYS = 30


# ─────────────────────────────────────────────────────────────────────
# Legacy fast-path (unchanged, backward compatible)
# ─────────────────────────────────────────────────────────────────────

def get_sector(ticker: str) -> str:
    """Return the sector for a ticker. Falls back to 'UNKNOWN'.

    Fast path — static map only. Use resolve_sector() for full
    4-tier resolution with DB cache + live lookup.
    """
    return _SECTOR_MAP.get(ticker.upper(), "UNKNOWN")


def get_sector_for_positions(positions: list) -> dict:
    """
    Given a list of positions (dicts with 'ticker' key),
    return {sector: count} dict.
    """
    sectors = {}
    for pos in positions:
        sector = get_sector(pos.get("ticker", "?"))
        sectors[sector] = sectors.get(sector, 0) + 1
    return sectors


# ─────────────────────────────────────────────────────────────────────
# 4-tier sector resolver
# ─────────────────────────────────────────────────────────────────────

def resolve_sector(ticker: str, allow_live: bool = True) -> Tuple[str, str, str]:
    """
    Resolve sector for a ticker using 4-tier fallback:
        1. Curated static map
        2. Persistent DB cache (sector_cache table)
        3. Live yfinance lookup (short-timeout, graceful)
        4. "UNKNOWN" fallback

    Returns (sector, industry, source) where source is one of:
        "curated", "yfinance", "unknown"

    Never raises — all failures degrade to ("UNKNOWN", "", "unknown").
    """
    t = ticker.upper().strip()
    if not t:
        return ("UNKNOWN", "", "unknown")

    # ── Tier 1: Curated static map ──
    if t in _SECTOR_MAP:
        return (_SECTOR_MAP[t], "", "curated")

    # ── Tier 2: DB cache ──
    cached = _check_cache(t)
    if cached is not None:
        return cached

    # ── Tier 3: Live yfinance lookup ──
    if allow_live:
        sector, industry = _live_lookup(t)
        if sector != "UNKNOWN":
            _write_cache(t, sector, industry, "yfinance")
            return (sector, industry, "yfinance")

    # ── Tier 4: UNKNOWN fallback ──
    _write_cache(t, "UNKNOWN", "", "unknown")
    return ("UNKNOWN", "", "unknown")


def resolve_sector_for_positions(positions: list, allow_live: bool = True) -> Dict[str, int]:
    """
    Like get_sector_for_positions but uses the full 4-tier resolver.

    Returns {sector: count} dict.
    """
    sectors: Dict[str, int] = {}
    for pos in positions:
        sector, _, _ = resolve_sector(pos.get("ticker", "?"), allow_live=allow_live)
        sectors[sector] = sectors.get(sector, 0) + 1
    return sectors


# ─────────────────────────────────────────────────────────────────────
# DB cache layer
# ─────────────────────────────────────────────────────────────────────

def _check_cache(ticker: str) -> Optional[Tuple[str, str, str]]:
    """
    Check sector_cache table for a cached resolution.

    Returns (sector, industry, source) or None if not found / expired.
    """
    try:
        from orca_v20.db_bootstrap import get_connection
        conn = get_connection()
        row = conn.execute(
            "SELECT sector, industry, source, expires_utc FROM sector_cache WHERE ticker = ?",
            (ticker.upper(),)
        ).fetchone()
        conn.close()

        if row is None:
            return None

        # Check expiry (curated entries have NULL expires_utc = never expire)
        expires = row["expires_utc"]
        if expires:
            try:
                exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) > exp_dt:
                    logger.debug(f"[sector_cache] {ticker} expired, will re-resolve")
                    return None
            except (ValueError, TypeError):
                pass  # Malformed expiry → treat as valid

        return (row["sector"], row["industry"] or "", row["source"])

    except Exception as e:
        logger.debug(f"[sector_cache] Cache check failed for {ticker}: {e}")
        return None


def _write_cache(ticker: str, sector: str, industry: str, source: str) -> None:
    """Persist a sector resolution to the DB cache."""
    try:
        from orca_v20.db_bootstrap import get_connection
        now = datetime.now(timezone.utc).isoformat()

        # Curated entries never expire; dynamic entries expire after _CACHE_TTL_DAYS
        expires = None
        if source != "curated":
            expires = (datetime.now(timezone.utc) + timedelta(days=_CACHE_TTL_DAYS)).isoformat()

        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO sector_cache
                (ticker, sector, industry, source, resolved_utc, expires_utc)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ticker.upper(), sector, industry, source, now, expires))
        conn.commit()
        conn.close()

    except Exception as e:
        logger.debug(f"[sector_cache] Cache write failed for {ticker}: {e}")


# ─────────────────────────────────────────────────────────────────────
# Live yfinance lookup
# ─────────────────────────────────────────────────────────────────────

def _live_lookup(ticker: str) -> Tuple[str, str]:
    """
    Fetch sector + industry from yfinance.

    Short timeout, graceful failure.  Never hard-fails the engine.
    Returns (sector, industry) or ("UNKNOWN", "") on any error.
    """
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker.upper())
        info = tk.info or {}

        # ETF detection — yfinance uses quoteType
        quote_type = info.get("quoteType", "")
        if quote_type == "ETF":
            return ("ETF_BROAD", "")

        # Equity sector lookup
        yf_sector = info.get("sector", "")
        yf_industry = info.get("industry", "")

        if yf_sector:
            mapped = _YFINANCE_SECTOR_MAP.get(yf_sector, "")
            if mapped:
                logger.debug(f"[sector_live] {ticker} → {mapped} ({yf_industry})")
                return (mapped, yf_industry)
            else:
                # Unmapped yfinance sector — log for manual review
                logger.info(f"[sector_live] {ticker} has unmapped yfinance sector: '{yf_sector}'")
                return ("UNKNOWN", yf_industry)

        return ("UNKNOWN", "")

    except Exception as e:
        logger.debug(f"[sector_live] yfinance lookup failed for {ticker}: {e}")
        return ("UNKNOWN", "")


# ─────────────────────────────────────────────────────────────────────
# Unknown-sector review queue
# ─────────────────────────────────────────────────────────────────────

def get_unknown_tickers() -> List[Dict]:
    """
    Query all tickers that resolved to UNKNOWN.

    Returns list of dicts for operator reporting / gradual backfill.
    """
    try:
        from orca_v20.db_bootstrap import get_connection
        conn = get_connection()
        rows = conn.execute("""
            SELECT ticker, industry, resolved_utc
            FROM sector_cache
            WHERE source = 'unknown'
            ORDER BY resolved_utc DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    except Exception as e:
        logger.debug(f"[sector_cache] Failed to query unknowns: {e}")
        return []


def get_dynamically_resolved_tickers() -> List[Dict]:
    """
    Query all tickers resolved via yfinance (not curated).

    Returns list of dicts for operator reporting.
    """
    try:
        from orca_v20.db_bootstrap import get_connection
        conn = get_connection()
        rows = conn.execute("""
            SELECT ticker, sector, industry, resolved_utc, expires_utc
            FROM sector_cache
            WHERE source = 'yfinance'
            ORDER BY resolved_utc DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    except Exception as e:
        logger.debug(f"[sector_cache] Failed to query dynamic: {e}")
        return []


def backfill_curated(ticker: str, sector: str, industry: str = "") -> None:
    """
    Promote a ticker to curated status in the cache.

    Use this to gradually backfill the static map from observation data.
    Sets source='curated' and removes expiry.
    """
    _write_cache(ticker.upper(), sector, industry, "curated")
    logger.info(f"[sector_cache] Backfilled {ticker} → {sector} (curated)")
