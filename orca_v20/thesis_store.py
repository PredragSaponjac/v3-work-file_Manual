"""
ORCA v20 — Thesis Persistence Store.

Manages thesis lifecycle: create, match, update, close.
Every idea is matched against existing theses before creating new ones.

Matching uses TF-IDF + cosine similarity on thesis_text + catalyst.
No external embedding model needed — pure sklearn/stdlib.

Key fields per idea:
    thesis_id               — assigned UUID for this thesis
    matched_existing_thesis_id — if matched to a prior thesis
    match_confidence        — cosine similarity of match
    thesis_status           — DRAFT → ACTIVE → CLOSED_*
"""

import hashlib
import json
import logging
import math
import os
import re
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import (
    IdeaCandidate, IdeaDirection, Thesis, ThesisDailySnapshot,
    ThesisStatus, CatalystStatus,
)

logger = logging.getLogger("orca_v20.thesis_store")


# ─────────────────────────────────────────────────────────────────────
# Lightweight TF-IDF embedding (no external deps)
# ─────────────────────────────────────────────────────────────────────

from orca_v20.text_utils import tokenize as _tokenize  # shared helper
from orca_v20.text_utils import _STOP_WORDS  # re-export for compatibility


def _build_tfidf_vector(tokens: List[str], idf_weights: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    """
    Build a TF-IDF vector.
    If idf_weights provided, applies real IDF. Otherwise pure TF.
    """
    counts = Counter(tokens)
    total = len(tokens) or 1
    vec = {}
    for word, count in counts.items():
        tf = count / total
        idf = idf_weights.get(word, 1.0) if idf_weights else 1.0
        vec[word] = tf * idf
    return vec


def _cosine_similarity(a: Dict[str, float], b: Dict[str, float]) -> float:
    """Cosine similarity between two sparse TF vectors."""
    if not a or not b:
        return 0.0
    common = set(a.keys()) & set(b.keys())
    dot = sum(a[k] * b[k] for k in common)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _idea_to_embedding_text(idea: IdeaCandidate) -> str:
    """Combine thesis + catalyst + ticker for matching."""
    return f"{idea.ticker} {idea.catalyst} {idea.thesis}"


def _compute_idf_from_corpus(documents: List[str]) -> Dict[str, float]:
    """
    B6: Compute real IDF weights from a corpus of thesis documents.
    IDF = log(N / (1 + df)) where df = # of documents containing the term.
    Rare terms get high IDF, common terms get low IDF.
    """
    if not documents:
        return {}

    n_docs = len(documents)
    doc_freq = Counter()

    for doc in documents:
        tokens = set(_tokenize(doc))
        for token in tokens:
            doc_freq[token] += 1

    idf = {}
    for word, df in doc_freq.items():
        idf[word] = math.log(n_docs / (1.0 + df)) + 1.0  # +1 smoothing

    return idf


def _get_thesis_corpus() -> List[str]:
    """Load all thesis texts from DB for IDF computation."""
    try:
        conn = get_connection()
        rows = conn.execute("SELECT thesis_text, catalyst FROM theses").fetchall()
        conn.close()
        return [f"{r['catalyst']} {r['thesis_text']}" for r in rows if r['thesis_text']]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────
# Core functions
# ─────────────────────────────────────────────────────────────────────

def get_active_theses(ticker: Optional[str] = None) -> List[Thesis]:
    """Query all active/draft theses, optionally filtered by ticker."""
    if not FLAGS.enable_thesis_persistence:
        return []

    try:
        conn = get_connection()
        if ticker:
            rows = conn.execute(
                "SELECT * FROM theses WHERE status IN ('ACTIVE','DRAFT') AND ticker = ?",
                (ticker.upper(),)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM theses WHERE status IN ('ACTIVE','DRAFT')"
            ).fetchall()
        conn.close()

        theses = []
        for r in rows:
            t = Thesis(
                thesis_id=r["thesis_id"],
                ticker=r["ticker"],
                idea_direction=IdeaDirection(r["idea_direction"]),
                catalyst=r["catalyst"] or "",
                expected_horizon=r["expected_horizon"] or "UNKNOWN",
                thesis_text=r["thesis_text"] or "",
                status=ThesisStatus(r["status"]),
                created_run_id=r["created_run_id"] or "",
                created_utc=r["created_utc"] or "",
                initial_confidence=r["initial_confidence"] or 0,
                current_confidence=r["current_confidence"] or 0,
                confidence_slope=r["confidence_slope"] or 0.0,
                times_seen=r["times_seen"] or 1,
                invalidation_trigger=r["invalidation_trigger"] or "",
            )
            theses.append(t)
        return theses

    except Exception as e:
        logger.error(f"Failed to query active theses: {e}")
        return []


def match_to_existing(idea: IdeaCandidate, ctx: RunContext) -> IdeaCandidate:
    """
    Check if this idea matches an existing active thesis.

    Uses TF-IDF cosine similarity on thesis_text + catalyst.
    Threshold: THRESHOLDS.thesis_match_threshold (default 0.80).
    """
    if not FLAGS.enable_thesis_persistence:
        idea.thesis_id = uuid.uuid4().hex[:12]
        idea.thesis_status = ThesisStatus.DRAFT
        return idea

    # Get existing theses for this ticker
    existing = get_active_theses(ticker=idea.ticker)

    if existing:
        # B6: Compute IDF weights from thesis corpus for better rare-term matching
        corpus = _get_thesis_corpus()
        idf_weights = _compute_idf_from_corpus(corpus) if len(corpus) >= 3 else None

        idea_text = _idea_to_embedding_text(idea)
        idea_tokens = _tokenize(idea_text)
        idea_vec = _build_tfidf_vector(idea_tokens, idf_weights)

        best_sim = 0.0
        best_thesis = None

        for thesis in existing:
            # FIX: Only match theses with the same direction — cosine similarity
            # alone could merge a BULLISH idea into a BEARISH thesis on the same ticker.
            if thesis.idea_direction != idea.idea_direction:
                continue

            thesis_text = f"{thesis.ticker} {thesis.catalyst} {thesis.thesis_text}"
            thesis_tokens = _tokenize(thesis_text)
            thesis_vec = _build_tfidf_vector(thesis_tokens, idf_weights)

            sim = _cosine_similarity(idea_vec, thesis_vec)
            if sim > best_sim:
                best_sim = sim
                best_thesis = thesis

        if best_thesis and best_sim >= THRESHOLDS.thesis_match_threshold:
            # Match found — reuse existing thesis
            idea.thesis_id = best_thesis.thesis_id
            idea.matched_existing_thesis_id = best_thesis.thesis_id
            idea.match_confidence = best_sim
            idea.thesis_status = best_thesis.status
            logger.info(
                f"  [{idea.ticker}] Matched existing thesis {best_thesis.thesis_id} "
                f"(sim={best_sim:.3f}, seen={best_thesis.times_seen}x)"
            )
            # Update times_seen
            _increment_times_seen(best_thesis.thesis_id, idea.confidence, ctx)
            return idea

    # No match — create new thesis
    idea.thesis_id = uuid.uuid4().hex[:12]
    idea.matched_existing_thesis_id = None
    idea.match_confidence = 0.0
    idea.thesis_status = ThesisStatus.DRAFT
    logger.info(f"  [{idea.ticker}] New thesis {idea.thesis_id}")
    return idea


def _increment_times_seen(thesis_id: str, new_confidence: int, ctx: RunContext) -> None:
    """Increment times_seen and update confidence for a matched thesis."""
    if ctx.dry_run:
        return
    try:
        conn = get_connection()
        conn.execute("""
            UPDATE theses
            SET times_seen = times_seen + 1,
                current_confidence = ?,
                last_updated_run_id = ?,
                last_updated_utc = ?
            WHERE thesis_id = ?
        """, (
            new_confidence,
            ctx.run_id,
            datetime.now(timezone.utc).isoformat(),
            thesis_id,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to update times_seen for {thesis_id}: {e}")


def persist_thesis(idea: IdeaCandidate, ctx: RunContext) -> None:
    """Write a new thesis to orca_v20.db (INSERT only for new theses)."""
    if not FLAGS.enable_thesis_persistence or ctx.dry_run:
        return

    # Only persist if this is a NEW thesis (not matched)
    if idea.matched_existing_thesis_id:
        return

    try:
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        # Resolve expected_horizon: use idea field if available, else UNKNOWN
        horizon_val = "UNKNOWN"
        if hasattr(idea, 'expected_horizon') and idea.expected_horizon:
            horizon_val = idea.expected_horizon.value if hasattr(idea.expected_horizon, 'value') else str(idea.expected_horizon)

        conn.execute("""
            INSERT OR IGNORE INTO theses (
                thesis_id, ticker, idea_direction, catalyst, thesis_text,
                status, created_run_id, created_utc,
                last_updated_run_id, last_updated_utc,
                initial_confidence, current_confidence, confidence_slope, times_seen,
                invalidation_trigger, expected_horizon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            idea.thesis_id,
            idea.ticker,
            idea.idea_direction.value,
            idea.catalyst,
            idea.thesis,
            ThesisStatus.DRAFT.value,
            ctx.run_id,
            now,
            ctx.run_id,
            now,
            idea.confidence,
            idea.confidence,
            0.0,
            1,
            idea.invalidation,
            horizon_val,
        ))
        conn.commit()
        conn.close()
        logger.debug(f"  [{idea.ticker}] Thesis {idea.thesis_id} persisted to DB")
    except Exception as e:
        logger.error(f"Failed to persist thesis for {idea.ticker}: {e}")
        ctx.add_error("thesis_store", f"{idea.ticker}: {e}")


def _fetch_underlying_price(ticker: str) -> Optional[float]:
    """Fetch current underlying price via yfinance. Returns None on failure."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, "last_price", None)
        if price and price > 0:
            return round(float(price), 2)
    except Exception as e:
        logger.debug(f"  [{ticker}] Price fetch failed (non-blocking): {e}")
    return None


def _fetch_iv_level(ticker: str) -> Optional[float]:
    """
    Fetch a simple IV proxy from orca_iv_history.db (latest ATM IV reading).
    Returns None if DB doesn't exist or no data.
    """
    try:
        import sqlite3
        from orca_v20.config import PATHS
        iv_db = PATHS.iv_history_db
        if not os.path.exists(iv_db):
            return None
        conn = sqlite3.connect(iv_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT atm_iv FROM iv_readings WHERE ticker = ? ORDER BY date DESC LIMIT 1",
            (ticker.upper(),)
        ).fetchone()
        conn.close()
        if row and row["atm_iv"]:
            return round(float(row["atm_iv"]), 4)
    except Exception as e:
        logger.debug(f"  [{ticker}] IV fetch failed (non-blocking): {e}")
    return None


def take_daily_snapshot(thesis_id: str, idea: IdeaCandidate, ctx: RunContext) -> None:
    """Record a daily snapshot for momentum tracking."""
    if not FLAGS.enable_thesis_persistence or ctx.dry_run:
        return

    # A4: Fetch real price and IV (graceful failure)
    price = _fetch_underlying_price(idea.ticker)
    iv = _fetch_iv_level(idea.ticker)

    try:
        conn = get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO thesis_daily_snapshots (
                thesis_id, snapshot_date, run_id,
                confidence, underlying_price, iv_level, catalyst_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            thesis_id,
            ctx.market_date,
            ctx.run_id,
            idea.confidence,
            price,
            iv,
            (idea.catalyst_status or CatalystStatus.PENDING).value,
        ))
        conn.commit()
        conn.close()
        logger.debug(
            f"  [{idea.ticker}] Snapshot for thesis {thesis_id} on {ctx.market_date} "
            f"(price={price}, iv={iv})"
        )
    except Exception as e:
        logger.error(f"Failed to take snapshot for {thesis_id}: {e}")


def close_thesis(thesis_id: str, status: ThesisStatus, reason: str = "") -> None:
    """Close a thesis (win/loss/expired/invalidated)."""
    if not FLAGS.enable_thesis_persistence:
        return

    try:
        conn = get_connection()
        conn.execute("""
            UPDATE theses
            SET status = ?, invalidated_reason = ?, last_updated_utc = ?
            WHERE thesis_id = ?
        """, (
            status.value,
            reason,
            datetime.now(timezone.utc).isoformat(),
            thesis_id,
        ))
        conn.commit()
        conn.close()
        logger.info(f"Thesis {thesis_id} closed → {status.value}: {reason}")
    except Exception as e:
        logger.error(f"Failed to close thesis {thesis_id}: {e}")


def auto_label_active_theses(ctx: RunContext) -> int:
    """
    B1: Automated thesis outcome labeling via price watcher.

    For each active thesis with a known entry snapshot:
        - Fetch current price
        - Compare against thesis direction + target/stop if available
        - Auto-close as WIN/LOSS if clear thresholds hit
        - Auto-downgrade confidence if materially invalidated

    Returns count of theses auto-labeled.
    """
    if not FLAGS.enable_thesis_persistence or ctx.dry_run:
        return 0

    labeled = 0
    conn = None
    try:
        conn = get_connection()

        from orca_v20.horizon import calendar_to_trading_days

        # Get all active theses with their most recent snapshot
        rows = conn.execute("""
            SELECT t.thesis_id, t.ticker, t.idea_direction, t.current_confidence,
                   t.invalidation_trigger, t.expected_horizon, t.created_utc,
                   s.underlying_price as entry_price, s.snapshot_date
            FROM theses t
            LEFT JOIN thesis_daily_snapshots s ON t.thesis_id = s.thesis_id
            WHERE t.status IN ('ACTIVE', 'DRAFT')
            ORDER BY s.snapshot_date ASC
        """).fetchall()

        # Group by thesis_id, keep first snapshot as entry
        thesis_entries = {}
        for r in rows:
            tid = r["thesis_id"]
            if tid not in thesis_entries:
                entry_price = r["entry_price"]
                # If no snapshot entry price, try to fetch current price as fallback
                # so the thesis doesn't become a ghost (never auto-labeled)
                if entry_price is None:
                    entry_price = _fetch_underlying_price(r["ticker"])
                    if entry_price:
                        logger.info(f"[auto-label] {tid} had no snapshot — using live price ${entry_price:.2f} as entry")
                if entry_price is not None and entry_price > 0:
                    thesis_entries[tid] = {
                        "ticker": r["ticker"],
                        "direction": r["idea_direction"],
                        "confidence": r["current_confidence"],
                        "entry_price": entry_price,
                        "invalidation": r["invalidation_trigger"] or "",
                        "expected_horizon": r["expected_horizon"] or "UNKNOWN",
                        "created_utc": r["created_utc"] or "",
                    }

        now_dt = datetime.now(timezone.utc)

        # Fix #8: Ghost thesis detection — close theses that have no valid
        # snapshot (all NULLs) after THRESHOLDS.thesis_stale_days days
        ghost_rows = conn.execute("""
            SELECT t.thesis_id, t.ticker, t.created_utc
            FROM theses t
            WHERE t.status IN ('ACTIVE', 'DRAFT')
            AND t.thesis_id NOT IN (
                SELECT DISTINCT s.thesis_id
                FROM thesis_daily_snapshots s
                WHERE s.underlying_price IS NOT NULL
            )
            AND julianday('now') - julianday(t.created_utc) > ?
        """, (THRESHOLDS.thesis_stale_days,)).fetchall()

        for gr in ghost_rows:
            close_thesis(
                gr["thesis_id"], ThesisStatus.CLOSED_EXPIRED,
                f"Ghost thesis: no valid price snapshot after {THRESHOLDS.thesis_stale_days} days"
            )
            labeled += 1
            logger.info(
                f"[auto-label] Ghost thesis {gr['thesis_id']} ({gr['ticker']}) closed — "
                f"no valid snapshot after {THRESHOLDS.thesis_stale_days} days"
            )

        for tid, info in thesis_entries.items():
            current_price = _fetch_underlying_price(info["ticker"])
            if current_price is None or current_price <= 0:
                continue

            entry = info["entry_price"]
            if entry <= 0:
                continue

            pct_move = (current_price - entry) / entry

            direction = info["direction"]
            directional_move = pct_move if direction == "BULLISH" else -pct_move

            # ── Horizon-aware logic ──
            expected_horizon = info["expected_horizon"]
            horizon_days = THRESHOLDS.horizon_days_map.get(expected_horizon, 10)

            # Compute thesis age in trading days
            trading_age = 0
            if info["created_utc"]:
                try:
                    created = datetime.fromisoformat(info["created_utc"].replace("Z", "+00:00"))
                    cal_days = (now_dt - created).days
                    trading_age = calendar_to_trading_days(cal_days)
                except Exception:
                    pass

            # Grace period: don't auto-label if too early (except UNKNOWN)
            grace = int(horizon_days * THRESHOLDS.horizon_grace_multiplier)
            if expected_horizon != "UNKNOWN" and trading_age < grace:
                logger.debug(
                    f"  [{info['ticker']}] Thesis {tid} too early to judge "
                    f"(age={trading_age}td < grace={grace}td, horizon={expected_horizon})"
                )
                continue

            # Per-horizon thresholds
            h_thresholds = THRESHOLDS.horizon_auto_thresholds.get(
                expected_horizon,
                THRESHOLDS.horizon_auto_thresholds.get("UNKNOWN", (0.15, -0.10)),
            )
            win_pct, loss_pct = h_thresholds

            # Auto-close WIN: per-horizon % move in thesis direction
            if directional_move >= win_pct:
                close_thesis(tid, ThesisStatus.CLOSED_WIN,
                            f"Auto-labeled: {directional_move*100:.1f}% move in thesis direction "
                            f"(entry={entry}, current={current_price}, horizon={expected_horizon})")
                labeled += 1
                continue

            # Auto-close LOSS: per-horizon % move against thesis direction
            if directional_move <= loss_pct:
                close_thesis(tid, ThesisStatus.CLOSED_LOSS,
                            f"Auto-labeled: {directional_move*100:.1f}% move against thesis "
                            f"(entry={entry}, current={current_price}, horizon={expected_horizon})")
                labeled += 1
                continue

            # Horizon expiry: auto-expire if thesis exceeded horizon * expiry_multiplier
            expiry_limit = int(horizon_days * THRESHOLDS.horizon_expiry_multiplier)
            if trading_age > expiry_limit:
                close_thesis(tid, ThesisStatus.CLOSED_EXPIRED,
                            f"Horizon expired: trading_age={trading_age} > {expiry_limit} "
                            f"(horizon={expected_horizon}, {horizon_days}td × {THRESHOLDS.horizon_expiry_multiplier})")
                labeled += 1
                continue

            # Confidence downgrade: global threshold (unchanged)
            if directional_move <= THRESHOLDS.thesis_auto_downgrade_pct and info["confidence"] > 3:
                new_conf = max(1, info["confidence"] - 2)
                conn.execute("""
                    UPDATE theses SET current_confidence = ?, last_updated_utc = ?
                    WHERE thesis_id = ?
                """, (new_conf, datetime.now(timezone.utc).isoformat(), tid))
                conn.commit()
                logger.info(
                    f"  [{info['ticker']}] Thesis {tid} confidence downgraded "
                    f"{info['confidence']} → {new_conf} "
                    f"(move={directional_move*100:.1f}%)"
                )

        if labeled > 0:
            logger.info(f"[auto_label] Auto-labeled {labeled} thesis outcomes")
        return labeled

    except Exception as e:
        logger.error(f"Failed to auto-label theses: {e}")
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def finalize_outcomes(ctx: RunContext) -> int:
    """
    Check for closed trades and finalize their thesis outcomes.
    This is the prerequisite for replay_engine — theses must be
    closed with outcomes before they can be replayed.

    Returns count of theses finalized.
    """
    if not FLAGS.enable_thesis_persistence:
        return 0

    conn = None
    try:
        conn = get_connection()

        # FIX: Read ALL data first, close connection, THEN call close_thesis.
        # The old code held conn open while calling close_thesis() which opens
        # its own connection — risking SQLITE_BUSY with overlapping connections.

        # Find trades that have been closed but thesis is still ACTIVE
        rows = conn.execute("""
            SELECT DISTINCT e.thesis_id, e.status as trade_status
            FROM etp_records e
            JOIN theses t ON e.thesis_id = t.thesis_id
            WHERE e.status IN ('CLOSED_WIN', 'CLOSED_LOSS', 'CLOSED_EXPIRED')
            AND t.status IN ('DRAFT', 'ACTIVE')
        """).fetchall()
        trade_closures = [(r["thesis_id"], r["trade_status"]) for r in rows]

        # Also find stale theses
        stale_rows = conn.execute("""
            SELECT thesis_id FROM theses
            WHERE status IN ('DRAFT', 'ACTIVE')
            AND julianday('now') - julianday(last_updated_utc) > ?
        """, (THRESHOLDS.thesis_stale_days,)).fetchall()
        stale_ids = [r["thesis_id"] for r in stale_rows]

        conn.close()
        conn = None  # mark closed

        # Now process closures without holding a connection
        finalized = 0
        for thesis_id, trade_status in trade_closures:
            if trade_status == "CLOSED_WIN":
                thesis_status = ThesisStatus.CLOSED_WIN
                reason = "Trade closed at profit target"
            elif trade_status == "CLOSED_LOSS":
                thesis_status = ThesisStatus.CLOSED_LOSS
                reason = "Trade closed at stop loss"
            else:
                thesis_status = ThesisStatus.CLOSED_EXPIRED
                reason = "Trade expired"

            close_thesis(thesis_id, thesis_status, reason)
            finalized += 1

        for thesis_id in stale_ids:
            close_thesis(thesis_id, ThesisStatus.CLOSED_EXPIRED,
                        f"Stale after {THRESHOLDS.thesis_stale_days} days inactive")
            finalized += 1

        if finalized > 0:
            logger.info(f"[thesis_store] Finalized {finalized} thesis outcomes")
        return finalized

    except Exception as e:
        logger.error(f"Failed to finalize thesis outcomes: {e}")
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_confidence_trajectory(lookback_days: int = 14) -> List[Dict]:
    """
    B3: Build confidence trajectory summary for all active theses.

    Returns list of dicts with:
        thesis_id, ticker, trajectory ("RISING", "FALLING", "STABLE"),
        latest_confidence, confidence_delta, latest_price
    """
    if not FLAGS.enable_thesis_persistence:
        return []

    try:
        conn = get_connection()
        theses = conn.execute("""
            SELECT thesis_id, ticker, current_confidence
            FROM theses WHERE status IN ('ACTIVE', 'DRAFT')
        """).fetchall()

        results = []
        for t in theses:
            tid = t["thesis_id"]
            snapshots = conn.execute("""
                SELECT confidence, underlying_price, snapshot_date
                FROM thesis_daily_snapshots
                WHERE thesis_id = ?
                AND julianday('now') - julianday(snapshot_date) <= ?
                ORDER BY snapshot_date ASC
            """, (tid, lookback_days)).fetchall()

            if len(snapshots) < 2:
                trajectory = "STABLE"
                delta = 0
            else:
                first_conf = snapshots[0]["confidence"]
                last_conf = snapshots[-1]["confidence"]
                delta = last_conf - first_conf
                if delta >= 2:
                    trajectory = "RISING"
                elif delta <= -2:
                    trajectory = "FALLING"
                else:
                    trajectory = "STABLE"

            latest_price = None
            if snapshots:
                latest_price = snapshots[-1]["underlying_price"]

            results.append({
                "thesis_id": tid,
                "ticker": t["ticker"],
                "trajectory": trajectory,
                "latest_confidence": t["current_confidence"],
                "confidence_delta": delta,
                "latest_price": latest_price,
                "snapshot_count": len(snapshots),
            })

        conn.close()
        return results

    except Exception as e:
        logger.error(f"Failed to compute confidence trajectories: {e}")
        return []


def compute_forward_outcomes(ctx: RunContext) -> int:
    """
    Compute multi-window forward outcomes for all active/recently-closed theses.

    For each thesis with stored daily snapshots:
        - Reads thesis_daily_snapshots (stored prices first)
        - Falls back to yfinance ONLY if snapshot coverage is insufficient
        - Computes directional return, MFE, MAE for each window [1, 3, 5, 10, 20]
        - Assigns HorizonOutcomeLabel and TimingQuality
        - Writes/upserts thesis_forward_outcomes table

    Returns count of forward outcome records written.
    """
    from orca_v20.horizon import (
        calendar_to_trading_days, compute_timing_quality, is_catalyst_intact,
    )
    from orca_v20.schemas import HorizonOutcomeLabel

    if not FLAGS.enable_thesis_persistence or ctx.dry_run:
        return 0

    written = 0
    eval_date = ctx.market_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        conn = get_connection()

        # Get all active + recently-closed theses
        theses_rows = conn.execute("""
            SELECT thesis_id, ticker, idea_direction, status, created_utc,
                   expected_horizon, invalidation_trigger
            FROM theses
            WHERE status IN ('ACTIVE', 'DRAFT', 'CLOSED_WIN', 'CLOSED_LOSS',
                           'CLOSED_EXPIRED', 'CLOSED_INVALIDATED')
        """).fetchall()

        now_dt = datetime.now(timezone.utc)

        for t in theses_rows:
            tid = t["thesis_id"]
            direction = t["idea_direction"]
            expected_horizon = t["expected_horizon"] or "UNKNOWN"
            horizon_days = THRESHOLDS.horizon_days_map.get(expected_horizon, 10)
            thesis_status = t["status"] or ""
            catalyst_status = ""  # could be enriched from snapshots if needed
            invalidation = t["invalidation_trigger"] or ""

            # Compute thesis age in trading days
            trading_age = 0
            if t["created_utc"]:
                try:
                    created = datetime.fromisoformat(
                        t["created_utc"].replace("Z", "+00:00")
                    )
                    cal_days = (now_dt - created).days
                    trading_age = calendar_to_trading_days(cal_days)
                except Exception:
                    pass

            # Get stored daily snapshots (prices)
            snapshots = conn.execute("""
                SELECT snapshot_date, underlying_price, catalyst_status
                FROM thesis_daily_snapshots
                WHERE thesis_id = ? AND underlying_price IS NOT NULL
                ORDER BY snapshot_date ASC
            """, (tid,)).fetchall()

            # Deduplicate snapshots by date (keep last per date, since budget
            # mode may produce multiple snapshots per day from different sprints)
            seen_dates = {}
            for s in snapshots:
                seen_dates[s["snapshot_date"]] = s
            snapshots = list(seen_dates.values())
            snapshots.sort(key=lambda s: s["snapshot_date"])

            if not snapshots or len(snapshots) < 2:
                continue

            entry_price = snapshots[0]["underlying_price"]
            if not entry_price or entry_price <= 0:
                continue

            # Get latest catalyst_status from snapshots
            last_snap = snapshots[-1]
            if last_snap["catalyst_status"]:
                catalyst_status = last_snap["catalyst_status"]

            prices = [s["underlying_price"] for s in snapshots if s["underlying_price"]]

            # If snapshots are sparse, try to fill with yfinance history
            max_window = max(THRESHOLDS.forward_outcome_windows)
            if len(prices) < max_window + 1 and trading_age >= 2:
                try:
                    import yfinance as yf
                    ticker_obj = yf.Ticker(t["ticker"])
                    hist = ticker_obj.history(period=f"{max_window + 5}d")
                    if len(hist) >= 2:
                        yf_prices = hist["Close"].tolist()
                        # Fix 15: filter to only dates >= thesis creation date
                        thesis_created_date = (t["created_utc"] or "")[:10]
                        if thesis_created_date and hasattr(hist.index, '__iter__'):
                            try:
                                yf_dates = [d.strftime("%Y-%m-%d") for d in hist.index]
                                yf_prices = [
                                    p for d, p in zip(yf_dates, yf_prices)
                                    if d >= thesis_created_date
                                ]
                            except Exception:
                                pass  # fallback: use all prices
                        if len(yf_prices) > len(prices):
                            prices = yf_prices
                            # FIX: Do NOT overwrite entry_price with yfinance's first price.
                            # The DB snapshot entry_price is the true price at thesis creation;
                            # yfinance history may start earlier (e.g. 25 days ago).
                            # entry_price is kept from snapshots[0]["underlying_price"] above.
                            logger.debug(
                                f"[forward] {tid} extended with yfinance: "
                                f"{len(prices)} prices (was {len(snapshots)} snapshots)"
                            )
                except Exception as e:
                    logger.debug(f"[forward] {tid} yfinance fallback failed: {e}")

            for window in THRESHOLDS.forward_outcome_windows:
                # Skip if insufficient data for this window
                if len(prices) < window + 1:
                    continue

                # Get prices up to window
                window_prices = prices[:window + 1]
                end_price = window_prices[-1]

                # Directional return
                raw_return = (end_price - entry_price) / entry_price
                directional_return = raw_return if direction == "BULLISH" else -raw_return

                # MFE / MAE (directional)
                if direction == "BULLISH":
                    mfe = (max(window_prices) - entry_price) / entry_price
                    mae = (min(window_prices) - entry_price) / entry_price
                else:
                    mfe = (entry_price - min(window_prices)) / entry_price
                    mae = (entry_price - max(window_prices)) / entry_price

                # Timing quality
                tq = compute_timing_quality(
                    directional_return, mfe, trading_age,
                    horizon_days, thesis_status, expected_horizon,
                )

                # Catalyst intact
                cat_intact = is_catalyst_intact(thesis_status, catalyst_status)

                # Horizon outcome label
                grace = int(horizon_days * THRESHOLDS.horizon_grace_multiplier)
                h_thresholds = THRESHOLDS.horizon_auto_thresholds.get(
                    expected_horizon, (0.15, -0.10),
                )
                win_pct, loss_pct = h_thresholds

                if thesis_status == "CLOSED_INVALIDATED":
                    outcome_label = HorizonOutcomeLabel.INVALIDATED.value
                elif trading_age < grace:
                    outcome_label = HorizonOutcomeLabel.TOO_EARLY.value
                elif directional_return >= win_pct:
                    outcome_label = HorizonOutcomeLabel.WORKED.value
                elif directional_return <= loss_pct:
                    outcome_label = HorizonOutcomeLabel.FAILED.value
                elif trading_age > horizon_days and cat_intact:
                    outcome_label = HorizonOutcomeLabel.LATE_BUT_INTACT.value
                elif trading_age <= horizon_days:
                    outcome_label = HorizonOutcomeLabel.ON_TRACK.value
                else:
                    outcome_label = HorizonOutcomeLabel.TOO_EARLY.value

                # Write/upsert
                conn.execute("""
                    INSERT OR REPLACE INTO thesis_forward_outcomes (
                        thesis_id, eval_date, window_days,
                        forward_return_pct, mfe_pct, mae_pct,
                        thesis_age_days, expected_horizon,
                        horizon_outcome_label, timing_quality,
                        catalyst_intact, created_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tid, eval_date, window,
                    round(directional_return, 6),
                    round(mfe, 6),
                    round(mae, 6),
                    trading_age, expected_horizon,
                    outcome_label, tq,
                    1 if cat_intact else 0,
                    datetime.now(timezone.utc).isoformat(),
                ))
                written += 1

        conn.commit()
        conn.close()

        if written > 0:
            logger.info(f"[forward_outcomes] Computed {written} forward outcome records")
        return written

    except Exception as e:
        logger.error(f"Failed to compute forward outcomes: {e}")
        return 0


def get_horizon_outcomes(thesis_id: str) -> List[Dict]:
    """
    Get all forward outcome records for a thesis, ordered by window_days.

    Returns list of dicts with all thesis_forward_outcomes columns.
    """
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT thesis_id, eval_date, window_days,
                   forward_return_pct, mfe_pct, mae_pct,
                   thesis_age_days, expected_horizon,
                   horizon_outcome_label, timing_quality,
                   catalyst_intact, created_utc
            FROM thesis_forward_outcomes
            WHERE thesis_id = ?
            ORDER BY eval_date DESC, window_days ASC
        """, (thesis_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to get horizon outcomes for {thesis_id}: {e}")
        return []


def run_thesis_matching(ideas: List[IdeaCandidate], ctx: RunContext) -> List[IdeaCandidate]:
    """
    Match all ideas against existing theses, persist new ones, take snapshots.
    """
    logger.info(f"[run_id={ctx.run_id}] Thesis matching for {len(ideas)} ideas")

    matched = []
    for idea in ideas:
        idea = match_to_existing(idea, ctx)
        persist_thesis(idea, ctx)
        take_daily_snapshot(idea.thesis_id, idea, ctx)
        matched.append(idea)

    new_count = sum(1 for i in matched if i.matched_existing_thesis_id is None)
    reuse_count = sum(1 for i in matched if i.matched_existing_thesis_id is not None)
    logger.info(f"[thesis_store] {new_count} new theses, {reuse_count} matched existing")
    ctx.mark_stage("thesis_matching")
    return matched
