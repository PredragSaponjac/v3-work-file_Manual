"""
ORCA v20 — Memory Store + Analog Retrieval.

SQLite-backed case memory with TF-IDF embeddings for analog retrieval.
Stores thesis outcomes, lessons, and key patterns for future reference.
Uses same lightweight TF-IDF approach as thesis_store (no external deps).
"""

import json
import logging
import math
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional

from orca_v20.config import FLAGS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.memory_store")


# ─────────────────────────────────────────────────────────────────────
# TF-IDF (shared approach with thesis_store)
# ─────────────────────────────────────────────────────────────────────

_STOP_WORDS = frozenset(
    "a an the is are was were be been being have has had do does did "
    "will would shall should may might can could and or but if then else "
    "for of to in on at by with from as into this that these those it "
    "its not no nor so yet also very too much more most than such".split()
)


def _tokenize(text: str) -> List[str]:
    words = re.findall(r"[a-z]+", text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) > 2]


def _build_tf_vector(tokens: List[str]) -> Dict[str, float]:
    counts = Counter(tokens)
    total = len(tokens) or 1
    return {word: count / total for word, count in counts.items()}


def _cosine_sim(a: Dict[str, float], b: Dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    common = set(a.keys()) & set(b.keys())
    dot = sum(a[k] * b[k] for k in common)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


# ─────────────────────────────────────────────────────────────────────
# Core functions
# ─────────────────────────────────────────────────────────────────────

def store_case(
    thesis_id: str,
    ticker: str,
    catalyst_type: str,
    setup_summary: str,
    outcome: str,
    pnl_pct: float,
    key_lesson: str,
    ctx: RunContext,
) -> None:
    """
    Store a completed thesis as a memory case with TF-IDF embedding.
    """
    if not FLAGS.enable_memory_retrieval:
        return
    if ctx.dry_run:
        return

    try:
        # Compute embedding for similarity search later
        tokens = _tokenize(f"{ticker} {catalyst_type} {setup_summary}")
        tf_vec = _build_tf_vector(tokens)
        embedding_json = json.dumps(tf_vec)

        conn = get_connection()
        conn.execute("""
            INSERT INTO memory_cases (
                thesis_id, ticker, catalyst_type,
                setup_summary, outcome, pnl_pct,
                key_lesson, embedding_json, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            thesis_id, ticker, catalyst_type,
            setup_summary, outcome, pnl_pct,
            key_lesson, embedding_json,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
        logger.info(f"  [{ticker}] Memory case stored for thesis {thesis_id}")
    except Exception as e:
        logger.error(f"Failed to store memory case for {ticker}: {e}")


def retrieve_analogs(
    ticker: str,
    catalyst_type: str,
    setup_summary: str,
    top_k: int = 5,
) -> List[Dict]:
    """
    Retrieve similar past cases for analog comparison.

    Returns list of dicts sorted by similarity:
        thesis_id, ticker, catalyst_type, setup_summary,
        outcome, pnl_pct, key_lesson, similarity
    """
    if not FLAGS.enable_memory_retrieval:
        return []

    try:
        # Build query vector
        query_tokens = _tokenize(f"{ticker} {catalyst_type} {setup_summary}")
        query_vec = _build_tf_vector(query_tokens)

        if not query_vec:
            return []

        conn = get_connection()
        rows = conn.execute("""
            SELECT thesis_id, ticker, catalyst_type,
                   setup_summary, outcome, pnl_pct,
                   key_lesson, embedding_json
            FROM memory_cases
        """).fetchall()
        conn.close()

        if not rows:
            return []

        # Score all cases by similarity
        scored = []
        for row in rows:
            try:
                stored_vec = json.loads(row["embedding_json"] or "{}")
            except (json.JSONDecodeError, TypeError):
                continue

            sim = _cosine_sim(query_vec, stored_vec)
            scored.append({
                "thesis_id": row["thesis_id"],
                "ticker": row["ticker"],
                "catalyst_type": row["catalyst_type"],
                "setup_summary": row["setup_summary"],
                "outcome": row["outcome"],
                "pnl_pct": row["pnl_pct"],
                "key_lesson": row["key_lesson"],
                "similarity": round(sim, 4),
            })

        # Sort by similarity descending, return top_k
        scored.sort(key=lambda x: x["similarity"], reverse=True)
        results = scored[:top_k]

        if results:
            logger.debug(
                f"  [{ticker}] Retrieved {len(results)} analogs "
                f"(best sim={results[0]['similarity']:.3f})"
            )

        return results

    except Exception as e:
        logger.error(f"Failed to retrieve analogs for {ticker}: {e}")
        return []


def get_analog_stats(ticker: str, catalyst_type: str, setup_summary: str) -> Dict:
    """
    Get aggregate stats from analogs (win_rate, avg_pnl, count).
    Used by quant_gate for evidence checks.
    """
    analogs = retrieve_analogs(ticker, catalyst_type, setup_summary, top_k=20)

    if not analogs:
        return {"count": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0}

    wins = sum(1 for a in analogs if (a.get("pnl_pct") or 0) > 0)
    total_pnl = sum(a.get("pnl_pct", 0) for a in analogs)

    return {
        "count": len(analogs),
        "win_rate": wins / len(analogs) if analogs else 0.0,
        "avg_pnl_pct": total_pnl / len(analogs) if analogs else 0.0,
    }
