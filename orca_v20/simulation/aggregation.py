"""
ORCA v20 — Simulation Aggregation.

Combines elite agent votes + crowd sentiment into a final verdict.

Verdict logic:
    PROCEED  — consensus is supportive, dissent < 40%
    DOWNSIZE — mixed signals, 40-60% dissent or weak confidence
    REJECT   — majority against, dissent > 60% or adversarial kill
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List

from orca_v20.config import FLAGS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import EliteAgentVote, IdeaCandidate, SimulationVerdict

logger = logging.getLogger("orca_v20.simulation.aggregation")

# Vote to numeric
_VOTE_NUMERIC = {
    "STRONG_BUY": 2.0,
    "BUY": 1.0,
    "HOLD": 0.0,
    "SELL": -1.0,
    "STRONG_SELL": -2.0,
}

# Direction alignment: what votes support the thesis?
_SUPPORTIVE_VOTES = {"STRONG_BUY", "BUY"}
_AGAINST_VOTES = {"SELL", "STRONG_SELL"}


def aggregate(
    idea: IdeaCandidate,
    elite_votes: List[EliteAgentVote],
    crowd_sentiment: float,
    ctx: RunContext,
) -> SimulationVerdict:
    """
    Produce a SimulationVerdict from elite votes + crowd.
    """
    if not FLAGS.enable_elite_simulation:
        return SimulationVerdict(
            thesis_id=idea.thesis_id or "",
            final_verdict="PROCEED",
        )

    if not elite_votes:
        return SimulationVerdict(
            thesis_id=idea.thesis_id or "",
            elite_votes=[],
            crowd_sentiment=crowd_sentiment,
            final_verdict="PROCEED",
        )

    # ── Compute elite consensus ──
    total_votes = len(elite_votes)
    supportive = sum(1 for v in elite_votes if v.vote in _SUPPORTIVE_VOTES)
    against = sum(1 for v in elite_votes if v.vote in _AGAINST_VOTES)
    neutral = total_votes - supportive - against

    # Weighted numeric consensus
    weighted_sum = sum(
        _VOTE_NUMERIC.get(v.vote, 0) * v.confidence
        for v in elite_votes
    )
    total_confidence = sum(v.confidence for v in elite_votes) or 1.0
    elite_weighted_consensus = weighted_sum / total_confidence

    # Average confidence
    avg_confidence = total_confidence / total_votes

    # Majority vote label
    if supportive > against and supportive > neutral:
        consensus_label = "BUY"
    elif against > supportive and against > neutral:
        consensus_label = "SELL"
    else:
        consensus_label = "HOLD"

    # ── Mark dissent ──
    # Dissent = voting against the majority
    for v in elite_votes:
        if consensus_label in ("BUY",) and v.vote in _AGAINST_VOTES:
            v.dissent_flag = True
        elif consensus_label in ("SELL",) and v.vote in _SUPPORTIVE_VOTES:
            v.dissent_flag = True

    dissent_count = sum(1 for v in elite_votes if v.dissent_flag)
    dissent_ratio = dissent_count / total_votes if total_votes > 0 else 0.0

    # ── Combine with crowd ──
    # Elite weight = 70%, Crowd weight = 30%
    combined_signal = elite_weighted_consensus * 0.7 + crowd_sentiment * 0.3

    # ── Final verdict ──
    if dissent_ratio > 0.6 or combined_signal < -0.5:
        final_verdict = "REJECT"
    elif dissent_ratio > 0.4 or abs(combined_signal) < 0.2:
        final_verdict = "DOWNSIZE"
    else:
        final_verdict = "PROCEED"

    verdict = SimulationVerdict(
        thesis_id=idea.thesis_id or "",
        elite_votes=elite_votes,
        elite_consensus=consensus_label,
        elite_confidence=round(avg_confidence, 4),
        crowd_sentiment=crowd_sentiment,
        dissent_ratio=round(dissent_ratio, 4),
        final_verdict=final_verdict,
    )

    # Persist to DB
    _persist_verdict(verdict, ctx)

    logger.info(
        f"  [{idea.ticker}] Simulation verdict: {final_verdict} "
        f"(elite={consensus_label}, crowd={crowd_sentiment:.3f}, "
        f"dissent={dissent_ratio:.2f})"
    )

    return verdict


def _persist_verdict(verdict: SimulationVerdict, ctx: RunContext) -> None:
    """Write aggregated verdict to DB."""
    if ctx.dry_run:
        return
    try:
        conn = get_connection()
        conn.execute("""
            INSERT INTO crowd_snapshots (
                run_id, thesis_id,
                elite_consensus, elite_confidence,
                crowd_sentiment, dissent_ratio, final_verdict,
                created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ctx.run_id,
            verdict.thesis_id,
            verdict.elite_consensus,
            verdict.elite_confidence,
            verdict.crowd_sentiment,
            verdict.dissent_ratio,
            verdict.final_verdict,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist simulation verdict: {e}")
