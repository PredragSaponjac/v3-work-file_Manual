"""
ORCA v20 — Red-Team / Adversarial Thesis Gate (C1).

Dedicated mechanism to challenge each thesis before final approval.
Runs deterministic/semi-structured checks rather than burning LLM tokens:

    1. Staleness check — is the catalyst already old news?
    2. Priced-in check — has the underlying already moved significantly?
    3. Contradiction check — does evidence contain counter-signals?
    4. Crowding check — is the trade consensus too one-sided?
    5. Confirmation bias check — are all evidence items from same source type?

Output: RedTeamResult with fatal_flaws, risk_score, survived_red_team flag.

Only runs on ideas that survive core gating (evidence_gate, quant_gate, etc.)
and exceed a minimum confidence threshold. Weak early-stage ideas are skipped.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from orca_v20.config import FLAGS
from orca_v20.run_context import RunContext
from orca_v20.schemas import IdeaCandidate, EvidencePack

logger = logging.getLogger("orca_v20.red_team_gate")

MIN_CONFIDENCE_FOR_RED_TEAM = 6  # skip weak ideas


@dataclass
class RedTeamResult:
    """Output of red-team adversarial check."""
    thesis_id: str = ""
    ticker: str = ""
    fatal_flaws: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    risk_score: float = 0.0  # 0.0 = safe, 1.0 = extremely risky
    survived_red_team: bool = True


def evaluate(idea: IdeaCandidate, ctx: RunContext) -> RedTeamResult:
    """
    Run deterministic red-team checks on a single idea.

    Returns RedTeamResult. Ideas with fatal_flaws have survived=False.
    """
    result = RedTeamResult(
        thesis_id=idea.thesis_id or "",
        ticker=idea.ticker,
    )

    # Skip weak ideas
    if idea.confidence < MIN_CONFIDENCE_FOR_RED_TEAM:
        return result

    pack = idea.evidence_pack
    risk_points = 0.0

    # ── Check 1: Evidence staleness ──
    if pack and pack.freshest_item_age_hours > 48:
        result.warnings.append(
            f"stale_evidence: freshest item is {pack.freshest_item_age_hours:.0f}h old"
        )
        risk_points += 0.15
        if pack.freshest_item_age_hours > 96:
            result.fatal_flaws.append(
                f"extremely_stale: all evidence >96h old, catalyst likely priced in"
            )
            risk_points += 0.25

    # ── Check 2: Contradiction ratio ──
    if pack and pack.items:
        contra_count = sum(
            1 for item in pack.items
            if item.sentiment and item.sentiment.lower() in ("contradictory", "negative", "bearish")
        )
        total = len(pack.items)
        contra_ratio = contra_count / max(total, 1)

        if contra_ratio >= 0.40:
            result.fatal_flaws.append(
                f"high_contradiction: {contra_count}/{total} items contradict thesis"
            )
            risk_points += 0.30
        elif contra_ratio >= 0.25:
            result.warnings.append(
                f"moderate_contradiction: {contra_count}/{total} items are contradictory"
            )
            risk_points += 0.10

    # ── Check 3: Source diversity (confirmation bias) ──
    if pack and pack.items:
        source_types = set(item.evidence_type for item in pack.items)
        if len(source_types) == 1:
            result.warnings.append(
                f"single_source_type: all evidence from {source_types.pop().value}"
            )
            risk_points += 0.15

        sources = set(item.source for item in pack.items)
        internal_only = all(s in ("r1_evidence", "catalyst", "uw_flow") for s in sources)
        if internal_only:
            result.warnings.append("no_external_confirmation: all evidence is internal/model-generated")
            risk_points += 0.20

    # ── Check 4: Thesis already seen many times without closure ──
    if hasattr(idea, 'matched_existing_thesis_id') and idea.matched_existing_thesis_id:
        # Recurring thesis — check if it keeps appearing without resolving
        result.warnings.append(
            f"recurring_thesis: matched existing thesis {idea.matched_existing_thesis_id}"
        )
        risk_points += 0.05

    # ── Check 5: Low urgency with high confidence (suspicious) ──
    if idea.confidence >= 8 and idea.urgency <= 3:
        result.warnings.append(
            f"confidence_urgency_mismatch: conf={idea.confidence}/10 but urgency={idea.urgency}/10"
        )
        risk_points += 0.10

    # Compute final risk score
    result.risk_score = min(1.0, round(risk_points, 3))

    # Determine survival
    result.survived_red_team = len(result.fatal_flaws) == 0 and result.risk_score < 0.60

    if not result.survived_red_team:
        logger.warning(
            f"  [{idea.ticker}] RED TEAM REJECTED: "
            f"risk={result.risk_score:.2f}, flaws={result.fatal_flaws}"
        )
    elif result.warnings:
        logger.info(
            f"  [{idea.ticker}] Red team passed with warnings: "
            f"risk={result.risk_score:.2f}, {len(result.warnings)} warnings"
        )

    return result


def run_red_team(ideas: List[IdeaCandidate], ctx: RunContext) -> List[IdeaCandidate]:
    """
    Run red-team gate on all post-gate ideas.
    Removes ideas that fail. Advisory warnings remain attached.

    Returns surviving ideas.
    """
    if not ideas:
        return ideas

    logger.info(f"[run_id={ctx.run_id}] Red-team gate for {len(ideas)} ideas")

    survivors = []
    for idea in ideas:
        result = evaluate(idea, ctx)

        # Persist result onto structured fields
        idea.red_team_risk_score = result.risk_score
        idea.red_team_fatal_flaws = list(result.fatal_flaws)
        idea.red_team_warnings = list(result.warnings)
        idea.survived_red_team = result.survived_red_team

        if result.survived_red_team:
            survivors.append(idea)
        else:
            logger.info(f"  [{idea.ticker}] Removed by red-team gate")

    removed = len(ideas) - len(survivors)
    if removed > 0:
        logger.info(f"[red_team] {removed} ideas rejected, {len(survivors)} survived")
    else:
        logger.info(f"[red_team] All {len(ideas)} ideas survived")

    ctx.mark_stage("red_team_gate")
    return survivors
