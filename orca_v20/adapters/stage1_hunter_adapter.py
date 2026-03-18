"""
Adapter: Stage 1 — Catalyst Hunter.

Wraps catalyst_hunter.run_multi_model_hunt() and normalizes
the output list of v3 idea dicts into v20 IdeaCandidate objects.

CRITICAL: idea_direction is set here and NEVER overwritten downstream.
"""

import logging
import sys
import os
import uuid
from typing import Dict, List, Optional, Tuple

# Ensure project root is on sys.path so v3 modules are importable
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.run_context import RunContext
from orca_v20.horizon import parse_horizon_from_window
from orca_v20.schemas import (
    CatalystStatus,
    ConsensusTag,
    IdeaCandidate,
    IdeaDirection,
    ThesisHorizon,
)

logger = logging.getLogger("orca_v20.adapters.stage1")


def _parse_direction(raw: str) -> IdeaDirection:
    """Normalize v3 direction string to v20 enum."""
    raw_upper = (raw or "").strip().upper()
    if raw_upper in ("BULLISH", "BULL", "LONG", "BUY"):
        return IdeaDirection.BULLISH
    elif raw_upper in ("BEARISH", "BEAR", "SHORT", "SELL"):
        return IdeaDirection.BEARISH
    elif raw_upper in ("NEUTRAL", "RANGE", "VOL"):
        return IdeaDirection.NEUTRAL
    else:
        logger.warning(f"Unknown direction '{raw}', defaulting to BULLISH")
        return IdeaDirection.BULLISH


def _parse_consensus_tag(raw: str) -> ConsensusTag:
    """Normalize v3 consensus tag string to v20 enum.

    v3 format: "3/3 UNANIMOUS", "2/3 (Claude+GPT)", "1/3 (Gemini only)"
    v20 format: ConsensusTag enum
    """
    raw_upper = (raw or "").strip().upper()
    if "UNANIMOUS" in raw_upper or "3/3" in raw_upper:
        return ConsensusTag.UNANIMOUS
    elif "2/3" in raw_upper:
        return ConsensusTag.MAJORITY
    elif "SPLIT" in raw_upper:
        return ConsensusTag.SPLIT
    else:
        return ConsensusTag.SINGLE


def _safe_int(val, default: int = 0) -> int:
    """Safely parse an integer from various types."""
    if val is None:
        return default
    try:
        # Handle "8/10", "8", 8, 8.0
        s = str(val).strip()
        if "/" in s:
            s = s.split("/")[0]
        return int(float(s))
    except (ValueError, TypeError):
        return default


def _parse_confidence(raw) -> int:
    """
    Normalize confidence from v3 into 1-10 integer.

    v3 R1 v4.8 returns qualitative labels like:
      "High — catalyst confirmed, strong conviction..."
      "Medium — reasonable thesis but uncertain timing..."
      "Low — speculative, limited evidence..."

    Also handles numeric formats: "8/10", "8", 8, 8.0
    """
    if raw is None:
        return 5  # default to mid-range, not 0

    s = str(raw).strip()

    # Try numeric first: "8/10", "8", "8.5"
    numeric = _safe_int(raw, default=-1)
    if numeric >= 1:
        return min(numeric, 10)

    # Try regex patterns: "7 out of 10", "Confidence: 8", "8/10"
    import re
    m = re.search(r'(\d+)\s*(?:out of|/)\s*10', s)
    if m:
        return min(int(m.group(1)), 10)
    m = re.search(r'[Cc]onfidence[:\s]+(\d+)', s)
    if m:
        return min(int(m.group(1)), 10)

    # Qualitative mapping (case-insensitive, check start of string)
    s_upper = s.upper()

    # Check for qualitative keywords at start or as standalone.
    # ORDER MATTERS: most-specific patterns first (e.g. "MEDIUM-HIGH" before "MEDIUM").
    if s_upper.startswith("VERY HIGH") or s_upper.startswith("EXTREMELY HIGH"):
        return 10
    elif s_upper.startswith("MEDIUM-HIGH") or s_upper.startswith("MEDIUM HIGH"):
        return 7
    elif s_upper.startswith("MEDIUM-LOW") or s_upper.startswith("MEDIUM LOW"):
        return 5
    elif s_upper.startswith("LOW-MEDIUM") or s_upper.startswith("LOW MEDIUM"):
        return 5
    elif s_upper.startswith("VERY LOW"):
        return 3
    elif s_upper.startswith("HIGH"):
        return 8
    elif s_upper.startswith("MEDIUM") or s_upper.startswith("MODERATE"):
        return 6
    elif s_upper.startswith("LOW"):
        return 4

    # Fallback: search anywhere in the string for keywords
    if "HIGH" in s_upper:
        return 8
    if "MEDIUM" in s_upper or "MODERATE" in s_upper:
        return 6
    if "LOW" in s_upper:
        return 4

    # Nothing matched — default to 5 (neutral, not 0)
    logger.warning(f"[confidence] Could not parse '{s[:60]}' — defaulting to 5")
    return 5


def normalize_v3_idea(v3_dict: Dict, ctx: RunContext) -> IdeaCandidate:
    """
    Convert a single v3 idea dict into a v20 IdeaCandidate.

    The v3 dict has keys from parse_catalyst_ideas() + merge_multi_model_ideas():
      ticker, company, direction, catalyst, catalyst_status, confidence,
      thesis, evidence, repricing_window, invalidation, crowding_risk,
      second_order, model_sources, model_consensus, consensus_tag, etc.
    """
    # Parse evidence: v3 has it as list of strings OR a single string
    raw_evidence = v3_dict.get("evidence", [])
    if isinstance(raw_evidence, str):
        raw_evidence = [raw_evidence] if raw_evidence else []
    elif not isinstance(raw_evidence, list):
        raw_evidence = []

    idea = IdeaCandidate(
        idea_id=uuid.uuid4().hex[:10],
        run_id=ctx.run_id,
        ticker=(v3_dict.get("ticker") or "").upper().strip(),
        company=v3_dict.get("company", ""),

        # CRITICAL: set idea_direction from thesis, not from strategy
        idea_direction=_parse_direction(v3_dict.get("direction", "")),

        catalyst=v3_dict.get("catalyst", ""),
        catalyst_status=CatalystStatus.PENDING,
        repricing_window=v3_dict.get("repricing_window", ""),
        expected_horizon=parse_horizon_from_window(v3_dict.get("repricing_window", "")),

        confidence=_parse_confidence(v3_dict.get("confidence")),
        confidence_raw=str(v3_dict.get("confidence", "") or ""),
        urgency=_safe_int(v3_dict.get("urgency"), 5),
        urgency_raw=str(v3_dict.get("urgency", "") or ""),

        thesis=v3_dict.get("thesis", ""),
        evidence=raw_evidence,
        invalidation=v3_dict.get("invalidation", ""),
        second_order=v3_dict.get("second_order", ""),
        crowding_risk=v3_dict.get("crowding_risk", ""),

        model_sources=v3_dict.get("model_sources", []),
        consensus_tag=_parse_consensus_tag(v3_dict.get("consensus_tag", "")),

        # Thesis fields — populated later by thesis_store
        thesis_id=None,
        matched_existing_thesis_id=None,
        match_confidence=0.0,

        # GPT two-stage metadata (if present)
        gpt_decision=(v3_dict.get("_gpt_decision") or "").upper().strip(),
        gpt_bull_score=float(v3_dict.get("_gpt_bull_score", 0) or 0),
        gpt_bear_score=float(v3_dict.get("_gpt_bear_score", 0) or 0),

        # Preserve raw v3 dict for passthrough to v3 stages
        _v3_raw=v3_dict,
    )

    return idea


def run_stage1(ctx: RunContext) -> List[IdeaCandidate]:
    """
    Execute Stage 1: Multi-model catalyst hunting.

    Calls v3 catalyst_hunter.run_multi_model_hunt(),
    then normalizes each idea into a v20 IdeaCandidate.
    """
    logger.info(f"[run_id={ctx.run_id}] Stage 1: Catalyst hunting")

    if ctx.dry_run:
        logger.info("[Stage 1] DRY RUN — skipping actual hunt")
        ctx.mark_stage("stage1_hunt")
        return []

    try:
        from catalyst_hunter import run_multi_model_hunt, save_results

        raw_text, v3_ideas = run_multi_model_hunt(seed="", dry_run=False)

        # Save results (same as v3 pipeline does)
        if raw_text or v3_ideas:
            save_results(raw_text or "(multi-model merge — see JSON)", v3_ideas)

        if not v3_ideas:
            logger.info("[Stage 1] No ideas returned from multi-model hunt")
            ctx.mark_stage("stage1_hunt")
            return []

        # Track GPT two-stage cost (attached to ideas by catalyst_hunter)
        gpt_cost = 0.0
        for idea_dict in v3_ideas:
            c = idea_dict.get("_gpt_cost_usd", 0)
            if c and c > gpt_cost:
                gpt_cost = c  # all ideas share same cost, take max
        if gpt_cost > 0:
            ctx.add_cost(gpt_cost)
            logger.info(f"[Stage 1] GPT two-stage cost: ${gpt_cost:.4f}")

        logger.info(f"[Stage 1] Multi-model hunt returned {len(v3_ideas)} ideas")

    except Exception as e:
        logger.error(f"[Stage 1] Catalyst hunt failed: {e}")
        ctx.add_error("stage1_hunt", str(e))
        ctx.mark_stage("stage1_hunt")
        return []

    # Normalize into v20 IdeaCandidates
    ideas = []
    for v3_dict in v3_ideas:
        try:
            idea = normalize_v3_idea(v3_dict, ctx)
            if idea.ticker:  # skip empty tickers
                ideas.append(idea)
                logger.info(
                    f"  #{len(ideas)}: {idea.ticker} ({idea.idea_direction.value}) "
                    f"— Conf: {idea.confidence} — {idea.consensus_tag.value}"
                )
        except Exception as e:
            logger.warning(f"  Failed to normalize idea: {e}")
            ctx.add_error("stage1_normalize", str(e))

    logger.info(f"[run_id={ctx.run_id}] Stage 1 complete: {len(ideas)} ideas normalized")
    ctx.mark_stage("stage1_hunt")
    return ideas
