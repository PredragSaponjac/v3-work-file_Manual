"""
Adapter: Stage 3 — Catalyst Confirmation.

Wraps catalyst_confirm.confirm_catalyst() and maps output
into v20 schema fields (catalyst_action, cds_score, catalyst_health).
Report framing logic lives in orca_v20.report_framing (shared module).
"""

import logging
import sys
import os
from typing import Dict, List, Tuple

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.report_framing import determine_report_framing
from orca_v20.run_context import RunContext
from orca_v20.schemas import CatalystAction, CatalystStatus, IdeaCandidate

logger = logging.getLogger("orca_v20.adapters.catalyst")


def _parse_action(raw: str) -> CatalystAction:
    """Normalize v3 catalyst action string."""
    raw_upper = (raw or "").strip().upper()
    if raw_upper == "KILL":
        return CatalystAction.KILL
    elif raw_upper == "CONFIRM":
        return CatalystAction.CONFIRM
    elif raw_upper == "DOWNGRADE":
        return CatalystAction.DOWNGRADE
    else:
        return CatalystAction.HOLD  # "HOLD FOR R2" -> HOLD


def _parse_catalyst_status(raw_health: str) -> CatalystStatus:
    """Map v3 catalyst_health string to v20 CatalystStatus."""
    h = (raw_health or "").upper()
    if "STRENGTHENING" in h or "STRONG" in h or "ACTIVE" in h:
        return CatalystStatus.CONFIRMED
    elif "STABLE" in h:
        return CatalystStatus.DEVELOPING
    elif "FADING" in h:
        return CatalystStatus.DEVELOPING
    elif "COLLIDING" in h or "INVALIDATED" in h or "DEAD" in h:
        return CatalystStatus.INVALIDATED
    else:
        return CatalystStatus.PENDING


# ---------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------

def confirm_idea(idea: IdeaCandidate, flow_result: dict, ctx: RunContext) -> IdeaCandidate:
    """
    Run catalyst confirmation on a single idea.

    Calls v3 catalyst_confirm.confirm_catalyst(idea, flow_result, prediction_data).
    """
    try:
        from catalyst_confirm import confirm_catalyst

        # Build prediction market context (optional)
        prediction_data = ""
        try:
            from catalyst_hunter import build_kalshi_context
            prediction_data = build_kalshi_context()
        except Exception:
            pass

        # Call v3 confirm -- pass the raw v3 dict
        result = confirm_catalyst(
            idea=idea._v3_raw or {
                "ticker": idea.ticker, "direction": idea.idea_direction.value,
                "catalyst": idea.catalyst, "thesis": idea.thesis,
            },
            flow_result=flow_result,
            prediction_market_data=prediction_data,
        )

        # Map to v20 fields
        idea.catalyst_action = _parse_action(result.get("action", "HOLD"))
        idea.cds_score = result.get("effective_cds", 0)
        idea.catalyst_health = result
        idea.catalyst_status = _parse_catalyst_status(result.get("catalyst_health", ""))

        ctx.add_cost(result.get("cost", 0))

    except Exception as e:
        logger.error(f"[{idea.ticker}] Catalyst confirmation failed: {e}")
        ctx.add_error("stage3_catalyst", f"{idea.ticker}: {e}")
        idea.catalyst_action = CatalystAction.HOLD

    return idea


def run_stage3(
    ideas: List[IdeaCandidate], ctx: RunContext
) -> Tuple[List[IdeaCandidate], List[IdeaCandidate]]:
    """
    Execute Stage 3: Catalyst confirmation + filtering + report framing.

    Mirrors v3 pipeline.py filtering logic exactly:
      - KILL -> filtered
      - DOWNGRADE + CDS < 30 -> filtered
      - CONTRADICTORY tape + not CONFIRMED -> filtered
      - framing == "invalidated" -> filtered

    Returns (survivors, filtered_out).
    """
    # ── Defensive hard cap (belt-and-suspenders) ──
    import orca_v20.config as _cfg
    if _cfg.BUDGET_MODE and len(ideas) > _cfg.THRESHOLDS.budget_max_stage3_candidates:
        cap = _cfg.THRESHOLDS.budget_max_stage3_candidates
        ideas = list(ideas)  # don't mutate caller's list
        ideas.sort(key=lambda x: x.confidence, reverse=True)
        skipped = ideas[cap:]
        ideas = ideas[:cap]
        logger.warning(
            f"[budget][DEFENSIVE] Stage 3 adapter hard cap: processing {cap}, "
            f"dropping {len(skipped)} ({[i.ticker for i in skipped]})"
        )

    logger.info(f"[run_id={ctx.run_id}] Stage 3: Catalyst confirmation for {len(ideas)} ideas")

    survivors = []
    filtered = []

    for idea in ideas:
        # Get flow result for this ticker
        flow_result = idea.flow_details or {}

        # Run confirmation
        confirmed = confirm_idea(idea, flow_result, ctx)
        conf = confirmed.catalyst_health or {}

        action_str = conf.get("action", "HOLD FOR R2") if isinstance(conf, dict) else "HOLD"
        health_str = conf.get("catalyst_health", "?") if isinstance(conf, dict) else "?"
        cds = conf.get("effective_cds", 0) if isinstance(conf, dict) else 0
        tape = (confirmed.tape_read or "?")

        # -- Filter logic (mirrors v3 pipeline.py exactly) --

        if confirmed.catalyst_action == CatalystAction.KILL:
            kill_reason = conf.get("kill_reason", "Catalyst unhealthy") if isinstance(conf, dict) else "killed"
            logger.info(f"  [{idea.ticker}] KILLED -- {kill_reason}")
            filtered.append(confirmed)
            continue

        if confirmed.catalyst_action == CatalystAction.DOWNGRADE and cds < 30:
            logger.info(f"  [{idea.ticker}] DOWNGRADED + CDS={cds} < 30 -- removed")
            filtered.append(confirmed)
            continue

        if "CONTRADICTORY" in tape.upper() and confirmed.catalyst_action != CatalystAction.CONFIRM:
            logger.info(f"  [{idea.ticker}] Flow CONTRADICTORY + not CONFIRMED -- removed")
            filtered.append(confirmed)
            continue

        # -- Report framing --
        framing = determine_report_framing(
            direction=confirmed.idea_direction.value,
            tape_read=tape,
            catalyst_action=action_str,
            catalyst_health=health_str,
            cds_score=cds,
        )

        # Enrich raw v3 dict for downstream stages (structurer, logger, report)
        if confirmed._v3_raw:
            confirmed._v3_raw["tape_read"] = tape
            confirmed._v3_raw["catalyst_health"] = health_str
            confirmed._v3_raw["catalyst_action"] = action_str
            confirmed._v3_raw["cds_score"] = cds
            confirmed._v3_raw["flow_details"] = flow_result
            confirmed._v3_raw["confirmation_details"] = conf
            confirmed._v3_raw["report_framing"] = framing["framing"]
            confirmed._v3_raw["report_label"] = framing["label"]

        # Final gate
        if framing["framing"] == "invalidated":
            logger.info(
                f"  [{idea.ticker}] INVALIDATED by framing "
                f"(Health: {health_str}, Action: {action_str}) -- removed"
            )
            filtered.append(confirmed)
            continue

        status_icon = "CONFIRMED" if action_str == "CONFIRM" else "HELD"
        logger.info(
            f"  [{idea.ticker}] {status_icon} | Health: {health_str} | CDS: {cds} "
            f"| Tape: [{tape}] | Framing: {framing['framing']}"
        )
        survivors.append(confirmed)

    logger.info(
        f"[run_id={ctx.run_id}] Stage 3 complete: "
        f"{len(survivors)} survived, {len(filtered)} filtered"
    )
    ctx.mark_stage("stage3_catalyst")
    return survivors, filtered
