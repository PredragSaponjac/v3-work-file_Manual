"""
Adapter: Stage 2 — UW Flow + Flow Reader.

Wraps uw_flow.build_ticker_flow_context() and flow_reader.read_flow()
to enrich IdeaCandidates with flow data and tape reads.
"""

import logging
import sys
import os
from typing import Dict, List

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from orca_v20.run_context import RunContext, SourceMode
from orca_v20.schemas import IdeaCandidate

logger = logging.getLogger("orca_v20.adapters.flow")


def enrich_with_flow(idea: IdeaCandidate, ctx: RunContext) -> IdeaCandidate:
    """
    Enrich a single IdeaCandidate with UW flow data + tape read.

    Calls v3 modules:
      uw_flow.classify_catalyst_speed(idea_dict)
      uw_flow.build_ticker_flow_context(ticker, ...)
      uw_flow.format_flow_for_prompt(flow_data)
      flow_reader.read_flow(ticker, direction, flow_text, catalyst_summary)
    """
    if ctx.source_mode in (SourceMode.NO_UW, SourceMode.MINIMAL):
        logger.info(f"[{idea.ticker}] Skipping flow (source_mode={ctx.source_mode.value})")
        idea.tape_read = "DATA UNAVAILABLE — UW disabled"
        idea.flow_details = {"tape_read": idea.tape_read, "cost": 0}
        return idea

    try:
        from uw_flow import classify_catalyst_speed, build_ticker_flow_context, format_flow_for_prompt
        from flow_reader import read_flow

        # Use raw v3 dict for classify_catalyst_speed (needs repricing_window etc.)
        v3_dict = idea._v3_raw or {
            "ticker": idea.ticker,
            "direction": idea.idea_direction.value,
            "catalyst": idea.catalyst,
            "repricing_window": idea.repricing_window,
        }

        direction = idea.idea_direction.value.capitalize()  # "Bullish" / "Bearish"

        # Step 1: Classify catalyst speed -> determines lookback depths
        speed_config = classify_catalyst_speed(v3_dict)
        lookback = speed_config["lookback"]
        logger.info(
            f"  [{idea.ticker}] catalyst speed = {speed_config['speed']} "
            f"(flow: {lookback['flow_alerts']}d, history: {lookback['contract_history']}d, "
            f"DP: {lookback['darkpool']}d)"
        )

        # Step 2: Fetch UW data with speed-adaptive lookback
        flow_data = build_ticker_flow_context(
            idea.ticker,
            include_history=True,
            lookback=lookback,
            direction=direction,
        )

        if flow_data.get("data_available"):
            # Step 3: Format for prompt
            flow_text = format_flow_for_prompt(flow_data)

            # Step 4: Claude reads the flow
            result = read_flow(
                idea.ticker,
                direction,
                flow_text,
                catalyst_summary=idea.catalyst[:200],
            )

            idea.tape_read = result.get("tape_read", "DATA UNAVAILABLE — VERIFY IN R2")
            idea.flow_details = result
            ctx.add_cost(result.get("cost", 0))
        else:
            logger.warning(f"  [{idea.ticker}] No UW flow data available")
            idea.tape_read = "DATA UNAVAILABLE — VERIFY IN R2"
            idea.flow_details = {"ticker": idea.ticker, "tape_read": idea.tape_read, "cost": 0}

    except Exception as e:
        logger.error(f"[{idea.ticker}] Flow enrichment failed: {e}")
        ctx.add_error("stage2_flow", f"{idea.ticker}: {e}")
        idea.tape_read = "DATA UNAVAILABLE — VERIFY IN R2"
        idea.flow_details = {"ticker": idea.ticker, "tape_read": idea.tape_read, "cost": 0}

    return idea


def run_stage2(ideas: List[IdeaCandidate], ctx: RunContext) -> List[IdeaCandidate]:
    """
    Execute Stage 2: Flow enrichment for all ideas.

    Defensive budget cap: even if the caller forgot to trim, this adapter
    will never process more than budget_max_stage2_candidates in budget mode.
    """
    import orca_v20.config as _cfg

    # ── Defensive hard cap (belt-and-suspenders) ──
    if _cfg.BUDGET_MODE and len(ideas) > _cfg.THRESHOLDS.budget_max_stage2_candidates:
        cap = _cfg.THRESHOLDS.budget_max_stage2_candidates
        ideas = sorted(ideas, key=lambda x: x.confidence, reverse=True)  # copy, don't mutate
        skipped = ideas[cap:]
        ideas = ideas[:cap]
        logger.warning(
            f"[budget][DEFENSIVE] Stage 2 adapter hard cap: processing {cap}, "
            f"dropping {len(skipped)} ({[i.ticker for i in skipped]})"
        )

    logger.info(f"[run_id={ctx.run_id}] Stage 2: Flow enrichment for {len(ideas)} ideas")

    enriched = []
    for idea in ideas:
        enriched.append(enrich_with_flow(idea, ctx))

    ctx.mark_stage("stage2_flow")
    return enriched
