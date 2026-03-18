"""
ORCA v20 — Elite Agent Simulation (15 agents).

Each agent has a specialized persona and independently evaluates a thesis.
Split into 3 tiers:
    4 BLIND   — see only ticker + catalyst + direction, no prior analysis
    7 GUIDED  — see full thesis + evidence + flow data
    4 ADVERSARIAL — specifically try to find reasons the trade will fail

When enable_elite_simulation is ON, calls router.call_model("elite_simulation", ...)
for each persona. When OFF or if the call fails, gracefully returns empty list.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from orca_v20.config import FLAGS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import EliteAgentVote, IdeaCandidate

logger = logging.getLogger("orca_v20.simulation.elite")

# ─────────────────────────────────────────────────────────────────────
# Agent personas with roles and tier assignments
# ─────────────────────────────────────────────────────────────────────

ELITE_PERSONAS = [
    # BLIND tier (4) — minimal context, independent judgment
    {"id": "vol_arb",       "persona": "vol_arb_specialist",      "tier": "blind",
     "prompt": "You are a volatility arbitrage specialist. Evaluate only the implied vol setup."},
    {"id": "macro",         "persona": "macro_strategist",         "tier": "blind",
     "prompt": "You are a macro strategist. Evaluate only the macro backdrop for this trade."},
    {"id": "technical",     "persona": "technical_analyst",        "tier": "blind",
     "prompt": "You are a pure technical analyst. Evaluate only chart structure and levels."},
    {"id": "behavioral",    "persona": "behavioral_finance",       "tier": "blind",
     "prompt": "You are a behavioral finance expert. Evaluate cognitive biases and positioning."},

    # GUIDED tier (7) — full context
    {"id": "flow",          "persona": "flow_specialist",          "tier": "guided",
     "prompt": "You are an options flow specialist. Evaluate the flow signals and OI patterns."},
    {"id": "event",         "persona": "event_driven_pm",          "tier": "guided",
     "prompt": "You are an event-driven PM. Evaluate the catalyst timing and expected payoff."},
    {"id": "quant",         "persona": "quant_systematic",         "tier": "guided",
     "prompt": "You are a quant/systematic trader. Evaluate the statistical edge and factor exposures."},
    {"id": "credit",        "persona": "credit_analyst",           "tier": "guided",
     "prompt": "You are a credit analyst. Evaluate credit risk and balance sheet implications."},
    {"id": "sector",        "persona": "sector_specialist",        "tier": "guided",
     "prompt": "You are a sector specialist. Evaluate the competitive dynamics and sector rotation."},
    {"id": "fundamental",   "persona": "fundamental_analyst",      "tier": "guided",
     "prompt": "You are a fundamental analyst. Evaluate valuation, earnings quality, and growth trajectory."},
    {"id": "energy",        "persona": "energy_specialist",        "tier": "guided",
     "prompt": "You are an energy/commodities specialist. Evaluate supply-demand and commodity linkages."},

    # ADVERSARIAL tier (4) — specifically challenge the thesis
    {"id": "risk",          "persona": "risk_manager",             "tier": "adversarial",
     "prompt": "You are a risk manager. Find every reason this trade could fail. Be adversarial."},
    {"id": "microstructure","persona": "market_microstructure",    "tier": "adversarial",
     "prompt": "You are a microstructure expert. Evaluate liquidity traps and execution risks."},
    {"id": "geopolitical",  "persona": "geopolitical_analyst",     "tier": "adversarial",
     "prompt": "You are a geopolitical analyst. Evaluate tail risks and policy/regulatory dangers."},
    {"id": "fi_crossover",  "persona": "fixed_income_crossover",   "tier": "adversarial",
     "prompt": "You are a fixed income crossover analyst. Challenge equity-only assumptions from a rates perspective."},
]


def _build_agent_prompt(agent: Dict, idea: IdeaCandidate, tier: str) -> str:
    """Build the user prompt for an elite agent based on tier."""
    if tier == "blind":
        # Minimal context
        return (
            f"Ticker: {idea.ticker}\n"
            f"Direction: {idea.idea_direction.value}\n"
            f"Catalyst: {idea.catalyst}\n\n"
            f"Give your vote: STRONG_BUY, BUY, HOLD, SELL, or STRONG_SELL.\n"
            f"Provide confidence (0.0-1.0) and brief reasoning (2-3 sentences).\n"
            f"Format: VOTE|CONFIDENCE|REASONING"
        )
    elif tier == "adversarial":
        # Full context + adversarial framing
        return (
            f"Ticker: {idea.ticker} | Direction: {idea.idea_direction.value}\n"
            f"Catalyst: {idea.catalyst}\n"
            f"Thesis: {idea.thesis}\n"
            f"Evidence: {'; '.join(idea.evidence[:5]) if idea.evidence else 'none'}\n"
            f"Tape: {idea.tape_read or 'N/A'}\n\n"
            f"YOUR MISSION: Challenge this thesis. Find the weakest links.\n"
            f"If you genuinely think the trade works despite your adversarial review, "
            f"say so — but set a high bar.\n"
            f"Vote: STRONG_BUY, BUY, HOLD, SELL, or STRONG_SELL.\n"
            f"Format: VOTE|CONFIDENCE|REASONING"
        )
    else:  # guided
        return (
            f"Ticker: {idea.ticker} | Direction: {idea.idea_direction.value}\n"
            f"Catalyst: {idea.catalyst}\n"
            f"Thesis: {idea.thesis}\n"
            f"Evidence: {'; '.join(idea.evidence[:5]) if idea.evidence else 'none'}\n"
            f"Tape: {idea.tape_read or 'N/A'}\n"
            f"Confidence: {idea.confidence}/10\n\n"
            f"Give your vote: STRONG_BUY, BUY, HOLD, SELL, or STRONG_SELL.\n"
            f"Provide confidence (0.0-1.0) and brief reasoning (2-3 sentences).\n"
            f"Format: VOTE|CONFIDENCE|REASONING"
        )


def _parse_vote_response(raw: str, agent_id: str) -> Dict:
    """Parse VOTE|CONFIDENCE|REASONING format from LLM response."""
    parts = raw.strip().split("|", 2)

    vote = "HOLD"
    confidence = 0.5
    reasoning = raw

    if len(parts) >= 1:
        v = parts[0].strip().upper().replace(" ", "_")
        if v in ("STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"):
            vote = v

    if len(parts) >= 2:
        try:
            confidence = float(parts[1].strip())
            confidence = max(0.0, min(1.0, confidence))
        except ValueError:
            confidence = 0.5

    if len(parts) >= 3:
        reasoning = parts[2].strip()

    # Dissent: adversarial agents who vote BUY, or guided agents who vote SELL
    is_dissent = False  # set by aggregation layer

    return {"vote": vote, "confidence": confidence, "reasoning": reasoning}


def simulate_elite(idea: IdeaCandidate, ctx: RunContext) -> List[EliteAgentVote]:
    """
    Run all 15 elite agents on a single idea.
    Returns list of EliteAgentVote.

    Gracefully handles failures — any agent that fails just doesn't vote.
    """
    if not FLAGS.enable_elite_simulation:
        return []

    logger.info(f"  [{idea.ticker}] Running elite simulation (15 agents)")

    votes = []

    for agent in ELITE_PERSONAS:
        try:
            from orca_v20.router import call_model

            system_prompt = agent["prompt"]
            user_prompt = _build_agent_prompt(agent, idea, agent["tier"])

            response = call_model(
                role="elite_simulation",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                ctx=ctx,
                max_tokens=500,
                thinking_budget=0,  # no thinking needed for vote format
            )

            parsed = _parse_vote_response(response.content, agent["id"])

            vote = EliteAgentVote(
                agent_id=agent["id"],
                agent_persona=agent["persona"],
                thesis_id=idea.thesis_id or "",
                vote=parsed["vote"],
                confidence=parsed["confidence"],
                reasoning=parsed["reasoning"][:500],
                dissent_flag=False,  # set in aggregation
            )
            votes.append(vote)

            logger.debug(
                f"    [{agent['id']}] ({agent['tier']}) → {parsed['vote']} "
                f"(conf={parsed['confidence']:.2f})"
            )

        except Exception as e:
            logger.warning(f"    [{agent['id']}] Elite agent failed: {e}")
            continue

    # Persist votes to DB
    _persist_votes(votes, ctx)

    logger.info(
        f"  [{idea.ticker}] Elite simulation: {len(votes)}/15 agents responded"
    )
    return votes


def _persist_votes(votes: List[EliteAgentVote], ctx: RunContext) -> None:
    """Write elite votes to DB."""
    if ctx.dry_run or not votes:
        return
    try:
        conn = get_connection()
        for v in votes:
            conn.execute("""
                INSERT INTO elite_agent_votes (
                    run_id, thesis_id, agent_id, agent_persona,
                    vote, confidence, reasoning, dissent_flag,
                    created_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ctx.run_id, v.thesis_id, v.agent_id, v.agent_persona,
                v.vote, v.confidence, v.reasoning, int(v.dissent_flag),
                datetime.now(timezone.utc).isoformat(),
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist elite votes: {e}")
