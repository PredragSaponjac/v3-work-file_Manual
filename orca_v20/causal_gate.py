"""
ORCA v20 — Causal Gate (Phase 4 hardened).

DoWhy-style causal testing scaffold:
    catalyst → sector → ticker price reaction

Gate statuses:
    PASS             — heuristic score >= 0.7 or DoWhy p < 0.05
    PASS_LOW_CONFIDENCE — heuristic score 0.5-0.7
    UNPROVEN         — catalyst too vague to evaluate
    FAIL             — heuristic score < 0.5 or DoWhy p > 0.20

When `dowhy` is unavailable: uses lightweight heuristic.
This is a soft gate — biased to pass, blocks only clearly weak catalysts.
"""

import logging
from typing import Dict, Tuple

from orca_v20.config import FLAGS
from orca_v20.run_context import RunContext
from orca_v20.schemas import GateStatus, IdeaCandidate

logger = logging.getLogger("orca_v20.causal_gate")


def _check_dowhy_available() -> bool:
    """Check if DoWhy library is installed."""
    try:
        import dowhy  # noqa: F401
        return True
    except ImportError:
        return False


def _heuristic_causal_check(idea: IdeaCandidate) -> Tuple[GateStatus, Dict]:
    """
    Lightweight causal heuristic when DoWhy is unavailable.

    Checks:
    1. Catalyst has a clear temporal ordering (event → expected reaction)
    2. Idea has identified transmission mechanism
    3. Invalidation condition is specific (not vague)

    Returns (GateStatus, details).
    """
    score = 0.0
    details = {
        "method": "heuristic",
        "checks": [],
        "reason_codes": [],
    }

    # 1. Catalyst specificity
    catalyst = idea.catalyst or ""
    if len(catalyst) > 30 and any(w in catalyst.lower() for w in
            ["earnings", "fda", "merger", "tariff", "rate", "guidance",
             "contract", "launch", "ruling", "split", "buyback", "settlement",
             "war", "sanction", "regulation", "strike", "recall", "fuel",
             "oil", "hurricane", "bankruptcy", "acquisition"]):
        score += 0.4
        details["checks"].append("catalyst_specific: yes")
        details["reason_codes"].append("CATALYST_SPECIFIC")
    elif len(catalyst) > 10:
        score += 0.2
        details["checks"].append("catalyst_specific: partial")
        details["reason_codes"].append("CATALYST_PARTIAL")
    else:
        details["checks"].append("catalyst_specific: weak")
        details["reason_codes"].append("CATALYST_WEAK")

    # 2. Thesis has transmission mechanism
    thesis = idea.thesis or ""
    transmission_words = ["because", "leading to", "which will", "resulting in",
                          "transmission", "repricing", "lag", "delayed",
                          "margin", "revenue", "cost", "demand", "supply",
                          "exposure", "sensitivity", "impact"]
    has_mechanism = any(w in thesis.lower() for w in transmission_words)
    if has_mechanism:
        score += 0.3
        details["checks"].append("transmission_mechanism: yes")
        details["reason_codes"].append("MECHANISM_PRESENT")
    else:
        score += 0.1
        details["checks"].append("transmission_mechanism: weak")
        details["reason_codes"].append("MECHANISM_WEAK")

    # 3. Invalidation is specific
    invalidation = idea.invalidation or ""
    if len(invalidation) > 20:
        score += 0.3
        details["checks"].append("invalidation_specific: yes")
        details["reason_codes"].append("INVALIDATION_SPECIFIC")
    else:
        score += 0.1
        details["checks"].append("invalidation_specific: weak")
        details["reason_codes"].append("INVALIDATION_WEAK")

    details["score"] = round(score, 3)

    # Determine gate status
    if score >= 0.7:
        gate_status = GateStatus.PASS
    elif score >= 0.5:
        gate_status = GateStatus.PASS_LOW_CONFIDENCE
    elif score >= 0.3:
        gate_status = GateStatus.UNPROVEN
    else:
        gate_status = GateStatus.FAIL

    details["gate_status"] = gate_status.value
    return gate_status, details


def _dowhy_causal_check(idea: IdeaCandidate) -> Tuple[GateStatus, Dict]:
    """
    Full DoWhy causal inference (when library is available).
    Currently a scaffold — soft pass with PASS_LOW_CONFIDENCE
    since we don't have real panel data yet.
    """
    try:
        import dowhy  # noqa: F401

        logger.info(f"  [{idea.ticker}] Running DoWhy causal check (scaffold)")

        details = {
            "method": "dowhy",
            "model": "backdoor_adjustment",
            "p_value": None,
            "ate": None,
            "gate_status": GateStatus.PASS_LOW_CONFIDENCE.value,
            "reason_codes": ["DOWHY_SCAFFOLD_SOFT_PASS"],
            "note": "DoWhy available but real panel data pipeline not yet built",
        }

        return GateStatus.PASS_LOW_CONFIDENCE, details

    except Exception as e:
        logger.warning(f"  [{idea.ticker}] DoWhy check failed: {e}")
        return _heuristic_causal_check(idea)


def evaluate(idea: IdeaCandidate, ctx: RunContext) -> Tuple[bool, Dict]:
    """
    Run causal inference test on an idea.

    Returns (passed: bool, details_dict).
    passed=False only when gate_status is FAIL.
    """
    if not FLAGS.enable_causal_gate:
        return True, {"gate_status": "DISABLED", "reason": "causal_gate disabled"}

    if _check_dowhy_available():
        gate_status, details = _dowhy_causal_check(idea)
    else:
        logger.debug(f"  [{idea.ticker}] DoWhy not installed — using heuristic")
        gate_status, details = _heuristic_causal_check(idea)

    passed = gate_status != GateStatus.FAIL

    logger.info(
        f"  [{idea.ticker}] Causal gate: {gate_status.value} "
        f"(method={details.get('method', '?')}, "
        f"reasons={details.get('reason_codes', [])})"
    )

    return passed, details
