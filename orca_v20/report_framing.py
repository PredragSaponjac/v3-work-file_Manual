"""
ORCA v20 — Report Framing Logic.

Reconciles thesis direction + tape read + catalyst confirmation
into a public-facing report framing label.

Ported from pipeline.py determine_report_framing() — identical logic,
no v3 import dependency. Pure function.
"""


_FRAMING_LABELS = {
    "conviction_call":      "{dir}",
    "bullish_with_caveats": "{dir}, but confirmation is mixed",
    "bearish_with_caveats": "{dir}, but confirmation is mixed",
    "watchlist":            "watchlist",
    "downgrade_note":       "thesis weakening",
    "invalidated":          "thesis broken",
}


def determine_report_framing(
    direction: str,
    tape_read: str,
    catalyst_action: str,
    catalyst_health: str,
    cds_score: int = None,
) -> dict:
    """
    Decide the public-facing framing for the executive report.

    Reconciles R1's original direction with Stage 2 tape and
    Stage 3 catalyst confirmation. The report writer should never
    have to reconcile a bullish headline with a fading catalyst —
    that decision is made here.

    Returns:
        {"framing": str, "label": str}

    framing values:
        "conviction_call"      — full confidence
        "bullish_with_caveats" — bullish but mixed signals
        "bearish_with_caveats" — bearish but mixed signals
        "watchlist"            — too weak for conviction
        "downgrade_note"       — thesis weakening
        "invalidated"          — thesis broken
    """
    direction = (direction or "").lower()
    tape = (tape_read or "").lower()
    action = (catalyst_action or "").lower()
    health = (catalyst_health or "").lower()

    # Invalidated: thesis is dead
    if action == "kill" or health in ("colliding", "invalidated", "dead"):
        framing = "invalidated"

    # Downgraded by Stage 3
    elif action == "downgrade":
        if "mixed" in tape or "neutral" in tape or health == "fading":
            framing = "watchlist"
        else:
            framing = "downgrade_note"

    # Confirmed or held — check tape + health alignment
    else:
        healthy = health in ("strengthening", "stable", "active", "strong")
        tape_supportive = "supportive" in tape
        tape_bad = "contradictory" in tape or "opposing" in tape
        tape_mixed = "mixed" in tape or "neutral" in tape

        if direction in ("bullish", "bearish"):
            if tape_supportive and healthy:
                framing = "conviction_call"
            elif tape_bad:
                framing = f"{direction}_with_caveats"
            elif tape_mixed and not healthy:
                framing = "watchlist"
            elif tape_mixed or not healthy:
                framing = f"{direction}_with_caveats"
            else:
                framing = "conviction_call"
        else:
            framing = "watchlist"

    dir_label = direction.capitalize() if direction else "Neutral"
    template = _FRAMING_LABELS.get(framing, "watchlist")
    label = template.format(dir=dir_label)
    return {"framing": framing, "label": label}
