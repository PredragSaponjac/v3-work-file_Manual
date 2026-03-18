"""
ORCA v20 — Shared text utilities.

Lightweight tokenizer and stop-word filter used by:
    - thesis_store.py (TF-IDF matching)
    - evidence_gate.py (keyword extraction)
    - daemon_rules.py (thesis overlap detection)
"""

import re
from typing import List

_STOP_WORDS = frozenset(
    "a an the is are was were be been being have has had do does did "
    "will would shall should may might can could and or but if then else "
    "for of to in on at by with from as into this that these those it "
    "its not no nor so yet also very too much more most than such "
    # v20 extension — low-signal filler words that dilute IDF/matching
    "over just about after before".split()
)


def tokenize(text: str) -> List[str]:
    """Simple word tokenizer: lowercase, alpha-only, no stop words, min length 3."""
    words = re.findall(r"[a-z]+", text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) > 2]
