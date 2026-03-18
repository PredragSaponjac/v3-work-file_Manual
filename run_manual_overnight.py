#!/usr/bin/env python3
"""
ORCA v20 — Manual Overnight Teacher Runner.

Runs the overnight teacher with MANUAL_MODE enabled:
    - All LLM calls (replay analyst, L2/L3 deep review) pause for human input
    - Mechanical parts (auto-label, snapshots, forward outcomes) run automatically
    - Prompts saved to manual_prompts/
    - $0 API cost

Usage:
    python run_manual_overnight.py                # standard nightly
    python run_manual_overnight.py --deep-review  # weekly deep review
    python run_manual_overnight.py --verbose      # debug logging
    python run_manual_overnight.py --dry-run      # no DB writes
"""

import os
import sys

# ── Force manual mode BEFORE any config imports ──
os.environ["ORCA_MANUAL_MODE"] = "1"
os.environ["ORCA_BUDGET_MODE"] = "1"
os.environ["ORCA_PUBLISH_REPORTS"] = "0"
os.environ["ORCA_PUBLISH_TELEGRAM"] = "0"
os.environ["ORCA_PUBLISH_X"] = "0"
os.environ["ORCA_MIRROR_SHEET"] = "0"
os.environ["PYTHONIOENCODING"] = "utf-8"

# Ensure project root is on sys.path
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env for non-LLM keys
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

import logging

def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("manual_overnight")

    # Clean and reset manual prompts
    from orca_v20.manual_bridge import clean_prompts_dir, reset_seq
    clean_prompts_dir()
    reset_seq()

    import orca_v20.config as _cfg
    assert _cfg.MANUAL_MODE, "MANUAL_MODE should be True"

    logger.info("=" * 60)
    logger.info("  ORCA v20 — MANUAL OVERNIGHT TEACHER")
    logger.info("  All LLM calls pause for human input.")
    logger.info("  Mechanical parts (auto-label, snapshots) run automatically.")
    logger.info("  $0 API cost.")
    logger.info("=" * 60)
    logger.info("")

    # Run overnight with the original main()
    import overnight_v20
    overnight_v20.main()

    logger.info("")
    logger.info("=" * 60)
    logger.info("  MANUAL OVERNIGHT COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
