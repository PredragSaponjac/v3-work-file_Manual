#!/usr/bin/env python3
"""
ORCA v20 — Manual (Human-in-the-Loop) Pipeline Runner.

Runs the full v20 pipeline with MANUAL_MODE enabled:
    - All LLM API calls are replaced with file-based prompt/response handoff
    - Prompts are saved to manual_prompts/
    - Pipeline waits for response files to be written
    - Designed to run in Claude Code where the orchestrator (Claude Opus)
      reads prompts, shows them to the user, and writes responses

Usage:
    python run_manual.py                    # standard manual run
    python run_manual.py --verbose          # debug logging
    python run_manual.py --dry-run          # skip all writes
    python run_manual.py --clean            # clean manual_prompts/ before run
    python run_manual.py --mode deep        # deep research mode
    python run_manual.py --no-uw            # skip Unusual Whales

No API keys needed. No timeout on manual responses.
"""

import argparse
import logging
import os
import sys

# ── Force manual mode BEFORE any config imports ──
os.environ["ORCA_MANUAL_MODE"] = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

# Ensure project root is on sys.path
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env (for non-LLM keys: Telegram, Google Sheets, UW, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

# ── Imports (after MANUAL_MODE is set) ──
import orca_v20.config as _cfg
from orca_v20.manual_bridge import clean_prompts_dir, reset_seq, MANUAL_PROMPTS_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ORCA v20 Manual Pipeline (human-in-the-loop LLM calls)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip all writes and notifications")
    parser.add_argument("--mode", choices=["fast", "standard", "deep"],
                        default="standard", help="Research depth mode")
    parser.add_argument("--no-uw", action="store_true",
                        help="Skip Unusual Whales data source")
    parser.add_argument("--minimal", action="store_true",
                        help="Minimal sources (scanner + news only)")
    parser.add_argument("--verbose", action="store_true",
                        help="Debug-level logging")
    parser.add_argument("--clean", action="store_true",
                        help="Clean manual_prompts/ directory before run")
    return parser.parse_args()


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("orca_v20.manual_runner")


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    # Confirm manual mode is active
    assert _cfg.MANUAL_MODE, "MANUAL_MODE should be True (set via ORCA_MANUAL_MODE=1)"

    # Disable publishing in manual mode (no accidental Telegram/X posts)
    _cfg.FLAGS.publish_telegram = False
    _cfg.FLAGS.publish_x = False
    _cfg.FLAGS.mirror_to_google_sheet = False

    logger.info("=" * 60)
    logger.info("  ORCA v20 — MANUAL MODE (Human-in-the-Loop)")
    logger.info("=" * 60)
    logger.info(f"  MANUAL_MODE = {_cfg.MANUAL_MODE}")
    logger.info(f"  Prompts dir: {MANUAL_PROMPTS_DIR}")
    logger.info(f"  Publishing: ALL DISABLED")
    logger.info(f"  Mode: {args.mode}")
    logger.info("=" * 60)
    logger.info("")
    logger.info("  How it works:")
    logger.info("  1. Pipeline saves each LLM prompt to manual_prompts/")
    logger.info("  2. Pipeline waits for the matching _response.txt file")
    logger.info("  3. You (or the orchestrator) write the response file")
    logger.info("  4. Pipeline continues to the next stage")
    logger.info("")

    # Clean prompts directory if requested
    if args.clean:
        removed = clean_prompts_dir()
        logger.info(f"  Cleaned {removed} files from manual_prompts/")

    # Reset sequence counter
    reset_seq()

    # ── Import and run the pipeline ──
    from orca_v20.config import FLAGS, PATHS, THRESHOLDS
    from orca_v20.db_bootstrap import bootstrap_db
    from orca_v20.run_context import ResearchMode, RunContext, SourceMode

    # Build RunContext
    ctx = RunContext(
        research_mode=ResearchMode(args.mode),
        source_mode=(
            SourceMode.MINIMAL if args.minimal
            else SourceMode.NO_UW if args.no_uw
            else SourceMode.FULL
        ),
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_positions=THRESHOLDS.max_positions,
        overflow_slots=THRESHOLDS.overflow_slots,
        hard_cap=THRESHOLDS.hard_cap,
        max_api_cost_usd=999.0,  # No API budget limit in manual mode
    )

    # Temporal context
    from pipeline_v20 import build_temporal_context, load_regime, run_pipeline, save_run_trace
    build_temporal_context(ctx)

    # Portfolio value
    from orca_v20.sizing import _resolve_portfolio_value
    ctx.portfolio_value = _resolve_portfolio_value(ctx)
    logger.info(f"Portfolio value: ${ctx.portfolio_value:,.0f}")

    # Load regime
    load_regime(ctx)

    # Bootstrap DB
    bootstrap_db()

    # Run pipeline
    logger.info("")
    logger.info("Starting pipeline... LLM calls will pause for manual input.")
    logger.info("")

    success = run_pipeline(ctx)

    logger.info("")
    if success:
        logger.info("Pipeline completed successfully.")
    else:
        logger.info("Pipeline completed with errors (check logs above).")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
