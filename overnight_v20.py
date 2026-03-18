#!/usr/bin/env python3
"""
ORCA v20 — Overnight Learning CLI Entrypoint.

Usage:
    python overnight_v20.py                   # standard nightly replay
    python overnight_v20.py --deep-review     # weekly deep review (premium models)
    python overnight_v20.py --dry-run         # no writes, no sends
    python overnight_v20.py --verbose         # debug logging

Schedule:
    Nightly: 8:00 PM CT (cron: 0 20 * * *)
    Weekly:  Sunday 6:00 PM CT (cron: 0 18 * * 0)
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env file (Telegram, X, EIA, Google Sheets, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass  # dotenv optional — env vars must be set externally

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import bootstrap_db
from orca_v20.run_context import ResearchMode, RunContext, SourceMode
from orca_v20.overnight import run_overnight


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("orca_v20.overnight_cli")


def main() -> None:
    parser = argparse.ArgumentParser(description="ORCA v20 Overnight Learning")
    parser.add_argument("--deep-review", action="store_true",
                        help="Enable premium Layer 3 deep review")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip all writes and notifications")
    parser.add_argument("--verbose", action="store_true",
                        help="Debug-level logging")
    parser.add_argument("--budget", action="store_true",
                        help="Budget sprint overnight (still premium models, lower budget cap)")
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)

    # Budget overnight: still uses premium models (normal RoleRouting)
    # but with a tighter budget cap. Does NOT set ORCA_BUDGET_MODE
    # because overnight should use premium models as the teacher.
    budget_cap = THRESHOLDS.overnight_hard_budget_usd
    if args.budget:
        # Use the budget-mode overnight cap if set, otherwise $10 default
        import orca_v20.config as _cfg
        if _cfg.BUDGET_MODE:
            budget_cap = THRESHOLDS.overnight_hard_budget_usd  # already overridden to $10
        else:
            budget_cap = 10.0  # explicit CLI override
        logger.info("=" * 60)
        logger.info("*** BUDGET SPRINT OVERNIGHT — PREMIUM TEACHER MODE ***")
        logger.info(f"  Models: PREMIUM (normal routing — NOT cheap)")
        logger.info(f"  Budget cap: ${budget_cap}")
        logger.info(f"  Purpose: Review cheap daytime outputs, write teacher artifacts")
        logger.info("=" * 60)

    ctx = RunContext(
        research_mode=ResearchMode.STANDARD,
        source_mode=SourceMode.MINIMAL,
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_api_cost_usd=budget_cap,
    )

    now = ctx.as_of_utc
    ctx.market_date = now.strftime("%Y-%m-%d")

    bootstrap_db()

    summary = run_overnight(ctx, deep_review=args.deep_review)

    # Write teacher artifacts
    if args.budget and summary:
        _write_teacher_artifacts(ctx, summary)

    logger.info(f"Overnight complete. Summary: {summary}")
    sys.exit(0)


def _write_teacher_artifacts(ctx: RunContext, summary: dict) -> None:
    """Write overnight teacher artifacts for budget sprint review."""
    import json
    from orca_v20.config import PATHS

    artifacts_dir = PATHS.artifacts_dir
    os.makedirs(artifacts_dir, exist_ok=True)

    prefix = f"{artifacts_dir}/{ctx.market_date}_overnight"

    # Overnight summary
    with open(f"{prefix}_summary.json", "w") as f:
        json.dump({
            "run_id": ctx.run_id,
            "market_date": ctx.market_date,
            "timestamp_utc": ctx.as_of_utc.isoformat(),
            "total_cost_usd": round(ctx.api_cost_usd, 4),
            "summary": summary,
        }, f, indent=2, default=str)

    # Horizon outcomes JSONL
    try:
        from orca_v20.thesis_store import get_horizon_outcomes
        from orca_v20.db_bootstrap import get_connection
        from orca_v20.config import THRESHOLDS

        conn = get_connection()
        theses = conn.execute("""
            SELECT thesis_id, ticker, expected_horizon
            FROM theses
            WHERE status IN ('ACTIVE', 'DRAFT', 'CLOSED_WIN', 'CLOSED_LOSS',
                           'CLOSED_EXPIRED', 'CLOSED_INVALIDATED')
        """).fetchall()
        conn.close()

        jsonl_path = f"{prefix}_horizon_outcomes.jsonl"
        with open(jsonl_path, "w") as f:
            for t in theses:
                tid = t["thesis_id"]
                outcomes = get_horizon_outcomes(tid)

                # Per-window outcome records
                computed_windows = []
                for o in outcomes:
                    o["record_type"] = "outcome"
                    f.write(json.dumps(o, default=str) + "\n")
                    computed_windows.append(o.get("window_days"))

                # Thesis summary record (computed vs skipped windows)
                all_windows = THRESHOLDS.forward_outcome_windows
                skipped = [w for w in all_windows if w not in computed_windows]
                summary_rec = {
                    "record_type": "thesis_summary",
                    "thesis_id": tid,
                    "ticker": t["ticker"],
                    "expected_horizon": t["expected_horizon"] or "UNKNOWN",
                    "windows_computed": sorted(computed_windows),
                    "windows_skipped": sorted(skipped),
                }
                f.write(json.dumps(summary_rec, default=str) + "\n")

        logger.info(f"[teacher] Horizon outcomes JSONL: {jsonl_path}")
    except Exception as e:
        logger.warning(f"[teacher] Horizon outcomes JSONL failed: {e}")

    logger.info(f"[teacher] Artifacts written to {artifacts_dir}/")


if __name__ == "__main__":
    main()
