"""
ORCA v20 — Publishing Module (Operator Activation).

Handles filtered publishing to Telegram, X (Twitter), and Google Sheets.

Publishing policies:
    Telegram: full institutional executive reports (LLM-generated, 1,800-2,500 chars)
              + KILL alerts, soft-stop/trim, nightly replay summary, daily run summary
    X:        filtered high-confidence only (confidence >= 8, ACTIONABLE,
              no contradiction, not capacity_constrained, not illiquid)
              Full institutional executive reports up to 4,000 chars (NOT 280)
    Sheet:    full structured trade set (all pipeline survivors)

Safety: publish_telegram / publish_x / mirror_to_google_sheet flags
        each independently controllable. dry_run blocks all sends.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from orca_v20.config import BUDGET_MODE, FLAGS, THRESHOLDS
from orca_v20.executive_report import (
    GOOGLE_SHEET_URL,
    format_telegram_report,
    format_x_report,
    generate_executive_reports,
)
from orca_v20.run_context import RunContext
from orca_v20.schemas import IdeaCandidate, StructuredTrade
from orca_v20.telegram_format import (
    format_telegram_message,
    format_daily_summary,
    format_replay_summary,
)

logger = logging.getLogger("orca_v20.publisher")


# ─────────────────────────────────────────────────────────────────────
# X (Twitter) filter
# ─────────────────────────────────────────────────────────────────────

def _passes_x_filter(trade: StructuredTrade, idea: IdeaCandidate,
                     execution_meta: Optional[Dict] = None) -> tuple:
    """
    Check if a trade passes the X publishing filter.
    Returns (passes: bool, reject_reasons: list).
    """
    reasons = []

    # Confidence threshold
    if trade.confidence < THRESHOLDS.x_min_confidence:
        reasons.append(f"confidence={trade.confidence} < {THRESHOLDS.x_min_confidence}")

    # Block KILL / HOLD / DOWNGRADE from X — only conviction trades post
    if THRESHOLDS.x_require_actionable:
        action = getattr(idea, 'catalyst_action', None)
        if action and action.value in ("KILL", "HOLD", "DOWNGRADE"):
            reasons.append(f"action={action.value} (not publishable)")

    # Capacity constrained
    if THRESHOLDS.x_block_capacity_constrained and execution_meta:
        if execution_meta.get("capacity_constrained"):
            reasons.append("capacity_constrained")

    # Illiquid
    if THRESHOLDS.x_block_illiquid and execution_meta:
        if execution_meta.get("illiquid"):
            reasons.append("illiquid")

    # Active contradiction flag
    if THRESHOLDS.x_block_contradicted:
        if getattr(idea, 'crowding_risk', '') and 'contradiction' in str(idea.crowding_risk).lower():
            reasons.append("active_contradiction")

    return (len(reasons) == 0, reasons)


def format_x_post(trade: StructuredTrade, idea: IdeaCandidate) -> str:
    """
    Format a concise X post (280 char target).
    """
    direction = "Bullish" if trade.idea_direction.value == "BULLISH" else "Bearish"
    conf_bar = "+" * min(trade.confidence, 10)

    parts = [
        f"${trade.ticker} — {direction}",
        f"Catalyst: {idea.catalyst[:80]}" if idea.catalyst else "",
        f"Structure: {trade.strategy_label}",
    ]

    if trade.strike_1:
        strike_str = f"${trade.strike_1:.0f}"
        if trade.strike_2:
            strike_str += f"/${trade.strike_2:.0f}"
        parts.append(f"Strikes: {strike_str} | Exp: {trade.expiry}")

    if trade.risk_reward:
        parts.append(f"R/R: {trade.risk_reward:.1f}x")

    parts.append(f"Confidence: [{conf_bar}] {trade.confidence}/10")
    parts.append("#ORCA #options #trading")

    return "\n".join(p for p in parts if p)


# ─────────────────────────────────────────────────────────────────────
# Telegram formatting
# ─────────────────────────────────────────────────────────────────────

class TelegramMessageType:
    """Message types for Telegram alerts."""
    NEW_ACTIONABLE = "NEW_ACTIONABLE"
    WATCH_PROMOTION = "WATCH_PROMOTION"
    KILL_ALERT = "KILL_ALERT"
    SOFT_STOP = "SOFT_STOP"
    TRIM_ALERT = "TRIM_ALERT"
    REPLAY_SUMMARY = "REPLAY_SUMMARY"
    RUN_SUMMARY = "RUN_SUMMARY"
    OVERNIGHT_SUMMARY = "OVERNIGHT_SUMMARY"
    BUDGET_WARNING = "BUDGET_WARNING"


def format_telegram_trade(trade: StructuredTrade, idea: IdeaCandidate,
                          msg_type: str = TelegramMessageType.NEW_ACTIONABLE) -> str:
    """Format a Telegram message for a single trade."""

    if msg_type == TelegramMessageType.KILL_ALERT:
        return (
            f"KILL ALERT: ${trade.ticker}\n"
            f"Reason: {idea.invalidation or 'catalyst invalidated'}\n"
            f"Action: close position immediately"
        )

    if msg_type == TelegramMessageType.SOFT_STOP:
        direction = "Bearish" if trade.idea_direction.value == "BEARISH" else "Bullish"
        action_label = idea.catalyst_action.value if idea.catalyst_action else "DOWNGRADE"
        lines = [
            f"ORCA Research — ${trade.ticker}",
            f"${trade.ticker} {action_label.lower()} — thesis weakening",
            "",
            f"Direction: {direction} | Confidence: {trade.confidence}/10",
        ]
        if idea.catalyst:
            lines.append(f"Catalyst: {idea.catalyst[:200]}")

        # Catalyst health + CDS (why it was downgraded)
        health_info = idea.catalyst_health
        if health_info and isinstance(health_info, dict):
            health_val = health_info.get("catalyst_health", "")
            cds = health_info.get("effective_cds", idea.cds_score)
            if health_val:
                health_line = f"Catalyst health: {health_val}"
                if cds:
                    health_line += f" (CDS: {cds}/100)"
                lines.append(health_line)
            kill_reason = health_info.get("kill_reason", "")
            if kill_reason:
                lines.append(f"Downgrade reason: {kill_reason[:200]}")
        elif health_info:
            lines.append(f"Catalyst health: {health_info}")

        if idea.invalidation:
            lines.append(f"Invalidation: {idea.invalidation[:150]}")
        lines.append("")
        lines.append("Action: monitoring only — not a conviction trade.")

        # Footer: thesis_id + run_id
        footer_parts = []
        if trade.thesis_id:
            footer_parts.append(f"Thesis: {trade.thesis_id[:12]}")
        if trade.run_id:
            footer_parts.append(f"Run: {trade.run_id[:12]}")
        if footer_parts:
            lines.append(f"\n{' | '.join(footer_parts)}")
        return "\n".join(lines)

    # Standard new actionable
    direction = "BULL" if trade.idea_direction.value == "BULLISH" else "BEAR"
    lines = [
        f"NEW {direction}: ${trade.ticker}",
        f"Catalyst: {idea.catalyst[:120]}",
        f"Strategy: {trade.strategy_label}",
    ]

    if trade.strike_1:
        s = f"${trade.strike_1:.0f}"
        if trade.strike_2:
            s += f"/${trade.strike_2:.0f}"
        lines.append(f"Strikes: {s} | Exp: {trade.expiry}")

    if trade.entry_price:
        lines.append(f"Entry: ${trade.entry_price:.2f} | R/R: {trade.risk_reward:.1f}x")

    if trade.contracts:
        lines.append(f"Size: {trade.contracts}x ({(trade.adjusted_size_pct or 0)*100:.1f}%)")

    lines.append(f"Confidence: {trade.confidence}/10 | Urgency: {trade.urgency}/10")

    if trade.estimated_slippage_pct:
        lines.append(f"Slippage: {trade.estimated_slippage_pct:.2f}%")

    return "\n".join(lines)


def format_telegram_run_summary(ctx: RunContext, trades: List[StructuredTrade],
                                 trace: Dict) -> str:
    """Format daily run summary for Telegram."""
    lines = [
        f"ORCA v20 Daily Summary — {ctx.market_date}",
        f"Run: {ctx.run_id}",
        "",
        f"Ideas generated: {trace.get('ideas_generated', 0)}",
        f"After gates: {trace.get('ideas_after_gates', 0)}",
        f"Trades structured: {trace.get('trades_structured', 0)}",
        f"Trades logged: {trace.get('trades_logged', 0)}",
        "",
        f"API cost: ${ctx.api_cost_usd:.2f}",
        f"Errors: {len(ctx.errors)}",
    ]

    if trades:
        lines.append("")
        lines.append("Trades:")
        for t in trades:
            d = "B" if t.idea_direction.value == "BULLISH" else "S"
            framing = t.report_framing or ""
            if framing in ("watchlist", "downgrade_note", "invalidated"):
                # Don't show trade structure for non-conviction framings
                lines.append(f"  ${t.ticker} [{framing}] c={t.confidence}")
            else:
                lines.append(f"  {d} ${t.ticker} {t.strategy_label} c={t.confidence}")

    # Confidence trajectory for active theses (RISING / FALLING movers)
    try:
        from orca_v20.thesis_store import get_confidence_trajectory
        trajectories = get_confidence_trajectory(ctx)
        movers = [t for t in trajectories if t["trajectory"] != "STABLE"]
        if movers:
            lines.append("")
            lines.append("Thesis Momentum:")
            for m in movers[:5]:
                arrow = "\u2191" if m["trajectory"] == "RISING" else "\u2193"
                price_str = f" @ ${m['latest_price']:.2f}" if m.get("latest_price") else ""
                lines.append(
                    f"  {arrow} ${m['ticker']} {m['trajectory']} "
                    f"(conf {m['latest_confidence']}, delta {m['confidence_delta']:+d}){price_str}"
                )
    except Exception as e:
        logger.debug(f"Could not add trajectory to run summary: {e}")

    # Footer with Google Sheet link
    lines.append("")
    lines.append(f"Trade log: {GOOGLE_SHEET_URL}")

    return "\n".join(lines)


def format_telegram_replay_summary(replay_results: List[Dict],
                                    budget_summary: Optional[Dict] = None,
                                    ctx: Optional[RunContext] = None) -> str:
    """Format nightly replay summary for Telegram."""
    if not replay_results:
        return "ORCA v20 Nightly Replay: no theses to replay tonight."

    rules_only = sum(1 for r in replay_results if r.get("replay_mode") == "RULES_ONLY")
    premium = sum(1 for r in replay_results if r.get("replay_mode") == "PREMIUM_ESCALATED")
    deferred = sum(1 for r in replay_results if r.get("replay_mode") == "DEFERRED_BUDGET")
    examples = sum(r.get("training_examples_generated", 0) for r in replay_results)

    lines = [
        f"ORCA v20 Nightly Replay Summary",
        f"Theses replayed: {len(replay_results)}",
        f"  Rules-only: {rules_only}",
        f"  Premium: {premium}",
        f"  Deferred (budget): {deferred}",
        f"Training examples: {examples}",
    ]

    # Top misses
    losses = [r for r in replay_results if r.get("realized_outcome") == "LOSS"]
    if losses:
        lines.append("")
        lines.append("Losses reviewed:")
        for r in losses[:3]:
            lines.append(f"  {r['ticker']}: {r.get('counterfactual_verdict', '')[:100]}")

    # Confidence trajectory for active theses (RISING / FALLING movers)
    if ctx:
        try:
            from orca_v20.thesis_store import get_confidence_trajectory
            trajectories = get_confidence_trajectory(ctx)
            movers = [t for t in trajectories if t["trajectory"] != "STABLE"]
            if movers:
                lines.append("")
                lines.append("Thesis Momentum:")
                for m in movers[:5]:
                    arrow = "\u2191" if m["trajectory"] == "RISING" else "\u2193"
                    price_str = f" @ ${m['latest_price']:.2f}" if m.get("latest_price") else ""
                    lines.append(
                        f"  {arrow} ${m['ticker']} {m['trajectory']} "
                        f"(conf {m['latest_confidence']}, delta {m['confidence_delta']:+d}){price_str}"
                    )
        except Exception as e:
            logger.debug(f"Could not add trajectory to replay summary: {e}")

    if budget_summary:
        lines.append("")
        lines.append(f"Overnight cost: ${budget_summary.get('total_cost', 0):.2f}")
        lines.append(f"Budget remaining: ${budget_summary.get('budget_remaining', 0):.2f}")

    # Footer with Google Sheet link
    lines.append("")
    lines.append(f"Trade log: {GOOGLE_SHEET_URL}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# Google Sheet row builder
# ─────────────────────────────────────────────────────────────────────

def build_sheet_row(trade: StructuredTrade, idea: IdeaCandidate,
                    execution_meta: Optional[Dict] = None,
                    replay_status: str = "") -> Dict:
    """
    Build a dict representing one row for Google Sheet logging.
    All columns that the sheet policy requires.
    """
    return {
        "run_id": trade.run_id,
        "market_date": "",  # filled by caller
        "ticker": trade.ticker,
        "idea_direction": trade.idea_direction.value,
        "thesis_id": trade.thesis_id,
        "thesis_status": idea.thesis_status.value if idea.thesis_status else "DRAFT",
        "action_bucket": (idea.catalyst_action.value if idea.catalyst_action else "UNKNOWN"),
        "confidence": trade.confidence,
        "urgency": trade.urgency,
        "catalyst": idea.catalyst[:200] if idea.catalyst else "",
        "thesis": idea.thesis[:300] if idea.thesis else "",
        "strategy_label": trade.strategy_label,
        "trade_expression": trade.trade_expression_type.value,
        "strike_1": trade.strike_1,
        "strike_2": trade.strike_2,
        "expiry": trade.expiry,
        "dte": trade.dte,
        "entry_price": trade.entry_price,
        "target_price": trade.target_price,
        "stop_price": trade.stop_price,
        "max_loss": trade.max_loss,
        "max_gain": trade.max_gain,
        "risk_reward": trade.risk_reward,
        "kelly_size_pct": trade.kelly_size_pct,
        "adjusted_size_pct": trade.adjusted_size_pct,
        "contracts": trade.contracts,
        "slippage_pct": trade.estimated_slippage_pct,
        "liquidity_score": trade.liquidity_score,
        "execution_impact": json.dumps(execution_meta) if execution_meta else "",
        "consensus_tag": trade.consensus_tag.value,
        "monitor_status": "ACTIVE",
        "replay_status": replay_status,
    }


# ─────────────────────────────────────────────────────────────────────
# Send helpers (wrap external APIs)
# ─────────────────────────────────────────────────────────────────────

def _send_telegram(text: str) -> bool:
    """Send a Telegram message via bot API."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("[telegram] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False

    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        if resp.status_code == 200:
            logger.info("[telegram] Message sent successfully")
            return True
        else:
            logger.warning(f"[telegram] Send failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"[telegram] Send error: {e}")
        return False


def _send_x_post(text: str) -> bool:
    """Post to X (Twitter) via API v2."""
    api_key = os.environ.get("X_API_KEY", "")
    api_secret = os.environ.get("X_API_SECRET", "")
    access_token = os.environ.get("X_ACCESS_TOKEN", "")
    access_secret = os.environ.get("X_ACCESS_SECRET", "")

    if not all([api_key, api_secret, access_token, access_secret]):
        logger.warning("[x] Missing X API credentials")
        return False

    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret,
        )
        resp = client.create_tweet(text=text[:4000])
        if resp and resp.data:
            logger.info(f"[x] Tweet posted: {resp.data.get('id', '')}")
            return True
        else:
            logger.warning("[x] Tweet post returned no data")
            return False
    except ImportError:
        logger.warning("[x] tweepy not installed — skipping X post")
        return False
    except Exception as e:
        logger.error(f"[x] Post error: {e}")
        return False


V20_SHEET_HEADERS = [
    "Date", "Run ID", "Ticker", "Direction", "Thesis ID", "Thesis Status",
    "Action Bucket", "Confidence", "Urgency", "Catalyst", "Thesis",
    "Strategy", "Trade Expression", "Strike 1", "Strike 2", "Expiry", "DTE",
    "Entry Price", "Target Price", "Stop Price", "Max Loss", "Max Gain",
    "Risk/Reward", "Kelly Size %", "Adjusted Size %", "Contracts",
    "Slippage %", "Liquidity Score", "Execution Impact",
    "Consensus Tag", "Monitor Status", "Replay Status",
]


def _sync_to_google_sheet(rows: List[Dict], ctx: RunContext) -> bool:
    """
    Sync v20 trades to Google Sheet.

    Uses a dedicated 'v20 Trades' worksheet. Creates it if it doesn't exist.
    Appends new rows with proper v20 column headers.
    Does NOT touch the legacy V3 'Trades' worksheet.
    """
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    creds_b64 = os.environ.get("GOOGLE_SHEETS_CREDS", "")

    if not sheet_id or not creds_b64:
        logger.warning("[sheet] Missing GOOGLE_SHEET_ID or GOOGLE_SHEETS_CREDS")
        return False

    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials

        creds_json = json.loads(base64.b64decode(creds_b64))
        credentials = Credentials.from_service_account_info(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(sheet_id)

        # Use dedicated v20 worksheet (create if not exists)
        ws_name = "v20 Trades"
        try:
            ws = spreadsheet.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=ws_name, rows=500, cols=len(V20_SHEET_HEADERS))
            logger.info(f"[sheet] Created '{ws_name}' worksheet")

        # Ensure header row exists
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(V20_SHEET_HEADERS)

        # Append each trade row (aligned to V20_SHEET_HEADERS)
        for row in rows:
            row["market_date"] = ctx.market_date or ""
            values = list(row.values())
            ws.append_row(values)

        logger.info(f"[sheet] Synced {len(rows)} rows to '{ws_name}' worksheet")
        return True

    except ImportError as e:
        logger.warning(f"[sheet] Missing dependency: {e}")
        return False
    except Exception as e:
        logger.error(f"[sheet] Sync error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────
# Public API — orchestrated publish
# ─────────────────────────────────────────────────────────────────────

def publish_trades(trades: List[StructuredTrade], ideas: List[IdeaCandidate],
                   ctx: RunContext, trace: Dict,
                   execution_metas: Optional[Dict[str, Dict]] = None) -> Dict:
    """
    Publish trades through all enabled channels.

    Flow:
      1. Generate executive reports (LLM-powered, institutional quality)
      2. Telegram: send full executive reports (1,800-2,500 chars each)
      3. X: send full executive reports (up to 4,000 chars, filtered)
      4. Google Sheet: structured trade rows

    If executive report generation fails for a trade, falls back to
    the short format so alerts still go out.

    Returns summary dict with counts of what was published where.
    """
    # ── Budget mode hard guard — publishing cannot leak even if flags are wrong ──
    if BUDGET_MODE:
        logger.info("[publisher] BUDGET MODE — all publishing disabled, skipping")
        return {
            "telegram_sent": 0, "telegram_failed": 0,
            "x_posted": 0, "x_filtered": 0, "x_failed": 0,
            "sheet_rows": 0, "sheet_synced": False,
            "reports_generated": 0, "report_cost_usd": 0.0,
            "budget_mode": True,
        }
    summary = {
        "telegram_sent": 0,
        "telegram_failed": 0,
        "x_posted": 0,
        "x_filtered": 0,
        "x_failed": 0,
        "sheet_rows": 0,
        "sheet_synced": False,
        "reports_generated": 0,
        "report_cost_usd": 0.0,
    }

    if ctx.dry_run:
        logger.info("[publisher] DRY RUN — skipping all publishing")
        return summary

    idea_map = {i.idea_id: i for i in ideas}
    exec_metas = execution_metas or {}

    # ── Step 1: Generate executive reports (LLM-powered) ──
    reports = []
    report_map = {}  # ticker → report dict

    if trades and (FLAGS.publish_telegram or FLAGS.publish_x):
        logger.info(f"[publisher] Generating executive reports for {len(trades)} trade(s)...")
        reports = generate_executive_reports(trades, ideas)
        summary["reports_generated"] = len(reports)
        summary["report_cost_usd"] = sum(r.get("cost", 0) for r in reports)

        # Index by ticker for lookup
        for r in reports:
            report_map[r["ticker"]] = r

        # Track cost
        ctx.api_cost_usd += summary["report_cost_usd"]

    # ── Step 2: Telegram — Research + Alert only; middle states → summary/sheet ──
    if FLAGS.publish_telegram:
        for trade in trades:
            idea = idea_map.get(trade.idea_id, IdeaCandidate())
            report = report_map.get(trade.ticker)

            # Returns None for middle states (watchlist/downgrade/hold)
            msg = format_telegram_message(trade, idea, report=report)

            if msg is None:
                # Middle state — no standalone Telegram report.
                # Will appear in daily summary + sheet only.
                logger.info(f"[telegram] {trade.ticker} is watchlist/monitor — skipping standalone message")
                continue

            if _send_telegram(msg):
                summary["telegram_sent"] += 1
            else:
                summary["telegram_failed"] += 1

        # Daily run summary (sectioned: Actionable / Watchlist / Invalidated)
        run_summary = format_daily_summary(ctx, trades, ideas, trace)
        _send_telegram(run_summary)

    # ── Step 3: X — conviction only; middle states + kills blocked ──
    if FLAGS.publish_x:
        from orca_v20.telegram_format import _resolve_framing, _surface_for_framing

        for trade in trades:
            idea = idea_map.get(trade.idea_id, IdeaCandidate())

            # Policy: only "research" surface trades get X posts.
            # Middle states (watchlist/downgrade/hold) → no X.
            # Kills/alerts → no X (already blocked by action filter).
            framing = _resolve_framing(trade, idea)
            surface = _surface_for_framing(framing)
            if surface != "research":
                logger.info(f"[x] {trade.ticker} skipped — surface={surface}, no X post for non-actionable")
                summary["x_filtered"] += 1
                continue

            meta = exec_metas.get(trade.ticker, {})
            passes, reasons = _passes_x_filter(trade, idea, meta)

            if passes:
                report = report_map.get(trade.ticker)
                if report:
                    # Full executive report for X (up to 4,000 chars)
                    post = format_x_report(report)
                else:
                    # Fallback: short format
                    post = format_x_post(trade, idea)

                if post and _send_x_post(post):
                    summary["x_posted"] += 1
                else:
                    summary["x_failed"] += 1
            else:
                logger.info(f"[x] {trade.ticker} filtered: {', '.join(reasons)}")
                summary["x_filtered"] += 1

    # ── Step 4: Google Sheet — full structured set ──
    if FLAGS.mirror_to_google_sheet:
        rows = []
        for trade in trades:
            idea = idea_map.get(trade.idea_id, IdeaCandidate())
            meta = exec_metas.get(trade.ticker, {})
            rows.append(build_sheet_row(trade, idea, meta))

        if rows:
            if _sync_to_google_sheet(rows, ctx):
                summary["sheet_synced"] = True
                summary["sheet_rows"] = len(rows)

    logger.info(
        f"[publisher] Done — Reports: {summary['reports_generated']} (${summary['report_cost_usd']:.4f}), "
        f"Telegram: {summary['telegram_sent']} sent, "
        f"X: {summary['x_posted']} posted / {summary['x_filtered']} filtered, "
        f"Sheet: {summary['sheet_rows']} rows"
    )
    return summary


def publish_telegram_alert(text: str, ctx: RunContext) -> bool:
    """Send a standalone Telegram alert (KILL, soft-stop, replay summary, etc.)."""
    if ctx.dry_run or not FLAGS.publish_telegram:
        return False
    return _send_telegram(text)


def publish_replay_summary(replay_results: List[Dict], budget_summary: Optional[Dict],
                            ctx: RunContext) -> bool:
    """Send nightly replay summary to Telegram."""
    msg = format_replay_summary(replay_results, budget_summary, ctx=ctx)
    return publish_telegram_alert(msg, ctx)
