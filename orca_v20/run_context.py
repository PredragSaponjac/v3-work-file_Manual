"""
RunContext — shared context object threaded through every v20 stage.

Created once at pipeline start, passed by reference to every adapter,
gate, engine, and persistence call.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ResearchMode(str, Enum):
    """How deep the pipeline digs for evidence."""
    FAST = "fast"          # minimal scraping, cached data only
    STANDARD = "standard"  # normal news + flow + regime
    DEEP = "deep"          # extended search, extra LLM passes


class SourceMode(str, Enum):
    """Which data sources are active."""
    FULL = "full"          # all sources (UW, news, Kalshi, regime, etc.)
    NO_UW = "no_uw"        # skip Unusual Whales (API down / cost savings)
    MINIMAL = "minimal"    # scanner + news only, no paid APIs


@dataclass
class RunContext:
    """
    Immutable-ish context for a single pipeline run.

    Created once at the top of pipeline_v20.py, then threaded through
    every function call. No global state — everything lives here.
    """

    # --- identity ---
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    as_of_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # --- temporal context ---
    market_date: Optional[str] = None        # YYYY-MM-DD trading date
    day_of_week: Optional[str] = None        # "Monday", "Tuesday", ...
    days_to_opex: Optional[int] = None       # days until monthly options expiry
    is_fomc_week: bool = False
    is_opex_week: bool = False
    is_earnings_season: bool = False

    # --- mode switches ---
    research_mode: ResearchMode = ResearchMode.STANDARD
    source_mode: SourceMode = SourceMode.FULL

    # --- regime (populated after regime_runner) ---
    spy_regime: Optional[str] = None         # "risk-on", "risk-off", "neutral"
    regime_conviction: Optional[float] = None
    vix_level: Optional[float] = None

    # --- budget / limits ---
    max_positions: int = 10
    overflow_slots: int = 5                  # 9+ confidence only
    hard_cap: int = 15
    max_api_cost_usd: float = 20.0           # per-run cost ceiling

    # --- tracking ---
    stages_completed: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    api_cost_usd: float = 0.0

    # --- source results (populated by source orchestrator) ---
    source_results: Optional[dict] = None    # full source_results dict from orchestrator

    # --- portfolio ---
    portfolio_value: Optional[float] = None  # resolved at startup from env/config/default

    # --- flags ---
    dry_run: bool = False                    # if True, skip all writes
    verbose: bool = False

    def mark_stage(self, stage_name: str) -> None:
        """Record that a stage completed successfully."""
        self.stages_completed.append({
            "stage": stage_name,
            "completed_utc": datetime.now(timezone.utc).isoformat(),
        })

    def add_error(self, stage: str, error: str) -> None:
        """Record a non-fatal error."""
        self.errors.append({
            "stage": stage,
            "error": error,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        })

    def add_cost(self, amount_usd: float) -> None:
        """Track cumulative API spend."""
        self.api_cost_usd += amount_usd

    def budget_remaining(self) -> float:
        return max(0.0, self.max_api_cost_usd - self.api_cost_usd)

    def is_over_budget(self) -> bool:
        return self.api_cost_usd >= self.max_api_cost_usd

    @property
    def temporal_context(self) -> dict:
        """Structured temporal snapshot for prompt injection."""
        return {
            "market_date": self.market_date,
            "day_of_week": self.day_of_week,
            "days_to_opex": self.days_to_opex,
            "is_fomc_week": self.is_fomc_week,
            "is_opex_week": self.is_opex_week,
            "is_earnings_season": self.is_earnings_season,
            "spy_regime": self.spy_regime,
            "regime_conviction": self.regime_conviction,
            "vix_level": self.vix_level,
        }

    def summary(self) -> dict:
        """JSON-safe run summary for logging."""
        return {
            "run_id": self.run_id,
            "as_of_utc": self.as_of_utc.isoformat(),
            "market_date": self.market_date,
            "research_mode": self.research_mode.value,
            "source_mode": self.source_mode.value,
            "stages_completed": [s["stage"] for s in self.stages_completed],
            "errors_count": len(self.errors),
            "api_cost_usd": round(self.api_cost_usd, 4),
            "dry_run": self.dry_run,
        }
