"""
ORCA v20 — Central configuration.

All tunables, model routing, thresholds, and feature flags live here.
No config is scattered across modules.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────
# Model routing
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ModelSpec:
    """Specification for a single LLM endpoint."""
    provider: str          # "anthropic", "openai", "google"
    model_id: str          # e.g. "claude-opus-4-6", "gpt-5.4-thinking"
    max_tokens: int = 16384
    thinking_budget: int = 0        # 0 = no extended thinking
    temperature: float = 1.0        # required 1.0 for extended thinking
    timeout_s: int = 300
    cost_per_1k_input: float = 0.0  # for budget tracking
    cost_per_1k_output: float = 0.0


# --- Model registry ---

MODELS: Dict[str, ModelSpec] = {
    # Anthropic
    "claude-opus": ModelSpec(
        provider="anthropic",
        model_id="claude-opus-4-6",
        max_tokens=16384,
        thinking_budget=10000,
        temperature=1.0,
        cost_per_1k_input=0.015,
        cost_per_1k_output=0.075,
    ),
    "claude-sonnet": ModelSpec(
        provider="anthropic",
        model_id="claude-sonnet-4-6",
        max_tokens=8192,
        thinking_budget=0,
        temperature=0.7,
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
    ),

    # OpenAI
    "gpt-thinking": ModelSpec(
        provider="openai",
        model_id="gpt-5.4-thinking",
        max_tokens=16384,
        temperature=1.0,
        cost_per_1k_input=0.005,
        cost_per_1k_output=0.015,
    ),
    "gpt-fast": ModelSpec(
        provider="openai",
        model_id="gpt-5.4",
        max_tokens=8192,
        temperature=0.5,
        cost_per_1k_input=0.002,
        cost_per_1k_output=0.006,
    ),

    # Google
    "gemini-pro": ModelSpec(
        provider="google",
        model_id="gemini-3.1-pro-preview",
        max_tokens=8192,
        temperature=0.7,
        cost_per_1k_input=0.001,
        cost_per_1k_output=0.004,
    ),

    # ── Budget-tier models (sprint mode) ──────────────────────────────
    "claude-haiku": ModelSpec(
        provider="anthropic",
        model_id="claude-haiku-4-5-20251001",
        max_tokens=8192,
        thinking_budget=0,
        temperature=0.7,
        timeout_s=120,
        cost_per_1k_input=0.0008,
        cost_per_1k_output=0.004,
    ),
    "gpt-mini": ModelSpec(
        provider="openai",
        model_id="gpt-4o-mini",
        max_tokens=8192,
        temperature=0.5,
        timeout_s=120,
        cost_per_1k_input=0.00015,
        cost_per_1k_output=0.0006,
    ),
    "gemini-flash": ModelSpec(
        provider="google",
        model_id="gemini-2.0-flash",
        max_tokens=8192,
        temperature=0.7,
        timeout_s=120,
        cost_per_1k_input=0.0001,
        cost_per_1k_output=0.0004,
    ),
}


# --- Role-to-model mapping ---
# Each pipeline role can be independently routed to any registered model.

@dataclass
class RoleRouting:
    """
    Maps pipeline roles to model keys from MODELS registry.

    Authoritative routing principle (Phase 4):
        Role A / CIO integrator       → gemini-pro   (synthesis, integration)
        Role B / thesis gen + breaker  → claude-opus  (deep reasoning, adversarial)
        Role C / final judge           → gpt-thinking (independent judgment)
        Support / guided agents        → claude-sonnet (cost-efficient reasoning)
        Extraction / tagging helpers   → gpt-fast     (cheap, fast)

    Quality-first on thesis formation and judgment.
    Cost-first on extraction and utilities.
    """

    # Stage 1: Catalyst hunting — 3 independent models (Role B primary)
    hunter_primary: str = "claude-opus"      # Role B: thesis generator
    hunter_secondary: str = "gpt-thinking"   # Role C: independent judge
    hunter_tertiary: str = "gemini-pro"      # Role A: integration view

    # Stage 2: Flow interpretation — deep reading (Role B)
    flow_reader: str = "claude-opus"

    # Stage 3: Catalyst confirmation — deep reasoning (Role B)
    catalyst_confirm: str = "claude-opus"

    # Evidence gate — utility/extraction (cost-first)
    evidence_gate: str = "claude-sonnet"

    # Thesis generation — alpha-bearing (Role B, quality-first)
    thesis_generator: str = "claude-opus"

    # Elite agent simulation — support agents (cost-efficient)
    elite_simulation: str = "claude-sonnet"

    # Consensus merge — integration (Role A)
    consensus_merge: str = "gemini-pro"

    # Validation passes — utility (cost-first)
    validator_a: str = "gpt-fast"
    validator_b: str = "gemini-pro"

    # Review / QA — quality (Role B)
    review_pass: str = "claude-opus"
    surgical_fix: str = "claude-opus"

    # Reporting — utility (cost-first)
    report_writer: str = "claude-sonnet"
    x_rewriter: str = "claude-sonnet"

    # EOD review — multi-model (quality-first)
    eod_primary: str = "claude-opus"         # Role B
    eod_secondary: str = "gpt-fast"          # utility
    eod_tertiary: str = "gemini-pro"         # Role A

    # Replay engine — analysis (cost-efficient)
    replay_analyst: str = "claude-sonnet"

    # CIO briefing — synthesis/integration (Role A)
    cio_briefing: str = "gemini-pro"

    def get_model(self, role: str) -> ModelSpec:
        """Resolve a role name to its ModelSpec."""
        model_key = getattr(self, role, None)
        if model_key is None:
            raise ValueError(f"Unknown role: {role}")
        if model_key not in MODELS:
            raise ValueError(f"Role '{role}' maps to unknown model '{model_key}'")
        return MODELS[model_key]


@dataclass
class BudgetRoleRouting(RoleRouting):
    """
    Budget sprint routing — all cheap models for intraday collection.

    Philosophy:
        Role B (thesis gen, deep reasoning) → claude-haiku (cheapest Anthropic)
        Role C (independent judge)          → gpt-mini
        Role A (integration)                → gemini-flash
        Support/utility                     → gpt-mini or gemini-flash

    Overnight stays on premium models (uses normal RoleRouting).
    """
    hunter_primary: str = "claude-haiku"
    hunter_secondary: str = "gpt-mini"
    hunter_tertiary: str = "gemini-flash"
    flow_reader: str = "claude-haiku"
    catalyst_confirm: str = "claude-haiku"
    evidence_gate: str = "gpt-mini"
    thesis_generator: str = "claude-haiku"
    elite_simulation: str = "gpt-mini"
    consensus_merge: str = "gemini-flash"
    validator_a: str = "gpt-mini"
    validator_b: str = "gemini-flash"
    review_pass: str = "claude-haiku"
    surgical_fix: str = "claude-haiku"
    report_writer: str = "gpt-mini"
    x_rewriter: str = "gpt-mini"
    eod_primary: str = "claude-haiku"
    eod_secondary: str = "gpt-mini"
    eod_tertiary: str = "gemini-flash"
    replay_analyst: str = "gpt-mini"
    cio_briefing: str = "gemini-flash"


# ── Manual mode detection ──
# When True, all LLM calls are replaced with file-based prompt/response handoff.
# Set via env var ORCA_MANUAL_MODE=1 or programmatically before import.
MANUAL_MODE: bool = os.environ.get("ORCA_MANUAL_MODE", "0").lower() in ("1", "true", "yes")

# ── Budget mode detection ──
BUDGET_MODE: bool = os.environ.get("ORCA_BUDGET_MODE", "0").lower() in ("1", "true", "yes")

if BUDGET_MODE:
    ROUTING = BudgetRoleRouting()
else:
    ROUTING = RoleRouting()


# ─────────────────────────────────────────────────────────────────────
# Pipeline thresholds
# ─────────────────────────────────────────────────────────────────────

@dataclass
class Thresholds:
    """All numeric thresholds in one place."""

    # Confidence gates
    min_confidence: int = 7
    overflow_confidence: int = 9       # required for overflow slots

    # IV/HV thresholds (from legacy)
    iv_rank_sell: float = 0.65
    iv_rank_buy: float = 0.25
    ivhv_overpriced: float = 1.20
    ivhv_underpriced: float = 0.80

    # Position limits
    max_positions: int = 10
    overflow_slots: int = 5
    hard_cap: int = 15
    max_dte: int = 45

    # Evidence gate (new in v20)
    min_evidence_sources: int = 2      # at least 2 independent source types
    min_evidence_freshness_hours: int = 72  # evidence must be < 72h old
    evidence_gate_pass_score: float = 0.6

    # Thesis matching (new in v20)
    thesis_match_threshold: float = 0.80   # cosine similarity for "same thesis"
    thesis_stale_days: int = 30            # auto-close after 30 days inactive

    # Quant gate (new in v20)
    min_analog_count: int = 3
    min_analog_win_rate: float = 0.55
    max_correlation_to_spy: float = 0.85   # reject if too correlated

    # Sizing (new in v20)
    kelly_fraction: float = 0.25           # quarter-Kelly
    max_single_position_pct: float = 0.10  # 10% of portfolio
    min_liquidity_oi: int = 500            # min open interest

    # Daemon rules (new in v20)
    max_drawdown_pct: float = 0.15         # 15% portfolio drawdown = halt
    max_consecutive_losses: int = 5
    correlation_kill_threshold: float = 0.90

    # Elite simulation cost control (Phase 4)
    elite_simulation_max_ideas: int = 3    # max ideas entering full 15-agent sim
    elite_shortlist_min_confidence: int = 5  # confidence floor for simulation entry

    # Cost
    max_api_cost_per_run: float = 20.0
    max_api_cost_per_month: float = 150.0

    # ── Overnight learning budget (Operator Activation) ──
    overnight_hard_budget_usd: float = 50.0     # HARD ceiling per night
    overnight_soft_budget_usd: float = 0.0      # 0 = disabled during warm-up
    overnight_warmup_days: int = 14             # days in LEARNING_ACCELERATION mode
    overnight_replay_lookback_days: int = 14    # replay theses closed in last N days
    overnight_false_negative_lookback: int = 7  # days to check rejected ideas

    # ── Auto-label thresholds (thesis outcome labeling) ──
    thesis_auto_win_pct: float = 0.15          # +15% directional move → CLOSED_WIN
    thesis_auto_loss_pct: float = -0.10        # -10% directional move → CLOSED_LOSS
    thesis_auto_downgrade_pct: float = -0.05   # -5% move → confidence downgrade by 2

    # ── Horizon-aware outcome tracking (all values in TRADING days) ──
    horizon_days_map: Dict[str, int] = field(default_factory=lambda: {
        "INTRADAY": 1, "1D": 1, "3D": 3, "5D": 5,
        "7_10D": 8, "2_4W": 20, "UNKNOWN": 10,
    })
    # Per-horizon auto-label thresholds: {horizon: (win_pct, loss_pct)}
    horizon_auto_thresholds: Dict[str, tuple] = field(default_factory=lambda: {
        "INTRADAY": (0.05, -0.03), "1D": (0.05, -0.04),
        "3D": (0.08, -0.06), "5D": (0.10, -0.08),
        "7_10D": (0.12, -0.10), "2_4W": (0.15, -0.10),
        "UNKNOWN": (0.15, -0.10),
    })
    forward_outcome_windows: List[int] = field(default_factory=lambda: [1, 3, 5, 10, 20])
    horizon_grace_multiplier: float = 0.5  # don't judge until trading_age >= horizon_days * this
    horizon_expiry_multiplier: float = 2.0  # auto-expire thesis after trading_age > horizon * this

    # ── X publishing filter thresholds ──
    x_min_confidence: int = 8                   # only post if confidence >= 8
    x_require_actionable: bool = True           # must be ACTIONABLE bucket
    x_block_capacity_constrained: bool = True   # block capacity_constrained
    x_block_illiquid: bool = True               # block illiquid
    x_block_contradicted: bool = True           # block if active contradiction

    # ── Budget sprint shortlist caps ──
    # Used in budget mode to cap expensive stages and keep cost low.
    # In normal mode these are effectively unlimited (high defaults).
    budget_max_stage2_candidates: int = 100     # no cap in normal mode
    budget_max_stage3_candidates: int = 100     # no cap in normal mode
    budget_max_structured: int = 100            # no cap in normal mode


THRESHOLDS = Thresholds()

# ── Budget mode threshold overrides ──
# Intentional: keep expensive stage caps TIGHT, log broadly at cheap stages.
# Do NOT blindly lower quality bars — cap volume into expensive stages instead.
if BUDGET_MODE:
    THRESHOLDS.max_api_cost_per_run = 3.0           # $3 max per run (cheap models)
    THRESHOLDS.max_api_cost_per_month = 80.0         # $80/month budget sprint
    THRESHOLDS.min_confidence = 6                    # mildly lower → more data, not junk
    THRESHOLDS.evidence_gate_pass_score = 0.5        # mildly lower → more data through
    THRESHOLDS.elite_simulation_max_ideas = 2        # tight cap on expensive sim
    THRESHOLDS.budget_max_stage2_candidates = 4      # tight: only 4 enter flow read
    THRESHOLDS.budget_max_stage3_candidates = 3      # tight: only 3 enter confirmation
    THRESHOLDS.budget_max_structured = 2             # tight: max 2 structured trades
    THRESHOLDS.overnight_hard_budget_usd = 10.0      # $10/night budget (still premium models)


# ─────────────────────────────────────────────────────────────────────
# Feature flags (toggle v20 modules on/off)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class FeatureFlags:
    """
    Feature toggles for gradual v20 rollout.
    Start with everything OFF, enable one-by-one as modules are built.
    """
    # Phase 2 — adapters wrap legacy
    use_v20_pipeline: bool = True          # master switch

    # Legacy mirroring — OFF by default, enable explicitly when ready
    mirror_to_v3_trade_log: bool = False   # call trade_logger.log_trades()

    # ── Operator Activation: publishing channels ──
    # A6: Defaults OFF for safety. Enable via env vars or config for live runs.
    # Live workflow sets: ORCA_PUBLISH_REPORTS=1, ORCA_PUBLISH_TELEGRAM=1, etc.
    publish_reports: bool = True           # master switch for report generation (LLM executive reports)
    publish_telegram: bool = False         # send Telegram alerts
    publish_x: bool = False               # send X (Twitter) posts (filtered)
    mirror_to_google_sheet: bool = False   # sync trades to Google Sheet

    # Phase 3 — new modules (ON = implemented and active)
    enable_evidence_gate: bool = True
    enable_thesis_persistence: bool = True
    enable_thesis_momentum: bool = True
    enable_replay_engine: bool = True            # Phase 5: activated
    enable_institutional_pressure: bool = True   # Phase 3B: yfinance public data
    enable_memory_retrieval: bool = True
    enable_elite_simulation: bool = True         # Phase 3B: router LLM calls (sonnet)
    enable_quant_gate: bool = True               # Phase 3B: yfinance market data
    enable_causal_gate: bool = True              # heuristic fallback works offline
    enable_factor_gate: bool = True              # Phase 3B: yfinance market data
    enable_sizing: bool = True
    enable_execution_impact: bool = True         # Phase 3B: yfinance option chains
    enable_daemon_rules: bool = True

    # Source adapters (source layer upgrade)
    enable_google_trends: bool = True         # Tier 2: attention/acceleration
    enable_nws: bool = True                   # Tier 1: official weather alerts
    enable_eia: bool = True                   # Tier 1: official energy data (EIA_API_KEY required)
    enable_gdelt: bool = True                 # Tier 2: broad news discovery
    enable_x_whitelist: bool = True           # Tier 3: optional domain-expert enrichment

    # Marine / Shipping source adapters
    enable_noaa_ports: bool = True            # Tier 1: NOAA CO-OPS port conditions (public, no auth)
    enable_aisstream: bool = True             # Tier 2: AIS vessel tracking (AISSTREAM_API_KEY required)
    enable_navcen: bool = False               # Stub: NAIS requires USCG auth, disabled by default

    # Phase 4 — stubs (future)
    enable_topological_engine: bool = False
    enable_world_model: bool = False
    enable_telemetry: bool = False
    enable_nowcast: bool = False


FLAGS = FeatureFlags()

# ── Env-var overrides for publish flags (live workflows set these) ──
def _apply_env_overrides() -> None:
    """Allow env vars to opt-in to publishing for live runs."""
    _bool_env = lambda key: os.environ.get(key, "").lower() in ("1", "true", "yes")
    if _bool_env("ORCA_PUBLISH_REPORTS"):
        FLAGS.publish_reports = True
    if _bool_env("ORCA_PUBLISH_TELEGRAM"):
        FLAGS.publish_telegram = True
    if _bool_env("ORCA_PUBLISH_X"):
        FLAGS.publish_x = True
    if _bool_env("ORCA_MIRROR_SHEET"):
        FLAGS.mirror_to_google_sheet = True

_apply_env_overrides()

# ── Budget mode FORCE-DISABLE publishing (overrides everything above) ──
# This runs AFTER env-var overrides so budget mode always wins.
# Even if someone sets ORCA_PUBLISH_TELEGRAM=1 in a budget workflow, it stays off.
if BUDGET_MODE:
    FLAGS.publish_reports = False
    FLAGS.publish_telegram = False
    FLAGS.publish_x = False
    FLAGS.mirror_to_google_sheet = False
    FLAGS.enable_replay_engine = False   # replay runs ONLY in overnight, not intraday


# ─────────────────────────────────────────────────────────────────────
# Source adapter config
# ─────────────────────────────────────────────────────────────────────

@dataclass
class SourceConfig:
    """Configuration for source adapters."""
    # Google Trends
    google_trends_rss_url: str = "https://trends.google.com/trending/rss?geo=US"
    google_trends_poll_minutes: int = 60

    # EIA — key from environment only (EIA_API_KEY)
    eia_api_key_env: str = "EIA_API_KEY"  # env var name, NOT the key itself

    # X whitelist
    x_ingest_enabled: bool = True          # separate from x_publish_enabled
    x_whitelist_handles: Dict = field(default_factory=lambda: {
        "weather": ["ryanhallyall", "RyanMaue", "WeatherProf"],
        "oil_energy": ["OilStockTrader", "DB_WTI", "tradingcrudeoil"],
    })

    # GDELT
    gdelt_default_queries: list = field(default_factory=lambda: [
        "oil price surge",
        "natural gas supply disruption",
        "hurricane energy infrastructure",
        "sanctions energy",
        "airline fuel costs",
        "shipping disruption",
        "commodity supply shock",
    ])

    # NWS
    nws_regions_of_interest: list = field(default_factory=lambda: [
        "TX", "LA", "OK", "ND", "CA", "FL",
    ])

    # ── Marine / Shipping ──
    aisstream_api_key_env: str = "AISSTREAM_API_KEY"  # env var name, NOT the key itself
    aisstream_snapshot_duration_sec: int = 60          # seconds to collect AIS positions

    marine_bounding_boxes: Dict = field(default_factory=lambda: {
        "hormuz": [[24.5, 55.5], [27.5, 57.0]],
        "persian_gulf": [[24.0, 49.0], [30.0, 55.0]],
        "houston_galveston": [[28.8, -95.5], [29.9, -94.0]],
        "corpus_christi": [[27.4, -97.3], [27.9, -96.8]],
        "calcasieu_cameron": [[29.6, -93.5], [29.9, -93.2]],
        "sabine_pass": [[29.6, -93.8], [29.9, -93.5]],
    })

    noaa_ports_stations: Dict = field(default_factory=lambda: {
        "houston_galveston": [
            {"id": "8771341", "name": "Galveston Bay Entrance"},
            {"id": "8770475", "name": "Port Arthur"},
        ],
        "corpus_christi": [
            {"id": "8775241", "name": "Aransas Pass"},
        ],
        "louisiana": [
            {"id": "8768094", "name": "Calcasieu Pass"},
            {"id": "8764227", "name": "LAWMA Pilottown"},
        ],
    })

    noaa_ports_products: list = field(default_factory=lambda: [
        "water_level", "wind", "air_pressure",
    ])


SOURCES = SourceConfig()


# ─────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────

@dataclass
class Paths:
    """All filesystem paths."""
    project_root: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @property
    def v20_db(self) -> str:
        return os.path.join(self.project_root, "orca_v20.db")

    @property
    def v3_trade_db(self) -> str:
        """READ ONLY — never write to this from v20."""
        return os.path.join(self.project_root, "orca_v3_trades.db")

    @property
    def iv_history_db(self) -> str:
        """READ ONLY — shared IV data."""
        return os.path.join(self.project_root, "orca_iv_history.db")

    @property
    def orca_results_dir(self) -> str:
        return os.path.join(self.project_root, "orca_results")

    @property
    def prompts_v20_dir(self) -> str:
        return os.path.join(self.project_root, "prompts", "v20")

    @property
    def regime_json(self) -> str:
        return os.path.join(self.project_root, "regime_prediction.json")

    @property
    def vix_json(self) -> str:
        return os.path.join(self.project_root, "vix_dislocation.json")

    @property
    def state_dir(self) -> str:
        """Persistent state directory for budget sprint (synced via state branch)."""
        return os.path.join(self.project_root, "state")

    @property
    def artifacts_dir(self) -> str:
        """Per-run artifacts for budget sprint (compact summaries, JSONL logs)."""
        return os.path.join(self.project_root, "artifacts")


PATHS = Paths()


# ─────────────────────────────────────────────────────────────────────
# Scheduling (Operator Activation)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ScheduleConfig:
    """Cron-ready scheduling defaults (America/Chicago)."""
    timezone: str = "America/Chicago"

    # Main pipeline: Mon-Fri 9:30 AM CT
    main_cron: str = "30 9 * * 1-5"
    main_command: str = "PYTHONIOENCODING=utf-8 python pipeline_v20.py --verbose"

    # Nightly learning: every night 8:00 PM CT
    nightly_cron: str = "0 20 * * *"
    nightly_command: str = "PYTHONIOENCODING=utf-8 python overnight_v20.py --verbose"

    # Weekly deep review: Sunday 6:00 PM CT
    weekly_cron: str = "0 18 * * 0"
    weekly_command: str = "PYTHONIOENCODING=utf-8 python overnight_v20.py --deep-review --verbose"


SCHEDULE = ScheduleConfig()
