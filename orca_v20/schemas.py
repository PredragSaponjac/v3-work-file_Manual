"""
ORCA v20 — Canonical typed schemas.

Every data object flowing through the v20 pipeline is defined here.
Adapters normalize legacy v3 dicts into these objects.

CRITICAL DESIGN RULE:
    idea_direction  = the thesis direction ("BULLISH" / "BEARISH")
    trade_expression_type = the options structure ("SELL_PUT_SPREAD", "BUY_CALL", etc.)
    These are NEVER conflated. Legacy v3 overwrites direction with strategy —
    v20 keeps them permanently separate.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────

class IdeaDirection(str, Enum):
    """Thesis direction — what we believe the underlying will do."""
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"      # range-bound / vol-play


class TradeExpressionType(str, Enum):
    """How the thesis is expressed in options."""
    BUY_CALL = "BUY_CALL"
    BUY_PUT = "BUY_PUT"
    SELL_CALL = "SELL_CALL"
    SELL_PUT = "SELL_PUT"
    BUY_CALL_SPREAD = "BUY_CALL_SPREAD"
    BUY_PUT_SPREAD = "BUY_PUT_SPREAD"
    SELL_CALL_SPREAD = "SELL_CALL_SPREAD"
    SELL_PUT_SPREAD = "SELL_PUT_SPREAD"
    BUY_STRADDLE = "BUY_STRADDLE"
    SELL_STRADDLE = "SELL_STRADDLE"
    BUY_STRANGLE = "BUY_STRANGLE"
    SELL_STRANGLE = "SELL_STRANGLE"
    IRON_CONDOR = "IRON_CONDOR"
    IRON_BUTTERFLY = "IRON_BUTTERFLY"
    CALENDAR_SPREAD = "CALENDAR_SPREAD"
    DIAGONAL_SPREAD = "DIAGONAL_SPREAD"
    CUSTOM = "CUSTOM"


class CatalystStatus(str, Enum):
    """Lifecycle status of a catalyst."""
    PENDING = "PENDING"          # catalyst hasn't fired yet
    DEVELOPING = "DEVELOPING"    # in progress, partial reaction
    CONFIRMED = "CONFIRMED"      # catalyst confirmed, trade active
    INVALIDATED = "INVALIDATED"  # catalyst failed / disproven
    EXPIRED = "EXPIRED"          # window passed without resolution


class ThesisStatus(str, Enum):
    """Lifecycle status of a persistent thesis."""
    DRAFT = "DRAFT"              # just generated, not yet gated
    ACTIVE = "ACTIVE"            # passed all gates, open for trading
    PAUSED = "PAUSED"            # temporarily suspended (daemon rule)
    CLOSED_WIN = "CLOSED_WIN"
    CLOSED_LOSS = "CLOSED_LOSS"
    CLOSED_EXPIRED = "CLOSED_EXPIRED"
    CLOSED_INVALIDATED = "CLOSED_INVALIDATED"


class ConsensusTag(str, Enum):
    """Multi-model consensus label."""
    UNANIMOUS = "UNANIMOUS"      # all 3 models agree
    MAJORITY = "MAJORITY"        # 2 of 3 agree
    SPLIT = "SPLIT"              # all disagree
    SINGLE = "SINGLE"            # only 1 model proposed


class CatalystAction(str, Enum):
    """Output of catalyst confirmation stage."""
    CONFIRM = "CONFIRM"
    KILL = "KILL"
    DOWNGRADE = "DOWNGRADE"
    HOLD = "HOLD"


class GateStatus(str, Enum):
    """Explicit gate verdict — no silent passes on missing evidence."""
    PASS = "PASS"                          # clear evidence supports passing
    PASS_LOW_CONFIDENCE = "PASS_LOW_CONFIDENCE"  # passed but data was thin
    UNPROVEN = "UNPROVEN"                  # insufficient data to decide
    FAIL = "FAIL"                          # clear evidence of failure


class ThesisHorizon(str, Enum):
    """Expected repricing window for a thesis."""
    INTRADAY = "INTRADAY"
    ONE_DAY = "1D"
    THREE_DAY = "3D"
    FIVE_DAY = "5D"
    SEVEN_TO_TEN_DAY = "7_10D"
    TWO_TO_FOUR_WEEK = "2_4W"
    UNKNOWN = "UNKNOWN"


class TimingQuality(str, Enum):
    """How well thesis timing matched actual outcome."""
    CORRECT_AND_TIMELY = "correct_and_timely"
    CORRECT_BUT_SLOW = "correct_but_slow"
    CORRECT_THESIS_POOR_TIMING = "correct_thesis_poor_timing"
    TOO_EARLY_TO_JUDGE = "too_early_to_judge"
    INVALIDATED_BEFORE_PLAYOUT = "invalidated_before_playout"
    FAILED_THESIS = "failed_thesis"


class HorizonOutcomeLabel(str, Enum):
    """Thesis lifecycle state with horizon awareness."""
    TOO_EARLY = "too_early_to_judge"
    ON_TRACK = "on_track"
    LATE_BUT_INTACT = "late_but_intact"
    WORKED = "worked"
    FAILED = "failed"
    INVALIDATED = "invalidated"


class EvidenceType(str, Enum):
    """Classification of an evidence item."""
    NEWS_ARTICLE = "NEWS_ARTICLE"
    SEC_FILING = "SEC_FILING"
    EARNINGS_REPORT = "EARNINGS_REPORT"
    OPTIONS_FLOW = "OPTIONS_FLOW"
    INSIDER_TRADE = "INSIDER_TRADE"
    ANALYST_RATING = "ANALYST_RATING"
    MACRO_EVENT = "MACRO_EVENT"
    TECHNICAL_SIGNAL = "TECHNICAL_SIGNAL"
    PREDICTION_MARKET = "PREDICTION_MARKET"
    WEATHER_EVENT = "WEATHER_EVENT"
    SOCIAL_SENTIMENT = "SOCIAL_SENTIMENT"
    OTHER = "OTHER"


# ─────────────────────────────────────────────────────────────────────
# Evidence
# ─────────────────────────────────────────────────────────────────────

@dataclass
class EvidenceItem:
    """A single piece of evidence supporting or contradicting a thesis."""
    evidence_id: str = ""              # unique ID
    evidence_type: EvidenceType = EvidenceType.OTHER
    source: str = ""                   # "reuters", "unusual_whales", "kalshi", etc.
    headline: str = ""
    summary: str = ""
    url: Optional[str] = None
    published_utc: Optional[str] = None
    relevance_score: float = 0.0       # 0.0 - 1.0
    sentiment: Optional[str] = None    # "positive", "negative", "neutral"
    ticker: Optional[str] = None
    raw_data: Optional[Dict] = None


@dataclass
class EvidencePack:
    """Collection of evidence for a single idea, scored and filtered."""
    ticker: str = ""
    items: List[EvidenceItem] = field(default_factory=list)
    total_sources: int = 0
    freshest_item_age_hours: float = 0.0
    aggregate_sentiment: Optional[str] = None
    gate_score: float = 0.0            # computed by evidence_gate
    gate_passed: bool = False


# ─────────────────────────────────────────────────────────────────────
# Ideas (Stage 1 output, pre-thesis)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class IdeaCandidate:
    """
    Normalized output from Stage 1 catalyst hunting.
    This is the v20 canonical form — adapters convert v3 dicts into this.
    """
    # --- identity ---
    idea_id: str = ""                  # assigned by adapter
    run_id: str = ""                   # from RunContext
    ticker: str = ""
    company: str = ""

    # --- thesis direction (NEVER overwritten by structuring) ---
    idea_direction: IdeaDirection = IdeaDirection.BULLISH

    # --- catalyst ---
    catalyst: str = ""
    catalyst_status: CatalystStatus = CatalystStatus.PENDING
    repricing_window: str = ""         # "1-3 days", "1-2 weeks", etc.
    expected_horizon: ThesisHorizon = ThesisHorizon.UNKNOWN

    # --- confidence ---
    confidence: int = 0                # 1-10 (parsed numeric)
    confidence_raw: str = ""           # original v3 string for audit ("Medium — ...")
    urgency: int = 0                   # 1-10 (parsed numeric)
    urgency_raw: str = ""              # original v3 string for audit

    # --- thesis content ---
    thesis: str = ""
    evidence: List[str] = field(default_factory=list)
    invalidation: str = ""
    second_order: str = ""
    crowding_risk: str = ""

    # --- multi-model consensus ---
    model_sources: List[str] = field(default_factory=list)  # ["claude", "gpt", "gemini"]
    consensus_tag: ConsensusTag = ConsensusTag.SINGLE

    # --- thesis matching (populated by thesis_store) ---
    thesis_id: Optional[str] = None
    matched_existing_thesis_id: Optional[str] = None
    match_confidence: float = 0.0
    thesis_status: ThesisStatus = ThesisStatus.DRAFT

    # --- flow data (populated by flow_adapter) ---
    tape_read: Optional[str] = None
    flow_details: Optional[Dict] = None

    # --- catalyst confirmation (populated by catalyst_adapter) ---
    catalyst_action: Optional[CatalystAction] = None
    cds_score: Optional[float] = None
    catalyst_health: Optional[Dict] = None

    # --- evidence pack (populated by evidence_gate) ---
    evidence_pack: Optional[EvidencePack] = None

    # --- red-team gate (populated by red_team_gate) ---
    red_team_risk_score: float = 0.0
    red_team_fatal_flaws: List[str] = field(default_factory=list)
    red_team_warnings: List[str] = field(default_factory=list)
    red_team_counter_thesis: str = ""
    survived_red_team: bool = True

    # --- thesis overlap (populated by daemon_rules.check_thesis_overlap) ---
    overlap_warnings: List[str] = field(default_factory=list)
    overlap_score: float = 0.0
    overlap_cluster_count: int = 0
    concentration_confidence: str = ""  # "high" | "reduced"

    # --- GPT two-stage metadata (populated by catalyst_hunter) ---
    gpt_decision: str = ""           # "ACCEPT" | "WATCHLIST" | "REJECT" | ""
    gpt_bull_score: float = 0.0
    gpt_bear_score: float = 0.0

    # --- raw legacy dict (for debugging / passthrough) ---
    _v3_raw: Optional[Dict] = None


# ─────────────────────────────────────────────────────────────────────
# Trade structuring (Stage 4+ output)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class StructuredTrade:
    """
    A fully structured trade ready for execution logging.
    trade_expression_type is SEPARATE from idea_direction.
    """
    # --- links back to idea ---
    idea_id: str = ""
    thesis_id: str = ""
    run_id: str = ""
    ticker: str = ""

    # --- direction vs. expression (THE KEY SEPARATION) ---
    idea_direction: IdeaDirection = IdeaDirection.BULLISH
    trade_expression_type: TradeExpressionType = TradeExpressionType.BUY_CALL

    # --- structure ---
    strategy_label: str = ""           # human-readable: "Bull Put Spread"
    strike_1: Optional[float] = None
    strike_2: Optional[float] = None
    expiry: Optional[str] = None       # YYYY-MM-DD
    dte: Optional[int] = None

    # --- pricing ---
    entry_price: Optional[float] = None
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    max_loss: Optional[float] = None
    max_gain: Optional[float] = None
    risk_reward: Optional[float] = None

    # --- greeks / vol ---
    iv_at_entry: Optional[float] = None
    iv_hv_ratio: Optional[float] = None
    delta: Optional[float] = None
    theta: Optional[float] = None

    # --- sizing (populated by sizing module) ---
    kelly_size_pct: Optional[float] = None
    adjusted_size_pct: Optional[float] = None  # after liquidity/impact
    contracts: Optional[int] = None

    # --- execution impact (populated by execution_impact module) ---
    estimated_slippage_pct: Optional[float] = None
    liquidity_score: Optional[float] = None

    # --- confidence / consensus ---
    confidence: int = 0
    confidence_raw: str = ""           # original v3 string for audit
    urgency: int = 0
    urgency_raw: str = ""              # original v3 string for audit
    consensus_tag: ConsensusTag = ConsensusTag.SINGLE
    expected_horizon: str = ""         # ThesisHorizon value string

    # --- report framing ---
    report_framing: Optional[str] = None
    report_label: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────
# Thesis persistence
# ─────────────────────────────────────────────────────────────────────

@dataclass
class Thesis:
    """
    A persistent thesis that lives across multiple pipeline runs.
    Tracks belief evolution, momentum, and lifecycle.
    """
    thesis_id: str = ""
    ticker: str = ""
    idea_direction: IdeaDirection = IdeaDirection.BULLISH
    catalyst: str = ""
    expected_horizon: str = ""       # ThesisHorizon value string
    thesis_text: str = ""
    status: ThesisStatus = ThesisStatus.DRAFT

    # --- creation ---
    created_run_id: str = ""
    created_utc: str = ""

    # --- last update ---
    last_updated_run_id: str = ""
    last_updated_utc: str = ""

    # --- belief tracking ---
    initial_confidence: int = 0
    current_confidence: int = 0
    confidence_slope: float = 0.0      # from momentum engine
    times_seen: int = 1                # how many runs reaffirmed this

    # --- linked trades ---
    trade_ids: List[str] = field(default_factory=list)

    # --- invalidation ---
    invalidation_trigger: str = ""
    invalidated_reason: Optional[str] = None


@dataclass
class ThesisDailySnapshot:
    """Daily snapshot for momentum calculation."""
    thesis_id: str = ""
    snapshot_date: str = ""            # YYYY-MM-DD
    run_id: str = ""
    confidence: int = 0
    underlying_price: Optional[float] = None
    iv_level: Optional[float] = None
    catalyst_status: CatalystStatus = CatalystStatus.PENDING
    notes: str = ""


@dataclass
class ForwardOutcome:
    """Multi-window forward return tracking for a single thesis."""
    thesis_id: str = ""
    eval_date: str = ""              # YYYY-MM-DD
    window_days: int = 0             # 1, 3, 5, 10, 20
    forward_return_pct: float = 0.0  # directional return %
    mfe_pct: float = 0.0            # max favorable excursion %
    mae_pct: float = 0.0            # max adverse excursion %
    thesis_age_days: int = 0
    expected_horizon: str = ""       # ThesisHorizon value
    horizon_outcome_label: str = ""  # HorizonOutcomeLabel value
    timing_quality: str = ""         # TimingQuality value
    catalyst_intact: bool = True


# ─────────────────────────────────────────────────────────────────────
# Simulation
# ─────────────────────────────────────────────────────────────────────

@dataclass
class EliteAgentVote:
    """Vote from a single elite agent in the simulation."""
    agent_id: str = ""
    agent_persona: str = ""            # "vol_arb_specialist", "macro_strategist", etc.
    thesis_id: str = ""
    vote: str = ""                     # "STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"
    confidence: float = 0.0
    reasoning: str = ""
    dissent_flag: bool = False


@dataclass
class SimulationVerdict:
    """Aggregated verdict from elite simulation + crowd."""
    thesis_id: str = ""
    elite_votes: List[EliteAgentVote] = field(default_factory=list)
    elite_consensus: str = ""          # majority vote
    elite_confidence: float = 0.0      # weighted average
    crowd_sentiment: float = 0.0       # DeGroot equilibrium
    dissent_ratio: float = 0.0         # fraction of dissenters
    final_verdict: str = ""            # "PROCEED", "DOWNSIZE", "REJECT"


# ─────────────────────────────────────────────────────────────────────
# Run trace (for auditing)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class RunTrace:
    """Complete trace of a single pipeline run for audit/replay."""
    run_id: str = ""
    started_utc: str = ""
    completed_utc: Optional[str] = None
    market_date: str = ""
    research_mode: str = ""
    source_mode: str = ""

    # --- stage results ---
    ideas_generated: int = 0
    ideas_after_flow: int = 0
    ideas_after_confirmation: int = 0
    ideas_after_gates: int = 0
    trades_structured: int = 0
    trades_logged: int = 0

    # --- cost ---
    total_api_cost_usd: float = 0.0

    # --- errors ---
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # --- flags ---
    dry_run: bool = False
    success: bool = False
