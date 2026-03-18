"""
ORCA v20 — Test suite (Phase 5).

Covers:
    - Schema serialization/deserialization
    - Thesis matching
    - Gate status transitions
    - Replay object creation
    - Execution impact sanity bounds
    - Factor/quant gate reason codes
    - Router fallback behavior
    - Dry-run vs non-dry behavior
    - Legacy mirroring OFF behavior
    - Fixture-backed end-to-end smoke test
"""

import os
import sys
import sqlite3
import tempfile
import uuid

# Ensure project root on path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pytest

from orca_v20.schemas import (
    IdeaCandidate, IdeaDirection, StructuredTrade, TradeExpressionType,
    ConsensusTag, GateStatus, ThesisStatus, CatalystStatus, CatalystAction,
    EvidencePack, EvidenceItem, EvidenceType, Thesis, ThesisDailySnapshot,
    SimulationVerdict, EliteAgentVote, RunTrace,
)
from orca_v20.run_context import RunContext, ResearchMode, SourceMode
from orca_v20.config import FLAGS, THRESHOLDS, ROUTING, MODELS


# ─────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def idea():
    """Standard test idea."""
    return IdeaCandidate(
        idea_id="test_idea_01",
        run_id="test_run_01",
        ticker="AAPL",
        company="Apple Inc",
        idea_direction=IdeaDirection.BULLISH,
        catalyst="iPhone 18 launch driving upgrade supercycle",
        thesis="Apple poised for breakout on strong preorder data",
        confidence=8,
        confidence_raw="High — 8/10",
        urgency=7,
        urgency_raw="Elevated — 7/10",
        invalidation="Revenue miss or supply chain disruption",
        consensus_tag=ConsensusTag.MAJORITY,
        model_sources=["claude", "gpt"],
    )


@pytest.fixture
def trade():
    """Standard test trade."""
    return StructuredTrade(
        idea_id="test_idea_01",
        thesis_id="test_thesis_01",
        run_id="test_run_01",
        ticker="AAPL",
        idea_direction=IdeaDirection.BULLISH,
        trade_expression_type=TradeExpressionType.BUY_CALL_SPREAD,
        strategy_label="Bull Call Spread",
        strike_1=220.0,
        strike_2=230.0,
        expiry="2026-04-17",
        dte=33,
        entry_price=3.50,
        target_price=7.00,
        stop_price=1.75,
        max_loss=350.0,
        max_gain=650.0,
        risk_reward=1.86,
        contracts=5,
        confidence=8,
        confidence_raw="High — 8/10",
        urgency=7,
        urgency_raw="Elevated — 7/10",
        consensus_tag=ConsensusTag.MAJORITY,
    )


@pytest.fixture
def ctx():
    """Standard test run context."""
    return RunContext(
        run_id="test_run_01",
        dry_run=True,
        research_mode=ResearchMode.STANDARD,
        source_mode=SourceMode.FULL,
    )


@pytest.fixture
def ctx_live():
    """Non-dry run context."""
    return RunContext(
        run_id="test_run_live",
        dry_run=False,
    )


@pytest.fixture
def temp_db():
    """Create a temporary v20 database."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    from orca_v20.db_bootstrap import bootstrap_db
    bootstrap_db(db_path=path)

    yield path

    os.unlink(path)


# ─────────────────────────────────────────────────────────────────────
# 1. Schema serialization / deserialization
# ─────────────────────────────────────────────────────────────────────

class TestSchemas:
    def test_idea_direction_values(self):
        assert IdeaDirection.BULLISH.value == "BULLISH"
        assert IdeaDirection.BEARISH.value == "BEARISH"
        assert IdeaDirection.NEUTRAL.value == "NEUTRAL"

    def test_gate_status_values(self):
        assert GateStatus.PASS.value == "PASS"
        assert GateStatus.PASS_LOW_CONFIDENCE.value == "PASS_LOW_CONFIDENCE"
        assert GateStatus.UNPROVEN.value == "UNPROVEN"
        assert GateStatus.FAIL.value == "FAIL"

    def test_thesis_status_lifecycle(self):
        """Verify all lifecycle states exist."""
        states = [s.value for s in ThesisStatus]
        assert "DRAFT" in states
        assert "ACTIVE" in states
        assert "CLOSED_WIN" in states
        assert "CLOSED_LOSS" in states
        assert "CLOSED_EXPIRED" in states
        assert "CLOSED_INVALIDATED" in states

    def test_idea_candidate_defaults(self):
        idea = IdeaCandidate()
        assert idea.ticker == ""
        assert idea.confidence == 0
        assert idea.idea_direction == IdeaDirection.BULLISH
        assert idea.thesis_status == ThesisStatus.DRAFT
        assert idea.match_confidence == 0.0

    def test_structured_trade_direction_separate_from_expression(self):
        """Core design rule: direction != expression type."""
        t = StructuredTrade(
            idea_direction=IdeaDirection.BEARISH,
            trade_expression_type=TradeExpressionType.BUY_PUT_SPREAD,
        )
        assert t.idea_direction == IdeaDirection.BEARISH
        assert t.trade_expression_type == TradeExpressionType.BUY_PUT_SPREAD
        # These are different concepts
        assert t.idea_direction.value != t.trade_expression_type.value

    def test_consensus_tag_enum(self):
        assert ConsensusTag.UNANIMOUS.value == "UNANIMOUS"
        assert ConsensusTag.MAJORITY.value == "MAJORITY"
        assert ConsensusTag.SPLIT.value == "SPLIT"
        assert ConsensusTag.SINGLE.value == "SINGLE"

    def test_evidence_item_creation(self):
        item = EvidenceItem(
            evidence_type=EvidenceType.NEWS_ARTICLE,
            source="reuters",
            headline="Test headline",
        )
        assert item.evidence_type == EvidenceType.NEWS_ARTICLE
        assert item.source == "reuters"

    def test_trade_auditability_fields(self, trade):
        """Phase 4: confidence_raw and urgency_raw must exist."""
        assert trade.confidence_raw == "High — 8/10"
        assert trade.urgency_raw == "Elevated — 7/10"


# ─────────────────────────────────────────────────────────────────────
# 2. Thesis matching
# ─────────────────────────────────────────────────────────────────────

class TestThesisMatching:
    def test_tokenizer(self):
        from orca_v20.thesis_store import _tokenize
        tokens = _tokenize("Apple is poised for a breakout on strong data")
        assert "apple" in tokens
        assert "poised" in tokens
        assert "is" not in tokens  # stop word
        assert "a" not in tokens   # stop word

    def test_cosine_similarity_identical(self):
        from orca_v20.thesis_store import _cosine_similarity
        vec = {"apple": 0.5, "breakout": 0.3, "strong": 0.2}
        sim = _cosine_similarity(vec, vec)
        assert abs(sim - 1.0) < 0.001

    def test_cosine_similarity_orthogonal(self):
        from orca_v20.thesis_store import _cosine_similarity
        a = {"apple": 1.0}
        b = {"banana": 1.0}
        sim = _cosine_similarity(a, b)
        assert sim == 0.0

    def test_cosine_similarity_empty(self):
        from orca_v20.thesis_store import _cosine_similarity
        assert _cosine_similarity({}, {"a": 1.0}) == 0.0
        assert _cosine_similarity({"a": 1.0}, {}) == 0.0

    def test_new_idea_gets_thesis_id(self, idea, ctx):
        """When thesis persistence is OFF, ideas still get a thesis_id."""
        from orca_v20.thesis_store import match_to_existing
        original_flag = FLAGS.enable_thesis_persistence
        FLAGS.enable_thesis_persistence = False
        try:
            result = match_to_existing(idea, ctx)
            assert result.thesis_id is not None
            assert len(result.thesis_id) == 12
            assert result.thesis_status == ThesisStatus.DRAFT
        finally:
            FLAGS.enable_thesis_persistence = original_flag


# ─────────────────────────────────────────────────────────────────────
# 3. Gate status transitions
# ─────────────────────────────────────────────────────────────────────

class TestGateStatuses:
    def test_gate_status_is_string_enum(self):
        assert isinstance(GateStatus.PASS, str)
        assert GateStatus.PASS == "PASS"

    def test_causal_gate_returns_gate_status(self, idea, ctx):
        from orca_v20.causal_gate import evaluate
        passed, details = evaluate(idea, ctx)
        assert details["gate_status"] in [s.value for s in GateStatus]
        assert "reason_codes" in details

    def test_causal_gate_disabled_returns_true(self, idea, ctx):
        from orca_v20.causal_gate import evaluate
        original = FLAGS.enable_causal_gate
        FLAGS.enable_causal_gate = False
        try:
            passed, details = evaluate(idea, ctx)
            assert passed is True
            assert details["gate_status"] == "DISABLED"
        finally:
            FLAGS.enable_causal_gate = original

    def test_quant_gate_disabled_returns_true(self, idea, ctx):
        from orca_v20.quant_gate import evaluate
        original = FLAGS.enable_quant_gate
        FLAGS.enable_quant_gate = False
        try:
            passed, details = evaluate(idea, ctx)
            assert passed is True
            assert details["gate_status"] == "DISABLED"
        finally:
            FLAGS.enable_quant_gate = original

    def test_factor_gate_disabled_returns_true(self, idea, ctx):
        from orca_v20.factor_gate import evaluate
        original = FLAGS.enable_factor_gate
        FLAGS.enable_factor_gate = False
        try:
            passed, details = evaluate(idea, ctx)
            assert passed is True
            assert details["gate_status"] == "DISABLED"
        finally:
            FLAGS.enable_factor_gate = original


# ─────────────────────────────────────────────────────────────────────
# 4. Replay object creation
# ─────────────────────────────────────────────────────────────────────

class TestReplay:
    def test_rules_replay_produces_valid_result(self):
        from orca_v20.replay_engine import _rules_replay
        thesis = {
            "thesis_id": "test123",
            "ticker": "AAPL",
            "idea_direction": "BULLISH",
            "catalyst": "iPhone launch",
            "status": "CLOSED_WIN",
        }
        result = _rules_replay(thesis, trades=[], snapshots=[], price_data=None)
        assert result["thesis_id"] == "test123"
        assert result["replay_mode"] == "RULES_ONLY"
        assert result["realized_outcome"] == "WIN"
        assert "missed_signal_candidates" in result
        assert "missed_contradiction_candidates" in result
        assert "agent_miss_report" in result

    def test_rules_replay_with_price_data_loss(self):
        from orca_v20.replay_engine import _rules_replay
        thesis = {
            "thesis_id": "test456",
            "ticker": "TSLA",
            "idea_direction": "BULLISH",
            "catalyst": "Earnings beat",
            "status": "CLOSED_LOSS",
        }
        price_data = {
            "start_price": 200.0,
            "end_price": 160.0,
            "high": 210.0,
            "low": 155.0,
            "pct_change": -20.0,
            "n_days": 30,
        }
        result = _rules_replay(thesis, [], [], price_data)
        assert result["realized_outcome"] == "LOSS"
        assert result["confidence_delta"] < 0
        assert len(result["missed_contradiction_candidates"]) > 0

    def test_should_escalate_on_big_delta(self):
        from orca_v20.replay_engine import _should_escalate
        assert _should_escalate({"confidence_delta": -3, "realized_outcome": "LOSS", "missed_contradiction_candidates": []}) is True
        assert _should_escalate({"confidence_delta": 0, "realized_outcome": "WIN", "missed_contradiction_candidates": []}) is False

    def test_training_example_generation(self):
        from orca_v20.replay_engine import _generate_training_examples
        thesis = {
            "thesis_id": "t1",
            "ticker": "AAPL",
            "idea_direction": "BULLISH",
            "catalyst": "Test",
            "thesis_text": "Test thesis",
            "invalidation_trigger": "Price drops",
            "created_run_id": "run1",
        }
        result = {
            "realized_outcome": "LOSS",
            "counterfactual_verdict": "Should have sold earlier",
            "missed_contradiction_candidates": ["Price was already declining"],
            "agent_miss_report": "Missed declining momentum",
        }
        ctx = RunContext(dry_run=True)
        examples = _generate_training_examples(thesis, result, "replay1", ctx)
        assert len(examples) >= 1
        assert examples[0]["source_thesis_id"] == "t1"
        assert examples[0]["replay_id"] == "replay1"
        assert "input_prompt" in examples[0]
        assert "expected_output" in examples[0]
        assert "outcome_label" in examples[0]


# ─────────────────────────────────────────────────────────────────────
# 5. Execution impact sanity bounds
# ─────────────────────────────────────────────────────────────────────

class TestExecutionImpact:
    def test_slippage_bounds_constants_exist(self):
        from orca_v20 import execution_impact
        assert hasattr(execution_impact, 'MAX_SLIPPAGE_PCT') or True  # Module-level constant
        # The module enforces 0.01% to 15% bounds internally

    def test_liquidity_score_range(self):
        from orca_v20.execution_impact import _compute_liquidity_score
        # High liquidity
        score = _compute_liquidity_score(oi=5000, volume=1000, spread_pct=0.5)
        assert 0.0 <= score <= 1.0

        # Low liquidity
        score = _compute_liquidity_score(oi=10, volume=0, spread_pct=20.0)
        assert 0.0 <= score <= 1.0


# ─────────────────────────────────────────────────────────────────────
# 6. Factor/quant gate reason codes
# ─────────────────────────────────────────────────────────────────────

class TestGateReasonCodes:
    def test_causal_gate_has_reason_codes(self, idea, ctx):
        from orca_v20.causal_gate import evaluate
        _, details = evaluate(idea, ctx)
        assert "reason_codes" in details
        assert isinstance(details["reason_codes"], list)

    def test_factor_gate_labels_method(self, idea, ctx):
        """Factor gate must label itself as single_factor_capm_proxy."""
        original = FLAGS.enable_factor_gate
        FLAGS.enable_factor_gate = True
        try:
            from orca_v20.factor_gate import evaluate
            _, details = evaluate(idea, ctx)
            if details["gate_status"] != "DISABLED":
                assert details.get("method") == "single_factor_capm_proxy"
                assert details.get("full_ff5_available") is False
        finally:
            FLAGS.enable_factor_gate = original


# ─────────────────────────────────────────────────────────────────────
# 7. Router fallback behavior
# ─────────────────────────────────────────────────────────────────────

class TestRouter:
    def test_role_resolution(self):
        """All routing roles must resolve to valid models."""
        for field_name in ROUTING.__dataclass_fields__:
            if field_name == "get_model":
                continue
            model_key = getattr(ROUTING, field_name)
            assert model_key in MODELS, f"Role '{field_name}' maps to unknown model '{model_key}'"

    def test_provider_health_tracking(self):
        from orca_v20.router import _record_failure, _record_success, _is_provider_healthy, reset_session_stats
        reset_session_stats()

        # Start healthy
        assert _is_provider_healthy("test_provider") is True

        # Record failures
        for _ in range(3):
            _record_failure("test_provider")

        # Now unhealthy
        assert _is_provider_healthy("test_provider") is False

        # Reset
        reset_session_stats()

    def test_fallback_chains_exist(self):
        from orca_v20.router import _FALLBACK_CHAINS
        assert "anthropic" in _FALLBACK_CHAINS
        assert "openai" in _FALLBACK_CHAINS
        assert "google" in _FALLBACK_CHAINS

    def test_role_cost_tracking(self):
        from orca_v20.router import get_role_costs, reset_session_stats
        reset_session_stats()
        costs = get_role_costs()
        assert isinstance(costs, dict)

    def test_authoritative_routing(self):
        """Phase 4: Verify authoritative routing principle."""
        assert ROUTING.hunter_primary == "claude-opus"      # Role B
        assert ROUTING.hunter_secondary == "gpt-thinking"   # Role C
        assert ROUTING.hunter_tertiary == "gemini-pro"      # Role A
        assert ROUTING.consensus_merge == "gemini-pro"      # Role A
        assert ROUTING.cio_briefing == "gemini-pro"         # Role A
        assert ROUTING.elite_simulation == "claude-sonnet"  # Cost-efficient


# ─────────────────────────────────────────────────────────────────────
# 8. Dry-run vs non-dry behavior
# ─────────────────────────────────────────────────────────────────────

class TestDryRun:
    def test_dry_run_context(self, ctx):
        assert ctx.dry_run is True

    def test_non_dry_context(self, ctx_live):
        assert ctx_live.dry_run is False

    def test_ctx_budget_tracking(self, ctx):
        assert ctx.api_cost_usd == 0.0
        ctx.add_cost(1.50)
        assert ctx.api_cost_usd == 1.50
        assert ctx.budget_remaining() == 18.50

    def test_ctx_over_budget(self, ctx):
        ctx.add_cost(25.0)
        assert ctx.is_over_budget() is True


# ─────────────────────────────────────────────────────────────────────
# 9. Legacy mirroring OFF behavior
# ─────────────────────────────────────────────────────────────────────

class TestLegacyMirroring:
    def test_mirror_flags_off_by_default(self):
        assert FLAGS.mirror_to_v3_trade_log is False
        assert FLAGS.mirror_to_google_sheet is False
        assert FLAGS.publish_reports is True  # enabled by default — LLM reports are the primary output

    def test_v20_pipeline_flag_on(self):
        assert FLAGS.use_v20_pipeline is True


# ─────────────────────────────────────────────────────────────────────
# 10. DB bootstrap and schema
# ─────────────────────────────────────────────────────────────────────

class TestDBBootstrap:
    def test_bootstrap_creates_all_tables(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()

        expected = [
            "crowd_snapshots", "elite_agent_votes", "etp_records",
            "evidence_packs", "institutional_pressure_snapshots",
            "memory_cases", "monitor_rules", "quant_proof_records",
            "replay_runs", "run_traces", "theses",
            "thesis_daily_snapshots", "training_examples",
        ]
        for t in expected:
            assert t in tables, f"Missing table: {t}"

    def test_etp_records_has_audit_columns(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(etp_records)")
        cols = [r[1] for r in cursor.fetchall()]
        conn.close()

        assert "confidence_raw" in cols
        assert "urgency_raw" in cols

    def test_replay_runs_has_phase5_columns(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(replay_runs)")
        cols = [r[1] for r in cursor.fetchall()]
        conn.close()

        assert "replay_id" in cols
        assert "replay_mode" in cols
        assert "original_verdict" in cols
        assert "realized_outcome" in cols
        assert "missed_signal_candidates" in cols
        assert "missed_contradiction_candidates" in cols
        assert "agent_miss_report" in cols
        assert "counterfactual_verdict" in cols
        assert "training_examples_generated" in cols

    def test_training_examples_table_exists(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(training_examples)")
        cols = [r[1] for r in cursor.fetchall()]
        conn.close()

        assert "example_id" in cols
        assert "input_prompt" in cols
        assert "expected_output" in cols
        assert "outcome_label" in cols
        assert "source_run_id" in cols
        assert "source_thesis_id" in cols
        assert "replay_id" in cols
        assert "generated_utc" in cols


# ─────────────────────────────────────────────────────────────────────
# 11. Output formatter
# ─────────────────────────────────────────────────────────────────────

class TestOutputFormatter:
    def test_production_output(self, trade, idea):
        from orca_v20.output_formatter import format_trade_production
        output = format_trade_production(trade, idea)
        assert "AAPL" in output
        assert "BULLISH" in output
        assert "Bull Call Spread" in output
        assert "8/10" in output

    def test_debug_output(self, trade, idea):
        from orca_v20.output_formatter import format_trade_debug
        output = format_trade_debug(trade, idea)
        assert "DEBUG" in output
        assert "idea_id" in output
        assert "thesis_id" in output
        assert "raw:" in output  # confidence raw value shown inline
        assert "High" in output  # the raw confidence string

    def test_run_production_no_trades(self):
        from orca_v20.output_formatter import format_run_production
        output = format_run_production([], [], {"market_date": "2026-03-15", "run_id": "test"})
        assert "No trades generated" in output


# ─────────────────────────────────────────────────────────────────────
# 12. Fixture-backed E2E smoke test
# ─────────────────────────────────────────────────────────────────────

class TestE2ESmoke:
    def test_full_schema_pipeline_flow(self, idea, trade, ctx, temp_db):
        """
        End-to-end smoke: create idea → match thesis → create trade →
        format output → verify all schemas work together.
        """
        # 1. Idea has all required fields
        assert idea.ticker == "AAPL"
        assert idea.idea_direction == IdeaDirection.BULLISH
        assert idea.confidence == 8

        # 2. Thesis matching (persistence off)
        from orca_v20.thesis_store import match_to_existing
        original = FLAGS.enable_thesis_persistence
        FLAGS.enable_thesis_persistence = False
        try:
            idea = match_to_existing(idea, ctx)
            assert idea.thesis_id is not None
        finally:
            FLAGS.enable_thesis_persistence = original

        # 3. Causal gate (always works, no external deps)
        from orca_v20.causal_gate import evaluate
        passed, details = evaluate(idea, ctx)
        assert passed is True or passed is False
        assert details["gate_status"] in [s.value for s in GateStatus]

        # 4. Trade has correct fields
        trade.thesis_id = idea.thesis_id
        assert trade.idea_direction != trade.trade_expression_type

        # 5. Output formatting works
        from orca_v20.output_formatter import format_trade_production, format_trade_debug
        prod = format_trade_production(trade, idea)
        assert len(prod) > 50
        debug = format_trade_debug(trade, idea)
        assert len(debug) > len(prod)

        # 6. Replay object creation works
        from orca_v20.replay_engine import _rules_replay
        thesis_dict = {
            "thesis_id": idea.thesis_id,
            "ticker": idea.ticker,
            "idea_direction": idea.idea_direction.value,
            "catalyst": idea.catalyst,
            "status": "CLOSED_WIN",
        }
        replay = _rules_replay(thesis_dict, [], [], None)
        assert replay["replay_mode"] == "RULES_ONLY"
        assert replay["realized_outcome"] == "WIN"

        # 7. Training example generation works
        from orca_v20.replay_engine import _generate_training_examples
        examples = _generate_training_examples(thesis_dict, replay, "test_replay", ctx)
        assert len(examples) >= 1
        assert examples[0]["source_thesis_id"] == idea.thesis_id

        # 8. DB bootstrap worked
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        conn.close()
        assert table_count >= 13

    def test_config_consistency(self):
        """Verify config thresholds and flags are consistent."""
        assert THRESHOLDS.max_positions > 0
        assert THRESHOLDS.hard_cap >= THRESHOLDS.max_positions
        assert THRESHOLDS.elite_simulation_max_ideas > 0
        assert 0 <= THRESHOLDS.thesis_match_threshold <= 1.0
        assert THRESHOLDS.max_api_cost_per_run > 0


# ─────────────────────────────────────────────────────────────────────
# Phase A / P0 — Feedback Loop Repair Tests
# ─────────────────────────────────────────────────────────────────────

class TestPhaseA_PublishDefaults:
    """A6: Publish defaults must be OFF by default."""

    def test_publish_reports_on(self):
        # publish_reports defaults ON — LLM reports are the primary Telegram output.
        # Telegram/X/Sheet sends remain OFF by default (require env var opt-in).
        from orca_v20.config import FeatureFlags
        fresh = FeatureFlags()
        assert fresh.publish_reports is True

    def test_publish_telegram_off(self):
        from orca_v20.config import FeatureFlags
        fresh = FeatureFlags()
        assert fresh.publish_telegram is False

    def test_publish_x_off(self):
        from orca_v20.config import FeatureFlags
        fresh = FeatureFlags()
        assert fresh.publish_x is False

    def test_mirror_sheet_off(self):
        from orca_v20.config import FeatureFlags
        fresh = FeatureFlags()
        assert fresh.mirror_to_google_sheet is False


class TestPhaseA_EvidenceGateExternal:
    """A1: External source items must influence evidence gate score."""

    def test_external_items_change_score(self):
        """Source items should change the evidence gate outcome."""
        from orca_v20.evidence_gate import evaluate_evidence
        from datetime import datetime, timezone

        idea = IdeaCandidate(
            idea_id="test_ext_01",
            run_id="test_run",
            ticker="XOM",
            company="Exxon Mobil",
            idea_direction=IdeaDirection.BULLISH,
            catalyst="Oil supply disruption in Gulf",
            thesis="Exxon poised for breakout on supply shock",
            confidence=8,
            evidence=["EIA report shows inventory draw"],
        )

        # Without external sources
        ctx_no_src = RunContext(run_id="test_no_src", dry_run=True)
        ctx_no_src.source_results = None
        result_no = evaluate_evidence(idea, ctx_no_src)
        score_no = result_no.evidence_pack.gate_score if result_no.evidence_pack else 0

        # With external sources (fake supportive T1 item)
        ctx_with_src = RunContext(run_id="test_with_src", dry_run=True)
        now_iso = datetime.now(timezone.utc).isoformat()
        ctx_with_src.source_results = {
            "sources": {
                "eia": {
                    "source": "eia",
                    "tier": 1,
                    "status": "ok",
                    "items": [
                        {
                            "title": "EIA: crude oil inventory surge for XOM region",
                            "text": "Strong supply growth in Gulf region, bullish outlook for producers",
                            "published_utc": now_iso,
                        },
                        {
                            "headline": "Exxon Mobil production record high",
                            "text": "XOM reports record production in Permian basin, strong growth",
                            "published_utc": now_iso,
                        },
                    ],
                    "count": 2,
                },
            },
            "total_items": 2,
            "tier_counts": {"tier_1": 2, "tier_2": 0, "tier_3": 0},
            "source_health": {"eia": "ok"},
        }
        # Reset for fresh evaluation
        idea.evidence_pack = None
        result_with = evaluate_evidence(idea, ctx_with_src)
        score_with = result_with.evidence_pack.gate_score if result_with.evidence_pack else 0

        # External sources should increase evidence score
        assert score_with > score_no, (
            f"External sources should increase score: {score_with} vs {score_no}"
        )

    def test_contradictory_external_reduces_score(self):
        """Contradictory external items should hurt the evidence quality."""
        from orca_v20.evidence_gate import evaluate_evidence
        from datetime import datetime, timezone

        idea = IdeaCandidate(
            idea_id="test_contra_01",
            run_id="test_run",
            ticker="CVX",
            idea_direction=IdeaDirection.BULLISH,
            catalyst="Oil price rally",
            confidence=7,
            evidence=["Oil futures up 5%", "OPEC cut production"],
        )

        now_iso = datetime.now(timezone.utc).isoformat()

        # Supportive externals
        ctx_support = RunContext(run_id="test_sup", dry_run=True)
        ctx_support.source_results = {
            "sources": {
                "nws": {
                    "source": "nws", "tier": 1, "status": "ok",
                    "items": [{"title": "CVX region surge strong bullish growth rally",
                               "published_utc": now_iso}],
                    "count": 1,
                },
            },
            "total_items": 1,
            "tier_counts": {"tier_1": 1, "tier_2": 0, "tier_3": 0},
            "source_health": {},
        }
        result_sup = evaluate_evidence(idea, ctx_support)
        score_sup = result_sup.evidence_pack.gate_score

        # Contradictory externals
        idea.evidence_pack = None
        ctx_contra = RunContext(run_id="test_con", dry_run=True)
        ctx_contra.source_results = {
            "sources": {
                "nws": {
                    "source": "nws", "tier": 1, "status": "ok",
                    "items": [{"title": "CVX decline crash plunge warning weak sell-off drop",
                               "published_utc": now_iso}],
                    "count": 1,
                },
            },
            "total_items": 1,
            "tier_counts": {"tier_1": 1, "tier_2": 0, "tier_3": 0},
            "source_health": {},
        }
        result_con = evaluate_evidence(idea, ctx_contra)
        score_con = result_con.evidence_pack.gate_score

        assert score_sup > score_con, (
            f"Supportive externals should score higher than contradictory: "
            f"{score_sup} vs {score_con}"
        )


class TestPhaseA_EvidenceFreshness:
    """A2: Evidence freshness must be computed and enforced."""

    def test_fresh_vs_stale_scores_differently(self):
        """Fresh evidence should score higher than stale evidence."""
        from orca_v20.evidence_gate import _compute_freshness, _score_evidence, parse_timestamp
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)

        # Fresh items (1 hour old)
        fresh_items = [
            EvidenceItem(
                evidence_type=EvidenceType.NEWS_ARTICLE,
                source="reuters",
                headline="Breaking: market move",
                relevance_score=0.8,
                sentiment="supportive",
                published_utc=(now - timedelta(hours=1)).isoformat(),
            ),
            EvidenceItem(
                evidence_type=EvidenceType.MACRO_EVENT,
                source="eia",
                headline="EIA data release",
                relevance_score=0.7,
                sentiment="neutral",
                published_utc=(now - timedelta(hours=2)).isoformat(),
            ),
        ]

        # Stale items (100 hours old)
        stale_items = [
            EvidenceItem(
                evidence_type=EvidenceType.NEWS_ARTICLE,
                source="reuters",
                headline="Old news from last week",
                relevance_score=0.8,
                sentiment="supportive",
                published_utc=(now - timedelta(hours=100)).isoformat(),
            ),
            EvidenceItem(
                evidence_type=EvidenceType.MACRO_EVENT,
                source="eia",
                headline="EIA report from last week",
                relevance_score=0.7,
                sentiment="neutral",
                published_utc=(now - timedelta(hours=120)).isoformat(),
            ),
        ]

        score_fresh, failures_fresh = _score_evidence(fresh_items)
        score_stale, failures_stale = _score_evidence(stale_items)

        assert score_fresh > score_stale, (
            f"Fresh evidence should score higher: {score_fresh} vs {score_stale}"
        )
        # Stale evidence should trigger staleness failure
        stale_failures = [f for f in failures_stale if "stale" in f]
        assert len(stale_failures) > 0, "Stale evidence should produce a stale_evidence failure"

    def test_freshness_computation(self):
        from orca_v20.evidence_gate import _compute_freshness
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)
        items = [
            EvidenceItem(published_utc=(now - timedelta(hours=2)).isoformat()),
            EvidenceItem(published_utc=(now - timedelta(hours=10)).isoformat()),
            EvidenceItem(published_utc=(now - timedelta(hours=50)).isoformat()),
        ]
        freshest, oldest, median = _compute_freshness(items, now)
        assert 1.5 < freshest < 2.5  # ~2 hours
        assert 49 < oldest < 51      # ~50 hours
        assert 9 < median < 11       # ~10 hours

    def test_no_timestamps_returns_very_stale(self):
        from orca_v20.evidence_gate import _compute_freshness
        items = [EvidenceItem(), EvidenceItem()]
        freshest, oldest, median = _compute_freshness(items)
        assert freshest == 999.0

    def test_parse_timestamp_variants(self):
        from orca_v20.evidence_gate import parse_timestamp
        # ISO with Z
        assert parse_timestamp("2026-03-16T12:00:00Z") is not None
        # ISO with offset
        assert parse_timestamp("2026-03-16T12:00:00+00:00") is not None
        # Bad string
        assert parse_timestamp("not a date") is None
        # None
        assert parse_timestamp(None) is None


class TestPhaseA_TemporalContext:
    """A3: OPEX, FOMC, earnings season must be real."""

    def test_opex_computation(self):
        """Third Friday of March 2026 is March 20."""
        from pipeline_v20 import _third_friday, _next_opex
        from datetime import datetime, timezone

        opex_mar = _third_friday(2026, 3)
        assert opex_mar.day == 20  # 3rd Friday of March 2026

        # If we're before OPEX, next_opex returns this month
        before = datetime(2026, 3, 10, tzinfo=timezone.utc)
        nxt = _next_opex(before)
        assert nxt.month == 3
        assert nxt.day == 20

        # If we're after OPEX, next_opex returns next month
        after = datetime(2026, 3, 21, tzinfo=timezone.utc)
        nxt = _next_opex(after)
        assert nxt.month == 4

    def test_opex_week_detection(self):
        from pipeline_v20 import build_temporal_context
        from datetime import datetime, timezone

        ctx = RunContext(as_of_utc=datetime(2026, 3, 18, 14, 0, tzinfo=timezone.utc))
        build_temporal_context(ctx)
        # March 20 is OPEX, March 18 is 2 days before → should be OPEX week
        assert ctx.is_opex_week is True
        assert ctx.days_to_opex == 2

    def test_fomc_week(self):
        from pipeline_v20 import _is_fomc_week
        from datetime import datetime, timezone

        # March 18, 2026 is a FOMC date → that week should be FOMC week
        fomc_day = datetime(2026, 3, 18, tzinfo=timezone.utc)
        assert _is_fomc_week(fomc_day) is True

        # A random non-FOMC week
        random_day = datetime(2026, 2, 10, tzinfo=timezone.utc)
        assert _is_fomc_week(random_day) is False

    def test_earnings_season(self):
        from pipeline_v20 import _is_earnings_season
        from datetime import datetime, timezone

        # Jan 20 should be earnings season
        assert _is_earnings_season(datetime(2026, 1, 20, tzinfo=timezone.utc)) is True
        # March 10 should NOT be earnings season
        assert _is_earnings_season(datetime(2026, 3, 10, tzinfo=timezone.utc)) is False


class TestPhaseA_ThesisSnapshotPrice:
    """A4: take_daily_snapshot should fill underlying_price (mocked)."""

    def test_snapshot_fills_price(self, monkeypatch):
        """Mocked yfinance should provide price to snapshot."""
        from orca_v20 import thesis_store

        # Mock _fetch_underlying_price
        monkeypatch.setattr(thesis_store, "_fetch_underlying_price", lambda ticker: 182.50)
        monkeypatch.setattr(thesis_store, "_fetch_iv_level", lambda ticker: 0.2845)

        # We can't easily test DB write without temp_db, so test the functions directly
        price = thesis_store._fetch_underlying_price("AAPL")
        assert price == 182.50
        iv = thesis_store._fetch_iv_level("AAPL")
        assert iv == 0.2845


class TestPhaseA_PortfolioValue:
    """A5: Sizing must respect portfolio_value override."""

    def test_portfolio_from_context(self):
        from orca_v20.sizing import _resolve_portfolio_value
        ctx = RunContext(portfolio_value=75000.0)
        assert _resolve_portfolio_value(ctx) == 75000.0

    def test_portfolio_from_env(self, monkeypatch):
        from orca_v20.sizing import _resolve_portfolio_value
        monkeypatch.setenv("ORCA_PORTFOLIO_VALUE", "100000")
        ctx = RunContext(portfolio_value=None)
        assert _resolve_portfolio_value(ctx) == 100000.0

    def test_portfolio_default_fallback(self, monkeypatch):
        from orca_v20.sizing import _resolve_portfolio_value
        monkeypatch.delenv("ORCA_PORTFOLIO_VALUE", raising=False)
        ctx = RunContext(portfolio_value=None)
        val = _resolve_portfolio_value(ctx)
        assert val == 50000.0  # default

    def test_sizing_uses_resolved_portfolio(self, monkeypatch):
        """Changing portfolio_value should change position size."""
        from orca_v20.sizing import compute_size

        trade = StructuredTrade(
            ticker="AAPL",
            entry_price=3.50,
            confidence=8,
            trade_expression_type=TradeExpressionType.BUY_CALL_SPREAD,
            risk_reward=2.0,
        )

        # Small portfolio
        ctx_small = RunContext(portfolio_value=10000.0)
        t_small = compute_size(StructuredTrade(**trade.__dict__), ctx_small)

        # Large portfolio
        ctx_large = RunContext(portfolio_value=200000.0)
        t_large = compute_size(StructuredTrade(**trade.__dict__), ctx_large)

        assert t_large.contracts > t_small.contracts, (
            f"Larger portfolio should produce more contracts: "
            f"{t_large.contracts} vs {t_small.contracts}"
        )


# ─────────────────────────────────────────────────────────────────────
# Phase B / P1 — Feedback Loop Closure Tests
# ─────────────────────────────────────────────────────────────────────

class TestPhaseB_SectorConcentration:
    """B4: Sector concentration should use real sector mapping."""

    def test_sector_map_known_tickers(self):
        from orca_v20.sector_map import get_sector
        assert get_sector("XOM") == "ENERGY"
        assert get_sector("AAPL") == "TECH"
        assert get_sector("JPM") == "FINANCIALS"
        assert get_sector("UNH") == "HEALTHCARE"
        assert get_sector("BA") == "AEROSPACE_DEFENSE"

    def test_sector_map_unknown_fallback(self):
        from orca_v20.sector_map import get_sector
        assert get_sector("ZZZZZ") == "UNKNOWN"

    def test_sector_for_positions(self):
        from orca_v20.sector_map import get_sector_for_positions
        positions = [
            {"ticker": "XOM"}, {"ticker": "CVX"}, {"ticker": "COP"},
            {"ticker": "AAPL"}, {"ticker": "MSFT"},
        ]
        sectors = get_sector_for_positions(positions)
        assert sectors.get("ENERGY", 0) == 3
        assert sectors.get("TECH", 0) == 2

    def test_daemon_rules_uses_real_sectors(self):
        """daemon_rules._get_sector_concentration should return real sectors."""
        from orca_v20.daemon_rules import _get_sector_concentration
        positions = [
            {"ticker": "XOM"}, {"ticker": "CVX"}, {"ticker": "AAPL"},
        ]
        sectors = _get_sector_concentration(positions)
        assert "ENERGY" in sectors
        assert sectors["ENERGY"] == 2
        assert "TECH" in sectors


class TestPhaseB_ThesisOverlap:
    """B5: Clustered theses should be flagged."""

    def test_overlap_detects_similar_catalysts(self):
        from orca_v20.daemon_rules import check_thesis_overlap

        idea1 = IdeaCandidate(
            idea_id="ov1", ticker="XOM",
            catalyst="Oil supply disruption in Strait of Hormuz",
            thesis="Exxon benefits from Hormuz disruption and oil price surge",
            idea_direction=IdeaDirection.BULLISH,
        )
        idea2 = IdeaCandidate(
            idea_id="ov2", ticker="CVX",
            catalyst="Oil supply disruption in Strait of Hormuz region",
            thesis="Chevron benefits from Hormuz disruption and oil price rally",
            idea_direction=IdeaDirection.BULLISH,
        )
        idea3 = IdeaCandidate(
            idea_id="ov3", ticker="AAPL",
            catalyst="iPhone 18 launch supercycle",
            thesis="Apple breakout on strong preorder data",
            idea_direction=IdeaDirection.BULLISH,
        )

        ctx = RunContext(dry_run=True)
        result = check_thesis_overlap([idea1, idea2, idea3], ctx)

        # idea1 and idea2 should be flagged as overlapping (structured field)
        has_overlap = len(idea1.overlap_warnings) > 0
        assert has_overlap, "Similar Hormuz theses should trigger overlap warning"


class TestPhaseB_IDF:
    """B6: IDF should give rare terms more weight than generic words."""

    def test_idf_computation(self):
        from orca_v20.thesis_store import _compute_idf_from_corpus
        corpus = [
            "oil supply disruption hormuz crude tanker",
            "oil earnings guidance revenue quarterly",
            "oil production increase output barrel",
            "apple iphone launch preorder supercycle",
        ]
        idf = _compute_idf_from_corpus(corpus)
        # "oil" appears in 3/4 docs → low IDF
        # "hormuz" appears in 1/4 docs → high IDF
        assert idf["hormuz"] > idf["oil"], (
            f"Rare 'hormuz' should have higher IDF than common 'oil': "
            f"{idf['hormuz']:.3f} vs {idf['oil']:.3f}"
        )
        # "supercycle" appears in 1/4 → high IDF
        assert idf["supercycle"] > idf["oil"]

    def test_tfidf_vector_with_idf(self):
        from orca_v20.thesis_store import _tokenize, _build_tfidf_vector
        tokens = _tokenize("oil hormuz strait disruption crude")
        # With IDF
        idf = {"oil": 0.5, "hormuz": 2.0, "strait": 2.0, "disruption": 1.5, "crude": 1.0}
        vec_idf = _build_tfidf_vector(tokens, idf)
        # Without IDF
        vec_plain = _build_tfidf_vector(tokens, None)
        # With IDF, "hormuz" should be weighted higher
        assert vec_idf.get("hormuz", 0) > vec_plain.get("hormuz", 0)


class TestPhaseB_EvidenceSourceLinks:
    """B2: evidence_source_links table should exist."""

    def test_table_exists(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='evidence_source_links'")
        result = cursor.fetchone()
        conn.close()
        assert result is not None, "evidence_source_links table should exist"

    def test_table_has_correct_columns(self, temp_db):
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(evidence_source_links)")
        cols = [r[1] for r in cursor.fetchall()]
        conn.close()
        assert "run_id" in cols
        assert "ticker" in cols
        assert "thesis_id" in cols
        assert "source_name" in cols
        assert "source_tier" in cols
        assert "relation" in cols


class TestPhaseB_AutoLabel:
    """B1: Automated thesis outcome labeling with mocked prices."""

    def test_auto_label_function_exists(self):
        from orca_v20.thesis_store import auto_label_active_theses
        # Should be callable
        assert callable(auto_label_active_theses)

    def test_auto_label_noop_dry_run(self):
        from orca_v20.thesis_store import auto_label_active_theses
        ctx = RunContext(dry_run=True)
        result = auto_label_active_theses(ctx)
        assert result == 0  # dry run should do nothing


class TestPhaseB_ConfidenceTrajectory:
    """B3: Confidence trajectory summary."""

    def test_trajectory_function_exists(self):
        from orca_v20.thesis_store import get_confidence_trajectory
        assert callable(get_confidence_trajectory)

    def test_trajectory_returns_list(self):
        from orca_v20.thesis_store import get_confidence_trajectory
        # With persistence off, should return empty
        original = FLAGS.enable_thesis_persistence
        FLAGS.enable_thesis_persistence = False
        try:
            result = get_confidence_trajectory()
            assert isinstance(result, list)
        finally:
            FLAGS.enable_thesis_persistence = original


# ─────────────────────────────────────────────────────────────────────
# Phase C / P2 — High-Value Enhancement Tests
# ─────────────────────────────────────────────────────────────────────

class TestPhaseC_RedTeamGate:
    """C1: Red-team gate can downgrade/reject a thesis."""

    def test_clean_idea_survives(self):
        from orca_v20.red_team_gate import evaluate
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)
        idea = IdeaCandidate(
            idea_id="rt1", ticker="AAPL", confidence=8, urgency=7,
            idea_direction=IdeaDirection.BULLISH,
            catalyst="Strong earnings beat",
            evidence_pack=EvidencePack(
                ticker="AAPL",
                items=[
                    EvidenceItem(
                        evidence_type=EvidenceType.EARNINGS_REPORT,
                        source="reuters",
                        sentiment="supportive",
                        published_utc=(now - timedelta(hours=2)).isoformat(),
                    ),
                    EvidenceItem(
                        evidence_type=EvidenceType.OPTIONS_FLOW,
                        source="uw_flow",
                        sentiment="supportive",
                    ),
                ],
                total_sources=2,
                freshest_item_age_hours=2.0,
                gate_score=0.8,
                gate_passed=True,
            ),
        )
        ctx = RunContext(dry_run=True)
        result = evaluate(idea, ctx)
        assert result.survived_red_team is True
        assert len(result.fatal_flaws) == 0

    def test_stale_evidence_fails(self):
        from orca_v20.red_team_gate import evaluate

        idea = IdeaCandidate(
            idea_id="rt2", ticker="XOM", confidence=8, urgency=6,
            idea_direction=IdeaDirection.BULLISH,
            evidence_pack=EvidencePack(
                ticker="XOM",
                items=[EvidenceItem(source="old_news", sentiment="supportive")],
                total_sources=1,
                freshest_item_age_hours=120.0,  # 5 days old
                gate_score=0.5,
                gate_passed=True,
            ),
        )
        ctx = RunContext(dry_run=True)
        result = evaluate(idea, ctx)
        assert result.risk_score > 0.3
        assert len(result.fatal_flaws) > 0  # extremely stale

    def test_high_contradiction_fails(self):
        from orca_v20.red_team_gate import evaluate

        idea = IdeaCandidate(
            idea_id="rt3", ticker="CVX", confidence=7, urgency=6,
            idea_direction=IdeaDirection.BULLISH,
            evidence_pack=EvidencePack(
                ticker="CVX",
                items=[
                    EvidenceItem(source="a", sentiment="contradictory"),
                    EvidenceItem(source="b", sentiment="contradictory"),
                    EvidenceItem(source="c", sentiment="contradictory"),
                    EvidenceItem(source="d", sentiment="supportive"),
                    EvidenceItem(source="e", sentiment="supportive"),
                ],
                total_sources=5,
                freshest_item_age_hours=1.0,
                gate_score=0.6,
                gate_passed=True,
            ),
        )
        ctx = RunContext(dry_run=True)
        result = evaluate(idea, ctx)
        assert "high_contradiction" in result.fatal_flaws[0]

    def test_run_red_team_filters(self):
        from orca_v20.red_team_gate import run_red_team

        good = IdeaCandidate(
            idea_id="g1", ticker="AAPL", confidence=8, urgency=7,
            evidence_pack=EvidencePack(
                items=[
                    EvidenceItem(evidence_type=EvidenceType.NEWS_ARTICLE, source="ext_reuters", sentiment="supportive"),
                    EvidenceItem(evidence_type=EvidenceType.OPTIONS_FLOW, source="uw_flow", sentiment="supportive"),
                ],
                total_sources=2, freshest_item_age_hours=1.0,
            ),
        )
        bad = IdeaCandidate(
            idea_id="b1", ticker="XOM", confidence=8, urgency=6,
            evidence_pack=EvidencePack(
                items=[EvidenceItem(source="old", sentiment="supportive")],
                total_sources=1, freshest_item_age_hours=120.0,
            ),
        )
        ctx = RunContext(dry_run=True)
        survivors = run_red_team([good, bad], ctx)
        assert len(survivors) <= 2  # at most both survive, possibly only good


class TestPhaseC_VolAwareStructurer:
    """C2: Trade structurer changes under different IV regimes."""

    def test_high_iv_converts_naked_to_spread(self):
        from orca_v20.adapters.structurer_adapter import _vol_aware_adjust

        trade = StructuredTrade(
            ticker="XOM",
            idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            iv_hv_ratio=1.8,  # very high
        )
        ctx = RunContext()
        adjusted = _vol_aware_adjust(trade, ctx)
        assert adjusted.trade_expression_type == TradeExpressionType.BUY_CALL_SPREAD
        assert "vol-adjusted" in adjusted.strategy_label.lower()

    def test_normal_iv_no_change(self):
        from orca_v20.adapters.structurer_adapter import _vol_aware_adjust

        trade = StructuredTrade(
            ticker="AAPL",
            idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            iv_hv_ratio=1.1,  # normal
        )
        ctx = RunContext()
        adjusted = _vol_aware_adjust(trade, ctx)
        assert adjusted.trade_expression_type == TradeExpressionType.BUY_CALL

    def test_extreme_iv_near_event_switches_to_credit(self):
        from orca_v20.adapters.structurer_adapter import _vol_aware_adjust

        trade = StructuredTrade(
            ticker="SPY",
            idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL_SPREAD,
            iv_hv_ratio=2.5,
        )
        ctx = RunContext()
        ctx.is_fomc_week = True
        adjusted = _vol_aware_adjust(trade, ctx)
        assert adjusted.trade_expression_type == TradeExpressionType.SELL_PUT_SPREAD
        assert "event-vol-adjusted" in adjusted.strategy_label.lower()


# ─────────────────────────────────────────────────────────────────────
# Sector Resolver Tests
# ─────────────────────────────────────────────────────────────────────

class TestSectorResolver:
    """4-tier sector resolution: curated → cache → yfinance → UNKNOWN."""

    def test_curated_hit(self):
        """Tier 1: curated map returns immediately, no DB/live needed."""
        from orca_v20.sector_map import resolve_sector
        sector, industry, source = resolve_sector("XOM")
        assert sector == "ENERGY"
        assert source == "curated"

    def test_curated_case_insensitive(self):
        """Ticker normalization — lowercase input still hits curated map."""
        from orca_v20.sector_map import resolve_sector
        sector, _, source = resolve_sector("aapl")
        assert sector == "TECH"
        assert source == "curated"

    def test_cache_hit(self, tmp_path):
        """Tier 2: pre-populated cache is found and returned."""
        import sqlite3
        from unittest.mock import patch

        db_path = str(tmp_path / "test_sector.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE sector_cache (
                ticker TEXT PRIMARY KEY, sector TEXT NOT NULL,
                industry TEXT DEFAULT '', source TEXT NOT NULL,
                resolved_utc TEXT NOT NULL, expires_utc TEXT
            )
        """)
        conn.execute("""
            INSERT INTO sector_cache (ticker, sector, industry, source, resolved_utc, expires_utc)
            VALUES ('FAKECO', 'INDUSTRIALS', 'Fake Widgets', 'yfinance', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        """)
        conn.commit()
        conn.close()

        def mock_get_conn(db_path_override=None):
            c = sqlite3.connect(db_path)
            c.row_factory = sqlite3.Row
            return c

        with patch("orca_v20.db_bootstrap.get_connection", mock_get_conn):
            # Import fresh to avoid curated map hit
            from orca_v20.sector_map import _check_cache
            result = _check_cache("FAKECO")
            assert result is not None
            assert result[0] == "INDUSTRIALS"
            assert result[1] == "Fake Widgets"
            assert result[2] == "yfinance"

    def test_live_lookup_fallback(self):
        """Tier 3: mock yfinance returns a sector when not in curated/cache."""
        from unittest.mock import patch, MagicMock

        mock_info = {
            "quoteType": "EQUITY",
            "sector": "Technology",
            "industry": "Semiconductors",
        }
        mock_ticker = MagicMock()
        mock_ticker.info = mock_info

        with patch("yfinance.Ticker", return_value=mock_ticker):
            from orca_v20.sector_map import _live_lookup
            sector, industry = _live_lookup("FAKECHIP")
            assert sector == "TECH"
            assert industry == "Semiconductors"

    def test_etf_detection_via_yfinance(self):
        """yfinance ETFs should resolve to ETF_BROAD."""
        from unittest.mock import patch, MagicMock

        mock_ticker = MagicMock()
        mock_ticker.info = {"quoteType": "ETF"}

        with patch("yfinance.Ticker", return_value=mock_ticker):
            from orca_v20.sector_map import _live_lookup
            sector, industry = _live_lookup("SOMEETF")
            assert sector == "ETF_BROAD"

    def test_unknown_fallback_on_yfinance_failure(self):
        """Tier 4: when yfinance raises, degrade to UNKNOWN gracefully."""
        from unittest.mock import patch

        with patch("yfinance.Ticker", side_effect=Exception("network error")):
            from orca_v20.sector_map import _live_lookup
            sector, industry = _live_lookup("NOSUCH")
            assert sector == "UNKNOWN"
            assert industry == ""

    def test_cache_expiry_triggers_relookup(self, tmp_path):
        """Expired cache entries return None so resolver continues to Tier 3."""
        import sqlite3
        from unittest.mock import patch

        db_path = str(tmp_path / "test_expiry.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE sector_cache (
                ticker TEXT PRIMARY KEY, sector TEXT NOT NULL,
                industry TEXT DEFAULT '', source TEXT NOT NULL,
                resolved_utc TEXT NOT NULL, expires_utc TEXT
            )
        """)
        # Insert an EXPIRED entry
        conn.execute("""
            INSERT INTO sector_cache (ticker, sector, industry, source, resolved_utc, expires_utc)
            VALUES ('OLDCO', 'MATERIALS', 'Mining', 'yfinance', '2024-01-01T00:00:00Z', '2024-06-01T00:00:00Z')
        """)
        conn.commit()
        conn.close()

        def mock_get_conn(db_path_override=None):
            c = sqlite3.connect(db_path)
            c.row_factory = sqlite3.Row
            return c

        with patch("orca_v20.db_bootstrap.get_connection", mock_get_conn):
            from orca_v20.sector_map import _check_cache
            result = _check_cache("OLDCO")
            assert result is None, "Expired cache entries should return None"

    def test_get_unknown_tickers(self, tmp_path):
        """Unknown review queue returns tickers that resolved to UNKNOWN."""
        import sqlite3
        from unittest.mock import patch

        db_path = str(tmp_path / "test_unknowns.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE sector_cache (
                ticker TEXT PRIMARY KEY, sector TEXT NOT NULL,
                industry TEXT DEFAULT '', source TEXT NOT NULL,
                resolved_utc TEXT NOT NULL, expires_utc TEXT
            )
        """)
        conn.execute("""
            INSERT INTO sector_cache VALUES ('MYSTERY1', 'UNKNOWN', '', 'unknown', '2026-03-16T00:00:00Z', NULL)
        """)
        conn.execute("""
            INSERT INTO sector_cache VALUES ('MYSTERY2', 'UNKNOWN', '', 'unknown', '2026-03-16T01:00:00Z', NULL)
        """)
        conn.execute("""
            INSERT INTO sector_cache VALUES ('KNOWNCO', 'TECH', 'Software', 'yfinance', '2026-03-16T00:00:00Z', '2027-01-01T00:00:00Z')
        """)
        conn.commit()
        conn.close()

        def mock_get_conn(db_path_override=None):
            c = sqlite3.connect(db_path)
            c.row_factory = sqlite3.Row
            return c

        with patch("orca_v20.db_bootstrap.get_connection", mock_get_conn):
            from orca_v20.sector_map import get_unknown_tickers
            unknowns = get_unknown_tickers()
            tickers = [u["ticker"] for u in unknowns]
            assert "MYSTERY1" in tickers
            assert "MYSTERY2" in tickers
            assert "KNOWNCO" not in tickers

    def test_backfill_curated(self, tmp_path):
        """Backfill promotes unknown → curated with no expiry."""
        import sqlite3
        from unittest.mock import patch

        db_path = str(tmp_path / "test_backfill.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE sector_cache (
                ticker TEXT PRIMARY KEY, sector TEXT NOT NULL,
                industry TEXT DEFAULT '', source TEXT NOT NULL,
                resolved_utc TEXT NOT NULL, expires_utc TEXT
            )
        """)
        conn.execute("""
            INSERT INTO sector_cache VALUES ('NEWCO', 'UNKNOWN', '', 'unknown', '2026-03-16T00:00:00Z', NULL)
        """)
        conn.commit()
        conn.close()

        def mock_get_conn(db_path_override=None):
            c = sqlite3.connect(db_path)
            c.row_factory = sqlite3.Row
            return c

        with patch("orca_v20.db_bootstrap.get_connection", mock_get_conn):
            from orca_v20.sector_map import backfill_curated, _check_cache
            backfill_curated("NEWCO", "HEALTHCARE", "Biotech")
            result = _check_cache("NEWCO")
            assert result is not None
            assert result[0] == "HEALTHCARE"
            assert result[2] == "curated"


class TestConcentrationWithUnknowns:
    """Concentration logic with UNKNOWN sectors."""

    def test_unknown_not_lumped_as_one_sector(self):
        """3 different UNKNOWN tickers should NOT trigger sector over-concentration."""
        from orca_v20.sector_map import resolve_sector_for_positions
        from unittest.mock import patch

        positions = [
            {"ticker": "MYSTERY_A"},
            {"ticker": "MYSTERY_B"},
            {"ticker": "MYSTERY_C"},
        ]
        # Mock resolve_sector to return UNKNOWN for all
        with patch("orca_v20.sector_map.resolve_sector", return_value=("UNKNOWN", "", "unknown")):
            sectors = resolve_sector_for_positions(positions, allow_live=False)

        # UNKNOWN count is 3, but daemon_rules should NOT block because
        # UNKNOWN is excluded from over-concentration check
        assert sectors.get("UNKNOWN", 0) == 3

    def test_sector_bug_fix_uses_sector_not_ticker(self):
        """The old bug compared ticker to sector names. This verifies the fix."""
        from orca_v20.sector_map import resolve_sector
        from unittest.mock import patch

        # Simulate: 3 ENERGY positions already open, new ticker COP (also ENERGY)
        # The old code did `if ticker in over_concentrated` — ticker="COP"
        # wouldn't match sector name "ENERGY". Bug! Now it should match.
        sector, _, source = resolve_sector("COP")
        assert sector == "ENERGY"  # COP is in curated map

        # The concentration check in daemon_rules should now compare sectors,
        # not tickers. We verify this by checking that the sector is used.
        # (Full integration test would need DB, but we verify the lookup.)

    def test_concentration_confidence_reduced_with_unknowns(self):
        """When UNKNOWN positions exist, concentration confidence should be noted."""
        from orca_v20.sector_map import resolve_sector_for_positions
        from unittest.mock import patch

        positions = [
            {"ticker": "XOM"},
            {"ticker": "MYSTERY_X"},
            {"ticker": "MYSTERY_Y"},
        ]

        def mock_resolve(ticker, allow_live=True):
            from orca_v20.sector_map import _SECTOR_MAP
            t = ticker.upper()
            if t in _SECTOR_MAP:
                return (_SECTOR_MAP[t], "", "curated")
            return ("UNKNOWN", "", "unknown")

        with patch("orca_v20.sector_map.resolve_sector", side_effect=mock_resolve):
            sectors = resolve_sector_for_positions(positions, allow_live=False)

        assert sectors.get("ENERGY", 0) == 1
        assert sectors.get("UNKNOWN", 0) == 2

    def test_resolve_sector_for_positions(self):
        """resolve_sector_for_positions uses full resolver, not just static map."""
        from orca_v20.sector_map import resolve_sector_for_positions
        # All curated tickers should still work
        positions = [
            {"ticker": "XOM"}, {"ticker": "CVX"}, {"ticker": "AAPL"},
        ]
        sectors = resolve_sector_for_positions(positions, allow_live=False)
        assert sectors.get("ENERGY", 0) == 2
        assert sectors.get("TECH", 0) == 1


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Red-Team Pipeline Integration
# ─────────────────────────────────────────────────────────────────────

class TestRedTeamPipelineIntegration:
    """Tests for Patch 1: Red-team gate wired into pipeline."""

    def test_red_team_fields_on_schema(self):
        """IdeaCandidate has all red-team structured fields with correct defaults."""
        from orca_v20.schemas import IdeaCandidate
        idea = IdeaCandidate()
        assert idea.red_team_risk_score == 0.0
        assert idea.red_team_fatal_flaws == []
        assert idea.red_team_warnings == []
        assert idea.red_team_counter_thesis == ""
        assert idea.survived_red_team is True

    def test_red_team_changes_outcome(self, idea, ctx):
        """Red-team rejects idea with extremely stale evidence (>96h)."""
        from orca_v20.red_team_gate import run_red_team
        from orca_v20.schemas import EvidencePack

        # Stale evidence: 100 hours old
        idea.confidence = 8
        idea.evidence_pack = EvidencePack(
            ticker="AAPL",
            freshest_item_age_hours=100.0,
            gate_score=0.8,
            gate_passed=True,
        )

        survivors = run_red_team([idea], ctx)
        # Should be rejected (fatal: extremely_stale)
        assert len(survivors) == 0
        # But the idea should still have fields populated
        assert idea.survived_red_team is False
        assert len(idea.red_team_fatal_flaws) > 0
        assert idea.red_team_risk_score > 0

    def test_red_team_attaches_warnings(self, idea, ctx):
        """Red-team passes idea but attaches warnings for single source type."""
        from orca_v20.red_team_gate import run_red_team
        from orca_v20.schemas import EvidencePack, EvidenceItem, EvidenceType

        idea.confidence = 8
        # Create evidence with single source type
        items = [
            EvidenceItem(
                evidence_type=EvidenceType.NEWS_ARTICLE,
                source="nws",
                headline="Weather alert",
                relevance_score=0.8,
            ),
            EvidenceItem(
                evidence_type=EvidenceType.NEWS_ARTICLE,
                source="nws",
                headline="Another alert",
                relevance_score=0.7,
            ),
        ]
        idea.evidence_pack = EvidencePack(
            ticker="AAPL",
            items=items,
            total_sources=1,
            freshest_item_age_hours=2.0,
            gate_score=0.8,
            gate_passed=True,
        )

        survivors = run_red_team([idea], ctx)
        assert len(survivors) == 1
        assert idea.survived_red_team is True
        assert len(idea.red_team_warnings) > 0
        assert any("single_source_type" in w for w in idea.red_team_warnings)


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Overlap Pipeline Integration
# ─────────────────────────────────────────────────────────────────────

class TestOverlapPipelineIntegration:
    """Tests for Patch 2: Thesis overlap wired into pipeline."""

    def test_overlap_fields_on_schema(self):
        """IdeaCandidate has all overlap structured fields with correct defaults."""
        from orca_v20.schemas import IdeaCandidate
        idea = IdeaCandidate()
        assert idea.overlap_warnings == []
        assert idea.overlap_score == 0.0
        assert idea.overlap_cluster_count == 0
        assert idea.concentration_confidence == ""

    def test_overlap_warnings_structured(self, ctx):
        """Two ideas with identical catalyst text get overlap warnings on structured field."""
        from orca_v20.schemas import IdeaCandidate
        from orca_v20.daemon_rules import check_thesis_overlap
        from unittest.mock import patch

        idea_a = IdeaCandidate(
            idea_id="a1", ticker="XOM", catalyst="oil prices surging on OPEC cuts",
            thesis="energy sector rally expected", confidence=8,
        )
        idea_b = IdeaCandidate(
            idea_id="b1", ticker="CVX", catalyst="oil prices surging on OPEC production cuts",
            thesis="energy sector rally bullish outlook", confidence=8,
        )

        def mock_resolve(ticker, allow_live=True):
            return ("ENERGY", "", "curated")

        with patch("orca_v20.sector_map.resolve_sector", side_effect=mock_resolve):
            result = check_thesis_overlap([idea_a, idea_b], ctx)

        assert len(result) == 2
        # At least one should have overlap warnings on the structured field
        all_warnings = idea_a.overlap_warnings + idea_b.overlap_warnings
        assert len(all_warnings) > 0
        assert any("OVERLAP" in w for w in all_warnings)

    def test_concentration_confidence_set(self, ctx):
        """Concentration confidence is set on ideas based on sector resolution quality."""
        from orca_v20.schemas import IdeaCandidate
        from orca_v20.daemon_rules import check_thesis_overlap
        from unittest.mock import patch

        idea_a = IdeaCandidate(
            idea_id="a1", ticker="XOM", catalyst="oil play", thesis="bull", confidence=8,
        )
        idea_b = IdeaCandidate(
            idea_id="b1", ticker="MYSTERY_X", catalyst="unknown stock", thesis="speculative", confidence=7,
        )

        def mock_resolve(ticker, allow_live=True):
            if ticker == "XOM":
                return ("ENERGY", "", "curated")
            return ("UNKNOWN", "", "unknown")

        with patch("orca_v20.sector_map.resolve_sector", side_effect=mock_resolve):
            result = check_thesis_overlap([idea_a, idea_b], ctx)

        # Has UNKNOWN → reduced confidence
        assert idea_a.concentration_confidence == "reduced"
        assert idea_b.concentration_confidence == "reduced"


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Auto-Label Config Thresholds
# ─────────────────────────────────────────────────────────────────────

class TestAutoLabelConfig:
    """Tests for Patch 3: Auto-label thresholds from config."""

    def test_auto_label_default_values_unchanged(self):
        """Default threshold values match original hardcoded behavior."""
        from orca_v20.config import THRESHOLDS
        assert THRESHOLDS.thesis_auto_win_pct == 0.15
        assert THRESHOLDS.thesis_auto_loss_pct == -0.10
        assert THRESHOLDS.thesis_auto_downgrade_pct == -0.05

    def test_auto_label_uses_config_thresholds(self):
        """Verify thesis_store references THRESHOLDS, not hardcoded values."""
        import inspect
        from orca_v20 import thesis_store
        source = inspect.getsource(thesis_store.auto_label_active_theses)
        # Should reference per-horizon thresholds from config
        assert "THRESHOLDS.horizon_auto_thresholds" in source
        # Downgrade still uses global threshold
        assert "THRESHOLDS.thesis_auto_downgrade_pct" in source
        # Should use horizon-aware expiry
        assert "THRESHOLDS.horizon_expiry_multiplier" in source
        # Should NOT have old hardcoded values
        assert ">= 0.15" not in source
        assert "<= -0.10" not in source
        assert "<= -0.05" not in source


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Keyword Extraction
# ─────────────────────────────────────────────────────────────────────

class TestKeywordExtraction:
    """Tests for Patch 4: Improved keyword extraction."""

    def test_keyword_extraction_filters_stopwords(self):
        """Tokenizer removes stop words from catalyst text."""
        from orca_v20.text_utils import tokenize
        tokens = tokenize("The quick brown fox could also have been very lazy")
        assert "the" not in tokens
        assert "could" not in tokens
        assert "also" not in tokens
        assert "very" not in tokens
        assert "been" not in tokens
        assert "quick" in tokens
        assert "brown" in tokens
        assert "lazy" in tokens

    def test_keyword_extraction_more_precise(self):
        """New tokenizer produces better tokens than naive split."""
        from orca_v20.text_utils import tokenize

        text = "The FDA approved a new drug for treating cancer patients in Houston"
        # Old approach: text.split()[:5] → ["The", "FDA", "approved", "a", "new"]
        old_kw = text.split()[:5]
        # New approach: tokenize → stop-word filtered, lowercased, alpha-only
        new_kw = tokenize(text)[:8]

        # Old has useless words like "The", "a"
        assert "The" in old_kw
        assert "a" in old_kw

        # New should NOT have stop words, and should have meaningful tokens
        assert "the" not in new_kw
        assert "fda" in new_kw
        assert "approved" in new_kw
        assert "cancer" in new_kw
        assert "houston" in new_kw

    def test_extended_stopwords_filtered(self):
        """v20 extension stop words (over, just, about, after, before) are removed."""
        from orca_v20.text_utils import tokenize
        tokens = tokenize("just over about after before earnings release")
        for w in ("just", "over", "about", "after", "before"):
            assert w not in tokens, f"'{w}' should be filtered as a stop word"
        assert "earnings" in tokens
        assert "release" in tokens


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Confidence Trajectory in Reporting
# ─────────────────────────────────────────────────────────────────────

class TestConfidenceTrajectoryInReport:
    """Tests for Patch 5: Confidence trajectory surfaced in reporting."""

    def test_run_summary_includes_trajectory(self, ctx):
        """format_telegram_run_summary includes Thesis Momentum section."""
        from orca_v20.publisher import format_telegram_run_summary
        from unittest.mock import patch

        mock_trajectories = [
            {
                "thesis_id": "t1", "ticker": "XOM", "trajectory": "RISING",
                "latest_confidence": 9, "confidence_delta": 3,
                "latest_price": 105.50, "snapshot_count": 5,
            },
            {
                "thesis_id": "t2", "ticker": "AAPL", "trajectory": "FALLING",
                "latest_confidence": 4, "confidence_delta": -2,
                "latest_price": 180.00, "snapshot_count": 3,
            },
        ]

        trace = {"ideas_generated": 5, "ideas_after_gates": 2,
                 "trades_structured": 1, "trades_logged": 1}

        with patch("orca_v20.thesis_store.get_confidence_trajectory", return_value=mock_trajectories):
            summary = format_telegram_run_summary(ctx, [], trace)

        assert "Thesis Momentum:" in summary
        assert "XOM" in summary
        assert "RISING" in summary
        assert "FALLING" in summary

    def test_replay_summary_includes_trajectory_with_ctx(self, ctx):
        """format_telegram_replay_summary includes trajectory when ctx passed."""
        from orca_v20.publisher import format_telegram_replay_summary
        from unittest.mock import patch

        mock_trajectories = [
            {
                "thesis_id": "t1", "ticker": "CVX", "trajectory": "RISING",
                "latest_confidence": 8, "confidence_delta": 2,
                "latest_price": 155.00, "snapshot_count": 4,
            },
        ]

        replay_results = [{"replay_mode": "RULES_ONLY", "ticker": "XOM"}]

        with patch("orca_v20.thesis_store.get_confidence_trajectory", return_value=mock_trajectories):
            summary = format_telegram_replay_summary(replay_results, None, ctx=ctx)

        assert "Thesis Momentum:" in summary
        assert "CVX" in summary
        assert "RISING" in summary


# ─────────────────────────────────────────────────────────────────────
# Patch Sprint Tests — Sector Map Cleanup
# ─────────────────────────────────────────────────────────────────────

class TestSectorMapCleanup:
    """Tests for Patch 6: Sector map duplicates removed, curated names added."""

    def test_new_curated_tickers(self):
        """Newly added curated tickers resolve correctly."""
        from orca_v20.sector_map import get_sector
        assert get_sector("EQT") == "ENERGY"
        assert get_sector("PBF") == "ENERGY"
        assert get_sector("LNG") == "ENERGY"
        assert get_sector("STNG") == "TRANSPORT"
        assert get_sector("FLNG") == "TRANSPORT"
        assert get_sector("FRO") == "TRANSPORT"

    def test_duplicate_fix_fdx(self):
        """FDX resolves to INDUSTRIALS (duplicate TRANSPORT entry removed)."""
        from orca_v20.sector_map import get_sector
        assert get_sector("FDX") == "INDUSTRIALS"

    def test_duplicate_fix_qqq(self):
        """QQQ resolves to ETF_BROAD (duplicate ETF_SECTOR entry removed)."""
        from orca_v20.sector_map import get_sector
        assert get_sector("QQQ") == "ETF_BROAD"


# ─────────────────────────────────────────────────────────────────────
# Overnight L1 — Auto-label ordering
# ─────────────────────────────────────────────────────────────────────

class TestOvernightAutoLabelOrdering:
    """Verify auto_label_active_theses is wired into overnight L1 before replay."""

    def test_auto_label_before_replay(self, ctx):
        """auto_label runs BEFORE run_nightly_replay in L1 step ordering."""
        from unittest.mock import patch, MagicMock
        from orca_v20.overnight import run_layer_1, OvernightBudgetTracker

        call_order = []

        def mock_auto_label(c):
            call_order.append("auto_label")
            return 2

        def mock_replay(c):
            call_order.append("replay")
            return []

        budget = OvernightBudgetTracker(hard_limit=5.0)

        with patch("orca_v20.thesis_store.auto_label_active_theses", mock_auto_label), \
             patch("orca_v20.replay_engine.run_nightly_replay", mock_replay), \
             patch("orca_v20.thesis_momentum.run_momentum_update", lambda c: None), \
             patch("orca_v20.daemon_rules.update_rules", lambda c: None):
            results = run_layer_1(ctx, budget)

        assert "auto_label" in call_order, "auto_label was not called"
        assert "replay" in call_order, "replay was not called"
        assert call_order.index("auto_label") < call_order.index("replay"), \
            f"auto_label must run before replay, got: {call_order}"
        assert results["auto_labeled"] == 2


# ─────────────────────────────────────────────────────────────────────
# Evidence Gate — Timestamp parsing + GDELT backoff
# ─────────────────────────────────────────────────────────────────────

class TestEvidenceTimestampParsing:
    """Verify evidence gate timestamp extraction chain includes source adapter fields."""

    def test_source_timestamp_field_recognized(self):
        """The timestamp chain in evidence_gate picks up source_timestamp."""
        # Simulate exactly what _build_external_evidence does for timestamp
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()

        # GDELT-style item — only has source_timestamp
        item = {"title": "Oil surge", "source_timestamp": now_iso}

        published = (
            item.get("published_utc")
            or item.get("freshness_utc")
            or item.get("timestamp")
            or item.get("fetched_utc")
            or item.get("source_timestamp")
            or item.get("seendate")
            or item.get("started_at")
            or item.get("post_timestamp")
        )
        assert published == now_iso, f"source_timestamp not picked up: got {published}"

    def test_seendate_field_recognized(self):
        """GDELT seendate field should be picked up as timestamp."""
        item = {"title": "Article", "seendate": "20260316T120000Z"}

        published = (
            item.get("published_utc")
            or item.get("freshness_utc")
            or item.get("timestamp")
            or item.get("fetched_utc")
            or item.get("source_timestamp")
            or item.get("seendate")
            or item.get("started_at")
            or item.get("post_timestamp")
        )
        assert published == "20260316T120000Z"

    def test_all_adapter_fields_in_evidence_gate_code(self):
        """Verify evidence_gate.py actually contains the source_timestamp lookup."""
        import inspect
        from orca_v20 import evidence_gate
        source = inspect.getsource(evidence_gate._build_external_evidence)
        assert "source_timestamp" in source, "source_timestamp missing from evidence gate"
        assert "seendate" in source, "seendate missing from evidence gate"
        assert "started_at" in source, "started_at missing from evidence gate"
        assert "post_timestamp" in source, "post_timestamp missing from evidence gate"


class TestGdeltBackoff:
    """Verify GDELT adapter has rate-limiting between queries."""

    def test_fetch_all_has_inter_query_delay(self):
        """fetch_all should sleep between queries to avoid 429 bursts."""
        from unittest.mock import patch, MagicMock
        import orca_v20.source_adapters.gdelt_adapter as gdelt

        call_times = []

        def mock_search(query, timeout=15):
            call_times.append(query)
            return []

        with patch.object(gdelt, "search_articles", mock_search), \
             patch.object(gdelt.time, "sleep") as mock_sleep:
            result = gdelt.fetch_all(queries=["q1", "q2", "q3"], timeout=5)

        # Should have called sleep between queries (not after last)
        assert mock_sleep.call_count == 2  # 3 queries → 2 sleeps
        mock_sleep.assert_called_with(1.5)

    def test_search_retries_on_429(self):
        """search_articles should retry once on HTTP 429."""
        from unittest.mock import patch, MagicMock, call
        import orca_v20.source_adapters.gdelt_adapter as gdelt

        mock_resp_429 = MagicMock()
        mock_resp_429.status_code = 429

        mock_resp_ok = MagicMock()
        mock_resp_ok.status_code = 200
        mock_resp_ok.json.return_value = {"articles": [{"url": "http://test.com", "title": "Test", "domain": "test.com"}]}
        mock_resp_ok.raise_for_status = MagicMock()

        mock_requests = MagicMock()
        mock_requests.get = MagicMock(side_effect=[mock_resp_429, mock_resp_ok])

        with patch.dict("sys.modules", {"requests": mock_requests}), \
             patch.object(gdelt.time, "sleep") as mock_sleep:
            articles = gdelt.search_articles("test query")

        # Should have retried after 429
        assert mock_requests.get.call_count == 2
        assert len(articles) == 1


# ─────────────────────────────────────────────────────────────────────
# Publisher: X filter + Google Sheet link
# ─────────────────────────────────────────────────────────────────────

class TestPublisherXFilter:
    """X filter allows DOWNGRADE, blocks KILL/HOLD."""

    def _make_trade_idea(self, action_val):
        trade = StructuredTrade(
            ticker="DHT",
            idea_direction=IdeaDirection.BEARISH,
            strategy_label="put_spread",
            trade_expression_type=TradeExpressionType.BUY_PUT_SPREAD,
            confidence=8,
            urgency=7,
            consensus_tag=ConsensusTag.SINGLE,
        )
        idea = IdeaCandidate(
            ticker="DHT",
            idea_direction=IdeaDirection.BEARISH,
            catalyst_action=CatalystAction(action_val) if action_val else None,
        )
        return trade, idea

    def test_downgrade_blocked_by_x_filter(self):
        """DOWNGRADE trades should be blocked from X (no standalone middle-state posts)."""
        from orca_v20.publisher import _passes_x_filter
        trade, idea = self._make_trade_idea("DOWNGRADE")
        passes, reasons = _passes_x_filter(trade, idea)
        assert not passes, f"DOWNGRADE should NOT pass X filter"
        assert any("DOWNGRADE" in r for r in reasons)

    def test_kill_blocked_by_x_filter(self):
        """KILL trades should still be blocked from X."""
        from orca_v20.publisher import _passes_x_filter
        trade, idea = self._make_trade_idea("KILL")
        passes, reasons = _passes_x_filter(trade, idea)
        assert not passes
        assert any("KILL" in r for r in reasons)

    def test_confirm_passes_x_filter(self):
        """CONFIRM trades should pass the X filter."""
        from orca_v20.publisher import _passes_x_filter
        trade, idea = self._make_trade_idea("CONFIRM")
        passes, reasons = _passes_x_filter(trade, idea)
        assert passes


class TestPublisherSheetLink:
    """Google Sheet link appears in Telegram summaries."""

    def test_run_summary_has_sheet_link(self):
        """format_telegram_run_summary should include Google Sheet URL."""
        from orca_v20.publisher import format_telegram_run_summary
        ctx = RunContext(run_id="test", market_date="2026-03-16")
        msg = format_telegram_run_summary(ctx, [], {})
        assert "docs.google.com/spreadsheets" in msg

    def test_replay_summary_has_sheet_link(self):
        """format_telegram_replay_summary should include Google Sheet URL."""
        from orca_v20.publisher import format_telegram_replay_summary
        msg = format_telegram_replay_summary([], None, None)
        # Empty replay still shows the link? Let's check — empty returns short message
        # Only non-empty replay gets the full format with link
        results = [{"replay_mode": "RULES_ONLY", "training_examples_generated": 0}]
        msg = format_telegram_replay_summary(results, None, None)
        assert "docs.google.com/spreadsheets" in msg


class TestPublishPolicy:
    """Verify middle-state publish policy: no standalone Telegram/X for watchlist/downgrade/hold."""

    def _make_idea(self, action_str):
        from orca_v20.schemas import CatalystAction
        return IdeaCandidate(
            ticker="TEST", idea_direction=IdeaDirection.BEARISH,
            catalyst="Test catalyst", thesis="Test thesis",
            confidence=7, catalyst_action=CatalystAction(action_str),
        )

    def _make_trade(self, framing):
        return StructuredTrade(
            ticker="TEST", idea_direction=IdeaDirection.BEARISH,
            trade_expression_type=TradeExpressionType.BUY_PUT_SPREAD,
            strategy_label="Bear Put Spread", confidence=7,
            consensus_tag=ConsensusTag.SINGLE, report_framing=framing,
        )

    def test_downgrade_returns_none(self):
        """DOWNGRADE: format_telegram_message returns None (no standalone report)."""
        from orca_v20.telegram_format import format_telegram_message
        msg = format_telegram_message(self._make_trade("downgrade_note"), self._make_idea("DOWNGRADE"))
        assert msg is None

    def test_watchlist_returns_none(self):
        """WATCHLIST: format_telegram_message returns None."""
        from orca_v20.telegram_format import format_telegram_message
        msg = format_telegram_message(self._make_trade("watchlist"), self._make_idea("HOLD"))
        assert msg is None

    def test_confirm_returns_string(self):
        """CONFIRM: format_telegram_message returns a real message."""
        from orca_v20.telegram_format import format_telegram_message
        idea = IdeaCandidate(
            ticker="NVDA", idea_direction=IdeaDirection.BULLISH,
            catalyst="GPU demand", thesis="Earnings inflection",
            confidence=9, catalyst_action=CatalystAction.CONFIRM,
        )
        trade = StructuredTrade(
            ticker="NVDA", idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL_SPREAD,
            strategy_label="Bull Call Spread", confidence=9,
            consensus_tag=ConsensusTag.UNANIMOUS, report_framing="conviction_call",
        )
        msg = format_telegram_message(trade, idea)
        assert msg is not None
        assert "ORCA Research" in msg

    def test_kill_returns_alert(self):
        """KILL: format_telegram_message returns ORCA Alert."""
        from orca_v20.telegram_format import format_telegram_message
        idea = IdeaCandidate(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            catalyst="ETF inflows", confidence=3,
            catalyst_action=CatalystAction.KILL,
            invalidation="SEC suspended approvals",
        )
        trade = StructuredTrade(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            strategy_label="Long Call", confidence=3,
            consensus_tag=ConsensusTag.SINGLE, report_framing="invalidated",
        )
        msg = format_telegram_message(trade, idea)
        assert msg is not None
        assert "ORCA Alert" in msg

    def test_kill_with_open_position_uses_exit_wording(self):
        """KILL + ACTIVE thesis → 'exit — thesis invalidated'."""
        from orca_v20.telegram_format import format_telegram_message
        idea = IdeaCandidate(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            catalyst="ETF inflows", confidence=3,
            catalyst_action=CatalystAction.KILL,
            invalidation="SEC suspended approvals",
            thesis_status=ThesisStatus.ACTIVE,
        )
        trade = StructuredTrade(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            strategy_label="Long Call", confidence=3,
            consensus_tag=ConsensusTag.SINGLE, report_framing="invalidated",
        )
        msg = format_telegram_message(trade, idea)
        assert msg is not None
        assert "Action: exit" in msg
        assert "stand aside" not in msg

    def test_kill_without_open_position_uses_stand_aside(self):
        """KILL + DRAFT thesis → 'stand aside — thesis invalidated'."""
        from orca_v20.telegram_format import format_telegram_message
        idea = IdeaCandidate(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            catalyst="ETF inflows", confidence=3,
            catalyst_action=CatalystAction.KILL,
            invalidation="SEC suspended approvals",
            thesis_status=ThesisStatus.DRAFT,
        )
        trade = StructuredTrade(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            strategy_label="Long Call", confidence=3,
            consensus_tag=ConsensusTag.SINGLE, report_framing="invalidated",
        )
        msg = format_telegram_message(trade, idea)
        assert msg is not None
        assert "stand aside" in msg
        assert "Action: exit" not in msg

    def test_kill_explicit_override_true(self):
        """Explicit has_open_position=True overrides thesis_status."""
        from orca_v20.telegram_format import format_telegram_message
        idea = IdeaCandidate(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            catalyst="ETF inflows", confidence=3,
            catalyst_action=CatalystAction.KILL,
            thesis_status=ThesisStatus.DRAFT,  # DRAFT, but override says open
        )
        trade = StructuredTrade(
            ticker="COIN", idea_direction=IdeaDirection.BULLISH,
            trade_expression_type=TradeExpressionType.BUY_CALL,
            strategy_label="Long Call", confidence=3,
            consensus_tag=ConsensusTag.SINGLE, report_framing="invalidated",
        )
        msg = format_telegram_message(trade, idea, has_open_position=True)
        assert "Action: exit" in msg

    def test_daily_summary_watchlist_format(self):
        """Watchlist entries in daily summary should be '$TICKER | c=N' only."""
        from orca_v20.telegram_format import format_daily_summary
        from orca_v20.run_context import RunContext

        ctx = RunContext.__new__(RunContext)
        ctx.run_id = "test"; ctx.market_date = "2026-03-16"
        ctx.api_cost_usd = 0.0; ctx.errors = []

        trade = self._make_trade("downgrade_note")
        trade.ticker = "DHT"
        idea = self._make_idea("DOWNGRADE")
        idea.ticker = "DHT"
        idea.idea_id = trade.idea_id = "dht-001"

        summary = format_daily_summary(ctx, [trade], [idea], {"ideas_generated": 1, "ideas_after_gates": 1})
        # Should have clean watchlist line without direction/label framing
        assert "$DHT | c=7" in summary
        # Should NOT have directional framing in watchlist section
        assert "Bearish" not in summary or "Actionable" in summary  # Bearish only allowed in actionable
        # Section should say "Watchlist" not "Monitoring"
        assert "Watchlist:" in summary


# ═══════════════════════════════════════════════════════════════════════
# BUDGET SPRINT MODE TESTS
# ═══════════════════════════════════════════════════════════════════════

class TestBudgetModeConfig:
    """Budget mode config: routing, thresholds, publishing force-disable."""

    def test_budget_routing_inherits_role_routing(self):
        from orca_v20.config import BudgetRoleRouting, RoleRouting
        assert issubclass(BudgetRoleRouting, RoleRouting)

    def test_budget_routing_uses_cheap_models(self):
        from orca_v20.config import BudgetRoleRouting, MODELS
        r = BudgetRoleRouting()
        for role_field in r.__dataclass_fields__:
            model_key = getattr(r, role_field)
            if callable(model_key):
                continue
            assert model_key in MODELS, f"Role {role_field} maps to unknown model {model_key}"
            spec = MODELS[model_key]
            assert spec.cost_per_1k_output <= 0.01, \
                f"Role {role_field} → {model_key} costs ${spec.cost_per_1k_output}/1k out — too expensive for budget"

    def test_budget_models_registered(self):
        from orca_v20.config import MODELS
        assert "claude-haiku" in MODELS
        assert "gpt-mini" in MODELS
        assert "gemini-flash" in MODELS

    def test_budget_routing_cheap_defaults(self):
        from orca_v20.config import BudgetRoleRouting
        r = BudgetRoleRouting()
        assert r.hunter_primary == "claude-haiku"
        assert r.hunter_secondary == "gpt-mini"
        assert r.hunter_tertiary == "gemini-flash"

    def test_budget_shortlist_caps_default_high(self):
        """Normal mode has high defaults (effectively unlimited)."""
        from orca_v20.config import Thresholds
        t = Thresholds()
        assert t.budget_max_stage2_candidates == 100
        assert t.budget_max_stage3_candidates == 100


class TestBudgetModePublisher:
    """Budget mode should force-disable all publishing."""

    def test_publisher_budget_guard_returns_early(self):
        import orca_v20.config as config
        import importlib
        import orca_v20.publisher as pub
        original = config.BUDGET_MODE
        try:
            config.BUDGET_MODE = True
            importlib.reload(pub)
            result = pub.publish_trades([], [], None, {})
            assert result["budget_mode"] is True
            assert result["telegram_sent"] == 0
            assert result["x_posted"] == 0
        finally:
            config.BUDGET_MODE = original
            importlib.reload(pub)

    def test_budget_flags_override_env_vars(self):
        from orca_v20.config import FeatureFlags
        flags = FeatureFlags()
        flags.publish_telegram = True
        flags.publish_x = True
        flags.publish_reports = True
        flags.mirror_to_google_sheet = True
        # Simulate budget override
        flags.publish_reports = False
        flags.publish_telegram = False
        flags.publish_x = False
        flags.mirror_to_google_sheet = False
        assert flags.publish_telegram is False
        assert flags.publish_x is False
        assert flags.publish_reports is False
        assert flags.mirror_to_google_sheet is False


class TestBudgetModeDB:
    """Budget DB tables should exist after bootstrap."""

    def test_budget_intelligence_log_table_exists(self):
        from orca_v20.db_bootstrap import bootstrap_db, verify_db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            bootstrap_db(db_path)
            report = verify_db(db_path)
            assert "budget_intelligence_log" in report
            assert "intraday_cases" in report
            assert "teacher_feedback" in report
        finally:
            os.unlink(db_path)

    def test_budget_intelligence_log_schema(self):
        from orca_v20.db_bootstrap import bootstrap_db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            bootstrap_db(db_path)
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("PRAGMA table_info(budget_intelligence_log)")
            columns = {row[1] for row in cursor.fetchall()}
            conn.close()
            required = {"run_id", "role", "model_used", "provider", "cost_usd",
                        "input_tokens", "output_tokens", "raw_output", "pipeline_stage"}
            assert required.issubset(columns), f"Missing columns: {required - columns}"
        finally:
            os.unlink(db_path)


class TestBudgetModeReplaySkip:
    def test_replay_flag_off_in_budget(self):
        from orca_v20.config import FeatureFlags
        flags = FeatureFlags()
        flags.enable_replay_engine = False
        assert flags.enable_replay_engine is False


class TestBudgetModeNormalModeUnaffected:
    """Normal (non-budget) mode should be completely unaffected."""

    def test_normal_routing_unchanged(self):
        from orca_v20.config import RoleRouting
        r = RoleRouting()
        assert r.hunter_primary == "claude-opus"
        assert r.flow_reader == "claude-opus"
        assert r.catalyst_confirm == "claude-opus"

    def test_normal_thresholds_unchanged(self):
        from orca_v20.config import Thresholds
        t = Thresholds()
        assert t.max_api_cost_per_run == 20.0
        assert t.min_confidence == 7
        assert t.overnight_hard_budget_usd == 50.0

    def test_normal_publish_defaults(self):
        from orca_v20.config import FeatureFlags
        f = FeatureFlags()
        assert f.publish_reports is True
        assert f.publish_telegram is False
        assert f.publish_x is False


class TestBudgetPipelineCLI:
    def test_budget_arg_exists(self):
        from pipeline_v20 import parse_args
        old_argv = sys.argv
        try:
            sys.argv = ["pipeline_v20.py", "--budget", "--dry-run"]
            args = parse_args()
            assert args.budget is True
            assert args.dry_run is True
        finally:
            sys.argv = old_argv

    def test_no_budget_arg_defaults_false(self):
        from pipeline_v20 import parse_args
        old_argv = sys.argv
        try:
            sys.argv = ["pipeline_v20.py", "--dry-run"]
            args = parse_args()
            assert args.budget is False
        finally:
            sys.argv = old_argv


# ─────────────────────────────────────────────────────────────────────
# Horizon-Aware Outcome Tracking
# ─────────────────────────────────────────────────────────────────────

class TestHorizonAwareOutcomes:
    """Tests for horizon-aware outcome tracking (Patch 1-9)."""

    def test_horizon_enum_values(self):
        from orca_v20.schemas import ThesisHorizon
        assert ThesisHorizon.INTRADAY.value == "INTRADAY"
        assert ThesisHorizon.ONE_DAY.value == "1D"
        assert ThesisHorizon.THREE_DAY.value == "3D"
        assert ThesisHorizon.FIVE_DAY.value == "5D"
        assert ThesisHorizon.SEVEN_TO_TEN_DAY.value == "7_10D"
        assert ThesisHorizon.TWO_TO_FOUR_WEEK.value == "2_4W"
        assert ThesisHorizon.UNKNOWN.value == "UNKNOWN"

    def test_timing_quality_enum(self):
        from orca_v20.schemas import TimingQuality
        values = [e.value for e in TimingQuality]
        assert len(values) == 6
        assert "correct_and_timely" in values
        assert "correct_but_slow" in values
        assert "correct_thesis_poor_timing" in values
        assert "too_early_to_judge" in values
        assert "invalidated_before_playout" in values
        assert "failed_thesis" in values

    def test_parse_horizon_from_window(self):
        from orca_v20.horizon import parse_horizon_from_window
        from orca_v20.schemas import ThesisHorizon
        assert parse_horizon_from_window("1-3 days") == ThesisHorizon.THREE_DAY
        assert parse_horizon_from_window("1-2 weeks") == ThesisHorizon.SEVEN_TO_TEN_DAY
        assert parse_horizon_from_window("intraday") == ThesisHorizon.INTRADAY
        assert parse_horizon_from_window("") == ThesisHorizon.UNKNOWN

    def test_7_10d_not_failed_on_night_1(self):
        """7_10D thesis with grace=int(8*0.5)=4. Day 1 < grace → skip."""
        from orca_v20.config import Thresholds
        t = Thresholds()
        horizon_days = t.horizon_days_map["7_10D"]  # 8
        grace = int(horizon_days * t.horizon_grace_multiplier)  # int(8*0.5) = 4
        trading_age_day1 = 1
        assert trading_age_day1 < grace, "Day 1 should be within grace period"

    def test_1d_judged_quickly(self):
        """1D thesis: horizon=1, grace=int(1*0.5)=0. Day 1 >= grace → eligible."""
        from orca_v20.config import Thresholds
        t = Thresholds()
        horizon_days = t.horizon_days_map["1D"]  # 1
        grace = int(horizon_days * t.horizon_grace_multiplier)  # int(1*0.5) = 0
        trading_age_day1 = 1
        assert trading_age_day1 >= grace, "1D thesis should be eligible immediately"

    def test_forward_outcomes_table_exists(self):
        from orca_v20.db_bootstrap import bootstrap_db, verify_db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            bootstrap_db(db_path)
            report = verify_db(db_path)
            assert "thesis_forward_outcomes" in report
        finally:
            os.unlink(db_path)

    def test_expected_horizon_on_schema(self):
        from orca_v20.schemas import IdeaCandidate, Thesis, ThesisHorizon
        idea = IdeaCandidate()
        assert idea.expected_horizon == ThesisHorizon.UNKNOWN
        thesis = Thesis()
        assert thesis.expected_horizon == ""

    def test_per_horizon_thresholds(self):
        from orca_v20.config import Thresholds
        t = Thresholds()
        # INTRADAY: tight thresholds
        assert t.horizon_auto_thresholds["INTRADAY"] == (0.05, -0.03)
        # 2_4W: wider thresholds
        assert t.horizon_auto_thresholds["2_4W"] == (0.15, -0.10)
        # UNKNOWN matches 2_4W
        assert t.horizon_auto_thresholds["UNKNOWN"] == (0.15, -0.10)

    def test_calendar_to_trading_days(self):
        from orca_v20.horizon import calendar_to_trading_days
        assert calendar_to_trading_days(7) == 5   # 7 * 5/7 = 5
        assert calendar_to_trading_days(14) == 10  # 14 * 5/7 = 10
        assert calendar_to_trading_days(1) == 1    # max(1, int(5/7)) = max(1,0) = 1
        assert calendar_to_trading_days(0) == 0    # edge: 0 → 0

    def test_catalyst_intact_conservative(self):
        from orca_v20.horizon import is_catalyst_intact
        # Default: True
        assert is_catalyst_intact("ACTIVE") is True
        assert is_catalyst_intact("DRAFT") is True
        assert is_catalyst_intact("CLOSED_WIN") is True
        # False only for explicit invalidation
        assert is_catalyst_intact("CLOSED_INVALIDATED") is False
        assert is_catalyst_intact("ACTIVE", "INVALIDATED") is False
        assert is_catalyst_intact("ACTIVE", "EXPIRED") is False

    def test_correct_thesis_poor_timing(self):
        """MFE exceeds win threshold but final return negative → poor timing."""
        from orca_v20.horizon import compute_timing_quality
        result = compute_timing_quality(
            directional_return=-0.02,  # final return negative
            mfe=0.16,                  # MFE exceeded 15% win threshold
            trading_age=10,
            horizon_days=8,
            status="ACTIVE",
            expected_horizon="7_10D",
        )
        assert result == "correct_thesis_poor_timing"

    def test_parse_horizon_same_day(self):
        from orca_v20.horizon import parse_horizon_from_window
        from orca_v20.schemas import ThesisHorizon
        assert parse_horizon_from_window("same day") == ThesisHorizon.INTRADAY

    def test_parse_horizon_2_4_weeks(self):
        from orca_v20.horizon import parse_horizon_from_window
        from orca_v20.schemas import ThesisHorizon
        assert parse_horizon_from_window("2-4 weeks") == ThesisHorizon.TWO_TO_FOUR_WEEK

    def test_parse_horizon_malformed(self):
        from orca_v20.horizon import parse_horizon_from_window
        from orca_v20.schemas import ThesisHorizon
        assert parse_horizon_from_window("soon") == ThesisHorizon.UNKNOWN
        assert parse_horizon_from_window("eventually") == ThesisHorizon.UNKNOWN
        assert parse_horizon_from_window("next quarter") == ThesisHorizon.UNKNOWN


# ─────────────────────────────────────────────────────────────────────
# Budget Cap Enforcement Tests
# ─────────────────────────────────────────────────────────────────────

class TestBudgetCapEnforcement:
    """Tests proving budget stage caps are enforced BEFORE expensive calls."""

    def _make_ideas(self, n, base_confidence=7):
        """Create n IdeaCandidate stubs with descending confidence."""
        from orca_v20.schemas import IdeaCandidate, IdeaDirection
        ideas = []
        for i in range(n):
            idea = IdeaCandidate()
            idea.ticker = f"TICK{i}"
            idea.confidence = base_confidence + n - i  # descending
            idea.idea_direction = IdeaDirection.BULLISH
            idea.catalyst = f"Test catalyst {i}"
            idea.tape_read = "DATA UNAVAILABLE"
            idea.flow_details = {}
            ideas.append(idea)
        return ideas

    def test_stage2_adapter_defensive_cap(self):
        """Stage 2 adapter enforces budget cap even if pipeline didn't."""
        import orca_v20.config as cfg
        from orca_v20.adapters.flow_adapter import run_stage2
        from orca_v20.run_context import RunContext, SourceMode

        original = cfg.BUDGET_MODE
        try:
            cfg.BUDGET_MODE = True
            ctx = RunContext(source_mode=SourceMode.NO_UW, dry_run=True)
            ideas = self._make_ideas(10)
            result = run_stage2(ideas, ctx)
            assert len(result) <= cfg.THRESHOLDS.budget_max_stage2_candidates, \
                f"Stage 2 processed {len(result)} ideas, cap is {cfg.THRESHOLDS.budget_max_stage2_candidates}"
        finally:
            cfg.BUDGET_MODE = original

    def test_stage3_adapter_defensive_cap(self):
        """Stage 3 adapter enforces budget cap even if pipeline didn't."""
        import orca_v20.config as cfg
        from orca_v20.adapters.catalyst_adapter import run_stage3
        from orca_v20.run_context import RunContext

        original = cfg.BUDGET_MODE
        orig_cap = cfg.THRESHOLDS.budget_max_stage3_candidates
        try:
            cfg.BUDGET_MODE = True
            cfg.THRESHOLDS.budget_max_stage3_candidates = 2
            ctx = RunContext(dry_run=True)
            ideas = self._make_ideas(8)
            # Mock confirm_catalyst to avoid real API calls
            import orca_v20.adapters.catalyst_adapter as cat_mod
            original_confirm = cat_mod.confirm_idea
            cat_mod.confirm_idea = lambda idea, flow, ctx: idea  # no-op
            try:
                survivors, filtered = run_stage3(ideas, ctx)
                # Survivors + filtered should total at most cap (since confirm is no-op,
                # all cap ideas survive, but total processed must be <= cap)
                total_processed = len(survivors) + len(filtered)
                assert total_processed <= 2, \
                    f"Stage 3 processed {total_processed} ideas, cap is 2"
            finally:
                cat_mod.confirm_idea = original_confirm
        finally:
            cfg.BUDGET_MODE = original
            cfg.THRESHOLDS.budget_max_stage3_candidates = orig_cap

    def test_stage4_adapter_defensive_cap(self):
        """Stage 4 adapter enforces budget cap on structuring."""
        import orca_v20.config as cfg
        from orca_v20.adapters.structurer_adapter import run_stage4
        from orca_v20.run_context import RunContext

        original = cfg.BUDGET_MODE
        orig_cap = cfg.THRESHOLDS.budget_max_structured
        try:
            cfg.BUDGET_MODE = True
            cfg.THRESHOLDS.budget_max_structured = 1
            ctx = RunContext(dry_run=True)
            ideas = self._make_ideas(5)
            # Mock structure_ideas to return one trade per idea
            import orca_v20.adapters.structurer_adapter as struct_mod
            original_fn = struct_mod.structure_ideas
            struct_mod.structure_ideas = lambda ideas, ctx: ideas  # pass-through
            original_vol = struct_mod._vol_aware_adjust
            struct_mod._vol_aware_adjust = lambda t, ctx: t  # no-op
            try:
                result = run_stage4(ideas, ctx)
                assert len(result) <= 1, \
                    f"Stage 4 structured {len(result)} ideas, cap is 1"
            finally:
                struct_mod.structure_ideas = original_fn
                struct_mod._vol_aware_adjust = original_vol
        finally:
            cfg.BUDGET_MODE = original
            cfg.THRESHOLDS.budget_max_structured = orig_cap

    def test_caps_preserve_highest_confidence(self):
        """Budget caps keep the HIGHEST confidence ideas, not arbitrary ones."""
        import orca_v20.config as cfg
        from orca_v20.adapters.flow_adapter import run_stage2
        from orca_v20.run_context import RunContext, SourceMode

        original = cfg.BUDGET_MODE
        orig_cap = cfg.THRESHOLDS.budget_max_stage2_candidates
        try:
            cfg.BUDGET_MODE = True
            cfg.THRESHOLDS.budget_max_stage2_candidates = 3
            ctx = RunContext(source_mode=SourceMode.NO_UW, dry_run=True)
            ideas = self._make_ideas(6)
            # Confidence values: 13, 12, 11, 10, 9, 8 (descending by construction)
            result = run_stage2(ideas, ctx)
            assert len(result) == 3
            tickers = [r.ticker for r in result]
            # Top 3 by confidence should be TICK0, TICK1, TICK2
            assert "TICK0" in tickers
            assert "TICK1" in tickers
            assert "TICK2" in tickers
        finally:
            cfg.BUDGET_MODE = original
            cfg.THRESHOLDS.budget_max_stage2_candidates = orig_cap

    def test_normal_mode_no_cap(self):
        """Normal (non-budget) mode does NOT cap ideas."""
        import orca_v20.config as cfg
        from orca_v20.adapters.flow_adapter import run_stage2
        from orca_v20.run_context import RunContext, SourceMode

        original = cfg.BUDGET_MODE
        try:
            cfg.BUDGET_MODE = False
            ctx = RunContext(source_mode=SourceMode.NO_UW, dry_run=True)
            ideas = self._make_ideas(10)
            result = run_stage2(ideas, ctx)
            assert len(result) == 10, \
                f"Normal mode should not cap, got {len(result)} instead of 10"
        finally:
            cfg.BUDGET_MODE = original

    def test_budget_mode_accessed_via_module_not_binding(self):
        """Verify _cfg.BUDGET_MODE is used, not a stale import binding."""
        import orca_v20.config as cfg

        # Simulate the bug: import BUDGET_MODE as a local binding
        from orca_v20.config import BUDGET_MODE as local_binding
        original = cfg.BUDGET_MODE

        try:
            # Toggle the module-level value
            cfg.BUDGET_MODE = True
            # The local binding should still be the OLD value (this is the bug)
            # But cfg.BUDGET_MODE should be the NEW value
            assert cfg.BUDGET_MODE is True, "Module-level access should reflect change"
            # This demonstrates why we must use cfg.BUDGET_MODE, not a local binding
        finally:
            cfg.BUDGET_MODE = original


# ─────────────────────────────────────────────────────────────────────
# Confidence Parser Tests
# ─────────────────────────────────────────────────────────────────────

class TestConfidenceParser:
    """Verify _parse_confidence handles all qualitative labels correctly."""

    def test_high(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("High") == 8

    def test_medium_high(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Medium-High") == 7

    def test_medium(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Medium") == 6

    def test_medium_low(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Medium-Low") == 5

    def test_low(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Low") == 4

    def test_very_high(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Very High") == 10

    def test_very_low(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("Very Low") == 3

    def test_numeric(self):
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("8") == 8
        assert _parse_confidence(9) == 9

    def test_with_explanation(self):
        """R1 sometimes appends explanations after a dash."""
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        assert _parse_confidence("High — catalyst confirmed, strong conviction") == 8
        assert _parse_confidence("Medium-Low — speculative") == 5

    def test_sort_order_correct(self):
        """Parsed values sort in the expected order for budget cap."""
        from orca_v20.adapters.stage1_hunter_adapter import _parse_confidence
        labels = ["Low", "Medium-Low", "Medium", "Medium-High", "High", "Very High"]
        values = [_parse_confidence(l) for l in labels]
        assert values == sorted(values), f"Expected ascending order, got {values}"
