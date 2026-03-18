"""
ORCA v20 — Database bootstrap / initialization layer.

ALL v20 tables live in orca_v20.db. This module is the single source of truth
for table schemas. No other module creates tables — they all call bootstrap_db()
at pipeline start.

Databases:
    orca_v20.db     — NEW, all v20 data (this module creates it)
    orca_v3_trades.db — READ ONLY from v20, never written
    orca_iv_history.db — READ ONLY from v20, shared IV data
"""

import sqlite3
import os
import logging
from typing import Optional

from orca_v20.config import PATHS

logger = logging.getLogger("orca_v20.db")


# ─────────────────────────────────────────────────────────────────────
# Table definitions (DDL)
# ─────────────────────────────────────────────────────────────────────

_TABLES = {

    # --- Thesis persistence ---
    "theses": """
        CREATE TABLE IF NOT EXISTS theses (
            thesis_id           TEXT PRIMARY KEY,
            ticker              TEXT NOT NULL,
            idea_direction      TEXT NOT NULL,
            catalyst            TEXT NOT NULL,
            thesis_text         TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'DRAFT',
            created_run_id      TEXT NOT NULL,
            created_utc         TEXT NOT NULL,
            last_updated_run_id TEXT NOT NULL,
            last_updated_utc    TEXT NOT NULL,
            initial_confidence  INTEGER NOT NULL DEFAULT 0,
            current_confidence  INTEGER NOT NULL DEFAULT 0,
            confidence_slope    REAL DEFAULT 0.0,
            times_seen          INTEGER DEFAULT 1,
            invalidation_trigger TEXT DEFAULT '',
            invalidated_reason  TEXT,
            UNIQUE(ticker, catalyst, idea_direction)
        )
    """,

    "thesis_daily_snapshots": """
        CREATE TABLE IF NOT EXISTS thesis_daily_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            thesis_id           TEXT NOT NULL,
            snapshot_date       TEXT NOT NULL,
            run_id              TEXT NOT NULL,
            confidence          INTEGER NOT NULL,
            underlying_price    REAL,
            iv_level            REAL,
            catalyst_status     TEXT DEFAULT 'PENDING',
            notes               TEXT DEFAULT '',
            FOREIGN KEY (thesis_id) REFERENCES theses(thesis_id),
            UNIQUE(thesis_id, snapshot_date)
        )
    """,

    # --- Forward outcomes (horizon-aware tracking) ---
    "thesis_forward_outcomes": """
        CREATE TABLE IF NOT EXISTS thesis_forward_outcomes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            thesis_id           TEXT NOT NULL,
            eval_date           TEXT NOT NULL,
            window_days         INTEGER NOT NULL,
            forward_return_pct  REAL,
            mfe_pct             REAL,
            mae_pct             REAL,
            thesis_age_days     INTEGER,
            expected_horizon    TEXT DEFAULT 'UNKNOWN',
            horizon_outcome_label TEXT DEFAULT '',
            timing_quality      TEXT DEFAULT '',
            catalyst_intact     INTEGER DEFAULT 1,
            created_utc         TEXT NOT NULL,
            UNIQUE(thesis_id, eval_date, window_days)
        )
    """,

    # --- Evidence persistence ---
    "evidence_packs": """
        CREATE TABLE IF NOT EXISTS evidence_packs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            thesis_id           TEXT,
            total_sources       INTEGER DEFAULT 0,
            freshest_item_age_h REAL DEFAULT 0.0,
            aggregate_sentiment TEXT,
            gate_score          REAL DEFAULT 0.0,
            gate_passed         INTEGER DEFAULT 0,
            items_json          TEXT DEFAULT '[]',
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Trade records (v20 native) ---
    "etp_records": """
        CREATE TABLE IF NOT EXISTS etp_records (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            idea_id             TEXT NOT NULL,
            thesis_id           TEXT,
            ticker              TEXT NOT NULL,
            idea_direction      TEXT NOT NULL,
            trade_expression    TEXT NOT NULL,
            strategy_label      TEXT DEFAULT '',
            strike_1            REAL,
            strike_2            REAL,
            expiry              TEXT,
            dte                 INTEGER,
            entry_price         REAL,
            target_price        REAL,
            stop_price          REAL,
            max_loss            REAL,
            max_gain            REAL,
            risk_reward         REAL,
            iv_at_entry         REAL,
            iv_hv_ratio         REAL,
            delta               REAL,
            theta               REAL,
            kelly_size_pct      REAL,
            adjusted_size_pct   REAL,
            contracts           INTEGER,
            slippage_pct        REAL,
            liquidity_score     REAL,
            confidence          INTEGER DEFAULT 0,
            confidence_raw      TEXT DEFAULT '',
            urgency             INTEGER DEFAULT 0,
            urgency_raw         TEXT DEFAULT '',
            consensus_tag       TEXT DEFAULT 'SINGLE',
            report_framing      TEXT,
            report_label        TEXT,
            expected_horizon    TEXT DEFAULT 'UNKNOWN',
            status              TEXT DEFAULT 'OPEN',
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Replay engine (Phase 5) ---
    "replay_runs": """
        CREATE TABLE IF NOT EXISTS replay_runs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            replay_id               TEXT NOT NULL UNIQUE,
            thesis_id               TEXT NOT NULL,
            replay_date             TEXT NOT NULL,
            original_run_id         TEXT NOT NULL,
            replay_mode             TEXT NOT NULL DEFAULT 'RULES_ONLY',
            original_verdict        TEXT DEFAULT '',
            realized_outcome        TEXT DEFAULT '',
            hindsight_verdict       TEXT DEFAULT '',
            counterfactual_verdict  TEXT DEFAULT '',
            what_we_missed          TEXT DEFAULT '',
            missed_signal_candidates    TEXT DEFAULT '[]',
            missed_contradiction_candidates TEXT DEFAULT '[]',
            agent_miss_report       TEXT DEFAULT '',
            confidence_delta        INTEGER DEFAULT 0,
            pnl_actual              REAL,
            pnl_counterfactual      REAL,
            lessons_json            TEXT DEFAULT '[]',
            training_examples_generated INTEGER DEFAULT 0,
            created_utc             TEXT NOT NULL,
            FOREIGN KEY (thesis_id) REFERENCES theses(thesis_id)
        )
    """,

    # --- Training examples (Phase 5) ---
    "training_examples": """
        CREATE TABLE IF NOT EXISTS training_examples (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            example_id          TEXT NOT NULL UNIQUE,
            input_prompt        TEXT NOT NULL,
            expected_output     TEXT NOT NULL,
            outcome_label       TEXT NOT NULL,
            source_run_id       TEXT NOT NULL,
            source_thesis_id    TEXT NOT NULL,
            replay_id           TEXT NOT NULL,
            generated_utc       TEXT NOT NULL,
            FOREIGN KEY (replay_id) REFERENCES replay_runs(replay_id)
        )
    """,

    # --- Institutional pressure ---
    "institutional_pressure_snapshots": """
        CREATE TABLE IF NOT EXISTS institutional_pressure_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            snapshot_date       TEXT NOT NULL,
            crowding_score      REAL DEFAULT 0.0,
            pain_level          REAL DEFAULT 0.0,
            gamma_exposure      REAL,
            dark_pool_pct       REAL,
            short_interest_pct  REAL,
            institutional_flow  TEXT,
            notes               TEXT DEFAULT '',
            created_utc         TEXT NOT NULL,
            UNIQUE(ticker, snapshot_date)
        )
    """,

    # --- Memory / analog cases ---
    "memory_cases": """
        CREATE TABLE IF NOT EXISTS memory_cases (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            thesis_id           TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            catalyst_type       TEXT DEFAULT '',
            setup_summary       TEXT DEFAULT '',
            outcome             TEXT DEFAULT '',
            pnl_pct             REAL,
            key_lesson          TEXT DEFAULT '',
            embedding_json      TEXT,
            created_utc         TEXT NOT NULL,
            FOREIGN KEY (thesis_id) REFERENCES theses(thesis_id)
        )
    """,

    # --- Elite agent simulation ---
    "elite_agent_votes": """
        CREATE TABLE IF NOT EXISTS elite_agent_votes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            thesis_id           TEXT NOT NULL,
            agent_id            TEXT NOT NULL,
            agent_persona       TEXT NOT NULL,
            vote                TEXT NOT NULL,
            confidence          REAL DEFAULT 0.0,
            reasoning           TEXT DEFAULT '',
            dissent_flag        INTEGER DEFAULT 0,
            created_utc         TEXT NOT NULL
        )
    """,

    "crowd_snapshots": """
        CREATE TABLE IF NOT EXISTS crowd_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            thesis_id           TEXT NOT NULL,
            elite_consensus     TEXT DEFAULT '',
            elite_confidence    REAL DEFAULT 0.0,
            crowd_sentiment     REAL DEFAULT 0.0,
            dissent_ratio       REAL DEFAULT 0.0,
            final_verdict       TEXT DEFAULT '',
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Quant gate proofs ---
    "quant_proof_records": """
        CREATE TABLE IF NOT EXISTS quant_proof_records (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            thesis_id           TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            analog_count        INTEGER DEFAULT 0,
            analog_win_rate     REAL DEFAULT 0.0,
            correlation_to_spy  REAL DEFAULT 0.0,
            factor_residual     REAL,
            causal_p_value      REAL,
            gate_passed         INTEGER DEFAULT 0,
            gate_reason         TEXT DEFAULT '',
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Daemon / monitor rules ---
    "monitor_rules": """
        CREATE TABLE IF NOT EXISTS monitor_rules (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_type           TEXT NOT NULL,
            rule_name           TEXT NOT NULL,
            threshold           REAL,
            current_value       REAL,
            triggered           INTEGER DEFAULT 0,
            action_taken        TEXT DEFAULT '',
            last_checked_utc    TEXT,
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Overnight budget log (Operator Activation) ---
    "overnight_budget_log": """
        CREATE TABLE IF NOT EXISTS overnight_budget_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            night_date          TEXT NOT NULL,
            total_spent         REAL DEFAULT 0.0,
            hard_limit          REAL DEFAULT 50.0,
            items_processed     INTEGER DEFAULT 0,
            items_deferred      INTEGER DEFAULT 0,
            items_escalated     INTEGER DEFAULT 0,
            cost_by_provider_json TEXT DEFAULT '{}',
            cost_by_role_json   TEXT DEFAULT '{}',
            exhausted           INTEGER DEFAULT 0,
            updated_utc         TEXT NOT NULL,
            UNIQUE(night_date)
        )
    """,

    # --- Replay job queue (Operator Activation) ---
    "replay_job_queue": """
        CREATE TABLE IF NOT EXISTS replay_job_queue (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            thesis_id           TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            layer               INTEGER NOT NULL DEFAULT 1,
            priority            INTEGER NOT NULL DEFAULT 0,
            reason              TEXT DEFAULT '',
            status              TEXT NOT NULL DEFAULT 'PENDING',
            result_json         TEXT DEFAULT '',
            created_utc         TEXT NOT NULL,
            updated_utc         TEXT,
            UNIQUE(thesis_id, layer)
        )
    """,

    # --- Evidence source backlinks (B2) ---
    "evidence_source_links": """
        CREATE TABLE IF NOT EXISTS evidence_source_links (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            thesis_id           TEXT,
            evidence_pack_id    INTEGER,
            source_name         TEXT NOT NULL,
            source_item_id      TEXT DEFAULT '',
            source_tier         INTEGER DEFAULT 3,
            relation            TEXT DEFAULT 'neutral',
            created_utc         TEXT NOT NULL
        )
    """,

    # --- Run traces ---
    # --- Sector cache (dynamic resolution) ---
    "sector_cache": """
        CREATE TABLE IF NOT EXISTS sector_cache (
            ticker          TEXT PRIMARY KEY,
            sector          TEXT NOT NULL,
            industry        TEXT DEFAULT '',
            source          TEXT NOT NULL,
            resolved_utc    TEXT NOT NULL,
            expires_utc     TEXT
        )
    """,

    # --- Budget intelligence log (crown jewel audit layer) ---
    "budget_intelligence_log": """
        CREATE TABLE IF NOT EXISTS budget_intelligence_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            timestamp_utc       TEXT NOT NULL,
            pipeline_stage      TEXT NOT NULL,
            role                TEXT NOT NULL,
            model_used          TEXT NOT NULL,
            provider            TEXT NOT NULL,
            ticker              TEXT DEFAULT '',
            idea_id             TEXT DEFAULT '',
            thesis_id           TEXT DEFAULT '',
            prompt_hash         TEXT DEFAULT '',
            input_tokens        INTEGER DEFAULT 0,
            output_tokens       INTEGER DEFAULT 0,
            cost_usd            REAL DEFAULT 0.0,
            latency_ms          INTEGER DEFAULT 0,
            raw_output          TEXT NOT NULL DEFAULT '',
            structured_output   TEXT DEFAULT '',
            quality_score       REAL DEFAULT 0.0,
            notes               TEXT DEFAULT ''
        )
    """,

    # --- Budget intraday case store (cheap-model snapshots for teacher review) ---
    "intraday_cases": """
        CREATE TABLE IF NOT EXISTS intraday_cases (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              TEXT NOT NULL,
            market_date         TEXT NOT NULL,
            session_bucket      TEXT DEFAULT '',
            ticker              TEXT NOT NULL,
            idea_id             TEXT DEFAULT '',
            thesis_id           TEXT DEFAULT '',
            direction           TEXT DEFAULT '',
            catalyst_text       TEXT DEFAULT '',
            catalyst_hash       TEXT DEFAULT '',
            confidence          INTEGER DEFAULT 0,
            stage_reached       TEXT DEFAULT '',
            flow_interpretation TEXT DEFAULT '',
            catalyst_verdict    TEXT DEFAULT '',
            gate_results_json   TEXT DEFAULT '{}',
            cheap_model_output  TEXT DEFAULT '',
            teacher_verdict     TEXT DEFAULT '',
            teacher_critique    TEXT DEFAULT '',
            teacher_label       TEXT DEFAULT '',
            disagreement_flag   INTEGER DEFAULT 0,
            playbook_tag        TEXT DEFAULT '',
            created_utc         TEXT NOT NULL,
            reviewed_utc        TEXT DEFAULT ''
        )
    """,

    # --- Teacher feedback (overnight premium writes) ---
    "teacher_feedback": """
        CREATE TABLE IF NOT EXISTS teacher_feedback (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id             INTEGER,
            run_id              TEXT NOT NULL,
            review_date         TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            verdict             TEXT DEFAULT '',
            critique            TEXT DEFAULT '',
            what_cheap_missed   TEXT DEFAULT '',
            escalation_suggestion TEXT DEFAULT '',
            playbook_note       TEXT DEFAULT '',
            calibration_note    TEXT DEFAULT '',
            outcome_tag         TEXT DEFAULT '',
            created_utc         TEXT NOT NULL,
            FOREIGN KEY (case_id) REFERENCES intraday_cases(id)
        )
    """,

    # --- Run traces ---
    "run_traces": """
        CREATE TABLE IF NOT EXISTS run_traces (
            run_id              TEXT PRIMARY KEY,
            started_utc         TEXT NOT NULL,
            completed_utc       TEXT,
            market_date         TEXT,
            research_mode       TEXT,
            source_mode         TEXT,
            ideas_generated     INTEGER DEFAULT 0,
            ideas_after_flow    INTEGER DEFAULT 0,
            ideas_after_confirm INTEGER DEFAULT 0,
            ideas_after_gates   INTEGER DEFAULT 0,
            trades_structured   INTEGER DEFAULT 0,
            trades_logged       INTEGER DEFAULT 0,
            total_api_cost_usd  REAL DEFAULT 0.0,
            errors_json         TEXT DEFAULT '[]',
            warnings_json       TEXT DEFAULT '[]',
            dry_run             INTEGER DEFAULT 0,
            success             INTEGER DEFAULT 0
        )
    """,
}


# ─────────────────────────────────────────────────────────────────────
# Indexes
# ─────────────────────────────────────────────────────────────────────

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_theses_ticker ON theses(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_theses_status ON theses(status)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_thesis ON thesis_daily_snapshots(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_date ON thesis_daily_snapshots(snapshot_date)",
    "CREATE INDEX IF NOT EXISTS idx_etp_run ON etp_records(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_etp_ticker ON etp_records(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_etp_thesis ON etp_records(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_etp_status ON etp_records(status)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_run ON evidence_packs(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_ticker ON evidence_packs(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_replay_thesis ON replay_runs(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_pressure_ticker ON institutional_pressure_snapshots(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_memory_ticker ON memory_cases(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_votes_run ON elite_agent_votes(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_votes_thesis ON elite_agent_votes(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_crowd_run ON crowd_snapshots(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_run ON quant_proof_records(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_traces_date ON run_traces(market_date)",
    "CREATE INDEX IF NOT EXISTS idx_replay_id ON replay_runs(replay_id)",
    "CREATE INDEX IF NOT EXISTS idx_training_replay ON training_examples(replay_id)",
    "CREATE INDEX IF NOT EXISTS idx_training_thesis ON training_examples(source_thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_budget_date ON overnight_budget_log(night_date)",
    "CREATE INDEX IF NOT EXISTS idx_queue_status ON replay_job_queue(status)",
    "CREATE INDEX IF NOT EXISTS idx_queue_layer ON replay_job_queue(layer)",
    "CREATE INDEX IF NOT EXISTS idx_queue_thesis ON replay_job_queue(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_esl_run ON evidence_source_links(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_esl_ticker ON evidence_source_links(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_esl_thesis ON evidence_source_links(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_sc_sector ON sector_cache(sector)",
    "CREATE INDEX IF NOT EXISTS idx_sc_source ON sector_cache(source)",
    # Forward outcomes indexes
    "CREATE INDEX IF NOT EXISTS idx_tfo_thesis ON thesis_forward_outcomes(thesis_id)",
    "CREATE INDEX IF NOT EXISTS idx_tfo_eval ON thesis_forward_outcomes(eval_date)",
    "CREATE INDEX IF NOT EXISTS idx_tfo_horizon ON thesis_forward_outcomes(expected_horizon)",
    # Budget sprint indexes
    "CREATE INDEX IF NOT EXISTS idx_bil_run ON budget_intelligence_log(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_bil_role ON budget_intelligence_log(role)",
    "CREATE INDEX IF NOT EXISTS idx_bil_ticker ON budget_intelligence_log(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_ic_run ON intraday_cases(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_ic_date ON intraday_cases(market_date)",
    "CREATE INDEX IF NOT EXISTS idx_ic_ticker ON intraday_cases(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_ic_teacher ON intraday_cases(teacher_label)",
    "CREATE INDEX IF NOT EXISTS idx_tf_case ON teacher_feedback(case_id)",
    "CREATE INDEX IF NOT EXISTS idx_tf_date ON teacher_feedback(review_date)",
    "CREATE INDEX IF NOT EXISTS idx_tf_ticker ON teacher_feedback(ticker)",
]


# ─────────────────────────────────────────────────────────────────────
# Bootstrap function
# ─────────────────────────────────────────────────────────────────────

def _run_migrations(conn) -> None:
    """
    Run ALTER TABLE migrations for columns added after initial schema.
    Safe to re-run — silently ignores already-existing columns.
    """
    migrations = [
        ("theses", "expected_horizon", "TEXT DEFAULT 'UNKNOWN'"),
        ("intraday_cases", "expected_horizon", "TEXT DEFAULT 'UNKNOWN'"),
        ("etp_records", "expected_horizon", "TEXT DEFAULT 'UNKNOWN'"),
    ]
    for table, column, col_def in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def}")
            conn.commit()
            logger.info(f"  Migration: added {table}.{column}")
        except Exception:
            # Column already exists — expected on subsequent runs
            pass

    # Migration: fix snapshot UNIQUE constraint (remove run_id from constraint)
    # Check if the old constraint includes run_id by inspecting the schema
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='thesis_daily_snapshots'"
        ).fetchone()
        if row and row[0] and "UNIQUE(thesis_id, snapshot_date, run_id)" in row[0]:
            logger.info("  Migration: rebuilding thesis_daily_snapshots UNIQUE constraint (dropping run_id)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS thesis_daily_snapshots_new (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    thesis_id           TEXT NOT NULL,
                    snapshot_date       TEXT NOT NULL,
                    run_id              TEXT NOT NULL,
                    confidence          INTEGER NOT NULL,
                    underlying_price    REAL,
                    iv_level            REAL,
                    catalyst_status     TEXT DEFAULT 'PENDING',
                    notes               TEXT DEFAULT '',
                    FOREIGN KEY (thesis_id) REFERENCES theses(thesis_id),
                    UNIQUE(thesis_id, snapshot_date)
                )
            """)
            conn.execute("""
                INSERT OR REPLACE INTO thesis_daily_snapshots_new
                    (id, thesis_id, snapshot_date, run_id, confidence,
                     underlying_price, iv_level, catalyst_status, notes)
                SELECT id, thesis_id, snapshot_date, run_id, confidence,
                       underlying_price, iv_level, catalyst_status, notes
                FROM thesis_daily_snapshots
            """)
            conn.execute("DROP TABLE thesis_daily_snapshots")
            conn.execute("ALTER TABLE thesis_daily_snapshots_new RENAME TO thesis_daily_snapshots")
            conn.commit()
            logger.info("  Migration: thesis_daily_snapshots UNIQUE constraint fixed")
    except Exception as e:
        logger.debug(f"  Migration: snapshot UNIQUE constraint check skipped: {e}")


def bootstrap_db(db_path: Optional[str] = None) -> str:
    """
    Create orca_v20.db and all tables + indexes if they don't exist.

    Called once at pipeline_v20.py startup. Idempotent — safe to call
    multiple times (all DDL uses IF NOT EXISTS).

    Returns the resolved db_path.
    """
    if db_path is None:
        db_path = PATHS.v20_db

    logger.info(f"Bootstrapping v20 database at: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")      # better concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")

    cursor = conn.cursor()

    # Create all tables
    for table_name, ddl in _TABLES.items():
        logger.debug(f"  Creating table: {table_name}")
        cursor.execute(ddl)

    # Create all indexes
    for idx_sql in _INDEXES:
        cursor.execute(idx_sql)

    conn.commit()

    # Run column migrations (safe to re-run — ignores already-existing columns)
    _run_migrations(conn)

    # Verify all tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    logger.info(f"v20 DB ready — {len(tables)} tables: {', '.join(tables)}")

    conn.close()
    return db_path


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """
    Get a connection to orca_v20.db.
    Caller is responsible for closing.
    """
    if db_path is None:
        db_path = PATHS.v20_db

    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"v20 DB not found at {db_path}. Call bootstrap_db() first."
        )

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def checkpoint_wal(db_path: Optional[str] = None) -> None:
    """
    Force a WAL checkpoint (TRUNCATE mode) so all data is flushed to the
    main DB file.  Call before git-committing the .db file.
    """
    if db_path is None:
        db_path = PATHS.v20_db
    if not os.path.exists(db_path):
        return
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    logger.info(f"WAL checkpoint (TRUNCATE) completed for {db_path}")


def verify_db(db_path: Optional[str] = None) -> dict:
    """
    Verify DB integrity. Returns table names and row counts.
    Useful for health checks and debugging.
    """
    if db_path is None:
        db_path = PATHS.v20_db

    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]

    report = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        report[table] = count

    conn.close()
    return report
