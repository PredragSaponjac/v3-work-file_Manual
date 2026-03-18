# ORCA v20 — Operator Guide

## Running v20

### Standard run
```bash
python pipeline_v20.py
```

### Dry run (no DB writes, no notifications)
```bash
python pipeline_v20.py --dry-run
```

### Deep research mode (extended search, extra LLM passes)
```bash
python pipeline_v20.py --mode deep
```

### Fast mode (minimal scraping, cached data only)
```bash
python pipeline_v20.py --mode fast
```

### Skip Unusual Whales data
```bash
python pipeline_v20.py --no-uw
```

### Minimal sources (scanner + news only)
```bash
python pipeline_v20.py --minimal
```

### Verbose/debug logging
```bash
python pipeline_v20.py --verbose
```

### Combine flags
```bash
python pipeline_v20.py --mode deep --verbose --dry-run
```

---

## Where Data Is Stored

| Database | Purpose | Access |
|----------|---------|--------|
| `orca_v20.db` | All v20 data | READ/WRITE |
| `orca_v3_trades.db` | Legacy v3 trades | READ ONLY |
| `orca_iv_history.db` | Shared IV history | READ ONLY |

All v20 tables live in `orca_v20.db`. No other module creates tables.

---

## Database Tables

| Table | Purpose |
|-------|---------|
| `theses` | Persistent thesis lifecycle (DRAFT → ACTIVE → CLOSED_*) |
| `thesis_daily_snapshots` | Daily confidence snapshots for momentum tracking |
| `evidence_packs` | Evidence gate results per idea |
| `etp_records` | Trade records (v20 native) — 36 columns including confidence_raw/urgency_raw |
| `replay_runs` | Counterfactual replay results (rules-based + premium) |
| `training_examples` | Replay-generated training examples for fine-tuning |
| `institutional_pressure_snapshots` | Crowding, SI, dark pool data per ticker |
| `memory_cases` | Analog case library for pattern matching |
| `elite_agent_votes` | Individual votes from 15-agent simulation |
| `crowd_snapshots` | Aggregated crowd/elite verdict per thesis |
| `quant_proof_records` | Quant gate evidence (correlation, analogs) |
| `monitor_rules` | Daemon rules for portfolio health checks |
| `run_traces` | Full audit trail per pipeline run |

---

## Inspecting Theses

```sql
-- All active theses
SELECT thesis_id, ticker, catalyst, current_confidence, times_seen, status
FROM theses WHERE status IN ('ACTIVE', 'DRAFT') ORDER BY current_confidence DESC;

-- Thesis history
SELECT * FROM thesis_daily_snapshots WHERE thesis_id = 'YOUR_THESIS_ID'
ORDER BY snapshot_date;

-- Closed theses (ready for replay)
SELECT thesis_id, ticker, status, invalidated_reason, last_updated_utc
FROM theses WHERE status LIKE 'CLOSED_%' ORDER BY last_updated_utc DESC;
```

---

## Inspecting Monitor Rules

```sql
-- All rules and their current state
SELECT rule_type, rule_name, threshold, current_value, triggered, action_taken
FROM monitor_rules ORDER BY rule_type;

-- Triggered rules only
SELECT * FROM monitor_rules WHERE triggered = 1;
```

---

## Interpreting Gate Statuses

Every gate returns one of four statuses:

| Status | Meaning | Trade Impact |
|--------|---------|--------------|
| `PASS` | Clear evidence supports passing | Proceeds normally |
| `PASS_LOW_CONFIDENCE` | Passed but data was thin | Proceeds with caution |
| `UNPROVEN` | Insufficient data to decide | Proceeds (benefit of doubt) |
| `FAIL` | Clear evidence of failure | Idea removed from pipeline |

Gates:
- **Evidence gate**: Checks source diversity and freshness
- **Quant gate**: SPY correlation + analog win rate
- **Causal gate**: Catalyst specificity + transmission mechanism
- **Factor gate**: Single-factor CAPM proxy (alpha vs beta)

---

## Replay Engine

Replay runs automatically after each pipeline execution on any closed theses from the last 7 days.

**Two layers:**
1. **RULES_ONLY** — Deterministic analysis using DB traces + price data. Always runs. Free.
2. **PREMIUM_ESCALATED** — LLM-powered deep analysis. Only triggers when confidence_delta >= 3 or significant loss with missed contradictions.

```sql
-- View replay results
SELECT replay_id, thesis_id, replay_mode, realized_outcome,
       confidence_delta, training_examples_generated
FROM replay_runs ORDER BY created_utc DESC;

-- View training examples
SELECT example_id, outcome_label, source_thesis_id, replay_id
FROM training_examples ORDER BY generated_utc DESC;
```

---

## Simulation Mode

The elite simulation runs 15 AI agents (diverse personas) to vote on each idea. Cost-controlled:
- Only top 3 ideas (by confidence, min 5) enter simulation
- Uses Claude Sonnet (cost-efficient) not Opus
- No extended thinking needed for vote format

---

## Legacy Mirroring (OFF by default)

Three toggles in `orca_v20/config.py` → `FeatureFlags`:

```python
mirror_to_v3_trade_log: bool = False   # Write to orca_v3_trades.db
mirror_to_google_sheet: bool = False   # Sync to Google Sheet
publish_reports: bool = False          # Send Telegram/X reports
```

To enable, edit `config.py` and set the desired flag to `True`. Each flag is independent.

**Warning:** Enabling `mirror_to_v3_trade_log` will write trades to the legacy v3 database. Enabling `mirror_to_google_sheet` requires valid Google Sheets credentials. Enabling `publish_reports` requires Telegram bot token.

---

## Model Routing

| Role | Model | Purpose |
|------|-------|---------|
| Thesis generation (Role B) | Claude Opus 4.6 | Deep reasoning, adversarial |
| Final judge (Role C) | GPT-5.4 Thinking | Independent judgment |
| CIO integrator (Role A) | Gemini 3.1 Pro | Synthesis, integration |
| Elite simulation agents | Claude Sonnet 4.6 | Cost-efficient votes |
| Extraction/tagging | GPT-5.4 Fast | Cheap, fast utilities |
| Replay analyst | Claude Sonnet 4.6 | Post-mortem analysis |

---

## Cost Controls

- Per-run budget ceiling: $20.00 (configurable in `THRESHOLDS.max_api_cost_per_run`)
- Per-month ceiling: $150.00
- Elite simulation limited to top 3 ideas
- Replay uses cheap rules-only by default, premium escalation only on significant misses
- Router tracks per-role costs for audit

---

## Troubleshooting

**Pipeline halted by daemon rules:**
Check `monitor_rules` table for triggered rules. Common causes: max drawdown exceeded, too many consecutive losses.

**All ideas filtered at Stage 3:**
Normal behavior when catalyst confirmation finds weak/contradictory flow. The pipeline correctly filters.

**Over budget warning:**
Router skips LLM calls when `api_cost_usd >= max_api_cost_per_run`. Increase threshold or reduce research depth.

**Provider unhealthy:**
After 3 consecutive failures, a provider is marked unhealthy for 120 seconds. Fallback chain activates automatically.
