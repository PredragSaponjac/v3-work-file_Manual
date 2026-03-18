"""
ORCA v20 — Evidence Gate.

Hard stop/go gate before thesis generation.
Requires minimum evidence sources, freshness, and aggregate quality.
Returns explicit failure_reason when blocked.

Scoring dimensions:
    1. Source diversity   — at least N independent source types
    2. Source freshness   — freshest item must be < N hours old
    3. Contradiction count — contradictions vs. supportive ratio
    4. Non-social confirmation — at least 1 non-social-media source
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from orca_v20.config import FLAGS, THRESHOLDS
from orca_v20.db_bootstrap import get_connection
from orca_v20.run_context import RunContext
from orca_v20.schemas import (
    EvidenceItem, EvidencePack, EvidenceType, IdeaCandidate,
)

logger = logging.getLogger("orca_v20.evidence_gate")


# ─────────────────────────────────────────────────────────────────────
# Evidence collection from idea fields
# ─────────────────────────────────────────────────────────────────────

_SOCIAL_TYPES = {EvidenceType.SOCIAL_SENTIMENT}

_TYPE_KEYWORDS = {
    "sec": EvidenceType.SEC_FILING,
    "filing": EvidenceType.SEC_FILING,
    "10-k": EvidenceType.SEC_FILING,
    "10-q": EvidenceType.SEC_FILING,
    "earnings": EvidenceType.EARNINGS_REPORT,
    "revenue": EvidenceType.EARNINGS_REPORT,
    "eps": EvidenceType.EARNINGS_REPORT,
    "analyst": EvidenceType.ANALYST_RATING,
    "upgrade": EvidenceType.ANALYST_RATING,
    "downgrade": EvidenceType.ANALYST_RATING,
    "flow": EvidenceType.OPTIONS_FLOW,
    "unusual": EvidenceType.OPTIONS_FLOW,
    "sweep": EvidenceType.OPTIONS_FLOW,
    "dark pool": EvidenceType.OPTIONS_FLOW,
    "insider": EvidenceType.INSIDER_TRADE,
    "macro": EvidenceType.MACRO_EVENT,
    "fed": EvidenceType.MACRO_EVENT,
    "fomc": EvidenceType.MACRO_EVENT,
    "tariff": EvidenceType.MACRO_EVENT,
    "technical": EvidenceType.TECHNICAL_SIGNAL,
    "support": EvidenceType.TECHNICAL_SIGNAL,
    "resistance": EvidenceType.TECHNICAL_SIGNAL,
    "kalshi": EvidenceType.PREDICTION_MARKET,
    "polymarket": EvidenceType.PREDICTION_MARKET,
    "weather": EvidenceType.WEATHER_EVENT,
    "hurricane": EvidenceType.WEATHER_EVENT,
    "maritime": EvidenceType.WEATHER_EVENT,
    "shipping": EvidenceType.WEATHER_EVENT,
    "tanker": EvidenceType.WEATHER_EVENT,
    "port": EvidenceType.WEATHER_EVENT,
    "vessel": EvidenceType.WEATHER_EVENT,
    "congestion": EvidenceType.WEATHER_EVENT,
    "surge": EvidenceType.WEATHER_EVENT,
    "hormuz": EvidenceType.MACRO_EVENT,
    "strait": EvidenceType.MACRO_EVENT,
    "twitter": EvidenceType.SOCIAL_SENTIMENT,
    "reddit": EvidenceType.SOCIAL_SENTIMENT,
    "sentiment": EvidenceType.SOCIAL_SENTIMENT,
}


def _classify_evidence_type(text: str) -> EvidenceType:
    """Classify an evidence string into an EvidenceType."""
    lower = text.lower()
    for keyword, etype in _TYPE_KEYWORDS.items():
        if keyword in lower:
            return etype
    return EvidenceType.NEWS_ARTICLE


# ─────────────────────────────────────────────────────────────────────
# Timestamp / freshness helpers  (A2)
# ─────────────────────────────────────────────────────────────────────

def parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    """
    Safely parse an ISO-8601 timestamp string to a timezone-aware datetime.
    Returns None on any failure.
    """
    if not raw:
        return None
    try:
        # Python 3.7+ fromisoformat handles most ISO variants
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def compute_age_hours(ts: Optional[datetime], now: Optional[datetime] = None) -> Optional[float]:
    """Compute age in hours from a timestamp to now. Returns None if ts is None."""
    if ts is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    delta = now - ts
    return max(0.0, delta.total_seconds() / 3600.0)


def _compute_freshness(items: List[EvidenceItem], now: Optional[datetime] = None) -> Tuple[float, float, float]:
    """
    Compute freshness stats from evidence items.
    Returns (freshest_hours, oldest_hours, median_hours).
    Uses published_utc first, falls back to None.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    ages = []
    for item in items:
        ts = parse_timestamp(item.published_utc)
        age = compute_age_hours(ts, now)
        if age is not None:
            ages.append(age)

    if not ages:
        return -1.0, -1.0, -1.0  # no timestamps → unknown (not stale)

    ages.sort()
    freshest = ages[0]
    oldest = ages[-1]
    mid = len(ages) // 2
    median = ages[mid] if len(ages) % 2 == 1 else (ages[mid - 1] + ages[mid]) / 2.0

    return freshest, oldest, median


# ─────────────────────────────────────────────────────────────────────
# Internal evidence from idea fields
# ─────────────────────────────────────────────────────────────────────

def _build_evidence_items(idea: IdeaCandidate) -> List[EvidenceItem]:
    """
    Build EvidenceItem list from idea's raw evidence strings.
    Also includes catalyst and flow data as implicit evidence.
    """
    items = []
    seen_types = set()

    # 1. Explicit evidence list from R1 parser
    for i, ev_str in enumerate(idea.evidence or []):
        if not ev_str or not ev_str.strip():
            continue
        etype = _classify_evidence_type(ev_str)
        items.append(EvidenceItem(
            evidence_id=f"{idea.idea_id}_ev{i}",
            evidence_type=etype,
            source="r1_evidence",
            headline=ev_str[:200],
            summary=ev_str,
            relevance_score=0.7,
            sentiment="supportive",
            ticker=idea.ticker,
        ))
        seen_types.add(etype)

    # 2. Catalyst text as evidence
    if idea.catalyst:
        cat_type = _classify_evidence_type(idea.catalyst)
        if cat_type not in seen_types:
            items.append(EvidenceItem(
                evidence_id=f"{idea.idea_id}_cat",
                evidence_type=cat_type,
                source="catalyst",
                headline=idea.catalyst[:200],
                summary=idea.catalyst,
                relevance_score=0.8,
                sentiment="supportive",
                ticker=idea.ticker,
            ))
            seen_types.add(cat_type)

    # 3. Flow data as evidence (if present)
    if idea.tape_read and idea.tape_read not in ("?", "N/A"):
        items.append(EvidenceItem(
            evidence_id=f"{idea.idea_id}_flow",
            evidence_type=EvidenceType.OPTIONS_FLOW,
            source="uw_flow",
            headline=f"Tape read: {idea.tape_read}",
            summary=str(idea.flow_details or idea.tape_read),
            relevance_score=0.8,
            sentiment="supportive" if "CONFIRM" in (idea.tape_read or "").upper() else "neutral",
            ticker=idea.ticker,
        ))
        seen_types.add(EvidenceType.OPTIONS_FLOW)

    return items


# ─────────────────────────────────────────────────────────────────────
# External source evidence (A1: wire source adapters into gate)
# ─────────────────────────────────────────────────────────────────────

def _classify_sentiment(idea: IdeaCandidate, text: str) -> str:
    """
    Classify whether an external source item is supportive, contradictory,
    or neutral relative to the idea's direction.
    """
    lower = text.lower()
    direction = idea.idea_direction.value  # BULLISH or BEARISH

    # Bearish keywords
    bearish_kw = ["decline", "drop", "crash", "cut", "downgrade", "miss",
                  "weak", "loss", "recession", "shortage", "risk", "warning",
                  "plunge", "sell-off", "selloff", "bear"]
    # Bullish keywords
    bullish_kw = ["surge", "rally", "beat", "upgrade", "strong", "growth",
                  "breakout", "profit", "bull", "rebound", "gain", "record"]

    bear_hits = sum(1 for kw in bearish_kw if kw in lower)
    bull_hits = sum(1 for kw in bullish_kw if kw in lower)

    if bear_hits == 0 and bull_hits == 0:
        return "neutral"

    text_leans_bullish = bull_hits > bear_hits

    if direction == "BULLISH":
        if text_leans_bullish:
            return "supportive"
        elif bear_hits > bull_hits:
            return "contradictory"
    elif direction == "BEARISH":
        if not text_leans_bullish:
            return "supportive"
        elif bull_hits > bear_hits:
            return "contradictory"

    return "neutral"


def _build_external_evidence(idea: IdeaCandidate, ctx: RunContext) -> List[EvidenceItem]:
    """
    Pull matching source items from ctx.source_results for this idea's ticker.
    Convert them into EvidenceItem objects with tier-weighted relevance.
    """
    if not ctx.source_results:
        return []

    try:
        from orca_v20.source_adapters.orchestrator import get_items_for_ticker
    except ImportError:
        logger.warning("Could not import orchestrator — skipping external evidence")
        return []

    # Build keyword list — tokenized, stop-word filtered, deduplicated
    from orca_v20.text_utils import tokenize
    keywords = []
    if idea.catalyst:
        keywords.extend(tokenize(idea.catalyst)[:8])
    if idea.thesis:
        keywords.extend(tokenize(idea.thesis)[:5])
    # Deduplicate while preserving order
    seen = set()
    keywords = [k for k in keywords if not (k in seen or seen.add(k))]

    matched_items = get_items_for_ticker(ctx.source_results, idea.ticker, keywords)

    external_items = []
    for i, item in enumerate(matched_items):
        # Build searchable text for classification
        text = " ".join([
            str(item.get("title", "")),
            str(item.get("headline", "")),
            str(item.get("text", "")),
            str(item.get("summary", "")),
            str(item.get("trend_title", "")),
            str(item.get("series_label", "")),
        ])

        # Classify evidence type from text
        etype = _classify_evidence_type(text) if text.strip() else EvidenceType.OTHER

        # Tier-weighted relevance
        tier_weight = item.get("_tier_weight", 1.0)
        base_relevance = 0.6
        relevance = min(1.0, base_relevance * tier_weight)

        # Classify sentiment relative to idea direction
        sentiment = _classify_sentiment(idea, text)

        # Extract timestamp — check all field names used by source adapters
        published = (
            item.get("published_utc")
            or item.get("freshness_utc")
            or item.get("timestamp")
            or item.get("fetched_utc")
            or item.get("source_timestamp")   # all v20 source adapters
            or item.get("seendate")           # GDELT article date
            or item.get("started_at")         # Google Trends pubDate
            or item.get("post_timestamp")     # X whitelist
        )

        source_name = item.get("_matched_source", "external")

        external_items.append(EvidenceItem(
            evidence_id=f"{idea.idea_id}_ext{i}",
            evidence_type=etype,
            source=f"ext_{source_name}",
            headline=(text[:200] if text else "External source item"),
            summary=text[:500] if text else "",
            published_utc=published,
            relevance_score=round(relevance, 3),
            sentiment=sentiment,
            ticker=idea.ticker,
            raw_data=item,
        ))

    if external_items:
        logger.info(
            f"  [{idea.ticker}] {len(external_items)} external source items merged "
            f"(support={sum(1 for e in external_items if e.sentiment == 'supportive')}, "
            f"contra={sum(1 for e in external_items if e.sentiment == 'contradictory')}, "
            f"neutral={sum(1 for e in external_items if e.sentiment == 'neutral')})"
        )

    return external_items


def _detect_contradictions(items: List[EvidenceItem]) -> int:
    """Count items with negative/contradictory sentiment."""
    return sum(
        1 for item in items
        if item.sentiment and item.sentiment.lower() in ("negative", "contradictory", "bearish")
    )


# ─────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────

def _score_evidence(items: List[EvidenceItem]) -> Tuple[float, List[str]]:
    """
    Score evidence pack on 4 dimensions. Returns (score, failure_reasons).

    Score is 0.0 - 1.0. Failures are hard stops.
    """
    failures = []

    if not items:
        return 0.0, ["no_evidence_items"]

    # 1. Source diversity: count distinct evidence types
    distinct_types = len(set(item.evidence_type for item in items))
    min_sources = THRESHOLDS.min_evidence_sources
    diversity_score = min(1.0, distinct_types / max(min_sources, 1))
    if distinct_types < min_sources:
        failures.append(f"insufficient_diversity: {distinct_types}/{min_sources} source types")

    # 2. Non-social confirmation: at least 1 non-social source
    non_social = [item for item in items if item.evidence_type not in _SOCIAL_TYPES]
    has_non_social = len(non_social) > 0
    if not has_non_social:
        failures.append("no_non_social_confirmation")

    # 3. Contradiction ratio
    n_contradictions = _detect_contradictions(items)
    contradiction_ratio = n_contradictions / max(len(items), 1)
    if contradiction_ratio > 0.5:
        failures.append(f"high_contradiction_ratio: {contradiction_ratio:.2f}")

    # 4. Relevance average
    avg_relevance = sum(item.relevance_score for item in items) / len(items)

    # 5. Freshness check (A2)
    freshest_h, oldest_h, median_h = _compute_freshness(items)
    max_fresh = THRESHOLDS.min_evidence_freshness_hours
    # Skip freshness check when no timestamps are available (freshest_h == -1)
    # This prevents MINIMAL mode from being blocked on freshness alone
    if freshest_h >= 0 and freshest_h > max_fresh:
        failures.append(f"stale_evidence: freshest={freshest_h:.1f}h > {max_fresh}h threshold")

    # Freshness score: 1.0 if very fresh, decays toward 0 as age approaches threshold
    # If no timestamps available, assume neutral (0.5) instead of penalizing
    if freshest_h < 0:
        freshness_score = 0.5
    else:
        freshness_score = max(0.0, 1.0 - (freshest_h / max(max_fresh, 1)))

    # Composite score (5 dimensions now)
    score = (
        diversity_score * 0.30 +
        (1.0 if has_non_social else 0.0) * 0.20 +
        (1.0 - contradiction_ratio) * 0.15 +
        avg_relevance * 0.15 +
        freshness_score * 0.20
    )

    return score, failures


# ─────────────────────────────────────────────────────────────────────
# DB persistence
# ─────────────────────────────────────────────────────────────────────

def _persist_evidence_pack(idea: IdeaCandidate, pack: EvidencePack, ctx: RunContext) -> None:
    """Write evidence pack to DB for audit."""
    if ctx.dry_run:
        return
    try:
        items_json = json.dumps([
            {
                "evidence_id": item.evidence_id,
                "type": item.evidence_type.value,
                "source": item.source,
                "headline": item.headline,
                "relevance": item.relevance_score,
                "sentiment": item.sentiment,
                "published_utc": item.published_utc,
            }
            for item in pack.items
        ])

        conn = get_connection()
        conn.execute("""
            INSERT INTO evidence_packs (
                run_id, ticker, thesis_id,
                total_sources, freshest_item_age_h,
                aggregate_sentiment, gate_score, gate_passed, items_json,
                created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ctx.run_id,
            idea.ticker,
            idea.thesis_id or "",
            pack.total_sources,
            pack.freshest_item_age_hours,
            pack.aggregate_sentiment,
            pack.gate_score,
            int(pack.gate_passed),
            items_json,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist evidence pack for {idea.ticker}: {e}")


def _persist_source_backlinks(
    idea: IdeaCandidate,
    external_items: List[EvidenceItem],
    ctx: RunContext,
) -> None:
    """B2: Persist source → evidence/thesis backlinks for replay analysis."""
    if ctx.dry_run or not external_items:
        return
    try:
        conn = get_connection()
        now = datetime.now(timezone.utc).isoformat()
        for item in external_items:
            raw = item.raw_data or {}
            conn.execute("""
                INSERT INTO evidence_source_links (
                    run_id, ticker, thesis_id, source_name,
                    source_item_id, source_tier, relation, created_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ctx.run_id,
                idea.ticker,
                idea.thesis_id or "",
                item.source,
                item.evidence_id,
                raw.get("_tier_weight", 1) if isinstance(raw.get("_tier_weight"), (int, float)) else 3,
                item.sentiment or "neutral",
                now,
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to persist source backlinks for {idea.ticker}: {e}")


# ─────────────────────────────────────────────────────────────────────
# Main gate
# ─────────────────────────────────────────────────────────────────────

def evaluate_evidence(idea: IdeaCandidate, ctx: RunContext) -> IdeaCandidate:
    """
    Evaluate evidence quality for a single idea.
    Sets idea.evidence_pack with gate_score, gate_passed, and failure reasons.
    """
    if not FLAGS.enable_evidence_gate:
        return idea

    # Build internal evidence from idea text/catalyst/flow
    items = _build_evidence_items(idea)

    # A1: Merge external source adapter items
    external = _build_external_evidence(idea, ctx)
    items.extend(external)

    score, failures = _score_evidence(items)

    # Determine aggregate sentiment
    sentiments = [item.sentiment for item in items if item.sentiment]
    supportive = sum(1 for s in sentiments if s in ("supportive", "positive", "bullish"))
    negative = sum(1 for s in sentiments if s in ("negative", "contradictory", "bearish"))
    if supportive > negative:
        agg_sentiment = "supportive"
    elif negative > supportive:
        agg_sentiment = "contradictory"
    else:
        agg_sentiment = "mixed"

    passed = score >= THRESHOLDS.evidence_gate_pass_score and len(failures) == 0

    # A2: Compute real freshness
    freshest_h, _, _ = _compute_freshness(items)

    pack = EvidencePack(
        ticker=idea.ticker,
        items=items,
        total_sources=len(set(item.evidence_type for item in items)),
        freshest_item_age_hours=round(freshest_h, 2),
        aggregate_sentiment=agg_sentiment,
        gate_score=round(score, 4),
        gate_passed=passed,
    )

    idea.evidence_pack = pack
    _persist_evidence_pack(idea, pack, ctx)
    _persist_source_backlinks(idea, external, ctx)

    if not passed:
        logger.info(
            f"  [{idea.ticker}] Evidence gate FAILED (score={score:.3f}): "
            f"{'; '.join(failures)}"
        )
    else:
        logger.info(
            f"  [{idea.ticker}] Evidence gate PASSED (score={score:.3f}, "
            f"{len(items)} items, {pack.total_sources} types, "
            f"freshest={freshest_h:.1f}h, ext={len(external)})"
        )

    return idea


def run_evidence_gate(
    ideas: List[IdeaCandidate], ctx: RunContext
) -> Tuple[List[IdeaCandidate], List[IdeaCandidate]]:
    """
    Run evidence gate on all ideas.
    Returns (passed, failed).
    """
    if not FLAGS.enable_evidence_gate:
        logger.info("[evidence_gate] Disabled — all ideas pass")
        return ideas, []

    logger.info(f"[run_id={ctx.run_id}] Evidence gate for {len(ideas)} ideas")

    passed, failed = [], []
    for idea in ideas:
        evaluated = evaluate_evidence(idea, ctx)
        if evaluated.evidence_pack and evaluated.evidence_pack.gate_passed:
            passed.append(evaluated)
        else:
            failed.append(evaluated)

    logger.info(f"[evidence_gate] {len(passed)} passed, {len(failed)} failed")
    ctx.mark_stage("evidence_gate")
    return passed, failed
