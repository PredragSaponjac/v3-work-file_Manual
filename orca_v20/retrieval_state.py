"""
ORCA v20 — Retrieval State Manager.

Manages evidence cache, source health, and regime context.
Central place for "what do we know right now" state.

Phase 1: scaffold.
Phase 3: implementation.
"""

import logging
from typing import Any, Dict, Optional

from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.retrieval_state")


class RetrievalState:
    """
    Holds ephemeral state for the current pipeline run.
    Populated by early stages, consumed by later stages.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._cache: Dict[str, Any] = {}
        self._source_health: Dict[str, str] = {}  # source_name → "healthy" / "degraded" / "down"

    def set(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default)

    def mark_source_health(self, source: str, status: str) -> None:
        """Record source health (healthy/degraded/down)."""
        self._source_health[source] = status
        if status != "healthy":
            logger.warning(f"Source '{source}' marked as {status}")

    def get_source_health(self, source: str) -> str:
        return self._source_health.get(source, "unknown")

    def is_source_available(self, source: str) -> bool:
        return self.get_source_health(source) in ("healthy", "unknown")

    def summary(self) -> Dict:
        return {
            "cached_keys": list(self._cache.keys()),
            "source_health": dict(self._source_health),
        }
