"""Stub: Telemetry / observability processor. Phase 4+."""

def emit_metric(name: str, value: float, tags: dict = None) -> None:
    """No-op metric emission. Future: Prometheus/Datadog integration."""
    pass

def emit_event(event_type: str, data: dict = None) -> None:
    """No-op event emission."""
    pass
