"""
ORCA v20 — Model router (Phase 4 hardened).

Thin abstraction over Anthropic / OpenAI / Google APIs with:
    - Retries with exponential backoff (max 2 retries)
    - Provider fallback chain (if primary fails, try secondary)
    - Configurable timeouts per provider
    - Role-level cost accounting
    - Provider health tracking

All LLM calls in v20 go through call_model() which:
    1. Resolves role → ModelSpec via config.ROUTING
    2. Routes to the correct provider SDK
    3. Retries on transient failures
    4. Falls back to alternate provider if primary is unhealthy
    5. Tracks cost in RunContext (total + per-role)
    6. Returns a uniform ModelResponse
"""

import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import orca_v20.config as _cfg  # ALL config reads must be late-bound
# FIX: Do NOT import MODELS/ROUTING at module level — they become stale
# after importlib.reload(config) in budget mode. Read via _cfg.* at call time.
from orca_v20.config import ModelSpec
from orca_v20.run_context import RunContext

logger = logging.getLogger("orca_v20.router")


# ─────────────────────────────────────────────────────────────────────
# Response / tracking types
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ModelResponse:
    """Uniform response from any LLM provider."""
    content: str = ""
    thinking: str = ""
    model_id: str = ""
    provider: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    thinking_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    retries_used: int = 0
    fallback_used: bool = False
    raw_response: Optional[Any] = None


# Provider health tracking (module-level singleton)
_provider_health: Dict[str, Dict] = defaultdict(lambda: {
    "consecutive_failures": 0,
    "last_failure_time": 0.0,
    "total_calls": 0,
    "total_failures": 0,
})

# Role-level cost accounting (module-level singleton)
_role_costs: Dict[str, float] = defaultdict(float)

# Retry / fallback config
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 1.5  # seconds
PROVIDER_UNHEALTHY_THRESHOLD = 3  # consecutive failures before marking unhealthy
PROVIDER_RECOVERY_SECS = 120  # seconds before retrying an unhealthy provider

# Fallback chains: if primary fails, try these in order
_FALLBACK_CHAINS = {
    "anthropic": ["openai", "google"],
    "openai": ["anthropic", "google"],
    "google": ["anthropic", "openai"],
}

# Fallback model keys by provider (cheapest suitable model)
_FALLBACK_MODELS = {
    "anthropic": "claude-sonnet",
    "openai": "gpt-fast",
    "google": "gemini-pro",
}


def _is_provider_healthy(provider: str) -> bool:
    """Check if a provider is considered healthy."""
    health = _provider_health[provider]
    if health["consecutive_failures"] < PROVIDER_UNHEALTHY_THRESHOLD:
        return True
    # Check if enough time has passed for recovery attempt
    elapsed = time.time() - health["last_failure_time"]
    return elapsed > PROVIDER_RECOVERY_SECS


def _record_success(provider: str) -> None:
    """Record a successful call."""
    health = _provider_health[provider]
    health["consecutive_failures"] = 0
    health["total_calls"] += 1


def _record_failure(provider: str) -> None:
    """Record a failed call."""
    health = _provider_health[provider]
    health["consecutive_failures"] += 1
    health["last_failure_time"] = time.time()
    health["total_calls"] += 1
    health["total_failures"] += 1


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

def call_model(
    role: str,
    system_prompt: str,
    user_prompt: str,
    ctx: RunContext,
    *,
    messages: Optional[List[Dict]] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    thinking_budget: Optional[int] = None,
    json_mode: bool = False,
) -> ModelResponse:
    """
    Route an LLM call through the v20 model router.

    Phase 4 hardened: retries + fallback + role-level cost tracking.
    Supports MANUAL_MODE: when enabled, saves prompt to disk and waits
    for a human to paste the LLM response into a file.
    """
    spec = _cfg.ROUTING.get_model(role)  # late-bound: sees budget routing after reload

    # ── MANUAL MODE: bypass all API calls ──
    if getattr(_cfg, "MANUAL_MODE", False):
        return _call_manual(role, system_prompt, user_prompt, messages, spec, ctx)

    # Budget check
    if ctx.is_over_budget():
        logger.warning(f"[{role}] Over budget (${ctx.api_cost_usd:.2f}), skipping call")
        return ModelResponse(
            content="[SKIPPED — over budget]",
            model_id=spec.model_id,
            provider=spec.provider,
        )

    logger.info(f"[{role}] Routing to {spec.provider}/{spec.model_id}")

    t0 = time.time()

    # Try primary with retries
    response = _call_with_retries(
        spec, system_prompt, user_prompt, messages,
        temperature, max_tokens, thinking_budget, json_mode, role
    )

    # If primary failed, try fallback chain.
    # Detect failure by checking for error marker prefix OR empty content.
    _primary_failed = (
        (response.content.startswith("[") and ("error" in response.content.lower()
                                               or "skipped" in response.content.lower()
                                               or "not installed" in response.content.lower()
                                               or "no " in response.content.lower()))
        or not response.content.strip()
    )
    if _primary_failed:
        fallback_providers = _FALLBACK_CHAINS.get(spec.provider, [])
        for fb_provider in fallback_providers:
            if not _is_provider_healthy(fb_provider):
                continue
            fb_model_key = _FALLBACK_MODELS.get(fb_provider)
            if fb_model_key and fb_model_key in _cfg.MODELS:
                fb_spec = _cfg.MODELS[fb_model_key]
                logger.warning(
                    f"[{role}] Primary {spec.provider} failed — "
                    f"falling back to {fb_provider}/{fb_spec.model_id}"
                )
                response = _call_with_retries(
                    fb_spec, system_prompt, user_prompt, messages,
                    temperature, max_tokens, 0, json_mode, role  # no thinking on fallback
                )
                response.fallback_used = True
                if not response.content.startswith("["):
                    break  # fallback succeeded

    response.latency_ms = int((time.time() - t0) * 1000)

    # Track cost (total + role-level)
    ctx.add_cost(response.cost_usd)
    _role_costs[role] += response.cost_usd

    logger.info(
        f"[{role}] Done — {response.input_tokens}in/{response.output_tokens}out "
        f"${response.cost_usd:.4f} ({response.latency_ms}ms)"
        + (f" [retries={response.retries_used}]" if response.retries_used else "")
        + (" [FALLBACK]" if response.fallback_used else "")
    )

    # ── Budget intelligence logging ──
    if _cfg.BUDGET_MODE and response.content and not response.content.startswith("["):
        _log_budget_intelligence(
            run_id=ctx.run_id,
            role=role,
            model_used=response.model_id,
            provider=response.provider,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            cost_usd=response.cost_usd,
            latency_ms=response.latency_ms,
            raw_output=response.content[:10000],
        )

    return response


def _call_manual(
    role: str,
    system_prompt: str,
    user_prompt: str,
    messages: Optional[List[Dict]],
    spec: ModelSpec,
    ctx: RunContext,
) -> ModelResponse:
    """
    Manual mode: save prompt to file, wait for human response.

    Returns a ModelResponse with the human-pasted content.
    """
    from orca_v20.manual_bridge import manual_llm_call

    model_hint = f"{spec.provider}/{spec.model_id}"
    logger.info(f"[{role}] MANUAL MODE — saving prompt (hint: {model_hint})")

    t0 = time.time()

    content = manual_llm_call(
        role=role,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model_hint=model_hint,
        messages=messages,
    )

    latency_ms = int((time.time() - t0) * 1000)

    logger.info(
        f"[{role}] MANUAL response received — {len(content)} chars ({latency_ms}ms wait)"
    )

    return ModelResponse(
        content=content,
        model_id=f"manual:{spec.model_id}",
        provider="manual",
        latency_ms=latency_ms,
    )


def _call_with_retries(
    spec: ModelSpec,
    system_prompt: str,
    user_prompt: str,
    messages: Optional[List[Dict]],
    temperature: Optional[float],
    max_tokens: Optional[int],
    thinking_budget: Optional[int],
    json_mode: bool,
    role: str,
) -> ModelResponse:
    """Call a provider with exponential backoff retries."""

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        if attempt > 0:
            backoff = RETRY_BACKOFF_BASE ** attempt
            logger.info(f"[{role}] Retry {attempt}/{MAX_RETRIES} after {backoff:.1f}s")
            time.sleep(backoff)

        try:
            if spec.provider == "anthropic":
                response = _call_anthropic(spec, system_prompt, user_prompt, messages,
                                           temperature, max_tokens, thinking_budget, json_mode)
            elif spec.provider == "openai":
                response = _call_openai(spec, system_prompt, user_prompt, messages,
                                        temperature, max_tokens, json_mode)
            elif spec.provider == "google":
                response = _call_google(spec, system_prompt, user_prompt, messages,
                                        temperature, max_tokens, json_mode)
            else:
                raise ValueError(f"Unknown provider: {spec.provider}")

            # Check if it's an error response
            if not response.content.startswith("["):
                _record_success(spec.provider)
                response.retries_used = attempt
                return response

            # SDK returned but with error content — might be transient
            if attempt < MAX_RETRIES and "rate" in response.content.lower():
                last_error = response.content
                continue
            else:
                response.retries_used = attempt
                return response

        except Exception as e:
            last_error = str(e)
            _record_failure(spec.provider)
            logger.warning(f"[{role}] Attempt {attempt+1} failed: {e}")
            if attempt == MAX_RETRIES:
                return ModelResponse(
                    content=f"[{spec.provider} error after {MAX_RETRIES+1} attempts: {str(e)[:200]}]",
                    model_id=spec.model_id,
                    provider=spec.provider,
                    retries_used=attempt,
                )

    # Should not reach here, but safety net
    return ModelResponse(
        content=f"[{spec.provider} exhausted retries: {str(last_error)[:200]}]",
        model_id=spec.model_id,
        provider=spec.provider,
        retries_used=MAX_RETRIES,
    )


# ─────────────────────────────────────────────────────────────────────
# Anthropic SDK
# ─────────────────────────────────────────────────────────────────────

def _call_anthropic(
    spec: ModelSpec,
    system_prompt: str,
    user_prompt: str,
    messages: Optional[List[Dict]],
    temperature: Optional[float],
    max_tokens: Optional[int],
    thinking_budget: Optional[int],
    json_mode: bool,
) -> ModelResponse:
    """Call Anthropic Claude API via anthropic SDK."""
    try:
        import anthropic
    except ImportError:
        return ModelResponse(
            content="[anthropic SDK not installed]",
            model_id=spec.model_id,
            provider="anthropic",
        )

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ModelResponse(
            content="[no ANTHROPIC_API_KEY]",
            model_id=spec.model_id,
            provider="anthropic",
        )

    client = anthropic.Anthropic(api_key=api_key, timeout=spec.timeout_s)

    msgs = messages or [{"role": "user", "content": user_prompt}]
    tb = thinking_budget if thinking_budget is not None else spec.thinking_budget
    temp = temperature if temperature is not None else spec.temperature
    mt = max_tokens if max_tokens is not None else spec.max_tokens

    kwargs = {
        "model": spec.model_id,
        "max_tokens": mt,
        "system": system_prompt,
        "messages": msgs,
    }

    if tb and tb > 0:
        kwargs["temperature"] = 1.0
        kwargs["thinking"] = {"type": "enabled", "budget_tokens": tb}
    else:
        kwargs["temperature"] = temp

    response = client.messages.create(**kwargs)

    content = ""
    thinking = ""
    for block in response.content:
        if block.type == "thinking":
            thinking += block.thinking
        elif block.type == "text":
            content += block.text

    usage = response.usage
    input_tok = usage.input_tokens
    output_tok = usage.output_tokens
    cost = estimate_cost(spec, input_tok, output_tok)

    return ModelResponse(
        content=content,
        thinking=thinking,
        model_id=spec.model_id,
        provider="anthropic",
        input_tokens=input_tok,
        output_tokens=output_tok,
        thinking_tokens=len(thinking.split()) if thinking else 0,
        cost_usd=cost,
        raw_response=response,
    )


# ─────────────────────────────────────────────────────────────────────
# OpenAI SDK
# ─────────────────────────────────────────────────────────────────────

def _call_openai(
    spec: ModelSpec,
    system_prompt: str,
    user_prompt: str,
    messages: Optional[List[Dict]],
    temperature: Optional[float],
    max_tokens: Optional[int],
    json_mode: bool,
) -> ModelResponse:
    """Call OpenAI GPT API via openai SDK."""
    try:
        import openai
    except ImportError:
        return ModelResponse(
            content="[openai SDK not installed]",
            model_id=spec.model_id,
            provider="openai",
        )

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return ModelResponse(
            content="[no OPENAI_API_KEY]",
            model_id=spec.model_id,
            provider="openai",
        )

    client = openai.OpenAI(api_key=api_key, timeout=spec.timeout_s)

    if not messages:
        msgs = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    else:
        msgs = messages

    temp = temperature if temperature is not None else spec.temperature
    mt = max_tokens if max_tokens is not None else spec.max_tokens

    kwargs = {
        "model": spec.model_id,
        "messages": msgs,
        "temperature": temp,
        "max_tokens": mt,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)

    content = response.choices[0].message.content or ""
    usage = response.usage
    input_tok = usage.prompt_tokens if usage else 0
    output_tok = usage.completion_tokens if usage else 0
    cost = estimate_cost(spec, input_tok, output_tok)

    return ModelResponse(
        content=content,
        model_id=spec.model_id,
        provider="openai",
        input_tokens=input_tok,
        output_tokens=output_tok,
        cost_usd=cost,
        raw_response=response,
    )


# ─────────────────────────────────────────────────────────────────────
# Google Gemini SDK
# ─────────────────────────────────────────────────────────────────────

def _call_google(
    spec: ModelSpec,
    system_prompt: str,
    user_prompt: str,
    messages: Optional[List[Dict]],
    temperature: Optional[float],
    max_tokens: Optional[int],
    json_mode: bool,
) -> ModelResponse:
    """Call Google Gemini API via google-generativeai SDK."""
    try:
        import google.generativeai as genai
    except ImportError:
        return ModelResponse(
            content="[google-generativeai SDK not installed]",
            model_id=spec.model_id,
            provider="google",
        )

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return ModelResponse(
            content="[no GEMINI_API_KEY]",
            model_id=spec.model_id,
            provider="google",
        )

    genai.configure(api_key=api_key)

    temp = temperature if temperature is not None else spec.temperature
    mt = max_tokens if max_tokens is not None else spec.max_tokens

    model = genai.GenerativeModel(
        model_name=spec.model_id,
        system_instruction=system_prompt,
        generation_config=genai.types.GenerationConfig(
            temperature=temp,
            max_output_tokens=mt,
        ),
    )

    prompt = user_prompt
    if messages:
        parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            parts.append(f"[{role}]: {content}")
        prompt = "\n".join(parts)

    response = model.generate_content(prompt)
    content = response.text if response.text else ""

    input_tok = 0
    output_tok = 0
    try:
        if hasattr(response, 'usage_metadata'):
            input_tok = getattr(response.usage_metadata, 'prompt_token_count', 0) or 0
            output_tok = getattr(response.usage_metadata, 'candidates_token_count', 0) or 0
    except Exception:
        pass

    cost = estimate_cost(spec, input_tok, output_tok)

    return ModelResponse(
        content=content,
        model_id=spec.model_id,
        provider="google",
        input_tokens=input_tok,
        output_tokens=output_tok,
        cost_usd=cost,
        raw_response=response,
    )


# ─────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────

def estimate_cost(spec: ModelSpec, input_tokens: int, output_tokens: int) -> float:
    """Estimate cost for a call."""
    return (
        (input_tokens / 1000) * spec.cost_per_1k_input
        + (output_tokens / 1000) * spec.cost_per_1k_output
    )


def list_available_models() -> Dict[str, str]:
    """Return {key: provider/model_id} for all registered models."""
    return {k: f"{v.provider}/{v.model_id}" for k, v in _cfg.MODELS.items()}


def list_role_assignments() -> Dict[str, str]:
    """Return {role: model_key} for current routing."""
    return {
        field: getattr(_cfg.ROUTING, field)
        for field in _cfg.ROUTING.__dataclass_fields__
    }


def get_role_costs() -> Dict[str, float]:
    """Return accumulated cost per role for the current session."""
    return dict(_role_costs)


def get_provider_health() -> Dict[str, Dict]:
    """Return provider health status."""
    return dict(_provider_health)


def reset_session_stats() -> None:
    """Reset role costs and provider health (for testing)."""
    _role_costs.clear()
    _provider_health.clear()


def get_session_summary() -> Dict:
    """
    Full session summary for operator review (Phase 5).
    Returns role costs, provider health, and total cost.
    """
    role_costs = dict(_role_costs)
    provider_health = {}
    for provider, health in _provider_health.items():
        provider_health[provider] = {
            "total_calls": health["total_calls"],
            "total_failures": health["total_failures"],
            "consecutive_failures": health["consecutive_failures"],
            "healthy": _is_provider_healthy(provider),
        }

    return {
        "role_costs": role_costs,
        "total_cost": round(sum(role_costs.values()), 4),
        "provider_health": provider_health,
        "roles_used": len(role_costs),
    }


def _log_budget_intelligence(
    run_id: str, role: str, model_used: str, provider: str,
    input_tokens: int, output_tokens: int, cost_usd: float,
    latency_ms: int, raw_output: str, ticker: str = "",
    idea_id: str = "", thesis_id: str = "", stage: str = "",
) -> None:
    """Log every budget-mode model call to budget_intelligence_log for audit."""
    try:
        from orca_v20.db_bootstrap import get_connection
        from datetime import datetime, timezone
        conn = get_connection()
        conn.execute("""
            INSERT INTO budget_intelligence_log
            (run_id, timestamp_utc, pipeline_stage, role, model_used, provider,
             ticker, idea_id, thesis_id, input_tokens, output_tokens,
             cost_usd, latency_ms, raw_output)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, datetime.now(timezone.utc).isoformat(),
            stage or role, role, model_used, provider,
            ticker, idea_id, thesis_id,
            input_tokens, output_tokens, cost_usd, latency_ms,
            raw_output,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"[budget_log] Failed: {e}")


def log_session_summary() -> None:
    """Log a human-readable session summary."""
    summary = get_session_summary()
    logger.info("─── Router Session Summary ───")
    logger.info(f"  Total cost: ${summary['total_cost']:.4f}")
    logger.info(f"  Roles used: {summary['roles_used']}")
    for role, cost in sorted(summary["role_costs"].items(), key=lambda x: -x[1]):
        logger.info(f"    {role}: ${cost:.4f}")
    for provider, health in summary["provider_health"].items():
        status = "✓" if health["healthy"] else "✗"
        logger.info(
            f"  [{status}] {provider}: {health['total_calls']} calls, "
            f"{health['total_failures']} failures"
        )
