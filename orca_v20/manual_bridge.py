"""
ORCA v20 — Manual LLM Bridge.

Replaces API calls with a file-based prompt/response handoff for human-in-the-loop
operation. Designed for local runs orchestrated by Claude Code:

    1. Pipeline hits an LLM call
    2. Bridge writes the prompt to manual_prompts/{seq}_{role}_prompt.txt
    3. Bridge prints a notice and polls for manual_prompts/{seq}_{role}_response.txt
    4. Claude Code reads the prompt, shows it to the user, user pastes LLM response
    5. Claude Code writes the response file
    6. Bridge picks it up and returns to caller

No timeout by default — waits until the response file exists and is non-empty.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("orca_v20.manual_bridge")

# ─────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANUAL_PROMPTS_DIR = _PROJECT_ROOT / "manual_prompts"

# Hunter roles that should use the v5.3 MASTER prompt instead of v4.8
_HUNTER_ROLES = {"hunter_primary", "hunter_secondary", "hunter_tertiary"}
_R1_MASTER_V53_PATH = _PROJECT_ROOT / "prompts" / "r1_master_v5_3.txt"

# Module-level sequence counter (increments per call within a run)
_call_seq = 0


def _next_seq() -> int:
    """Return the next sequence number for this run."""
    global _call_seq
    _call_seq += 1
    return _call_seq


def reset_seq() -> None:
    """Reset sequence counter (call at start of each run)."""
    global _call_seq
    _call_seq = 0


# ─────────────────────────────────────────────────────────────────────
# Prompt / response file management
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ManualCall:
    """Represents a pending manual LLM call."""
    seq: int
    role: str
    model_hint: str
    prompt_file: str
    response_file: str


def save_prompt(
    role: str,
    system_prompt: str,
    user_prompt: str,
    model_hint: str = "",
    *,
    messages: Optional[list] = None,
) -> ManualCall:
    """
    Save a prompt to disk and return paths for the response.

    The prompt file contains a header with metadata + the full system/user prompt
    so the human can copy-paste it into their LLM subscription chat.
    """
    MANUAL_PROMPTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── v5.3 MASTER prompt override for hunter roles ──
    # The automated pipeline loads v4.8 as the system_prompt, but in manual
    # mode we want the improved v5.3 MASTER gating.  The user_prompt
    # (intelligence packet built by Stage 0 scrapers) stays unchanged.
    if role in _HUNTER_ROLES and _R1_MASTER_V53_PATH.exists():
        v53_text = _R1_MASTER_V53_PATH.read_text(encoding="utf-8")
        logger.info(
            f"[MANUAL] Overriding system prompt for {role}: "
            f"v4.8 ({len(system_prompt)} chars) → v5.3 MASTER ({len(v53_text)} chars)"
        )
        system_prompt = v53_text

    seq = _next_seq()
    ts = datetime.now().strftime("%H%M%S")
    safe_role = role.replace("/", "_").replace("\\", "_")

    prompt_fname = f"{seq:03d}_{safe_role}_prompt.txt"
    response_fname = f"{seq:03d}_{safe_role}_response.txt"

    prompt_path = MANUAL_PROMPTS_DIR / prompt_fname
    response_path = MANUAL_PROMPTS_DIR / response_fname

    # Build the prompt file content
    lines = []
    lines.append("=" * 72)
    lines.append(f"ORCA MANUAL LLM CALL — #{seq}")
    lines.append(f"Role: {role}")
    lines.append(f"Model hint: {model_hint or 'any'}")
    lines.append(f"Timestamp: {datetime.now().isoformat()}")
    lines.append("=" * 72)
    lines.append("")

    lines.append("─── SYSTEM PROMPT ───")
    lines.append(system_prompt or "(none)")
    lines.append("")
    lines.append("─── USER PROMPT ───")

    if messages:
        for msg in messages:
            r = msg.get("role", "user")
            c = msg.get("content", "")
            lines.append(f"[{r}]:")
            lines.append(c)
            lines.append("")
    else:
        lines.append(user_prompt or "(none)")

    lines.append("")
    lines.append("=" * 72)
    lines.append("PASTE THE LLM RESPONSE INTO THE CORRESPONDING _response.txt FILE")
    lines.append(f"Response file: {response_path.name}")
    lines.append("=" * 72)

    prompt_path.write_text("\n".join(lines), encoding="utf-8")

    call = ManualCall(
        seq=seq,
        role=role,
        model_hint=model_hint,
        prompt_file=str(prompt_path),
        response_file=str(response_path),
    )

    logger.info(
        f"[MANUAL #{seq}] Prompt saved: {prompt_fname} | "
        f"Waiting for: {response_fname} | Model hint: {model_hint or 'any'}"
    )

    # Print prominent notice for the orchestrator (Claude Code)
    print()
    print("=" * 60)
    print(f"  MANUAL LLM CALL #{seq} — {role}")
    print(f"  Model hint: {model_hint or 'any'}")
    print(f"  Prompt: {prompt_path}")
    print(f"  Response: {response_path}")
    print("  >>> Waiting for response file to be written <<<")
    print("=" * 60)
    print()

    return call


def wait_for_response(
    response_file: str,
    poll_interval: float = 2.0,
    timeout: Optional[float] = None,
) -> str:
    """
    Poll for the response file to exist and contain text.

    Args:
        response_file: Path to the response file to watch.
        poll_interval: Seconds between checks (default 2s).
        timeout: Maximum seconds to wait (None = wait forever).

    Returns:
        The text content of the response file.

    Raises:
        TimeoutError: If timeout is set and exceeded.
    """
    path = Path(response_file)
    start = time.time()
    check_count = 0

    while True:
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                logger.info(
                    f"[MANUAL] Response received: {path.name} "
                    f"({len(content)} chars, waited {time.time() - start:.1f}s)"
                )
                return content

        check_count += 1
        if check_count % 15 == 0:  # Log every 30s
            elapsed = time.time() - start
            logger.info(f"[MANUAL] Still waiting for {path.name} ({elapsed:.0f}s elapsed)")

        if timeout is not None and (time.time() - start) > timeout:
            raise TimeoutError(
                f"Manual response not received within {timeout}s: {response_file}"
            )

        time.sleep(poll_interval)


def manual_llm_call(
    role: str,
    system_prompt: str,
    user_prompt: str,
    model_hint: str = "",
    *,
    messages: Optional[list] = None,
    poll_interval: float = 2.0,
    timeout: Optional[float] = None,
) -> str:
    """
    Complete manual LLM call: save prompt, wait for response, return text.

    This is the all-in-one function for code that just wants a string back.
    """
    call = save_prompt(
        role=role,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model_hint=model_hint,
        messages=messages,
    )
    return wait_for_response(
        call.response_file,
        poll_interval=poll_interval,
        timeout=timeout,
    )


def clean_prompts_dir() -> int:
    """Remove all files in manual_prompts/. Returns count of files removed."""
    if not MANUAL_PROMPTS_DIR.exists():
        return 0
    count = 0
    for f in MANUAL_PROMPTS_DIR.iterdir():
        if f.is_file() and f.name != ".gitkeep":
            f.unlink()
            count += 1
    return count
