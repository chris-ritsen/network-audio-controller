from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from netaudio import core

MUTATION_ERRORS = (OSError, RuntimeError, ValueError)


async def read_audio_capability(application, device, kind: str) -> dict:
    from netaudio.dante.state import apply_audio_capability

    if kind not in {"encoding", "sample_rate", "sample_rate_pullup"}:
        raise ValueError("unsupported audio capability")

    status = await getattr(application, f"probe_{kind}_status")(device)
    apply_audio_capability(device, status, kind=kind)

    return status


@dataclass(frozen=True)
class ReadbackResult:
    matched: bool
    observed: Any = None
    observed_available: bool = False
    error: Exception | None = None


def audio_readback_result(status: dict | None, expected: int) -> ReadbackResult:
    result = core.audio_capability_readback(status, expected)

    return ReadbackResult(
        matched=result["effective_state_confirmed"],
        observed=result["current_value"],
        observed_available=result["state"] != "unavailable",
    )


async def readback_after_notification(
    read: Callable[[], Awaitable[Any]],
    expected: Any,
) -> ReadbackResult:
    try:
        observed = await read()
    except MUTATION_ERRORS as exception:
        return ReadbackResult(matched=False, error=exception)

    return ReadbackResult(
        matched=observed == expected,
        observed=observed,
        observed_available=True,
    )
