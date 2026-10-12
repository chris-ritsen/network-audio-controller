from __future__ import annotations

import asyncio
import signal
import time

POLL_SECONDS = 0.05


def ignore_shutdown_signals() -> None:
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, signal.SIG_IGN)


async def wait_for_exit(process, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while process.is_alive():
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(POLL_SECONDS)
    process.join(0)
    return True
