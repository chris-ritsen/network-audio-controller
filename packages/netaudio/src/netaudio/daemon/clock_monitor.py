from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from netaudio.dante.clock_control import clock_record_revision, clock_status_fresh

logger = logging.getLogger("netaudio")


class ClockStatusMonitor:
    """Keep direct clock observations fresh without duplicating unsolicited publications."""

    def __init__(self, application, *, interval: float = 2.0, concurrency: int = 8):
        self.application = application
        self.interval = interval
        self.concurrency = concurrency

    async def _poll_device(self, device) -> None:
        if not device.online or device.requires_managed_control or not device.ipv4:
            return
        snapshot = {"clock_status": device.clock_status, "clock_observed_at": device.clock_observed_at}
        now = datetime.now(timezone.utc)
        if clock_status_fresh(snapshot, now):
            observed = datetime.fromisoformat(device.clock_observed_at.replace("Z", "+00:00"))
            if (now - observed).total_seconds() < self.interval:
                return
        try:
            clock_record_revision(device)
        except (RuntimeError, ValueError):
            return
        try:
            await self.application.probe_clocking_status(device, timeout=1.0)
        except (RuntimeError, ValueError, OSError) as exception:
            if device.clock_observed_at is not None:
                device.clock_observed_at = None
                self.application.state._emit_device_updated(device)
            logger.debug("Clock readback unavailable for %s: %s", device.server_name, exception)

    async def poll_once(self) -> None:
        devices = list(self.application.devices.values())
        for start in range(0, len(devices), self.concurrency):
            await asyncio.gather(*(self._poll_device(device) for device in devices[start : start + self.concurrency]))

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            await self.poll_once()
