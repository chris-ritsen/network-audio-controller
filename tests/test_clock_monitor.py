from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.daemon.clock_monitor import ClockStatusMonitor
from netaudio.dante.device import DanteDevice


def clock_device(name="clock.local.", age=3):
    device = DanteDevice(name)
    device.online = True
    device.ipv4 = "192.0.2.1"
    device.clock_status = {"status_supported": True, "record_revision": 0x0738}
    device.clock_observed_at = (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()
    return device


@pytest.mark.asyncio
async def test_clock_monitor_uses_only_read_only_probes_and_skips_fresh_managed_or_offline_devices():
    direct = clock_device()
    fresh = clock_device("fresh.local.", age=0)
    managed = clock_device("managed.local.")
    managed.management_state = "managed"
    offline = clock_device("offline.local.")
    offline.online = False
    unknown = clock_device("unknown.local.")
    unknown.clock_status = None
    app = SimpleNamespace(
        devices={d.server_name: d for d in (direct, fresh, managed, offline, unknown)},
        probe_clocking_status=AsyncMock(),
    )
    await ClockStatusMonitor(app).poll_once()
    app.probe_clocking_status.assert_awaited_once_with(direct, timeout=1.0)


@pytest.mark.asyncio
async def test_clock_monitor_failure_marks_previous_observation_unavailable():
    device = clock_device()
    app = SimpleNamespace(
        devices={device.server_name: device},
        probe_clocking_status=AsyncMock(side_effect=OSError("timeout")),
        state=SimpleNamespace(_emit_device_updated=MagicMock()),
    )
    await ClockStatusMonitor(app).poll_once()
    assert device.clock_observed_at is None
    assert device.clock_status["record_revision"] == 0x0738
    app.state._emit_device_updated.assert_called_once_with(device)
    await ClockStatusMonitor(app).poll_once()
    assert app.probe_clocking_status.await_count == 2
