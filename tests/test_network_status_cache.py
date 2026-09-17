from unittest.mock import AsyncMock

import pytest

from netaudio.daemon.network_cache import NetworkStatusCache
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device import DanteDevice
from tests.test_daemon_discovery import _DiscoveryHarness


def device(name="desk.local.", ip="192.0.2.10", port=4440):
    result = DanteDevice(name)
    result.ipv4 = ip
    result.services = {"arc": {"type": SERVICE_ARC, "port": port}}
    return result


def observed():
    result = device()
    result.link_speed_mbps = 1000
    result.interfaces = [{"mode": "dynamic", "ip_address": "192.0.2.10", "link_speed_mbps": 1000}]
    return result


def test_cache_is_available_until_restart():
    cache = NetworkStatusCache()
    cache.remember(observed())
    restored = device()
    assert cache.restore(restored)
    assert restored.link_speed_mbps == 1000
    assert restored.interfaces == observed().interfaces
    assert not NetworkStatusCache().restore(device())


@pytest.mark.parametrize("target", [device("another.local."), device(ip="192.0.2.11"), device(port=4540)])
def test_cache_does_not_cross_control_endpoints(tmp_path, target):
    cache = NetworkStatusCache()
    cache.remember(observed())
    assert not cache.restore(target)
    assert target.link_speed_mbps is None


def test_fresh_observation_replaces_saved_speed_and_restore_never_overwrites_live_data(tmp_path):
    cache = NetworkStatusCache()
    fresh = observed()
    cache.remember(fresh)
    fresh.link_speed_mbps = 100
    fresh.interfaces[0]["link_speed_mbps"] = 100
    assert not cache.restore(fresh)
    cache.remember(fresh)
    restored = device()
    cache.restore(restored)
    assert restored.link_speed_mbps == 100
    assert restored.interfaces[0]["link_speed_mbps"] == 100


def test_cache_evicts_least_recently_used_endpoint():
    cache = NetworkStatusCache(max_devices=2)
    first, second, third = observed(), observed(), observed()
    second.server_name = "second.local."
    third.server_name = "third.local."
    cache.remember(first)
    cache.remember(second)
    assert cache.restore(device())
    cache.remember(third)
    assert len(cache.records) == 2
    assert not cache.restore(device("second.local."))
    assert cache.restore(device())


@pytest.mark.asyncio
async def test_discovery_restores_status_before_populating_controls(tmp_path):
    cache = NetworkStatusCache()
    cache.remember(observed())
    restored = device()
    harness = _DiscoveryHarness({restored.server_name: restored})
    harness.network_status_cache = cache
    assert await harness._refresh_arc_device(restored, restored.server_name, True)
    assert restored.link_speed_mbps == 1000
    assert restored.interfaces == observed().interfaces
    assert harness.spawned == [f"delayed-controls:{restored.server_name}"]


@pytest.mark.asyncio
async def test_restored_interfaces_skip_startup_probe(tmp_path):
    cache = NetworkStatusCache()
    cache.remember(observed())
    restored = device()
    cache.restore(restored)
    harness = _DiscoveryHarness({restored.server_name: restored})
    state = harness.application.state
    restored.name = "Desk"
    restored.tx_count = 0
    restored.supported_sample_rates = [48000]
    restored.supported_encodings = [24]
    restored.supported_gain_levels = []
    restored.fetch_controls_data = AsyncMock(return_value={})
    state._probe_with_retries = AsyncMock()
    state._refresh_clock_status = AsyncMock()
    state._refresh_lock_status = AsyncMock()
    harness.application.probe_interface_status = AsyncMock()
    await state.fetch_device_controls(restored.server_name)
    harness.application.probe_interface_status.assert_not_called()
