import json
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


def test_survives_restart_without_a_device_query(tmp_path):
    path = tmp_path / "network-status.json"
    NetworkStatusCache(path).remember(observed())
    restored = device()
    assert NetworkStatusCache(path).restore(restored)
    assert restored.link_speed_mbps == 1000
    assert restored.interfaces == observed().interfaces


@pytest.mark.parametrize("target", [device("another.local."), device(ip="192.0.2.11"), device(port=4540)])
def test_cache_does_not_cross_control_endpoints(tmp_path, target):
    cache = NetworkStatusCache(tmp_path / "network-status.json")
    cache.remember(observed())
    assert not cache.restore(target)
    assert target.link_speed_mbps is None


def test_fresh_observation_replaces_saved_speed_and_restore_never_overwrites_live_data(tmp_path):
    path = tmp_path / "network-status.json"
    cache = NetworkStatusCache(path)
    fresh = observed()
    cache.remember(fresh)
    fresh.link_speed_mbps = 100
    fresh.interfaces[0]["link_speed_mbps"] = 100
    assert not cache.restore(fresh)
    cache.remember(fresh)
    restored = device()
    NetworkStatusCache(path).restore(restored)
    assert restored.link_speed_mbps == 100
    assert restored.interfaces[0]["link_speed_mbps"] == 100


def test_unchanged_or_unavailable_values_do_not_rewrite_cache(tmp_path):
    path = tmp_path / "network-status.json"
    cache = NetworkStatusCache(path)
    cache.remember(observed())
    modified = path.stat().st_mtime_ns
    cache.remember(observed())
    cache.remember(device())
    assert path.stat().st_mtime_ns == modified


@pytest.mark.parametrize(
    "data",
    [
        "broken",
        "[]",
        "null",
        json.dumps({json.dumps(["desk.local.", "192.0.2.10", 4440]): {"link_speed_mbps": "fast", "interfaces": []}}),
    ],
)
def test_bad_cache_is_ignored(tmp_path, data):
    path = tmp_path / "network-status.json"
    path.write_text(data)
    assert not NetworkStatusCache(path).restore(device())


@pytest.mark.asyncio
async def test_discovery_restores_status_before_populating_controls(tmp_path):
    cache = NetworkStatusCache(tmp_path / "network-status.json")
    cache.remember(observed())
    restored = device()
    harness = _DiscoveryHarness({restored.server_name: restored})
    harness.network_status_cache = NetworkStatusCache(cache.path)
    assert await harness._refresh_arc_device(restored, restored.server_name, True)
    assert restored.link_speed_mbps == 1000
    assert restored.interfaces == observed().interfaces
    assert harness.spawned == [f"delayed-controls:{restored.server_name}"]


@pytest.mark.asyncio
async def test_restored_interfaces_skip_startup_probe(tmp_path):
    cache = NetworkStatusCache(tmp_path / "network-status.json")
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
