import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.common.app_config import settings as app_settings
from netaudio.daemon.server import NetaudioDaemon, _probe_device, _stale_device_minutes_from_config
from netaudio.dante.device import DanteDevice


def _daemon(devices):
    daemon = object.__new__(NetaudioDaemon)
    daemon.application = SimpleNamespace(
        devices=devices,
        unregister_device=MagicMock(side_effect=lambda server_name: devices.pop(server_name, None)),
    )
    daemon._offline_failures = {}
    daemon._offline_candidate_since = {}
    daemon._pending_offline_tasks = {}
    daemon._unreachable_devices_reported = set()
    daemon._background_tasks = set()
    daemon._last_status_field_refresh_monotonic = time.monotonic()
    daemon.running = True
    daemon.state = SimpleNamespace()
    return daemon


def _device(server_name, *, age_seconds, online=False):
    device = DanteDevice(server_name=server_name)
    device.ipv4 = "192.0.2.10"
    device.online = online
    device.last_seen = time.time() - age_seconds
    return device


def test_reachability_uses_advertised_port_and_only_requires_device_name(monkeypatch):
    from netaudio import core

    names = {4440: "Windows-PC", 4540: "studio-media-b", 4640: "studio-media-c"}
    requests = []

    class Client:
        def __init__(self, ip, *, arc_port, timeout_ms, attempts, local_ip=None):
            self.port = arc_port
            requests.append((ip, arc_port))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def get_device_name(self):
            return names[self.port]

        def get_device_info(self):
            raise AssertionError("An unsupported full-info query must not mark a responsive device offline")

    monkeypatch.setattr(core, "CoreClient", Client)
    for port in names:
        assert _probe_device("192.0.2.10", port)
    assert requests == [("192.0.2.10", port) for port in names]


@pytest.mark.asyncio
async def test_online_checks_do_not_confuse_devices_sharing_an_ip(monkeypatch):
    from netaudio.dante.const import SERVICE_ARC

    devices = {}
    for port in (4440, 4540, 4640):
        device = _device(f"device-{port}.local.", age_seconds=600, online=True)
        device.services = {f"arc-{port}": {"type": SERVICE_ARC, "port": port}}
        devices[device.server_name] = device
    daemon = _daemon(devices)
    daemon.mark_device_offline_verified = MagicMock()
    probe = MagicMock(side_effect=lambda _ip, port: port != 4640)
    monkeypatch.setattr("netaudio.daemon.server._probe_device", probe)
    for name in devices:
        await daemon._verify_quiet_online_device(name)
    assert [call.args for call in probe.call_args_list] == [("192.0.2.10", port) for port in (4440, 4540, 4640)]
    daemon.mark_device_offline_verified.assert_called_once_with("device-4640.local.", reason="active_revalidation")


def test_expire_stale_devices_forgets_devices_unseen_for_the_configured_window(monkeypatch):
    monkeypatch.setattr(app_settings, "stale_device_minutes", 60)
    devices = {
        "fresh.local.": _device("fresh.local.", age_seconds=10 * 60),
        "stale.local.": _device("stale.local.", age_seconds=61 * 60),
        "stale-online.local.": _device("stale-online.local.", age_seconds=90 * 60, online=True),
    }
    never_seen = DanteDevice(server_name="never.local.")
    never_seen.last_seen = None
    devices["never.local."] = never_seen
    daemon = _daemon(devices)
    daemon._offline_failures["stale.local."] = 3

    expired = daemon._expire_stale_devices()

    assert expired == ["stale.local.", "stale-online.local."]
    assert set(devices) == {"fresh.local.", "never.local."}
    assert daemon._offline_failures == {}


def test_expire_stale_devices_is_disabled_by_zero_minutes(monkeypatch):
    monkeypatch.setattr(app_settings, "stale_device_minutes", 0)
    devices = {"stale.local.": _device("stale.local.", age_seconds=10 * 24 * 3600)}
    daemon = _daemon(devices)

    assert daemon._expire_stale_devices() == []
    assert "stale.local." in devices


def test_forget_device_clears_offline_tracking_and_unregisters():
    devices = {"ghost.local.": _device("ghost.local.", age_seconds=0)}
    daemon = _daemon(devices)
    daemon._offline_failures["ghost.local."] = 2
    daemon._offline_candidate_since["ghost.local."] = time.monotonic()

    daemon.forget_device("ghost.local.")

    assert devices == {}
    assert daemon._offline_failures == {}
    assert daemon._offline_candidate_since == {}
    daemon.application.unregister_device.assert_called_once_with("ghost.local.")


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({}, 60),
        ({"stale_device_minutes": 15}, 15.0),
        ({"stale_device_minutes": 2.5}, 2.5),
        ({"stale_device_minutes": 0}, 0.0),
    ],
)
def test_stale_device_minutes_from_config(monkeypatch, config, expected):
    monkeypatch.setattr(app_settings, "stale_device_minutes", 60)

    assert _stale_device_minutes_from_config(config) == expected


@pytest.mark.parametrize("raw_value", ["60", True, -1])
def test_stale_device_minutes_rejects_invalid_values(raw_value):
    with pytest.raises(ValueError, match="daemon.stale_device_minutes"):
        _stale_device_minutes_from_config({"stale_device_minutes": raw_value})


def test_load_daemon_config_reads_top_level_and_active_profile_tables(monkeypatch, tmp_path):
    from netaudio.common import config_loader

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        'active_profile = "home"\n\n[daemon]\nstale_device_minutes = 30\n\n'
        '[profiles.home]\nname = "home"\n\n[profiles.home.daemon]\nstale_device_minutes = 5\n'
    )
    monkeypatch.setattr(config_loader, "default_config_path", lambda: config_path)

    assert config_loader.load_daemon_config() == {"stale_device_minutes": 5}

    config_path.write_text("[daemon]\nstale_device_minutes = 30\n")
    assert config_loader.load_daemon_config() == {"stale_device_minutes": 30}

    config_path.write_text('[capture]\ndb = "x"\n')
    assert config_loader.load_daemon_config() == {}


@pytest.mark.asyncio
async def test_refresh_status_fields_targets_online_devices_with_missing_fields():
    known = _device("known.local.", age_seconds=0, online=True)
    known.clock_source_code = 1
    known.is_locked = False
    missing = _device("missing.local.", age_seconds=0, online=True)
    offline = _device("offline.local.", age_seconds=0, online=False)
    daemon = _daemon({"known.local.": known, "missing.local.": missing, "offline.local.": offline})
    refreshed = []

    async def refresh_status_fields(server_name, reason):
        refreshed.append((server_name, reason))

    daemon.state = SimpleNamespace(refresh_status_fields=refresh_status_fields)

    daemon._refresh_status_fields()
    for task in list(daemon._background_tasks):
        await task
    assert refreshed == [("missing.local.", "missing status fields")]

    refreshed.clear()
    daemon._last_status_field_refresh_monotonic = time.monotonic() - 10_000
    daemon._refresh_status_fields()
    for task in list(daemon._background_tasks):
        await task
    assert sorted(refreshed) == [("known.local.", "periodic refresh"), ("missing.local.", "periodic refresh")]


def test_managed_device_is_not_marked_offline_by_direct_discovery_failures():
    managed = _device("managed.local.", age_seconds=60, online=True)
    managed.management_state = "managed"
    daemon = _daemon({managed.server_name: managed})

    daemon.mark_device_offline(managed.server_name)
    daemon.mark_device_offline_verified(managed.server_name, "direct probe")

    assert daemon._offline_failures == {}
    assert daemon._offline_candidate_since == {}
    assert daemon._pending_offline_tasks == {}


@pytest.mark.asyncio
async def test_managed_device_is_not_sent_to_direct_revalidation(monkeypatch):
    managed = _device("managed.local.", age_seconds=60, online=True)
    managed.management_state = "managed"
    managed.ipv4 = None
    daemon = _daemon({managed.server_name: managed})
    direct_probe = MagicMock(side_effect=AssertionError("managed device reached direct probe"))
    monkeypatch.setattr("netaudio.daemon.server._probe_device", direct_probe)
    daemon._spawn_background = MagicMock()

    await daemon._verify_quiet_online_device(managed.server_name)
    await daemon._verify_quiet_online_devices()
    await daemon._recover_known_devices(offline_only=False)

    direct_probe.assert_not_called()
    daemon._spawn_background.assert_not_called()


@pytest.mark.asyncio
async def test_managed_device_without_address_can_refresh_status_fields():
    managed = _device("managed.local.", age_seconds=0, online=True)
    managed.management_state = "managed"
    managed.ipv4 = None
    daemon = _daemon({managed.server_name: managed})
    daemon.state = SimpleNamespace(refresh_status_fields=AsyncMock())

    daemon._refresh_status_fields()
    for task in list(daemon._background_tasks):
        await task

    daemon.state.refresh_status_fields.assert_awaited_once_with(managed.server_name, "missing status fields")
