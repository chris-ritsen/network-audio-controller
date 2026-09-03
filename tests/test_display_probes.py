from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from netaudio.cli import state
from netaudio.cli_support import execution as common_module
from netaudio.dante.device import DanteDevice


@pytest.fixture(autouse=True)
def _clear_selection(monkeypatch):
    monkeypatch.setattr(state, "names", [])
    monkeypatch.setattr(state, "hosts", [])
    monkeypatch.setattr(state, "server_names", [])
    monkeypatch.setattr(state, "macs", [])


def _device(server_name, *, online=True, emulated=False, ipv4="192.0.2.10"):
    device = DanteDevice(server_name=server_name)
    device.name = server_name.removesuffix(".local.")
    device.ipv4 = ipv4
    device.online = online
    if emulated:
        device.mac_address = "525400123456"
    return device


def test_probe_candidates_skip_offline_and_emulated_devices_unless_selected(caplog):
    devices = {
        "online.local.": _device("online.local."),
        "offline.local.": _device("offline.local.", online=False),
        "emulated.local.": _device("emulated.local.", emulated=True),
        "unaddressed.local.": _device("unaddressed.local.", ipv4=None),
    }

    with caplog.at_level("DEBUG", logger="netaudio"):
        candidates = common_module._probe_candidates(devices, "clock status")

    assert list(candidates) == ["online.local."]
    assert not [record for record in caplog.records if record.levelname == "WARNING"]
    assert "Skipping clock status probe for offline.local.: device is offline" in caplog.text
    assert "Skipping clock status probe for emulated.local.: device is emulated" in caplog.text

    state.names = ["offline"]
    candidates = common_module._probe_candidates(devices, "clock status")
    assert list(candidates) == ["offline.local."]

    state.names = ["*"]
    candidates = common_module._probe_candidates(devices, "clock status")
    assert list(candidates) == ["online.local.", "offline.local.", "emulated.local."]


@pytest.mark.asyncio
async def test_populate_controls_skips_offline_devices_and_marks_unreachable_devices_offline(caplog):
    reachable = _device("reachable.local.")
    reachable.populate_from_core = AsyncMock(return_value=True)
    unreachable = _device("unreachable.local.", ipv4="192.0.2.11")
    unreachable.populate_from_core = AsyncMock(side_effect=OSError("io error"))
    offline = _device("offline.local.", online=False)
    offline.populate_from_core = AsyncMock()

    with caplog.at_level("DEBUG", logger="netaudio"):
        await common_module._populate_controls(
            {
                "reachable.local.": reachable,
                "unreachable.local.": unreachable,
                "offline.local.": offline,
            }
        )

    reachable.populate_from_core.assert_awaited_once()
    unreachable.populate_from_core.assert_awaited_once()
    offline.populate_from_core.assert_not_awaited()
    assert unreachable.online is False
    assert reachable.online is True
    warnings = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
    assert warnings == ["Could not reach unreachable.local. (192.0.2.11): io error"]


@pytest.mark.asyncio
async def test_populate_controls_routes_enrolled_devices_through_the_managed_application():
    device = _device("managed.local.")
    device.ddm_device_id = "001dc1fffe50692e:0"
    device.ddm_enrolment_state = "ENROLLED"
    device.management_state = "managed"
    device.tx_channels[1] = SimpleNamespace(name="already-in-inventory")
    device.populate_from_core = AsyncMock()
    application = SimpleNamespace(
        _populate_device_controls=AsyncMock(),
        probe_sample_rate_status=AsyncMock(return_value=(48000, [48000, 96000])),
        probe_encoding_status=AsyncMock(return_value=(24, [16, 24])),
    )

    await common_module._populate_controls({device.server_name: device}, application)

    application._populate_device_controls.assert_awaited_once_with(device, include_channels=False)
    application.probe_sample_rate_status.assert_awaited_once_with(device.ipv4, timeout=2.0)
    application.probe_encoding_status.assert_awaited_once_with(device.ipv4, timeout=2.0)
    device.populate_from_core.assert_not_awaited()
    assert device.sample_rate == 48000
    assert device.encoding == 24


@pytest.mark.asyncio
async def test_enrich_clock_fields_skips_devices_the_daemon_already_described(monkeypatch):
    described = _device("described.local.")
    described.clock_role = "Follower"
    unknown = _device("unknown.local.", ipv4="192.0.2.11")
    probed = []

    async def probe_clocking_status(device):
        probed.append(device.server_name)
        raise common_module.CapabilityProbeTimeout("clock status probe timed out")

    application = SimpleNamespace(probe_clocking_status=probe_clocking_status)

    failures = await common_module._enrich_clock_fields(
        application,
        {"described.local.": described, "unknown.local.": unknown},
    )

    assert probed == ["unknown.local."]
    assert list(failures) == ["unknown.local."]
    assert "clock_status" in unknown.failed_queries
    assert "clock_status" not in described.failed_queries


@pytest.mark.asyncio
async def test_enrich_lock_states_only_unknown_skips_daemon_supplied_values(monkeypatch):
    described = _device("described.local.")
    described.is_locked = False
    unknown = _device("unknown.local.", ipv4="192.0.2.11")
    application = SimpleNamespace(
        probe_lock_status=AsyncMock(
            side_effect=common_module.CapabilityProbeTimeout("lock status readback timed out"),
        ),
    )

    failures = await common_module._enrich_lock_states(
        application,
        {"described.local.": described, "unknown.local.": unknown},
        only_unknown=True,
    )

    application.probe_lock_status.assert_awaited_once_with("192.0.2.11", timeout=4.0)
    assert described.is_locked is False
    assert list(failures) == ["unknown.local."]
    assert "is_locked" in unknown.failed_queries


@pytest.mark.asyncio
async def test_load_display_devices_warns_once_per_explicitly_selected_unreachable_device(monkeypatch, caplog):
    device = _device("ghost.local.", online=False, ipv4="192.0.2.99")
    state.names = ["ghost"]

    async def daemon_devices():
        return {"ghost.local.": device}

    async def enrich_clock(application, devices):
        return {"ghost.local.": common_module.CapabilityProbeTimeout("clock status probe timed out")}

    async def enrich_lock(application, devices, only_unknown=False):
        return {"ghost.local.": common_module.CapabilityProbeTimeout("lock status probe timed out")}

    async def populate_controls(devices):
        return None

    application = SimpleNamespace(attach_devices=lambda devices: None)
    monkeypatch.setattr(common_module, "get_devices_from_daemon", daemon_devices)
    monkeypatch.setattr(common_module, "_populate_controls", populate_controls)
    monkeypatch.setattr(common_module, "_enrich_clock_fields", enrich_clock)
    monkeypatch.setattr(common_module, "_enrich_lock_states", enrich_lock)

    with caplog.at_level("DEBUG", logger="netaudio"):
        devices = await common_module._load_display_devices(application)

    assert list(devices) == ["ghost.local."]
    warnings = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
    assert warnings == ["Could not reach ghost.local. (192.0.2.99): clock status probe timed out"]

    caplog.clear()
    state.names = []
    with caplog.at_level("DEBUG", logger="netaudio"):
        await common_module._load_display_devices(application)
    assert not [record for record in caplog.records if record.levelname == "WARNING"]
    assert "Could not reach ghost.local. (192.0.2.99): clock status probe timed out" in caplog.text
