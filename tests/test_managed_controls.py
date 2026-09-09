import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio.daemon.http.api import DaemonHTTPServer
from netaudio.daemon.server import NetaudioDaemon
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from tests.http_api_test_support import FakeWriter


def managed_record(server="manager", context="main", domain="domain", device_id="device"):
    key = f"ddm:{server}:{domain}:{device_id}"
    return {
        "server_name": key,
        "name": "Managed device",
        "online": True,
        "ipv4": "192.0.2.10",
        "management_state": "managed",
        "ddm_enrolment_state": "ENROLLED",
        "ddm_device_id": device_id,
        "ddm_server_profile": server,
        "ddm_context": context,
        "ddm_domain_id": domain,
        "ddm_last_sync": 100,
        "inventory_sources": ["ddm"],
        "direct_control_available": False,
        "channels": {"receivers": {"1": {"name": "Input", "ddm_channel_id": "rx1"}}, "transmitters": {}},
    }


def server_with_inventory(*records):
    inventory = {record["server_name"]: record for record in records}
    registry = SimpleNamespace(enabled=True, serialize_devices=lambda _: copy.deepcopy(inventory))
    application = DanteApplication()
    return DaemonHTTPServer(application, application.state, managed_inventory=registry), inventory


def test_managed_controls_survive_polls_without_creating_a_direct_inventory_source():
    record = managed_record()
    server, inventory = server_with_inventory(record)
    device = server._find_device(record["server_name"])
    device.configured_latency = 2.0
    device.supported_sample_rates = [48000, 96000]
    device.encoding = 24
    device.supported_encodings = [16, 24]
    device.gain_device_type = "output"
    device.gain_levels = [4]
    device.supported_gain_levels = [1, 2, 3, 4, 5]
    assert server.application._device_by_control_key(server.application._control_key(device)) is device
    assert server.managed_controls.direct_devices() == {}

    inventory[device.server_name]["ddm_last_sync"] = 101
    inventory[device.server_name]["name"] = "Updated name"
    state = server._serialized_devices()[device.server_name]

    assert server._find_device(device.server_name) is device
    assert state["name"] == "Updated name"
    assert state["configured_latency_ms"] == 2.0
    assert state["supported_sample_rates_hz"] == [48000, 96000]
    assert state["encoding"] == 24
    assert state["gain_levels"] == [4]
    assert state["channels"]["receivers"][1]["gain_level_label"] == "0 dBV"
    assert state["inventory_sources"] == ["ddm"]
    assert state["direct_control_available"] is False


@pytest.mark.asyncio
async def test_managed_publications_are_applied_to_the_exact_server_scope():
    east = managed_record("east", "east-main")
    west = managed_record("west", "west-main")
    server, _ = server_with_inventory(east, west)
    east_device = server._find_device(east["server_name"])
    west_device = server._find_device(west["server_name"])
    app = server.application
    assert app._device_by_ip("192.0.2.10") is None
    await app.state.on_device_status(
        DanteEvent(
            type=EventType.DEVICE_STATUS_RECEIVED,
            data={
                "kind": "sample_rate",
                "source_ip": app._control_key(west_device),
                "status": {"sample_rate": 48000, "supported_sample_rates": [48000, 96000]},
            },
        )
    )
    assert east_device.sample_rate is None
    assert west_device.sample_rate == 48000
    assert server._serialized_devices()[west_device.server_name]["supported_sample_rates_hz"] == [48000, 96000]
    assert app.state._pending_status == {}


def test_reassigned_context_discards_the_old_control_object():
    record = managed_record()
    server, inventory = server_with_inventory(record)
    old = server._find_device(record["server_name"])
    old.configured_latency = 2.0
    inventory[old.server_name]["ddm_context"] = "replacement"
    fresh = server._find_device(old.server_name)
    assert fresh is not old
    assert fresh.configured_latency is None
    assert fresh.ddm_context == "replacement"
    assert old.online is False


@pytest.mark.parametrize("disabled", [False, True])
def test_removal_or_logout_invalidates_managed_controls(disabled):
    record = managed_record()
    server, inventory = server_with_inventory(record)
    device = server._find_device(record["server_name"])
    if disabled:
        server.managed_inventory.enabled = False
    else:
        inventory.clear()
    assert server._serialized_devices() == {}
    assert server.application.devices == {}
    assert device.online is False


def test_correlated_device_keeps_one_identity_and_loses_enrollment_when_unenrolled():
    record = managed_record()
    record.update(server_name="device.local.", inventory_sources=["direct", "ddm"], direct_control_available=True)
    server, inventory = server_with_inventory(record)
    direct = DanteDevice(record["server_name"])
    server.application.attach_devices({direct.server_name: direct})
    managed = server._find_device(direct.server_name)
    managed.configured_latency = 2.0
    assert list(server.managed_controls.direct_devices()) == [direct.server_name]
    assert list(server._serialized_devices()) == [direct.server_name]
    inventory[direct.server_name].update(
        management_state="unenrolled", ddm_enrolment_state="UNENROLLED", ddm_domain_id=None
    )
    server._serialized_devices()
    assert managed.requires_managed_control is False
    assert managed.configured_latency == 2.0


def test_logout_does_not_turn_a_correlated_enrolled_device_into_a_direct_control_target():
    record = managed_record()
    record.update(server_name="device.local.", inventory_sources=["direct", "ddm"], direct_control_available=True)
    server, _ = server_with_inventory(record)
    server.application.attach_devices({record["server_name"]: DanteDevice(record["server_name"])})
    device = server._find_device(record["server_name"])
    server.managed_inventory.enabled = False
    state = server._serialized_devices()[device.server_name]
    assert device.requires_managed_control is True
    assert state["management_state"] == "managed"
    assert state["online"] is False


def test_inventory_restores_subscription_channel_identity():
    record = managed_record()
    record["subscriptions"] = [
        {"rx_channel": "Input", "rx_device": record["name"], "tx_channel": "Left", "tx_device": "Source"}
    ]
    server, inventory = server_with_inventory(record)
    device = server._find_device(record["server_name"])
    assert device.subscriptions[0].rx_channel is device.rx_channels[1]
    inventory[device.server_name]["ddm_last_sync"] += 1
    server._serialized_devices()
    assert device.subscriptions[0].rx_channel is device.rx_channels[1]


def test_managed_subscription_status_survives_runtime_serialization():
    record = managed_record()
    record["subscriptions"] = [
        {
            "rx_channel": "Input",
            "rx_device": record["name"],
            "tx_channel": "Left",
            "tx_device": "Source",
            "ddm_status": "DYNAMIC",
            "ddm_status_message": "Active subscription",
            "ddm_summary": "CONNECTED",
        }
    ]
    server, _ = server_with_inventory(record)
    server._find_device(record["server_name"])
    status = server._serialized_devices()[record["server_name"]]["subscriptions"][0]["status"]
    assert status["state"] == "connected"
    assert status["severity"] == "ok"
    assert status["detail"] == "Active subscription"


@pytest.mark.asyncio
async def test_inventory_poll_schedules_one_managed_refresh_per_device_without_an_sse_client():
    record = managed_record()
    server, inventory = server_with_inventory(record)
    daemon = object.__new__(NetaudioDaemon)
    daemon.application = server.application
    daemon.state = server.state
    daemon.state.refresh_device = AsyncMock()
    daemon.http_api = server
    daemon.running = True
    daemon._redis = None
    daemon._background_tasks = set()
    daemon._managed_control_tasks = {}
    daemon._managed_control_refreshes = {}
    await daemon._on_managed_inventory_changed()
    await asyncio.gather(*daemon._background_tasks)
    await daemon._on_managed_inventory_changed()
    daemon.state.refresh_device.assert_awaited_once_with(record["server_name"])
    assert server.sse_clients == {}
    assert record["server_name"] in server.application.devices
    inventory[record["server_name"]]["ddm_context"] = "replacement"
    await daemon._on_managed_inventory_changed()
    await asyncio.gather(*daemon._background_tasks)
    assert daemon.state.refresh_device.await_count == 2


@pytest.mark.asyncio
async def test_refresh_populates_the_persistent_managed_device():
    record = managed_record()
    server, _ = server_with_inventory(record)
    app = server.application
    app._populate_device_controls = AsyncMock()
    for name in (
        "probe_aes67_state",
        "probe_sample_rate_status",
        "probe_encoding_status",
        "probe_gain_status",
        "probe_preferred_leader_state",
        "probe_interface_status",
        "probe_clocking_status",
        "probe_lock_status",
    ):
        setattr(app, name, AsyncMock())
    device = server._find_device(record["server_name"])
    device.supported_sample_rates = [48000]
    device.supported_encodings = [24]
    device.supported_gain_levels = [1, 2, 3, 4, 5]
    writer = FakeWriter()
    await server._dispatch("POST", "/refresh", json.dumps({"device": device.server_name}).encode(), writer)
    assert writer.response() == (200, {"success": True})
    app._populate_device_controls.assert_awaited_once_with(device)
    app.probe_sample_rate_status.assert_awaited_once_with(device)
    app.probe_encoding_status.assert_awaited_once_with(device)
    app.probe_gain_status.assert_awaited_once_with(device)
    app.probe_lock_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("configured, status", [(2_000_000, 200), (1_000_000, 409), (None, 504)])
async def test_managed_latency_requires_matching_readback(configured, status):
    record = managed_record()
    server, _ = server_with_inventory(record)
    device = server._find_device(record["server_name"])
    app = server.application
    app.set_latency = AsyncMock(return_value=bytes.fromhex("2809000a123411010001"))
    app.get_latency_settings = AsyncMock(
        return_value={"configured_latency_ns": configured, "active_latency_ns": 250000}
    )
    writer = FakeWriter()
    await server._dispatch(
        "POST", "/set-latency", json.dumps({"device": device.server_name, "latency": 2}).encode(), writer
    )
    assert writer.response()[0] == status
    app.set_latency.assert_awaited_once_with(device, 2)
    app.get_latency_settings.assert_awaited_once_with(device)


@pytest.mark.asyncio
@pytest.mark.parametrize("latency", [None, True, "2", -1, float("inf"), float("nan")])
async def test_invalid_latency_never_sends_a_mutation(latency):
    record = managed_record()
    server, _ = server_with_inventory(record)
    server.application.set_latency = AsyncMock()
    writer = FakeWriter()
    await server._dispatch(
        "POST", "/set-latency", json.dumps({"device": record["server_name"], "latency": latency}).encode(), writer
    )
    assert writer.response()[0] == 400
    server.application.set_latency.assert_not_awaited()


def test_unenrollment_clears_restored_direct_metadata_before_first_managed_poll():
    record = managed_record()
    record.update(server_name="device.local.", management_state="unenrolled", ddm_enrolment_state="UNENROLLED")
    record.pop("ddm_domain_id")
    server, _ = server_with_inventory(record)
    direct = DanteDevice(record["server_name"])
    direct.management_state = "managed"
    direct.ddm_enrolment_state = "ENROLLED"
    direct.ddm_domain_id = "previous-domain"
    direct.configured_latency = 2.0
    server.application.attach_devices({direct.server_name: direct})
    server._serialized_devices()
    assert server._find_device(direct.server_name) is direct
    assert direct.requires_managed_control is False
    assert direct.ddm_domain_id is None
    assert direct.configured_latency == 2.0
