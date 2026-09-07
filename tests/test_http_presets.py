import asyncio
import hashlib
import json
from unittest.mock import AsyncMock

import pytest
from netaudio import DanteDevice
from netaudio.daemon.http import presets
from netaudio.presets.parsing import parse_preset_xml

from tests.http_api_test_support import FakeWriter, make_http_server, post


@pytest.fixture
def server():
    device = DanteDevice("device.local.")
    device.name = "Desk"
    device.ipv4 = "192.0.2.10"
    device.rx_count = device.tx_count = 0
    device.fetch_device_name = AsyncMock(return_value="Desk")
    device.get_rx_channels = AsyncMock()
    device.get_tx_channels = AsyncMock()
    result = make_http_server({device.server_name: device})
    result.application.get_device_settings = AsyncMock(return_value={"configured_latency_ns": 1_000_000})
    result.application.probe_preferred_leader_state = AsyncMock(return_value=True)
    result.publish_inventory_snapshot = AsyncMock()
    return result


def xml(fields='<preferred_master value="true"/>', extra=""):
    return f"<preset><name>Show</name><device><friendly_name>Desk</friendly_name>{fields}</device>{extra}</preset>"


def load_body(content=None, **changes):
    content = content or xml()
    return {
        "xml": content,
        "digest": hashlib.sha256(content.encode()).hexdigest(),
        "confirmed": True,
        "targets": {"Desk": "device.local."},
        "excluded": [],
        **changes,
    }


@pytest.mark.asyncio
async def test_preview_is_offline_and_matches_only_exact_scope(server):
    status, data = await post(server, "/presets/preview", {"xml": xml(), "devices": []})
    assert status == 200
    assert data["devices"][0]["targets"] == []
    server.application.devices["device.local."].fetch_device_name.assert_not_called()
    status, data = await post(server, "/presets/preview", {"xml": xml(), "devices": ["device.local."]})
    assert data["devices"][0]["targets"][0]["id"] == "device.local."
    assert data["devices"][0]["settings"] == [{"label": "Preferred leader", "value": "On"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        "",
        "<broken",
        "<other/>",
        "<!DOCTYPE preset><preset/>",
        "<preset/>",
        xml(extra="<device><name>Desk</name></device>"),
        xml('<txchannel danteId="1"><label>A</label></txchannel><txchannel danteId="1"><label>B</label></txchannel>'),
        " " * (presets.MAX_PRESET_BYTES + 1),
    ],
    ids=[
        "empty",
        "malformed",
        "wrong-root",
        "doctype",
        "no-devices",
        "duplicate-device",
        "duplicate-channel",
        "oversized",
    ],
)
async def test_invalid_xml_refused(server, content):
    status, _ = await post(server, "/presets/preview", {"xml": content, "devices": []})
    assert status == 400
    server.application.set_preferred_leader.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"confirmed": False},
        {"digest": "changed"},
        {"targets": {"Desk": "Desk"}},
        {"targets": {"Desk": "missing"}},
        {"excluded": ["Desk"]},
        {"confirm_destructive": "yes"},
    ],
)
async def test_load_requires_review_and_exact_targets(server, changes):
    status, _ = await post(server, "/presets/load", load_body(**changes))
    assert status == 400
    server.application.set_preferred_leader.assert_not_called()


@pytest.mark.asyncio
async def test_missing_device_requires_explicit_skip(server):
    content = xml(extra='<device><name>Missing</name><preferred_master value="false"/></device>')
    assert (await post(server, "/presets/load", load_body(content)))[0] == 400
    status, data = await post(server, "/presets/load", load_body(content, excluded=["Missing"]))
    assert status == 200 and data["complete"] is True
    server.application.set_preferred_leader.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("offline,identity", [(True, "Desk"), (False, "Renamed")])
async def test_load_rechecks_availability_and_identity(server, offline, identity):
    device = server.application.devices["device.local."]
    device.online = not offline
    device.fetch_device_name.return_value = identity
    status, _ = await post(server, "/presets/load", load_body())
    assert status in (400, 409)
    server.application.set_preferred_leader.assert_not_called()


@pytest.mark.asyncio
async def test_all_preflight_happens_before_any_write(server):
    content = xml('<preferred_master value="true"/><encoding>7</encoding>')
    status, data = await post(server, "/presets/load", load_body(content))
    assert status == 409 and "No changes were sent" in data["error"]
    server.application.set_preferred_leader.assert_not_called()


@pytest.mark.asyncio
async def test_success_is_verified_and_published(server):
    status, data = await post(server, "/presets/load", load_body())
    assert status == 200 and data["complete"]
    assert data["report"]["failures"] == data["report"]["unverified"] == 0
    server.application.probe_preferred_leader_state.assert_awaited()
    server.publish_inventory_snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_stops_remaining_settings(server):
    server.application.set_preferred_leader.side_effect = RuntimeError("Device refused the change")
    server.application.set_interface = AsyncMock()
    content = xml('<preferred_master value="true"/><interface><ipv4_address mode="dynamic"/></interface>')
    status, data = await post(server, "/presets/load", load_body(content))
    assert status == 200 and not data["complete"]
    assert data["report"]["failures"] == 1
    server.application.set_interface.assert_not_called()


@pytest.mark.asyncio
async def test_apply_timeout_does_not_claim_no_changes(server, monkeypatch):
    async def delayed(*args, **kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(presets, "PRESET_APPLY_TIMEOUT", 0.01)
    server.application.set_preferred_leader.side_effect = delayed
    status, data = await post(server, "/presets/load", load_body())
    assert status == 200 and not data["complete"] and data["interrupted"]
    assert data["report"]["unverified"] == 1


@pytest.mark.asyncio
async def test_concurrent_operation_refused(server):
    async with server._preset_operation_lock:
        assert (await post(server, "/presets/load", load_body()))[0] == 409


@pytest.mark.asyncio
async def test_save_routing_excludes_unselected_categories(server):
    status, data = await post(
        server, "/presets/save", {"name": "Show", "devices": ["device.local."], "sections": ["routing"]}
    )
    assert status == 200 and data["filename"] == "Show.xml"
    assert parse_preset_xml(data["xml"])[1] == {"Desk": {"name": "Desk"}}
    server.application.get_device_settings.assert_not_called()
    server.application.probe_interface_status.assert_not_called()
    server.application.devices["device.local."].get_rx_channels.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_audio_uses_fresh_readback(server):
    status, data = await post(
        server, "/presets/save", {"name": "Show", "devices": ["device.local."], "sections": ["audio"]}
    )
    assert status == 200
    _, config = parse_preset_xml(data["xml"])
    assert config["Desk"]["latency"] == 1
    assert config["Desk"]["sample_rate"] == 48000
    assert config["Desk"]["preferred_leader"] is True
    server.application.get_device_settings.return_value = {}
    status, data = await post(
        server, "/presets/save", {"name": "Show", "devices": ["device.local."], "sections": ["audio"]}
    )
    assert status == 409 and "xml" not in data


@pytest.mark.asyncio
async def test_save_refuses_unknown_network_and_incomplete_channels(server):
    server.application.probe_interface_status.return_value = [{"mode": "unknown"}]
    body = {"name": "Show", "devices": ["device.local."], "sections": ["network"]}
    assert (await post(server, "/presets/save", body))[0] == 409
    server.application.devices["device.local."].rx_count = 2
    assert (await post(server, "/presets/save", {**body, "sections": ["routing"]}))[0] == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["save", "preview", "load"])
async def test_cross_origin_refused(server, path):
    writer = FakeWriter()
    await server._dispatch(
        "POST",
        f"/presets/{path}",
        json.dumps(load_body()).encode(),
        writer,
        headers={"origin": "http://other.test", "host": "localhost:9000"},
    )
    assert writer.response()[0] == 403
