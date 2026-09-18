"""Retained packet fixtures are unsolicited observations, not current request replies."""

import asyncio
from unittest.mock import AsyncMock

from netaudio.dante.state import apply_device_status
from tests.test_device_controls import device as panel_device
from tests.status_test_support import receive_packets


def test_unsolicited_bluetooth_fixture_preserves_state_without_confirming_request(load_fixture):
    app, device = panel_device()
    receive_packets(
        app, [b"unrelated", load_fixture("avio-bt-1_bluetooth_status_connected.bin")], (str(device.ipv4), 8702)
    )
    assert device.bluetooth_connected is True
    assert device.bluetooth_device == "s00pcan-iphone-17"
    assert device.device_controls["unsolicited_status"]["bluetooth_connection"]["correlated"] is False
    assert not device.device_controls.get("correlated_status")


def test_disconnected_fixture_does_not_invent_connected_name(load_fixture):
    app, device = panel_device()
    receive_packets(app, [load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")], (str(device.ipv4), 8702))
    assert device.bluetooth_connected is False
    assert device.bluetooth_device == ""


def test_refresh_timeout_marks_prior_observation_unavailable(load_fixture):
    app, device = panel_device()
    receive_packets(app, [load_fixture("avio-bt-1_bluetooth_status_connected.bin")], (str(device.ipv4), 8702))
    app._send_settings = AsyncMock()
    status = asyncio.run(app.inspect_device_controls(device, timeout=0.001))
    assert not status["observations"]["bluetooth_connection"]["fresh"]
    assert device.bluetooth_connected is True


def test_apply_panel_status_reports_changes_once():
    _, device = panel_device()
    status = {
        "observed_at_unix": 1.0,
        "observations": [{"category": "bluetooth_connection", "value": {"state": 1, "peer_name": "peer"}}],
    }
    assert apply_device_status(device, "panel_status", status)
    assert not apply_device_status(device, "panel_status", status)
