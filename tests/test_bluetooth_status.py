import asyncio

import pytest

from netaudio.dante.application import CapabilityProbeTimeout, DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.state import apply_device_status


def _application_with_device():
    application = DanteApplication()
    device = DanteDevice(server_name="avio-bt-1.local.")
    device.ipv4 = "192.168.1.61"
    application.attach_devices({device.server_name: device})
    return application, device


def _publish_on_request(application, packets):
    async def request_bluetooth_status(device_ip_address, host_mac=None):
        for packet in packets:
            application.notifications._on_packet(packet, (device_ip_address, 8702))

    application.send_bluetooth_status_request = request_bluetooth_status


def test_probe_bluetooth_status_ignores_non_bluetooth_publications(load_fixture):
    application, device = _application_with_device()
    disconnected = load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")
    _publish_on_request(application, [b"unrelated publication", disconnected])

    status = asyncio.run(application.probe_bluetooth_status(device))

    assert status["connected"] is False
    assert device.bluetooth_connected is False
    assert device.bluetooth_device is None


def test_probe_bluetooth_status_preserves_connected_device_name(load_fixture):
    application, device = _application_with_device()
    connected = load_fixture("avio-bt-1_bluetooth_status_connected.bin")
    _publish_on_request(application, [connected])

    status = asyncio.run(application.probe_bluetooth_status(device))

    assert status["device_name"] == "s00pcan-iphone-17"
    assert device.bluetooth_connected is True
    assert device.bluetooth_device == "s00pcan-iphone-17"


def test_probe_bluetooth_status_times_out_without_a_publication():
    application, device = _application_with_device()
    _publish_on_request(application, [])

    with pytest.raises(CapabilityProbeTimeout, match="bluetooth status readback timed out"):
        asyncio.run(application.probe_bluetooth_status(device, timeout=0.01))


def test_apply_bluetooth_status_reports_changes_once():
    device = DanteDevice(server_name="avio-bt-1.local.")
    status = {"connected": True, "device_name": "s00pcan-iphone-17"}

    assert apply_device_status(device, "bluetooth_status", status) is True
    assert apply_device_status(device, "bluetooth_status", status) is False
    assert device.bluetooth_device == "s00pcan-iphone-17"
