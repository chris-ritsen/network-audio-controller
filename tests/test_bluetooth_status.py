import asyncio

from netaudio.dante.device import DanteDevice


def test_get_bluetooth_status_ignores_non_bluetooth_publications(monkeypatch, load_fixture):
    device = DanteDevice(server_name="avio-bt-1.local.")
    device.ipv4 = "192.168.1.61"
    disconnected = load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")

    def receive(_packet, _device_ip, parse_packet):
        assert parse_packet(b"unrelated publication") is False
        return parse_packet(disconnected)

    monkeypatch.setattr(device, "_receive_solicited_control_publication", receive)

    result = asyncio.run(device.get_bluetooth_status(host_mac=bytes.fromhex("c20f456899f5")))

    assert result is None
    assert device.bluetooth_connected is False
    assert device.bluetooth_device is None


def test_get_bluetooth_status_preserves_connected_device_name(monkeypatch, load_fixture):
    device = DanteDevice(server_name="avio-bt-1.local.")
    device.ipv4 = "192.168.1.61"
    connected = load_fixture("avio-bt-1_bluetooth_status_connected.bin")

    def receive(_packet, _device_ip, parse_packet):
        return parse_packet(connected)

    monkeypatch.setattr(device, "_receive_solicited_control_publication", receive)

    result = asyncio.run(device.get_bluetooth_status(host_mac=bytes.fromhex("c20f456899f5")))

    assert result == "s00pcan-iphone-17"
    assert device.bluetooth_connected is True
    assert device.bluetooth_device == "s00pcan-iphone-17"
