import pytest

from netaudio import core
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.services.heartbeat import parse_signal_presence_records
from netaudio.dante.virtual_device import _McastInfoProtocol, VirtualDevice, VirtualDeviceConfig


pytestmark = pytest.mark.skipif(not core.available(), reason="netaudio-core not built")


class RecordingTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet, destination):
        self.sent.append((packet, destination))


def test_virtual_device_reports_configured_encoding_capabilities():
    device = VirtualDevice(
        VirtualDeviceConfig(
            encoding=20,
            supported_encodings=[20, 24, 32],
        )
    )

    packet = device._build_audio_capability_status_packet(
        0x0082,
        device.config.encoding,
        device.config.supported_encodings,
    )

    assert core.parse_response("encoding_status", packet) == {
        "current_encoding": 20,
        "supported_encodings": [20, 24, 32],
    }
    assert device._pcm_capability_property() is None


def test_virtual_device_derives_pcm_metadata_from_configured_capabilities():
    device = VirtualDevice(
        VirtualDeviceConfig(
            encoding=24,
            supported_encodings=[24],
        )
    )
    configurable_device = VirtualDevice(
        VirtualDeviceConfig(
            encoding=32,
            supported_encodings=[16, 24, 32],
        )
    )

    assert device._pcm_capability_property() == "3 0x4"
    assert configurable_device._pcm_capability_property() == "4 0xe"
    assert device._build_channel_metadata() == bytes.fromhex("0000bb80010100180400001800180004")
    assert configurable_device._build_channel_metadata() == bytes.fromhex("0000bb8001010020040000200020000e")


def test_virtual_device_does_not_invent_pcm_metadata_for_unknown_encodings():
    device = VirtualDevice(
        VirtualDeviceConfig(
            encoding=20,
            supported_encodings=[20, 24, 32],
        )
    )

    assert device._pcm_capability_property() is None
    assert device._build_channel_metadata() is None
    assert int.from_bytes(device._handle_tx_channels(1, b"")[8:10], "big") == 0x0030
    assert int.from_bytes(device._handle_rx_channels(1, b"")[8:10], "big") == 0x0030


def test_virtual_device_reports_configured_sample_rate_capabilities():
    device = VirtualDevice(
        VirtualDeviceConfig(
            sample_rate=384_000,
            supported_sample_rates=[48_000, 96_000, 384_000],
        )
    )

    packet = device._build_audio_capability_status_packet(
        0x0080,
        device.config.sample_rate,
        device.config.supported_sample_rates,
    )

    assert core.parse_response("sample_rate_status", packet) == {
        "current_sample_rate": 384_000,
        "supported_sample_rates": [48_000, 96_000, 384_000],
    }


def test_virtual_device_settings_distinguish_configured_and_active_latency():
    device = VirtualDevice(
        VirtualDeviceConfig(
            configured_latency_ns=250_000,
            active_latency_ns=1_000_000,
        )
    )

    packet = device._handle_device_settings(7, b"")
    parsed = core.parse_response("device_settings", packet)

    assert parsed["configured_latency_ns"] == 250_000
    assert parsed["active_latency_ns"] == 1_000_000
    assert parsed["default_latency_ns"] == 1_000_000
    assert parsed["min_latency_ns"] == 150_000
    assert parsed["max_latency_ns"] == 21_333_334


def test_same_host_encoding_probe_receives_status_response():
    device = VirtualDevice()
    device._local_ip = "192.0.2.10"
    recording_transport = RecordingTransport()
    device._mcast_transport = recording_transport
    protocol = _McastInfoProtocol(device)
    packet, _, _ = DanteDeviceCommands(host_mac=b"\x02\x00\x00\x00\x00\x01").command_probe_encoding()

    protocol.datagram_received(packet, ("192.0.2.10", 49152))

    assert len(recording_transport.sent) == 1
    response, destination = recording_transport.sent[0]
    assert destination == ("224.0.0.231", 8702)
    assert core.parse_response("encoding_status", response)["supported_encodings"] == [24, 16, 32]


def test_virtual_device_heartbeat_round_trips_odd_signal_presence_count():
    device = VirtualDevice(
        VirtualDeviceConfig(
            tx_channels=["TX 1", "TX 2"],
            rx_channels=["RX 1"],
        )
    )
    recording_transport = RecordingTransport()
    device._mcast_transport = recording_transport

    device._send_heartbeat()

    assert len(recording_transport.sent) == 1
    packet, destination = recording_transport.sent[0]
    assert destination == ("224.0.0.233", 8708)

    signal_record = packet[0x20 + 0x10 :]
    assert len(signal_record) == 0x1C
    assert signal_record[:8] == bytes.fromhex("001c800200040010")
    assert signal_record[0x18:] == bytes.fromhex("ffffff00")

    [parsed] = parse_signal_presence_records(packet)
    assert parsed["tx_count"] == 2
    assert parsed["rx_count"] == 1
    assert parsed["tx_levels"] == [0xFF, 0xFF]
    assert parsed["rx_levels"] == [0xFF]
    assert parsed["padding_length"] == 1
