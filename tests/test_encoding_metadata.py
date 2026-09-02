from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio.core import binding
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer


def controls_input(channel_audio_metadata):
    return {
        "name": None,
        "counts": (1, 1, None),
        "aes67": None,
        "settings": None,
        "channel_audio_metadata": channel_audio_metadata,
        "rx": [],
        "tx": [],
    }


def test_core_client_reads_metadata_from_first_available_inventory(monkeypatch):
    requests = []

    def request(packet, port):
        requests.append((packet, port))
        return b"transmitter-response"

    client = SimpleNamespace(_arc_port=4440, _device_ip="192.168.1.108", request=request)
    monkeypatch.setattr(binding, "build_command", lambda specification: specification["command"].encode())
    monkeypatch.setattr(
        binding,
        "parse_response",
        lambda kind, response: {
            "kind": kind,
            "response": response.decode(),
            "current_encoding": 24,
            "supported_encodings": [24],
        },
    )

    result = binding.CoreClient.get_channel_audio_metadata(client, 128, 128)

    assert result["kind"] == "channel_audio_metadata"
    assert result["response"] == "transmitter-response"
    assert requests == [(b"transmitters", 4440)]


def test_core_client_falls_back_from_tx_to_rx_inventory(monkeypatch):
    requests = []

    def request(packet, port):
        requests.append((packet, port))
        return packet + b"-response"

    def parse_response(kind, response):
        if response == b"transmitters-response":
            raise binding.NetaudioCoreError(10, kind)
        return {"current_encoding": 24, "supported_encodings": [24]}

    client = SimpleNamespace(_arc_port=4440, _device_ip="192.168.1.108", request=request)
    monkeypatch.setattr(binding, "build_command", lambda specification: specification["command"].encode())
    monkeypatch.setattr(binding, "parse_response", parse_response)

    result = binding.CoreClient.get_channel_audio_metadata(client, 128, 128)

    assert result == {"current_encoding": 24, "supported_encodings": [24]}
    assert requests == [(b"transmitters", 4440), (b"receivers", 4440)]


@pytest.mark.asyncio
async def test_control_fetch_reuses_rx_inventory_metadata_and_applies_property_capabilities(monkeypatch):
    device = DanteDevice()
    core_client = MagicMock()
    core_client.observer = None
    core_client.get_channel_count.return_value = (2, 2, False)
    core_client.get_rx_inventory.return_value = {
        "channels": [],
        "channel_audio_metadata": {
            "sample_rate": 48_000,
            "current_encoding": 24,
            "encoding_capability_bitmap": 0x0004,
            "supported_encodings": [24],
        },
    }
    core_client.get_tx_channels.return_value = []
    core_client.get_device_name.return_value = "avio-input"
    core_client.get_device_settings.return_value = None
    core_client.get_property_directory.return_value = {
        "properties": [{"property_id": 0x8020, "flags": 0x0001}],
        "aes67_supported": False,
    }
    device.ipv4 = "192.0.2.10"

    async def call_core(operation, **_options):
        return operation(core_client)

    monkeypatch.setattr(device, "call_core", call_core)

    controls = await device.fetch_controls_data()

    assert controls["aes67_supported"] is False
    assert controls["settings_properties"] == [{"property_id": 0x8020, "flags": 0x0001}]
    assert controls["channel_metadata_supported_encodings"] == [24]
    core_client.get_rx_inventory.assert_called_once_with(2)
    core_client.get_rx_channels.assert_not_called()
    core_client.get_channel_audio_metadata.assert_not_called()
    core_client.get_aes67_configured.assert_not_called()


def test_channel_metadata_populates_unknown_encoding_capability():
    device = DanteDevice()
    controls = device.controls_data_from_core(
        controls_input(
            {
                "sample_rate": 48_000,
                "current_encoding": 24,
                "encoding_capability_bitmap": 0x0004,
                "supported_encodings": [24],
            }
        )
    )

    device.apply_controls(controls)

    assert device.encoding == 24
    assert device.supported_encodings == [24]
    assert device.encoding_configurable is False
    assert DanteDeviceSerializer.to_json(device)["encoding_configurable"] is False


def test_channel_metadata_does_not_override_conmon_capability():
    device = DanteDevice()
    device.encoding = 24
    device.supported_encodings = [16, 24, 32]
    controls = device.controls_data_from_core(
        controls_input(
            {
                "sample_rate": 48_000,
                "current_encoding": 24,
                "encoding_capability_bitmap": 0x0004,
                "supported_encodings": [24],
            }
        )
    )

    device.apply_controls(controls)

    assert device.encoding == 24
    assert device.supported_encodings == [16, 24, 32]
    assert device.encoding_configurable is True


def test_channel_metadata_does_not_replace_conflicting_known_encoding():
    device = DanteDevice()
    device.encoding = 32
    controls = device.controls_data_from_core(
        controls_input(
            {
                "sample_rate": 48_000,
                "current_encoding": 24,
                "encoding_capability_bitmap": 0x0004,
                "supported_encodings": [24],
            }
        )
    )

    device.apply_controls(controls)

    assert device.encoding == 32
    assert device.supported_encodings is None
    assert device.encoding_configurable is None
    assert "encoding_configurable" not in DanteDeviceSerializer.to_json(device)
