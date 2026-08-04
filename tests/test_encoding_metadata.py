from types import SimpleNamespace

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
