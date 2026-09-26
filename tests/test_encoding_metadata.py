from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer


@pytest.mark.parametrize(
    "status,expected,state,observed",
    [
        ({"current_value": 24, "requested_value": 16}, 24, "confirmed", 24),
        ({"current_value": 16, "requested_value": 24}, 24, "unverified", 16),
        ({"current_value": 0}, 0, "confirmed", 0),
        (None, 24, "unavailable", None),
        ({"requested_value": 24}, 24, "unavailable", None),
        ({"current_value": True}, 1, "unavailable", None),
        ({"current_value": "24"}, 24, "unavailable", None),
        ({"current_value": -1}, 24, "unavailable", None),
        ({"current_value": 2**32}, 24, "unavailable", None),
    ],
)
def test_audio_readback_requires_an_applied_unsigned_value(status, expected, state, observed):
    result = core.audio_capability_readback(status, expected)

    assert result == {
        "state": state,
        "requested_value": expected,
        "current_value": observed,
        "effective_state_confirmed": state == "confirmed",
    }


def controls_input(channel_audio_metadata):
    return {
        "name": None,
        "counts": {
            "tx_count": 1,
            "rx_count": 1,
            "locked": None,
            "transmit_flow_authoring_capability_word": 0,
            "receiver_telemetry_capacity": None,
        },
        "aes67": None,
        "settings": None,
        "channel_audio_metadata": channel_audio_metadata,
        "rx": [],
        "tx": [],
    }


@pytest.mark.parametrize(
    "capability_word,protocol,identity_field,identifier_max,media_modes",
    [
        (0, 0x2729, "global_flow_id", 32, ["native_dante"]),
        (0x1000, 0x2809, "media_local_flow_id", 65535, ["native_dante", "rtp_aes67"]),
    ],
)
def test_native_authoring_profile_survives_device_serialization(
    capability_word, protocol, identity_field, identifier_max, media_modes
):
    device = DanteDevice()
    data = controls_input(None)
    data["counts"]["transmit_flow_authoring_capability_word"] = capability_word
    device.apply_controls(device.controls_data_from_core(data))

    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["transmit_flow_authoring"] == {
        "protocol_id": protocol,
        "identity_field": identity_field,
        "identifier_max": identifier_max,
        "media_modes": media_modes,
        "supports_flow_options": capability_word == 0x1000,
    }

    data["counts"]["transmit_flow_authoring_capability_word"] = None
    device.apply_controls(device.controls_data_from_core(data))
    assert getattr(device, "transmit_flow_authoring", None) is None


@pytest.mark.asyncio
async def test_control_fetch_reuses_rx_inventory_metadata_and_applies_property_capabilities(monkeypatch):
    device = DanteDevice()
    core_client = MagicMock()
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
    core_client.execute.side_effect = lambda spec: (
        bytes.fromhex("28090010000110000001000000020002") if spec["command"] == "channel_count" else None
    )
    core_client.get_property_directory.return_value = {
        "properties": [{"property_id": 0x8020, "flags": 0x0001}],
        "aes67_configured_property_advertised": False,
    }
    device.ipv4 = "192.0.2.10"

    async def call_core(operation, **_options):
        return operation(core_client)

    monkeypatch.setattr(device, "call_core", call_core)

    controls = await device.fetch_controls_data()

    assert controls["aes67_configured_property_advertised"] is False
    assert "aes67_configuration_supported" not in controls
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
