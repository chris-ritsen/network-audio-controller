import json

import pytest

from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription import DanteSubscription


@pytest.mark.parametrize("direction,channel_type,label", [("input", "tx", "+24 dBu"), ("output", "rx", "+18 dBu")])
def test_gain_presentation_uses_direction_and_preserves_unknown_levels(direction, channel_type, label):
    device = DanteDevice(server_name="analog.local.")
    device.gain_device_type = direction
    device.gain_levels = [1, 99]
    device.supported_gain_levels = [1, 99]

    serialized = DanteDeviceSerializer.to_json(device)

    assert serialized["gain_level_choices"] == [{"value": 1, "label": label}, {"value": 99, "label": "Unknown"}]
    assert device.gain_level_label_for_channel(1, channel_type) == label
    assert device.gain_level_label_for_channel(2, channel_type) == "Unknown"
    assert device.gain_level_for_channel(1, "rx" if channel_type == "tx" else "tx") is None


def test_device_clock_source_presentation_matches_native_choices():
    device = DanteDevice(server_name="clock.local.")
    device.clock_source_code = 1
    device.supported_clock_sources = [1, 99]

    serialized = DanteDeviceSerializer.to_json(device)

    assert serialized["clock_source"] == "external/BNC"
    assert serialized["clock_source_choices"] == [
        {"code": 0, "label": "internal"},
        {"code": 1, "label": "external/BNC"},
    ]


def make_device():
    device = DanteDevice(server_name="avio.local.")
    device.ipv4 = "192.168.1.60"
    device.name = "Studio-AVIO"
    device.online = True
    device.mac_address = "001dc1aabbcc"
    device.model_id = "DAI2"
    device.bluetooth_connected = False
    device.sample_rate = 48000
    device.supported_sample_rates = [44100, 48000]
    device.encoding = 24
    device.supported_encodings = [24, 16, 32]
    device.aes67_configuration_supported = True
    device.dante_model_primary_capabilities = 0x8E78F65A
    device.dante_model_monitoring_capabilities = 0x1B
    device.detailed_metering_supported = True
    device.interface_statistics_supported = True
    device.clock_monitoring_supported = True
    device.per_channel_signal_presence_supported = False
    device.rx_flow_maximum_latency_monitoring_supported = True
    device.rx_flow_late_packet_monitoring_supported = True
    device.settings_properties = [
        {"property_id": 0x8020, "flags": 0x0001},
        {"property_id": 0x0063, "flags": 0x0001},
    ]
    device.gain_device_type = "output"
    device.gain_levels = [4, 5]
    device.supported_gain_levels = [1, 2, 3, 4, 5]
    device.latency = 0.15
    device.active_latency = 0.15
    device.configured_latency = 0.25
    device.default_latency = 1.0
    device.min_latency = 0.15
    device.max_latency = 21.333334
    device.link_speed_mbps = 100
    device.is_locked = False
    device.tx_count = 2
    device.rx_count = 2
    device.tx_count_raw = 2
    device.rx_count_raw = 2
    device.last_seen = 1765000000.0
    device.services = {
        "Studio-AVIO._netaudio-arc._udp.local.": {
            "type": "_netaudio-arc._udp.local.",
            "port": 4440,
            "ipv4": "192.168.1.60",
        }
    }

    for number, name in ((1, "ch1"), (2, "ch2")):
        channel = DanteChannel()
        channel.channel_type = "rx"
        channel.device = device
        channel.number = number
        channel.name = name
        channel.friendly_name = f"Friendly {number}"
        device.rx_channels[number] = channel

    tx_channel = DanteChannel()
    tx_channel.channel_type = "tx"
    tx_channel.device = device
    tx_channel.number = 1
    tx_channel.name = "out1"
    device.tx_channels[1] = tx_channel

    subscription = DanteSubscription()
    subscription.rx_channel = device.rx_channels[1]
    subscription.rx_device = device
    subscription.rx_channel_name = "ch1"
    subscription.rx_device_name = "Studio-AVIO"
    subscription.tx_channel_name = "out1"
    subscription.tx_device_name = "Mixer"
    subscription.status_code = 0x0009
    subscription.rx_channel_status_code = 0x0009
    device.subscriptions = [subscription]

    return device


def roundtrip(device):
    wire = json.loads(json.dumps(DanteDeviceSerializer.to_json(device), default=str))
    return DanteDeviceSerializer.device_from_json(wire)


def test_subscription_identity_survives_duplicate_labels_reordering_and_preset_export():
    from netaudio.dante.subscription_operations import subscription_sources
    from netaudio.monitoring.signals import snapshot_from_device
    from netaudio.presets.serialization import device_preset_config

    device = make_device()

    for channel in device.rx_channels.values():
        channel.name = "Duplicate"
        channel.friendly_name = None

    second = DanteSubscription()
    second.rx_channel = device.rx_channels[2]
    second.rx_device = device
    second.tx_channel_name = "out2"
    second.tx_device_name = "Mixer"
    device.subscriptions.insert(0, second)
    wire = json.loads(json.dumps(DanteDeviceSerializer.to_json(device), default=str))

    assert [item["rx_channel_number"] for item in wire["subscriptions"]] == [2, 1]

    restored = DanteDeviceSerializer.device_from_json(wire)
    assert restored.subscriptions[0].rx_channel is restored.rx_channels[2]
    assert restored.subscriptions[1].rx_channel is restored.rx_channels[1]
    assert subscription_sources(restored, [1, 2]) == {1: ("out1", "Mixer"), 2: ("out2", "Mixer")}

    saved = device_preset_config(restored, {"routing"})
    assert saved["rx_subscriptions"][1]["tx_channel"] == "out1"
    assert saved["rx_subscriptions"][2]["tx_channel"] == "out2"
    assert snapshot_from_device(restored)["subscriptions"] == DanteDeviceSerializer.to_json(restored)["subscriptions"]


@pytest.mark.parametrize("identity", [None, True, 0, "1", 99])
def test_missing_or_invalid_subscription_identity_never_resolves_by_label(identity):
    from netaudio.dante.subscription_operations import subscription_sources
    from netaudio.presets.serialization import device_preset_config

    wire = DanteDeviceSerializer.to_json(make_device())
    wire["subscriptions"][0]["rx_channel_number"] = identity
    restored = DanteDeviceSerializer.device_from_json(wire)

    assert restored.subscriptions[0].rx_channel is None

    with pytest.raises(RuntimeError, match="identity"):
        subscription_sources(restored, [1])

    with pytest.raises(ValueError, match="identity"):
        device_preset_config(restored, {"routing"})


def test_channel_status_refresh_keeps_same_name_subscriptions_separate():
    device = make_device()

    for channel in device.rx_channels.values():
        channel.name = "Duplicate"

    device.apply_receiver_channel_inventory(
        {
            "records": [
                {
                    "channel_number": 2,
                    "local_channel_name": "Duplicate",
                    "source_device_name": "Mixer",
                    "source_channel_name": "out2",
                },
                {
                    "channel_number": 1,
                    "local_channel_name": "Renamed",
                    "source_device_name": "Mixer",
                    "source_channel_name": "out1",
                },
            ]
        }
    )

    assert {item.rx_channel.number: item.tx_channel_name for item in device.subscriptions} == {1: "out1", 2: "out2"}

    device.apply_receiver_channel_inventory(
        {
            "records": [
                {"channel_number": 2, "local_channel_name": "Renamed"},
                {
                    "channel_number": 1,
                    "local_channel_name": "Renamed",
                    "source_device_name": "Mixer",
                    "source_channel_name": "out1",
                },
            ]
        }
    )

    assert [(item.rx_channel.number, item.tx_channel_name) for item in device.subscriptions] == [(1, "out1")]


class TestSerializerRoundtrip:
    def test_scalar_fields_survive(self):
        restored = roundtrip(make_device())
        assert restored.server_name == "avio.local."
        assert str(restored.ipv4) == "192.168.1.60"
        assert restored.name == "Studio-AVIO"
        assert restored.online is True
        assert restored.mac_address == "001dc1aabbcc"
        assert restored.model_id == "DAI2"
        assert restored.bluetooth_connected is False
        assert restored.bluetooth_device is None
        assert restored.sample_rate == 48000
        assert restored.supported_sample_rates == [44100, 48000]
        assert restored.encoding == 24
        assert restored.supported_encodings == [24, 16, 32]
        assert restored.aes67_configuration_supported is True
        assert restored.dante_model_primary_capabilities == 0x8E78F65A
        assert restored.dante_model_monitoring_capabilities == 0x1B
        assert restored.detailed_metering_supported is True
        assert restored.interface_statistics_supported is True
        assert restored.clock_monitoring_supported is True
        assert restored.per_channel_signal_presence_supported is False
        assert restored.rx_flow_maximum_latency_monitoring_supported is True
        assert restored.rx_flow_late_packet_monitoring_supported is True
        assert restored.settings_properties == [
            {"property_id": 0x8020, "flags": 0x0001},
            {"property_id": 0x0063, "flags": 0x0001},
        ]
        assert restored.gain_device_type == "output"
        assert restored.gain_levels == [4, 5]
        assert restored.supported_gain_levels == [1, 2, 3, 4, 5]
        assert restored.gain_level_choices == [
            {"value": 1, "label": "+18 dBu"},
            {"value": 2, "label": "+4 dBu"},
            {"value": 3, "label": "0 dBu"},
            {"value": 4, "label": "0 dBV"},
            {"value": 5, "label": "-10 dBV"},
        ]
        assert restored.latency == 0.15
        assert restored.active_latency == 0.15
        assert restored.configured_latency == 0.25
        assert restored.default_latency == 1.0
        assert restored.min_latency == 0.15
        assert restored.max_latency == 21.333334
        assert restored.link_speed_mbps == 100
        assert restored.standard_latency_choices == [0.15, 0.25, 0.5, 1.0, 2.0, 5.0]
        assert restored.is_locked is False
        assert restored.tx_count == 2
        assert restored.rx_count == 2
        assert restored.tx_count_raw == 2
        assert restored.rx_count_raw == 2
        assert restored.last_seen == 1765000000.0

    def test_services_survive_for_port_resolution(self):
        restored = roundtrip(make_device())
        service = restored.get_service("_netaudio-arc._udp.local.")
        assert service is not None
        assert service["port"] == 4440
        assert restored._arc_port() == 4440

    def test_channels_survive_with_numbers_and_names(self):
        restored = roundtrip(make_device())
        assert set(restored.rx_channels.keys()) == {1, 2}
        assert restored.rx_channels[1].name == "ch1"
        assert restored.rx_channels[1].friendly_name == "Friendly 1"
        assert restored.rx_channels[1].device.gain_level_for_channel(1, "rx") == 4
        assert DanteDeviceSerializer.channel_to_json(restored.rx_channels[1])["gain_level_label"] == "0 dBV"
        assert restored.rx_channels[1].channel_type == "rx"
        assert restored.rx_channels[1].device is restored
        assert restored.tx_channels[1].name == "out1"

    def test_subscriptions_survive_with_status_codes(self):
        restored = roundtrip(make_device())
        assert len(restored.subscriptions) == 1
        subscription = restored.subscriptions[0]
        assert subscription.rx_channel_name == "ch1"
        assert subscription.tx_channel_name == "out1"
        assert subscription.tx_device_name == "Mixer"
        assert subscription.status_code == 0x0009
        assert subscription.rx_channel_status_code == 0x0009

    def test_locked_device_survives(self):
        device = make_device()
        device.is_locked = True
        restored = roundtrip(device)
        assert restored.is_locked is True

    def test_unknown_lock_state_is_explicit_in_serialized_state(self):
        device = make_device()
        device.is_locked = None

        serialized = DanteDeviceSerializer.to_json(device)
        restored = DanteDeviceSerializer.device_from_json(serialized)

        assert "is_locked" in serialized
        assert serialized["is_locked"] is None
        assert restored.is_locked is None

    def test_offline_device_survives(self):
        device = make_device()
        device.online = False
        restored = roundtrip(device)
        assert restored.online is False
