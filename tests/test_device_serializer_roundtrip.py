import json

from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription import DanteSubscription


def make_device():
    device = DanteDevice(server_name="avio.local.")
    device.ipv4 = "192.168.1.60"
    device.name = "Studio-AVIO"
    device.online = True
    device.mac_address = "001dc1aabbcc"
    device.model_id = "DAI2"
    device.sample_rate = 48000
    device.supported_sample_rates = [44100, 48000]
    device.encoding = 24
    device.supported_encodings = [24, 16, 32]
    device.latency = 0.15
    device.active_latency = 0.15
    device.configured_latency = 0.25
    device.default_latency = 1.0
    device.min_latency = 0.15
    device.max_latency = 21.333334
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


class TestSerializerRoundtrip:
    def test_scalar_fields_survive(self):
        restored = roundtrip(make_device())
        assert restored.server_name == "avio.local."
        assert str(restored.ipv4) == "192.168.1.60"
        assert restored.name == "Studio-AVIO"
        assert restored.online is True
        assert restored.mac_address == "001dc1aabbcc"
        assert restored.model_id == "DAI2"
        assert restored.sample_rate == 48000
        assert restored.supported_sample_rates == [44100, 48000]
        assert restored.encoding == 24
        assert restored.supported_encodings == [24, 16, 32]
        assert restored.latency == 0.15
        assert restored.active_latency == 0.15
        assert restored.configured_latency == 0.25
        assert restored.default_latency == 1.0
        assert restored.min_latency == 0.15
        assert restored.max_latency == 21.333334
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

    def test_offline_device_survives(self):
        device = make_device()
        device.online = False
        restored = roundtrip(device)
        assert restored.online is False
