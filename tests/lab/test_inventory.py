from __future__ import annotations

from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription_operations import plan_receiver_subscription_commands

PLANNED_TRANSMITTER = "netaudio-lab-plan"


def channel_names(device: dict) -> dict[str, dict[str, str]]:
    return {
        side: {number: channel.get("name") for number, channel in device["channels"][side].items()}
        for side in ("receivers", "transmitters")
    }


def test_channel_counts(inventory, lab_device):
    device = inventory[lab_device]
    assert len(device["channels"]["receivers"]) == device["rx_count"]
    assert len(device["channels"]["transmitters"]) == device["tx_count"]


def test_channels_named(inventory, lab_device):
    names = channel_names(inventory[lab_device])
    assert [
        f"{side} {number}" for side, channels in names.items() for number, name in channels.items() if not name
    ] == []


def test_discovered(inventory, lab_device):
    device = inventory[lab_device]
    assert device["ipv4"]
    assert device["sample_rate_hz"]


def test_receivers_plan_subscriptions(inventory, lab_device):
    device = DanteDeviceSerializer.device_from_json(inventory[lab_device])
    records = [
        {"action": "set", "rx_channel": channel.number, "tx_channel": "1", "tx_device": PLANNED_TRANSMITTER}
        for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number)
    ]
    if records:
        assert plan_receiver_subscription_commands(device, records)


def test_subscriptions_name_receivers(netaudio, inventory, lab_device):
    receivers = {int(number) for number in inventory[lab_device]["channels"]["receivers"]}
    for subscription in netaudio.subscriptions(lab_device):
        assert subscription["rx_channel_number"] in receivers
        assert subscription["tx_channel"]
        assert subscription["tx_device"]


def test_transports_agree(direct_inventory, daemon_inventory, lab_device):
    direct = direct_inventory[lab_device]
    daemon = daemon_inventory[lab_device]
    assert channel_names(daemon) == channel_names(direct)
    assert (daemon["rx_count"], daemon["tx_count"], daemon["sample_rate_hz"]) == (
        direct["rx_count"],
        direct["tx_count"],
        direct["sample_rate_hz"],
    )
