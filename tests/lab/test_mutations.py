from __future__ import annotations

import pytest

CHECK_NAME = "netaudio-lab-check"
VIRTUAL_CHANNEL_STATUS_GAP = "netaudio virtual devices do not answer modern ARC channel status queries"


def require_writable(name: str | None) -> str:
    if name is None:
        pytest.skip("the lab names no writable device")
    return name


def channel_name(netaudio, device: str, side: str, number: str) -> str:
    return netaudio.device(device)["channels"][side][number]["name"]


@pytest.mark.parametrize(("side", "reference"), [("receivers", "rx:1"), ("transmitters", "tx:1")])
def test_channel_rename_round_trip(request, lab, netaudio, direct_netaudio, writable_device, side, reference):
    device = require_writable(writable_device)
    if lab.named(device).virtual:
        request.applymarker(pytest.mark.xfail(reason=VIRTUAL_CHANNEL_STATUS_GAP, strict=True))
    original = channel_name(direct_netaudio, device, side, "1")
    assert original != CHECK_NAME
    try:
        netaudio.run("--verify", "-n", device, "channel", "name", reference, CHECK_NAME)
        assert channel_name(direct_netaudio, device, side, "1") == CHECK_NAME
    finally:
        netaudio.run("--verify", "-n", device, "channel", "name", reference, original)
    assert channel_name(direct_netaudio, device, side, "1") == original


def test_latency_round_trip(netaudio, direct_netaudio, writable_device):
    device = require_writable(writable_device)
    state = direct_netaudio.device(device)
    original = state["configured_latency_ms"]
    choices = [choice for choice in state["standard_latency_choices_ms"] if choice != original]
    if not choices:
        pytest.skip(f"{device} offers no other standard latency")
    try:
        netaudio.run("-n", device, "device", "config", "latency", f"{choices[0]:g}")
        assert direct_netaudio.device(device)["configured_latency_ms"] == choices[0]
    finally:
        netaudio.run("-n", device, "device", "config", "latency", f"{original:g}")
    assert direct_netaudio.device(device)["configured_latency_ms"] == original


def test_subscription_round_trip(lab, netaudio, direct_netaudio):
    if len(lab.writable) < 2:
        pytest.skip("the lab names fewer than two writable devices")
    transmitter, receiver = lab.writable[0].name, lab.writable[1].name
    receivers = sorted(int(number) for number in direct_netaudio.device(receiver)["channels"]["receivers"])
    occupied = {subscription["rx_channel_number"] for subscription in direct_netaudio.subscriptions(receiver)}
    free = [number for number in receivers if number not in occupied]
    if not free:
        pytest.skip(f"every receiver on {receiver} is subscribed")
    number = free[0]
    source = channel_name(direct_netaudio, transmitter, "transmitters", "1")
    netaudio.run("--verify", "subscription", "add", "--tx", f"1@{transmitter}", "--rx", f"{number}@{receiver}")
    try:
        added = [
            (subscription["tx_device"], subscription["tx_channel"])
            for subscription in direct_netaudio.subscriptions(receiver)
            if subscription["rx_channel_number"] == number
        ]
        assert added == [(transmitter, source)]
    finally:
        netaudio.run("--verify", "subscription", "remove", "--rx", f"{number}@{receiver}")
    assert number not in {subscription["rx_channel_number"] for subscription in direct_netaudio.subscriptions(receiver)}
