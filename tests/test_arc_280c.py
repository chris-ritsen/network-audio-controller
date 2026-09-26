import json
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device import DanteDevice
from netaudio.dante.dissection.header import parse_packet_header


@pytest.fixture
def captures(load_fixture):
    return json.loads(load_fixture("arc_280c_capture.json"))


def device_for_capture():
    device = DanteDevice("capture.local.", app=DanteApplication())
    device.ipv4 = "192.0.2.1"
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.12"}}}
    return device


@pytest.mark.asyncio
@pytest.mark.parametrize("model,tx_count,rx_count", [("shure_mxwapx4", 5, 4), ("shure_mxa920", 10, 5)])
async def test_captured_revision_reaches_device_inventory_without_rewriting_packets(
    captures, model, tx_count, rx_count
):
    device = device_for_capture()
    device.execute = AsyncMock(
        side_effect=[
            bytes.fromhex(captures[model][query]["response"])
            for query in ("transmitter_channel_status", "receiver_channel_status")
        ]
    )

    await device.get_tx_channels()
    await device.get_rx_channels()

    assert len(device.tx_channels) == tx_count
    assert len(device.rx_channels) == rx_count
    assert device.execute.await_count == 2
    for call in device.execute.await_args_list:
        assert call.args[0]["protocol_id"] == 0x280C


def test_capture_diagnostics_recognize_every_contributed_exchange(captures):
    for model in ("shure_mxwapx4", "shure_mxa920"):
        for exchange in captures[model].values():
            for direction, encoded in exchange.items():
                packet = bytes.fromhex(encoded)
                header = parse_packet_header(packet)
                assert header is not None
                assert header["protocol_id"] == 0x280C
                assert header["opcode"] == int.from_bytes(packet[6:8], "big")
                if direction == "response":
                    assert header["result_accepted"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["set", "clear"])
async def test_modern_revision_never_falls_back_to_legacy_subscription_writes(captures, action):
    device = device_for_capture()
    device.execute = AsyncMock(
        return_value=bytes.fromhex(captures["shure_mxa920"]["receiver_channel_status"]["response"])
    )
    await device.get_rx_channels()
    device.execute.reset_mock()
    record = {"action": action, "rx_channel": 1}
    if action == "set":
        record.update(tx_channel="Mic", tx_device="Source")

    await device.application._send_subscription_records(device, [record])
    device.execute.assert_awaited_once()
    packet = core.build_command(device.execute.call_args.args[0])
    assert packet[:2] == bytes.fromhex("280c")
    assert packet[6:8] == bytes.fromhex("3410")


def test_modern_revision_reconciliation_skips_satisfied_routes():
    request = {
        "protocol_id": 0x280C,
        "managed": False,
        "channels": [{"number": 1, "media_type_code": 3}],
        "subscriptions": [{"number": 1, "tx_channel": "Mic", "tx_device": "Source"}],
        "expected": [{"number": 1, "source": ["Mic", "Source"]}],
    }
    assert core.plan_subscription_reconciliation(request)["batches"] == []
    request["expected"] = [{"number": 1, "source": None}]
    assert len(core.plan_subscription_reconciliation(request)["batches"]) == 1


def test_receiver_flow_continuation_preserves_protocol():
    packet = core.build_command(
        {"command": "query_modern_arc_receiver_flow_status", "protocol_id": 0x280C, "starting_flow": 2}
    )
    assert packet[16:24] == bytes.fromhex("0001000100020000")
