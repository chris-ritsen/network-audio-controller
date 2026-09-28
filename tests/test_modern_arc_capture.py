from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.arc_protocol import (
    ArcProtocolError,
    modern_arc_protocol_identifier_for_device,
)
from netaudio.dante.const import (
    PROTOCOL_ARC_2809,
    PROTOCOL_ARC_280C,
    PROTOCOL_ARC_280F,
    SERVICE_ARC,
)
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from tests.modern_arc_test_support import modern_arc_payloads


def _arc_device(version: str, responses: list[bytes] | None = None):
    return SimpleNamespace(
        execute=AsyncMock(side_effect=responses or []),
        services={
            "arc": {
                "type": SERVICE_ARC,
                "properties": {"arcp_vers": version},
            }
        },
    )


def _without_transaction_id(payload: bytes) -> bytes:
    return payload[:4] + bytes(2) + payload[6:]


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["rx", "tx"])
@pytest.mark.parametrize("version", [None, "", "2.8.256", "invalid"])
async def test_device_inventory_never_falls_back_to_legacy_without_a_known_revision(direction, version):
    application = DanteApplication()
    device = DanteDevice("receiver.local.", app=application)
    device.ipv4 = "192.0.2.1"
    device.services = _arc_device(version).services
    device.call_core = AsyncMock()
    device.execute = AsyncMock()

    with pytest.raises(ArcProtocolError):
        await getattr(device, f"get_{direction}_channels")()

    device.call_core.assert_not_awaited()
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("direction,path", [("rx", "receiver_0x3400"), ("tx", "transmitter_0x2400")])
async def test_device_inventory_consumes_native_protocol_and_media_metadata(direction, path):
    device = DanteDevice("receiver.local.", app=DanteApplication())
    device.ipv4 = "192.0.2.1"
    device.services = _arc_device("2.8.15").services
    device.execute = AsyncMock(side_effect=modern_arc_payloads("pagination", path, source_port=4_840))

    await getattr(device, f"get_{direction}_channels")()

    channels = getattr(device, f"{direction}_channels")
    assert len(channels) == 64
    assert all(channel.media_type == "audio" for channel in channels.values())


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.8.9", PROTOCOL_ARC_2809),
        ("2.8.12", PROTOCOL_ARC_280C),
        ("2.8.15", PROTOCOL_ARC_280F),
        ("2.8.16", PROTOCOL_ARC_280F),
        ("2.9.0", PROTOCOL_ARC_280F),
    ],
)
def test_modern_arc_protocol_selection_caps_at_280f(version, expected):
    assert modern_arc_protocol_identifier_for_device(_arc_device(version)) == expected


@pytest.mark.parametrize("version", ["", "2.8.256", "invalid"])
def test_modern_arc_protocol_selection_rejects_unrecognized_versions(version):
    with pytest.raises(ArcProtocolError, match="unsupported ARC protocol version"):
        modern_arc_protocol_identifier_for_device(_arc_device(version))


def test_managed_device_uses_observed_2809_protocol_without_mdns_metadata():
    device = SimpleNamespace(requires_managed_control=True, services={})

    assert modern_arc_protocol_identifier_for_device(device) == PROTOCOL_ARC_2809


@pytest.mark.parametrize(
    ("path", "command_name"),
    [
        ("transmitter_0x2400", "query_modern_arc_transmitter_channel_status"),
        ("receiver_0x3400", "query_modern_arc_receiver_channel_status"),
    ],
)
def test_public_command_builder_reproduces_every_captured_280f_request(path, command_name):
    for request in modern_arc_payloads("pagination", path, source_port=49_818):
        built = core.build_command(
            {
                "command": command_name,
                "protocol_id": PROTOCOL_ARC_280F,
                "media_selector": int.from_bytes(request[18:20], "big"),
                "starting_channel_identifier": int.from_bytes(request[20:22], "big"),
                "ending_channel_identifier": int.from_bytes(request[22:24], "big"),
                "message_id": int.from_bytes(request[4:6], "big"),
            }
        )
        assert built == request


@pytest.mark.asyncio
async def test_transmitter_operation_fetches_and_merges_all_four_captured_pages():
    responses = modern_arc_payloads("pagination", "transmitter_0x2400", source_port=4_840)
    device = _arc_device("2.8.15", responses)

    result = await DanteApplication().query_modern_arc_transmitter_channel_status(device)

    assert result["protocol_id"] == PROTOCOL_ARC_280F
    assert result["page_count"] == 4
    assert result["page_capacities"] == [32, 32, 32, 32]
    assert result["total_record_count"] == 64
    assert [record["channel_number"] for record in result["records"]] == list(range(1, 65))
    assert result["records"][32]["channel_number"] == 33
    assert result["records"][32]["media_local_channel_id"] == 33

    actual_requests = [core.build_command(awaited.args[0]) for awaited in device.execute.await_args_list]
    captured_requests = modern_arc_payloads("pagination", "transmitter_0x2400", source_port=49_818)
    assert [_without_transaction_id(request) for request in actual_requests] == [
        _without_transaction_id(request) for request in captured_requests
    ]


@pytest.mark.asyncio
async def test_receiver_operation_fetches_a_short_final_page_and_merges_all_records():
    responses = modern_arc_payloads("pagination", "receiver_0x3400", source_port=4_840)
    device = _arc_device("2.8.15", responses)

    result = await DanteApplication().query_modern_arc_receiver_channel_status(device)

    assert result["protocol_id"] == PROTOCOL_ARC_280F
    assert result["page_count"] == 6
    assert result["page_capacities"] == [16, 16, 16, 16, 16, 16]
    assert result["total_record_count"] == 64
    assert result["records"][-1]["channel_number"] == 64
    assert result["records"][-1]["media_local_channel_id"] == 64

    actual_requests = [core.build_command(awaited.args[0]) for awaited in device.execute.await_args_list]
    captured_requests = modern_arc_payloads("pagination", "receiver_0x3400", source_port=49_818)
    assert [_without_transaction_id(request) for request in actual_requests] == [
        _without_transaction_id(request) for request in captured_requests
    ]


def test_flow_fixtures_expose_media_identity_and_ordered_audio_slots():
    baseline_response = modern_arc_payloads(
        "transmitter_flow_0x2600",
        "accepted_audio_baseline",
        source_port=4_940,
    )[-1]
    baseline = core.parse_response("transmitter_flow_status_page", baseline_response)

    assert [
        (flow["global_flow_id"], flow["media_type_code"], flow["media_local_flow_id"]) for flow in baseline["flows"]
    ] == [(1, 3, 1), (2, 3, 2), (3, 3, 3)]
    assert [flow["transmitter_channel_ids_by_slot"] for flow in baseline["flows"]] == [
        [5, 6, 7, 8],
        [1, 3, 2, 4],
        [7, 8, 0, 0],
    ]
    assert [flow["populated_slot_count"] for flow in baseline["flows"]] == [4, 4, 2]
    assert {flow["channel_slot_segment_header"] for flow in baseline["flows"]} == {0x0709}


def test_mixed_media_and_rejected_treatment_keep_media_local_identity_separate():
    mixed_response = modern_arc_payloads(
        "transmitter_flow_0x2600",
        "accepted_mixed_media",
        source_port=5_040,
    )[-1]
    mixed = core.parse_response("transmitter_flow_status_page", mixed_response)
    assert [
        (flow["global_flow_id"], flow["media_type_code"], flow["media_local_flow_id"]) for flow in mixed["flows"]
    ] == [
        (1, 3, 1),
        (2, 4, 1),
    ]
    assert mixed["flows"][1]["channel_slot_count"] is None
    assert mixed["flows"][1]["transmitter_channel_ids_by_slot"] == []

    rejected_response = modern_arc_payloads(
        "transmitter_flow_0x2600",
        "rejected_media_local_identity_treatment",
        source_port=4_940,
    )[0]
    rejected = core.parse_response("transmitter_flow_status_page", rejected_response)
    assert [flow["media_local_flow_id"] for flow in rejected["flows"]] == [21, 38, 55]
