from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.channel_status_paging import (
    ChannelStatusPageAccumulator,
    ChannelStatusPaginationError,
    modern_arc_protocol_identifier_for_device,
)
from netaudio.dante.const import (
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    PROTOCOL_ARC_2809,
    PROTOCOL_ARC_280F,
    SERVICE_ARC,
)
from netaudio.dante.application import DanteApplication
from tests.modern_arc_test_support import modern_arc_fixture, modern_arc_payloads


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


def _parsed_pages(path: str, source_port: int, response_kind: str) -> list[dict]:
    return [
        core.parse_response(response_kind, payload)
        for payload in modern_arc_payloads("pagination", path, source_port=source_port)
    ]


def test_fixture_records_exact_digest_bound_capture_provenance():
    captures = modern_arc_fixture()["_provenance"]["captures"]
    assert captures["controller-pagination-8112.pcap"]["sha256"] == (
        "c3497651fe101f9073486b25b465d816a9e63b05bc0e28f2990fa028f38e042c"
    )
    assert captures["controller-flow-baseline.pcap"]["sha256"] == (
        "1775e0c7d171df638388d1d2a73ea4f4acff409f3bd48184d9d03e8072ab0abd"
    )


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.8.15", PROTOCOL_ARC_280F),
        ("2.8.9", PROTOCOL_ARC_2809),
    ],
)
def test_modern_arc_protocol_selection_uses_the_exact_advertised_version(version, expected):
    assert modern_arc_protocol_identifier_for_device(_arc_device(version)) == expected


@pytest.mark.parametrize("version", ["2.8.16", "invalid", ""])
def test_modern_arc_protocol_selection_rejects_unrecognized_versions(version):
    with pytest.raises(ChannelStatusPaginationError, match="unsupported ARC protocol version"):
        modern_arc_protocol_identifier_for_device(_arc_device(version))


def test_managed_device_uses_observed_2809_protocol_without_mdns_metadata():
    device = SimpleNamespace(requires_managed_control=True, services={})

    assert modern_arc_protocol_identifier_for_device(device) == PROTOCOL_ARC_2809


@pytest.mark.parametrize(
    ("path", "command_name"),
    [
        ("transmitter_0x2400", "query_transmitter_channel_status_2809"),
        ("receiver_0x3400", "query_receiver_channel_status_2809"),
    ],
)
def test_public_command_builder_reproduces_every_captured_280f_request(path, command_name):
    for request in modern_arc_payloads("pagination", path, source_port=49_818):
        built = core.build_command(
            {
                "command": command_name,
                "protocol_id": PROTOCOL_ARC_280F,
                "media_type": int.from_bytes(request[18:20], "big"),
                "starting_channel_identifier": int.from_bytes(request[20:22], "big"),
                "ending_channel_identifier": int.from_bytes(request[22:24], "big"),
                "transaction_id": int.from_bytes(request[4:6], "big"),
            }
        )
        assert built == request


@pytest.mark.asyncio
async def test_transmitter_operation_fetches_and_merges_all_four_captured_pages():
    responses = modern_arc_payloads("pagination", "transmitter_0x2400", source_port=4_840)
    device = _arc_device("2.8.15", responses)

    result = await DanteApplication().query_transmitter_channel_status_2809(device)

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

    result = await DanteApplication().query_receiver_channel_status_2809(device)

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


def test_page_accumulator_rejects_no_progress_conflicts_and_page_limit():
    pages = _parsed_pages(
        "transmitter_0x2400",
        4_840,
        "transmitter_channel_status_page_2809",
    )

    no_progress = ChannelStatusPageAccumulator(PROTOCOL_ARC_280F, OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809)
    assert no_progress.add(pages[0]) == (3, 17, 0)
    with pytest.raises(ChannelStatusPaginationError, match="no progress"):
        no_progress.add(pages[0])

    conflicting = deepcopy(pages[1])
    conflicting["records"][0]["channel_number"] = 1
    global_conflict = ChannelStatusPageAccumulator(
        PROTOCOL_ARC_280F,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    )
    global_conflict.add(pages[0])
    with pytest.raises(ChannelStatusPaginationError, match="conflicting global ID"):
        global_conflict.add(conflicting)

    duplicate = deepcopy(pages[1])
    duplicate["records"][0] = deepcopy(pages[0]["records"][0])
    duplicate["records"][0]["friendly_channel_name"] = "conflicting"
    duplicate_conflict = ChannelStatusPageAccumulator(
        PROTOCOL_ARC_280F,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    )
    duplicate_conflict.add(pages[0])
    with pytest.raises(ChannelStatusPaginationError, match="conflicting duplicate"):
        duplicate_conflict.add(duplicate)

    gapped = deepcopy(pages[0])
    gapped["records"][1]["media_local_channel_id"] = 17
    first_unresolved = ChannelStatusPageAccumulator(
        PROTOCOL_ARC_280F,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    )
    assert first_unresolved.add(gapped) == (3, 2, 0)

    bounded = ChannelStatusPageAccumulator(
        PROTOCOL_ARC_280F,
        OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
        maximum_pages=1,
    )
    bounded.add(pages[0])
    with pytest.raises(ChannelStatusPaginationError, match="page limit"):
        bounded.add(pages[1])


def test_flow_fixtures_expose_media_identity_and_ordered_audio_slots():
    baseline_response = modern_arc_payloads(
        "transmitter_flow_0x2600",
        "accepted_audio_baseline",
        source_port=4_940,
    )[-1]
    baseline = core.parse_response("transmitter_flow_status_page", baseline_response)

    assert [
        (flow["global_flow_id"], flow["media_type"], flow["media_local_flow_id"]) for flow in baseline["flows"]
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
    assert [(flow["global_flow_id"], flow["media_type"], flow["media_local_flow_id"]) for flow in mixed["flows"]] == [
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
