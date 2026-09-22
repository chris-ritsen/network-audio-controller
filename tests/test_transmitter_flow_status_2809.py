import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.const import SERVICE_ARC
from tests.protocol_test_fixtures import load_protocol_packet


@pytest.mark.asyncio
async def test_transmitter_flow_operation_rejects_revision_change():
    from netaudio.dante.application import DanteApplication

    response = bytearray(_packet(0x2809, 0x2600, 7196))
    response[:2] = (0x280F).to_bytes(2, "big")
    device = SimpleNamespace(
        execute=AsyncMock(return_value=bytes(response)),
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}},
    )

    with pytest.raises(RuntimeError, match="protocol"):
        await DanteApplication().query_modern_arc_transmitter_flow_status(device)


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [None, b"truncated", bytes.fromhex("2809000a000126000030")])
async def test_transmitter_flow_operation_rejects_unavailable_or_invalid_pages(response):
    from netaudio.dante.application import DanteApplication

    device = SimpleNamespace(
        execute=AsyncMock(return_value=response),
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}},
    )

    with pytest.raises(RuntimeError) as error:
        await DanteApplication().query_modern_arc_transmitter_flow_status(device)

    assert "0x" not in str(error.value)


def _packet(protocol_identifier: int, opcode: int, packet_identifier: int) -> bytes:
    return load_protocol_packet(
        "transmitter_flow_status",
        f"protocol_{protocol_identifier:04x}_opcode_{opcode:04x}_id_{packet_identifier}.bin",
    )


def test_query_builder_is_byte_identical_to_the_controller_zero_tail_form():
    request = _packet(0x2809, 0x2600, 7185)
    built = core.build_command(
        {
            "command": "query_tx_flows",
            "flow_protocol_id": 0x2809,
            "starting_flow": 1,
            "message_id": 0x0225,
        }
    )

    assert built == request


def test_parser_preserves_zero_unicast_and_multicast_status_records():
    zero_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 7196),
    )
    causal_pre_action_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 29605),
    )
    unicast_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 29630),
    )
    multicast_page = core.parse_response(
        "transmitter_flow_status_page",
        _packet(0x2809, 0x2600, 7675),
    )

    assert zero_page["maximum_flow_slots"] == 2
    assert zero_page["reported_flow_count"] == 0
    assert zero_page["flows"] == []
    assert causal_pre_action_page["reported_flow_count"] == 0

    for page, number, kind, address, port, subscriber, subscriber_flow in (
        (unicast_page, 1, "unicast", "192.168.1.108", 14341, "lx-dante", "3"),
        (multicast_page, 2, "multicast", "239.255.255.56", 4321, None, None),
    ):
        assert page["maximum_flow_slots"] == 2
        assert page["reported_flow_count"] == len(page["flows"]) == 1
        flow = page["flows"][0]
        assert flow["global_flow_id"] == number
        assert flow["flow_type"] == kind
        assert flow["sample_rate"] == 48000
        assert flow["encoding"] == 24
        assert flow["channel_slot_count"] == 2
        assert flow["transmitter_channel_ids_by_slot"] == [1, 2]
        assert flow["destination_internet_protocol_version_four_address"] == address
        assert flow["destination_user_datagram_port"] == port
        assert flow["subscriber_device_name"] == subscriber
        assert flow["subscriber_flow_name"] == subscriber_flow


@pytest.mark.asyncio
async def test_product_inventory_uses_the_typed_2809_status_parser(monkeypatch):
    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts, device=None):
        assert core.build_command(command_specification)
        return _packet(0x2809, 0x2600, 29630)

    monkeypatch.setattr(flows, "_request", request)

    inventory = await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2809)

    assert inventory["maximum_flow_slots"] == 2
    assert inventory["reported_flow_count"] == 1
    assert inventory["flows"][0]["subscriber_device_name"] == "lx-dante"


@pytest.mark.asyncio
async def test_detection_accepts_2809_after_earlier_protocol_identifiers_fail(monkeypatch):
    attempted_protocol_identifiers = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts, device=None):
        attempted_protocol_identifiers.append(command_specification["flow_protocol_id"])
        if command_specification["flow_protocol_id"] == 0x2809:
            return _packet(0x2809, 0x2600, 29630)
        return None

    monkeypatch.setattr(flows, "_request", request)

    device = SimpleNamespace(services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.41"}}})
    assert await flows.detect_flow_protocol("192.0.2.10", 4440, device=device) == 0x2809
    assert attempted_protocol_identifiers == [0x2729, 0x2801, 0x2809]


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_fallback", [True, False])
async def test_detection_requires_a_flow_page_not_just_an_accepted_acknowledgement(monkeypatch, valid_fallback):
    attempted = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts, device=None):
        protocol = command_specification["flow_protocol_id"]
        attempted.append(protocol)

        if protocol == 0x2809 and valid_fallback:
            return _packet(0x2809, 0x2600, 29630)

        return core.build_response(
            {
                "protocol_id": protocol,
                "transaction_id": 1,
                "result_code": 1,
                "response": {"kind": "device_name", "name": "not-a-flow-page"},
            }
        )

    monkeypatch.setattr(flows, "_request", request)
    device = SimpleNamespace(services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.41"}}})

    assert await flows.detect_flow_protocol("192.0.2.10", 4440, device=device) == (0x2809 if valid_fallback else None)
    assert attempted == ([0x2729, 0x2801, 0x2809] if valid_fallback else [0x2729, 0x2801, 0x2809, 0x280F])
