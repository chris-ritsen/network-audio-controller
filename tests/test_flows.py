import pytest

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.const import RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED


@pytest.mark.asyncio
async def test_flow_protocol_detection_accepts_more_pages(monkeypatch):
    command_specifications = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        command_specifications.append(command_specification)
        return b"first-page"

    monkeypatch.setattr(flows, "_request", request)
    monkeypatch.setattr(core, "parse_response", lambda kind, response: RESULT_CODE_SUCCESS_EXTENDED)

    flow_protocol_id = await flows.detect_flow_protocol("192.0.2.10", 4440)

    assert flow_protocol_id == 0x2729
    assert command_specifications == [{"command": "query_tx_flows", "flow_protocol_id": 0x2729, "starting_flow": 1}]


@pytest.mark.asyncio
async def test_flow_query_follows_capture_backed_starting_flow(monkeypatch):
    responses = iter((b"first-page", b"second-page"))
    command_specifications = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        command_specifications.append(command_specification)
        return next(responses)

    result_codes = {
        b"first-page": RESULT_CODE_SUCCESS_EXTENDED,
        b"second-page": RESULT_CODE_SUCCESS,
    }
    flow_pages = {
        b"first-page": {
            "max_flow_slots": 32,
            "flows": [
                {"flow_number": 1, "flow_type": "unicast"},
                {"flow_number": 28, "flow_type": "multicast"},
            ],
        },
        b"second-page": {
            "max_flow_slots": 32,
            "flows": [{"flow_number": 29, "flow_type": "multicast"}],
        },
    }

    def parse_response(kind, response):
        return result_codes[response] if kind == "result_code" else flow_pages[response]

    monkeypatch.setattr(flows, "_request", request)
    monkeypatch.setattr(core, "parse_response", parse_response)

    inventory = await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2729)

    assert inventory["max_flow_slots"] == 32
    assert [flow["flow_number"] for flow in inventory["flows"]] == [1, 28, 29]
    assert [specification["starting_flow"] for specification in command_specifications] == [1, 29]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flow_pages",
    [
        ({"max_flow_slots": 32, "flows": []},),
        ({"max_flow_slots": 32, "flows": [{"flow_number": 32}]},),
        (
            {"max_flow_slots": 32, "flows": [{"flow_number": 1}]},
            {"max_flow_slots": 32, "flows": [{"flow_number": 1}]},
        ),
        (
            {"max_flow_slots": 32, "flows": [{"flow_number": 28}]},
            {"max_flow_slots": 32, "flows": [{"flow_number": 5}]},
        ),
        (
            {"max_flow_slots": 32, "flows": [{"flow_number": 16}]},
            {"max_flow_slots": 16, "flows": [{"flow_number": 17}]},
        ),
    ],
)
async def test_flow_query_rejects_invalid_pagination(monkeypatch, flow_pages):
    responses = iter(bytes([index]) for index in range(len(flow_pages)))
    page_by_response = {bytes([index]): page for index, page in enumerate(flow_pages)}

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts):
        return next(responses)

    def parse_response(kind, response):
        if kind == "result_code":
            return RESULT_CODE_SUCCESS_EXTENDED
        return page_by_response[response]

    monkeypatch.setattr(flows, "_request", request)
    monkeypatch.setattr(core, "parse_response", parse_response)

    assert await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2729) is None


@pytest.mark.asyncio
async def test_flow_list_compatibility_wrapper_returns_only_flows(monkeypatch):
    inventory = {"max_flow_slots": 4, "flows": [{"flow_number": 1}]}

    async def query(device_ip, arc_port, flow_protocol_id):
        return inventory

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)

    assert await flows.query_tx_flows("192.0.2.10", 4440, 0x2729) == inventory["flows"]


@pytest.mark.asyncio
async def test_preferred_inventory_uses_2809_status_page_when_present(monkeypatch):
    queried = []

    async def query(device_ip, arc_port, flow_protocol_id):
        queried.append(flow_protocol_id)
        if flow_protocol_id == 0x2809:
            return {
                "max_flow_slots": 2,
                "reported_flow_count": 1,
                "flows": [
                    {
                        "flow_number": 1,
                        "flow_type": "unicast",
                        "subscriber_device_name": "lx-dante",
                    }
                ],
            }
        raise AssertionError("Brooklyn inventory should not be queried when 0x2809 succeeds")

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)

    inventory = await flows.query_preferred_tx_flow_inventory("192.0.2.10", 4440, 0x2729)
    assert queried == [0x2809]
    assert inventory["flows"][0]["subscriber_device_name"] == "lx-dante"


@pytest.mark.asyncio
async def test_preferred_inventory_falls_back_to_mutation_protocol(monkeypatch):
    queried = []

    async def query(device_ip, arc_port, flow_protocol_id):
        queried.append(flow_protocol_id)
        if flow_protocol_id == 0x2809:
            return None
        return {"max_flow_slots": 16, "flows": [{"flow_number": 32, "flow_type": "multicast"}]}

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)

    inventory = await flows.query_preferred_tx_flow_inventory("192.0.2.10", 4440, 0x2729)
    assert queried == [0x2809, 0x2729]
    assert inventory["flows"][0]["flow_number"] == 32


def test_receiver_flow_status_page_conversion_preserves_raw_unresolved_fields():
    page = {
        "maximum_flow_slots": 4,
        "flows": [
            {
                "flow_number": 2,
                "flow_type_code": 0x0002,
                "local_receiver_channel_count": 2,
                "receiver_mapping_descriptor_hexadecimal": "00010002",
                "status_code_at_record_offset_62": 0x0009,
                "destination_internet_protocol_version_four_address": "192.0.2.20",
                "destination_user_datagram_port": 14336,
                "sample_rate": 48000,
                "encoding": 24,
                "latency_nanoseconds": 1000000,
            }
        ],
    }

    inventory = flows.inventory_from_receiver_flow_status_page(page)

    flow = inventory["flows"][0]
    assert "receiver_channel_numbers_by_flow_channel" not in flow
    assert "subscription_status_code" not in flow
    assert flow["receiver_mapping_descriptor_hexadecimal"] == "00010002"
    assert flow["status_code_at_record_offset_62"] == 0x0009
    assert flow["local_receiver_channel_count"] == 2
    assert flow["flow_type"] == "0x0002"
