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
