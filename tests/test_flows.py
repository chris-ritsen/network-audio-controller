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
                        "global_flow_id": 1,
                        "populated_transmitter_channel_ids": [1],
                        "populated_slot_count": 1,
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


def _run_without_context(run, *arguments, **options):
    import asyncio

    return asyncio.run(run(None, {}, *arguments, **options))


def _flow_list_device():
    from types import SimpleNamespace

    return SimpleNamespace(
        name="avio-usb-1", server_name="AVIOUSB-1.local.", ipv4="192.0.2.10", flow_protocol_id=0x2809
    )


def test_flow_list_formats_sample_rate_and_encoding_with_shared_formatters(monkeypatch):
    from typer.testing import CliRunner

    from netaudio.commands import flow as flow_commands

    device = _flow_list_device()

    async def query_inventory(device_ip, arc_port, flow_protocol_id):
        assert (device_ip, arc_port, flow_protocol_id) == ("192.0.2.10", 4440, 0x2809)
        return {
            "flows": [
                {
                    "global_flow_id": 2,
                    "flow_type": "multicast",
                    "populated_transmitter_channel_ids": [1, 2],
                    "populated_slot_count": 2,
                    "sample_rate": 48000,
                    "encoding": 24,
                    "destination_internet_protocol_version_four_address": "239.255.60.163",
                    "destination_user_datagram_port": 4321,
                }
            ],
            "max_flow_slots": 4,
            "reported_flow_count": 1,
        }

    monkeypatch.setattr(flow_commands, "_selected_device", lambda _devices: (device, 4440))
    monkeypatch.setattr(flow_commands, "run_command", _run_without_context)
    monkeypatch.setattr(flows, "query_preferred_tx_flow_inventory", query_inventory)

    result = CliRunner().invoke(flow_commands.app, ["list"])

    assert result.exit_code == 0, result.output
    header, row = [line for line in result.output.splitlines() if line.strip()][:2]
    assert "Sample Rate" in header and "Encoding" in header
    assert "48 kHz" in row and "PCM24" in row
    assert "48000" not in row


def test_flow_list_uses_the_returned_status_schema_when_mutations_use_2729(monkeypatch):
    from typer.testing import CliRunner

    from netaudio.commands import flow as flow_commands

    device = _flow_list_device()
    device.flow_protocol_id = 0x2729

    async def query_inventory(device_ip, arc_port, flow_protocol_id):
        assert (device_ip, arc_port, flow_protocol_id) == ("192.0.2.10", 4440, 0x2729)
        return {
            "flows": [
                {
                    "global_flow_id": 2,
                    "flow_type": "multicast",
                    "populated_transmitter_channel_ids": [1, 2],
                    "populated_slot_count": 2,
                    "sample_rate": 48_000,
                    "encoding": 24,
                    "destination_internet_protocol_version_four_address": "239.255.60.163",
                    "destination_user_datagram_port": 4321,
                }
            ],
            "max_flow_slots": 4,
            "reported_flow_count": 1,
        }

    monkeypatch.setattr(flow_commands, "_selected_device", lambda _devices: (device, 4440))
    monkeypatch.setattr(flow_commands, "run_command", _run_without_context)
    monkeypatch.setattr(flows, "query_preferred_tx_flow_inventory", query_inventory)

    result = CliRunner().invoke(flow_commands.app, ["list"])

    assert result.exit_code == 0, result.output
    assert " 2 " in result.output
    assert "1, 2" in result.output


def test_receiver_flow_list_formats_latency_in_milliseconds(monkeypatch):
    from typer.testing import CliRunner

    from netaudio.commands import flow as flow_commands

    device = _flow_list_device()

    async def query_inventory(queried_device):
        assert queried_device is device
        return {
            "flows": [
                {
                    "flow_number": 1,
                    "flow_type": "unicast",
                    "receiver_channel_numbers_by_flow_channel": [[1], [2]],
                    "subscription_status_code": 0x0009,
                    "destination_internet_protocol_version_four_address": "192.0.2.10",
                    "destination_user_datagram_port": 14336,
                    "sample_rate": 48000,
                    "encoding": 24,
                    "frames_per_packet": 8,
                    "latency_nanoseconds": 1_000_000,
                }
            ],
            "maximum_flow_slots": 2,
        }

    monkeypatch.setattr(flow_commands, "_selected_device", lambda _devices: (device, 4440))
    monkeypatch.setattr(flow_commands, "run_command", _run_without_context)
    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", query_inventory)

    result = CliRunner().invoke(flow_commands.app, ["receiver-list"])

    assert result.exit_code == 0, result.output
    header, row = [line for line in result.output.splitlines() if line.strip()][:2]
    assert "Latency" in header
    assert "Latency (ns)" not in header
    assert "1 ms" in row and "48 kHz" in row and "PCM24" in row
    assert "1000000" not in row
