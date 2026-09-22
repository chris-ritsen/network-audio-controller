import ctypes
import struct
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from tests.protocol_test_fixtures import load_protocol_packet


@pytest.mark.parametrize(
    "factory, args",
    [
        (core.ChannelInventory, ("rx", 0x2809)),
        (core.ChannelInventory, ("tx", 0x2809)),
        (core.ReceiverFlowInventory, (0x2809,)),
        (core.TransmitFlowInventory, (0x2809,)),
    ],
)
def test_inventory_page_limit_cannot_wrap_through_c_abi(factory, args):
    wrapping_limit = (1 << (ctypes.sizeof(ctypes.c_size_t) * 8)) + 1

    with pytest.raises(ValueError, match="maximum_pages"):
        with factory(*args, maximum_pages=wrapping_limit):
            pass


def receiver_pages(protocol):
    if protocol == 0x2729:
        first = bytearray(load_protocol_packet("receiver_flow_inventory", "protocol_2729_opcode_3200_id_8172.bin"))
        first[8:10] = (0x8112).to_bytes(2, "big")
        terminal = struct.pack(">5HBB16H", protocol, 44, 1, 0x3200, 1, 16, 0, *([0] * 16))

        return bytes(first), terminal, 6, [1, 2, 3, 5]

    first = (Path(__file__).parent / "fixtures/issue_59/receiver_flow_partial.bin").read_bytes()
    terminal = bytearray(load_protocol_packet("receiver_flow_status", "protocol_2809_opcode_3600_id_4.bin"))
    terminal[16] = 16
    terminal[34:36] = (16).to_bytes(2, "big")

    return first, bytes(terminal), 16, list(range(1, 17))


@pytest.mark.parametrize("protocol", [0x2729, 0x2809])
def test_receiver_inventory_aggregates_packets_and_owns_continuation(protocol):
    first, terminal, next_id, identifiers = receiver_pages(protocol)

    with core.ReceiverFlowInventory(protocol) as inventory:
        inventory.accept(first)
        assert inventory.state()["inventory"] is None
        assert inventory.state()["next_command"]["starting_flow"] == next_id
        inventory.accept(terminal)
        result = inventory.state()["inventory"]
        assert [flow["flow_number"] for flow in result["flows"]] == identifiers
        assert result["reported_flow_count"] == len(identifiers)
        assert result["page_disposition"] == "complete"
        assert len(result["pages"]) == 2
        assert inventory.state()["next_command"] is None


@pytest.mark.parametrize("protocol", [0x2729, 0x2809])
@pytest.mark.parametrize("fault", ["duplicate", "capacity", "revision", "truncated", "rejected"])
def test_receiver_inventory_rejection_preserves_accepted_pages(protocol, fault):
    first, terminal, _, identifiers = receiver_pages(protocol)
    invalid = bytearray(first if fault == "duplicate" else terminal)

    if fault == "capacity":
        invalid[10 if protocol == 0x2729 else 16] = 17
    elif fault == "revision":
        invalid[0:2] = (0x280F).to_bytes(2, "big")
    elif fault == "truncated":
        invalid = invalid[:-1]
    elif fault == "rejected":
        invalid = bytearray(struct.pack(">5H", protocol, 10, 1, 0x3200 if protocol == 0x2729 else 0x3600, 0x30))

    with core.ReceiverFlowInventory(protocol) as inventory:
        inventory.accept(first)
        baseline = inventory.state()

        with pytest.raises(core.NetaudioCoreError):
            inventory.accept(bytes(invalid))

        assert inventory.state() == baseline
        inventory.accept(terminal)
        assert inventory.state()["inventory"]["reported_flow_count"] == len(identifiers)


@pytest.mark.parametrize("protocol", [0x2729, 0x2809])
def test_receiver_inventory_bounds_incomplete_queries(protocol):
    first, terminal, _, _ = receiver_pages(protocol)

    with core.ReceiverFlowInventory(protocol, maximum_pages=1) as inventory:
        inventory.accept(first)

        with pytest.raises(core.NetaudioCoreError, match="page limit"):
            inventory.accept(terminal)


def test_receiver_inventory_rejects_empty_continuation_and_unknown_protocol():
    _, terminal, _, _ = receiver_pages(0x2729)
    empty_continuation = bytearray(terminal)
    empty_continuation[8:10] = (0x8112).to_bytes(2, "big")

    with core.ReceiverFlowInventory(0x2729) as inventory:
        with pytest.raises(core.NetaudioCoreError, match="no progress"):
            inventory.accept(bytes(empty_continuation))

        inventory.accept(terminal)
        assert inventory.state()["inventory"]["flows"] == []

    for protocol in (0x2801, 0x2810):
        with pytest.raises(core.NetaudioCoreError):
            core.ReceiverFlowInventory(protocol)


@pytest.mark.asyncio
async def test_legacy_receiver_query_transports_native_continuation(monkeypatch):
    first, terminal, next_id, identifiers = receiver_pages(0x2729)
    request = AsyncMock(side_effect=[first, terminal])
    monkeypatch.setattr(flows, "_request", request)
    result = await flows.query_receiver_flow_inventory("192.0.2.10", 4440)
    assert [flow["flow_number"] for flow in result["flows"]] == identifiers
    assert [call.args[2]["starting_flow"] for call in request.await_args_list] == [1, next_id]


def synthetic_page(numbers, *, capacity=32, more=False, protocol=0x2729):
    """Synthetic unicast records for pagination invariants, not captured evidence."""
    records = [struct.pack(">HHIIHH", number, 0x0011, 48000, 24, 1, 1) for number in numbers]
    offset = 12 + capacity * 2
    pointers = [offset + index * 16 for index in range(len(records))]
    pointers.extend([0] * (capacity - len(records)))
    body = bytes([capacity, len(records)]) + struct.pack(f">{capacity}H", *pointers) + b"".join(records)

    return struct.pack(">5H", protocol, 10 + len(body), 1, 0x2200, 0x8112 if more else 1) + body


def test_native_flow_inventory_merges_pages_and_rejects_use_after_completion():
    with core.TransmitFlowInventory(0x2729) as inventory:
        inventory.accept(synthetic_page([1, 28], more=True))
        assert inventory.state()["next_command"]["starting_flow"] == 29
        inventory.accept(synthetic_page([29]))
        result = inventory.state()
        assert result["next_command"] is None
        assert [record["flow_number"] for record in result["inventory"]["flows"]] == [1, 28, 29]

        with pytest.raises(core.NetaudioCoreError, match="complete"):
            inventory.accept(synthetic_page([]))

        assert inventory.state() == result

    with pytest.raises(RuntimeError, match="closed"):
        inventory.state()


@pytest.mark.parametrize(
    "invalid",
    [
        synthetic_page([1, 3]),
        synthetic_page([2], capacity=16),
        synthetic_page([], more=True),
        synthetic_page([2], protocol=0x2801),
        b"truncated",
    ],
    ids=["duplicate", "capacity-change", "no-progress", "revision-change", "truncation"],
)
def test_rejected_flow_page_does_not_contaminate_inventory(invalid):
    with core.TransmitFlowInventory(0x2729) as inventory:
        inventory.accept(synthetic_page([1], more=True))
        baseline = inventory.state()

        with pytest.raises(core.NetaudioCoreError):
            inventory.accept(invalid)

        assert inventory.state() == baseline
        inventory.accept(synthetic_page([2]))
        assert [record["flow_number"] for record in inventory.state()["inventory"]["flows"]] == [1, 2]


def test_flow_inventory_enforces_page_budget_and_protocol_selection():
    with pytest.raises(core.NetaudioCoreError):
        core.TransmitFlowInventory(0x2810)

    with core.TransmitFlowInventory(0x2729, maximum_pages=1) as inventory:
        inventory.accept(synthetic_page([1], more=True))

        with pytest.raises(core.NetaudioCoreError, match="page limit"):
            inventory.accept(synthetic_page([2]))


def test_native_flow_inventory_replays_retained_modern_capture():
    response = (Path(__file__).parent / "fixtures/transmit_flow_lifecycle/modern-2809-create-readback.bin").read_bytes()

    with core.TransmitFlowInventory(0x2809) as inventory:
        inventory.accept(response)
        result = inventory.state()["inventory"]
        assert result == core.parse_response("transmitter_flow_status_page", response)
        assert result["reported_flow_count"] == 1
        assert result["flows"][0]["global_flow_id"] == 2


@pytest.mark.asyncio
async def test_python_flow_query_transports_native_inventory_commands(monkeypatch):
    request = AsyncMock(side_effect=[synthetic_page([1, 28], more=True), synthetic_page([29])])
    monkeypatch.setattr(flows, "_request", request)
    result = await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2729)
    assert [record["flow_number"] for record in result["flows"]] == [1, 28, 29]
    assert [call.args[2]["starting_flow"] for call in request.await_args_list] == [1, 29]
