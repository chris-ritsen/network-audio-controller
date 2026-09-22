from __future__ import annotations

import pytest

from netaudio import core
from tests.modern_arc_test_support import modern_arc_payloads


@pytest.mark.parametrize("channel_type,path", [("tx", "transmitter_0x2400"), ("rx", "receiver_0x3400")])
def test_native_inventory_replays_captured_exchange_and_closes(channel_type, path):
    responses = modern_arc_payloads("pagination", path, source_port=4840)
    requests = modern_arc_payloads("pagination", path, source_port=49818)
    assert len(requests) == len(responses)

    with core.ChannelInventory(channel_type, 0x280F) as inventory:
        for expected, response in zip(requests, responses):
            state = inventory.state()
            assert state == inventory.state()
            assert state["inventory"] is None
            command = {**state["next_command"], "message_id": int.from_bytes(expected[4:6], "big")}
            assert core.build_command(command) == expected
            inventory.accept(response)

        state = inventory.state()
        assert state["next_command"] is None
        assert state["inventory"]["total_record_count"] == 64
        assert state["inventory"]["page_count"] == len(responses)
        assert [record["channel_number"] for record in state["inventory"]["records"]] == list(range(1, 65))

        with pytest.raises(core.NetaudioCoreError, match="complete"):
            inventory.accept(responses[-1])

        assert inventory.state() == state

    inventory.close()

    with pytest.raises(RuntimeError, match="closed"):
        inventory.state()

    with pytest.raises(RuntimeError, match="closed"):
        inventory.accept(responses[0])


@pytest.mark.parametrize(
    "channel_type,protocol_id,maximum_pages", [("bad", 0x280F, 256), ("tx", 0x2810, 256), ("rx", 0x2809, 0)]
)
def test_inventory_rejects_invalid_configuration(channel_type, protocol_id, maximum_pages):
    with pytest.raises(core.NetaudioCoreError):
        core.ChannelInventory(channel_type, protocol_id, maximum_pages=maximum_pages)


@pytest.mark.parametrize(
    "failure", ["repeat", "truncated", "wrong_protocol", "wrong_direction", "global_conflict", "changed_duplicate"]
)
def test_rejected_page_does_not_change_inventory(failure):
    responses = modern_arc_payloads("pagination", "transmitter_0x2400", source_port=4840)
    invalid = bytearray(responses[1])
    message = "malformed"

    if failure == "repeat":
        invalid = responses[0]
        message = "no progress"
    elif failure == "truncated":
        invalid = invalid[:-1]
    elif failure == "wrong_protocol":
        invalid[:2] = (0x2809).to_bytes(2, "big")
        message = "protocol"
    elif failure == "wrong_direction":
        invalid = modern_arc_payloads("pagination", "receiver_0x3400", source_port=4840)[0]
    elif failure == "global_conflict":
        page = core.parse_response("modern_arc_transmitter_channel_status_page", invalid)
        # Conflict late in the page, after otherwise acceptable new records.
        offset = page["records"][-1]["record_pointer"] + 2
        invalid[offset : offset + 2] = (1).to_bytes(2, "big")
        message = "conflicting global ID"
    elif failure == "changed_duplicate":
        invalid = bytearray(responses[0])
        page = core.parse_response("modern_arc_transmitter_channel_status_page", invalid)
        invalid[page["records"][0]["friendly_channel_name_pointer"]] = ord("Z")
        message = "conflicting duplicate"

    with core.ChannelInventory("tx", 0x280F) as inventory:
        inventory.accept(responses[0])
        state = inventory.state()

        with pytest.raises(core.NetaudioCoreError, match=message):
            inventory.accept(bytes(invalid))

        assert inventory.state() == state

        for response in responses[1:]:
            inventory.accept(response)

        assert inventory.state()["inventory"]["total_record_count"] == 64


def test_inventory_requests_first_missing_media_local_identity():
    response = bytearray(modern_arc_payloads("pagination", "transmitter_0x2400", source_port=4840)[0])
    page = core.parse_response("modern_arc_transmitter_channel_status_page", response)
    offset = page["records"][1]["record_pointer"] + 8
    response[offset : offset + 2] = (17).to_bytes(2, "big")

    with core.ChannelInventory("tx", 0x280F) as inventory:
        inventory.accept(bytes(response))
        command = inventory.state()["next_command"]
        assert command["media_selector"] == 3
        assert command["starting_channel_identifier"] == 2
        assert command["ending_channel_identifier"] == 0


def test_inventory_page_limit_is_enforced_without_advancing():
    responses = modern_arc_payloads("pagination", "transmitter_0x2400", source_port=4840)

    with core.ChannelInventory("tx", 0x280F, maximum_pages=1) as inventory:
        inventory.accept(responses[0])
        state = inventory.state()

        with pytest.raises(core.NetaudioCoreError, match="page limit"):
            inventory.accept(responses[1])

        assert inventory.state() == state
