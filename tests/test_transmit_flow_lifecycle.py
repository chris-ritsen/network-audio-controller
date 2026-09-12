from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from netaudio import core
from netaudio.dante import flow_lifecycle
from netaudio.dante.transmit_flow import (
    FlowIdentity,
    FlowLifecycleState,
    FlowProtocolRequirements,
    FlowSocket,
    FlowType,
    MediaMode,
    RedundancyConstraint,
    TransmitFlowSpecification,
    TransmitterChannelSlot,
    compare_transmit_flows,
)


def specification(**changes) -> TransmitFlowSpecification:
    values = {
        "media_mode": MediaMode.NATIVE_DANTE,
        "flow_type": FlowType.MULTICAST,
        "channel_slots": (
            TransmitterChannelSlot(1, 1),
            TransmitterChannelSlot(2, 2),
        ),
        "identity": FlowIdentity(global_flow_id=2),
        "protocol": FlowProtocolRequirements(protocol_id=0x2729),
    }
    values.update(changes)
    return TransmitFlowSpecification(**values)


FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "transmit_flow_lifecycle"


def fixture(name: str) -> bytes:
    return (FIXTURE_DIRECTORY / name).read_bytes()


def test_promoted_flow_fixtures_are_digest_bound_and_builders_match_requests():
    provenance = json.loads((FIXTURE_DIRECTORY / "provenance.json").read_text(encoding="utf-8"))
    records = {name: record for cohort in provenance["cohorts"].values() for name, record in cohort["files"].items()}
    for name, record in records.items():
        payload = fixture(name)
        assert len(payload) == record["size_bytes"]
        assert hashlib.sha256(payload).hexdigest() == record["sha256"]

    assert core.build_command(
        {
            "command": "create_tx_flow",
            "flow_protocol_id": 0x2729,
            "flow_slot": 2,
            "channels": [2],
            "transaction_id": 0x2201,
        }
    ) == fixture("legacy-2729-create-request.bin")
    assert core.build_command(
        {
            "command": "delete_tx_flow",
            "flow_protocol_id": 0x2729,
            "flow_slot": 2,
            "transaction_id": 0x2202,
        }
    ) == fixture("legacy-2729-delete-request.bin")
    assert core.build_command(
        {
            "command": "create_multicast_flow_2809",
            "channels": [1, 2],
            "request_options_word": 0,
            "transaction_id": 0x3CF2,
        }
    ) == fixture("modern-2809-create-request.bin")
    assert core.build_command(
        {
            "command": "delete_tx_flow",
            "flow_protocol_id": 0x2809,
            "flow_slot": 2,
            "transaction_id": 0x1602,
        }
    ) == fixture("modern-2809-delete-request.bin")


def test_promoted_modern_acknowledgement_and_readback_reject_truncation():
    acknowledgement = fixture("modern-2809-create-acknowledgement.bin")
    assert core.parse_response("multicast_flow_creation_2809", acknowledgement)["channels"] == [1, 2]
    for length in range(len(acknowledgement)):
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("multicast_flow_creation_2809", acknowledgement[:length])

    readback = fixture("modern-2809-create-readback.bin")
    assert core.parse_response("transmitter_flow_status_page", readback)["reported_flow_count"] == 1
    for length in range(len(readback)):
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("transmitter_flow_status_page", readback[:length])


def test_native_builders_reject_channel_overflow_and_invalid_slot_mappings():
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(
            {
                "command": "create_multicast_flow_2809",
                "channels": list(range(1, 216)),
                "request_options_word": 0,
            }
        )
    for channels in ([], [0], [1, 1]):
        with pytest.raises(core.NetaudioCoreError):
            core.build_command(
                {
                    "command": "create_tx_flow",
                    "flow_protocol_id": 0x2729,
                    "flow_slot": 2,
                    "channels": channels,
                }
            )


def device(protocol_id=0x2729, *, managed=False, locked=False):
    return SimpleNamespace(
        flow_protocol_id=protocol_id,
        requires_managed_control=managed,
        is_locked=locked,
        tx_channels={1: object(), 2: object()},
        sample_rate=48_000,
        encoding=24,
        ipv4="192.0.2.10",
        topology_mutation_lock=__import__("asyncio").Lock(),
        _arc_port=lambda: 4440,
    )


def test_canonical_specification_preserves_unknown_fields_and_raw_readback():
    source = {
        "schema_version": 1,
        "media_mode": "native_dante",
        "flow_type": "multicast",
        "name": "Program",
        "channel_slots": [
            {"slot": 1, "transmitter_channel": 7, "vendor_slot_value": 99},
            {"slot": 3, "transmitter_channel": 9},
        ],
        "sample_rate_hz": 48_000,
        "encoding_bits": 24,
        "frames_per_packet": 48,
        "primary_destination": {
            "address": "239.69.1.2",
            "port": 5004,
            "interface": "primary",
            "socket_extension": "kept",
        },
        "secondary_destination": {
            "address": "239.69.1.3",
            "port": 5004,
            "interface": "secondary",
        },
        "redundancy": "required",
        "identity": {"global_flow_id": 4, "media_type_code": 3, "media_local_flow_id": 2},
        "protocol": {
            "protocol_id": 0x2809,
            "protocol_version": "2.8.9",
            "cohort": "modern_2809",
            "required_capabilities": ["aes67_configuration_supported"],
            "protocol_extension": [1, 2],
        },
        "raw_fields": {"raw_record_hexadecimal": "0011", "unknown_word": 17},
        "future_top_level": {"preserved": True},
    }

    parsed = TransmitFlowSpecification.from_dict(source)

    assert parsed.to_dict() == source
    assert parsed.channel_slots[0].extra_fields == {"vendor_slot_value": 99}


@pytest.mark.parametrize(
    "change, message",
    [
        ({"channel_slots": (TransmitterChannelSlot(2, 1), TransmitterChannelSlot(1, 2))}, "strictly ascending"),
        ({"channel_slots": (TransmitterChannelSlot(1, 1), TransmitterChannelSlot(2, 1))}, "duplicates"),
        (
            {
                "primary_destination": FlowSocket("239.1.1.1", 5004, "primary"),
                "secondary_destination": FlowSocket("239.1.1.2", 5004, "primary"),
                "redundancy": RedundancyConstraint.REQUIRED,
            },
            "different interfaces",
        ),
    ],
)
def test_canonical_specification_rejects_invalid_mapping_or_destination(change, message):
    with pytest.raises(ValueError, match=message):
        specification(**change)


def test_inventory_conversion_retains_the_complete_raw_record():
    record = {
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 2,
        "flow_name": "Program",
        "flow_type": "multicast",
        "transmitter_channel_ids_by_slot": [1, 0, 2],
        "sample_rate": 48_000,
        "encoding": 24,
        "frames_per_packet": 48,
        "destination_internet_protocol_version_four_address": "239.1.2.3",
        "destination_user_datagram_port": 5004,
        "unmapped_extension": "retained",
    }

    parsed = TransmitFlowSpecification.from_inventory_record(record, protocol_id=0x2809)

    assert [(entry.slot, entry.transmitter_channel) for entry in parsed.channel_slots] == [(1, 1), (3, 2)]
    assert parsed.raw_fields == record
    assert parsed.to_dict()["raw_fields"]["unmapped_extension"] == "retained"


def test_comparison_only_requires_optional_authoring_fields_when_requested():
    requested = specification(sample_rate_hz=None, encoding_bits=None)
    effective = TransmitFlowSpecification.from_inventory_record(
        {
            "flow_number": 2,
            "flow_type": "multicast",
            "channels": [1, 2],
            "sample_rate": 48_000,
            "encoding": 24,
        },
        protocol_id=0x2729,
    )
    assert compare_transmit_flows(requested, effective).matches
    changed = specification(sample_rate_hz=96_000)
    comparison = compare_transmit_flows(changed, effective)
    assert not comparison.matches
    assert comparison.differences[0].field == "sample_rate_hz"


def test_planner_separates_supported_direct_and_unsupported_managed_or_rtp_paths():
    direct = flow_lifecycle.plan_create_transmit_flow(device(), specification())
    assert direct.supported and direct.serializer_cohort == "legacy_2729_explicit_slot_multicast"

    managed = flow_lifecycle.plan_create_transmit_flow(device(managed=True), specification())
    assert not managed.supported and managed.transport == "ddm"
    assert "managed transmit-flow writes" in "; ".join(managed.reasons)

    rtp = specification(media_mode=MediaMode.RTP_AES67)
    unsupported = flow_lifecycle.plan_create_transmit_flow(device(), rtp)
    assert not unsupported.supported
    assert "RTP/AES67" in "; ".join(unsupported.reasons)


def test_planner_rejects_unsupported_fields_and_unproven_cohorts():
    named = flow_lifecycle.plan_create_transmit_flow(device(), specification(name="Program"))
    assert not named.supported and "flow name" in "; ".join(named.reasons)

    unproven = specification(protocol=FlowProtocolRequirements(protocol_id=0x2801))
    plan = flow_lifecycle.plan_create_transmit_flow(device(protocol_id=0x2801), unproven)
    assert not plan.supported and "digest-bound" in "; ".join(plan.reasons)


@pytest.mark.asyncio
async def test_legacy_create_preserves_acknowledgement_and_verifies_fresh_readback(monkeypatch):
    before = {"max_flow_slots": 4, "flows": []}
    after = {
        "max_flow_slots": 4,
        "flows": [
            {
                "flow_number": 2,
                "flow_type": "multicast",
                "channels": [1, 2],
                "sample_rate": 48_000,
                "encoding": 24,
            }
        ],
    }
    inventories = iter((before, after))
    sent = []

    async def read_inventory(_device, protocol_id):
        assert protocol_id == 0x2729
        return deepcopy(next(inventories))

    async def send_once(_device, command):
        sent.append(command)
        return bytes.fromhex("2729000a000122010001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert result.request_acknowledgement["result_code"] == 1
    assert result.device_confirmation is None
    assert result.persistence_confirmation is None
    assert result.effective_state_confirmation is True
    assert result.comparison.matches
    assert sent == [
        {
            "command": "create_tx_flow",
            "flow_protocol_id": 0x2729,
            "flow_slot": 2,
            "channels": [1, 2],
        }
    ]


@pytest.mark.asyncio
async def test_create_reports_acknowledged_without_readback_as_partial(monkeypatch):
    inventories = iter(({"max_flow_slots": 4, "flows": []}, None))

    async def read_inventory(_device, _protocol_id):
        return next(inventories)

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122010001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)
    monkeypatch.setattr(flow_lifecycle, "VERIFICATION_TIMEOUT_SECONDS", 0)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.PARTIAL
    assert result.request_acknowledgement["accepted"] is True
    assert result.device_confirmation is None
    assert result.effective_state_confirmation is None
    assert result.persistence_confirmation is None
    assert result.verification_observations[-1]["outcome"] == "unavailable"
    assert result.decoded_audio_confirmed is False


@pytest.mark.asyncio
async def test_rejected_acknowledgement_does_not_invent_confirmation(monkeypatch):
    reads = []

    async def read_inventory(_device, _protocol_id):
        reads.append(True)
        return {"max_flow_slots": 4, "flows": []}

    async def send_once(_device, _command):
        return b"rejected"

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)
    monkeypatch.setattr(
        flow_lifecycle,
        "_acknowledgement",
        lambda _response: {"received": True, "parseable": True, "result_code": 7, "accepted": False},
    )

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.REJECTED
    assert result.device_confirmation is None
    assert result.effective_state_confirmation is None
    assert result.persistence_confirmation is None
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_delete_requires_absent_readback_and_unchanged_unrelated_flows(monkeypatch):
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    other = {
        "flow_number": 1,
        "flow_type": "unicast",
        "channels": [1],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": [deepcopy(other), target]},
            {"max_flow_slots": 4, "flows": [deepcopy(other)]},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return next(inventories)

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122020001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.delete_transmit_flow(device(), 2)

    assert result.state is FlowLifecycleState.DELETED
    assert result.effective is None
    assert result.requested.identity.global_flow_id == 2
    assert result.effective_state_confirmation is True
    assert result.device_confirmation is None


@pytest.mark.asyncio
async def test_delete_acknowledged_timeout_is_partial_and_does_not_retry_write(monkeypatch):
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventory = {"max_flow_slots": 4, "flows": [target]}
    sent = []

    async def read_inventory(_device, _protocol_id):
        return deepcopy(inventory)

    async def send_once(_device, command):
        sent.append(command)
        return bytes.fromhex("2729000a000122020001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)
    monkeypatch.setattr(flow_lifecycle, "VERIFICATION_TIMEOUT_SECONDS", 0)

    result = await flow_lifecycle.delete_transmit_flow(device(), 2)

    assert result.state is FlowLifecycleState.PARTIAL
    assert result.device_confirmation is None
    assert result.effective_state_confirmation is None
    assert result.effective.identity.global_flow_id == 2
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_create_polls_until_change_is_visible_and_sends_only_once(monkeypatch):
    before = {"max_flow_slots": 4, "flows": []}
    after = {
        "max_flow_slots": 4,
        "flows": [
            {
                "flow_number": 2,
                "flow_type": "multicast",
                "channels": [1, 2],
                "sample_rate": 48_000,
                "encoding": 24,
            }
        ],
    }
    inventories = iter((before, before, after))
    sent = []

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, command):
        sent.append(command)
        return bytes.fromhex("2729000a000122010001")

    async def no_delay(_deadline):
        return True

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)
    monkeypatch.setattr(flow_lifecycle, "_wait_for_next_poll", no_delay)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert [item["outcome"] for item in result.verification_observations] == [
        "available",
        "not_yet_visible",
        "confirmed",
    ]
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_create_lost_acknowledgement_can_be_confirmed_by_fresh_readback(monkeypatch):
    before = {"max_flow_slots": 4, "flows": []}
    after = {
        "max_flow_slots": 4,
        "flows": [
            {
                "flow_number": 2,
                "flow_type": "multicast",
                "channels": [1, 2],
                "sample_rate": 48_000,
                "encoding": 24,
            }
        ],
    }
    inventories = iter((before, after))

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return None

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert result.request_acknowledgement is None
    assert result.device_confirmation is None
    assert result.effective_state_confirmation is True


@pytest.mark.asyncio
async def test_create_definitive_contradiction_is_inconsistent(monkeypatch):
    before = {"max_flow_slots": 4, "flows": []}
    contradictory = {
        "max_flow_slots": 4,
        "flows": [
            {
                "flow_number": 2,
                "flow_type": "multicast",
                "channels": [2, 1],
                "sample_rate": 48_000,
                "encoding": 24,
            }
        ],
    }
    inventories = iter((before, contradictory))

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122010001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.INCONSISTENT
    assert result.device_confirmation is None
    assert result.effective_state_confirmation is False
    assert result.comparison is not None and not result.comparison.matches


@pytest.mark.asyncio
async def test_unrelated_volatile_fields_do_not_create_false_inconsistency(monkeypatch):
    other_before = {
        "flow_number": 1,
        "flow_type": "unicast",
        "channels": [1],
        "sample_rate": 48_000,
        "encoding": 24,
        "diagnostic_counter": 1,
    }
    other_after = {**other_before, "diagnostic_counter": 2, "freshness": "new"}
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": [other_before]},
            {"max_flow_slots": 4, "flows": [other_after, target]},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122010001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert result.verification_observations[-1]["inventory"]["flows"][0]["diagnostic_counter"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["created", "deleted", "durable"])
async def test_actual_unrelated_flow_change_is_recorded_without_blocking_confirmation(monkeypatch, change):
    other = {
        "flow_number": 1,
        "flow_type": "unicast",
        "channels": [1],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    extra = {
        "flow_number": 3,
        "flow_type": "unicast",
        "channels": [2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    before_flows = [other]
    if change == "deleted":
        before_flows.append(extra)
        after_flows = [target, other]
    elif change == "created":
        after_flows = [target, other, extra]
    else:
        after_flows = [target, {**other, "encoding": 16}]
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": before_flows},
            {"max_flow_slots": 4, "flows": after_flows},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122010001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert result.effective_state_confirmation is True
    assert "concurrent_topology_activity" in result.verification_observations[-1]["details"]


@pytest.mark.asyncio
async def test_create_continues_polling_after_concurrent_topology_activity(monkeypatch):
    other = {
        "flow_number": 1,
        "flow_type": "unicast",
        "channels": [1],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    changed_other = {**other, "encoding": 16}
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": [other]},
            {"max_flow_slots": 4, "flows": [changed_other]},
            {"max_flow_slots": 4, "flows": [changed_other, target]},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122010001")

    async def no_delay(_deadline):
        return True

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)
    monkeypatch.setattr(flow_lifecycle, "_wait_for_next_poll", no_delay)

    result = await flow_lifecycle.create_transmit_flow(device(), specification())

    assert result.state is FlowLifecycleState.CONFIRMED
    assert [item["outcome"] for item in result.verification_observations] == [
        "available",
        "not_yet_visible",
        "confirmed",
    ]
    assert all("concurrent_topology_activity" in item["details"] for item in result.verification_observations[1:])


@pytest.mark.asyncio
async def test_delete_confirms_absence_despite_concurrent_topology_activity(monkeypatch):
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    other = {
        "flow_number": 1,
        "flow_type": "unicast",
        "channels": [1],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": [other, target]},
            {"max_flow_slots": 4, "flows": [{**other, "encoding": 16}]},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122020001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.delete_transmit_flow(device(), 2)

    assert result.state is FlowLifecycleState.DELETED
    assert result.effective_state_confirmation is True
    assert "concurrent_topology_activity" in result.verification_observations[-1]["details"]


@pytest.mark.asyncio
async def test_delete_correlated_target_change_is_inconsistent(monkeypatch):
    target = {
        "flow_number": 2,
        "flow_type": "multicast",
        "channels": [1, 2],
        "sample_rate": 48_000,
        "encoding": 24,
    }
    inventories = iter(
        (
            {"max_flow_slots": 4, "flows": [target]},
            {"max_flow_slots": 4, "flows": [{**target, "encoding": 16}]},
        )
    )

    async def read_inventory(_device, _protocol_id):
        return deepcopy(next(inventories))

    async def send_once(_device, _command):
        return bytes.fromhex("2729000a000122020001")

    monkeypatch.setattr(flow_lifecycle, "_read_inventory", read_inventory)
    monkeypatch.setattr(flow_lifecycle, "_send_once", send_once)

    result = await flow_lifecycle.delete_transmit_flow(device(), 2)

    assert result.state is FlowLifecycleState.INCONSISTENT
    assert result.effective_state_confirmation is False
    assert result.comparison is not None and not result.comparison.matches


def test_partial_inventory_cannot_supply_effective_flow_or_preset_comparison_state():
    from tests.issue_59_fixtures import packet

    partial = core.parse_response("modern_arc_receiver_flow_status_page", packet("receiver_flow_partial.bin"))
    with pytest.raises(flow_lifecycle.flows.FlowValidationError, match="complete flow inventory is unavailable"):
        flow_lifecycle.canonical_inventory(partial, 0x2809)
