from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.heartbeat_connection_health import ReceiverFlowConnectionHealthTracker
from netaudio.dante.services.heartbeat import (
    DanteHeartbeatService,
    parse_connection_health_records,
)

DEVICE_EXTENDED_UNIQUE_IDENTIFIER = "001dc1fffe50368b"
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "heartbeat_connection_health" / "sequence-41132.json"
SIGNAL_PRESENCE_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "signal_presence" / "avio_bluetooth_frame_1367_udp.bin"
)


def heartbeat_packet(
    sequence: int,
    latency_sample_counts: tuple[int, int],
    raw_impairment_values: tuple[int, int],
    start_receiver_flow_index: int = 0,
    device_extended_unique_identifier: str = DEVICE_EXTENDED_UNIQUE_IDENTIFIER,
) -> bytes:
    latency_record = bytearray.fromhex("00208003000400140000000000020000001800000000bb800000000000000000")
    latency_record[8:10] = sequence.to_bytes(2, "big")
    latency_record[14:16] = start_receiver_flow_index.to_bytes(2, "big")
    latency_record[24:28] = latency_sample_counts[0].to_bytes(4, "big")
    latency_record[28:32] = latency_sample_counts[1].to_bytes(4, "big")

    raw_impairment_record = bytearray.fromhex("001c8004000400100000000000020000001400000000000000000000")
    raw_impairment_record[8:10] = sequence.to_bytes(2, "big")
    raw_impairment_record[14:16] = start_receiver_flow_index.to_bytes(2, "big")
    raw_impairment_record[20:24] = raw_impairment_values[0].to_bytes(4, "big")
    raw_impairment_record[24:28] = raw_impairment_values[1].to_bytes(4, "big")

    records = latency_record + raw_impairment_record
    packet_length = 32 + len(records)
    header = bytearray(b"\xff\xfe" + packet_length.to_bytes(2, "big") + bytes(28))
    header[8:16] = bytes.fromhex(device_extended_unique_identifier)
    return bytes(header + records)


BASELINE_PACKET = heartbeat_packet(41130, (14, 0), (0, 0))
FIRST_AFTER_TREATMENT_PACKET = heartbeat_packet(41131, (14, 0), (0, 0))
TREATMENT_PACKET = heartbeat_packet(41132, (1006, 0), (825, 0))
RECOVERED_LATENCY_PACKET = heartbeat_packet(41133, (14, 0), (825, 0))


def device_state():
    return SimpleNamespace(
        server_name="avio-usb-1.local.",
        name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        receiver_flow_connection_health=None,
        update_last_seen=MagicMock(),
    )


def parsed(packet: bytes) -> dict:
    parsed_records = parse_connection_health_records(packet)
    assert parsed_records is not None
    return parsed_records


def tracker_update(
    tracker: ReceiverFlowConnectionHealthTracker,
    packet: bytes,
    observed_monotonic: float,
    observed_at: str = "2026-08-22T11:54:32.145509Z",
) -> dict:
    state = tracker.update(parsed(packet), observed_at, observed_monotonic)
    assert state is not None
    return state


def tracker_history(tracker: ReceiverFlowConnectionHealthTracker) -> dict:
    state = tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    assert state is not None
    return state


def test_parser_uses_hash_pinned_payload_extracted_from_retained_pcap():
    fixture = json.loads(FIXTURE_PATH.read_text())
    payload = bytes.fromhex(fixture["udp_payload_hex"])

    assert fixture["source_capture_sha256"] == "8288ff84a30a57a7b9fce46fec7f95bdef0362dfb5a4075298c6f2888c80c398"
    assert hashlib.sha256(payload).hexdigest() == fixture["udp_payload_sha256"]
    assert fixture["frame_number"] == 5256
    assert fixture["source_udp_port"] == 8700
    assert fixture["destination_udp_port"] == 8708

    treatment = core.parse_response("heartbeat_connection_health", payload)
    assert treatment == {
        "device_extended_unique_identifier": DEVICE_EXTENDED_UNIQUE_IDENTIFIER,
        "latency_records": [
            {
                "record_length": 32,
                "extension_length": 4,
                "payload_length": 20,
                "sequence": 41132,
                "unknown_word_at_offset_10": 0,
                "entry_count": 2,
                "start_receiver_flow_index": 0,
                "vector_offset": 24,
                "unknown_word_at_offset_18": 0,
                "sample_rate_hertz": 48000,
                "entries": [
                    {"receiver_flow_index": 0, "latency_sample_count": 1006},
                    {"receiver_flow_index": 1, "latency_sample_count": 0},
                ],
            }
        ],
        "raw_impairment_records": [
            {
                "record_length": 28,
                "extension_length": 4,
                "payload_length": 16,
                "sequence": 41132,
                "unknown_word_at_offset_10": 0,
                "entry_count": 2,
                "start_receiver_flow_index": 0,
                "vector_offset": 20,
                "unknown_word_at_offset_18": 0,
                "entries": [
                    {"receiver_flow_index": 0, "raw_impairment_value": 825},
                    {"receiver_flow_index": 1, "raw_impairment_value": 0},
                ],
            }
        ],
    }


def test_service_retains_timestamped_history_with_one_based_public_flow_slots():
    monotonic_times = iter([100.0, 101.2, 102.4, 103.6])
    wall_times = iter([1787399672.145509, 1787399673.862406, 1787399675.068372, 1787399676.273409])
    device = device_state()
    on_device_updated = MagicMock()
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        monotonic_clock=lambda: next(monotonic_times),
        wall_clock=lambda: next(wall_times),
    )

    for packet in (
        BASELINE_PACKET,
        FIRST_AFTER_TREATMENT_PACKET,
        TREATMENT_PACKET,
        RECOVERED_LATENCY_PACKET,
    ):
        service._on_packet(packet, ("192.168.1.247", 8700))

    state = device.receiver_flow_connection_health
    assert state["device_extended_unique_identifier"] == DEVICE_EXTENDED_UNIQUE_IDENTIFIER
    assert state["fresh"] is True
    assert state["sequence"] == 41133
    assert state["observed_at"] == "2026-08-22T11:54:36.273409Z"
    assert state["observation_timestamp_source"] == "local_receive_time"
    assert len(state["flows"]) == 1
    flow = state["flows"][0]
    assert flow["receiver_flow_index"] == 0
    assert flow["receiver_flow_slot"] == 1
    assert flow["current_latency_nanoseconds"] == 291667
    assert flow["average_latency_nanoseconds"] == 5458334
    assert flow["peak_latency_nanoseconds"] == 20958333
    assert flow["raw_impairment_value"] == 825
    assert flow["raw_impairment_delta"] == 0
    assert flow["history_sample_count"] == 4
    assert "history" not in flow
    history_state = service.connection_health_history(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    assert history_state is not None
    history = history_state["flows"][0]["history"]
    assert [sample["raw_impairment_delta"] for sample in history] == [None, 0, 825, 0]
    assert [sample["latency_sample_count"] for sample in history] == [14, 14, 1006, 14]
    assert on_device_updated.call_count == 4


def test_nonzero_wire_index_maps_to_one_based_public_flow_slot():
    tracker = ReceiverFlowConnectionHealthTracker(history_limit=2)
    packet = heartbeat_packet(7, (18, 0), (7, 0), start_receiver_flow_index=7)
    state = tracker_update(tracker, packet, 100.0)

    assert state["flows"][0]["receiver_flow_index"] == 7
    assert state["flows"][0]["receiver_flow_slot"] == 8


def test_sequence_gap_preserves_history_but_suppresses_raw_impairment_delta():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(tracker, BASELINE_PACKET, 100.0)
    state = tracker_update(tracker, TREATMENT_PACKET, 102.4)

    flow = state["flows"][0]
    history = tracker_history(tracker)["flows"][0]["history"]
    assert len(history) == 2
    assert flow["raw_impairment_value"] == 825
    assert flow["raw_impairment_delta"] is None


def test_older_reordered_sequence_cannot_replace_fresh_state_and_wrap_is_accepted():
    tracker = ReceiverFlowConnectionHealthTracker()
    sequence_100 = heartbeat_packet(100, (14, 0), (0, 0))
    sequence_99 = heartbeat_packet(99, (1006, 0), (825, 0))
    tracker_update(tracker, sequence_100, 100.0)

    assert tracker.update(parsed(sequence_99), "2026-08-22T11:54:33.145509Z", 101.0) is None
    assert tracker_history(tracker)["sequence"] == 100

    wrapping_tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(wrapping_tracker, heartbeat_packet(65535, (14, 0), (0, 0)), 100.0)
    wrapped = tracker_update(wrapping_tracker, heartbeat_packet(0, (18, 0), (0, 0)), 101.0)
    assert wrapped["sequence"] == 0
    assert wrapped["flows"][0]["history_sample_count"] == 2


def test_stale_resumption_accepts_sequence_reset_without_inheriting_history():
    tracker = ReceiverFlowConnectionHealthTracker(freshness_seconds=5.0)
    tracker_update(tracker, heartbeat_packet(100, (14, 0), (825, 0)), 100.0)
    resumed = tracker_update(tracker, heartbeat_packet(2, (18, 0), (825, 0)), 105.0)

    assert resumed["sequence"] == 2
    assert resumed["flows"][0]["history_sample_count"] == 1
    assert resumed["flows"][0]["raw_impairment_delta"] is None


def test_distinct_heartbeat_extended_unique_identifiers_never_share_history():
    tracker = ReceiverFlowConnectionHealthTracker()
    other_device_extended_unique_identifier = "001dc1fffe50368c"
    tracker_update(tracker, heartbeat_packet(1, (14, 0), (0, 0)), 100.0)
    tracker_update(
        tracker,
        heartbeat_packet(
            7,
            (1006, 0),
            (825, 0),
            device_extended_unique_identifier=other_device_extended_unique_identifier,
        ),
        101.0,
    )

    first_history = tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    second_history = tracker.history_snapshot(other_device_extended_unique_identifier)
    assert first_history is not None
    assert second_history is not None
    assert first_history["sequence"] == 1
    assert second_history["sequence"] == 7
    assert first_history["flows"][0]["history_sample_count"] == 1
    assert second_history["flows"][0]["history_sample_count"] == 1


def test_flow_rebuild_signals_reset_history_and_never_emit_negative_delta():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(tracker, heartbeat_packet(1, (14, 0), (825, 0)), 100.0)
    reset = tracker_update(tracker, heartbeat_packet(2, (14, 0), (0, 0)), 101.0)

    assert reset["flows"][0]["history_sample_count"] == 1
    assert reset["flows"][0]["raw_impairment_delta"] is None

    removed = tracker_update(tracker, heartbeat_packet(3, (0, 0), (0, 0)), 102.0)
    assert removed["flows"] == []
    rebuilt = tracker_update(tracker, heartbeat_packet(4, (14, 0), (0, 0)), 103.0)
    assert rebuilt["flows"][0]["history_sample_count"] == 1


def test_duplicate_and_unpaired_records_do_not_replace_live_state():
    monotonic_times = iter([100.0, 101.0, 102.0])
    wall_times = iter([1787399672.145509, 1787399673.145509, 1787399674.145509])
    device = device_state()
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        monotonic_clock=lambda: next(monotonic_times),
        wall_clock=lambda: next(wall_times),
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    original = device.receiver_flow_connection_health
    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    assert device.receiver_flow_connection_health == original

    unpaired = bytearray(TREATMENT_PACKET)
    unpaired[72:74] = (41131).to_bytes(2, "big")
    service._on_packet(bytes(unpaired), ("192.168.1.247", 8700))
    assert device.receiver_flow_connection_health == original


def test_freshness_boundary_is_exactly_five_seconds():
    tracker = ReceiverFlowConnectionHealthTracker(freshness_seconds=5.0)
    tracker_update(tracker, BASELINE_PACKET, 100.0)

    assert tracker.expire(104.999999) == []
    expired = tracker.expire(105.0)
    assert len(expired) == 1
    assert expired[0][0] == DEVICE_EXTENDED_UNIQUE_IDENTIFIER
    assert expired[0][1]["fresh"] is False
    assert tracker.expire(110.0) == []


@pytest.mark.asyncio
async def test_service_schedules_evented_expiry_and_emits_device_update():
    device = device_state()
    expired = asyncio.Event()

    def on_device_updated(updated_device):
        if updated_device.receiver_flow_connection_health["fresh"] is False:
            expired.set()

    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        connection_health_freshness_seconds=0.01,
    )
    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))

    await asyncio.wait_for(expired.wait(), timeout=0.5)
    assert device.receiver_flow_connection_health["fresh"] is False
    await service.stop()


def test_source_port_does_not_gate_connection_health_or_signal_presence():
    device = device_state()
    on_device_updated = MagicMock()
    on_signal_presence = MagicMock()
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        on_signal_presence=on_signal_presence,
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 49152))

    device.update_last_seen.assert_called_once_with()
    assert device.receiver_flow_connection_health is not None
    on_device_updated.assert_called_once_with(device)

    signal_device = device_state()
    signal_service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: signal_device,
        on_signal_presence=on_signal_presence,
    )
    signal_service._on_packet(SIGNAL_PRESENCE_FIXTURE_PATH.read_bytes(), ("192.168.1.61", 49153))
    on_signal_presence.assert_called_once()


def test_zero_count_zero_rate_one_sided_and_malformed_vectors_are_rejected():
    zero_count = bytearray(TREATMENT_PACKET)
    zero_count[44:46] = (0).to_bytes(2, "big")
    assert parse_connection_health_records(bytes(zero_count)) is None

    zero_rate = bytearray(TREATMENT_PACKET)
    zero_rate[52:56] = (0).to_bytes(4, "big")
    assert parse_connection_health_records(bytes(zero_rate)) is None

    one_sided = TREATMENT_PACKET[:64]
    one_sided = one_sided[:2] + len(one_sided).to_bytes(2, "big") + one_sided[4:]
    assert parse_connection_health_records(one_sided) is None

    malformed = bytearray(TREATMENT_PACKET)
    malformed[48:50] = (20).to_bytes(2, "big")
    assert parse_connection_health_records(bytes(malformed)) is None


def test_connection_health_survives_device_serialization_roundtrip():
    device = DanteDevice(server_name="avio-usb-1.local.")
    device.ipv4 = "192.168.1.247"
    device.receiver_flow_connection_health = {
        "device_extended_unique_identifier": DEVICE_EXTENDED_UNIQUE_IDENTIFIER,
        "fresh": True,
        "sequence": 41132,
        "flows": [
            {
                "receiver_flow_index": 0,
                "receiver_flow_slot": 1,
                "current_latency_nanoseconds": 20958333,
                "average_latency_nanoseconds": 20958333,
                "peak_latency_nanoseconds": 20958333,
                "raw_impairment_value": 825,
                "raw_impairment_delta": 825,
                "history": [],
            }
        ],
    }

    serialized = DanteDeviceSerializer.to_json(device)
    restored = DanteDeviceSerializer.device_from_json(serialized)

    assert restored.receiver_flow_connection_health == device.receiver_flow_connection_health


def test_invalid_tracker_configuration_fails_loudly():
    with pytest.raises(ValueError):
        ReceiverFlowConnectionHealthTracker(freshness_seconds=0)
    with pytest.raises(ValueError):
        ReceiverFlowConnectionHealthTracker(history_limit=0)
