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
from netaudio.dante.services.heartbeat import DanteHeartbeatService, parse_connection_health_records

DEVICE_EXTENDED_UNIQUE_IDENTIFIER = "001dc1fffe50368b"
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "heartbeat_connection_health" / "sequence-41132.json"
SIGNAL_PRESENCE_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "signal_presence" / "avio_bluetooth_frame_1367_udp.bin"
)


def heartbeat_packet(
    *,
    latency_sequence: int | None = None,
    latency_sample_counts: tuple[int, ...] = (14, 0),
    latency_start_index: int = 0,
    late_packet_sequence: int | None = None,
    late_packet_counts: tuple[int, ...] = (0, 0),
    late_packet_start_index: int = 0,
    device_extended_unique_identifier: str = DEVICE_EXTENDED_UNIQUE_IDENTIFIER,
) -> bytes:
    records = bytearray()
    if latency_sequence is not None:
        length = 24 + 4 * len(latency_sample_counts)
        latency_record = bytearray(length)
        latency_record[0:2] = length.to_bytes(2, "big")
        latency_record[2:4] = (0x8003).to_bytes(2, "big")
        latency_record[4:6] = (4).to_bytes(2, "big")
        latency_record[6:8] = (length - 12).to_bytes(2, "big")
        latency_record[8:10] = latency_sequence.to_bytes(2, "big")
        latency_record[12:14] = len(latency_sample_counts).to_bytes(2, "big")
        latency_record[14:16] = latency_start_index.to_bytes(2, "big")
        latency_record[16:18] = (24).to_bytes(2, "big")
        latency_record[20:24] = (48_000).to_bytes(4, "big")
        for index, sample_count in enumerate(latency_sample_counts):
            latency_record[24 + index * 4 : 28 + index * 4] = sample_count.to_bytes(4, "big")
        records.extend(latency_record)
    if late_packet_sequence is not None:
        length = 20 + 4 * len(late_packet_counts)
        late_packet_record = bytearray(length)
        late_packet_record[0:2] = length.to_bytes(2, "big")
        late_packet_record[2:4] = (0x8004).to_bytes(2, "big")
        late_packet_record[4:6] = (4).to_bytes(2, "big")
        late_packet_record[6:8] = (length - 12).to_bytes(2, "big")
        late_packet_record[8:10] = late_packet_sequence.to_bytes(2, "big")
        late_packet_record[12:14] = len(late_packet_counts).to_bytes(2, "big")
        late_packet_record[14:16] = late_packet_start_index.to_bytes(2, "big")
        late_packet_record[16:18] = (20).to_bytes(2, "big")
        for index, count in enumerate(late_packet_counts):
            late_packet_record[20 + index * 4 : 24 + index * 4] = count.to_bytes(4, "big")
        records.extend(late_packet_record)
    packet_length = 32 + len(records)
    header = bytearray(b"\xff\xfe" + packet_length.to_bytes(2, "big") + bytes(28))
    header[8:16] = bytes.fromhex(device_extended_unique_identifier)
    return bytes(header + records)


BASELINE_PACKET = heartbeat_packet(latency_sequence=41130, late_packet_sequence=41130)
TREATMENT_PACKET = heartbeat_packet(
    latency_sequence=41132,
    latency_sample_counts=(1006, 0),
    late_packet_sequence=41132,
    late_packet_counts=(825, 0),
)


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
    result = parse_connection_health_records(packet)
    assert result is not None
    return result


def tracker_update(
    tracker: ReceiverFlowConnectionHealthTracker,
    packet: bytes,
    observed_monotonic: float,
    observed_at: str = "2026-08-22T11:54:32.145509Z",
) -> dict:
    state = tracker.update(parsed(packet), observed_at, observed_monotonic)
    assert state is not None
    return state


def test_parser_uses_digest_bound_permitted_capture_and_late_packet_names():
    fixture = json.loads(FIXTURE_PATH.read_text())
    payload = bytes.fromhex(fixture["udp_payload_hex"])

    assert fixture["source_capture_sha256"] == "8288ff84a30a57a7b9fce46fec7f95bdef0362dfb5a4075298c6f2888c80c398"
    assert hashlib.sha256(payload).hexdigest() == fixture["udp_payload_sha256"]
    treatment = core.parse_response("heartbeat_connection_health", payload)
    assert treatment["device_extended_unique_identifier"] == DEVICE_EXTENDED_UNIQUE_IDENTIFIER
    assert treatment["latency_records"][0]["sequence"] == 41132
    assert treatment["latency_records"][0]["sample_rate_hertz"] == 48_000
    assert treatment["latency_records"][0]["entries"][0] == {
        "receiver_flow_index": 0,
        "latency_sample_count": 1006,
    }
    assert treatment["late_packet_records"][0]["sequence"] == 41132
    assert treatment["late_packet_records"][0]["entries"][0] == {
        "receiver_flow_index": 0,
        "late_packet_count": 825,
    }


def test_parser_accepts_either_record_family_without_the_other():
    latency = parsed(heartbeat_packet(latency_sequence=9, latency_sample_counts=(18,)))
    assert len(latency["latency_records"]) == 1
    assert latency["late_packet_records"] == []

    late = parsed(heartbeat_packet(late_packet_sequence=700, late_packet_counts=(12,)))
    assert late["latency_records"] == []
    assert len(late["late_packet_records"]) == 1


def test_tracker_keeps_sequences_observation_times_and_histories_independent():
    tracker = ReceiverFlowConnectionHealthTracker()
    latency_state = tracker_update(
        tracker,
        heartbeat_packet(latency_sequence=10, latency_sample_counts=(14,)),
        100.0,
        "2026-08-22T11:54:32.000000Z",
    )
    assert latency_state["late_packet_stream"] is None

    combined = tracker_update(
        tracker,
        heartbeat_packet(late_packet_sequence=500, late_packet_counts=(7,)),
        102.0,
        "2026-08-22T11:54:34.000000Z",
    )
    assert combined["latency_stream"] == {
        "sequence": 10,
        "observed_at": "2026-08-22T11:54:32.000000Z",
        "fresh": True,
    }
    assert combined["late_packet_stream"] == {
        "sequence": 500,
        "observed_at": "2026-08-22T11:54:34.000000Z",
        "fresh": True,
    }
    assert combined["flows"][0]["current_latency_nanoseconds"] == 291_667
    assert combined["flows"][0]["late_packet_count"] == 7

    updated = tracker_update(
        tracker,
        heartbeat_packet(latency_sequence=11, latency_sample_counts=(18,)),
        103.0,
        "2026-08-22T11:54:35.000000Z",
    )
    assert updated["latency_stream"]["sequence"] == 11
    assert updated["late_packet_stream"]["sequence"] == 500
    history = tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    assert history is not None
    assert len(history["flows"][0]["latency_history"]) == 2
    assert len(history["flows"][0]["late_packet_history"]) == 1


def test_tracker_merges_different_flow_ranges_by_receiver_flow_index_only():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(
        tracker,
        heartbeat_packet(latency_sequence=1, latency_sample_counts=(18,), latency_start_index=0),
        100.0,
    )
    state = tracker_update(
        tracker,
        heartbeat_packet(late_packet_sequence=99, late_packet_counts=(7,), late_packet_start_index=7),
        101.0,
    )
    assert [flow["receiver_flow_index"] for flow in state["flows"]] == [0, 7]
    assert state["flows"][0]["receiver_flow_slot"] == 1
    assert state["flows"][1]["receiver_flow_slot"] == 8
    assert "late_packet_count" not in state["flows"][0]
    assert "current_latency_nanoseconds" not in state["flows"][1]


def test_late_packet_delta_depends_only_on_its_own_consecutive_sequence():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(tracker, heartbeat_packet(late_packet_sequence=20, late_packet_counts=(10,)), 100.0)
    consecutive = tracker_update(tracker, heartbeat_packet(late_packet_sequence=21, late_packet_counts=(13,)), 101.0)
    assert consecutive["flows"][0]["late_packet_delta"] == 3

    tracker_update(tracker, heartbeat_packet(latency_sequence=900, latency_sample_counts=(18,)), 102.0)
    gap = tracker_update(tracker, heartbeat_packet(late_packet_sequence=23, late_packet_counts=(20,)), 103.0)
    assert gap["flows"][0]["late_packet_delta"] is None


def test_each_stream_expires_from_its_own_observation_time():
    tracker = ReceiverFlowConnectionHealthTracker(freshness_seconds=5.0)
    tracker_update(tracker, heartbeat_packet(latency_sequence=1, latency_sample_counts=(14,)), 100.0)
    tracker_update(tracker, heartbeat_packet(late_packet_sequence=7, late_packet_counts=(1,)), 102.0)

    [(device_id, partly_stale)] = tracker.expire(105.0)
    assert device_id == DEVICE_EXTENDED_UNIQUE_IDENTIFIER
    assert partly_stale["fresh"] is True
    assert partly_stale["latency_stream"]["fresh"] is False
    assert partly_stale["late_packet_stream"]["fresh"] is True
    assert tracker.seconds_until_expiry(DEVICE_EXTENDED_UNIQUE_IDENTIFIER, 105.0) == pytest.approx(2.0)

    [(device_id, fully_stale)] = tracker.expire(107.0)
    assert device_id == DEVICE_EXTENDED_UNIQUE_IDENTIFIER
    assert fully_stale["fresh"] is False
    assert fully_stale["late_packet_stream"]["fresh"] is False


def test_duplicate_or_older_sequence_in_one_stream_does_not_block_the_other():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(
        tracker,
        heartbeat_packet(latency_sequence=100, latency_sample_counts=(14,), late_packet_sequence=10),
        100.0,
    )
    state = tracker_update(
        tracker,
        heartbeat_packet(
            latency_sequence=99,
            latency_sample_counts=(1006,),
            late_packet_sequence=11,
            late_packet_counts=(4,),
        ),
        101.0,
    )
    assert state["latency_stream"]["sequence"] == 100
    assert state["late_packet_stream"]["sequence"] == 11
    assert state["flows"][0]["current_latency_nanoseconds"] == 291_667


def test_stale_stream_accepts_sequence_reset_without_inheriting_history():
    tracker = ReceiverFlowConnectionHealthTracker(freshness_seconds=5.0)
    tracker_update(tracker, heartbeat_packet(late_packet_sequence=100, late_packet_counts=(825,)), 100.0)
    resumed = tracker_update(tracker, heartbeat_packet(late_packet_sequence=2, late_packet_counts=(825,)), 105.0)
    assert resumed["late_packet_stream"]["sequence"] == 2
    assert resumed["flows"][0]["late_packet_history_sample_count"] == 1
    assert resumed["flows"][0]["late_packet_delta"] is None


def test_counter_reset_discards_late_packet_history_and_never_emits_negative_delta():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(tracker, heartbeat_packet(late_packet_sequence=1, late_packet_counts=(825,)), 100.0)
    reset = tracker_update(tracker, heartbeat_packet(late_packet_sequence=2, late_packet_counts=(0,)), 101.0)
    assert reset["flows"] == [
        {
            "receiver_flow_index": 0,
            "receiver_flow_slot": 1,
            "late_packet_count": 0,
            "late_packet_delta": None,
            "late_packet_history_sample_count": 1,
        }
    ]
    history = tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    assert history is not None
    assert history["late_packet_stream"]["sequence"] == 2


def test_distinct_device_identifiers_never_share_stream_state():
    tracker = ReceiverFlowConnectionHealthTracker()
    other = "001dc1fffe50368c"
    tracker_update(tracker, heartbeat_packet(latency_sequence=1), 100.0)
    tracker_update(
        tracker,
        heartbeat_packet(late_packet_sequence=7, late_packet_counts=(3,), device_extended_unique_identifier=other),
        101.0,
    )
    first = tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER)
    second = tracker.history_snapshot(other)
    assert first is not None and second is not None
    assert first["latency_stream"]["sequence"] == 1
    assert first["late_packet_stream"] is None
    assert second["latency_stream"] is None
    assert second["late_packet_stream"]["sequence"] == 7


@pytest.mark.asyncio
async def test_service_reschedules_expiry_for_the_second_stream():
    device = device_state()
    fully_expired = asyncio.Event()

    def on_device_updated(updated_device):
        state = updated_device.receiver_flow_connection_health
        if (
            state is not None
            and state["latency_stream"] is not None
            and state["late_packet_stream"] is not None
            and state["latency_stream"]["fresh"] is False
            and state["late_packet_stream"]["fresh"] is False
        ):
            fully_expired.set()

    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        connection_health_freshness_seconds=0.03,
    )
    service._on_packet(heartbeat_packet(latency_sequence=1), ("192.168.1.247", 8700))
    await asyncio.sleep(0.015)
    service._on_packet(heartbeat_packet(late_packet_sequence=20, late_packet_counts=(1,)), ("192.168.1.247", 8700))

    await asyncio.wait_for(fully_expired.wait(), timeout=0.5)
    assert device.receiver_flow_connection_health["latency_stream"]["fresh"] is False
    assert device.receiver_flow_connection_health["late_packet_stream"]["fresh"] is False
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

    signal_service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device_state(),
        on_signal_presence=on_signal_presence,
    )
    signal_service._on_packet(SIGNAL_PRESENCE_FIXTURE_PATH.read_bytes(), ("192.168.1.61", 49153))
    on_signal_presence.assert_called_once()


def test_connection_health_survives_device_serialization_roundtrip():
    device = DanteDevice(server_name="avio-usb-1.local.")
    device.ipv4 = "192.168.1.247"
    tracker = ReceiverFlowConnectionHealthTracker()
    device.receiver_flow_connection_health = tracker_update(tracker, TREATMENT_PACKET, 100.0)

    serialized = DanteDeviceSerializer.to_json(device)
    restored = DanteDeviceSerializer.device_from_json(serialized)
    assert restored.receiver_flow_connection_health == device.receiver_flow_connection_health


def test_invalid_tracker_configuration_fails_loudly():
    with pytest.raises(ValueError):
        ReceiverFlowConnectionHealthTracker(freshness_seconds=0)
    with pytest.raises(ValueError):
        ReceiverFlowConnectionHealthTracker(history_limit=0)
