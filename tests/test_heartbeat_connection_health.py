from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
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


def test_invalid_second_stream_cannot_partially_commit_first_stream():
    tracker = ReceiverFlowConnectionHealthTracker()
    tracker_update(tracker, BASELINE_PACKET, 100.0)
    baseline = deepcopy(tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER))
    update = parsed(TREATMENT_PACKET)
    update["late_packet_records"][0]["entries"][0]["late_packet_count"] = -1

    assert tracker.update(update, "later", 101.0) is None
    assert tracker.history_snapshot(DEVICE_EXTENDED_UNIQUE_IDENTIFIER) == baseline


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
        "telemetry_index": 0,
        "latency_sample_count": 1006,
    }
    assert treatment["late_packet_records"][0]["sequence"] == 41132
    assert treatment["late_packet_records"][0]["entries"][0] == {
        "telemetry_index": 0,
        "late_packet_count": 825,
    }


def test_parser_accepts_either_record_family_without_the_other():
    latency = parsed(heartbeat_packet(latency_sequence=9, latency_sample_counts=(18,)))
    assert len(latency["latency_records"]) == 1
    assert latency["late_packet_records"] == []

    late = parsed(heartbeat_packet(late_packet_sequence=700, late_packet_counts=(12,)))
    assert late["latency_records"] == []
    assert len(late["late_packet_records"]) == 1


@pytest.mark.asyncio
async def test_service_reschedules_expiry_for_the_second_stream():
    device = device_state()
    fully_expired = asyncio.Event()

    def on_device_updated(updated_device):
        state = updated_device.receiver_flow_connection_health
        if state is not None and not state["fresh"]:
            fully_expired.set()

    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        connection_health_freshness_seconds=0.3,
    )
    service._on_packet(heartbeat_packet(latency_sequence=1), ("192.168.1.247", 8700))
    await asyncio.sleep(0.1)
    service._on_packet(heartbeat_packet(late_packet_sequence=20, late_packet_counts=(1,)), ("192.168.1.247", 8700))

    await asyncio.wait_for(fully_expired.wait(), timeout=2)
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
