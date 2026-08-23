from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio.dante.services.heartbeat import (
    DanteHeartbeatService,
    parse_connection_health_records,
    parse_heartbeat_device_extended_unique_identifier,
    parse_interface_traffic_records,
)

FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "heartbeat_cross_family"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIRECTORY / name).read_text())


def frame_payload(fixture: dict, frame_number: int) -> bytes:
    frame = next(frame for frame in fixture["frames"] if frame["frame_number"] == frame_number)
    payload = bytes.fromhex(frame["udp_payload_hex"])
    assert hashlib.sha256(payload).hexdigest() == frame["udp_payload_sha256"]
    return payload


def device_state(name: str):
    return SimpleNamespace(
        server_name=f"{name}.local.",
        name=name,
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        receiver_flow_connection_health=None,
        update_last_seen=MagicMock(),
    )


@pytest.mark.parametrize(
    (
        "fixture_name",
        "interface_frame_number",
        "connection_frame_number",
        "expected_identifier",
        "expected_interface_count",
        "expected_active_receiver_flow_indices",
    ),
    [
        ("lx-dante-1031.json", 11, 12, "001dc10812580000", 2, [0, 3, 4, 6]),
        ("a32-1032.json", 1345, 1332, "001dc119245c0000", 1, [0, 1, 2, 3]),
    ],
)
def test_authentic_non_8700_heartbeat_families_parse_and_update_devices(
    fixture_name,
    interface_frame_number,
    connection_frame_number,
    expected_identifier,
    expected_interface_count,
    expected_active_receiver_flow_indices,
):
    fixture = load_fixture(fixture_name)
    interface_payload = frame_payload(fixture, interface_frame_number)
    connection_payload = frame_payload(fixture, connection_frame_number)
    device = device_state(fixture["device"])
    on_device_updated = MagicMock()
    monotonic_times = iter([100.0, 101.0])
    wall_times = iter([1786471789.768030, 1786471789.768914])
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        monotonic_clock=lambda: next(monotonic_times),
        wall_clock=lambda: next(wall_times),
    )
    source_address = (fixture["source_ipv4"], fixture["source_udp_port"])

    assert parse_heartbeat_device_extended_unique_identifier(interface_payload) == expected_identifier
    assert parse_heartbeat_device_extended_unique_identifier(connection_payload) == expected_identifier
    [interface_record] = parse_interface_traffic_records(interface_payload)
    connection_records = parse_connection_health_records(connection_payload)
    assert interface_record["interface_entry_count"] == expected_interface_count
    assert connection_records is not None
    assert connection_records["device_extended_unique_identifier"] == expected_identifier

    service._on_packet(interface_payload, source_address)
    service._on_packet(connection_payload, source_address)

    assert device.network_interface_traffic["device_extended_unique_identifier"] == expected_identifier
    assert device.network_interface_traffic["interface_entry_count"] == expected_interface_count
    assert device.receiver_flow_connection_health["device_extended_unique_identifier"] == expected_identifier
    assert [flow["receiver_flow_index"] for flow in device.receiver_flow_connection_health["flows"]] == (
        expected_active_receiver_flow_indices
    )
    assert [flow["receiver_flow_slot"] for flow in device.receiver_flow_connection_health["flows"]] == [
        receiver_flow_index + 1 for receiver_flow_index in expected_active_receiver_flow_indices
    ]
    assert device.update_last_seen.call_count == 2
    assert on_device_updated.call_count == 2


@pytest.mark.parametrize(
    ("fixture_name", "expected_capture_hash"),
    [
        ("lx-dante-1031.json", "b5e135bbcbc630b3a32b34eded7cc7688b9334b11972ad34afda201c73fb47ce"),
        ("a32-1032.json", "eee75b3418dede520d6bfb48fa2c772d1f33a1ecdadeef49545b5f944363439b"),
    ],
)
def test_cross_family_fixture_provenance_and_payload_hashes(fixture_name, expected_capture_hash):
    fixture = load_fixture(fixture_name)
    assert fixture["source_capture_sha256"] == expected_capture_hash
    assert fixture["destination_udp_port"] == 8708
    assert fixture["source_udp_port"] != 8700
    for frame in fixture["frames"]:
        payload = bytes.fromhex(frame["udp_payload_hex"])
        assert hashlib.sha256(payload).hexdigest() == frame["udp_payload_sha256"]


def test_interface_traffic_alone_emits_one_device_update():
    fixture = load_fixture("lx-dante-1031.json")
    payload = frame_payload(fixture, 11)
    device = device_state("lx-dante")
    device.clock_frequency_offset_parts_per_billion = 0
    on_device_updated = MagicMock()
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
        monotonic_clock=lambda: 100.0,
    )

    service._on_packet(payload, (fixture["source_ipv4"], fixture["source_udp_port"]))

    on_device_updated.assert_called_once_with(device)
    assert device.network_interface_traffic["sequence"] == 17177


def test_removed_device_identity_discards_connection_and_interface_history():
    fixture = load_fixture("lx-dante-1031.json")
    interface_payload = frame_payload(fixture, 11)
    connection_payload = frame_payload(fixture, 12)
    device = device_state("lx-dante")
    devices = {device.server_name: device}
    monotonic_times = iter([100.0, 101.0])
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        get_devices=lambda: devices,
        monotonic_clock=lambda: next(monotonic_times),
    )
    source_address = (fixture["source_ipv4"], fixture["source_udp_port"])
    service._on_packet(interface_payload, source_address)
    service._on_packet(connection_payload, source_address)
    identifier = "001dc10812580000"
    assert service.connection_health_history(identifier) is not None

    devices.clear()
    service._check_stale_devices()

    assert service.connection_health_history(identifier) is None
    assert device.receiver_flow_connection_health is None
    assert device.network_interface_traffic is None
    assert service._heartbeat_devices == {}
    assert service._previous_interface_traffic_samples == {}


def test_replaced_device_object_starts_new_identity_owned_history():
    fixture = load_fixture("lx-dante-1031.json")
    connection_payload = frame_payload(fixture, 12)
    first_device = device_state("lx-dante-old")
    replacement_device = device_state("lx-dante-new")
    current_device = first_device
    monotonic_times = iter([100.0, 101.0])
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: current_device,
        monotonic_clock=lambda: next(monotonic_times),
    )
    source_address = (fixture["source_ipv4"], fixture["source_udp_port"])
    service._on_packet(connection_payload, source_address)
    assert first_device.receiver_flow_connection_health["flows"][0]["history_sample_count"] == 1

    current_device = replacement_device
    service._on_packet(connection_payload, source_address)

    assert first_device.receiver_flow_connection_health is None
    assert replacement_device.receiver_flow_connection_health["flows"][0]["history_sample_count"] == 1
