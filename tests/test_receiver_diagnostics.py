"""Synthetic monitoring inputs through the native parser and observation tracker."""

from copy import deepcopy
import json

import pytest

from netaudio import core
from netaudio.dante.heartbeat_connection_health import ReceiverFlowConnectionHealthTracker
from tests.test_heartbeat_connection_health import DEVICE_EXTENDED_UNIQUE_IDENTIFIER as DEVICE, heartbeat_packet, parsed


TOPOLOGY = {
    "capacity": {"base_receive_flow_capacity": 2, "network_interface_count": 2, "resource_extension_offset": 0},
    "complete": True,
    "flows": [
        {
            "media_type_code": 3,
            "media_local_flow_id": 1,
            "global_flow_id": 9,
            "latency_nanoseconds": 1_000_000,
            "source": "sender-a",
        },
        {
            "media_type_code": 3,
            "media_local_flow_id": 2,
            "global_flow_id": 4,
            "latency_nanoseconds": 2_000_000,
            "source": "sender-b",
        },
    ],
}


def test_receiver_history_retention_does_not_inflate_native_update_requests(monkeypatch, tmp_path):
    from netaudio.core import binding

    original = binding._encode_command_spec
    request_sizes = []

    def encode(request):
        result = original(request)
        if isinstance(request, dict) and "records" in request and "history_limit" in request:
            request_sizes.append(len(result))
        return result

    monkeypatch.setattr(binding, "_encode_command_spec", encode)
    tracker = ReceiverFlowConnectionHealthTracker(history_limit=300)
    for sequence in range(305):
        result = update(tracker, sequence, (0, (24, 48, 72, 96)), now=100 + sequence / 100)

    history = tracker.history_snapshot(DEVICE)
    assert len(history["paths"]) == 4
    for path in history["paths"]:
        assert len(path["latency"]["history"]) == 300
        assert path["latency"]["history"][0]["sequence"] == 5
        assert path["latency"]["history"][-1]["sequence"] == 304
    assert all(not path["latency"].get("history") for path in result["paths"])
    assert max(request_sizes) < 5000
    assert update(tracker, 304, (0, (24, 48, 72, 96)), now=103.1) is None
    tracker.expire(110)
    update(tracker, 1, (0, (25, 49, 73, 97)), now=111)
    resumed = tracker.history_snapshot(DEVICE)
    assert resumed["paths"][0]["latency"]["history"][-1]["epoch"] == 1
    assert len(resumed["paths"][0]["latency"]["history"]) == 300
    (tmp_path / "receiver-history-cost.json").write_text(
        json.dumps(
            {
                "maximum_request_bytes": max(request_sizes),
                "paths": 4,
                "samples_per_path": 300,
            }
        )
    )


@pytest.mark.parametrize("length", [16, 26, 32, 40, 46])
def test_capacity_fields_are_independently_nullable(length):
    packet = bytearray(length)
    packet[:10] = bytes.fromhex("28090000000110000001")
    packet[2:4] = length.to_bytes(2, "big")
    for offset, value in ((22, 4), (24, 2), (30, 2), (36, 7), (38, 9), (44, 0)):
        if offset + 2 <= length:
            packet[offset : offset + 2] = value.to_bytes(2, "big")
    capacity = core.parse_response("channel_count", bytes(packet))["receiver_telemetry_capacity"]
    assert capacity["base_receive_flow_capacity"] == (2 if length >= 26 else None)
    assert capacity["network_interface_count"] == (2 if length >= 32 else None)
    assert capacity["resource_extension_offset"] == (0 if length >= 46 else None)
    assert capacity["raw_response"] == list(packet)


def update(tracker, sequence, indices=(0, (24,)), now=100, topology=TOPOLOGY, late=False):
    start, values = indices
    kwargs = (
        {"late_packet_sequence": sequence, "late_packet_counts": values, "late_packet_start_index": start}
        if late
        else {
            "latency_sequence": sequence,
            "latency_sample_counts": values,
            "latency_start_index": start,
        }
    )
    return tracker.update(parsed(heartbeat_packet(**kwargs)), str(now), now, topology=topology)


def test_paths_pair_network_and_media_local_identity_and_merge_partial_updates():
    tracker = ReceiverFlowConnectionHealthTracker()
    update(tracker, 1, (0, (24, 48)))
    result = update(tracker, 1, (2, (72, 96)), now=101)
    assert [
        (p["network_interface_index"], p["audio_receiver_flow_id"], p["global_flow_id"]) for p in result["paths"]
    ] == [(0, 1, 9), (0, 2, 4), (1, 1, 9), (1, 2, 4)]
    assert result["complete"] is False
    assert update(tracker, 1, (2, (72, 96)), now=102) is None
    tracker.expire(105)
    snapshot = tracker.history_snapshot(DEVICE)
    assert snapshot["paths"][0]["latency"]["fresh"] is False
    assert snapshot["paths"][2]["latency"]["fresh"] is True


def test_epochs_keep_history_without_reinterpreting_old_topology():
    tracker = ReceiverFlowConnectionHealthTracker()
    update(tracker, 65535)
    update(tracker, 0, now=101)
    assert update(tracker, 65534, now=102) is None
    result = update(tracker, 0, (0, (25,)), now=103)
    assert result["diagnostics"][-1]["kind"] == "conflicting_duplicate"
    tracker.expire(107)
    update(tracker, 1, now=108)
    changed = deepcopy(TOPOLOGY)
    changed["flows"][0]["source"] = "different-sender"
    update(tracker, 2, now=109, topology=changed)
    samples = tracker.history_snapshot(DEVICE)["paths"][0]["latency"]["history"]
    assert len(samples) == 4
    assert [s["epoch"] for s in samples] == [0, 0, 1, 2]
    assert samples[0]["evidence"]["source"] == "sender-a"
    assert samples[-1]["evidence"]["source"] == "different-sender"


@pytest.mark.parametrize(
    "capacity",
    [None, {}, {"base_receive_flow_capacity": 2, "network_interface_count": 2, "resource_extension_offset": 12}],
)
def test_ambiguous_topology_keeps_raw_index_without_flow_identity(capacity):
    tracker = ReceiverFlowConnectionHealthTracker()
    result = update(tracker, 1, topology={"capacity": capacity, "flows": [], "complete": False})
    path = result["paths"][0]
    assert path["telemetry_index"] == 0
    assert path["network_interface_index"] is None
    assert path["global_flow_id"] is None
    assert path["attribution_status"] == "unresolved"


def test_histogram_uses_exact_samples_excludes_zero_and_preserves_counter_discontinuities():
    tracker = ReceiverFlowConnectionHealthTracker()
    for sequence, value in enumerate((0, 1, 47, 48)):
        update(tracker, sequence, (0, (value,)), now=100 + sequence)
    path = tracker.history_snapshot(DEVICE)["paths"][0]
    histogram = path["latency"]["histogram"]
    assert histogram["counts"][0] == 1
    assert histogram["counts"][19] == 1
    assert histogram["overflow"] == 1
    assert sum(histogram["counts"]) + histogram["overflow"] == 3
    update(tracker, 10, (0, (100,)), now=104, late=True)
    update(tracker, 12, (0, (105,)), now=105, late=True)
    path = tracker.history_snapshot(DEVICE)["paths"][0]
    assert path["late_packets"]["delta"] == 5
    assert path["late_packets"]["current"]["sequence_gap"] == 1
    update(tracker, 13, (0, (2,)), now=106, late=True)
    path = tracker.history_snapshot(DEVICE)["paths"][0]
    assert path["late_packets"]["delta"] is None
    assert len(path["late_packets"]["history"]) == 3


def test_declared_geometry_accepts_relocation_and_retains_valid_siblings():
    original = heartbeat_packet(latency_sequence=1, late_packet_sequence=1)
    latency = bytearray(original[32:64])
    latency[0:2] = (40).to_bytes(2, "big")
    latency[6:8] = (28).to_bytes(2, "big")
    latency[16:18] = (28).to_bytes(2, "big")
    latency[24:24] = b"pad!"
    latency.extend(b"tail")
    packet = bytearray(original[:32]) + latency + original[64:]
    packet[2:4] = len(packet).to_bytes(2, "big")
    result = core.parse_connection_health(bytes(packet))
    assert result["latency_records"][0]["entries"][0]["latency_sample_count"] == 14
    assert result["latency_records"][0]["raw_record"] == list(latency)
    packet[32 + 16 : 32 + 18] = (44).to_bytes(2, "big")
    result = core.parse_connection_health(bytes(packet))
    assert result["latency_records"] == []
    assert len(result["late_packet_records"]) == 1
    assert result["diagnostics"][0]["kind"] == "malformed_record"


def test_clock_equal_advancing_and_older_observations_through_heartbeat_handler():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from netaudio.dante.services.heartbeat import DanteHeartbeatService
    from tests.test_heartbeat_clock_frequency_offset import TREATMENT_PACKET

    device = SimpleNamespace(
        server_name="clock", online=True, clock_frequency_offset_parts_per_billion=None, update_last_seen=MagicMock()
    )
    changed = MagicMock()
    service = DanteHeartbeatService(device_by_ip=lambda _: device, on_device_updated=changed)
    service._on_packet(TREATMENT_PACKET, ("192.0.2.1", 1030))
    advancing = bytearray(TREATMENT_PACKET)
    advancing[-8:-6] = (6685).to_bytes(2, "big")
    service._on_packet(bytes(advancing), ("192.0.2.1", 1030))
    assert changed.call_count == 2
    older = bytearray(TREATMENT_PACKET)
    older[-4:] = (12345).to_bytes(4, "big")
    service._on_packet(bytes(older), ("192.0.2.1", 1030))
    assert device.clock_frequency_offset_parts_per_billion == -222222


@pytest.mark.asyncio
async def test_diagnostics_export_and_local_reset_send_no_commands_or_journal_recovery():
    import json
    from unittest.mock import AsyncMock, MagicMock
    from tests.http_api_test_support import make_http_server, FakeWriter, make_device
    from netaudio.dante.services.heartbeat import DanteHeartbeatService

    device = make_device()
    device.execute = AsyncMock()
    device.update_last_seen = MagicMock()
    device.receiver_telemetry_capacity = TOPOLOGY["capacity"]
    device.receiver_flow_completeness = "complete"
    device.receiver_flows = TOPOLOGY["flows"]
    service = DanteHeartbeatService(device_by_ip=lambda _: device, monotonic_clock=lambda: 100.0)
    service._on_packet(heartbeat_packet(latency_sequence=1), ("192.0.2.1", 1030))
    server = make_http_server({device.server_name: device})
    server.diagnostics = service
    server.publish_device_updated = AsyncMock()
    writer = FakeWriter()
    await server._route("GET", "/diagnostics/" + device.server_name, b"", writer, None)
    code, exported = writer.response()
    assert code == 200
    assert len(exported["receiver"]["paths"][0]["latency"]["history"]) == 1
    writer = FakeWriter()
    await server._route("POST", "/diagnostics/reset", json.dumps({"device": device.server_name}).encode(), writer, None)
    code, reset = writer.response()
    assert code == 200
    assert len(reset["receiver"]["paths"][0]["latency"]["history"]) == 1
    assert reset["receiver"]["paths"][0]["latency"]["statistics"]["count"] == 0
    device.execute.assert_not_awaited()
    server.publish_device_updated.assert_not_awaited()


@pytest.mark.asyncio
async def test_diagnostics_can_omit_receiver_history():
    from unittest.mock import AsyncMock, MagicMock
    from tests.http_api_test_support import make_http_server, FakeWriter, make_device
    from netaudio.dante.services.heartbeat import DanteHeartbeatService

    device = make_device()
    device.execute = AsyncMock()
    device.update_last_seen = MagicMock()
    device.receiver_telemetry_capacity = TOPOLOGY["capacity"]
    device.receiver_flow_completeness = "complete"
    device.receiver_flows = TOPOLOGY["flows"]
    service = DanteHeartbeatService(device_by_ip=lambda _: device, monotonic_clock=lambda: 100.0)
    service._on_packet(heartbeat_packet(latency_sequence=1), ("192.0.2.1", 1030))
    server = make_http_server({device.server_name: device})
    server.diagnostics = service
    writer = FakeWriter()
    await server._route("GET", "/diagnostics/" + device.server_name + "?receiver_history=0", b"", writer, None)
    code, summary = writer.response()
    assert code == 200
    latency = summary["receiver"]["paths"][0]["latency"]
    assert "history" not in latency
    assert latency["histogram"]["counts"]
    assert latency["statistics"]["count"] == 1
    assert "history" not in summary["receiver"]["paths"][0]["late_packets"]


def test_latency_telemetry_requests_missing_flow_inventory():
    from unittest.mock import MagicMock
    from tests.http_api_test_support import make_device
    from netaudio.dante.services.heartbeat import DanteHeartbeatService

    device = make_device()
    device.update_last_seen = MagicMock()
    device.receiver_telemetry_capacity = TOPOLOGY["capacity"]
    device.receiver_flow_completeness = "unknown"
    device.receiver_flows = []
    requested = []
    service = DanteHeartbeatService(
        device_by_ip=lambda _: device, monotonic_clock=lambda: 100.0, on_receiver_flows_needed=requested.append
    )
    service._on_packet(heartbeat_packet(latency_sequence=1), ("192.0.2.1", 1030))
    assert requested == [device]
    device.receiver_flow_completeness = "complete"
    device.receiver_flows = TOPOLOGY["flows"]
    service._on_packet(heartbeat_packet(latency_sequence=2), ("192.0.2.1", 1030))
    assert requested == [device]
    path = service.diagnostics_snapshot(device)["receiver"]["paths"][0]
    assert path["attribution_status"] == "resolved"
    assert path["evidence"]["source"] == "sender-a"


def test_browser_renders_native_path_and_histogram_values(tmp_path):
    import json
    import subprocess

    tracker = ReceiverFlowConnectionHealthTracker()
    update(tracker, 1, (2, (24,)), now=100)
    payload = {"device": "receiver", "receiver": tracker.history_snapshot(DEVICE), "clock": None}
    script = """
      import {readFileSync} from 'node:fs';
      import {h} from 'preact';
      import {render} from 'preact-render-to-string';
      import {DiagnosticsView} from './packages/netaudio/src/netaudio/daemon/http/webapp/device/diagnostics.js';
      console.log(render(h(DiagnosticsView, {data: JSON.parse(readFileSync(0, 'utf8'))})));
    """
    result = subprocess.run(
        [
            "node",
            "--import",
            "./tests/webapp/loader.mjs",
            "--import",
            "./tests/webapp/setup.mjs",
            "--input-type=module",
            "-e",
            script,
        ],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=True,
    )
    assert "Audio flow 1" in result.stdout and "Network 2" in result.stdout
    assert "500" in result.stdout and "Reported maximum" in result.stdout
    assert "1 observations" in result.stdout
    (tmp_path / "receiver-diagnostics.html").write_text(result.stdout)


def test_path_pressure_issue_keeps_network_scope_and_does_not_recover_when_missing():
    from netaudio.monitoring.issues import IssueEngine

    engine = IssueEngine()
    tracker = ReceiverFlowConnectionHealthTracker()
    health = update(tracker, 1, (2, (48,)))
    snapshot = {
        "device_identity": "receiver",
        "server_name": "receiver",
        "name": "Receiver",
        "online": True,
        "receiver_flow_completeness": "complete",
        "receiver_flow_connection_health": health,
    }
    events = engine.observe_snapshot(snapshot, timestamp="2026-09-26T12:00:00Z")
    issue = next(event.current for event in events if event.current.kind.value == "receiver_latency_pressure")
    assert "network:1" in issue.scope.flow_identity
    snapshot["receiver_flow_connection_health"] = {"paths": []}
    events = engine.observe_snapshot(snapshot, timestamp="2026-09-26T12:00:01Z")
    assert not any(event.kind.value == "resolved" for event in events)


def test_clock_series_bins_and_opt_in_variation_recovery_require_fresh_windows():
    state = None

    def observe(raw=None, now=100, enabled=None):
        nonlocal state
        result = core.clock_observation_update(
            {
                "previous": state,
                "packet": None,
                "conmon_status": None if raw is None else {"clock_frequency_offset_parts_per_billion": raw},
                "observed_at": str(now),
                "observed_monotonic": now,
                "freshness_seconds": 5,
                "history_limit": 300,
                "warning_enabled": enabled,
            }
        )
        if result is not None:
            state = result
        return state

    for index in range(11):
        observe(-200001 if index % 2 else 200000, 100 + index)
    assert state["conmon"]["histogram"]["underflow"] == 5
    assert state["conmon"]["histogram"]["overflow"] == 6
    assert state["variation"]["conmon"]["deviation_ppb"] > 10000
    assert state["variation"]["conmon"]["active"] is False
    observe(0, 111, enabled=True)
    assert state["variation"]["conmon"]["active"] is True
    observe(now=120)
    assert state["variation"]["conmon"]["observable"] is False
    assert state["variation"]["conmon"]["active"] is True
    for index in range(16):
        observe(0, 121 + index)
    assert state["variation"]["conmon"]["recovered"] is True
    assert state["heartbeat"]["current"] is None


@pytest.mark.parametrize(
    "offset,value", [(4, 0), (4, 65535), (6, 65535), (12, 65535), (14, 65535), (16, 12), (16, 65535)]
)
def test_malformed_latency_geometry_keeps_other_bounded_records(offset, value):
    packet = bytearray(heartbeat_packet(latency_sequence=1, late_packet_sequence=1))
    packet[32 + offset : 34 + offset] = value.to_bytes(2, "big")
    result = core.parse_connection_health(bytes(packet))
    assert result["latency_records"] == []
    assert len(result["late_packet_records"]) == 1
    assert result["diagnostics"]


def test_zero_rate_stays_raw_and_bounded_history_does_not_invent_a_latency():
    tracker = ReceiverFlowConnectionHealthTracker(history_limit=2)
    for sequence in range(3):
        packet = bytearray(heartbeat_packet(latency_sequence=sequence, latency_sample_counts=(24,)))
        packet[52:56] = bytes(4)
        tracker.update(parsed(bytes(packet)), str(sequence), 100 + sequence, topology=TOPOLOGY)
    state = tracker.history_snapshot(DEVICE)
    series = state["paths"][0]["latency"]
    assert len(series["history"]) == state["retention_limit"] == 2
    assert series["current"]["raw"] == 24
    assert series["current"]["value"] is None
    assert sum(series["histogram"]["counts"]) == 0


def test_queued_clock_publications_accumulate_but_cached_reapplication_does_not():
    from netaudio.dante.device import DanteDevice
    from netaudio.dante.services.notification_packet_handlers import _parse_ptp_clock_status
    from netaudio.dante.state import apply_device_status
    from tests.test_clock_port_records import CLOCK_STATUS_PACKET

    device = DanteDevice("receiver")
    first = _parse_ptp_clock_status(CLOCK_STATUS_PACKET, "192.0.2.1", device)
    second = _parse_ptp_clock_status(CLOCK_STATUS_PACKET, "192.0.2.1", device)
    apply_device_status(device, first.kind, first.status)
    apply_device_status(device, second.kind, second.status)
    assert device.clock_observations["conmon"]["statistics"]["count"] == 2
    apply_device_status(device, second.kind, second.status)
    assert device.clock_observations["conmon"]["statistics"]["count"] == 2
