import asyncio
import json
import struct
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from netaudio_lib.dante.const import DEVICE_ARC_PORT, SERVICE_ARC
from netaudio_lib.dante.device_commands import DanteDeviceCommands
from netaudio_lib.dante.protocol_verifier import ProtocolVerifier


@pytest.fixture
def temp_db(tmp_path):
    return str(tmp_path / "test_verifier.sqlite")


@pytest.fixture
def output_dir(tmp_path):
    return str(tmp_path / "provenance_output")


def _fake_response(opcode=0x1001, status=0x0001):
    packet = struct.pack(">HBB", 0x27FF, 0x00, 10)
    packet += struct.pack(">HH", 1, opcode)
    packet += struct.pack(">H", status)
    return packet


@pytest.mark.asyncio
async def test_session_lifecycle(temp_db, output_dir):
    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="lifecycle_test",
                output_dir=output_dir,
            ) as verifier:
                assert verifier.session_id is not None
                assert verifier.packet_store is not None
                assert verifier.service is not None

                session = verifier.packet_store.get_session(verifier.session_id)
                assert session is not None
                assert session["name"] == "lifecycle_test"
                assert session["ended_ns"] is None

                session_id = verifier.session_id
                packet_store_ref = verifier.packet_store

    from netaudio_lib.dante.packet_store import PacketStore

    store = PacketStore(db_path=temp_db)
    session = store.get_session(session_id)
    assert session["ended_ns"] is not None
    store.close()


@pytest.mark.asyncio
async def test_markers_stored(temp_db, output_dir):
    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="marker_test",
                output_dir=output_dir,
            ) as verifier:
                verifier.hypothesis("test_hyp", note="Testing hypothesis")
                verifier.observation("test_obs", note="Observed result", data={"value": 42})
                verifier.marker("custom_marker", marker_type="step", note="A step")

                markers = verifier.packet_store.get_markers(verifier.session_id)
                non_system = [m for m in markers if m["marker_type"] != "system"]

                assert len(non_system) == 3

                hyp = next(m for m in non_system if m["label"] == "test_hyp")
                assert hyp["marker_type"] == "hypothesis"
                assert hyp["note"] == "Testing hypothesis"

                obs = next(m for m in non_system if m["label"] == "test_obs")
                assert obs["marker_type"] == "observation"
                assert obs["data"] == {"value": 42}

                step = next(m for m in non_system if m["label"] == "custom_marker")
                assert step["marker_type"] == "step"


@pytest.mark.asyncio
async def test_bundle_export(temp_db, output_dir):
    fake_response = _fake_response()

    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="export_test",
                output_dir=output_dir,
            ) as verifier:
                verifier.packet_store.store_packet(
                    payload=fake_response,
                    source_type="test_request",
                    device_ip="192.168.1.100",
                    direction="request",
                    session_id=verifier.session_id,
                )

                verifier.hypothesis("test_export", note="Testing export")

    output_path = Path(output_dir)
    assert output_path.exists()

    manifest_path = output_path / "manifest.json"
    assert manifest_path.exists()

    with open(manifest_path) as manifest_file:
        manifest = json.load(manifest_file)

    assert manifest["session_name"] == "export_test"
    assert manifest["scope"]["device_ip"] == "192.168.1.100"
    assert len(manifest["samples"]) >= 1
    assert len(manifest["markers"]) >= 1

    bin_files = list(output_path.glob("*.bin"))
    assert len(bin_files) >= 1

    marker_types = {m["marker_type"] for m in manifest["markers"]}
    assert "system" in marker_types
    assert "hypothesis" in marker_types


@pytest.mark.asyncio
async def test_send_command_unwraps_tuple(temp_db, output_dir):
    commands = DanteDeviceCommands()
    command_tuple = commands.command_device_name()

    assert isinstance(command_tuple, tuple)
    assert len(command_tuple) == 2
    assert isinstance(command_tuple[0], bytes)
    assert command_tuple[1] == SERVICE_ARC

    fake_response = _fake_response(opcode=0x1002)

    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="command_test",
                output_dir=output_dir,
            ) as verifier:
                with patch.object(verifier._service, "request", new_callable=AsyncMock, return_value=fake_response):
                    response = await verifier.send_command(
                        command_tuple,
                        label="get_name",
                    )

                    assert response == fake_response
                    verifier._service.request.assert_called_once()
                    call_kwargs = verifier._service.request.call_args
                    assert (
                        call_kwargs.kwargs.get("port") == DEVICE_ARC_PORT
                        or call_kwargs[1].get("port") == DEVICE_ARC_PORT
                    )


@pytest.mark.asyncio
async def test_send_with_labels_creates_markers(temp_db, output_dir):
    fake_response = _fake_response()

    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="label_test",
                output_dir=output_dir,
            ) as verifier:
                with patch.object(verifier._service, "request", new_callable=AsyncMock, return_value=fake_response):
                    await verifier.send(
                        b"\x27\xff\x00\x0a\x00\x01\x10\x01\x00\x00",
                        port=4440,
                        label="test_send",
                    )

                markers = verifier.packet_store.get_markers(verifier.session_id)
                step_markers = [m for m in markers if m["marker_type"] == "step"]
                labels = [m["label"] for m in step_markers]
                assert "send_test_send" in labels
                assert "recv_test_send" in labels


@pytest.mark.asyncio
async def test_include_evidence_from_ambient(temp_db, output_dir):
    ambient_packet = struct.pack(">HH", 0x2729, 40)
    ambient_packet += struct.pack(">HH", 0x0001, 0x1101)
    ambient_packet += struct.pack(">H", 0x0001)
    ambient_packet += b"\x00" * 30

    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="evidence_test",
                output_dir=output_dir,
            ) as verifier:
                verifier.packet_store.store_packet(
                    payload=ambient_packet,
                    source_type="tshark",
                    device_ip="192.168.1.100",
                    direction="request",
                    session_id=None,
                )

                verifier.packet_store.store_packet(
                    payload=_fake_response(),
                    source_type="tshark",
                    device_ip="192.168.1.200",
                    direction="request",
                    session_id=None,
                )

                found = verifier.include_evidence(
                    label="set_latency_observed",
                    note="Captured set-latency from Dante Controller",
                    device_ip="192.168.1.100",
                    opcode=0x1101,
                )

                assert len(found) == 1
                assert found[0]["device_ip"] == "192.168.1.100"

    output_path = Path(output_dir)
    manifest_path = output_path / "manifest.json"
    assert manifest_path.exists()

    with open(manifest_path) as manifest_file:
        manifest = json.load(manifest_file)

    assert manifest["evidence_packet_count"] == 1
    evidence_samples = [s for s in manifest["samples"] if s.get("evidence")]
    assert len(evidence_samples) == 1
    assert evidence_samples[0]["file"].startswith("evidence_")

    evidence_markers = [m for m in manifest["markers"] if m["marker_type"] == "evidence"]
    assert len(evidence_markers) == 1
    assert evidence_markers[0]["label"] == "evidence_set_latency_observed"

    evidence_bins = list(output_path.glob("evidence_*.bin"))
    assert len(evidence_bins) == 1


@pytest.mark.asyncio
async def test_include_evidence_by_payload_hex(temp_db, output_dir):
    latency_250us = b"\x00\x03\xd0\x90"
    packet_with_latency = struct.pack(">HH", 0x2729, 40)
    packet_with_latency += struct.pack(">HH", 0x0001, 0x1101)
    packet_with_latency += struct.pack(">H", 0x0001)
    packet_with_latency += b"\x00" * 24 + latency_250us + latency_250us

    packet_without = struct.pack(">HH", 0x2729, 40)
    packet_without += struct.pack(">HH", 0x0002, 0x1101)
    packet_without += struct.pack(">H", 0x0001)
    packet_without += b"\x00" * 30

    with patch("netaudio_lib.dante.protocol_verifier.load_capture_profile", return_value=({}, Path("/fake"))):
        with patch("netaudio_lib.dante.protocol_verifier.resolve_db_from_config", return_value=temp_db):
            async with ProtocolVerifier(
                device_ip="192.168.1.100",
                device_name="test-device",
                session_name="hex_filter_test",
                output_dir=output_dir,
            ) as verifier:
                verifier.packet_store.store_packet(
                    payload=packet_with_latency,
                    source_type="tshark",
                    device_ip="192.168.1.100",
                    direction="request",
                )

                verifier.packet_store.store_packet(
                    payload=packet_without,
                    source_type="tshark",
                    device_ip="192.168.1.100",
                    direction="request",
                )

                found = verifier.include_evidence(
                    label="latency_250us_packets",
                    payload_hex_contains="0003d090",
                )

                assert len(found) == 1


@pytest.mark.asyncio
async def test_query_packets_filters(tmp_path):
    from netaudio_lib.dante.packet_store import PacketStore

    db_path = str(tmp_path / "query_test.sqlite")
    store = PacketStore(db_path=db_path)

    pkt_a = struct.pack(">HHHH", 0x2729, 20, 0x0001, 0x1101) + struct.pack(">H", 0x0001)
    pkt_b = struct.pack(">HHHH", 0x2729, 20, 0x0002, 0x1002) + struct.pack(">H", 0x0001)
    pkt_c = struct.pack(">HHHH", 0x27FF, 20, 0x0003, 0x1101) + struct.pack(">H", 0x0001)

    store.store_packet(payload=pkt_a, source_type="tshark", device_ip="192.168.1.100", direction="request")
    store.store_packet(payload=pkt_b, source_type="tshark", device_ip="192.168.1.200", direction="request")
    store.store_packet(payload=pkt_c, source_type="netaudio_request", device_ip="192.168.1.100", direction="request")

    by_ip = store.query_packets(device_ip="192.168.1.100")
    assert len(by_ip) == 2

    by_opcode = store.query_packets(opcode=0x1101)
    assert len(by_opcode) == 2

    by_protocol = store.query_packets(protocol_id=0x2729)
    assert len(by_protocol) == 2

    by_combo = store.query_packets(device_ip="192.168.1.100", opcode=0x1101, protocol_id=0x2729)
    assert len(by_combo) == 1

    by_source = store.query_packets(source_type="tshark")
    assert len(by_source) == 2

    by_hex = store.query_packets(payload_hex_contains="2729")
    assert len(by_hex) == 2

    store.close()
