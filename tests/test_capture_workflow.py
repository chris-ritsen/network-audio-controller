import json
import struct

from typer.testing import CliRunner

from netaudio.commands import capture as capture_commands
from netaudio.commands import fact as fact_commands
from netaudio.commands import provenance as provenance_commands
from netaudio.dante.fact_store import add_fact, get_fact, update_fact
from netaudio.dante.packet_store import PacketStore
from netaudio.dante.protocol_verifier import export_session_bundle


runner = CliRunner()


def build_arc_packet(opcode: int, transaction_id: int = 1) -> bytes:
    return struct.pack(">HHHH", 0x27FF, 8, transaction_id, opcode)


def build_conmon_packet(message_type: int, sequence: int = 1) -> bytes:
    packet = bytearray(28)
    struct.pack_into(">HH", packet, 0, 0xFFFF, len(packet))
    struct.pack_into(">H", packet, 4, sequence)
    struct.pack_into(">H", packet, 26, message_type)
    return bytes(packet)


def test_packet_list_uses_session_membership(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="target", started_ns=100)
    packet_store.store_packet(
        payload=build_arc_packet(0x1002),
        source_type="tshark",
        session_id=session_id,
        timestamp_ns=200,
    )
    packet_store.store_packet(
        payload=build_arc_packet(0x1003),
        source_type="tshark",
        timestamp_ns=250,
    )
    packet_store.end_session(session_id, ended_ns=300)
    packet_store.close()

    result = runner.invoke(
        capture_commands.app,
        ["packet", "list", "--db", str(database_path), "--session", "target"],
    )

    assert result.exit_code == 0
    assert "1 matched, showing 1" in result.output


def test_session_search_includes_secondary_session_links(tmp_path):
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    primary_session_id = packet_store.start_session(name="primary")
    secondary_session_id = packet_store.start_session(name="secondary")
    packet_id = packet_store.store_packet(
        payload=build_arc_packet(0x1002),
        source_type="tshark",
        session_id=primary_session_id,
    )
    packet_store.link_packet_to_session(packet_id, secondary_session_id)

    packets = packet_store.search_packets(session_id=secondary_session_id)
    packet_count = packet_store.search_packets_count(session_id=secondary_session_id)
    packet_store.close()

    assert [packet["id"] for packet in packets] == [packet_id]
    assert packet_count == 1


def test_evidence_count_accepts_canonical_and_legacy_markers(tmp_path):
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="evidence")
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="canonical",
        data={"packet_ids": [10]},
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="legacy",
        data={"packet_id": 11},
    )

    assert packet_store.get_session_evidence_count(session_id) == 2
    packet_store.close()


def test_fact_evidence_marker_uses_packet_id_list(tmp_path, monkeypatch):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="fact_evidence")
    packet_id = packet_store.store_packet(
        payload=build_arc_packet(0x1002),
        source_type="tshark",
        session_id=session_id,
    )
    packet_store.close()
    monkeypatch.setattr(
        "netaudio.capture.sessions.open_packet_store",
        lambda: PacketStore(db_path=str(database_path)),
    )

    fact_commands._create_evidence_markers(
        [(session_id, packet_id)],
        "arc_opcode",
        "0x1002",
        "query_device_name",
    )
    fact_commands._create_evidence_markers(
        [(session_id, packet_id)],
        "arc_opcode",
        "0x1002",
        "query_device_name",
    )

    packet_store = PacketStore(db_path=str(database_path))
    evidence_markers = packet_store.get_markers(session_id, marker_types=["evidence"])
    packet_store.close()

    assert len(evidence_markers) == 1
    assert evidence_markers[0]["data"]["packet_ids"] == [packet_id]


def test_fact_update_can_replace_evidence(tmp_path):
    facts_path = tmp_path / "facts.json"
    add_fact(
        facts_path,
        "conmon_message",
        "0x100A",
        "gain_control",
        evidence=["old_session:1"],
    )

    update_fact(
        facts_path,
        "conmon_message",
        "0x100A",
        evidence=["gain_session:2", "gain_session:3"],
        replace_evidence=True,
    )

    fact = get_fact(facts_path, "conmon_message", "0x100A")
    assert fact is not None
    assert fact["evidence"] == ["gain_session:2", "gain_session:3"]


def test_verified_fact_requires_packet_evidence(tmp_path, monkeypatch):
    facts_path = tmp_path / "facts.json"
    monkeypatch.setattr(fact_commands, "_resolve_facts_path", lambda: facts_path)

    result = runner.invoke(
        fact_commands.app,
        [
            "add",
            "--category",
            "arc_opcode",
            "--key",
            "0x1002",
            "--name",
            "query_device_name",
            "--confidence",
            "verified",
        ],
    )

    assert result.exit_code == 1
    assert "verified facts require" in result.output
    assert not facts_path.exists()


def test_verify_accepts_exported_tarball(tmp_path):
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="archive_verification")
    packet_id = packet_store.store_packet(
        payload=build_arc_packet(0x1002),
        source_type="tshark",
        session_id=session_id,
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="device_name",
        data={"packet_ids": [packet_id]},
    )
    packet_store.end_session(session_id)
    bundle_path = export_session_bundle(packet_store, session_id, output_dir=str(tmp_path))
    packet_store.close()

    result = runner.invoke(provenance_commands.app, ["verify", str(bundle_path)])

    assert result.exit_code == 0
    assert "Packets verified: 1/1" in result.output


def test_empty_bundle_fails_verification_and_audit(tmp_path):
    bundle_path = tmp_path / "empty"
    bundle_path.mkdir()
    (bundle_path / "manifest.json").write_text(
        json.dumps(
            {
                "session_id": 1,
                "session_name": "empty",
                "markers": [],
                "samples": [],
            }
        )
    )

    assert provenance_commands._verify_single_bundle(bundle_path) is False
    assert provenance_commands._audit_single_bundle(bundle_path) is False


def test_evidence_output_uses_conmon_message_type(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="conmon")
    packet_id = packet_store.store_packet(
        payload=build_conmon_packet(0x100A, sequence=0x1234),
        source_type="tshark",
        session_id=session_id,
    )
    packet_store.close()

    result = runner.invoke(
        provenance_commands.app,
        [
            "evidence",
            "--db",
            str(database_path),
            "--session",
            "conmon",
            "--label",
            "gain_control",
            "--packet-id",
            str(packet_id),
        ],
    )

    assert result.exit_code == 0
    assert "opcode=0x100A" in result.output
    assert "opcode=0x1234" not in result.output
