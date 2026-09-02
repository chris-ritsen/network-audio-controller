import hashlib
import json
import struct
import tarfile

from typer.testing import CliRunner

from netaudio.commands import capture as capture_commands
from netaudio.commands import fact as fact_commands
from netaudio.commands import provenance as provenance_commands
from netaudio.commands.capture_helpers import _parse_field_spec
from netaudio.dante.fact_store import (
    FactRecord,
    FactUpdate,
    add_fact,
    check_facts,
    disprove_fact,
    get_fact,
    quarantine_fact,
    update_fact,
)
from netaudio.dante.packet_store import ArtifactRecord, PacketQuery, PacketRecord, PacketStore
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
        PacketRecord(
            payload=build_arc_packet(0x1002),
            source_type="tshark",
            session_id=session_id,
            timestamp_ns=200,
        )
    )
    packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x1003),
            source_type="tshark",
            timestamp_ns=250,
        )
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
        PacketRecord(
            payload=build_arc_packet(0x1002),
            source_type="tshark",
            session_id=primary_session_id,
        )
    )
    packet_store.link_packet_to_session(packet_id, secondary_session_id)

    packets = packet_store.search_packets(PacketQuery(session_id=secondary_session_id))
    packet_count = packet_store.search_packets_count(PacketQuery(session_id=secondary_session_id))
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
        PacketRecord(
            payload=build_arc_packet(0x1002),
            source_type="tshark",
            session_id=session_id,
        )
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
    add_fact(facts_path, "conmon_message", "0x100A", FactRecord("gain_control", evidence=["old_session:1"]))

    update_fact(
        facts_path,
        "conmon_message",
        "0x100A",
        FactUpdate(evidence=["gain_session:2", "gain_session:3"], replace_evidence=True),
    )

    fact = get_fact(facts_path, "conmon_message", "0x100A")
    assert fact is not None
    assert fact["evidence"] == ["gain_session:2", "gain_session:3"]


def test_fact_update_can_clear_packet_fields(tmp_path, monkeypatch):
    facts_path = tmp_path / "facts.json"
    add_fact(
        facts_path,
        "configuration_persistence",
        "preferred_encoding",
        FactRecord(
            "preferred_encoding_persistence",
            fields=[{"name": "preferred_encoding", "offset": 13, "length": 1, "dtype": "uint8"}],
        ),
    )
    monkeypatch.setattr(fact_commands, "_resolve_facts_path", lambda: facts_path)

    result = runner.invoke(
        fact_commands.app,
        [
            "update",
            "--category",
            "configuration_persistence",
            "--key",
            "preferred_encoding",
            "--clear-fields",
        ],
    )

    assert result.exit_code == 0
    assert get_fact(facts_path, "configuration_persistence", "preferred_encoding")["fields"] == []


def test_fact_field_parser_accepts_explicit_packet_direction():
    assert _parse_field_spec("request:flow_slot:14:2:uint16_be:32") == {
        "name": "flow_slot",
        "offset": 14,
        "length": 2,
        "dtype": "uint16_be",
        "value": "32",
        "direction": "request",
    }
    assert _parse_field_spec("response:result_code:8:2:uint16_be") == {
        "name": "result_code",
        "offset": 8,
        "length": 2,
        "dtype": "uint16_be",
        "direction": "response",
    }


def test_fact_check_applies_scoped_fields_only_to_matching_packet_directions(tmp_path):
    facts_path = tmp_path / "facts.json"
    bundle_directory = tmp_path / "bundles"
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="direction_scoped_fields")
    request_id = packet_store.store_packet(
        PacketRecord(
            payload=bytes.fromhex("2729001016c122020000000100000020"),
            source_type="pcap_import",
            direction="request",
            session_id=session_id,
            timestamp_ns=100,
        )
    )
    response_id = packet_store.store_packet(
        PacketRecord(
            payload=bytes.fromhex("2729000a16c122020001"),
            source_type="pcap_import",
            direction="response",
            session_id=session_id,
            timestamp_ns=200,
        )
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="delete_exchange",
        data={"packet_ids": [request_id, response_id]},
    )
    packet_store.end_session(session_id, ended_ns=300)
    export_session_bundle(packet_store, session_id, output_dir=str(bundle_directory))
    packet_store.close()

    add_fact(
        facts_path,
        "arc_opcode",
        "0x2202",
        FactRecord(
            "delete_tx_flow",
            fields=[
                {
                    "name": "record_count",
                    "offset": 10,
                    "length": 2,
                    "dtype": "uint16_be",
                    "value": "1",
                    "direction": "request",
                },
                {
                    "name": "result_code",
                    "offset": 8,
                    "length": 2,
                    "dtype": "uint16_be",
                    "value": "1",
                    "direction": "response",
                },
            ],
            evidence=[
                f"direction_scoped_fields:{request_id}",
                f"direction_scoped_fields:{response_id}",
            ],
        ),
    )

    results = check_facts(facts_path, provenance_dir=bundle_directory)

    assert len(results) == 1
    assert results[0]["errors"] == []
    assert [field["name"] for field in results[0]["verified_fields"]] == ["record_count", "result_code"]


def test_fact_check_rejects_scoped_field_without_matching_direction_evidence(tmp_path):
    facts_path = tmp_path / "facts.json"
    bundle_directory = tmp_path / "bundles"
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="request_only_evidence")
    request_id = packet_store.store_packet(
        PacketRecord(
            payload=bytes.fromhex("2729001016c122020000000100000020"),
            source_type="pcap_import",
            direction="request",
            session_id=session_id,
            timestamp_ns=100,
        )
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="delete_request",
        data={"packet_ids": [request_id]},
    )
    packet_store.end_session(session_id, ended_ns=200)
    export_session_bundle(packet_store, session_id, output_dir=str(bundle_directory))
    packet_store.close()

    add_fact(
        facts_path,
        "arc_opcode",
        "0x2202",
        FactRecord(
            "delete_tx_flow",
            fields=[
                {
                    "name": "result_code",
                    "offset": 8,
                    "length": 2,
                    "dtype": "uint16_be",
                    "direction": "response",
                }
            ],
            evidence=[f"request_only_evidence:{request_id}"],
        ),
    )

    results = check_facts(facts_path, provenance_dir=bundle_directory)

    assert results[0]["errors"] == ["field result_code: no response evidence packet"]


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


def test_verified_fact_can_use_an_explicit_evidence_database(tmp_path, monkeypatch):
    database_path = tmp_path / "capture.sqlite"
    facts_path = tmp_path / "facts.json"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="isolated_experiment")
    packet_id = packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x3000),
            source_type="pcap_import",
            session_id=session_id,
        )
    )
    packet_store.close()
    monkeypatch.setattr(fact_commands, "_resolve_facts_path", lambda: facts_path)

    result = runner.invoke(
        fact_commands.app,
        [
            "add",
            "--category",
            "arc_field",
            "--key",
            "receiver_subscription_status",
            "--name",
            "receiver subscription result",
            "--confidence",
            "verified",
            "--evidence",
            f"isolated_experiment:{packet_id}",
            "--db",
            str(database_path),
        ],
    )

    assert result.exit_code == 0
    assert get_fact(facts_path, "arc_field", "receiver_subscription_status")["evidence"] == [
        f"isolated_experiment:{packet_id}"
    ]

    packet_store = PacketStore(db_path=str(database_path))
    markers = packet_store.get_markers(session_id, marker_types=["evidence"])
    packet_store.close()
    assert any(marker["data"]["fact"] == "arc_field:receiver_subscription_status" for marker in markers)


def test_verify_accepts_exported_tarball(tmp_path):
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="archive_verification")
    packet_id = packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x1002),
            source_type="tshark",
            session_id=session_id,
        )
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


def test_curated_artifact_is_copied_into_verified_bundle(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    artifact_path = tmp_path / "controller-observation.md"
    artifact_content = b"# Controller observation\n\nStatus 0x0009 rendered as a working unicast route.\n"
    artifact_path.write_bytes(artifact_content)

    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="curated_artifact")
    packet_id = packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x3000),
            source_type="tshark",
            session_id=session_id,
        )
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="subscription_status_packet",
        data={"packet_ids": [packet_id]},
    )
    packet_store.close()

    result = runner.invoke(
        provenance_commands.app,
        [
            "artifact",
            str(artifact_path),
            "--label",
            "controller_subscription_status_observation",
            "--role",
            "controller-ui-observation",
            "--note",
            "Ordinary Controller UI observation for the controlled wire value.",
            "--session-id",
            str(session_id),
            "--db",
            str(database_path),
        ],
    )

    assert result.exit_code == 0
    assert hashlib.sha256(artifact_content).hexdigest() in result.output

    artifact_path.write_bytes(b"changed after ingestion")
    packet_store = PacketStore(db_path=str(database_path))
    artifacts = packet_store.get_session_artifacts(session_id)
    bundle_path = export_session_bundle(packet_store, session_id, output_dir=str(tmp_path / "bundles"))
    packet_store.close()

    assert len(artifacts) == 1
    assert artifacts[0]["content"] == artifact_content

    manifest, files = provenance_commands._load_bundle(bundle_path)
    artifact_entry = manifest["artifacts"][0]
    sample_entry = manifest["samples"][0]
    assert manifest["format_version"] == 2
    assert artifact_entry["role"] == "controller-ui-observation"
    assert artifact_entry["sha256"] == hashlib.sha256(artifact_content).hexdigest()
    assert files[artifact_entry["file"]] == artifact_content
    assert sample_entry["sha256"] == hashlib.sha256(build_arc_packet(0x3000)).hexdigest()
    assert provenance_commands._verify_single_bundle(bundle_path) is True


def test_exact_captured_payload_can_be_ingested_with_original_metadata(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    payload_path = tmp_path / "controller-request.bin"
    payload = struct.pack(">HHHH", 0x2729, 8, 0x49A4, 0x2013)
    payload_path.write_bytes(payload)
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="captured_payload")
    packet_store.close()

    result = runner.invoke(
        provenance_commands.app,
        [
            "ingest-payload",
            str(payload_path),
            "--source-ip",
            "192.168.1.156",
            "--source-port",
            "51519",
            "--destination-ip",
            "192.168.1.107",
            "--destination-port",
            "4440",
            "--device-ip",
            "192.168.1.107",
            "--direction",
            "request",
            "--timestamp-ns",
            "1786750000000000000",
            "--source-host",
            "macbook",
            "--interface",
            "en0",
            "--label",
            "controller_transmit_channel_rename",
            "--session-id",
            str(session_id),
            "--db",
            str(database_path),
        ],
    )

    assert result.exit_code == 0
    packet_store = PacketStore(db_path=str(database_path))
    packet = packet_store.get_packet(1)
    markers = packet_store.get_markers(session_id)
    bundle_path = export_session_bundle(packet_store, session_id, output_dir=str(tmp_path / "bundles"))
    packet_store.close()
    assert packet["payload"] == payload
    assert packet["src_ip"] == "192.168.1.156"
    assert packet["dst_ip"] == "192.168.1.107"
    assert packet["source_host"] == "macbook"
    assert packet["timestamp_ns"] == 1786750000000000000
    assert markers[0]["data"]["source_payload"]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert provenance_commands._verify_single_bundle(bundle_path) is True


def test_exact_packets_can_be_copied_from_a_read_only_capture_database(tmp_path):
    source_database_path = tmp_path / "source.sqlite"
    destination_database_path = tmp_path / "destination.sqlite"
    request = bytes.fromhex("28090022285224000000000000000000000100010001000000000000830283060310")
    response = bytes.fromhex("2809000a285224000030")

    source_store = PacketStore(db_path=str(source_database_path))
    source_request_identifier = source_store.store_packet(
        PacketRecord(
            payload=request,
            source_type="tshark",
            src_ip="192.168.1.156",
            src_port=62367,
            dst_ip="192.168.1.61",
            dst_port=4440,
            device_ip="192.168.1.61",
            direction="request",
            timestamp_ns=1_786_750_000_000_000_000,
            source_host="macbook",
            interface="en0",
        )
    )
    source_response_identifier = source_store.store_packet(
        PacketRecord(
            payload=response,
            source_type="tshark",
            src_ip="192.168.1.61",
            src_port=4440,
            dst_ip="192.168.1.156",
            dst_port=62367,
            device_ip="192.168.1.61",
            direction="response",
            timestamp_ns=1_786_750_000_004_000_000,
            source_host="macbook",
            interface="en0",
        )
    )
    source_store.close()
    source_database_sha256 = hashlib.sha256(source_database_path.read_bytes()).hexdigest()

    destination_store = PacketStore(db_path=str(destination_database_path))
    session_id = destination_store.start_session(name="copied_packets")
    destination_store.close()

    result = runner.invoke(
        provenance_commands.app,
        [
            "ingest-packet",
            "--source-db",
            str(source_database_path),
            "--packet-id",
            str(source_request_identifier),
            "--packet-id",
            str(source_response_identifier),
            "--label",
            "controller_transmitter_channel_status",
            "--session-id",
            str(session_id),
            "--db",
            str(destination_database_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert hashlib.sha256(source_database_path.read_bytes()).hexdigest() == source_database_sha256
    destination_store = PacketStore(db_path=str(destination_database_path))
    copied_request = destination_store.get_packet(1)
    copied_response = destination_store.get_packet(2)
    markers = destination_store.get_markers(session_id)
    destination_store.close()

    assert copied_request["payload"] == request
    assert copied_request["src_ip"] == "192.168.1.156"
    assert copied_request["dst_ip"] == "192.168.1.61"
    assert copied_request["direction"] == "request"
    assert copied_request["source_type"] == "curated_packet_import"
    assert copied_request["source_host"] == "macbook"
    assert copied_request["interface"] == "en0"
    assert copied_response["payload"] == response
    assert copied_response["direction"] == "response"
    assert markers[0]["data"]["packet_ids"] == [1, 2]
    assert [entry["source_packet_id"] for entry in markers[0]["data"]["source_packets"]] == [
        source_request_identifier,
        source_response_identifier,
    ]


def test_provenance_archives_are_reproducible(tmp_path):
    packet_store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
    session_id = packet_store.start_session(name="reproducible_bundle")
    packet_id = packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x3000),
            source_type="tshark",
            session_id=session_id,
        )
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="packet",
        data={"packet_ids": [packet_id]},
    )
    packet_store.add_artifact(
        ArtifactRecord(
            session_id=session_id,
            label="analysis",
            role="protocol-analysis",
            filename="analysis.json",
            media_type="application/json",
            content=b'{"status":"verified"}\n',
        )
    )

    for bundle_format in ("tar.gz", "zip"):
        first = export_session_bundle(
            packet_store,
            session_id,
            output_dir=str(tmp_path / "first"),
            bundle_format=bundle_format,
        )
        second = export_session_bundle(
            packet_store,
            session_id,
            output_dir=str(tmp_path / "second"),
            bundle_format=bundle_format,
        )
        assert first.read_bytes() == second.read_bytes()

    packet_store.close()


def test_bundle_verification_rejects_modified_artifact(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="tampered_artifact")
    packet_id = packet_store.store_packet(
        PacketRecord(
            payload=build_arc_packet(0x3000),
            source_type="tshark",
            session_id=session_id,
        )
    )
    packet_store.add_marker(
        session_id=session_id,
        marker_type="evidence",
        label="packet",
        data={"packet_ids": [packet_id]},
    )
    packet_store.add_artifact(
        ArtifactRecord(
            session_id=session_id,
            label="ui_observation",
            role="controller-ui-observation",
            filename="observation.txt",
            media_type="text/plain",
            content=b"controlled observation",
        )
    )
    bundle_path = export_session_bundle(packet_store, session_id, output_dir=str(tmp_path / "bundles"))
    packet_store.close()

    extracted_path = tmp_path / "extracted"
    with tarfile.open(bundle_path, "r:gz") as archive:
        for member in archive.getmembers():
            source = archive.extractfile(member)
            if source is None:
                continue
            destination = extracted_path / member.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(source.read())
    bundle_directory = next(path.parent for path in extracted_path.rglob("manifest.json"))
    manifest = json.loads((bundle_directory / "manifest.json").read_text())
    artifact_file = bundle_directory / manifest["artifacts"][0]["file"]
    artifact_file.write_bytes(b"modified")

    assert provenance_commands._verify_single_bundle(bundle_directory) is False


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
        PacketRecord(
            payload=build_conmon_packet(0x100A, sequence=0x1234),
            source_type="tshark",
            session_id=session_id,
        )
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


def test_hypothesis_command_records_a_falsifiable_session_marker(tmp_path):
    database_path = tmp_path / "capture.sqlite"
    packet_store = PacketStore(db_path=str(database_path))
    session_id = packet_store.start_session(name="preferred_leader_persistence")
    packet_store.close()

    result = runner.invoke(
        provenance_commands.app,
        [
            "hypothesis",
            "--db",
            str(database_path),
            "--session-id",
            str(session_id),
            "--label",
            "preferred_leader_persists",
            "--note",
            "A gated preferred-leader write creates retained state restored on reboot.",
        ],
    )

    assert result.exit_code == 0
    packet_store = PacketStore(db_path=str(database_path))
    markers = packet_store.get_markers(session_id, marker_types=["hypothesis"])
    packet_store.close()
    assert len(markers) == 1
    assert markers[0]["label"] == "preferred_leader_persists"
    assert markers[0]["note"] == ("A gated preferred-leader write creates retained state restored on reboot.")
    assert markers[0]["data"] is None


def test_check_facts_does_not_fail_disproved_facts(tmp_path):
    facts_path = tmp_path / "facts.json"
    add_fact(facts_path, "conmon_message", "0x0BC8", FactRecord("identify", note="wrong opcode", confidence="observed"))
    disprove_fact(facts_path, "conmon_message", "0x0BC8", reason="0x0BC8 is a sequence, not an opcode")

    results = check_facts(facts_path, provenance_dir=tmp_path)

    assert results == [
        {
            "fact_key": "conmon_message:0x0BC8",
            "name": "identify",
            "category": "conmon_message",
            "key": "0x0BC8",
            "confidence": "disproved",
            "evidence_count": 0,
            "errors": [],
            "verified_fields": [],
            "status": "disproved",
        }
    ]


def test_check_facts_does_not_fail_quarantined_facts(tmp_path):
    facts_path = tmp_path / "facts.json"
    add_fact(
        facts_path,
        "conmon_message",
        "0x0084",
        FactRecord("sample_rate_pullup_status", note="catalog updated; bundle not curated", confidence="verified"),
    )
    quarantined = quarantine_fact(
        facts_path,
        "conmon_message",
        "0x0084",
        reason="no curated provenance bundle",
    )

    assert quarantined["quarantine"]["reason"] == "no curated provenance bundle"
    results = check_facts(facts_path, provenance_dir=tmp_path)

    assert results[0]["status"] == "quarantined"
    assert results[0]["errors"] == []
    assert results[0]["quarantine_reason"] == "no curated provenance bundle"
