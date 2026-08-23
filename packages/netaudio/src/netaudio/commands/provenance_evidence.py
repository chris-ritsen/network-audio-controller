from __future__ import annotations

import hashlib
import ipaddress
import mimetypes
import socket
import sqlite3
import sys
import zlib
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.provenance import _verify_parse_header
from netaudio.commands.capture_helpers import (
    _load_capture_profile,
    _normalize_marker_label,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_session_reference,
)
from netaudio.commands.provenance_app import app
from netaudio.commands.provenance_support import _sha256_path
from netaudio.dante.packet_store import PacketStore
from netaudio.dante.tshark_capture import TsharkCapture


EVIDENCE_ARTIFACT_ROLES = frozenset(
    {
        "controller-ui-observation",
        "experiment-input",
        "experiment-output",
        "guest-state",
        "protocol-analysis",
    }
)


@app.command("evidence")
def provenance_evidence(
    packet_ids_positional: Optional[list[int]] = typer.Argument(None, help="Packet IDs to tag as evidence."),
    label: str = typer.Option(..., "--label", help="Evidence label for this marker."),
    note: Optional[str] = typer.Option(None, "--note", help="Descriptive note about this evidence."),
    packet_id: Optional[list[int]] = typer.Option(None, "--packet-id", help="Specific packet IDs to tag (repeatable)."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", help="Filter packets by device IP."),
    opcode: Optional[str] = typer.Option(None, "--opcode", help="Filter packets by opcode (hex, e.g. 0x1100)."),
    direction: Optional[str] = typer.Option(
        None, "--direction", help="Filter packets by direction (request/response)."
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )

        resolved_packet_ids = []

        all_explicit_ids = list(packet_ids_positional or []) + list(packet_id or [])
        for pid in all_explicit_ids:
            if pid not in resolved_packet_ids:
                pkt = store.get_packet(pid)
                if not pkt:
                    print(f"Capture: Packet #{pid} not found.", file=sys.stderr)
                    raise typer.Exit(1)
                resolved_packet_ids.append(pid)

        query_kwargs = {}
        if device_ip:
            query_kwargs["device_ip"] = device_ip
        if opcode:
            query_kwargs["opcode"] = int(opcode, 16) if opcode.startswith("0x") else int(opcode)
        if direction:
            query_kwargs["direction"] = direction

        if query_kwargs:
            query_kwargs["session_id"] = resolved_session_id
            matched = store.query_packets(**query_kwargs)
            for pkt in matched:
                if pkt["id"] not in resolved_packet_ids:
                    resolved_packet_ids.append(pkt["id"])

        if not resolved_packet_ids:
            print("Capture: No packets matched the given filters.", file=sys.stderr)
            raise typer.Exit(1)

        normalized_label = _normalize_marker_label(label)
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or f"Tagged {len(resolved_packet_ids)} packets as evidence",
            data={
                "packet_ids": resolved_packet_ids,
                "filters": {
                    k: (f"0x{v:04X}" if k == "opcode" else v) for k, v in query_kwargs.items() if k != "session_id"
                },
            },
        )

        print(f"Capture: Evidence marker #{marker_id} added to session #{resolved_session_id}")
        print(f"Capture: Label: {normalized_label}")
        print(f"Capture: Packets tagged: {len(resolved_packet_ids)}")
        for pid in resolved_packet_ids[:20]:
            pkt = store.get_packet(pid)
            if pkt:
                payload = pkt.get("payload", b"")
                opcode_hex = ""
                header = _verify_parse_header(payload)
                if header and header.get("opcode") is not None:
                    opcode_hex = f" opcode=0x{header['opcode']:04X}"
                pkt_direction = pkt.get("direction", "?")
                print(f"  #{pid} {pkt_direction}{opcode_hex} {len(payload)}B")
        if len(resolved_packet_ids) > 20:
            print(f"  ... and {len(resolved_packet_ids) - 20} more")
    finally:
        store.close()


@app.command("artifact")
def provenance_artifact(
    file: str = typer.Argument(..., help="Path to a curated evidence artifact."),
    label: str = typer.Option(..., "--label", help="Evidence label for this artifact."),
    role: str = typer.Option(..., "--role", help="Artifact role in the experiment."),
    note: Optional[str] = typer.Option(None, "--note", help="Why this artifact advances the protocol finding."),
    media_type: Optional[str] = typer.Option(None, "--media-type", help="Override the detected media type."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    normalized_role = role.strip().lower()
    if normalized_role not in EVIDENCE_ARTIFACT_ROLES:
        choices = ", ".join(sorted(EVIDENCE_ARTIFACT_ROLES))
        print(f"unsupported artifact role {role!r}; choose one of: {choices}", file=sys.stderr)
        raise typer.Exit(1)

    source_path = Path(file).expanduser().resolve()
    if not source_path.is_file():
        print(f"evidence artifact not found: {source_path}", file=sys.stderr)
        raise typer.Exit(1)
    content = source_path.read_bytes()
    if not content:
        print(f"evidence artifact is empty: {source_path}", file=sys.stderr)
        raise typer.Exit(1)

    detected_media_type = mimetypes.guess_type(source_path.name)[0] or "application/octet-stream"
    resolved_media_type = media_type.strip() if media_type and media_type.strip() else detected_media_type
    normalized_label = _normalize_marker_label(label)
    source_stat = source_path.stat()

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )
        artifact_id = store.add_artifact(
            session_id=resolved_session_id,
            label=normalized_label,
            role=normalized_role,
            filename=source_path.name,
            media_type=resolved_media_type,
            content=content,
            note=note,
            source_path=str(source_path),
            source_host=socket.gethostname(),
            source_modified_ns=source_stat.st_mtime_ns,
        )
        artifact = store.get_artifact(artifact_id)
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or f"Attached curated {normalized_role} artifact",
            source_host=socket.gethostname(),
            data={"artifact_ids": [artifact_id]},
        )
    finally:
        store.close()

    print(f"Capture: Evidence artifact #{artifact_id} added to session #{resolved_session_id}")
    print(f"Capture: Evidence marker #{marker_id}: {normalized_label}")
    print(f"Capture: Role: {normalized_role}")
    print(f"Capture: SHA-256: {artifact['sha256']}")
    print(f"Capture: Size: {artifact['size']} bytes")


@app.command("ingest-pcap")
def provenance_ingest_pcap(
    pcap: str = typer.Argument(..., help="Path to an existing packet capture."),
    frame: Optional[list[int]] = typer.Option(None, "--frame", help="Frame number to ingest (repeatable)."),
    device_ip: str = typer.Option(..., "--device-ip", help="Device address used to determine packet direction."),
    label: str = typer.Option(..., "--label", help="Evidence label for the selected frames."),
    note: Optional[str] = typer.Option(None, "--note", help="Why these frames prove the protocol finding."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.tshark_capture import TsharkCapture

    _require_positive_session_id(session_id, "--session-id")
    selected_frames = sorted(set(frame or []))
    if not selected_frames or any(frame_number <= 0 for frame_number in selected_frames):
        print("at least one positive --frame is required", file=sys.stderr)
        raise typer.Exit(1)

    capture_path = Path(pcap).expanduser().resolve()
    if not capture_path.is_file():
        print(f"capture file not found: {capture_path}", file=sys.stderr)
        raise typer.Exit(1)
    capture_size = capture_path.stat().st_size
    capture_sha256 = _sha256_path(capture_path)
    normalized_label = _normalize_marker_label(label)
    source_host = socket.gethostname()

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )
        try:
            imported = TsharkCapture.read_pcap_frames(
                packet_store=store,
                pcap_path=capture_path,
                frame_numbers=selected_frames,
                device_ip=device_ip,
                session_id=resolved_session_id,
                source_host=source_host,
            )
        except (RuntimeError, ValueError) as exception:
            print(str(exception), file=sys.stderr)
            raise typer.Exit(1)

        packet_ids = [entry["packet_id"] for entry in imported]
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or "Imported selected packet-capture frames as protocol evidence",
            source_host=source_host,
            data={
                "packet_ids": packet_ids,
                "source_capture": {
                    "path": str(capture_path),
                    "sha256": capture_sha256,
                    "size": capture_size,
                    "frame_numbers": selected_frames,
                },
            },
        )
    finally:
        store.close()

    print(f"Capture: Evidence marker #{marker_id} added to session #{resolved_session_id}")
    print(f"Capture: Source SHA-256: {capture_sha256}")
    for entry in imported:
        print(
            f"  frame {entry['frame_number']} -> packet #{entry['packet_id']} "
            f"{entry['direction']} {entry['payload_size']}B"
        )


@app.command("ingest-payload")
def provenance_ingest_payload(
    file: str = typer.Argument(..., help="Path to one exact captured UDP payload."),
    source_ip: str = typer.Option(..., "--source-ip", help="Original source IPv4 address."),
    source_port: int = typer.Option(..., "--source-port", help="Original source UDP port."),
    destination_ip: str = typer.Option(..., "--destination-ip", help="Original destination IPv4 address."),
    destination_port: int = typer.Option(..., "--destination-port", help="Original destination UDP port."),
    device_ip: str = typer.Option(..., "--device-ip", help="Dante device IPv4 address."),
    direction: str = typer.Option(..., "--direction", help="Packet direction: request or response."),
    timestamp_ns: int = typer.Option(..., "--timestamp-ns", help="Original capture timestamp in nanoseconds."),
    source_host: str = typer.Option(..., "--source-host", help="Host that made the original capture."),
    interface: Optional[str] = typer.Option(None, "--interface", help="Original capture interface."),
    label: str = typer.Option(..., "--label", help="Evidence label for this payload."),
    note: Optional[str] = typer.Option(None, "--note", help="Why this payload proves the protocol finding."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    payload_path = Path(file).expanduser().resolve()
    if not payload_path.is_file():
        print(f"captured payload not found: {payload_path}", file=sys.stderr)
        raise typer.Exit(1)
    payload = payload_path.read_bytes()
    if not 8 <= len(payload) <= 65_507:
        print("captured UDP payload must contain between 8 and 65507 bytes", file=sys.stderr)
        raise typer.Exit(1)
    declared_length = int.from_bytes(payload[2:4], "big")
    if declared_length != len(payload):
        print(
            f"captured payload length mismatch: header declares {declared_length}, file contains {len(payload)}",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    try:
        source_address = ipaddress.IPv4Address(source_ip)
        destination_address = ipaddress.IPv4Address(destination_ip)
        device_address = ipaddress.IPv4Address(device_ip)
    except ipaddress.AddressValueError as exception:
        print(f"invalid IPv4 address: {exception}", file=sys.stderr)
        raise typer.Exit(1)
    normalized_direction = direction.strip().lower()
    if normalized_direction not in {"request", "response"}:
        print("--direction must be request or response", file=sys.stderr)
        raise typer.Exit(1)
    if normalized_direction == "request" and destination_address != device_address:
        print("request destination must equal --device-ip", file=sys.stderr)
        raise typer.Exit(1)
    if normalized_direction == "response" and source_address != device_address:
        print("response source must equal --device-ip", file=sys.stderr)
        raise typer.Exit(1)
    if not 1 <= source_port <= 65_535 or not 1 <= destination_port <= 65_535:
        print("UDP ports must be between 1 and 65535", file=sys.stderr)
        raise typer.Exit(1)
    if timestamp_ns <= 0:
        print("--timestamp-ns must be positive", file=sys.stderr)
        raise typer.Exit(1)
    normalized_source_host = source_host.strip()
    if not normalized_source_host:
        print("--source-host must not be empty", file=sys.stderr)
        raise typer.Exit(1)

    normalized_label = _normalize_marker_label(label)
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )
        packet_id = store.store_packet(
            payload=payload,
            source_type="curated_import",
            src_ip=str(source_address),
            src_port=source_port,
            dst_ip=str(destination_address),
            dst_port=destination_port,
            device_ip=str(device_address),
            direction=normalized_direction,
            session_id=resolved_session_id,
            timestamp_ns=timestamp_ns,
            source_host=normalized_source_host,
            interface=interface.strip() if interface and interface.strip() else None,
        )
        if packet_id is None:
            raise RuntimeError("captured payload was not stored")
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or "Imported one exact captured UDP payload as protocol evidence",
            source_host=socket.gethostname(),
            data={
                "packet_ids": [packet_id],
                "source_payload": {
                    "path": str(payload_path),
                    "sha256": payload_sha256,
                    "size": len(payload),
                },
            },
        )
    finally:
        store.close()

    print(f"Capture: Evidence marker #{marker_id} added to session #{resolved_session_id}")
    print(f"Capture: Source SHA-256: {payload_sha256}")
    print(f"  payload -> packet #{packet_id} {normalized_direction} {len(payload)}B")


@app.command("ingest-packet")
def provenance_ingest_packet(
    source_db: str = typer.Option(..., "--source-db", help="Read-only source capture database path."),
    packet_id: Optional[list[int]] = typer.Option(
        None,
        "--packet-id",
        help="Source packet ID to ingest (repeatable).",
    ),
    label: str = typer.Option(..., "--label", help="Evidence label for the selected packets."),
    note: Optional[str] = typer.Option(None, "--note", help="Why these packets prove the protocol finding."),
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to active.",
    ),
    db: Optional[str] = typer.Option(None, "--db", help="Destination SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    selected_packet_identifiers = list(dict.fromkeys(packet_id or []))
    if not selected_packet_identifiers or any(identifier <= 0 for identifier in selected_packet_identifiers):
        print("at least one positive --packet-id is required", file=sys.stderr)
        raise typer.Exit(1)

    source_path = Path(source_db).expanduser().resolve()
    if not source_path.is_file():
        print(f"source capture database not found: {source_path}", file=sys.stderr)
        raise typer.Exit(1)

    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = Path(_resolve_db_from_config(db, profile_cfg)).expanduser().resolve()
    if source_path == resolved_db:
        print("source and destination capture databases must differ", file=sys.stderr)
        raise typer.Exit(1)

    source_connection = None
    try:
        source_connection = sqlite3.connect(f"{source_path.as_uri()}?mode=ro", uri=True)
        source_connection.row_factory = sqlite3.Row
        source_rows = []
        for source_packet_identifier in selected_packet_identifiers:
            row = source_connection.execute(
                "SELECT * FROM packets WHERE id = ?",
                (source_packet_identifier,),
            ).fetchone()
            if row is None:
                print(f"source packet #{source_packet_identifier} does not exist", file=sys.stderr)
                raise typer.Exit(1)
            source_rows.append(dict(row))
    except sqlite3.Error as exception:
        print(f"could not read source capture database: {exception}", file=sys.stderr)
        raise typer.Exit(1)
    finally:
        if source_connection is not None:
            source_connection.close()

    validated_packets = []
    for row in source_rows:
        stored_payload = row.get("payload")
        if isinstance(stored_payload, str):
            try:
                payload = bytes.fromhex(stored_payload)
            except ValueError:
                print(f"source packet #{row['id']} has invalid hexadecimal payload storage", file=sys.stderr)
                raise typer.Exit(1)
        else:
            try:
                payload = zlib.decompress(stored_payload)
            except (TypeError, zlib.error):
                payload = bytes(stored_payload or b"")
        if len(payload) < 8 or int.from_bytes(payload[2:4], "big") != len(payload):
            print(f"source packet #{row['id']} has an invalid captured payload", file=sys.stderr)
            raise typer.Exit(1)
        timestamp_ns = row.get("timestamp_ns")
        if not isinstance(timestamp_ns, int) or timestamp_ns <= 0:
            print(f"source packet #{row['id']} has an invalid timestamp", file=sys.stderr)
            raise typer.Exit(1)
        validated_packets.append((row, payload))

    normalized_label = _normalize_marker_label(label)
    store = PacketStore(db_path=str(resolved_db))
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )
        imported = []
        for row, payload in validated_packets:
            imported_packet_identifier = store.store_packet(
                payload=payload,
                source_type="curated_packet_import",
                src_ip=row.get("src_ip"),
                src_port=row.get("src_port"),
                dst_ip=row.get("dst_ip"),
                dst_port=row.get("dst_port"),
                device_name=row.get("device_name"),
                device_ip=row.get("device_ip"),
                direction=row.get("direction"),
                multicast_group=row.get("multicast_group"),
                multicast_port=row.get("multicast_port"),
                session_id=resolved_session_id,
                timestamp_ns=row["timestamp_ns"],
                source_host=row.get("source_host"),
                interface=row.get("interface"),
            )
            if imported_packet_identifier is None:
                raise RuntimeError(f"source packet #{row['id']} was not stored")
            imported.append(
                {
                    "source_packet_id": int(row["id"]),
                    "packet_id": imported_packet_identifier,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                    "size": len(payload),
                }
            )
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="evidence",
            label=normalized_label,
            note=note or "Imported exact packets from a read-only capture database",
            source_host=socket.gethostname(),
            data={
                "packet_ids": [entry["packet_id"] for entry in imported],
                "source_database": str(source_path),
                "source_packets": imported,
            },
        )
    finally:
        store.close()

    print(f"Capture: Evidence marker #{marker_id} added to session #{resolved_session_id}")
    for entry in imported:
        print(
            f"  source packet #{entry['source_packet_id']} -> packet #{entry['packet_id']} "
            f"{entry['size']}B {entry['payload_sha256']}"
        )
