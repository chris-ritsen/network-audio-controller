from __future__ import annotations

import socket
import struct
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.provenance import _extract_field
from netaudio.commands.capture_helpers import (
    _compact_hexdump,
    _label_packet,
    _load_capture_profile,
    _normalize_marker_label,
    _parse_field_spec,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_facts_path,
    _resolve_provenance_bundle_path,
    _resolve_session_reference,
)
from netaudio.commands.provenance_app import app
from netaudio.dante.packet_store import PacketStore


@app.command("analysis")
def provenance_analysis(
    label: str = typer.Option(..., "--label", help="Analysis label."),
    note: str = typer.Option(..., "--note", help="What was found in the packet(s)."),
    packet_id: Optional[list[int]] = typer.Option(None, "--packet-id", help="Packet ID(s) analyzed (repeatable)."),
    field: Optional[list[str]] = typer.Option(
        None,
        "--field",
        help="Field extracted: direction:name:offset:length:type:value; direction is optional. Repeatable.",
    ),
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
        if packet_id:
            for pid in packet_id:
                pkt = store.get_packet(pid)
                if not pkt:
                    print(f"Capture: Packet #{pid} not found.", file=sys.stderr)
                    raise typer.Exit(1)
                resolved_packet_ids.append(pid)

        fields_parsed = [_parse_field_spec(f) for f in field] if field else []

        normalized_label = _normalize_marker_label(label)
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="analysis",
            label=normalized_label,
            note=note,
            data={
                "packet_ids": resolved_packet_ids,
                "fields": fields_parsed,
            },
        )

        print(f"Capture: Analysis marker #{marker_id} added to session #{resolved_session_id}")
        print(f"Capture: Label: {normalized_label}")
        if resolved_packet_ids:
            print(f"Capture: Packets referenced: {', '.join(f'#{p}' for p in resolved_packet_ids)}")
        if fields_parsed:
            for field_entry in fields_parsed:
                value_str = f" value={field_entry['value']}" if "value" in field_entry else ""
                print(
                    f"  {field_entry['name']}: offset={field_entry['offset']} "
                    f"len={field_entry['length']} type={field_entry['dtype']}"
                    f"{value_str}"
                )
    finally:
        store.close()


@app.command("hypothesis")
def provenance_hypothesis(
    label: str = typer.Option(..., "--label", help="Hypothesis label."),
    note: str = typer.Option(..., "--note", help="Falsifiable claim being tested."),
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
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    normalized_label = _normalize_marker_label(label)

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="active",
        )
        marker_id = store.add_marker(
            session_id=resolved_session_id,
            marker_type="hypothesis",
            label=normalized_label,
            note=note,
            source_host=socket.gethostname(),
        )
    finally:
        store.close()

    print(f"Capture: Hypothesis marker #{marker_id} added to session #{resolved_session_id}")
    print(f"Capture: Label: {normalized_label}")


@app.command("analyze")
def provenance_analyze(
    bundle: str = typer.Argument(..., help="Path to provenance bundle (.tar.gz or directory)."),
    raw: bool = typer.Option(False, "--raw", help="Show raw hexdump for each packet."),
):
    from netaudio.dante.fact_store import _load_bundle as lib_load_bundle, list_facts, _verify_field

    bundle_path = _resolve_provenance_bundle_path(bundle)
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = lib_load_bundle(bundle_path)
    if not manifest:
        print(f"Empty or invalid bundle: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    facts_path = _resolve_facts_path()
    all_facts = list_facts(facts_path) if facts_path.exists() else []

    arc_facts_by_opcode = {}
    conmon_facts_by_type = {}
    for fact in all_facts:
        if fact["category"] == "arc_opcode":
            try:
                arc_facts_by_opcode[int(fact["key"], 16)] = fact
            except ValueError:
                pass
        elif fact["category"] == "conmon_message":
            try:
                conmon_facts_by_type[int(fact["key"], 16)] = fact
            except ValueError:
                pass

    samples = manifest.get("samples", [])
    scope = manifest.get("scope", {})
    session_name = manifest.get("session_name", bundle_path.stem)

    print(f"Bundle: {bundle_path.name}")
    if scope.get("device_ip"):
        print(f"Device: {scope.get('device_ip', '?')}  {scope.get('device_name', '')}")
    print(f"Session: {session_name}")
    print(f"Packets: {len(samples)}")
    print()

    responses = [s for s in samples if s.get("direction") == "response"]

    device_profile = {}

    for sample in responses:
        filename = sample.get("file", "")
        payload = files.get(filename)
        if payload is None:
            continue

        protocol_id = sample.get("protocol_id", 0)
        opcode_val = sample.get("opcode", 0)
        opcode_hex = f"0x{opcode_val:04X}" if opcode_val else "?"

        fact = None
        if protocol_id in (0x27FF, 0x2809, 0x1200):
            fact = arc_facts_by_opcode.get(opcode_val)
        elif protocol_id == 0xFFFF and len(payload) >= 28:
            message_type = struct.unpack(">H", payload[26:28])[0]
            fact = conmon_facts_by_type.get(message_type)

        label = _label_packet(payload)
        src = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
        dst = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"

        print(f"  {opcode_hex}  {label or '?':30s}  {src} -> {dst}  {len(payload)}B")

        if fact and fact.get("fields"):
            for field_def in fact["fields"]:
                result = _extract_field(payload, field_def)
                if result:
                    print(f"    {result['name']:25s} = {result['display']}")
                    if result.get("profile_key"):
                        device_profile[result["profile_key"]] = result["display"]

        if raw:
            for line in _compact_hexdump(payload, max_lines=4):
                print(line)

        print()

    if device_profile:
        print("Device Profile:")
        for profile_key, value in device_profile.items():
            print(f"  {profile_key:25s} = {value}")
