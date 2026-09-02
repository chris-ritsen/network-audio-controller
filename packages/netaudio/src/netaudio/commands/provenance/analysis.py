from __future__ import annotations

import socket
import struct
from typing import Optional

import typer

from netaudio.capture.packets import _compact_hexdump, _label_packet
from netaudio.capture.provenance import _extract_field
from netaudio.commands.capture.options import (
    _load_capture_profile,
    _normalize_marker_label,
    _parse_field_spec,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_facts_path,
    _resolve_provenance_bundle_path,
    _resolve_session_reference,
)
from netaudio.commands.capture.reporting import emit_report
from netaudio.dante.packet_store import PacketStore


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
                    typer.echo(f"Capture: Packet #{pid} not found.", err=True)
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

    finally:
        store.close()

    lines = [
        f"Capture: Analysis marker #{marker_id} added to session #{resolved_session_id}",
        f"Capture: Label: {normalized_label}",
    ]
    if resolved_packet_ids:
        lines.append(f"Capture: Packets referenced: {', '.join(f'#{p}' for p in resolved_packet_ids)}")
    for field_entry in fields_parsed:
        value_str = f" value={field_entry['value']}" if "value" in field_entry else ""
        lines.append(
            f"  {field_entry['name']}: offset={field_entry['offset']} "
            f"len={field_entry['length']} type={field_entry['dtype']}"
            f"{value_str}"
        )
    emit_report(
        lines,
        {
            "fields": fields_parsed,
            "id": marker_id,
            "label": normalized_label,
            "note": note,
            "packet_ids": resolved_packet_ids,
            "session_id": resolved_session_id,
            "type": "analysis",
        },
    )


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

    emit_report(
        [
            f"Capture: Hypothesis marker #{marker_id} added to session #{resolved_session_id}",
            f"Capture: Label: {normalized_label}",
        ],
        {
            "id": marker_id,
            "label": normalized_label,
            "note": note,
            "session_id": resolved_session_id,
            "type": "hypothesis",
        },
    )


def provenance_analyze(
    bundle: str = typer.Argument(..., help="Path to provenance bundle (.tar.gz or directory)."),
    raw: bool = typer.Option(False, "--raw", help="Show raw hexdump for each packet."),
):
    from netaudio.dante.fact_store import _load_bundle as lib_load_bundle
    from netaudio.dante.fact_store import list_facts

    bundle_path = _resolve_provenance_bundle_path(bundle)
    if not bundle_path.exists():
        typer.echo(f"Bundle not found: {bundle}", err=True)
        raise typer.Exit(1)

    manifest, files = lib_load_bundle(bundle_path)
    if not manifest:
        typer.echo(f"Empty or invalid bundle: {bundle_path}", err=True)
        raise typer.Exit(1)

    facts_path = _resolve_facts_path()
    all_facts = list_facts(facts_path) if facts_path.exists() else []
    fact_index = _index_facts(all_facts)

    samples = manifest.get("samples", [])
    scope = manifest.get("scope", {})
    session_name = manifest.get("session_name", bundle_path.stem)

    lines = [f"Bundle: {bundle_path.name}"]
    if scope.get("device_ip"):
        lines.append(f"Device: {scope.get('device_ip', '?')}  {scope.get('device_name', '')}")
    lines.extend([f"Session: {session_name}", f"Packets: {len(samples)}", ""])

    device_profile = {}
    analyzed_packets = []
    for sample in samples:
        if sample.get("direction") != "response":
            continue
        payload = files.get(sample.get("file", ""))
        if payload is None:
            continue
        analysis_lines, analyzed = _analyze_response_sample(sample, payload, fact_index, raw)
        lines.extend(analysis_lines)
        analyzed_packets.append(analyzed)
        for extracted in analyzed["fields"]:
            if extracted.get("profile_key"):
                device_profile[extracted["profile_key"]] = extracted["display"]

    if device_profile:
        lines.append("Device Profile:")
        lines.extend(f"  {profile_key:25s} = {value}" for profile_key, value in device_profile.items())
    emit_report(
        lines,
        {
            "bundle": bundle_path.name,
            "device_profile": device_profile,
            "packets": analyzed_packets,
            "path": str(bundle_path),
            "sample_count": len(samples),
            "scope": scope,
            "session_name": session_name,
        },
    )


def _index_facts(all_facts: list[dict]) -> dict[str, dict[int, dict]]:
    fact_index: dict[str, dict[int, dict]] = {"arc_opcode": {}, "conmon_message": {}}
    for fact in all_facts:
        category = fact["category"]
        if category not in fact_index:
            continue
        try:
            fact_index[category][int(fact["key"], 16)] = fact
        except ValueError:
            continue
    return fact_index


def _matching_fact(sample: dict, payload: bytes, fact_index: dict[str, dict[int, dict]]) -> dict | None:
    protocol_id = sample.get("protocol_id", 0)
    if protocol_id in (0x27FF, 0x2809, 0x1200):
        return fact_index["arc_opcode"].get(sample.get("opcode", 0))
    if protocol_id == 0xFFFF and len(payload) >= 28:
        message_type = struct.unpack(">H", payload[26:28])[0]
        return fact_index["conmon_message"].get(message_type)
    return None


def _analyze_response_sample(
    sample: dict, payload: bytes, fact_index: dict[str, dict[int, dict]], raw: bool
) -> tuple[list[str], dict]:
    opcode_value = sample.get("opcode", 0)
    opcode_hex = f"0x{opcode_value:04X}" if opcode_value else "?"
    label = _label_packet(payload)
    source = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
    destination = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"
    lines = [f"  {opcode_hex}  {label or '?':30s}  {source} -> {destination}  {len(payload)}B"]
    extracted_fields = []
    fact = _matching_fact(sample, payload, fact_index)
    if fact and fact.get("fields"):
        for field_definition in fact["fields"]:
            result = _extract_field(payload, field_definition)
            if result:
                lines.append(f"    {result['name']:25s} = {result['display']}")
                extracted_fields.append(result)
    if raw:
        lines.extend(_compact_hexdump(payload, max_lines=4))
    lines.append("")
    analyzed = {
        "destination": destination,
        "fact": fact.get("name") if fact else None,
        "fields": extracted_fields,
        "label": label,
        "opcode": opcode_hex,
        "packet_id": sample.get("packet_id"),
        "size": len(payload),
        "source": source,
    }
    return lines, analyzed
