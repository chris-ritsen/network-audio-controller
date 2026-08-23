from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import typer

from netaudio.capture.provenance import _decode_packet_payload, _verify_parse_header
from netaudio.commands.capture_helpers import (
    _hexdump,
    _resolve_marker_window,
    _resolve_session_reference,
    _valid_label,
)
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.packet_store import PacketStore


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _interactive_label_opcodes(
    rows: list[sqlite3.Row],
    opcode_labels: dict[tuple[int, int], str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        protocol_id = int(row["protocol_id"])
        opcode = int(row["opcode"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        key = (protocol_id, opcode)
        if key in opcode_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] protocol=0x{protocol_id:04X} opcode=0x{opcode:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        opcode_labels[key] = label
        changed = True
    return changed


def _interactive_label_messages(
    rows: list[sqlite3.Row],
    message_labels: dict[int, str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        message_type = int(row["message_type"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if message_type in message_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] message_type=0x{message_type:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        message_labels[message_type] = label
        changed = True
    return changed


def _interactive_label_statuses(
    rows: list[dict[str, int]],
    status_labels: dict[int, dict[str, object]],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        status_code = int(row["status_code"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if status_code in status_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] subscription_status=0x{status_code:04X} ({status_code}) "
            f"seen={seen} sample_id={sample_id}\n"
            "state,label (blank=skip, q=quit): "
        )
        value = input(prompt).strip()
        if not value:
            continue
        if value.lower() in {"q", "quit", "exit"}:
            return changed

        if "," in value:
            state, label = value.split(",", 1)
            state = state.strip() or "unknown"
            label = label.strip()
        else:
            state = "unknown"
            label = value

        if not _valid_label(label):
            print("invalid status label, skipping")
            continue

        status_labels[status_code] = {
            "state": state,
            "label": label,
            "detail": None,
            "labels": [label],
        }
        changed = True
    return changed


def _resolve_provenance_scope(
    store: PacketStore,
    *,
    id: int | None,
    session: str | None,
    from_label: str | None,
    to_label: str | None,
) -> tuple[int | None, int | None, int | None]:
    if id is None and not session and not from_label and not to_label:
        return None, None, None

    resolved_session_id, _ = _resolve_session_reference(
        store,
        session_id=id,
        session=session,
        default_selector="latest",
    )
    start_ns, end_ns = _resolve_marker_window(
        store,
        session_id=resolved_session_id,
        from_label=from_label,
        to_label=to_label,
    )
    return resolved_session_id, start_ns, end_ns


def _verify_single_bundle(bundle_path: Path) -> bool:
    try:
        manifest, files = _load_bundle(bundle_path)
    except (OSError, ValueError, json.JSONDecodeError, typer.Exit) as exception:
        print(f"FAIL: {exception}")
        return False

    print(f"Bundle: {bundle_path.name}")
    print(f"  Session: {manifest.get('session_name', 'unknown')} (id={manifest.get('session_id')})")
    print(f"  Scope: {json.dumps(manifest.get('scope', {}))}")
    print(f"  Packet count: {manifest.get('count', 0)}")

    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])

    if markers:
        print(f"\n  Markers ({len(markers)}):")
        for marker_item in markers:
            marker_type = marker_item.get("marker_type", "?")
            label = marker_item.get("label", "?")
            note = marker_item.get("note", "")
            data = marker_item.get("data")
            prefix = {
                "analysis": "A",
                "evidence": "E",
                "hypothesis": "H",
                "observation": "O",
                "system": "S",
                "step": ">",
            }.get(marker_type, "?")
            line = f"    [{prefix}] {label}"
            if note:
                line += f" — {note}"
            print(line)
            if data:
                print(f"        data: {json.dumps(data, default=str)}")

    all_ok = True
    verified_count = 0

    print(f"\n  Packets ({len(samples)}):")
    if not samples:
        print("    MISSING: bundle contains no packet samples")
        all_ok = False
    for sample in samples:
        filename = sample["file"]
        direction = sample.get("direction", "?")
        expected_protocol = sample.get("protocol_id")
        expected_opcode = sample.get("opcode")

        data = files.get(filename)
        if data is None:
            print(f"    MISSING: {filename}")
            all_ok = False
            continue

        if len(data) == 0:
            print(f"    EMPTY: {filename}")
            all_ok = False
            continue

        integrity_error = _bundle_entry_integrity_error(sample, data)
        if integrity_error:
            print(f"    INTEGRITY: {filename}: {integrity_error}")
            all_ok = False
            continue

        header = _verify_parse_header(data)
        if header is None:
            print(f"    UNPARSEABLE: {filename} ({len(data)} bytes)")
            all_ok = False
            continue

        protocol_match = expected_protocol is None or header["protocol_id"] == expected_protocol
        opcode_match = expected_opcode is None or header["opcode"] == expected_opcode

        if not protocol_match or not opcode_match:
            print(f"    MISMATCH: {filename}")
            if not protocol_match:
                print(f"      expected protocol 0x{expected_protocol:04X}, got 0x{header['protocol_id']:04X}")
            if not opcode_match:
                print(f"      expected opcode 0x{expected_opcode:04X}, got 0x{header['opcode']:04X}")
            all_ok = False
            continue

        status_str = ""
        if header.get("status") is not None:
            status_str = f" status=0x{header['status']:04X}"

        opcode_name = header.get("opcode_name", f"0x{header['opcode']:04X}")
        print(f"    OK: {filename} [{direction}] {header['protocol_name']} {opcode_name}{status_str} ({len(data)}B)")
        verified_count += 1

    verified_artifact_count = 0
    if artifacts:
        print(f"\n  Artifacts ({len(artifacts)}):")
    for artifact in artifacts:
        filename = artifact.get("file", "")
        data = files.get(filename)
        if data is None:
            print(f"    MISSING: {filename}")
            all_ok = False
            continue
        integrity_error = _bundle_entry_integrity_error(artifact, data)
        if integrity_error:
            print(f"    INTEGRITY: {filename}: {integrity_error}")
            all_ok = False
            continue
        print(f"    OK: {filename} [{artifact.get('role', '?')}] sha256={artifact.get('sha256')} ({len(data)}B)")
        verified_artifact_count += 1

    hypotheses = [m for m in markers if m.get("marker_type") == "hypothesis"]
    observations = [m for m in markers if m.get("marker_type") == "observation"]

    print(f"\n  Verification Summary:")
    print(f"    Packets verified: {verified_count}/{len(samples)}")
    print(f"    Artifacts verified: {verified_artifact_count}/{len(artifacts)}")
    print(f"    Hypotheses: {len(hypotheses)}")
    print(f"    Observations: {len(observations)}")

    if all_ok and verified_count == len(samples) and verified_artifact_count == len(artifacts):
        print(f"\n  RESULT: PASS — all {verified_count} packets verified, {len(observations)} observations recorded")
        return True

    print(f"\n  RESULT: FAIL — {len(samples) - verified_count} packet(s) could not be verified")
    return False


def _format_audit_packet(data: bytes, indent: str = "      ", direction: str | None = None) -> str:
    decoded = _decode_packet_payload(data)
    lines = []
    lines.append(f"{indent}Protocol: {decoded.get('protocol_name', '?')} ({decoded.get('protocol', '?')})")

    if "opcode" in decoded:
        lines.append(f"{indent}Opcode:   {decoded.get('opcode_name', '?')} ({decoded['opcode']})")

    if "transaction_id" in decoded:
        lines.append(f"{indent}TxnID:    {decoded['transaction_id']}")

    if "status" in decoded:
        if direction == "request":
            lines.append(f"{indent}Operand:  {decoded['status']} (request field)")
        else:
            ok = "OK" if decoded.get("status_ok") else "FAIL"
            lines.append(f"{indent}Status:   {decoded['status']} ({ok})")

    lines.append(f"{indent}Length:   {decoded.get('actual_length', '?')} bytes")
    lines.append(f"{indent}Hex:      {decoded.get('raw_hex', '')}")

    if decoded.get("words"):
        lines.append(f"{indent}Words:")
        for word in decoded["words"]:
            annotation = ""
            if word["u32"] > 0 and word["u32"] <= 10_000_000:
                us = word["u32"] / 1000
                ms = word["u32"] / 1_000_000
                if us == int(us) and 1 <= us <= 10000:
                    annotation = f"  ({us:.0f} us = {ms} ms)"
            lines.append(f"{indent}  [{word['offset']:3d}] 0x{word['hex']} = {word['u32']}{annotation}")

    return "\n".join(lines)


def _bundle_entry_integrity_error(entry: dict, data: bytes) -> str | None:
    expected_size = entry.get("size")
    if expected_size is not None and int(expected_size) != len(data):
        return f"expected {expected_size} bytes, found {len(data)}"

    expected_sha256 = entry.get("sha256")
    if expected_sha256 is not None:
        actual_sha256 = hashlib.sha256(data).hexdigest()
        if actual_sha256 != expected_sha256:
            return f"expected sha256 {expected_sha256}, found {actual_sha256}"
    return None


def _load_bundle(bundle_path: Path) -> tuple[dict, dict[str, bytes]]:
    import tarfile
    import zipfile

    if bundle_path.suffix == ".gz" and bundle_path.stem.endswith(".tar"):
        with tarfile.open(bundle_path, "r:gz") as tar:
            manifest = None
            files = {}
            for member in tar.getmembers():
                name = Path(member.name).name
                data = tar.extractfile(member)
                if data is None:
                    continue
                content = data.read()
                if name == "manifest.json":
                    manifest = json.loads(content)
                else:
                    files[name] = content
            if manifest is None:
                raise typer.Exit(f"No manifest.json in {bundle_path}")
            return manifest, files

    if bundle_path.suffix == ".zip":
        with zipfile.ZipFile(bundle_path, "r") as archive:
            manifest = None
            files = {}
            for member_name in archive.namelist():
                name = Path(member_name).name
                if not name:
                    continue
                content = archive.read(member_name)
                if name == "manifest.json":
                    manifest = json.loads(content)
                else:
                    files[name] = content
            if manifest is None:
                raise typer.Exit(f"No manifest.json in {bundle_path}")
            return manifest, files

    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.exists():
        raise typer.Exit(f"No manifest.json in {bundle_path}")

    with open(manifest_path) as manifest_file:
        manifest = json.load(manifest_file)

    files = {}
    entries = list(manifest.get("samples", [])) + list(manifest.get("artifacts", []))
    for entry in entries:
        filename = entry.get("file")
        if not isinstance(filename, str) or not filename or Path(filename).name != filename:
            raise typer.Exit(f"Invalid bundle filename: {filename!r}")
        entry_path = bundle_path / filename
        if entry_path.is_file():
            files[filename] = entry_path.read_bytes()

    return manifest, files


def _audit_single_bundle(bundle_path: Path) -> bool:
    try:
        manifest, files = _load_bundle(bundle_path)
    except (OSError, ValueError, json.JSONDecodeError, typer.Exit) as exception:
        print(f"FAIL: {exception}")
        return False

    scope = manifest.get("scope", {})
    session_count = manifest.get("session_packet_count", manifest.get("count", 0))
    evidence_count = manifest.get("evidence_packet_count", 0)
    artifacts = manifest.get("artifacts", [])

    print(f"{'=' * 72}")
    print(f"PROVENANCE AUDIT: {manifest.get('session_name', 'unknown')}")
    print(f"{'=' * 72}")
    print(f"  Session ID:      {manifest.get('session_id')}")
    print(f"  Target Device:   {scope.get('device_name', '?')} ({scope.get('device_ip', '?')})")
    print(f"  Session Packets: {session_count}")
    print(f"  Evidence Packets: {evidence_count}")
    print(f"  Evidence Artifacts: {len(artifacts)}")

    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])

    marker_type_icons = {
        "hypothesis": "HYPOTHESIS",
        "observation": "OBSERVATION",
        "step": "STEP",
        "evidence": "EVIDENCE",
        "system": "SYSTEM",
    }

    rule = ("-" if app_settings.no_color else "─") * 72
    print(f"\n{rule}")
    print("TIMELINE")
    print(rule)

    all_events = []

    for marker_item in markers:
        all_events.append(
            {
                "type": "marker",
                "timestamp_ns": marker_item.get("timestamp_ns", 0),
                "data": marker_item,
            }
        )

    for sample in samples:
        all_events.append(
            {
                "type": "packet",
                "timestamp_ns": sample.get("timestamp_ns", 0),
                "data": sample,
            }
        )

    all_events.sort(key=lambda event: event["timestamp_ns"])

    for event in all_events:
        ts_iso = ""
        if event["type"] == "marker":
            marker_item = event["data"]
            ts_iso = marker_item.get("timestamp_iso", "")
            marker_type = marker_item.get("marker_type", "?")
            label = marker_item.get("label", "?")
            note = marker_item.get("note", "")
            icon = marker_type_icons.get(marker_type, marker_type.upper())

            if marker_type == "system":
                print(f"\n  [{ts_iso}] {icon}: {note}")
                continue

            print(f"\n  [{ts_iso}] {icon}: {label}")
            if note:
                print(f"    {note}")

            marker_data = marker_item.get("data")
            if marker_data:
                if marker_type == "hypothesis":
                    for key, value in marker_data.items():
                        print(f"    {key}: {value}")
                elif marker_type == "observation":
                    for key, value in marker_data.items():
                        if key == "response_hex":
                            continue
                        print(f"    {key}: {value}")
                elif marker_type == "evidence":
                    query = marker_data.get("query", {})
                    active_filters = {k: v for k, v in query.items() if v is not None}
                    if active_filters:
                        print(f"    Query: {json.dumps(active_filters)}")
                    filters = marker_data.get("filters", {})
                    if filters:
                        filter_parts = [f"{k}={v}" for k, v in filters.items()]
                        print(f"    Filters: {', '.join(filter_parts)}")
                    evidence_pids = marker_data.get("packet_ids", [])
                    if evidence_pids:
                        print(f"    Packets: {len(evidence_pids)}")
                        sample_lookup = {s.get("packet_id"): s for s in samples}
                        for pid in evidence_pids[:20]:
                            sample_entry = sample_lookup.get(pid)
                            if sample_entry:
                                direction = sample_entry.get("direction") or "multicast"
                                opcode_hex = sample_entry.get("opcode_hex", "?")
                                src = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
                                dst = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
                                filename = sample_entry.get("file", "")
                                print(f"      #{pid} {direction:9s} {opcode_hex} {src} -> {dst}")
                                payload = files.get(filename)
                                if payload:
                                    print(_hexdump(payload, indent="        "))
                        if len(evidence_pids) > 20:
                            print(f"      ... and {len(evidence_pids) - 20} more")
                    else:
                        print(f"    Matched: {marker_data.get('packet_count', 0)} packets")

        elif event["type"] == "packet":
            sample = event["data"]
            ts_iso = sample.get("timestamp_iso", "")
            direction = sample.get("direction", "?")
            filename = sample.get("file", "?")
            is_evidence = sample.get("evidence", False)
            source_label = "EVIDENCE PACKET" if is_evidence else "PACKET"
            source_session = (
                f" (from session {sample['session_id']})" if is_evidence and sample.get("session_id") else ""
            )

            src_dst = ""
            if sample.get("src_ip") and sample.get("dst_ip"):
                arrow = "->" if app_settings.no_color else "\u2192"
                src_dst = f" {sample['src_ip']}:{sample.get('src_port', '?')} {arrow} {sample['dst_ip']}:{sample.get('dst_port', '?')}"

            print(f"\n  [{ts_iso}] {source_label} [{direction}]{src_dst}{source_session}")

            payload = files.get(filename)
            if payload:
                print(_format_audit_packet(payload, direction=direction))

    rule = ("-" if app_settings.no_color else "─") * 72
    print(f"\n{rule}")
    print("VERIFICATION SUMMARY")
    print(rule)

    hypotheses = [m for m in markers if m.get("marker_type") == "hypothesis"]
    observations = [m for m in markers if m.get("marker_type") == "observation"]
    evidence_markers = [m for m in markers if m.get("marker_type") == "evidence"]

    verified_count = 0
    integrity_failures = []
    for sample in samples:
        data = files.get(sample["file"])
        if data and not _bundle_entry_integrity_error(sample, data):
            verified_count += 1
        elif data:
            integrity_failures.append(f"{sample['file']}: {_bundle_entry_integrity_error(sample, data)}")

    verified_artifact_count = 0
    for artifact in artifacts:
        data = files.get(artifact.get("file", ""))
        if data and not _bundle_entry_integrity_error(artifact, data):
            verified_artifact_count += 1
        elif data:
            integrity_failures.append(f"{artifact.get('file', '?')}: {_bundle_entry_integrity_error(artifact, data)}")

    print(f"  Packets present:  {verified_count}/{len(samples)}")
    print(f"  Artifacts present: {verified_artifact_count}/{len(artifacts)}")
    print(f"  Hypotheses:       {len(hypotheses)}")
    print(f"  Observations:     {len(observations)}")
    print(f"  Evidence queries: {len(evidence_markers)}")

    if not samples:
        print("\n  RESULT: FAIL — bundle contains no packet samples")
        return False

    for failure in integrity_failures:
        print(f"  Integrity failure: {failure}")

    if verified_count == len(samples) and verified_artifact_count == len(artifacts):
        print(f"\n  RESULT: PASS")
        return True

    print(f"\n  RESULT: FAIL — {len(samples) - verified_count} packet(s) missing")
    return False
