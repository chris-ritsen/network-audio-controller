from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

import typer

from netaudio.capture.packets import _hexdump
from netaudio.capture.provenance import _decode_packet_payload, _verify_parse_header
from netaudio.commands.capture.options import _resolve_marker_window, _resolve_session_reference
from netaudio.common.app_config import settings as app_settings
from netaudio.dante.packet_store import PacketStore

AUDIT_MARKER_HEADINGS = {
    "evidence": "EVIDENCE",
    "hypothesis": "HYPOTHESIS",
    "observation": "OBSERVATION",
    "step": "STEP",
    "system": "SYSTEM",
}
AUDIT_RULE_WIDTH = 72
EVIDENCE_PACKET_PREVIEW_LIMIT = 20
VERIFY_MARKER_PREFIXES = {
    "analysis": "A",
    "evidence": "E",
    "hypothesis": "H",
    "observation": "O",
    "step": ">",
    "system": "S",
}


@dataclass
class BundleReport:
    bundle_path: Path
    data: dict
    lines: list[str] = field(default_factory=list)
    passed: bool = False


@dataclass
class BundleIntegritySummary:
    artifact_count: int
    evidence_marker_count: int
    hypothesis_count: int
    integrity_failures: list[str]
    observation_count: int
    sample_count: int
    verified_artifact_count: int
    verified_sample_count: int

    @property
    def complete(self) -> bool:
        return self.verified_sample_count == self.sample_count and self.verified_artifact_count == self.artifact_count

    def as_dict(self) -> dict:
        return {
            "artifacts": self.artifact_count,
            "evidence_queries": self.evidence_marker_count,
            "hypotheses": self.hypothesis_count,
            "integrity_failures": list(self.integrity_failures),
            "observations": self.observation_count,
            "packets": self.sample_count,
            "verified_artifacts": self.verified_artifact_count,
            "verified_packets": self.verified_sample_count,
        }


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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


def _audit_rule() -> str:
    return ("-" if app_settings.no_color else "─") * AUDIT_RULE_WIDTH


def _load_bundle_report(bundle_path: Path) -> tuple[dict, dict[str, bytes]] | BundleReport:
    try:
        return _load_bundle(bundle_path)
    except (OSError, ValueError, json.JSONDecodeError, typer.Exit) as exception:
        return BundleReport(
            bundle_path=bundle_path,
            data={"bundle": bundle_path.name, "error": str(exception), "passed": False, "path": str(bundle_path)},
            lines=[f"FAIL: {exception}"],
        )


def _sample_verification(sample: dict, files: dict[str, bytes]) -> tuple[list[str], dict]:
    filename = sample["file"]
    data = files.get(filename)
    record = {"direction": sample.get("direction", "?"), "file": filename, "status": "ok"}
    if data is None:
        record["status"] = "missing"
        return [f"    MISSING: {filename}"], record
    if len(data) == 0:
        record["status"] = "empty"
        return [f"    EMPTY: {filename}"], record
    integrity_error = _bundle_entry_integrity_error(sample, data)
    if integrity_error:
        record.update(detail=integrity_error, status="integrity")
        return [f"    INTEGRITY: {filename}: {integrity_error}"], record
    header = _verify_parse_header(data)
    if header is None:
        record.update(size=len(data), status="unparseable")
        return [f"    UNPARSEABLE: {filename} ({len(data)} bytes)"], record
    expected_protocol = sample.get("protocol_id")
    expected_opcode = sample.get("opcode")
    protocol_match = expected_protocol is None or header["protocol_id"] == expected_protocol
    opcode_match = expected_opcode is None or header["opcode"] == expected_opcode
    if not protocol_match or not opcode_match:
        lines = [f"    MISMATCH: {filename}"]
        if not protocol_match:
            lines.append(f"      expected protocol 0x{expected_protocol:04X}, got 0x{header['protocol_id']:04X}")
        if not opcode_match:
            lines.append(f"      expected opcode 0x{expected_opcode:04X}, got 0x{header['opcode']:04X}")
        record.update(observed_opcode=header["opcode"], observed_protocol_id=header["protocol_id"], status="mismatch")
        return lines, record
    status_text = ""
    if header.get("status") is not None:
        status_text = f" status=0x{header['status']:04X}"
    opcode_name = header.get("opcode_name", f"0x{header['opcode']:04X}")
    record.update(
        opcode=header["opcode"],
        opcode_name=opcode_name,
        protocol_name=header["protocol_name"],
        size=len(data),
        status_code=header.get("status"),
    )
    line = f"    OK: {filename} [{record['direction']}] {header['protocol_name']} {opcode_name}{status_text} ({len(data)}B)"
    return [line], record


def _artifact_verification(artifact: dict, files: dict[str, bytes]) -> tuple[list[str], dict]:
    filename = artifact.get("file", "")
    data = files.get(filename)
    record = {"file": filename, "role": artifact.get("role", "?"), "sha256": artifact.get("sha256"), "status": "ok"}
    if data is None:
        record["status"] = "missing"
        return [f"    MISSING: {filename}"], record
    integrity_error = _bundle_entry_integrity_error(artifact, data)
    if integrity_error:
        record.update(detail=integrity_error, status="integrity")
        return [f"    INTEGRITY: {filename}: {integrity_error}"], record
    record["size"] = len(data)
    line = f"    OK: {filename} [{artifact.get('role', '?')}] sha256={artifact.get('sha256')} ({len(data)}B)"
    return [line], record


def _verify_marker_lines(markers: list[dict]) -> list[str]:
    if not markers:
        return []
    lines = [f"\n  Markers ({len(markers)}):"]
    for marker_item in markers:
        marker_type = marker_item.get("marker_type", "?")
        prefix = VERIFY_MARKER_PREFIXES.get(marker_type, "?")
        line = f"    [{prefix}] {marker_item.get('label', '?')}"
        note = marker_item.get("note", "")
        if note:
            line += f" — {note}"
        lines.append(line)
        data = marker_item.get("data")
        if data:
            lines.append(f"        data: {json.dumps(data, default=str)}")
    return lines


def verify_bundle(bundle_path: Path) -> BundleReport:
    loaded = _load_bundle_report(bundle_path)
    if isinstance(loaded, BundleReport):
        return loaded
    manifest, files = loaded
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])
    lines = [
        f"Bundle: {bundle_path.name}",
        f"  Session: {manifest.get('session_name', 'unknown')} (id={manifest.get('session_id')})",
        f"  Scope: {json.dumps(manifest.get('scope', {}))}",
        f"  Packet count: {manifest.get('count', 0)}",
    ]
    lines.extend(_verify_marker_lines(markers))
    lines.append(f"\n  Packets ({len(samples)}):")
    if not samples:
        lines.append("    MISSING: bundle contains no packet samples")
    sample_records = []
    for sample in samples:
        sample_lines, record = _sample_verification(sample, files)
        lines.extend(sample_lines)
        sample_records.append(record)
    artifact_records = []
    if artifacts:
        lines.append(f"\n  Artifacts ({len(artifacts)}):")
    for artifact in artifacts:
        artifact_lines, record = _artifact_verification(artifact, files)
        lines.extend(artifact_lines)
        artifact_records.append(record)
    verified_count = sum(1 for record in sample_records if record["status"] == "ok")
    verified_artifact_count = sum(1 for record in artifact_records if record["status"] == "ok")
    hypotheses = [marker for marker in markers if marker.get("marker_type") == "hypothesis"]
    observations = [marker for marker in markers if marker.get("marker_type") == "observation"]
    lines.append("\n  Verification Summary:")
    lines.append(f"    Packets verified: {verified_count}/{len(samples)}")
    lines.append(f"    Artifacts verified: {verified_artifact_count}/{len(artifacts)}")
    lines.append(f"    Hypotheses: {len(hypotheses)}")
    lines.append(f"    Observations: {len(observations)}")
    passed = bool(samples) and verified_count == len(samples) and verified_artifact_count == len(artifacts)
    if passed:
        lines.append(
            f"\n  RESULT: PASS — all {verified_count} packets verified, {len(observations)} observations recorded"
        )
    else:
        lines.append(f"\n  RESULT: FAIL — {len(samples) - verified_count} packet(s) could not be verified")
    data = {
        "artifacts": artifact_records,
        "bundle": bundle_path.name,
        "hypotheses": len(hypotheses),
        "markers": markers,
        "observations": len(observations),
        "packet_count": manifest.get("count", 0),
        "packets": sample_records,
        "passed": passed,
        "path": str(bundle_path),
        "scope": manifest.get("scope", {}),
        "session_id": manifest.get("session_id"),
        "session_name": manifest.get("session_name", "unknown"),
        "verified_artifacts": verified_artifact_count,
        "verified_packets": verified_count,
    }
    return BundleReport(bundle_path=bundle_path, data=data, lines=lines, passed=passed)


def _verify_single_bundle(bundle_path: Path) -> bool:
    report = verify_bundle(bundle_path)
    for line in report.lines:
        print(line)
    return report.passed


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


def _audit_header_lines(manifest: dict) -> list[str]:
    scope = manifest.get("scope", {})
    session_count = manifest.get("session_packet_count", manifest.get("count", 0))
    evidence_count = manifest.get("evidence_packet_count", 0)
    artifacts = manifest.get("artifacts", [])
    return [
        f"{'=' * AUDIT_RULE_WIDTH}",
        f"PROVENANCE AUDIT: {manifest.get('session_name', 'unknown')}",
        f"{'=' * AUDIT_RULE_WIDTH}",
        f"  Session ID:      {manifest.get('session_id')}",
        f"  Target Device:   {scope.get('device_name', '?')} ({scope.get('device_ip', '?')})",
        f"  Session Packets: {session_count}",
        f"  Evidence Packets: {evidence_count}",
        f"  Evidence Artifacts: {len(artifacts)}",
    ]


def _audit_timeline_events(manifest: dict) -> list[dict]:
    events = []
    for marker_item in manifest.get("markers", []):
        events.append({"type": "marker", "timestamp_ns": marker_item.get("timestamp_ns", 0), "data": marker_item})
    for sample in manifest.get("samples", []):
        events.append({"type": "packet", "timestamp_ns": sample.get("timestamp_ns", 0), "data": sample})
    events.sort(key=lambda event: event["timestamp_ns"])
    return events


def _audit_evidence_lines(marker_data: dict, samples: list[dict], files: dict[str, bytes]) -> list[str]:
    lines = []
    query = marker_data.get("query", {})
    active_filters = {key: value for key, value in query.items() if value is not None}
    if active_filters:
        lines.append(f"    Query: {json.dumps(active_filters)}")
    filters = marker_data.get("filters", {})
    if filters:
        filter_parts = [f"{key}={value}" for key, value in filters.items()]
        lines.append(f"    Filters: {', '.join(filter_parts)}")
    evidence_packet_ids = marker_data.get("packet_ids", [])
    if not evidence_packet_ids:
        lines.append(f"    Matched: {marker_data.get('packet_count', 0)} packets")
        return lines
    lines.append(f"    Packets: {len(evidence_packet_ids)}")
    sample_lookup = {sample.get("packet_id"): sample for sample in samples}
    for packet_id in evidence_packet_ids[:EVIDENCE_PACKET_PREVIEW_LIMIT]:
        sample_entry = sample_lookup.get(packet_id)
        if not sample_entry:
            continue
        direction = sample_entry.get("direction") or "multicast"
        opcode_hex = sample_entry.get("opcode_hex", "?")
        source = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
        destination = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
        lines.append(f"      #{packet_id} {direction:9s} {opcode_hex} {source} -> {destination}")
        payload = files.get(sample_entry.get("file", ""))
        if payload:
            lines.append(_hexdump(payload, indent="        "))
    if len(evidence_packet_ids) > EVIDENCE_PACKET_PREVIEW_LIMIT:
        lines.append(f"      ... and {len(evidence_packet_ids) - EVIDENCE_PACKET_PREVIEW_LIMIT} more")
    return lines


def _audit_marker_lines(marker_item: dict, samples: list[dict], files: dict[str, bytes]) -> list[str]:
    timestamp_iso = marker_item.get("timestamp_iso", "")
    marker_type = marker_item.get("marker_type", "?")
    label = marker_item.get("label", "?")
    note = marker_item.get("note", "")
    heading = AUDIT_MARKER_HEADINGS.get(marker_type, marker_type.upper())
    if marker_type == "system":
        return [f"\n  [{timestamp_iso}] {heading}: {note}"]
    lines = [f"\n  [{timestamp_iso}] {heading}: {label}"]
    if note:
        lines.append(f"    {note}")
    marker_data = marker_item.get("data")
    if not marker_data:
        return lines
    if marker_type == "hypothesis":
        lines.extend(f"    {key}: {value}" for key, value in marker_data.items())
    elif marker_type == "observation":
        lines.extend(f"    {key}: {value}" for key, value in marker_data.items() if key != "response_hex")
    elif marker_type == "evidence":
        lines.extend(_audit_evidence_lines(marker_data, samples, files))
    return lines


def _audit_packet_lines(sample: dict, files: dict[str, bytes]) -> list[str]:
    timestamp_iso = sample.get("timestamp_iso", "")
    direction = sample.get("direction", "?")
    is_evidence = sample.get("evidence", False)
    source_label = "EVIDENCE PACKET" if is_evidence else "PACKET"
    source_session = f" (from session {sample['session_id']})" if is_evidence and sample.get("session_id") else ""
    endpoints = ""
    if sample.get("src_ip") and sample.get("dst_ip"):
        arrow = "->" if app_settings.no_color else "→"
        endpoints = f" {sample['src_ip']}:{sample.get('src_port', '?')} {arrow} {sample['dst_ip']}:{sample.get('dst_port', '?')}"
    lines = [f"\n  [{timestamp_iso}] {source_label} [{direction}]{endpoints}{source_session}"]
    payload = files.get(sample.get("file", "?"))
    if payload:
        lines.append(_format_audit_packet(payload, direction=direction))
    return lines


def _audit_timeline_data(events: list[dict], files: dict[str, bytes]) -> list[dict]:
    timeline = []
    for event in events:
        if event["type"] == "marker":
            timeline.append({"event": "marker", **event["data"]})
            continue
        sample = event["data"]
        payload = files.get(sample.get("file", "?"))
        entry = {"event": "packet", **sample}
        if payload:
            entry["decoded"] = _decode_packet_payload(payload)
        timeline.append(entry)
    return timeline


def _audit_integrity(manifest: dict, files: dict[str, bytes]) -> BundleIntegritySummary:
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])
    integrity_failures = []
    verified_count = 0
    for sample in samples:
        data = files.get(sample["file"])
        if not data:
            continue
        integrity_error = _bundle_entry_integrity_error(sample, data)
        if integrity_error:
            integrity_failures.append(f"{sample['file']}: {integrity_error}")
        else:
            verified_count += 1
    verified_artifact_count = 0
    for artifact in artifacts:
        data = files.get(artifact.get("file", ""))
        if not data:
            continue
        integrity_error = _bundle_entry_integrity_error(artifact, data)
        if integrity_error:
            integrity_failures.append(f"{artifact.get('file', '?')}: {integrity_error}")
        else:
            verified_artifact_count += 1
    return BundleIntegritySummary(
        artifact_count=len(artifacts),
        evidence_marker_count=sum(1 for marker in markers if marker.get("marker_type") == "evidence"),
        hypothesis_count=sum(1 for marker in markers if marker.get("marker_type") == "hypothesis"),
        integrity_failures=integrity_failures,
        observation_count=sum(1 for marker in markers if marker.get("marker_type") == "observation"),
        sample_count=len(samples),
        verified_artifact_count=verified_artifact_count,
        verified_sample_count=verified_count,
    )


def _audit_summary_lines(summary: BundleIntegritySummary) -> tuple[list[str], bool]:
    rule = _audit_rule()
    lines = [
        f"\n{rule}",
        "VERIFICATION SUMMARY",
        rule,
        f"  Packets present:  {summary.verified_sample_count}/{summary.sample_count}",
        f"  Artifacts present: {summary.verified_artifact_count}/{summary.artifact_count}",
        f"  Hypotheses:       {summary.hypothesis_count}",
        f"  Observations:     {summary.observation_count}",
        f"  Evidence queries: {summary.evidence_marker_count}",
    ]
    if summary.sample_count == 0:
        lines.append("\n  RESULT: FAIL — bundle contains no packet samples")
        return lines, False
    lines.extend(f"  Integrity failure: {failure}" for failure in summary.integrity_failures)
    if summary.complete:
        lines.append("\n  RESULT: PASS")
        return lines, True
    lines.append(f"\n  RESULT: FAIL — {summary.sample_count - summary.verified_sample_count} packet(s) missing")
    return lines, False


def audit_bundle(bundle_path: Path) -> BundleReport:
    loaded = _load_bundle_report(bundle_path)
    if isinstance(loaded, BundleReport):
        return loaded
    manifest, files = loaded
    samples = manifest.get("samples", [])
    events = _audit_timeline_events(manifest)
    rule = _audit_rule()
    lines = _audit_header_lines(manifest)
    lines.extend([f"\n{rule}", "TIMELINE", rule])
    for event in events:
        if event["type"] == "marker":
            lines.extend(_audit_marker_lines(event["data"], samples, files))
        else:
            lines.extend(_audit_packet_lines(event["data"], files))
    summary = _audit_integrity(manifest, files)
    summary_lines, passed = _audit_summary_lines(summary)
    lines.extend(summary_lines)
    scope = manifest.get("scope", {})
    data = {
        "bundle": bundle_path.name,
        "device": {"ip": scope.get("device_ip"), "name": scope.get("device_name")},
        "evidence_packets": manifest.get("evidence_packet_count", 0),
        "passed": passed,
        "path": str(bundle_path),
        "session_id": manifest.get("session_id"),
        "session_name": manifest.get("session_name", "unknown"),
        "session_packets": manifest.get("session_packet_count", manifest.get("count", 0)),
        "summary": summary.as_dict(),
        "timeline": _audit_timeline_data(events, files),
    }
    return BundleReport(bundle_path=bundle_path, data=data, lines=lines, passed=passed)


def _audit_single_bundle(bundle_path: Path) -> bool:
    report = audit_bundle(bundle_path)
    for line in report.lines:
        print(line)
    return report.passed
