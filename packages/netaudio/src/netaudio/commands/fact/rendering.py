from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from netaudio.icons import icon

CONFIDENCE_MARKERS = {"disproved": "✗", "inferred": "~", "observed": "○", "uncertain": "?", "verified": "+"}
CHECK_CONFIDENCE_MARKERS = {**CONFIDENCE_MARKERS, "disproved": "x"}


@dataclass
class EvidenceLookup:
    bundle_path: Path | None = None
    destination: str = ""
    destination_port: int | None = None
    direction: str = "multicast"
    error: str | None = None
    filename: str = ""
    manifest: dict = field(default_factory=dict)
    opcode: str = "?"
    packet_id: int | None = None
    payload: bytes | None = None
    reference: str = ""
    sample_found: bool = False
    session_reference: str | None = None
    source: str = ""

    @property
    def session_level(self) -> bool:
        return self.bundle_path is not None and self.packet_id is None

    def data(self) -> dict[str, Any]:
        session = self.manifest.get("session") or {}
        return {
            "bundle": self.bundle_path.name if self.bundle_path else None,
            "destination": self.destination or None,
            "direction": self.direction if self.sample_found else None,
            "error": self.error,
            "opcode": self.opcode if self.sample_found else None,
            "packet_id": self.packet_id,
            "payload_hex": self.payload.hex() if self.payload is not None else None,
            "reference": self.reference,
            "session": {"id": session.get("id"), "name": session.get("name"), "started": session.get("started")}
            if session
            else None,
            "session_reference": self.session_reference,
            "size": len(self.payload) if self.payload is not None else None,
            "source": self.source or None,
        }


def lookup_evidence(provenance_dir: Path, reference: str) -> EvidenceLookup:
    from netaudio.dante.fact_store import _find_bundle, _load_bundle, _parse_evidence_ref

    session_reference, packet_id_string = _parse_evidence_ref(reference)
    if session_reference is None:
        return EvidenceLookup(error=f"invalid evidence ref: {reference}", reference=reference)
    bundle_path = _find_bundle(provenance_dir, session_reference)
    if bundle_path is None:
        return EvidenceLookup(
            error=f"bundle not found: {session_reference}", reference=reference, session_reference=session_reference
        )
    manifest, files = _load_bundle(bundle_path)
    lookup = EvidenceLookup(
        bundle_path=bundle_path, manifest=manifest, reference=reference, session_reference=session_reference
    )
    if packet_id_string is None:
        return lookup
    lookup.packet_id = int(packet_id_string)
    samples_by_packet_id = {sample.get("packet_id"): sample for sample in manifest.get("samples", [])}
    sample = samples_by_packet_id.get(lookup.packet_id)
    if sample is None:
        lookup.error = f"packet #{lookup.packet_id} not in bundle"
        return lookup
    lookup.sample_found = True
    lookup.direction = sample.get("direction") or "multicast"
    opcode_value = sample.get("opcode")
    lookup.opcode = f"0x{opcode_value:04X}" if isinstance(opcode_value, int) else str(opcode_value or "?")
    lookup.source = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
    lookup.destination = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"
    lookup.destination_port = sample.get("dst_port")
    lookup.filename = sample.get("file", "")
    lookup.payload = files.get(lookup.filename)
    if lookup.payload is None:
        lookup.error = f"payload file missing: {lookup.filename}"
    return lookup


def field_value_suffix(field_definition: dict) -> str:
    return f" = {field_definition['value']}" if "value" in field_definition else ""


def field_direction_prefix(field_definition: dict, template: str) -> str:
    return template.format(field_definition["direction"]) if field_definition.get("direction") else ""


def field_verification_results(fact: dict, payload: bytes, direction: str) -> list[dict]:
    from netaudio.dante.fact_store import _field_applies_to_direction, _verify_field

    return [
        _verify_field(payload, field_definition)
        for field_definition in fact.get("fields", [])
        if _field_applies_to_direction(field_definition, direction)
    ]


def field_verification_lines(results: list[dict], indent: str) -> list[str]:
    lines = []
    for result in results:
        if result["ok"]:
            lines.append(f"{indent}[PASS] {result['name']}: {result['expected']} == {result['actual']}")
        else:
            lines.append(f"{indent}[FAIL] {result['error']}")
    return lines


def fact_written_lines(
    heading: str, category: str, key: str, name: str, confidence: str, note: str | None, body: str | None
) -> list[str]:
    lines = [f"{icon('info')}{heading}: {category}:{key} = {name}", f"  Confidence: {confidence}"]
    if note:
        lines.append(f"  Note: {note}")
    if body:
        body_lines = body.splitlines()
        if len(body_lines) <= 3:
            lines.extend(f"  {line}" for line in body_lines)
        else:
            lines.append(f"  Body: {len(body_lines)} lines")
    return lines


def parsed_field_lines(fields: list[dict]) -> list[str]:
    return [
        f"  Field{field_direction_prefix(definition, ' [{}]')}: {definition['name']} @ offset {definition['offset']}, "
        f"{definition['length']}B {definition['dtype']}{field_value_suffix(definition)}"
        for definition in fields
    ]


def fact_list_entry_lines(fact: dict, verbose: bool) -> list[str]:
    from netaudio.dante.fact_store import get_confidence

    fact_confidence = get_confidence(fact)
    suffix = ""
    if fact_confidence == "disproved":
        disprovals = fact.get("disprovals", [])
        if disprovals:
            device = disprovals[-1].get("device_ip", "")
            suffix = f"  (disproved{' on ' + device if device else ''})"
    lines = [f"  {CONFIDENCE_MARKERS.get(fact_confidence, ' ')} {fact['key']:16s} {fact['name']}{suffix}"]
    if not verbose:
        return lines
    if fact.get("note"):
        lines.append(f"    {fact['note']}")
    if fact.get("body"):
        lines.extend(f"    {line}" for line in fact["body"].splitlines())
    for definition in fact.get("fields", []):
        lines.append(
            f"    field{field_direction_prefix(definition, ' [{}]')}: {definition['name']} @ "
            f"{definition['offset']}+{definition['length']} {definition['dtype']}{field_value_suffix(definition)}"
        )
    lines.extend(f"    evidence: {reference}" for reference in fact.get("evidence", []))
    lines.append("")
    return lines


def fact_list_lines(facts: list[dict], verbose: bool) -> list[str]:
    lines = []
    current_category = None
    for fact in facts:
        if fact["category"] != current_category:
            current_category = fact["category"]
            lines.append(f"[{current_category}]")
        lines.extend(fact_list_entry_lines(fact, verbose))
    return lines


def fact_header_lines(fact: dict) -> list[str]:
    from netaudio.dante.fact_store import get_confidence

    lines = [
        f"{icon('info')}Fact: {fact['category']}:{fact['key']}",
        f"  Name:       {fact['name']}",
        f"  Confidence: {get_confidence(fact)}",
    ]
    if fact.get("note"):
        lines.append(f"  Note:       {fact['note']}")
    if fact.get("supersedes"):
        lines.append(f"  Supersedes: {fact['supersedes']}")
    if fact.get("body"):
        lines.append("")
        lines.extend(f"  {line}" for line in fact["body"].splitlines())
        lines.append("")
    if fact.get("fields"):
        lines.append("  Fields:")
        for definition in fact["fields"]:
            lines.append(
                f"    {field_direction_prefix(definition, '[{}] ')}{definition['name']:20s} offset "
                f"{definition['offset']:>4d}  {definition['length']}B  {definition['dtype']}"
                f"{field_value_suffix(definition)}"
            )
    if fact.get("evidence"):
        lines.append("  Evidence:")
        lines.extend(f"    {reference}" for reference in fact["evidence"])
    return lines


def fact_status_lines(fact: dict) -> list[str]:
    lines = []
    if fact.get("disprovals"):
        lines.append("  Disprovals:")
        for disproval in fact["disprovals"]:
            device = disproval.get("device_ip", "unknown device")
            response_size = disproval.get("response_size")
            size_text = f" ({response_size}B response)" if response_size else ""
            lines.append(f"    {device}{size_text}: {disproval.get('reason', '')}")
            for mismatch in disproval.get("field_mismatches", []):
                lines.append(f"      {mismatch.get('name', '?')}: {mismatch.get('error', '')}")
    confidence_log = fact.get("confidence_log", [])
    if len(confidence_log) > 1:
        lines.append("  Confidence log:")
        for entry in confidence_log:
            timestamp_ns = entry.get("timestamp_ns", 0)
            timestamp_seconds = timestamp_ns / 1_000_000_000 if timestamp_ns else 0
            timestamp = (
                datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d %H:%M") if timestamp_seconds else "?"
            )
            lines.append(f"    {timestamp}  {entry['level']}")
    if fact.get("history"):
        lines.append(f"  History ({len(fact['history'])} revision(s)):")
        for entry in fact["history"]:
            action = entry.get("action", "updated")
            lines.append(f"    {action}: was {entry.get('previous_name')} [{entry.get('previous_confidence')}]")
    return lines


def proof_entry_lines(fact: dict, lookup: EvidenceLookup) -> tuple[list[str], dict[str, Any]]:
    from netaudio.dante.dissection.rendering import dissect_and_render

    data = lookup.data()
    if lookup.bundle_path is None:
        return [f"  [ERROR] {lookup.error}"], data
    lines = [f"\n  Bundle: {lookup.bundle_path.name}"]
    session = lookup.manifest.get("session")
    if session:
        lines.append(f"    Session: #{session.get('id', '?')} {session.get('name', '')}")
        lines.append(f"    Started: {session.get('started', '?')}")
    if lookup.session_level:
        lines.append("    (session-level evidence, no specific packet)")
        return lines, data
    if not lookup.sample_found:
        lines.append(f"    [ERROR] {lookup.error}")
        return lines, data
    lines.append(f"\n    Packet #{lookup.packet_id}  {lookup.direction}  opcode={lookup.opcode}")
    lines.append(f"      {lookup.source} -> {lookup.destination}")
    if lookup.payload is None:
        lines.append(f"      [ERROR] {lookup.error}")
        return lines, data
    dissection = dissect_and_render(lookup.payload, indent="        ", direction=lookup.direction)
    lines.extend([f"      Size: {len(lookup.payload)}B", "      Payload:", dissection])
    data["dissection"] = dissection
    if fact.get("fields"):
        results = field_verification_results(fact, lookup.payload, lookup.direction)
        lines.extend(["", "      Field verification:", *field_verification_lines(results, "        ")])
        data["field_verification"] = results
    return lines, data


def check_proof_lines(fact: dict, provenance_dir: Path) -> tuple[list[str], list[dict[str, Any]]]:
    from netaudio.dante.dissection.rendering import dissect_and_render

    lines = []
    proof = []
    if fact.get("note"):
        lines.append(f"         note: {fact['note']}")
    for reference in fact["evidence"]:
        lookup = lookup_evidence(provenance_dir, reference)
        if lookup.bundle_path is None:
            continue
        data = lookup.data()
        if lookup.session_level:
            lines.append(f"         evidence: {reference} (session-level)")
            proof.append(data)
            continue
        if lookup.payload is None:
            continue
        dissection = dissect_and_render(lookup.payload, indent="           ", direction=lookup.direction)
        lines.extend(
            [
                f"\n         --- {reference} ---",
                f"         Packet #{lookup.packet_id}  {lookup.direction}  opcode={lookup.opcode}  "
                f"{len(lookup.payload)}B",
                f"         {lookup.source} -> {lookup.destination}",
                dissection,
            ]
        )
        data["dissection"] = dissection
        if fact.get("fields"):
            results = field_verification_results(fact, lookup.payload, lookup.direction)
            lines.extend(field_verification_lines(results, "           "))
            data["field_verification"] = results
        proof.append(data)
    return lines, proof


@dataclass
class CheckStatus:
    counter: str
    icon_name: str
    label: str


def check_status(result: dict) -> CheckStatus:
    if result.get("status") == "disproved":
        return CheckStatus("disproved", "fail", "DISP")
    if result.get("status") == "quarantined":
        return CheckStatus("quarantined", "warning", "QUAR")
    if result["errors"]:
        return CheckStatus("failed", "fail", "FAIL")
    if result["verified_fields"]:
        return CheckStatus("passed", "success", "PASS")
    return CheckStatus("warned", "warning", "WARN")


def check_result_lines(result: dict, status: CheckStatus) -> list[str]:
    marker = CHECK_CONFIDENCE_MARKERS.get(result.get("confidence", ""), " ")
    lines = [f"  {icon(status.icon_name)}[{status.label}] {marker} {result['fact_key']:30s} {result['name']}"]
    if result.get("quarantine_reason"):
        lines.append(f"         quarantine: {result['quarantine_reason']}")
    for verified in result["verified_fields"]:
        if verified.get("expected") is not None:
            lines.append(f"         field {verified['name']}: {verified['expected']} == {verified['actual']}")
        else:
            lines.append(f"         field {verified['name']}: {verified['actual']}")
    lines.extend(f"         {error}" for error in result["errors"])
    return lines


@dataclass
class CheckTotals:
    disproved: int = 0
    failed: int = 0
    passed: int = 0
    quarantined: int = 0
    warned: int = 0

    def count(self, status: CheckStatus) -> None:
        setattr(self, status.counter, getattr(self, status.counter) + 1)

    @property
    def total(self) -> int:
        return self.passed + self.warned + self.quarantined + self.disproved + self.failed

    def summary_line(self) -> str:
        return (
            f"{self.total} facts checked: {self.passed} passed, {self.warned} no fields to verify, "
            f"{self.quarantined} quarantined, {self.disproved} disproved, {self.failed} failed"
        )

    def data(self) -> dict[str, int]:
        return {
            "disproved": self.disproved,
            "failed": self.failed,
            "passed": self.passed,
            "quarantined": self.quarantined,
            "total": self.total,
            "warned": self.warned,
        }
