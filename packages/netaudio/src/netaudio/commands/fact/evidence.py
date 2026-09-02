from __future__ import annotations

import sqlite3
import struct
from dataclasses import dataclass, field

import typer

from netaudio.commands.capture.options import _resolve_facts_path
from netaudio.commands.capture.reporting import structured_output_selected
from netaudio.icons import icon

SESSION_SELECTORS = {"active", "latest"}


@dataclass
class EvidenceSessionResolution:
    lines: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)


@dataclass
class VerificationEntry:
    fact: dict
    fact_key: str
    port: int
    request_packet: bytes

    def data(self) -> dict:
        return {
            "fact_key": self.fact_key,
            "name": self.fact["name"],
            "port": self.port,
            "request_packet_hex": self.request_packet.hex(),
        }


@dataclass
class FieldEvaluation:
    results: list[dict]
    status: str


@dataclass
class VerificationTotals:
    failed: int = 0
    passed: int = 0
    timed_out: int = 0

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.timed_out

    def parts(self) -> list[str]:
        parts = []
        if self.passed:
            parts.append(f"{self.passed} passed")
        if self.failed:
            parts.append(f"{self.failed} failed")
        if self.timed_out:
            parts.append(f"{self.timed_out} timed out")
        return parts

    def data(self) -> dict[str, int]:
        return {"failed": self.failed, "passed": self.passed, "timed_out": self.timed_out, "total": self.total}


@dataclass
class VerificationReport:
    lines: list[str] = field(default_factory=list)
    results: list[dict] = field(default_factory=list)
    totals: VerificationTotals = field(default_factory=VerificationTotals)

    def emit(self, *lines: str) -> None:
        self.lines.extend(lines)
        if not structured_output_selected():
            for line in lines:
                typer.echo(line)

    def data(self) -> dict:
        return {"results": self.results, "summary": self.totals.data()}


def _open_evidence_store(
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
):
    from netaudio.capture.sessions import open_packet_store

    try:
        options = {}
        if db is not None:
            options["db"] = db
        if config is not None:
            options["config"] = config
        if profile is not None:
            options["profile"] = profile
        return open_packet_store(**options)
    except (OSError, ValueError, sqlite3.Error) as exception:
        typer.echo(f"Capture: unable to open the packet database: {exception}", err=True)
        raise typer.Exit(1)


def _resolve_evidence_sessions(
    evidence_refs: list[str],
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
) -> EvidenceSessionResolution:
    resolution = EvidenceSessionResolution(references=list(evidence_refs))
    if not any(reference.split(":", 1)[0] in SESSION_SELECTORS for reference in evidence_refs):
        return resolution

    store = _open_evidence_store(db=db, config=config, profile=profile)
    try:
        resolution.references = []
        for reference in evidence_refs:
            parts = reference.split(":")
            if len(parts) != 2 or parts[0] not in SESSION_SELECTORS:
                resolution.references.append(reference)
                continue

            session_selector, packet_id_string = parts
            session = store.get_latest_session(active_only=session_selector == "active")
            if not session or not session.get("name"):
                typer.echo(f"Capture: no {session_selector} session found for evidence reference.", err=True)
                raise typer.Exit(1)

            resolution.references.append(f"{session['name']}:{packet_id_string}")
            resolution.lines.append(f"  Resolved {session_selector} -> {session['name']}")
        return resolution
    finally:
        store.close()


def _parse_evidence_reference(store, reference: str) -> tuple[int, int]:
    parts = reference.split(":")
    if len(parts) != 2:
        typer.echo(f"Capture: invalid evidence reference '{reference}'; expected session_name:packet_id.", err=True)
        raise typer.Exit(1)

    session_name, packet_id_string = parts
    try:
        packet_id = int(packet_id_string)
    except ValueError:
        typer.echo(f"Capture: invalid packet ID in evidence reference '{reference}'.", err=True)
        raise typer.Exit(1)
    if packet_id <= 0:
        typer.echo(f"Capture: packet ID must be positive in evidence reference '{reference}'.", err=True)
        raise typer.Exit(1)

    session = store.find_session_by_name(session_name, active_only=False)
    if not session:
        typer.echo(f"Capture: evidence session '{session_name}' was not found.", err=True)
        raise typer.Exit(1)
    if not store.get_packet(packet_id):
        typer.echo(f"Capture: evidence packet #{packet_id} was not found.", err=True)
        raise typer.Exit(1)
    return int(session["id"]), packet_id


def _validate_evidence_references(
    evidence_refs: list[str],
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
) -> list[tuple[int, int]]:
    store = _open_evidence_store(db=db, config=config, profile=profile)
    try:
        return [_parse_evidence_reference(store, reference) for reference in evidence_refs]
    finally:
        store.close()


def _create_evidence_markers(
    evidence_targets: list[tuple[int, int]],
    category: str,
    key: str,
    name: str,
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
) -> list[str]:
    store = _open_evidence_store(db=db, config=config, profile=profile)
    lines = []
    try:
        packet_ids_by_session = {}
        for session_id, packet_id in evidence_targets:
            packet_ids_by_session.setdefault(session_id, [])
            if packet_id not in packet_ids_by_session[session_id]:
                packet_ids_by_session[session_id].append(packet_id)

        for session_id, packet_ids in packet_ids_by_session.items():
            label = f"evidence_{category}_{key}"
            summary = f"{category}:{key} ({name}) — {len(packet_ids)} packet(s)"
            existing_markers = store.get_markers(session_id, marker_types=["evidence"])
            marker_exists = any(
                marker.get("label") == label
                and marker.get("data", {}).get("fact") == f"{category}:{key}"
                and marker.get("data", {}).get("packet_ids") == packet_ids
                for marker in existing_markers
            )
            if marker_exists:
                lines.append(f"  Marker: #{session_id} {label} already exists")
                continue
            store.add_marker(
                session_id=session_id,
                label=label,
                marker_type="evidence",
                summary=summary,
                data={"packet_ids": packet_ids, "fact": f"{category}:{key}"},
            )
            lines.append(f"  Marker: #{session_id} {label}")
        return lines
    finally:
        store.close()


def _retagged_request(packet: bytes) -> bytes:
    if len(packet) < 6:
        return packet
    original_transaction = struct.unpack(">H", packet[4:6])[0]
    new_transaction = (original_transaction + 0x4000) & 0xFFFF
    return packet[:4] + struct.pack(">H", new_transaction) + packet[6:]


def _evaluate_response_fields(fact: dict, response: bytes) -> FieldEvaluation:
    from netaudio.dante.fact_store import _field_applies_to_direction, _verify_field

    results = [
        _verify_field(response, definition)
        for definition in fact.get("fields", [])
        if _field_applies_to_direction(definition, "response")
    ]
    failures = [result for result in results if not result["ok"]]
    if not failures:
        return FieldEvaluation(results, "PASS")
    if all(result.get("bounds") for result in failures):
        return FieldEvaluation(results, "BOUNDS")
    return FieldEvaluation(results, "FAIL")


def _field_result_lines(results: list[dict]) -> list[str]:
    lines = []
    for result in results:
        field_name = result["name"]
        if not result["ok"]:
            if result.get("bounds"):
                lines.append(f"           {field_name:24s} BOUNDS  {result['error']}")
            else:
                lines.append(f"           {field_name:24s} FAIL    {result.get('error', '')}")
        elif result.get("expected") is not None:
            lines.append(f"           {field_name:24s} {result['actual']:>16s} == {result['expected']}")
        else:
            lines.append(f"           {field_name:24s} {result['actual']:>16s}")
    return lines


def _auto_disprove(entry: VerificationEntry, results: list[dict], device_ip: str, response: bytes) -> str:
    from netaudio.dante.fact_store import disprove_fact

    mismatches = [result for result in results if not result["ok"]]
    reasons = []
    for mismatch in mismatches:
        if mismatch.get("bounds"):
            reasons.append(f"{mismatch['name']}: out of bounds")
        else:
            reasons.append(f"{mismatch['name']}: {mismatch.get('error', 'mismatch')}")
    reason = f"Verification failed on {device_ip} ({len(response)}B response): {'; '.join(reasons)}"
    disprove_fact(
        _resolve_facts_path(),
        category=entry.fact["category"],
        key=entry.fact["key"],
        reason=reason,
        device_ip=device_ip,
        response_size=len(response),
        field_mismatches=mismatches,
    )
    return reason


async def _verify_entry(
    verifier, entry: VerificationEntry, report: VerificationReport, device_ip: str, timeout: float, auto_disprove: bool
) -> None:
    label = f"verify_{entry.fact_key.replace(':', '_')}"
    response = await verifier.send(
        _retagged_request(entry.request_packet), port=entry.port, timeout=timeout, label=label
    )
    if response is None:
        report.emit(f"  {icon('timeout')}[TIMEOUT] {entry.fact_key:30s} {entry.fact['name']}")
        report.totals.timed_out += 1
        report.results.append({**entry.data(), "status": "timeout"})
        verifier.observation(f"{label}_timeout", note=f"No response for {entry.fact_key}")
        return

    evaluation = _evaluate_response_fields(entry.fact, response)
    if evaluation.status == "PASS":
        report.totals.passed += 1
    else:
        report.totals.failed += 1
    report.emit(
        f"  [{evaluation.status:6s}] {entry.fact_key:30s} {entry.fact['name']}  ({len(response)}B)",
        *_field_result_lines(evaluation.results),
    )
    result_data = {
        **entry.data(),
        "fields": evaluation.results,
        "response_hex": response.hex(),
        "response_len": len(response),
        "status": evaluation.status.lower(),
    }
    verifier.observation(
        f"{label}_result",
        note=f"{entry.fact_key}: {evaluation.status}",
        data={"status": evaluation.status.lower(), "response_len": len(response), "fields": evaluation.results},
    )
    if auto_disprove and evaluation.status in ("FAIL", "BOUNDS"):
        result_data["disproved_reason"] = _auto_disprove(entry, evaluation.results, device_ip, response)
        report.emit(f"           -> auto-disproved {entry.fact_key}")
    report.results.append(result_data)


async def _run_fact_verify(
    verify_plan: list[VerificationEntry],
    device_ip: str,
    timeout: float,
    session_name: str,
    config: str | None,
    profile: str | None,
    db_override: str | None,
    auto_disprove: bool = False,
) -> VerificationReport:
    from netaudio.dante.protocol_verifier import ProtocolVerifier

    report = VerificationReport()
    async with ProtocolVerifier(
        device_ip=device_ip,
        session_name=session_name,
        config=config,
        profile=profile,
        db=db_override,
        record=False,
    ) as verifier:
        verifier.marker(
            "fact_verify_started",
            marker_type="system",
            note=f"Verifying {len(verify_plan)} facts against {device_ip}",
        )
        for entry in verify_plan:
            await _verify_entry(verifier, entry, report, device_ip, timeout, auto_disprove)

        parts = report.totals.parts()
        report.emit("", f"Verification complete: {report.totals.total} facts — {', '.join(parts)}")
        verifier.marker(
            "fact_verify_finished",
            marker_type="system",
            note=f"Verification complete: {', '.join(parts)}",
            data=report.totals.data(),
        )
    return report
