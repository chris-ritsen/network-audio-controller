from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.packets import _hexdump
from netaudio.capture.provenance import (
    _check_message_labels,
    _check_opcode_labels,
    _extract_seed_samples,
    _load_label_overrides,
    _query_observed_messages,
    _query_observed_opcodes,
    _query_observed_subscription_statuses,
    _scan_observed_from_fixtures,
    _write_seed_samples,
)
from netaudio.commands.capture.options import (
    _default_fixture_root,
    _default_label_overrides_path,
    _default_provenance_output_dir,
    _load_capture_profile,
    _normalize_marker_label,
    _parse_set_message,
    _parse_set_opcode,
    _parse_set_status,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_session_reference,
)
from netaudio.commands.capture.reporting import emit_report
from netaudio.commands.provenance.bundles import (
    BundleReport,
    _load_bundle,
    _resolve_provenance_scope,
    audit_bundle,
    verify_bundle,
)
from netaudio.commands.provenance.labeling import (
    _interactive_label_messages,
    _interactive_label_opcodes,
    _interactive_label_statuses,
)
from netaudio.dante.clean_labels import (
    load_clean_labels,
    load_clean_subscription_status_labels,
    resolve_clean_labels_path,
    save_clean_labels,
)
from netaudio.dante.packet_store import PacketStore


def provenance_seed(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    out: Optional[str] = typer.Option(None, "--out", help="Output fixture directory."),
    clean: bool = typer.Option(False, "--clean", help="Delete existing .bin fixtures and manifest before writing."),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID scope."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    from_label: Optional[str] = typer.Option(
        None, "--from-label", help="Start at first marker label in scoped session."
    ),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker label in scoped session."),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Only include packets where src or dst matches this IP."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db).expanduser().resolve()
    if not db_path.exists():
        raise typer.Exit(f"capture database not found: {db_path}")

    output_dir = Path(out).expanduser().resolve() if out else _default_provenance_output_dir().resolve()
    if clean and output_dir.exists():
        for fixture in output_dir.glob("*.bin"):
            fixture.unlink()
        manifest_path = output_dir / "manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, start_ns, end_ns = _resolve_provenance_scope(
            store,
            id=id,
            session=session,
            from_label=from_label,
            to_label=to_label,
        )
    finally:
        store.close()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows, status_samples = _extract_seed_samples(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
    finally:
        conn.close()

    if not rows and not status_samples:
        raise typer.Exit("no packets matched scope in capture database. capture traffic first, or loosen filters.")

    manifest_path = _write_seed_samples(
        rows,
        status_samples,
        output_dir,
        db_path=db_path,
        session_id=resolved_session_id,
        from_label=_normalize_marker_label(from_label) if from_label else None,
        to_label=_normalize_marker_label(to_label) if to_label else None,
        device_ip=device_ip,
    )

    print(f"Capture: Seeded fixtures: {len(rows) + len(status_samples)}")
    print(f"Capture: Output: {output_dir}")
    print(f"Capture: Manifest: {manifest_path}")


def provenance_label(
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    labels: Optional[str] = typer.Option(None, "--labels", help="Labels JSON path."),
    interactive: bool = typer.Option(
        True, "--interactive/--no-interactive", help="Prompt for unlabeled observed entries."
    ),
    set_opcode: Optional[list[str]] = typer.Option(None, "--set-opcode", help="Set label: protocol:opcode=label"),
    set_message: Optional[list[str]] = typer.Option(None, "--set-message", help="Set label: message_type=label"),
    set_status: Optional[list[str]] = typer.Option(None, "--set-status", help="Set label: status_code=state:label"),
    id: Optional[int] = typer.Option(None, "--id", help="Session ID scope."),
    session: Optional[str] = typer.Option(None, "--session", help="Session reference (ID, name, latest, active)."),
    from_label: Optional[str] = typer.Option(
        None, "--from-label", help="Start at first marker label in scoped session."
    ),
    to_label: Optional[str] = typer.Option(None, "--to-label", help="End at last marker label in scoped session."),
    device_ip: Optional[str] = typer.Option(
        None, "--device-ip", help="Only include packets where src or dst matches this IP."
    ),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(id, "--id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)
    db_path = Path(resolved_db).expanduser().resolve()
    if not db_path.exists():
        raise typer.Exit(f"capture database not found: {db_path}")

    labels_path = resolve_clean_labels_path(labels)
    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, start_ns, end_ns = _resolve_provenance_scope(
            store,
            id=id,
            session=session,
            from_label=from_label,
            to_label=to_label,
        )
    finally:
        store.close()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        opcode_rows = _query_observed_opcodes(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
        message_rows = _query_observed_messages(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
        status_rows = _query_observed_subscription_statuses(
            conn,
            session_id=resolved_session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            device_ip=device_ip,
        )
    finally:
        conn.close()

    if not opcode_rows and not message_rows and not status_rows:
        raise typer.Exit("no observed opcodes/messages/status values matched scope")

    opcode_labels, message_labels = load_clean_labels(labels_path)
    status_labels = load_clean_subscription_status_labels(labels_path)
    changed = False

    for token in set_opcode or []:
        try:
            key, value = _parse_set_opcode(token)
        except ValueError as exception:
            raise typer.Exit(f"invalid --set-opcode {token!r}: {exception}")
        opcode_labels[key] = value
        changed = True

    for token in set_message or []:
        try:
            key, value = _parse_set_message(token)
        except ValueError as exception:
            raise typer.Exit(f"invalid --set-message {token!r}: {exception}")
        message_labels[key] = value
        changed = True

    for token in set_status or []:
        try:
            key, value = _parse_set_status(token)
        except ValueError as exception:
            raise typer.Exit(f"invalid --set-status {token!r}: {exception}")
        status_labels[key] = value
        changed = True

    observed_opcode_keys = {(int(row["protocol_id"]), int(row["opcode"])) for row in opcode_rows}
    observed_message_keys = {int(row["message_type"]) for row in message_rows}
    observed_status_keys = {int(row["status_code"]) for row in status_rows}
    unlabeled_opcodes = [
        row for row in opcode_rows if (int(row["protocol_id"]), int(row["opcode"])) not in opcode_labels
    ]
    unlabeled_messages = [row for row in message_rows if int(row["message_type"]) not in message_labels]
    unlabeled_statuses = [row for row in status_rows if int(row["status_code"]) not in status_labels]

    print(f"db: {db_path}")
    print(f"labels: {labels_path}")
    print(
        "observed opcodes/messages/statuses: "
        f"{len(observed_opcode_keys)}/{len(observed_message_keys)}/{len(observed_status_keys)} "
        f"unlabeled: {len(unlabeled_opcodes)}/{len(unlabeled_messages)}/{len(unlabeled_statuses)}"
    )

    if interactive:
        if unlabeled_opcodes:
            changed = _interactive_label_opcodes(unlabeled_opcodes, opcode_labels) or changed
        if unlabeled_messages:
            changed = _interactive_label_messages(unlabeled_messages, message_labels) or changed
        if unlabeled_statuses:
            changed = _interactive_label_statuses(unlabeled_statuses, status_labels) or changed

    if not changed:
        print("no label changes")
        return

    saved_path = save_clean_labels(
        opcode_labels,
        message_labels,
        status_labels,
        labels_path,
    )
    print(f"saved labels: {saved_path}")


def provenance_check(
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root to scan for .bin payload samples."
    ),
    labels: Optional[str] = typer.Option(None, "--labels", help="Labels JSON path."),
    overrides: Optional[str] = typer.Option(None, "--overrides", help="Optional JSON override allowlist path."),
):
    fixture_root = Path(fixtures_root).expanduser().resolve() if fixtures_root else _default_fixture_root().resolve()
    if not fixture_root.exists():
        raise typer.Exit(f"fixture root not found: {fixture_root}")

    labels_path = resolve_clean_labels_path(labels)
    overrides_path = Path(overrides).expanduser().resolve() if overrides else _default_label_overrides_path().resolve()

    observed_opcodes, observed_messages, observed_statuses = _scan_observed_from_fixtures(fixture_root)
    opcode_overrides, message_overrides, status_overrides = _load_label_overrides(overrides_path)
    clean_opcode_labels, clean_message_labels = load_clean_labels(labels_path)
    clean_status_labels = load_clean_subscription_status_labels(labels_path)

    failures: list[str] = []
    failures.extend(_check_opcode_labels(observed_opcodes, opcode_overrides))
    failures.extend(_check_message_labels(observed_messages, message_overrides))

    arc_variant_protocols = {0x27FF, 0x2809}
    for protocol, opcode in sorted(clean_opcode_labels):
        label = clean_opcode_labels[(protocol, opcode)]
        if not label or label == f"0x{opcode:04X}":
            continue
        key = (protocol, opcode)
        if key in observed_opcodes or key in opcode_overrides:
            continue
        if protocol in arc_variant_protocols:
            if (0x27FF, opcode) in observed_opcodes or (0x2809, opcode) in observed_opcodes:
                continue
        failures.append(
            f"unproven clean opcode label: protocol=0x{protocol:04X} opcode=0x{opcode:04X} "
            f"label={label!r} file={labels_path}"
        )

    for message_type in sorted(clean_message_labels):
        label = clean_message_labels[message_type]
        if not label or label == f"msg:0x{message_type:04X}":
            continue
        if message_type in observed_messages or message_type in message_overrides:
            continue
        failures.append(f"unproven clean settings label: msg=0x{message_type:04X} label={label!r} file={labels_path}")

    for status_code in sorted(clean_status_labels):
        entry = clean_status_labels[status_code]
        label = entry.get("label")
        if not isinstance(label, str) or not label.strip():
            continue
        if status_code in observed_statuses or status_code in status_overrides:
            continue
        failures.append(
            f"unproven clean subscription-status label: status=0x{status_code:04X} label={label!r} file={labels_path}"
        )

    if failures:
        typer.echo("label provenance check failed:", err=True)
        for failure in failures:
            typer.echo(f"  - {failure}", err=True)
        typer.echo(
            f"add vetted exceptions to {overrides_path} if a label is intentionally manual.",
            err=True,
        )
        raise typer.Exit(1)

    print(
        "label provenance OK: "
        f"observed_opcodes={len(observed_opcodes)} "
        f"observed_messages={len(observed_messages)} "
        f"observed_subscription_statuses={len(observed_statuses)} "
        f"clean_labels={len(clean_opcode_labels) + len(clean_message_labels) + len(clean_status_labels)}"
    )


def _discover_bundle_paths(bundle: str | None, fixtures_root: str | None) -> list[Path]:
    if bundle:
        return [Path(bundle).expanduser().resolve()]
    root = Path(fixtures_root).expanduser().resolve() if fixtures_root else _default_fixture_root().resolve()
    provenance_dir = root / "provenance" if root.name != "provenance" else root
    if not provenance_dir.exists():
        raise typer.Exit(f"Provenance directory not found: {provenance_dir}")
    bundle_paths = sorted(
        [path for path in provenance_dir.iterdir() if path.is_dir() and (path / "manifest.json").exists()]
        + list(provenance_dir.glob("*.tar.gz"))
        + list(provenance_dir.glob("*.zip"))
    )
    if not bundle_paths:
        raise typer.Exit(f"No session bundles found in {provenance_dir}")
    return bundle_paths


def _report_bundle_results(bundle_paths: list[Path], inspect_bundle, rule_width: int) -> None:
    reports = []
    for bundle_path in bundle_paths:
        if not bundle_path.exists():
            reports.append(
                BundleReport(
                    bundle_path=bundle_path,
                    data={
                        "bundle": bundle_path.name,
                        "error": "Bundle not found",
                        "passed": False,
                        "path": str(bundle_path),
                    },
                    lines=[f"FAIL: Bundle not found: {bundle_path}"],
                )
            )
            continue
        reports.append(inspect_bundle(bundle_path))
    total = len(reports)
    passed = sum(1 for report in reports if report.passed)
    failed = total - passed
    lines = []
    for report in reports:
        lines.extend(report.lines)
        if report.bundle_path.exists():
            lines.append("")
    if total > 1:
        lines.append(f"{'=' * rule_width}")
        lines.append(f"Total: {total} bundles, {passed} passed, {failed} failed")
    data = {"bundles": [report.data for report in reports], "failed": failed, "passed": passed, "total": total}
    emit_report(lines, data)
    if failed > 0:
        raise typer.Exit(1)


def provenance_verify(
    bundle: Optional[str] = typer.Argument(
        None, help="Path to a specific bundle directory, .tar.gz, or .zip. Omit to scan all bundles."
    ),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    _report_bundle_results(_discover_bundle_paths(bundle, fixtures_root), verify_bundle, 60)


def _show_evidence_marker_lines(marker_data: dict, sample_by_id: dict, files: dict[str, bytes]) -> list[str]:
    indent = f"{'':14s}  {'':26s}"
    lines = []
    packet_ids = marker_data["packet_ids"]
    filters = marker_data.get("filters", {})
    if filters:
        filter_parts = [f"{key}={value}" for key, value in filters.items()]
        lines.append(f"{indent}  filters: {', '.join(filter_parts)}")
    for packet_id in packet_ids[:20]:
        sample_entry = sample_by_id.get(packet_id)
        if not sample_entry:
            continue
        direction = sample_entry.get("direction") or "multicast"
        opcode_hex = sample_entry.get("opcode_hex", "?")
        source = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
        destination = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
        payload = files.get(sample_entry.get("file", ""))
        size = f"{len(payload)}B" if payload else "?"
        lines.append(f"{indent}    #{packet_id} {direction:9s} {opcode_hex} {source} -> {destination} {size}")
        if payload:
            lines.append(_hexdump(payload, indent=f"{indent}    "))
    if len(packet_ids) > 20:
        lines.append(f"{indent}    ... and {len(packet_ids) - 20} more")
    return lines


def _show_timeline_lines(manifest: dict, files: dict[str, bytes]) -> list[str]:
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])
    lines = ["\nTimeline:", f"{'Type':14s}  {'Timestamp':26s}  {'Label':34s}", "-" * 80]
    sample_by_id = {sample.get("packet_id"): sample for sample in samples}
    for marker in markers:
        marker_type = marker.get("marker_type", "?")
        lines.append(f"{marker_type:14s}  {marker.get('timestamp_iso', ''):26s}  {marker.get('label', '')}")
        note = marker.get("note")
        if note:
            lines.append(f"{'':14s}  {'':26s}  note: {note}")
        marker_data = marker.get("data")
        if marker_data and marker_type == "evidence" and marker_data.get("packet_ids"):
            lines.extend(_show_evidence_marker_lines(marker_data, sample_by_id, files))
    if artifacts:
        lines.append("\nArtifacts:")
        for artifact in artifacts:
            lines.append(
                f"  {artifact.get('label', '?')} [{artifact.get('role', '?')}] "
                f"{artifact.get('file', '?')} sha256={artifact.get('sha256', '?')}"
            )
    return lines


def provenance_show(
    bundle: str = typer.Argument(..., help="Path to a bundle directory or .tar.gz file."),
):
    bundle_path = Path(bundle).expanduser().resolve()
    if not bundle_path.exists():
        typer.echo(f"Bundle not found: {bundle_path}", err=True)
        raise typer.Exit(1)

    manifest, files = _load_bundle(bundle_path)

    scope = manifest.get("scope", {})
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])
    session_packets = manifest.get("session_packet_count", manifest.get("count", 0))
    evidence_packets = manifest.get("evidence_packet_count", 0)

    lines = [
        f"Session #{manifest.get('session_id', '?')}",
        f"  Name:        {manifest.get('session_name', '')}",
        f"  Device:      {scope.get('device_name', '')} ({scope.get('device_ip', '')})",
        f"  Started:     {manifest.get('started_iso', '')}",
        f"  Ended:       {manifest.get('ended_iso', '')}",
        f"  Packets:     {session_packets} session, {evidence_packets} evidence",
        f"  Markers:     {len(markers)}",
        f"  Artifacts:   {len(artifacts)}",
    ]
    if markers or samples or artifacts:
        lines.extend(_show_timeline_lines(manifest, files))
    data = {
        "artifacts": artifacts,
        "bundle": bundle_path.name,
        "ended": manifest.get("ended_iso", ""),
        "evidence_packets": evidence_packets,
        "markers": markers,
        "path": str(bundle_path),
        "samples": samples,
        "scope": scope,
        "session_id": manifest.get("session_id"),
        "session_name": manifest.get("session_name", ""),
        "session_packets": session_packets,
        "started": manifest.get("started_iso", ""),
    }
    emit_report(lines, data)


def provenance_audit(
    bundle: Optional[str] = typer.Argument(
        None, help="Path to a bundle directory or .tar.gz. Omit to scan all bundles."
    ),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    _report_bundle_results(_discover_bundle_paths(bundle, fixtures_root), audit_bundle, 72)


def provenance_export(
    session_id: Optional[int] = typer.Option(None, "--session-id", help="Session ID."),
    session: Optional[str] = typer.Option(
        None,
        "--session",
        help="Session reference (ID, name, latest, active). Defaults to latest.",
    ),
    out: Optional[str] = typer.Option(None, "--out", help="Output directory for the bundle."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    _require_positive_session_id(session_id, "--session-id")
    profile_cfg, _ = _load_capture_profile(config, profile)
    resolved_db = _resolve_db_from_config(db, profile_cfg)

    from netaudio.dante.protocol_verifier import export_session_bundle

    store = PacketStore(db_path=resolved_db)
    try:
        resolved_session_id, _ = _resolve_session_reference(
            store,
            session_id=session_id,
            session=session,
            default_selector="latest",
        )
        bundle_path = export_session_bundle(store, resolved_session_id, output_dir=out)
        evidence_count = store.get_session_evidence_count(resolved_session_id)
    finally:
        store.close()
    emit_report(
        [f"Capture: Exported bundle: {bundle_path}"],
        {"bundle": str(bundle_path), "evidence_packets": evidence_count, "session_id": resolved_session_id},
    )
    if evidence_count == 0:
        typer.echo(
            "Capture: Warning: no packet evidence was tagged; this is a local draft, not a promotable provenance bundle.",
            err=True,
        )
