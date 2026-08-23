from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Optional

import typer

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
from netaudio.commands.capture_helpers import (
    _default_fixture_root,
    _default_label_overrides_path,
    _default_provenance_output_dir,
    _hexdump,
    _load_capture_profile,
    _normalize_marker_label,
    _parse_set_message,
    _parse_set_opcode,
    _parse_set_status,
    _require_positive_session_id,
    _resolve_db_from_config,
    _resolve_session_reference,
)
from netaudio.commands.provenance_app import app
from netaudio.commands.provenance_support import (
    _audit_single_bundle,
    _interactive_label_messages,
    _interactive_label_opcodes,
    _interactive_label_statuses,
    _load_bundle,
    _resolve_provenance_scope,
    _verify_single_bundle,
)
from netaudio.dante.clean_labels import (
    load_clean_labels,
    load_clean_subscription_status_labels,
    resolve_clean_labels_path,
    save_clean_labels,
)
from netaudio.dante.packet_store import PacketStore


@app.command("seed")
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


@app.command("label")
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
        except Exception as exception:
            raise typer.Exit(f"invalid --set-opcode {token!r}: {exception}")
        opcode_labels[key] = value
        changed = True

    for token in set_message or []:
        try:
            key, value = _parse_set_message(token)
        except Exception as exception:
            raise typer.Exit(f"invalid --set-message {token!r}: {exception}")
        message_labels[key] = value
        changed = True

    for token in set_status or []:
        try:
            key, value = _parse_set_status(token)
        except Exception as exception:
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


@app.command("check")
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
        print("label provenance check failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            f"add vetted exceptions to {overrides_path} if a label is intentionally manual.",
            file=sys.stderr,
        )
        raise typer.Exit(1)

    print(
        "label provenance OK: "
        f"observed_opcodes={len(observed_opcodes)} "
        f"observed_messages={len(observed_messages)} "
        f"observed_subscription_statuses={len(observed_statuses)} "
        f"clean_labels={len(clean_opcode_labels) + len(clean_message_labels) + len(clean_status_labels)}"
    )


@app.command("verify")
def provenance_verify(
    bundle: Optional[str] = typer.Argument(
        None, help="Path to a specific bundle directory, .tar.gz, or .zip. Omit to scan all bundles."
    ),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    if bundle:
        bundle_paths = [Path(bundle).expanduser().resolve()]
    else:
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

    results = {}
    for bundle_path in bundle_paths:
        if not bundle_path.exists():
            print(f"FAIL: Bundle not found: {bundle_path}")
            results[str(bundle_path)] = False
            continue
        result = _verify_single_bundle(bundle_path)
        results[str(bundle_path)] = result
        print()

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    if total > 1:
        print(f"{'=' * 60}")
        print(f"Total: {total} bundles, {passed} passed, {failed} failed")

    if failed > 0:
        raise typer.Exit(1)


@app.command("show")
def provenance_show(
    bundle: str = typer.Argument(..., help="Path to a bundle directory or .tar.gz file."),
):
    bundle_path = Path(bundle).expanduser().resolve()
    if not bundle_path.exists():
        print(f"Bundle not found: {bundle_path}", file=sys.stderr)
        raise typer.Exit(1)

    manifest, files = _load_bundle(bundle_path)

    scope = manifest.get("scope", {})
    markers = manifest.get("markers", [])
    samples = manifest.get("samples", [])
    artifacts = manifest.get("artifacts", [])

    print(f"Session #{manifest.get('session_id', '?')}")
    print(f"  Name:        {manifest.get('session_name', '')}")
    print(f"  Device:      {scope.get('device_name', '')} ({scope.get('device_ip', '')})")
    print(f"  Started:     {manifest.get('started_iso', '')}")
    print(f"  Ended:       {manifest.get('ended_iso', '')}")
    print(
        f"  Packets:     {manifest.get('session_packet_count', manifest.get('count', 0))} session, {manifest.get('evidence_packet_count', 0)} evidence"
    )
    print(f"  Markers:     {len(markers)}")
    print(f"  Artifacts:   {len(artifacts)}")

    if not markers and not samples and not artifacts:
        return

    print("\nTimeline:")
    print(f"{'Type':14s}  {'Timestamp':26s}  {'Label':34s}")
    print("-" * 80)

    sample_by_id = {s.get("packet_id"): s for s in samples}

    for marker in markers:
        marker_type = marker.get("marker_type", "?")
        ts = marker.get("timestamp_iso", "")
        label = marker.get("label", "")
        note = marker.get("note")
        marker_data = marker.get("data")

        print(f"{marker_type:14s}  {ts:26s}  {label}")

        if note:
            print(f"{'':14s}  {'':26s}  note: {note}")

        if marker_data and marker_type == "evidence" and marker_data.get("packet_ids"):
            packet_ids = marker_data["packet_ids"]
            filters = marker_data.get("filters", {})
            if filters:
                filter_parts = [f"{k}={v}" for k, v in filters.items()]
                print(f"{'':14s}  {'':26s}  filters: {', '.join(filter_parts)}")
            for pid in packet_ids[:20]:
                sample_entry = sample_by_id.get(pid)
                if sample_entry:
                    direction = sample_entry.get("direction") or "multicast"
                    opcode_hex = sample_entry.get("opcode_hex", "?")
                    src = f"{sample_entry.get('src_ip', '?')}:{sample_entry.get('src_port', '?')}"
                    dst = f"{sample_entry.get('dst_ip', '?')}:{sample_entry.get('dst_port', '?')}"
                    filename = sample_entry.get("file", "")
                    payload = files.get(filename)
                    size = f"{len(payload)}B" if payload else "?"
                    print(f"{'':14s}  {'':26s}    #{pid} {direction:9s} {opcode_hex} {src} -> {dst} {size}")
                    if payload:
                        print(_hexdump(payload, indent=f"{'':14s}  {'':26s}    "))
            if len(packet_ids) > 20:
                print(f"{'':14s}  {'':26s}    ... and {len(packet_ids) - 20} more")

    if artifacts:
        print("\nArtifacts:")
        for artifact in artifacts:
            print(
                f"  {artifact.get('label', '?')} [{artifact.get('role', '?')}] "
                f"{artifact.get('file', '?')} sha256={artifact.get('sha256', '?')}"
            )


@app.command("audit")
def provenance_audit(
    bundle: Optional[str] = typer.Argument(
        None, help="Path to a bundle directory or .tar.gz. Omit to scan all bundles."
    ),
    fixtures_root: Optional[str] = typer.Option(
        None, "--fixtures-root", help="Fixture root containing provenance session dirs."
    ),
):
    if bundle:
        bundle_paths = [Path(bundle).expanduser().resolve()]
    else:
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

    results = {}
    for bundle_path in bundle_paths:
        if not bundle_path.exists():
            print(f"FAIL: Bundle not found: {bundle_path}")
            results[str(bundle_path)] = False
            continue
        result = _audit_single_bundle(bundle_path)
        results[str(bundle_path)] = result
        print()

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    if total > 1:
        print(f"{'=' * 72}")
        print(f"Total: {total} bundles, {passed} passed, {failed} failed")

    if failed > 0:
        raise typer.Exit(1)


@app.command("export")
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
        print(f"Capture: Exported bundle: {bundle_path}")
        if store.get_session_evidence_count(resolved_session_id) == 0:
            print(
                "Capture: Warning: no packet evidence was tagged; this is a local draft, not a promotable provenance bundle.",
                file=sys.stderr,
            )
    finally:
        store.close()
