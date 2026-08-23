from __future__ import annotations

import struct
import sys

import typer

from netaudio.commands.capture_helpers import _resolve_facts_path
from netaudio.icons import icon


SESSION_SELECTORS = {"active", "latest"}


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
    except Exception as exception:
        print(f"Capture: unable to open the packet database: {exception}", file=sys.stderr)
        raise typer.Exit(1)


def _resolve_evidence_sessions(
    evidence_refs: list[str],
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
) -> list[str]:
    if not any(reference.split(":", 1)[0] in SESSION_SELECTORS for reference in evidence_refs):
        return evidence_refs

    store = _open_evidence_store(db=db, config=config, profile=profile)
    try:
        resolved = []
        for reference in evidence_refs:
            parts = reference.split(":")
            if len(parts) != 2:
                resolved.append(reference)
                continue

            session_selector, packet_id_string = parts
            if session_selector not in SESSION_SELECTORS:
                resolved.append(reference)
                continue

            if session_selector == "active":
                session = store.get_latest_session(active_only=True)
            else:
                session = store.get_latest_session(active_only=False)

            if not session or not session.get("name"):
                print(f"Capture: no {session_selector} session found for evidence reference.", file=sys.stderr)
                raise typer.Exit(1)

            resolved.append(f"{session['name']}:{packet_id_string}")
            print(f"  Resolved {session_selector} -> {session['name']}")
        return resolved
    finally:
        store.close()


def _validate_evidence_references(
    evidence_refs: list[str],
    db: str | None = None,
    config: str | None = None,
    profile: str | None = None,
) -> list[tuple[int, int]]:
    store = _open_evidence_store(db=db, config=config, profile=profile)
    try:
        evidence_targets = []
        for reference in evidence_refs:
            parts = reference.split(":")
            if len(parts) != 2:
                print(
                    f"Capture: invalid evidence reference '{reference}'; expected session_name:packet_id.",
                    file=sys.stderr,
                )
                raise typer.Exit(1)

            session_name, packet_id_string = parts
            try:
                packet_id = int(packet_id_string)
            except ValueError:
                print(f"Capture: invalid packet ID in evidence reference '{reference}'.", file=sys.stderr)
                raise typer.Exit(1)
            if packet_id <= 0:
                print(f"Capture: packet ID must be positive in evidence reference '{reference}'.", file=sys.stderr)
                raise typer.Exit(1)

            session = store.find_session_by_name(session_name, active_only=False)
            if not session:
                print(f"Capture: evidence session '{session_name}' was not found.", file=sys.stderr)
                raise typer.Exit(1)
            if not store.get_packet(packet_id):
                print(f"Capture: evidence packet #{packet_id} was not found.", file=sys.stderr)
                raise typer.Exit(1)

            evidence_targets.append((int(session["id"]), packet_id))
        return evidence_targets
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
):
    store = _open_evidence_store(db=db, config=config, profile=profile)
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
                print(f"  Marker: #{session_id} {label} already exists")
                continue
            store.add_marker(
                session_id=session_id,
                label=label,
                marker_type="evidence",
                summary=summary,
                data={"packet_ids": packet_ids, "fact": f"{category}:{key}"},
            )
            print(f"  Marker: #{session_id} {label}")
    finally:
        store.close()


async def _run_fact_verify(
    verify_plan: list[dict],
    device_ip: str,
    timeout: float,
    session_name: str,
    config: str | None,
    profile: str | None,
    db_override: str | None,
    auto_disprove: bool = False,
):
    import struct
    from netaudio.dante.fact_store import _field_applies_to_direction, _verify_field, disprove_fact
    from netaudio.dante.protocol_verifier import ProtocolVerifier

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

        passed = 0
        failed = 0
        timed_out = 0

        for entry in verify_plan:
            fact = entry["fact"]
            fk = entry["fact_key"]
            packet = entry["request_packet"]
            port = entry["port"]

            if len(packet) >= 6:
                original_txn = struct.unpack(">H", packet[4:6])[0]
                new_txn = (original_txn + 0x4000) & 0xFFFF
                packet = packet[:4] + struct.pack(">H", new_txn) + packet[6:]

            label = f"verify_{fk.replace(':', '_')}"
            response = await verifier.send(packet, port=port, timeout=timeout, label=label)

            if response is None:
                print(f"  {icon('timeout')}[TIMEOUT] {fk:30s} {fact['name']}")
                timed_out += 1
                verifier.observation(
                    f"{label}_timeout",
                    note=f"No response for {fk}",
                )
                continue

            fields = [field for field in fact.get("fields", []) if _field_applies_to_direction(field, "response")]
            all_ok = True
            has_bounds = False
            field_results = []

            for field_def in fields:
                result = _verify_field(response, field_def)
                field_results.append(result)
                if not result["ok"]:
                    if result.get("bounds"):
                        has_bounds = True
                    all_ok = False

            if all_ok:
                status_str = "PASS"
                passed += 1
            elif has_bounds and not any(not r["ok"] and not r.get("bounds") for r in field_results):
                status_str = "BOUNDS"
                failed += 1
            else:
                status_str = "FAIL"
                failed += 1

            print(f"  [{status_str:6s}] {fk:30s} {fact['name']}  ({len(response)}B)")

            for result in field_results:
                field_name = result["name"]
                if not result["ok"]:
                    if result.get("bounds"):
                        print(f"           {field_name:24s} BOUNDS  {result['error']}")
                    else:
                        print(f"           {field_name:24s} FAIL    {result.get('error', '')}")
                elif result.get("expected") is not None:
                    print(f"           {field_name:24s} {result['actual']:>16s} == {result['expected']}")
                else:
                    print(f"           {field_name:24s} {result['actual']:>16s}")

            verifier.observation(
                f"{label}_result",
                note=f"{fk}: {status_str}",
                data={
                    "status": status_str.lower(),
                    "response_len": len(response),
                    "fields": field_results,
                },
            )

            if auto_disprove and status_str in ("FAIL", "BOUNDS"):
                mismatches = [r for r in field_results if not r["ok"]]
                reasons = []
                for mismatch in mismatches:
                    if mismatch.get("bounds"):
                        reasons.append(f"{mismatch['name']}: out of bounds")
                    else:
                        reasons.append(f"{mismatch['name']}: {mismatch.get('error', 'mismatch')}")
                reason = f"Verification failed on {device_ip} ({len(response)}B response): {'; '.join(reasons)}"
                facts_path = _resolve_facts_path()
                disprove_fact(
                    facts_path,
                    category=fact["category"],
                    key=fact["key"],
                    reason=reason,
                    device_ip=device_ip,
                    response_size=len(response),
                    field_mismatches=mismatches,
                )
                print(f"           -> auto-disproved {fk}")

        print()
        total = passed + failed + timed_out
        parts = []
        if passed:
            parts.append(f"{passed} passed")
        if failed:
            parts.append(f"{failed} failed")
        if timed_out:
            parts.append(f"{timed_out} timed out")
        print(f"Verification complete: {total} facts — {', '.join(parts)}")

        verifier.marker(
            "fact_verify_finished",
            marker_type="system",
            note=f"Verification complete: {', '.join(parts)}",
            data={"passed": passed, "failed": failed, "timed_out": timed_out},
        )
