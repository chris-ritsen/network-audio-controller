from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.commands.capture_helpers import (
    _compact_hexdump,
    _hexdump,
    _parse_field_spec,
    _resolve_facts_path,
)


from netaudio.icons import icon

app = typer.Typer(help="Protocol fact registry — what we know and how we proved it.", no_args_is_help=True)

SESSION_SELECTORS = {"active", "latest"}


def _resolve_evidence_sessions(evidence_refs: list[str]) -> list[str]:
    from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config
    from netaudio.dante.packet_store import PacketStore

    try:
        profile_config, _ = load_capture_profile(None, None)
        db_path = resolve_db_from_config(None, profile_config)
        store = PacketStore(db_path=db_path)
    except Exception:
        return evidence_refs

    resolved = []
    for ref in evidence_refs:
        parts = ref.split(":")
        if len(parts) != 2:
            resolved.append(ref)
            continue

        session_selector, packet_id_str = parts
        if session_selector not in SESSION_SELECTORS:
            resolved.append(ref)
            continue

        if session_selector == "active":
            session = store.get_latest_session(active_only=True)
        else:
            session = store.get_latest_session(active_only=False)

        if session and session.get("name"):
            resolved.append(f"{session['name']}:{packet_id_str}")
            print(f"  Resolved {session_selector} -> {session['name']}")
        else:
            print(f"  Warning: no {session_selector} session found, keeping '{ref}'", file=sys.stderr)
            resolved.append(ref)

    store.close()
    return resolved


def _create_evidence_markers(evidence_refs: list[str], category: str, key: str, name: str):
    from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config
    from netaudio.dante.packet_store import PacketStore

    try:
        profile_config, _ = load_capture_profile(None, None)
        db_path = resolve_db_from_config(None, profile_config)
        store = PacketStore(db_path=db_path)
    except Exception:
        return

    for ref in evidence_refs:
        parts = ref.split(":")
        if len(parts) != 2:
            continue

        session_name, packet_id_str = parts
        try:
            packet_id = int(packet_id_str)
        except ValueError:
            continue

        session = store.find_session_by_name(session_name, active_only=False)
        if not session:
            continue

        session_id = session["id"]
        packet = store.get_packet(packet_id)
        if not packet:
            continue

        label = f"evidence_{category}_{key}_{packet_id}"
        summary = f"{category}:{key} ({name}) — packet #{packet_id}"

        store.add_marker(
            session_id=session_id,
            label=label,
            marker_type="evidence",
            summary=summary,
            data={"packet_id": packet_id, "fact": f"{category}:{key}"},
        )
        print(f"  Marker: #{session_id} {label}")

    store.close()


@app.command("add")
def fact_add(
    category: str = typer.Option(..., "--category", "-c", help="Fact category (e.g. arc_opcode, conmon_message, multicast_announcement)."),
    key: str = typer.Option(..., "--key", "-k", help="Unique key within category (e.g. 0x1001, 0x0081)."),
    name: str = typer.Option(..., "--name", help="Human-readable name for this protocol element."),
    note: Optional[str] = typer.Option(None, "--note", help="Short summary of what this does."),
    body: Optional[str] = typer.Option(None, "--body", help="Detailed content (markdown, structured text, JSON). Use --body-file for longer content."),
    body_file: Optional[str] = typer.Option(None, "--body-file", help="Read body from file (use - for stdin)."),
    field: Optional[list[str]] = typer.Option(
        None,
        "--field",
        help="Field definition: name:offset:length:type[:expected_value]. Repeatable.",
    ),
    evidence: Optional[list[str]] = typer.Option(
        None,
        "--evidence",
        "-e",
        help="Evidence reference: session_name:packet_id or just session_name. Repeatable.",
    ),
    confidence: str = typer.Option("verified", "--confidence", help="Confidence level: verified, inferred, uncertain."),
    supersedes: Optional[str] = typer.Option(None, "--supersedes", help="Fact key this replaces (category:key)."),
    protocol: Optional[str] = typer.Option(None, "--protocol", help="Protocol ID this fact applies to (e.g. 0xFFFF, 0x2729). Enables auto-dissection."),
    match: Optional[str] = typer.Option(None, "--match", help="Payload offset:size where the key value is found (e.g. 6:2). Enables auto-dissection."),
):
    from netaudio.dante.fact_store import add_fact

    facts_path = _resolve_facts_path()
    fields_parsed = [_parse_field_spec(f) for f in field] if field else []

    if evidence:
        evidence = _resolve_evidence_sessions(evidence)

    resolved_body = body
    if body_file:
        if body_file == "-":
            resolved_body = sys.stdin.read()
        else:
            body_path = Path(body_file)
            if not body_path.exists():
                print(f"Capture: body file not found: {body_file}", file=sys.stderr)
                raise typer.Exit(1)
            resolved_body = body_path.read_text()

    parsed_protocol_id = None
    if protocol:
        parts = [p.strip() for p in protocol.split(",")]
        if len(parts) == 1:
            parsed_protocol_id = int(parts[0], 0)
        else:
            parsed_protocol_id = [int(p, 0) for p in parts]

    parsed_match_offset = None
    parsed_match_size = None
    if match:
        match_parts = match.split(":")
        parsed_match_offset = int(match_parts[0])
        parsed_match_size = int(match_parts[1]) if len(match_parts) > 1 else 2

    fact = add_fact(
        path=facts_path,
        category=category,
        key=key,
        name=name,
        note=note,
        body=resolved_body,
        fields=fields_parsed,
        evidence=evidence or [],
        confidence=confidence,
        supersedes=supersedes,
        protocol_id=parsed_protocol_id,
        match_offset=parsed_match_offset,
        match_size=parsed_match_size,
    )

    print(f"{icon('info')}Fact: {category}:{key} = {name}")
    print(f"  Confidence: {confidence}")
    if note:
        print(f"  Note: {note}")
    if resolved_body:
        body_lines = resolved_body.splitlines()
        if len(body_lines) <= 3:
            for line in body_lines:
                print(f"  {line}")
        else:
            print(f"  Body: {len(body_lines)} lines")
    if fields_parsed:
        for f in fields_parsed:
            value_str = f" = {f['value']}" if "value" in f else ""
            print(f"  Field: {f['name']} @ offset {f['offset']}, {f['length']}B {f['dtype']}{value_str}")
    if evidence:
        for ref in evidence:
            print(f"  Evidence: {ref}")
        _create_evidence_markers(evidence, category, key, name)
    if "history" in fact:
        print(f"  (updated existing fact, {len(fact['history'])} previous version(s))")


@app.command("update")
def fact_update(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    name: Optional[str] = typer.Option(None, "--name", help="Update human-readable name."),
    note: Optional[str] = typer.Option(None, "--note", help="Update note."),
    body: Optional[str] = typer.Option(None, "--body", help="Update body content."),
    body_file: Optional[str] = typer.Option(None, "--body-file", help="Read body from file (use - for stdin)."),
    field: Optional[list[str]] = typer.Option(
        None,
        "--field",
        help="Replace field definitions: name:offset:length:type[:expected_value]. Repeatable.",
    ),
    evidence: Optional[list[str]] = typer.Option(
        None,
        "--evidence",
        "-e",
        help="Add evidence reference: session_name:packet_id. Repeatable.",
    ),
    confidence: Optional[str] = typer.Option(None, "--confidence", help="Update confidence level: verified, observed, inferred, uncertain."),
    supersedes: Optional[str] = typer.Option(None, "--supersedes", help="Fact key this replaces (category:key)."),
    protocol: Optional[str] = typer.Option(None, "--protocol", help="Protocol ID (e.g. 0xFFFF, 0x2729)."),
    match: Optional[str] = typer.Option(None, "--match", help="Payload offset:size for auto-dissection (e.g. 6:2)."),
):
    from netaudio.dante.fact_store import update_fact, get_confidence

    facts_path = _resolve_facts_path()
    fields_parsed = [_parse_field_spec(f) for f in field] if field else None

    if evidence:
        evidence = _resolve_evidence_sessions(evidence)

    resolved_body = body
    if body_file:
        if body_file == "-":
            resolved_body = sys.stdin.read()
        else:
            body_path = Path(body_file)
            if not body_path.exists():
                print(f"Capture: body file not found: {body_file}", file=sys.stderr)
                raise typer.Exit(1)
            resolved_body = body_path.read_text()

    parsed_protocol_id = None
    if protocol:
        parts = [p.strip() for p in protocol.split(",")]
        if len(parts) == 1:
            parsed_protocol_id = int(parts[0], 0)
        else:
            parsed_protocol_id = [int(p, 0) for p in parts]

    parsed_match_offset = None
    parsed_match_size = None
    if match:
        match_parts = match.split(":")
        parsed_match_offset = int(match_parts[0])
        parsed_match_size = int(match_parts[1]) if len(match_parts) > 1 else 2

    fact = update_fact(
        path=facts_path,
        category=category,
        key=key,
        name=name,
        note=note,
        body=resolved_body,
        fields=fields_parsed,
        evidence=evidence,
        confidence=confidence,
        supersedes=supersedes,
        protocol_id=parsed_protocol_id,
        match_offset=parsed_match_offset,
        match_size=parsed_match_size,
    )

    if fact is None:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)

    print(f"{icon('info')}Updated: {category}:{key} = {fact['name']}")
    print(f"  Confidence: {get_confidence(fact)}")
    if fact.get("note"):
        print(f"  Note: {fact['note']}")
    if evidence:
        for ref in evidence:
            print(f"  Evidence: {ref}")
        _create_evidence_markers(evidence, category, key, fact["name"])
    if "history" in fact:
        print(f"  ({len(fact['history'])} revision(s))")


@app.command("list")
def fact_list(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Filter by category."),
):
    from netaudio.cli import state
    from netaudio.dante.fact_store import list_facts, get_categories, get_confidence

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        print("No facts registered yet.")
        return

    if category is None:
        categories = get_categories(facts_path)
        if not categories:
            print("No facts registered yet.")
            return
        print(f"Categories: {', '.join(categories)}")
        print()

    facts = list_facts(facts_path, category=category)

    if not facts:
        if category:
            print(f"No facts in category '{category}'.")
        return

    current_category = None
    for fact in facts:
        if fact["category"] != current_category:
            current_category = fact["category"]
            print(f"[{current_category}]")

        fact_confidence = get_confidence(fact)
        confidence_marker = {"verified": "+", "observed": "○", "inferred": "~", "uncertain": "?", "disproved": "✗"}.get(
            fact_confidence, " "
        )
        suffix = ""
        if fact_confidence == "disproved":
            disprovals = fact.get("disprovals", [])
            if disprovals:
                latest = disprovals[-1]
                device = latest.get("device_ip", "")
                suffix = f"  (disproved{' on ' + device if device else ''})"
        print(f"  {confidence_marker} {fact['key']:16s} {fact['name']}{suffix}")

        if state.verbose:
            if fact.get("note"):
                print(f"    {fact['note']}")
            if fact.get("body"):
                for line in fact["body"].splitlines():
                    print(f"    {line}")
            for f in fact.get("fields", []):
                value_str = f" = {f['value']}" if "value" in f else ""
                print(f"    field: {f['name']} @ {f['offset']}+{f['length']} {f['dtype']}{value_str}")
            for ref in fact.get("evidence", []):
                print(f"    evidence: {ref}")
            print()

    print(f"\n{len(facts)} facts ({'+' if category else 'all categories'})")


@app.command("show")
def fact_show(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    prove: bool = typer.Option(False, "--prove", "-p", help="Show full proof: load evidence bundles, dissect packets, verify fields."),
    provenance_dir: Optional[str] = typer.Option(None, "--provenance-dir", help="Path to provenance bundles directory."),
):
    from netaudio.dante.fact_store import get_fact, get_confidence, _parse_evidence_ref, _find_bundle, _load_bundle, _verify_field

    facts_path = _resolve_facts_path()
    fact = get_fact(facts_path, category, key)

    if not fact:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)

    print(f"{icon('info')}Fact: {fact['category']}:{fact['key']}")
    print(f"  Name:       {fact['name']}")
    print(f"  Confidence: {get_confidence(fact)}")
    if fact.get("note"):
        print(f"  Note:       {fact['note']}")
    if fact.get("supersedes"):
        print(f"  Supersedes: {fact['supersedes']}")

    if fact.get("body"):
        print()
        for line in fact["body"].splitlines():
            print(f"  {line}")
        print()

    if fact.get("fields"):
        print(f"  Fields:")
        for f in fact["fields"]:
            value_str = f" = {f['value']}" if "value" in f else ""
            print(f"    {f['name']:20s} offset {f['offset']:>4d}  {f['length']}B  {f['dtype']}{value_str}")

    if fact.get("evidence"):
        print(f"  Evidence:")
        for ref in fact["evidence"]:
            print(f"    {ref}")

    if fact.get("disprovals"):
        print(f"  Disprovals:")
        for disproval in fact["disprovals"]:
            device = disproval.get("device_ip", "unknown device")
            reason = disproval.get("reason", "")
            resp_size = disproval.get("response_size")
            print(f"    {device}{f' ({resp_size}B response)' if resp_size else ''}: {reason}")
            for mismatch in disproval.get("field_mismatches", []):
                print(f"      {mismatch.get('name', '?')}: {mismatch.get('error', '')}")

    confidence_log = fact.get("confidence_log", [])
    if len(confidence_log) > 1:
        print(f"  Confidence log:")
        for entry in confidence_log:
            timestamp_ns = entry.get("timestamp_ns", 0)
            timestamp_s = timestamp_ns / 1_000_000_000 if timestamp_ns else 0
            from datetime import datetime
            timestamp_str = datetime.fromtimestamp(timestamp_s).strftime("%Y-%m-%d %H:%M") if timestamp_s else "?"
            print(f"    {timestamp_str}  {entry['level']}")

    if fact.get("history"):
        print(f"  History ({len(fact['history'])} revision(s)):")
        for entry in fact["history"]:
            action = entry.get("action", "updated")
            print(f"    {action}: was {entry.get('previous_name')} [{entry.get('previous_confidence')}]")

    if prove and fact.get("evidence"):
        prov_dir = Path(provenance_dir) if provenance_dir else facts_path.parent
        print()
        print("Proof:")
        print("=" * 80)

        for ref in fact["evidence"]:
            session_ref, packet_id_str = _parse_evidence_ref(ref)
            if session_ref is None:
                print(f"  [ERROR] invalid evidence ref: {ref}")
                continue

            bundle_path = _find_bundle(prov_dir, session_ref)
            if bundle_path is None:
                print(f"  [ERROR] bundle not found: {session_ref}")
                continue

            manifest, files = _load_bundle(bundle_path)
            print(f"\n  Bundle: {bundle_path.name}")

            if manifest.get("session"):
                session_meta = manifest["session"]
                print(f"    Session: #{session_meta.get('id', '?')} {session_meta.get('name', '')}")
                print(f"    Started: {session_meta.get('started', '?')}")

            if packet_id_str is None:
                print(f"    (session-level evidence, no specific packet)")
                continue

            packet_id = int(packet_id_str)
            sample_by_id = {s.get("packet_id"): s for s in manifest.get("samples", [])}
            sample = sample_by_id.get(packet_id)

            if sample is None:
                print(f"    [ERROR] packet #{packet_id} not in bundle")
                continue

            direction = sample.get("direction") or "multicast"
            opcode_val = sample.get("opcode")
            opcode_str = f"0x{opcode_val:04X}" if isinstance(opcode_val, int) else str(opcode_val or "?")
            src = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
            dst = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"
            print(f"\n    Packet #{packet_id}  {direction}  opcode={opcode_str}")
            print(f"      {src} -> {dst}")

            filename = sample.get("file", "")
            payload = files.get(filename)

            if payload is None:
                print(f"      [ERROR] payload file missing: {filename}")
                continue

            print(f"      Size: {len(payload)}B")
            print(f"      Payload:")
            from netaudio.dante.packet_dissector import dissect_and_render
            print(dissect_and_render(payload, indent="        "))

            if fact.get("fields"):
                print()
                print(f"      Field verification:")
                for field_def in fact["fields"]:
                    result = _verify_field(payload, field_def)
                    if result["ok"]:
                        print(f"        [PASS] {result['name']}: {result['expected']} == {result['actual']}")
                    else:
                        print(f"        [FAIL] {result['error']}")

        print()
        print("=" * 80)


@app.command("check")
def fact_check(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Check only facts in this category."),
    prove: bool = typer.Option(False, "--prove", "-p", help="Show full proof: hexdump evidence packets and verify fields."),
    provenance_dir: Optional[str] = typer.Option(None, "--provenance-dir", help="Path to provenance bundles directory."),
):
    from netaudio.dante.fact_store import check_facts, list_facts, get_fact, _parse_evidence_ref, _find_bundle, _load_bundle, _verify_field

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        print("No facts registered yet.")
        return

    prov_dir = Path(provenance_dir) if provenance_dir else facts_path.parent
    results = check_facts(facts_path, provenance_dir=prov_dir)

    if category:
        all_facts = list_facts(facts_path, category=category)
        category_keys = {f"{f['category']}:{f['key']}" for f in all_facts}
        results = [r for r in results if r["fact_key"] in category_keys]

    passed = 0
    warned = 0
    failed = 0

    for result in results:
        errors = result["errors"]
        verified = result["verified_fields"]

        if not errors and verified:
            status = "PASS"
            status_icon = icon("success")
            passed += 1
        elif not errors and not verified:
            status = "WARN"
            status_icon = icon("warning")
            warned += 1
        else:
            status = "FAIL"
            status_icon = icon("fail")
            failed += 1

        confidence_marker = {"verified": "+", "observed": "○", "inferred": "~", "uncertain": "?"}.get(
            result.get("confidence", ""), " "
        )
        print(f"  {status_icon}[{status}] {confidence_marker} {result['fact_key']:30s} {result['name']}")

        for v in verified:
            if v.get("expected") is not None:
                print(f"         field {v['name']}: {v['expected']} == {v['actual']}")
            else:
                print(f"         field {v['name']}: {v['actual']}")

        for err in errors:
            print(f"         {err}")

        if prove:
            fact = get_fact(facts_path, result["category"], result["key"])
            if fact and fact.get("evidence"):
                if fact.get("note"):
                    print(f"         note: {fact['note']}")

                for ref in fact["evidence"]:
                    session_ref, packet_id_str = _parse_evidence_ref(ref)
                    if session_ref is None:
                        continue

                    bundle_path = _find_bundle(prov_dir, session_ref)
                    if bundle_path is None:
                        continue

                    manifest, files = _load_bundle(bundle_path)

                    if packet_id_str is None:
                        print(f"         evidence: {ref} (session-level)")
                        continue

                    packet_id = int(packet_id_str)
                    sample_by_id = {s.get("packet_id"): s for s in manifest.get("samples", [])}
                    sample = sample_by_id.get(packet_id)

                    if sample is None:
                        continue

                    direction = sample.get("direction") or "multicast"
                    opcode_val = sample.get("opcode")
                    opcode_str = f"0x{opcode_val:04X}" if isinstance(opcode_val, int) else str(opcode_val or "?")
                    src = f"{sample.get('src_ip', '?')}:{sample.get('src_port', '?')}"
                    dst = f"{sample.get('dst_ip', '?')}:{sample.get('dst_port', '?')}"

                    filename = sample.get("file", "")
                    payload = files.get(filename)
                    if payload is None:
                        continue

                    print(f"\n         --- {ref} ---")
                    print(f"         Packet #{packet_id}  {direction}  opcode={opcode_str}  {len(payload)}B")
                    print(f"         {src} -> {dst}")
                    from netaudio.dante.packet_dissector import dissect_and_render
                    print(dissect_and_render(payload, indent="           "))

                    if fact.get("fields"):
                        for field_def in fact["fields"]:
                            field_result = _verify_field(payload, field_def)
                            if field_result["ok"]:
                                print(f"           [PASS] {field_result['name']}: {field_result['expected']} == {field_result['actual']}")
                            else:
                                print(f"           [FAIL] {field_result['error']}")

            print()

    print()
    total = passed + warned + failed
    print(f"{total} facts checked: {passed} passed, {warned} no fields to verify, {failed} failed")

    if failed > 0:
        raise typer.Exit(1)


QUERYABLE_FACTS = {
    "arc_opcode:0x1003",
    "arc_opcode:0x1100",
    "arc_opcode:0x2000",
    "arc_opcode:0x2010",
    "cmc_opcode:0x3010",
}

WRITABLE_FACTS = {
    "arc_opcode:0x1001",
    "arc_opcode:0x1101",
    "arc_opcode:0x3010",
    "conmon_message:0x0081",
}

REACTIVE_FACTS = {
    "conmon_message:0x0080",
    "conmon_message:0x0090",
    "conmon_message:0x0092",
    "protocol_structure:arc_header",
}

PORT_BY_CATEGORY = {
    "arc_opcode": 4440,
    "cmc_opcode": 8800,
}


@app.command("verify")
def fact_verify(
    device_ip: str = typer.Option(..., "--device-ip", "-d", help="Target device IP address."),
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    key: Optional[str] = typer.Option(None, "--key", "-k", help="Verify a single fact."),
    write: bool = typer.Option(False, "--write", help="Include write commands (dangerous)."),
    auto_disprove: bool = typer.Option(False, "--auto-disprove", help="Automatically disprove facts that fail verification."),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout per packet."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be sent without sending."),
    session_name: Optional[str] = typer.Option(None, "--session-name", help="Override verification session name."),
    provenance_dir: Optional[str] = typer.Option(None, "--provenance-dir", help="Path to provenance bundles directory."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.fact_store import (
        list_facts,
        _parse_evidence_ref,
        _find_bundle,
        _load_bundle,
    )

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        print("No facts registered yet.", file=sys.stderr)
        raise typer.Exit(1)

    prov_dir = Path(provenance_dir) if provenance_dir else facts_path.parent
    all_facts = list_facts(facts_path, category=category)

    if key:
        all_facts = [f for f in all_facts if f["key"] == key]

    allowed = set(QUERYABLE_FACTS)
    if write:
        allowed |= WRITABLE_FACTS

    verifiable = []
    for fact in all_facts:
        fk = f"{fact['category']}:{fact['key']}"
        if fk in REACTIVE_FACTS:
            continue
        if fk not in allowed:
            if not write:
                print(f"  [SKIP] {fk:30s} {fact['name']} (write command, use --write)")
            continue
        verifiable.append(fact)

    if not verifiable:
        print("No verifiable facts found.", file=sys.stderr)
        raise typer.Exit(1)

    verify_plan = []

    for fact in verifiable:
        fk = f"{fact['category']}:{fact['key']}"
        request_packet = None
        request_port = PORT_BY_CATEGORY.get(fact["category"], 4440)

        for ref in fact.get("evidence", []):
            session_ref, packet_id_str = _parse_evidence_ref(ref)
            if session_ref is None or packet_id_str is None:
                continue

            bundle_path = _find_bundle(prov_dir, session_ref)
            if bundle_path is None:
                continue

            manifest, files = _load_bundle(bundle_path)
            packet_id = int(packet_id_str)
            sample_by_id = {s.get("packet_id"): s for s in manifest.get("samples", [])}
            sample = sample_by_id.get(packet_id)

            if sample and sample.get("direction") == "request":
                filename = sample.get("file", "")
                payload = files.get(filename)
                if payload:
                    request_packet = payload
                    if sample.get("dst_port"):
                        request_port = sample["dst_port"]
                    break

        if request_packet is None:
            print(f"  [SKIP] {fk:30s} {fact['name']} (no request packet in evidence)")
            continue

        verify_plan.append({
            "fact": fact,
            "fact_key": fk,
            "request_packet": request_packet,
            "port": request_port,
        })

    if not verify_plan:
        print("No facts with request packets to verify.", file=sys.stderr)
        raise typer.Exit(1)

    print(f"Verifying {len(verify_plan)} facts against {device_ip}")
    print()

    if dry_run:
        for entry in verify_plan:
            fact = entry["fact"]
            fk = entry["fact_key"]
            packet = entry["request_packet"]
            port = entry["port"]
            print(f"  {fk:30s} -> {device_ip}:{port}  {len(packet)}B")
            for line in _compact_hexdump(packet, max_lines=2):
                print(line)
            if fact.get("fields"):
                field_names = [f["name"] for f in fact["fields"]]
                print(f"    verify: {', '.join(field_names)}")
        print(f"\nDry run: {len(verify_plan)} packets would be sent.")
        return

    asyncio.run(_run_fact_verify(
        verify_plan=verify_plan,
        device_ip=device_ip,
        timeout=timeout,
        session_name=session_name or f"fact_verify_{device_ip.replace('.', '_')}",
        config=config,
        profile=profile,
        db_override=db,
        auto_disprove=auto_disprove,
    ))


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
    from netaudio.dante.fact_store import _verify_field, disprove_fact
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

            fields = fact.get("fields", [])
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


_CATEGORY_TITLES = {
    "protocol_structure": "Protocol Structure",
    "arc_opcode": "ARC Protocol (Port 4440)",
    "cmc_opcode": "CMC Protocol (Port 8800)",
    "conmon_message": "Conmon Protocol (Port 8700/8702)",
}

_CATEGORY_ORDER = ["protocol_structure", "arc_opcode", "cmc_opcode", "conmon_message"]


def _category_sort_key(category: str) -> tuple[int, str]:
    try:
        return (_CATEGORY_ORDER.index(category), category)
    except ValueError:
        return (len(_CATEGORY_ORDER), category)


def _format_field_table(fields: list[dict]) -> str:
    lines = []
    lines.append("| Offset | Length | Type | Field | Value |")
    lines.append("|--------|--------|------|-------|-------|")
    for field in sorted(fields, key=lambda f: f.get("offset", 0)):
        value = field.get("value", "")
        lines.append(
            f"| {field['offset']} | {field['length']} | {field['dtype']} "
            f"| {field['name']} | {value} |"
        )
    return "\n".join(lines)


def _spec_overview() -> list[str]:
    """Generate the Overview section with transport fundamentals and constants."""
    lines = []
    lines.append("## Overview")
    lines.append("")
    lines.append("This documents the Dante **control protocol** — device discovery, configuration, "
                 "routing, and monitoring. It does not cover the audio transport (RTP/AES67). "
                 "All control traffic is **UDP** with **big-endian** (network byte order) encoding. "
                 "Strings are **null-terminated ASCII**.")
    lines.append("")
    lines.append("### Discovery")
    lines.append("")
    lines.append("Devices are discovered via mDNS (Bonjour). Browse these service types:")
    lines.append("")
    lines.append("| Service | Type |")
    lines.append("|---------|------|")
    lines.append("| ARC (Audio Routing & Control) | `_netaudio-arc._udp.local.` |")
    lines.append("| Channel | `_netaudio-chan._udp.local.` |")
    lines.append("| CMC (Control & Monitoring) | `_netaudio-cmc._udp.local.` |")
    lines.append("| DBC (Device Browsing) | `_netaudio-dbc._udp.local.` |")
    lines.append("")
    lines.append("The mDNS TXT record for `_netaudio-arc` contains the device's ARC port "
                 "(usually 4440 but can vary). The resolved IP address is the device's control address.")
    lines.append("")
    lines.append("### Ports")
    lines.append("")
    lines.append("| Port | Protocol | Direction | Description |")
    lines.append("|------|----------|-----------|-------------|")
    lines.append("| 4440 | ARC | Unicast | Audio routing, channel queries, subscriptions, device naming, latency |")
    lines.append("| 8700 | Conmon | Unicast | Device settings: sample rate, reboot, identify, gain, encoding |")
    lines.append("| 8702 | Conmon | Multicast | Notifications: device announcements, sample rate, config changes |")
    lines.append("| 8708 | Heartbeat | Multicast | Device heartbeat / presence (not used for control) |")
    lines.append("| 8800 | CMC | Unicast | Control & monitoring: registration, subscription status polling |")
    lines.append("| 8751 | Metering | Multicast | Audio level metering (device-configurable port) |")
    lines.append("")
    lines.append("### Multicast Groups")
    lines.append("")
    lines.append("| Address | Usage |")
    lines.append("|---------|-------|")
    lines.append("| 224.0.0.231 | Control/monitoring notifications (port 8702) |")
    lines.append("| 224.0.0.233 | Device heartbeat (port 8708) |")
    lines.append("")
    lines.append("### Request/Response Pattern")
    lines.append("")
    lines.append("Most ARC and CMC commands follow a request/response pattern over unicast UDP. "
                 "The response echoes the request's `transaction_id`. Some Conmon commands "
                 "(set_sample_rate, reboot, identify) are **fire-and-forget** — confirmation "
                 "arrives as a multicast notification burst on 224.0.0.231:8702.")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _build_spec_data(
    facts_path: Path,
    category_filter: Optional[str] = None,
) -> dict:
    from netaudio.dante.fact_store import list_facts, get_categories, get_confidence

    categories = get_categories(facts_path)
    if category_filter:
        categories = [category_filter]
    categories = sorted(categories, key=_category_sort_key)

    all_facts = list_facts(facts_path)
    publishable_facts = [f for f in all_facts if get_confidence(f) != "disproved"]

    confidence_counts = {"verified": 0, "observed": 0, "inferred": 0, "uncertain": 0}
    for fact in publishable_facts:
        conf = get_confidence(fact)
        if conf in confidence_counts:
            confidence_counts[conf] += 1

    seen_message_types: set[str] = set()

    spec_categories = []
    for cat in categories:
        category_facts = [f for f in publishable_facts if f["category"] == cat]
        if not category_facts:
            continue

        title = _CATEGORY_TITLES.get(cat, cat.replace("_", " ").title())
        entries = []
        for fact in category_facts:
            dedup_key = f"{fact['name']}:{fact['key']}"
            if dedup_key in seen_message_types:
                continue
            seen_message_types.add(dedup_key)

            confidence = get_confidence(fact)
            entry = {
                "key": fact["key"],
                "name": fact["name"],
                "category": cat,
                "confidence": confidence,
            }
            if fact.get("note"):
                entry["note"] = fact["note"]
            if fact.get("body"):
                entry["body"] = fact["body"]
            fields = fact.get("fields", [])
            if fields:
                entry["fields"] = [
                    {
                        "offset": field["offset"],
                        "length": field["length"],
                        "dtype": field["dtype"],
                        "name": field["name"],
                        "value": field.get("value", ""),
                    }
                    for field in sorted(fields, key=lambda f: f.get("offset", 0))
                ]
            entries.append(entry)

        spec_categories.append({
            "category": cat,
            "title": title,
            "facts": entries,
        })

    return {
        "title": "Dante Control Protocol Reference",
        "total": sum(confidence_counts.values()),
        "confidence": {k: v for k, v in confidence_counts.items() if v},
        "categories": spec_categories,
    }


def _spec_to_markdown(spec_data: dict) -> str:
    lines = []
    lines.append("# Dante Control Protocol Reference")
    lines.append("")

    total = spec_data["total"]
    confidence = spec_data["confidence"]
    num_categories = len(spec_data["categories"])
    summary_parts = [f"{level}: {count}" for level, count in confidence.items()]
    lines.append(f"**{total} documented protocol elements** across {num_categories} categories.")
    if summary_parts:
        lines.append(" | ".join(summary_parts))
    lines.append("")
    lines.append("---")
    lines.append("")

    lines.extend(_spec_overview())

    for cat_data in spec_data["categories"]:
        lines.append(f"## {cat_data['title']}")
        lines.append("")

        for fact in cat_data["facts"]:
            lines.append(f"### {fact['key']} — {fact['name']}")
            lines.append("")

            if fact["confidence"] in ("inferred", "observed", "uncertain"):
                lines.append(f"*Status: {fact['confidence']}*")
                lines.append("")

            if fact.get("note"):
                lines.append(fact["note"])
                lines.append("")

            if fact.get("body"):
                for body_line in fact["body"].splitlines():
                    if body_line.startswith("#"):
                        body_line = "##" + body_line
                    lines.append(body_line)
                lines.append("")

            fields = fact.get("fields", [])
            if fields and not fact.get("body"):
                lines.append("#### Fields")
                lines.append("")
                lines.append(_format_field_table(fields))
                lines.append("")

    return "\n".join(lines)


def _spec_to_plain(spec_data: dict, terminal_width: int = 120) -> str:
    import shutil
    import textwrap

    terminal_width = shutil.get_terminal_size((120, 24)).columns

    lines = []
    lines.append("\033[1mDante Control Protocol Reference\033[0m")
    lines.append("")

    total = spec_data["total"]
    confidence = spec_data["confidence"]
    summary_parts = [f"\033[1m{count}\033[0m {level}" for level, count in confidence.items()]
    lines.append(f"{total} documented protocol elements across {len(spec_data['categories'])} categories")
    lines.append("  " + "  ".join(summary_parts))
    lines.append("")

    _CONFIDENCE_COLORS = {
        "verified": "\033[32m",
        "observed": "\033[33m",
        "inferred": "\033[90m",
        "uncertain": "\033[91m",
    }

    for cat_index, cat_data in enumerate(spec_data["categories"]):
        if cat_index > 0:
            lines.append("")
        lines.append(f"\033[1;4m{cat_data['title']}\033[0m")
        lines.append("")

        for fact_index, fact in enumerate(cat_data["facts"]):
            if fact_index > 0:
                lines.append("")

            confidence_val = fact["confidence"]
            confidence_color = _CONFIDENCE_COLORS.get(confidence_val, "")
            confidence_tag = ""
            if confidence_val in ("inferred", "observed", "uncertain"):
                confidence_tag = f"  {confidence_color}{confidence_val}\033[0m"

            lines.append(f"  \033[1m{fact['key']}\033[0m  {fact['name']}{confidence_tag}")

            if fact.get("note"):
                lines.append("")
                note_wrapped = textwrap.fill(
                    fact["note"],
                    width=terminal_width,
                    initial_indent="    ",
                    subsequent_indent="    ",
                )
                lines.append(f"\033[90m{note_wrapped}\033[0m")

            fields = fact.get("fields", [])
            if fields and not fact.get("body"):
                lines.append("")
                max_name_len = max(len(field["name"]) for field in fields)
                max_type_len = max(len(field["dtype"]) for field in fields)
                for field in fields:
                    value_str = f"  \033[36m{field['value']}\033[0m" if field.get("value") else ""
                    lines.append(
                        f"    \033[90m{field['offset']:3d}:{field['offset'] + field['length']:<3d}\033[0m"
                        f"  {field['dtype']:<{max_type_len}s}"
                        f"  {field['name']:<{max_name_len}s}"
                        f"{value_str}"
                    )

            if fact.get("body"):
                lines.append("")
                body_lines = fact["body"].splitlines()
                for body_line in body_lines:
                    if body_line.startswith("#") or body_line.startswith("|"):
                        continue
                    stripped = body_line.strip()
                    if stripped:
                        wrapped = textwrap.fill(
                            stripped,
                            width=terminal_width,
                            initial_indent="    ",
                            subsequent_indent="    ",
                        )
                        lines.append(f"\033[90m{wrapped}\033[0m")

        lines.append("")

    return "\n".join(lines)


@app.command("spec")
def fact_spec(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    output: Optional[str] = typer.Option(None, "--output", help="Write to file instead of stdout."),
    markdown: bool = typer.Option(False, "--markdown", "--md", help="Force markdown output."),
):
    from netaudio._common import output_single
    from netaudio.cli import OutputFormat, state as cli_state

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        print("No facts registered yet.", file=sys.stderr)
        raise typer.Exit(1)

    from netaudio.dante.fact_store import get_categories
    categories = get_categories(facts_path)
    if not categories:
        print("No facts registered yet.", file=sys.stderr)
        raise typer.Exit(1)

    spec_data = _build_spec_data(facts_path, category_filter=category)

    if markdown or (output and output.endswith(".md")):
        text = _spec_to_markdown(spec_data)
    elif cli_state.output_format in (OutputFormat.json, OutputFormat.yaml):
        output_single(spec_data)
        return
    else:
        text = _spec_to_plain(spec_data)

    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(text)
        print(f"Wrote spec to {output_path}", file=sys.stderr)
    else:
        print(text)


@app.command("remove")
def fact_remove(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
):
    from netaudio.dante.fact_store import remove_fact

    facts_path = _resolve_facts_path()
    removed = remove_fact(facts_path, category, key)

    if removed:
        print(f"{icon('remove')}Removed: {category}:{key}")
    else:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)


@app.command("disprove")
def fact_disprove(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    reason: str = typer.Option(..., "--reason", help="Why this fact is wrong."),
    device_ip: Optional[str] = typer.Option(None, "--device-ip", "-d", help="Device that disproved it."),
):
    from netaudio.dante.fact_store import disprove_fact

    facts_path = _resolve_facts_path()
    result = disprove_fact(
        facts_path,
        category=category,
        key=key,
        reason=reason,
        device_ip=device_ip,
    )

    if result:
        print(f"{icon('fail')}Disproved: {category}:{key}")
        print(f"  Reason: {reason}")
        if device_ip:
            print(f"  Device: {device_ip}")
    else:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)


@app.command("reinstate")
def fact_reinstate(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    confidence: str = typer.Option("verified", "--confidence", help="New confidence level."),
    note: Optional[str] = typer.Option(None, "--note", help="Updated note."),
):
    from netaudio.dante.fact_store import reinstate_fact

    facts_path = _resolve_facts_path()
    result = reinstate_fact(
        facts_path,
        category=category,
        key=key,
        confidence=confidence,
        note=note,
    )

    if result:
        print(f"{icon('success')}Reinstated: {category}:{key} (confidence={confidence})")
    else:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)
