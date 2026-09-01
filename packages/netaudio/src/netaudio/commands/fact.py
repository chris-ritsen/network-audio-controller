from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS

from netaudio.capture.fact import (
    _build_spec_data,
    _spec_to_markdown,
    _spec_to_plain,
)
from netaudio.commands.capture_helpers import _parse_field_spec, _resolve_facts_path
from netaudio.capture.packets import _compact_hexdump


from netaudio.icons import icon

from netaudio.commands.fact_support import (
    _create_evidence_markers,
    _resolve_evidence_sessions,
    _run_fact_verify,
    _validate_evidence_references,
)


app = typer.Typer(
    help="Protocol fact registry — what we know and how we proved it.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


@app.command("add", help="Record a new protocol fact with evidence references.")
def fact_add(
    category: str = typer.Option(
        ..., "--category", "-c", help="Fact category (e.g. arc_opcode, conmon_message, multicast_announcement)."
    ),
    key: str = typer.Option(..., "--key", "-k", help="Unique key within category (e.g. 0x1001, 0x0081)."),
    name: str = typer.Option(..., "--name", help="Human-readable name for this protocol element."),
    note: Optional[str] = typer.Option(None, "--note", help="Short summary of what this does."),
    body: Optional[str] = typer.Option(
        None, "--body", help="Detailed content (markdown, structured text, JSON). Use --body-file for longer content."
    ),
    body_file: Optional[str] = typer.Option(None, "--body-file", help="Read body from file (use - for stdin)."),
    field: Optional[list[str]] = typer.Option(
        None,
        "--field",
        help="Field definition: direction:name:offset:length:type:value; direction and value are optional. Repeatable.",
    ),
    evidence: Optional[list[str]] = typer.Option(
        None,
        "--evidence",
        "-e",
        help="Evidence reference: session_name:packet_id. Repeatable.",
    ),
    confidence: str = typer.Option(
        "observed", "--confidence", help="Confidence level: verified, observed, inferred, uncertain."
    ),
    supersedes: Optional[str] = typer.Option(None, "--supersedes", help="Fact key this replaces (category:key)."),
    protocol: Optional[str] = typer.Option(
        None, "--protocol", help="Protocol ID this fact applies to (e.g. 0xFFFF, 0x2729). Enables auto-dissection."
    ),
    match: Optional[str] = typer.Option(
        None, "--match", help="Payload offset:size where the key value is found (e.g. 6:2). Enables auto-dissection."
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database containing evidence sessions."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.fact_store import add_fact

    facts_path = _resolve_facts_path()
    fields_parsed = [_parse_field_spec(f) for f in field] if field else []

    evidence_targets = []
    if evidence:
        evidence = _resolve_evidence_sessions(evidence, db=db, config=config, profile=profile)
        evidence_targets = _validate_evidence_references(evidence, db=db, config=config, profile=profile)
    if confidence == "verified" and not evidence:
        print(
            "Capture: verified facts require at least one session_name:packet_id evidence reference.", file=sys.stderr
        )
        raise typer.Exit(1)

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
            direction_str = f" [{f['direction']}]" if f.get("direction") else ""
            print(f"  Field{direction_str}: {f['name']} @ offset {f['offset']}, {f['length']}B {f['dtype']}{value_str}")
    if evidence:
        for ref in evidence:
            print(f"  Evidence: {ref}")
        _create_evidence_markers(
            evidence_targets,
            category,
            key,
            name,
            db=db,
            config=config,
            profile=profile,
        )
    if "history" in fact:
        print(f"  (updated existing fact, {len(fact['history'])} previous version(s))")


@app.command("update", help="Update the fields, evidence, or confidence of an existing fact.")
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
        help="Replace field definitions: direction:name:offset:length:type:value; direction and value are optional. Repeatable.",
    ),
    clear_fields: bool = typer.Option(
        False,
        "--clear-fields",
        help="Remove packet field definitions from this fact.",
    ),
    evidence: Optional[list[str]] = typer.Option(
        None,
        "--evidence",
        "-e",
        help="Add evidence reference: session_name:packet_id. Repeatable.",
    ),
    replace_evidence: bool = typer.Option(
        False,
        "--replace-evidence",
        help="Replace existing evidence references instead of appending.",
    ),
    confidence: Optional[str] = typer.Option(
        None, "--confidence", help="Update confidence level: verified, observed, inferred, uncertain."
    ),
    supersedes: Optional[str] = typer.Option(None, "--supersedes", help="Fact key this replaces (category:key)."),
    protocol: Optional[str] = typer.Option(None, "--protocol", help="Protocol ID (e.g. 0xFFFF, 0x2729)."),
    match: Optional[str] = typer.Option(None, "--match", help="Payload offset:size for auto-dissection (e.g. 6:2)."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database containing evidence sessions."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.fact_store import get_confidence, get_fact, update_fact

    facts_path = _resolve_facts_path()
    if clear_fields and field:
        print("Capture: --clear-fields cannot be combined with --field.", file=sys.stderr)
        raise typer.Exit(1)
    fields_parsed = [] if clear_fields else [_parse_field_spec(f) for f in field] if field else None

    evidence_targets = []
    if evidence:
        evidence = _resolve_evidence_sessions(evidence, db=db, config=config, profile=profile)
        evidence_targets = _validate_evidence_references(evidence, db=db, config=config, profile=profile)
    if confidence == "verified":
        existing_fact = get_fact(facts_path, category, key)
        existing_evidence = existing_fact.get("evidence", []) if existing_fact else []
        if not existing_evidence and not evidence:
            print(
                "Capture: verified facts require at least one session_name:packet_id evidence reference.",
                file=sys.stderr,
            )
            raise typer.Exit(1)

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
        replace_evidence=replace_evidence,
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
        _create_evidence_markers(
            evidence_targets,
            category,
            key,
            fact["name"],
            db=db,
            config=config,
            profile=profile,
        )
    if "history" in fact:
        print(f"  ({len(fact['history'])} revision(s))")


@app.command("list", help="List recorded facts, optionally filtered by category.")
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
                direction_str = f" [{f['direction']}]" if f.get("direction") else ""
                print(f"    field{direction_str}: {f['name']} @ {f['offset']}+{f['length']} {f['dtype']}{value_str}")
            for ref in fact.get("evidence", []):
                print(f"    evidence: {ref}")
            print()

    print(f"\n{len(facts)} facts ({'+' if category else 'all categories'})")


@app.command("show", help="Show one fact and optionally its full proof.")
def fact_show(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    prove: bool = typer.Option(
        False, "--prove", "-p", help="Show full proof: load evidence bundles, dissect packets, verify fields."
    ),
    provenance_dir: Optional[str] = typer.Option(
        None, "--provenance-dir", help="Path to provenance bundles directory."
    ),
):
    from netaudio.dante.fact_store import (
        get_fact,
        get_confidence,
        _parse_evidence_ref,
        _find_bundle,
        _field_applies_to_direction,
        _load_bundle,
        _verify_field,
    )

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
        print("  Fields:")
        for f in fact["fields"]:
            value_str = f" = {f['value']}" if "value" in f else ""
            direction_str = f"[{f['direction']}] " if f.get("direction") else ""
            print(
                f"    {direction_str}{f['name']:20s} offset {f['offset']:>4d}  {f['length']}B  {f['dtype']}{value_str}"
            )

    if fact.get("evidence"):
        print("  Evidence:")
        for ref in fact["evidence"]:
            print(f"    {ref}")

    if fact.get("disprovals"):
        print("  Disprovals:")
        for disproval in fact["disprovals"]:
            device = disproval.get("device_ip", "unknown device")
            reason = disproval.get("reason", "")
            resp_size = disproval.get("response_size")
            print(f"    {device}{f' ({resp_size}B response)' if resp_size else ''}: {reason}")
            for mismatch in disproval.get("field_mismatches", []):
                print(f"      {mismatch.get('name', '?')}: {mismatch.get('error', '')}")

    confidence_log = fact.get("confidence_log", [])
    if len(confidence_log) > 1:
        print("  Confidence log:")
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
                print("    (session-level evidence, no specific packet)")
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
            print("      Payload:")
            from netaudio.dante.packet_dissection_rendering import dissect_and_render

            print(dissect_and_render(payload, indent="        ", direction=direction))

            if fact.get("fields"):
                print()
                print("      Field verification:")
                for field_def in fact["fields"]:
                    if not _field_applies_to_direction(field_def, direction):
                        continue
                    result = _verify_field(payload, field_def)
                    if result["ok"]:
                        print(f"        [PASS] {result['name']}: {result['expected']} == {result['actual']}")
                    else:
                        print(f"        [FAIL] {result['error']}")

        print()
        print("=" * 80)


@app.command("check", help="Check every fact's evidence against stored provenance bundles.")
def fact_check(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Check only facts in this category."),
    prove: bool = typer.Option(
        False, "--prove", "-p", help="Show full proof: hexdump evidence packets and verify fields."
    ),
    provenance_dir: Optional[str] = typer.Option(
        None, "--provenance-dir", help="Path to provenance bundles directory."
    ),
):
    from netaudio.dante.fact_store import (
        check_facts,
        list_facts,
        get_fact,
        _parse_evidence_ref,
        _find_bundle,
        _field_applies_to_direction,
        _load_bundle,
        _verify_field,
    )

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
    quarantined = 0
    disproved = 0
    failed = 0

    for result in results:
        errors = result["errors"]
        verified = result["verified_fields"]

        if result.get("status") == "disproved":
            status = "DISP"
            status_icon = icon("fail")
            disproved += 1
        elif result.get("status") == "quarantined":
            status = "QUAR"
            status_icon = icon("warning")
            quarantined += 1
        elif not errors and verified:
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

        confidence_marker = {"verified": "+", "observed": "○", "inferred": "~", "uncertain": "?", "disproved": "x"}.get(
            result.get("confidence", ""), " "
        )
        print(f"  {status_icon}[{status}] {confidence_marker} {result['fact_key']:30s} {result['name']}")
        if result.get("quarantine_reason"):
            print(f"         quarantine: {result['quarantine_reason']}")

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
                    from netaudio.dante.packet_dissection_rendering import dissect_and_render

                    print(dissect_and_render(payload, indent="           ", direction=direction))

                    if fact.get("fields"):
                        for field_def in fact["fields"]:
                            if not _field_applies_to_direction(field_def, direction):
                                continue
                            field_result = _verify_field(payload, field_def)
                            if field_result["ok"]:
                                print(
                                    f"           [PASS] {field_result['name']}: {field_result['expected']} == {field_result['actual']}"
                                )
                            else:
                                print(f"           [FAIL] {field_result['error']}")

            print()

    print()
    total = passed + warned + quarantined + disproved + failed
    print(
        f"{total} facts checked: {passed} passed, {warned} no fields to verify, "
        f"{quarantined} quarantined, {disproved} disproved, {failed} failed"
    )

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


@app.command("verify", help="Verify facts against a live device by sending their request packets.")
def fact_verify(
    device_ip: str = typer.Option(..., "--device-ip", "-d", help="Target device IP address."),
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    key: Optional[str] = typer.Option(None, "--key", "-k", help="Verify a single fact."),
    write: bool = typer.Option(False, "--write", help="Include write commands (dangerous)."),
    auto_disprove: bool = typer.Option(
        False, "--auto-disprove", help="Automatically disprove facts that fail verification."
    ),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout per packet."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be sent without sending."),
    session_name: Optional[str] = typer.Option(None, "--session-name", help="Override verification session name."),
    provenance_dir: Optional[str] = typer.Option(
        None, "--provenance-dir", help="Path to provenance bundles directory."
    ),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    from netaudio.dante.fact_store import (
        list_facts,
        _parse_evidence_ref,
        _find_bundle,
        _field_applies_to_direction,
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

        verify_plan.append(
            {
                "fact": fact,
                "fact_key": fk,
                "request_packet": request_packet,
                "port": request_port,
            }
        )

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
                field_names = [
                    field["name"] for field in fact["fields"] if _field_applies_to_direction(field, "response")
                ]
                if field_names:
                    print(f"    verify: {', '.join(field_names)}")
        print(f"\nDry run: {len(verify_plan)} packets would be sent.")
        return

    asyncio.run(
        _run_fact_verify(
            verify_plan=verify_plan,
            device_ip=device_ip,
            timeout=timeout,
            session_name=session_name or f"fact_verify_{device_ip.replace('.', '_')}",
            config=config,
            profile=profile,
            db_override=db,
            auto_disprove=auto_disprove,
        )
    )


@app.command("spec", help="Render the fact registry as a protocol specification document.")
def fact_spec(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    output: Optional[str] = typer.Option(None, "--output", help="Write to file instead of stdout."),
    markdown: bool = typer.Option(False, "--markdown", "--md", help="Force markdown output."),
    prove: bool = typer.Option(
        False, "--prove", "-p", help="Annotate with provenance: evidence bundles and packet IDs."
    ),
):
    from netaudio._common_output import output_single
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

    spec_data = _build_spec_data(facts_path, category_filter=category, include_provenance=prove)

    if markdown or (output and output.endswith(".md")):
        text = _spec_to_markdown(spec_data)
    elif cli_state.output_format in (OutputFormat.json, OutputFormat.yaml):
        output_single(spec_data)
        return
    else:
        text = _spec_to_plain(spec_data, facts_path=facts_path)

    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(text)
        print(f"Wrote spec to {output_path}", file=sys.stderr)
    else:
        print(text)


from netaudio.commands.fact_lifecycle import (
    fact_disprove,
    fact_quarantine,
    fact_reinstate,
    fact_remove,
    fact_unquarantine,
)

app.command("disprove", help="Mark a fact as disproved with a reason.")(fact_disprove)
app.command("quarantine", help="Mark a fact as temporarily uncheckable.")(fact_quarantine)
app.command("reinstate", help="Restore a disproved or quarantined fact.")(fact_reinstate)
app.command("remove", help="Delete a fact from the registry.")(fact_remove)
app.command("unquarantine", help="Return a quarantined fact to active status.")(fact_unquarantine)
