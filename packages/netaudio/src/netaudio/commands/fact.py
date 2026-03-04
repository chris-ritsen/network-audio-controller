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


app = typer.Typer(help="Protocol fact registry — what we know and how we proved it.", no_args_is_help=True)


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
):
    from netaudio_lib.dante.fact_store import add_fact

    facts_path = _resolve_facts_path()
    fields_parsed = [_parse_field_spec(f) for f in field] if field else []

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
    )

    print(f"Fact: {category}:{key} = {name}")
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
    if "history" in fact:
        print(f"  (updated existing fact, {len(fact['history'])} previous version(s))")


@app.command("list")
def fact_list(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Filter by category."),
):
    from netaudio.cli import state
    from netaudio_lib.dante.fact_store import list_facts, get_categories

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

        confidence_marker = {"verified": "+", "inferred": "~", "uncertain": "?", "disproved": "✗"}.get(
            fact.get("confidence", ""), " "
        )
        suffix = ""
        if fact.get("confidence") == "disproved":
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
    from netaudio_lib.dante.fact_store import get_fact, _parse_evidence_ref, _find_bundle, _load_bundle, _verify_field

    facts_path = _resolve_facts_path()
    fact = get_fact(facts_path, category, key)

    if not fact:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)

    print(f"Fact: {fact['category']}:{fact['key']}")
    print(f"  Name:       {fact['name']}")
    print(f"  Confidence: {fact.get('confidence', 'unknown')}")
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
            from netaudio_lib.dante.packet_dissector import dissect_and_render
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
    from netaudio_lib.dante.fact_store import check_facts, list_facts, get_fact, _parse_evidence_ref, _find_bundle, _load_bundle, _verify_field

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
            passed += 1
        elif not errors and not verified:
            status = "WARN"
            warned += 1
        else:
            status = "FAIL"
            failed += 1

        confidence_marker = {"verified": "+", "inferred": "~", "uncertain": "?"}.get(
            result.get("confidence", ""), " "
        )
        print(f"  [{status}] {confidence_marker} {result['fact_key']:30s} {result['name']}")

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
                    from netaudio_lib.dante.packet_dissector import dissect_and_render
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
    from netaudio_lib.dante.fact_store import (
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
    from netaudio_lib.dante.fact_store import _verify_field, disprove_fact
    from netaudio_lib.dante.protocol_verifier import ProtocolVerifier

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
                print(f"  [TIMEOUT] {fk:30s} {fact['name']}")
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


def _format_evidence_list(evidence: list[str]) -> str:
    lines = []
    for ref in evidence:
        parts = ref.split(":")
        if len(parts) == 2:
            lines.append(f"- `{parts[0]}` packet #{parts[1]}")
        else:
            lines.append(f"- `{ref}`")
    return "\n".join(lines)


@app.command("spec")
def fact_spec(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write to file instead of stdout."),
):
    from netaudio_lib.dante.fact_store import list_facts, get_categories

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        print("No facts registered yet.", file=sys.stderr)
        raise typer.Exit(1)

    categories = get_categories(facts_path)
    if not categories:
        print("No facts registered yet.", file=sys.stderr)
        raise typer.Exit(1)

    if category:
        categories = [category]

    categories = sorted(categories, key=_category_sort_key)

    lines = []
    lines.append("# Dante Protocol Reference")
    lines.append("")
    lines.append("Derived from observed wire traffic. Every fact is backed by captured packet evidence.")
    lines.append("")

    all_facts = list_facts(facts_path)
    all_sessions = set()
    for fact in all_facts:
        for ref in fact.get("evidence", []):
            session_ref = ref.split(":")[0]
            all_sessions.add(session_ref)

    confidence_counts = {"verified": 0, "inferred": 0, "uncertain": 0}
    for fact in all_facts:
        conf = fact.get("confidence", "unknown")
        if conf in confidence_counts:
            confidence_counts[conf] += 1

    lines.append(f"**{len(all_facts)} facts** across {len(categories)} categories, "
                 f"backed by {len(all_sessions)} capture sessions.")
    lines.append(f"Verified: {confidence_counts['verified']} | "
                 f"Inferred: {confidence_counts['inferred']} | "
                 f"Uncertain: {confidence_counts['uncertain']}")
    lines.append("")
    lines.append("---")
    lines.append("")

    for cat in categories:
        facts = list_facts(facts_path, category=cat)
        if not facts:
            continue

        title = _CATEGORY_TITLES.get(cat, cat.replace("_", " ").title())
        lines.append(f"## {title}")
        lines.append("")

        for fact in facts:
            confidence = fact.get("confidence", "unknown")
            confidence_badge = {"verified": "[verified]", "inferred": "[inferred]", "uncertain": "[uncertain]"}.get(
                confidence, f"[{confidence}]"
            )

            lines.append(f"### {fact['key']} — {fact['name']} {confidence_badge}")
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

            evidence_refs = fact.get("evidence", [])
            if evidence_refs:
                lines.append("#### Evidence")
                lines.append("")
                lines.append(_format_evidence_list(evidence_refs))
                lines.append("")

    text = "\n".join(lines)

    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(text)
        print(f"Wrote spec to {output_path}")
    else:
        print(text)


@app.command("remove")
def fact_remove(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
):
    from netaudio_lib.dante.fact_store import remove_fact

    facts_path = _resolve_facts_path()
    removed = remove_fact(facts_path, category, key)

    if removed:
        print(f"Removed: {category}:{key}")
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
    from netaudio_lib.dante.fact_store import disprove_fact

    facts_path = _resolve_facts_path()
    result = disprove_fact(
        facts_path,
        category=category,
        key=key,
        reason=reason,
        device_ip=device_ip,
    )

    if result:
        print(f"Disproved: {category}:{key}")
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
    from netaudio_lib.dante.fact_store import reinstate_fact

    facts_path = _resolve_facts_path()
    result = reinstate_fact(
        facts_path,
        category=category,
        key=key,
        confidence=confidence,
        note=note,
    )

    if result:
        print(f"Reinstated: {category}:{key} (confidence={confidence})")
    else:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)
