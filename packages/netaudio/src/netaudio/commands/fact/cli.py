from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import typer

from netaudio.capture.fact import (
    _build_spec_data,
    _spec_to_markdown,
    _spec_to_plain,
)
from netaudio.capture.packets import _compact_hexdump
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.capture.options import _resolve_facts_path
from netaudio.commands.capture.reporting import emit_report
from netaudio.commands.fact.evidence import (
    VerificationEntry,
    _create_evidence_markers,
    _run_fact_verify,
)
from netaudio.commands.fact.inputs import EvidenceOptions, parse_fact_content, parse_fields, resolve_evidence
from netaudio.commands.fact.rendering import (
    CheckTotals,
    check_proof_lines,
    check_result_lines,
    check_status,
    fact_header_lines,
    fact_list_lines,
    fact_status_lines,
    fact_written_lines,
    lookup_evidence,
    parsed_field_lines,
    proof_entry_lines,
)
from netaudio.icons import icon

app = typer.Typer(
    help="Protocol fact registry — what we know and how we proved it.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _provenance_directory(provenance_dir: str | None, facts_path: Path) -> Path:
    return Path(provenance_dir) if provenance_dir else facts_path.parent


def _require_facts_path() -> Path:
    facts_path = _resolve_facts_path()
    if not facts_path.exists():
        typer.echo("No facts registered yet.", err=True)
        raise typer.Exit(1)
    return facts_path


def _marker_lines(resolution, category: str, key: str, name: str, options: EvidenceOptions) -> list[str]:
    if not resolution.references:
        return []
    lines = [f"  Evidence: {reference}" for reference in resolution.references]
    lines.extend(
        _create_evidence_markers(
            resolution.targets,
            category,
            key,
            name,
            db=options.db,
            config=options.config,
            profile=options.profile,
        )
    )
    return lines


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
    from netaudio.dante.fact_store import FactRecord, add_fact

    facts_path = _resolve_facts_path()
    fields_parsed = parse_fields(field) or []
    options = EvidenceOptions(config=config, db=db, profile=profile)
    resolution = resolve_evidence(evidence, options)
    if confidence == "verified" and not resolution.references:
        typer.echo("Capture: verified facts require at least one session_name:packet_id evidence reference.", err=True)
        raise typer.Exit(1)
    content = parse_fact_content(body, body_file, fields_parsed, protocol, match)

    fact = add_fact(
        facts_path,
        category,
        key,
        FactRecord(
            name,
            body=content.body,
            confidence=confidence,
            evidence=resolution.references,
            fields=fields_parsed,
            match_offset=content.match_offset,
            match_size=content.match_size,
            note=note,
            protocol_id=content.protocol_id,
            supersedes=supersedes,
        ),
    )

    lines = resolution.lines + fact_written_lines("Fact", category, key, name, confidence, note, content.body)
    lines.extend(parsed_field_lines(fields_parsed))
    lines.extend(_marker_lines(resolution, category, key, name, options))
    if "history" in fact:
        lines.append(f"  (updated existing fact, {len(fact['history'])} previous version(s))")
    emit_report(lines, {"fact": fact})


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
    from netaudio.dante.fact_store import FactUpdate, get_confidence, get_fact, update_fact

    facts_path = _resolve_facts_path()
    if clear_fields and field:
        typer.echo("Capture: --clear-fields cannot be combined with --field.", err=True)
        raise typer.Exit(1)
    fields_parsed = parse_fields(field, clear_fields)
    options = EvidenceOptions(config=config, db=db, profile=profile)
    resolution = resolve_evidence(evidence, options)
    if confidence == "verified":
        existing_fact = get_fact(facts_path, category, key)
        existing_evidence = existing_fact.get("evidence", []) if existing_fact else []
        if not existing_evidence and not resolution.references:
            typer.echo(
                "Capture: verified facts require at least one session_name:packet_id evidence reference.",
                err=True,
            )
            raise typer.Exit(1)
    content = parse_fact_content(body, body_file, fields_parsed, protocol, match)

    fact = update_fact(
        facts_path,
        category,
        key,
        FactUpdate(
            body=content.body,
            confidence=confidence,
            evidence=resolution.references or None,
            fields=fields_parsed,
            match_offset=content.match_offset,
            match_size=content.match_size,
            name=name,
            note=note,
            protocol_id=content.protocol_id,
            replace_evidence=replace_evidence,
            supersedes=supersedes,
        ),
    )

    if fact is None:
        typer.echo(f"Fact not found: {category}:{key}", err=True)
        raise typer.Exit(1)

    lines = resolution.lines + [
        f"{icon('info')}Updated: {category}:{key} = {fact['name']}",
        f"  Confidence: {get_confidence(fact)}",
    ]
    if fact.get("note"):
        lines.append(f"  Note: {fact['note']}")
    lines.extend(_marker_lines(resolution, category, key, fact["name"], options))
    if "history" in fact:
        lines.append(f"  ({len(fact['history'])} revision(s))")
    emit_report(lines, {"fact": fact})


@app.command("list", help="List recorded facts, optionally filtered by category.")
def fact_list(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Filter by category."),
):
    from netaudio.cli import state
    from netaudio.dante.fact_store import get_categories, list_facts

    facts_path = _resolve_facts_path()
    categories = get_categories(facts_path) if facts_path.exists() else []
    if not facts_path.exists() or (category is None and not categories):
        emit_report(["No facts registered yet."], {"categories": [], "facts": []})
        return

    lines = [] if category else [f"Categories: {', '.join(categories)}", ""]
    facts = list_facts(facts_path, category=category)
    if not facts:
        if category:
            lines.append(f"No facts in category '{category}'.")
        emit_report(lines, {"categories": categories, "facts": []})
        return

    lines.extend(fact_list_lines(facts, state.verbose))
    lines.append(f"\n{len(facts)} facts ({'+' if category else 'all categories'})")
    emit_report(lines, {"categories": categories, "facts": facts})


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
    from netaudio.dante.fact_store import get_fact

    facts_path = _resolve_facts_path()
    fact = get_fact(facts_path, category, key)

    if not fact:
        typer.echo(f"Fact not found: {category}:{key}", err=True)
        raise typer.Exit(1)

    lines = fact_header_lines(fact) + fact_status_lines(fact)
    proof = []
    if prove and fact.get("evidence"):
        provenance_directory = _provenance_directory(provenance_dir, facts_path)
        lines.extend(["", "Proof:", "=" * 80])
        for reference in fact["evidence"]:
            entry_lines, entry_data = proof_entry_lines(fact, lookup_evidence(provenance_directory, reference))
            lines.extend(entry_lines)
            proof.append(entry_data)
        lines.extend(["", "=" * 80])
    emit_report(lines, {"fact": fact, "proof": proof})


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
    from netaudio.dante.fact_store import check_facts, get_fact, list_facts

    facts_path = _resolve_facts_path()
    if not facts_path.exists():
        emit_report(["No facts registered yet."], {"results": [], "summary": CheckTotals().data()})
        return

    provenance_directory = _provenance_directory(provenance_dir, facts_path)
    results = check_facts(facts_path, provenance_dir=provenance_directory)
    if category:
        category_keys = {f"{fact['category']}:{fact['key']}" for fact in list_facts(facts_path, category=category)}
        results = [result for result in results if result["fact_key"] in category_keys]

    totals = CheckTotals()
    lines = []
    for result in results:
        status = check_status(result)
        totals.count(status)
        result["status_label"] = status.label
        lines.extend(check_result_lines(result, status))
        if prove:
            fact = get_fact(facts_path, result["category"], result["key"])
            if fact and fact.get("evidence"):
                proof_lines, result["proof"] = check_proof_lines(fact, provenance_directory)
                lines.extend(proof_lines)
            lines.append("")

    lines.extend(["", totals.summary_line()])
    emit_report(lines, {"results": results, "summary": totals.data()})
    if totals.failed > 0:
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


@dataclass
class VerificationSelection:
    entries: list[VerificationEntry]
    skipped_lines: list[str]


def _request_evidence(fact: dict, provenance_directory: Path) -> tuple[bytes | None, int | None]:
    for reference in fact.get("evidence", []):
        lookup = lookup_evidence(provenance_directory, reference)
        if lookup.direction == "request" and lookup.payload:
            return lookup.payload, lookup.destination_port
    return None, None


def _select_verification_entries(facts: list[dict], provenance_directory: Path, write: bool) -> VerificationSelection:
    allowed = set(QUERYABLE_FACTS)
    if write:
        allowed |= WRITABLE_FACTS
    verifiable = []
    skipped_lines = []
    for fact in facts:
        fact_key = f"{fact['category']}:{fact['key']}"
        if fact_key in REACTIVE_FACTS:
            continue
        if fact_key not in allowed:
            if not write:
                skipped_lines.append(f"  [SKIP] {fact_key:30s} {fact['name']} (write command, use --write)")
            continue
        verifiable.append(fact)
    if not verifiable:
        for line in skipped_lines:
            typer.echo(line)
        typer.echo("No verifiable facts found.", err=True)
        raise typer.Exit(1)

    entries = []
    for fact in verifiable:
        fact_key = f"{fact['category']}:{fact['key']}"
        request_packet, destination_port = _request_evidence(fact, provenance_directory)
        if request_packet is None:
            skipped_lines.append(f"  [SKIP] {fact_key:30s} {fact['name']} (no request packet in evidence)")
            continue
        port = destination_port or PORT_BY_CATEGORY.get(fact["category"], 4440)
        entries.append(VerificationEntry(fact=fact, fact_key=fact_key, port=port, request_packet=request_packet))
    if not entries:
        for line in skipped_lines:
            typer.echo(line)
        typer.echo("No facts with request packets to verify.", err=True)
        raise typer.Exit(1)
    return VerificationSelection(entries=entries, skipped_lines=skipped_lines)


def _dry_run_lines(entries: list[VerificationEntry], device_ip: str) -> list[str]:
    from netaudio.dante.fact_store import _field_applies_to_direction

    lines = []
    for entry in entries:
        lines.append(f"  {entry.fact_key:30s} -> {device_ip}:{entry.port}  {len(entry.request_packet)}B")
        lines.extend(_compact_hexdump(entry.request_packet, max_lines=2))
        field_names = [
            definition["name"]
            for definition in entry.fact.get("fields", [])
            if _field_applies_to_direction(definition, "response")
        ]
        if field_names:
            lines.append(f"    verify: {', '.join(field_names)}")
    lines.append(f"\nDry run: {len(entries)} packets would be sent.")
    return lines


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
    from netaudio.dante.fact_store import list_facts

    facts_path = _require_facts_path()
    provenance_directory = _provenance_directory(provenance_dir, facts_path)
    facts = list_facts(facts_path, category=category)
    if key:
        facts = [fact for fact in facts if fact["key"] == key]

    selection = _select_verification_entries(facts, provenance_directory, write)
    heading = [*selection.skipped_lines, f"Verifying {len(selection.entries)} facts against {device_ip}", ""]
    plan_data = [entry.data() for entry in selection.entries]
    if dry_run:
        emit_report(heading + _dry_run_lines(selection.entries, device_ip), {"device_ip": device_ip, "plan": plan_data})
        return

    from netaudio.commands.capture.reporting import structured_output_selected

    if not structured_output_selected():
        for line in heading:
            typer.echo(line)
    report = asyncio.run(
        _run_fact_verify(
            verify_plan=selection.entries,
            device_ip=device_ip,
            timeout=timeout,
            session_name=session_name or f"fact_verify_{device_ip.replace('.', '_')}",
            config=config,
            profile=profile,
            db_override=db,
            auto_disprove=auto_disprove,
        )
    )
    emit_report([], {"device_ip": device_ip, "plan": plan_data, **report.data()})


@app.command("spec", help="Render the fact registry as a protocol specification document.")
def fact_spec(
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Limit to one category."),
    output: Optional[str] = typer.Option(None, "--output", help="Write to file instead of stdout."),
    markdown: bool = typer.Option(False, "--markdown", "--md", help="Force markdown output."),
    prove: bool = typer.Option(
        False, "--prove", "-p", help="Annotate with provenance: evidence bundles and packet IDs."
    ),
):
    from netaudio.cli import OutputFormat
    from netaudio.cli import state as cli_state
    from netaudio.cli_support.output import output_single

    facts_path = _resolve_facts_path()

    if not facts_path.exists():
        typer.echo("No facts registered yet.", err=True)
        raise typer.Exit(1)

    from netaudio.dante.fact_store import get_categories

    categories = get_categories(facts_path)
    if not categories:
        typer.echo("No facts registered yet.", err=True)
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
        typer.echo(f"Wrote spec to {output_path}", err=True)
    else:
        typer.echo(text)


from netaudio.commands.fact.lifecycle import (
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
