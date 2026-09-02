from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import typer

from netaudio.commands.capture.options import _parse_field_spec
from netaudio.commands.fact.evidence import _resolve_evidence_sessions, _validate_evidence_references


@dataclass
class EvidenceOptions:
    config: str | None = None
    db: str | None = None
    profile: str | None = None


@dataclass
class EvidenceResolution:
    lines: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    targets: list[tuple[int, int]] = field(default_factory=list)


@dataclass
class FactContent:
    body: str | None = None
    fields: list[dict] | None = None
    match_offset: int | None = None
    match_size: int | None = None
    protocol_id: int | list[int] | None = None


def resolve_evidence(evidence: list[str] | None, options: EvidenceOptions) -> EvidenceResolution:
    if not evidence:
        return EvidenceResolution()
    resolution = _resolve_evidence_sessions(evidence, db=options.db, config=options.config, profile=options.profile)
    targets = _validate_evidence_references(
        resolution.references, db=options.db, config=options.config, profile=options.profile
    )
    return EvidenceResolution(lines=resolution.lines, references=resolution.references, targets=targets)


def read_body(body: str | None, body_file: str | None) -> str | None:
    if not body_file:
        return body
    if body_file == "-":
        return sys.stdin.read()
    body_path = Path(body_file)
    if not body_path.exists():
        typer.echo(f"Capture: body file not found: {body_file}", err=True)
        raise typer.Exit(1)
    return body_path.read_text()


def parse_protocol_option(protocol: str | None) -> int | list[int] | None:
    if not protocol:
        return None
    parts = [part.strip() for part in protocol.split(",")]
    if len(parts) == 1:
        return int(parts[0], 0)
    return [int(part, 0) for part in parts]


def parse_match_option(match: str | None) -> tuple[int | None, int | None]:
    if not match:
        return None, None
    match_parts = match.split(":")
    return int(match_parts[0]), int(match_parts[1]) if len(match_parts) > 1 else 2


def parse_fields(specifications: list[str] | None, clear_fields: bool = False) -> list[dict] | None:
    if clear_fields:
        return []
    if specifications:
        return [_parse_field_spec(specification) for specification in specifications]
    return None


def parse_fact_content(
    body: str | None,
    body_file: str | None,
    fields: list[dict] | None,
    protocol: str | None,
    match: str | None,
) -> FactContent:
    match_offset, match_size = parse_match_option(match)
    return FactContent(
        body=read_body(body, body_file),
        fields=fields,
        match_offset=match_offset,
        match_size=match_size,
        protocol_id=parse_protocol_option(protocol),
    )
