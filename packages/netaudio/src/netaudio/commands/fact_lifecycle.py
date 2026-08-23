from __future__ import annotations

import sys
from typing import Optional

import typer

from netaudio.commands.capture_helpers import _resolve_facts_path
from netaudio.icons import icon


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


def fact_quarantine(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
    reason: str = typer.Option(..., "--reason", help="Why this fact cannot currently be checked."),
):
    from netaudio.dante.fact_store import quarantine_fact

    facts_path = _resolve_facts_path()
    result = quarantine_fact(facts_path, category=category, key=key, reason=reason)

    if result:
        print(f"{icon('warning')}Quarantined: {category}:{key}")
        print(f"  Reason: {reason}")
    else:
        print(f"Fact not found: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)


def fact_unquarantine(
    category: str = typer.Option(..., "--category", "-c", help="Fact category."),
    key: str = typer.Option(..., "--key", "-k", help="Fact key."),
):
    from netaudio.dante.fact_store import clear_quarantine

    facts_path = _resolve_facts_path()
    result = clear_quarantine(facts_path, category=category, key=key)

    if result:
        print(f"{icon('success')}Cleared quarantine: {category}:{key}")
    else:
        print(f"Fact not found or not quarantined: {category}:{key}", file=sys.stderr)
        raise typer.Exit(1)
