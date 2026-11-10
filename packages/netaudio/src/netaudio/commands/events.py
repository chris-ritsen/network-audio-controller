from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.output import local_time, output_table, readable
from netaudio.daemon.client import call_daemon

app = typer.Typer(
    help="Inspect the retained monitoring event journal.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _fetch(**filters) -> dict:
    query = urlencode({name: value for name, value in filters.items() if value is not None})
    return asyncio.run(call_daemon("GET", f"/event-journal?{query}" if query else "/event-journal"))


def _field(key: str, value) -> str:
    if key.endswith(("_at", "_seen")) and isinstance(value, str):
        value = local_time(value)
    return f"{key.replace('_', ' ')} {readable(value)}"


def _shown(key: str, value) -> bool:
    return value is not None and not isinstance(value, (dict, list)) and not key.endswith(("_id", "_sha256", "icon"))


def _summary(value: dict) -> str:
    if "issue_id" in value:
        count = value.get("occurrence_count")
        seen = "seen once" if count == 1 else f"seen {count} times"
        return f"{readable(value.get('kind')).replace('_', ' ')}, {readable(value.get('observation_state'))}, {seen}"
    return ": ".join(str(value[key]) for key in ("label", "detail") if value.get(key))


def _values(previous, current) -> list[str]:
    if not isinstance(previous, dict) and not isinstance(current, dict):
        return [readable(previous), readable(current)]
    sides = [side if isinstance(side, dict) else {} for side in (previous, current)]
    if any("issue_id" in side or "label" in side for side in sides):
        return [_summary(side) for side in sides]
    changed = sorted(key for key in sides[0].keys() | sides[1].keys() if sides[0].get(key) != sides[1].get(key))
    return [", ".join(_field(key, side[key]) for key in changed if _shown(key, side.get(key))) for side in sides]


def _row(event: dict) -> list[str]:
    return [
        local_time(event.get("timestamp")),
        str(event.get("severity") or ""),
        str(event.get("kind") or "").replace("_", " "),
        str(event.get("device_name") or event.get("server_name") or event.get("device_identity") or ""),
        str(event.get("flow_identity") or event.get("channel_identity") or event.get("interface_identity") or ""),
        str(event.get("operation_name") or "").replace("_", " "),
        str(event.get("lifecycle_phase") or "").replace("_", " "),
        *_values(event.get("previous_value"), event.get("current_value")),
    ]


@app.command("list", help="List recent retained monitoring events.")
def list_events(
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[str] = typer.Option(None, "--kind", help="Filter by event kind."),
    severity: Optional[str] = typer.Option(None, "--severity", help="Filter by severity."),
    since: Optional[str] = typer.Option(None, "--since", help="Include events at or after this ISO-8601 timestamp."),
    limit: int = typer.Option(100, "--limit", min=1, help="Maximum number of newest events to show."),
):
    payload = _fetch(device=device, kind=kind, severity=severity, since=since, limit=limit)
    output_table(
        ["Time", "Severity", "Kind", "Device", "Subject", "Operation", "Phase", "Previous", "Current"],
        [_row(event) for event in payload["events"]],
        json_data=payload,
        omit_empty_columns=True,
        empty_message="No retained monitoring events matched.",
    )


@app.command("export", help="Export retained events as stable JSON with their raw evidence.")
def export_events(
    destination: Optional[Path] = typer.Option(None, "--file", help="Write JSON to this file instead of stdout."),
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[str] = typer.Option(None, "--kind", help="Filter by event kind."),
    severity: Optional[str] = typer.Option(None, "--severity", help="Filter by severity."),
    since: Optional[str] = typer.Option(None, "--since", help="Include events at or after this ISO-8601 timestamp."),
):
    payload = _fetch(device=device, kind=kind, severity=severity, since=since)
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if destination is None:
        typer.echo(content, nl=False)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    typer.echo(f"Exported {payload.get('count', 0)} events to {destination}.")


@app.command("clear", help="Clear the locally retained event history.")
def clear_events():
    result = asyncio.run(call_daemon("DELETE", "/event-journal"))
    typer.echo(f"Cleared {result.get('cleared', 0)} locally retained events.")
