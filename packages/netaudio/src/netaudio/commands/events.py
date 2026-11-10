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


def _row(event: dict) -> list[str]:
    return [
        local_time(event.get("timestamp")),
        str(event.get("severity") or ""),
        str(event.get("kind") or "").replace("_", " "),
        str(event.get("device_name") or event.get("server_name") or event.get("device_identity") or ""),
        str(event.get("flow_identity") or event.get("channel_identity") or event.get("interface_identity") or ""),
        str(event.get("operation_name") or "").replace("_", " "),
        str(event.get("lifecycle_phase") or "").replace("_", " "),
        readable(event.get("previous_value")),
        readable(event.get("current_value")),
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
