from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.output import output_table
from netaudio.daemon.client import clear_event_journal_on_daemon, get_event_journal_from_daemon
from netaudio.monitoring import EventSeverity, MonitoringEventKind

app = typer.Typer(
    help="Inspect the daemon's retained monitoring event journal.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)

EVENT_HEADERS = [
    "Timestamp",
    "Severity",
    "Kind",
    "Device",
    "Subject",
    "Operation",
    "Phase",
    "Previous",
    "Current",
]


def _daemon_error(status: int | None, data: dict | None) -> None:
    if status is None:
        message = "The netaudio daemon is not running or did not respond."
    else:
        message = str(data.get("error")) if data and data.get("error") else f"Daemon returned HTTP {status}."
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=1)


def _fetch(
    *,
    device: str | None,
    kind: MonitoringEventKind | None,
    severity: EventSeverity | None,
    since: str | None,
    limit: int | None,
) -> dict:
    status, data = asyncio.run(
        get_event_journal_from_daemon(
            device=device,
            kind=kind.value if kind is not None else None,
            severity=severity.value if severity is not None else None,
            since=since,
            limit=limit,
        )
    )
    if status != 200 or data is None:
        _daemon_error(status, data)
    return data


def _compact(value) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _subject(event: dict) -> str:
    for key in ("flow_identity", "channel_identity", "interface_identity"):
        value = event.get(key)
        if value:
            return str(value)
    return ""


def _rows(events: list[dict]) -> list[list[str]]:
    return [
        [
            str(event.get("timestamp") or ""),
            str(event.get("severity") or ""),
            str(event.get("kind") or ""),
            str(event.get("device_name") or event.get("server_name") or event.get("device_identity") or ""),
            _subject(event),
            str(event.get("operation_name") or ""),
            str(event.get("lifecycle_phase") or ""),
            _compact(event.get("previous_value")),
            _compact(event.get("current_value")),
        ]
        for event in events
    ]


@app.command("list")
def list_events(
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[MonitoringEventKind] = typer.Option(None, "--kind", help="Filter by event kind."),
    severity: Optional[EventSeverity] = typer.Option(None, "--severity", help="Filter by severity."),
    since: Optional[str] = typer.Option(None, "--since", help="Include events at or after this ISO-8601 timestamp."),
    limit: int = typer.Option(100, "--limit", min=1, help="Maximum number of newest events to show."),
):
    """List recent monitoring events retained by the daemon."""
    payload = _fetch(device=device, kind=kind, severity=severity, since=since, limit=limit)
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    output_table(
        EVENT_HEADERS,
        _rows(events),
        json_data=payload,
        empty_message="No retained monitoring events matched.",
    )


@app.command("export")
def export_events(
    destination: Optional[Path] = typer.Option(None, "--file", help="Write JSON to this file instead of stdout."),
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[MonitoringEventKind] = typer.Option(None, "--kind", help="Filter by event kind."),
    severity: Optional[EventSeverity] = typer.Option(None, "--severity", help="Filter by severity."),
    since: Optional[str] = typer.Option(None, "--since", help="Include events at or after this ISO-8601 timestamp."),
):
    """Export retained events as stable JSON with their raw evidence."""
    payload = _fetch(device=device, kind=kind, severity=severity, since=since, limit=None)
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if destination is None:
        typer.echo(content, nl=False)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    typer.echo(f"Exported {payload.get('count', 0)} event(s) to {destination}.")


@app.command("clear")
def clear_events():
    """Clear only the daemon's locally retained event history."""
    status, data = asyncio.run(clear_event_journal_on_daemon())
    if status != 200 or data is None:
        _daemon_error(status, data)
    typer.echo(f"Cleared {data.get('cleared', 0)} locally retained event(s).")
