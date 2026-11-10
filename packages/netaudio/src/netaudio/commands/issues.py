from __future__ import annotations

import asyncio
from typing import Optional
from urllib.parse import urlencode

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.output import local_time, output_table
from netaudio.daemon.client import call_daemon

app = typer.Typer(
    help="Inspect current and resolved monitoring issues without changing devices.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _row(issue: dict) -> list[str]:
    scope = issue.get("scope") or {}
    return [
        local_time(issue.get("last_seen")),
        str(issue.get("severity") or ""),
        str(issue.get("state") or ""),
        str(issue.get("observation_state") or ""),
        str(issue.get("kind") or "").replace("_", " "),
        str(scope.get("device_name") or scope.get("server_name") or scope.get("device_identity") or ""),
        str(scope.get("flow_identity") or scope.get("channel_identity") or scope.get("interface_identity") or ""),
        str(issue.get("occurrence_count") or ""),
        str(issue.get("summary") or ""),
    ]


@app.command("list", help="List monitoring issues and their history.")
def list_issues(
    state: str = typer.Option("open", "--state", help="Show open issues (current) or resolved issue history."),
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[str] = typer.Option(None, "--kind", help="Filter by issue kind."),
    severity: Optional[str] = typer.Option(None, "--severity", help="Filter by severity."),
    limit: int = typer.Option(100, "--limit", min=1, help="Maximum number of newest issues to show."),
):
    filters = {"device": device, "kind": kind, "limit": limit, "severity": severity, "state": state}
    query = urlencode({name: value for name, value in filters.items() if value is not None})
    payload = asyncio.run(call_daemon("GET", f"/issues?{query}"))
    output_table(
        ["Last Seen", "Severity", "State", "Observation", "Kind", "Device", "Scope", "Count", "Summary"],
        [_row(issue) for issue in payload["issues"]],
        json_data=payload,
        omit_empty_columns=True,
        empty_message="No monitoring issues matched.",
    )
