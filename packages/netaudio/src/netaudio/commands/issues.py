from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.output import output_table
from netaudio.daemon.client import get_issues_from_daemon
from netaudio.monitoring import EventSeverity, IssueKind, IssueLifecycleState

app = typer.Typer(
    help="Inspect current and resolved monitoring issues without changing devices.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _scope(issue: dict) -> str:
    scope = issue.get("scope") if isinstance(issue.get("scope"), dict) else {}
    return str(scope.get("flow_identity") or scope.get("channel_identity") or scope.get("interface_identity") or "")


def _rows(issues: list[dict]) -> list[list[str]]:
    rows = []
    for issue in issues:
        scope = issue.get("scope") if isinstance(issue.get("scope"), dict) else {}
        rows.append(
            [
                str(issue.get("last_seen") or ""),
                str(issue.get("severity") or ""),
                str(issue.get("state") or ""),
                str(issue.get("observation_state") or ""),
                str(issue.get("kind") or ""),
                str(scope.get("device_name") or scope.get("server_name") or scope.get("device_identity") or ""),
                _scope(issue),
                str(issue.get("occurrence_count") or ""),
                str(issue.get("summary") or ""),
            ]
        )
    return rows


@app.command("list")
def list_issues(
    state: Optional[IssueLifecycleState] = typer.Option(
        IssueLifecycleState.OPEN,
        "--state",
        help="Show open issues (current) or resolved issue history.",
    ),
    device: Optional[str] = typer.Option(None, "--device", help="Match a device identity, name, or server name."),
    kind: Optional[IssueKind] = typer.Option(None, "--kind", help="Filter by issue kind."),
    severity: Optional[EventSeverity] = typer.Option(None, "--severity", help="Filter by severity."),
    limit: int = typer.Option(100, "--limit", min=1, help="Maximum number of newest issues to show."),
):
    """List lifecycle-managed issues retained by the daemon."""
    status, payload = asyncio.run(
        get_issues_from_daemon(
            state=state.value if state is not None else None,
            device=device,
            kind=kind.value if kind is not None else None,
            severity=severity.value if severity is not None else None,
            limit=limit,
        )
    )
    if status != 200 or payload is None:
        if status is None:
            message = "The netaudio daemon is not running or did not respond."
        else:
            message = (
                str(payload.get("error")) if payload and payload.get("error") else f"Daemon returned HTTP {status}."
            )
        typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(code=1)
    issues = payload.get("issues") if isinstance(payload.get("issues"), list) else []
    output_table(
        ["Last seen", "Severity", "State", "Observation", "Kind", "Device", "Scope", "Count", "Summary"],
        _rows(issues),
        json_data=payload,
        empty_message="No monitoring issues matched.",
    )
