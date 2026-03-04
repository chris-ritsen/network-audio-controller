from __future__ import annotations

import sys
from typing import Optional

import typer

from netaudio._common import output_table, output_single


app = typer.Typer(help="Bug tracker — document and track known defects.", no_args_is_help=True)


def _resolve_bugs_dir():
    from netaudio_lib.dante.bug_store import DEFAULT_BUGS_DIR

    return DEFAULT_BUGS_DIR


def _parse_context(context_args: list[str] | None) -> dict:
    if not context_args:
        return {}

    result = {}

    for item in context_args:
        if "=" not in item:
            print(f"Invalid context format: {item!r} (expected key=value)", file=sys.stderr)
            raise typer.Exit(1)

        key, value = item.split("=", 1)
        result[key] = value

    return result


def _collect_sessions(bug: dict) -> list[int]:
    sessions = []

    for event in bug.get("history", []):
        session_id = event.get("session_id")

        if session_id is not None and session_id not in sessions:
            sessions.append(session_id)

    return sessions


def _short_iso(iso: str) -> str:
    if "T" in iso:
        return iso.split("T")[0] + " " + iso.split("T")[1][:8]

    return iso


def _status_icon(status: str) -> str:
    return {"open": "○", "closed": "✓", "reopened": "◎"}.get(status, "?")


def _bug_to_row(bug: dict) -> list[str]:
    sessions = _collect_sessions(bug)
    session_str = ", ".join(f"#{s}" for s in sessions)
    tags = ", ".join(bug.get("tags", []))
    reported = _short_iso(bug.get("reported_iso", "?"))
    updated = _short_iso(bug.get("updated_iso", "?"))

    context = bug.get("context", {})
    context_parts = []

    for key in ("device_model", "affected_devices", "platform"):
        val = context.get(key)

        if val and not (key == "platform" and val == "all"):
            context_parts.append(f"{key}={val}")

    context_str = ", ".join(context_parts)

    return [
        _status_icon(bug["status"]),
        bug["label"],
        bug["summary"],
        reported,
        updated,
        session_str or "-",
        tags or "-",
        context_str or "-",
    ]


_LIST_HEADERS = ["Status", "Label", "Summary", "Reported", "Updated", "Sessions", "Tags", "Context"]


def _is_structured():
    from netaudio.cli import OutputFormat, state as cli_state

    return cli_state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml)


def _print_bug_detail(bug: dict, action_msg: str | None = None):
    if _is_structured():
        output_single(bug)
        return

    status_display = {"open": "OPEN", "closed": "CLOSED", "reopened": "REOPENED"}

    if action_msg:
        print(action_msg)
    else:
        print(f"Bug: {bug['label']}")

    print(f"  Status:   {status_display.get(bug['status'], bug['status'])}")
    print(f"  Summary:  {bug['summary']}")
    print(f"  Reported: {bug.get('reported_iso', '?')}")
    print(f"  Updated:  {bug.get('updated_iso', '?')}")

    if bug.get("tags"):
        print(f"  Tags:     {', '.join(bug['tags'])}")

    if bug.get("context"):
        print(f"  Context:")

        for key, value in bug["context"].items():
            print(f"    {key}: {value}")

    sessions = _collect_sessions(bug)

    if sessions:
        print(f"  Sessions: {', '.join(f'#{s}' for s in sessions)}")

    if bug.get("history"):
        print()
        print("  History:")

        for event in bug["history"]:
            action = event["action"].upper()
            timestamp = _short_iso(event.get("timestamp_iso", "?"))
            parts = [f"    [{action:8s}] {timestamp}"]

            if event.get("session_id") is not None:
                parts.append(f"session #{event['session_id']}")

            if event.get("marker_id") is not None:
                parts.append(f"marker #{event['marker_id']}")

            print("  ".join(parts))

            if event.get("note"):
                for line in event["note"].splitlines():
                    print(f"               {line}")


@app.command("report")
def bug_report(
    label: str = typer.Argument(help="Unique bug identifier (e.g. device_list_channel_count)."),
    summary: str = typer.Option(..., "--summary", "-s", help="One-line description of the bug."),
    note: Optional[str] = typer.Option(
        None, "--note", help="Detailed description: evidence, code references, reproduction steps."
    ),
    tag: Optional[list[str]] = typer.Option(
        None, "--tag", "-t", help="Categorization tag (repeatable). E.g. device-specific, display, protocol."
    ),
    context: Optional[list[str]] = typer.Option(
        None,
        "--context",
        "-c",
        help="Environment context as key=value (repeatable). E.g. device_model=DIOBT, platform=all.",
    ),
    session: Optional[int] = typer.Option(None, "--session", help="Link to capture session ID."),
    marker_id: Optional[int] = typer.Option(None, "--marker-id", help="Link to specific marker ID."),
):
    from netaudio_lib.dante.bug_store import report_bug

    bugs_dir = _resolve_bugs_dir()
    context_dict = _parse_context(context)

    try:
        bug = report_bug(
            bugs_dir=bugs_dir,
            label=label,
            summary=summary,
            note=note,
            tags=tag or [],
            context=context_dict,
            session_id=session,
            marker_id=marker_id,
        )
    except ValueError as err:
        print(str(err), file=sys.stderr)
        raise typer.Exit(1)

    _print_bug_detail(bug, f"Bug: {bug['label']} -> reported")


@app.command("close")
def bug_close(
    label: str = typer.Argument(help="Bug label to close."),
    note: Optional[str] = typer.Option(None, "--note", help="Why this bug is fixed."),
    session: Optional[int] = typer.Option(None, "--session", help="Session where the fix was made."),
    marker_id: Optional[int] = typer.Option(None, "--marker-id", help="Marker documenting the fix."),
):
    from netaudio_lib.dante.bug_store import close_bug

    bugs_dir = _resolve_bugs_dir()

    try:
        bug = close_bug(
            bugs_dir=bugs_dir,
            label=label,
            note=note,
            session_id=session,
            marker_id=marker_id,
        )
    except ValueError as err:
        print(str(err), file=sys.stderr)
        raise typer.Exit(1)

    _print_bug_detail(bug, f"Bug: {bug['label']} -> closed")


@app.command("reopen")
def bug_reopen(
    label: str = typer.Argument(help="Bug label to reopen."),
    note: Optional[str] = typer.Option(None, "--note", help="Why this bug is being reopened."),
    session: Optional[int] = typer.Option(None, "--session", help="Session where the regression was found."),
    marker_id: Optional[int] = typer.Option(None, "--marker-id", help="Marker documenting the regression."),
):
    from netaudio_lib.dante.bug_store import reopen_bug

    bugs_dir = _resolve_bugs_dir()

    try:
        bug = reopen_bug(
            bugs_dir=bugs_dir,
            label=label,
            note=note,
            session_id=session,
            marker_id=marker_id,
        )
    except ValueError as err:
        print(str(err), file=sys.stderr)
        raise typer.Exit(1)

    _print_bug_detail(bug, f"Bug: {bug['label']} -> reopened")


def _parse_date_to_ns(value: str) -> int:
    from datetime import datetime, timedelta, timezone

    local_tz = datetime.now(timezone.utc).astimezone().tzinfo

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=local_tz)
            return int(dt.timestamp() * 1e9)
        except ValueError:
            continue

    relative_days = {"today": 0, "yesterday": 1, "week": 7, "month": 30}

    if value in relative_days:
        dt = datetime.now(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        dt -= timedelta(days=relative_days[value])
        return int(dt.timestamp() * 1e9)

    raise typer.BadParameter(f"Invalid date: {value!r}. Use YYYY-MM-DD, today, yesterday, week, or month.")


@app.command("list")
def bug_list(
    all: bool = typer.Option(False, "--all", "-a", help="Show all bugs including closed."),
    closed: bool = typer.Option(False, "--closed", help="Show only closed bugs."),
    tag: Optional[str] = typer.Option(None, "--tag", "-t", help="Filter by tag."),
    reported_after: Optional[str] = typer.Option(
        None, "--reported-after", help="Bugs reported after date (YYYY-MM-DD, today, yesterday, week, month)."
    ),
    reported_before: Optional[str] = typer.Option(None, "--reported-before", help="Bugs reported before date."),
    updated_after: Optional[str] = typer.Option(None, "--updated-after", help="Bugs updated after date."),
    updated_before: Optional[str] = typer.Option(None, "--updated-before", help="Bugs updated before date."),
):
    from netaudio_lib.dante.bug_store import list_bugs

    bugs_dir = _resolve_bugs_dir()

    if closed:
        status_filter = "closed"
    elif all:
        status_filter = None
    else:
        status_filter = None

    bugs = list_bugs(
        bugs_dir,
        status=status_filter,
        tag=tag,
        reported_after_ns=_parse_date_to_ns(reported_after) if reported_after else None,
        reported_before_ns=_parse_date_to_ns(reported_before) if reported_before else None,
        updated_after_ns=_parse_date_to_ns(updated_after) if updated_after else None,
        updated_before_ns=_parse_date_to_ns(updated_before) if updated_before else None,
    )

    if not all and not closed:
        bugs = [b for b in bugs if b["status"] != "closed"]

    if not bugs:
        if closed:
            print("No closed bugs.")
        elif all:
            print("No bugs tracked.")
        else:
            print("No open bugs.")
        return

    from netaudio.cli import OutputFormat, state as cli_state

    rows = [_bug_to_row(bug) for bug in bugs]
    json_data = bugs

    output_table(_LIST_HEADERS, rows, json_data=json_data)

    if cli_state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml, OutputFormat.csv):
        return

    open_count = sum(1 for b in bugs if b["status"] != "closed")
    closed_count = sum(1 for b in bugs if b["status"] == "closed")
    total = len(bugs)

    if all:
        print(f"\n{total} bugs: {open_count} open, {closed_count} closed")
    elif closed:
        print(f"\n{closed_count} closed bug(s)")
    else:
        print(f"\n{open_count} open bug(s)")


@app.command("show")
def bug_show(
    label: str = typer.Argument(help="Bug label to show."),
):
    from netaudio_lib.dante.bug_store import get_bug

    bugs_dir = _resolve_bugs_dir()
    bug = get_bug(bugs_dir, label)

    if bug is None:
        print(f"Bug not found: {label}", file=sys.stderr)
        raise typer.Exit(1)

    _print_bug_detail(bug)
