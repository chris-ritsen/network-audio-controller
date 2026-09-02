from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer

from netaudio.cli_support.output import output_single, output_table
from netaudio.commands.ddm.operations import register_schema_operations
from netaudio.commands.ddm.render import report_errors
from netaudio.commands.ddm.transport import execute, fail
from netaudio.daemon.client import (
    get_ddm_devices_from_daemon,
    get_ddm_status_from_daemon,
    refresh_ddm_inventory_on_daemon,
)
from netaudio.ddm import Schema, SchemaError, command_name

app = typer.Typer(
    help=(
        "Dante Domain Manager through its Managed API. Every GraphQL query and mutation is a command here, "
        "generated from the bundled schema; status, devices, and refresh read the daemon's inventory."
    ),
    no_args_is_help=True,
)

DAEMON_UNAVAILABLE = "netaudio daemon is not running."


def _status_rows(result: dict) -> list[list[str]]:
    fields = (
        ("State", result.get("state")),
        ("Endpoint", result.get("url")),
        ("Fresh", result.get("fresh")),
        ("Refresh Interval", result.get("refresh_interval")),
        ("Domains", result.get("domain_count")),
        ("Enrolled Devices", result.get("enrolled_device_count")),
        ("Unenrolled Devices", result.get("unenrolled_device_count")),
        ("Last Error", result.get("last_error")),
    )
    return [[name, "" if value is None else str(value)] for name, value in fields]


@app.command("status")
def status() -> None:
    """Show the daemon's Managed API connection and inventory state."""
    result = asyncio.run(get_ddm_status_from_daemon())
    if result is None:
        fail(DAEMON_UNAVAILABLE)
    output_table(["Setting", "Value"], _status_rows(result), json_data=result)


@app.command("devices")
def devices() -> None:
    """List devices the daemon has seen through the Managed API, merged with direct discovery."""
    result = asyncio.run(get_ddm_devices_from_daemon())
    if result is None:
        fail(DAEMON_UNAVAILABLE)
    rows = []
    for key, device in sorted((result or {}).items(), key=lambda item: (item[1].get("name") or item[0]).lower()):
        rows.append(
            [
                device.get("name") or key,
                device.get("management_state") or "",
                device.get("ddm_domain_name") or "",
                device.get("ddm_connection_state") or "",
                device.get("ipv4") if device.get("ipv4") not in (None, "None") else "",
                "" if device.get("tx_count") is None else str(device.get("tx_count")),
                "" if device.get("rx_count") is None else str(device.get("rx_count")),
                ", ".join(device.get("control_transports") or []),
            ]
        )
    output_table(
        ["Name", "Management", "Domain", "Connection", "IP Address", "TX", "RX", "Control"],
        rows,
        json_data=result,
        empty_message="The daemon has not seen any devices through the Managed API.",
    )


@app.command("refresh")
def refresh() -> None:
    """Ask the daemon to re-read the Managed API inventory now."""
    response_status, result = asyncio.run(refresh_ddm_inventory_on_daemon())
    if response_status is None or result is None:
        fail(DAEMON_UNAVAILABLE)
    output_table(["Setting", "Value"], _status_rows(result), json_data=result)
    if response_status != 200:
        raise typer.Exit(code=1)


@app.command("graphql")
def graphql(
    query: Optional[str] = typer.Argument(None, help="GraphQL document to send. Omit when using --file."),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Read the GraphQL document from a file."),
    variables: Optional[str] = typer.Option(None, "--variables", "-V", help="Variables as a JSON object."),
    operation_name: Optional[str] = typer.Option(
        None, "--operation-name", help="Operation to run when the document defines several."
    ),
) -> None:
    """Send any GraphQL document to the Managed API and print the response."""
    if file is not None:
        try:
            query = file.read_text(encoding="utf-8")
        except OSError as exception:
            fail(f"could not read {file}: {exception}")
    if not query or not query.strip():
        fail("pass a GraphQL document or --file")
    decoded_variables = {}
    if variables:
        try:
            decoded_variables = json.loads(variables)
        except json.JSONDecodeError as exception:
            fail(f"--variables must be a JSON object: {exception}")
        if not isinstance(decoded_variables, dict):
            fail("--variables must be a JSON object")
    response = execute(query, decoded_variables, operation_name)
    report_errors(response)
    if response.get("data") is None:
        raise typer.Exit(code=1)
    output_single(json.dumps(response["data"], indent=2, sort_keys=True))


@app.command("schema")
def schema(
    type_name: Optional[str] = typer.Argument(None, help="Type to describe. Omit to list queries and mutations."),
) -> None:
    """Browse the bundled Managed API schema: operations, or the fields of one type."""
    loaded = Schema.load()
    if type_name:
        try:
            lines = loaded.describe_type(type_name)
        except SchemaError as exception:
            fail(str(exception))
        typer.echo("\n".join(lines))
        return
    rows = []
    for operation, fields in (("query", loaded.query_fields), ("mutation", loaded.mutation_fields)):
        for field in fields:
            arguments = ", ".join(f"{argument.name}: {argument.type.render()}" for argument in field.arguments)
            rows.append([operation, command_name(field.name), field.name, arguments, field.type.render()])
    output_table(["Operation", "Command", "GraphQL Field", "Arguments", "Returns"], rows)


register_schema_operations(app, Schema.load())
