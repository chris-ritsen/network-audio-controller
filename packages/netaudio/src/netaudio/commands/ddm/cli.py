from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Optional

import typer

from netaudio import core
from netaudio.cli_support.output import output_single, output_table
from netaudio.commands.ddm.operations import register_schema_operations
from netaudio.commands.ddm.render import report_errors
from netaudio.commands.ddm.transport import execute, fail
from netaudio.daemon.client import (
    get_ddm_devices_from_daemon,
    get_ddm_status_from_daemon,
    refresh_ddm_inventory_on_daemon,
)
from netaudio.ddm import (
    ControllerServiceError,
    ManagedAPIError,
    Schema,
    SchemaError,
    authenticate_with_password,
    command_name,
    discover_ddm_servers,
    identify_managed_device,
    identify_managed_device_with_api_key,
)

app = typer.Typer(
    help=(
        "Discover Dante Domain Manager, use its Controller service, or query its Managed API. "
        "GraphQL queries and mutations are generated from the bundled schema; status, devices, and refresh "
        "read the daemon's inventory."
    ),
    no_args_is_help=True,
)

DAEMON_UNAVAILABLE = "netaudio daemon is not running."
PASSWORD_LOGIN_FIELD = "UserLoginWithPassword"


@app.command("discover")
def discover(
    timeout: float = typer.Option(
        2.0, "--timeout", "-t", min=0.1, max=60.0, help="Seconds to listen for DDM advertisements."
    ),
) -> None:
    """Discover DDM servers and their advertised control services."""
    try:
        servers = asyncio.run(discover_ddm_servers(timeout=timeout))
    except (OSError, ValueError) as exception:
        fail(f"DDM discovery failed: {exception}")
    rows = [
        [
            server.server_name,
            ", ".join(server.ipv4_addresses),
            str(server.controller_service.port) if server.controller_service else "",
            str(server.device_service.port) if server.device_service else "",
        ]
        for server in servers
    ]
    output_table(
        ["Server", "IP Addresses", "Controller Port", "Device Port"],
        rows,
        json_data=[server.to_json() for server in servers],
        empty_message="No DDM servers discovered.",
    )


def _discovered_controller(timeout: float) -> tuple[str, int]:
    try:
        servers = asyncio.run(discover_ddm_servers(timeout=timeout))
    except (OSError, ValueError) as exception:
        fail(f"DDM discovery failed: {exception}")
    controllers = [server for server in servers if server.controller_service is not None]
    if not controllers:
        fail("no DDM Controller service was discovered; pass --server")
    if len(controllers) > 1:
        names = ", ".join(server.server_name.rstrip(".") for server in controllers)
        fail(f"multiple DDM Controller services were discovered ({names}); pass --server")
    selected = controllers[0]
    return selected.server_name.rstrip("."), selected.controller_service.port


@app.command("identify")
def identify(
    device_id: str = typer.Argument(..., help="Managed device ID, with optional :0 process suffix."),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        "-u",
        help="DDM username or email address; prompts for its password.",
    ),
    api_key: bool = typer.Option(
        False,
        "--api-key",
        help="Prompt for a DDM API key instead of a username and password.",
    ),
    api_key_file: Optional[Path] = typer.Option(
        None,
        "--api-key-file",
        help="Read a DDM API key from this file instead of prompting.",
    ),
    server: Optional[str] = typer.Option(None, "--server", help="DDM hostname or IP; discovered by mDNS when omitted."),
    auth_port: Optional[int] = typer.Option(
        None,
        "--auth-port",
        min=1,
        max=65535,
        help="DDM HTTPS authentication port; defaults to its advertisement or 8443.",
    ),
    timeout: float = typer.Option(10.0, "--timeout", "-t", min=0.1, max=60.0, help="Network timeout in seconds."),
    ca_file: Optional[Path] = typer.Option(None, "--ca-file", help="CA certificate used to verify DDM TLS."),
    insecure_tls: bool = typer.Option(
        False,
        "--insecure-tls",
        help="Disable DDM TLS certificate verification. Intended only for isolated labs.",
    ),
) -> None:
    """Authenticate to DDM and identify one enrolled device."""
    if username is not None and (api_key or api_key_file is not None):
        fail("pass --username or an API-key option, not both")
    if api_key and api_key_file is not None:
        fail("pass --api-key or --api-key-file, not both")
    if username is None and not api_key and api_key_file is None:
        fail("pass --username, --api-key, or --api-key-file")
    selected_server = server
    selected_port = auth_port
    if selected_server is None:
        selected_server, advertised_port = _discovered_controller(min(timeout, 2.0))
        selected_port = selected_port or advertised_port
    selected_port = selected_port or 8443
    mac = core.host_mac()
    if mac is None:
        fail("could not determine a host MAC address for the Identify request")
    try:
        if username is not None:
            password = typer.prompt("Password", hide_input=True)
            identify_managed_device(
                selected_server,
                username,
                password,
                device_id,
                mac,
                auth_port=selected_port,
                timeout=timeout,
                ca_file=ca_file,
                insecure_tls=insecure_tls,
            )
        else:
            if api_key_file is not None:
                try:
                    credential = api_key_file.read_text(encoding="ascii").rstrip("\r\n")
                except (OSError, UnicodeError) as exception:
                    fail(f"could not read DDM API key from {api_key_file}: {exception}")
            else:
                credential = typer.prompt("API key", hide_input=True)
            identify_managed_device_with_api_key(
                selected_server,
                credential,
                device_id,
                mac,
                auth_port=selected_port,
                timeout=timeout,
                ca_file=ca_file,
                insecure_tls=insecure_tls,
            )
    except (ControllerServiceError, core.NetaudioCoreError, ValueError) as exception:
        fail(str(exception))
    typer.echo(f"Identified managed device {device_id} through {selected_server}.")


def _login_configuration() -> tuple[str | None, Path | None]:
    from netaudio.common.config_loader import default_config_path, load_config_document
    from netaudio.common.managed_api import resolve_managed_api_configuration

    config_path = default_config_path()
    configuration = resolve_managed_api_configuration(
        load_config_document(config_path),
        base_directory=config_path.parent,
    )
    return configuration.url, configuration.credential_file


def _write_credential_file(path: Path, credential: str) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temporary_path = Path(temporary_name)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii", newline="\n") as output:
            descriptor = -1
            output.write(f"{credential}\n")
            output.flush()
            os.fsync(output.fileno())
        temporary_path.chmod(0o600)
        os.replace(temporary_path, destination)
        destination.chmod(0o600)
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)
        raise


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


@app.command("login")
def login(
    username: str = typer.Option(..., "--username", "-u", help="DDM username or email address."),
    url: Optional[str] = typer.Option(None, "--url", help="Managed API URL ending in /graphql."),
    credential_file: Optional[Path] = typer.Option(
        None,
        "--credential-file",
        help="File in which to save the returned Managed API credential.",
    ),
    allow_insecure_http: bool = typer.Option(
        False,
        "--allow-insecure-http",
        help="Allow the password to be sent to an HTTP endpoint. Intended only for isolated labs.",
    ),
) -> None:
    """Log in with a hidden password prompt and save a Managed API credential."""
    try:
        configured_url, configured_credential_file = _login_configuration()
    except ValueError as exception:
        fail(str(exception))
    endpoint = url or configured_url
    destination = credential_file or configured_credential_file
    if endpoint is None:
        fail("pass --url or configure ddm.url")
    if destination is None:
        fail("pass --credential-file or configure ddm.api_key_file")
    password = typer.prompt("Password", hide_input=True)
    try:
        credential = authenticate_with_password(
            endpoint,
            username,
            password,
            allow_insecure_http=allow_insecure_http,
        )
    except (ManagedAPIError, ValueError) as exception:
        fail(str(exception))
    try:
        _write_credential_file(destination, credential)
    except OSError as exception:
        fail(f"could not save Managed API credential to {destination}: {exception}")
    typer.echo(f"Saved Managed API credential to {destination}")


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
            command = "login" if field.name == PASSWORD_LOGIN_FIELD else command_name(field.name)
            rows.append([operation, command, field.name, arguments, field.type.render()])
    output_table(["Operation", "Command", "GraphQL Field", "Arguments", "Returns"], rows)


register_schema_operations(app, Schema.load(), excluded_fields=frozenset({PASSWORD_LOGIN_FIELD}))
