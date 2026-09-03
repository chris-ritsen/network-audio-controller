from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

import typer

from netaudio.cli_support.output import output_single, output_table
from netaudio.commands.ddm.operations import operation_command_path, register_schema_operations
from netaudio.commands.ddm.render import report_errors
from netaudio.commands.ddm.transport import execute, fail
from netaudio.daemon.client import (
    get_ddm_devices_from_daemon,
    get_ddm_status_from_daemon,
    refresh_ddm_inventory_on_daemon,
)
from netaudio.ddm import (
    ControllerAPIClient,
    ControllerServiceError,
    ManagedAPIError,
    ManagedAPIClient,
    Schema,
    SchemaError,
    authenticate_with_password,
    discover_ddm_servers,
)

app = typer.Typer(
    help=(
        "Discover Dante Domain Manager or query its Managed API. "
        "GraphQL queries and mutations are generated from the bundled schema; status, devices, and refresh "
        "read the daemon's inventory."
    ),
    no_args_is_help=True,
)
context_app = typer.Typer(help="List and select saved DDM server/domain contexts.", no_args_is_help=True)
api_app = typer.Typer(
    help="Use the low-level Dante Managed API. Ordinary device control uses the normal NetAudio commands.",
    no_args_is_help=True,
)
app.add_typer(context_app, name="context")
app.add_typer(api_app, name="api")

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


def _ddm_configuration():
    from netaudio.common.config_loader import default_config_path, load_config_document
    from netaudio.common.managed_api import resolve_ddm_configuration

    config_path = default_config_path()
    configuration = resolve_ddm_configuration(
        load_config_document(config_path),
        base_directory=config_path.parent,
    )
    return configuration, config_path


def _active_context() -> str | None:
    from netaudio.cli_support.context import _get_state

    return _get_state().ddm_context


def _slug(value: str, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip().rstrip(".")).strip(".-")
    return normalized or fallback


def _unused_name(base: str, existing: set[str]) -> str:
    if base not in existing:
        return base
    suffix = 2
    while f"{base}-{suffix}" in existing:
        suffix += 1
    return f"{base}-{suffix}"


def _choose_index(description: str, labels: list[str]) -> int:
    if len(labels) == 1:
        return 0
    typer.echo(f"Available {description}:")
    for number, label in enumerate(labels, 1):
        typer.echo(f"  {number}. {label}")
    selected = typer.prompt(f"Select {description}", type=typer.IntRange(1, len(labels)))
    return selected - 1


def _select_discovered_server(timeout: float):
    try:
        servers = asyncio.run(discover_ddm_servers(timeout=timeout))
    except (OSError, ValueError) as exception:
        fail(f"DDM discovery failed: {exception}")
    candidates = [server for server in servers if server.controller_service and server.ipv4_addresses]
    if not candidates:
        fail("no DDM Controller services were discovered; pass --url to connect manually")
    index = _choose_index(
        "DDM server",
        [f"{server.server_name} ({', '.join(server.ipv4_addresses)})" for server in candidates],
    )
    return candidates[index]


def _select_domain(domains, requested: str | None):
    available = sorted((domain for domain in domains if domain is not None), key=lambda item: item.name or item.id)
    if not available:
        fail("the authenticated account cannot see any DDM domains")
    if requested is not None:
        exact = [
            domain
            for domain in available
            if domain.id == requested or (domain.name and domain.name.casefold() == requested.casefold())
        ]
        if len(exact) != 1:
            fail(f"domain {requested!r} did not uniquely match an ID or name")
        return exact[0]
    index = _choose_index("domain", [f"{domain.name or '(unnamed)'} ({domain.id})" for domain in available])
    return available[index]


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
        ("Default Context", result.get("default_context")),
        ("Servers", result.get("server_count")),
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
    selected_context = _active_context()
    result = asyncio.run(get_ddm_devices_from_daemon(selected_context))
    if result is None:
        fail(DAEMON_UNAVAILABLE)
    rows = []
    selected_devices = {
        key: device
        for key, device in (result or {}).items()
        if selected_context is None or device.get("ddm_context") == selected_context
    }
    for key, device in sorted(selected_devices.items(), key=lambda item: (item[1].get("name") or item[0]).lower()):
        rows.append(
            [
                device.get("name") or key,
                device.get("ddm_context") or "",
                device.get("ddm_server_profile") or "",
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
        ["Name", "Context", "Server", "Management", "Domain", "Connection", "IP Address", "TX", "RX", "Control"],
        rows,
        json_data=selected_devices,
        empty_message="The daemon has not seen any devices through the Managed API.",
    )


@app.command("refresh")
def refresh() -> None:
    """Ask the daemon to re-read the Managed API inventory now."""
    response_status, result = asyncio.run(refresh_ddm_inventory_on_daemon(_active_context()))
    if response_status is None or result is None:
        fail(DAEMON_UNAVAILABLE)
    output_table(["Setting", "Value"], _status_rows(result), json_data=result)
    if response_status != 200:
        raise typer.Exit(code=1)


@app.command("login")
def login(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="DDM username or email address."),
    url: Optional[str] = typer.Option(None, "--url", help="Managed API URL ending in /graphql."),
    server_profile: Optional[str] = typer.Option(
        None,
        "--server-profile",
        help="Name for the saved DDM server profile, or an existing profile to update.",
    ),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain ID or unique domain name."),
    context_name: Optional[str] = typer.Option(
        None, "--context-name", help="Name for the saved server/domain context."
    ),
    save_credential_file: Optional[Path] = typer.Option(
        None,
        "--save-credential",
        help="File in which to save the returned Managed API credential.",
    ),
    credential_file: Optional[Path] = typer.Option(
        None,
        "--credential-file",
        help="Authenticate with an existing Managed API credential file instead of username/password.",
    ),
    make_default: bool = typer.Option(False, "--default", help="Make the saved context the default."),
    discovery_timeout: float = typer.Option(
        2.0,
        "--discovery-timeout",
        min=0.1,
        max=60.0,
        help="Seconds to listen when discovering a server because --url was omitted.",
    ),
) -> None:
    """Discover or select DDM, authenticate, choose a domain, and save a context."""
    try:
        configuration, config_path = _ddm_configuration()
    except ValueError as exception:
        fail(str(exception))

    endpoint = url
    discovered_name = None
    if endpoint is None and server_profile is not None and server_profile in configuration.servers:
        endpoint = configuration.server(server_profile).url
    if endpoint is None:
        discovered = _select_discovered_server(discovery_timeout)
        discovered_name = discovered.server_name
        service = discovered.controller_service
        if service is None:
            fail("the selected DDM server did not advertise a Controller service")
        try:
            endpoint = (
                ControllerAPIClient(
                    discovered.ipv4_addresses[0],
                    port=service.port,
                    timeout=discovery_timeout,
                )
                .endpoints()
                .graphql_url
            )
        except (ControllerServiceError, ValueError) as exception:
            fail(f"could not obtain the Managed API URL from the selected DDM server: {exception}")
    if endpoint is None:
        fail("could not determine the Managed API URL")
    if server_profile is not None and server_profile in configuration.servers:
        configured_endpoint = configuration.server(server_profile).url
        if configured_endpoint != endpoint:
            fail(
                f"DDM server profile {server_profile!r} already points to {configured_endpoint}; "
                "choose a new --server-profile name for a different URL"
            )

    credential = None
    source_credential_file = credential_file.expanduser().resolve() if credential_file is not None else None
    if source_credential_file is not None:
        client = ManagedAPIClient(endpoint, credential_file=source_credential_file)
    else:
        login_name = username or typer.prompt("Username")
        password = typer.prompt("Password", hide_input=True)
        try:
            credential = authenticate_with_password(endpoint, login_name, password)
        except (ManagedAPIError, ValueError) as exception:
            fail(str(exception))
        client = ManagedAPIClient(endpoint, credential=credential)
    try:
        inventory = client.inventory()
    except (ManagedAPIError, ValueError) as exception:
        fail(str(exception))
    if inventory.data is None:
        messages = "; ".join(issue.message for issue in inventory.errors)
        fail(messages or "DDM returned no domain inventory")
    selected_domain = _select_domain(inventory.data.domains or (), domain)

    if server_profile is None:
        from urllib.parse import urlsplit

        suggested_server = _slug(discovered_name or urlsplit(endpoint).hostname or "ddm", "ddm")
        matching = [name for name, server in configuration.servers.items() if server.url == endpoint]
        server_profile = matching[0] if matching else _unused_name(suggested_server, set(configuration.servers))
    suggested_context = f"{server_profile}-{_slug(selected_domain.name or selected_domain.id, 'domain')}"
    if context_name is None:
        matching = [
            name
            for name, context in configuration.contexts.items()
            if context.server == server_profile and context.domain_id == selected_domain.id
        ]
        context_name = matching[0] if matching else _unused_name(suggested_context, set(configuration.contexts))
    elif context_name in configuration.contexts:
        configured_context = configuration.context(context_name)
        if configured_context.server != server_profile or configured_context.domain_id != selected_domain.id:
            fail(
                f"DDM context {context_name!r} already selects server {configured_context.server!r} "
                f"and domain {configured_context.domain_id}; choose a new --context-name"
            )

    destination = source_credential_file or save_credential_file
    if destination is None:
        destination = config_path.parent / "credentials" / f"{server_profile}.credential"
    if credential is not None:
        try:
            _write_credential_file(destination, credential)
        except OSError as exception:
            fail(f"could not save Managed API credential to {destination}: {exception}")
    try:
        from netaudio.common.ddm_config_store import save_ddm_context

        save_ddm_context(
            config_path,
            server_name=server_profile,
            url=endpoint,
            credential_file=destination,
            context_name=context_name,
            domain_id=selected_domain.id,
            domain_name=selected_domain.name,
            make_default=make_default or configuration.default_context is None,
        )
    except (OSError, ValueError) as exception:
        fail(f"could not save DDM context: {exception}")
    typer.echo(f"Saved DDM context {context_name} for {selected_domain.name or selected_domain.id} on {server_profile}")


@context_app.command("list")
def context_list() -> None:
    """List saved DDM server/domain contexts."""
    try:
        configuration, _ = _ddm_configuration()
    except ValueError as exception:
        fail(str(exception))
    rows = []
    records = []
    for name, context in sorted(configuration.contexts.items()):
        server = configuration.server(context.server)
        record = {
            "name": name,
            "default": name == configuration.default_context,
            "server": context.server,
            "url": server.url,
            "domain_id": context.domain_id,
            "domain_name": context.domain_name,
        }
        records.append(record)
        rows.append(
            [
                name,
                "yes" if record["default"] else "",
                context.server,
                context.domain_name or "",
                context.domain_id,
                server.url or "",
            ]
        )
    output_table(
        ["Context", "Default", "Server", "Domain", "Domain ID", "URL"],
        rows,
        json_data=records,
        empty_message="No DDM contexts are configured. Run netaudio ddm login.",
    )


@context_app.command("current")
def context_current() -> None:
    """Show the context selected by --context, the environment, or configuration."""
    selected = _active_context()
    if selected is None:
        fail("no DDM context is selected")
    output_single(selected)


@context_app.command("use")
def context_use(name: str = typer.Argument(..., help="Saved DDM context name.")) -> None:
    """Set the default DDM context."""
    try:
        configuration, config_path = _ddm_configuration()
        configuration.context(name)
        from netaudio.common.ddm_config_store import set_default_ddm_context

        set_default_ddm_context(config_path, name)
    except (OSError, ValueError) as exception:
        fail(str(exception))
    typer.echo(f"Default DDM context is now {name}")


@api_app.command("graphql", no_args_is_help=True)
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


@api_app.command("schema")
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
            command = (
                "ddm login"
                if field.name == PASSWORD_LOGIN_FIELD
                else " ".join(operation_command_path(operation, field.name))
            )
            rows.append([operation, command, field.name, arguments, field.type.render()])
    output_table(["Operation", "Command", "GraphQL Field", "Arguments", "Returns"], rows)


register_schema_operations(api_app, Schema.load(), excluded_fields=frozenset({PASSWORD_LOGIN_FIELD}))
