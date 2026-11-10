from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlsplit

import typer

from netaudio.cli_support.context import _get_state
from netaudio.cli_support.output import output_single, output_table
from netaudio.commands.ddm.operations import operation_command_path, register_schema_operations
from netaudio.commands.ddm.render import redact_sensitive_values, report_errors
from netaudio.commands.ddm.schema import Schema, SchemaError
from netaudio.commands.ddm.transport import execute, fail
from netaudio.daemon.client import ask_daemon, call_daemon

app = typer.Typer(
    help="Manage Dante Domain Manager connections, enrollment and the Managed API.",
    no_args_is_help=True,
)
context_app = typer.Typer(help="List and select saved DDM server/domain contexts.", no_args_is_help=True)
api_app = typer.Typer(
    help="Use the low-level Dante Managed API. Ordinary device control uses the normal NetAudio commands.",
    no_args_is_help=True,
)
app.add_typer(context_app, name="context")
app.add_typer(api_app, name="api")

PASSWORD_LOGIN_FIELD = "UserLoginWithPassword"
EXCLUDED_GENERATED_FIELDS = frozenset(
    {
        PASSWORD_LOGIN_FIELD,
        "AdminInitialPasswordSet",
        "DeviceSerialportconfigSet",
        "UserAPIKeyAdd",
    }
)


def _scoped(path: str) -> str:
    context = _get_state().ddm_context
    return f"{path}?context={quote(context, safe='')}" if context is not None else path


def _choose(description: str, labels: list[str]) -> int:
    if len(labels) == 1:
        return 0
    typer.echo(f"Available {description}:")
    for number, label in enumerate(labels, 1):
        typer.echo(f"  {number}. {label}")
    return typer.prompt(f"Select {description}", type=typer.IntRange(1, len(labels))) - 1


def _select_domain(domains: list[dict], requested: str | None) -> dict:
    available = sorted(domains, key=lambda domain: domain.get("name") or domain["id"])
    if not available:
        fail("the authenticated account cannot see any DDM domains")
    if requested is not None:
        wanted = requested.casefold()
        exact = [
            domain for domain in available if wanted in {domain["id"].casefold(), (domain.get("name") or "").casefold()}
        ]
        if len(exact) != 1:
            fail(f"domain {requested!r} did not uniquely match an ID or name")
        return exact[0]
    labels = [f"{domain.get('name') or '(unnamed)'} ({domain['id']})" for domain in available]
    return available[_choose("domain", labels)]


def _last_error(result: dict) -> str | None:
    if result.get("last_error"):
        return str(result["last_error"])
    errors = [
        f"{name}: {server['last_error']}"
        for name, server in sorted((result.get("servers") or {}).items())
        if isinstance(server, dict) and server.get("last_error")
    ]
    return "; ".join(errors) or None


def _show_status(result: dict) -> None:
    fields = (
        ("State", result.get("state")),
        ("Default Context", result.get("default_context")),
        ("Servers", result.get("server_count")),
        ("Fresh", result.get("fresh")),
        ("Domains", result.get("domain_count")),
        ("Devices", result.get("device_count")),
        ("Last Error", _last_error(result)),
    )
    rows = [[name, str(value)] for name, value in fields if value is not None]
    output_table(["Setting", "Value"], rows, json_data=result)


@app.command("status", help="Show the Managed API connection and inventory state.")
def status() -> None:
    _show_status(asyncio.run(call_daemon("GET", "/ddm/status")))


async def run_devices() -> None:
    devices = await call_daemon("GET", _scoped("/ddm/devices"))
    if not devices:
        error = _last_error(await call_daemon("GET", "/ddm/status"))
        if error:
            fail(f"The Managed API is unreachable: {error}")
    rows = [
        [
            device.get("name") or key,
            device.get("ddm_context") or "",
            device.get("ddm_server_profile") or "",
            device.get("management_state") or "",
            device.get("ddm_domain_name") or "",
            device.get("ddm_connection_state") or "",
            device.get("ipv4") or "",
            "" if device.get("tx_count") is None else str(device["tx_count"]),
            "" if device.get("rx_count") is None else str(device["rx_count"]),
            ", ".join(device.get("control_transports") or []),
        ]
        for key, device in sorted(devices.items(), key=lambda item: (item[1].get("name") or item[0]).lower())
    ]
    output_table(
        ["Name", "Context", "Server", "Management", "Domain", "Connection", "IP Address", "TX", "RX", "Control"],
        rows,
        json_data=devices,
        empty_message="No devices have been seen through the Managed API.",
    )


@app.command("devices", help="List devices seen through the Managed API.")
def devices() -> None:
    asyncio.run(run_devices())


def _matching_devices(devices: dict, identifier: str) -> list[dict]:
    wanted = identifier.casefold()
    return [
        device
        for device in devices.values()
        if device.get("ddm_device_id")
        and wanted in {(device.get("name") or "").casefold(), device["ddm_device_id"].casefold()}
    ]


async def _inventory_device(identifier: str) -> dict:
    matches = _matching_devices(await call_daemon("GET", _scoped("/ddm/devices")), identifier)
    if not matches:
        await call_daemon("POST", "/ddm/refresh", {"context": _get_state().ddm_context}, timeout=30.0)
        matches = _matching_devices(await call_daemon("GET", _scoped("/ddm/devices")), identifier)
    if not matches:
        fail(f"{identifier} is not in the DDM inventory.")
    if len(matches) > 1:
        servers = ", ".join(sorted({device.get("ddm_server_profile") or "" for device in matches}))
        fail(f"{identifier} matches devices on several DDM servers ({servers}); choose one with --context.")
    return matches[0]


async def _change_enrollment(device: dict, change: dict, done: str, pending: str) -> None:
    request = {"device_id": device["ddm_device_id"], "server": device["ddm_server_profile"], **change}
    response_status, result = await ask_daemon("POST", "/ddm/enrollment", request, timeout=60.0)
    result = result if isinstance(result, dict) else {}
    if response_status != 200:
        fail(result.get("error") or f"DDM did not accept the {change['action']} request.")
    if not result.get("state_verified"):
        fail(pending)
    typer.echo(done)


async def run_enroll(device_name: str, domain_name: str, yes: bool) -> None:
    device = await _inventory_device(device_name)
    server = device["ddm_server_profile"]
    domains = [domain for domain in await call_daemon("GET", "/ddm/domains") if domain["ddm_server_profile"] == server]
    wanted = domain_name.casefold()
    domain = next(
        (domain for domain in domains if wanted in {(domain.get("name") or "").casefold(), domain["id"].casefold()}),
        None,
    )
    if domain is None:
        names = ", ".join(sorted(domain.get("name") or domain["id"] for domain in domains)) or "none"
        fail(f"{domain_name} is not a domain on {server}. Domains: {names}.")
    label = device.get("name") or device["ddm_device_id"]
    domain_label = domain.get("name") or domain["id"]
    if not yes and not typer.confirm(f"Enroll {label} in {domain_label}?"):
        raise typer.Exit(code=1)
    await _change_enrollment(
        device,
        {"action": "enroll", "domain_id": domain["id"]},
        f"Enrolled {label} in {domain_label}.",
        f"DDM accepted the request, but its inventory does not show {label} in {domain_label} yet.",
    )


@app.command("enroll", help="Enroll a device in a Dante Domain Manager domain, keeping its configuration.")
def enroll(
    device_name: str = typer.Argument(..., metavar="DEVICE", help="Device name or DDM device ID."),
    domain_name: str = typer.Argument(..., metavar="DOMAIN", help="Domain name or ID."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    asyncio.run(run_enroll(device_name, domain_name, yes))


async def run_unenroll(device_name: str, yes: bool) -> None:
    device = await _inventory_device(device_name)
    label = device.get("name") or device["ddm_device_id"]
    domain = device.get("ddm_domain_name") or device.get("ddm_domain_id") or "its domain"
    if not yes and not typer.confirm(f"Unenroll {label} from {domain}?"):
        raise typer.Exit(code=1)
    await _change_enrollment(
        device,
        {"action": "unenroll"},
        f"Unenrolled {label} from {domain}.",
        f"DDM accepted the request, but its inventory still shows {label} in a domain.",
    )


@app.command("unenroll", help="Unenroll a device from its Dante Domain Manager domain, keeping its configuration.")
def unenroll(
    device_name: str = typer.Argument(..., metavar="DEVICE", help="Device name or DDM device ID."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    asyncio.run(run_unenroll(device_name, yes))


@app.command("refresh", help="Read the Managed API inventory again now.")
def refresh() -> None:
    response_status, result = asyncio.run(
        ask_daemon("POST", "/ddm/refresh", {"context": _get_state().ddm_context}, timeout=30.0)
    )
    if not isinstance(result, dict) or "state" not in result:
        fail((result or {}).get("error") or "the Managed API inventory could not be refreshed")
    _show_status(result)
    if response_status != 200:
        raise typer.Exit(code=1)


def _profile_name(url: str, servers: dict[str, dict]) -> str:
    matching = [name for name, server in servers.items() if server.get("url") == url]
    if matching:
        return matching[0]
    base = re.sub(r"[^A-Za-z0-9_.-]+", "-", urlsplit(url).hostname or "").strip(".-") or "ddm"
    name, suffix = base, 2
    while name in servers:
        name, suffix = f"{base}-{suffix}", suffix + 1
    return name


async def run_login(
    username: str | None,
    url: str | None,
    server_profile: str | None,
    domain: str | None,
    credential_file: Path | None,
) -> None:
    connections = await call_daemon("GET", "/ddm/connections")
    servers = {server["name"]: server for server in connections["servers"]}
    if server_profile is None and url is None and len(servers) == 1:
        server_profile = next(iter(servers))
    if server_profile is None:
        if url is None:
            fail("pass --url for a new DDM server, or --server-profile for a saved one")
        server_profile = _profile_name(url, servers)
    elif url is None and server_profile not in servers:
        fail(f"{server_profile} is not a saved DDM server; pass --url to add it")

    if credential_file is not None:
        try:
            api_key = credential_file.expanduser().read_text(encoding="ascii").strip()
        except (OSError, UnicodeError) as exception:
            fail(f"could not read {credential_file}: {exception}")
        authentication = {"api_key": api_key, "method": "api_key"}
    else:
        login_name = username or typer.prompt("Username")
        password = typer.prompt("Password", hide_input=True)
        authentication = {"method": "password", "password": password, "username": login_name}
    await call_daemon("POST", "/ddm/login", {"server": server_profile, "url": url, **authentication}, timeout=60.0)

    domains = [
        candidate
        for candidate in await call_daemon("GET", "/ddm/domains")
        if candidate["ddm_server_profile"] == server_profile
    ]
    selected = _select_domain(domains, domain)
    connections = await call_daemon(
        "POST", "/ddm/context", {"domain_id": selected["id"], "server": server_profile}, timeout=60.0
    )
    context = next(
        context["name"]
        for context in connections["contexts"]
        if context["server"] == server_profile and context["domain_id"] == selected["id"]
    )
    typer.echo(f"Saved DDM context {context} for {selected.get('name') or selected['id']} on {server_profile}")


@app.command("login", help="Authenticate with a DDM server, choose a domain and save the context.")
def login(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="DDM username or email address."),
    url: Optional[str] = typer.Option(None, "--url", help="Managed API URL ending in /graphql."),
    server_profile: Optional[str] = typer.Option(
        None,
        "--server-profile",
        help="Name for the saved DDM server profile, or an existing profile to update.",
    ),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain ID or unique domain name."),
    credential_file: Optional[Path] = typer.Option(
        None,
        "--credential-file",
        help="Authenticate with a Managed API key read from this file instead of a username and password.",
    ),
) -> None:
    asyncio.run(run_login(username, url, server_profile, domain, credential_file))


@context_app.command("list", help="List saved DDM server/domain contexts.")
def context_list() -> None:
    connections = asyncio.run(call_daemon("GET", "/ddm/connections"))
    urls = {server["name"]: server.get("url") for server in connections["servers"]}
    records = [
        {
            "default": context["name"] == connections["default_context"],
            "domain_id": context["domain_id"],
            "domain_name": context.get("domain_name"),
            "name": context["name"],
            "server": context["server"],
            "url": urls.get(context["server"]),
        }
        for context in sorted(connections["contexts"], key=lambda context: context["name"])
    ]
    rows = [
        [
            record["name"],
            "yes" if record["default"] else "",
            record["server"],
            record["domain_name"] or "",
            record["domain_id"],
            record["url"] or "",
        ]
        for record in records
    ]
    output_table(
        ["Context", "Default", "Server", "Domain", "Domain ID", "URL"],
        rows,
        json_data=records,
        empty_message="No DDM contexts are configured. Run netaudio ddm login.",
    )


@context_app.command("current", help="Show the context selected by --context, the environment or configuration.")
def context_current() -> None:
    selected = _get_state().ddm_context or asyncio.run(call_daemon("GET", "/ddm/connections"))["default_context"]
    if selected is None:
        fail("no DDM context is selected")
    output_single(selected)


@context_app.command("use", help="Set the default DDM context.")
def context_use(name: str = typer.Argument(..., help="Saved DDM context name.")) -> None:
    asyncio.run(call_daemon("POST", "/ddm/context", {"context": name}))
    typer.echo(f"Default DDM context is now {name}")


@api_app.command("graphql", no_args_is_help=True)
def graphql(
    query: Optional[str] = typer.Argument(None, help="GraphQL document to send. Omit when using --file."),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Read the GraphQL document from a file."),
    variables_file: Optional[Path] = typer.Option(
        None,
        "--variables-file",
        "-V",
        help="Read a JSON variables object from a file.",
    ),
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
    if variables_file is not None:
        try:
            decoded_variables = json.loads(variables_file.read_text(encoding="utf-8"))
        except OSError as exception:
            fail(f"could not read {variables_file}: {exception}")
        except json.JSONDecodeError as exception:
            fail(f"--variables-file must contain a JSON object: {exception}")
        if not isinstance(decoded_variables, dict):
            fail("--variables-file must contain a JSON object")
    response = execute(query, decoded_variables, operation_name)
    has_errors = report_errors(response)
    data = response.get("data")
    if data is not None:
        output_single(json.dumps(redact_sensitive_values(data), indent=2, sort_keys=True))
    if has_errors or data is None:
        raise typer.Exit(code=1)


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
                else (
                    "not exposed"
                    if field.name in EXCLUDED_GENERATED_FIELDS
                    else " ".join(operation_command_path(operation, field.name))
                )
            )
            rows.append([operation, command, field.name, arguments, field.type.render()])
    output_table(["Operation", "Command", "GraphQL Field", "Arguments", "Returns"], rows)


register_schema_operations(api_app, Schema.load(), excluded_fields=EXCLUDED_GENERATED_FIELDS)
