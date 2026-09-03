from __future__ import annotations

import json
from typing import Any

import typer

from netaudio.cli_support.output import output_single, output_table
from netaudio.commands.ddm.transport import fail

SCALAR_TYPES = (bool, float, int, str)
UNSPECIFIED_ADDRESS = "0.0.0.0"
REDACTED = "[redacted]"


def is_sensitive_name(key: str) -> bool:
    normalized = "".join(character for character in key.casefold() if character.isalnum())
    return normalized.endswith(("apikey", "authorization", "password", "token", "credential", "secret"))


def redact_sensitive_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: REDACTED if is_sensitive_name(str(key)) else redact_sensitive_values(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive_values(item) for item in value]
    return value


def _scalar(value: Any) -> bool:
    return value is None or isinstance(value, SCALAR_TYPES)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _domain_rows(domains: list[dict]) -> tuple[list[str], list[list[str]]]:
    rows = []
    for domain in domains:
        devices = [device for device in domain.get("devices") or [] if device]
        status = domain.get("status") or {}
        rows.append(
            [
                _cell(domain.get("name")),
                _cell(domain.get("id")),
                str(len(devices)),
                _cell(status.get("summary")),
                _cell(status.get("clocking")),
                _cell(status.get("connectivity")),
                _cell(status.get("latency")),
                _cell(status.get("subscriptions")),
            ]
        )
    return ["Name", "ID", "Devices", "Status", "Clocking", "Connectivity", "Latency", "Subscriptions"], rows


def _device_rows(devices: list[dict]) -> tuple[list[str], list[list[str]]]:
    rows = []
    for device in devices:
        identity = device.get("identity") or {}
        connection = device.get("connection") or {}
        status = device.get("status") or {}
        interfaces = [interface for interface in device.get("interfaces") or [] if interface]
        rows.append(
            [
                _cell(device.get("name")),
                _cell(device.get("id")),
                _cell(identity.get("productModelName")),
                _cell(identity.get("danteVersion")),
                _cell(device.get("enrolmentState")),
                _cell(connection.get("state")),
                _cell(status.get("summary")),
                ", ".join(
                    _cell(interface.get("address"))
                    for interface in interfaces
                    if interface.get("address") not in (None, "", UNSPECIFIED_ADDRESS)
                ),
                str(len(device.get("txChannels") or [])),
                str(len(device.get("rxChannels") or [])),
            ]
        )
    return ["Name", "ID", "Model", "Dante", "Enrolment", "Connection", "Status", "IP Address", "TX", "RX"], rows


def _generic_rows(items: list[dict]) -> tuple[list[str], list[list[str]]]:
    columns = sorted({key for item in items for key, value in item.items() if _scalar(value)})
    return columns, [[_cell(item.get(column)) for column in columns] for item in items]


def _object_rows(value: dict) -> tuple[list[str], list[list[str]]]:
    rows = []
    for key in sorted(value):
        field_value = value[key]
        rows.append([key, _cell(field_value) if _scalar(field_value) else json.dumps(field_value, sort_keys=True)])
    return ["Field", "Value"], rows


def report_errors(response: dict) -> bool:
    found = False
    for issue in response.get("errors") or []:
        found = True
        message = issue.get("message") if isinstance(issue, dict) else str(issue)
        path = issue.get("path") if isinstance(issue, dict) else None
        location = f" at {'.'.join(str(part) for part in path)}" if path else ""
        typer.echo(f"Managed API error{location}: {message}", err=True)
    return found


def render_result(
    response: dict,
    field_name: str,
    type_name: str,
    title: str | None = None,
    *,
    require_ok: bool = False,
) -> None:
    if report_errors(response):
        fail("the Managed API request failed")
    data = response.get("data")
    if data is None or field_name not in data:
        fail("the Managed API returned no data")
    value = redact_sensitive_values(data[field_name])
    if require_ok and (not isinstance(value, dict) or value.get("ok") is not True):
        error = value.get("error") if isinstance(value, dict) else None
        detail = error.get("message") if isinstance(error, dict) else None
        fail(f"{field_name} did not confirm success" + (f": {detail}" if detail else ""))
    if isinstance(value, list):
        items = [item for item in value if isinstance(item, dict)]
        if type_name == "Domain":
            headers, rows = _domain_rows(items)
        elif type_name == "Device":
            headers, rows = _device_rows(items)
        else:
            headers, rows = _generic_rows(items)
        output_table(headers, rows, json_data=value, title=title, empty_message=f"No {field_name} returned.")
        return
    if isinstance(value, dict):
        headers, rows = _object_rows(value)
        output_table(headers, rows, json_data=value, title=title)
        return
    output_single(json.dumps(value))


__all__ = ["is_sensitive_name", "redact_sensitive_values", "render_result", "report_errors"]
