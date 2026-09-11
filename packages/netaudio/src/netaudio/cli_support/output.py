import csv
import io
import json as json_module
import re
import xml.etree.ElementTree as ET
from typing import Any, Optional

import typer

from netaudio import DanteDevice
from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import _get_state, _iconize_headers
from netaudio.presets.serialization import format_devices_xml


_XML_ELEMENT_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")


def _format_text(headers: list[str], rows: list[list[str]]) -> str:
    all_rows = [headers] + [[str(value) for value in row] for row in rows]
    widths = [max(len(row[i]) for row in all_rows) for i in range(len(headers))]
    numeric = [all(row[i].isdigit() for row in all_rows[1:] if row[i]) for i in range(len(headers))]
    lines = []
    for row in all_rows:
        parts = [
            row[i].rjust(widths[i]) if numeric[i] and row is not all_rows[0] else row[i].ljust(widths[i])
            for i in range(len(row))
        ]
        lines.append("  ".join(parts).rstrip())
    return "\n".join(lines)


def _format_csv(headers: list[str], rows: list[list[str]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    return buffer.getvalue().rstrip("\n")


def _format_json(data: Any) -> str:
    return json_module.dumps(data, indent=2, default=str)


def _format_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list, tuple)):
        return json_module.dumps(value, default=str)
    return str(value)


def _format_single_csv(data: Any) -> str:
    if isinstance(data, dict):
        return _format_csv(
            ["Field", "Value"],
            [[str(key), _format_csv_value(value)] for key, value in data.items()],
        )
    if isinstance(data, (list, tuple)):
        return _format_csv(["Value"], [[_format_csv_value(value)] for value in data])
    return _format_csv(["Value"], [[_format_csv_value(data)]])


def _xml_element(parent: ET.Element, name: str, value: Any) -> None:
    if _XML_ELEMENT_NAME_PATTERN.fullmatch(name):
        element = ET.SubElement(parent, name)
    else:
        element = ET.SubElement(parent, "item", key=name)

    if isinstance(value, dict):
        for key, nested_value in value.items():
            _xml_element(element, str(key), nested_value)
    elif isinstance(value, (list, tuple)):
        for nested_value in value:
            _xml_element(element, "item", nested_value)
    elif value is None:
        element.set("nil", "true")
    elif isinstance(value, bool):
        element.text = str(value).lower()
    else:
        element.text = str(value)


def _format_xml(data: Any) -> str:
    root = ET.Element("netaudio")
    if isinstance(data, dict):
        for key, value in data.items():
            _xml_element(root, str(key), value)
    elif isinstance(data, (list, tuple)):
        for value in data:
            _xml_element(root, "item", value)
    elif data is None:
        root.set("nil", "true")
    elif isinstance(data, bool):
        root.text = str(data).lower()
    else:
        root.text = str(data)
    ET.indent(root, space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode")


def _format_yaml(data: Any) -> str:
    try:
        import yaml
    except ImportError:
        typer.echo("Error: pyyaml not installed. Run: uv add pyyaml", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return yaml.dump(data, default_flow_style=False, sort_keys=False).rstrip("\n")


def _format_table(headers: list[str], rows: list[list[str]], title: Optional[str] = None) -> str:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    state = _get_state()
    table = Table(title=title)

    for header in headers:
        table.add_column(header)

    for row in rows:
        table.add_row(*[Text.from_ansi(str(value)) for value in row])

    console = Console(no_color=state.no_color)
    with console.capture() as capture:
        console.print(table)
    return capture.get().rstrip("\n")


def drop_empty_columns(headers: list[str], rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    kept_indexes = [
        index for index in range(len(headers)) if any(str(row[index]) != "" for row in rows if index < len(row))
    ]
    kept_headers = [headers[index] for index in kept_indexes]
    kept_rows = [[row[index] for index in kept_indexes if index < len(row)] for row in rows]
    return kept_headers, kept_rows


def _structured_output_selected() -> bool:
    from netaudio.cli import OutputFormat

    return _get_state().output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml)


def structured_output_selected() -> bool:
    return _structured_output_selected()


def _render_table_text(headers: list[str], rows: list[list[str]], title: Optional[str]) -> None:
    from netaudio.cli import OutputFormat

    output_format = _get_state().output_format
    display_headers = _iconize_headers(headers)
    if output_format == OutputFormat.pretty:
        typer.echo(_format_table(display_headers, rows, title=title))
    elif output_format == OutputFormat.csv:
        typer.echo(_format_csv(headers, rows))
    else:
        if title:
            typer.echo(title)
        typer.echo(_format_text(display_headers, rows))


def _render_structured(json_data: Any, devices: Optional[dict[str, DanteDevice]] = None) -> None:
    from netaudio.cli import OutputFormat

    output_format = _get_state().output_format
    if output_format == OutputFormat.yaml:
        typer.echo(_format_yaml(json_data))
    elif output_format == OutputFormat.xml:
        typer.echo(format_devices_xml(devices) if devices else _format_xml(json_data))
    else:
        typer.echo(_format_json(json_data))


def output_table(
    headers: list[str],
    rows: list[list[str]],
    json_data: Any = None,
    title: Optional[str] = None,
    devices: Optional[dict[str, DanteDevice]] = None,
    omit_empty_columns: bool = False,
    empty_message: Optional[str] = None,
) -> None:
    if json_data is None:
        json_data = [dict(zip(headers, row)) for row in rows]

    if _structured_output_selected():
        _render_structured(json_data, devices)
        return

    if not rows and empty_message is not None:
        typer.echo(empty_message)
        return

    if omit_empty_columns and rows:
        headers, rows = drop_empty_columns(headers, rows)

    _render_table_text(headers, rows, title)


def output_sections(
    sections: list[tuple[str, list[str], list[list[str]]]],
    json_data: Any,
    devices: Optional[dict[str, DanteDevice]] = None,
) -> None:
    if _structured_output_selected():
        _render_structured(json_data, devices)
        return

    for title, headers, rows in sections:
        _render_table_text(headers, rows, title)


def output_single(data: Any, device: Optional[DanteDevice] = None) -> None:
    from netaudio.cli import OutputFormat

    if _get_state().output_format == OutputFormat.csv:
        typer.echo(_format_single_csv(data))
        return
    if _structured_output_selected():
        devices = {device.server_name or "device": device} if device else None
        _render_structured(data, devices)
        return
    typer.echo(data)


def output_value(label: str, key: str, value: Any, formatted_value: Any = None) -> None:
    from netaudio.cli import OutputFormat

    if _get_state().output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.csv, OutputFormat.yaml):
        output_single({key: value})
        return
    display_value = value if formatted_value is None else formatted_value
    typer.echo(f"{label}: {display_value}")
