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
from netaudio.dante.latency import milliseconds_to_microseconds


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


def _hex_encode(text: str, pad_to: int = 16) -> str:
    import binascii

    encoded = binascii.hexlify(text.encode()).decode().upper()
    return encoded.ljust(pad_to, "0")


def _append_preset_identity(element: ET.Element, device: DanteDevice) -> None:
    _sub_text(element, "name", device.name or "")
    _sub_text(element, "default_name", device.server_name.replace(".local.", "") if device.server_name else "")

    instance_id = ET.SubElement(element, "instance_id")
    mac = (device.mac_address or "").replace(":", "").upper()
    if mac:
        if len(mac) == 12:
            mac = mac[:6] + "FFFE" + mac[6:]
        _sub_text(instance_id, "device_id", mac)
    _sub_text(instance_id, "process_id", "0")

    if device.manufacturer:
        _sub_text(element, "manufacturer_id", _hex_encode(device.manufacturer))
        _sub_text(element, "manufacturer_name", device.manufacturer)

    dante_model = device.dante_model or device.model_id or ""
    model_id = device.model_id or ""

    if model_id:
        model_id_hex = _hex_encode(model_id)
        _sub_text(element, "model_id", model_id_hex)
        _sub_text(element, "model_name", dante_model or model_id)
        if device.product_version:
            _sub_text(element, "model_version", device.product_version)
        _sub_text(element, "device_type", model_id_hex)
        _sub_text(element, "device_type_string", model_id)

    _sub_text(element, "friendly_name", device.name or "")


def _append_preset_audio_settings(element: ET.Element, device: DanteDevice) -> None:
    if device.preferred_leader is not None:
        ET.SubElement(element, "preferred_master", value=str(device.preferred_leader).lower())

    if device.sample_rate:
        _sub_text(element, "samplerate", str(device.sample_rate))

    if device.encoding:
        _sub_text(element, "encoding", str(device.encoding))

    latency = device.configured_latency
    if latency is None and device.active_latency is None:
        latency = device.latency
    if latency:
        _sub_text(element, "unicast_latency", str(milliseconds_to_microseconds(latency)))


def _append_preset_interfaces(element: ET.Element, device: DanteDevice) -> None:
    for index, iface in enumerate(device.interfaces or []):
        iface_element = ET.SubElement(element, "interface", network=str(index))
        mode = iface.get("mode", "")
        if mode == "static":
            ip_element = ET.SubElement(iface_element, "ipv4_address", mode="static")
            _sub_text(ip_element, "ip_address", iface.get("ip_address", ""))
            _sub_text(ip_element, "subnet_mask", iface.get("netmask", ""))
            _sub_text(ip_element, "gateway", iface.get("gateway", ""))
            _sub_text(ip_element, "dns_server", iface.get("dns_server", ""))
        else:
            ET.SubElement(iface_element, "ipv4_address", mode="dynamic")


def _append_preset_channels(element: ET.Element, device: DanteDevice) -> None:
    for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number):
        tx_element = ET.SubElement(element, "txchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(tx_element, "label", channel.friendly_name or channel.name)

    for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number):
        rx_element = ET.SubElement(element, "rxchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(rx_element, "name", channel.friendly_name or channel.name)

        for subscription in device.subscriptions:
            if subscription.rx_channel_name == channel.name or subscription.rx_channel_name == channel.friendly_name:
                if subscription.tx_channel_name:
                    _sub_text(rx_element, "subscribed_channel", subscription.tx_channel_name)
                if subscription.tx_device_name:
                    _sub_text(rx_element, "subscribed_device", subscription.tx_device_name)
                break


def _device_to_preset_xml(device: DanteDevice) -> ET.Element:
    element = ET.Element("device")
    _append_preset_identity(element, device)
    _append_preset_audio_settings(element, device)
    _append_preset_interfaces(element, device)
    _append_preset_channels(element, device)
    return element


def _sub_text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    child = ET.SubElement(parent, tag)
    child.text = text
    return child


def format_devices_xml(devices: dict[str, DanteDevice], preset_name: str = "netaudio") -> str:
    root = ET.Element("preset", version="2.1.0")
    _sub_text(root, "name", preset_name)
    _sub_text(root, "description", "Dante Controller preset")

    for server_name, device in sorted(devices.items(), key=lambda item: item[1].name or item[0]):
        root.append(_device_to_preset_xml(device))

    ET.indent(root, space="    ")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(root, encoding="unicode")


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
