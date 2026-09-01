import csv
import io
import json as json_module
import xml.etree.ElementTree as ET
from typing import Any, Optional

import typer

from netaudio import DanteDevice
from netaudio._common_cli import _get_state, _iconize_headers
from netaudio._exit_codes import ExitCode
from netaudio.dante.latency import milliseconds_to_microseconds


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


def _hex_encode(text: str, pad_to: int = 16) -> str:
    import binascii

    encoded = binascii.hexlify(text.encode()).decode().upper()
    return encoded.ljust(pad_to, "0")


def _device_to_preset_xml(device: DanteDevice) -> ET.Element:
    element = ET.Element("device")

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

    if device.interfaces:
        for index, iface in enumerate(device.interfaces):
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


def output_table(
    headers: list[str],
    rows: list[list[str]],
    json_data: Any = None,
    title: Optional[str] = None,
    devices: Optional[dict[str, DanteDevice]] = None,
    omit_empty_columns: bool = False,
) -> None:
    from netaudio.cli import OutputFormat

    state = _get_state()
    output_format = state.output_format

    if json_data is None:
        json_data = [dict(zip(headers, row)) for row in rows]

    if omit_empty_columns and rows:
        headers, rows = drop_empty_columns(headers, rows)

    display_headers = _iconize_headers(headers)

    if output_format == OutputFormat.plain:
        if title:
            typer.echo(title)
        typer.echo(_format_text(display_headers, rows))
    elif output_format == OutputFormat.table:
        if title:
            typer.echo(title)
        typer.echo(_format_text(display_headers, rows))
    elif output_format == OutputFormat.pretty:
        typer.echo(_format_table(display_headers, rows, title=title))
    elif output_format == OutputFormat.json:
        typer.echo(_format_json(json_data))
    elif output_format == OutputFormat.xml:
        if devices:
            typer.echo(format_devices_xml(devices))
        else:
            typer.echo(_format_json(json_data))
    elif output_format == OutputFormat.csv:
        typer.echo(_format_csv(headers, rows))
    elif output_format == OutputFormat.yaml:
        typer.echo(_format_yaml(json_data))


def output_single(data: Any, device: Optional[DanteDevice] = None) -> None:
    from netaudio.cli import OutputFormat

    state = _get_state()
    output_format = state.output_format

    if output_format == OutputFormat.json:
        typer.echo(_format_json(data))
    elif output_format == OutputFormat.xml:
        if device:
            devices = {device.server_name or "device": device}
            typer.echo(format_devices_xml(devices))
        else:
            typer.echo(_format_json(data))
    elif output_format == OutputFormat.yaml:
        typer.echo(_format_yaml(data))
    else:
        typer.echo(data)
