from __future__ import annotations

import datetime
from typing import Any

import typer


def structured_output_selected() -> bool:
    from netaudio.cli import OutputFormat, state

    return state.output_format in (OutputFormat.json, OutputFormat.xml, OutputFormat.yaml)


def emit_report(lines: list[str], data: Any) -> None:
    from netaudio.cli_support.output import output_single

    if structured_output_selected():
        output_single(data)
        return
    for line in lines:
        typer.echo(line)


def format_packet_timestamp(timestamp_ns: int, include_date: bool = False) -> str:
    timestamp = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
    pattern = "%Y-%m-%d %H:%M:%S.%f" if include_date else "%H:%M:%S.%f"
    return timestamp.strftime(pattern)[:-3]


def packet_row_data(row: dict) -> dict:
    from netaudio.capture.packets import _label_packet

    payload = row.get("payload") or b""
    if isinstance(payload, str):
        payload = bytes.fromhex(payload)
    timestamp_ns = int(row.get("timestamp_ns") or 0)
    return {
        "device_ip": row.get("device_ip"),
        "direction": row.get("direction") or "multicast",
        "dst_ip": row.get("dst_ip"),
        "dst_port": row.get("dst_port"),
        "id": int(row["id"]),
        "interface": row.get("interface"),
        "label": _label_packet(payload, include_code=True),
        "payload_hex": payload.hex(),
        "session_id": row.get("session_id"),
        "size": len(payload),
        "source_host": row.get("source_host"),
        "source_type": row.get("source_type"),
        "src_ip": row.get("src_ip"),
        "src_port": row.get("src_port"),
        "timestamp": format_packet_timestamp(timestamp_ns, include_date=True),
        "timestamp_ns": timestamp_ns,
    }
