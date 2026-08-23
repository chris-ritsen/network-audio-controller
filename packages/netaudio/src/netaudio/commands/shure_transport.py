from __future__ import annotations

import logging
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum

import typer

from netaudio.shure.discovery import get_shure_neighbor_entries


SHURE_CONTROL_PORT = 2202


logger = logging.getLogger("netaudio")


class Protocol(str, Enum):
    rep = "rep"
    report = "report"


class ShureCommandError(RuntimeError):
    __slots__ = ()


class ShureCommandTimeout(ShureCommandError):
    __slots__ = ()


class ShureCommandRejected(ShureCommandError):
    __slots__ = ()


MODEL_PREFIXES = [
    ("AD4D", Protocol.rep),
    ("AD4Q", Protocol.rep),
    ("P10T", Protocol.report),
]


def _protocol_for_model(model_name):
    upper_model_name = model_name.upper()
    for prefix, protocol in MODEL_PREFIXES:
        if upper_model_name.startswith(prefix):
            return protocol
    return None


@dataclass
class ShureDevice:
    ip: str
    mac: str
    protocol: Protocol
    model: str
    name: str
    port: int = SHURE_CONTROL_PORT


AD4D_DEVICE_RW_KEYS = [
    "DEVICE_ID",
]

AD4D_DEVICE_RO_KEYS = [
    "MODEL",
    "FW_VER",
    "RF_BAND",
    "TRANSMISSION_MODE",
    "QUADVERSITY_MODE",
    "ENCRYPTION_MODE",
]

AD4D_CHANNEL_RW_KEYS = [
    "CHAN_NAME",
    "AUDIO_GAIN",
    "AUDIO_MUTE",
    "FREQUENCY",
    "GROUP_CHANNEL",
    "METER_RATE",
    "FLASH",
]

AD4D_CHANNEL_RO_KEYS = [
    "FD_MODE",
    "ENCRYPTION_STATUS",
    "INTERFERENCE_STATUS",
    "UNREGISTERED_TX_STATUS",
    "AUDIO_LEVEL_PEAK",
    "AUDIO_LEVEL_RMS",
    "CHAN_QUALITY",
    "RSSI",
    "ANTENNA_STATUS",
    "TX_BATT_MINS",
    "TX_BATT_TYPE",
    "TX_BATT_CHARGE_PERCENT",
    "TX_BATT_BARS",
    "TX_BATT_CYCLE_COUNT",
    "TX_BATT_TEMP_F",
    "TX_MODEL",
    "TX_DEVICE_ID",
    "TX_POWER_LEVEL",
    "TX_MUTE_MODE_STATUS",
]

P10T_DEVICE_RW_KEYS = ["DEVICE_NAME"]

P10T_DEVICE_RO_KEYS = []

P10T_CHANNEL_RW_KEYS = [
    "CHAN_NAME",
    "AUDIO_IN_LVL",
    "GROUP_CHAN",
    "FREQUENCY",
    "RF_TX_LVL",
    "RF_MUTE",
    "AUDIO_TX_MODE",
    "AUDIO_IN_LINE_LVL",
    "METER_RATE",
]

AD4D_BRACE_KEYS = {
    "CHAN_NAME",
    "DEVICE_ID",
    "GROUP_CHANNEL",
    "GROUP_CHANNEL2",
    "TX_DEVICE_ID",
    "SLOT_TX_DEVICE_ID",
}

PROTOCOL_CONFIGS = {
    Protocol.rep: {
        "channels": ("1", "2", "3", "4"),
        "device_rw_keys": AD4D_DEVICE_RW_KEYS,
        "device_ro_keys": AD4D_DEVICE_RO_KEYS,
        "channel_rw_keys": AD4D_CHANNEL_RW_KEYS,
        "channel_ro_keys": AD4D_CHANNEL_RO_KEYS,
        "brace_keys": AD4D_BRACE_KEYS,
        "response_prefix": "REP",
    },
    Protocol.report: {
        "channels": ("1", "2"),
        "device_rw_keys": P10T_DEVICE_RW_KEYS,
        "device_ro_keys": P10T_DEVICE_RO_KEYS,
        "channel_rw_keys": P10T_CHANNEL_RW_KEYS,
        "channel_ro_keys": [],
        "brace_keys": set(),
        "response_prefix": "REPORT",
    },
}


def _parse_ad4d_line(line):
    parts = line.strip("<> ").split(maxsplit=3)
    if len(parts) < 2 or parts[0] != "REP":
        return None
    if parts[1].isdigit():
        if len(parts) < 3:
            return None
        channel = int(parts[1])
        key = parts[2]
        value = parts[3] if len(parts) > 3 else ""
    else:
        channel = None
        key = parts[1]
        value = parts[2] if len(parts) > 2 else ""
        if len(parts) > 3:
            value = f"{value} {parts[3]}"
    value = value.strip()
    if value.startswith("{") and value.endswith("}"):
        value = value[1:-1]
    return channel, key.strip(), value.strip()


def _parse_p10t_line(line):
    parts = line.strip("<> ").split(maxsplit=3)
    if len(parts) < 2 or parts[0] != "REPORT":
        return None
    if parts[1] in ("1", "2"):
        if len(parts) < 3:
            return None
        return int(parts[1]), parts[2], parts[3] if len(parts) > 3 else ""
    value = parts[2] if len(parts) > 2 else ""
    if len(parts) > 3:
        value = f"{value} {parts[3]}"
    return None, parts[1], value


def _probe_device(ip_address, port=SHURE_CONTROL_PORT):
    try:
        with socket.create_connection((ip_address, port), timeout=0.5) as connection_socket:
            connection_socket.sendall(b"< GET MODEL >\r\n< GET DEVICE_ID >\r\n< GET DEVICE_NAME >\r\n")
            connection_socket.settimeout(0.1)

            response_chunks = []
            deadline = time.monotonic() + 0.15
            while time.monotonic() < deadline:
                try:
                    chunk = connection_socket.recv(4096)
                    if not chunk:
                        break
                    response_chunks.append(chunk)
                except socket.timeout:
                    break

            raw_response = b"".join(response_chunks).decode(
                "utf-8",
                errors="replace",
            )

            model = None
            name = None

            model_match = re.search(r"REP MODEL\s+\{?([^}>]+)", raw_response)
            if model_match:
                model = model_match.group(1).strip()

            name_match = re.search(r"REP DEVICE_ID\s+\{([^}]+)\}", raw_response)
            if name_match:
                name = name_match.group(1).strip()

            if not name:
                name_match = re.search(
                    r"REPORT DEVICE_NAME\s+([^>]+?)\s*>",
                    raw_response,
                )
                if name_match:
                    name = name_match.group(1).strip()

            if model:
                protocol = _protocol_for_model(model)
                if not protocol:
                    protocol = Protocol.rep if "< REP " in raw_response else Protocol.report
                return protocol, model, name or model
            if name:
                protocol = _protocol_for_model(name) or Protocol.report
                return protocol, name, name

    except OSError as exception:
        logger.debug(f"Shure probe failed for {ip_address}:{port}: {exception}")

    return None, None, None


def _discover_shure_devices():
    entries = get_shure_neighbor_entries()
    if not entries:
        return []

    devices = []
    with ThreadPoolExecutor(max_workers=min(32, len(entries))) as pool:
        futures = {
            pool.submit(_probe_device, ip_address): (ip_address, mac_address) for ip_address, mac_address in entries
        }
        for future in as_completed(futures):
            ip_address, mac_address = futures[future]
            try:
                protocol, model, name = future.result()
            except Exception as exception:
                typer.echo(
                    f"Warning: Shure probe failed for {ip_address}: {exception}",
                    err=True,
                )
                continue
            if protocol and model and name:
                devices.append(
                    ShureDevice(
                        ip=ip_address,
                        mac=mac_address,
                        protocol=protocol,
                        model=model,
                        name=name,
                    )
                )
    return sorted(devices, key=lambda discovered_device: discovered_device.ip)


def _resolve_target(host, protocol, port):
    if host:
        if not protocol:
            detected, _, _ = _probe_device(host, port or SHURE_CONTROL_PORT)
            if not detected:
                typer.echo("Could not detect protocol. Use --device or check connectivity.", err=True)
                raise typer.Exit(code=1)
            protocol = detected
        return host, protocol, port or SHURE_CONTROL_PORT

    from netaudio.cli import state

    devices = _discover_shure_devices()

    if state.names:
        devices = [
            discovered_device
            for discovered_device in devices
            if any(discovered_device.name.lower() == requested_name.lower() for requested_name in state.names)
        ]
    if state.hosts:
        devices = [discovered_device for discovered_device in devices if discovered_device.ip in state.hosts]

    if protocol:
        devices = [discovered_device for discovered_device in devices if discovered_device.protocol == protocol]

    if not devices:
        typer.echo("No Shure devices found. Use -n/-h to filter, or pass host directly.", err=True)
        raise typer.Exit(code=1)

    if len(devices) > 1:
        names = ", ".join(f"{discovered_device.name} ({discovered_device.ip})" for discovered_device in devices)
        typer.echo(f"Multiple Shure devices found: {names}. Use -n/-h or --device to narrow.", err=True)
        raise typer.Exit(code=1)

    selected_device = devices[0]
    return (
        selected_device.ip,
        selected_device.protocol,
        port or selected_device.port,
    )


def _send(
    host,
    port,
    protocol,
    command=None,
    expect_key=None,
    bulk=False,
    require_response=False,
    allow_no_response=False,
):
    configuration = PROTOCOL_CONFIGS[protocol]
    response_prefix = configuration["response_prefix"]
    parse_response = _parse_ad4d_line if protocol == Protocol.rep else _parse_p10t_line
    if not bulk and command is None:
        raise ValueError("a command is required for a non-bulk Shure request")

    try:
        with socket.create_connection((host, port), timeout=0.5) as connection_socket:
            if bulk:
                commands = [f"GET {key}" for key in configuration["device_rw_keys"] + configuration["device_ro_keys"]]
                for channel in configuration["channels"]:
                    commands.extend(
                        f"GET {channel} {key}"
                        for key in configuration["channel_rw_keys"] + configuration["channel_ro_keys"]
                    )
                payload = "".join(f"< {requested_command} >\r\n" for requested_command in commands)
                connection_socket.sendall(payload.encode())
            else:
                connection_socket.sendall(f"< {command} >\r\n".encode())

            connection_socket.settimeout(0.1)
            response_chunks = []
            deadline = time.monotonic() + 0.3
            while time.monotonic() < deadline:
                try:
                    chunk = connection_socket.recv(4096)
                    if not chunk:
                        break
                    response_chunks.append(chunk)
                except socket.timeout:
                    if require_response:
                        continue
                    break
    except (socket.timeout, TimeoutError) as exception:
        if require_response:
            raise ShureCommandTimeout(f"timed out communicating with {host}:{port}") from exception
        raise
    except OSError as exception:
        if require_response:
            raise ShureCommandError(f"could not communicate with {host}:{port}: {exception}") from exception
        raise

    raw_response = b"".join(response_chunks).decode("utf-8", errors="replace")
    response_frames = re.findall(r"<\s*([^<>]*?)\s*>", raw_response)
    response_bodies = [
        response_frame.strip()
        for response_frame in response_frames
        if response_frame.strip().split(maxsplit=1)[0:1] == [response_prefix]
    ]

    error_responses = [
        response_body
        for response_body in response_bodies
        if re.match(
            rf"^{re.escape(response_prefix)}\s+ERR(?:\s|$)",
            response_body,
        )
    ]
    if require_response and error_responses:
        detail = error_responses[0][len(response_prefix) :].strip()
        requested = "bulk query" if bulk else repr(command)
        raise ShureCommandRejected(f"device rejected {requested}: {detail}")

    response_lines = [
        f"< {response_body} >"
        for response_body in response_bodies
        if not re.match(
            rf"^{re.escape(response_prefix)}\s+ERR(?:\s|$)",
            response_body,
        )
    ]

    if bulk:
        merged_responses = {}
        for response_line in response_lines:
            parsed = parse_response(response_line)
            if not parsed:
                continue
            channel, key, value = parsed
            if channel is None:
                merged_responses[key] = value
            else:
                merged_responses.setdefault(channel, {})[key] = value
        if require_response and not merged_responses and not allow_no_response:
            raise ShureCommandTimeout("device returned no usable bulk response")
        return merged_responses

    for response_line in response_lines:
        parsed = parse_response(response_line)
        if not parsed:
            continue
        channel, key, value = parsed
        match_key = f"{channel} {key}" if channel is not None else key
        if expect_key is None or match_key == expect_key:
            return value

    if require_response and not allow_no_response:
        expected = f" for {expect_key}" if expect_key else ""
        raise ShureCommandTimeout(f"device returned no matching response{expected}")
    return None


def _format_plain(data, indent=0):
    lines = []
    padding = "  " * indent
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{padding}{key}:")
                lines.extend(_format_plain(value, indent + 1))
            else:
                lines.append(f"{padding}{key}: {value}")
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                lines.append(f"{padding}-")
                lines.extend(_format_plain(item, indent + 1))
            else:
                lines.append(f"{padding}- {item}")
    else:
        lines.append(f"{padding}{data}")
    return lines
