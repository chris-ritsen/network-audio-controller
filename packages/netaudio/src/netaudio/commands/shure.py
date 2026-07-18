from __future__ import annotations

import asyncio
import logging
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import typer

from netaudio.shure.discovery import get_shure_neighbor_entries

app = typer.Typer(help="Shure wireless device control.", no_args_is_help=True)
device_app = typer.Typer(help="Shure device commands.", no_args_is_help=True)
channel_app = typer.Typer(help="Shure channel commands.", no_args_is_help=True)
app.add_typer(device_app, name="device")
app.add_typer(channel_app, name="channel")

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


def _send_for_cli(host, port, protocol, *, command, expect_key, allow_no_response=False):
    try:
        return _send(
            host,
            port,
            protocol,
            command=command,
            expect_key=expect_key,
            require_response=True,
            allow_no_response=allow_no_response,
        )
    except (ShureCommandError, OSError) as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=1) from exception


def _readback_matches(requested: str, reported: str | None) -> bool:
    return reported is not None and requested.strip() == reported.strip()


def _validate_set_value(value: str, *, braced: bool) -> None:
    forbidden = {"<", ">", "\r", "\n", "\x00"}
    if any(character in value for character in forbidden):
        raise ValueError("values cannot contain angle brackets, newlines, or NUL bytes")
    if braced and ("{" in value or "}" in value):
        raise ValueError("this value cannot contain braces")


def _collect_device_queries(devices, fetch):
    if not devices:
        return [], []
    successes = {}
    failures = {}
    with ThreadPoolExecutor(max_workers=min(32, len(devices))) as pool:
        futures = {pool.submit(fetch, device): (index, device) for index, device in enumerate(devices)}
        for future in as_completed(futures):
            index, device = futures[future]
            try:
                successes[index] = future.result()
            except Exception as exception:
                failures[index] = (device, exception)
    return (
        [successes[index] for index in sorted(successes)],
        [failures[index] for index in sorted(failures)],
    )


def _report_device_query_failures(failures, operation):
    for device, exception in failures:
        label = device.name or device.model or device.ip
        target = f"{label} ({device.ip})" if label != device.ip else device.ip
        typer.echo(
            f"Error: {operation} failed for {target}: {exception}",
            err=True,
        )


def _query_device_info(device, discovered, *, require_channels=False):
    raw_response = _send(
        device.ip,
        device.port,
        device.protocol,
        bulk=True,
        require_response=True,
    )
    if not isinstance(raw_response, dict) or not raw_response:
        raise ShureCommandTimeout("device returned no usable bulk response")
    identity_key = "DEVICE_ID" if device.protocol == Protocol.rep else "DEVICE_NAME"
    if not raw_response.get(identity_key):
        raise ShureCommandTimeout(f"device returned no {identity_key} response")
    if require_channels and not any(
        raw_response.get(int(channel_number)) for channel_number in PROTOCOL_CONFIGS[device.protocol]["channels"]
    ):
        raise ShureCommandTimeout("device returned no channel responses")
    return _parse_device(
        device.ip,
        device.port,
        device.protocol,
        raw_response,
        discovered=discovered,
    )


@device_app.command("list")
def shure_device_list():
    from netaudio.cli import state
    from netaudio._common import output_single

    discovered = _discover_shure_devices()
    if not discovered:
        typer.echo("No Shure devices found on the network.")
        return

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        device_infos, failures = _collect_device_queries(
            discovered,
            lambda device: _query_device_info(device, discovered),
        )
        output_single([device_info.to_json() for device_info in device_infos])
        _report_device_query_failures(failures, "device query")
        if failures:
            raise typer.Exit(code=1)
        return

    def _fetch_firmware(device):
        key = "FW_VER"
        supported_keys = (
            PROTOCOL_CONFIGS[device.protocol]["device_rw_keys"] + PROTOCOL_CONFIGS[device.protocol]["device_ro_keys"]
        )
        if key not in supported_keys:
            return device, "N/A"
        firmware = _send(
            device.ip,
            device.port,
            device.protocol,
            command=f"GET {key}",
            expect_key=key,
            require_response=True,
        )
        if firmware is None:
            raise ShureCommandTimeout(f"device returned no matching response for {key}")
        return device, firmware

    dante_query_error = None
    try:
        dante_devices = asyncio.run(_get_dante_devices()) or {}
    except Exception as exception:
        dante_devices = {}
        dante_query_error = exception
    dante_by_mac = {}
    for server_name, dante_device in dante_devices.items():
        if dante_device.mac_address:
            dante_by_mac[_normalize_mac(dante_device.mac_address)] = (
                dante_device.name or server_name,
                str(dante_device.ipv4) if dante_device.ipv4 else "",
            )

    firmware_results, failures = _collect_device_queries(discovered, _fetch_firmware)
    firmwares = {id(device): firmware for device, firmware in firmware_results}
    failed_devices = {id(device) for device, _exception in failures}
    correlation_failures = []

    headers = f"{'Name':<13}{'Model':<8}{'IP Address':<17}{'MAC Address':<20}{'Firmware':<13}{'Dante Name':<16}{'Dante IP':<17}{'Dante MAC':<20}"
    typer.echo(headers)

    for discovered_device in discovered:
        firmware = "ERROR" if id(discovered_device) in failed_devices else firmwares[id(discovered_device)]
        try:
            dante_mac = _load_correlation(discovered_device.mac) or ""
        except Exception as exception:
            dante_mac = ""
            correlation_failures.append((discovered_device, exception))
        dante_name = ""
        dante_ip = ""
        if dante_mac:
            dante_name, dante_ip = dante_by_mac.get(dante_mac, ("", ""))

        typer.echo(
            f"{discovered_device.name:<13}"
            f"{discovered_device.model:<8}"
            f"{discovered_device.ip:<17}"
            f"{discovered_device.mac:<20}"
            f"{(firmware or ''):<13}"
            f"{dante_name:<16}"
            f"{dante_ip:<17}"
            f"{dante_mac:<20}"
        )

    _report_device_query_failures(failures, "firmware query")
    _report_device_query_failures(correlation_failures, "saved correlation lookup")
    if dante_query_error is not None:
        typer.echo(f"Error: Dante correlation query failed: {dante_query_error}", err=True)
    if failures or correlation_failures or dante_query_error is not None:
        raise typer.Exit(code=1)


@channel_app.command("list")
def shure_channel_list(
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Protocol type."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    from netaudio.cli import state
    from netaudio._common import output_single
    from netaudio.shure.device import ShureChannel, ShureP10TChannel

    if host:
        resolved_host, resolved_protocol, resolved_port = _resolve_target(host, device, port)
        devices_to_query = [
            ShureDevice(
                ip=resolved_host,
                mac="",
                protocol=resolved_protocol,
                model="",
                name="",
                port=resolved_port,
            )
        ]
    else:
        devices_to_query = _discover_shure_devices()
        if state.names:
            devices_to_query = [
                discovered_device
                for discovered_device in devices_to_query
                if any(discovered_device.name.lower() == requested_name.lower() for requested_name in state.names)
            ]
        if state.hosts:
            devices_to_query = [
                discovered_device for discovered_device in devices_to_query if discovered_device.ip in state.hosts
            ]

    if not devices_to_query:
        typer.echo("No Shure devices found.", err=True)
        raise typer.Exit(code=1)

    def _fetch(device):
        return device, _query_device_info(
            device,
            devices_to_query,
            require_channels=True,
        )

    query_results, failures = _collect_device_queries(devices_to_query, _fetch)

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        all_channels = []
        for _device, device_info in query_results:
            for channel_number, channel in sorted(device_info.channels.items()):
                channel_json = {
                    "device": device_info.name,
                    "channel": channel_number,
                }
                if isinstance(channel, ShureChannel):
                    channel_json.update(
                        {
                            "name": channel.name,
                            "active": channel.active,
                            "antenna": channel.antenna_status,
                        }
                    )
                    if channel.transmitter and channel.transmitter.connected:
                        tx = channel.transmitter
                        channel_json["tx_model"] = tx.model
                        channel_json["battery_pct"] = tx.battery_charge_percent
                        channel_json["battery_min"] = tx.battery_minutes
                        channel_json["mute"] = tx.mute_status
                elif isinstance(channel, ShureP10TChannel):
                    channel_json.update(
                        {
                            "name": channel.name,
                            "frequency": channel.frequency,
                            "rf_mute": channel.rf_mute,
                        }
                    )
                all_channels.append(channel_json)
        output_single(all_channels)
    else:
        for queried_device, device_info in query_results:
            if len(devices_to_query) > 1:
                typer.echo(f"\n{device_info.name} ({queried_device.ip}):")

            for channel_number, channel in sorted(device_info.channels.items()):
                if isinstance(channel, ShureChannel):
                    status = "ACTIVE" if channel.active else "--"
                    display_parts = [
                        f"ch{channel_number}",
                        f"{(channel.name or ''):<12}",
                        f"{status:<8}",
                    ]
                    if channel.active and channel.transmitter and channel.transmitter.connected:
                        tx = channel.transmitter
                        display_parts.append(f"TX:{tx.model}")
                        if tx.battery_charge_percent is not None:
                            display_parts.append(f"batt:{tx.battery_charge_percent}%")
                        elif tx.battery_hours is not None:
                            display_parts.append(f"batt:{tx.battery_hours:.1f}h")
                        if tx.mute_status:
                            display_parts.append(f"mute:{tx.mute_status}")
                        if channel.audio_level_rms is not None:
                            display_parts.append(f"rms:{channel.audio_level_rms}")
                        if channel.signal_quality is not None and channel.signal_quality < 255:
                            display_parts.append(f"qual:{channel.signal_quality}")
                    typer.echo("  ".join(display_parts))
                elif isinstance(channel, ShureP10TChannel):
                    display_parts = [
                        f"ch{channel_number}",
                        f"{(channel.name or ''):<12}",
                    ]
                    if channel.frequency:
                        display_parts.append(f"freq:{channel.frequency}")
                    if channel.rf_mute:
                        display_parts.append("RF_MUTED")
                    if channel.audio_in_level_l is not None:
                        display_parts.append(f"L:{channel.audio_in_level_l}")
                    if channel.audio_in_level_r is not None:
                        display_parts.append(f"R:{channel.audio_in_level_r}")
                    typer.echo("  ".join(display_parts))

    _report_device_query_failures(failures, "channel query")
    if failures:
        raise typer.Exit(code=1)


def _load_correlation(shure_mac):
    config_path = _config_path()
    if not config_path.exists():
        return None
    for line in config_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("["):
            continue
        if "=" in line and '"' in line:
            line_parts = line.split("=", 1)
            stored_shure_mac = line_parts[0].strip().strip('"')
            if _mac_match(stored_shure_mac, shure_mac):
                return line_parts[1].strip().strip('"')
    return None


def _parse_device(
    resolved_host,
    resolved_port,
    resolved_protocol,
    raw_response,
    discovered=None,
):
    from netaudio.shure.device import parse_ad4d, parse_p10t

    devices = discovered if discovered is not None else _discover_shure_devices()
    shure_device = next(
        (discovered_device for discovered_device in devices if discovered_device.ip == resolved_host),
        None,
    )
    mac_address = shure_device.mac if shure_device else ""

    if resolved_protocol == Protocol.rep:
        device_info = parse_ad4d(raw_response, resolved_host, mac_address)
    else:
        device_info = parse_p10t(raw_response, resolved_host, mac_address)

    if mac_address:
        device_info.dante_mac = _load_correlation(mac_address)

    return device_info


@device_app.command("show")
def shure_device_show(
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname (omit to auto-discover)."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type (auto-detected)."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    from netaudio.cli import state

    resolved_host, resolved_device, resolved_port = _resolve_target(host, device, port)
    target = ShureDevice(
        ip=resolved_host,
        mac="",
        protocol=resolved_device,
        model="",
        name="",
        port=resolved_port,
    )
    try:
        device_info = _query_device_info(target, [target])
    except Exception as exception:
        typer.echo(
            f"Error: device query failed for {resolved_host}: {exception}",
            err=True,
        )
        raise typer.Exit(code=1) from exception

    from netaudio._common import output_single

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        output_single(device_info.to_json())
    else:
        typer.echo("\n".join(_format_plain(device_info.to_json())))


@app.command("get")
def shure_get(
    key: str = typer.Argument(..., help="Key to query (e.g. CHAN_NAME, MODEL)."),
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname (omit to auto-discover)."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type (auto-detected)."),
    channel: Optional[str] = typer.Option(None, "--channel", "-c", help="Channel number."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    resolved_host, resolved_device, resolved_port = _resolve_target(host, device, port)
    configuration = PROTOCOL_CONFIGS[resolved_device]
    upper_key = key.upper()
    all_device_keys = configuration["device_rw_keys"] + configuration["device_ro_keys"]
    all_keys = all_device_keys + configuration["channel_rw_keys"] + configuration["channel_ro_keys"]

    if upper_key not in all_keys:
        typer.echo(f"Unknown key: {upper_key}", err=True)
        raise typer.Exit(code=1)

    is_device_key = upper_key in all_device_keys

    if is_device_key:
        full_key = upper_key
    else:
        if not channel:
            typer.echo(f"{upper_key} is a channel key; --channel is required.", err=True)
            raise typer.Exit(code=1)
        if channel not in configuration["channels"]:
            typer.echo(
                f"Channel must be one of {configuration['channels']}.",
                err=True,
            )
            raise typer.Exit(code=1)
        full_key = f"{channel} {upper_key}"

    result = _send_for_cli(
        resolved_host,
        resolved_port,
        resolved_device,
        command=f"GET {full_key}",
        expect_key=full_key,
    )
    typer.echo(result)


@app.command("set")
def shure_set(
    key: str = typer.Argument(..., help="Key to set (e.g. CHAN_NAME, AUDIO_GAIN)."),
    value: str = typer.Argument(..., help="Value to set."),
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname (omit to auto-discover)."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type (auto-detected)."),
    channel: Optional[str] = typer.Option(None, "--channel", "-c", help="Channel number."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    resolved_host, resolved_device, resolved_port = _resolve_target(host, device, port)
    configuration = PROTOCOL_CONFIGS[resolved_device]
    upper_key = key.upper()

    if upper_key in configuration["device_ro_keys"]:
        typer.echo(f"{upper_key} is read-only.", err=True)
        raise typer.Exit(code=1)

    if upper_key in configuration["channel_ro_keys"]:
        typer.echo(f"{upper_key} is read-only.", err=True)
        raise typer.Exit(code=1)

    if upper_key in configuration["device_rw_keys"]:
        full_key = upper_key
    else:
        if upper_key not in configuration["channel_rw_keys"]:
            typer.echo(f"Unknown key: {upper_key}", err=True)
            raise typer.Exit(code=1)

        if not channel:
            typer.echo(f"{upper_key} is a channel key; --channel is required.", err=True)
            raise typer.Exit(code=1)
        if channel not in configuration["channels"]:
            typer.echo(
                f"Channel must be one of {configuration['channels']}.",
                err=True,
            )
            raise typer.Exit(code=1)
        full_key = f"{channel} {upper_key}"

    braced = upper_key in configuration["brace_keys"]
    try:
        _validate_set_value(value, braced=braced)
    except ValueError as exception:
        typer.echo(f"Error: invalid value for {full_key}: {exception}", err=True)
        raise typer.Exit(code=1) from exception

    formatted_value = f"{{{value}}}" if braced else value
    _send_for_cli(
        resolved_host,
        resolved_port,
        resolved_device,
        command=f"SET {full_key} {formatted_value}",
        expect_key=full_key,
        allow_no_response=True,
    )
    reported = _send_for_cli(
        resolved_host,
        resolved_port,
        resolved_device,
        command=f"GET {full_key}",
        expect_key=full_key,
    )
    if not isinstance(reported, str) or not _readback_matches(value, reported):
        typer.echo(
            f"Error: verification failed for {full_key}: requested {value!r}, device reports {reported!r}",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(f"Set {full_key} to {reported!r} (verified)")


@app.command("keys")
def shure_keys(
    device: Protocol = typer.Option(..., "--device", "-d", help="Protocol type."),
):
    configuration = PROTOCOL_CONFIGS[device]

    if configuration["device_rw_keys"]:
        typer.echo("Device-level keys (read/write):")
        for key in configuration["device_rw_keys"]:
            typer.echo(f"  {key}")

    if configuration["device_ro_keys"]:
        typer.echo("Device-level keys (read-only):")
        for key in configuration["device_ro_keys"]:
            typer.echo(f"  {key}")

    typer.echo(f"\nChannel-level keys (read/write, channels {', '.join(configuration['channels'])}):")
    for key in configuration["channel_rw_keys"]:
        typer.echo(f"  {key}")

    if configuration["channel_ro_keys"]:
        typer.echo("\nChannel-level keys (read-only):")
        for key in configuration["channel_ro_keys"]:
            typer.echo(f"  {key}")


NOISE_FLOOR_SHURE = 200
NOISE_FLOOR_DANTE = 240


def _normalize_mac(mac):
    normalized_address = re.sub(r"[:\-.]", "", mac).lower()[:12]
    return ":".join(normalized_address[index : index + 2] for index in range(0, len(normalized_address), 2))


def _mac_match(first_address, second_address):
    return _normalize_mac(first_address) == _normalize_mac(second_address)


def _config_path():
    from netaudio.common.config_loader import default_config_path

    return default_config_path()


def _save_correlation(shure_mac, dante_mac):
    config_path = _config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config_text = config_path.read_text() if config_path.exists() else ""
    section_header = "[shure.correlations]"

    output = []
    skip_section = False
    in_section = False
    replaced = False

    for line in config_text.splitlines(keepends=True):
        stripped = line.strip()

        if stripped.startswith("[shure.correlations."):
            header_mac_address = stripped.split('"')[1] if '"' in stripped else ""
            if _mac_match(header_mac_address, shure_mac):
                skip_section = True
                continue

        if skip_section:
            if stripped.startswith("["):
                skip_section = False
            else:
                continue

        if stripped == section_header:
            in_section = True
            output.append(line)
            continue

        if in_section:
            if stripped.startswith("["):
                in_section = False
            elif '"' in stripped:
                stored_mac_address = stripped.split('"')[1]
                if _mac_match(stored_mac_address, shure_mac):
                    output.append(f'"{shure_mac}" = "{dante_mac}"\n')
                    replaced = True
                    continue

        output.append(line)

    if not replaced:
        if not any(line.strip() == section_header for line in output):
            if output and not output[-1].endswith("\n"):
                output.append("\n")
            output.append(f"\n{section_header}\n")
        output.append(f'"{shure_mac}" = "{dante_mac}"\n')

    config_path.write_text("".join(output))
    return config_path


def _shure_query(host, port, commands):
    with socket.create_connection((host, port), timeout=2) as connection_socket:
        connection_socket.settimeout(0.3)
        connection_socket.sendall("".join(f"< {requested_command} >\r\n" for requested_command in commands).encode())

        response = b""
        expected_response_count = len(commands)
        while response.count(b"REP") < expected_response_count:
            try:
                chunk = connection_socket.recv(4096)
                if not chunk:
                    break
                response += chunk
            except socket.timeout:
                break

    parsed_responses = {}
    for match in re.finditer(
        r"REP\s+(\d)\s+(\S+)\s+\{?([^}>]*?)\}?\s*>",
        response.decode("utf-8", errors="replace"),
    ):
        channel = int(match.group(1))
        parsed_responses.setdefault(channel, {})[match.group(2)] = match.group(3).strip()
    return parsed_responses


def _sample_shure_levels(host, port, channels):
    responses = _shure_query(
        host,
        port,
        [f"GET {channel} AUDIO_LEVEL_RMS" for channel in channels],
    )
    return {
        channel: int(values["AUDIO_LEVEL_RMS"]) for channel, values in responses.items() if "AUDIO_LEVEL_RMS" in values
    }


async def _sample_all_dante_levels(dante_devices):
    from netaudio.daemon.client import meter_snapshot_from_daemon

    result = {}
    server_names = list(dante_devices)
    snapshots = await asyncio.gather(*(meter_snapshot_from_daemon(server_name) for server_name in server_names))
    for server_name, snapshot in zip(server_names, snapshots):
        if snapshot and snapshot.get("tx"):
            result[server_name] = {
                int(channel): meter_info["level"] if isinstance(meter_info, dict) else meter_info
                for channel, meter_info in snapshot["tx"].items()
            }
    return result


def _get_active_shure_channels(host, port, channels):
    responses = _shure_query(
        host,
        port,
        [command for channel in channels for command in [f"GET {channel} TX_MODEL", f"GET {channel} ANTENNA_STATUS"]],
    )

    return [
        int(channel)
        for channel in channels
        if responses.get(int(channel), {}).get("TX_MODEL", "UNKNOWN") != "UNKNOWN"
        and responses.get(int(channel), {}).get("ANTENNA_STATUS", "XX") != "XX"
    ]


async def _sample_both(shure_host, shure_port, active_channels, dante_devices):
    event_loop = asyncio.get_running_loop()
    shure_future = event_loop.run_in_executor(
        None,
        _sample_shure_levels,
        shure_host,
        shure_port,
        [str(channel) for channel in active_channels],
    )
    dante_future = _sample_all_dante_levels(dante_devices)
    shure_levels, all_dante_levels = await asyncio.gather(shure_future, dante_future)
    return shure_levels, all_dante_levels


def _match_by_level(shure_levels, all_dante_levels):
    active_shure_levels = {channel: level for channel, level in shure_levels.items() if level < NOISE_FLOOR_SHURE}
    if not active_shure_levels:
        return None, None

    for server_name, dante_levels in all_dante_levels.items():
        active_dante_levels = {channel: level for channel, level in dante_levels.items() if level < NOISE_FLOOR_DANTE}
        if not active_dante_levels:
            continue

        ranked_shure_channels = sorted(
            active_shure_levels,
            key=lambda channel: active_shure_levels[channel],
        )
        ranked_dante_channels = sorted(
            active_dante_levels,
            key=lambda channel: active_dante_levels[channel],
        )

        channel_mapping = dict(zip(ranked_shure_channels, ranked_dante_channels))
        return server_name, channel_mapping

    return None, None


def _format_sample(shure_levels, all_dante_levels, active_channels):
    shure_parts = [f"ch{channel}={shure_levels.get(channel, '?')}" for channel in active_channels]
    dante_parts = []
    for server_name, levels in all_dante_levels.items():
        active_tx_levels = [
            f"tx{channel}={level}" for channel, level in sorted(levels.items()) if level < NOISE_FLOOR_DANTE
        ]
        if active_tx_levels:
            dante_parts.append(f"{server_name}: {' '.join(active_tx_levels)}")
    return f"  shure: {' '.join(shure_parts)}  dante: {', '.join(dante_parts) or '(silent)'}"


def _looks_like_mac(value):
    normalized_address = re.sub(r"[:\-.]", "", value)
    return len(normalized_address) in (12, 16) and all(
        character in "0123456789abcdefABCDEF" for character in normalized_address
    )


async def _get_dante_devices():
    from netaudio.daemon.client import get_devices_from_daemon

    return await get_devices_from_daemon()


async def _wait_for_next_sample(interval: float) -> None:
    event_loop = asyncio.get_running_loop()
    sample_ready = event_loop.create_future()

    def mark_sample_ready() -> None:
        if not sample_ready.done():
            sample_ready.set_result(None)

    timer = event_loop.call_later(interval, mark_sample_ready)
    try:
        await sample_ready
    finally:
        timer.cancel()


def _resolve_as_shure(value):
    for device in _discover_shure_devices():
        if (
            device.ip == value
            or device.name.lower() == value.lower()
            or (_looks_like_mac(value) and _mac_match(value, device.mac))
        ):
            return _normalize_mac(device.mac)
    if _looks_like_mac(value) and _normalize_mac(value).startswith("00:0e:dd"):
        return _normalize_mac(value)
    return None


def _resolve_as_dante(value):
    dante_devices = asyncio.run(_get_dante_devices()) or {}
    for server_name, device in dante_devices.items():
        if not device.mac_address:
            continue
        if (
            value.lower() == (device.name or "").lower()
            or value == str(device.ipv4)
            or server_name.lower().startswith(value.lower())
            or (_looks_like_mac(value) and _mac_match(value, device.mac_address))
        ):
            return _normalize_mac(device.mac_address)
    if _looks_like_mac(value):
        return _normalize_mac(value)
    return None


@app.command("associate")
def shure_associate(
    device_a: str = typer.Argument(..., help="Device identifier (MAC, IP, or name)."),
    device_b: Optional[str] = typer.Argument(None, help="Second device (omit to use -n/-h for Shure side)."),
):
    if device_b:
        from netaudio.cli import state

        if state.names or state.hosts:
            typer.echo("Use either two positional args or -n/-h with one arg, not both.", err=True)
            raise typer.Exit(code=1)

        first_shure_mac = _resolve_as_shure(device_a)
        second_shure_mac = _resolve_as_shure(device_b)
        first_dante_mac = _resolve_as_dante(device_a)
        second_dante_mac = _resolve_as_dante(device_b)

        if first_shure_mac and second_dante_mac:
            _save_correlation(first_shure_mac, second_dante_mac)
            return
        if second_shure_mac and first_dante_mac:
            _save_correlation(second_shure_mac, first_dante_mac)
            return

        typer.echo("Could not determine which is Shure and which is Dante.", err=True)
        raise typer.Exit(code=1)

    shure_host, _, _ = _resolve_target(None, None, None)
    shure_device_info = next(
        (discovered_device for discovered_device in _discover_shure_devices() if discovered_device.ip == shure_host),
        None,
    )
    if not shure_device_info:
        typer.echo("No Shure device matched. Use -n/-h or pass two arguments.", err=True)
        raise typer.Exit(code=1)

    dante_mac = _resolve_as_dante(device_a)
    if not dante_mac:
        typer.echo(f"Could not resolve Dante device: {device_a}", err=True)
        raise typer.Exit(code=1)

    _save_correlation(_normalize_mac(shure_device_info.mac), dante_mac)


@app.command("correlate")
def shure_correlate(
    host: Optional[str] = typer.Argument(None, help="Shure device IP or hostname."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="Shure TCP control port."),
    dante_name: Optional[str] = typer.Option(None, "--dante", help="Dante device name filter."),
    timeout: float = typer.Option(30.0, "--timeout", "-t", help="Max seconds to wait for correlation."),
    consecutive: int = typer.Option(5, "--consecutive", help="Consecutive matching samples required."),
    save: bool = typer.Option(True, "--save/--no-save", help="Save correlation to config."),
):
    asyncio.run(_correlate_async(host, device, port, dante_name, timeout, consecutive, save))


async def _correlate_async(host, device, port, dante_name, timeout, consecutive, save):
    from netaudio.daemon.client import (
        get_devices_from_daemon,
        meter_start_on_daemon,
        meter_stop_on_daemon,
    )

    shure_host, shure_protocol, shure_port = _resolve_target(host, device, port)
    if shure_protocol != Protocol.rep:
        typer.echo(
            "Correlation requires an AD4D or AD4Q receiver.",
            err=True,
        )
        raise typer.Exit(code=1)
    active_channels = _get_active_shure_channels(
        shure_host,
        shure_port,
        PROTOCOL_CONFIGS[shure_protocol]["channels"],
    )

    if not active_channels:
        typer.echo("No active transmitters found.", err=True)
        raise typer.Exit(code=1)

    all_dante_devices = await get_devices_from_daemon()
    if not all_dante_devices:
        typer.echo("Daemon not running. Start the daemon first: netaudio server start", err=True)
        raise typer.Exit(code=1)

    if dante_name:
        dante_devices = {
            server_name: dante_device
            for server_name, dante_device in all_dante_devices.items()
            if dante_name.lower() in (dante_device.name or server_name).lower()
        }
        if not dante_devices:
            typer.echo(f"No Dante device matching '{dante_name}'.", err=True)
            raise typer.Exit(code=1)
    else:
        dante_devices = all_dante_devices

    client_identifier = "shure_correlate"
    started_server_names = []
    try:
        for server_name in dante_devices:
            await meter_start_on_daemon(server_name, client_identifier)
            started_server_names.append(server_name)

        typer.echo(
            f"Active Shure channels: {', '.join(str(channel) for channel in active_channels)}",
            err=True,
        )
        typer.echo(f"Dante devices: {len(dante_devices)}", err=True)
        typer.echo("Sampling... enable tone generator on desired channels.", err=True)

        previous_match = None
        consecutive_matches = 0
        start_time = time.monotonic()

        while time.monotonic() - start_time < timeout:
            shure_levels, all_dante_levels = await _sample_both(
                shure_host,
                shure_port,
                active_channels,
                dante_devices,
            )
            matched_server_name, channel_mapping = _match_by_level(
                shure_levels,
                all_dante_levels,
            )

            if matched_server_name and channel_mapping and len(channel_mapping) == len(active_channels):
                current_match = (matched_server_name, channel_mapping)
            else:
                current_match = None

            if current_match and current_match == previous_match:
                consecutive_matches += 1
            else:
                consecutive_matches = 1 if current_match else 0

            previous_match = current_match

            if consecutive_matches >= consecutive and current_match:
                confirmed_server_name, _channel_mapping = current_match
                elapsed = time.monotonic() - start_time
                typer.echo(
                    f"Correlated to {confirmed_server_name} in {elapsed:.1f}s",
                    err=True,
                )

                if save:
                    shure_device_info = next(
                        (
                            discovered_device
                            for discovered_device in _discover_shure_devices()
                            if discovered_device.ip == shure_host
                        ),
                        None,
                    )
                    dante_device = dante_devices.get(confirmed_server_name)
                    if shure_device_info and dante_device and dante_device.mac_address:
                        config_path = _save_correlation(
                            shure_device_info.mac,
                            _normalize_mac(dante_device.mac_address),
                        )
                        typer.echo(f"Saved to {config_path}", err=True)
                return

            typer.echo(
                _format_sample(
                    shure_levels,
                    all_dante_levels,
                    active_channels,
                ),
                err=True,
            )
            await _wait_for_next_sample(0.1)

        typer.echo("Timed out.", err=True)
        raise typer.Exit(code=1)
    finally:
        await asyncio.gather(
            *(meter_stop_on_daemon(server_name, client_identifier) for server_name in started_server_names)
        )
