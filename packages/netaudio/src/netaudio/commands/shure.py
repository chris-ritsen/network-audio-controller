from __future__ import annotations

import asyncio
import re
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import typer

app = typer.Typer(help="Shure wireless device control.", no_args_is_help=True)
device_app = typer.Typer(help="Shure device commands.", no_args_is_help=True)
channel_app = typer.Typer(help="Shure channel commands.", no_args_is_help=True)
app.add_typer(device_app, name="device")
app.add_typer(channel_app, name="channel")

SHURE_CONTROL_PORT = 2202
SHURE_OUI = "00:0e:dd"


class Protocol(str, Enum):
    rep = "rep"
    report = "report"


MODEL_PREFIXES = [
    ("AD4D", Protocol.rep),
    ("AD4Q", Protocol.rep),
    ("P10T", Protocol.report),
]


def _protocol_for_model(model):
    upper = model.upper()
    for prefix, protocol in MODEL_PREFIXES:
        if upper.startswith(prefix):
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

_ad4d_rep_re = re.compile(r"^<\s*REP\s+(?:(\d)\s+)?([A-Z0-9_]+)\s+\{?([^>}]+?)\}?\s*>?$")


def _parse_ad4d_line(line):
    m = _ad4d_rep_re.match(line.strip())
    if not m:
        return None
    channel, key, value = m.groups()
    return int(channel) if channel else None, key.strip(), value.strip()


def _parse_p10t_line(line):
    parts = line.strip("<> ").split()
    if len(parts) < 3 or parts[0] != "REPORT":
        return None
    if parts[1] in ("1", "2"):
        return int(parts[1]), parts[2], parts[3] if len(parts) > 3 else None
    return None, parts[1], parts[2] if len(parts) > 2 else None


def _get_shure_arp_entries():
    result = subprocess.run(["ip", "neigh", "show"], capture_output=True, text=True)
    entries = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[3] == "lladdr":
            ip = parts[0]
            mac = parts[4].lower()
            if mac.startswith(SHURE_OUI):
                entries.append((ip, mac))
    return entries


def _probe_device(ip, port=SHURE_CONTROL_PORT):
    try:
        with socket.create_connection((ip, port), timeout=0.5) as sock:
            sock.sendall(b"< GET MODEL >\r\n< GET DEVICE_ID >\r\n< GET DEVICE_NAME >\r\n")
            sock.settimeout(0.1)

            chunks = []
            end = time.time() + 0.15
            while time.time() < end:
                try:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                except socket.timeout:
                    break

            raw = b"".join(chunks).decode("utf-8", errors="ignore")

            model = None
            name = None

            m = re.search(r"REP MODEL\s+\{?([^}>]+)", raw)
            if m:
                model = m.group(1).strip()

            m = re.search(r"REP DEVICE_ID\s+\{([^}]+)\}", raw)
            if m:
                name = m.group(1).strip()

            if not name:
                m = re.search(r"REPORT DEVICE_NAME\s+(\S+)", raw)
                if m:
                    name = m.group(1).strip()

            if model:
                protocol = _protocol_for_model(model)
                if not protocol:
                    protocol = Protocol.rep if "< REP " in raw else Protocol.report
                return protocol, model, name or model
            if name:
                protocol = _protocol_for_model(name) or Protocol.report
                return protocol, name, name

    except (OSError, socket.error):
        pass

    return None, None, None


def _discover_shure_devices():
    entries = _get_shure_arp_entries()
    if not entries:
        return []

    devices = []
    with ThreadPoolExecutor(max_workers=len(entries)) as pool:
        futures = {pool.submit(_probe_device, ip): (ip, mac) for ip, mac in entries}
        for future in as_completed(futures):
            ip, mac = futures[future]
            protocol, model, name = future.result()
            if protocol:
                devices.append(ShureDevice(ip=ip, mac=mac, protocol=protocol, model=model, name=name))
    return devices


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
        devices = [d for d in devices if any(d.name.lower() == n.lower() for n in state.names)]
    if state.hosts:
        devices = [d for d in devices if d.ip in state.hosts]

    if protocol:
        devices = [d for d in devices if d.protocol == protocol]

    if not devices:
        typer.echo("No Shure devices found. Use -n/-h to filter, or pass host directly.", err=True)
        raise typer.Exit(code=1)

    if len(devices) > 1:
        names = ", ".join(f"{d.name} ({d.ip})" for d in devices)
        typer.echo(f"Multiple Shure devices found: {names}. Use -n/-h or --device to narrow.", err=True)
        raise typer.Exit(code=1)

    d = devices[0]
    return d.ip, d.protocol, port or d.port


def _send(host, port, protocol, command=None, expect_key=None, bulk=False):
    cfg = PROTOCOL_CONFIGS[protocol]
    prefix = cfg["response_prefix"]
    parse = _parse_ad4d_line if protocol == Protocol.rep else _parse_p10t_line

    with socket.create_connection((host, port), timeout=0.5) as sock:
        if bulk:
            cmds = [f"GET {k}" for k in cfg["device_rw_keys"] + cfg["device_ro_keys"]]
            for ch in cfg["channels"]:
                cmds.extend(
                    f"GET {ch} {k}"
                    for k in cfg["channel_rw_keys"] + cfg["channel_ro_keys"]
                )
            payload = "".join(f"< {cmd} >\r\n" for cmd in cmds)
            sock.sendall(payload.encode())
        else:
            sock.sendall(f"< {command} >\r\n".encode())

        sock.settimeout(0.1)
        chunks = []
        end = time.time() + 0.3
        while time.time() < end:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            except socket.timeout:
                break

        raw = b"".join(chunks).decode("utf-8", errors="ignore")
        lines = [
            f"< {prefix} {part.strip()}"
            for part in raw.split(f"< {prefix} ")
            if part.strip() and "ERR" not in part
        ]

        if bulk:
            merged = {}
            for line in lines:
                parsed = parse(line)
                if not parsed:
                    continue
                ch, key, value = parsed
                if ch is None:
                    merged[key] = value
                else:
                    merged.setdefault(ch, {})[key] = value
            return merged

        for line in lines:
            parsed = parse(line)
            if not parsed:
                continue
            ch, key, value = parsed
            match_key = f"{ch} {key}" if ch is not None else key
            if expect_key is None or match_key == expect_key:
                if command and command.startswith("SET"):
                    return None
                return value

        return None


def _format_plain(data, indent=0):
    lines = []
    pad = "  " * indent
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{pad}{k}:")
                lines.extend(_format_plain(v, indent + 1))
            else:
                lines.append(f"{pad}{k}: {v}")
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                lines.append(f"{pad}-")
                lines.extend(_format_plain(item, indent + 1))
            else:
                lines.append(f"{pad}- {item}")
    else:
        lines.append(f"{pad}{data}")
    return lines


def _format_shure_plain(data):
    return _format_plain(data)


@device_app.command("list")
def shure_device_list():
    from netaudio.cli import state
    from netaudio._common import output_single

    discovered = _discover_shure_devices()
    if not discovered:
        typer.echo("No Shure devices found on the network.")
        return

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        def _fetch_one(d):
            raw = _send(d.ip, d.port, d.protocol, bulk=True)
            return _parse_device(d.ip, d.port, d.protocol, raw, discovered=discovered)

        with ThreadPoolExecutor(max_workers=len(discovered)) as pool:
            device_infos = list(pool.map(_fetch_one, discovered))
        output_single([d.to_json() for d in device_infos])
        return

    def _fetch_firmware(d):
        key = "FW_VER"
        return _send(d.ip, d.port, d.protocol, command=f"GET {key}", expect_key=key)

    dante_devices = asyncio.run(_get_dante_devices()) or {}
    dante_by_mac = {}
    for sn, dev in dante_devices.items():
        if dev.mac_address:
            dante_by_mac[_normalize_mac(dev.mac_address)] = (dev.name or sn, str(dev.ipv4) if dev.ipv4 else "")

    with ThreadPoolExecutor(max_workers=len(discovered)) as pool:
        firmwares = list(pool.map(_fetch_firmware, discovered))

    headers = f"{'Name':<13}{'Model':<8}{'IP Address':<17}{'MAC Address':<20}{'Firmware':<13}{'Dante Name':<16}{'Dante IP':<17}{'Dante MAC':<20}"
    typer.echo(headers)

    for d, fw in zip(discovered, firmwares):
        dante_mac = _load_correlation(d.mac) or ""
        dante_name = ""
        dante_ip = ""
        if dante_mac:
            info = dante_by_mac.get(dante_mac, ("", ""))
            dante_name, dante_ip = info

        typer.echo(
            f"{d.name:<13}"
            f"{d.model:<8}"
            f"{d.ip:<17}"
            f"{d.mac:<20}"
            f"{(fw or ''):<13}"
            f"{dante_name:<16}"
            f"{dante_ip:<17}"
            f"{dante_mac:<20}"
        )


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
        devices_to_query = [ShureDevice(
            ip=resolved_host, mac="", protocol=resolved_protocol,
            model="", name="", port=resolved_port,
        )]
    else:
        devices_to_query = _discover_shure_devices()
        if state.names:
            devices_to_query = [d for d in devices_to_query if any(d.name.lower() == n.lower() for n in state.names)]
        if state.hosts:
            devices_to_query = [d for d in devices_to_query if d.ip in state.hosts]

    if not devices_to_query:
        typer.echo("No Shure devices found.", err=True)
        raise typer.Exit(code=1)

    def _fetch(d):
        raw = _send(d.ip, d.port, d.protocol, bulk=True)
        return d, _parse_device(d.ip, d.port, d.protocol, raw, discovered=devices_to_query)

    with ThreadPoolExecutor(max_workers=len(devices_to_query)) as pool:
        results = list(pool.map(_fetch, devices_to_query))

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        all_channels = []
        for d, info in results:
            for num, ch in sorted(info.channels.items()):
                cj = {"device": info.name, "channel": num}
                if isinstance(ch, ShureChannel):
                    cj.update({"name": ch.name, "active": ch.active, "antenna": ch.antenna_status})
                    if ch.transmitter and ch.transmitter.connected:
                        tx = ch.transmitter
                        cj["tx_model"] = tx.model
                        cj["battery_pct"] = tx.battery_charge_percent
                        cj["battery_min"] = tx.battery_minutes
                        cj["mute"] = tx.mute_status
                elif isinstance(ch, ShureP10TChannel):
                    cj.update({"name": ch.name, "frequency": ch.frequency, "rf_mute": ch.rf_mute})
                all_channels.append(cj)
        output_single(all_channels)
        return

    for d, info in results:
        if len(results) > 1:
            typer.echo(f"\n{info.name} ({d.ip}):")

        for num, ch in sorted(info.channels.items()):
            if isinstance(ch, ShureChannel):
                status = "ACTIVE" if ch.active else "--"
                parts = [f"ch{num}", f"{(ch.name or ''):<12}", f"{status:<8}"]
                if ch.active and ch.transmitter and ch.transmitter.connected:
                    tx = ch.transmitter
                    parts.append(f"TX:{tx.model}")
                    if tx.battery_charge_percent is not None:
                        parts.append(f"batt:{tx.battery_charge_percent}%")
                    elif tx.battery_hours is not None:
                        parts.append(f"batt:{tx.battery_hours:.1f}h")
                    if tx.mute_status:
                        parts.append(f"mute:{tx.mute_status}")
                    if ch.audio_level_rms is not None:
                        parts.append(f"rms:{ch.audio_level_rms}")
                    if ch.signal_quality is not None and ch.signal_quality < 255:
                        parts.append(f"qual:{ch.signal_quality}")
                typer.echo("  ".join(parts))
            elif isinstance(ch, ShureP10TChannel):
                parts = [f"ch{num}", f"{(ch.name or ''):<12}"]
                if ch.frequency:
                    parts.append(f"freq:{ch.frequency}")
                if ch.rf_mute:
                    parts.append("RF_MUTED")
                if ch.audio_in_level_l is not None:
                    parts.append(f"L:{ch.audio_in_level_l}")
                if ch.audio_in_level_r is not None:
                    parts.append(f"R:{ch.audio_in_level_r}")
                typer.echo("  ".join(parts))


def _load_correlation(shure_mac):
    path = _config_path()
    if not path.exists():
        return None
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("["):
            continue
        if "=" in line and '"' in line:
            parts = line.split("=", 1)
            key_mac = parts[0].strip().strip('"')
            if _mac_match(key_mac, shure_mac):
                return parts[1].strip().strip('"')
    return None


def _parse_device(resolved_host, resolved_port, resolved_protocol, raw, discovered=None):
    from netaudio.shure.device import parse_ad4d, parse_p10t

    devices = discovered if discovered is not None else _discover_shure_devices()
    shure_info = next((d for d in devices if d.ip == resolved_host), None)
    mac = shure_info.mac if shure_info else ""

    if resolved_protocol == Protocol.rep:
        device_info = parse_ad4d(raw, resolved_host, mac)
    else:
        device_info = parse_p10t(raw, resolved_host, mac)

    if mac:
        device_info.dante_mac = _load_correlation(mac)

    return device_info


@device_app.command("show")
def shure_device_show(
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname (omit to auto-discover)."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type (auto-detected)."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    from netaudio.cli import state

    resolved_host, resolved_device, resolved_port = _resolve_target(host, device, port)
    raw = _send(resolved_host, resolved_port, resolved_device, bulk=True)
    device_info = _parse_device(resolved_host, resolved_port, resolved_device, raw)

    from netaudio._common import output_single

    if state.output_format.value in ("json", "yaml", "xml", "csv"):
        output_single(device_info.to_json())
    else:
        typer.echo("\n".join(_format_shure_plain(device_info.to_json())))


@app.command("get")
def shure_get(
    key: str = typer.Argument(..., help="Key to query (e.g. CHAN_NAME, MODEL)."),
    host: Optional[str] = typer.Argument(None, help="Device IP or hostname (omit to auto-discover)."),
    device: Optional[Protocol] = typer.Option(None, "--device", "-d", help="Device type (auto-detected)."),
    channel: Optional[str] = typer.Option(None, "--channel", "-c", help="Channel number."),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="TCP control port."),
):
    resolved_host, resolved_device, resolved_port = _resolve_target(host, device, port)
    cfg = PROTOCOL_CONFIGS[resolved_device]
    upper_key = key.upper()
    all_device_keys = cfg["device_rw_keys"] + cfg["device_ro_keys"]
    all_keys = all_device_keys + cfg["channel_rw_keys"] + cfg["channel_ro_keys"]

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
        if channel not in cfg["channels"]:
            typer.echo(f"Channel must be one of {cfg['channels']}.", err=True)
            raise typer.Exit(code=1)
        full_key = f"{channel} {upper_key}"

    result = _send(resolved_host, resolved_port, resolved_device, command=f"GET {full_key}", expect_key=full_key)
    if result is not None:
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
    cfg = PROTOCOL_CONFIGS[resolved_device]
    upper_key = key.upper()

    if upper_key in cfg["device_ro_keys"]:
        typer.echo(f"{upper_key} is read-only.", err=True)
        raise typer.Exit(code=1)

    if upper_key in cfg["channel_ro_keys"]:
        typer.echo(f"{upper_key} is read-only.", err=True)
        raise typer.Exit(code=1)

    if upper_key in cfg["device_rw_keys"]:
        formatted_value = f"{{{value}}}" if upper_key in cfg["brace_keys"] else value
        _send(resolved_host, resolved_port, resolved_device, command=f"SET {upper_key} {formatted_value}")
        return

    if upper_key not in cfg["channel_rw_keys"]:
        typer.echo(f"Unknown key: {upper_key}", err=True)
        raise typer.Exit(code=1)

    if not channel:
        typer.echo(f"{upper_key} is a channel key; --channel is required.", err=True)
        raise typer.Exit(code=1)
    if channel not in cfg["channels"]:
        typer.echo(f"Channel must be one of {cfg['channels']}.", err=True)
        raise typer.Exit(code=1)

    full_key = f"{channel} {upper_key}"
    formatted_value = f"{{{value}}}" if upper_key in cfg["brace_keys"] else value

    _send(resolved_host, resolved_port, resolved_device, command=f"SET {full_key} {formatted_value}")


@app.command("keys")
def shure_keys(
    device: Protocol = typer.Option(..., "--device", "-d", help="Protocol type."),
):
    cfg = PROTOCOL_CONFIGS[device]

    if cfg["device_rw_keys"]:
        typer.echo("Device-level keys (read/write):")
        for k in cfg["device_rw_keys"]:
            typer.echo(f"  {k}")

    if cfg["device_ro_keys"]:
        typer.echo("Device-level keys (read-only):")
        for k in cfg["device_ro_keys"]:
            typer.echo(f"  {k}")

    typer.echo(f"\nChannel-level keys (read/write, channels {', '.join(cfg['channels'])}):")
    for k in cfg["channel_rw_keys"]:
        typer.echo(f"  {k}")

    if cfg["channel_ro_keys"]:
        typer.echo(f"\nChannel-level keys (read-only):")
        for k in cfg["channel_ro_keys"]:
            typer.echo(f"  {k}")


NOISE_FLOOR_SHURE = 200
NOISE_FLOOR_DANTE = 240


def _normalize_mac(mac):
    raw = re.sub(r"[:\-.]", "", mac).lower()[:12]
    return ":".join(raw[i:i+2] for i in range(0, len(raw), 2))


def _mac_match(a, b):
    return _normalize_mac(a) == _normalize_mac(b)


def _config_path():
    from netaudio.common.config_loader import default_config_path
    return default_config_path()


def _save_correlation(shure_mac, dante_mac):
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    text = path.read_text() if path.exists() else ""
    section_header = "[shure.correlations]"

    output = []
    skip_section = False
    in_section = False
    replaced = False

    for line in text.splitlines(keepends=True):
        stripped = line.strip()

        if stripped.startswith("[shure.correlations."):
            mac_in_header = stripped.split('"')[1] if '"' in stripped else ""
            if _mac_match(mac_in_header, shure_mac):
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
                mac_in_line = stripped.split('"')[1]
                if _mac_match(mac_in_line, shure_mac):
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

    path.write_text("".join(output))
    return path


def _shure_query(host, port, commands):
    sock = socket.create_connection((host, port), timeout=2)
    sock.settimeout(0.3)
    sock.sendall("".join(f"< {command} >\r\n" for command in commands).encode())

    response = b""
    expected = len(commands)
    while response.count(b"REP") < expected:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
        except socket.timeout:
            break
    sock.close()

    parsed = {}
    for match in re.finditer(r"REP\s+(\d)\s+(\S+)\s+\{?([^}>]*?)\}?\s*>", response.decode("utf-8", errors="ignore")):
        channel = int(match.group(1))
        parsed.setdefault(channel, {})[match.group(2)] = match.group(3).strip()
    return parsed


def _sample_shure_levels(host, port, channels):
    responses = _shure_query(host, port, [f"GET {channel} AUDIO_LEVEL_RMS" for channel in channels])
    return {channel: int(values["AUDIO_LEVEL_RMS"]) for channel, values in responses.items() if "AUDIO_LEVEL_RMS" in values}


async def _sample_all_dante_levels(dante_devices):
    from netaudio.daemon.client import meter_snapshot_from_daemon

    result = {}
    for server_name in dante_devices:
        snapshot = await meter_snapshot_from_daemon(server_name)
        if snapshot and snapshot.get("tx"):
            result[server_name] = {
                int(channel): info["level"] if isinstance(info, dict) else info
                for channel, info in snapshot["tx"].items()
            }
    return result


def _get_active_shure_channels(host, port, channels):
    responses = _shure_query(host, port, [
        command
        for channel in channels
        for command in [f"GET {channel} TX_MODEL", f"GET {channel} ANTENNA_STATUS"]
    ])

    return [
        int(channel)
        for channel in channels
        if responses.get(int(channel), {}).get("TX_MODEL", "UNKNOWN") != "UNKNOWN"
        and responses.get(int(channel), {}).get("ANTENNA_STATUS", "XX") != "XX"
    ]


async def _sample_both(shure_host, shure_port, active_channels, dante_devices):
    loop = asyncio.get_event_loop()
    shure_future = loop.run_in_executor(
        None, _sample_shure_levels, shure_host, shure_port, [str(channel) for channel in active_channels]
    )
    dante_future = _sample_all_dante_levels(dante_devices)
    shure_levels, all_dante_levels = await asyncio.gather(shure_future, dante_future)
    return shure_levels, all_dante_levels


def _match_by_level(shure_levels, all_dante_levels):
    hot_shure = {channel: level for channel, level in shure_levels.items() if level < NOISE_FLOOR_SHURE}
    if not hot_shure:
        return None, None

    for server_name, dante_levels in all_dante_levels.items():
        hot_dante = {channel: level for channel, level in dante_levels.items() if level < NOISE_FLOOR_DANTE}
        if not hot_dante:
            continue

        shure_ranked = sorted(hot_shure, key=lambda channel: hot_shure[channel])
        dante_ranked = sorted(hot_dante, key=lambda channel: hot_dante[channel])

        channel_mapping = dict(zip(shure_ranked, dante_ranked))
        return server_name, channel_mapping

    return None, None


def _format_sample(shure_levels, all_dante_levels, active_channels):
    shure_parts = [f"ch{channel}={shure_levels.get(channel, '?')}" for channel in active_channels]
    dante_parts = []
    for server_name, levels in all_dante_levels.items():
        hot = [f"tx{channel}={level}" for channel, level in sorted(levels.items()) if level < NOISE_FLOOR_DANTE]
        if hot:
            dante_parts.append(f"{server_name}: {' '.join(hot)}")
    return f"  shure: {' '.join(shure_parts)}  dante: {', '.join(dante_parts) or '(silent)'}"


def _looks_like_mac(value):
    raw = re.sub(r"[:\-.]", "", value)
    return len(raw) in (12, 16) and all(c in "0123456789abcdefABCDEF" for c in raw)



async def _get_dante_devices():
    from netaudio.daemon.client import get_devices_from_daemon
    return await get_devices_from_daemon()


def _resolve_as_shure(value):
    for device in _discover_shure_devices():
        if (device.ip == value
                or device.name.lower() == value.lower()
                or (_looks_like_mac(value) and _mac_match(value, device.mac))):
            return _normalize_mac(device.mac)
    if _looks_like_mac(value) and _normalize_mac(value).startswith("00:0e:dd"):
        return _normalize_mac(value)
    return None


def _resolve_as_dante(value):
    dante_devices = asyncio.run(_get_dante_devices()) or {}
    for server_name, device in dante_devices.items():
        if not device.mac_address:
            continue
        if (value.lower() == (device.name or "").lower()
                or value == str(device.ipv4)
                or server_name.lower().startswith(value.lower())
                or (_looks_like_mac(value) and _mac_match(value, device.mac_address))):
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

        shure_a = _resolve_as_shure(device_a)
        shure_b = _resolve_as_shure(device_b)
        dante_a = _resolve_as_dante(device_a)
        dante_b = _resolve_as_dante(device_b)

        if shure_a and dante_b:
            _save_correlation(shure_a, dante_b)
            return
        if shure_b and dante_a:
            _save_correlation(shure_b, dante_a)
            return

        typer.echo("Could not determine which is Shure and which is Dante.", err=True)
        raise typer.Exit(code=1)

    shure_host, _, _ = _resolve_target(None, None, None)
    shure_device_info = next((d for d in _discover_shure_devices() if d.ip == shure_host), None)
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

    shure_host, shure_device, shure_port = _resolve_target(host, device, port)
    active_channels = _get_active_shure_channels(shure_host, shure_port, PROTOCOL_CONFIGS[shure_device]["channels"])

    if not active_channels:
        typer.echo("No active transmitters found.", err=True)
        raise typer.Exit(code=1)

    all_dante_devices = await get_devices_from_daemon()
    if not all_dante_devices:
        typer.echo("Daemon not running. Start the daemon first: netaudio server start", err=True)
        raise typer.Exit(code=1)

    if dante_name:
        dante_devices = {
            sn: dev for sn, dev in all_dante_devices.items()
            if dante_name.lower() in (dev.name or sn).lower()
        }
        if not dante_devices:
            typer.echo(f"No Dante device matching '{dante_name}'.", err=True)
            raise typer.Exit(code=1)
    else:
        dante_devices = all_dante_devices

    client_id = "shure_correlate"
    for server_name in dante_devices:
        await meter_start_on_daemon(server_name, client_id)

    try:
        typer.echo(f"Active Shure channels: {', '.join(str(ch) for ch in active_channels)}", err=True)
        typer.echo(f"Dante devices: {len(dante_devices)}", err=True)
        typer.echo("Sampling... enable tone generator on desired channels.", err=True)

        previous_match = None
        streak = 0
        start = time.monotonic()

        while time.monotonic() - start < timeout:
            shure_levels, all_dante_levels = await _sample_both(shure_host, shure_port, active_channels, dante_devices)
            matched_server, channel_mapping = _match_by_level(shure_levels, all_dante_levels)

            if matched_server and channel_mapping and len(channel_mapping) == len(active_channels):
                current_match = (matched_server, channel_mapping)
            else:
                current_match = None

            if current_match and current_match == previous_match:
                streak += 1
            else:
                streak = 1 if current_match else 0

            previous_match = current_match

            if streak >= consecutive and current_match:
                confirmed_server, _ = current_match
                elapsed = time.monotonic() - start
                typer.echo(f"Correlated to {confirmed_server} in {elapsed:.1f}s", err=True)

                if save:
                    shure_device_info = next((d for d in _discover_shure_devices() if d.ip == shure_host), None)
                    dante_device = dante_devices.get(confirmed_server)
                    if shure_device_info and dante_device and dante_device.mac_address:
                        path = _save_correlation(shure_device_info.mac, _normalize_mac(dante_device.mac_address))
                        typer.echo(f"Saved to {path}", err=True)
                return

            typer.echo(_format_sample(shure_levels, all_dante_levels, active_channels), err=True)
            await asyncio.sleep(0.1)

        typer.echo("Timed out.", err=True)
        raise typer.Exit(code=1)
    finally:
        for server_name in dante_devices:
            await meter_stop_on_daemon(server_name, client_id)
