from fnmatch import fnmatch
from glob import has_magic
from typing import Optional

from netaudio import DanteDevice
from netaudio._common_cli import _get_state
from netaudio.common.app_config import settings


def _normalize_mac(mac: str) -> str:
    raw = mac.replace(":", "").replace("-", "").replace(".", "").lower()
    if len(raw) == 16 and raw[6:10] == "fffe":
        raw = raw[:6] + raw[10:]
    elif len(raw) == 16 and raw.endswith("0000"):
        raw = raw[:12]
    return raw


def _strip_separators(mac: str) -> str:
    return mac.replace(":", "").replace("-", "").replace(".", "").lower()


def _mac_matches(device_mac: str, pattern: str) -> bool:
    raw_device = _strip_separators(device_mac)
    raw_pattern = _strip_separators(pattern)

    if raw_device == raw_pattern:
        return True

    return _normalize_mac(device_mac) == _normalize_mac(pattern)


def filter_devices(devices: dict[str, DanteDevice], include_names: bool = True) -> dict[str, DanteDevice]:
    state = _get_state()

    if not state.names and not state.hosts and not state.server_names and not state.macs:
        return devices

    filtered = {}

    for server_name, device in devices.items():
        if include_names and state.names and not any(fnmatch(device.name or "", pat) for pat in state.names):
            continue

        if state.hosts and not any(str(device.ipv4) == h for h in state.hosts):
            continue

        if state.server_names and not any(fnmatch(server_name, pat) for pat in state.server_names):
            continue

        if state.macs and not any(_mac_matches(device.mac_address or "", pat) for pat in state.macs):
            continue

        filtered[server_name] = device

    return filtered


def sort_devices(devices: dict[str, DanteDevice]) -> list[tuple[str, DanteDevice]]:
    state = _get_state()

    sort_keys = {
        "mac": lambda item: item[1].mac_address or "",
        "name": lambda item: item[1].name or "",
        "ip": lambda item: tuple(int(part) for part in str(item[1].ipv4).split(".")) if item[1].ipv4 else (0,),
        "model": lambda item: item[1].model_id or "",
        "server-name": lambda item: item[0],
    }

    return sorted(devices.items(), key=sort_keys[state.sort_field], reverse=state.sort_reverse)


def set_device_filter(device_arg: str) -> None:
    state = _get_state()
    state.names = [device_arg]


def parse_qualified_name(s: str) -> tuple[str, str]:
    if "@" not in s:
        typer.echo(f"Error: expected channel@device format, got: {s}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    channel, device = s.rsplit("@", 1)
    return channel, device


def find_device(devices: dict[str, DanteDevice], identifier: str) -> Optional[DanteDevice]:
    for server_name, device in devices.items():
        if device.name == identifier:
            return device
        if device.ipv4 and str(device.ipv4) == identifier:
            return device
        if server_name == identifier or server_name.startswith(identifier + "."):
            return device

    return None


def find_channel(device: DanteDevice, channel_id: str, channel_type: str):
    channels = device.rx_channels if channel_type == "rx" else device.tx_channels

    try:
        number = int(channel_id)
        for channel in channels.values():
            if channel.number == number:
                return channel
    except ValueError:
        pass

    for channel in channels.values():
        if channel.name == channel_id or channel.friendly_name == channel_id:
            return channel

    return None
