from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Optional

import typer

from netaudio import DanteDevice
from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import _get_state


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

    if not state.names and not state.hosts and not state.server_names and not state.macs and not state.ddm_context:
        return devices

    filtered = {}

    for server_name, device in devices.items():
        if (
            state.ddm_context
            and getattr(device, "ddm_device_id", None)
            and getattr(device, "ddm_context", None) != state.ddm_context
        ):
            continue

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


CHANNEL_REFERENCE_FORMS = "tx:NUMBER, rx:NUMBER, tx:NAME, rx:NAME, or a bare channel name"
QUALIFIED_CHANNEL_FORMS = "tx:1@DEVICE, rx:1@DEVICE, or CHANNEL-NAME@DEVICE"

CHANNEL_DIRECTION_LABELS = {"rx": "RX", "tx": "TX"}


@dataclass(frozen=True)
class ChannelReference:
    direction: Optional[str]
    identifier: str


def _channel_reference_error(message: str) -> typer.Exit:
    typer.echo(f"Error: {message}; accepted forms are {CHANNEL_REFERENCE_FORMS}.", err=True)
    return typer.Exit(code=ExitCode.ERROR)


def parse_channel_reference(value: str, default_direction: Optional[str] = None) -> ChannelReference:
    text = value.strip()
    if not text:
        raise _channel_reference_error("channel reference is empty")

    prefix, separator, remainder = text.partition(":")
    if separator:
        direction = prefix.strip().lower()
        identifier = remainder.strip()
        if direction not in CHANNEL_DIRECTION_LABELS:
            raise _channel_reference_error(f"unknown channel direction {prefix.strip()!r} in {value!r}")
        if not identifier:
            raise _channel_reference_error(f"missing channel after {direction}: in {value!r}")
        return ChannelReference(direction, identifier)

    return ChannelReference(default_direction, text)


def parse_qualified_channel(value: str, default_direction: Optional[str] = None) -> tuple[ChannelReference, str]:
    channel_text, separator, device_identifier = value.rpartition("@")
    if not separator or not channel_text.strip() or not device_identifier.strip():
        typer.echo(
            f"Error: expected CHANNEL@DEVICE, got {value!r}; accepted forms are {QUALIFIED_CHANNEL_FORMS}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    return parse_channel_reference(channel_text, default_direction), device_identifier.strip()


def resolve_channel(device: DanteDevice, reference: ChannelReference):
    device_label = device.name or device.server_name or str(device.ipv4)
    if reference.direction is not None:
        channel = find_channel(device, reference.identifier, reference.direction)
        if channel is None:
            typer.echo(
                f"Error: {CHANNEL_DIRECTION_LABELS[reference.direction]} channel {reference.identifier!r} "
                f"not found on {device_label}.",
                err=True,
            )
            raise typer.Exit(code=ExitCode.ERROR)
        return reference.direction, channel

    matches = [
        (direction, channel)
        for direction in ("rx", "tx")
        for channel in [find_channel(device, reference.identifier, direction)]
        if channel is not None
    ]
    if not matches:
        typer.echo(
            f"Error: channel {reference.identifier!r} not found on {device_label}; "
            f"accepted forms are {CHANNEL_REFERENCE_FORMS}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    if len(matches) > 1:
        typer.echo(
            f"Error: channel {reference.identifier!r} matches both TX and RX on {device_label}; "
            f"use tx:{reference.identifier} or rx:{reference.identifier}.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)
    return matches[0]


def match_device_identifier(devices: dict[str, DanteDevice], identifier: str) -> dict[str, DanteDevice]:
    matches = {}
    for server_name, device in devices.items():
        if (
            device.name == identifier
            or (device.ipv4 and str(device.ipv4) == identifier)
            or server_name == identifier
            or server_name.startswith(identifier + ".")
        ):
            matches[server_name] = device
    return matches


def select_device(devices: dict[str, DanteDevice], allow_many: bool = False) -> list[tuple[str, DanteDevice]]:
    if not devices:
        typer.echo("Error: device not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    if allow_many:
        return sort_devices(devices)
    if len(devices) > 1:
        names = ", ".join(device.name or server_name for server_name, device in sort_devices(devices))
        typer.echo(f"Error: multiple devices matched: {names}; narrow the filter or pass --all.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    return list(devices.items())


def find_channel(device: DanteDevice, channel_id: str, channel_type: str):
    channels = device.rx_channels if channel_type == "rx" else device.tx_channels

    if channel_id.isdigit():
        number = int(channel_id)
        for channel in channels.values():
            if channel.number == number:
                return channel

    for channel in channels.values():
        if channel.name == channel_id or channel.friendly_name == channel_id:
            return channel

    return None
