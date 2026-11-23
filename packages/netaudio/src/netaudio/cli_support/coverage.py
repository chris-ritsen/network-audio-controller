from __future__ import annotations

import typer

from netaudio.cli_support.selection import filter_devices, require_selected_devices, sort_devices
from netaudio.daemon import engine
from netaudio.daemon.client import DaemonRefusal, DaemonSilent, DaemonUnavailable, call_daemon
from netaudio.device_record import Device

_cut_short = False


def cut_short() -> bool:
    return _cut_short


def dante_interfaces(settings: dict) -> list[dict]:
    selected = settings.get("dante_interfaces") or []
    rows = []
    for interface in settings.get("network_interfaces") or []:
        addresses = interface.get("addresses") or []
        rows.append(
            {
                "addresses": addresses,
                "connected": bool(interface.get("running")),
                "name": interface.get("name"),
                "used_for_dante": not selected
                or interface.get("name") in selected
                or any(address in selected for address in addresses),
            }
        )
    known = {row["name"] for row in rows} | {address for row in rows for address in row["addresses"]}
    for name in selected:
        if name not in known:
            rows.append({"addresses": [], "connected": False, "name": name, "used_for_dante": True})
    return rows


def _count(number: int) -> str:
    return f"{number} device" if number == 1 else f"{number} devices"


def _joined(items: list[str]) -> str:
    if len(items) < 2:
        return "".join(items)
    return f"{', '.join(items[:-1])} and {items[-1]}"


def _unfinished(devices: dict[str, Device]) -> tuple[list[str], list[tuple[str, str]]]:
    reading = []
    unread = []
    for server_name, device in sort_devices(devices):
        if not device.online:
            continue
        label = device.name or server_name
        if device.inventory_state == "reading":
            reading.append(label)
        elif device.inventory_state == "unread":
            unread.append((label, (device.read_errors or {}).get("inventory") or "it did not answer"))
    return reading, unread


def warn(devices: dict[str, Device]) -> None:
    selected = filter_devices(devices)
    reading, unread = _unfinished(selected)
    if reading:
        online = sum(1 for device in selected.values() if device.online)
        verb = "was" if len(reading) == 1 else "were"
        typer.echo(
            f"Warning: {len(reading)} of {_count(online)} {verb} still being read "
            f"and {'is' if len(reading) == 1 else 'are'} incomplete here: {', '.join(reading)}.",
            err=True,
        )
    for label, reason in unread:
        typer.echo(f"Warning: {label} could not be read: {reason}.", err=True)
    if (reading or unread) and engine.port() is not None:
        typer.echo(
            "No daemon is running, so this command searched the network itself. "
            'Run "netaudio daemon start" to keep one running.',
            err=True,
        )


def listed(devices: dict[str, Device]) -> dict[str, Device]:
    global _cut_short

    selected = require_selected_devices(filter_devices(devices))
    reading, _ = _unfinished(selected)
    _cut_short = _cut_short or bool(reading)
    return selected


async def searched() -> str:
    try:
        settings = await call_daemon("GET", "/settings", timeout=30.0)
    except (DaemonRefusal, DaemonSilent, DaemonUnavailable):
        return "No Dante devices found."
    used = [
        f"{row['name']} ({', '.join(row['addresses'])})" if row["addresses"] else f"{row['name']} (not present)"
        for row in dante_interfaces(settings)
        if row["used_for_dante"] and (row["connected"] or not row["addresses"])
    ]
    if not used:
        return "No network interface is up for Dante."
    return f"No Dante devices found on {_joined(used)}."


async def nothing(devices: dict[str, Device], selected: dict[str, Device], subject: str) -> str:
    if not devices:
        return await searched()
    return f"No {subject} on {_count(len(selected))}."
