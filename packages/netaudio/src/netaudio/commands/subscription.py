from __future__ import annotations

from typing import NoReturn, Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support import coverage
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import ansi, run_with_devices
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import (
    filter_devices,
    match_device_identifier,
    parse_qualified_channel,
    resolve_channel,
    select_device,
    sort_devices,
)
from netaudio.daemon.client import call_daemon
from netaudio.icons import SEVERITY_PRESENTATION, icon, severity_icon

app = typer.Typer(help="Manage audio subscriptions.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


def _fail(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _device_by_identifier(devices, identifier: str, side: str):
    matches = match_device_identifier(devices, identifier)
    if not matches:
        _fail(f"{side} device '{identifier}' not found.")
    [(server_name, device)] = select_device(matches)
    return server_name, device


def _status_label(status: dict) -> str:
    label = status.get("label") or status.get("status") or ""
    severity = status.get("severity") or ""
    marker = severity_icon(severity)
    color = SEVERITY_PRESENTATION.get(severity, {}).get("color")
    colored = ansi(color, label) if color else label
    return f"{marker} {colored}" if marker else colored


async def run_subscription_list(devices, include_unused: bool) -> None:
    selected = coverage.listed(devices)
    subscriptions = [
        subscription
        for _, device in sort_devices(selected)
        for subscription in device.record.get("subscriptions") or []
        if include_unused or subscription.get("tx_device")
    ]
    output_table(
        ["RX Channel", "RX Device", "TX Channel", "TX Device", "Status"],
        [
            [
                subscription.get("rx_channel") or "",
                subscription.get("rx_device") or "",
                subscription.get("tx_channel") or "",
                subscription.get("tx_device") or "",
                _status_label(subscription.get("status") or {}),
            ]
            for subscription in subscriptions
        ],
        json_data=subscriptions,
        empty_message=await coverage.nothing(devices, selected, "active subscriptions"),
    )


@app.command("list")
def subscription_list(
    include_unused: bool = typer.Option(
        False,
        "--all",
        help="Include unused receiver channels that have no configured source.",
    ),
):
    """List configured subscriptions."""
    run_with_devices(run_subscription_list, include_unused)


async def _apply(routes: list[dict], labels: list[str], done: str) -> None:
    result = await call_daemon("POST", "/subscriptions/apply", {"routes": routes})
    failed = False
    for label, outcome in zip(labels, result["routes"]):
        if outcome.get("ok"):
            typer.echo(f"{done}{label}")
        else:
            failed = True
            typer.echo(f"{icon('fail')}FAILED {label}: {outcome.get('error') or 'not applied'}", err=True)
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)


async def run_subscription_add_single(devices, tx: str, rx: str) -> None:
    tx_reference, tx_device_id = parse_qualified_channel(tx, "tx")
    rx_reference, rx_device_id = parse_qualified_channel(rx, "rx")
    if tx_reference.direction != "tx":
        _fail(f"--tx must name a transmitter channel (tx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {tx!r}")
    if rx_reference.direction != "rx":
        _fail(f"--rx must name a receiver channel (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {rx!r}")
    _, tx_device = _device_by_identifier(devices, tx_device_id, "TX")
    rx_server_name, rx_device = _device_by_identifier(devices, rx_device_id, "RX")
    _, tx_channel = resolve_channel(tx_device, tx_reference)
    _, rx_channel = resolve_channel(rx_device, rx_reference)
    await _apply(
        [
            {
                "rx_device": rx_server_name,
                "rx_channel": rx_channel.number,
                "tx_device": tx_device.name,
                "tx_channel": tx_channel.friendly_name or tx_channel.name,
            }
        ],
        [f"{rx_reference.identifier}@{rx_device.name} <- {tx_reference.identifier}@{tx_device.name}"],
        icon("add"),
    )


def _bulk_pairs(tx_device, rx_device, count: int, offset_tx: int, offset_rx: int):
    tx_sorted = sorted(tx_device.tx_channels.values(), key=lambda channel: channel.number)
    rx_sorted = sorted(rx_device.rx_channels.values(), key=lambda channel: channel.number)

    if not tx_sorted:
        _fail(f"no TX channels on {tx_device.name}")
    if not rx_sorted:
        _fail(f"no RX channels on {rx_device.name}")
    if offset_tx >= len(tx_sorted):
        _fail(f"--offset-tx {offset_tx} is outside the {len(tx_sorted)} TX channels on {tx_device.name}")
    if offset_rx >= len(rx_sorted):
        _fail(f"--offset-rx {offset_rx} is outside the {len(rx_sorted)} RX channels on {rx_device.name}")

    available_pairs = min(len(tx_sorted) - offset_tx, len(rx_sorted) - offset_rx)
    if count > available_pairs:
        _fail(f"--count {count} exceeds the {available_pairs} channel pairs available after applying offsets")
    pair_count = count or available_pairs
    pairs = list(zip(tx_sorted[offset_tx : offset_tx + pair_count], rx_sorted[offset_rx : offset_rx + pair_count]))
    if not pairs:
        _fail("no channel pairs are available to subscribe")
    if not tx_device.name:
        _fail("the TX device must have a Dante name")
    return pairs


async def run_subscription_add_bulk(devices, tx: str, rx: str, count: int, offset_tx: int, offset_rx: int) -> None:
    _, tx_device = _device_by_identifier(devices, tx, "TX")
    rx_server_name, rx_device = _device_by_identifier(devices, rx, "RX")
    routes = []
    labels = []
    for tx_channel, rx_channel in _bulk_pairs(tx_device, rx_device, count, offset_tx, offset_rx):
        tx_name = tx_channel.friendly_name or tx_channel.name
        rx_name = rx_channel.friendly_name or rx_channel.name
        routes.append(
            {
                "rx_device": rx_server_name,
                "rx_channel": rx_channel.number,
                "tx_device": tx_device.name,
                "tx_channel": tx_name,
            }
        )
        labels.append(f"{rx_name}@{rx_device.name} <- {tx_name}@{tx_device.name}")
    await _apply(routes, labels, icon("add"))


@app.command()
def add(
    tx: str = typer.Option(
        ...,
        "--tx",
        help="TX source: tx:1@DEVICE, 1@DEVICE, or NAME@DEVICE (single), or DEVICE (bulk 1:1).",
    ),
    rx: str = typer.Option(
        ...,
        "--rx",
        help="RX destination: rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE (single), or DEVICE (bulk 1:1).",
    ),
    count: int = typer.Option(
        0,
        "--count",
        "-c",
        min=0,
        help="Number of channels (bulk only, 0 = all available pairs).",
    ),
    offset_tx: int = typer.Option(
        0,
        "--offset-tx",
        min=0,
        help="Starting TX channel offset (bulk only, 0-based).",
    ),
    offset_rx: int = typer.Option(
        0,
        "--offset-rx",
        min=0,
        help="Starting RX channel offset (bulk only, 0-based).",
    ),
):
    """Add subscriptions. Single: --tx tx:1@DEVICE --rx rx:1@DEVICE. Bulk: --tx DEVICE --rx DEVICE."""
    if not tx or not rx:
        _fail("both --tx and --rx are required")
    if count < 0 or offset_tx < 0 or offset_rx < 0:
        _fail("--count and channel offsets must be nonnegative")

    is_single = "@" in tx and "@" in rx

    if is_single:
        if count or offset_tx or offset_rx:
            _fail("--count and channel offsets are only valid for bulk subscriptions")
        run_with_devices(run_subscription_add_single, tx, rx)
        return

    if "@" in tx or "@" in rx:
        _fail("both --tx and --rx must be CHANNEL@DEVICE or both must be device names")
    run_with_devices(run_subscription_add_bulk, tx, rx, count, offset_tx, offset_rx)


async def run_subscription_remove(devices, rx: list[str] | None, all_channels: bool) -> None:
    routes = []
    labels = []
    if all_channels:
        selected = filter_devices(devices)
        if not selected:
            _fail("no devices matched the global filters")
        for server_name, device in sort_devices(selected):
            for subscription in device.record.get("subscriptions") or []:
                if subscription.get("tx_device"):
                    routes.append({"rx_device": server_name, "rx_channel": subscription["rx_channel_number"]})
                    labels.append(f"{subscription['rx_channel']}@{device.name}")
    for rx_spec in rx or []:
        if "@" not in rx_spec:
            _fail(
                f"expected CHANNEL@DEVICE for --rx {rx_spec!r} (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE); "
                "use global device filters with --all to remove every subscription on a device"
            )
        rx_reference, rx_device_id = parse_qualified_channel(rx_spec, "rx")
        if rx_reference.direction != "rx":
            _fail(f"--rx must name a receiver channel (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {rx_spec!r}")
        server_name, rx_device = _device_by_identifier(devices, rx_device_id, "RX")
        _, rx_channel = resolve_channel(rx_device, rx_reference)
        route = {"rx_device": server_name, "rx_channel": rx_channel.number}
        if route not in routes:
            routes.append(route)
            labels.append(f"{rx_channel.friendly_name or rx_channel.name}@{rx_device.name}")
    if not routes:
        typer.echo("No active subscriptions.")
        return
    await _apply(routes, labels, f"{icon('remove')}Removed: ")


@app.command()
def remove(
    rx: Optional[list[str]] = typer.Option(
        None,
        "--rx",
        help="RX channel to unsubscribe: rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE (repeatable).",
    ),
    all_channels: bool = typer.Option(
        False,
        "--all",
        help="Remove all subscriptions on devices selected by the global filters.",
    ),
):
    """Remove subscriptions from RX channels. Supports bulk removal."""
    if all_channels and rx:
        _fail("use either specific --rx channels or --all, not both")
    if not all_channels and not rx:
        _fail("--rx is required unless --all is used")
    run_with_devices(run_subscription_remove, rx, all_channels)
