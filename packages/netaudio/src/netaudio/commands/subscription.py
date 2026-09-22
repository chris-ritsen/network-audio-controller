from __future__ import annotations

from typing import NoReturn, Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import run_command
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import (
    filter_devices,
    require_selected_devices,
    match_device_identifier,
    parse_qualified_channel,
    resolve_channel,
    select_device,
    sort_devices,
)
from netaudio.dante.readback import MUTATION_ERRORS
from netaudio.dante.subscription_operations import (
    subscription_sources,
    reconcile_receiver_subscriptions,
)
from netaudio.icons import icon

app = typer.Typer(help="Manage audio subscriptions.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


def _fail(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _device_label(device) -> str:
    return (
        getattr(device, "name", None) or getattr(device, "server_name", None) or str(getattr(device, "ipv4", "device"))
    )


def _device_by_identifier(devices, identifier: str, side: str):
    matches = match_device_identifier(devices, identifier)
    if not matches:
        typer.echo(f"Error: {side} device '{identifier}' not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    [(_, device)] = select_device(matches)
    return device


def _subscription_has_configured_source(subscription) -> bool:
    return bool(getattr(subscription, "has_configured_source", getattr(subscription, "tx_device_name", None)))


async def run_subscription_list(application, devices, include_unused: bool) -> None:
    from netaudio.core import (
        subscription_status,
    )
    from netaudio.dante.device_serializer import DanteDeviceSerializer

    devices = require_selected_devices(filter_devices(devices))

    all_subscriptions = []

    for server_name, device in sort_devices(devices):
        for subscription in device.subscriptions:
            if include_unused or _subscription_has_configured_source(subscription):
                all_subscriptions.append(subscription)

    from netaudio.cli_support.execution import ansi, report_inventory_failures
    from netaudio.icons import SEVERITY_PRESENTATION, severity_icon

    def _status_label(subscription):
        code = subscription.status_code
        if code is None:
            return "; ".join(subscription.status_text())
        entry = subscription_status(code, subscription.rx_channel_status_code)
        severity = str(entry["severity"])
        label = str(entry["label"])
        if subscription.status_message:
            label = "; ".join((label, *subscription.status_message))
            if severity == "ok":
                severity = "warning"
        marker = severity_icon(severity)
        color = SEVERITY_PRESENTATION.get(severity, {}).get("color")
        colored_label = ansi(color, label) if color else label
        return f"{marker} {colored_label}" if marker else colored_label

    headers = ["RX Channel", "RX Device", "TX Channel", "TX Device", "Status"]
    rows = []
    json_data = [DanteDeviceSerializer.subscription_to_json(s) for s in all_subscriptions]

    for subscription in all_subscriptions:
        configured = _subscription_has_configured_source(subscription)
        rows.append(
            [
                subscription.rx_channel_name or "",
                subscription.rx_device_name or "",
                (subscription.tx_channel_name or "") if configured else "",
                (subscription.tx_device_name or "") if configured else "",
                _status_label(subscription),
            ]
        )

    output_table(headers, rows, json_data=json_data, empty_message="No active subscriptions.")
    if report_inventory_failures(devices, "subscriptions"):
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("list")
def subscription_list(
    include_unused: bool = typer.Option(
        False,
        "--all",
        help="Include unused receiver channels that have no configured source.",
    ),
):
    """List configured subscriptions."""
    run_command(run_subscription_list, include_unused)


async def run_subscription_add_single(application, devices, tx: str, rx: str) -> None:
    tx_reference, tx_device_id = parse_qualified_channel(tx, "tx")
    rx_reference, rx_device_id = parse_qualified_channel(rx, "rx")
    if tx_reference.direction != "tx":
        _fail(f"--tx must name a transmitter channel (tx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {tx!r}")
    if rx_reference.direction != "rx":
        _fail(f"--rx must name a receiver channel (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {rx!r}")

    tx_device = _device_by_identifier(devices, tx_device_id, "TX")
    rx_device = _device_by_identifier(devices, rx_device_id, "RX")

    _, tx_channel = resolve_channel(tx_device, tx_reference)
    _, rx_channel = resolve_channel(rx_device, rx_reference)

    tx_channel_name = tx_channel.friendly_name or tx_channel.name

    if not tx_channel_name or not tx_device.name:
        _fail("the TX channel and device must have Dante names")

    try:
        result = await reconcile_receiver_subscriptions(
            application, rx_device, {rx_channel.number: (tx_channel_name, tx_device.name)}
        )
    except MUTATION_ERRORS as error:
        _fail(f"could not request subscription: {error}")

    if result.failures:
        _fail(result.failures[rx_channel.number])

    label = f"{rx_reference.identifier}@{rx_device.name} <- {tx_reference.identifier}@{tx_device.name}"

    if result.unchanged:
        typer.echo(f"UNCHANGED {label} (already subscribed)")
    else:
        typer.echo(f"{icon('add')}{label} (verified)")


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


async def run_subscription_add_bulk(
    application,
    devices,
    tx: str,
    rx: str,
    count: int,
    offset_tx: int,
    offset_rx: int,
) -> None:
    tx_device = _device_by_identifier(devices, tx, "TX")
    rx_device = _device_by_identifier(devices, rx, "RX")

    try:
        await rx_device.get_rx_channels()
    except MUTATION_ERRORS as error:
        _fail(f"could not read current subscriptions from {_device_label(rx_device)} before making changes: {error}")

    desired = {}
    labels = {}

    for tx_channel, rx_channel in _bulk_pairs(tx_device, rx_device, count, offset_tx, offset_rx):
        tx_name = tx_channel.friendly_name or tx_channel.name

        if not tx_name:
            _fail(f"TX channel {tx_channel.number} has no Dante name")

        rx_name = rx_channel.friendly_name or rx_channel.name
        desired[rx_channel.number] = (tx_name, tx_device.name)
        labels[rx_channel.number] = f"{rx_name}@{rx_device.name} <- {tx_name}@{tx_device.name}"

    try:
        result = await reconcile_receiver_subscriptions(application, rx_device, desired)
    except MUTATION_ERRORS as error:
        _fail(f"could not apply subscriptions to {_device_label(rx_device)}: {error}")

    for number, label in labels.items():
        if number in result.unchanged:
            typer.echo(f"UNCHANGED {label} (already subscribed)")
        elif number in result.verified:
            typer.echo(f"MODIFIED {label} (verified)")
        else:
            typer.echo(f"{icon('fail')}FAILED {label}: {result.failures[number]}", err=True)

    if result.failures:
        raise typer.Exit(code=ExitCode.ERROR)


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
        run_command(run_subscription_add_single, tx, rx)
        return

    if "@" in tx or "@" in rx:
        _fail("both --tx and --rx must be CHANNEL@DEVICE or both must be device names")
    run_command(run_subscription_add_bulk, tx, rx, count, offset_tx, offset_rx)


def _subscribed_channels(device):
    sources = subscription_sources(device, (channel.number for channel in device.rx_channels.values()))

    return [
        channel
        for channel in sorted(device.rx_channels.values(), key=lambda candidate: candidate.number)
        if sources[channel.number] is not None
    ]


async def _removals_for_all(devices) -> dict[int, dict]:
    device_removals: dict[int, dict] = {}
    selected = filter_devices(devices)
    if not selected:
        _fail("no devices matched the global filters")
    for device in selected.values():
        try:
            await device.get_rx_channels()
        except MUTATION_ERRORS as error:
            _fail(f"could not read current subscriptions from {_device_label(device)}: {error}")
        channels = _subscribed_channels(device)
        if not channels:
            typer.echo(f"No active subscriptions on {_device_label(device)}.")
            continue
        device_removals[id(device)] = {"device": device, "channels": channels}
    return device_removals


async def _removals_for_channels(devices, rx: list[str]) -> dict[int, dict]:
    device_removals: dict[int, dict] = {}
    refreshed_devices = set()
    for rx_spec in rx:
        if "@" not in rx_spec:
            _fail(
                f"expected CHANNEL@DEVICE for --rx {rx_spec!r} (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE); "
                "use global device filters with --all to remove every subscription on a device"
            )
        rx_reference, rx_device_id = parse_qualified_channel(rx_spec, "rx")
        if rx_reference.direction != "rx":
            _fail(f"--rx must name a receiver channel (rx:1@DEVICE, 1@DEVICE, or NAME@DEVICE), got {rx_spec!r}")
        rx_device = _device_by_identifier(devices, rx_device_id, "RX")

        if id(rx_device) not in refreshed_devices:
            try:
                await rx_device.get_rx_channels()
            except MUTATION_ERRORS as error:
                _fail(f"could not read current subscriptions from {_device_label(rx_device)}: {error}")
            refreshed_devices.add(id(rx_device))

        _, rx_channel = resolve_channel(rx_device, rx_reference)
        if subscription_sources(rx_device, [rx_channel.number])[rx_channel.number] is None:
            _fail(f"RX channel '{rx_reference.identifier}' on {rx_device.name} is not subscribed")

        entry = device_removals.setdefault(id(rx_device), {"device": rx_device, "channels": []})
        if all(existing.number != rx_channel.number for existing in entry["channels"]):
            entry["channels"].append(rx_channel)
    return device_removals


async def run_subscription_remove(application, devices, rx: list[str] | None, all_channels: bool) -> None:
    if all_channels:
        device_removals = await _removals_for_all(devices)
    else:
        device_removals = await _removals_for_channels(devices, rx or [])

    failures = 0
    for entry in device_removals.values():
        rx_device = entry["device"]
        channels = entry["channels"]
        if not channels:
            continue

        try:
            result = await reconcile_receiver_subscriptions(
                application, rx_device, {channel.number: None for channel in channels}
            )
        except MUTATION_ERRORS as error:
            failures += 1
            typer.echo(
                f"{icon('fail')}FAILED to request subscription removal on {rx_device.name}: {error}",
                err=True,
            )
            continue

        for channel in channels:
            channel_name = channel.friendly_name or channel.name
            label = f"{channel_name}@{rx_device.name}"

            if channel.number in result.failures:
                failures += 1
                typer.echo(f"{icon('fail')}FAILED {label}: {result.failures[channel.number]}", err=True)
            elif channel.number in result.unchanged:
                typer.echo(f"UNCHANGED {label} (already unsubscribed)")
            else:
                typer.echo(f"{icon('remove')}Removed: {label} (verified)")

    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


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
    run_command(run_subscription_remove, rx, all_channels)
