from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import NoReturn, Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.services.notification import (
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
)

from netaudio._common import (
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    readback_after_notification,
    send_and_wait_for_notification,
)
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices, find_channel, find_device, parse_qualified_name, sort_devices
from netaudio._exit_codes import ExitCode
from netaudio.icons import icon

app = typer.Typer(help="Manage audio subscriptions.", no_args_is_help=True)


@dataclass(frozen=True)
class SubscriptionReconciliationResult:
    unchanged: dict[int, tuple[str, str] | None]
    verified: dict[int, tuple[str, str] | None]
    failures: dict[int, str]


def _fail(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _device_label(device) -> str:
    return (
        getattr(device, "name", None) or getattr(device, "server_name", None) or str(getattr(device, "ipv4", "device"))
    )


def _channel_by_number(channels, channel_number):
    channels = channels or {}
    channel = channels.get(channel_number)
    if channel is not None:
        return channel
    return next(
        (candidate for candidate in channels.values() if candidate.number == channel_number),
        None,
    )


def _subscription_signature(device, channel_number):
    channel = _channel_by_number(device.rx_channels, channel_number)
    if channel is None:
        raise RuntimeError(f"RX channel {channel_number} was unavailable during readback")
    subscriptions = getattr(device, "subscriptions", None) or []

    for subscription in subscriptions:
        if getattr(subscription, "_netaudio_rx_channel_number", None) != channel_number:
            continue
        if subscription.tx_channel_name and subscription.tx_device_name:
            return subscription.tx_channel_name, subscription.tx_device_name
        return None

    rx_names = {name for name in (channel.name, channel.friendly_name) if name}
    for subscription in subscriptions:
        if subscription.rx_channel_name not in rx_names:
            continue
        if subscription.tx_channel_name and subscription.tx_device_name:
            return subscription.tx_channel_name, subscription.tx_device_name
    return None


def _index_fresh_subscriptions(device):
    channels = list((getattr(device, "rx_channels", None) or {}).values())
    subscriptions = getattr(device, "subscriptions", None) or []
    if len(channels) != len(subscriptions):
        return
    for channel, subscription in zip(channels, subscriptions):
        subscription._netaudio_rx_channel_number = channel.number


async def _read_subscription_signatures(device, channel_numbers):
    await device.get_rx_channels()
    _index_fresh_subscriptions(device)
    return {channel_number: _subscription_signature(device, channel_number) for channel_number in channel_numbers}


async def _verify_subscriptions(device, expected):
    channel_numbers = tuple(expected)
    return await readback_after_notification(
        lambda: _read_subscription_signatures(device, channel_numbers),
        expected,
    )


async def _send_subscription_change(send, packet, device, arc_port):
    async with device.topology_mutation_lock:
        return await send_and_wait_for_notification(
            send,
            packet,
            device.ipv4,
            arc_port,
            (
                NOTIFICATION_RX_CHANNEL_CHANGE,
                NOTIFICATION_RX_FLOW_CHANGE,
                NOTIFICATION_ROUTING_DEVICE_CHANGE,
            ),
            expect_response=False,
        )


async def reconcile_receiver_subscriptions(
    send,
    device,
    desired_sources: dict[int, tuple[str, str] | None],
) -> SubscriptionReconciliationResult:
    await device.get_rx_channels()
    _index_fresh_subscriptions(device)

    current_sources = {
        receiver_channel_number: _subscription_signature(device, receiver_channel_number)
        for receiver_channel_number in desired_sources
    }
    unchanged = {
        receiver_channel_number: desired_source
        for receiver_channel_number, desired_source in desired_sources.items()
        if current_sources[receiver_channel_number] == desired_source
    }
    pending = {
        receiver_channel_number: desired_source
        for receiver_channel_number, desired_source in desired_sources.items()
        if current_sources[receiver_channel_number] != desired_source
    }
    verified: dict[int, tuple[str, str] | None] = {}
    failures: dict[int, str] = {}
    commands = DanteDeviceCommands()
    arc_port = _get_arc_port(device)

    removals = [
        receiver_channel_number for receiver_channel_number, desired_source in pending.items() if desired_source is None
    ]
    additions = [
        (receiver_channel_number, desired_source)
        for receiver_channel_number, desired_source in pending.items()
        if desired_source is not None
    ]

    for batch_start in range(0, len(removals), 16):
        batch = removals[batch_start : batch_start + 16]
        try:
            packet, _ = commands.command_remove_subscriptions(batch)
            await _send_subscription_change(send, packet, device, arc_port)
        except Exception as exception:
            for receiver_channel_number in batch:
                failures[receiver_channel_number] = f"request failed: {exception}"
            continue

        expected = {receiver_channel_number: None for receiver_channel_number in batch}
        readback = await _verify_subscriptions(device, expected)
        observed = readback.observed if isinstance(readback.observed, dict) else {}
        for receiver_channel_number in batch:
            if (
                readback.observed_available
                and receiver_channel_number in observed
                and observed[receiver_channel_number] is None
            ):
                verified[receiver_channel_number] = None
            elif readback.observed_available:
                failures[receiver_channel_number] = f"fresh readback reports {observed.get(receiver_channel_number)!r}"
            else:
                failures[receiver_channel_number] = f"fresh readback unavailable: {readback.error}"

    for batch_start in range(0, len(additions), 16):
        batch = additions[batch_start : batch_start + 16]
        records = [
            (receiver_channel_number, desired_source[0], desired_source[1])
            for receiver_channel_number, desired_source in batch
        ]
        try:
            packet, _ = commands.command_add_subscriptions(records)
            await _send_subscription_change(send, packet, device, arc_port)
        except Exception as exception:
            for receiver_channel_number, _ in batch:
                failures[receiver_channel_number] = f"request failed: {exception}"
            continue

        expected = dict(batch)
        readback = await _verify_subscriptions(device, expected)
        observed = readback.observed if isinstance(readback.observed, dict) else {}
        for receiver_channel_number, desired_source in batch:
            if observed.get(receiver_channel_number) == desired_source and readback.observed_available:
                verified[receiver_channel_number] = desired_source
            elif readback.observed_available:
                failures[receiver_channel_number] = f"fresh readback reports {observed.get(receiver_channel_number)!r}"
            else:
                failures[receiver_channel_number] = f"fresh readback unavailable: {readback.error}"

    return SubscriptionReconciliationResult(
        unchanged=unchanged,
        verified=verified,
        failures=failures,
    )


def _readback_failure(action: str, device, result) -> str:
    label = _device_label(device)
    if result.observed_available:
        return f"{action} sent to {label}, but fresh readback reports {result.observed!r}"
    detail = f": {result.error}" if result.error is not None else ""
    return f"{action} sent to {label}, but fresh readback was unavailable{detail}"


def _subscription_has_configured_source(subscription) -> bool:
    return bool(getattr(subscription, "has_configured_source", getattr(subscription, "tx_device_name", None)))


@app.command("list")
def subscription_list(
    include_unused: bool = typer.Option(
        False,
        "--all",
        help="Include unused receiver channels that have no configured source.",
    ),
):
    """List configured subscriptions."""

    async def _run():
        from netaudio.dante.const import (
            SUBSCRIPTION_STATUS_INFO,
            subscription_status_entry,
            subscription_status_label,
        )
        from netaudio.dante.device_serializer import DanteDeviceSerializer

        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        all_subscriptions = []

        for server_name, device in sort_devices(devices):
            for subscription in device.subscriptions:
                if include_unused or _subscription_has_configured_source(subscription):
                    all_subscriptions.append(subscription)

        if not all_subscriptions:
            typer.echo("No active subscriptions.")
            return

        from netaudio._common import ansi
        from netaudio.icons import SEVERITY_PRESENTATION, severity_icon

        def _status_label(code: int):
            if SUBSCRIPTION_STATUS_INFO.get(code) is None:
                return ""
            entry = subscription_status_entry(code)
            severity = str(entry["severity"])
            label = subscription_status_label(code)
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
                    _status_label(subscription.status_code),
                ]
            )

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())


@app.command()
def add(
    tx: str = typer.Option(..., "--tx", help="TX source: channel@device (single) or device (bulk 1:1)."),
    rx: str = typer.Option(..., "--rx", help="RX destination: channel@device (single) or device (bulk 1:1)."),
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
    """Add subscriptions. Single: --tx channel@device --rx channel@device. Bulk: --tx device --rx device."""

    commands = DanteDeviceCommands()

    async def _run():
        if not tx or not rx:
            _fail("both --tx and --rx are required")
        if count < 0 or offset_tx < 0 or offset_rx < 0:
            _fail("--count and channel offsets must be nonnegative")

        is_single = "@" in tx and "@" in rx

        if is_single:
            if count or offset_tx or offset_rx:
                _fail("--count and channel offsets are only valid for bulk subscriptions")
            tx_channel_id, tx_device_id = parse_qualified_name(tx)
            rx_channel_id, rx_device_id = parse_qualified_name(rx)

            async with _command_context() as (devices, send):
                tx_device = find_device(devices, tx_device_id)
                if tx_device is None:
                    typer.echo(f"Error: TX device '{tx_device_id}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_device = find_device(devices, rx_device_id)
                if rx_device is None:
                    typer.echo(f"Error: RX device '{rx_device_id}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_channel = find_channel(tx_device, tx_channel_id, "tx")
                if tx_channel is None:
                    typer.echo(f"Error: TX channel '{tx_channel_id}' not found on {tx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_channel = find_channel(rx_device, rx_channel_id, "rx")
                if rx_channel is None:
                    typer.echo(f"Error: RX channel '{rx_channel_id}' not found on {rx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                tx_channel_name = tx_channel.friendly_name or tx_channel.name
                if not tx_channel_name or not tx_device.name:
                    _fail("the TX channel and device must have Dante names")
                packet, _ = commands.command_add_subscription(rx_channel.number, tx_channel_name, tx_device.name)
                arc_port = _get_arc_port(rx_device)
                try:
                    await _send_subscription_change(send, packet, rx_device, arc_port)
                except Exception as error:
                    _fail(f"could not request subscription: {error}")

                expected = {
                    rx_channel.number: (tx_channel_name, tx_device.name),
                }
                result = await _verify_subscriptions(rx_device, expected)
                if not result.matched:
                    _fail(_readback_failure("subscription change", rx_device, result))
                typer.echo(
                    f"{icon('add')}{rx_channel_id}@{rx_device.name} <- {tx_channel_id}@{tx_device.name} (verified)"
                )
        else:
            if "@" in tx or "@" in rx:
                _fail("both --tx and --rx must be channel@device or both must be device names")

            async with _command_context() as (devices, send):
                tx_device = find_device(devices, tx)
                if tx_device is None:
                    typer.echo(f"Error: TX device '{tx}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                rx_device = find_device(devices, rx)
                if rx_device is None:
                    typer.echo(f"Error: RX device '{rx}' not found.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                try:
                    await rx_device.get_rx_channels()
                except Exception as error:
                    _fail(
                        f"could not read current subscriptions from "
                        f"{_device_label(rx_device)} before making changes: {error}"
                    )
                _index_fresh_subscriptions(rx_device)

                tx_sorted = sorted(tx_device.tx_channels.values(), key=lambda channel: channel.number)
                rx_sorted = sorted(rx_device.rx_channels.values(), key=lambda channel: channel.number)

                if not tx_sorted:
                    typer.echo(f"Error: no TX channels on {tx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                if not rx_sorted:
                    typer.echo(f"Error: no RX channels on {rx_device.name}.", err=True)
                    raise typer.Exit(code=ExitCode.ERROR)

                if offset_tx >= len(tx_sorted):
                    _fail(f"--offset-tx {offset_tx} is outside the {len(tx_sorted)} TX channels on {tx_device.name}")
                if offset_rx >= len(rx_sorted):
                    _fail(f"--offset-rx {offset_rx} is outside the {len(rx_sorted)} RX channels on {rx_device.name}")

                available_pairs = min(
                    len(tx_sorted) - offset_tx,
                    len(rx_sorted) - offset_rx,
                )
                if count > available_pairs:
                    _fail(
                        f"--count {count} exceeds the {available_pairs} channel pairs available after applying offsets"
                    )
                pair_count = count or available_pairs
                pairs = list(
                    zip(
                        tx_sorted[offset_tx : offset_tx + pair_count],
                        rx_sorted[offset_rx : offset_rx + pair_count],
                    )
                )

                if not pairs:
                    _fail("no channel pairs are available to subscribe")
                if not tx_device.name:
                    _fail("the TX device must have a Dante name")

                modified_pairs = []
                for tx_channel, rx_channel in pairs:
                    tx_channel_name = tx_channel.friendly_name or tx_channel.name
                    if not tx_channel_name:
                        _fail(f"TX channel {tx_channel.number} has no Dante name")
                    expected_signature = (tx_channel_name, tx_device.name)
                    rx_channel_name = rx_channel.friendly_name or rx_channel.name
                    if _subscription_signature(rx_device, rx_channel.number) == expected_signature:
                        typer.echo(
                            f"UNCHANGED {rx_channel_name}@{rx_device.name} <- "
                            f"{tx_channel_name}@{tx_device.name} (already subscribed)"
                        )
                    else:
                        modified_pairs.append((tx_channel, rx_channel))

                if not modified_pairs:
                    return

                arc_port = _get_arc_port(rx_device)

                batch_size = 16
                failures = 0
                for batch_start in range(0, len(modified_pairs), batch_size):
                    batch = modified_pairs[batch_start : batch_start + batch_size]
                    subscriptions = []
                    expected = {}
                    for tx_channel, rx_channel in batch:
                        tx_channel_name = tx_channel.friendly_name or tx_channel.name
                        subscriptions.append((rx_channel.number, tx_channel_name, tx_device.name))
                        expected[rx_channel.number] = (tx_channel_name, tx_device.name)

                    try:
                        packet, _ = commands.command_add_subscriptions(subscriptions)
                        await _send_subscription_change(send, packet, rx_device, arc_port)
                    except Exception as error:
                        failures += len(batch)
                        for tx_channel, rx_channel in batch:
                            tx_channel_name = tx_channel.friendly_name or tx_channel.name
                            rx_channel_name = rx_channel.friendly_name or rx_channel.name
                            typer.echo(
                                f"{icon('fail')}FAILED {rx_channel_name}@{rx_device.name} <- {tx_channel_name}@{tx_device.name}: {error}",
                                err=True,
                            )
                        continue

                    result = await _verify_subscriptions(rx_device, expected)
                    observed = result.observed if isinstance(result.observed, dict) else {}
                    for tx_channel, rx_channel in batch:
                        tx_channel_name = tx_channel.friendly_name or tx_channel.name
                        rx_channel_name = rx_channel.friendly_name or rx_channel.name
                        expected_signature = expected[rx_channel.number]
                        if observed.get(rx_channel.number) == expected_signature:
                            typer.echo(
                                f"MODIFIED {rx_channel_name}@{rx_device.name} <- "
                                f"{tx_channel_name}@{tx_device.name} (verified)"
                            )
                            continue

                        failures += 1
                        if result.observed_available:
                            detail = repr(observed.get(rx_channel.number))
                        else:
                            detail = "unavailable"
                        typer.echo(
                            f"{icon('fail')}FAILED {rx_channel_name}@{rx_device.name} <- "
                            f"{tx_channel_name}@{tx_device.name}: fresh readback was {detail}",
                            err=True,
                        )

                if failures:
                    raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command()
def remove(
    rx: Optional[list[str]] = typer.Option(
        None,
        "--rx",
        help="RX channel@device to unsubscribe (repeatable).",
    ),
    all_channels: bool = typer.Option(
        False,
        "--all",
        help="Remove all subscriptions on devices selected by the global filters.",
    ),
):
    """Remove subscriptions from RX channels. Supports bulk removal."""

    commands = DanteDeviceCommands()

    async def _run():
        if all_channels and rx:
            _fail("use either specific --rx channels or --all, not both")
        if not all_channels and not rx:
            _fail("--rx is required unless --all is used")

        def get_subscribed_channels(device):
            return [
                channel
                for channel in sorted(
                    device.rx_channels.values(),
                    key=lambda candidate: candidate.number,
                )
                if _subscription_signature(device, channel.number) is not None
            ]

        async with _command_context() as (devices, send):
            device_removals: dict[int, dict] = {}
            if all_channels:
                selected = filter_devices(devices)
                if not selected:
                    _fail("no devices matched the global filters")
                for device in selected.values():
                    try:
                        await device.get_rx_channels()
                    except Exception as error:
                        _fail(f"could not read current subscriptions from {_device_label(device)}: {error}")
                    _index_fresh_subscriptions(device)
                    channels = get_subscribed_channels(device)
                    if not channels:
                        typer.echo(f"No active subscriptions on {_device_label(device)}.")
                        continue
                    device_removals[id(device)] = {
                        "device": device,
                        "channels": channels,
                    }
            else:
                refreshed_devices = set()
                for rx_spec in rx or []:
                    if "@" not in rx_spec:
                        _fail(
                            f"expected channel@device for --rx {rx_spec!r}; use global device "
                            "filters with --all to remove every subscription on a device"
                        )
                    rx_channel_id, rx_device_id = parse_qualified_name(rx_spec)
                    rx_device = find_device(devices, rx_device_id)
                    if rx_device is None:
                        _fail(f"RX device '{rx_device_id}' not found")

                    if id(rx_device) not in refreshed_devices:
                        try:
                            await rx_device.get_rx_channels()
                        except Exception as error:
                            _fail(f"could not read current subscriptions from {_device_label(rx_device)}: {error}")
                        _index_fresh_subscriptions(rx_device)
                        refreshed_devices.add(id(rx_device))

                    rx_channel = find_channel(rx_device, rx_channel_id, "rx")
                    if rx_channel is None:
                        _fail(f"RX channel '{rx_channel_id}' not found on {rx_device.name}")
                    if _subscription_signature(rx_device, rx_channel.number) is None:
                        _fail(f"RX channel '{rx_channel_id}' on {rx_device.name} is not subscribed")

                    entry = device_removals.setdefault(
                        id(rx_device),
                        {"device": rx_device, "channels": []},
                    )
                    if all(existing.number != rx_channel.number for existing in entry["channels"]):
                        entry["channels"].append(rx_channel)

            failures = 0
            for entry in device_removals.values():
                rx_device = entry["device"]
                channels = entry["channels"]
                if not channels:
                    continue

                channel_numbers = [channel.number for channel in channels]
                packet, _ = commands.command_remove_subscriptions(channel_numbers)
                arc_port = _get_arc_port(rx_device)
                try:
                    await _send_subscription_change(send, packet, rx_device, arc_port)
                except Exception as error:
                    failures += 1
                    typer.echo(
                        f"{icon('fail')}FAILED to request subscription removal on {rx_device.name}: {error}",
                        err=True,
                    )
                    continue

                expected = {channel_number: None for channel_number in channel_numbers}
                result = await _verify_subscriptions(rx_device, expected)
                if not result.matched:
                    failures += 1
                    typer.echo(
                        f"{icon('fail')}FAILED: {_readback_failure('subscription removal', rx_device, result)}",
                        err=True,
                    )
                    continue

                for channel in channels:
                    channel_name = channel.friendly_name or channel.name
                    typer.echo(f"{icon('remove')}Removed: {channel_name}@{rx_device.name} (verified)")

            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())
