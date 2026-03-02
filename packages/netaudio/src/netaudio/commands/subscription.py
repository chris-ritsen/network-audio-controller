from __future__ import annotations

import asyncio

import typer

from netaudio._common import (
    _discover,
    _populate_controls,
    filter_devices,
    find_channel,
    find_device,
    output_single,
    output_table,
    parse_qualified_name,
    sort_devices,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Manage audio subscriptions.", no_args_is_help=True)


def _compact_status(subscription) -> str:
    labels = subscription.status_text()
    if labels:
        return labels[0]
    return ""


@app.command("list")
def subscription_list():
    """List all active subscriptions."""

    async def _run():
        from netaudio.cli import OutputFormat, state

        devices = await _discover()
        await _populate_controls(devices)
        devices = filter_devices(devices)

        all_subscriptions = []

        for server_name, device in sort_devices(devices):
            for subscription in device.subscriptions:
                all_subscriptions.append(subscription)

        if not all_subscriptions:
            typer.echo("No active subscriptions.")
            return

        json_data = [subscription.to_json() for subscription in all_subscriptions]

        if state.output_format in (OutputFormat.json, OutputFormat.yaml):
            output_single(json_data)
            return

        if state.output_format in (OutputFormat.table, OutputFormat.pretty):
            headers = ["RX Channel", "RX Device", "TX Channel", "TX Device", "Status"]
            rows = []
            for subscription in all_subscriptions:
                rows.append([
                    subscription.rx_channel_name or "",
                    subscription.rx_device_name or "",
                    subscription.tx_channel_name or "",
                    subscription.tx_device_name or "",
                    _compact_status(subscription),
                ])
            output_table(headers, rows, json_data=json_data)
            return

        lines = []
        for subscription in all_subscriptions:
            rx = f"{subscription.rx_channel_name}@{subscription.rx_device_name}"
            status = _compact_status(subscription)

            if subscription.tx_channel_name and subscription.tx_device_name:
                tx = f"{subscription.tx_channel_name}@{subscription.tx_device_name}"
                lines.append(f"{rx} <- {tx} {status}")
            else:
                lines.append(f"{rx} {status}")

        typer.echo("\n".join(lines))

    asyncio.run(_run())


@app.command()
def add(
    tx: str = typer.Argument(help="TX source as channel@device."),
    rx: str = typer.Argument(help="RX destination as channel@device."),
):
    """Add a subscription (route audio from TX to RX)."""

    async def _run():
        tx_channel_id, tx_device_id = parse_qualified_name(tx)
        rx_channel_id, rx_device_id = parse_qualified_name(rx)

        devices = await _discover()
        await _populate_controls(devices)

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

        await rx_device.operations.add_subscription(rx_channel, tx_channel, tx_device)
        typer.echo(f"Subscribed: {rx_channel_id}@{rx_device.name} <- {tx_channel_id}@{tx_device.name}")

    asyncio.run(_run())


@app.command()
def remove(
    rx: str = typer.Argument(help="RX channel to unsubscribe as channel@device."),
):
    """Remove a subscription from an RX channel."""

    async def _run():
        rx_channel_id, rx_device_id = parse_qualified_name(rx)

        devices = await _discover()
        await _populate_controls(devices)

        rx_device = find_device(devices, rx_device_id)
        if rx_device is None:
            typer.echo(f"Error: RX device '{rx_device_id}' not found.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        rx_channel = find_channel(rx_device, rx_channel_id, "rx")
        if rx_channel is None:
            typer.echo(f"Error: RX channel '{rx_channel_id}' not found on {rx_device.name}.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        await rx_device.operations.remove_subscription(rx_channel)
        typer.echo(f"Removed subscription: {rx_channel_id}@{rx_device.name}")

    asyncio.run(_run())
