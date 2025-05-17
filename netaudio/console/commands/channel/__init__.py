import asyncio
import ipaddress
import json
import socket

import typer
from typing_extensions import Annotated

from netaudio.common.app_config import settings as app_settings
from netaudio.common.mdns_cache import MdnsCache
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.channel import (
    DanteChannel,
)
from netaudio.dante.const import SERVICE_CMC
from netaudio.dante.device import DanteDevice

from .._utils import (
    CustomJSONEncoder,
    filter_devices_util,
)

app = typer.Typer(
    name="channel", help="Control and list device channels.", no_args_is_help=True
)


async def fetch_and_prepare_channel_data(
    device_name_filter: str | None, device_host_filter: str | None
):
    if app_settings.refresh:
        mdns_cache = MdnsCache()
        mdns_cache.clear()
        mdns_cache.close()

    dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)
    devices = await dante_browser.get_devices()

    if not isinstance(devices, dict):
        devices = {}

    for device_obj_for_controls in devices.values():
        if hasattr(
            device_obj_for_controls, "get_controls"
        ) and asyncio.iscoroutinefunction(device_obj_for_controls.get_controls):
            try:
                await device_obj_for_controls.get_controls()
            except Exception:
                pass
        elif hasattr(device_obj_for_controls, "get_controls"):
            try:
                device_obj_for_controls.get_controls()
            except Exception:
                pass

    devices_to_process = filter_devices_util(
        devices, name_filter=device_name_filter, host_filter=device_host_filter
    )

    return dict(
        sorted(
            devices_to_process.items(),
            key=lambda item: (
                item[1].name if hasattr(item[1], "name") and item[1].name else item[0]
            ),
        )
    )


@app.command(
    "list", help="List channels for specified devices, or all discoverable devices."
)
def list_channels_command(
    json_output: Annotated[
        bool, typer.Option("--json", help="Output as JSON.")
    ] = False,
    device_host: Annotated[
        str | None, typer.Option(help="Filter by device host name or IP address.")
    ] = None,
    device_name: Annotated[
        str | None, typer.Option(help="Filter by device name.")
    ] = None,
):
    """Lists Tx and Rx channels for Dante devices."""
    if device_name and device_host:
        typer.echo(
            "Error: Cannot use --device-name and --device-host simultaneously.",
            err=True,
        )
        raise typer.Exit(code=1)

    processed_devices = asyncio.run(
        fetch_and_prepare_channel_data(
            device_name_filter=device_name, device_host_filter=device_host
        )
    )

    if not processed_devices:
        typer.echo("No devices found or matching the filter.")
        raise typer.Exit()

    if json_output:
        output_data = {}
        for dev_id, device_obj in processed_devices.items():
            output_data[
                device_obj.name
                if hasattr(device_obj, "name") and device_obj.name
                else dev_id
            ] = {
                "receivers": {
                    k: v for k, v in getattr(device_obj, "rx_channels", {}).items()
                },
                "transmitters": {
                    k: v for k, v in getattr(device_obj, "tx_channels", {}).items()
                },
            }
        typer.echo(json.dumps(output_data, indent=2, cls=CustomJSONEncoder))
    else:
        first_device = True
        for _, device_obj in processed_devices.items():
            if not first_device:
                typer.echo("")
            first_device = False

            device_display_name = (
                device_obj.name
                if hasattr(device_obj, "name") and device_obj.name
                else _
            )
            typer.secho(f"{device_display_name}", fg=typer.colors.GREEN, bold=True)

            tx_channels = getattr(device_obj, "tx_channels", {})
            if tx_channels:
                typer.secho("  Tx Channels:", fg=typer.colors.CYAN)
                for _, channel in tx_channels.items():
                    typer.echo(f"    {channel}")

            rx_channels = getattr(device_obj, "rx_channels", {})
            if rx_channels:
                if tx_channels:
                    typer.echo("")
                typer.secho("  Rx Channels:", fg=typer.colors.BLUE)
                for _, channel in rx_channels.items():
                    typer.echo(f"    {channel}")
