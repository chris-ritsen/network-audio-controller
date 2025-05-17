import asyncio
import ipaddress
import json
import socket
from enum import Enum

import typer
from typing_extensions import Annotated

from netaudio.dante.browser import DanteBrowser
from netaudio.dante.device import DanteDevice

from .._utils import filter_devices_util, get_host_by_name_util
from ..device import get_target_device

app = typer.Typer(
    name="config", help="Configure Dante device settings.", no_args_is_help=True
)


class ChannelTypeEnum(str, Enum):
    rx = "rx"
    tx = "tx"


@app.command("set-name", help="Set the device name or a specific channel name.")
async def set_name(
    new_name: Annotated[str, typer.Argument(help="The new name to set.")],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
    channel_number: Annotated[
        int | None, typer.Option(help="Specify a channel number to rename.")
    ] = None,
    channel_type: Annotated[
        ChannelTypeEnum | None,
        typer.Option(
            case_sensitive=False,
            help="Specify channel type (rx/tx) if renaming a channel.",
        ),
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)
    if not target_device:
        raise typer.Exit(code=1)

    if len(new_name) > 31:
        typer.secho(
            f"Warning: New name '{new_name}' is longer than 31 characters and will be truncated.",
            fg=typer.colors.YELLOW,
        )
        new_name = new_name[:31]

    if channel_number is not None and channel_type is not None:
        typer.echo(
            f"Setting {channel_type.value} channel {channel_number} of device '{target_device.name}' to '{new_name}'..."
        )
        if hasattr(target_device, "set_channel_name") and asyncio.iscoroutinefunction(
            target_device.set_channel_name
        ):
            await target_device.set_channel_name(
                channel_type.value, channel_number, new_name
            )
        else:
            typer.echo(
                "Error: Device object does not support 'set_channel_name' or it's not async.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.secho(
            f"Successfully set {channel_type.value} channel {channel_number} name to '{new_name}'.",
            fg=typer.colors.GREEN,
        )
    elif channel_number is None and channel_type is None:
        typer.echo(
            f"Setting device name for '{target_device.name}' ({target_device.ipv4}) to '{new_name}'..."
        )
        if hasattr(target_device, "set_name") and asyncio.iscoroutinefunction(
            target_device.set_name
        ):
            await target_device.set_name(new_name)
        else:
            typer.echo(
                "Error: Device object does not support 'set_name' or it's not async.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.secho(
            f"Successfully set device name to '{new_name}'. Note: The device may re-advertise under the new name.",
            fg=typer.colors.GREEN,
        )
    else:
        typer.echo(
            "Error: To set a channel name, both --channel-number and --channel-type are required.",
            err=True,
        )
        typer.echo(
            "To set a device name, do not use --channel-number or --channel-type.",
            err=True,
        )
        raise typer.Exit(code=1)


@app.command(
    "reset-name",
    help="Reset the device name or a specific channel name to factory defaults.",
)
async def reset_name(
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
    channel_number: Annotated[
        int | None, typer.Option(help="Specify a channel number to reset its name.")
    ] = None,
    channel_type: Annotated[
        ChannelTypeEnum | None,
        typer.Option(
            case_sensitive=False,
            help="Specify channel type (rx/tx) if resetting a channel name.",
        ),
    ] = None,
    device: Annotated[
        bool,
        typer.Option(
            help="Reset the device name (default if no channel options specified)."
        ),
    ] = False,
    channel: Annotated[
        bool,
        typer.Option(
            help="Reset a specific channel's name (requires --channel-number and --channel-type)."
        ),
    ] = False,
):
    target_device = await get_target_device(device_name, device_host)
    if not target_device:
        raise typer.Exit(code=1)

    is_device_reset = device or not channel
    is_channel_reset = channel

    if is_device_reset and is_channel_reset:
        typer.echo(
            "Error: Cannot reset both device name and channel name in a single command. Use separate commands or flags.",
            err=True,
        )
        raise typer.Exit(code=1)

    if is_channel_reset:
        if channel_number is None or channel_type is None:
            typer.echo(
                "Error: For channel name reset, --channel-number and --channel-type are required along with --channel flag.",
                err=True,
            )
            raise typer.Exit(code=1)

        typer.echo(
            f"Resetting {channel_type.value} channel {channel_number} name for device '{target_device.name}'..."
        )
        if hasattr(target_device, "reset_channel_name") and asyncio.iscoroutinefunction(
            target_device.reset_channel_name
        ):
            await target_device.reset_channel_name(channel_type.value, channel_number)
        else:
            typer.echo(
                "Error: Device object does not support 'reset_channel_name' or it's not async.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.secho(
            f"Successfully reset {channel_type.value} channel {channel_number} name.",
            fg=typer.colors.GREEN,
        )

    elif is_device_reset:
        if channel_number is not None or channel_type is not None:
            typer.echo(
                "Error: --channel-number and --channel-type should not be used when resetting device name. Did you mean to use --channel flag?",
                err=True,
            )
            raise typer.Exit(code=1)

        typer.echo(
            f"Resetting device name for '{target_device.name}' ({target_device.ipv4})..."
        )
        if hasattr(target_device, "reset_name") and asyncio.iscoroutinefunction(
            target_device.reset_name
        ):
            await target_device.reset_name()
        else:
            typer.echo(
                "Error: Device object does not support 'reset_name' or it's not async.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.secho(
            f"Successfully reset device name. The device may re-advertise under its default name.",
            fg=typer.colors.GREEN,
        )
    else:
        typer.echo(
            "Error: Specify whether to reset the device name (e.g. --device) or a channel name (e.g. --channel).",
            err=True,
        )
        raise typer.Exit(code=1)


class EncodingEnum(str, Enum):
    PCM16 = "16"
    PCM24 = "24"
    PCM32 = "32"


@app.command("set-encoding", help="Set the audio encoding for the device.")
async def set_encoding(
    encoding: Annotated[
        EncodingEnum,
        typer.Argument(
            case_sensitive=False, help="Audio encoding (16, 24, or 32 bit)."
        ),
    ],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)
    if not target_device:
        raise typer.Exit(code=1)

    encoding_value = int(encoding.value)

    typer.echo(
        f"Setting audio encoding for device '{target_device.name}' to {encoding_value}-bit PCM..."
    )
    if hasattr(target_device, "set_encoding") and asyncio.iscoroutinefunction(
        target_device.set_encoding
    ):
        await target_device.set_encoding(encoding_value)
    else:
        typer.echo(
            "Error: Device object does not support 'set_encoding' or it's not async.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.secho(
        f"Successfully set encoding to {encoding_value}-bit PCM for '{target_device.name}'.",
        fg=typer.colors.GREEN,
    )


class SampleRateEnum(str, Enum):
    SR44100 = "44100"
    SR48000 = "48000"
    SR88200 = "88200"
    SR96000 = "96000"
    SR176400 = "176400"
    SR192000 = "192000"


@app.command("set-sample-rate", help="Set the sample rate for the device.")
async def set_sample_rate(
    sample_rate: Annotated[
        SampleRateEnum,
        typer.Argument(
            case_sensitive=False, help="Sample rate (e.g., 44100, 48000, 96000)."
        ),
    ],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)

    if not target_device:
        raise typer.Exit(code=1)

    rate_value = int(sample_rate.value)

    typer.echo(
        f"Setting sample rate for device '{target_device.name}' to {rate_value} Hz..."
    )

    if hasattr(target_device, "set_sample_rate") and asyncio.iscoroutinefunction(
        target_device.set_sample_rate
    ):
        await target_device.set_sample_rate(rate_value)
    else:
        typer.echo(
            "Error: Device object does not support 'set_sample_rate' or it's not async.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.secho(
        f"Successfully set sample rate to {rate_value} Hz for '{target_device.name}'.",
        fg=typer.colors.GREEN,
    )


@app.command("set-latency", help="Set the device latency in milliseconds.")
async def set_latency(
    latency_ms: Annotated[
        float,
        typer.Argument(
            help="Device latency in milliseconds (e.g., 0.25, 0.5, 1.0, 2.0, 4.0, 5.0)."
        ),
    ],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)
    if not target_device:
        raise typer.Exit(code=1)

    typer.echo(
        f"Setting latency for device '{target_device.name}' to {latency_ms} ms..."
    )

    if hasattr(target_device, "set_latency") and asyncio.iscoroutinefunction(
        target_device.set_latency
    ):
        await target_device.set_latency(latency_ms)
    else:
        typer.echo(
            "Error: Device object does not support 'set_latency' or it's not async.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.secho(
        f"Successfully set latency to {latency_ms} ms for '{target_device.name}'.",
        fg=typer.colors.GREEN,
    )


class GainLevelEnum(str, Enum):
    PLUS_24dBu_MINUS_10dBV = "1"
    PLUS_4dBu = "2"
    PLUS_0dBu = "3"
    PLUS_0dBV = "4"
    MINUS_10dBV_PLUS_24dBu = "5"


@app.command(
    "set-gain",
    help="Set gain level for an AVIO device channel. Lower numbers = higher gain.",
)
async def set_gain(
    channel_number: Annotated[
        int, typer.Argument(help="Channel number to set gain for.")
    ],
    gain_level: Annotated[
        GainLevelEnum,
        typer.Argument(
            help="Gain level (1-5). 1 is highest gain (+24/+18dBu), 5 is lowest (-10dBV). Consult device spec for exact values."
        ),
    ],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)

    if not target_device:
        raise typer.Exit(code=1)

    gain_level_value = int(gain_level.value)

    device_type_str = None
    model_id = getattr(target_device, "model_id", None)

    if model_id in ["DAI1", "DAI2"]:
        device_type_str = "input"
    elif model_id in ["DAO1", "DAO2"]:
        device_type_str = "output"

    if not device_type_str:
        typer.echo(
            f"Error: Device '{target_device.name}' (Model: {model_id}) does not appear to be a supported AVIO device for gain control or model ID is unknown.",
            err=True,
        )
        raise typer.Exit(code=1)

    typer.echo(
        f"Setting gain for {device_type_str} channel {channel_number} on device '{target_device.name}' to level {gain_level_value}..."
    )

    if hasattr(target_device, "set_gain_level") and asyncio.iscoroutinefunction(
        target_device.set_gain_level
    ):
        await target_device.set_gain_level(
            channel_number, gain_level_value, device_type_str
        )
    else:
        typer.echo(
            "Error: Device object does not support 'set_gain_level' or it's not async.",
            err=True,
        )
        raise typer.Exit(code=1)


@app.command("enable-aes67", help="Enable or disable AES67 mode on the device.")
async def enable_aes67(
    enable: Annotated[
        bool, typer.Argument(help="Set to true to enable AES67, false to disable.")
    ],
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    target_device = await get_target_device(device_name, device_host)

    if not target_device:
        raise typer.Exit(code=1)

    action = "Enabling" if enable else "Disabling"
    typer.echo(f"{action} AES67 mode on device '{target_device.name}'...")

    if hasattr(target_device, "enable_aes67") and asyncio.iscoroutinefunction(
        target_device.enable_aes67
    ):
        await target_device.enable_aes67(enable)
    else:
        typer.echo(
            "Error: Device object does not support 'enable_aes67' or it's not async.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.secho(
        f"Successfully {action.lower()}d AES67 mode on '{target_device.name}'.",
        fg=typer.colors.GREEN,
    )
