from __future__ import annotations

import asyncio
from datetime import datetime
from typing import NoReturn, Optional
from urllib.parse import quote

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import daemon_devices
from netaudio.cli_support.output import output_single, output_table, structured_output_selected
from netaudio.daemon.client import DaemonRefusal, ask_daemon, call_daemon

app = typer.Typer(help="Shure wireless device control.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
device_app = typer.Typer(help="Shure device commands.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
channel_app = typer.Typer(help="Shure channel commands.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
app.add_typer(device_app, name="device")
app.add_typer(channel_app, name="channel")

CORRELATION_SECTION = "[shure.correlations]"
DEVICE_HELP = "Shure device name, IP address or MAC address. Optional when only one Shure device is known."


def _fail(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _text(value) -> str:
    return "" if value is None else str(value)


def _control(record: dict) -> str:
    status = record.get("control_status") or {}
    return ": ".join(str(part) for part in (status.get("state"), status.get("error")) if part)


def _local_time(value: str | None) -> str:
    return datetime.fromisoformat(value).astimezone().strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _frequency(value) -> str:
    return f"{value / 1000:.3f} MHz" if value else ""


def _battery(transmitter: dict) -> str:
    if transmitter.get("battery_charge_percent") is not None:
        return f"{transmitter['battery_charge_percent']}%"
    if transmitter.get("battery_hours") is not None:
        return f"{transmitter['battery_hours']} h"
    return ""


def _flag(value) -> str:
    return {True: "yes", False: "no"}.get(value, "")


async def _device(selector: str | None) -> dict:
    if selector is not None:
        return await call_daemon("GET", f"/shure/devices/{quote(selector, safe='')}")
    devices = await call_daemon("GET", "/shure/devices")
    if len(devices) != 1:
        _fail(
            "no Shure devices are known"
            if not devices
            else "several Shure devices are known; name one by device name, IP address or MAC address"
        )
    return next(iter(devices.values()))


async def run_device_list() -> None:
    devices = await call_daemon("GET", "/shure/devices")
    rows = [
        [
            _text(record.get("name")),
            _text(record.get("model")),
            _text(record.get("device_type")),
            _text(record.get("ip")),
            _text(record.get("mac")),
            _text(record.get("firmware_version")),
            "online" if record.get("online") else "offline",
            _control(record),
        ]
        for record in sorted(devices.values(), key=lambda record: _text(record.get("name")).lower())
    ]
    output_table(
        ["Name", "Model", "Type", "IP Address", "MAC Address", "Firmware", "Status", "Control"],
        rows,
        json_data=devices,
        empty_message="No Shure devices found on the network.",
    )


@device_app.command("list", help="List discovered Shure devices.")
def shure_device_list():
    asyncio.run(run_device_list())


async def run_device_show(selector: str | None) -> None:
    record = await _device(selector)
    fields = (
        ("Name", record.get("name")),
        ("Model", record.get("model")),
        ("Type", record.get("device_type")),
        ("IP Address", record.get("ip")),
        ("MAC Address", record.get("mac")),
        ("Firmware", record.get("firmware_version")),
        ("RF Band", record.get("rf_band")),
        ("Transmission Mode", record.get("transmission_mode")),
        ("Quadversity Mode", record.get("quadversity_mode")),
        ("Encryption Mode", record.get("encryption_mode")),
        ("Status", "online" if record.get("online") else "offline"),
        ("Control", _control(record)),
        ("Last Seen", _local_time(record.get("last_seen"))),
    )
    output_table(
        ["Field", "Value"],
        [[name, str(value)] for name, value in fields if value not in (None, "")],
        json_data=record,
    )


@device_app.command("show", help="Show details for one Shure device.")
def shure_device_show(device: Optional[str] = typer.Argument(None, help=DEVICE_HELP)):
    asyncio.run(run_device_show(device))


def _channel_row(record: dict, number: str, channel: dict) -> list[str]:
    transmitter = channel.get("transmitter") or {}
    return [
        _text(record.get("name")),
        number,
        _text(channel.get("name")),
        _frequency(channel.get("frequency")),
        _text(channel.get("group_channel")),
        _flag(channel.get("active")),
        _text(transmitter.get("model")),
        _battery(transmitter),
        _text(transmitter.get("mute_status")),
        _text(channel.get("audio_gain")),
        _flag(channel.get("audio_mute")),
        _text(channel.get("audio_level_rms")),
        _text(channel.get("signal_quality")),
        _flag(channel.get("rf_mute")),
        _text(channel.get("audio_in_level_l")),
        _text(channel.get("audio_in_level_r")),
    ]


async def run_channel_list(selector: str | None) -> None:
    if selector is None:
        records = list((await call_daemon("GET", "/shure/devices")).values())
    else:
        records = [await _device(selector)]
    records.sort(key=lambda record: _text(record.get("name")).lower())
    output_table(
        [
            "Device",
            "Channel",
            "Name",
            "Frequency",
            "Group/Channel",
            "Active",
            "Transmitter",
            "Battery",
            "Transmitter Mute",
            "Gain",
            "Muted",
            "RMS Level",
            "Quality",
            "RF Muted",
            "Input Left",
            "Input Right",
        ],
        [
            _channel_row(record, number, channel)
            for record in records
            for number, channel in sorted(record["channels"].items(), key=lambda item: int(item[0]))
        ],
        json_data={record["mac"]: record["channels"] for record in records},
        omit_empty_columns=True,
        empty_message="No Shure channels found.",
    )


@channel_app.command("list", help="List channels on Shure devices.")
def shure_channel_list(
    device: Optional[str] = typer.Argument(None, help="Shure device name, IP address or MAC address."),
):
    asyncio.run(run_channel_list(device))


async def run_get(key: str, selector: str | None, channel: int | None) -> None:
    record = await _device(selector)
    key = key.upper()
    if channel is None:
        properties = record["properties"]
        if key not in properties and any(key in entry["properties"] for entry in record["channels"].values()):
            _fail(f"{key} is a channel key; --channel is required.")
    else:
        entry = record["channels"].get(str(channel))
        if entry is None:
            _fail(f"channel must be one of {', '.join(sorted(record['channels'], key=int))}")
        properties = entry["properties"]
    if key not in properties:
        _fail(f"{record.get('name') or record['mac']} has not reported {key}")
    output_single(properties[key])


@app.command("get", help="Show a value a Shure device has reported.")
def shure_get(
    key: str = typer.Argument(..., help="Key to show (e.g. CHAN_NAME, MODEL)."),
    device: Optional[str] = typer.Argument(None, help=DEVICE_HELP),
    channel: Optional[int] = typer.Option(None, "--channel", "-c", min=1, help="Channel number."),
):
    asyncio.run(run_get(key, device, channel))


async def run_set(key: str, value: str, selector: str | None, channel: int | None) -> None:
    record = await _device(selector)
    request = {"device": record["mac"], "setting": key, "value": value}
    if channel is not None:
        request["channel"] = channel
    status, result = await ask_daemon("POST", "/shure/set", request)
    if structured_output_selected() and isinstance(result, dict):
        output_single(result)
    if status != 200:
        raise DaemonRefusal(status, result)
    if not structured_output_selected():
        typer.echo(f"Set {key.upper()} to {result.get('reported')!r} (verified)")


@app.command("set", help="Set a value on a Shure device.")
def shure_set(
    key: str = typer.Argument(..., help="Key to set (e.g. CHAN_NAME, AUDIO_GAIN)."),
    value: str = typer.Argument(..., help="Value to set."),
    device: Optional[str] = typer.Argument(None, help=DEVICE_HELP),
    channel: Optional[int] = typer.Option(None, "--channel", "-c", min=1, help="Channel number."),
):
    asyncio.run(run_set(key, value, device, channel))


def _save_correlation(shure_mac: str, dante_mac: str) -> None:
    from netaudio.common.config_loader import default_config_path

    path = default_config_path()
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    entry = f'"{shure_mac}" = "{dante_mac}"'
    headers = [index for index, line in enumerate(lines) if line.strip() == CORRELATION_SECTION]
    if not headers:
        lines.extend(([""] if lines else []) + [CORRELATION_SECTION, entry])
    else:
        start = headers[0] + 1
        end = next((index for index in range(start, len(lines)) if lines[index].strip().startswith("[")), len(lines))
        existing = [
            index
            for index in range(start, end)
            if lines[index].partition("=")[0].strip().strip('"').lower() == shure_mac
        ]
        if existing:
            lines[existing[0]] = entry
        else:
            lines.insert(start, entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def run_associate(shure_selector: str, dante_selector: str) -> None:
    wireless = await _device(shure_selector)
    wanted = dante_selector.casefold()
    matches = [
        device
        for server_name, device in (await daemon_devices()).items()
        if device.mac_address
        and wanted
        in {
            _text(device.name).casefold(),
            server_name.casefold(),
            _text(device.ipv4).casefold(),
            device.mac_address.casefold(),
        }
    ]
    if len(matches) != 1:
        _fail(f"{dante_selector} did not match exactly one Dante device")
    _save_correlation(wireless["mac"].lower(), matches[0].mac_address.lower())
    typer.echo(f"Associated {wireless.get('name') or wireless['mac']} with {matches[0].name}")


@app.command("associate", help="Associate a Shure device with the Dante device that carries its audio.")
def shure_associate(
    shure_device: str = typer.Argument(..., metavar="SHURE", help="Shure device name, IP address or MAC address."),
    dante_device: str = typer.Argument(
        ..., metavar="DANTE", help="Dante device name, server name, IP address or MAC address."
    ),
):
    asyncio.run(run_associate(shure_device, dante_device))
