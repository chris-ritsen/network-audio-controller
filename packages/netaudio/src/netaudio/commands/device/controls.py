from __future__ import annotations

import json
from enum import Enum
from typing import Any, Optional

import typer

from netaudio.cli_support.execution import run_with_devices
from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.daemon.client import DaemonRefusal, ask_daemon

app = typer.Typer(help="Inspect and change analog, Bluetooth, video and serial settings.", no_args_is_help=True)


class Category(str, Enum):
    analog_level = "analog_level"
    bluetooth_identification = "bluetooth_identification"
    bluetooth_discovery = "bluetooth_discovery"
    bluetooth_pairing = "bluetooth_pairing"
    video_format = "video_format"
    codec_format = "codec_format"
    serial = "serial"
    bandwidth = "bandwidth"
    hdcp = "hdcp"


async def _run(devices, action: str, category: str = "", value=None, confirm: bool = False):
    [(server_name, _)] = select_device(filter_devices(devices))
    request: dict[str, Any] = {"device": server_name, "action": action}
    if category:
        request.update(category=category, requested=value, confirm_clear=confirm)
    status, result = await ask_daemon("POST", "/device-controls", request)
    if action == "inspect":
        if status != 200:
            raise DaemonRefusal(status, result)
        rows = [
            [
                name.replace("_", " "),
                json.dumps(observation["value"], ensure_ascii=False),
                "fresh" if observation["fresh"] else "unavailable",
            ]
            for name, observation in result.get("observations", {}).items()
        ]
        analog = result.get("analog") or {}
        labels = {choice["value"]: choice["label"] for choice in analog.get("choices") or []}
        for channel, level in enumerate(analog.get("levels") or [], 1):
            rows.append(
                [
                    f"Analog {analog.get('direction') or ''} channel {channel}",
                    labels.get(level, f"Unknown ({level})"),
                    "fresh",
                ]
            )
        output_table(["Setting", "Reported value", "State"], rows, json_data=result)
        return

    if action == "plan":
        if "action" not in (result or {}):
            raise DaemonRefusal(status, result)
        output_table(["Action", "Reason"], [[result["action"], result.get("reason") or ""]], json_data=result)
        return

    if "effective_state_confirmed" not in (result or {}):
        raise DaemonRefusal(status, result)
    output_table(
        ["Setting", "Result"],
        [
            [
                category.replace("_", " "),
                "Confirmed"
                if result["effective_state_confirmed"]
                else result.get("reason") or (result.get("plan") or {}).get("reason") or "Sent; readback unavailable",
            ]
        ],
        json_data=result,
    )
    if not result["effective_state_confirmed"]:
        raise typer.Exit(1)


def _settings(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Settings must be valid JSON: {exc.msg}") from exc


@app.command("inspect")
def inspect_controls():
    """Read the device's settings and supported choices."""
    run_with_devices(_run, "inspect")


@app.command()
def plan(
    category: Category = typer.Argument(..., help="Setting category to inspect or change."),
    settings: str = typer.Argument(..., help="Requested setting as JSON."),
):
    """Preview one setting without sending a change."""
    run_with_devices(_run, "plan", category.value, _settings(settings), False)


@app.command()
def apply(
    category: Category = typer.Argument(..., help="Setting category to inspect or change."),
    settings: str = typer.Argument(..., help="Requested setting as JSON."),
    confirm_clear: bool = typer.Option(False, help="Confirm clearing all remembered Bluetooth devices."),
):
    """Apply one setting once, then check its readback."""
    run_with_devices(_run, "apply", category.value, _settings(settings), confirm_clear)


@app.command("analog-level")
def analog_level(
    channel: int = typer.Argument(..., help="Reported analog channel, 1 or 2."),
    level: int = typer.Argument(..., help="Reference level enumeration, 1 through 5."),
    apply: bool = typer.Option(False, "--apply", help="Apply the change; otherwise preview it."),
):
    """Set analog reference level 1–5 on a reported channel."""
    run_with_devices(_run, "apply" if apply else "plan", "analog_level", {"channel": channel, "level": level})


@app.command("bluetooth-name")
def bluetooth_name(
    name: Optional[str] = typer.Option(None, help="Custom Bluetooth name, up to 32 characters."),
    device_name: bool = typer.Option(False, help="Use the Dante device name."),
    apply: bool = typer.Option(False, help="Apply; otherwise preview."),
):
    """Use a custom Bluetooth name or --device-name."""
    if (name is None) == (not device_name):
        raise typer.BadParameter("Specify a name or --device-name.")
    run_with_devices(
        _run,
        "apply" if apply else "plan",
        "bluetooth_identification",
        {"name_source": 1 if device_name else 2, "custom_name": name or ""},
    )


@app.command("bluetooth-discovery")
def bluetooth_discovery(
    enabled: bool = typer.Option(..., "--on/--off", help="Enable or disable Bluetooth discoverability."),
    apply: bool = typer.Option(False, help="Apply; otherwise preview."),
):
    """Allow or prevent Bluetooth discovery."""
    run_with_devices(_run, "apply" if apply else "plan", "bluetooth_discovery", enabled)


@app.command("clear-pairing")
def clear_pairing(confirm: bool = typer.Option(False, "--confirm", help="Clear all remembered Bluetooth devices.")):
    """Forget all paired Bluetooth devices."""
    if not confirm:
        raise typer.BadParameter("Use --confirm to clear the remembered pairing list.")
    run_with_devices(_run, "apply", "bluetooth_pairing", "clear", True)


@app.command("bandwidth")
def bandwidth(
    target: int = typer.Argument(..., help="Target Mbit/s, or 0 to disable user control."),
    apply: bool = typer.Option(False, help="Apply; otherwise preview."),
):
    """Set transmitter bandwidth within the device's reported bounds."""
    run_with_devices(_run, "apply" if apply else "plan", "bandwidth", {"target": target, "enabled": target != 0})


@app.command("serial")
def serial(
    baud: int = typer.Argument(..., help="Baud rate supported by the serial port."),
    data_bits: int = typer.Option(8, help="7 or 8 data bits."),
    parity: int = typer.Option(0, help="0 none, 1 even, 2 odd."),
    stop_bits: int = typer.Option(1, help="1 or 2 stop bits."),
    apply: bool = typer.Option(False, help="Apply; otherwise preview."),
):
    """Set the serial port format; sends no serial data."""
    run_with_devices(
        _run,
        "apply" if apply else "plan",
        "serial",
        {
            "baud_rate": baud,
            "data_bits": data_bits,
            "parity": parity,
            "stop_bits": stop_bits,
            "hardware_flow_control": 0,
            "software_flow_control": 0,
        },
    )
