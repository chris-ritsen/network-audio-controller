from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import change_devices, run_with_devices
from netaudio.cli_support.output import output_single, output_table, output_value, structured_output_selected
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.daemon.client import DaemonRefusal, ask_daemon, call_daemon
from netaudio.dante.clock_config import format_clock_source_code, format_clock_subdomain
from netaudio.dante.sample_rate_pullup import format_supported_sample_rate_pullup_values, sample_rate_pullup_label

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)

top_app = typer.Typer(
    help="Manage netaudio configuration.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)


@top_app.command("edit")
def config_edit():
    """Open config.toml in $EDITOR."""
    from netaudio.common.config_loader import default_config_path

    config_path = default_config_path()

    if not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("")

    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL")
    if not editor:
        if sys.platform == "darwin":
            editor = "open -t"
        elif sys.platform == "win32":
            editor = "notepad"
        else:
            editor = "vi"

    try:
        subprocess.run([*editor.split(), str(config_path)], check=True)
    except FileNotFoundError:
        typer.echo(f"Error: editor not found: {editor}", err=True)
        raise typer.Exit(code=1)
    except subprocess.CalledProcessError as exception:
        typer.echo(f"Error: editor exited with code {exception.returncode}", err=True)
        raise typer.Exit(code=1)


@top_app.command("path")
def config_path():
    """Show the config file path."""
    from netaudio.common.config_loader import default_config_path

    if structured_output_selected():
        output_single({"path": str(default_config_path())})
        return
    typer.echo(str(default_config_path()))


SECRET_CONFIGURATION_KEY_MARKERS = ("key", "password", "secret", "token")


def _merge_configuration(base: dict, overlay: dict) -> dict:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_configuration(merged[key], value)
        else:
            merged[key] = value
    return merged


def _flatten_configuration(data: dict, prefix: str = "") -> list[tuple[str, object]]:
    flattened: list[tuple[str, object]] = []
    for key in sorted(data):
        value = data[key]
        qualified_key = f"{prefix}{key}"
        if isinstance(value, dict):
            flattened.extend(_flatten_configuration(value, f"{qualified_key}."))
        else:
            flattened.append((qualified_key, value))
    return flattened


def _format_configuration_value(key: str, value: object) -> str:
    leaf = key.rsplit(".", 1)[-1].lower()
    if any(marker in leaf for marker in SECRET_CONFIGURATION_KEY_MARKERS) and value not in (None, ""):
        return "(set; hidden)"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def effective_configuration(config_path) -> dict:
    from netaudio.common.config_loader import load_capture_profile, tomllib

    if tomllib is None:
        raise ValueError("TOML parser unavailable. Install 'tomli' or use Python 3.11+.")
    data = tomllib.loads(config_path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Config {config_path} must contain a TOML table.")
    top_level = {key: value for key, value in data.items() if key != "profiles"}
    if not isinstance(data.get("profiles"), dict):
        return top_level
    selected_profile, _ = load_capture_profile(None, None)
    if selected_profile is data:
        return top_level
    return _merge_configuration(top_level, selected_profile)


@top_app.command("show", help="Print the effective configuration as key = value lines.")
def config_show():
    from netaudio.common.config_loader import default_config_path

    config_path = default_config_path()
    structured = structured_output_selected()
    if not structured:
        typer.echo(f"path = {config_path}")
    if not config_path.exists():
        if structured:
            output_single({"path": str(config_path), "configuration": {}})
        else:
            typer.echo("(no configuration file; defaults apply)")
        return

    try:
        configuration = effective_configuration(config_path)
    except ValueError as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if structured:
        output_single(
            {
                "path": str(config_path),
                "configuration": {
                    key: _format_configuration_value(key, value) for key, value in _flatten_configuration(configuration)
                },
            }
        )
        return
    for key, value in _flatten_configuration(configuration):
        typer.echo(f"{key} = {_format_configuration_value(key, value)}")


def _targets(devices, all_devices: bool):
    return select_device(filter_devices(devices), allow_many=all_devices)


def _show(targets, all_devices: bool, title: str, key: str, value_of, text_of) -> None:
    if all_devices:
        output_table(
            ["Name", title],
            [[device.name or server_name, text_of(value_of(device))] for server_name, device in targets],
        )
        return
    value = value_of(targets[0][1])
    output_value(title, key, value, text_of(value))


def _switch(value: str) -> bool:
    if value.lower() not in ("on", "off"):
        typer.echo("Error: expected 'on' or 'off'.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    return value.lower() == "on"


def _state_label(value) -> str:
    if value is None:
        return "N/A"
    return "on" if value else "off"


def _number(value) -> str:
    return "unknown" if value is None else f"{value:g}"


async def run_sample_rate(devices, rate: int | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if rate is None:
        _show(
            targets,
            all_devices,
            "Sample rate",
            "sample_rate_hz",
            lambda device: device.sample_rate,
            lambda value: f"{value} Hz" if value else "N/A",
        )
        return

    rows = []
    results = {}
    failed = False
    for server_name, device in targets:
        label = device.name or server_name
        status, result = await ask_daemon("POST", "/set-sample-rate", {"device": server_name, "sample_rate": rate})
        results[server_name] = result
        if status != 200:
            failed = True
            rows.append([label, "Failed", str(DaemonRefusal(status, result))])
        elif result.get("changed"):
            rows.append([label, "Verified", f"{rate} Hz"])
        else:
            rows.append([label, "Unchanged", f"already {rate} Hz; no write sent"])
    output_table(["Device", "Result", "Detail"], rows, json_data=results)
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("sample-rate")
def sample_rate(
    rate: Optional[int] = typer.Argument(None, help="Sample rate in hertz."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the sample rate."""
    run_with_devices(run_sample_rate, rate, all_devices)


async def run_sample_rate_pullup(devices, selection: str | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if selection is None:
        if all_devices:
            output_table(
                ["Name", "Applied", "Requested", "Supported"],
                [
                    [
                        device.name or server_name,
                        sample_rate_pullup_label(device.sample_rate_pullup_raw_value),
                        sample_rate_pullup_label(device.requested_sample_rate_pullup_raw_value),
                        format_supported_sample_rate_pullup_values(device.supported_sample_rate_pullup_raw_values),
                    ]
                    for server_name, device in targets
                ],
            )
            return
        device = targets[0][1]
        if structured_output_selected():
            output_single(
                {
                    "applied": sample_rate_pullup_label(device.sample_rate_pullup_raw_value),
                    "applied_raw_value": device.sample_rate_pullup_raw_value,
                    "requested": sample_rate_pullup_label(device.requested_sample_rate_pullup_raw_value),
                    "requested_raw_value": device.requested_sample_rate_pullup_raw_value,
                    "supported": device.sample_rate_pullup_configuration_supported,
                    "supported_raw_values": device.supported_sample_rate_pullup_raw_values,
                }
            )
            return
        output_single(sample_rate_pullup_label(device.sample_rate_pullup_raw_value))
        return

    await change_devices(
        targets,
        "/set-sample-rate-pullup",
        {"value": selection},
        lambda label: f"Set sample-rate pull-up for {label}: {selection}",
    )


@app.command("sample-rate-pullup")
def sample_rate_pullup(
    selection: Optional[str] = typer.Argument(
        None,
        help="none, +4.1667%, +0.1%, -0.1%, -4.0%, or a raw integer.",
    ),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set sample-rate pull-up/down."""
    run_with_devices(run_sample_rate_pullup, selection, all_devices)


async def run_encoding(devices, bits: int | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if bits is None:
        _show(
            targets,
            all_devices,
            "Encoding",
            "encoding_bits",
            lambda device: device.encoding,
            lambda value: f"{value}-bit" if value is not None else "N/A",
        )
        return
    await change_devices(
        targets,
        "/set-encoding",
        {"encoding": bits},
        lambda label: f"Set encoding for {label}: {bits}-bit",
    )


@app.command()
def encoding(
    bits: Optional[int] = typer.Argument(None, help="Encoding bit depth."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the encoding bit depth."""
    run_with_devices(run_encoding, bits, all_devices)


def _latency_values(device) -> dict:
    return {
        "active_latency_ms": device.active_latency,
        "configured_latency_ms": device.configured_latency,
        "default_latency_ms": device.default_latency,
        "min_latency_ms": device.min_latency,
        "max_latency_ms": device.max_latency,
        "latency_options_ms": device.standard_latency_choices,
    }


def _latency_range(values: dict) -> str:
    if values["min_latency_ms"] is None or values["max_latency_ms"] is None:
        return "unknown"
    return f"{values['min_latency_ms']:g}-{values['max_latency_ms']:g}"


def _latency_choices(values: dict) -> str:
    choices = values["latency_options_ms"]
    if choices is None:
        return "unknown"
    return ", ".join(f"{choice:g}" for choice in choices) or "none"


async def run_latency(devices, value: float | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if value is None:
        readings = {server_name: _latency_values(device) for server_name, device in targets}
        if all_devices:
            output_table(
                ["Name", "Active (ms)", "Configured (ms)", "Default (ms)", "Reported range (ms)", "Choices (ms)"],
                [
                    [
                        device.name or server_name,
                        _number(readings[server_name]["active_latency_ms"]),
                        _number(readings[server_name]["configured_latency_ms"]),
                        _number(readings[server_name]["default_latency_ms"]),
                        _latency_range(readings[server_name]),
                        _latency_choices(readings[server_name]),
                    ]
                    for server_name, device in targets
                ],
                json_data={
                    server_name: {"name": device.name or server_name, **readings[server_name]}
                    for server_name, device in targets
                },
            )
            return
        values = readings[targets[0][0]]
        if structured_output_selected():
            output_single(values)
            return
        output_single(
            "\n".join(
                [
                    f"Active latency: {_number(values['active_latency_ms'])} ms",
                    f"Configured latency: {_number(values['configured_latency_ms'])} ms",
                    f"Default latency: {_number(values['default_latency_ms'])} ms",
                    f"Reported latency range: {_latency_range(values)} ms",
                    f"Latency options: {_latency_choices(values)} ms",
                ]
            )
        )
        return

    if not math.isfinite(value) or value < 0:
        typer.echo("Error: latency must be a finite, nonnegative number.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    await change_devices(
        targets,
        "/set-latency",
        {"latency": value},
        lambda label: f"Set configured latency for {label}: {value:g} ms",
    )


@app.command()
def latency(
    value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
    all_devices: bool = typer.Option(False, "--all", help="Read or apply to all devices."),
):
    """Get the complete device latency state or set and verify latency."""
    run_with_devices(run_latency, value, all_devices)


async def run_performance(devices, path: str, body: dict, all_devices: bool) -> None:
    rows = []
    results = {}
    failed = False
    for server_name, device in _targets(devices, all_devices):
        label = device.name or server_name
        status, result = await ask_daemon("POST", path, {"device": server_name, **body})
        results[server_name] = result
        state = (result or {}).get("state")
        if state is None:
            failed = True
            rows.append([label, "Failed", str(DaemonRefusal(status, result))])
            continue
        failed = failed or status not in (200, 202)
        rows.append([label, state.replace("_", " ").capitalize(), result.get("message", "")])
    output_table(["Device", "Result", "Detail"], rows, json_data=results)
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("receive-flow-performance")
def receive_flow_performance(
    latency_microseconds: int = typer.Argument(..., help="Receive-flow latency in microseconds."),
    frames_per_packet: int = typer.Argument(..., help="Receive-flow frames per packet."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all selected devices."),
):
    """Set and verify receive-flow latency and frames per packet."""
    run_with_devices(
        run_performance,
        "/set-receive-flow-performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
        all_devices,
    )


@app.command("transmit-flow-performance")
def transmit_flow_performance(
    latency_microseconds: int = typer.Argument(..., help="Transmit-flow latency in microseconds."),
    frames_per_packet: int = typer.Argument(..., help="Transmit-flow frames per packet."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all selected devices."),
):
    """Set and verify transmit-flow latency and frames per packet."""
    run_with_devices(
        run_performance,
        "/set-transmit-flow-performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
        all_devices,
    )


@app.command("unicast-performance")
def unicast_performance(
    latency_microseconds: int = typer.Argument(..., help="Unicast latency in microseconds."),
    frames_per_packet: int = typer.Argument(..., help="Unicast frames per packet."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all selected devices."),
):
    """Set and verify the unicast properties advertised by each device."""
    run_with_devices(
        run_performance,
        "/set-unicast-performance",
        {"latency_microseconds": latency_microseconds, "frames_per_packet": frames_per_packet},
        all_devices,
    )


@app.command("receive-flow-default-slots")
def receive_flow_default_slots(
    slots: int = typer.Argument(..., help="Default receive-flow slot count."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all selected devices."),
):
    """Set and verify the default receive-flow slot count."""
    run_with_devices(run_performance, "/set-receive-flow-default-slots", {"default_slots": slots}, all_devices)


@app.command("store-current-configuration")
def store_current_configuration(
    all_devices: bool = typer.Option(False, "--all", help="Request storage on all selected devices."),
):
    """Request storage without claiming persistence confirmation."""
    run_with_devices(run_performance, "/store-current-configuration", {}, all_devices)


def _aes67_reboot_required(device) -> bool:
    return (
        device.aes67_current is not None
        and device.aes67_configured is not None
        and device.aes67_current != device.aes67_configured
    )


def _show_aes67(targets, all_devices: bool) -> None:
    if all_devices:
        output_table(
            ["Name", "Supported", "Current", "Configured", "Multicast Prefix", "Reboot Required"],
            [
                [
                    device.name or server_name,
                    {None: "unknown", True: "yes", False: "no"}[device.aes67_configuration_supported],
                    _state_label(device.aes67_current),
                    _state_label(device.aes67_configured),
                    device.aes67_multicast_prefix or "",
                    "yes" if _aes67_reboot_required(device) else "no",
                ]
                for server_name, device in targets
            ],
        )
        return
    device = targets[0][1]
    if device.aes67_configuration_supported is False:
        output_single("unsupported")
        return
    if structured_output_selected():
        output_single(
            {
                "configured": _state_label(device.aes67_configured),
                "current": _state_label(device.aes67_current),
                "multicast_prefix": device.aes67_multicast_prefix,
                "reboot_required": _aes67_reboot_required(device),
                "configuration_supported": device.aes67_configuration_supported,
            }
        )
        return
    if device.aes67_multicast_prefix:
        typer.echo(f"multicast prefix: {device.aes67_multicast_prefix}")
    if _aes67_reboot_required(device):
        typer.echo(f"current: {_state_label(device.aes67_current)}")
        typer.echo(f"configured: {_state_label(device.aes67_configured)}")
        typer.echo("reboot required", err=True)
        return
    known = device.aes67_configured if device.aes67_current is None else device.aes67_current
    output_single(_state_label(known))


async def run_aes67(devices, enabled: str | None, multicast_prefix: str | None, all_devices: bool) -> None:
    is_enabled = None if enabled is None else _switch(enabled)
    targets = _targets(devices, all_devices)
    if multicast_prefix is not None:
        await change_devices(
            targets,
            "/set-aes67-multicast-prefix",
            {"prefix": multicast_prefix},
            lambda label: f"Set AES67 multicast prefix for {label}: {multicast_prefix}",
        )
    if is_enabled is not None:
        await change_devices(
            targets,
            "/set-aes67",
            {"enabled": is_enabled},
            lambda label: f"Set AES67 configured state for {label}: {_state_label(is_enabled)}",
        )
    if is_enabled is None and multicast_prefix is None:
        _show_aes67(targets, all_devices)


@app.command()
def aes67(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    multicast_prefix: Optional[str] = typer.Option(
        None,
        "--multicast-prefix",
        help="AES67 RTP multicast prefix, for example 239.69.0.0.",
    ),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set AES67 mode."""
    run_with_devices(run_aes67, enabled, multicast_prefix, all_devices)


async def run_preferred_leader(devices, enabled: str | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if enabled is None:
        _show(
            targets,
            all_devices,
            "Preferred leader",
            "preferred_leader",
            lambda device: device.preferred_leader,
            _state_label,
        )
        return
    preferred = _switch(enabled)
    await change_devices(
        targets,
        "/set-preferred-leader",
        {"preferred": preferred},
        lambda label: f"Preferred leader {_state_label(preferred)} confirmed for {label}.",
    )


@app.command("preferred-leader")
def preferred_leader(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set preferred leader mode."""
    run_with_devices(run_preferred_leader, enabled, all_devices)


async def run_clock_source(devices, selection: str | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if selection is None:
        _show(
            targets,
            all_devices,
            "Clock source",
            "clock_source",
            lambda device: format_clock_source_code(device.clock_source_code),
            str,
        )
        return
    try:
        source = int(selection, 0)
    except ValueError:
        typer.echo("Error: clock source must be a decimal or hexadecimal code.", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from None
    await change_devices(
        targets,
        "/set-clock-source",
        {"clock_source": source},
        lambda label: f"Set clock source for {label}: {format_clock_source_code(source)}",
    )


@app.command("clock-source")
def clock_source(
    selection: Optional[str] = typer.Argument(None, help="Raw source code in decimal or hexadecimal."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the device's raw clock-source code."""
    run_with_devices(run_clock_source, selection, all_devices)


async def run_clock_subdomain(devices, selection: str | None, all_devices: bool) -> None:
    targets = _targets(devices, all_devices)
    if selection is None:
        _show(
            targets,
            all_devices,
            "Clock subdomain",
            "clock_subdomain",
            lambda device: None if device.clock_subdomain is None else format_clock_subdomain(device.clock_subdomain),
            lambda value: "N/A" if value is None else value,
        )
        return
    await change_devices(
        targets,
        "/set-clock-subdomain",
        {"subdomain": selection},
        lambda label: f"Set clock subdomain for {label}: {selection}",
    )


@app.command("clock-subdomain")
def clock_subdomain(
    selection: Optional[str] = typer.Argument(
        None,
        help="ASCII name, hex:<bytes>, or unset.",
    ),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the PTP subdomain name."""
    run_with_devices(run_clock_subdomain, selection, all_devices)


INTERFACE_HEADERS = ["Name", "Interface", "State", "Mode", "IP Address", "Netmask", "Gateway", "DNS", "Reboot Required"]


def _interface_rows(label: str, interfaces: list) -> list[list[str]]:
    rows = []
    for entry in interfaces:
        for state, configuration in (("Active", entry), ("Configured", entry.get("configured"))):
            if configuration is None:
                continue
            rows.append(
                [
                    label,
                    (entry.get("interface") or "unknown").capitalize(),
                    state,
                    {"dynamic": "DHCP", "static": "Static"}.get(configuration.get("mode"), "Unknown"),
                    *[configuration.get(key) or "" for key in ("ip_address", "netmask", "gateway", "dns_server")],
                    "yes" if entry.get("reboot_required") else "no",
                ]
            )
    return rows


async def run_interface(devices, change: dict | None, all_devices: bool) -> None:
    rows = []
    results = {}
    failed = False
    for server_name, device in _targets(devices, all_devices or change is None):
        label = device.name or server_name
        try:
            if change is None:
                result = await call_daemon("GET", f"/interfaces/{server_name}")
            else:
                result = await call_daemon("POST", "/interface", {"device": server_name, **change})
                typer.echo(f"Configured {change['interface']} interface on {label}. No reboot sent.", err=True)
        except DaemonRefusal as refusal:
            failed = True
            typer.echo(f"Error: {label}: {refusal}", err=True)
            continue
        results[server_name] = result
        rows.extend(_interface_rows(label, result.get("interfaces") or []))
    output_table(INTERFACE_HEADERS, rows, json_data=results)
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("interface")
def interface(
    mode: Optional[str] = typer.Argument(None, help="dhcp or static"),
    ip_address: Optional[str] = typer.Option(None, "--ip", help="IP address (static only)."),
    netmask: Optional[str] = typer.Option(None, "--netmask", help="Subnet mask (static only)."),
    dns_server: Optional[str] = typer.Option(None, "--dns", help="DNS server (static only)."),
    gateway: Optional[str] = typer.Option(None, "--gateway", help="Gateway (static only)."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
    interface: str = typer.Option("primary", "--interface", help="Target primary or secondary interface."),
):
    """Read active/configured network settings or set and verify an interface."""
    change = None
    if mode is not None:
        addresses = {"ip": ip_address, "netmask": netmask, "dns": dns_server, "gateway": gateway}
        change = {
            "mode": mode,
            "interface": interface,
            **{key: value for key, value in addresses.items() if value is not None},
        }
    run_with_devices(run_interface, change, all_devices)


def _redundancy_label(mode) -> str:
    return {"switched": "Switched", "redundant": "Redundant", "split_redundant": "Split/Redundant"}.get(mode, "Unknown")


async def run_redundancy(devices, mode: str | None, all_devices: bool) -> None:
    rows = []
    results = {}
    failed = False
    for server_name, device in _targets(devices, all_devices):
        label = device.name or server_name
        try:
            if mode is None:
                result = await call_daemon("GET", f"/redundancy/{server_name}")
            else:
                result = await call_daemon("POST", "/redundancy", {"device": server_name, "mode": mode})
                typer.echo(f"Configured Dante redundancy on {label}. No reboot sent.", err=True)
        except DaemonRefusal as refusal:
            failed = True
            results[server_name] = refusal.data
            typer.echo(f"Error: {label}: {refusal}", err=True)
            continue
        results[server_name] = result
        status = result["redundancy"]
        availability = status["operation_availability"]
        rows.append(
            [
                label,
                {True: "supported", False: "unsupported", None: "unknown"}[status.get("advertised_support")],
                _redundancy_label(status.get("current_mode")),
                _redundancy_label(status.get("configured_mode")),
                ", ".join(_redundancy_label(choice.get("mode")) for choice in status.get("available_modes") or []),
                "yes" if availability["writable"] else ", ".join(availability["reasons"]),
                "yes" if status["reboot_required"] else "no",
            ]
        )
    output_table(
        ["Device", "Capability", "Active", "Configured", "Available Modes", "Writable", "Reboot Required"],
        rows,
        json_data=results,
    )
    if failed:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("redundancy")
def redundancy(
    mode: Optional[str] = typer.Argument(None, help="switched, redundant, or split_redundant"),
    all_devices: bool = typer.Option(False, "--all", help="Read or apply to all devices."),
):
    """Read or set and verify Dante Redundancy. Does not reboot devices."""
    run_with_devices(run_redundancy, mode, all_devices)


CLOCK_CONTROL_PROFILES = {"current": 1850, "legacy": 1844}


async def run_clock_configuration(devices, changes: dict, control_profile: int) -> None:
    [(server_name, device)] = select_device(filter_devices(devices))
    status, result = await ask_daemon(
        "POST",
        "/set-clock-configuration",
        {"device": server_name, "changes": changes, "control_profile": control_profile},
    )
    result = result if isinstance(result, dict) else {}
    confirmed = status == 200 and bool(result.get("effective_state_confirmed"))
    if structured_output_selected():
        output_single(result)
    elif confirmed:
        settings = ", ".join(key.replace("_", " ") for key in sorted(changes))
        typer.echo(f"Set clock settings for {device.name or server_name}: {settings} (confirmed)")
    else:
        typer.echo(f"Error: {result.get('error') or 'the device did not confirm the clock settings'}", err=True)
    if not confirmed:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("clock")
def clock_configuration(
    source: Optional[int] = typer.Option(
        None, "--source", help="0 internal, 1 external/BNC, 2 AES; device support required."
    ),
    preferred: Optional[bool] = typer.Option(
        None, "--preferred/--not-preferred", help="Enable or disable preferred leader."
    ),
    subdomain: Optional[str] = typer.Option(None, "--subdomain", help="PTPv1 subdomain name, at most 15 bytes."),
    global_unicast: Optional[bool] = typer.Option(
        None, "--global-unicast/--no-global-unicast", help="Enable or disable global unicast delay requests."
    ),
    ptpv1_unicast: Optional[bool] = typer.Option(
        None, "--ptpv1-unicast/--no-ptpv1-unicast", help="Enable or disable unicast delay requests on all PTPv1 ports."
    ),
    profile: str = typer.Option("current", "--profile", help="Clock serializer: current or legacy."),
    follower_only: Optional[bool] = typer.Option(None, "--follower-only/--no-follower-only"),
    domain: Optional[int] = typer.Option(None, "--ptpv2-domain", min=0, max=255),
    priority1: Optional[int] = typer.Option(None, "--priority1", min=0, max=255),
    priority2: Optional[int] = typer.Option(None, "--priority2", min=0, max=255),
    dscp: Optional[int] = typer.Option(None, "--multicast-dscp", min=0, max=63),
    settings: Optional[str] = typer.Option(
        None, "--settings", help="Typed clock changes as a JSON object, including per-port settings."
    ),
):
    """Apply clock settings together and confirm them with fresh readback."""
    if profile not in CLOCK_CONTROL_PROFILES:
        raise typer.BadParameter("Profile must be current or legacy.")
    changes = {
        key: value
        for key, value in {
            "clock_source": source,
            "preferred_leader": preferred,
            "subdomain": subdomain,
            "global_unicast_delay_requests": global_unicast,
            "aggregate_ptpv1_unicast_delay_requests": ptpv1_unicast,
            "follower_only": follower_only,
            "ptpv2_domain": domain,
            "ptpv2_priority1": priority1,
            "ptpv2_priority2": priority2,
            "multicast_dscp": dscp,
        }.items()
        if value is not None
    }
    if settings is not None:
        extra = json.loads(settings)
        if not isinstance(extra, dict) or changes.keys() & extra.keys():
            raise typer.BadParameter("Settings must be an object without duplicate command-line fields.")
        changes.update(extra)
    if not changes:
        raise typer.BadParameter("Pass at least one clock setting.")

    run_with_devices(run_clock_configuration, changes, CLOCK_CONTROL_PROFILES[profile])
