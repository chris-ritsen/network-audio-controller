from __future__ import annotations

import asyncio
import math
import os
import subprocess
import sys
from typing import Optional

import typer

from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.latency import nanoseconds_to_milliseconds
from netaudio.dante.services.notification import (
    NOTIFICATION_AES67_STATUS,
    NOTIFICATION_ENCODING_STATUS,
    NOTIFICATION_LATENCY_CHANGE,
    NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_SETTINGS_CHANGE,
)

from netaudio._common import (
    ReadbackResult,
    _command_context,
    _discover,
    _get_arc_port,
    _populate_controls,
    filter_devices,
    output_single,
    output_table,
    readback_after_notification,
    send_and_wait_for_notification,
    sort_devices,
)
from netaudio._exit_codes import ExitCode

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True)

top_app = typer.Typer(help="Manage netaudio configuration.", no_args_is_help=True)

VALID_SAMPLE_RATES = [44100, 48000, 88200, 96000, 176400, 192000]
VALID_ENCODINGS = [16, 24, 32]

MOVED_COMMANDS = ["sample-rate", "encoding", "latency", "aes67"]


def _moved_command(name: str):
    def handler(ctx: typer.Context):
        typer.echo(f"This command has moved. Use: netaudio device config {name}", err=True)
        raise typer.Exit(code=1)

    return handler


for _name in MOVED_COMMANDS:
    top_app.command(_name, hidden=True, context_settings={"allow_extra_args": True, "allow_interspersed_args": False})(
        _moved_command(_name)
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

    typer.echo(str(default_config_path()))


def _resolve_targets(filtered, all_devices):
    if all_devices:
        if not filtered:
            typer.echo("Error: no devices found.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)
        return list(sort_devices(filtered))

    if len(filtered) == 0:
        typer.echo("Error: device not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if len(filtered) > 1:
        names = ", ".join(device.name or server_name for server_name, device in filtered.items())
        typer.echo(f"Error: multiple devices matched: {names}", err=True)
        typer.echo("Use -n to select a device or --all for all devices.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return [next(iter(filtered.items()))]


async def _read_settings_value(device, key):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict) or settings.get(key) is None:
        raise RuntimeError(f"{key} readback was unavailable")
    return settings[key]


async def _read_aes67_configured(device):
    configured = await device.operations.get_aes67_configured()
    if configured is None:
        raise RuntimeError("AES67 configured-state readback was unavailable")
    return configured


async def _read_latency_milliseconds(device):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict):
        raise RuntimeError("active latency readback was unavailable")
    latency_nanoseconds = settings.get("active_latency_ns")
    if latency_nanoseconds is None:
        raise RuntimeError("active latency readback was unavailable")
    return nanoseconds_to_milliseconds(latency_nanoseconds)


async def _read_sample_rate_status_result(send, device):
    current_sample_rate, supported_sample_rates = await send.probe_sample_rate_status(device.ipv4)
    device.sample_rate = current_sample_rate
    device.supported_sample_rates = supported_sample_rates
    return current_sample_rate, supported_sample_rates


async def _read_sample_rate_status(send, device):
    current_sample_rate, _ = await _read_sample_rate_status_result(send, device)
    return current_sample_rate


async def _read_encoding_status_result(send, device):
    current_encoding, supported_encodings = await send.probe_encoding_status(device.ipv4)
    device.encoding = current_encoding
    device.supported_encodings = supported_encodings
    return current_encoding, supported_encodings


async def _read_encoding_status(send, device):
    current_encoding, _ = await _read_encoding_status_result(send, device)
    return current_encoding


def _targets_supporting_value(
    targets,
    requested_value,
    supported_values_field,
    fallback_values,
    capability_description,
):
    supported_targets = []
    failures = 0
    for server_name, device in targets:
        label = device.name or server_name
        supported_values = getattr(device, supported_values_field)
        if supported_values is None:
            if requested_value in fallback_values:
                supported_targets.append((server_name, device))
                continue
            typer.echo(
                f"Error: {capability_description} capabilities are unavailable for {label}; "
                f"cannot verify that {requested_value} is supported.",
                err=True,
            )
            failures += 1
            continue
        if requested_value not in supported_values:
            typer.echo(
                f"Error: {label} reports supported {capability_description} values {supported_values}; "
                f"{requested_value} is not supported.",
                err=True,
            )
            failures += 1
            continue
        supported_targets.append((server_name, device))
    return supported_targets, failures


async def _send_verified_change(
    targets,
    send,
    packet,
    port_for,
    expected,
    read_for,
    action,
    success_message,
    notification_ids,
    send_kwargs=None,
    capability_name=None,
    probe_status_for=None,
):
    send_kwargs = send_kwargs or {}

    async def _send_and_read(server_name, device):
        label = device.name or server_name
        try:
            capability_sender = getattr(send, "send_and_wait_for_capability_value", None)
            if capability_name is not None and probe_status_for is not None and capability_sender is not None:
                status = await capability_sender(
                    packet,
                    device.ipv4,
                    port_for(device),
                    capability_name,
                    expected,
                    lambda: probe_status_for(device),
                    **send_kwargs,
                )
                if status is None:
                    return label, ReadbackResult(matched=False), None
                observed_value, _ = status
                return (
                    label,
                    ReadbackResult(
                        matched=observed_value == expected,
                        observed=observed_value,
                        observed_available=True,
                    ),
                    None,
                )
            await send_and_wait_for_notification(
                send,
                packet,
                device.ipv4,
                port_for(device),
                notification_ids,
                **send_kwargs,
            )
        except TimeoutError:
            result = await readback_after_notification(lambda: read_for(device), expected)
            return label, result, None
        except Exception as exception:
            return label, None, exception

        result = await readback_after_notification(lambda: read_for(device), expected)
        return label, result, None

    results = await asyncio.gather(*(_send_and_read(server_name, device) for server_name, device in targets))

    failures = 0
    for label, result, send_error in results:
        if send_error is not None:
            typer.echo(f"Error: could not send {action} to {label}: {send_error}", err=True)
            failures += 1
            continue
        if result.matched:
            typer.echo(success_message(label))
            continue

        failures += 1
        if result.observed_available:
            typer.echo(
                f"Error: {action} sent to {label}, but the device reports {result.observed!r} instead of {expected!r}.",
                err=True,
            )
        else:
            detail = f": {result.error}" if result.error is not None else ""
            typer.echo(
                f"Error: {action} sent to {label}, but readback was unavailable{detail}; the change was not verified.",
                err=True,
            )
    return failures


async def _send_requested_change(targets, request_for, action, success_message):
    async def _request(server_name, device):
        label = device.name or server_name
        try:
            await request_for(device)
            return label, None
        except Exception as exception:
            return label, exception

    results = await asyncio.gather(*(_request(server_name, device) for server_name, device in targets))

    failures = 0
    for label, error in results:
        if error is not None:
            typer.echo(f"Error: could not request {action} for {label}: {error}", err=True)
            failures += 1
        else:
            typer.echo(success_message(label))
    return failures


@app.command("sample-rate")
def sample_rate(
    rate: Optional[int] = typer.Argument(None, help=f"Sample rate: {VALID_SAMPLE_RATES}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the sample rate."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if rate is None:
                if all_devices:
                    output_table(
                        ["Name", "Sample Rate"],
                        [[device.name or server_name, device.sample_rate or ""] for server_name, device in targets],
                    )
                else:
                    output_single(targets[0][1].sample_rate)
                return

            if rate <= 0 or rate > 0xFFFFFFFF:
                typer.echo("Error: sample rate must be between 1 and 4294967295 Hz.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            supported_targets, capability_failures = _targets_supporting_value(
                targets,
                rate,
                "supported_sample_rates",
                VALID_SAMPLE_RATES,
                "sample rate",
            )

            packet, _, port = commands.command_set_sample_rate(rate)
            failures = await _send_verified_change(
                supported_targets,
                send,
                packet,
                lambda _device: port,
                rate,
                lambda device: _read_sample_rate_status(send, device),
                "sample rate change",
                lambda label: f"Set sample rate for {label}: {rate} Hz (verified)",
                (NOTIFICATION_SAMPLE_RATE_STATUS, NOTIFICATION_SETTINGS_CHANGE),
                send_kwargs={"expect_response": False},
                capability_name="sample_rate",
                probe_status_for=lambda device: _read_sample_rate_status_result(send, device),
            )
            if failures + capability_failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command()
def encoding(
    bits: Optional[int] = typer.Argument(None, help=f"Encoding bit depth: {VALID_ENCODINGS}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the encoding bit depth."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if bits is None:
                if all_devices:
                    output_table(
                        ["Name", "Encoding"],
                        [
                            [device.name or server_name, device.encoding if device.encoding is not None else "N/A"]
                            for server_name, device in targets
                        ],
                    )
                else:
                    output_single(targets[0][1].encoding if targets[0][1].encoding is not None else "N/A")
                return

            if bits <= 0 or bits > 0xFFFFFFFF:
                typer.echo("Error: encoding value must be between 1 and 4294967295.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            supported_targets, capability_failures = _targets_supporting_value(
                targets,
                bits,
                "supported_encodings",
                VALID_ENCODINGS,
                "encoding",
            )

            packet, _, port = commands.command_set_encoding(bits)
            failures = await _send_verified_change(
                supported_targets,
                send,
                packet,
                lambda _device: port,
                bits,
                lambda device: _read_encoding_status(send, device),
                "encoding change",
                lambda label: f"Set encoding for {label}: {bits}-bit (verified)",
                (NOTIFICATION_ENCODING_STATUS, NOTIFICATION_SETTINGS_CHANGE),
                send_kwargs={"expect_response": False},
                capability_name="encoding",
                probe_status_for=lambda device: _read_encoding_status_result(send, device),
            )
            if failures + capability_failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command()
def latency(
    value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the device latency."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if value is None:

                async def _read_target(server_name, device):
                    try:
                        return server_name, device, await _read_latency_milliseconds(device), None
                    except Exception as exception:
                        return server_name, device, None, exception

                readings = await asyncio.gather(*(_read_target(server_name, device) for server_name, device in targets))
                failures = [
                    (server_name, device, exception)
                    for server_name, device, _, exception in readings
                    if exception is not None
                ]
                if failures:
                    for server_name, device, exception in failures:
                        typer.echo(
                            f"Error: could not read latency from {device.name or server_name}: {exception}",
                            err=True,
                        )
                    raise typer.Exit(code=ExitCode.ERROR)
                if all_devices:
                    output_table(
                        ["Name", "Latency"],
                        [
                            [device.name or server_name, f"{latency_value:g}"]
                            for server_name, device, latency_value, _ in readings
                        ],
                    )
                else:
                    output_single(readings[0][2])
                return

            if not math.isfinite(value) or value < 0:
                typer.echo("Error: latency must be a finite, nonnegative number.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            packet, _ = commands.command_set_latency(value)
            expected_ns = int(round(value * 1_000_000))
            failures = await _send_verified_change(
                targets,
                send,
                packet,
                _get_arc_port,
                expected_ns,
                lambda device: _read_settings_value(device, "active_latency_ns"),
                "latency change",
                lambda label: f"Set latency for {label}: {value:g} ms (verified)",
                (NOTIFICATION_LATENCY_CHANGE, NOTIFICATION_SETTINGS_CHANGE),
            )
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


def _aes67_state_label(value):
    if value is None:
        return "N/A"
    return "on" if value else "off"


def _aes67_reboot_required(device):
    if device.aes67_current is not None and device.aes67_configured is not None:
        return device.aes67_current != device.aes67_configured
    return False


@app.command()
def aes67(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set AES67 mode."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if enabled is None:
                if all_devices:
                    headers = ["Name", "Current", "Configured", "Reboot Required"]
                    rows = []
                    for server_name, device in targets:
                        rows.append(
                            [
                                device.name or server_name,
                                _aes67_state_label(device.aes67_current),
                                _aes67_state_label(device.aes67_configured),
                                "yes" if _aes67_reboot_required(device) else "no",
                            ]
                        )
                    output_table(headers, rows)
                else:
                    device = targets[0][1]
                    current_label = _aes67_state_label(device.aes67_current)
                    configured_label = _aes67_state_label(device.aes67_configured)
                    reboot = _aes67_reboot_required(device)
                    if device.aes67_current is None and device.aes67_configured is not None:
                        output_single(configured_label)
                    elif device.aes67_current is not None and device.aes67_current == device.aes67_configured:
                        output_single(current_label)
                    elif device.aes67_current is None and device.aes67_configured is None:
                        output_single("N/A")
                    else:
                        typer.echo(f"current: {current_label}", err=False)
                        typer.echo(f"configured: {configured_label}", err=False)
                        if reboot:
                            typer.echo("reboot required", err=True)
                return

            if enabled.lower() not in ("on", "off"):
                typer.echo("Error: expected 'on' or 'off'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            is_enabled = enabled.lower() == "on"
            packet, _, port = commands.command_enable_aes67(is_enabled)
            failures = await _send_verified_change(
                targets,
                send,
                packet,
                lambda _device: port,
                is_enabled,
                _read_aes67_configured,
                "AES67 configuration change",
                lambda label: f"Set AES67 configured state for {label}: {enabled.lower()} (verified)",
                (NOTIFICATION_AES67_STATUS, NOTIFICATION_SETTINGS_CHANGE),
                send_kwargs={"expect_response": False, "repeat": 3, "interval_ms": 100},
            )
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command("preferred-leader")
def preferred_leader(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set preferred leader mode."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if enabled is None:
                if all_devices:

                    def _pref_display(device):
                        if device.preferred_leader is None:
                            return "N/A"
                        return "on" if device.preferred_leader else "off"

                    output_table(
                        ["Name", "Preferred Leader"],
                        [[device.name or server_name, _pref_display(device)] for server_name, device in targets],
                    )
                else:
                    device = targets[0][1]
                    if device.preferred_leader is None:
                        output_single("N/A")
                    else:
                        output_single("on" if device.preferred_leader else "off")
                return

            if enabled.lower() not in ("on", "off"):
                typer.echo("Error: expected 'on' or 'off'.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            is_preferred = enabled.lower() == "on"
            packet, _, port = commands.command_set_preferred_leader(is_preferred)

            async def _request_preferred(device):
                await send(
                    packet,
                    device.ipv4,
                    port,
                    expect_response=False,
                    repeat=3,
                    interval_ms=500,
                )

            failures = await _send_requested_change(
                targets,
                _request_preferred,
                "preferred leader change",
                lambda label: f"Preferred leader change requested for {label}: {enabled.lower()}; not verified.",
            )
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())


@app.command("interface")
def interface(
    mode: Optional[str] = typer.Argument(None, help="dhcp or static"),
    ip_address: Optional[str] = typer.Option(None, "--ip", help="IP address (static only)."),
    netmask: Optional[str] = typer.Option(None, "--netmask", help="Subnet mask (static only)."),
    dns_server: Optional[str] = typer.Option(None, "--dns", help="DNS server (static only)."),
    gateway: Optional[str] = typer.Option(None, "--gateway", help="Gateway (static only)."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set interface configuration."""

    commands = DanteDeviceCommands()

    async def _run():
        if mode is None:
            from netaudio.daemon.client import get_devices_from_daemon

            devices = await get_devices_from_daemon()

            if devices is not None:
                devices = filter_devices(devices)
            else:
                from netaudio.dante.application import DanteApplication
                from netaudio.common.app_config import settings

                application = DanteApplication()
                await application.startup()
                try:
                    devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
                    devices = filter_devices(devices or {})
                finally:
                    await application.shutdown()

            headers = ["Name", "Interface", "Mode", "IP Address", "Netmask", "Gateway", "DNS", "Pending"]
            rows = []
            json_data = {}

            for server_name, device in sort_devices(devices):
                interfaces = device.interfaces
                pending_config = device.interface_pending_config
                pending_label = ""
                if pending_config:
                    pending_mode = pending_config.get("mode", "")
                    if pending_mode == "static":
                        pending_label = f"static {pending_config.get('ip_address', '')}"
                    else:
                        pending_label = pending_mode

                if not interfaces:
                    rows.append(
                        [
                            device.name or server_name,
                            "0",
                            "",
                            str(device.ipv4) if device.ipv4 else "",
                            "",
                            "",
                            "",
                            pending_label,
                        ]
                    )
                    continue

                for index, iface in enumerate(interfaces):
                    rows.append(
                        [
                            device.name or server_name,
                            str(index),
                            iface.get("mode", ""),
                            iface.get("ip_address", ""),
                            iface.get("netmask", ""),
                            iface.get("gateway", ""),
                            iface.get("dns_server", ""),
                            pending_label if index == 0 else "",
                        ]
                    )

                device_json = {
                    "name": device.name,
                    "interfaces": interfaces,
                }
                if pending_config:
                    device_json["pending_config"] = pending_config
                json_data[server_name] = device_json

            output_table(headers, rows, json_data=json_data)
            return

        if mode not in ("dhcp", "static"):
            typer.echo("Error: mode must be 'dhcp' or 'static'.", err=True)
            raise typer.Exit(code=ExitCode.ERROR)

        if mode == "static":
            if not all([ip_address, netmask, dns_server, gateway]):
                typer.echo("Error: --ip, --netmask, --dns, and --gateway are required for static mode.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            targets = _resolve_targets(filtered, all_devices)

            if mode == "dhcp":
                packet, _, port = commands.command_set_interface_dhcp()
            else:
                packet, _, port = commands.command_set_interface_static(ip_address, netmask, dns_server, gateway)
            failures = await _send_requested_change(
                targets,
                lambda device: send(
                    packet,
                    str(device.ipv4),
                    port,
                    expect_response=False,
                ),
                "interface change",
                lambda label: f"Interface change requested for {label}: {mode}; not verified.",
            )

            typer.echo("Reboot required for changes to take effect.", err=True)
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_run())
