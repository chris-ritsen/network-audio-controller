from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

import typer

logger = logging.getLogger("netaudio")

from netaudio.dante.const import BLUETOOTH_MODEL_IDS
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_operations import (
    core_lock_device,
    core_unlock_device,
    validate_dante_name,
    validate_pin,
)
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.latency import standard_latency_choices_for_range
from netaudio.dante.clock_config import format_clock_subdomain
from netaudio.dante.sample_rate_pullup import (
    format_supported_sample_rate_pullup_values,
    sample_rate_pullup_label,
)
from netaudio.dante.services.notification import (
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_SETTINGS_CHANGE,
)

from netaudio._common import (
    CapabilityProbeTimeout,
    _command_context,
    _discover,
    _get_arc_port,
    _load_device_for_show,
    _populate_controls,
    _probe_lock_status_once,
    _resolve_one,
    filter_devices,
    output_single,
    output_table,
    readback_after_notification,
    send_and_wait_for_notification,
    sort_devices,
)
from netaudio.icons import icon, icon_only

app = typer.Typer(help="Manage Dante devices.", no_args_is_help=True)


class ClearConfigurationMode(str, Enum):
    ALL = "all"
    PRESERVE_INTERNET_PROTOCOL_SETTINGS = "preserve-network"


from netaudio.commands.device_display import (
    _channel_matches,
    _device_show_rows,
    _diagnostic_audio_capabilities_data,
    _diagnostic_audio_capability_rows,
    _format_aes67,
    _format_bluetooth,
    _format_channel_count,
    _format_clock_frequency_offset,
    _format_clock_port_record,
    _format_encoding,
    _format_last_seen,
    _format_latency,
    _format_latency_milliseconds,
    _format_latency_range,
    _format_link_speed,
    _format_mac,
    _format_reference_levels,
    _format_reference_options,
    _format_sample_rate,
    _format_show_latency_range,
    _format_show_standard_latencies,
    _format_standard_latency_choices,
    _format_supported_encodings,
    _format_supported_sample_rates,
    _format_transmitter_flow,
)


async def _lock_via_relay(pin: str, action: str) -> dict | None:
    from netaudio.cli import state
    from netaudio.daemon.client import _relay_request

    device_name = None
    if state.names:
        device_name = state.names[0]
    elif state.hosts:
        device_name = state.hosts[0]

    if not device_name:
        devices = await _discover()
        filtered = filter_devices(devices)
        _, device = _resolve_one(filtered)
        device_name = device.name or device.server_name

    status, data = await _relay_request("POST", f"/{action}", body={"device": device_name, "pin": pin}, timeout=8.0)
    if status is None:
        return None
    return data


def _get_lock_key() -> bytes:
    from netaudio.common.app_config import settings as app_settings

    if app_settings.device_lock_key:
        return app_settings.device_lock_key

    from netaudio.common.config_loader import config_search_paths, load_capture_profile

    profile_cfg, _ = load_capture_profile(None, None)
    lock_key_value = profile_cfg.get("device_lock_key")
    if lock_key_value:
        key = lock_key_value.encode("ascii")
        app_settings.device_lock_key = key
        return key

    from netaudio.common.key_extract import extract_lock_key, find_dante_controller_binary

    binary_path = find_dante_controller_binary()
    if binary_path:
        typer.echo(f"Dante Controller found: {binary_path}", err=True)
        typer.echo("Lock/unlock requires a key from your Dante Controller installation.", err=True)
        typer.echo("Extract it? [Y/n] ", err=True, nl=False)
        answer = input().strip().lower()
        if answer in ("", "y", "yes"):
            key = extract_lock_key()
            if key:
                app_settings.device_lock_key = key
                return key
            typer.echo("Error: could not extract key from Dante Controller binary.", err=True)
            raise typer.Exit(code=1)

    typer.echo("Error: device lock requires a key.", err=True)
    typer.echo("", err=True)
    typer.echo("Options:", err=True)
    typer.echo("  1. Install Dante Controller — key is extracted automatically", err=True)
    typer.echo("  2. Set NETAUDIO_DEVICE_LOCK_KEY environment variable", err=True)
    typer.echo("  3. Add device_lock_key to config.toml:", err=True)
    typer.echo("", err=True)
    for search_path in config_search_paths():
        exists = search_path.exists()
        marker = "*" if exists else " "
        typer.echo(f"     {marker} {search_path}", err=True)
    raise typer.Exit(code=1)


def _report_lock_failure(action: str, result: dict) -> None:
    error = result.get("error")
    if error:
        typer.echo(f"Error: {action} failed: {error}", err=True)
    elif result.get("status") is not None:
        typer.echo(f"Error: {action} failed (status 0x{result['status']:04x})", err=True)
    else:
        typer.echo(f"Error: {action} failed: unknown", err=True)
    raise typer.Exit(code=1)


async def _standalone_lock_operation(device_ip: str, pin: str, lock_key: bytes, *, locking: bool) -> dict:
    operation = core_lock_device if locking else core_unlock_device
    result = await operation(device_ip, pin, lock_key)
    if result.get("success") is not True:
        return result

    observation = await _probe_lock_status_once(device_ip)
    if observation is None:
        return {
            **result,
            "success": False,
            "is_locked": None,
            "error": "lock status readback was not reported",
        }
    if observation.is_locked is not locking:
        return {
            **result,
            "success": False,
            "is_locked": observation.is_locked,
            "lock_state_code": observation.lock_state_code,
            "error": "lock operation did not reach the requested state",
        }
    return {
        **result,
        "is_locked": observation.is_locked,
        "lock_state_code": observation.lock_state_code,
        "observation_source": "observed_after_0x1008",
    }


@app.command("list")
def device_list(
    json_flag: bool = typer.Option(False, "-j", "--json", help="Shorthand for --output=json."),
):
    """List discovered Dante devices."""

    async def _run():
        from netaudio.cli import OutputFormat, state

        if json_flag:
            state.output_format = OutputFormat.json

        devices = await _discover()
        await _populate_controls(devices, strict=False)
        devices = filter_devices(devices)
        from netaudio._common import _enrich_clock_fields, _enrich_lock_states

        await _enrich_clock_fields(devices)
        await _enrich_lock_states(devices)
        if state.verbose:
            await asyncio.gather(
                *[
                    device.operations.get_aes67_configured()
                    for device in devices.values()
                    if device.aes67_multicast_prefix is None and device.aes67_supported is not False
                ],
                return_exceptions=True,
            )

        sorted_devices = list(sort_devices(devices))

        any_bluetooth = any(device.model_id in BLUETOOTH_MODEL_IDS for _, device in sorted_devices)

        compact_headers = [
            "Name",
            "Status",
            "IP Address",
            "MAC Address",
            "Model",
            "Lock",
            "TX",
            "RX",
            "Last Seen",
            "Server Name",
        ]
        verbose_extras = [
            "Manufacturer",
            "Product Version",
            "Board",
            "Firmware",
            "Software",
            "Link Speed",
            "Sample Rate",
            "Supported Sample Rates",
            "Encoding",
            "Supported Encodings",
            "Bit Depth",
            "Latency",
            "Configured Latency",
            "Latency Range",
            "Standard Latencies",
            "AES67",
            "Sample Rate Pull-Up",
            "Preferred Leader",
            "Clock Subdomain",
            "PTP Role",
        ]
        if any_bluetooth:
            verbose_extras.append("Bluetooth")
        verbose_headers = compact_headers + verbose_extras

        headers = verbose_headers if state.verbose else compact_headers
        rows = []
        json_data = {}

        for server_name, device in sorted_devices:
            last_seen = device.last_seen
            name_display = device.name or ""

            if device.is_locked is True:
                lock_display = icon("lock") or "locked"
            elif device.is_locked is False:
                lock_display = icon("unlock") or "unlocked"
            else:
                lock_display = ""

            row = [
                name_display,
                "online" if device.online else "offline",
                str(device.ipv4) if device.ipv4 else "",
                _format_mac(device.mac_address),
                device.dante_model or device.model_id or "",
                lock_display,
                _format_channel_count(device.tx_channels, device.tx_count),
                _format_channel_count(device.rx_channels, device.rx_count),
                _format_last_seen(last_seen),
                server_name,
            ]

            if state.verbose:
                row.append(device.manufacturer or "")
                row.append(device.product_version or "")
                row.append(device.board_name or device.dante_model_id or "")
                row.append(device.firmware_version or "")
                row.append(device.software_version or "")
                row.append(_format_link_speed(device.link_speed_mbps) if device.link_speed_mbps is not None else "")
                row.append(str(device.sample_rate or ""))
                row.append(", ".join(str(value) for value in device.supported_sample_rates or []))

                encoding = device.encoding
                row.append(f"PCM{encoding}" if encoding is not None else "")

                supported_encodings = device.supported_encodings
                row.append(", ".join(f"PCM{value}" for value in supported_encodings or []))

                bit_depth = device.bit_depth
                row.append(str(bit_depth) if bit_depth is not None else "")

                latency = device.active_latency if device.active_latency is not None else device.latency
                row.append(f"{latency}ms" if latency is not None else "")

                configured_latency = device.configured_latency
                row.append(f"{configured_latency}ms" if configured_latency is not None else "")
                row.append(_format_latency_range(device.min_latency, device.max_latency))
                row.append(_format_standard_latency_choices(device.min_latency, device.max_latency))

                row.append(_format_aes67(device))
                if device.sample_rate_pullup_raw_value is not None:
                    row.append(sample_rate_pullup_label(device.sample_rate_pullup_raw_value))
                else:
                    row.append("")

                preferred_leader = device.preferred_leader
                if preferred_leader is not None:
                    row.append("on" if preferred_leader else "off")
                else:
                    row.append("")
                row.append(format_clock_subdomain(device.clock_subdomain) if device.clock_subdomain is not None else "")

                row.append(device.clock_role or "")

                if any_bluetooth:
                    row.append(_format_bluetooth(device) if device.model_id in BLUETOOTH_MODEL_IDS else "")

            rows.append(row)
            json_data[server_name] = DanteDeviceSerializer.to_json(device)

        output_table(headers, rows, json_data=json_data, devices=devices)

    asyncio.run(_run())


@app.command("show")
def device_show():
    """Show detailed device information."""

    async def _run():
        from netaudio.cli import OutputFormat, state

        include_channels = state.output_format in (OutputFormat.json, OutputFormat.yaml, OutputFormat.xml)
        server_name, device = await _load_device_for_show(include_channels=include_channels)
        from netaudio._common import _enrich_lock_states

        await _enrich_lock_states({server_name: device})
        data = DanteDeviceSerializer.to_json(device)
        output_table(
            ["Field", "Value"],
            _device_show_rows(device),
            json_data=data,
            title=device.name or server_name,
            devices={server_name: device},
        )

    asyncio.run(_run())


@app.command("capabilities")
def device_capabilities(
    timeout: float = typer.Option(15.0, "--timeout", min=0.1, help="Diagnostic response timeout in seconds."),
):
    """Inspect licensed and sample-rate-dependent audio capabilities."""

    async def _run():
        from netaudio.dante.diagnostic_logs import DeviceLogExportError

        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: no devices matched.", err=True)
                raise typer.Exit(code=1)
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection to exactly one device.",
                    err=True,
                )
                raise typer.Exit(code=1)
            server_name, device = _resolve_one(filtered)
            device_name = device.name or server_name
            if device.ipv4 is None:
                typer.echo(f"Error: {device_name} has no control address.", err=True)
                raise typer.Exit(code=1)
            try:
                result = await send.export_device_logs(device.ipv4, timeout=timeout)
            except (CapabilityProbeTimeout, DeviceLogExportError) as exception:
                typer.echo(f"Error: {exception}", err=True)
                raise typer.Exit(code=1) from None
            capabilities = result.audio_capabilities
            if capabilities is None:
                typer.echo(
                    f"Error: {device_name} returned diagnostic logs without recognized audio capability records.",
                    err=True,
                )
                raise typer.Exit(code=1)
            output_table(
                ["Capability", "Value"],
                _diagnostic_audio_capability_rows(capabilities),
                json_data=_diagnostic_audio_capabilities_data(capabilities),
                title=device_name,
            )

    asyncio.run(_run())


@app.command()
def identify(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Identify every matched device. Required when more than one device matches.",
    ),
):
    """Blink the identify LED on a device."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: device not found.", err=True)
                raise typer.Exit(code=1)

            if all_devices:
                targets = list(filtered.items())
            else:
                if len(filtered) > 1:
                    typer.echo(
                        "Error: multiple devices matched. Narrow the selection with a device "
                        "filter, or pass --all to identify every match.",
                        err=True,
                    )
                    raise typer.Exit(code=1)
                targets = [_resolve_one(filtered)]

            for server_name, device in targets:
                packet, _, port = commands.command_identify()
                await send(packet, device.ipv4, port, expect_response=False)
                typer.echo(f"{icon('identify')}Identified: {device.name}")

    asyncio.run(_run())


@app.command()
def reboot(
    all_devices: bool = typer.Option(
        False,
        "--all",
        help="Reboot every matched device. Required when more than one device matches.",
    ),
):
    """Reboot a device."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("Error: no devices matched.", err=True)
            raise typer.Exit(code=1)

        if all_devices:
            targets = list(filtered.items())
        else:
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection with a device "
                    "filter, or pass --all to reboot every match.",
                    err=True,
                )
                raise typer.Exit(code=1)
            targets = [_resolve_one(filtered)]

        for server_name, device in targets:
            await device.operations.reboot()
            typer.echo(f"Reboot requested: {device.name or server_name}")

    asyncio.run(_run())


@app.command("factory-reset")
def factory_reset(
    confirm: str = typer.Option(
        ...,
        "--confirm",
        help="Exact device name required to authorize erasing all retained configuration.",
    ),
):
    """Erase all retained configuration and request a factory reset."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices)
        filtered = filter_devices(devices)
        if not filtered:
            typer.echo("Error: no devices matched.", err=True)
            raise typer.Exit(code=1)
        if len(filtered) > 1:
            typer.echo(
                "Error: multiple devices matched. Narrow the selection to exactly one device.",
                err=True,
            )
            raise typer.Exit(code=1)

        server_name, device = _resolve_one(filtered)
        device_name = device.name or server_name
        if confirm != device_name:
            typer.echo(
                f"Error: --confirm must exactly match {device_name!r}.",
                err=True,
            )
            raise typer.Exit(code=1)

        await device.operations.factory_reset()
        typer.echo(f"Factory reset requested: {device_name}")

    asyncio.run(_run())


@app.command("clear-configuration")
def clear_configuration(
    mode: ClearConfigurationMode = typer.Option(
        ...,
        "--mode",
        help="Configuration to clear: all, or preserve-network.",
    ),
    confirm: str = typer.Option(
        ...,
        "--confirm",
        help="Exact device name required to authorize clearing retained configuration.",
    ),
):
    """Clear retained configuration with verified device acknowledgement."""

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: no devices matched.", err=True)
                raise typer.Exit(code=1)
            if len(filtered) > 1:
                typer.echo(
                    "Error: multiple devices matched. Narrow the selection to exactly one device.",
                    err=True,
                )
                raise typer.Exit(code=1)

            server_name, device = _resolve_one(filtered)
            device_name = device.name or server_name
            if confirm != device_name:
                typer.echo(
                    f"Error: --confirm must exactly match {device_name!r}.",
                    err=True,
                )
                raise typer.Exit(code=1)
            if device.ipv4 is None:
                typer.echo(f"Error: {device_name} has no control address.", err=True)
                raise typer.Exit(code=1)

            preserve_internet_protocol_settings = mode is ClearConfigurationMode.PRESERVE_INTERNET_PROTOCOL_SETTINGS
            status = await send.clear_configuration(
                device.ipv4,
                preserve_internet_protocol_settings,
            )
            typer.echo(
                f"Clear-configuration accepted: {device_name} "
                f"(result {status['action_result_code']}, mode {mode.value})"
            )

    asyncio.run(_run())


from netaudio.commands.device_exports import export_capability, export_logs
from netaudio.commands.device_network_status import network_status

app.command("export-logs")(export_logs)
app.command("export-capability")(export_capability)
app.command("network-status")(network_status)


from netaudio.commands.config import app as device_config_app

app.add_typer(device_config_app, name="config")

lock_app = typer.Typer(help="Device lock management.", no_args_is_help=True)
app.add_typer(lock_app, name="lock", hidden=True)


@lock_app.command("set")
def lock_set(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to lock the device with."),
):
    async def _run():
        result = await _lock_via_relay(pin, "lock")
        if result is not None:
            if not result.get("success"):
                _report_lock_failure("lock", result)
            elif result.get("already"):
                typer.echo("already locked", err=True)
            return

        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _standalone_lock_operation(device_ip, pin, lock_key, locking=True)

        if not result["success"]:
            _report_lock_failure("lock", result)
        elif result["already"]:
            typer.echo("already locked", err=True)

    asyncio.run(_run())


@lock_app.command("clear")
def lock_clear(
    pin: str = typer.Argument(..., help="4-digit numeric PIN to unlock the device."),
):
    async def _run():
        result = await _lock_via_relay(pin, "unlock")
        if result is not None:
            if not result.get("success"):
                _report_lock_failure("unlock", result)
            elif result.get("already"):
                typer.echo("already unlocked", err=True)
            return

        lock_key = _get_lock_key()

        error = validate_pin(pin)
        if error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(code=1)

        device_ip = await _resolve_lock_ip()

        result = await _standalone_lock_operation(device_ip, pin, lock_key, locking=False)

        if not result["success"]:
            _report_lock_failure("unlock", result)
        elif result["already"]:
            typer.echo("already unlocked", err=True)

    asyncio.run(_run())


@lock_app.command("status")
def lock_status():
    """Show device lock status."""

    async def _run():
        devices = await _discover()
        await _populate_controls(devices, strict=False)
        filtered = filter_devices(devices)

        from netaudio._common import _enrich_lock_states

        await _enrich_lock_states(filtered)

        if not filtered:
            typer.echo("Error: no devices found.", err=True)
            raise typer.Exit(code=1)

        from netaudio.cli import state as cli_state

        headers = ["Name", "IP Address", "Lock Status"]
        rows = []
        json_data = {}

        for server_name, device in sort_devices(filtered):
            if device.is_locked is True:
                status_display = f"{icon('lock')}locked"
            elif device.is_locked is False:
                status_display = f"{icon('unlock')}unlocked"
            else:
                status_display = "unknown"

            rows.append(
                [
                    device.name or "",
                    str(device.ipv4) if device.ipv4 else "",
                    status_display,
                ]
            )
            json_data[server_name] = {
                "name": device.name,
                "ipv4": str(device.ipv4),
                "is_locked": device.is_locked,
            }

        output_table(headers, rows, json_data=json_data)

    asyncio.run(_run())


async def _resolve_lock_ip() -> str:
    from netaudio.cli import state

    if state.hosts:
        return state.hosts[0]

    devices = await _discover()
    filtered = filter_devices(devices)
    _, device = _resolve_one(filtered)
    return str(device.ipv4)


@app.command()
def name(
    new_name: Optional[str] = typer.Argument(None, help="New name (omit to get, empty string to reset)."),
):
    """Get or set device name."""

    commands = DanteDeviceCommands()

    async def _run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            server_name, device = _resolve_one(filtered)

            if new_name is None:
                output_single(device.name)
                return

            arc_port = _get_arc_port(device)

            if new_name == "":
                packet, _ = commands.command_reset_name()
                try:
                    await send(packet, device.ipv4, arc_port)
                except Exception as exception:
                    typer.echo(f"Error: could not request name reset for {server_name}: {exception}", err=True)
                    raise typer.Exit(code=1)
                typer.echo(f"{icon('name')}Name reset requested for {server_name}; not verified.")
            else:
                for candidate_server_name, candidate_device in devices.items():
                    if candidate_device is device:
                        continue
                    if candidate_device.name and candidate_device.name.lower() == new_name.lower():
                        typer.echo(
                            f"Error: name '{new_name}' already in use by "
                            f"{candidate_device.name} ({candidate_server_name})",
                            err=True,
                        )
                        raise typer.Exit(code=1)

                error = validate_dante_name(new_name)
                if error:
                    typer.echo(f"Error: {error}", err=True)
                    raise typer.Exit(code=1)

                packet, _ = commands.command_set_name(new_name)
                try:
                    await send_and_wait_for_notification(
                        send,
                        packet,
                        device.ipv4,
                        arc_port,
                        (NOTIFICATION_ROUTING_DEVICE_CHANGE, NOTIFICATION_SETTINGS_CHANGE),
                    )
                except Exception as exception:
                    typer.echo(
                        f"Error: could not send name change to {device.name or server_name}: {exception}", err=True
                    )
                    raise typer.Exit(code=1)

                async def _read_name():
                    reported_name = await device.fetch_device_name()
                    if not isinstance(reported_name, str):
                        raise RuntimeError("device name readback was unavailable")
                    return reported_name

                result = await readback_after_notification(_read_name, new_name)
                if result.matched:
                    typer.echo(f"{icon('name')}Set name: {new_name} (verified)")
                    return

                label = device.name or server_name
                if result.observed_available:
                    typer.echo(
                        f"Error: name change sent to {label}, but the device reports "
                        f"{result.observed!r} instead of {new_name!r}.",
                        err=True,
                    )
                else:
                    detail = f": {result.error}" if result.error is not None else ""
                    typer.echo(
                        f"Error: name change sent to {label}, but readback was unavailable{detail}; "
                        "the change was not verified.",
                        err=True,
                    )
                raise typer.Exit(code=1)

    asyncio.run(_run())


from netaudio.commands.device_clock import clock

app.command()(clock)


from netaudio.commands.flow import app as flow_app

app.add_typer(flow_app, name="flow", hidden=True)

from netaudio.commands.device_meter import (
    _render_meter_bar,
    _render_meter_display,
    measure_timeout,
    meter_app,
    meter_callback,
    start,
    status,
    stop,
)

app.add_typer(meter_app, name="meter", hidden=True)
