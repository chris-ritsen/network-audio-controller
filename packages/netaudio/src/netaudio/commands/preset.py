from __future__ import annotations

import asyncio
import os
import xml.etree.ElementTree as ET
import uuid
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.dante.latency import MICROSECONDS_PER_MILLISECOND

app = typer.Typer(help="Save and load device presets (DC-compatible XML).", no_args_is_help=True)

UNSUPPORTED_LOAD_FIELDS = {
    "encoding": "encoding",
    "latency": "latency",
    "additional_interfaces": "additional network interfaces",
    "tx_labels": "TX channel labels",
    "rx_subscriptions": "RX subscriptions",
}


def _write_preset_atomic(path: Path, content: str, *, force: bool) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())

        if force:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


async def _start_preset_readback_application():
    from netaudio.dante.application import DanteApplication

    application = DanteApplication()
    try:
        await application.startup()
    except Exception:
        await application.shutdown()
        raise
    return application


async def _read_preferred_leader(application, device):
    state = await application.probe_preferred_leader_state(
        str(device.ipv4),
        timeout=1.0,
    )
    if state is None:
        raise RuntimeError("preferred-leader readback was unavailable")
    return state


def _expected_interface_config(config: dict) -> dict:
    mode = config["interface_mode"]
    if mode in ("dynamic", "dhcp"):
        return {"mode": "dynamic"}
    return {
        "mode": "static",
        "ip_address": config["ip_address"],
        "netmask": config["netmask"],
        "dns_server": config["dns_server"],
        "gateway": config["gateway"],
    }


async def _read_interface_config(application, device, expected: dict):
    interfaces = await application.probe_interface_status(
        str(device.ipv4),
        timeout=1.0,
    )
    if not interfaces:
        raise RuntimeError("interface readback was unavailable")
    reported = device.interface_pending_config or interfaces[0]
    return {field: reported.get(field) for field in expected}


@app.command("save")
def preset_save(
    output: str = typer.Argument(..., help="Output file path (.xml)."),
    preset_name: Optional[str] = typer.Option(None, "--name", "-n", help="Preset name (defaults to filename)."),
    force: bool = typer.Option(False, "--force", help="Replace an existing preset file."),
):
    output_path = Path(output)
    if output_path.exists() and not force:
        typer.echo(
            f"Error: refusing to overwrite existing file: {output}; use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)

    async def _run():
        from netaudio._common import _command_context, filter_devices, format_devices_xml

        async with _command_context() as (devices, send):
            devices = filter_devices(devices)

            if not devices:
                typer.echo("Error: no devices found.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            name = preset_name or output_path.stem
            xml_content = format_devices_xml(devices, preset_name=name)

            try:
                _write_preset_atomic(output_path, xml_content, force=force)
            except FileExistsError as exception:
                typer.echo(
                    f"Error: refusing to overwrite existing file: {output}; use --force to replace it.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR) from exception
            except OSError as exception:
                typer.echo(f"Error: could not save preset to {output}: {exception}", err=True)
                raise typer.Exit(code=ExitCode.ERROR) from exception
            typer.echo(f"Saved {len(devices)} devices to {output}", err=True)

    asyncio.run(_run())


def _parse_preset(preset_path: Path) -> tuple[str, dict[str, dict[str, Any]]]:
    tree = ET.parse(preset_path)
    root = tree.getroot()
    preset_name = root.findtext("name", "unknown")

    preset_devices: dict[str, dict[str, Any]] = {}
    for device_element in root.findall("device"):
        device_name = device_element.findtext("friendly_name") or device_element.findtext("name", "")
        if not device_name:
            continue

        device_config: dict[str, Any] = {"name": device_name}

        preferred_elem = device_element.find("preferred_master")
        if preferred_elem is not None:
            preferred_value = preferred_elem.get("value", "").strip().lower()
            if preferred_value not in ("true", "false"):
                raise ValueError(f"{device_name}: preferred_master value must be true or false")
            device_config["preferred_leader"] = preferred_value == "true"

        sample_rate = device_element.findtext("samplerate")
        if sample_rate:
            device_config["sample_rate"] = int(sample_rate)

        encoding = device_element.findtext("encoding")
        if encoding:
            device_config["encoding"] = int(encoding)

        latency = device_element.findtext("unicast_latency")
        if latency:
            device_config["latency"] = int(latency) / MICROSECONDS_PER_MILLISECOND

        interface_elements = device_element.findall("interface")
        interface_elem = interface_elements[0] if interface_elements else None
        if len(interface_elements) > 1:
            device_config["additional_interfaces"] = len(interface_elements) - 1
        if interface_elem is not None:
            ip_elem = interface_elem.find("ipv4_address")
            if ip_elem is not None:
                mode = ip_elem.get("mode", "dynamic")
                device_config["interface_mode"] = mode
                if mode == "static":
                    device_config["ip_address"] = ip_elem.findtext("ip_address", "")
                    device_config["netmask"] = ip_elem.findtext("subnet_mask", "")
                    device_config["gateway"] = ip_elem.findtext("gateway", "")
                    device_config["dns_server"] = ip_elem.findtext("dns_server", "")

        tx_labels = {}
        for tx_elem in device_element.findall("txchannel"):
            dante_id = tx_elem.get("danteId")
            label = tx_elem.findtext("label", "")
            if dante_id and label:
                tx_labels[int(dante_id)] = label

        rx_subscriptions = {}
        for rx_elem in device_element.findall("rxchannel"):
            dante_id = rx_elem.get("danteId")
            subscribed_channel = rx_elem.findtext("subscribed_channel")
            subscribed_device = rx_elem.findtext("subscribed_device")
            if dante_id and subscribed_channel:
                rx_subscriptions[int(dante_id)] = {
                    "tx_channel": subscribed_channel,
                    "tx_device": subscribed_device or ".",
                }

        if tx_labels:
            device_config["tx_labels"] = tx_labels
        if rx_subscriptions:
            device_config["rx_subscriptions"] = rx_subscriptions

        if device_name in preset_devices:
            raise ValueError(f"duplicate preset device name: {device_name!r}")
        preset_devices[device_name] = device_config

    return preset_name, preset_devices


def _unsupported_load_fields(config: dict) -> list[str]:
    return [label for field, label in UNSUPPORTED_LOAD_FIELDS.items() if field in config]


def _validate_supported_config(device_name: str, config: dict) -> None:
    from netaudio.commands.config import VALID_SAMPLE_RATES

    sample_rate = config.get("sample_rate")
    if sample_rate is not None and sample_rate not in VALID_SAMPLE_RATES:
        raise ValueError(f"{device_name}: unsupported sample rate {sample_rate}; expected one of {VALID_SAMPLE_RATES}")

    mode = config.get("interface_mode")
    if mode is not None and mode not in ("dynamic", "dhcp", "static"):
        raise ValueError(f"{device_name}: unsupported interface mode {mode!r}")
    if mode == "static":
        missing = [
            option
            for option, field in (
                ("ip", "ip_address"),
                ("netmask", "netmask"),
                ("dns", "dns_server"),
                ("gateway", "gateway"),
            )
            if not config.get(field)
        ]
        if missing:
            raise ValueError(f"{device_name}: static interface is missing {', '.join(missing)}")


async def _read_sample_rate(device):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict) or settings.get("sample_rate") is None:
        raise RuntimeError("sample-rate readback was unavailable")
    return settings["sample_rate"]


@app.command("load")
def preset_load(
    input_file: str = typer.Argument(..., help="Input preset file (.xml)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be applied without making changes."),
):
    preset_path = Path(input_file)
    if not preset_path.exists():
        typer.echo(f"Error: file not found: {input_file}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    try:
        preset_name, preset_devices = _parse_preset(preset_path)
    except (ET.ParseError, OSError, ValueError) as exception:
        typer.echo(f"Error: invalid preset {input_file}: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception

    typer.echo(f"Preset: {preset_name} ({len(preset_devices)} devices)", err=True)

    if dry_run:
        for device_name, config in preset_devices.items():
            typer.echo(f"\n{device_name}:")
            if "preferred_leader" in config:
                typer.echo(f"  preferred leader: {'on' if config['preferred_leader'] else 'off'}")
            if "sample_rate" in config:
                typer.echo(f"  sample rate: {config['sample_rate']}")
            if "encoding" in config:
                typer.echo(f"  encoding: {config['encoding']} (unsupported for load)")
            if "latency" in config:
                typer.echo(f"  latency: {config['latency']:g} ms (unsupported for load)")
            if "interface_mode" in config:
                mode = config["interface_mode"]
                if mode == "static":
                    typer.echo(
                        f"  interface: static {config.get('ip_address', '')} mask={config.get('netmask', '')} gw={config.get('gateway', '')} dns={config.get('dns_server', '')}"
                    )
                else:
                    typer.echo(f"  interface: {mode}")
            if "additional_interfaces" in config:
                count = config["additional_interfaces"]
                typer.echo(f"  additional interfaces: {count} (unsupported for load)")
            if "tx_labels" in config:
                for channel_number, label in sorted(config["tx_labels"].items()):
                    typer.echo(f"  tx {channel_number}: {label} (unsupported for load)")
            if "rx_subscriptions" in config:
                for channel_number, subscription in sorted(config["rx_subscriptions"].items()):
                    tx_device = subscription["tx_device"]
                    if tx_device == ".":
                        tx_device = device_name
                    typer.echo(
                        f"  rx {channel_number}: {subscription['tx_channel']}@{tx_device} (unsupported for load)"
                    )
        return

    async def _apply():
        from netaudio._common import (
            _command_context,
            filter_devices,
            readback_after_notification,
            send_and_wait_for_notification,
        )
        from netaudio.cli import state as cli_state
        from netaudio.dante.device_commands import DanteDeviceCommands
        from netaudio.dante.services.notification import (
            NOTIFICATION_CLOCKING_STATUS,
            NOTIFICATION_INTERFACE_STATUS,
            NOTIFICATION_SAMPLE_RATE_STATUS,
            NOTIFICATION_SETTINGS_CHANGE,
        )

        async with _command_context() as (devices, send):
            devices = filter_devices(devices)
            if not devices:
                typer.echo("Error: no devices matched the global filters.", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            commands = DanteDeviceCommands()
            devices_by_name: dict[str, list] = {}
            for server_name, device in devices.items():
                if device.name:
                    devices_by_name.setdefault(device.name, []).append((server_name, device))

            matched = []
            unmatched_preset_names = []
            for device_name, config in preset_devices.items():
                candidates = devices_by_name.get(device_name, [])
                if not candidates:
                    unmatched_preset_names.append(device_name)
                    continue
                if len(candidates) > 1:
                    servers = ", ".join(server_name for server_name, _ in candidates)
                    typer.echo(
                        f"Error: preset device name {device_name!r} is ambiguous: {servers}",
                        err=True,
                    )
                    raise typer.Exit(code=ExitCode.ERROR)
                server_name, device = candidates[0]
                matched.append((device_name, server_name, device, config))

            filters_active = bool(cli_state.names or cli_state.hosts or cli_state.server_names or cli_state.macs)
            if unmatched_preset_names and not filters_active:
                typer.echo(
                    "Error: preset load was refused before sending any changes because "
                    "these preset devices were not found:",
                    err=True,
                )
                for device_name in unmatched_preset_names:
                    typer.echo(f"  - {device_name}", err=True)
                typer.echo(
                    "Use global device filters to intentionally load only a selected subset.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR)

            if not matched:
                typer.echo(
                    "Error: no selected devices have matching entries in this preset.",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR)

            preflight_errors = []
            plans = []
            for device_name, server_name, device, config in matched:
                unsupported = _unsupported_load_fields(config)
                if unsupported:
                    preflight_errors.append(f"{device_name}: unsupported fields: {', '.join(unsupported)}")
                    continue
                try:
                    _validate_supported_config(device_name, config)
                    actions = []
                    if "sample_rate" in config:
                        packet, _, port = commands.command_set_sample_rate(config["sample_rate"])
                        actions.append(("sample_rate", packet, port))
                    if "preferred_leader" in config:
                        packet, _, port = commands.command_set_preferred_leader(config["preferred_leader"])
                        actions.append(("preferred_leader", packet, port))
                    if "interface_mode" in config:
                        mode = config["interface_mode"]
                        if mode in ("dynamic", "dhcp"):
                            packet, _, port = commands.command_set_interface_dhcp()
                        else:
                            packet, _, port = commands.command_set_interface_static(
                                config["ip_address"],
                                config["netmask"],
                                config["dns_server"],
                                config["gateway"],
                            )
                        actions.append(("interface", packet, port))
                    plans.append((device_name, server_name, device, config, actions))
                except Exception as exception:
                    preflight_errors.append(f"{device_name}: {exception}")

            if preflight_errors:
                typer.echo(
                    "Error: preset load was refused before sending any changes:",
                    err=True,
                )
                for error in preflight_errors:
                    typer.echo(f"  - {error}", err=True)
                raise typer.Exit(code=ExitCode.ERROR)

            needs_fresh_state = any(
                action in ("preferred_leader", "interface") for _, _, _, _, actions in plans for action, _, _ in actions
            )
            readback_application = None
            readback_start_error = None
            if needs_fresh_state:
                try:
                    readback_application = await _start_preset_readback_application()
                    application_devices = readback_application.devices
                    if isinstance(application_devices, dict):
                        for _, server_name, device, _, _ in plans:
                            application_devices[server_name] = device
                except Exception as exception:
                    readback_start_error = exception

            needs_reboot = []
            failures = 0
            action_results = []
            try:
                for device_name, server_name, device, config, actions in plans:
                    if not actions:
                        action_results.append((device_name, "no supported changes"))
                        continue

                    for action, packet, port in actions:
                        action_label = action.replace("_", " ")
                        try:
                            notification_ids = {
                                "sample_rate": (
                                    NOTIFICATION_SAMPLE_RATE_STATUS,
                                    NOTIFICATION_SETTINGS_CHANGE,
                                ),
                                "preferred_leader": (
                                    NOTIFICATION_CLOCKING_STATUS,
                                    NOTIFICATION_SETTINGS_CHANGE,
                                ),
                                "interface": (
                                    NOTIFICATION_INTERFACE_STATUS,
                                    NOTIFICATION_SETTINGS_CHANGE,
                                ),
                            }[action]
                            if action == "preferred_leader":
                                await send_and_wait_for_notification(
                                    send,
                                    packet,
                                    device.ipv4,
                                    port,
                                    notification_ids,
                                    expect_response=False,
                                    repeat=3,
                                    interval_ms=500,
                                )
                            else:
                                await send_and_wait_for_notification(
                                    send,
                                    packet,
                                    device.ipv4,
                                    port,
                                    notification_ids,
                                    expect_response=False,
                                )
                        except Exception as exception:
                            failures += 1
                            action_results.append(
                                (
                                    device_name,
                                    f"{action_label}: FAILED to send request: {exception}",
                                )
                            )
                            continue

                        if action == "sample_rate":
                            expected = config["sample_rate"]
                            result = await readback_after_notification(
                                lambda device=device: _read_sample_rate(device),
                                expected,
                            )
                            if result.matched:
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate {expected} Hz (verified)",
                                    )
                                )
                                continue

                            failures += 1
                            if result.observed_available:
                                detail = f"device reports {result.observed!r}"
                            else:
                                detail = f"fresh readback was unavailable: {result.error}"
                            action_results.append(
                                (
                                    device_name,
                                    f"sample rate {expected} Hz: FAILED ({detail})",
                                )
                            )
                            continue

                        if action == "preferred_leader":
                            expected = config["preferred_leader"]
                            enabled = "on" if expected else "off"
                            if readback_application is None:
                                reason = (
                                    f" (readback unavailable: {readback_start_error})" if readback_start_error else ""
                                )
                                action_results.append(
                                    (
                                        device_name,
                                        f"preferred leader {enabled} requested; not verified{reason}",
                                    )
                                )
                                continue

                            result = await readback_after_notification(
                                lambda application=readback_application, device=device: _read_preferred_leader(
                                    application, device
                                ),
                                expected,
                            )
                            if result.matched:
                                action_results.append(
                                    (
                                        device_name,
                                        f"preferred leader {enabled} (verified)",
                                    )
                                )
                            elif result.observed_available:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"preferred leader {enabled}: FAILED (device reports {result.observed!r})",
                                    )
                                )
                            else:
                                detail = f": {result.error}" if result.error is not None else ""
                                action_results.append(
                                    (
                                        device_name,
                                        f"preferred leader {enabled} requested; not verified "
                                        f"(fresh readback unavailable{detail})",
                                    )
                                )
                            continue

                        mode = config["interface_mode"]
                        needs_reboot.append(device_name)
                        if readback_application is None:
                            reason = f" (readback unavailable: {readback_start_error})" if readback_start_error else ""
                            action_results.append(
                                (
                                    device_name,
                                    f"interface {mode} requested; not verified{reason}",
                                )
                            )
                            continue

                        expected = _expected_interface_config(config)
                        result = await readback_after_notification(
                            lambda application=readback_application, device=device, expected=expected: (
                                _read_interface_config(application, device, expected)
                            ),
                            expected,
                        )
                        if result.matched:
                            action_results.append((device_name, f"interface {mode} (verified)"))
                        elif result.observed_available:
                            action_results.append(
                                (
                                    device_name,
                                    f"interface {mode} requested; not verified "
                                    f"(device currently reports {result.observed!r}; reboot may be pending)",
                                )
                            )
                        else:
                            detail = f": {result.error}" if result.error is not None else ""
                            action_results.append(
                                (
                                    device_name,
                                    f"interface {mode} requested; not verified "
                                    f"(fresh readback unavailable{detail}; reboot may be pending)",
                                )
                            )
            finally:
                if readback_application is not None:
                    try:
                        await readback_application.shutdown()
                    except Exception as exception:
                        typer.echo(
                            f"Warning: could not stop preset readback service: {exception}",
                            err=True,
                        )

            typer.echo("\nPreset load summary:", err=True)
            for device_name, result in action_results:
                typer.echo(f"  {device_name}: {result}", err=True)

            if needs_reboot:
                typer.echo(
                    f"\nReboot required: {', '.join(dict.fromkeys(needs_reboot))}",
                    err=True,
                )
            if failures:
                raise typer.Exit(code=ExitCode.ERROR)

    asyncio.run(_apply())


@app.command("show")
def preset_show(
    input_file: str = typer.Argument(..., help="Input preset file (.xml)."),
):
    preset_load(input_file=input_file, dry_run=True)
