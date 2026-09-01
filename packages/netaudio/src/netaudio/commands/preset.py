from __future__ import annotations

import asyncio
import math
import os
import xml.etree.ElementTree as ET
import uuid
from functools import partial
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.commands.preset_display import show_preset_dry_run
from netaudio.dante.latency import MICROSECONDS_PER_MILLISECOND
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyMutationOutcomeUnknownError,
    change_sample_rate_with_command_sender,
)

app = typer.Typer(help="Save and load device presets (DC-compatible XML).", no_args_is_help=True)

UNSUPPORTED_LOAD_FIELDS = {
    "additional_interfaces": "additional network interfaces",
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
        temporary.unlink(missing_ok=True)


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
        from netaudio._common import _command_context
        from netaudio._common_output import format_devices_xml
        from netaudio._common_selection import filter_devices

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

        transmitter_channel_names = {}
        for tx_elem in device_element.findall("txchannel"):
            dante_id = tx_elem.get("danteId")
            label = tx_elem.findtext("label", "")
            if dante_id and label:
                transmitter_channel_names[int(dante_id)] = label

        receiver_subscriptions = {}
        receiver_channel_elements = device_element.findall("rxchannel")
        for receiver_channel_element in receiver_channel_elements:
            dante_identifier = receiver_channel_element.get("danteId")
            if dante_identifier is None:
                raise ValueError(f"{device_name}: receiver channel is missing danteId")
            try:
                receiver_channel_number = int(dante_identifier)
            except ValueError as exception:
                raise ValueError(f"{device_name}: receiver channel danteId must be an integer") from exception
            if not 1 <= receiver_channel_number <= 0xFFFF:
                raise ValueError(f"{device_name}: receiver channel danteId must be from 1 through 65535")
            if receiver_channel_number in receiver_subscriptions:
                raise ValueError(f"{device_name}: duplicate receiver channel danteId {receiver_channel_number}")

            subscribed_channel = (receiver_channel_element.findtext("subscribed_channel") or "").strip()
            subscribed_device = (receiver_channel_element.findtext("subscribed_device") or "").strip()
            if subscribed_device and not subscribed_channel:
                raise ValueError(
                    f"{device_name}: receiver channel {receiver_channel_number} has a subscribed device without a channel"
                )
            if subscribed_channel:
                receiver_subscriptions[receiver_channel_number] = {
                    "tx_channel": subscribed_channel,
                    "tx_device": subscribed_device or ".",
                }
            else:
                receiver_subscriptions[receiver_channel_number] = None

        if transmitter_channel_names:
            device_config["transmitter_channel_names"] = transmitter_channel_names
        if receiver_channel_elements:
            device_config["rx_subscriptions"] = receiver_subscriptions

        if device_name in preset_devices:
            raise ValueError(f"duplicate preset device name: {device_name!r}")
        preset_devices[device_name] = device_config

    return preset_name, preset_devices


def _unsupported_load_fields(config: dict) -> list[str]:
    return [label for field, label in UNSUPPORTED_LOAD_FIELDS.items() if field in config]


def _validate_supported_config(device_name: str, config: dict, device) -> None:
    from netaudio.commands.config import VALID_SAMPLE_RATES

    sample_rate = config.get("sample_rate")
    if sample_rate is not None:
        if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or not 1 <= sample_rate <= 0xFFFFFFFF:
            raise ValueError(f"{device_name}: sample rate must be an integer from 1 through 4294967295")
        supported_sample_rates = device.supported_sample_rates
        if supported_sample_rates is None and sample_rate not in VALID_SAMPLE_RATES:
            raise ValueError(
                f"{device_name}: sample-rate capabilities are unavailable; "
                f"cannot verify that {sample_rate} is supported"
            )
        if supported_sample_rates is not None and sample_rate not in supported_sample_rates:
            raise ValueError(
                f"{device_name}: device reports supported sample rates {supported_sample_rates}; "
                f"{sample_rate} is not supported"
            )

    encoding = config.get("encoding")
    if encoding is not None:
        if isinstance(encoding, bool) or not isinstance(encoding, int) or not 1 <= encoding <= 0xFFFFFFFF:
            raise ValueError(f"{device_name}: encoding must be an integer from 1 through 4294967295")
        supported_encodings = device.supported_encodings
        if supported_encodings is None:
            raise ValueError(f"{device_name}: encoding capabilities are unavailable")
        if encoding not in supported_encodings:
            raise ValueError(
                f"{device_name}: device reports supported encodings {supported_encodings}; {encoding} is not supported"
            )

    latency = config.get("latency")
    if latency is not None and (
        isinstance(latency, bool) or not isinstance(latency, (int, float)) or not math.isfinite(latency) or latency < 0
    ):
        raise ValueError(f"{device_name}: latency must be a finite, nonnegative number")

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


async def _read_encoding(send, device):
    current_encoding, supported_encodings = await send.probe_encoding_status(device.ipv4)
    device.encoding = current_encoding
    device.supported_encodings = supported_encodings
    return current_encoding


async def _read_latency(device):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict) or settings.get("active_latency_ns") is None:
        raise RuntimeError("active latency readback was unavailable")
    return settings["active_latency_ns"]


async def _read_audio_setting(action, send, device):
    if action == "sample_rate":
        return await _read_sample_rate(device)
    if action == "encoding":
        return await _read_encoding(send, device)
    if action == "latency":
        return await _read_latency(device)
    raise ValueError(f"unsupported audio setting: {action}")


@app.command("load")
def preset_load(
    input_file: str = typer.Argument(..., help="Input preset file (.xml)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be applied without making changes."),
    confirm_destructive: bool = typer.Option(
        False,
        "--confirm-destructive",
        help="Confirm permanent transmitter-flow membership loss caused by sample-rate restoration.",
    ),
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
        show_preset_dry_run(preset_devices)
        return

    async def _apply():
        from netaudio._common import (
            _command_context,
            _get_arc_port,
            readback_after_notification,
            send_and_wait_for_notification,
        )
        from netaudio._common_selection import filter_devices
        from netaudio.cli import state as cli_state
        from netaudio.commands.subscription import reconcile_receiver_subscriptions
        from netaudio.dante.device_commands import DanteDeviceCommands
        from netaudio.dante.transmitter_channel_name_reconciliation import (
            reconcile_transmitter_channel_names,
        )
        from netaudio.dante.services.notification import (
            NOTIFICATION_CLOCKING_STATUS,
            NOTIFICATION_ENCODING_STATUS,
            NOTIFICATION_INTERFACE_STATUS,
            NOTIFICATION_LATENCY_CHANGE,
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
                    _validate_supported_config(device_name, config, device)
                    actions = []
                    if "sample_rate" in config:
                        actions.append(("sample_rate", config["sample_rate"], None))
                    if "encoding" in config:
                        packet, _, port = commands.command_set_encoding(config["encoding"])
                        actions.append(("encoding", packet, port))
                    if "latency" in config:
                        packet, _ = commands.command_set_latency(config["latency"])
                        actions.append(("latency", packet, _get_arc_port(device)))
                    if "preferred_leader" in config:
                        packet, _, port = commands.command_set_preferred_leader(config["preferred_leader"])
                        actions.append(("preferred_leader", packet, port))
                    if "transmitter_channel_names" in config:
                        await device.get_tx_channels()
                        available_transmitter_channels = {
                            channel.number for channel in (device.tx_channels or {}).values()
                        }
                        for transmitter_channel_number, channel_name in config["transmitter_channel_names"].items():
                            if transmitter_channel_number not in available_transmitter_channels:
                                raise ValueError(f"transmitter channel {transmitter_channel_number} is unavailable")
                            commands.command_set_channel_name(
                                "tx",
                                transmitter_channel_number,
                                channel_name,
                            )
                        actions.append(
                            (
                                "transmitter_channel_names",
                                config["transmitter_channel_names"],
                                None,
                            )
                        )
                    if "rx_subscriptions" in config:
                        await device.get_rx_channels()
                        available_receiver_channels = {
                            channel.number for channel in (device.rx_channels or {}).values()
                        }
                        desired_sources = {}
                        for receiver_channel_number, subscription in config["rx_subscriptions"].items():
                            if receiver_channel_number not in available_receiver_channels:
                                raise ValueError(f"receiver channel {receiver_channel_number} is unavailable")
                            if subscription is None:
                                desired_sources[receiver_channel_number] = None
                            else:
                                transmitter_device_name = subscription["tx_device"]
                                if transmitter_device_name == ".":
                                    transmitter_device_name = device_name
                                desired_sources[receiver_channel_number] = (
                                    subscription["tx_channel"],
                                    transmitter_device_name,
                                )

                        removals = [
                            receiver_channel_number
                            for receiver_channel_number, desired_source in desired_sources.items()
                            if desired_source is None
                        ]
                        additions = [
                            (receiver_channel_number, desired_source[0], desired_source[1])
                            for receiver_channel_number, desired_source in desired_sources.items()
                            if desired_source is not None
                        ]
                        for batch_start in range(0, len(removals), 16):
                            commands.command_remove_subscriptions(removals[batch_start : batch_start + 16])
                        for batch_start in range(0, len(additions), 16):
                            commands.command_add_subscriptions(additions[batch_start : batch_start + 16])
                        actions.append(("receiver_subscriptions", desired_sources, None))
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

                    for action, payload, port in actions:
                        action_label = action.replace("_", " ")
                        if action == "sample_rate":
                            try:
                                result = await change_sample_rate_with_command_sender(
                                    send,
                                    device,
                                    payload,
                                    confirm_destructive=confirm_destructive,
                                )
                            except SampleRateTopologyChangedButUnverifiedError as exception:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate: CHANGED BUT UNVERIFIED ({exception})",
                                    )
                                )
                                continue
                            except SampleRateTopologyMutationOutcomeUnknownError as exception:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate: MUTATION OUTCOME UNKNOWN ({exception})",
                                    )
                                )
                                continue
                            except Exception as exception:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate: REFUSED ({exception})",
                                    )
                                )
                                continue
                            if result.changed:
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate {result.observed_sample_rate_hertz} Hz and topology (verified)",
                                    )
                                )
                            else:
                                action_results.append(
                                    (
                                        device_name,
                                        f"sample rate already {result.observed_sample_rate_hertz} Hz "
                                        "(verified; no write sent)",
                                    )
                                )
                            continue
                        if action == "receiver_subscriptions":
                            try:
                                result = await reconcile_receiver_subscriptions(
                                    send,
                                    device,
                                    payload,
                                )
                            except Exception as exception:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"receiver subscriptions: FAILED ({exception})",
                                    )
                                )
                                continue

                            if result.unchanged and not result.verified and not result.failures:
                                action_results.append(
                                    (
                                        device_name,
                                        f"receiver subscriptions already match ({len(result.unchanged)} channels)",
                                    )
                                )
                            for receiver_channel_number, desired_source in sorted(result.verified.items()):
                                if desired_source is None:
                                    description = f"receiver channel {receiver_channel_number} unsubscribed"
                                else:
                                    description = (
                                        f"receiver channel {receiver_channel_number} <- "
                                        f"{desired_source[0]}@{desired_source[1]}"
                                    )
                                action_results.append((device_name, f"{description} (verified)"))
                            for receiver_channel_number, detail in sorted(result.failures.items()):
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"receiver channel {receiver_channel_number}: FAILED ({detail})",
                                    )
                                )
                            continue
                        if action == "transmitter_channel_names":
                            try:
                                result = await reconcile_transmitter_channel_names(
                                    send,
                                    device,
                                    payload,
                                )
                            except Exception as exception:
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"transmitter channel names: FAILED ({exception})",
                                    )
                                )
                                continue

                            if result.unchanged and not result.verified and not result.failures:
                                action_results.append(
                                    (
                                        device_name,
                                        f"transmitter channel names already match ({len(result.unchanged)} channels)",
                                    )
                                )
                            for transmitter_channel_number, channel_name in sorted(result.verified.items()):
                                action_results.append(
                                    (
                                        device_name,
                                        f"transmitter channel {transmitter_channel_number}: {channel_name} (verified)",
                                    )
                                )
                            for transmitter_channel_number, detail in sorted(result.failures.items()):
                                failures += 1
                                action_results.append(
                                    (
                                        device_name,
                                        f"transmitter channel {transmitter_channel_number}: FAILED ({detail})",
                                    )
                                )
                            continue

                        packet = payload
                        try:
                            notification_ids = {
                                "encoding": (
                                    NOTIFICATION_ENCODING_STATUS,
                                    NOTIFICATION_SETTINGS_CHANGE,
                                ),
                                "latency": (
                                    NOTIFICATION_LATENCY_CHANGE,
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
                            elif action == "encoding":
                                await send_and_wait_for_notification(
                                    send,
                                    packet,
                                    device.ipv4,
                                    port,
                                    notification_ids,
                                    expect_response=False,
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

                        if action in ("encoding", "latency"):
                            if action == "encoding":
                                expected = config["encoding"]
                                success = f"encoding {expected}-bit"
                            else:
                                expected = int(round(config["latency"] * 1_000_000))
                                success = f"latency {config['latency']:g} ms"
                            result = await readback_after_notification(
                                partial(_read_audio_setting, action, send, device),
                                expected,
                            )
                            if result.matched:
                                action_results.append((device_name, f"{success} (verified)"))
                                continue

                            failures += 1
                            if result.observed_available:
                                detail = f"device reports {result.observed!r}"
                            else:
                                detail = f"fresh readback was unavailable: {result.error}"
                            action_results.append(
                                (
                                    device_name,
                                    f"{success}: FAILED ({detail})",
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
                        if device.interface_pending_config is not None:
                            needs_reboot.append(device_name)
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
