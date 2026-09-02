from __future__ import annotations

import math
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio._common import run_command
from netaudio._common_cli import HELP_CONTEXT_SETTINGS

from netaudio._exit_codes import ExitCode
from netaudio.commands.preset_display import show_preset_dry_run
from netaudio.dante.latency import MICROSECONDS_PER_MILLISECOND
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyMutationOutcomeUnknownError,
)

app = typer.Typer(
    help="Save and load device presets (DC-compatible XML).",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)

PRESET_REFERENCE_HELP = "Preset name in the preset directory, or an explicit .xml path."

UNSUPPORTED_LOAD_FIELDS = {
    "additional_interfaces": "additional network interfaces",
}


def preset_directory() -> Path:
    from netaudio.common.config_loader import default_config_path, get_config_value

    configured, _ = get_config_value("preset_directory")
    if configured:
        return Path(str(configured)).expanduser()
    return default_config_path().parent / "presets"


def _is_explicit_preset_path(reference: str) -> bool:
    path = Path(reference).expanduser()
    return path.is_absolute() or len(path.parts) > 1 or path.suffix.lower() == ".xml"


def resolve_preset_path(reference: str, *, for_write: bool) -> Path:
    path = Path(reference).expanduser()
    if _is_explicit_preset_path(reference):
        return path
    if not for_write and path.exists():
        return path
    return preset_directory() / f"{reference}.xml"


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


async def run_preset_save(application, devices, output_path: Path, preset_name: str | None, force: bool) -> None:
    from netaudio._common_output import format_devices_xml
    from netaudio._common_selection import filter_devices

    devices = filter_devices(devices)

    if not devices:
        typer.echo("Error: no devices found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    name = preset_name or output_path.stem
    xml_content = format_devices_xml(devices, preset_name=name)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_preset_atomic(output_path, xml_content, force=force)
    except FileExistsError as exception:
        typer.echo(
            f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR) from exception
    except OSError as exception:
        typer.echo(f"Error: could not save preset to {output_path}: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception
    typer.echo(f"Saved {len(devices)} devices to {output_path}", err=True)


@app.command("save", help="Save the selected devices' configuration as a preset.")
def preset_save(
    output: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    preset_name: Optional[str] = typer.Option(None, "--name", "-n", help="Preset name (defaults to filename)."),
    force: bool = typer.Option(False, "--force", help="Replace an existing preset file."),
):
    output_path = resolve_preset_path(output, for_write=True)
    if output_path.exists() and not force:
        typer.echo(
            f"Error: refusing to overwrite existing file: {output_path}; use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=ExitCode.ERROR)

    run_command(run_preset_save, output_path, preset_name, force)


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


async def _read_encoding(application, device):
    from netaudio.commands.config_readback import _read_encoding_status

    return await _read_encoding_status(application, device)


async def _read_latency(device):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict) or settings.get("active_latency_ns") is None:
        raise RuntimeError("active latency readback was unavailable")
    return settings["active_latency_ns"]


async def _read_audio_setting(action, application, device):
    if action == "sample_rate":
        return await _read_sample_rate(device)
    if action == "encoding":
        return await _read_encoding(application, device)
    if action == "latency":
        return await _read_latency(device)
    raise ValueError(f"unsupported audio setting: {action}")


async def run_preset_load(application, devices, preset_devices: dict, confirm_destructive: bool) -> None:
    from netaudio._common import readback_after_notification
    from netaudio._common_selection import filter_devices
    from netaudio.cli import state as cli_state
    from netaudio.commands.config_readback import MUTATION_ERRORS
    from netaudio.commands.subscription import reconcile_receiver_subscriptions
    from netaudio.dante.transmitter_channel_name_reconciliation import (
        reconcile_transmitter_channel_names,
    )

    devices = filter_devices(devices)
    if not devices:
        typer.echo("Error: no devices matched the global filters.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

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
            "Error: preset load was refused before sending any changes because these preset devices were not found:",
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
                actions.append(("encoding", config["encoding"], None))
            if "latency" in config:
                actions.append(("latency", config["latency"], None))
            if "preferred_leader" in config:
                actions.append(("preferred_leader", config["preferred_leader"], None))
            if "transmitter_channel_names" in config:
                await device.get_tx_channels()
                available_transmitter_channels = {channel.number for channel in (device.tx_channels or {}).values()}
                for transmitter_channel_number in config["transmitter_channel_names"]:
                    if transmitter_channel_number not in available_transmitter_channels:
                        raise ValueError(f"transmitter channel {transmitter_channel_number} is unavailable")
                actions.append(
                    (
                        "transmitter_channel_names",
                        config["transmitter_channel_names"],
                        None,
                    )
                )
            if "rx_subscriptions" in config:
                await device.get_rx_channels()
                available_receiver_channels = {channel.number for channel in (device.rx_channels or {}).values()}
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

                actions.append(("receiver_subscriptions", desired_sources, None))
            if "interface_mode" in config:
                mode = config["interface_mode"]
                if mode in ("dynamic", "dhcp"):
                    actions.append(("interface", ("dhcp", None), None))
                else:
                    static_configuration = {
                        "dns_server": config["dns_server"],
                        "gateway": config["gateway"],
                        "ip_address": config["ip_address"],
                        "netmask": config["netmask"],
                    }
                    actions.append(("interface", ("static", static_configuration), None))
            plans.append((device_name, server_name, device, config, actions))
        except (*MUTATION_ERRORS, LookupError, TypeError) as exception:
            preflight_errors.append(f"{device_name}: {exception}")

    if preflight_errors:
        typer.echo(
            "Error: preset load was refused before sending any changes:",
            err=True,
        )
        for error in preflight_errors:
            typer.echo(f"  - {error}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

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
                        result = await application.set_sample_rate(
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
                    except MUTATION_ERRORS as exception:
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
                                f"sample rate already {result.observed_sample_rate_hertz} Hz (verified; no write sent)",
                            )
                        )
                    continue
                if action == "receiver_subscriptions":
                    try:
                        result = await reconcile_receiver_subscriptions(
                            application,
                            device,
                            payload,
                        )
                    except MUTATION_ERRORS as exception:
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
                                f"receiver channel {receiver_channel_number} <- {desired_source[0]}@{desired_source[1]}"
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
                            application,
                            device,
                            payload,
                        )
                    except MUTATION_ERRORS as exception:
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

                try:
                    if action == "preferred_leader":
                        await application.set_preferred_leader(device, payload)
                    elif action == "encoding":
                        await application.set_encoding(device, payload)
                    elif action == "latency":
                        await application.set_latency(device, payload)
                    else:
                        interface_mode, static_configuration = payload
                        await application.set_interface(device, interface_mode, static_configuration)
                except MUTATION_ERRORS as exception:
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
                        partial(_read_audio_setting, action, application, device),
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
                    result = await readback_after_notification(
                        lambda device=device: _read_preferred_leader(application, device),
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
                expected = _expected_interface_config(config)
                result = await readback_after_notification(
                    lambda device=device, expected=expected: _read_interface_config(application, device, expected),
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
        pass

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


@app.command("load", help="Apply a saved preset to the devices it names.")
def preset_load(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be applied without making changes."),
    confirm_destructive: bool = typer.Option(
        False,
        "--confirm-destructive",
        help="Confirm permanent transmitter-flow membership loss caused by sample-rate restoration.",
    ),
):
    preset_path = resolve_preset_path(input_file, for_write=False)
    if not preset_path.exists():
        typer.echo(f"Error: preset not found: {preset_path}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    try:
        preset_name, preset_devices = _parse_preset(preset_path)
    except (ET.ParseError, OSError, ValueError) as exception:
        typer.echo(f"Error: invalid preset {preset_path}: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR) from exception

    typer.echo(f"Preset: {preset_name} ({len(preset_devices)} devices)", err=True)

    if dry_run:
        show_preset_dry_run(preset_devices)
        return

    run_command(run_preset_load, preset_devices, confirm_destructive)


@app.command("show", help="Show what a saved preset would apply, without changing anything.")
def preset_show(
    input_file: str = typer.Argument(..., help=PRESET_REFERENCE_HELP),
):
    preset_load(input_file=input_file, dry_run=True)


@app.command("list", help="List presets saved in the preset directory.")
def preset_list():
    from netaudio._common_output import output_table
    from netaudio.cli import OutputFormat, state

    directory = preset_directory()
    rows = []
    json_data = {}
    for preset_path in sorted(directory.glob("*.xml")) if directory.is_dir() else []:
        try:
            preset_name, preset_devices = _parse_preset(preset_path)
        except (ET.ParseError, OSError, ValueError) as exception:
            typer.echo(f"Warning: skipping {preset_path}: {exception}", err=True)
            continue
        saved_at = datetime.fromtimestamp(preset_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        rows.append([preset_path.stem, preset_name, str(len(preset_devices)), saved_at, str(preset_path)])
        json_data[preset_path.stem] = {
            "device_count": len(preset_devices),
            "devices": sorted(preset_devices),
            "name": preset_name,
            "path": str(preset_path),
            "saved": saved_at,
        }

    if not rows and state.output_format in (OutputFormat.plain, OutputFormat.pretty, OutputFormat.table):
        typer.echo(f"No presets in {directory}.")
        return

    output_table(["Preset", "Name", "Devices", "Saved", "Path"], rows, json_data=json_data)
