from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import subprocess
import sys
from typing import Optional

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.cli_support.execution import CapabilityProbeTimeout, run_command
from netaudio.cli_support.output import output_single, output_table
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.commands.config.latency import run_latency
from netaudio.dante.clock_config import (
    format_clock_subdomain,
    parse_clock_subdomain_selection,
)
from netaudio.dante.sample_rate_pullup import (
    format_supported_sample_rate_pullup_values,
    parse_sample_rate_pullup_selection,
    sample_rate_pullup_label,
)
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyError,
    SampleRateTopologyMutationOutcomeUnknownError,
)

app = typer.Typer(help="Get or set device configuration.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)

top_app = typer.Typer(
    help="Manage netaudio configuration.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)

VALID_SAMPLE_RATES = [44100, 48000, 88200, 96000, 176400, 192000]
VALID_ENCODINGS = [16, 24, 32]

MOVED_COMMANDS = ["sample-rate", "encoding", "latency", "aes67"]


def _moved_command(name: str):
    def handler(ctx: typer.Context):
        typer.echo(f"This command has moved. Use: netaudio device config {name}", err=True)
        raise typer.Exit(code=1)

    return handler


for _name in MOVED_COMMANDS:
    top_app.command(
        _name,
        hidden=True,
        help=f"Moved to 'netaudio device config {_name}'.",
        context_settings={"allow_extra_args": True, "allow_interspersed_args": False},
    )(_moved_command(_name))


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
    typer.echo(f"path = {config_path}")
    if not config_path.exists():
        typer.echo("(no configuration file; defaults apply)")
        return

    try:
        configuration = effective_configuration(config_path)
    except ValueError as exception:
        typer.echo(f"Error: {exception}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    for key, value in _flatten_configuration(configuration):
        typer.echo(f"{key} = {_format_configuration_value(key, value)}")


from netaudio.commands.config.readback import (
    MUTATION_ERRORS,
    _collect_target_readings,
    _device_advertises_aes67_multicast_prefix,
    _read_aes67_configured,
    _read_aes67_multicast_prefix,
    _read_sample_rate_pullup_status,
    _read_sample_rate_pullup_status_result,
    _render_cached_reading,
    _report_reading_failures,
    _send_requested_change,
    _send_verified_change,
    _targets_supporting_value,
)


def _sample_rate_rows(label, result, exception):
    rows = []
    if result is not None:
        preflight = result.preflight
        if result.changed:
            rows.append(
                [
                    label,
                    "Verified",
                    f"{preflight.current_sample_rate_hertz} -> {result.observed_sample_rate_hertz} Hz; "
                    f"{result.resulting_snapshot.capacity.transmit_channel_count} TX / "
                    f"{result.resulting_snapshot.capacity.receive_channel_count} RX",
                ]
            )
        else:
            rows.append(
                [
                    label,
                    "Unchanged",
                    f"fresh readback already reports {result.observed_sample_rate_hertz} Hz; no write sent",
                ]
            )
    else:
        preflight = exception.preflight if isinstance(exception, SampleRateTopologyError) else None
        if isinstance(exception, SampleRateTopologyChangedButUnverifiedError):
            outcome = "Changed but unverified"
        elif isinstance(exception, SampleRateTopologyMutationOutcomeUnknownError):
            outcome = "Outcome unknown"
        else:
            outcome = "Refused"
        rows.append([label, outcome, str(exception)])
    if preflight is None:
        return rows
    if preflight.target_capacity is not None:
        rows.append(
            [
                label,
                "Target capacity",
                f"{preflight.target_capacity.transmit_channel_count} TX / "
                f"{preflight.target_capacity.receive_channel_count} RX at "
                f"{preflight.target_sample_rate_hertz} Hz",
            ]
        )
    for subscription in preflight.reversible_receiver_clipping:
        rows.append(
            [
                label,
                "Reversible RX clipping",
                f"RX {subscription.receiver_channel_number} {subscription.receiver_channel_name} <- "
                f"{subscription.transmitter_channel_name}@{subscription.transmitter_device_name}",
            ]
        )
    for membership_loss in preflight.destructive_transmitter_membership_loss:
        removed = ",".join(str(value) for value in membership_loss.removed_channel_members)
        retained = ",".join(str(value) for value in membership_loss.retained_channel_members)
        rows.append(
            [
                label,
                "Destructive TX membership loss",
                f"flow {membership_loss.flow_number}: remove {removed}; retain {retained}",
            ]
        )
    for uncharacterized_flow in preflight.uncharacterized_transmitter_flows:
        rows.append(
            [
                label,
                "Uncharacterized TX flow",
                f"flow {uncharacterized_flow.flow_number}: {uncharacterized_flow.reason}",
            ]
        )
    if (
        preflight.topology_characterized
        and not preflight.reversible_receiver_clipping
        and not preflight.destructive_transmitter_membership_loss
        and not preflight.uncharacterized_transmitter_flows
    ):
        rows.append([label, "Topology", "no active subscription or flow member is out of range"])
    return rows


def _sample_rate_error_data(exception) -> dict:
    error_data = {"success": False, "error": str(exception)}
    if isinstance(exception, SampleRateTopologyChangedButUnverifiedError):
        error_data["change_sent"] = True
        error_data["state_verified"] = False
        error_data["observed_sample_rate_hertz"] = exception.observed_sample_rate_hertz
    elif isinstance(exception, SampleRateTopologyMutationOutcomeUnknownError):
        error_data["mutation_attempted"] = True
        error_data["state_verified"] = False
    if isinstance(exception, SampleRateTopologyError) and exception.preflight is not None:
        error_data["preflight"] = exception.preflight.to_dict()
    return error_data


async def run_sample_rate(application, devices, rate: int | None, all_devices: bool, confirm_destructive: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

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

    async def change_target(server_name, device):
        try:
            result = await application.set_sample_rate(
                device,
                rate,
                confirm_destructive=confirm_destructive,
            )
            return server_name, device, result, None
        except (*MUTATION_ERRORS, SampleRateTopologyError) as exception:
            return server_name, device, None, exception

    outcomes = await asyncio.gather(*(change_target(server_name, device) for server_name, device in targets))
    rows = []
    json_data = {}
    failures = 0
    for server_name, device, result, exception in outcomes:
        label = device.name or server_name
        if result is not None:
            json_data[server_name] = result.to_dict()
        else:
            failures += 1
            json_data[server_name] = _sample_rate_error_data(exception)
        rows.extend(_sample_rate_rows(label, result, exception))
    output_table(["Device", "Result", "Detail"], rows, json_data=json_data)
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("sample-rate")
def sample_rate(
    rate: Optional[int] = typer.Argument(None, help=f"Sample rate: {VALID_SAMPLE_RATES}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
    confirm_destructive: bool = typer.Option(
        False,
        "--confirm-destructive",
        help="Confirm permanent removal of out-of-capacity transmitter flow members.",
    ),
):
    """Get or set the sample rate."""
    run_command(run_sample_rate, rate, all_devices, confirm_destructive)


def _pullup_cell(exception, value):
    if exception is None:
        return value
    if isinstance(exception, CapabilityProbeTimeout):
        return "unsupported"
    return "unavailable"


async def _render_sample_rate_pullup(application, targets, all_devices: bool) -> None:
    readings = await _collect_target_readings(
        targets,
        lambda server_name, device: _read_sample_rate_pullup_status_result(application, device),
    )
    hard_failures = [
        reading for reading in readings if reading[2] is not None and not isinstance(reading[2], CapabilityProbeTimeout)
    ]
    if hard_failures and len(hard_failures) == len(readings):
        _report_reading_failures("sample-rate pull-up", hard_failures)

    if all_devices:
        output_table(
            ["Name", "Applied", "Requested", "Supported"],
            [
                [
                    device.name or server_name,
                    _pullup_cell(exception, sample_rate_pullup_label(device.sample_rate_pullup_raw_value)),
                    _pullup_cell(exception, sample_rate_pullup_label(device.requested_sample_rate_pullup_raw_value)),
                    _pullup_cell(
                        exception,
                        format_supported_sample_rate_pullup_values(device.supported_sample_rate_pullup_raw_values),
                    ),
                ]
                for server_name, device, exception in readings
            ],
        )
        return
    device = readings[0][1]
    exception = readings[0][2]
    if exception is not None:
        if isinstance(exception, CapabilityProbeTimeout):
            output_single("unsupported")
            return
        _report_reading_failures("sample-rate pull-up", readings[:1])
    output_single(sample_rate_pullup_label(device.sample_rate_pullup_raw_value))


async def run_sample_rate_pullup(application, devices, selection: str | None, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

    if selection is None:
        await _render_sample_rate_pullup(application, targets, all_devices)
        return

    try:
        raw_value = parse_sample_rate_pullup_selection(selection)
    except ValueError as exception:
        typer.echo(f"Error: {exception}.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    for server_name, device in targets:
        if device.supported_sample_rate_pullup_raw_values is None:
            try:
                await _read_sample_rate_pullup_status_result(application, device)
            except MUTATION_ERRORS as exception:
                typer.echo(
                    f"Error: sample-rate pull-up capabilities are unavailable for {device.name or server_name}: "
                    f"{exception}",
                    err=True,
                )
                raise typer.Exit(code=ExitCode.ERROR)

    supported_targets, capability_failures = _targets_supporting_value(
        targets,
        raw_value,
        "supported_sample_rate_pullup_raw_values",
        (),
        "sample-rate pull-up",
    )
    if not supported_targets:
        raise typer.Exit(code=ExitCode.ERROR)

    failures = await _send_verified_change(
        supported_targets,
        lambda device: application.set_sample_rate_pullup(device, raw_value),
        raw_value,
        "sample-rate pull-up change",
        lambda label: f"Set sample-rate pull-up for {label}: {sample_rate_pullup_label(raw_value)} (verified)",
        read_for=lambda device: _read_sample_rate_pullup_status(application, device),
    )
    if failures + capability_failures:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("sample-rate-pullup")
def sample_rate_pullup(
    selection: Optional[str] = typer.Argument(
        None,
        help="none, +4.1667%, +0.1%, -0.1%, -4.0%, or a raw integer.",
    ),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set sample-rate pull-up/down."""
    run_command(run_sample_rate_pullup, selection, all_devices)


async def run_encoding(application, devices, bits: int | None, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

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

    failures = await _send_verified_change(
        supported_targets,
        lambda device: application.set_encoding(device, bits),
        bits,
        "encoding change",
        lambda label: f"Set encoding for {label}: {bits}-bit (verified)",
    )
    if failures + capability_failures:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command()
def encoding(
    bits: Optional[int] = typer.Argument(None, help=f"Encoding bit depth: {VALID_ENCODINGS}"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the encoding bit depth."""
    run_command(run_encoding, bits, all_devices)


@app.command()
def latency(
    value: Optional[float] = typer.Argument(None, help="Latency in milliseconds."),
    all_devices: bool = typer.Option(False, "--all", help="Read or apply to all devices."),
):
    """Get the complete device latency state or set and verify latency."""
    run_command(run_latency, value, all_devices)


def _aes67_state_label(value):
    if value is None:
        return "N/A"
    return "on" if value else "off"


def _aes67_support_label(value):
    if value is None:
        return "unknown"
    return "yes" if value else "no"


def _aes67_reboot_required(device):
    if device.aes67_current is not None and device.aes67_configured is not None:
        return device.aes67_current != device.aes67_configured
    return False


async def _set_aes67_multicast_prefix(application, targets, multicast_prefix: str) -> None:
    try:
        requested_multicast_prefix = str(ipaddress.IPv4Address(multicast_prefix))
    except (ipaddress.AddressValueError, ValueError):
        typer.echo("Error: AES67 multicast prefix must be an IPv4 address.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)
    prefix_targets = []
    capability_failures = 0
    for server_name, device in targets:
        if not _device_advertises_aes67_multicast_prefix(device):
            typer.echo(
                f"Error: {device.name or server_name} does not advertise an AES67 multicast prefix.",
                err=True,
            )
            capability_failures += 1
        else:
            prefix_targets.append((server_name, device))
    failures = await _send_verified_change(
        prefix_targets,
        lambda device: application.set_aes67_multicast_prefix(device, requested_multicast_prefix),
        requested_multicast_prefix,
        "AES67 multicast prefix change",
        lambda label: f"Set AES67 multicast prefix for {label}: {requested_multicast_prefix} (verified)",
        read_for=lambda device: _read_aes67_multicast_prefix(application, device),
    )
    if failures + capability_failures:
        raise typer.Exit(code=ExitCode.ERROR)


async def _render_aes67(application, targets, all_devices: bool) -> None:
    if all_devices:
        headers = ["Name", "Supported", "Current", "Configured", "Multicast Prefix", "Reboot Required"]
        rows = [
            [
                device.name or server_name,
                _aes67_support_label(device.aes67_supported),
                _aes67_state_label(device.aes67_current),
                _aes67_state_label(device.aes67_configured),
                device.aes67_multicast_prefix or "",
                "yes" if _aes67_reboot_required(device) else "no",
            ]
            for server_name, device in targets
        ]
        output_table(headers, rows)
        return
    server_name, device = targets[0]
    if device.aes67_supported is False:
        output_single("unsupported")
        return
    if device.aes67_multicast_prefix is None:
        try:
            await application.get_aes67_configured(device)
        except MUTATION_ERRORS as exception:
            typer.echo(
                f"Warning: could not read AES67 configuration from {device.name or server_name}: {exception}",
                err=True,
            )
    current_label = _aes67_state_label(device.aes67_current)
    configured_label = _aes67_state_label(device.aes67_configured)
    if device.aes67_multicast_prefix:
        typer.echo(f"multicast prefix: {device.aes67_multicast_prefix}")
    if device.aes67_current is None and device.aes67_configured is not None:
        output_single(configured_label)
    elif device.aes67_current is not None and device.aes67_current == device.aes67_configured:
        output_single(current_label)
    elif device.aes67_current is None and device.aes67_configured is None:
        output_single("N/A")
    else:
        typer.echo(f"current: {current_label}", err=False)
        typer.echo(f"configured: {configured_label}", err=False)
        if _aes67_reboot_required(device):
            typer.echo("reboot required", err=True)


async def run_aes67(
    application,
    devices,
    enabled: str | None,
    multicast_prefix: str | None,
    all_devices: bool,
) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

    if multicast_prefix is not None:
        await _set_aes67_multicast_prefix(application, targets, multicast_prefix)
        if enabled is None:
            return

    if enabled is None:
        await _render_aes67(application, targets, all_devices)
        return

    if enabled.lower() not in ("on", "off"):
        typer.echo("Error: expected 'on' or 'off'.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    is_enabled = enabled.lower() == "on"
    supported_targets = []
    capability_failures = 0
    for server_name, device in targets:
        if device.aes67_supported is False:
            typer.echo(
                f"Error: {device.name or server_name} does not support AES67 configuration.",
                err=True,
            )
            capability_failures += 1
        else:
            supported_targets.append((server_name, device))

    failures = await _send_verified_change(
        supported_targets,
        lambda device: application.set_aes67_enabled(device, is_enabled),
        is_enabled,
        "AES67 configuration change",
        lambda label: f"Set AES67 configured state for {label}: {enabled.lower()} (verified)",
        read_for=lambda device: _read_aes67_configured(application, device),
    )
    if failures + capability_failures:
        raise typer.Exit(code=ExitCode.ERROR)


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
    run_command(run_aes67, enabled, multicast_prefix, all_devices)


def _preferred_leader_label(device) -> str:
    if device.preferred_leader is None:
        return "N/A"
    return "on" if device.preferred_leader else "off"


async def run_preferred_leader(application, devices, enabled: str | None, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

    if enabled is None:
        if all_devices:
            output_table(
                ["Name", "Preferred Leader"],
                [[device.name or server_name, _preferred_leader_label(device)] for server_name, device in targets],
            )
        else:
            output_single(_preferred_leader_label(targets[0][1]))
        return

    if enabled.lower() not in ("on", "off"):
        typer.echo("Error: expected 'on' or 'off'.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    is_preferred = enabled.lower() == "on"
    failures = await _send_requested_change(
        targets,
        lambda device: application.set_preferred_leader(device, is_preferred),
        "preferred leader change",
        lambda label: f"Preferred leader change requested for {label}: {enabled.lower()}; not verified.",
    )
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("preferred-leader")
def preferred_leader(
    enabled: Optional[str] = typer.Argument(None, help="on or off"),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set preferred leader mode."""
    run_command(run_preferred_leader, enabled, all_devices)


async def _read_clock_subdomain(application, device):
    parsed = await application.probe_clocking_status(device)
    clock_subdomain = parsed.get("clock_subdomain")
    if clock_subdomain is None:
        raise RuntimeError("clock subdomain readback was unavailable")
    return bytes(clock_subdomain)


@app.command("clock-source")
def clock_source(
    selection: Optional[str] = typer.Argument(None, help="Not implemented."),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set clock source."""

    if selection is None:
        output_single("not implemented")
        return
    typer.echo(
        "Error: clock source is not implemented; Controller labels for this field are unnamed.",
        err=True,
    )
    raise typer.Exit(code=ExitCode.ERROR)


async def run_clock_subdomain(application, devices, selection: str | None, all_devices: bool) -> None:
    targets = select_device(filter_devices(devices), allow_many=all_devices)

    if selection is None:

        async def _read_target(server_name, device):
            if device.clock_subdomain is None:
                await _read_clock_subdomain(application, device)

        await _render_cached_reading(
            targets,
            all_devices,
            "clock subdomain",
            "Clock Subdomain",
            _read_target,
            lambda device: format_clock_subdomain(device.clock_subdomain),
        )
        return

    try:
        requested_subdomain = parse_clock_subdomain_selection(selection)
    except ValueError as exception:
        typer.echo(f"Error: {exception}.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    failures = await _send_verified_change(
        targets,
        lambda device: application.set_clock_subdomain(device, requested_subdomain),
        requested_subdomain,
        "clock subdomain change",
        lambda label: f"Set clock subdomain for {label}: {format_clock_subdomain(requested_subdomain)} (verified)",
        read_for=lambda device: _read_clock_subdomain(application, device),
    )
    if failures:
        raise typer.Exit(code=ExitCode.ERROR)


@app.command("clock-subdomain")
def clock_subdomain(
    selection: Optional[str] = typer.Argument(
        None,
        help="ASCII name, hex:<bytes>, or unset.",
    ),
    all_devices: bool = typer.Option(False, "--all", help="Apply to all devices."),
):
    """Get or set the PTP subdomain name."""
    run_command(run_clock_subdomain, selection, all_devices)


from netaudio.commands.config.interface import interface

app.command("interface")(interface)
