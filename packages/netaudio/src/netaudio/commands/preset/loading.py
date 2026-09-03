from __future__ import annotations

import math
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Awaitable, Callable

import typer

from netaudio._exit_codes import ExitCode
from netaudio.cli_support.execution import readback_after_notification
from netaudio.cli_support.selection import filter_devices
from netaudio.commands.config.readback import MUTATION_ERRORS
from netaudio.commands.preset.parsing import INTERFACE_MODES, STATIC_INTERFACE_FIELDS
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyMutationOutcomeUnknownError,
)

UNSUPPORTED_LOAD_FIELDS = {
    "additional_interfaces": "additional network interfaces",
}


@dataclass
class MatchedPresetDevice:
    config: dict[str, Any]
    device: Any
    device_name: str
    server_name: str


@dataclass
class PresetAction:
    kind: str
    payload: Any


@dataclass
class PresetDeviceActions:
    actions: list[PresetAction]
    config: dict[str, Any]
    device: Any
    device_name: str
    server_name: str


@dataclass
class PresetLoadPlan:
    device_actions: list[PresetDeviceActions]


@dataclass
class PresetLoadReport:
    failures: int = 0
    needs_reboot: list[str] = field(default_factory=list)
    results: list[tuple[str, str]] = field(default_factory=list)

    def record(self, device_name: str, text: str, *, failed: bool = False) -> None:
        if failed:
            self.failures += 1
        self.results.append((device_name, text))


@dataclass
class PresetLoadContext:
    application: Any
    confirm_destructive: bool
    report: PresetLoadReport


ActionHandler = Callable[[PresetLoadContext, PresetDeviceActions, PresetAction], Awaitable[None]]


def _refuse(lines: list[str]) -> None:
    for line in lines:
        typer.echo(line, err=True)
    raise typer.Exit(code=ExitCode.ERROR)


def _match_preset_devices(devices: dict, preset_devices: dict[str, dict[str, Any]]) -> list[MatchedPresetDevice]:
    from netaudio.cli import state as cli_state

    devices = filter_devices(devices)
    if not devices:
        _refuse(["Error: no devices matched the global filters."])
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
            _refuse([f"Error: preset device name {device_name!r} is ambiguous: {servers}"])
        server_name, device = candidates[0]
        matched.append(
            MatchedPresetDevice(config=config, device=device, device_name=device_name, server_name=server_name)
        )
    filters_active = bool(cli_state.names or cli_state.hosts or cli_state.server_names or cli_state.macs)
    if unmatched_preset_names and not filters_active:
        _refuse(
            [
                "Error: preset load was refused before sending any changes because these preset devices were not found:",
                *(f"  - {device_name}" for device_name in unmatched_preset_names),
                "Use global device filters to intentionally load only a selected subset.",
            ]
        )
    if not matched:
        _refuse(["Error: no selected devices have matching entries in this preset."])
    return matched


def _unsupported_load_fields(config: dict) -> list[str]:
    return [label for field_name, label in UNSUPPORTED_LOAD_FIELDS.items() if field_name in config]


def _validate_sample_rate(device_name: str, config: dict, device) -> None:
    from netaudio.commands.config.cli import VALID_SAMPLE_RATES

    sample_rate = config.get("sample_rate")
    if sample_rate is None:
        return
    if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or not 1 <= sample_rate <= 0xFFFFFFFF:
        raise ValueError(f"{device_name}: sample rate must be an integer from 1 through 4294967295")
    supported_sample_rates = device.supported_sample_rates
    if supported_sample_rates is None and sample_rate not in VALID_SAMPLE_RATES:
        raise ValueError(
            f"{device_name}: sample-rate capabilities are unavailable; cannot verify that {sample_rate} is supported"
        )
    if supported_sample_rates is not None and sample_rate not in supported_sample_rates:
        raise ValueError(
            f"{device_name}: device reports supported sample rates {supported_sample_rates}; "
            f"{sample_rate} is not supported"
        )


def _validate_encoding(device_name: str, config: dict, device) -> None:
    encoding = config.get("encoding")
    if encoding is None:
        return
    if isinstance(encoding, bool) or not isinstance(encoding, int) or not 1 <= encoding <= 0xFFFFFFFF:
        raise ValueError(f"{device_name}: encoding must be an integer from 1 through 4294967295")
    supported_encodings = device.supported_encodings
    if supported_encodings is None:
        raise ValueError(f"{device_name}: encoding capabilities are unavailable")
    if encoding not in supported_encodings:
        raise ValueError(
            f"{device_name}: device reports supported encodings {supported_encodings}; {encoding} is not supported"
        )


def _validate_latency(device_name: str, config: dict) -> None:
    latency = config.get("latency")
    if latency is not None and (
        isinstance(latency, bool) or not isinstance(latency, (int, float)) or not math.isfinite(latency) or latency < 0
    ):
        raise ValueError(f"{device_name}: latency must be a finite, nonnegative number")


def _validate_interface(device_name: str, config: dict) -> None:
    mode = config.get("interface_mode")
    if mode is not None and mode not in INTERFACE_MODES:
        raise ValueError(f"{device_name}: unsupported interface mode {mode!r}")
    if mode == "static":
        missing = [option for option, field_name in STATIC_INTERFACE_FIELDS if not config.get(field_name)]
        if missing:
            raise ValueError(f"{device_name}: static interface is missing {', '.join(missing)}")


def _validate_supported_config(device_name: str, config: dict, device) -> None:
    _validate_sample_rate(device_name, config, device)
    _validate_encoding(device_name, config, device)
    _validate_latency(device_name, config)
    _validate_interface(device_name, config)


def _plan_audio_actions(config: dict) -> list[PresetAction]:
    return [
        PresetAction(kind=kind, payload=config[kind])
        for kind in ("sample_rate", "encoding", "latency", "preferred_leader")
        if kind in config
    ]


async def _plan_transmitter_channel_names(device, config: dict) -> PresetAction:
    await device.get_tx_channels()
    available_transmitter_channels = {channel.number for channel in (device.tx_channels or {}).values()}
    for transmitter_channel_number in config["transmitter_channel_names"]:
        if transmitter_channel_number not in available_transmitter_channels:
            raise ValueError(f"transmitter channel {transmitter_channel_number} is unavailable")
    return PresetAction(kind="transmitter_channel_names", payload=config["transmitter_channel_names"])


async def _plan_receiver_subscriptions(device, device_name: str, config: dict) -> PresetAction:
    await device.get_rx_channels()
    available_receiver_channels = {channel.number for channel in (device.rx_channels or {}).values()}
    desired_sources = {}
    for receiver_channel_number, subscription in config["rx_subscriptions"].items():
        if receiver_channel_number not in available_receiver_channels:
            raise ValueError(f"receiver channel {receiver_channel_number} is unavailable")
        if subscription is None:
            desired_sources[receiver_channel_number] = None
            continue
        transmitter_device_name = subscription["tx_device"]
        if transmitter_device_name == ".":
            transmitter_device_name = device_name
        desired_sources[receiver_channel_number] = (subscription["tx_channel"], transmitter_device_name)
    return PresetAction(kind="receiver_subscriptions", payload=desired_sources)


def _plan_interface_action(config: dict) -> PresetAction:
    mode = config["interface_mode"]
    if mode in ("dynamic", "dhcp"):
        return PresetAction(kind="interface", payload=("dhcp", None))
    static_configuration = {
        "dns_server": config["dns_server"],
        "gateway": config["gateway"],
        "ip_address": config["ip_address"],
        "netmask": config["netmask"],
    }
    return PresetAction(kind="interface", payload=("static", static_configuration))


async def _plan_device_actions(matched: MatchedPresetDevice) -> PresetDeviceActions:
    config = matched.config
    _validate_supported_config(matched.device_name, config, matched.device)
    actions = _plan_audio_actions(config)
    if "transmitter_channel_names" in config:
        actions.append(await _plan_transmitter_channel_names(matched.device, config))
    if "rx_subscriptions" in config:
        actions.append(await _plan_receiver_subscriptions(matched.device, matched.device_name, config))
    if "interface_mode" in config:
        actions.append(_plan_interface_action(config))
    return PresetDeviceActions(
        actions=actions,
        config=config,
        device=matched.device,
        device_name=matched.device_name,
        server_name=matched.server_name,
    )


async def _build_plan(matched_devices: list[MatchedPresetDevice]) -> PresetLoadPlan:
    preflight_errors = []
    device_actions = []
    for matched in matched_devices:
        unsupported = _unsupported_load_fields(matched.config)
        if unsupported:
            preflight_errors.append(f"{matched.device_name}: unsupported fields: {', '.join(unsupported)}")
            continue
        try:
            device_actions.append(await _plan_device_actions(matched))
        except (*MUTATION_ERRORS, LookupError, TypeError) as exception:
            preflight_errors.append(f"{matched.device_name}: {exception}")
    if preflight_errors:
        _refuse(
            [
                "Error: preset load was refused before sending any changes:",
                *(f"  - {error}" for error in preflight_errors),
            ]
        )
    return PresetLoadPlan(device_actions=device_actions)


async def _read_sample_rate(application, device):
    settings = await application.get_device_settings(device)
    if not isinstance(settings, dict) or settings.get("sample_rate") is None:
        raise RuntimeError("sample-rate readback was unavailable")
    return settings["sample_rate"]


async def _read_encoding(application, device):
    from netaudio.commands.config.readback import _read_encoding_status

    return await _read_encoding_status(application, device)


async def _read_latency(application, device):
    settings = await application.get_device_settings(device)
    if not isinstance(settings, dict) or settings.get("active_latency_ns") is None:
        raise RuntimeError("active latency readback was unavailable")
    return settings["active_latency_ns"]


async def _read_audio_setting(action, application, device):
    if action == "sample_rate":
        return await _read_sample_rate(application, device)
    if action == "encoding":
        return await _read_encoding(application, device)
    if action == "latency":
        return await _read_latency(application, device)
    raise ValueError(f"unsupported audio setting: {action}")


async def _read_preferred_leader(application, device):
    state = await application.probe_preferred_leader_state(device, timeout=1.0)
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
    interfaces = await application.probe_interface_status(device, timeout=1.0)
    if not interfaces:
        raise RuntimeError("interface readback was unavailable")
    reported = device.interface_pending_config or interfaces[0]
    return {field_name: reported.get(field_name) for field_name in expected}


async def _apply_sample_rate(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    report = context.report
    try:
        result = await context.application.set_sample_rate(
            entry.device, action.payload, confirm_destructive=context.confirm_destructive
        )
    except SampleRateTopologyChangedButUnverifiedError as exception:
        report.record(entry.device_name, f"sample rate: CHANGED BUT UNVERIFIED ({exception})", failed=True)
        return
    except SampleRateTopologyMutationOutcomeUnknownError as exception:
        report.record(entry.device_name, f"sample rate: MUTATION OUTCOME UNKNOWN ({exception})", failed=True)
        return
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"sample rate: REFUSED ({exception})", failed=True)
        return
    if result.changed:
        report.record(entry.device_name, f"sample rate {result.observed_sample_rate_hertz} Hz and topology (verified)")
    else:
        report.record(
            entry.device_name,
            f"sample rate already {result.observed_sample_rate_hertz} Hz (verified; no write sent)",
        )


async def _apply_receiver_subscriptions(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    from netaudio.commands.subscription import reconcile_receiver_subscriptions

    report = context.report
    try:
        result = await reconcile_receiver_subscriptions(context.application, entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"receiver subscriptions: FAILED ({exception})", failed=True)
        return
    if result.unchanged and not result.verified and not result.failures:
        report.record(entry.device_name, f"receiver subscriptions already match ({len(result.unchanged)} channels)")
    for receiver_channel_number, desired_source in sorted(result.verified.items()):
        if desired_source is None:
            description = f"receiver channel {receiver_channel_number} unsubscribed"
        else:
            description = f"receiver channel {receiver_channel_number} <- {desired_source[0]}@{desired_source[1]}"
        report.record(entry.device_name, f"{description} (verified)")
    for receiver_channel_number, detail in sorted(result.failures.items()):
        report.record(entry.device_name, f"receiver channel {receiver_channel_number}: FAILED ({detail})", failed=True)


async def _apply_transmitter_channel_names(
    context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction
) -> None:
    from netaudio.dante.transmitter_channel_name_reconciliation import reconcile_transmitter_channel_names

    report = context.report
    try:
        result = await reconcile_transmitter_channel_names(context.application, entry.device, action.payload)
    except MUTATION_ERRORS as exception:
        report.record(entry.device_name, f"transmitter channel names: FAILED ({exception})", failed=True)
        return
    if result.unchanged and not result.verified and not result.failures:
        report.record(entry.device_name, f"transmitter channel names already match ({len(result.unchanged)} channels)")
    for transmitter_channel_number, channel_name in sorted(result.verified.items()):
        report.record(entry.device_name, f"transmitter channel {transmitter_channel_number}: {channel_name} (verified)")
    for transmitter_channel_number, detail in sorted(result.failures.items()):
        report.record(
            entry.device_name, f"transmitter channel {transmitter_channel_number}: FAILED ({detail})", failed=True
        )


async def _send_request(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> bool:
    application = context.application
    try:
        if action.kind == "preferred_leader":
            await application.set_preferred_leader(entry.device, action.payload)
        elif action.kind == "encoding":
            await application.set_encoding(entry.device, action.payload)
        elif action.kind == "latency":
            await application.set_latency(entry.device, action.payload)
        else:
            interface_mode, static_configuration = action.payload
            await application.set_interface(entry.device, interface_mode, static_configuration)
    except MUTATION_ERRORS as exception:
        action_label = action.kind.replace("_", " ")
        context.report.record(entry.device_name, f"{action_label}: FAILED to send request: {exception}", failed=True)
        return False
    return True


async def _apply_audio_setting(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    if action.kind == "encoding":
        expected = entry.config["encoding"]
        success = f"encoding {expected}-bit"
    else:
        expected = int(round(entry.config["latency"] * 1_000_000))
        success = f"latency {entry.config['latency']:g} ms"
    result = await readback_after_notification(
        partial(_read_audio_setting, action.kind, context.application, entry.device), expected
    )
    if result.matched:
        context.report.record(entry.device_name, f"{success} (verified)")
        return
    if result.observed_available:
        detail = f"device reports {result.observed!r}"
    else:
        detail = f"fresh readback was unavailable: {result.error}"
    context.report.record(entry.device_name, f"{success}: FAILED ({detail})", failed=True)


async def _apply_preferred_leader(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    expected = entry.config["preferred_leader"]
    enabled = "on" if expected else "off"
    result = await readback_after_notification(
        lambda device=entry.device: _read_preferred_leader(context.application, device), expected
    )
    if result.matched:
        context.report.record(entry.device_name, f"preferred leader {enabled} (verified)")
    elif result.observed_available:
        context.report.record(
            entry.device_name, f"preferred leader {enabled}: FAILED (device reports {result.observed!r})", failed=True
        )
    else:
        detail = f": {result.error}" if result.error is not None else ""
        context.report.record(
            entry.device_name,
            f"preferred leader {enabled} requested; not verified (fresh readback unavailable{detail})",
        )


async def _apply_interface(context: PresetLoadContext, entry: PresetDeviceActions, action: PresetAction) -> None:
    if not await _send_request(context, entry, action):
        return
    mode = entry.config["interface_mode"]
    expected = _expected_interface_config(entry.config)
    result = await readback_after_notification(
        lambda device=entry.device, expected=expected: _read_interface_config(context.application, device, expected),
        expected,
    )
    if entry.device.interface_pending_config is not None:
        context.report.needs_reboot.append(entry.device_name)
    if result.matched:
        context.report.record(entry.device_name, f"interface {mode} (verified)")
    elif result.observed_available:
        context.report.record(
            entry.device_name,
            f"interface {mode} requested; not verified "
            f"(device currently reports {result.observed!r}; reboot may be pending)",
        )
    else:
        detail = f": {result.error}" if result.error is not None else ""
        context.report.record(
            entry.device_name,
            f"interface {mode} requested; not verified (fresh readback unavailable{detail}; reboot may be pending)",
        )


ACTION_HANDLERS: dict[str, ActionHandler] = {
    "encoding": _apply_audio_setting,
    "interface": _apply_interface,
    "latency": _apply_audio_setting,
    "preferred_leader": _apply_preferred_leader,
    "receiver_subscriptions": _apply_receiver_subscriptions,
    "sample_rate": _apply_sample_rate,
    "transmitter_channel_names": _apply_transmitter_channel_names,
}


async def _apply_plan(context: PresetLoadContext, plan: PresetLoadPlan) -> None:
    for entry in plan.device_actions:
        if not entry.actions:
            context.report.record(entry.device_name, "no supported changes")
            continue
        for action in entry.actions:
            await ACTION_HANDLERS[action.kind](context, entry, action)


def _report_preset_load(report: PresetLoadReport) -> None:
    typer.echo("\nPreset load summary:", err=True)
    for device_name, result in report.results:
        typer.echo(f"  {device_name}: {result}", err=True)
    if report.needs_reboot:
        typer.echo(f"\nReboot required: {', '.join(dict.fromkeys(report.needs_reboot))}", err=True)
    if report.failures:
        raise typer.Exit(code=ExitCode.ERROR)


async def run_preset_load(application, devices, preset_devices: dict, confirm_destructive: bool) -> None:
    matched_devices = _match_preset_devices(devices, preset_devices)
    plan = await _build_plan(matched_devices)
    context = PresetLoadContext(
        application=application, confirm_destructive=confirm_destructive, report=PresetLoadReport()
    )
    await _apply_plan(context, plan)
    _report_preset_load(context.report)
