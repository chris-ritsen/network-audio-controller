import asyncio

import typer

from netaudio import core
from netaudio._common import ReadbackResult, readback_after_notification
from netaudio._common_output import output_single, output_table
from netaudio._exit_codes import ExitCode
from netaudio.dante.latency import nanoseconds_to_milliseconds
from netaudio.dante.state import apply_device_status

MUTATION_ERRORS = (core.NetaudioCoreError, OSError, RuntimeError, TimeoutError, ValueError)


async def _read_aes67_configured(device):
    configured = await device.operations.get_aes67_configured()
    if configured is None:
        raise RuntimeError("AES67 configured-state readback was unavailable")
    return configured


async def _read_aes67_multicast_prefix(device):
    await device.operations.get_aes67_configured()
    prefix = device.aes67_multicast_prefix
    if prefix is None:
        raise RuntimeError("AES67 multicast prefix readback was unavailable")
    return prefix


def _device_advertises_aes67_multicast_prefix(device) -> bool:
    from netaudio.dante.device import device_advertises_aes67_multicast_prefix

    return device_advertises_aes67_multicast_prefix(device)


async def _read_latency_milliseconds(device):
    settings = await device.operations.get_device_settings()
    if not isinstance(settings, dict):
        raise RuntimeError("active latency readback was unavailable")
    latency_nanoseconds = settings.get("active_latency_ns")
    if latency_nanoseconds is None:
        raise RuntimeError("active latency readback was unavailable")
    return nanoseconds_to_milliseconds(latency_nanoseconds)


async def _read_capability_status(application, device, kind: str, supported_field: str):
    probe = {
        "encoding": application.probe_encoding_status,
        "sample_rate": application.probe_sample_rate_status,
        "sample_rate_pullup": application.probe_sample_rate_pullup_status,
    }[kind]
    current_value, supported_values = await probe(str(device.ipv4))
    apply_device_status(device, kind, {kind: current_value, supported_field: supported_values})
    return current_value, supported_values


async def _read_sample_rate_status_result(application, device):
    return await _read_capability_status(application, device, "sample_rate", "supported_sample_rates")


async def _read_encoding_status_result(application, device):
    return await _read_capability_status(application, device, "encoding", "supported_encodings")


async def _read_encoding_status(application, device):
    current_encoding, _ = await _read_encoding_status_result(application, device)
    return current_encoding


async def _read_sample_rate_pullup_status_result(application, device):
    return await _read_capability_status(
        application,
        device,
        "sample_rate_pullup",
        "supported_sample_rate_pullup_raw_values",
    )


async def _read_sample_rate_pullup_status(application, device):
    current_raw_value, _ = await _read_sample_rate_pullup_status_result(application, device)
    return current_raw_value


async def _collect_target_readings(targets, read_target):
    readings = []
    for server_name, device in targets:
        try:
            await read_target(server_name, device)
            readings.append((server_name, device, None))
        except MUTATION_ERRORS as exception:
            readings.append((server_name, device, exception))
    return readings


def _report_reading_failures(subject, readings):
    for server_name, device, exception in readings:
        if exception is not None:
            typer.echo(
                f"Error: could not read {subject} from {device.name or server_name}: {exception}",
                err=True,
            )
    raise typer.Exit(code=ExitCode.ERROR)


async def _render_cached_reading(targets, all_devices, subject, header, read_target, format_device_value):
    readings = await _collect_target_readings(targets, read_target)
    if readings and all(exception is not None for _, _, exception in readings):
        _report_reading_failures(subject, readings)
    if all_devices:
        output_table(
            ["Name", header],
            [
                [
                    device.name or server_name,
                    format_device_value(device) if exception is None else "unavailable",
                ]
                for server_name, device, exception in readings
            ],
        )
    else:
        server_name, device, exception = readings[0]
        if exception is not None:
            _report_reading_failures(subject, readings[:1])
        output_single(format_device_value(device))


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


def _readback_from_status(status, expected) -> ReadbackResult:
    if status is None:
        return ReadbackResult(matched=False)
    observed_value, _ = status
    return ReadbackResult(matched=observed_value == expected, observed=observed_value, observed_available=True)


async def _send_verified_change(targets, mutate_for, expected, action, success_message, read_for=None):
    async def _send_and_read(server_name, device):
        label = device.name or server_name
        try:
            status = await mutate_for(device)
        except MUTATION_ERRORS as exception:
            return label, None, exception
        if read_for is None:
            return label, _readback_from_status(status, expected), None
        return label, await readback_after_notification(lambda: read_for(device), expected), None

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
        except MUTATION_ERRORS as exception:
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
