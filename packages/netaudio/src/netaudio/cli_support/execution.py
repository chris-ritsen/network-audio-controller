from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from glob import has_magic
from typing import Any, Optional

import typer

from netaudio import DanteDevice
from netaudio.cli_support.context import _get_state
from netaudio.cli_support.selection import filter_devices, select_device
from netaudio.common.app_config import settings
from netaudio.daemon.client import get_devices_from_daemon
from netaudio.dante.application import CapabilityProbeTimeout, DanteApplication
from netaudio.dante.const import DEVICE_ARC_PORT, SERVICE_ARC
from netaudio.dante.state import apply_device_status

__all__ = [
    "CapabilityProbeTimeout",
    "ReadbackResult",
    "ansi",
    "readback_after_notification",
    "run_command",
    "select_device",
]

logger = logging.getLogger("netaudio")


@dataclass(frozen=True)
class ReadbackResult:
    matched: bool
    observed: Any = None
    observed_available: bool = False
    error: Optional[Exception] = None


async def readback_after_notification(
    read: Callable[[], Awaitable[Any]],
    expected: Any,
) -> ReadbackResult:
    try:
        observed = await read()
    except (RuntimeError, OSError, TimeoutError, ValueError) as exception:
        return ReadbackResult(matched=False, error=exception)
    return ReadbackResult(
        matched=observed == expected,
        observed=observed,
        observed_available=True,
    )


def ansi(code: str, text: str) -> str:
    if settings.no_color:
        return str(text)
    return f"\033[{code}m{text}\033[0m"


def _make_dante_application(packet_store=None, session_id=None) -> DanteApplication:
    return DanteApplication(packet_store=packet_store, dissect=_get_state().dissect, session_id=session_id)


async def _discover_with_application(application: DanteApplication) -> dict[str, DanteDevice]:
    devices = await get_devices_from_daemon()
    if devices is not None:
        application.attach_devices(devices)
        return devices
    return await application.discover_and_populate(timeout=settings.mdns_timeout) or {}


def _get_arc_port(device: DanteDevice) -> int:
    if device.services:
        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port", DEVICE_ARC_PORT)
    return DEVICE_ARC_PORT


async def _populate_audio_capabilities(application: DanteApplication, device: DanteDevice) -> None:
    if device.ipv4 is None:
        return
    probes = []
    if device.supported_sample_rates is None:
        probes.append(("supported_sample_rates", application.probe_sample_rate_status, "sample_rate"))
    if device.supported_encodings is None:
        probes.append(("supported_encodings", application.probe_encoding_status, "encoding"))
    for query_name, probe, status_kind in probes:
        try:
            current_value, supported_values = await probe(device.ipv4, timeout=2.0)
        except (RuntimeError, OSError) as exception:
            logger.debug(f"{query_name} unavailable for {device.server_name or device.name}: {exception}")
            device.failed_queries.add(query_name)
            continue
        apply_device_status(device, status_kind, {status_kind: current_value, query_name: supported_values})
        device.failed_queries.discard(query_name)


async def _populate_show_details(application: DanteApplication, device: DanteDevice, include_channels: bool) -> None:
    if device.clock_source_code is None or device.clock_subdomain is None:
        try:
            await application.probe_clocking_status(device)
            device.failed_queries.discard("clock_status")
        except (RuntimeError, OSError) as exception:
            device.failed_queries.add("clock_status")
            logger.debug(f"Clock status unavailable for {device.server_name or device.name}: {exception}")
    if device.aes67_multicast_prefix is None and device.aes67_supported is not False:
        try:
            await application.get_aes67_configured(device)
            device.failed_queries.discard("aes67")
        except (RuntimeError, OSError) as exception:
            device.failed_queries.add("aes67")
            logger.debug(f"AES67 multicast prefix unavailable for {device.server_name or device.name}: {exception}")
    await _populate_audio_capabilities(application, device)
    if include_channels or device.transmitter_flows is None:
        await application.apply_avio_status_pages(device)


async def _load_device_for_show(application: DanteApplication, include_channels: bool) -> tuple[str, DanteDevice]:
    devices = await get_devices_from_daemon()
    if devices is not None:
        application.attach_devices(devices)
        [(server_name, device)] = select_device(filter_devices(devices))
        if include_channels:
            await _populate_controls({server_name: device})
        if device.online:
            await _populate_show_details(application, device, include_channels=include_channels)
        return server_name, device

    state = _get_state()
    show_timeout = settings.mdns_timeout if state.timeout_explicit else min(settings.mdns_timeout, 2.0)
    selected_devices = {}
    literal_name = state.names[0] if len(state.names) == 1 and not has_magic(state.names[0]) else None
    if literal_name is not None:
        exact_devices = await application.discover_named_device(literal_name, timeout=show_timeout)
        exact_candidates = filter_devices(exact_devices, include_names=False)
        await application.populate_device_names(
            exact_candidates,
            request_timeout_milliseconds=500,
            request_attempts=1,
        )
        selected_devices = filter_devices(exact_candidates)
    if not selected_devices:
        discovered_devices = await application.wait_for_discovery(timeout=show_timeout)
        identity_candidates = filter_devices(discovered_devices, include_names=False)
        await application.populate_device_names(
            identity_candidates,
            request_timeout_milliseconds=500,
            request_attempts=1,
        )
        selected_devices = filter_devices(identity_candidates)

    [(server_name, device)] = select_device(selected_devices)
    selected_devices = {server_name: device}
    await application.populate_devices(
        selected_devices,
        timeout=show_timeout,
        include_channels=include_channels,
    )
    [(server_name, device)] = select_device(filter_devices(selected_devices))
    await _populate_show_details(application, device, include_channels=include_channels)
    return server_name, device


def _capture_session():
    state = _get_state()
    if not state.capture and not state.dissect:
        return None, None
    from netaudio._capture import open_capture_session

    store, session_id = open_capture_session()
    if store and session_id:
        typer.echo(f"Capture: recording to session #{session_id}", err=True)
    return store, session_id


@asynccontextmanager
async def _command_context(discover_devices: bool = True):
    store, session_id = _capture_session()
    application = _make_dante_application(packet_store=store, session_id=session_id)
    try:
        await application.startup()
        try:
            devices = {}
            if discover_devices:
                devices = await _discover_with_application(application)
                await _populate_controls(devices)
            yield devices, application
        finally:
            await application.shutdown()
    finally:
        if store:
            store.close()


def run_command(run: Callable[..., Awaitable[Any]], *arguments, discover_devices: bool = True, **options) -> Any:
    async def _run():
        async with _command_context(discover_devices=discover_devices) as (devices, application):
            return await run(application, devices, *arguments, **options)

    return asyncio.run(_run())


def _explicit_selection() -> bool:
    state = _get_state()
    return bool(state.names or state.hosts or state.server_names or state.macs)


def _device_label(device: DanteDevice) -> str:
    return device.server_name or device.name or str(device.ipv4)


def _unreachable_message(device: DanteDevice, reason: object) -> str:
    address = str(device.ipv4) if device.ipv4 else "no address"
    return f"Could not reach {_device_label(device)} ({address}): {reason}"


def _log_unreachable(device: DanteDevice, reason: object) -> None:
    if _explicit_selection():
        logger.warning(_unreachable_message(device, reason))
    else:
        logger.debug(_unreachable_message(device, reason))


def _probe_candidates(devices: dict[str, DanteDevice], probe_name: str) -> dict[str, DanteDevice]:
    explicit = _explicit_selection()
    if explicit:
        devices = filter_devices(devices)
    candidates: dict[str, DanteDevice] = {}
    for server_name, device in devices.items():
        if device.ipv4 is None:
            continue
        if not explicit:
            if not device.online:
                logger.debug(f"Skipping {probe_name} probe for {_device_label(device)}: device is offline")
                continue
            if device.kind == "emulated":
                logger.debug(f"Skipping {probe_name} probe for {_device_label(device)}: device is emulated")
                continue
        candidates[server_name] = device
    return candidates


async def _populate_controls(devices: dict[str, DanteDevice]) -> None:
    explicit = _explicit_selection()
    if explicit:
        devices = filter_devices(devices)
    unpopulated = []
    for device in devices.values():
        if device.tx_channels or device.rx_channels or not device.ipv4:
            continue
        if not device.online and not explicit:
            logger.debug(f"Skipping control population for {_device_label(device)}: device is offline")
            continue
        unpopulated.append(device)

    if not unpopulated:
        return

    population_results = await asyncio.gather(
        *(device.populate_from_core() for device in unpopulated),
        return_exceptions=True,
    )

    for device, result in zip(unpopulated, population_results):
        if not isinstance(result, BaseException):
            continue
        if isinstance(result, asyncio.CancelledError):
            raise result
        logger.warning(_unreachable_message(device, result))
        logger.debug(f"Control population failure for {_device_label(device)}", exc_info=result)
        device.online = False


async def _enrich_clock_fields(
    application: DanteApplication,
    devices: dict[str, DanteDevice],
) -> dict[str, BaseException]:
    missing = {
        server_name: device
        for server_name, device in _probe_candidates(devices, "clock status").items()
        if device.clock_role is None and device.clock_source_code is None
    }
    if not missing:
        return {}
    results = await asyncio.gather(
        *[application.probe_clocking_status(device) for device in missing.values()],
        return_exceptions=True,
    )
    failures: dict[str, BaseException] = {}
    for server_name, result in zip(missing, results):
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, BaseException):
            failures[server_name] = result
            missing[server_name].failed_queries.add("clock_status")
    return failures


async def _enrich_lock_states(
    application: DanteApplication,
    devices: dict[str, DanteDevice],
    *,
    only_unknown: bool = False,
) -> dict[str, BaseException]:
    candidates = {
        server_name: device
        for server_name, device in _probe_candidates(devices, "lock status").items()
        if not only_unknown or device.is_locked is None
    }
    if not candidates:
        return {}

    results = await asyncio.gather(
        *[
            application.probe_lock_status(
                str(device.ipv4),
                timeout=settings.lock_state_timeout,
            )
            for device in candidates.values()
        ],
        return_exceptions=True,
    )
    failures: dict[str, BaseException] = {}
    for server_name, device, result in zip(candidates, candidates.values(), results):
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, BaseException):
            failures[server_name] = result
            device.is_locked = None
            device.failed_queries.add("is_locked")
            continue
        device.is_locked = result.is_locked
        device.failed_queries.discard("is_locked")
    return failures


async def _load_display_devices(
    application: DanteApplication,
    include_channels: bool = False,
) -> dict[str, DanteDevice]:
    devices = filter_devices(await _discover_with_application(application))
    await _populate_controls(devices)
    if include_channels:
        for device in devices.values():
            await _populate_show_details(application, device, include_channels=True)
    unreachable = await _enrich_clock_fields(application, devices)
    for server_name, reason in (await _enrich_lock_states(application, devices, only_unknown=True)).items():
        unreachable.setdefault(server_name, reason)
    for server_name, reason in unreachable.items():
        _log_unreachable(devices[server_name], reason)
    return devices
