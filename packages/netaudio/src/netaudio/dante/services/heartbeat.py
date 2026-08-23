from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from netaudio.dante.const import DEVICE_HEARTBEAT_PORT, MULTICAST_GROUP_HEARTBEAT
from netaudio.dante.heartbeat_connection_health import (
    CONNECTION_HEALTH_FRESHNESS_SECONDS,
    ReceiverFlowConnectionHealthTracker,
)
from netaudio.dante.service import DanteMulticastService

logger = logging.getLogger("netaudio")

OFFLINE_THRESHOLD_SECONDS = 15.0
SWEEP_INTERVAL_SECONDS = 5.0


def parse_signal_presence_records(data: bytes) -> list[dict]:
    from netaudio import core

    try:
        records = core.parse_response("signal_presence", data)
    except core.NetaudioCoreError:
        return []
    return records if isinstance(records, list) else []


def parse_heartbeat_device_extended_unique_identifier(data: bytes) -> str | None:
    from netaudio import core

    try:
        identity = core.parse_response("heartbeat_identity", data)
    except core.NetaudioCoreError:
        return None
    if not isinstance(identity, dict):
        return None
    device_extended_unique_identifier = identity.get("device_extended_unique_identifier")
    if not isinstance(device_extended_unique_identifier, str):
        return None
    return device_extended_unique_identifier


def parse_clock_frequency_offset_records(data: bytes) -> list[dict]:
    from netaudio import core

    try:
        records = core.parse_response("heartbeat_clock_frequency_offset", data)
    except core.NetaudioCoreError:
        return []
    return records if isinstance(records, list) else []


def parse_interface_traffic_records(data: bytes) -> list[dict]:
    from netaudio import core

    try:
        records = core.parse_response("heartbeat_interface_traffic", data)
    except core.NetaudioCoreError:
        return []
    return records if isinstance(records, list) else []


def parse_connection_health_records(data: bytes) -> dict | None:
    from netaudio import core

    try:
        records = core.parse_response("heartbeat_connection_health", data)
    except core.NetaudioCoreError:
        return None
    if not isinstance(records, dict):
        return None
    if not records.get("latency_records") and not records.get("raw_impairment_records"):
        return None
    return records


class DanteHeartbeatService(DanteMulticastService):
    def __init__(
        self,
        device_by_ip=None,
        get_devices=None,
        mark_offline=None,
        interface_ip=None,
        on_signal_presence=None,
        on_device_updated=None,
        monotonic_clock=None,
        wall_clock=None,
        connection_health_freshness_seconds=CONNECTION_HEALTH_FRESHNESS_SECONDS,
    ):
        super().__init__(
            multicast_group=MULTICAST_GROUP_HEARTBEAT,
            multicast_port=DEVICE_HEARTBEAT_PORT,
            interface_ip=interface_ip,
        )
        self._device_by_ip = device_by_ip
        self._get_devices = get_devices
        self._mark_offline = mark_offline
        self._on_signal_presence = on_signal_presence
        self._on_device_updated = on_device_updated
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._wall_clock = wall_clock or time.time
        self._previous_interface_traffic_samples: dict[str, tuple[int, float]] = {}
        self._connection_health = ReceiverFlowConnectionHealthTracker(
            freshness_seconds=connection_health_freshness_seconds
        )
        self._heartbeat_devices = {}
        self._connection_health_expiry_handles: dict[str, asyncio.TimerHandle] = {}
        self._sweep_task = None

    async def start(self) -> None:
        await super().start()
        self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        for device_extended_unique_identifier in tuple(self._heartbeat_devices):
            self._discard_device_identity(device_extended_unique_identifier)
        if self._sweep_task:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None
        await super().stop()

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        device_extended_unique_identifier = parse_heartbeat_device_extended_unique_identifier(data)
        if device_extended_unique_identifier is None:
            return
        source_ip = addr[0]
        device = self._device_by_ip(source_ip) if self._device_by_ip else None
        if device:
            observed_monotonic = self._monotonic_clock()
            observed_wall_time = self._wall_clock()
            device_state_changed = self._associate_device_identity(device_extended_unique_identifier, device)
            device.update_last_seen()
            if not device.online:
                logger.info(f"Device back online (heartbeat received): {device.server_name}")
                device.online = True

            clock_records = parse_clock_frequency_offset_records(data)
            if clock_records:
                clock_frequency_offset = clock_records[-1]["clock_frequency_offset_parts_per_billion"]
                if device.clock_frequency_offset_parts_per_billion != clock_frequency_offset:
                    device.clock_frequency_offset_parts_per_billion = clock_frequency_offset
                    device_state_changed = True

            interface_traffic_records = parse_interface_traffic_records(data)
            if interface_traffic_records and self._update_interface_traffic(
                device,
                device_extended_unique_identifier,
                interface_traffic_records[-1],
                observed_monotonic,
            ):
                device_state_changed = True

            connection_health_records = parse_connection_health_records(data)
            if connection_health_records:
                state = self._connection_health.update(
                    connection_health_records,
                    self._observation_timestamp(observed_wall_time),
                    observed_monotonic,
                )
                if state is not None:
                    device.receiver_flow_connection_health = state
                    self._schedule_connection_health_expiry(device_extended_unique_identifier)
                    device_state_changed = True

            if device_state_changed:
                self._notify_device_updated(device)

        if self._on_signal_presence:
            for record in parse_signal_presence_records(data):
                try:
                    self._on_signal_presence(record, addr)
                except Exception as exception:
                    logger.debug(f"Signal-presence callback failed for {source_ip}: {exception}")

    def _update_interface_traffic(
        self,
        device,
        device_extended_unique_identifier: str,
        record: dict,
        observed_at: float,
    ) -> bool:
        sequence = record["sequence"]
        previous = self._previous_interface_traffic_samples.get(device_extended_unique_identifier)
        if previous is not None and sequence == previous[0]:
            return False

        interval_seconds = None
        if previous is not None and ((sequence - previous[0]) & 0xFFFF) == 1:
            elapsed_seconds = observed_at - previous[1]
            if elapsed_seconds > 0:
                interval_seconds = elapsed_seconds
        self._previous_interface_traffic_samples[device_extended_unique_identifier] = (sequence, observed_at)

        interfaces = []
        for parsed_interface in record["interfaces"]:
            interface = dict(parsed_interface)
            if interval_seconds is not None:
                interface["estimated_transmit_bits_per_second"] = interface["transmit_octets"] * 8 / interval_seconds
                interface["estimated_receive_bits_per_second"] = interface["receive_octets"] * 8 / interval_seconds
            interfaces.append(interface)

        total_transmit_octets = sum(interface["transmit_octets"] for interface in interfaces)
        total_receive_octets = sum(interface["receive_octets"] for interface in interfaces)
        state = {
            "device_extended_unique_identifier": device_extended_unique_identifier,
            "sequence": sequence,
            "interval_seconds": interval_seconds,
            "interface_entry_count": record["interface_entry_count"],
            "interface_entry_width": record["interface_entry_width"],
            "interfaces": interfaces,
            "total_transmit_octets": total_transmit_octets,
            "total_receive_octets": total_receive_octets,
        }
        if interval_seconds is not None:
            state["estimated_total_transmit_bits_per_second"] = total_transmit_octets * 8 / interval_seconds
            state["estimated_total_receive_bits_per_second"] = total_receive_octets * 8 / interval_seconds
        device.network_interface_traffic = state
        return True

    def _associate_device_identity(self, device_extended_unique_identifier: str, device) -> bool:
        identity_changed = False
        for known_identifier, known_device in tuple(self._heartbeat_devices.items()):
            if known_device is device and known_identifier != device_extended_unique_identifier:
                self._discard_device_identity(known_identifier)
                identity_changed = True
        known_device = self._heartbeat_devices.get(device_extended_unique_identifier)
        if known_device is not None and known_device is not device:
            self._discard_device_identity(device_extended_unique_identifier)
            identity_changed = True
        self._heartbeat_devices[device_extended_unique_identifier] = device
        return identity_changed

    def _discard_device_identity(self, device_extended_unique_identifier: str) -> None:
        device = self._heartbeat_devices.pop(device_extended_unique_identifier, None)
        self._previous_interface_traffic_samples.pop(device_extended_unique_identifier, None)
        self._connection_health.remove_device(device_extended_unique_identifier)
        expiry_handle = self._connection_health_expiry_handles.pop(device_extended_unique_identifier, None)
        if expiry_handle is not None:
            expiry_handle.cancel()
        if device is None:
            return
        connection_health = getattr(device, "receiver_flow_connection_health", None)
        if (
            isinstance(connection_health, dict)
            and connection_health.get("device_extended_unique_identifier") == device_extended_unique_identifier
        ):
            device.receiver_flow_connection_health = None
        interface_traffic = getattr(device, "network_interface_traffic", None)
        if (
            isinstance(interface_traffic, dict)
            and interface_traffic.get("device_extended_unique_identifier") == device_extended_unique_identifier
        ):
            device.network_interface_traffic = None

    def _discard_untracked_device_identities(self, devices: dict) -> None:
        retained_devices = {id(device) for device in devices.values()}
        for device_extended_unique_identifier, device in tuple(self._heartbeat_devices.items()):
            if id(device) not in retained_devices:
                self._discard_device_identity(device_extended_unique_identifier)

    def _observation_timestamp(self, observed_wall_time: float) -> str:
        observed_at = datetime.fromtimestamp(observed_wall_time, timezone.utc)
        return observed_at.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _notify_device_updated(self, device) -> None:
        if self._on_device_updated is not None:
            self._on_device_updated(device)

    def connection_health_history(self, device_extended_unique_identifier: str) -> dict | None:
        return self._connection_health.history_snapshot(device_extended_unique_identifier)

    def _schedule_connection_health_expiry(
        self,
        device_extended_unique_identifier: str,
        delay_seconds: float | None = None,
    ) -> None:
        expiry_handle = self._connection_health_expiry_handles.pop(device_extended_unique_identifier, None)
        if expiry_handle is not None:
            expiry_handle.cancel()
        try:
            event_loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        delay = self._connection_health.freshness_seconds if delay_seconds is None else delay_seconds
        self._connection_health_expiry_handles[device_extended_unique_identifier] = event_loop.call_later(
            delay,
            self._expire_connection_health,
            device_extended_unique_identifier,
        )

    def _expire_connection_health(self, device_extended_unique_identifier: str) -> None:
        self._connection_health_expiry_handles.pop(device_extended_unique_identifier, None)
        observed_monotonic = self._monotonic_clock()
        remaining_seconds = self._connection_health.seconds_until_expiry(
            device_extended_unique_identifier,
            observed_monotonic,
        )
        if remaining_seconds is None:
            return
        if remaining_seconds > 0:
            self._schedule_connection_health_expiry(device_extended_unique_identifier, remaining_seconds)
            return
        state = self._connection_health.expire_device(device_extended_unique_identifier, observed_monotonic)
        device = self._heartbeat_devices.get(device_extended_unique_identifier)
        if state is None or device is None:
            return
        device.receiver_flow_connection_health = state
        self._notify_device_updated(device)

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
                self._check_stale_devices()
            except asyncio.CancelledError:
                break
            except Exception as exception:
                logger.debug(f"Heartbeat sweep error: {exception}")

    def _check_stale_devices(self) -> None:
        if not self._get_devices:
            return

        devices = self._get_devices()
        self._discard_untracked_device_identities(devices)
        if not self._mark_offline:
            return

        now = self._wall_clock()

        for server_name, device in list(devices.items()):
            if not device.online:
                continue
            if device.last_seen is None:
                continue
            age = now - device.last_seen
            if age > OFFLINE_THRESHOLD_SECONDS:
                logger.info(f"Device heartbeat stale for {age:.1f}s: {server_name}")
                self._mark_offline(server_name)
