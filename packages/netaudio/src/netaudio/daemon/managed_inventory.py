from __future__ import annotations

import asyncio
import copy
import inspect
import ipaddress
import logging
import re
import time
from dataclasses import asdict, dataclass
from typing import Awaitable, Callable, Iterable, Optional

from netaudio.common.managed_api import DDMConfiguration, DDMContextConfiguration, ManagedAPIConfiguration
from netaudio.core import subscription_state_for_identifier
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.ddm import Device, Domain, InventoryResult, ManagedAPIClient, ManagedAPIError


logger = logging.getLogger("netaudio")

DDM_KEY_PREFIX = "ddm:"


@dataclass(frozen=True)
class ManagedDeviceObservation:
    device: Device
    domain_id: str | None
    domain_name: str | None
    synced_at: float | None = None
    fresh: bool | None = None
    server_profile: str = "default"
    context_name: str | None = None


def _normalized_mac(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    hexadecimal = re.sub(r"[^0-9a-fA-F]", "", value).lower()
    if len(hexadecimal) == 16:
        if hexadecimal[6:10] == "fffe":
            hexadecimal = f"{hexadecimal[:6]}{hexadecimal[10:]}"
        elif hexadecimal.endswith("0000"):
            hexadecimal = hexadecimal[:12]
    if len(hexadecimal) not in {12, 16} or set(hexadecimal) == {"0"}:
        return None
    return hexadecimal


def _direct_macs(record: dict) -> set[str]:
    values: list[object] = [record.get("mac_address")]
    for interface in record.get("interfaces") or []:
        if isinstance(interface, dict):
            values.extend((interface.get("mac_address"), interface.get("macAddress")))
    return {normalized for value in values if (normalized := _normalized_mac(value)) is not None}


def _managed_macs(device: Device) -> set[str]:
    return {
        normalized
        for interface in device.interfaces or ()
        if interface is not None
        if (normalized := _normalized_mac(interface.mac_address)) is not None
    }


def _managed_ips(device: Device) -> set[str]:
    addresses = set()
    for interface in device.interfaces or ():
        if interface is None or not interface.address:
            continue
        try:
            addresses.add(str(ipaddress.ip_address(interface.address)))
        except ValueError:
            continue
    return addresses


def _unique_cross_source_matches(
    direct: dict[str, dict], observations: tuple[ManagedDeviceObservation, ...]
) -> dict[int, str]:
    candidate_sets: dict[int, set[str]] = {}
    for index, observation in enumerate(observations):
        managed_macs = _managed_macs(observation.device)
        candidates = {key for key, record in direct.items() if managed_macs & _direct_macs(record)}
        candidate_sets[index] = candidates

    matches: dict[int, str] = {}
    for index, candidates in candidate_sets.items():
        if len(candidates) != 1:
            continue
        candidate = next(iter(candidates))
        contenders = [other for other, values in candidate_sets.items() if candidate in values]
        if len(contenders) == 1:
            matches[index] = candidate
    return matches


def _ddm_key(observation: ManagedDeviceObservation) -> str:
    domain = observation.domain_id or "unenrolled"
    return f"{DDM_KEY_PREFIX}{observation.server_profile}:{domain}:{observation.device.id}"


def _interface_json(device: Device) -> list[dict] | None:
    if device.interfaces is None:
        return None
    return [asdict(interface) for interface in device.interfaces if interface is not None]


def _first_value(values: Iterable[str | None]) -> str | None:
    return next((value for value in values if value), None)


def _managed_name(device: Device) -> str:
    if device.name:
        return device.name
    if device.identity is None:
        return device.id
    return _first_value((device.identity.actual_name, device.identity.default_name)) or device.id


def _managed_channel_json(channel, *, receive: bool) -> dict:
    signal = asdict(channel.signal_presence) if channel.signal_presence is not None else None
    result = {
        "name": channel.name,
        "ddm_channel_id": channel.id,
        "ddm_media_type": channel.media_type,
        "ddm_signal_presence": signal,
    }
    if receive:
        result.update(
            {
                "ddm_enabled": channel.enabled,
                "ddm_status": channel.status,
                "ddm_status_message": channel.status_message,
                "ddm_summary": channel.summary,
                "ddm_encryption_scheme": channel.encryption_scheme,
                "ddm_can_subscribe_self": channel.can_subscribe_self,
            }
        )
    else:
        result["ddm_encryption_policy"] = channel.encryption_policy
    return {key: value for key, value in result.items() if value is not None}


def _managed_channels(device: Device) -> dict:
    receivers = {
        str(channel.index): _managed_channel_json(channel, receive=True) for channel in device.rx_channels or ()
    }
    transmitters = {
        str(channel.index): _managed_channel_json(channel, receive=False) for channel in device.tx_channels or ()
    }
    return {"receivers": receivers, "transmitters": transmitters}


MANAGED_SUBSCRIPTION_SEVERITIES = {"connected": "ok", "error": "error", "warning": "warning"}


def _managed_subscription_status(channel) -> dict:
    summary = channel.summary.casefold() if isinstance(channel.summary, str) and channel.summary else None
    state = subscription_state_for_identifier(channel.status)
    return {
        "code": None,
        "detail": channel.status_message,
        "icon": "",
        "label": channel.summary or channel.status or "unknown",
        "severity": MANAGED_SUBSCRIPTION_SEVERITIES.get(summary, "info"),
        "state": state,
        "status": channel.status,
    }


def _managed_subscriptions(device: Device) -> list[dict]:
    subscriptions = []
    for channel in device.rx_channels or ():
        status = channel.status.casefold() if isinstance(channel.status, str) else channel.status
        if (
            not channel.subscribed_device
            and not channel.subscribed_channel
            and status in {None, "none"}
            and channel.summary in {None, "NONE"}
        ):
            continue
        subscriptions.append(
            {
                "rx_channel": channel.name,
                "rx_device": _managed_name(device),
                "tx_channel": channel.subscribed_channel,
                "tx_device": channel.subscribed_device,
                "status": _managed_subscription_status(channel),
                "ddm_status": channel.status,
                "ddm_status_message": channel.status_message,
                "ddm_summary": channel.summary,
            }
        )
    return subscriptions


def _managed_metadata(observation: ManagedDeviceObservation, synced_at: float) -> dict:
    device = observation.device
    connection = device.connection
    return {
        "inventory_id": _ddm_key(observation),
        "inventory_sources": ["ddm"],
        "management_state": "managed" if observation.domain_id else "unenrolled",
        "control_transports": ["ddm"],
        "ddm_device_id": device.id,
        "ddm_server_profile": observation.server_profile,
        "ddm_context": observation.context_name,
        "ddm_domain_id": observation.domain_id,
        "ddm_domain_name": observation.domain_name,
        "ddm_enrolment_state": device.enrolment_state,
        "ddm_connection_state": connection.state if connection else None,
        "ddm_connection_last_changed": connection.last_changed if connection else None,
        "ddm_last_sync": synced_at,
        "ddm_identity": asdict(device.identity) if device.identity else None,
        "ddm_status": asdict(device.status) if device.status else None,
        "ddm_capabilities": asdict(device.capabilities) if device.capabilities else None,
        "ddm_clock_preferences": asdict(device.clock_preferences) if device.clock_preferences else None,
        "ddm_clocking_state": asdict(device.clocking_state) if device.clocking_state else None,
        "ddm_parameters": [asdict(item) for item in device.parameters] if device.parameters is not None else None,
        "ddm_inputs": [asdict(item) if item else None for item in device.inputs] if device.inputs is not None else None,
        "ddm_outputs": [asdict(item) if item else None for item in device.outputs]
        if device.outputs is not None
        else None,
    }


def _managed_device_json(observation: ManagedDeviceObservation, synced_at: float, fresh: bool) -> dict:
    device = observation.device
    identity = device.identity
    interfaces = _interface_json(device)
    address = _first_value(sorted(_managed_ips(device)))
    mac_address = _first_value(interface.mac_address for interface in device.interfaces or () if interface is not None)
    connection_state = device.connection.state if device.connection else None
    normalized_connection = connection_state.upper() if connection_state else None
    ready = bool(fresh and normalized_connection == "READY")
    if not fresh or normalized_connection not in {"READY", "DISCONNECTED"}:
        availability_state = "unknown"
    else:
        availability_state = "online" if ready else "offline"
    metadata = _managed_metadata(observation, synced_at)
    record = {
        "channels": _managed_channels(device),
        "ipv4": address or "None",
        "name": _managed_name(device),
        "online": ready,
        "availability_state": availability_state,
        "server_name": _ddm_key(observation),
        "services": {},
        "subscriptions": _managed_subscriptions(device),
        "mac_address": mac_address,
        "manufacturer": device.manufacturer.name if device.manufacturer else "",
        "model": identity.product_model_name if identity else (device.product.name if device.product else ""),
        "model_id": identity.product_model_id if identity else None,
        "dante_model": device.platform.name if device.platform else "",
        "dante_model_id": device.platform.platform_id if device.platform else "",
        "product_version": identity.product_version if identity else None,
        "software_version": identity.product_software_version if identity else None,
        "firmware_version": identity.dante_version if identity else None,
        "tx_count": len(device.tx_channels) if device.tx_channels is not None else None,
        "rx_count": len(device.rx_channels) if device.rx_channels is not None else None,
        "preferred_leader": device.clock_preferences.leader if device.clock_preferences else None,
        "interfaces": interfaces,
        "direct_control_available": False,
        "field_sources": {
            "identity": "ddm",
            "channels": "ddm",
            "subscriptions": "ddm",
            "availability": "ddm",
            "clock": "ddm",
        },
        **metadata,
    }
    return {key: value for key, value in record.items() if value is not None}


def _overlay_channel_metadata(direct_channels: dict, managed_channels: dict) -> None:
    for direction in ("receivers", "transmitters"):
        direct_direction = direct_channels.setdefault(direction, {})
        managed_direction = managed_channels.get(direction, {})
        if not direct_direction:
            direct_channels[direction] = copy.deepcopy(managed_direction)
            continue
        for number, managed_channel in managed_direction.items():
            direct_channel = direct_direction.get(number)
            if direct_channel is None:
                direct_channel = direct_direction.get(int(number)) if number.isdigit() else None
            if isinstance(direct_channel, dict):
                for key, value in managed_channel.items():
                    if key.startswith("ddm_"):
                        direct_channel[key] = copy.deepcopy(value)


def _merge_observation(direct_record: dict, managed_record: dict) -> dict:
    merged = copy.deepcopy(direct_record)
    for key, value in managed_record.items():
        if key.startswith("ddm_") or key in {"inventory_id", "management_state", "availability_state"}:
            merged[key] = copy.deepcopy(value)
    merged["inventory_sources"] = ["direct", "ddm"]
    merged["control_transports"] = ["direct", "ddm"]
    merged["direct_control_available"] = True
    for key in ("name", "manufacturer", "model", "model_id", "dante_model", "dante_model_id", "ipv4"):
        if merged.get(key) in {None, "", "None"} and managed_record.get(key) not in {None, "", "None"}:
            merged[key] = copy.deepcopy(managed_record[key])
    if "preferred_leader" in managed_record:
        merged["preferred_leader"] = managed_record["preferred_leader"]
    merged["online"] = bool(direct_record.get("online") or managed_record.get("online"))
    merged["availability_state"] = "online" if merged["online"] else managed_record.get("availability_state", "offline")
    field_sources = dict(managed_record.get("field_sources") or {})
    field_sources.update(
        {
            "identity": "direct",
            "audio_configuration": "direct",
            "channels": "direct" if direct_record.get("channels") else "ddm",
            "subscriptions": "direct" if direct_record.get("subscriptions") else "ddm",
        }
    )
    merged["field_sources"] = field_sources
    _overlay_channel_metadata(merged.setdefault("channels", {}), managed_record.get("channels") or {})
    if not merged.get("subscriptions"):
        merged["subscriptions"] = copy.deepcopy(managed_record.get("subscriptions") or [])
    return merged


def merge_device_inventory(
    direct_devices: dict[str, object],
    observations: tuple[ManagedDeviceObservation, ...],
    *,
    synced_at: float,
    fresh: bool,
) -> dict[str, dict]:
    direct = {key: DanteDeviceSerializer.to_json(device) for key, device in direct_devices.items()}
    matches = _unique_cross_source_matches(direct, observations)
    merged: dict[str, dict] = {}
    matched_direct = set(matches.values())
    for key, record in direct.items():
        if key in matched_direct:
            continue
        annotated = copy.deepcopy(record)
        annotated.update(
            {
                "inventory_id": _first_value(sorted(_direct_macs(record))) or f"direct:{key}",
                "inventory_sources": ["direct"],
                "management_state": None,
                "control_transports": ["direct"],
                "direct_control_available": True,
                "availability_state": "online" if record.get("online") else "offline",
                "field_sources": {"identity": "direct", "audio_configuration": "direct"},
            }
        )
        merged[key] = annotated
    for index, observation in enumerate(observations):
        observation_synced_at = observation.synced_at if observation.synced_at is not None else synced_at
        observation_fresh = observation.fresh if observation.fresh is not None else fresh
        managed_record = _managed_device_json(observation, observation_synced_at, observation_fresh)
        direct_key = matches.get(index)
        if direct_key is None:
            key = managed_record["server_name"]
            if key in merged:
                key = f"{key}:{index}"
                managed_record["server_name"] = key
            merged[key] = managed_record
        else:
            managed_record["server_name"] = direct_key
            merged[direct_key] = _merge_observation(direct[direct_key], managed_record)
    return dict(sorted(merged.items()))


InventoryCallback = Callable[[], Optional[Awaitable[None]]]


class ManagedInventoryService:
    def __init__(
        self,
        configuration: ManagedAPIConfiguration,
        *,
        client: ManagedAPIClient | None = None,
        clock: Callable[[], float] = time.time,
        contexts: tuple[DDMContextConfiguration, ...] = (),
    ):
        error = configuration.configuration_error
        if error:
            raise ValueError(error)
        self.configuration = configuration
        self.client = client or self._make_client(configuration)
        self.clock = clock
        self.contexts = contexts
        self._contexts_by_domain = {context.domain_id: context.name for context in contexts}
        self._domains: dict[str, Domain] = {}
        self._unenrolled: dict[str, Device] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._callback: InventoryCallback | None = None
        self.last_attempt: float | None = None
        self.last_success: float | None = None
        self.last_error: str | None = None
        self.graphql_errors: tuple[dict, ...] = ()
        self._authoritative = False
        self._domains_current = False
        self._unenrolled_current = False
        self._domains_last_success: float | None = None
        self._unenrolled_last_success: float | None = None

    @staticmethod
    def _make_client(configuration: ManagedAPIConfiguration) -> ManagedAPIClient | None:
        if not configuration.enabled:
            return None
        arguments = {}
        if configuration.credential is not None:
            arguments["credential"] = configuration.credential
        else:
            arguments["credential_file"] = configuration.credential_file
        return ManagedAPIClient(configuration.url or "", **arguments)

    @property
    def enabled(self) -> bool:
        return self.client is not None

    @property
    def fresh(self) -> bool:
        if self.last_success is None:
            return False
        return self.clock() - self.last_success <= max(30.0, self.configuration.refresh_interval * 3)

    def set_callback(self, callback: InventoryCallback | None) -> None:
        self._callback = callback

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        await self.refresh()
        self._task = asyncio.create_task(self._refresh_loop(), name="managed-api-inventory")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def refresh(self) -> bool:
        if self.client is None:
            return False
        async with self._lock:
            previous_state = self._state_signature()
            self.last_attempt = self.clock()
            try:
                result = await self.client.inventory_async()
                changed = self._apply_result(result)
                accepted = result.data is not None
            except (ManagedAPIError, ValueError) as error:
                self._authoritative = False
                self._domains_current = False
                self._unenrolled_current = False
                self.graphql_errors = ()
                self.last_error = str(error)
                logger.warning("Managed API inventory refresh failed: %s", error)
                changed = False
                accepted = False
            state_changed = previous_state != self._state_signature()
        if changed or state_changed:
            await self._notify()
        return accepted

    def _state_signature(self) -> tuple:
        return (
            self._authoritative,
            self._domains_current,
            self._unenrolled_current,
            self.last_error,
            self.graphql_errors,
        )

    def _apply_result(self, result: InventoryResult) -> bool:
        self.graphql_errors = tuple(dict(issue.raw) for issue in result.errors)
        if result.data is None:
            self._authoritative = False
            self._domains_current = False
            self._unenrolled_current = False
            self.last_error = (
                "; ".join(issue["message"] for issue in self.graphql_errors) or "Managed API returned no inventory data"
            )
            return False
        previous = (self._domains.copy(), self._unenrolled.copy())
        observed_at = self.clock()
        if result.data.domains is not None:
            self._domains = self._domain_map(result.data.domains)
            self._domains_current = True
            self._domains_last_success = observed_at
        else:
            self._domains_current = False
        if result.data.unenrolled_devices is not None:
            self._unenrolled = self._device_map(result.data.unenrolled_devices)
            self._unenrolled_current = True
            self._unenrolled_last_success = observed_at
        else:
            self._unenrolled_current = False
        self._authoritative = result.successful
        if result.successful:
            self.last_success = observed_at
        self.last_error = "; ".join(issue["message"] for issue in self.graphql_errors) or None
        return previous != (self._domains, self._unenrolled)

    def _root_fresh(self, last_success: float | None, current: bool) -> bool:
        if not current or last_success is None:
            return False
        return self.clock() - last_success <= max(30.0, self.configuration.refresh_interval * 3)

    @staticmethod
    def _domain_map(domains) -> dict[str, Domain]:
        return {domain.id: domain for domain in domains or () if domain is not None}

    @staticmethod
    def _device_map(devices) -> dict[str, Device]:
        return {device.id: device for device in devices or () if device is not None}

    async def _notify(self) -> None:
        if self._callback is None:
            return
        result = self._callback()
        if inspect.isawaitable(result):
            await result

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.configuration.refresh_interval)
                await self.refresh()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Managed API inventory loop failed")

    def observations(self) -> tuple[ManagedDeviceObservation, ...]:
        observations: dict[str, ManagedDeviceObservation] = {}
        domains_fresh = self._root_fresh(self._domains_last_success, self._domains_current)
        for domain in self._domains.values():
            for device in domain.devices or ():
                if device is None:
                    continue
                observations[device.id] = ManagedDeviceObservation(
                    device,
                    domain.id,
                    domain.name,
                    self._domains_last_success,
                    domains_fresh,
                    self.configuration.name,
                    self._contexts_by_domain.get(domain.id),
                )
        unenrolled_fresh = self._root_fresh(self._unenrolled_last_success, self._unenrolled_current)
        for device_id, device in self._unenrolled.items():
            existing = observations.get(device_id)
            if existing is not None and (existing.fresh or not unenrolled_fresh):
                continue
            observations[device_id] = ManagedDeviceObservation(
                device,
                None,
                None,
                self._unenrolled_last_success,
                unenrolled_fresh,
                self.configuration.name,
                None,
            )
        return tuple(observations.values())

    def serialize_devices(self, direct_devices: dict[str, object]) -> dict[str, dict]:
        return merge_device_inventory(
            direct_devices,
            self.observations(),
            synced_at=self.last_success or 0.0,
            fresh=self.fresh,
        )

    def status(self) -> dict:
        state = "disabled"
        if self.enabled:
            state = "ready" if self.last_success is not None and self.last_error is None else "degraded"
            if self.last_attempt is None:
                state = "starting"
        observations = self.observations()
        return {
            "enabled": self.enabled,
            "state": state,
            "url": self.configuration.url,
            "server_profile": self.configuration.name,
            "refresh_interval": self.configuration.refresh_interval,
            "fresh": self.fresh,
            "authoritative": self.fresh and self._authoritative,
            "domains_fresh": self._root_fresh(self._domains_last_success, self._domains_current),
            "unenrolled_fresh": self._root_fresh(
                self._unenrolled_last_success,
                self._unenrolled_current,
            ),
            "last_attempt": self.last_attempt,
            "last_success": self.last_success,
            "last_error": self.last_error,
            "graphql_errors": list(self.graphql_errors),
            "domain_count": len(self._domains),
            "device_count": len(observations),
            "enrolled_device_count": sum(1 for item in observations if item.domain_id is not None),
            "unenrolled_device_count": sum(1 for item in observations if item.domain_id is None),
        }

    def domains(self) -> list[dict]:
        result = []
        for domain in sorted(self._domains.values(), key=lambda item: item.name or item.id):
            record = asdict(domain)
            record["ddm_server_profile"] = self.configuration.name
            record["ddm_context"] = self._contexts_by_domain.get(domain.id)
            result.append(record)
        return result


class ManagedInventoryRegistry:
    def __init__(
        self,
        configuration: DDMConfiguration,
        *,
        services: dict[str, ManagedInventoryService] | None = None,
    ):
        self.configuration = configuration
        contexts_by_server: dict[str, list[DDMContextConfiguration]] = {}
        for context in configuration.contexts.values():
            contexts_by_server.setdefault(context.server, []).append(context)
        self.services = (
            services
            if services is not None
            else {
                name: ManagedInventoryService(
                    server,
                    contexts=tuple(contexts_by_server.get(name, ())),
                )
                for name, server in configuration.servers.items()
            }
        )

    @property
    def enabled(self) -> bool:
        return any(service.enabled for service in self.services.values())

    def set_callback(self, callback: InventoryCallback | None) -> None:
        for service in self.services.values():
            service.set_callback(callback)

    async def start(self) -> None:
        await asyncio.gather(*(service.start() for service in self.services.values()))

    async def stop(self) -> None:
        await asyncio.gather(*(service.stop() for service in self.services.values()))

    def _selected_service(self, context_name: str | None = None) -> ManagedInventoryService:
        server = self.configuration.selected_server(context_name)
        try:
            return self.services[server.name]
        except KeyError as error:
            raise ValueError(f"DDM server profile {server.name!r} is disabled") from error

    def client_for_context(self, context_name: str | None = None) -> ManagedAPIClient:
        client = self._selected_service(context_name).client
        if client is None:
            raise ValueError("Managed API is not configured for the selected context")
        return client

    async def refresh(self, context_name: str | None = None) -> bool:
        if context_name is not None:
            return await self._selected_service(context_name).refresh()
        results = await asyncio.gather(*(service.refresh() for service in self.services.values() if service.enabled))
        return bool(results) and all(results)

    def observations(self) -> tuple[ManagedDeviceObservation, ...]:
        return tuple(observation for service in self.services.values() for observation in service.observations())

    def serialize_devices(self, direct_devices: dict[str, object]) -> dict[str, dict]:
        return merge_device_inventory(direct_devices, self.observations(), synced_at=0.0, fresh=False)

    def status(self) -> dict:
        servers = {name: service.status() for name, service in sorted(self.services.items())}
        enabled = [status for status in servers.values() if status["enabled"]]
        if not enabled:
            state = "disabled"
        elif any(status["state"] == "degraded" for status in enabled):
            state = "degraded"
        elif any(status["state"] == "starting" for status in enabled):
            state = "starting"
        else:
            state = "ready"
        return {
            "enabled": bool(enabled),
            "state": state,
            "default_context": self.configuration.default_context,
            "server_count": len(enabled),
            "domain_count": sum(status["domain_count"] for status in enabled),
            "device_count": sum(status["device_count"] for status in enabled),
            "enrolled_device_count": sum(status["enrolled_device_count"] for status in enabled),
            "unenrolled_device_count": sum(status["unenrolled_device_count"] for status in enabled),
            "fresh": bool(enabled) and all(status["fresh"] for status in enabled),
            "servers": servers,
        }

    def domains(self) -> list[dict]:
        return [domain for service in self.services.values() for domain in service.domains()]


__all__ = [
    "ManagedDeviceObservation",
    "ManagedInventoryRegistry",
    "ManagedInventoryService",
    "merge_device_inventory",
]
