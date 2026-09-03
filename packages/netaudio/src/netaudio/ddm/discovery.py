from __future__ import annotations

import asyncio
import ipaddress
import math
from dataclasses import dataclass, field
from typing import Any

from zeroconf import IPVersion, ServiceStateChange

from netaudio.common.app_config import settings as app_settings


DDM_CONTROLLER_SERVICE = "_dante-ddm-c._tcp.local."
DDM_DEVICE_SERVICE = "_dante-ddm-d._udp.local."
DDM_SERVICE_TYPES = (DDM_CONTROLLER_SERVICE, DDM_DEVICE_SERVICE)


@dataclass(frozen=True)
class DDMService:
    instance_name: str
    service_type: str
    port: int
    properties: tuple[tuple[str, str], ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "instance_name": self.instance_name,
            "service_type": self.service_type,
            "port": self.port,
            "properties": dict(self.properties),
        }


@dataclass(frozen=True)
class DDMServer:
    server_name: str
    ipv4_addresses: tuple[str, ...]
    controller_service: DDMService | None = None
    device_service: DDMService | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "server_name": self.server_name,
            "ipv4_addresses": list(self.ipv4_addresses),
            "controller_service": self.controller_service.to_json() if self.controller_service else None,
            "device_service": self.device_service.to_json() if self.device_service else None,
        }


@dataclass(frozen=True)
class _ResolvedService:
    server_name: str
    ipv4_addresses: tuple[str, ...]
    service: DDMService


@dataclass
class _ServiceGroup:
    addresses: set[str] = field(default_factory=set)
    controller: DDMService | None = None
    device: DDMService | None = None


def _decode_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_properties(properties: dict[object, object]) -> tuple[tuple[str, str], ...]:
    decoded = []
    for raw_key, raw_value in properties.items():
        key = _decode_text(raw_key)
        if key:
            decoded.append((key, _decode_text(raw_value)))
    return tuple(sorted(decoded))


def _instance_name(name: str, service_type: str) -> str:
    suffix = f".{service_type}"
    return name[: -len(suffix)] if name.endswith(suffix) else name


def _ipv4_addresses(addresses: list[str]) -> tuple[str, ...]:
    valid = set()
    for address in addresses:
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError:
            continue
        if parsed.version == 4:
            valid.add(str(parsed))
    return tuple(sorted(valid, key=lambda address: int(ipaddress.ip_address(address))))


def _merge_services(records: list[_ResolvedService]) -> tuple[DDMServer, ...]:
    grouped: dict[str, _ServiceGroup] = {}
    for record in sorted(
        records, key=lambda item: (item.server_name, item.service.service_type, item.service.instance_name)
    ):
        group = grouped.setdefault(record.server_name, _ServiceGroup())
        group.addresses.update(record.ipv4_addresses)
        if record.service.service_type == DDM_CONTROLLER_SERVICE:
            if group.controller is None:
                group.controller = record.service
        elif record.service.service_type == DDM_DEVICE_SERVICE:
            if group.device is None:
                group.device = record.service

    servers = []
    for server_name, group in sorted(grouped.items()):
        addresses = tuple(sorted(group.addresses, key=lambda address: int(ipaddress.ip_address(address))))
        servers.append(
            DDMServer(
                server_name=server_name,
                ipv4_addresses=addresses,
                controller_service=group.controller,
                device_service=group.device,
            )
        )
    return tuple(servers)


async def discover_ddm_servers(
    *,
    timeout: float = 2.0,
    interfaces: list[str] | None = None,
) -> tuple[DDMServer, ...]:
    """Discover DDM's Controller-facing and device-facing mDNS services."""
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 60:
        raise ValueError("timeout must be greater than 0 and no more than 60 seconds")

    # Import the asynchronous classes here so discovery remains straightforward
    # to isolate in deterministic tests and does not create network state at import.
    from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

    options: dict[str, object] = {"ip_version": IPVersion.V4Only}
    selected_interfaces = interfaces
    if selected_interfaces is None and app_settings.interface_ip:
        selected_interfaces = [app_settings.interface_ip]
    if selected_interfaces:
        options["interfaces"] = selected_interfaces

    aio_zc = AsyncZeroconf(**options)
    tasks: set[asyncio.Task[None]] = set()
    records: list[_ResolvedService] = []

    async def resolve(zeroconf, service_type: str, name: str) -> None:
        info = AsyncServiceInfo(service_type, name)
        if not await info.async_request(zeroconf, max(1, int(timeout * 1000))):
            return
        addresses = _ipv4_addresses(info.parsed_addresses())
        if not info.server or not addresses or not 1 <= info.port <= 65535:
            return
        records.append(
            _ResolvedService(
                server_name=info.server,
                ipv4_addresses=addresses,
                service=DDMService(
                    instance_name=_instance_name(name, service_type),
                    service_type=service_type,
                    port=info.port,
                    properties=_decode_properties(info.properties),
                ),
            )
        )

    def handle(zeroconf, service_type: str, name: str, state_change: ServiceStateChange) -> None:
        if state_change is ServiceStateChange.Removed:
            return
        task = asyncio.create_task(resolve(zeroconf, service_type, name))
        tasks.add(task)

    browser = AsyncServiceBrowser(
        aio_zc.zeroconf,
        list(DDM_SERVICE_TYPES),
        handlers=[handle],
    )
    try:
        await asyncio.sleep(timeout)
        await browser.async_cancel()
        if tasks:
            await asyncio.gather(*tasks)
    finally:
        await aio_zc.async_close()

    return _merge_services(records)
