from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

from netaudio import core
from netaudio.common.managed_api import ManagedAPIConfiguration
from netaudio.ddm.client import ManagedAPIClient
from netaudio.ddm.controller import (
    identify_managed_device_with_api_key,
    normalize_device_id,
    query_managed_arc_with_api_key,
    query_managed_settings_with_api_key,
    reboot_managed_device_with_api_key,
)


class ManagedDeviceControlError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManagedOperationResult:
    operation: str
    successful: bool = True


SUBSCRIPTION_MUTATION = (
    "mutation DeviceRxChannelsSubscriptionSet($input: DeviceRxChannelsSubscriptionSetInput!) "
    "{ DeviceRxChannelsSubscriptionSet(input: $input) { ok } }"
)
DEVICE_NAME_MUTATION = "mutation DeviceNameSet($input: DeviceNameSetInput!) { DeviceNameSet(input: $input) { ok } }"
PREFERRED_LEADER_MUTATION = (
    "mutation DeviceClockingPreferredLeaderSet($input: DeviceClockingPreferredLeaderSetInput!) "
    "{ DeviceClockingPreferredLeaderSet(input: $input) { ok } }"
)


def device_requires_managed_control(device) -> bool:
    enrolment = str(getattr(device, "ddm_enrolment_state", "") or "").casefold()
    management = str(getattr(device, "management_state", "") or "").casefold()
    return enrolment in {"enrolled", "managed"} or management == "managed"


class ManagedDeviceTransport:
    def __init__(
        self,
        configuration: ManagedAPIConfiguration,
        *,
        client: ManagedAPIClient | None = None,
    ):
        error = configuration.configuration_error
        if error:
            raise ManagedDeviceControlError(error)
        if not configuration.enabled:
            raise ManagedDeviceControlError("DDM control is not configured")
        endpoint = urlsplit(configuration.url or "")
        server = endpoint.hostname
        if not server:
            raise ManagedDeviceControlError("DDM Controller server could not be determined from the server profile URL")
        self.configuration = configuration
        self.server = server
        self.client = client or ManagedAPIClient(
            configuration.url or "",
            credential=configuration.credential,
            credential_file=configuration.credential_file,
        )

    def _credential(self) -> str:
        return self.client.read_credential()

    @staticmethod
    def _device_id(device) -> str:
        device_id = getattr(device, "ddm_device_id", None)
        if not isinstance(device_id, str) or not device_id:
            raise ManagedDeviceControlError("managed device has no DDM device ID")
        if getattr(device, "online", True) is False:
            raise ManagedDeviceControlError(f"managed device {device_id} is offline")
        return device_id

    @staticmethod
    def _domain_options(device) -> dict[str, str]:
        domain_id = getattr(device, "ddm_domain_id", None)
        return {"expected_domain_id": domain_id} if isinstance(domain_id, str) and domain_id else {}

    async def _control_device_id(self, device) -> str:
        device_id = self._device_id(device)
        try:
            normalize_device_id(device_id)
            return device_id
        except ValueError:
            if len(device_id) != 32 or any(character not in "0123456789abcdef" for character in device_id.lower()):
                raise ManagedDeviceControlError("The device's managed Controller identity is unsupported") from None

        fresh = await self.fetch_device(device, require_unique_primary=True)
        return self._primary_control_id(fresh, device)

    @staticmethod
    def _primary_control_id(fresh, observed_device=None) -> str:
        primary = fresh.interfaces[0] if fresh.interfaces else None
        mac_address = getattr(primary, "mac_address", None)
        if mac_address in (None, "") and observed_device is not None:
            # Enrollment can omit the primary MAC while retaining the same DDM
            # inventory ID and address. Keep the independently read interface
            # identity only across that explicitly correlated transition.
            observed = [
                interface
                for interface in getattr(observed_device, "interfaces", None) or ()
                if isinstance(interface, dict)
                and interface.get("interface") == "primary"
                and interface.get("ip_address") == getattr(primary, "address", None)
            ]
            if len(observed) == 1 and fresh.id == getattr(observed_device, "ddm_device_id", None):
                mac_address = observed[0].get("mac_address")
        try:
            mac = bytes.fromhex(mac_address.replace(":", "").replace("-", ""))
        except (AttributeError, TypeError, ValueError):
            mac = b""
        if len(mac) != 6 or not any(mac) or mac[0] & 1:
            raise ManagedDeviceControlError("The device's primary interface identity is unavailable")
        # DDM's inventory identifier can differ from the Controller service's
        # EUI-64 identity. Service discovery still has to announce this target.
        return (mac[:3] + b"\xff\xfe" + mac[3:]).hex()

    @staticmethod
    def _host_mac() -> bytes:
        host_mac = core.host_mac()
        if host_mac is None:
            raise ManagedDeviceControlError("could not determine the host MAC address required for DDM control")
        return host_mac

    async def execute(self, device, specification: Mapping[str, Any]) -> bytes | None:
        device_id = await self._control_device_id(device)
        command = specification.get("command")
        if command == "reboot":
            fresh = await self.fetch_device(device)
            if fresh.capabilities is None or fresh.capabilities.can_reset is not True:
                raise ManagedDeviceControlError("The device does not report managed reboot support")
            if fresh.connection is None or fresh.connection.state != "READY":
                raise ManagedDeviceControlError("The device is not ready for a managed reboot")
            await asyncio.to_thread(
                reboot_managed_device_with_api_key,
                self.server,
                self._credential(),
                device_id,
                self._host_mac(),
                **self._domain_options(device),
            )
            return None
        if command == "identify":
            await asyncio.to_thread(
                identify_managed_device_with_api_key,
                self.server,
                self._credential(),
                device_id,
                self._host_mac(),
                **self._domain_options(device),
            )
            return None

        try:
            plan = core.build_managed_command(
                dict(specification), host_mac=core.host_mac(), message_id=core.next_message_id()
            )
        except core.NetaudioCoreError as exception:
            raise ManagedDeviceControlError(
                f"{command} is not available through DDM control: {exception}"
            ) from exception

        packet = bytes(plan["packet"])

        if plan["transport"] == "arc":
            return await asyncio.to_thread(
                query_managed_arc_with_api_key,
                self.server,
                self._credential(),
                device_id,
                packet,
                **self._domain_options(device),
            )
        if plan["transport"] == "settings":
            return await asyncio.to_thread(
                query_managed_settings_with_api_key,
                self.server,
                self._credential(),
                device_id,
                packet,
                plan["response_opcode"],
                **self._domain_options(device),
            )
        raise ManagedDeviceControlError(f"{command} has no supported managed transport")

    async def set_subscriptions(self, device, records) -> ManagedOperationResult:
        subscriptions = [
            {
                "rxChannelIndex": int(rx_channel),
                "subscribedChannel": str(tx_channel),
                "subscribedDevice": str(tx_device),
            }
            for rx_channel, tx_channel, tx_device in records
        ]
        return await self._mutation(
            "DeviceRxChannelsSubscriptionSet",
            SUBSCRIPTION_MUTATION,
            {"deviceId": self._device_id(device), "subscriptions": subscriptions},
        )

    async def remove_subscriptions(self, device, channel_numbers) -> ManagedOperationResult:
        return await self.set_subscriptions(
            device,
            [(channel_number, "", "") for channel_number in channel_numbers],
        )

    async def reset_device_name(self, device) -> ManagedOperationResult:
        return await self.set_device_name(device, "")

    async def set_device_name(self, device, name: str) -> ManagedOperationResult:
        return await self._mutation(
            "DeviceNameSet",
            DEVICE_NAME_MUTATION,
            {"deviceId": self._device_id(device), "name": name},
        )

    async def set_preferred_leader(self, device, enabled: bool) -> ManagedOperationResult:
        return await self._mutation(
            "DeviceClockingPreferredLeaderSet",
            PREFERRED_LEADER_MUTATION,
            {"deviceId": self._device_id(device), "enabled": enabled},
        )

    async def fetch_device(self, device, *, require_unique_primary=False):
        device_id = self._device_id(device)
        result = await self.client.inventory_async()
        if result.data is None:
            detail = "; ".join(issue.message for issue in result.errors) or "no inventory data"
            raise ManagedDeviceControlError(f"DDM inventory read failed: {detail}")
        for domain in result.data.domains or ():
            if domain is None:
                continue
            expected_domain_id = getattr(device, "ddm_domain_id", None)
            if expected_domain_id is not None and domain.id != expected_domain_id:
                continue
            for candidate in domain.devices or ():
                if candidate is not None and candidate.id == device_id:
                    if require_unique_primary:
                        control_id = self._primary_control_id(candidate, device)
                        for other in domain.devices or ():
                            if other is None or other.id == candidate.id:
                                continue
                            if getattr(getattr(other, "connection", None), "state", None) == "DISCONNECTED":
                                continue
                            try:
                                other_control_id = self._primary_control_id(other)
                            except ManagedDeviceControlError:
                                continue
                            if other_control_id == control_id:
                                raise ManagedDeviceControlError("The device's primary interface identity is not unique")
                    return candidate
        raise ManagedDeviceControlError(f"managed device {device_id} was absent from fresh DDM inventory")

    async def _mutation(
        self,
        operation: str,
        query: str,
        input_value: Mapping[str, Any],
    ) -> ManagedOperationResult:
        result = await self.client.execute_async(query, {"input": dict(input_value)}, operation)
        if result.errors:
            raise ManagedDeviceControlError("; ".join(issue.message for issue in result.errors))
        payload = result.data.get(operation) if result.data is not None else None
        if not isinstance(payload, Mapping) or payload.get("ok") is not True:
            raise ManagedDeviceControlError(f"DDM did not accept {operation}")
        return ManagedOperationResult(operation=operation)


__all__ = [
    "ManagedDeviceControlError",
    "ManagedDeviceTransport",
    "ManagedOperationResult",
    "device_requires_managed_control",
]
