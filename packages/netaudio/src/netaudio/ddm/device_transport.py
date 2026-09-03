from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

from netaudio import core
from netaudio.common.managed_api import ManagedAPIConfiguration
from netaudio.dante.const import PROTOCOL_ARC_2809, PROTOCOL_SETTINGS
from netaudio.ddm.client import ManagedAPIClient
from netaudio.ddm.controller import (
    identify_managed_device_with_api_key,
    query_managed_arc_with_api_key,
    query_managed_settings_with_api_key,
)


class ManagedDeviceControlError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManagedOperationResult:
    operation: str
    successful: bool = True


MODERN_PROTOCOL_COMMANDS = frozenset(
    {
        "channel_count",
        "device_info",
        "property_directory",
        "query_receiver_port_ranges",
        "set_channel_name",
        "set_latency",
        "transmitter_names",
    }
)

SETTINGS_RESPONSE_OPCODES = {
    "bluetooth_status": 0x100E,
    "clear_all_configuration": 0x0078,
    "clear_all_configuration_preserving_internet_protocol_settings": 0x0078,
    "dante_model": 0x0060,
    "enable_aes67": 0x1007,
    "make_model": 0x00C0,
    "probe_aes67": 0x1007,
    "probe_clear_configuration_status": 0x0078,
    "probe_encoding": 0x0082,
    "probe_gain_level": 0x100B,
    "probe_interface_status": 0x0011,
    "probe_link_status": 0x0040,
    "probe_lock_reset_status": 0x1009,
    "probe_preferred_leader": 0x0020,
    "probe_sample_rate": 0x0080,
    "probe_sample_rate_pullup": 0x0084,
    "probe_switch_configuration": 0x0014,
    "refresh_clock_status": 0x0020,
    "set_clock_source": 0x0020,
    "set_clock_subdomain": 0x0020,
    "set_encoding": 0x0082,
    "set_gain_level": 0x100B,
    "set_interface_dhcp": 0x0011,
    "set_interface_static": 0x0011,
    "set_preferred_leader": 0x0020,
    "set_sample_rate": 0x0080,
    "set_sample_rate_pullup": 0x0084,
}

SETTINGS_COMMANDS_REQUIRING_HOST_MAC = frozenset(
    {
        "bluetooth_status",
        "clear_all_configuration",
        "clear_all_configuration_preserving_internet_protocol_settings",
        "enable_aes67",
        "probe_aes67",
        "probe_clear_configuration_status",
        "probe_encoding",
        "probe_gain_level",
        "probe_interface_status",
        "probe_link_status",
        "probe_lock_reset_status",
        "probe_preferred_leader",
        "probe_sample_rate",
        "probe_sample_rate_pullup",
        "probe_switch_configuration",
        "refresh_clock_status",
        "set_clock_source",
        "set_clock_subdomain",
        "set_gain_level",
        "set_interface_dhcp",
        "set_interface_static",
        "set_preferred_leader",
        "set_sample_rate_pullup",
    }
)

COMMANDS_WITHOUT_MESSAGE_ID = frozenset(
    {
        "bluetooth_status",
        "dante_model",
        "make_model",
        "probe_interface_status",
    }
)

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

    @staticmethod
    def _host_mac() -> bytes:
        host_mac = core.host_mac()
        if host_mac is None:
            raise ManagedDeviceControlError("could not determine the host MAC address required for DDM control")
        return host_mac

    def _prepare_specification(self, specification: Mapping[str, Any]) -> dict[str, Any]:
        prepared = dict(specification)
        command = prepared.get("command")
        if not isinstance(command, str) or not command:
            raise ValueError("command specification has no command")
        if command in MODERN_PROTOCOL_COMMANDS:
            protocol_id = prepared.setdefault("protocol_id", PROTOCOL_ARC_2809)
            if not isinstance(protocol_id, int) or isinstance(protocol_id, bool) or protocol_id != PROTOCOL_ARC_2809:
                rendered_protocol = f"0x{protocol_id:04X}" if isinstance(protocol_id, int) else repr(protocol_id)
                raise ManagedDeviceControlError(
                    f"{command} selected protocol {rendered_protocol}, which DDM control does not support"
                )
        if command in SETTINGS_COMMANDS_REQUIRING_HOST_MAC and "host_mac" not in prepared:
            prepared["host_mac"] = self._host_mac().hex()
        if command not in COMMANDS_WITHOUT_MESSAGE_ID and not any(
            key in prepared for key in ("message_id", "sequence", "transaction_id")
        ):
            prepared["message_id"] = core.next_message_id()
        return prepared

    async def execute(self, device, specification: Mapping[str, Any]) -> bytes | None:
        device_id = self._device_id(device)
        command = specification.get("command")
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

        prepared = self._prepare_specification(specification)
        packet = core.build_command(prepared)
        if len(packet) < 2:
            raise ManagedDeviceControlError(f"{command} produced an invalid packet")
        protocol_id = int.from_bytes(packet[:2], "big")
        if protocol_id == PROTOCOL_ARC_2809:
            return await asyncio.to_thread(
                query_managed_arc_with_api_key,
                self.server,
                self._credential(),
                device_id,
                packet,
                **self._domain_options(device),
            )
        if protocol_id == PROTOCOL_SETTINGS:
            expected_opcode = SETTINGS_RESPONSE_OPCODES.get(str(command))
            if expected_opcode is None:
                raise ManagedDeviceControlError(f"{command} has no observed DDM settings completion and was not sent")
            return await asyncio.to_thread(
                query_managed_settings_with_api_key,
                self.server,
                self._credential(),
                device_id,
                packet,
                expected_opcode,
                **self._domain_options(device),
            )
        raise ManagedDeviceControlError(
            f"{command} uses protocol 0x{protocol_id:04X}, which is not available through DDM control"
        )

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

    async def fetch_device(self, device):
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
