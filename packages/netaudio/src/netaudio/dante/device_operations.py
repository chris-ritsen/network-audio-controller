from __future__ import annotations

import asyncio
import ipaddress
import math
import re

from netaudio.dante.channel_frontend import (
    channel_result_code,
    receiver_channel_name_protocol_identifier_from_probe,
    transmitter_channel_name_protocol_identifier_from_probe,
)
from netaudio.dante.channel_status_paging import (
    ChannelStatusPageAccumulator,
    modern_arc_protocol_identifier_for_device,
)
from netaudio.dante.const import (
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809,
    OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
    PROTOCOL_ARC_2809,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
)
from netaudio.dante.latency import latency_controls_from_settings
from netaudio.dante.services.notification_packet_handlers import STATUS_KIND_AES67
from netaudio.dante.state import apply_device_status

DANTE_NAME_MAX_LENGTH = 31
DANTE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$")


def validate_dante_name(name: str) -> str | None:
    if len(name) > DANTE_NAME_MAX_LENGTH:
        return f"Name exceeds {DANTE_NAME_MAX_LENGTH} characters"

    if not DANTE_NAME_PATTERN.match(name):
        if name.startswith("-") or name.endswith("-"):
            return "Name cannot begin or end with a hyphen"
        return "Name must contain only A-Z, a-z, 0-9, and hyphens"

    return None


def channel_status_query_specification(
    channel_type: str,
    protocol_id: int = PROTOCOL_ARC_2809,
    media_type: int = 1,
    starting_channel_identifier: int = 1,
    ending_channel_identifier: int = 0,
) -> dict:
    command = "query_receiver_channel_status_2809" if channel_type == "rx" else "query_transmitter_channel_status_2809"
    return {
        "command": command,
        "ending_channel_identifier": ending_channel_identifier,
        "media_type": media_type,
        "protocol_id": protocol_id,
        "starting_channel_identifier": starting_channel_identifier,
    }


def subscription_records(subscriptions) -> list[dict]:
    return [
        {"rx_channel": rx_channel, "tx_channel": tx_channel, "tx_device": tx_device}
        for rx_channel, tx_channel, tx_device in subscriptions
    ]


class DanteDeviceOperations:
    def __init__(self, device):
        self.device = device

    async def set_channel_name(
        self,
        channel_type,
        channel_number,
        new_channel_name,
        protocol_id=None,
    ):
        if protocol_id is None:
            protocol_id = await self.resolve_channel_name_protocol_identifier(channel_type)
        specification = {
            "channel_number": channel_number,
            "channel_type": channel_type,
            "command": "set_channel_name",
            "name": new_channel_name,
        }
        if protocol_id is not None:
            specification["protocol_id"] = protocol_id
        return await self.device.execute(specification)

    async def resolve_channel_name_protocol_identifier(self, channel_type: str):
        if channel_type == "rx":
            attribute_name = "receiver_channel_name_protocol_identifier"
            resolve = receiver_channel_name_protocol_identifier_from_probe
        else:
            attribute_name = "transmitter_channel_name_protocol_identifier"
            resolve = transmitter_channel_name_protocol_identifier_from_probe
        cached_protocol_identifier = getattr(self.device, attribute_name, None)
        if cached_protocol_identifier is not None:
            return cached_protocol_identifier

        response = await self.device.execute(channel_status_query_specification(channel_type))
        protocol_identifier = resolve(response)
        setattr(self.device, attribute_name, protocol_identifier)
        return protocol_identifier

    def _parse_status_page(self, response, description, page_kind):
        from netaudio import core

        try:
            page = core.parse_response(page_kind, response)
        except core.NetaudioCoreError as exception:
            raise RuntimeError(f"{description} returned an invalid status page") from exception
        if not isinstance(page, dict):
            raise RuntimeError(f"{description} returned an invalid status page")
        return page

    async def _query_channel_status_pages(self, channel_type):
        protocol_id = modern_arc_protocol_identifier_for_device(self.device)
        if channel_type == "rx":
            opcode = OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
            page_kind = "receiver_channel_status_page_2809"
            description = "receiver channel status query"
            cache_attribute = "receiver_channel_name_protocol_identifier"
        else:
            opcode = OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
            page_kind = "transmitter_channel_status_page_2809"
            description = "transmitter channel status query"
            cache_attribute = "transmitter_channel_name_protocol_identifier"

        accumulator = ChannelStatusPageAccumulator(protocol_id, opcode)
        request_range = (1, 1, 0)
        while request_range is not None:
            media_type, starting_channel_identifier, ending_channel_identifier = request_range
            response = await self.device.execute(
                channel_status_query_specification(
                    channel_type,
                    protocol_id=protocol_id,
                    media_type=media_type,
                    starting_channel_identifier=starting_channel_identifier,
                    ending_channel_identifier=ending_channel_identifier,
                )
            )
            result_code = channel_result_code(response, description)
            if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
                raise RuntimeError(f"{description} failed with result 0x{result_code:04X}")
            page = self._parse_status_page(response, description, page_kind)
            request_range = accumulator.add(page)

        setattr(self.device, cache_attribute, protocol_id)
        return accumulator.result()

    async def _query_status_page_2809(self, specification, description, page_kind):
        from netaudio import core

        response = await self.device.execute(specification)
        if response is None:
            raise RuntimeError(f"{description} did not receive a response")
        try:
            result_code = core.parse_response("result_code", response)
        except core.NetaudioCoreError as exception:
            raise RuntimeError(f"{description} returned an invalid response") from exception
        if not isinstance(result_code, int):
            raise RuntimeError(f"{description} returned an invalid response")
        if result_code != RESULT_CODE_SUCCESS:
            raise RuntimeError(f"{description} failed with result 0x{result_code:04X}")
        return self._parse_status_page(response, description, page_kind)

    async def query_receiver_channel_status_2809(self):
        return await self._query_channel_status_pages("rx")

    async def query_transmitter_channel_status_2809(self):
        return await self._query_channel_status_pages("tx")

    async def query_receiver_flow_status_2809(self):
        return await self._query_status_page_2809(
            {"command": "query_receiver_flow_status_2809"},
            "receiver flow status query",
            "receiver_flow_status_page_2809",
        )

    async def query_transmitter_flow_status_2809(self):
        return await self._query_status_page_2809(
            {"command": "query_tx_flows", "flow_protocol_id": PROTOCOL_ARC_2809, "starting_flow": 1},
            "transmitter flow status query",
            "transmitter_flow_status_page",
        )

    async def reboot(self, host_mac=None):
        await self._send_registered_system_reset("reboot", host_mac)

    async def factory_reset(self, host_mac=None):
        await self._send_registered_system_reset("factory_reset", host_mac)

    async def clear_configuration(
        self,
        preserve_internet_protocol_settings: bool,
        timeout: float = 2.0,
    ) -> dict:
        application = self.device.application
        if application is None:
            raise RuntimeError("verified clear-configuration requires an active Dante application")
        return await application.clear_configuration(
            self.device._require_address(),
            preserve_internet_protocol_settings,
            timeout,
        )

    async def _send_registered_system_reset(self, command: str, host_mac):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        device_ip_address = self.device._require_address()
        await self._register_controller_for_system_reset(device_ip_address, host_mac)
        await self.device.execute({"command": command, "host_mac": host_mac.hex()})

    async def _register_controller_for_system_reset(self, device_ip_address: str, host_mac: bytes) -> None:
        application = self.device.application
        application_service = getattr(application, "cmc", None)
        if application_service is not None:
            await application_service.require_registration(device_ip_address, host_mac)
            return

        from netaudio.dante.services.cmc import DanteCMCService

        operation_service = DanteCMCService(self.device.transport, host_media_access_control_address=host_mac)
        try:
            await operation_service.require_registration(device_ip_address, host_mac)
        finally:
            await operation_service.stop()

    async def set_latency(self, latency):
        latency_milliseconds = float(latency)
        if not math.isfinite(latency_milliseconds) or latency_milliseconds < 0:
            raise ValueError("latency must be a finite, nonnegative number")
        return await self.device.execute({"command": "set_latency", "latency": latency_milliseconds})

    async def set_gain_level(self, channel_number, gain_level, device_type):
        application = self.device.application
        if application is None:
            raise RuntimeError("verified gain control requires an active Dante application")
        return await application.set_gain_level(
            self.device,
            channel_number,
            gain_level,
            device_type,
        )

    async def set_aes67_multicast_prefix(self, prefix: str):
        try:
            normalized_prefix = str(ipaddress.IPv4Address(prefix))
        except (ipaddress.AddressValueError, ValueError) as exception:
            raise ValueError("AES67 multicast prefix must be an IPv4 address") from exception
        return await self.device.execute({"command": "set_aes67_multicast_prefix", "prefix": normalized_prefix})

    async def set_encoding(self, encoding):
        supported_encodings = self.device.supported_encodings
        if supported_encodings is not None and encoding not in supported_encodings:
            raise ValueError(f"requested encoding {encoding} is not supported; device reports {supported_encodings}")
        await self.device.execute({"command": "set_encoding", "encoding": encoding})

    async def set_sample_rate(self, sample_rate, confirm_destructive=False):
        application = self.device.application
        if application is None:
            raise RuntimeError("topology-safe sample-rate control requires an active Dante application")
        return await application.set_sample_rate(
            self.device,
            sample_rate,
            confirm_destructive=confirm_destructive,
        )

    async def set_sample_rate_pullup(self, raw_value):
        application = self.device.application
        if application is None:
            raise RuntimeError("verified sample rate pull-up control requires an active Dante application")
        return await application.set_sample_rate_pullup(self.device, raw_value)

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
        return await self.add_subscription_by_name(rx_channel.number, tx_channel_name, tx_device.name)

    async def add_subscription_by_name(self, rx_channel_number, tx_channel_name, tx_device_name):
        return await self.add_subscriptions_by_name([(rx_channel_number, tx_channel_name, tx_device_name)])

    async def add_subscriptions(self, subscriptions):
        records = []
        for rx_channel, tx_channel, tx_device in subscriptions:
            tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
            records.append((rx_channel.number, tx_channel_name, tx_device.name))
        return await self.add_subscriptions_by_name(records)

    async def add_subscriptions_by_name(self, records):
        async with self.device.topology_mutation_lock:
            return await self.device.execute(
                {"command": "add_subscriptions", "subscriptions": subscription_records(records)}
            )

    async def remove_subscription(self, rx_channel):
        return await self.remove_subscriptions_by_number([rx_channel.number])

    async def remove_subscriptions(self, rx_channels):
        return await self.remove_subscriptions_by_number([channel.number for channel in rx_channels])

    async def remove_subscriptions_by_number(self, channel_numbers):
        async with self.device.topology_mutation_lock:
            return await self.device.execute({"command": "remove_subscriptions", "rx_channels": list(channel_numbers)})

    async def reset_channel_name(self, channel_type, channel_number):
        return await self.device.execute(
            {"channel_number": channel_number, "channel_type": channel_type, "command": "reset_channel_name"}
        )

    async def set_name(self, name):
        error = validate_dante_name(name)
        if error:
            raise ValueError(error)
        return await self.device.execute({"command": "set_name", "name": name})

    async def reset_name(self):
        return await self.device.execute({"command": "reset_name"})

    async def lock_device(self, pin: str, key: bytes) -> dict:
        key_error = _validate_lock_key(key)
        if key_error:
            return key_error
        return await core_lock_device(str(self.device.ipv4), pin, key)

    async def unlock_device(self, pin: str, key: bytes) -> dict:
        key_error = _validate_lock_key(key)
        if key_error:
            return key_error
        return await core_unlock_device(str(self.device.ipv4), pin, key)

    async def get_device_settings(self):
        if self.device.ipv4 is None:
            return None
        settings = await self.device.call_core(lambda client: client.get_device_settings())
        self._apply_device_settings(settings)
        return settings

    async def get_latency_settings(self):
        from netaudio import core

        response = await self.device.execute({"command": "query_latency_config"})
        if response is None:
            return None
        settings = core.parse_response("device_settings", response)
        self._apply_device_settings(settings)
        return settings

    def _apply_device_settings(self, settings):
        if not isinstance(settings, dict):
            return
        controls = latency_controls_from_settings(settings)
        if settings.get("sample_rate"):
            controls["sample_rate"] = settings["sample_rate"]
        self.device.apply_controls(controls)

    async def get_aes67_configured(self):
        from netaudio import core

        if self.device.ipv4 is None:
            return None
        try:
            response = await self.device.execute({"command": "query_latency_config"})
        except core.NetaudioCoreError as error:
            if error.status != core.STATUS_TIMEOUT:
                raise
            return None
        if response is None:
            return None
        configured = core.parse_response("aes67_configured", response)
        settings = core.parse_response("device_settings", response)
        prefix = settings.get("aes67_multicast_prefix") if isinstance(settings, dict) else None
        apply_device_status(
            self.device,
            STATUS_KIND_AES67,
            {"aes67_configured": configured, "aes67_multicast_prefix": prefix},
        )
        return configured


LOCK_OPERATION_LOCK = 1
LOCK_OPERATION_UNLOCK = 2

LOCK_STATUS_ALREADY = 0x1102
LOCK_STATUS_SUCCESS = 0x0000


def validate_pin(pin: str) -> str | None:
    if len(pin) != 4:
        return "PIN must be exactly 4 digits"
    if not pin.isdigit():
        return "PIN must contain only digits"
    return None


def _lock_key_not_configured() -> dict:
    return {
        "already": False,
        "error": "device_lock_key not configured",
        "lock_state": None,
        "not_configured": True,
        "status": None,
        "success": False,
    }


def _lock_key_invalid(actual_length: int, expected_length: int) -> dict:
    return {
        "already": False,
        "error": f"device_lock_key must be {expected_length} bytes, got {actual_length}",
        "lock_state": None,
        "not_configured": False,
        "status": None,
        "success": False,
    }


def _validate_lock_key(key: bytes) -> dict | None:
    if not key:
        return _lock_key_not_configured()
    from netaudio.core.binding import LOCK_KEY_LENGTH

    if len(key) != LOCK_KEY_LENGTH:
        return _lock_key_invalid(len(key), LOCK_KEY_LENGTH)
    return None


async def core_lock_device(device_ip: str, pin: str, key: bytes) -> dict:
    key_error = _validate_lock_key(key)
    if key_error:
        return key_error
    return await _device_lock_operation(device_ip, pin, key, LOCK_OPERATION_LOCK)


async def core_unlock_device(device_ip: str, pin: str, key: bytes) -> dict:
    key_error = _validate_lock_key(key)
    if key_error:
        return key_error
    return await _device_lock_operation(device_ip, pin, key, LOCK_OPERATION_UNLOCK)


async def _device_lock_operation(device_ip: str, pin: str, key: bytes, operation: int) -> dict:
    from netaudio import core

    if operation not in (LOCK_OPERATION_LOCK, LOCK_OPERATION_UNLOCK):
        raise ValueError(f"unknown lock operation: {operation}")

    def _run():
        with core.CoreClient(device_ip) as client:
            if operation == LOCK_OPERATION_LOCK:
                return client.lock(pin, key)
            return client.unlock(pin, key)

    try:
        result = await asyncio.to_thread(_run)
    except core.NetaudioCoreError as error:
        return _lock_core_error(error)
    result.setdefault("success", result.get("status") in (LOCK_STATUS_SUCCESS, LOCK_STATUS_ALREADY))
    result.setdefault("already", result.get("status") == LOCK_STATUS_ALREADY)
    return result


def _lock_core_error(error: Exception) -> dict:
    return {
        "already": False,
        "error": str(error),
        "lock_state": None,
        "not_configured": False,
        "status": getattr(error, "status", None),
        "success": False,
    }
