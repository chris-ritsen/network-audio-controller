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
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
)
from netaudio.dante.latency import latency_controls_from_settings

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
            if channel_type == "rx":
                protocol_id = await self._resolve_receiver_channel_name_protocol_identifier()
            elif channel_type == "tx":
                protocol_id = await self._resolve_transmitter_channel_name_protocol_identifier()
        cmd_args = self.device.commands.command_set_channel_name(
            channel_type,
            channel_number,
            new_channel_name,
            protocol_id=protocol_id,
        )
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_channel_name")

        return response

    async def _request_receiver_channel_status_2809(self):
        command_arguments = self.device.commands.command_query_receiver_channel_status_2809()
        return await self.device.dante_command(
            *command_arguments,
            logical_command_name="query_receiver_channel_status_2809",
        )

    async def _request_transmitter_channel_status_2809(self):
        command_arguments = self.device.commands.command_query_transmitter_channel_status_2809()
        return await self.device.dante_command(
            *command_arguments,
            logical_command_name="query_transmitter_channel_status_2809",
        )

    async def _resolve_receiver_channel_name_protocol_identifier(self):
        cached_protocol_identifier = getattr(
            self.device,
            "receiver_channel_name_protocol_identifier",
            None,
        )
        if cached_protocol_identifier is not None:
            return cached_protocol_identifier

        response = await self._request_receiver_channel_status_2809()
        protocol_identifier = receiver_channel_name_protocol_identifier_from_probe(response)
        self.device.receiver_channel_name_protocol_identifier = protocol_identifier
        return protocol_identifier

    async def _resolve_transmitter_channel_name_protocol_identifier(self):
        cached_protocol_identifier = getattr(
            self.device,
            "transmitter_channel_name_protocol_identifier",
            None,
        )
        if cached_protocol_identifier is not None:
            return cached_protocol_identifier

        response = await self._request_transmitter_channel_status_2809()
        protocol_identifier = transmitter_channel_name_protocol_identifier_from_probe(response)
        self.device.transmitter_channel_name_protocol_identifier = protocol_identifier
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
            command_builder = self.device.commands.command_query_receiver_channel_status
            opcode = OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
            page_kind = "receiver_channel_status_page_2809"
            description = "receiver channel status query"
            logical_name = "query_receiver_channel_status_2809"
            cache_attribute = "receiver_channel_name_protocol_identifier"
        else:
            command_builder = self.device.commands.command_query_transmitter_channel_status
            opcode = OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
            page_kind = "transmitter_channel_status_page_2809"
            description = "transmitter channel status query"
            logical_name = "query_transmitter_channel_status_2809"
            cache_attribute = "transmitter_channel_name_protocol_identifier"

        accumulator = ChannelStatusPageAccumulator(protocol_id, opcode)
        request_range = (1, 1, 0)
        while request_range is not None:
            media_type, starting_channel_identifier, ending_channel_identifier = request_range
            command_arguments = command_builder(
                protocol_id=protocol_id,
                media_type=media_type,
                starting_channel_identifier=starting_channel_identifier,
                ending_channel_identifier=ending_channel_identifier,
            )
            response = await self.device.dante_command(
                *command_arguments,
                logical_command_name=logical_name,
            )
            result_code = channel_result_code(response, description)
            if result_code not in (RESULT_CODE_SUCCESS, RESULT_CODE_SUCCESS_EXTENDED):
                raise RuntimeError(f"{description} failed with result 0x{result_code:04X}")
            page = self._parse_status_page(response, description, page_kind)
            request_range = accumulator.add(page)

        setattr(self.device, cache_attribute, protocol_id)
        return accumulator.result()

    async def _query_status_page_2809(self, command_arguments, logical_command_name, description, page_kind):
        from netaudio import core

        response = await self.device.dante_command(
            *command_arguments,
            logical_command_name=logical_command_name,
        )
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
            self.device.commands.command_query_receiver_flow_status_2809(),
            "query_receiver_flow_status_2809",
            "receiver flow status query",
            "receiver_flow_status_page_2809",
        )

    async def query_transmitter_flow_status_2809(self):
        return await self._query_status_page_2809(
            self.device.commands.command_query_transmitter_flow_status_2809(),
            "query_transmitter_flow_status_2809",
            "transmitter flow status query",
            "transmitter_flow_status_page",
        )

    async def identify(self):
        command_arguments = self.device.commands.command_identify()
        await self._send_without_response(command_arguments)

    async def reboot(self, host_mac=None):
        await self._send_registered_system_reset(self.device.commands.command_reboot, host_mac)

    async def factory_reset(self, host_mac=None):
        await self._send_registered_system_reset(self.device.commands.command_factory_reset, host_mac)

    async def clear_configuration(
        self,
        preserve_internet_protocol_settings: bool,
        timeout: float = 2.0,
    ) -> dict:
        application = getattr(self.device, "_app", None)
        if application is None:
            raise RuntimeError("verified clear-configuration requires an active Dante application")
        device_ip_address = str(self.device.ipv4) if self.device.ipv4 else None
        if device_ip_address is None:
            raise RuntimeError("device has no control address")
        return await application.clear_configuration(
            device_ip_address,
            preserve_internet_protocol_settings,
            timeout,
        )

    async def _send_registered_system_reset(self, command_builder, host_mac):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        device_ip_address = str(self.device.ipv4) if self.device.ipv4 else None
        if device_ip_address is None:
            raise RuntimeError("device has no control address")
        await self._register_controller_for_system_reset(device_ip_address, host_mac)
        packet, _, port = command_builder(host_mac=host_mac)
        await self._send_repeated_settings_command(packet, port, 1, 0)

    async def _register_controller_for_system_reset(self, device_ip_address: str, host_mac: bytes) -> None:
        application = getattr(self.device, "_app", None)
        application_service = getattr(application, "cmc", None)
        if application_service is not None and application_service.is_started:
            await application_service.require_registration(device_ip_address, host_mac)
            return

        from netaudio.dante.services.cmc import DanteCMCService

        operation_service = DanteCMCService(host_media_access_control_address=host_mac)
        await operation_service.start()
        try:
            await operation_service.require_registration(device_ip_address, host_mac)
        finally:
            await operation_service.stop()

    async def set_latency(self, latency):
        latency_milliseconds = float(latency)
        if not math.isfinite(latency_milliseconds) or latency_milliseconds < 0:
            raise ValueError("latency must be a finite, nonnegative number")
        cmd_args = self.device.commands.command_set_latency(latency_milliseconds)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_latency")

        return response

    async def set_gain_level(self, channel_number, gain_level, device_type):
        if self.device._app is None:
            raise RuntimeError("verified gain control requires an active Dante application")
        return await self.device._app.set_gain_level_state(
            self.device,
            channel_number,
            gain_level,
            device_type,
        )

    async def enable_aes67(self, is_enabled: bool, host_mac=None, retries=3, retry_delay=0.1):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        packet, _, port = self.device.commands.command_enable_aes67(is_enabled=is_enabled, host_mac=host_mac)
        await self._send_repeated_settings_command(packet, port, retries, retry_delay)

    async def set_aes67_multicast_prefix(self, prefix: str):
        try:
            normalized_prefix = str(ipaddress.IPv4Address(prefix))
        except (ipaddress.AddressValueError, ValueError) as exception:
            raise ValueError("AES67 multicast prefix must be an IPv4 address") from exception
        command_arguments = self.device.commands.command_set_aes67_multicast_prefix(normalized_prefix)
        return await self.device.dante_command(
            *command_arguments,
            logical_command_name="set_aes67_multicast_prefix",
        )

    async def _send_repeated_settings_command(self, packet, port, retries, retry_delay):
        client = self.device._core_client()
        if client is None:
            raise RuntimeError("device has no control address")
        interval_milliseconds = int(round(retry_delay * 1000))
        await asyncio.to_thread(
            client.request,
            packet,
            port,
            False,
            retries,
            interval_milliseconds,
        )

    async def _send_without_response(self, command_arguments):
        await self.device.dante_send_command(*command_arguments)

    async def set_encoding(self, encoding):
        supported_encodings = self.device.supported_encodings
        if supported_encodings is not None and encoding not in supported_encodings:
            raise ValueError(f"requested encoding {encoding} is not supported; device reports {supported_encodings}")
        command_arguments = self.device.commands.command_set_encoding(encoding)
        await self._send_without_response(command_arguments)

    async def _request_sample_rate_change(self, sample_rate):
        command_arguments = self.device.commands.command_set_sample_rate(sample_rate)
        await self._send_without_response(command_arguments)

    async def set_sample_rate(self, sample_rate, confirm_destructive=False):
        if self.device._app is None:
            raise RuntimeError("topology-safe sample-rate control requires an active Dante application")
        return await self.device._app.set_sample_rate_state(
            self.device,
            sample_rate,
            confirm_destructive=confirm_destructive,
        )

    async def set_sample_rate_pullup(self, raw_value):
        if self.device._app is None:
            raise RuntimeError("verified sample rate pull-up control requires an active Dante application")
        return await self.device._app.set_sample_rate_pullup_state(self.device, raw_value)

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
        return await self.add_subscription_by_name(rx_channel.number, tx_channel_name, tx_device.name)

    async def add_subscription_by_name(self, rx_channel_number, tx_channel_name, tx_device_name):
        cmd_args = self.device.commands.command_add_subscription(rx_channel_number, tx_channel_name, tx_device_name)
        async with self.device.topology_mutation_lock:
            response = await self.device.dante_command(*cmd_args, logical_command_name="add_subscription")

        return response

    async def add_subscriptions(self, subscriptions):
        records = []
        for rx_channel, tx_channel, tx_device in subscriptions:
            tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
            records.append((rx_channel.number, tx_channel_name, tx_device.name))
        return await self.add_subscriptions_by_name(records)

    async def add_subscriptions_by_name(self, records):
        cmd_args = self.device.commands.command_add_subscriptions(records)
        async with self.device.topology_mutation_lock:
            response = await self.device.dante_command(*cmd_args, logical_command_name="add_subscriptions")

        return response

    async def remove_subscription(self, rx_channel):
        cmd_args = self.device.commands.command_remove_subscription(rx_channel.number)
        async with self.device.topology_mutation_lock:
            response = await self.device.dante_command(*cmd_args, logical_command_name="remove_subscription")

        return response

    async def remove_subscriptions(self, rx_channels):
        channel_numbers = [channel.number for channel in rx_channels]
        cmd_args = self.device.commands.command_remove_subscriptions(channel_numbers)
        async with self.device.topology_mutation_lock:
            response = await self.device.dante_command(*cmd_args, logical_command_name="remove_subscriptions")

        return response

    async def reset_channel_name(self, channel_type, channel_number):
        cmd_args = self.device.commands.command_reset_channel_name(channel_type, channel_number)
        response = await self.device.dante_command(*cmd_args, logical_command_name="reset_channel_name")

        return response

    async def set_name(self, name):
        error = validate_dante_name(name)
        if error:
            raise ValueError(error)

        cmd_args = self.device.commands.command_set_name(name)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_name")

        return response

    async def reset_name(self):
        cmd_args = self.device.commands.command_reset_name()
        response = await self.device.dante_command(*cmd_args, logical_command_name="reset_name")

        return response

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
        client = self.device._core_client()
        if client is None:
            return None
        import asyncio

        settings = await asyncio.to_thread(client.get_device_settings)
        self._apply_device_settings(settings)
        return settings

    async def get_latency_settings(self):
        from netaudio import core

        command_arguments = self.device.commands.command_query_latency_config()
        response = await self.device.dante_command(
            *command_arguments,
            logical_command_name="query_latency_config",
        )
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
        client = self.device._core_client()
        if client is None:
            return None
        from netaudio import core

        def _query():
            packet = core.build_command({"command": "query_latency_config"})
            try:
                return client.request(packet, client._arc_port)
            except core.NetaudioCoreError as error:
                if error.status != core.STATUS_TIMEOUT:
                    raise
                return None

        response = await asyncio.to_thread(_query)
        if response is None:
            return None
        configured = core.parse_response("aes67_configured", response)
        settings = core.parse_response("device_settings", response)
        if configured is not None:
            self.device.aes67_configured = configured
        prefix = settings.get("aes67_multicast_prefix") if isinstance(settings, dict) else None
        if prefix is not None:
            self.device.aes67_multicast_prefix = prefix
        return configured


LOCK_OPERATION_LOCK = 1
LOCK_OPERATION_UNLOCK = 2

LOCK_STATUS_SUCCESS = 0x0000
LOCK_STATUS_ALREADY = 0x1102


def validate_pin(pin: str) -> str | None:
    if len(pin) != 4:
        return "PIN must be exactly 4 digits"
    if not pin.isdigit():
        return "PIN must contain only digits"
    return None


def _lock_key_not_configured() -> dict:
    return {
        "status": None,
        "lock_state": None,
        "success": False,
        "already": False,
        "error": "device_lock_key not configured",
        "not_configured": True,
    }


def _lock_key_invalid(actual_length: int, expected_length: int) -> dict:
    return {
        "status": None,
        "lock_state": None,
        "success": False,
        "already": False,
        "error": f"device_lock_key must be {expected_length} bytes, got {actual_length}",
        "not_configured": False,
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
        "status": getattr(error, "status", None),
        "lock_state": None,
        "success": False,
        "already": False,
        "error": str(error),
        "not_configured": False,
    }
