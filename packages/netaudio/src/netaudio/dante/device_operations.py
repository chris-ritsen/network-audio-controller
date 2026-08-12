from __future__ import annotations

import asyncio
import math
import re

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

    async def set_channel_name(self, channel_type, channel_number, new_channel_name):
        cmd_args = self.device.commands.command_set_channel_name(channel_type, channel_number, new_channel_name)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_channel_name")

        return response

    async def factory_reset(self):
        if not hasattr(self.device.commands, "command_factory_reset"):
            raise RuntimeError("factory-reset is not available in this build")
        cmd_args = self.device.commands.command_factory_reset()
        response = await self.device.dante_command(*cmd_args, logical_command_name="factory_reset")

        return response

    async def identify(self):
        command_arguments = self.device.commands.command_identify()
        await self._send_without_response(command_arguments)

    async def reboot(self, host_mac=None, retries=3, retry_delay=0.1):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        packet, _, port = self.device.commands.command_reboot(host_mac=host_mac)
        await self._send_repeated_settings_command(packet, port, retries, retry_delay)

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

    async def set_sample_rate(self, sample_rate):
        supported_sample_rates = self.device.supported_sample_rates
        if supported_sample_rates is not None and sample_rate not in supported_sample_rates:
            raise ValueError(
                f"requested sample rate {sample_rate} is not supported; device reports {supported_sample_rates}"
            )
        command_arguments = self.device.commands.command_set_sample_rate(sample_rate)
        await self._send_without_response(command_arguments)

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
        return await self.add_subscription_by_name(rx_channel.number, tx_channel_name, tx_device.name)

    async def add_subscription_by_name(self, rx_channel_number, tx_channel_name, tx_device_name):
        cmd_args = self.device.commands.command_add_subscription(rx_channel_number, tx_channel_name, tx_device_name)
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
        response = await self.device.dante_command(*cmd_args, logical_command_name="add_subscriptions")

        return response

    async def remove_subscription(self, rx_channel):
        cmd_args = self.device.commands.command_remove_subscription(rx_channel.number)
        response = await self.device.dante_command(*cmd_args, logical_command_name="remove_subscription")

        return response

    async def remove_subscriptions(self, rx_channels):
        channel_numbers = [channel.number for channel in rx_channels]
        cmd_args = self.device.commands.command_remove_subscriptions(channel_numbers)
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
        result = await core_lock_device(str(self.device.ipv4), pin, key)
        self._apply_lock_result(result)
        return result

    async def unlock_device(self, pin: str, key: bytes) -> dict:
        key_error = _validate_lock_key(key)
        if key_error:
            return key_error
        result = await core_unlock_device(str(self.device.ipv4), pin, key)
        self._apply_lock_result(result)
        return result

    def _apply_lock_result(self, result: dict) -> None:
        if not result.get("success"):
            return
        self.device.is_locked = result.get("lock_state") == LOCK_STATE_LOCKED
        if self.device._app is not None:
            from netaudio.dante.events import DanteEvent, EventType

            self.device._app.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=self.device.name,
                    server_name=self.device.server_name,
                )
            )

    async def get_device_settings(self):
        client = self.device._core_client()
        if client is None:
            return None
        import asyncio

        settings = await asyncio.to_thread(client.get_device_settings)
        controls = latency_controls_from_settings(settings)
        if settings.get("sample_rate"):
            controls["sample_rate"] = settings["sample_rate"]
        self.device.apply_controls(controls)
        return settings

    async def get_aes67_configured(self):
        client = self.device._core_client()
        if client is None:
            return None
        configured = await asyncio.to_thread(client.get_aes67_configured)
        if configured is not None:
            self.device.aes67_configured = configured
        return configured

LOCK_OPERATION_LOCK = 1
LOCK_OPERATION_UNLOCK = 2

LOCK_STATUS_SUCCESS = 0x0000
LOCK_STATUS_ALREADY = 0x1102

LOCK_STATE_LOCKED = 0x0001


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
