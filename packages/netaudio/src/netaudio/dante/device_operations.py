from __future__ import annotations

import asyncio
import logging
import re

from netaudio.dante.latency import nanoseconds_to_milliseconds

logger = logging.getLogger("netaudio")

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
        command_identify_args = self.device.commands.command_identify()
        packet = command_identify_args[0]
        port = command_identify_args[2]
        await self.device.dante_send_command(packet, port=port)

    async def reboot(self, host_mac=None, retries=3, retry_delay=0.1):
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac

            host_mac = _get_host_mac()
        packet, _, port = self.device.commands.command_reboot(host_mac=host_mac)
        await self._send_repeated_settings_command(packet, port, retries, retry_delay)

    async def set_latency(self, latency):
        cmd_args = self.device.commands.command_set_latency(latency)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_latency")

        return response

    async def set_gain_level(self, channel_number, gain_level, device_type):
        cmd_args = self.device.commands.command_set_gain_level(channel_number, gain_level, device_type)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_gain_level")

        return response

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

    async def set_encoding(self, encoding):
        cmd_args = self.device.commands.command_set_encoding(encoding)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_encoding")

        return response

    async def set_sample_rate(self, sample_rate):
        supported_sample_rates = self.device.supported_sample_rates
        if supported_sample_rates is not None and sample_rate not in supported_sample_rates:
            raise ValueError(
                f"requested sample rate {sample_rate} is not supported; device reports {supported_sample_rates}"
            )
        cmd_args = self.device.commands.command_set_sample_rate(sample_rate)
        response = await self.device.dante_command(*cmd_args, logical_command_name="set_sample_rate")

        return response

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
        result = await _device_lock_operation(str(self.device.ipv4), pin, key, operation=LOCK_OPERATION_LOCK)
        self._apply_lock_result(result)
        return result

    async def unlock_device(self, pin: str, key: bytes) -> dict:
        result = await _device_lock_operation(str(self.device.ipv4), pin, key, operation=LOCK_OPERATION_UNLOCK)
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
        if settings.get("sample_rate"):
            self.device.sample_rate = settings["sample_rate"]
        if settings.get("latency_ns") is not None:
            self.device.latency = nanoseconds_to_milliseconds(settings["latency_ns"])
        if settings.get("min_latency_ns") is not None:
            self.device.min_latency = nanoseconds_to_milliseconds(settings["min_latency_ns"])
        if settings.get("max_latency_ns") is not None:
            self.device.max_latency = nanoseconds_to_milliseconds(settings["max_latency_ns"])
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

LOCK_STATE_UNLOCKED = 0x0000
LOCK_STATE_LOCKED = 0x0001


def validate_pin(pin: str) -> str | None:
    if len(pin) != 4:
        return "PIN must be exactly 4 digits"
    if not pin.isdigit():
        return "PIN must contain only digits"
    return None


async def _device_lock_operation(device_ip: str, pin: str, key: bytes, operation: int) -> dict:
    from netaudio import core

    if operation not in (LOCK_OPERATION_LOCK, LOCK_OPERATION_UNLOCK):
        raise ValueError(f"unknown lock operation: {operation}")

    def run() -> dict:
        with core.CoreClient(device_ip) as client:
            if operation == LOCK_OPERATION_LOCK:
                return client.lock(pin, key)
            return client.unlock(pin, key)

    return await asyncio.to_thread(run)
