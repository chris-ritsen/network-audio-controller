import asyncio
import logging
import re
import struct

logger = logging.getLogger("netaudio")

DANTE_NAME_MAX_LENGTH = 31
DANTE_NAME_PATTERN = re.compile(r'^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$')


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
        cmd_args = self.device.commands.command_set_channel_name(
            channel_type, channel_number, new_channel_name
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_channel_name"
        )

        return response

    async def factory_reset(self):
        if not hasattr(self.device.commands, "command_factory_reset"):
            raise RuntimeError("factory-reset is not available in this build")
        cmd_args = self.device.commands.command_factory_reset()
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="factory_reset"
        )

        return response

    async def identify(self):
        command_identify_args = self.device.commands.command_identify()
        packet = command_identify_args[0]
        port = command_identify_args[2]

        if self.device._app is not None:
            device_ip = str(self.device.ipv4) if self.device.ipv4 else None
            if device_ip:
                self.device._app.settings.send(packet, device_ip, port)
        else:
            await self.device.dante_send_command(packet, port=port)

    async def reboot(self, host_mac=None, retries=3, retry_delay=0.1):
        import asyncio
        import socket as socket_module
        if host_mac is None:
            from netaudio.dante.services.cmc import _get_host_mac
            host_mac = _get_host_mac()
        packet, _, port = self.device.commands.command_reboot(host_mac=host_mac)
        device_ip = str(self.device.ipv4)

        if self.device._app is not None:
            for attempt in range(retries):
                self.device._app.settings.send(packet, device_ip, port)
                if attempt < retries - 1:
                    await asyncio.sleep(retry_delay)
        else:
            sock = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_DGRAM)
            try:
                for attempt in range(retries):
                    sock.sendto(packet, (device_ip, port))
                    if attempt < retries - 1:
                        await asyncio.sleep(retry_delay)
            finally:
                sock.close()

    async def set_latency(self, latency):
        cmd_args = self.device.commands.command_set_latency(latency)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_latency"
        )

        return response

    async def set_gain_level(self, channel_number, gain_level, device_type):
        cmd_args = self.device.commands.command_set_gain_level(
            channel_number, gain_level, device_type
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_gain_level"
        )

        return response

    async def enable_aes67(self, is_enabled: bool):
        cmd_args = self.device.commands.command_enable_aes67(is_enabled=is_enabled)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="enable_aes67"
        )

        return response

    async def set_encoding(self, encoding):
        cmd_args = self.device.commands.command_set_encoding(encoding)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_encoding"
        )

        return response

    async def set_sample_rate(self, sample_rate):
        cmd_args = self.device.commands.command_set_sample_rate(sample_rate)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_sample_rate"
        )

        return response

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
        cmd_args = self.device.commands.command_add_subscription(
            rx_channel.number, tx_channel_name, tx_device.name
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="add_subscription"
        )

        return response

    async def remove_subscription(self, rx_channel):
        cmd_args = self.device.commands.command_remove_subscription(rx_channel.number)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="remove_subscription"
        )

        return response

    async def reset_channel_name(self, channel_type, channel_number):
        cmd_args = self.device.commands.command_reset_channel_name(channel_type, channel_number)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="reset_channel_name"
        )

        return response

    async def set_name(self, name):
        error = validate_dante_name(name)
        if error:
            raise ValueError(error)

        cmd_args = self.device.commands.command_set_name(name)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_name"
        )

        return response

    async def reset_name(self):
        cmd_args = self.device.commands.command_reset_name()
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="reset_name"
        )

        return response

    async def lock_device(self, pin: str, key: bytes) -> dict:
        return await _device_lock_operation(str(self.device.ipv4), pin, key, operation=1)

    async def unlock_device(self, pin: str, key: bytes) -> dict:
        return await _device_lock_operation(str(self.device.ipv4), pin, key, operation=2)

    async def get_device_settings(self):
        cmd_args = self.device.commands.command_device_settings()
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="get_device_settings"
        )

        if response:
            settings = self.device.parser.parse_device_settings(response)
            if "latency" in settings:
                self.device.latency = settings["latency"]
            if "sample_rate" in settings:
                self.device.sample_rate = settings["sample_rate"]
            if "min_latency_ns" in settings and settings["min_latency_ns"] is not None:
                self.device.min_latency = settings["min_latency_ns"] / 1_000_000.0
            if "max_latency_ns" in settings and settings["max_latency_ns"] is not None:
                self.device.max_latency = settings["max_latency_ns"] / 1_000_000.0
            return settings

        return None


LOCK_DDP_HEADER = struct.pack(">HHHH", 8, 0x0001, 0x1000, 0x0200)

LOCK_OPERATION_LOCK = 1
LOCK_OPERATION_UNLOCK = 2

LOCK_STATUS_SUCCESS = 0x0000
LOCK_STATUS_ALREADY = 0x1102

LOCK_STATE_UNLOCKED = 0x0000
LOCK_STATE_LOCKED = 0x0001

LOCK_TIMEOUT = 5.0
LOCK_MAX_RETRIES = 4


def validate_pin(pin: str) -> str | None:
    if len(pin) != 4:
        return "PIN must be exactly 4 digits"
    if not pin.isdigit():
        return "PIN must contain only digits"
    return None


async def _device_lock_operation(device_ip: str, pin: str, key: bytes, operation: int) -> dict:
    import nacl.bindings
    from netaudio.dante.const import DEVICE_LOCK_PORT

    loop = asyncio.get_running_loop()
    sock = await loop.run_in_executor(None, _create_lock_socket)

    try:
        address = (device_ip, DEVICE_LOCK_PORT)
        sequence = 1

        for attempt in range(LOCK_MAX_RETRIES):
            challenge_request = LOCK_DDP_HEADER + struct.pack(
                ">HHHHHH", 0x000C, 0x2FFE, 0x0004, 0x0000, sequence, 0x0000
            )
            sock.sendto(challenge_request, address)

            try:
                data, _ = sock.recvfrom(1024)
            except TimeoutError:
                logger.warning(f"Lock challenge timeout (attempt {attempt + 1}/{LOCK_MAX_RETRIES})")
                continue

            message = data[8:]
            nonce_offset = struct.unpack(">H", message[12:14])[0]
            nonce_length = struct.unpack(">H", message[14:16])[0]
            nonce = message[nonce_offset:nonce_offset + nonce_length]

            if len(nonce) != 24:
                logger.warning(f"Invalid nonce length: {len(nonce)}")
                continue

            token = nacl.bindings.crypto_secretbox(pin.encode("ascii"), nonce, key)

            auth_header = struct.pack(
                ">HHHHHHHHH",
                0x0028, 0x2FFF, 0x0004, 0x0008,
                sequence, 0x0000,
                operation,
                0x0014, 0x0014,
            )
            auth_message = LOCK_DDP_HEADER + auth_header + struct.pack(">H", 0x0000) + token
            sock.sendto(auth_message, address)

            try:
                response_data, _ = sock.recvfrom(1024)
            except TimeoutError:
                logger.warning(f"Lock auth timeout (attempt {attempt + 1}/{LOCK_MAX_RETRIES})")
                continue

            response_message = response_data[8:]
            status = struct.unpack(">H", response_message[10:12])[0]
            lock_state = struct.unpack(">H", response_message[12:14])[0]

            return {
                "status": status,
                "lock_state": lock_state,
                "success": status in (LOCK_STATUS_SUCCESS, LOCK_STATUS_ALREADY),
                "already": status == LOCK_STATUS_ALREADY,
            }

        raise TimeoutError(f"Lock operation failed after {LOCK_MAX_RETRIES} attempts")
    finally:
        sock.close()


def _create_lock_socket():
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(LOCK_TIMEOUT)
    return sock
