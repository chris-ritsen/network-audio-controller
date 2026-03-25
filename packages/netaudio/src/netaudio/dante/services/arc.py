import logging
import struct

from netaudio.dante.const import (
    FLOW_PROTOCOL_IDS,
    FLOW_TYPE_MULTICAST,
    HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS,
    OPCODE_CREATE_TX_FLOW,
    OPCODE_CREATE_TX_FLOW_2809,
    OPCODE_DELETE_TX_FLOW,
    OPCODE_DELETE_TX_FLOW_2809,
    OPCODE_QUERY_TX_FLOWS,
    OPCODE_QUERY_TX_FLOWS_2809,
    RESULT_CODE_LOCK_REJECTION,
    RESULT_CODE_SUCCESS,
)
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.service import DanteUnicastService

logger = logging.getLogger("netaudio")


class DanteARCService(DanteUnicastService):
    def __init__(self, packet_store=None, dissect=False):
        super().__init__(packet_store=packet_store, dissect=dissect)
        self._commands = DanteDeviceCommands()
        self._parser = DanteDeviceParser()

    async def get_device_name(self, device_ip: str, arc_port: int) -> str | None:
        command_args = self._commands.command_device_name(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_device_name",
        )
        if response and len(response) > 10:
            return response[10:-1].decode("ascii")
        return None

    async def get_channel_count(self, device_ip: str, arc_port: int) -> tuple[int, int, bool | None] | None:
        command_args = self._commands.command_channel_count(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_channel_count",
        )
        if response and len(response) >= 16:
            tx_count = int.from_bytes(response[13:14], "big")
            rx_count = int.from_bytes(response[15:16], "big")
            lock_status = None
            if len(response) >= 36:
                lock_field = int.from_bytes(response[34:36], "big")
                lock_status = lock_field != 0
            return tx_count, rx_count, lock_status
        return None

    async def get_aes67_config(self, device_ip: str, arc_port: int) -> bool | None:
        command_args = self._commands.command_get_aes67_config(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_aes67_config",
        )
        if response:
            if b'\x63\x00\x03' in response:
                return True
            elif b'\x63\x00\x01' in response:
                return False
        return None

    async def get_rx_channels(self, device, arc_port: int):
        device_ip = str(device.ipv4)

        async def command_func(command, service_type=None, port=None, logical_command_name="unknown"):
            return await self.request(
                command, device_ip, arc_port,
                device_name=device.name,
                logical_command_name=logical_command_name,
            )

        return await self._parser.get_rx_channels(device, command_func)

    async def get_tx_channels(self, device, arc_port: int):
        device_ip = str(device.ipv4)

        async def command_func(command, service_type=None, port=None, logical_command_name="unknown"):
            return await self.request(
                command, device_ip, arc_port,
                device_name=device.name,
                logical_command_name=logical_command_name,
            )

        return await self._parser.get_tx_channels(device, command_func)

    async def get_controls(self, device, arc_port: int) -> None:
        device_ip = str(device.ipv4)

        try:
            if not device.name:
                name = await self.get_device_name(device_ip, arc_port)
                if name:
                    device.name = name
                else:
                    logger.debug(f"Failed to get device name for {device.server_name}")

            counts = await self.get_channel_count(device_ip, arc_port)
            if counts:
                device.tx_count = device.tx_count_raw = counts[0]
                device.rx_count = device.rx_count_raw = counts[1]
                if counts[2] is not None:
                    device.is_locked = counts[2]

            if device.aes67_enabled is None:
                try:
                    aes67_status = await self.get_aes67_config(device_ip, arc_port)
                    if aes67_status is not None:
                        device.aes67_enabled = aes67_status
                except Exception as exception:
                    logger.debug(f"Error getting AES67 config: {exception}")

            if device.tx_count:
                tx_channels = await self.get_tx_channels(device, arc_port)
                if tx_channels:
                    device.tx_channels = tx_channels

            if device.rx_count:
                rx_channels, subscriptions = await self.get_rx_channels(device, arc_port)
                if rx_channels:
                    device.rx_channels = rx_channels
                    device.subscriptions = subscriptions

            # if getattr(device, "model_id", None) in HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS:
            #     lock_state = await self.probe_lock_state(device_ip, arc_port)
            #     if lock_state is not None:
            #         device.is_locked = lock_state

            device.error = None
        except Exception as exception:
            device.error = exception
            logger.debug(f"Error getting controls for {device.server_name}: {exception}")

    async def set_channel_name(
        self, device_ip: str, arc_port: int, channel_type: str, channel_number: int, new_name: str
    ) -> bytes | None:
        command_args = self._commands.command_set_channel_name(channel_type, channel_number, new_name)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_channel_name",
        )

    async def reset_channel_name(
        self, device_ip: str, arc_port: int, channel_type: str, channel_number: int
    ) -> bytes | None:
        command_args = self._commands.command_reset_channel_name(channel_type, channel_number)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="reset_channel_name",
        )

    async def add_subscription(
        self, device_ip: str, arc_port: int,
        rx_channel_number: int, tx_channel_name: str, tx_device_name: str,
    ) -> bytes | None:
        command_args = self._commands.command_add_subscription(
            rx_channel_number, tx_channel_name, tx_device_name
        )
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="add_subscription",
        )

    async def remove_subscription(
        self, device_ip: str, arc_port: int, rx_channel_number: int
    ) -> bytes | None:
        command_args = self._commands.command_remove_subscription(rx_channel_number)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="remove_subscription",
        )

    async def set_latency(self, device_ip: str, arc_port: int, latency: float) -> bytes | None:
        command_args = self._commands.command_set_latency(latency)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_latency",
        )

    async def probe_lock_state(self, device_ip: str, arc_port: int) -> bool | None:
        command_args = self._commands.command_set_latency(1.0)
        response = await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="probe_lock_state",
        )
        if response and len(response) >= 10:
            result_code = struct.unpack(">H", response[8:10])[0]
            if result_code == RESULT_CODE_LOCK_REJECTION:
                return True
            if result_code == RESULT_CODE_SUCCESS:
                return False
        return None

    async def set_name(self, device_ip: str, arc_port: int, name: str) -> bytes | None:
        command_args = self._commands.command_set_name(name)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_name",
        )

    async def reset_name(self, device_ip: str, arc_port: int) -> bytes | None:
        command_args = self._commands.command_reset_name()
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="reset_name",
        )

    def _flow_opcodes(self, flow_protocol_id: int) -> tuple[int, int, int]:
        if flow_protocol_id == 0x2809:
            return (OPCODE_QUERY_TX_FLOWS_2809, OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW_2809)
        return (OPCODE_QUERY_TX_FLOWS, OPCODE_CREATE_TX_FLOW, OPCODE_DELETE_TX_FLOW)

    def _build_flow_packet(self, protocol_id: int, opcode: int, body: bytes) -> bytes:
        transaction_id = self._next_transaction_id()
        length = 10 + len(body)
        header = struct.pack(">HHHHH", protocol_id, length, transaction_id, opcode, 0x0000)
        return header + body

    async def detect_flow_protocol(self, device_ip: str, arc_port: int) -> int | None:
        for protocol_id in FLOW_PROTOCOL_IDS:
            query_opcode = OPCODE_QUERY_TX_FLOWS_2809 if protocol_id == 0x2809 else OPCODE_QUERY_TX_FLOWS
            packet = self._build_flow_packet(protocol_id, query_opcode, b"\x00\x00")
            response = await self.request(
                packet, device_ip, arc_port,
                timeout=0.5,
                logical_command_name="detect_flow_protocol",
            )
            if response and len(response) >= 10:
                result_code = struct.unpack(">H", response[8:10])[0]
                if result_code == RESULT_CODE_SUCCESS:
                    return protocol_id
        return None

    async def query_tx_flows(self, device_ip: str, arc_port: int, flow_protocol_id: int) -> list[dict] | None:
        query_opcode, _, _ = self._flow_opcodes(flow_protocol_id)
        packet = self._build_flow_packet(flow_protocol_id, query_opcode, b"\x00\x00")
        response = await self.request(
            packet, device_ip, arc_port,
            timeout=1.0,
            logical_command_name="query_tx_flows",
        )
        if not response or len(response) < 12:
            return None

        result_code = struct.unpack(">H", response[8:10])[0]
        if result_code != RESULT_CODE_SUCCESS:
            return None

        body = response[10:]
        if len(body) < 2:
            return []

        max_flow_slots = body[0]
        active_count = body[1]

        flows = []
        offset = 2
        record_index = 0

        while record_index < active_count and offset + 4 <= len(body):
            flow_number = struct.unpack(">H", body[offset:offset + 2])[0]
            record_pointer = struct.unpack(">H", body[offset + 2:offset + 4])[0]

            record_body_offset = record_pointer - 10
            if 0 <= record_body_offset < len(body):
                flow_record = self._parse_flow_record(body, record_body_offset, flow_number, response)
                if flow_record:
                    flows.append(flow_record)

            offset += 4
            record_index += 1

        return flows

    def _parse_flow_record(self, body: bytes, offset: int, flow_number: int, full_response: bytes) -> dict | None:
        if offset + 20 > len(body):
            return None

        flow_type = struct.unpack(">H", body[offset + 2:offset + 4])[0]
        sample_rate = struct.unpack(">I", body[offset + 4:offset + 8])[0]
        encoding = struct.unpack(">H", body[offset + 8:offset + 10])[0]
        fpp = struct.unpack(">H", body[offset + 10:offset + 12])[0]

        channel_count = 0
        channels = []

        if flow_type == FLOW_TYPE_MULTICAST:
            if offset + 24 <= len(body):
                channel_count = struct.unpack(">H", body[offset + 22:offset + 24])[0]
                channel_offset = offset + 24
                for channel_index in range(channel_count):
                    if channel_offset + 2 <= len(body):
                        channel_number = struct.unpack(">H", body[channel_offset:channel_offset + 2])[0]
                        channels.append(channel_number)
                        channel_offset += 2

        return {
            "flow_number": flow_number,
            "flow_type": "multicast" if flow_type == FLOW_TYPE_MULTICAST else f"0x{flow_type:04X}",
            "sample_rate": sample_rate,
            "encoding": encoding,
            "fpp": fpp,
            "channel_count": channel_count,
            "channels": channels,
        }

    async def create_tx_flow(
        self,
        device_ip: str,
        arc_port: int,
        flow_protocol_id: int,
        flow_slot: int,
        channels: list[int],
    ) -> bytes | None:
        _, create_opcode, _ = self._flow_opcodes(flow_protocol_id)

        if flow_protocol_id == 0x2809:
            body = self._build_create_flow_body_2809(flow_slot, channels)
        else:
            body = self._build_create_flow_body(flow_slot, channels)

        packet = self._build_flow_packet(flow_protocol_id, create_opcode, body)
        return await self.request(
            packet, device_ip, arc_port,
            timeout=2.0,
            logical_command_name="create_tx_flow",
        )

    def _build_create_flow_body(self, flow_slot: int, channels: list[int]) -> bytes:
        body = struct.pack(">HH", 0x0101, 0x0010)
        body += struct.pack(">HH", 0x0000, flow_slot)
        body += struct.pack(">H", FLOW_TYPE_MULTICAST)
        body += b"\x00" * 10
        body += struct.pack(">H", len(channels))
        for channel_number in channels:
            body += struct.pack(">H", channel_number)
        pointer = 10 + len(body) + 2 + 2
        body += struct.pack(">H", pointer)
        body += b"\x00\x02"
        body += b"\x0a\x00"
        body += b"\x00" * 14
        body += b"\x00\x01\x00\x00"
        return body

    def _build_create_flow_body_2809(self, flow_slot: int, channels: list[int]) -> bytes:
        body = struct.pack(">HH", 0x0101, 0x0001)
        body += struct.pack(">HH", 0x0000, flow_slot)
        body += struct.pack(">H", FLOW_TYPE_MULTICAST)
        body += b"\x00" * 10
        body += struct.pack(">H", len(channels))
        for channel_number in channels:
            body += struct.pack(">H", channel_number)
        pointer = 10 + len(body) + 2 + 2
        body += struct.pack(">H", pointer)
        body += b"\x00\x02"
        body += b"\x0a\x00"
        body += b"\x00" * 14
        body += b"\x00\x01\x00\x00"
        return body

    async def delete_tx_flow(
        self,
        device_ip: str,
        arc_port: int,
        flow_protocol_id: int,
        flow_slot: int,
    ) -> bytes | None:
        _, _, delete_opcode = self._flow_opcodes(flow_protocol_id)
        body = struct.pack(">HHH", 0x0001, 0x0000, flow_slot)
        packet = self._build_flow_packet(flow_protocol_id, delete_opcode, body)
        return await self.request(
            packet, device_ip, arc_port,
            timeout=2.0,
            logical_command_name="delete_tx_flow",
        )
