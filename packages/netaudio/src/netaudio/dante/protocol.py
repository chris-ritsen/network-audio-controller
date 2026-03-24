import random
import socket
import struct
from dataclasses import dataclass, field
from typing import Optional

from netaudio.dante.const import (
    DEVICE_SETTINGS_INFO_LATENCY,
    DEVICE_SETTINGS_INFO_SAMPLE_RATE,
    OPCODE_CHANNEL_COUNT,
    OPCODE_DEVICE_NAME,
    OPCODE_RX_CHANNELS,
    OPCODE_TX_CHANNEL_INFO,
    PROTOCOL_ID,
    RESULT_CODE_SUCCESS,
    RESULT_CODE_SUCCESS_EXTENDED,
    SUBSCRIPTION_STATUS_NONE,
)


@dataclass
class ChannelCount:
    capability_flags: int
    tx_count: int
    rx_count: int
    tx_active: int
    rx_active: int
    max_tx_flows: int
    max_rx_flows: int


@dataclass
class DeviceInfo:
    model_name: str
    display_name: str
    model_code: str
    port: str
    raw_metadata: bytes = field(repr=False, default=b"")


@dataclass
class DeviceSettings:
    sample_rate: Optional[int] = None
    latency_us: Optional[int] = None
    min_latency_us: Optional[int] = None
    max_latency_us: Optional[int] = None
    info_codes: dict = field(default_factory=dict)


@dataclass
class RxChannel:
    number: int
    name: str
    tx_channel_name: Optional[str] = None
    tx_device_name: Optional[str] = None
    status_code: int = 0
    subscription_status: int = SUBSCRIPTION_STATUS_NONE
    sample_rate: Optional[int] = None


@dataclass
class TxChannel:
    number: int
    name: str
    friendly_name: Optional[str] = None
    sample_rate: Optional[int] = None
    group: int = 0


@dataclass
class DanteResponse:
    protocol: int
    length: int
    transaction_id: int
    opcode: int
    result_code: int
    body: bytes


class DantePacket:
    @staticmethod
    def build_request(
        opcode: int,
        payload: bytes = b"\x00\x00",
        protocol: int = PROTOCOL_ID,
        transaction_id: Optional[int] = None,
    ) -> bytes:
        if transaction_id is None:
            transaction_id = random.randint(0, 0xFFFF)

        length = 8 + len(payload)
        header = struct.pack(">HHHH", protocol, length, transaction_id, opcode)
        return header + payload

    @staticmethod
    def parse_response(data: bytes) -> DanteResponse:
        if len(data) < 10:
            raise ValueError(f"Response too short: {len(data)} bytes")

        protocol, length, transaction_id, opcode, result = struct.unpack(
            ">HHHHH", data[:10]
        )
        body = data[10:] if len(data) > 10 else b""

        return DanteResponse(
            protocol=protocol,
            length=length,
            transaction_id=transaction_id,
            opcode=opcode,
            result_code=result,
            body=body,
        )


class DanteParser:
    @staticmethod
    def _get_string(data: bytes, offset: int) -> str:
        if offset >= len(data):
            return ""

        end = data.find(b"\x00", offset)

        if end == -1:
            end = len(data)

        return data[offset:end].decode("utf-8", errors="replace")

    @staticmethod
    def parse_channel_count(response: DanteResponse) -> ChannelCount:
        body = response.body

        if len(body) < 16:
            raise ValueError(f"Channel count body too short: {len(body)}")

        flags, tx, rx, tx_active, rx_active, max_tx, max_rx = struct.unpack(
            ">HHHHHHH", body[:14]
        )

        return ChannelCount(
            capability_flags=flags,
            tx_count=tx,
            rx_count=rx,
            tx_active=tx_active,
            rx_active=rx_active,
            max_tx_flows=max_tx,
            max_rx_flows=max_rx,
        )

    @staticmethod
    def parse_device_info(response: DanteResponse) -> DeviceInfo:
        body = response.body

        if len(body) < 18:
            raise ValueError(f"Device info body too short: {len(body)}")

        def get_string_at_pointer(pointer: int) -> str:
            if pointer == 0:
                return ""

            offset = pointer - 10

            if offset < 0 or offset >= len(body):
                return ""

            return DanteParser._get_string(body, offset)

        code_pointer = struct.unpack(">H", body[6:8])[0]
        port_pointer = struct.unpack(">H", body[8:10])[0]

        model_pointer = struct.unpack(">H", body[12:14])[0]
        display_pointer = struct.unpack(">H", body[14:16])[0]

        model_name = get_string_at_pointer(model_pointer)
        display_name = get_string_at_pointer(display_pointer)

        return DeviceInfo(
            model_name=model_name,
            display_name=display_name,
            model_code=get_string_at_pointer(code_pointer),
            port=get_string_at_pointer(port_pointer),
            raw_metadata=body[18:50] if len(body) > 50 else body[18:],
        )

    @staticmethod
    def parse_device_settings(response: DanteResponse) -> DeviceSettings:
        body = response.body

        if len(body) < 2:
            raise ValueError(f"Device settings body too short: {len(body)}")

        field_count = body[0]
        record_count = body[1]
        settings = DeviceSettings()
        settings.info_codes = {}
        offset = 2

        for _ in range(record_count):
            if offset + 4 > len(body):
                break

            info_code, value_offset = struct.unpack(">HH", body[offset : offset + 4])
            offset += 4

            if value_offset + 4 <= len(response.body) + 10:
                value_body_offset = value_offset - 10
                if 0 <= value_body_offset < len(body) - 3:
                    value = struct.unpack(
                        ">I", body[value_body_offset : value_body_offset + 4]
                    )[0]
                    settings.info_codes[info_code] = value

                    if info_code == DEVICE_SETTINGS_INFO_SAMPLE_RATE:
                        settings.sample_rate = value
                    elif info_code == DEVICE_SETTINGS_INFO_LATENCY:
                        settings.latency_us = value
                    elif info_code == 0x8205:
                        settings.min_latency_us = value
                    elif info_code == 0x8302:
                        settings.max_latency_us = value

        return settings

    @staticmethod
    def parse_rx_channels(response: DanteResponse) -> list[RxChannel]:
        body = response.body
        channels = []

        if len(body) < 4:
            return channels

        record_size = 20
        header_size = 2
        record_start = header_size
        record_index = 0

        while record_start + record_size <= len(body):
            record = body[record_start : record_start + record_size]
            if len(record) < record_size:
                break

            (
                channel_number,
                flags,
                sample_rate_offset,
                tx_channel_offset,
                tx_device_offset,
                rx_channel_offset,
                status,
                subscription_status_code,
            ) = struct.unpack(">HHHHHHHH", record[:16])

            if channel_number == 0 or rx_channel_offset > len(body) + 100:
                break

            def get_string_at_pointer(pointer: int) -> str:
                if pointer == 0:
                    return ""

                string_offset = pointer - 10

                if string_offset < 0 or string_offset >= len(body):
                    return ""

                return DanteParser._get_string(body, string_offset)

            sample_rate = None

            if sample_rate_offset > 0:
                sample_rate_body_offset = sample_rate_offset - 10

                if 0 <= sample_rate_body_offset < len(body) - 3:
                    sample_rate = struct.unpack(
                        ">I",
                        body[sample_rate_body_offset : sample_rate_body_offset + 4],
                    )[0]

            channel = RxChannel(
                number=channel_number,
                name=get_string_at_pointer(rx_channel_offset),
                tx_channel_name=get_string_at_pointer(tx_channel_offset) or None,
                tx_device_name=get_string_at_pointer(tx_device_offset) or None,
                status_code=status,
                subscription_status=subscription_status_code,
                sample_rate=sample_rate if sample_rate and sample_rate > 0 else None,
            )

            channels.append(channel)

            record_start += record_size
            record_index += 1

            if record_index > 64:
                break

        return channels

    @staticmethod
    def parse_tx_channels(response: DanteResponse) -> list[TxChannel]:
        body = response.body
        channels = []

        if len(body) < 4:
            return channels

        record_size = 8
        header_size = 2
        record_start = header_size
        record_index = 0

        while record_start + record_size <= len(body):
            record = body[record_start : record_start + record_size]
            if len(record) < record_size:
                break

            channel_number, sample_rate_offset, group, name_offset = struct.unpack(
                ">HHHH", record
            )

            expected_channel = channels[-1].number + 1 if channels else channel_number
            if channel_number == 0 or channel_number != expected_channel:
                break

            def get_string_at_pointer(pointer: int) -> str:
                if pointer == 0:
                    return ""

                string_offset = pointer - 10

                if string_offset < 0 or string_offset >= len(body):
                    return ""

                return DanteParser._get_string(body, string_offset)

            sample_rate = None

            if sample_rate_offset > 0:
                sample_rate_body_offset = sample_rate_offset - 10

                if 0 <= sample_rate_body_offset < len(body) - 3:
                    sample_rate = struct.unpack(
                        ">I",
                        body[sample_rate_body_offset : sample_rate_body_offset + 4],
                    )[0]

            channel = TxChannel(
                number=channel_number,
                name=get_string_at_pointer(name_offset),
                sample_rate=sample_rate if sample_rate and sample_rate > 0 else None,
                group=group,
            )

            channels.append(channel)
            record_start += record_size
            record_index += 1

            if record_index > 128:
                break

        return channels


class DanteClient:
    def __init__(self, ip: str, port: int = 4440, timeout: float = 2.0):
        self.ip = ip
        self.port = port
        self.timeout = timeout

    def _send_receive(self, request: bytes) -> Optional[bytes]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(self.timeout)
        try:
            sock.bind(("", 0))
            sock.sendto(request, (self.ip, self.port))
            response, _ = sock.recvfrom(4096)
            return response
        except socket.timeout:
            return None
        finally:
            sock.close()

    def query(
        self, opcode: int, payload: bytes = b"\x00\x00"
    ) -> Optional[DanteResponse]:
        request = DantePacket.build_request(opcode, payload)
        response = self._send_receive(request)

        if response:
            return DantePacket.parse_response(response)

        return None

    def get_channel_count(self) -> Optional[ChannelCount]:
        response = self.query(OPCODE_CHANNEL_COUNT)

        if response and response.result_code == RESULT_CODE_SUCCESS:
            return DanteParser.parse_channel_count(response)

        return None

    def get_device_name(self) -> Optional[str]:
        response = self.query(OPCODE_DEVICE_NAME)

        if response and response.result_code == RESULT_CODE_SUCCESS:
            return response.body.rstrip(b"\x00").decode("utf-8", errors="replace")

        return None

    def get_device_info(self) -> Optional[DeviceInfo]:
        response = self.query(0x1003)

        if response and response.result_code == RESULT_CODE_SUCCESS:
            return DanteParser.parse_device_info(response)

        return None

    def get_device_settings(self) -> Optional[DeviceSettings]:
        response = self.query(0x1100)

        if response and response.result_code == RESULT_CODE_SUCCESS:
            return DanteParser.parse_device_settings(response)

        return None

    def get_rx_channels(self, page: int = 0) -> list[RxChannel]:
        payload = struct.pack(">HHHH", 0x0000, 0x0001, (page * 16) + 1, 0x0000)
        response = self.query(OPCODE_RX_CHANNELS, payload)

        if response and response.result_code in (
            RESULT_CODE_SUCCESS,
            RESULT_CODE_SUCCESS_EXTENDED,
        ):
            return DanteParser.parse_rx_channels(response)

        return []

    def get_tx_channels(self, page: int = 0) -> list[TxChannel]:
        payload = struct.pack(">HHHH", 0x0000, 0x0001, (page * 16) + 1, 0x0000)
        response = self.query(OPCODE_TX_CHANNEL_INFO, payload)

        if response and response.result_code in (
            RESULT_CODE_SUCCESS,
            RESULT_CODE_SUCCESS_EXTENDED,
        ):
            return DanteParser.parse_tx_channels(response)

        return []

    def get_all_rx_channels(self) -> list[RxChannel]:
        count = self.get_channel_count()

        if not count:
            return []

        all_channels = []
        pages = (count.rx_count + 15) // 16

        for page in range(pages):
            channels = self.get_rx_channels(page)
            all_channels.extend(channels)

            if len(channels) < 16:
                break

        return all_channels

    def get_all_tx_channels(self) -> list[TxChannel]:
        count = self.get_channel_count()

        if not count:
            return []

        all_channels = []
        pages = max(1, count.tx_count // 16)

        for page in range(0, pages, 2):
            all_channels.extend(self.get_tx_channels(page))

        return all_channels


def discover_device(ip: str, port: int = 4440) -> Optional[dict]:
    client = DanteClient(ip, port)
    name = client.get_device_name()

    if not name:
        return None

    info = client.get_device_info()
    settings = client.get_device_settings()
    count = client.get_channel_count()

    return {
        "ip": ip,
        "name": name,
        "model": info.model_name if info else None,
        "display_name": info.display_name if info else None,
        "model_code": info.model_code if info else None,
        "sample_rate": settings.sample_rate if settings else None,
        "latency_ms": settings.latency_us / 1000.0
        if settings and settings.latency_us
        else None,
        "tx_channels": count.tx_count if count else 0,
        "rx_channels": count.rx_count if count else 0,
        "capability_flags": f"0x{count.capability_flags:04X}" if count else None,
    }
