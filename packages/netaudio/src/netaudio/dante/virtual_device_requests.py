from __future__ import annotations

import logging
import socket
import struct

from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
    MCAST_HEADER_LENGTH,
    MULTICAST_GROUP_CONTROL_MONITORING,
    PROTOCOL_ARC_2729,
    PROTOCOL_ARC_2809,
    PROTOCOL_ID,
    RESULT_CODE_SUCCESS,
    ArcOpcode as Opcode,
    ProtocolId as Protocol,
)
from netaudio.dante.packet_header import parse_packet_header


logger = logging.getLogger("netaudio")


class VirtualDeviceRequestHandler:
    def _handle_request(self, data: bytes, addr: tuple[str, int]) -> bytes | None:
        if len(data) < 10:
            return None

        start_code = struct.unpack(">H", data[:2])[0]

        if start_code in (0xFFFF, 0xFFFE) and len(data) >= MCAST_HEADER_LENGTH:
            return self._handle_mcast_format_request(data, addr)

        header = parse_packet_header(data)
        if header is None:
            return None
        length = header["length"]
        seqnum = header["transaction_id"]
        opcode1 = header["opcode"]
        opcode2 = header["result_code"]
        content = data[10:]

        if start_code in (PROTOCOL_ID, PROTOCOL_ARC_2729, PROTOCOL_ARC_2809):
            handler = self._opcode_handlers.get(opcode1)
            if handler:
                return handler(self, seqnum, data, start_code)
            logger.debug(f"Unhandled ARC opcode 0x{opcode1:04x} from {addr}")
            return None

        if start_code == Protocol.ARC_2809:
            return self._handle_aes67_config(start_code, seqnum, opcode1, content)

        if opcode1 == 0x1001 and opcode2 == 0:
            return self._handle_cmc_advertisement(start_code, seqnum, opcode1)

        logger.debug(
            f"Unhandled packet from {addr}: start=0x{start_code:04x} "
            f"op1=0x{opcode1:04x} op2=0x{opcode2:04x} len={length} "
            f"data={data[: min(32, len(data))].hex()}"
        )
        return None

    def _handle_mcast_format_request(self, data: bytes, addr: tuple[str, int]) -> None:
        if struct.unpack(">H", data[0:2])[0] not in (0xFFFF, 0xFFFE):
            return
        if struct.unpack(">H", data[2:4])[0] != len(data):
            return
        if data[16:24] != b"Audinate":
            return
        opcode = data[24:32]
        if opcode[0] != 0x07 or opcode[2] != 0:
            return
        info_type = opcode[3]
        logger.debug(f"Mcast-format request from {addr}: opcode={opcode.hex()} info_type=0x{info_type:02x}")

        if info_type == 0x61:
            self._send_mcast_board_info()
            self._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0x60, 0, 0, 0, 0]), self._build_board_info_content()
            )
        elif info_type == 0xC1:
            self._send_mcast_product_info()
            self._send_unicast_from_settings(
                addr, bytes([0x07, 0x2A, 0x00, 0xC0, 0, 0, 0, 0]), self._build_product_info_content()
            )
        elif info_type == 0x21:
            self._send_mcast_clock_stats()
        elif info_type == 0x13:
            self._send_mcast_network_info()
        elif info_type == 0x77:
            self._mcast_send(
                MULTICAST_GROUP_CONTROL_MONITORING,
                DEVICE_INFO_PORT,
                0xFFFF,
                bytes([0x07, 0x2A, 0x00, 0x78, 0, 0, 0, 0]),
                bytes([0, 0, 0, 3, 0, 0, 0, 0]),
            )
        elif info_type == 0x81:
            if len(data) < 40:
                return
            operation_mode, requested_sample_rate = struct.unpack(">II", data[32:40])
            if operation_mode == 1 and requested_sample_rate in self._config.supported_sample_rates:
                self._config.sample_rate = requested_sample_rate
            elif operation_mode != 0:
                return
            self._send_sample_rate_status()
        elif info_type == 0x83:
            if len(data) < 40:
                return
            operation_mode, requested_encoding = struct.unpack(">II", data[32:40])
            if operation_mode == 1 and requested_encoding in self._config.supported_encodings:
                self._config.encoding = requested_encoding
            elif operation_mode != 0:
                return
            self._send_encoding_status()
        else:
            logger.debug(f"Unhandled mcast info_type 0x{info_type:02x} from {addr}")

    def _send_unicast_from_settings(self, addr: tuple[str, int], opcode: bytes, content: bytes) -> None:
        if self._mcast_transport:
            packet = self._build_mcast_packet(0xFFFF, opcode, content)
            self._mcast_transport.sendto(packet, addr)
            logger.debug(f"Sent unicast response to {addr} from port 8700 ({len(packet)}B)")

    def _build_board_info_content(self) -> bytes:
        content = bytearray(200)
        content[0:4] = bytes([4, 1, 0, 6])
        content[0x23] = 2
        content[4:8] = bytes([4, 1, 0, 3])
        content[0x27] = 1
        content[0x28:0x2C] = bytes([1, 0, 0, 0])
        content[0x14] = 0
        content[0x15] = 0
        content[0x16] = 0x10
        content[0x17] = 0
        content[0xBB] = 0x1F
        board_name = self._config.name.encode("utf-8")[:8]
        content[12 : 12 + len(board_name)] = board_name
        content[0x38 : 0x38 + min(len(board_name), 16)] = board_name[:16]
        return bytes(content)

    def _build_product_info_content(self) -> bytes:
        content = bytearray(336)
        mfr = self._config.manufacturer.encode("utf-8")
        model = self._config.model.encode("utf-8")
        board = self._config.name.encode("utf-8")
        content[0 : min(8, len(mfr))] = mfr[:8]
        content[8 : 8 + min(8, len(board))] = board[:8]
        content[0x2C : 0x2C + min(16, len(mfr))] = mfr[:16]
        content[0xAC : 0xAC + min(16, len(model))] = model[:16]
        content[0x1C:0x20] = bytes([0, 1, 0, 0])
        return bytes(content)

    def _handle_cmc_advertisement(self, start_code: int, seqnum: int, opcode1: int) -> bytes:
        ip_bytes = socket.inet_aton(self._local_ip) if self._local_ip else b"\x00\x00\x00\x00"
        device_id = b"\x00\x00" + ip_bytes + b"\x00\x00"

        body = struct.pack(">H", 0x0000)
        body += device_id
        body += struct.pack(">H", 1)
        body += struct.pack(">H", 0)
        body += ip_bytes
        body += struct.pack(">H", DEVICE_SETTINGS_PORT)
        body += struct.pack(">H", 0)

        length = 10 + len(body)
        header = struct.pack(">HHHHH", start_code, length, seqnum, opcode1, RESULT_CODE_SUCCESS)
        return header + body

    def _handle_aes67_config(self, start_code: int, seqnum: int, opcode1: int, content: bytes) -> bytes:
        body = b"\x63\x00\x01"
        length = 10 + len(body)
        header = struct.pack(">HHHHH", start_code, length, seqnum, opcode1, RESULT_CODE_SUCCESS)
        return header + body

    def _build_response(
        self,
        transaction_id: int,
        opcode: int,
        body: bytes,
        protocol_id: int = PROTOCOL_ID,
        result_code: int = RESULT_CODE_SUCCESS,
    ) -> bytes:
        length = 10 + len(body)
        header = struct.pack(">HHHHH", protocol_id, length, transaction_id, opcode, result_code)
        return header + body

    def _handle_device_name(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        name_bytes = self._config.name.encode("utf-8") + b"\x00"
        return self._build_response(transaction_id, Opcode.DEVICE_NAME, name_bytes, protocol_id)

    def _handle_channel_count(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        tx_count = len(self._config.tx_channels)
        rx_count = len(self._config.rx_channels)
        body = struct.pack(">BB", 0, 0x30)
        body += struct.pack(">HH", tx_count, rx_count)
        body += struct.pack(">H", 4)
        body += struct.pack(">H", 8)
        body += struct.pack(">H", 8)
        body += struct.pack(">HH", 32, 32)
        body += struct.pack(">H", tx_count + rx_count)
        body += struct.pack(">HH", 1, 1)
        body += bytes(12)
        return self._build_response(transaction_id, Opcode.CHANNEL_COUNT, body, protocol_id)

    def _handle_device_info(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        fixed_header_size = 34
        strings_base = 10 + fixed_header_size

        board_name = self._config.model.encode("utf-8") + b"\x00"
        revision_str = b"1.0.0\x00"
        friendly_hostname = self._config.name.encode("utf-8") + b"\x00"
        mac_hex = self._mac.replace(":", "")
        factory_hostname = f"netaudio-{mac_hex}".encode("utf-8") + b"\x00"

        board_name_offset = strings_base
        revision_offset = board_name_offset + len(board_name)
        friendly_hostname_offset = revision_offset + len(revision_str)
        factory_hostname_offset = friendly_hostname_offset + len(friendly_hostname)

        body = bytearray(fixed_header_size)
        struct.pack_into(">H", body, 0, 0)
        struct.pack_into(">H", body, 2, 0)
        struct.pack_into(">H", body, 4, 0)
        struct.pack_into(">H", body, 6, board_name_offset)
        struct.pack_into(">H", body, 8, revision_offset)
        struct.pack_into(">H", body, 10, 0x0500)
        struct.pack_into(">H", body, 12, friendly_hostname_offset)
        struct.pack_into(">H", body, 14, factory_hostname_offset)
        struct.pack_into(">H", body, 16, friendly_hostname_offset)
        body[18:30] = bytes(12)
        struct.pack_into(">H", body, 30, 0x2729)
        struct.pack_into(">H", body, 32, 0)

        body += board_name + revision_str + friendly_hostname + factory_hostname

        return self._build_response(transaction_id, Opcode.DEVICE_INFO, bytes(body), protocol_id)

    def _handle_tx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.tx_channels
        num_ch = len(channels)
        record_size = 8
        header_size = 10
        body_header_size = 2

        sample_rate_area_offset = header_size + body_header_size + (num_ch * record_size)
        metadata = self._build_channel_metadata()
        if metadata is None:
            return self._build_response(
                transaction_id,
                Opcode.TX_CHANNELS,
                b"",
                protocol_id,
                result_code=0x0030,
            )
        string_area_offset = sample_rate_area_offset + len(metadata)

        name_offsets = []
        string_table = bytearray()
        for ch_name in channels:
            name_offsets.append(string_area_offset + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(
                ">HHHH",
                i + 1,
                0x0007,
                sample_rate_area_offset,
                name_offsets[i],
            )
        body += metadata + bytes(string_table)

        return self._build_response(transaction_id, Opcode.TX_CHANNELS, body, protocol_id)

    def _handle_tx_channel_names(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.tx_channels
        num_ch = len(channels)
        record_size = 6
        header_size = 10
        body_header_size = 2

        string_area_offset = header_size + body_header_size + (num_ch * record_size)
        pad_to_align = (4 - (string_area_offset % 4)) % 4
        string_area_offset += pad_to_align

        name_offsets = []
        string_table = bytearray()
        for ch_name in channels:
            name_offsets.append(string_area_offset + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(">HHH", i + 1, i + 1, name_offsets[i])
        body += bytes(pad_to_align)
        body += bytes(string_table)

        return self._build_response(transaction_id, 0x2010, body, protocol_id)

    def _handle_rx_channels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        channels = self._config.rx_channels
        num_ch = len(channels)
        record_size = 20
        header_size = 10
        body_header_size = 2

        sample_rate_area_offset = header_size + body_header_size + (num_ch * record_size)
        metadata = self._build_channel_metadata()
        if metadata is None:
            return self._build_response(
                transaction_id,
                Opcode.RX_CHANNELS,
                b"",
                protocol_id,
                result_code=0x0030,
            )

        string_table = bytearray()
        string_base = sample_rate_area_offset + len(metadata)

        name_offsets = []
        src_ch_offsets = []
        src_dev_offsets = []
        rx_status_codes = []
        sub_status_codes = []

        for i, ch_name in enumerate(channels):
            ch_num = i + 1
            name_offsets.append(string_base + len(string_table))
            string_table += ch_name.encode("utf-8") + b"\x00"

            sub = self._subscriptions.get(ch_num)
            if sub:
                src_ch_name, src_dev_name = sub
                src_ch_offsets.append(string_base + len(string_table))
                string_table += src_ch_name.encode("utf-8") + b"\x00"
                src_dev_offsets.append(string_base + len(string_table))
                string_table += src_dev_name.encode("utf-8") + b"\x00"
                rx_status_codes.append(0x0000)
                sub_status_codes.append(0x0009)
            else:
                src_ch_offsets.append(0)
                src_dev_offsets.append(0)
                rx_status_codes.append(0x0000)
                sub_status_codes.append(0x0000)

        body = struct.pack(">BB", 0x02, num_ch)
        for i in range(num_ch):
            body += struct.pack(
                ">HHHHHH",
                i + 1,
                0x0006,
                sample_rate_area_offset,
                src_ch_offsets[i],
                src_dev_offsets[i],
                name_offsets[i],
            )
            body += struct.pack(">HH", rx_status_codes[i], sub_status_codes[i])
            body += struct.pack(">I", 0)
        body += metadata + bytes(string_table)

        return self._build_response(transaction_id, Opcode.RX_CHANNELS, body, protocol_id)

    def _handle_property_directory(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        properties = [
            (0x8020, 0x0001),
            (0x8021, 0x0003),
            (0x0022, 0x0003),
            (0x0023, 0x0003),
            (0x0024, 0x0001),
            (0x8060, 0x0003),
            (0x0062, 0x0003),
            (0x0063, 0x0001),
            (0x0201, 0x0003),
            (0x8204, 0x0003),
            (0x8205, 0x0003),
            (0x020A, 0x0001),
            (0x020B, 0x0001),
            (0x0210, 0x0003),
            (0x0211, 0x0003),
            (0x0212, 0x0003),
            (0x0213, 0x0001),
            (0x0214, 0x0001),
            (0x0222, 0x0003),
            (0x8301, 0x0003),
            (0x8306, 0x0001),
            (0x8302, 0x0001),
            (0x8321, 0x0001),
            (0x0310, 0x0001),
            (0x0311, 0x0001),
            (0x0312, 0x0001),
            (0x0303, 0x0003),
            (0x83F0, 0x0001),
            (0x0601, 0x0001),
            (0x0309, 0x0001),
            (0x0209, 0x0001),
        ]
        body = struct.pack(">H", len(properties))
        for prop_id, flags in properties:
            body += struct.pack(">HH", prop_id, flags)
        return self._build_response(transaction_id, 0x1102, body, protocol_id)

    def _handle_device_settings(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        settings = [
            (0x8020, self._config.sample_rate),
            (0x8204, self._config.default_latency_ns),
            (0x8205, self._config.latency_ns),
            (0x8301, int(self._config.active_latency_ns)),
            (0x8302, self._config.maximum_latency_ns),
            (0x8306, self._config.minimum_latency_ns),
        ]
        first_value_offset = 10 + 2 + len(settings) * 4
        body = struct.pack(">BB", 2, len(settings))
        for index, (info_code, _) in enumerate(settings):
            body += struct.pack(">HH", info_code, first_value_offset + index * 4)
        for _, value in settings:
            body += struct.pack(">I", value)

        return self._build_response(transaction_id, Opcode.DEVICE_SETTINGS, body, protocol_id)

    def _handle_device_settings_set(
        self,
        transaction_id: int,
        data: bytes,
        protocol_id: int = PROTOCOL_ID,
    ) -> bytes:
        if len(data) < 12:
            return self._handle_unsupported(transaction_id, data, protocol_id)
        record_count = data[11]
        configured_latency_nanoseconds = None
        active_latency_nanoseconds = None
        for record_index in range(record_count):
            record_offset = 12 + record_index * 4
            if record_offset + 4 > len(data):
                return self._handle_unsupported(transaction_id, data, protocol_id)
            info_code, value_pointer = struct.unpack(">HH", data[record_offset : record_offset + 4])
            if info_code not in (0x8205, 0x8301):
                continue
            if value_pointer + 4 > len(data):
                return self._handle_unsupported(transaction_id, data, protocol_id)
            value = struct.unpack(">I", data[value_pointer : value_pointer + 4])[0]
            if info_code == 0x8205:
                configured_latency_nanoseconds = value
            else:
                active_latency_nanoseconds = value
        if configured_latency_nanoseconds is None or active_latency_nanoseconds != configured_latency_nanoseconds:
            return self._handle_unsupported(transaction_id, data, protocol_id)

        self._config.latency_ns = configured_latency_nanoseconds
        self._config.active_latency_ns = active_latency_nanoseconds
        response_body = struct.pack(">BB", 4, 4)
        response_body += struct.pack(">HH", 0x8205, 28)
        response_body += struct.pack(">HH", 0x0211, 4)
        response_body += struct.pack(">HH", 0x8301, 32)
        response_body += struct.pack(">HH", 0x0310, 4)
        response_body += struct.pack(">II", configured_latency_nanoseconds, active_latency_nanoseconds)
        return self._build_response(transaction_id, Opcode.DEVICE_SETTINGS_SET, response_body, protocol_id)

    def _handle_tx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x2200, body, protocol_id)

    def _handle_rx_flows(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x3200, body, protocol_id)

    def _handle_rx_subscriptions(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = bytes([0x38, 0x00, 0x38, 0xFD, 0x38, 0xFE, 0x38, 0xFF])
        return self._build_response(transaction_id, 0x3300, body, protocol_id)

    def _handle_tx_flow_labels(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = struct.pack(">BB", 0x02, 0x00)
        return self._build_response(transaction_id, 0x2204, body, protocol_id)

    def _handle_channel_name_set(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        opcode = parse_packet_header(data)["opcode"]
        body = data[10:]
        if len(body) >= 4:
            ch_num = struct.unpack(">H", body[0:2])[0]
            name_off = struct.unpack(">H", body[2:4])[0]
            new_name = self._get_string_from_data(data, name_off)
            if opcode == Opcode.TX_CHANNEL_NAME_SET:
                idx = ch_num - 1
                if 0 <= idx < len(self._config.tx_channels):
                    self._config.tx_channels[idx] = new_name
                    logger.info(f"Renamed TX ch {ch_num} -> '{new_name}'")
            elif opcode == Opcode.RX_CHANNEL_NAME_SET:
                idx = ch_num - 1
                if 0 <= idx < len(self._config.rx_channels):
                    self._config.rx_channels[idx] = new_name
                    logger.info(f"Renamed RX ch {ch_num} -> '{new_name}'")
        return self._build_response(transaction_id, opcode, b"", protocol_id)

    def _handle_subscription_add(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = data[10:]
        if len(body) < 8:
            return self._build_response(transaction_id, 0x3010, b"", protocol_id)

        num_records = body[1]
        offset = 2
        for _ in range(num_records):
            if offset + 6 > len(body):
                break
            ch_num, src_ch_off, src_dev_off = struct.unpack(">HHH", body[offset : offset + 6])
            offset += 6
            if ch_num == 0:
                continue
            if src_ch_off == 0 and src_dev_off == 0:
                self._subscriptions.pop(ch_num, None)
                logger.info(f"Unsubscribed RX ch {ch_num}")
            else:
                src_ch_name = self._get_string_from_data(data, src_ch_off)
                src_dev_name = self._get_string_from_data(data, src_dev_off)
                self._subscriptions[ch_num] = (src_ch_name, src_dev_name)
                logger.info(f"Subscribed RX ch {ch_num} <- {src_ch_name}@{src_dev_name}")

        return self._build_response(transaction_id, 0x3010, b"", protocol_id)

    def _handle_subscription_remove(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        body = data[10:]
        if len(body) >= 2:
            ch_num = struct.unpack(">H", body[0:2])[0]
            self._subscriptions.pop(ch_num, None)
            logger.info(f"Unsubscribed RX ch {ch_num}")
        return self._build_response(transaction_id, 0x3014, b"", protocol_id)

    def _get_string_from_data(self, data: bytes, offset: int) -> str:
        if offset == 0 or offset >= len(data):
            return ""
        end = data.index(0, offset) if 0 in data[offset:] else len(data)
        return data[offset:end].decode("utf-8", errors="replace")

    def _handle_unsupported(self, transaction_id: int, data: bytes, protocol_id: int = PROTOCOL_ID) -> bytes:
        opcode = struct.unpack(">H", data[6:8])[0]
        return self._build_response(transaction_id, opcode, b"", protocol_id, result_code=0x0030)

    _opcode_handlers = {
        Opcode.DEVICE_NAME: _handle_device_name,
        Opcode.CHANNEL_COUNT: _handle_channel_count,
        Opcode.DEVICE_INFO: _handle_device_info,
        Opcode.TX_CHANNELS: _handle_tx_channels,
        Opcode.TX_CHANNEL_NAMES: _handle_tx_channel_names,
        Opcode.RX_CHANNELS: _handle_rx_channels,
        Opcode.DEVICE_SETTINGS: _handle_device_settings,
        Opcode.DEVICE_SETTINGS_SET: _handle_device_settings_set,
        0x1102: _handle_property_directory,
        0x2200: _handle_tx_flows,
        0x2204: _handle_tx_flow_labels,
        0x2320: _handle_unsupported,
        Opcode.TX_CHANNEL_NAME_SET: _handle_channel_name_set,
        Opcode.RX_CHANNEL_NAME_SET: _handle_channel_name_set,
        0x3010: _handle_subscription_add,
        0x3014: _handle_subscription_remove,
        0x3200: _handle_rx_flows,
        0x3300: _handle_rx_subscriptions,
    }
