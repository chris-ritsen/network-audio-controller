import struct
from enum import IntEnum

from netaudio_lib.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    SERVICE_ARC,
)


class Protocol(IntEnum):
    CONTROL = 0x27FF
    SETTINGS = 0xFFFF
    CMC = 0x1200
    AES67_CONFIG = 0x2809


class Opcode(IntEnum):
    CHANNEL_COUNT = 0x1000
    DEVICE_NAME_SET = 0x1001
    DEVICE_NAME = 0x1002
    DEVICE_INFO = 0x1003
    DEVICE_SETTINGS = 0x1100
    DEVICE_SETTINGS_SET = 0x1101
    TX_CHANNELS = 0x2000
    TX_CHANNEL_NAMES = 0x2010
    TX_CHANNEL_NAME_SET = 0x2013
    RX_CHANNELS = 0x3000
    RX_CHANNEL_NAME_SET = 0x3001
    SUBSCRIPTION_ADD = 0x3010
    SUBSCRIPTION_REMOVE = 0x3014


class DanteDeviceCommands:
    @staticmethod
    def _build_control_packet(opcode: int, payload: bytes = b'\x00\x00', transaction_id: int = 0) -> bytes:
        length = 8 + len(payload)
        header = struct.pack(">HH", Protocol.CONTROL, length)
        header += struct.pack(">HH", transaction_id, opcode)
        return header + payload

    def command_device_info(self):
        return (self._build_control_packet(Opcode.DEVICE_INFO), SERVICE_ARC)

    def command_device_name(self, transaction_id=0):
        return (self._build_control_packet(Opcode.DEVICE_NAME, transaction_id=transaction_id), SERVICE_ARC)

    def command_channel_count(self, transaction_id=0):
        return (self._build_control_packet(Opcode.CHANNEL_COUNT, transaction_id=transaction_id), SERVICE_ARC)

    def command_device_settings(self):
        return (self._build_control_packet(Opcode.DEVICE_SETTINGS), SERVICE_ARC)

    def command_set_name(self, name):
        name_bytes = name.encode('utf-8')
        payload = struct.pack(">H", 0) + name_bytes + b'\x00'
        return (self._build_control_packet(Opcode.DEVICE_NAME_SET, payload), SERVICE_ARC)

    def command_reset_name(self):
        return (self._build_control_packet(Opcode.DEVICE_NAME_SET), SERVICE_ARC)

    def command_receivers(self, page=0, transaction_id=0):
        starting_channel = page * 16 + 1
        payload = struct.pack(">HBBHH", 0, 0, 1, starting_channel, 0)
        return (self._build_control_packet(Opcode.RX_CHANNELS, payload, transaction_id=transaction_id), SERVICE_ARC)

    def command_transmitters(self, page=0, friendly_names=False):
        opcode = Opcode.TX_CHANNEL_NAMES if friendly_names else Opcode.TX_CHANNELS
        starting_channel = page * 32 + 1
        payload = struct.pack(">HBBHH", 0, 0, 1, starting_channel, 0)
        return (self._build_control_packet(opcode, payload), SERVICE_ARC)

    def command_reset_channel_name(self, channel_type, channel_number):
        if channel_type == "rx":
            payload = struct.pack(">HBBBB", 0, 2, 1, 0, channel_number)
            payload += struct.pack(">H", 0x14)
            payload += b'\x00\x00\x00\x00'
            return (self._build_control_packet(Opcode.RX_CHANNEL_NAME_SET, payload), SERVICE_ARC)
        else:
            payload = struct.pack(">HBBBBBB", 0, 2, 1, 0, 0, 0, channel_number)
            payload += struct.pack(">H", 0x18)
            payload += b'\x00\x00\x00\x00\x00\x00'
            return (self._build_control_packet(Opcode.TX_CHANNEL_NAME_SET, payload), SERVICE_ARC)

    def command_set_channel_name(self, channel_type, channel_number, new_channel_name):
        name_bytes = new_channel_name.encode('utf-8')

        if channel_type == "rx":
            payload = struct.pack(">HBBBB", 0, 2, 1, 0, channel_number)
            payload += struct.pack(">H", 0x14)
            payload += b'\x00\x00\x00\x00'
            payload += name_bytes + b'\x00'
            return (self._build_control_packet(Opcode.RX_CHANNEL_NAME_SET, payload), SERVICE_ARC)
        else:
            payload = struct.pack(">HBBBBBB", 0, 2, 1, 0, 0, 0, channel_number)
            payload += struct.pack(">H", 0x18)
            payload += b'\x00\x00\x00\x00\x00\x00'
            payload += name_bytes + b'\x00'
            return (self._build_control_packet(Opcode.TX_CHANNEL_NAME_SET, payload), SERVICE_ARC)

    def command_add_subscription(self, rx_channel_number, tx_channel_name, tx_device_name):
        packet, service = self.command_add_subscriptions([
            (rx_channel_number, tx_channel_name, tx_device_name),
        ])
        return (packet, service)

    def command_add_subscriptions(self, subscriptions):
        count = len(subscriptions)
        if count < 1 or count > 16:
            raise ValueError(f"Bulk subscription count must be 1-16, got {count}")

        packet_header_size = 8
        first_record_size = 10
        additional_record_size = 6
        records_total = first_record_size + additional_record_size * (count - 1)
        padding_size = max(0, 44 - records_total)
        string_table_offset = packet_header_size + records_total + padding_size

        string_table = bytearray()
        records = []

        for rx_channel_number, tx_channel_name, tx_device_name in subscriptions:
            tx_channel_bytes = tx_channel_name.encode('utf-8')
            tx_device_bytes = tx_device_name.encode('utf-8')

            tx_channel_absolute = string_table_offset + len(string_table)
            string_table += tx_channel_bytes + b'\x00'

            tx_device_absolute = string_table_offset + len(string_table)
            string_table += tx_device_bytes + b'\x00'

            records.append((rx_channel_number, tx_channel_absolute, tx_device_absolute))

        payload = struct.pack(">HBBBB", 0, 2, count, 0, records[0][0])
        payload += struct.pack(">BB", records[0][1] >> 8, records[0][1] & 0xFF)
        payload += struct.pack(">BB", records[0][2] >> 8, records[0][2] & 0xFF)
        for rx_channel_number, tx_offset, dev_offset in records[1:]:
            payload += struct.pack(">BBHH", 0, rx_channel_number, tx_offset, dev_offset)
        payload += b'\x00' * padding_size
        payload += bytes(string_table)

        return (self._build_control_packet(Opcode.SUBSCRIPTION_ADD, payload), SERVICE_ARC)

    def command_remove_subscription(self, rx_channel):
        return self.command_remove_subscriptions([rx_channel])

    def command_remove_subscriptions(self, rx_channels):
        count = len(rx_channels)
        if count < 1:
            raise ValueError("Must specify at least one channel to unsubscribe")

        payload = struct.pack(">I", count)
        for rx_channel in rx_channels:
            payload += struct.pack(">I", rx_channel)

        return (self._build_control_packet(Opcode.SUBSCRIPTION_REMOVE, payload), SERVICE_ARC)

    def command_set_latency(self, latency):
        latency_us = int(latency * 1000000)
        payload = struct.pack(">H", 0)
        payload += bytes([0x05, 0x03, 0x82, 0x05, 0x00])
        payload += bytes([0x20, 0x02, 0x11, 0x00, 0x10, 0x83])
        payload += bytes([0x01, 0x00, 0x24, 0x82, 0x19, 0x83])
        payload += bytes([0x01, 0x83, 0x02, 0x83, 0x06])
        payload += struct.pack(">I", latency_us)[1:]
        payload += b'\x00'
        payload += struct.pack(">I", latency_us)[1:]
        return (self._build_control_packet(Opcode.DEVICE_SETTINGS_SET, payload), SERVICE_ARC)

    def command_identify(self):
        mac = b'\x00' * 6
        magic = b'Audinate\x07\x31'

        payload = struct.pack(">HH", 0x0BC8, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x00, 0x63, 0x00, 0x00, 0x00, 0x64])

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_set_encoding(self, encoding):
        mac = b'RT\x00\x00\x00\x00'
        magic = b'Audinate\x07\x27'

        payload = struct.pack(">HH", 0x03D7, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x00, 0x83, 0x00, 0x00, 0x00, 0x64])
        payload += bytes([0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00])
        payload += struct.pack(">B", encoding)

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_set_sample_rate(self, sample_rate):
        mac = b'RT\x00\x00\x00\x00'
        magic = b'Audinate\x07\x27'

        payload = struct.pack(">HH", 0x03D4, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x00, 0x81, 0x00, 0x00, 0x00, 0x64])
        payload += bytes([0x00, 0x00, 0x00, 0x01, 0x00])
        payload += struct.pack(">I", sample_rate)[1:]

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_set_gain_level(self, channel_number, gain_level, device_type):
        mac = b'RT\x00\x00\x00\x00'
        magic = b'Audinate\x07\x27'

        if device_type == "input":
            command_id = 0x0344
            type_byte = 0x01
        else:
            command_id = 0x0326
            type_byte = 0x02

        payload = struct.pack(">HH", command_id, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += struct.pack(">BB", 0x10, type_byte)
        payload += bytes([0x0A, 0x00, 0x00, 0x00, 0x00, 0x00])
        payload += bytes([0x01, 0x00, 0x01, 0x00, 0x0C, 0x00])
        payload += struct.pack(">BB", 0x10, type_byte)
        payload += bytes([0x00, 0x00, 0x00, 0x00, 0x00])
        payload += struct.pack(">B", channel_number)
        payload += bytes([0x00, 0x00, 0x00])
        payload += struct.pack(">B", gain_level)

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_enable_aes67(self, is_enabled: bool, host_mac=None):
        mac = host_mac if host_mac else b'\x00' * 6
        if isinstance(mac, str):
            mac = bytes.fromhex(mac.replace(":", ""))
        magic = b'Audinate\x07\x34'
        enable_byte = 0x01 if is_enabled else 0x00

        payload = struct.pack(">HH", 0x22DC, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x10, 0x06, 0x00, 0x00, 0x00, 0x64])
        payload += bytes([0x00, 0x01, 0x00])
        payload += struct.pack(">B", enable_byte)

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_get_aes67_config(self, transaction_id=0):
        payload = struct.pack(">H", 0x15B0)
        payload += struct.pack(">H", 0x1100)
        payload += b'\x00\x00\x00\x00'
        payload += bytes([0x13, 0x02, 0x01, 0x82, 0x04, 0x82, 0x05, 0x02])
        payload += bytes([0x10, 0x02, 0x11, 0x82, 0x18, 0x82, 0x19, 0x83])
        payload += bytes([0x01, 0x83, 0x02, 0x83, 0x06, 0x03, 0x10, 0x03])
        payload += bytes([0x11, 0x03, 0x03, 0x80, 0x21, 0x00, 0xF0, 0x80])
        payload += bytes([0x60, 0x00, 0x22, 0x00, 0x63, 0x00, 0x64])

        length = len(payload) + 4
        packet = struct.pack(">HH", Protocol.AES67_CONFIG, length)
        packet += struct.pack(">H", transaction_id)
        packet += payload

        return (packet, SERVICE_ARC, None)

    def command_volume_start(self, device_name, ipv4, mac, port, timeout=True, transaction_id=0):
        if isinstance(mac, str):
            mac = bytes.fromhex(mac)

        name_bytes = device_name.encode('utf-8') + b'\x00'
        padded_name_len = len(name_bytes)
        if padded_name_len % 2 != 0:
            name_bytes += b'\x00'
            padded_name_len += 1

        offset_field_1 = 0x0A + padded_name_len
        offset_field_2 = 0x0C + padded_name_len
        tail_offset = offset_field_2 + 4

        body = struct.pack(">H", 0x3010)
        body += struct.pack(">H", 0)
        body += struct.pack(">H", 0)
        body += mac
        body += struct.pack(">H", 0)
        body += struct.pack(">H", 4)
        body += struct.pack(">H", offset_field_1)
        body += struct.pack(">H", 2)
        body += struct.pack(">H", offset_field_2)
        body += struct.pack(">H", 0x000A)
        body += name_bytes
        body += struct.pack(">H", 0)
        body += struct.pack(">H", 1)
        body += struct.pack(">H", tail_offset)
        body += struct.pack(">H", 1)
        body += struct.pack(">H", port)
        body += struct.pack(">H", 1 if timeout else 0)
        body += struct.pack(">H", 0)
        body += (ipv4.packed if ipv4 else b'\x00\x00\x00\x00')
        body += struct.pack(">H", port)
        body += b'\x00' * 10

        total_length = 4 + 2 + len(body)
        packet = struct.pack(">H", Protocol.CMC)
        packet += struct.pack(">H", total_length)
        packet += struct.pack(">H", transaction_id)
        packet += body

        return (packet, None, DEVICE_CONTROL_PORT)

    def command_volume_stop(self, device_name, ipv4, mac, port):
        import ipaddress
        return self.command_volume_start(
            device_name, ipaddress.IPv4Address(0), mac, 0, timeout=False
        )

    def command_metering_start(self, device_name, ipv4, mac, port, timeout=True, transaction_id=0):
        return self.command_volume_start(
            device_name, ipv4, mac, port, timeout=timeout, transaction_id=transaction_id
        )

    def command_metering_stop(self, device_name, ipv4, mac, port):
        return self.command_volume_stop(device_name, ipv4, mac, port)

    def command_bluetooth_status(self, host_mac=None):
        mac = host_mac if host_mac else b'\x00' * 6
        if isinstance(mac, str):
            mac = bytes.fromhex(mac.replace(":", ""))
        magic = b'Audinate\x07\x3a'

        payload = struct.pack(">HH", 0, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x10, 0x0d, 0x00, 0x00, 0x00, 0x00])
        payload += bytes([0x00, 0x0c, 0x00, 0x0c, 0x0a, 0x0a])
        payload += bytes([0x10, 0x09, 0x1a, 0x06, 0x0a, 0x04])
        payload += bytes([0x0a, 0x02, 0x08, 0x01])

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return (packet, None, DEVICE_SETTINGS_PORT)

    def command_make_model(self, mac):
        if isinstance(mac, str):
            mac = bytes.fromhex(mac)

        magic = b'Audinate\x07\x31'

        payload = struct.pack(">HH", 0x0FDB, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x00, 0xC1, 0x00, 0x00, 0x00, 0x00])

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return packet

    def command_dante_model(self, mac):
        if isinstance(mac, str):
            mac = bytes.fromhex(mac)

        magic = b'Audinate\x07\x31'

        payload = struct.pack(">HH", 0x0FDB, 0)
        payload += mac
        payload += struct.pack(">H", 0)
        payload += magic
        payload += bytes([0x00, 0x61, 0x00, 0x00, 0x00, 0x00])

        length = len(payload) + 4
        packet = struct.pack(">HBB", Protocol.SETTINGS, 0x00, length)
        packet += payload

        return packet
