from __future__ import annotations

import struct
from dataclasses import dataclass


NOTIFICATION_TOPOLOGY_CHANGE = 16
NOTIFICATION_INTERFACE_STATUS = 17
NOTIFICATION_CLOCKING_STATUS = 32
NOTIFICATION_VERSIONS_STATUS = 96
NOTIFICATION_CLEAR_CONFIG_STATUS = 120
NOTIFICATION_SAMPLE_RATE_STATUS = 128
NOTIFICATION_ENCODING_STATUS = 130
NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS = 132
NOTIFICATION_GAIN_STATUS = 4107
NOTIFICATION_DEVICE_REBOOT = 146
NOTIFICATION_MANF_VERSIONS_STATUS = 192
NOTIFICATION_ROUTING_READY = 256
NOTIFICATION_TX_CHANNEL_CHANGE = 257
NOTIFICATION_RX_CHANNEL_CHANGE = 258
NOTIFICATION_TX_LABEL_CHANGE = 259
NOTIFICATION_TX_FLOW_CHANGE = 260
NOTIFICATION_RX_FLOW_CHANGE = 261
NOTIFICATION_PROPERTY_CHANGE = 262
NOTIFICATION_LATENCY_CHANGE = 262
NOTIFICATION_ROUTING_DEVICE_CHANGE = 288
NOTIFICATION_SETTINGS_CHANGE = 4110
NOTIFICATION_AES67_STATUS = 4103

NOTIFICATION_NAMES = {
    NOTIFICATION_TOPOLOGY_CHANGE: "Topology Change",
    NOTIFICATION_INTERFACE_STATUS: "Interface Status",
    NOTIFICATION_CLOCKING_STATUS: "Clocking Status",
    NOTIFICATION_VERSIONS_STATUS: "Versions Status",
    NOTIFICATION_CLEAR_CONFIG_STATUS: "Clear Config Status",
    NOTIFICATION_SAMPLE_RATE_STATUS: "Sample Rate Status",
    NOTIFICATION_ENCODING_STATUS: "Encoding Status",
    NOTIFICATION_SAMPLE_RATE_PULLUP_STATUS: "Sample Rate Pull-Up Status",
    NOTIFICATION_GAIN_STATUS: "Gain Status",
    NOTIFICATION_DEVICE_REBOOT: "Device Reboot",
    NOTIFICATION_MANF_VERSIONS_STATUS: "Manufacturer Versions Status",
    NOTIFICATION_ROUTING_READY: "Routing Ready",
    NOTIFICATION_TX_CHANNEL_CHANGE: "TX Channel Change",
    NOTIFICATION_RX_CHANNEL_CHANGE: "RX Channel Change",
    NOTIFICATION_TX_LABEL_CHANGE: "TX Label Change",
    NOTIFICATION_TX_FLOW_CHANGE: "TX Flow Change",
    NOTIFICATION_RX_FLOW_CHANGE: "RX Flow Change",
    NOTIFICATION_PROPERTY_CHANGE: "Property Change",
    NOTIFICATION_ROUTING_DEVICE_CHANGE: "Routing Device Change",
    NOTIFICATION_SETTINGS_CHANGE: "Settings Change",
    NOTIFICATION_AES67_STATUS: "AES67 Status",
}

CONMON_OPCODE_INTERFACE_STATUS = 0x0011
CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS = 0x0014
CONMON_OPCODE_LINK_STATUS = 0x0040
CONMON_OPCODE_MAKE_MODEL_RESPONSE = 0x00C0
CONMON_OPCODE_DANTE_MODEL_RESPONSE = 0x0060
CONMON_OPCODE_SAMPLE_RATE_STATUS = 0x0080
CONMON_OPCODE_ENCODING_STATUS = 0x0082
CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS = 0x0084
CONMON_OPCODE_GAIN_STATUS = 0x100B
CONMON_OPCODE_AES67_CURRENT_NEW = 0x1007
CONMON_OPCODE_LOCK_RESET_STATUS = 0x1009
CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS = 0x0078
CONMON_OPCODE_ROUTING_CAPACITY_STATUS = NOTIFICATION_ROUTING_READY
CONMON_OPCODE_EXPORT_FRAGMENT = 0xFF05
CONMON_AES67_CURRENT_NEW_OFFSET = 0x21
CONMON_OPCODE_PTP_CLOCK_STATUS = 0x0020
CONMON_PREFERRED_LEADER_OFFSET = 0x26
CONMON_CLOCK_FREQUENCY_OFFSET_PARTS_PER_BILLION_OFFSET = 0x28
CONMON_CLOCK_PORT_STATE_OFFSET = 0x48
CLOCK_PORT_STATE_LEADER = 0x0006
CLOCK_PORT_STATE_FOLLOWER = 0x0009

CLOCK_PORT_ROLE_MAP = {
    CLOCK_PORT_STATE_LEADER: "Leader",
    CLOCK_PORT_STATE_FOLLOWER: "Follower",
}
PROTOCOL_SETTINGS = 0xFFFF
PROTOCOL_CONTROL = 0x27FF

AES67_CURRENT_NEW_MAP = {
    0x00: (False, False),
    0x01: (True, False),
    0x02: (False, True),
    0x03: (True, True),
}


def parse_aes67_current_new_byte(state_byte: int) -> tuple[bool | None, bool | None]:
    result = AES67_CURRENT_NEW_MAP.get(state_byte)
    if result is not None:
        return result
    return (None, None)


@dataclass(frozen=True)
class CapabilityStatusChanges:
    device_state_changed: bool = False
    current_value_changed: bool = False
    supported_values_changed: bool = False


def extract_conmon_opcode(data: bytes) -> int | None:
    if len(data) < 0x20:
        return None
    magic_position = data.find(b"Audinate", 4)
    if magic_position < 0:
        return None
    opcode_position = magic_position + 10
    if opcode_position + 2 > len(data):
        return None
    return struct.unpack(">H", data[opcode_position : opcode_position + 2])[0]


def parse_make_model_response(data: bytes) -> tuple[str, str, str]:
    from netaudio import core

    parsed = core.parse_response("make_model", data)
    return parsed["product_name"], parsed["product_version"], parsed["manufacturer"]


def parse_dante_model_response(data: bytes) -> tuple[str, str]:
    from netaudio import core

    parsed = core.parse_response("dante_model", data)
    return parsed["board_codename"], parsed["board_name"]
