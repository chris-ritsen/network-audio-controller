from __future__ import annotations

import struct

from netaudio.dante.const import (
    ARC_PROTOCOL_IDS,
    PROTOCOL_DDP_LOCK,
    PROTOCOL_NAMES,
    PROTOCOL_SETTINGS,
    RESULT_CODE_NAMES,
)

ARC_HEADER_LENGTH = 10
CONMON_MESSAGE_TYPE_OFFSET = 26
DDP_LOCK_HEADER_LENGTH = 12
MINIMUM_HEADER_LENGTH = 8


def conmon_message_type(data: bytes) -> int | None:
    from netaudio import core

    if len(data) < CONMON_MESSAGE_TYPE_OFFSET + 2:
        return None
    try:
        return core.parse_response("conmon_opcode", data)["opcode"]
    except core.NetaudioCoreError:
        return struct.unpack(">H", data[CONMON_MESSAGE_TYPE_OFFSET : CONMON_MESSAGE_TYPE_OFFSET + 2])[0]


def parse_packet_header(data: bytes) -> dict | None:
    from netaudio.dante.debug_formatter import get_opcode_name, get_settings_message_type_name

    if len(data) < MINIMUM_HEADER_LENGTH:
        return None

    protocol_id, length = struct.unpack(">HH", data[0:4])

    if protocol_id == PROTOCOL_SETTINGS:
        message_type = conmon_message_type(data)
        if message_type is not None:
            return {
                "length": length,
                "opcode": message_type,
                "opcode_name": get_settings_message_type_name(message_type),
                "protocol_id": protocol_id,
                "protocol_name": PROTOCOL_NAMES[PROTOCOL_SETTINGS],
                "result_code": None,
                "result_name": None,
                "transaction_id": None,
            }

    if protocol_id == PROTOCOL_DDP_LOCK and len(data) >= DDP_LOCK_HEADER_LENGTH:
        direction_field = struct.unpack(">H", data[6:8])[0]
        opcode = struct.unpack(">H", data[10:12])[0]
        sequence = struct.unpack(">H", data[16:18])[0] if len(data) >= 18 else None
        return {
            "length": length,
            "opcode": opcode,
            "opcode_name": get_opcode_name(protocol_id, opcode),
            "protocol_id": protocol_id,
            "protocol_name": PROTOCOL_NAMES[PROTOCOL_DDP_LOCK],
            "result_code": direction_field,
            "result_name": None,
            "transaction_id": sequence,
        }

    transaction_id, opcode = struct.unpack(">HH", data[4:8])
    result_code = struct.unpack(">H", data[8:10])[0] if len(data) >= ARC_HEADER_LENGTH else None
    return {
        "length": length,
        "opcode": opcode,
        "opcode_name": get_opcode_name(protocol_id, opcode),
        "protocol_id": protocol_id,
        "protocol_name": PROTOCOL_NAMES.get(protocol_id),
        "result_code": result_code,
        "result_name": RESULT_CODE_NAMES.get(result_code) if result_code is not None else None,
        "transaction_id": transaction_id,
    }


def is_arc_protocol(protocol_id: int | None) -> bool:
    return protocol_id in ARC_PROTOCOL_IDS
