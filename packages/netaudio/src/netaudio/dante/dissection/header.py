from __future__ import annotations

from netaudio import core
from netaudio.dante.debug_formatter import opcode_label, get_settings_message_type_name


def parse_packet_header(data: bytes) -> dict | None:
    try:
        header = core.parse_response("packet_header", data)
    except core.NetaudioCoreError as error:
        if error.category != "binary_response":
            raise

        return None

    family = header.pop("family")
    opcode = header["opcode"]
    header["opcode_name"] = get_settings_message_type_name(opcode) if family == "settings" else opcode_label(header)
    return header
