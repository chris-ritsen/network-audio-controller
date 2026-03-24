import struct
import sys
from functools import lru_cache

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.clean_labels import (
    load_clean_labels,
    load_clean_subscription_status_labels,
)


class Colors:
    @property
    def RESET(self):
        return "" if app_settings.no_color else "\033[0m"

    @property
    def BOLD(self):
        return "" if app_settings.no_color else "\033[1m"

    @property
    def DIM(self):
        return "" if app_settings.no_color else "\033[2m"

    @property
    def RED(self):
        return "" if app_settings.no_color else "\033[31m"

    @property
    def GREEN(self):
        return "" if app_settings.no_color else "\033[32m"

    @property
    def YELLOW(self):
        return "" if app_settings.no_color else "\033[33m"

    @property
    def BLUE(self):
        return "" if app_settings.no_color else "\033[34m"

    @property
    def MAGENTA(self):
        return "" if app_settings.no_color else "\033[35m"

    @property
    def CYAN(self):
        return "" if app_settings.no_color else "\033[36m"

    @property
    def WHITE(self):
        return "" if app_settings.no_color else "\033[37m"


C = Colors()

PROTOCOL_NAMES = {
    0x1200: "PROTOCOL_CMC",
    0x27FF: "PROTOCOL_ARC",
    0x2801: "PROTOCOL_ARC_2801",
    0x2809: "PROTOCOL_ARC_SETTINGS",
    0xFFFF: "PROTOCOL_SETTINGS",
}

OPCODE_NAMES_BY_PROTOCOL = {}
SETTINGS_MESSAGE_TYPE_NAMES = {}


@lru_cache(maxsize=1)
def _external_labels():
    return load_clean_labels()


@lru_cache(maxsize=1)
def _external_subscription_status_labels():
    return load_clean_subscription_status_labels()


def get_opcode_name(protocol, opcode):
    opcode_labels, _ = _external_labels()
    external_label = opcode_labels.get((protocol, opcode))
    if external_label:
        return external_label

    arc_protocols = (0x27FF, 0x2801, 0x2809)
    if protocol in arc_protocols:
        for fallback_protocol in arc_protocols:
            if fallback_protocol != protocol:
                external_label = opcode_labels.get((fallback_protocol, opcode))
                if external_label:
                    return external_label

    return f"0x{opcode:04X}"


def get_settings_message_type_name(message_type):
    _, message_labels = _external_labels()
    external_label = message_labels.get(message_type)
    if external_label:
        return external_label

    return f"msg:0x{message_type:04X}"

RESULT_NAMES = {
    0x0001: "RESULT_CODE_SUCCESS",
    0x0022: "RESULT_CODE_ERROR",
    0x8112: "RESULT_CODE_SUCCESS_EXTENDED",
}


def get_subscription_status_name(status_code):
    status_labels = _external_subscription_status_labels()
    entry = status_labels.get(status_code)
    if isinstance(entry, dict):
        label = entry.get("label")
        if isinstance(label, str) and label.strip():
            return label.strip()

        labels = entry.get("labels")
        if isinstance(labels, list):
            for value in labels:
                if isinstance(value, str) and value.strip():
                    return value.strip()

    return f"status:{status_code}"


def get_subscription_status_state(status_code):
    status_labels = _external_subscription_status_labels()
    entry = status_labels.get(status_code)
    if isinstance(entry, dict):
        state = entry.get("state")
        if isinstance(state, str) and state.strip():
            return state.strip()

    if status_code == 0:
        return "none"

    return "unknown"


def get_string_at_offset(data: bytes, offset: int) -> str:
    if offset >= len(data):
        return ""

    end = data.find(b"\x00", offset)

    if end == -1:
        end = len(data)

    return data[offset:end].decode("utf-8", errors="replace")


def format_hex(data: bytes) -> str:
    return " ".join(f"{b:02X}" for b in data)


def print_field(offset: int, length: int, raw: bytes, name: str, value: str, color: str = None):
    if color is None:
        color = C.WHITE
    offset_str = f"{C.DIM}[{offset:3d}:{offset+length:3d}]{C.RESET}"
    hex_str = format_hex(raw)
    print(f"  {offset_str} {hex_str:20s}  {color}{name:28s}{C.RESET} = {value}", file=sys.stderr)


def print_const(offset: int, length: int, raw: bytes, const_name: str):
    offset_str = f"{C.DIM}[{offset:3d}:{offset+length:3d}]{C.RESET}"
    hex_str = f"{C.YELLOW}{C.BOLD}{format_hex(raw)}{C.RESET}"
    print(f"  {offset_str} {hex_str:30s}  {C.YELLOW}{const_name}{C.RESET}", file=sys.stderr)


def print_pointer(offset: int, length: int, raw: bytes, name: str, ptr_value: int, body: bytes):
    offset_str = f"{C.DIM}[{offset:3d}:{offset+length:3d}]{C.RESET}"
    hex_str = f"{C.MAGENTA}{format_hex(raw)}{C.RESET}"

    string_offset = ptr_value - 10

    if 0 <= string_offset < len(body):
        resolved = get_string_at_offset(body, string_offset)

        if resolved:
            end_offset = string_offset + len(resolved)
            string_bytes = body[string_offset:end_offset]
            string_hex = format_hex(string_bytes)

            ptr_str = f"{C.DIM}-> @{ptr_value}:{C.RESET} {C.DIM}{string_hex}{C.RESET} {C.CYAN}\"{resolved}\"{C.RESET}"
        else:
            ptr_str = f"{C.DIM}-> @{ptr_value} (empty){C.RESET}"
    else:
        ptr_str = f"{C.DIM}-> @{ptr_value}{C.RESET}"

    print(f"  {offset_str} {hex_str:30s}  {C.MAGENTA}{name:28s}{C.RESET} {ptr_str}", file=sys.stderr)


def print_record_separator(num: int, record_type: str):
    print(f"\n  {C.BLUE}{C.BOLD}Record {num}{C.RESET} {C.DIM}({record_type}){C.RESET}", file=sys.stderr)


def print_sample_rate_offset(offset: int, length: int, raw: bytes, name: str, offset_value: int, body: bytes):
    offset_str = f"{C.DIM}[{offset:3d}:{offset+length:3d}]{C.RESET}"
    hex_str = f"{C.MAGENTA}{format_hex(raw)}{C.RESET}"

    body_offset = offset_value - 10

    if 0 <= body_offset and body_offset + 4 <= len(body):
        sample_rate_bytes = body[body_offset:body_offset + 4]
        sample_rate = struct.unpack(">I", sample_rate_bytes)[0]
        raw_hex = format_hex(sample_rate_bytes)
        result_str = f"{C.DIM}-> @{offset_value}:{C.RESET} {C.DIM}{raw_hex}{C.RESET} {C.CYAN}{sample_rate} Hz{C.RESET}"
    else:
        result_str = f"{C.DIM}-> @{offset_value} (out of bounds){C.RESET}"

    print(f"  {offset_str} {hex_str:30s}  {C.MAGENTA}{name:28s}{C.RESET} {result_str}", file=sys.stderr)


def format_hex_dump(data: bytes, highlights: list = None, bytes_per_line: int = 16):
    if highlights is None:
        highlights = []

    for line_start in range(0, len(data), bytes_per_line):
        line_end = min(line_start + bytes_per_line, len(data))
        result = []

        for i in range(line_start, line_end):
            color = C.DIM
            for start, end, c in highlights:
                if start <= i < end:
                    color = c
                    break
            result.append(f"{color}{data[i]:02X}{C.RESET}")

        print(f"  {' '.join(result)}", file=sys.stderr)


def format_request(data: bytes, device_name: str, command_name: str):
    if len(data) < 8:
        return

    protocol = struct.unpack(">H", data[0:2])[0]
    length = struct.unpack(">H", data[2:4])[0]
    transaction_id = struct.unpack(">H", data[4:6])[0]
    opcode = struct.unpack(">H", data[6:8])[0]

    opcode_name = get_opcode_name(protocol, opcode)

    print(f"\n{C.CYAN}{C.BOLD}>>> REQUEST{C.RESET} {C.DIM}{device_name}{C.RESET} {C.BOLD}({opcode_name}){C.RESET} {C.DIM}[{command_name}]{C.RESET}", file=sys.stderr)
    print(f"{C.DIM}{'─' * 80}{C.RESET}", file=sys.stderr)

    highlights = [
        (0, 2, C.YELLOW),
        (2, 4, C.BLUE),
        (4, 6, C.DIM),
        (6, 8, C.YELLOW),
    ]
    if len(data) > 8:
        highlights.append((8, len(data), C.CYAN))

    format_hex_dump(data, highlights)

    print(f"\n  {C.DIM}Parsed:{C.RESET}", file=sys.stderr)

    if protocol in PROTOCOL_NAMES:
        print_const(0, 2, data[0:2], PROTOCOL_NAMES[protocol])
    else:
        print_field(0, 2, data[0:2], "protocol", f"0x{protocol:04X}", C.DIM)

    print_field(2, 2, data[2:4], "length", f"{length} bytes", C.BLUE)
    print_field(4, 2, data[4:6], "transaction_id", f"0x{transaction_id:04X}", C.DIM)

    resolved_opcode_name = get_opcode_name(protocol, opcode)
    if not resolved_opcode_name.startswith("0x"):
        print_const(6, 2, data[6:8], resolved_opcode_name)
    else:
        print_field(6, 2, data[6:8], "opcode", resolved_opcode_name, C.YELLOW)

    if len(data) > 8:
        body = data[8:]

        if opcode in (0x3000, 0x2000, 0x2010) and len(body) >= 8:
            print_field(8, 2, body[0:2], "unknown", f"0x{struct.unpack('>H', body[0:2])[0]:04X}", C.DIM)
            print_field(10, 2, body[2:4], "unknown", f"0x{struct.unpack('>H', body[2:4])[0]:04X}", C.DIM)
            print_field(12, 2, body[4:6], "start_channel", str(struct.unpack(">H", body[4:6])[0]), C.BLUE)
            print_field(14, 2, body[6:8], "unknown", f"0x{struct.unpack('>H', body[6:8])[0]:04X}", C.DIM)


def format_response(data: bytes, device_name: str, command_name: str):
    if len(data) < 10:
        return

    protocol = struct.unpack(">H", data[0:2])[0]
    length = struct.unpack(">H", data[2:4])[0]
    transaction_id = struct.unpack(">H", data[4:6])[0]
    opcode = struct.unpack(">H", data[6:8])[0]
    result = struct.unpack(">H", data[8:10])[0]

    opcode_name = get_opcode_name(protocol, opcode)

    print(f"\n{C.GREEN}{C.BOLD}<<< RESPONSE{C.RESET} {C.DIM}{device_name}{C.RESET} {C.BOLD}({opcode_name}){C.RESET} {C.DIM}[{command_name}]{C.RESET}", file=sys.stderr)
    print(f"{C.DIM}{'─' * 80}{C.RESET}", file=sys.stderr)

    result_color = C.RED if result == 0x0022 else C.GREEN
    highlights = [
        (0, 2, C.YELLOW),
        (2, 4, C.BLUE),
        (4, 6, C.DIM),
        (6, 8, C.YELLOW),
        (8, 10, result_color),
    ]
    if len(data) > 10:
        highlights.append((10, len(data), C.CYAN))

    format_hex_dump(data, highlights)

    print(f"\n  {C.DIM}Parsed:{C.RESET}", file=sys.stderr)

    if protocol in PROTOCOL_NAMES:
        print_const(0, 2, data[0:2], PROTOCOL_NAMES[protocol])
    else:
        print_field(0, 2, data[0:2], "protocol", f"0x{protocol:04X}", C.DIM)

    print_field(2, 2, data[2:4], "length", f"{length} bytes", C.BLUE)
    print_field(4, 2, data[4:6], "transaction_id", f"0x{transaction_id:04X}", C.DIM)

    resolved_opcode_name = get_opcode_name(protocol, opcode)
    if not resolved_opcode_name.startswith("0x"):
        print_const(6, 2, data[6:8], resolved_opcode_name)
    else:
        print_field(6, 2, data[6:8], "opcode", resolved_opcode_name, C.YELLOW)

    if result in RESULT_NAMES:
        color = C.RED if result == 0x0022 else C.GREEN
        print(f"  {C.DIM}[  8: 10]{C.RESET} {color}{C.BOLD}{format_hex(data[8:10])}{C.RESET}  {color}{RESULT_NAMES[result]}{C.RESET}", file=sys.stderr)
    else:
        print_field(8, 2, data[8:10], "result_code", f"0x{result:04X}", C.YELLOW)

    if len(data) <= 10:
        return

    body = data[10:]

    if opcode == 0x1002:
        name = body.rstrip(b"\x00").decode("utf-8", errors="replace")
        print(f"\n  {C.DIM}Parsed:{C.RESET}", file=sys.stderr)
        print(f"  {C.CYAN}\"{name}\"{C.RESET}", file=sys.stderr)

    elif opcode == 0x1000:
        format_channel_count_body(body)

    elif opcode == 0x2010:
        format_tx_friendly_names_body(body)

    elif opcode == 0x3000:
        format_rx_channels_body(body)

    elif opcode == 0x2000:
        format_tx_channels_body(body)


def format_channel_count_body(body: bytes):
    print(f"\n  {C.DIM}Parsed (Channel Count):{C.RESET}", file=sys.stderr)

    if len(body) < 14:
        return

    unknown0 = struct.unpack(">H", body[0:2])[0]
    tx = struct.unpack(">H", body[2:4])[0]
    rx = struct.unpack(">H", body[4:6])[0]
    unknown1 = struct.unpack(">H", body[6:8])[0]
    unknown2 = struct.unpack(">H", body[8:10])[0]
    unknown3 = struct.unpack(">H", body[10:12])[0]
    unknown4 = struct.unpack(">H", body[12:14])[0]

    print_field(10, 2, body[0:2], "unknown", f"0x{unknown0:04X}", C.DIM)
    print_field(12, 2, body[2:4], "tx_count", str(tx), C.BLUE)
    print_field(14, 2, body[4:6], "rx_count", str(rx), C.BLUE)
    print_field(16, 2, body[6:8], "unknown", f"0x{unknown1:04X}", C.DIM)
    print_field(18, 2, body[8:10], "unknown", f"0x{unknown2:04X}", C.DIM)
    print_field(20, 2, body[10:12], "unknown", f"0x{unknown3:04X}", C.DIM)
    print_field(22, 2, body[12:14], "unknown", f"0x{unknown4:04X}", C.DIM)


def format_tx_friendly_names_body(body: bytes):
    print(f"\n  {C.DIM}Parsed (TX Channel Friendly Names):{C.RESET}", file=sys.stderr)

    if len(body) < 2:
        return

    record_size = 6
    header_size = 2
    record_start = header_size
    record_index = 0
    last_channel = None

    while record_start + record_size <= len(body):
        record = body[record_start : record_start + record_size]
        channel_num = struct.unpack(">H", record[0:2])[0]

        if channel_num == 0:
            break

        if last_channel is not None and channel_num != last_channel + 1:
            break

        name_ptr = struct.unpack(">H", record[4:6])[0]

        if name_ptr < 10 + record_start + record_size:
            break

        unknown = struct.unpack(">H", record[2:4])[0]

        print_record_separator(record_index + 1, "TX Channel")
        print_field(10 + record_start, 2, record[0:2], "channel_number", str(channel_num), C.BLUE)
        print_field(10 + record_start + 2, 2, record[2:4], "unknown", f"0x{unknown:04X}", C.DIM)
        print_pointer(10 + record_start + 4, 2, record[4:6], "name_offset", name_ptr, body)

        last_channel = channel_num
        record_start += record_size
        record_index += 1


def format_tx_channels_body(body: bytes):
    print(f"\n  {C.DIM}Parsed (TX Channels):{C.RESET}", file=sys.stderr)

    if len(body) < 2:
        return

    record_size = 8
    header_size = 2
    record_start = header_size
    record_index = 0
    last_channel = None
    first_channel_group = None

    while record_start + record_size <= len(body):
        record = body[record_start : record_start + record_size]
        channel_num = struct.unpack(">H", record[0:2])[0]

        if channel_num == 0:
            break

        if last_channel is not None and channel_num != last_channel + 1:
            break

        name_ptr = struct.unpack(">H", record[6:8])[0]

        if name_ptr > 0 and name_ptr < 10 + record_start + record_size:
            break

        unknown = struct.unpack(">H", record[2:4])[0]
        channel_group = struct.unpack(">H", record[4:6])[0]

        if first_channel_group is None:
            first_channel_group = channel_group

        if channel_group != first_channel_group:
            break

        print_record_separator(record_index + 1, "TX Channel")
        print_field(10 + record_start, 2, record[0:2], "channel_number", str(channel_num), C.BLUE)
        print_field(10 + record_start + 2, 2, record[2:4], "unknown", f"0x{unknown:04X}", C.DIM)

        print_sample_rate_offset(10 + record_start + 4, 2, record[4:6], "sample_rate_offset", channel_group, body)

        print_pointer(10 + record_start + 6, 2, record[6:8], "name_offset", name_ptr, body)

        last_channel = channel_num
        record_start += record_size
        record_index += 1


def format_rx_channels_body(body: bytes):
    print(f"\n  {C.DIM}Parsed (RX Channels):{C.RESET}", file=sys.stderr)

    if len(body) < 2:
        return

    record_size = 20
    header_size = 2
    record_start = header_size
    record_index = 0
    last_channel = None

    while record_start + record_size <= len(body):
        record = body[record_start : record_start + record_size]
        channel_num = struct.unpack(">H", record[0:2])[0]

        if channel_num == 0:
            break

        if last_channel is not None and channel_num != last_channel + 1:
            break

        rx_channel_ptr = struct.unpack(">H", record[10:12])[0]

        if rx_channel_ptr > 0 and rx_channel_ptr < 10 + record_start + record_size:
            break

        unknown = struct.unpack(">H", record[2:4])[0]
        sample_rate_offset_val = struct.unpack(">H", record[4:6])[0]
        tx_channel_offset = struct.unpack(">H", record[6:8])[0]
        tx_device_offset = struct.unpack(">H", record[8:10])[0]
        status = struct.unpack(">H", record[12:14])[0]
        sub_status = struct.unpack(">H", record[14:16])[0]

        print_record_separator(record_index + 1, "RX Channel")
        print_field(10 + record_start, 2, record[0:2], "channel_number", str(channel_num), C.BLUE)
        print_field(10 + record_start + 2, 2, record[2:4], "unknown", f"0x{unknown:04X}", C.DIM)

        if record_index == 0 and tx_device_offset != 0:
            print_sample_rate_offset(10 + record_start + 4, 2, record[4:6], "sample_rate_offset", sample_rate_offset_val, body)
        else:
            print_field(10 + record_start + 4, 2, record[4:6], "unknown", f"0x{sample_rate_offset_val:04X}", C.DIM)

        print_pointer(10 + record_start + 6, 2, record[6:8], "tx_channel_offset", tx_channel_offset, body)
        print_pointer(10 + record_start + 8, 2, record[8:10], "tx_device_offset", tx_device_offset, body)
        print_pointer(10 + record_start + 10, 2, record[10:12], "rx_channel_offset", rx_channel_ptr, body)
        print_field(10 + record_start + 12, 2, record[12:14], "status", f"0x{status:04X}", C.DIM)

        sub_name = get_subscription_status_name(sub_status)
        sub_state = get_subscription_status_state(sub_status)

        if sub_state == "connected":
            print(f"  {C.DIM}[{10+record_start+14:3d}:{10+record_start+16:3d}]{C.RESET} {C.GREEN}{C.BOLD}{format_hex(record[14:16])}{C.RESET}  {C.GREEN}subscription_status          {C.RESET} = {sub_name}", file=sys.stderr)
        else:
            print_field(10 + record_start + 14, 2, record[14:16], "subscription_status", sub_name, C.YELLOW)

        last_channel = channel_num
        record_start += record_size
        record_index += 1
