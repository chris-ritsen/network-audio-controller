from __future__ import annotations

import struct

from netaudio.dante.debug_formatter import get_subscription_status_name
from netaudio.dante.packet_dissection_models import DissectedPacket, Span
from netaudio.dante.packet_dissection_values import _format_hz


def _get_null_terminated_string(payload: bytes, abs_offset: int) -> str:
    if abs_offset < 0 or abs_offset >= len(payload):
        return ""
    end = payload.find(b"\x00", abs_offset)
    if end < 0:
        end = len(payload)
    return payload[abs_offset:end].decode("ascii", errors="replace")


def _dissect_rx_channels_body(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 32:
        return

    record_size = 20
    body_start = 12

    max_per_page = payload[10]
    channel_count = payload[11]

    header_section_ref = "arc_opcode:0x3000_header"
    header_section_label = "RX Channel Body Header"
    result.sections.append((header_section_ref, header_section_label))

    result.spans.append(
        Span(
            offset=10,
            length=1,
            name="max_per_page",
            raw=payload[10:11],
            value=str(max_per_page),
            detail="",
            fact_ref=header_section_ref,
            section=header_section_label,
            dtype="uint8",
        )
    )
    covered.add(10)

    result.spans.append(
        Span(
            offset=11,
            length=1,
            name="channel_count",
            raw=payload[11:12],
            value=str(channel_count),
            detail="",
            fact_ref=header_section_ref,
            section=header_section_label,
            dtype="uint8",
        )
    )
    covered.add(11)

    section_ref = "arc_opcode:0x3000_body"
    section_label = "RX Channel Records"
    result.sections.append((section_ref, section_label))

    record_index = 0
    offset = body_start
    max_records = channel_count if channel_count > 0 else 64
    metadata_pointer = None

    while offset + record_size <= len(payload) and record_index < max_records:
        channel_number = struct.unpack(">H", payload[offset : offset + 2])[0]
        if channel_number == 0:
            break

        flags = struct.unpack(">H", payload[offset + 2 : offset + 4])[0]
        sample_rate_pointer = struct.unpack(">H", payload[offset + 4 : offset + 6])[0]
        if metadata_pointer is None and sample_rate_pointer > 0:
            metadata_pointer = sample_rate_pointer
        tx_channel_pointer = struct.unpack(">H", payload[offset + 6 : offset + 8])[0]
        tx_device_pointer = struct.unpack(">H", payload[offset + 8 : offset + 10])[0]
        rx_channel_pointer = struct.unpack(">H", payload[offset + 10 : offset + 12])[0]
        status = struct.unpack(">H", payload[offset + 12 : offset + 14])[0]
        subscription_status = struct.unpack(">H", payload[offset + 14 : offset + 16])[0]

        rx_channel_name = ""
        if rx_channel_pointer > 0:
            rx_channel_name = _get_null_terminated_string(payload, rx_channel_pointer)

        tx_channel_name = ""
        if tx_channel_pointer > 0:
            tx_channel_name = _get_null_terminated_string(payload, tx_channel_pointer)

        tx_device_name = ""
        if tx_device_pointer > 0:
            tx_device_name = _get_null_terminated_string(payload, tx_device_pointer)

        sample_rate = None
        if sample_rate_pointer > 0 and sample_rate_pointer + 4 <= len(payload):
            raw_rate = struct.unpack(">I", payload[sample_rate_pointer : sample_rate_pointer + 4])[0]
            if 8000 <= raw_rate <= 384000:
                sample_rate = raw_rate

        subscription_status_detail = get_subscription_status_name(subscription_status)

        channel_label = rx_channel_name or str(channel_number)
        subscription_info = ""
        if tx_channel_name and tx_device_name:
            subscription_info = f"{tx_channel_name}@{tx_device_name}"
        elif tx_channel_name:
            subscription_info = tx_channel_name

        channel_detail = channel_label
        if subscription_info:
            channel_detail += f" <- {subscription_info}"
        if sample_rate:
            channel_detail += f" ({_format_hz(sample_rate)})"

        result.spans.append(
            Span(
                offset=offset,
                length=2,
                name="channel_number",
                raw=payload[offset : offset + 2],
                value=str(channel_number),
                detail=channel_detail,
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset, offset + 2):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 2,
                length=2,
                name="flags",
                raw=payload[offset + 2 : offset + 4],
                value=f"0x{flags:04X}",
                detail="",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 2, offset + 4):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 4,
                length=2,
                name="sample_rate_ptr",
                raw=payload[offset + 4 : offset + 6],
                value=f"0x{sample_rate_pointer:04X}",
                detail=_format_hz(sample_rate) if sample_rate else "",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 4, offset + 6):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 6,
                length=2,
                name="tx_channel_ptr",
                raw=payload[offset + 6 : offset + 8],
                value=f"0x{tx_channel_pointer:04X}",
                detail=tx_channel_name if tx_channel_name and tx_channel_name != "." else "",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 6, offset + 8):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 8,
                length=2,
                name="tx_device_ptr",
                raw=payload[offset + 8 : offset + 10],
                value=f"0x{tx_device_pointer:04X}",
                detail=tx_device_name if tx_device_name and tx_device_name != "." else "",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 8, offset + 10):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 10,
                length=2,
                name="rx_channel_ptr",
                raw=payload[offset + 10 : offset + 12],
                value=f"0x{rx_channel_pointer:04X}",
                detail=rx_channel_name,
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 10, offset + 12):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 12,
                length=2,
                name="status",
                raw=payload[offset + 12 : offset + 14],
                value=f"0x{status:04X}",
                detail="",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 12, offset + 14):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 14,
                length=2,
                name="subscription_status",
                raw=payload[offset + 14 : offset + 16],
                value=f"0x{subscription_status:04X}",
                detail=subscription_status_detail,
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 14, offset + 16):
            covered.add(byte_offset)

        if offset + 16 < offset + record_size:
            result.spans.append(
                Span(
                    offset=offset + 16,
                    length=4,
                    name="padding",
                    raw=payload[offset + 16 : offset + 20],
                    value=payload[offset + 16 : offset + 20].hex(),
                    detail="",
                    fact_ref=section_ref,
                    section=section_label,
                    dtype="hex",
                )
            )
            for byte_offset in range(offset + 16, offset + 20):
                covered.add(byte_offset)

        offset += record_size
        record_index += 1

    if metadata_pointer is not None and metadata_pointer + 16 <= len(payload):
        _dissect_rx_metadata_block(payload, metadata_pointer, result, covered)
        string_area_start = metadata_pointer + 16
    else:
        string_area_start = offset

    _dissect_string_area(payload, string_area_start, "arc_opcode:0x3000_strings", "RX Channel Strings", result, covered)


def _dissect_rx_metadata_block(
    payload: bytes,
    start_offset: int,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    section_ref = "arc_opcode:0x3000_metadata"
    section_label = "RX Channel Metadata"
    result.sections.append((section_ref, section_label))

    sample_rate = struct.unpack(">I", payload[start_offset : start_offset + 4])[0]
    result.spans.append(
        Span(
            offset=start_offset,
            length=4,
            name="sample_rate",
            raw=payload[start_offset : start_offset + 4],
            value=str(sample_rate),
            detail=_format_hz(sample_rate) if 8000 <= sample_rate <= 384000 else "",
            fact_ref=section_ref,
            section=section_label,
            dtype="uint32_be",
        )
    )
    for byte_offset in range(start_offset, start_offset + 4):
        covered.add(byte_offset)

    field_names = ["unknown_0x04", "unknown_0x06", "unknown_0x08", "unknown_0x0A", "unknown_0x0C", "unknown_0x0E"]
    for index, field_name in enumerate(field_names):
        field_offset = start_offset + 4 + index * 2
        field_value = struct.unpack(">H", payload[field_offset : field_offset + 2])[0]
        result.spans.append(
            Span(
                offset=field_offset,
                length=2,
                name=field_name,
                raw=payload[field_offset : field_offset + 2],
                value=str(field_value),
                detail="",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(field_offset, field_offset + 2):
            covered.add(byte_offset)


def _dissect_tx_channels_body(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 20:
        return

    record_size = 8
    body_start = 12

    max_per_page = payload[10]
    channel_count = payload[11]

    header_section_ref = "arc_opcode:0x2000_header"
    header_section_label = "TX Channel Body Header"
    result.sections.append((header_section_ref, header_section_label))

    result.spans.append(
        Span(
            offset=10,
            length=1,
            name="max_per_page",
            raw=payload[10:11],
            value=str(max_per_page),
            detail="",
            fact_ref=header_section_ref,
            section=header_section_label,
            dtype="uint8",
        )
    )
    covered.add(10)

    result.spans.append(
        Span(
            offset=11,
            length=1,
            name="channel_count",
            raw=payload[11:12],
            value=str(channel_count),
            detail="",
            fact_ref=header_section_ref,
            section=header_section_label,
            dtype="uint8",
        )
    )
    covered.add(11)

    section_ref = "arc_opcode:0x2000_body"
    section_label = "TX Channel Records"
    result.sections.append((section_ref, section_label))

    record_index = 0
    offset = body_start
    max_records = channel_count if channel_count > 0 else 128
    metadata_pointer = None

    while offset + record_size <= len(payload) and record_index < max_records:
        channel_number = struct.unpack(">H", payload[offset : offset + 2])[0]
        if channel_number == 0:
            break

        unknown_field = struct.unpack(">H", payload[offset + 2 : offset + 4])[0]
        metadata_ptr = struct.unpack(">H", payload[offset + 4 : offset + 6])[0]
        name_pointer = struct.unpack(">H", payload[offset + 6 : offset + 8])[0]

        if metadata_pointer is None and metadata_ptr > 0:
            metadata_pointer = metadata_ptr

        channel_name = ""
        if name_pointer > 0:
            channel_name = _get_null_terminated_string(payload, name_pointer)

        sample_rate = None
        if metadata_ptr > 0 and metadata_ptr + 4 <= len(payload):
            raw_rate = struct.unpack(">I", payload[metadata_ptr : metadata_ptr + 4])[0]
            if 8000 <= raw_rate <= 384000:
                sample_rate = raw_rate

        channel_detail = channel_name or str(channel_number)
        if sample_rate:
            channel_detail += f" ({_format_hz(sample_rate)})"

        result.spans.append(
            Span(
                offset=offset,
                length=2,
                name="channel_number",
                raw=payload[offset : offset + 2],
                value=str(channel_number),
                detail=channel_detail,
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset, offset + 2):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 2,
                length=2,
                name="unknown_0x02",
                raw=payload[offset + 2 : offset + 4],
                value=f"0x{unknown_field:04X}",
                detail="",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 2, offset + 4):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 4,
                length=2,
                name="metadata_ptr",
                raw=payload[offset + 4 : offset + 6],
                value=f"0x{metadata_ptr:04X}",
                detail=_format_hz(sample_rate) if sample_rate else "",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 4, offset + 6):
            covered.add(byte_offset)

        result.spans.append(
            Span(
                offset=offset + 6,
                length=2,
                name="name_ptr",
                raw=payload[offset + 6 : offset + 8],
                value=f"0x{name_pointer:04X}",
                detail=channel_name,
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(offset + 6, offset + 8):
            covered.add(byte_offset)

        offset += record_size
        record_index += 1

    if metadata_pointer is not None and metadata_pointer + 16 <= len(payload):
        _dissect_tx_metadata_block(payload, metadata_pointer, result, covered)
        string_area_start = metadata_pointer + 16
    else:
        string_area_start = offset

    _dissect_string_area(payload, string_area_start, "arc_opcode:0x2000_strings", "TX Channel Strings", result, covered)


def _dissect_tx_metadata_block(
    payload: bytes,
    start_offset: int,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    section_ref = "arc_opcode:0x2000_metadata"
    section_label = "TX Channel Metadata"
    result.sections.append((section_ref, section_label))

    sample_rate = struct.unpack(">I", payload[start_offset : start_offset + 4])[0]
    result.spans.append(
        Span(
            offset=start_offset,
            length=4,
            name="sample_rate",
            raw=payload[start_offset : start_offset + 4],
            value=str(sample_rate),
            detail=_format_hz(sample_rate) if 8000 <= sample_rate <= 384000 else "",
            fact_ref=section_ref,
            section=section_label,
            dtype="uint32_be",
        )
    )
    for byte_offset in range(start_offset, start_offset + 4):
        covered.add(byte_offset)

    field_names = ["unknown_0x04", "unknown_0x06", "unknown_0x08", "unknown_0x0A", "unknown_0x0C", "unknown_0x0E"]
    for index, field_name in enumerate(field_names):
        field_offset = start_offset + 4 + index * 2
        field_value = struct.unpack(">H", payload[field_offset : field_offset + 2])[0]
        result.spans.append(
            Span(
                offset=field_offset,
                length=2,
                name=field_name,
                raw=payload[field_offset : field_offset + 2],
                value=str(field_value),
                detail="",
                fact_ref=section_ref,
                section=section_label,
                dtype="uint16_be",
            )
        )
        for byte_offset in range(field_offset, field_offset + 2):
            covered.add(byte_offset)


def _dissect_string_area(
    payload: bytes,
    start_offset: int,
    section_ref: str,
    section_label: str,
    result: DissectedPacket,
    covered: set[int],
) -> None:
    if start_offset >= len(payload):
        return

    string_data = payload[start_offset:]
    strings_found = []
    pos = 0

    while pos < len(string_data):
        null_pos = string_data.find(b"\x00", pos)
        if null_pos < 0:
            break
        string_val = string_data[pos:null_pos].decode("ascii", errors="replace")
        if string_val:
            strings_found.append((start_offset + pos, string_val))
        pos = null_pos + 1

    if strings_found:
        result.sections.append((section_ref, section_label))
        for string_offset, string_val in strings_found:
            string_length = len(string_val) + 1
            result.spans.append(
                Span(
                    offset=string_offset,
                    length=string_length,
                    name="string",
                    raw=payload[string_offset : string_offset + string_length],
                    value=f'"{string_val}"',
                    detail="",
                    fact_ref=section_ref,
                    section=section_label,
                    dtype="ascii",
                )
            )
            for byte_offset in range(string_offset, string_offset + string_length):
                covered.add(byte_offset)
