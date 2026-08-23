from __future__ import annotations

import struct

from netaudio.dante.packet_dissection_models import DEVICE_SETTINGS_PROPERTY_NAMES, DissectedPacket, Span
from netaudio.dante.packet_dissection_values import _humanize_value


def _append_dissected_span(
    payload: bytes,
    result: DissectedPacket,
    covered: set[int],
    *,
    offset: int,
    length: int,
    name: str,
    value: str,
    detail: str,
    fact_reference: str,
    section: str,
    data_type: str,
) -> None:
    if offset < 0 or length <= 0 or offset + length > len(payload):
        return

    result.spans = [
        span
        for span in result.spans
        if span.offset != offset or span.length != length or span.fact_ref == fact_reference
    ]
    result.spans.append(
        Span(
            offset=offset,
            length=length,
            name=name,
            raw=payload[offset : offset + length],
            value=value,
            detail=detail,
            fact_ref=fact_reference,
            section=section,
            dtype=data_type,
        )
    )
    for byte_offset in range(offset, offset + length):
        covered.add(byte_offset)


def _device_settings_property_name(property_identifier: int) -> str:
    return DEVICE_SETTINGS_PROPERTY_NAMES.get(property_identifier, f"property_0x{property_identifier:04x}")


def _dissect_encoding_status(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 40:
        return

    fact_reference = "conmon_message:0x0082_body"
    section = "Encoding Status and Capabilities"
    result.sections.append((fact_reference, section))

    supported_encoding_count = struct.unpack(">H", payload[34:36])[0]
    current_encoding = struct.unpack(">I", payload[36:40])[0]
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=34,
        length=2,
        name="supported_encoding_count",
        value=str(supported_encoding_count),
        detail="",
        fact_reference=fact_reference,
        section=section,
        data_type="uint16_be",
    )
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=36,
        length=4,
        name="current_encoding",
        value=str(current_encoding),
        detail=f"PCM{current_encoding}",
        fact_reference=fact_reference,
        section=section,
        data_type="uint32_be",
    )
    supported_encoding_start = 48
    available_encoding_count = max(0, len(payload) - supported_encoding_start) // 4
    decoded_encoding_count = min(supported_encoding_count, available_encoding_count)
    for encoding_index in range(decoded_encoding_count):
        encoding_offset = supported_encoding_start + encoding_index * 4
        encoding = struct.unpack(">I", payload[encoding_offset : encoding_offset + 4])[0]
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=encoding_offset,
            length=4,
            name=f"supported_encoding_{encoding_index + 1}",
            value=str(encoding),
            detail=f"PCM{encoding}",
            fact_reference=fact_reference,
            section=section,
            data_type="uint32_be",
        )


def _dissect_encoding_control(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 40:
        return

    fact_reference = "conmon_message:0x0083_body"
    section = "Encoding Control"
    result.sections.append((fact_reference, section))

    control_constant = struct.unpack(">I", payload[28:32])[0]
    operation_mode = struct.unpack(">I", payload[32:36])[0]
    encoding_operand = struct.unpack(">I", payload[36:40])[0]
    operation_detail = {0: "read current and supported encodings", 1: "set encoding"}.get(
        operation_mode, "unknown mode"
    )
    operand_name = "target_encoding" if operation_mode == 1 else "ignored_encoding_operand"
    operand_detail = f"PCM{encoding_operand}" if operation_mode == 1 else "ignored in read mode"

    _append_dissected_span(
        payload,
        result,
        covered,
        offset=28,
        length=4,
        name="control_constant",
        value=f"0x{control_constant:08X}",
        detail="",
        fact_reference=fact_reference,
        section=section,
        data_type="uint32_be",
    )
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=32,
        length=4,
        name="operation_mode",
        value=str(operation_mode),
        detail=operation_detail,
        fact_reference=fact_reference,
        section=section,
        data_type="uint32_be",
    )
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=36,
        length=4,
        name=operand_name,
        value=str(encoding_operand),
        detail=operand_detail,
        fact_reference=fact_reference,
        section=section,
        data_type="uint32_be",
    )


def _dissect_routing_capacity_status(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) != 40:
        return

    fact_reference = "conmon_message:0x0100_body"
    section = "Routing Capacity Status"
    result.sections.append((fact_reference, section))
    unmapped_prefix_word = struct.unpack(">I", payload[28:32])[0]
    state_code = struct.unpack(">H", payload[32:34])[0]
    unmapped_word = struct.unpack(">H", payload[34:36])[0]
    transmit_channel_count = struct.unpack(">H", payload[36:38])[0]
    receive_channel_count = struct.unpack(">H", payload[38:40])[0]
    state_detail = {0x0001: "transitioning", 0x0101: "routing ready"}.get(state_code, "unknown")
    fields = (
        (28, 4, "unmapped_prefix_word", f"0x{unmapped_prefix_word:08X}", "", "uint32_be"),
        (32, 2, "state_code", f"0x{state_code:04X}", state_detail, "uint16_be"),
        (34, 2, "unmapped_word", f"0x{unmapped_word:04X}", "", "uint16_be"),
        (36, 2, "transmit_channel_count", str(transmit_channel_count), "", "uint16_be"),
        (38, 2, "receive_channel_count", str(receive_channel_count), "", "uint16_be"),
    )
    for offset, length, name, value, detail, data_type in fields:
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=offset,
            length=length,
            name=name,
            value=value,
            detail=detail,
            fact_reference=fact_reference,
            section=section,
            data_type=data_type,
        )


def _dissect_device_settings_query(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 12:
        return

    fact_reference = "arc_opcode:0x1100_query"
    section = "Device Settings Query"
    result.sections.append((fact_reference, section))
    query_prefix = payload[10]
    property_count = payload[11]
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=10,
        length=1,
        name="query_prefix",
        value=f"0x{query_prefix:02X}",
        detail="",
        fact_reference=fact_reference,
        section=section,
        data_type="uint8",
    )
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=11,
        length=1,
        name="property_count",
        value=str(property_count),
        detail="",
        fact_reference=fact_reference,
        section=section,
        data_type="uint8",
    )

    available_property_count = max(0, len(payload) - 12) // 2
    decoded_property_count = min(property_count, available_property_count)
    for property_index in range(decoded_property_count):
        property_offset = 12 + property_index * 2
        property_identifier = struct.unpack(">H", payload[property_offset : property_offset + 2])[0]
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=property_offset,
            length=2,
            name="requested_property_id",
            value=f"0x{property_identifier:04X}",
            detail=_device_settings_property_name(property_identifier),
            fact_reference=fact_reference,
            section=section,
            data_type="uint16_be",
        )


def _dissect_device_settings_response(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 12:
        return

    table_fact_reference = "arc_opcode:0x1100_table"
    table_section = "Device Settings Property Table"
    values_fact_reference = "arc_opcode:0x1100_values"
    values_section = "Device Settings Values"
    result.sections.append((table_fact_reference, table_section))

    table_prefix = payload[10]
    property_count = payload[11]
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=10,
        length=1,
        name="table_prefix",
        value=f"0x{table_prefix:02X}",
        detail="",
        fact_reference=table_fact_reference,
        section=table_section,
        data_type="uint8",
    )
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=11,
        length=1,
        name="property_count",
        value=str(property_count),
        detail="",
        fact_reference=table_fact_reference,
        section=table_section,
        data_type="uint8",
    )

    declared_values_offset = 12 + property_count * 4
    available_property_count = max(0, len(payload) - 12) // 4
    decoded_property_count = min(property_count, available_property_count)
    referenced_pointer_set = set()
    for property_index in range(decoded_property_count):
        property_offset = 12 + property_index * 4
        property_identifier, property_word = struct.unpack(">HH", payload[property_offset : property_offset + 4])
        if (
            property_identifier & 0x8000
            and declared_values_offset <= property_word
            and property_word + 4 <= len(payload)
        ):
            referenced_pointer_set.add(property_word)
    referenced_pointers = sorted(referenced_pointer_set)
    referenced_value_ends = {
        pointer: referenced_pointers[index + 1] if index + 1 < len(referenced_pointers) else len(payload)
        for index, pointer in enumerate(referenced_pointers)
    }
    values_section_added = False

    for property_index in range(decoded_property_count):
        property_offset = 12 + property_index * 4
        property_identifier = struct.unpack(">H", payload[property_offset : property_offset + 2])[0]
        property_word = struct.unpack(">H", payload[property_offset + 2 : property_offset + 4])[0]

        if property_identifier == 0:
            _append_dissected_span(
                payload,
                result,
                covered,
                offset=property_offset,
                length=2,
                name="unsupported_marker",
                value="0x0000",
                detail="",
                fact_reference=table_fact_reference,
                section=table_section,
                data_type="uint16_be",
            )
            _append_dissected_span(
                payload,
                result,
                covered,
                offset=property_offset + 2,
                length=2,
                name="unsupported_property_id",
                value=f"0x{property_word:04X}",
                detail=_device_settings_property_name(property_word),
                fact_reference=table_fact_reference,
                section=table_section,
                data_type="uint16_be",
            )
            continue

        property_name = _device_settings_property_name(property_identifier)
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=property_offset,
            length=2,
            name="property_id",
            value=f"0x{property_identifier:04X}",
            detail=property_name,
            fact_reference=table_fact_reference,
            section=table_section,
            data_type="uint16_be",
        )

        if property_identifier & 0x8000:
            value_end = referenced_value_ends.get(property_word)
            pointer_is_valid = value_end is not None
            pointer_detail = (
                f"@0x{property_word:04X}, {value_end - property_word} bytes"
                if value_end is not None
                else "out of bounds"
            )
            _append_dissected_span(
                payload,
                result,
                covered,
                offset=property_offset + 2,
                length=2,
                name="value_pointer",
                value=f"0x{property_word:04X}",
                detail=pointer_detail,
                fact_reference=table_fact_reference,
                section=table_section,
                data_type="uint16_be",
            )
            if not pointer_is_valid:
                continue

            if not values_section_added:
                result.sections.append((values_fact_reference, values_section))
                values_section_added = True
            if property_identifier in DEVICE_SETTINGS_PROPERTY_NAMES:
                property_value = struct.unpack(">I", payload[property_word : property_word + 4])[0]
                property_value_display = _humanize_value(
                    property_name,
                    property_value,
                    f"0x{property_value:08X}",
                    "uint32_be",
                )
                _append_dissected_span(
                    payload,
                    result,
                    covered,
                    offset=property_word,
                    length=4,
                    name=property_name,
                    value=property_value_display,
                    detail="",
                    fact_reference=values_fact_reference,
                    section=values_section,
                    data_type="uint32_be",
                )
                if value_end > property_word + 4:
                    extension = payload[property_word + 4 : value_end]
                    _append_dissected_span(
                        payload,
                        result,
                        covered,
                        offset=property_word + 4,
                        length=len(extension),
                        name=f"{property_name}_extension",
                        value=extension.hex(),
                        detail="",
                        fact_reference=values_fact_reference,
                        section=values_section,
                        data_type="hex",
                    )
            else:
                property_value = payload[property_word:value_end]
                _append_dissected_span(
                    payload,
                    result,
                    covered,
                    offset=property_word,
                    length=len(property_value),
                    name=property_name,
                    value=property_value.hex(),
                    detail="",
                    fact_reference=values_fact_reference,
                    section=values_section,
                    data_type="hex",
                )
        else:
            _append_dissected_span(
                payload,
                result,
                covered,
                offset=property_offset + 2,
                length=2,
                name="inline_value",
                value=f"0x{property_word:04X}",
                detail=property_name,
                fact_reference=table_fact_reference,
                section=table_section,
                data_type="uint16_be",
            )


def _dissect_property_directory(payload: bytes, result: DissectedPacket, covered: set[int]) -> None:
    if len(payload) < 12:
        return

    fact_reference = "arc_opcode:0x1102_directory"
    section = "Property Directory"
    result.sections.append((fact_reference, section))
    property_count = struct.unpack(">H", payload[10:12])[0]
    _append_dissected_span(
        payload,
        result,
        covered,
        offset=10,
        length=2,
        name="property_count",
        value=str(property_count),
        detail="",
        fact_reference=fact_reference,
        section=section,
        data_type="uint16_be",
    )

    available_property_count = max(0, len(payload) - 12) // 4
    decoded_property_count = min(property_count, available_property_count)
    for property_index in range(decoded_property_count):
        property_offset = 12 + property_index * 4
        property_identifier = struct.unpack(">H", payload[property_offset : property_offset + 2])[0]
        property_flags = struct.unpack(">H", payload[property_offset + 2 : property_offset + 4])[0]
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=property_offset,
            length=2,
            name="property_id",
            value=f"0x{property_identifier:04X}",
            detail=_device_settings_property_name(property_identifier),
            fact_reference=fact_reference,
            section=section,
            data_type="uint16_be",
        )
        _append_dissected_span(
            payload,
            result,
            covered,
            offset=property_offset + 2,
            length=2,
            name="property_flags",
            value=f"0x{property_flags:04X}",
            detail="",
            fact_reference=fact_reference,
            section=section,
            data_type="uint16_be",
        )
