from __future__ import annotations

import struct

from netaudio.commands.firmware.constants import (
    CAPABILITY_9_CHANNEL_NAME_SIZE,
    CAPABILITY_9_DEVICE_DESCRIPTOR_SIZE,
    CAPABILITY_9_OEM_DESCRIPTOR_SIZE,
)
from netaudio.commands.firmware.validation import _validate_capability_payload


def _read_str(data, offset, maximum_length=128):
    if offset >= len(data):
        return ""
    end = data.find(b"\x00", offset, offset + maximum_length)
    if end == -1:
        end = offset + maximum_length
    raw_bytes = data[offset:end]
    try:
        decoded = raw_bytes.decode("ascii")
    except UnicodeDecodeError:
        return ""
    while decoded and ord(decoded[-1]) < 32:
        decoded = decoded[:-1]
    return decoded


def _is_printable(text):
    return bool(text) and all(32 <= ord(character) < 127 for character in text)


def _find_str(payload, candidates, maximum_length=128):
    for offset in candidates:
        if offset < len(payload):
            text = _read_str(payload, offset, maximum_length)
            text = text.rstrip("\x01\x02\x03\x04\x05\x06\x07\x08")
            if _is_printable(text) and len(text) >= 2:
                return text
    return ""


def _find_channel_names(payload, base_candidates, maximum_channels=64, stride=32):
    for base in base_candidates:
        first = _read_str(payload, base, stride)
        if not first:
            continue
        names = []
        for channel_index in range(maximum_channels):
            offset = base + channel_index * stride
            if offset >= len(payload):
                break
            channel_name = _read_str(payload, offset, stride)
            if channel_name:
                names.append(channel_name)
            else:
                break
        if names:
            return names
    return []


def _find_oem_strings(payload, manufacturer_header, search_start):
    """Search for manufacturer_header in config, then read manufacturer and
    product_name at fixed relative offsets (+16 and +144 respectively)."""
    if not manufacturer_header or len(manufacturer_header) < 2:
        return "", ""
    needle = manufacturer_header.encode("ascii")
    header_offset = payload.find(needle + b"\x00", search_start)
    if header_offset == -1:
        header_offset = payload.find(needle, search_start)
    if header_offset == -1:
        return "", ""
    manufacturer = _read_str(payload, header_offset + 16, 64)
    if not _is_printable(manufacturer):
        manufacturer = ""
    product = _read_str(payload, header_offset + 144, 128)
    if not _is_printable(product):
        product = ""
    return manufacturer, product


def _validate_capability_range(payload, offset, size, field_name):
    if offset > len(payload) or size > len(payload) - offset:
        raise ValueError(f"{field_name} extends past capability payload: {offset + size} > {len(payload)}")


def _decode_capability_utf8(payload, offset, size, field_name):
    _validate_capability_range(payload, offset, size, field_name)
    raw_value = payload[offset : offset + size]
    terminator_offset = raw_value.find(b"\x00")
    if terminator_offset != -1:
        raw_value = raw_value[:terminator_offset]
    try:
        return raw_value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{field_name} contains invalid UTF-8") from error


def _parse_capability_9_layout(payload):
    header = _validate_capability_payload(payload)
    device_descriptor_offset = header["device_descriptor_offset"]
    _validate_capability_range(
        payload,
        device_descriptor_offset,
        CAPABILITY_9_DEVICE_DESCRIPTOR_SIZE,
        "Capability device descriptor",
    )

    transmit_channel_count, receive_channel_count = struct.unpack_from(">HH", payload, device_descriptor_offset + 8)
    transmit_channel_names_offset, receive_channel_names_offset = struct.unpack_from(
        ">HH", payload, device_descriptor_offset + 0x10
    )
    oem_descriptor_offset = struct.unpack_from(">H", payload, device_descriptor_offset + 0x1C)[0]

    _validate_capability_range(
        payload,
        transmit_channel_names_offset,
        transmit_channel_count * CAPABILITY_9_CHANNEL_NAME_SIZE,
        "Capability transmit channel name array",
    )
    _validate_capability_range(
        payload,
        receive_channel_names_offset,
        receive_channel_count * CAPABILITY_9_CHANNEL_NAME_SIZE,
        "Capability receive channel name array",
    )
    _validate_capability_range(
        payload,
        oem_descriptor_offset,
        CAPABILITY_9_OEM_DESCRIPTOR_SIZE,
        "Capability OEM descriptor",
    )

    return {
        "device_descriptor_offset": device_descriptor_offset,
        "transmit_channel_count": transmit_channel_count,
        "receive_channel_count": receive_channel_count,
        "transmit_channel_names_offset": transmit_channel_names_offset,
        "receive_channel_names_offset": receive_channel_names_offset,
        "oem_descriptor_offset": oem_descriptor_offset,
    }


def _decode_capability_channel_names(payload, offset, count, field_name):
    return [
        _decode_capability_utf8(
            payload,
            offset + channel_index * CAPABILITY_9_CHANNEL_NAME_SIZE,
            CAPABILITY_9_CHANNEL_NAME_SIZE,
            f"{field_name} {channel_index}",
        )
        for channel_index in range(count)
    ]


def _extract_capability_9(payload, manufacturer_header):
    if len(manufacturer_header) != 8:
        raise ValueError(f"DNT manufacturer header must contain 8 bytes, received {len(manufacturer_header)}")

    layout = _parse_capability_9_layout(payload)
    device_descriptor_offset = layout["device_descriptor_offset"]
    oem_descriptor_offset = layout["oem_descriptor_offset"]
    manufacturer_short_bytes = payload[oem_descriptor_offset + 4 : oem_descriptor_offset + 12]
    if manufacturer_short_bytes != manufacturer_header:
        raise ValueError("Capability OEM manufacturer short name does not match the DNT manufacturer header")

    transmit_channel_names = _decode_capability_channel_names(
        payload,
        layout["transmit_channel_names_offset"],
        layout["transmit_channel_count"],
        "Capability transmit channel name",
    )
    receive_channel_names = _decode_capability_channel_names(
        payload,
        layout["receive_channel_names_offset"],
        layout["receive_channel_count"],
        "Capability receive channel name",
    )

    return {
        "board_name": _decode_capability_utf8(payload, device_descriptor_offset, 8, "Capability device board name"),
        "tx_channel_names": transmit_channel_names,
        "tx_channel_count": layout["transmit_channel_count"],
        "rx_channel_names": receive_channel_names,
        "rx_channel_count": layout["receive_channel_count"],
        "model_id": payload[oem_descriptor_offset + 0x0C : oem_descriptor_offset + 0x14].hex(),
        "manufacturer_short": _decode_capability_utf8(
            payload, oem_descriptor_offset + 4, 8, "Capability OEM manufacturer short name"
        ),
        "manufacturer": _decode_capability_utf8(
            payload, oem_descriptor_offset + 0x14, 128, "Capability OEM manufacturer"
        ),
        "product_name": _decode_capability_utf8(
            payload, oem_descriptor_offset + 0x94, 128, "Capability OEM product name"
        ),
    }


def _extract_capability_14(payload, manufacturer_header=""):
    facts = {}
    board = _read_str(payload, 0x01D4, 32)
    facts["board_name"] = board.rstrip("'") if board else ""
    facts["model_id"] = _find_str(payload, [0x0244, 0x0268, 0x0254, 0x0278], 32)
    facts["tx_channel_names"] = _find_channel_names(payload, [0x060C, 0x0634, 0x061C, 0x0644], maximum_channels=8)
    facts["tx_channel_count"] = len(facts["tx_channel_names"])
    facts["rx_channel_names"] = _find_channel_names(payload, [0x080C, 0x0834, 0x081C, 0x0844], maximum_channels=8)
    facts["rx_channel_count"] = len(facts["rx_channel_names"])
    manufacturer, product = _find_oem_strings(payload, manufacturer_header, 0x0900)
    facts["manufacturer_short"] = manufacturer_header
    facts["manufacturer"] = manufacturer
    facts["product_name"] = product
    for version_offset in [0x0B80, 0x0BA8, 0x0B90, 0x0BB8]:
        if version_offset + 4 <= len(payload):
            version_bytes = payload[version_offset : version_offset + 4]
            if any(byte != 0 for byte in version_bytes) and all(byte < 100 for byte in version_bytes):
                facts["config_version"] = f"{version_bytes[0]}.{version_bytes[1]}.{version_bytes[2]}.{version_bytes[3]}"
                break
    for guid_offset in [0x0BB0, 0x0BD8, 0x0BC0, 0x0BE8]:
        guid_text = _read_str(payload, guid_offset, 40)
        if _is_printable(guid_text) and len(guid_text) > 8 and "-" in guid_text:
            facts["model_guid"] = guid_text
            break
    return facts
