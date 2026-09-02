from __future__ import annotations

import struct

from netaudio.commands.firmware.constants import (
    CAPABILITY_9_CHANNEL_NAME_SIZE,
    CAPABILITY_9_DEVICE_DESCRIPTOR_SIZE,
    CAPABILITY_9_OEM_DESCRIPTOR_SIZE,
)
from netaudio.commands.firmware.validation import _validate_capability_payload


def _read_str(data, off, maxlen=128):
    if off >= len(data):
        return ""
    end = data.find(b"\x00", off, off + maxlen)
    if end == -1:
        end = off + maxlen
    s = data[off:end]
    try:
        decoded = s.decode("ascii")
    except UnicodeDecodeError:
        return ""
    while decoded and ord(decoded[-1]) < 32:
        decoded = decoded[:-1]
    return decoded


def _is_printable(s):
    return bool(s) and all(32 <= ord(c) < 127 for c in s)


def _find_str(cfg, candidates, maxlen=128):
    for off in candidates:
        if off < len(cfg):
            s = _read_str(cfg, off, maxlen)
            s = s.rstrip("\x01\x02\x03\x04\x05\x06\x07\x08")
            if _is_printable(s) and len(s) >= 2:
                return s
    return ""


def _find_channel_names(cfg, base_candidates, max_channels=64, stride=32):
    for base in base_candidates:
        first = _read_str(cfg, base, stride)
        if not first:
            continue
        names = []
        for i in range(max_channels):
            off = base + i * stride
            if off >= len(cfg):
                break
            s = _read_str(cfg, off, stride)
            if s:
                names.append(s)
            else:
                break
        if names:
            return names
    return []


def _find_oem_strings(cfg, mfg_header, search_start):
    """Search for manufacturer_header in config, then read manufacturer and
    product_name at fixed relative offsets (+16 and +144 respectively)."""
    if not mfg_header or len(mfg_header) < 2:
        return "", ""
    needle = mfg_header.encode("ascii")
    idx = cfg.find(needle + b"\x00", search_start)
    if idx == -1:
        idx = cfg.find(needle, search_start)
    if idx == -1:
        return "", ""
    mfg = _read_str(cfg, idx + 16, 64)
    if not _is_printable(mfg):
        mfg = ""
    product = _read_str(cfg, idx + 144, 128)
    if not _is_printable(product):
        product = ""
    return mfg, product


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


def _extract_capability_9(cfg, manufacturer_header):
    if len(manufacturer_header) != 8:
        raise ValueError(f"DNT manufacturer header must contain 8 bytes, received {len(manufacturer_header)}")

    layout = _parse_capability_9_layout(cfg)
    device_descriptor_offset = layout["device_descriptor_offset"]
    oem_descriptor_offset = layout["oem_descriptor_offset"]
    manufacturer_short_bytes = cfg[oem_descriptor_offset + 4 : oem_descriptor_offset + 12]
    if manufacturer_short_bytes != manufacturer_header:
        raise ValueError("Capability OEM manufacturer short name does not match the DNT manufacturer header")

    transmit_channel_names = _decode_capability_channel_names(
        cfg,
        layout["transmit_channel_names_offset"],
        layout["transmit_channel_count"],
        "Capability transmit channel name",
    )
    receive_channel_names = _decode_capability_channel_names(
        cfg,
        layout["receive_channel_names_offset"],
        layout["receive_channel_count"],
        "Capability receive channel name",
    )

    return {
        "board_name": _decode_capability_utf8(cfg, device_descriptor_offset, 8, "Capability device board name"),
        "tx_channel_names": transmit_channel_names,
        "tx_channel_count": layout["transmit_channel_count"],
        "rx_channel_names": receive_channel_names,
        "rx_channel_count": layout["receive_channel_count"],
        "model_id": cfg[oem_descriptor_offset + 0x0C : oem_descriptor_offset + 0x14].hex(),
        "manufacturer_short": _decode_capability_utf8(
            cfg, oem_descriptor_offset + 4, 8, "Capability OEM manufacturer short name"
        ),
        "manufacturer": _decode_capability_utf8(cfg, oem_descriptor_offset + 0x14, 128, "Capability OEM manufacturer"),
        "product_name": _decode_capability_utf8(cfg, oem_descriptor_offset + 0x94, 128, "Capability OEM product name"),
    }


def _extract_capability_14(cfg, mfg_header=""):
    facts = {}
    board = _read_str(cfg, 0x01D4, 32)
    facts["board_name"] = board.rstrip("'") if board else ""
    facts["model_id"] = _find_str(cfg, [0x0244, 0x0268, 0x0254, 0x0278], 32)
    facts["tx_channel_names"] = _find_channel_names(cfg, [0x060C, 0x0634, 0x061C, 0x0644], max_channels=8)
    facts["tx_channel_count"] = len(facts["tx_channel_names"])
    facts["rx_channel_names"] = _find_channel_names(cfg, [0x080C, 0x0834, 0x081C, 0x0844], max_channels=8)
    facts["rx_channel_count"] = len(facts["rx_channel_names"])
    mfg, product = _find_oem_strings(cfg, mfg_header, 0x0900)
    facts["manufacturer_short"] = mfg_header
    facts["manufacturer"] = mfg
    facts["product_name"] = product
    for off in [0x0B80, 0x0BA8, 0x0B90, 0x0BB8]:
        if off + 4 <= len(cfg):
            v = cfg[off : off + 4]
            if any(b != 0 for b in v) and all(b < 100 for b in v):
                facts["config_version"] = f"{v[0]}.{v[1]}.{v[2]}.{v[3]}"
                break
    for off in [0x0BB0, 0x0BD8, 0x0BC0, 0x0BE8]:
        s = _read_str(cfg, off, 40)
        if _is_printable(s) and len(s) > 8 and "-" in s:
            facts["model_guid"] = s
            break
    return facts
