from __future__ import annotations

import struct
from dataclasses import dataclass, field
import zlib

from netaudio.commands.firmware.constants import (
    BROOKLYN2_FLASH_PARTITIONS,
    BROOKLYN2_FLASH_SIZE,
    UIMAGE_HEADER_SIZE,
)


def _crc32_with_zeroed_field(data, field_offset):
    checksum = zlib.crc32(data[:field_offset])
    checksum = zlib.crc32(bytes(4), checksum)
    return zlib.crc32(data[field_offset + 4 :], checksum)


def _validate_dnt_checksums(data, header_length):
    stored_checksum = struct.unpack_from(">I", data, 0x18)[0]
    calculated_checksum = _crc32_with_zeroed_field(data, 0x18)
    if stored_checksum != calculated_checksum:
        raise ValueError(
            f"Invalid DNT checksum: stored 0x{stored_checksum:08x}, calculated 0x{calculated_checksum:08x}"
        )

    stored_header_checksum = struct.unpack_from(">I", data, 0x3C)[0]
    header = bytearray(data[:header_length])
    header[0x18:0x1C] = bytes(4)
    header[0x3C:0x40] = bytes(4)
    calculated_header_checksum = zlib.crc32(header)
    if stored_header_checksum != calculated_header_checksum:
        raise ValueError(
            "Invalid DNT header checksum: "
            f"stored 0x{stored_header_checksum:08x}, calculated 0x{calculated_header_checksum:08x}"
        )


def _validate_uimage(payload, expected_image_type, expected_load_address):
    if len(payload) < UIMAGE_HEADER_SIZE:
        raise ValueError("uImage payload is shorter than its header")
    if struct.unpack_from(">I", payload, 0)[0] != 0x27051956:
        raise ValueError("Invalid uImage magic")

    stored_header_checksum = struct.unpack_from(">I", payload, 4)[0]
    header = bytearray(payload[:UIMAGE_HEADER_SIZE])
    header[4:8] = bytes(4)
    calculated_header_checksum = zlib.crc32(header)
    if stored_header_checksum != calculated_header_checksum:
        raise ValueError(
            "Invalid uImage header checksum: "
            f"stored 0x{stored_header_checksum:08x}, calculated 0x{calculated_header_checksum:08x}"
        )

    data_size, load_address, entry_point, stored_data_checksum = struct.unpack_from(">IIII", payload, 12)
    if len(payload) != UIMAGE_HEADER_SIZE + data_size:
        raise ValueError(
            f"Invalid uImage size: header declares {data_size} data bytes, file has {len(payload) - UIMAGE_HEADER_SIZE}"
        )
    calculated_data_checksum = zlib.crc32(payload[UIMAGE_HEADER_SIZE:])
    if stored_data_checksum != calculated_data_checksum:
        raise ValueError(
            "Invalid uImage data checksum: "
            f"stored 0x{stored_data_checksum:08x}, calculated 0x{calculated_data_checksum:08x}"
        )
    if payload[28] != 5 or payload[29] != 14 or payload[30] != expected_image_type or payload[31] != 0:
        raise ValueError(
            "Unexpected uImage platform fields: "
            f"os={payload[28]}, architecture={payload[29]}, type={payload[30]}, compression={payload[31]}"
        )
    if load_address != expected_load_address or entry_point != expected_load_address:
        raise ValueError(
            "Unexpected uImage addresses: "
            f"load=0x{load_address:08x}, entry=0x{entry_point:08x}, expected=0x{expected_load_address:08x}"
        )

    image_name = payload[32:64].split(b"\x00", 1)[0].decode("ascii")
    return {
        "data_size": data_size,
        "load_address": load_address,
        "entry_point": entry_point,
        "header_crc32": f"{stored_header_checksum:08x}",
        "data_crc32": f"{stored_data_checksum:08x}",
        "image_name": image_name,
    }


def _validate_capability_payload(payload):
    if len(payload) < 20 or payload[:8] != b"Audinate":
        raise ValueError("Invalid Audinate capability header")
    declared_size, header_size = struct.unpack_from(">HH", payload, 10)
    if declared_size != len(payload):
        raise ValueError(f"Invalid capability size: header declares {declared_size}, file has {len(payload)}")
    if header_size != 20:
        raise ValueError(f"Unexpected capability header size: {header_size}")
    stored_checksum = struct.unpack_from(">I", payload, 16)[0]
    calculated_checksum = _crc32_with_zeroed_field(payload, 16)
    if stored_checksum != calculated_checksum:
        raise ValueError(
            f"Invalid capability checksum: stored 0x{stored_checksum:08x}, calculated 0x{calculated_checksum:08x}"
        )
    return {
        "declared_size": declared_size,
        "header_size": header_size,
        "device_descriptor_offset": struct.unpack_from(">H", payload, 14)[0],
        "crc32": f"{stored_checksum:08x}",
    }


def _validate_cramfs_payload(payload):
    if len(payload) < 0x4C or struct.unpack_from("<I", payload, 0)[0] != 0x28CD3D45:
        raise ValueError("Invalid little-endian CramFS header")
    declared_size, flags = struct.unpack_from("<II", payload, 4)
    if declared_size != len(payload):
        raise ValueError(f"Invalid CramFS size: header declares {declared_size}, file has {len(payload)}")
    if not flags & 1 or flags & ~0x7FF:
        raise ValueError(f"Unsupported CramFS flags: 0x{flags:x}")
    file_count = struct.unpack_from("<I", payload, 0x2C)[0]
    if file_count == 0:
        raise ValueError("CramFS file count is zero")
    stored_checksum = struct.unpack_from("<I", payload, 0x20)[0]
    calculated_checksum = _crc32_with_zeroed_field(payload, 0x20)
    if stored_checksum != calculated_checksum:
        raise ValueError(
            f"Invalid CramFS checksum: stored 0x{stored_checksum:08x}, calculated 0x{calculated_checksum:08x}"
        )
    return {
        "declared_size": declared_size,
        "flags": flags,
        "file_count": file_count,
        "crc32": f"{stored_checksum:08x}",
    }


def _validate_brooklyn2_kernel_payload(payload):
    kernel_data = payload[UIMAGE_HEADER_SIZE:]
    rootfs_offset = 0xF8000
    if len(kernel_data) < rootfs_offset + 0x4C + 4:
        raise ValueError("Brooklyn II kernel payload is too short for its embedded root filesystem")
    rootfs_size = struct.unpack_from("<I", kernel_data, rootfs_offset + 4)[0]
    rootfs_end = rootfs_offset + rootfs_size
    if rootfs_end + 4 != len(kernel_data):
        raise ValueError("Brooklyn II kernel payload does not end with a four-byte checksum after its root filesystem")
    rootfs_validation = _validate_cramfs_payload(kernel_data[rootfs_offset:rootfs_end])
    stored_checksum = struct.unpack_from(">I", kernel_data, rootfs_end)[0]
    calculated_checksum = sum(kernel_data[:rootfs_end])
    calculated_checksum = (calculated_checksum & 0xFFFF) + (calculated_checksum >> 16)
    calculated_checksum = (calculated_checksum & 0xFFFF) + (calculated_checksum >> 16)
    if stored_checksum != calculated_checksum:
        raise ValueError(
            "Invalid Brooklyn II kernel payload checksum: "
            f"stored 0x{stored_checksum:08x}, calculated 0x{calculated_checksum:08x}"
        )
    return {
        "rootfs_data_offset": rootfs_offset,
        "rootfs_partition_offset": UIMAGE_HEADER_SIZE + rootfs_offset,
        "rootfs": rootfs_validation,
        "sysv_checksum": f"{stored_checksum:08x}",
    }


@dataclass
class Spartan6BitstreamScan:
    commands: list = field(default_factory=list)
    explicit_checksums: list = field(default_factory=list)
    idcodes: list = field(default_factory=list)
    packet_count: int = 0
    type_two_payloads: list = field(default_factory=list)
    word_offset: int = 20


def _scan_spartan6_type_one_packet(payload, header, packet_offset, scan):
    operation = (header >> 11) & 3
    register = (header >> 5) & 0x3F
    word_count = header & 0x1F
    data_end = scan.word_offset + word_count * 2
    if data_end > len(payload):
        raise ValueError(f"Truncated Spartan-6 type-1 packet at 0x{packet_offset:x}")
    data_words = struct.unpack_from(f">{word_count}H", payload, scan.word_offset) if word_count else ()
    scan.word_offset = data_end
    if operation != 2:
        return
    if register == 14 and word_count == 2:
        scan.idcodes.append((data_words[0] << 16) | data_words[1])
    elif register == 0 and word_count == 2:
        scan.explicit_checksums.append((data_words[0] << 16) | data_words[1])
    elif register == 5 and word_count == 1:
        scan.commands.append(data_words[0])


def _scan_spartan6_type_two_packet(payload, header, packet_offset, scan):
    operation = (header >> 11) & 3
    if header & 0x1F:
        raise ValueError(f"Invalid Spartan-6 type-2 header at 0x{packet_offset:x}")
    if scan.word_offset + 4 > len(payload):
        raise ValueError(f"Truncated Spartan-6 type-2 count at 0x{packet_offset:x}")
    count_high, count_low = struct.unpack_from(">HH", payload, scan.word_offset)
    scan.word_offset += 4
    word_count = (count_high << 16) | count_low
    data_end = scan.word_offset + word_count * 2
    if data_end > len(payload):
        raise ValueError(f"Truncated Spartan-6 type-2 payload at 0x{packet_offset:x}")
    scan.word_offset = data_end
    if operation != 2:
        return
    if scan.word_offset + 4 > len(payload):
        raise ValueError(f"Missing Spartan-6 automatic CRC at 0x{packet_offset:x}")
    automatic_checksum = struct.unpack_from(">I", payload, scan.word_offset)[0]
    scan.word_offset += 4
    scan.type_two_payloads.append(
        {
            "packet_offset": packet_offset,
            "word_count": word_count,
            "automatic_crc32": f"{automatic_checksum:08x}",
        }
    )


def _scan_spartan6_bitstream(payload):
    scan = Spartan6BitstreamScan()
    while scan.word_offset < len(payload):
        packet_offset = scan.word_offset
        header = struct.unpack_from(">H", payload, scan.word_offset)[0]
        scan.word_offset += 2
        packet_type = header >> 13
        scan.packet_count += 1
        if packet_type == 1:
            _scan_spartan6_type_one_packet(payload, header, packet_offset, scan)
        elif packet_type == 2:
            _scan_spartan6_type_two_packet(payload, header, packet_offset, scan)
        else:
            raise ValueError(f"Unsupported Spartan-6 packet type {packet_type} at 0x{packet_offset:x}")
    return scan


def _validate_fpga_payload(payload):
    if len(payload) < 20 or payload[:16] != bytes([0xFF]) * 16 or payload[16:20] != b"\xaa\x99\x55\x66":
        raise ValueError("Invalid Spartan-6 bitstream prefix")
    if len(payload) % 2:
        raise ValueError("Spartan-6 bitstream has an odd byte count")

    scan = _scan_spartan6_bitstream(payload)
    if not scan.idcodes or any(idcode != 0x04004093 for idcode in scan.idcodes):
        raise ValueError(f"Unexpected Spartan-6 IDCODE values: {[f'{idcode:08x}' for idcode in scan.idcodes]}")
    if not scan.type_two_payloads:
        raise ValueError("Spartan-6 bitstream contains no type-2 FDRI payload")
    if not scan.explicit_checksums:
        raise ValueError("Spartan-6 bitstream contains no explicit CRC write")
    if not scan.commands or scan.commands[-1] != 13:
        raise ValueError("Spartan-6 bitstream does not end its command sequence with DESYNC")

    return {
        "sync_word_offset": 16,
        "packet_count": scan.packet_count,
        "idcodes": [f"{idcode:08x}" for idcode in scan.idcodes],
        "type_two_payloads": scan.type_two_payloads,
        "explicit_crc32_writes": [f"{checksum:08x}" for checksum in scan.explicit_checksums],
        "commands": scan.commands,
    }


def _validate_brooklyn2_flash_layout():
    expected_offset = 0
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS:
        if partition_offset != expected_offset:
            raise RuntimeError(f"Brooklyn II flash layout is discontinuous before {partition_name}")
        expected_offset += partition_size
    if expected_offset != BROOKLYN2_FLASH_SIZE:
        raise RuntimeError(f"Brooklyn II flash layout ends at 0x{expected_offset:x}")
