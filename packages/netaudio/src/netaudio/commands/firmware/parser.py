from __future__ import annotations

import ctypes
import errno
import json
import os
import struct
import sys
from pathlib import Path

from netaudio.commands.firmware.capabilities import _extract_capability_9, _extract_capability_14, _read_str
from netaudio.commands.firmware.constants import (
    CRAMFS_MAGIC_BE,
    CRAMFS_MAGIC_LE,
    DNT_PARSER_VERSION,
    GZIP_MAGIC,
    LINUX_CURRENT_WORKING_DIRECTORY_DESCRIPTOR,
    LINUX_RENAME_WITHOUT_REPLACEMENT,
    MACOS_RENAME_EXCLUSIVE,
    PARTITION_NAMES,
)
from netaudio.commands.firmware.validation import _validate_dnt_checksums


def _section_table_layout(data, header_length):
    if not 0x40 <= header_length <= len(data):
        raise ValueError(f"Invalid DNT header length: {header_length}")

    section_table_offset, section_count, section_entry_size = struct.unpack_from(">III", data, 0x24)
    if section_table_offset < 0x40:
        raise ValueError(f"Invalid DNT section table offset: {section_table_offset}")
    if section_entry_size < 16:
        raise ValueError(f"Invalid DNT section entry size: {section_entry_size}")

    section_table_end = section_table_offset + section_count * section_entry_size
    if section_table_end > header_length:
        raise ValueError(f"DNT section table extends past header: {section_table_end} > {header_length}")

    return section_table_offset, section_count, section_entry_size


def _parse_sections(data, header_length):
    section_table_offset, section_count, section_entry_size = _section_table_layout(data, header_length)
    sections = []
    for section_index in range(section_count):
        entry_offset = section_table_offset + section_index * section_entry_size
        partition_id, section_version, body_offset, section_size = struct.unpack_from(">IIII", data, entry_offset)
        file_offset = header_length + body_offset
        if file_offset > len(data) or section_size > len(data) - file_offset:
            raise ValueError(
                f"DNT section {section_index} extends past end of file: {file_offset + section_size} > {len(data)}"
            )
        sections.append(
            {
                "partition_id": partition_id,
                "partition_name": PARTITION_NAMES.get(partition_id, f"unknown-{partition_id}"),
                "version": (
                    f"{(section_version >> 24) & 0xFF}.{(section_version >> 16) & 0xFF}."
                    f"{(section_version >> 8) & 0xFF}.{section_version & 0xFF}"
                ),
                "body_offset": body_offset,
                "file_offset": file_offset,
                "size": section_size,
            }
        )
    return sections


def _detect_content(data, offset, size):
    if offset + 4 > len(data):
        return None
    blob = data[offset : offset + min(size, 16)]
    if blob[:4] == b"AUDI":
        return "nested-dnt"
    if blob[:2] == GZIP_MAGIC[:2]:
        return "gzip"
    if blob[:4] in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
        return "cramfs"
    return None


def _scan_for_embedded(data, section_file_offset, section_size):
    found = []
    blob = data[section_file_offset : section_file_offset + section_size]
    for magic, label, endian in [
        (CRAMFS_MAGIC_LE, "cramfs", "<"),
        (CRAMFS_MAGIC_BE, "cramfs", ">"),
    ]:
        idx = 0
        while True:
            pos = blob.find(magic, idx)
            if pos == -1:
                break
            file_offset = section_file_offset + pos
            filesystem_size = 0
            if pos + 8 <= len(blob):
                filesystem_size = struct.unpack(f"{endian}I", blob[pos + 4 : pos + 8])[0]
            found.append(
                {
                    "type": label,
                    "file_offset": file_offset,
                    "section_offset": pos,
                    "size": filesystem_size,
                    "endian": "big" if endian == ">" else "little",
                }
            )
            idx = pos + 1

    for needle, label in [(b"Linux version ", "linux-banner")]:
        pos = blob.find(needle)
        if pos != -1:
            banner = _read_str(blob, pos, 200)
            found.append(
                {
                    "type": label,
                    "file_offset": section_file_offset + pos,
                    "section_offset": pos,
                    "text": banner,
                }
            )
    return found


def _parse_dnt_bytes(data, source):
    if len(data) < 0x50 or data[:4] != b"AUDI":
        return None

    hdr_len = struct.unpack(">I", data[4:8])[0]
    if not 0x40 <= hdr_len <= len(data):
        raise ValueError(f"Invalid DNT header length: {hdr_len}")
    _validate_dnt_checksums(data, hdr_len)
    dev_type_id = struct.unpack(">I", data[16:20])[0]
    fw = data[20:24]

    result = {
        "dnt_parser_version": DNT_PARSER_VERSION,
        "file": str(source),
        "file_size": len(data),
        "header_length": hdr_len,
        "device_type_id": dev_type_id,
        "firmware_version": f"{fw[0]}.{fw[1]}.{fw[2]}.{fw[3]}",
        "manufacturer_header": _read_str(data, 0x1C, 8),
        "crc32": f"{struct.unpack_from('>I', data, 0x18)[0]:08x}",
        "header_crc32": f"{struct.unpack_from('>I', data, 0x3C)[0]:08x}",
    }

    sections = _parse_sections(data, hdr_len)
    result["sections"] = sections

    for sec in sections:
        cfg = data[sec["file_offset"] : sec["file_offset"] + sec["size"]]
        if sec["partition_id"] == 9:
            result.update(_extract_capability_9(cfg, data[0x1C:0x24]))
            result["capability_partition_id"] = 9
            break
        elif sec["partition_id"] == 14:
            result.update(_extract_capability_14(cfg, result["manufacturer_header"]))
            result["capability_partition_id"] = 14
            break

    if not result.get("product_name"):
        for needle in [b"Audinate Dante ", b"AVIO-"]:
            idx = data.find(needle)
            if idx != -1:
                result["product_name"] = _read_str(data, idx, 64)
                break

    return result


def parse_dnt(path):
    source = Path(path)
    return _parse_dnt_bytes(source.read_bytes(), source)


def _load_resume_results(path):
    with open(path) as file_handle:
        results = json.load(file_handle)
    if not isinstance(results, list):
        raise ValueError("Firmware resume file must contain a JSON list")
    for result in results:
        if not isinstance(result, dict) or result.get("dnt_parser_version") != DNT_PARSER_VERSION:
            raise ValueError(f"Firmware resume file is incompatible with DNT parser version {DNT_PARSER_VERSION}")
    return results


def _collect_dnt_files(paths):
    dnt_files = []
    for p in paths:
        p = Path(p)
        if p.is_file() and p.suffix == ".dnt":
            dnt_files.append(p)
        elif p.is_dir():
            dnt_files.extend(sorted(p.rglob("*.dnt")))
    return dnt_files


def _synchronize_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    directory_descriptor = os.open(path, flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _write_synchronized_file(path: Path, data: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_publish_directory_without_replacement(
    temporary_directory: Path,
    output_directory: Path,
) -> None:
    if sys.platform == "linux":
        library = ctypes.CDLL(None, use_errno=True)
        operation = getattr(library, "renameat2", None)
        if operation is None:
            raise RuntimeError("Atomic no-replace directory publication is unavailable on this Linux libc")
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        operation.restype = ctypes.c_int
        result = operation(
            LINUX_CURRENT_WORKING_DIRECTORY_DESCRIPTOR,
            os.fsencode(temporary_directory),
            LINUX_CURRENT_WORKING_DIRECTORY_DESCRIPTOR,
            os.fsencode(output_directory),
            LINUX_RENAME_WITHOUT_REPLACEMENT,
        )
        error_number = ctypes.get_errno()
    elif sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        operation = getattr(library, "renamex_np", None)
        if operation is None:
            raise RuntimeError("Atomic no-replace directory publication is unavailable on this macOS libc")
        operation.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(
            os.fsencode(temporary_directory),
            os.fsencode(output_directory),
            MACOS_RENAME_EXCLUSIVE,
        )
        error_number = ctypes.get_errno()
    else:
        raise RuntimeError(f"Atomic no-replace directory publication is unsupported on {sys.platform}")
    if result == 0:
        return
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, os.strerror(error_number), output_directory)
    raise OSError(error_number, os.strerror(error_number), output_directory)


def _publish_output_directory_without_replacement(
    temporary_directory: Path,
    output_directory: Path,
) -> None:
    _synchronize_directory(temporary_directory)
    try:
        _atomic_publish_directory_without_replacement(temporary_directory, output_directory)
    except FileExistsError as error:
        raise ValueError(f"Output directory already exists: {output_directory}") from error
    _synchronize_directory(output_directory.parent)
