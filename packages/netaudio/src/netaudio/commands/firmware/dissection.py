from __future__ import annotations

import struct
from pathlib import Path
from typing import Optional

import typer

from netaudio.commands.firmware.capabilities import _is_printable, _read_str
from netaudio.commands.firmware.constants import CRAMFS_MAGIC_BE, CRAMFS_MAGIC_LE, PARTITION_NAMES
from netaudio.commands.firmware.parser import _parse_sections, _section_table_layout


def firmware_hexdump(
    path: Path = typer.Argument(..., help=".dnt file."),
    offset: int = typer.Option(0, "--offset", help="Start offset in file."),
    length: int = typer.Option(256, "--length", "-l", help="Number of bytes to dump."),
    section: Optional[int] = typer.Option(
        None,
        "--section",
        "-s",
        help="Dump from this section index instead of file offset.",
    ),
):
    """Hex dump a region of a .dnt file."""
    with open(path, "rb") as f:
        data = f.read()

    if section is not None:
        if len(data) < 0x50 or data[:4] != b"AUDI":
            typer.echo(f"Not a .dnt file: {path}", err=True)
            raise typer.Exit(code=1)
        hdr_len = struct.unpack(">I", data[4:8])[0]
        sections = _parse_sections(data, hdr_len)
        if section < 0 or section >= len(sections):
            typer.echo(f"Section {section} out of range.", err=True)
            raise typer.Exit(code=1)
        sec = sections[section]
        data = data[sec["file_offset"] : sec["file_offset"] + sec["size"]]

    start = offset
    end = min(start + length, len(data))
    chunk = data[start:end]

    for i in range(0, len(chunk), 16):
        row = chunk[i : i + 16]
        hex_part = " ".join(f"{b:02x}" for b in row)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
        typer.echo(f"{start + i:08x}  {hex_part:<48}  {ascii_part}")


_RESET = "\033[0m"
_COLOR_SECTION = "\033[1;36m"
_COLOR_SEPARATOR = "\033[36m"
_COLOR_OFFSET = "\033[37m"
_COLOR_FIELD = "\033[1;37m"
_COLOR_TYPE = "\033[37m"
_COLOR_ASCII = "\033[90m"

_FIELD_PALETTE = [
    "\033[38;5;117m",
    "\033[38;5;186m",
    "\033[38;5;174m",
    "\033[38;5;150m",
    "\033[38;5;183m",
    "\033[38;5;216m",
    "\033[38;5;152m",
    "\033[38;5;223m",
]

HEADER_FIELDS = [
    (0x00, 4, "magic", "magic", "char[4]"),
    (0x04, 4, "u32", "header_length", "u32 BE"),
    (0x08, 4, "u32", "unknown_08", "u32 BE"),
    (0x0C, 4, "u32", "unknown_0c", "u32 BE"),
    (0x10, 4, "type", "device_type_id", "u32 BE"),
    (0x14, 4, "version", "firmware_version", "u8[4]"),
    (0x18, 4, "crc", "audi_crc32", "u32 BE"),
    (0x1C, 8, "string", "manufacturer", "char[8]"),
    (0x24, 4, "u32", "section_table_offset", "u32 BE"),
    (0x28, 4, "u32", "section_count", "u32 BE"),
    (0x2C, 4, "u32", "section_entry_size", "u32 BE"),
    (0x30, 4, "u32", "extra_entries_offset", "u32 BE"),
    (0x34, 4, "u32", "extra_entry_count", "u32 BE"),
    (0x38, 4, "u32", "extra_entry_size", "u32 BE"),
    (0x3C, 4, "crc", "header_checksum", "u32 BE"),
]

SECTION_ENTRY_FIELDS = [
    (0, 4, "type", "partition_id", "u32 BE"),
    (4, 4, "version", "section_version", "u8[4]"),
    (8, 4, "u32", "data_offset", "u32 BE"),
    (12, 4, "u32", "data_size", "u32 BE"),
]


def _format_value(chunk, kind, endian=">"):
    if kind == "magic":
        if all(32 <= byte_value < 127 for byte_value in chunk):
            return f'"{chunk.decode("ascii")}"'
        byte_order = "little" if endian == "<" else "big"
        value = int.from_bytes(chunk, byte_order)
        return f"0x{value:0{len(chunk) * 2}X}"
    if kind == "u32" and len(chunk) == 4:
        value = struct.unpack(f"{endian}I", chunk)[0]
        return f"{value:,} (0x{value:X})"
    if kind == "version" and len(chunk) == 4:
        return f"{chunk[0]}.{chunk[1]}.{chunk[2]}.{chunk[3]}"
    if kind == "type" and len(chunk) == 4:
        value = struct.unpack(f"{endian}I", chunk)[0]
        name = PARTITION_NAMES.get(value, "")
        return f"{value}" + (f" ({name})" if name else "")
    if kind == "type" and len(chunk) == 1:
        return f"{chunk[0]}"
    if kind == "crc" and len(chunk) == 4:
        value = struct.unpack(f"{endian}I", chunk)[0]
        return f"0x{value:08X}"
    if kind == "string":
        end = chunk.find(b"\x00")
        if end == -1:
            end = len(chunk)
        try:
            decoded_text = chunk[:end].decode("ascii")
        except UnicodeDecodeError:
            decoded_text = ""
        while decoded_text and ord(decoded_text[-1]) < 32:
            decoded_text = decoded_text[:-1]
        if decoded_text and _is_printable(decoded_text):
            return f'"{decoded_text}"'
    return ""


def _span_lines(data, file_offset, length, kind, name, dtype, endian=">", field_idx=0):
    field_bytes = data[file_offset : file_offset + length]
    display_value = _format_value(field_bytes, kind, endian)
    field_color = _FIELD_PALETTE[field_idx % len(_FIELD_PALETTE)]
    lines = []

    if kind == "string" and length > 8:
        null_pos = field_bytes.find(b"\x00")
        string_byte_count = null_pos if null_pos != -1 else length
    else:
        string_byte_count = length

    if length <= 8:
        hex_str = " ".join(f"{byte:02x}" for byte in field_bytes)
        lines.append(
            f"  {_COLOR_OFFSET}{file_offset:08x}{_RESET}  "
            f"{field_color}{hex_str:<24s}{_RESET} "
            f"{_COLOR_TYPE}{dtype:<10s}{_RESET} "
            f"{_COLOR_FIELD}{name:<24s}{_RESET}"
            f"{f' = {field_color}{display_value}{_RESET}' if display_value else ''}"
        )
    else:
        first_row = field_bytes[:8]
        colored_byte_count = min(8, string_byte_count)
        colored_hex = " ".join(f"{byte:02x}" for byte in first_row[:colored_byte_count])
        trailing_hex = " ".join(f"{byte:02x}" for byte in first_row[colored_byte_count:])
        combined_hex = f"{field_color}{colored_hex}{_RESET}"
        if trailing_hex:
            combined_hex += f" {_COLOR_TYPE}{trailing_hex}{_RESET}"
        raw_hex_width = len(" ".join(f"{byte:02x}" for byte in first_row))
        padding = 24 - raw_hex_width
        lines.append(
            f"  {_COLOR_OFFSET}{file_offset:08x}{_RESET}  "
            f"{combined_hex}{' ' * max(padding, 0)} "
            f"{_COLOR_TYPE}{dtype:<10s}{_RESET} "
            f"{_COLOR_FIELD}{name:<24s}{_RESET}"
            f"{f' = {field_color}{display_value}{_RESET}' if display_value else ''}"
        )
        for row_offset in range(8, length, 8):
            row_bytes = field_bytes[row_offset : row_offset + 8]
            colored_in_row = max(0, min(8, string_byte_count - row_offset))
            if colored_in_row > 0:
                colored_hex = " ".join(f"{byte:02x}" for byte in row_bytes[:colored_in_row])
                trailing_hex = " ".join(f"{byte:02x}" for byte in row_bytes[colored_in_row:])
                row_hex = f"{field_color}{colored_hex}{_RESET}"
                if trailing_hex:
                    row_hex += f" {_COLOR_TYPE}{trailing_hex}{_RESET}"
            else:
                row_hex = f"{_COLOR_TYPE}{' '.join(f'{byte:02x}' for byte in row_bytes)}{_RESET}"
            absolute_offset = file_offset + row_offset
            lines.append(f"  {_COLOR_OFFSET}{absolute_offset:08x}{_RESET}  {row_hex}")
    return lines


def _section_header(title):
    return f"{_COLOR_SECTION}{title}{_RESET}\n{_COLOR_SEPARATOR}{'─' * 90}{_RESET}"


def _dissect_uimage(data, abs_off, lines, fi):
    if abs_off + 64 > len(data):
        return fi
    magic = struct.unpack(">I", data[abs_off : abs_off + 4])[0]
    if magic != 0x27051956:
        return fi
    lines.append(_section_header(f"uImage Header (0x{abs_off:X})"))
    fields = [
        (0, 4, "magic", "magic", "u32 BE"),
        (4, 4, "crc", "header_crc32", "u32 BE"),
        (8, 4, "u32", "timestamp", "u32 BE"),
        (12, 4, "u32", "data_size", "u32 BE"),
        (16, 4, "u32", "load_address", "u32 BE"),
        (20, 4, "u32", "entry_point", "u32 BE"),
        (24, 4, "crc", "data_crc32", "u32 BE"),
        (28, 1, "type", "os", "u8"),
        (29, 1, "type", "arch", "u8"),
        (30, 1, "type", "image_type", "u8"),
        (31, 1, "type", "compression", "u8"),
        (32, 32, "string", "image_name", "char[32]"),
    ]
    for off, length, kind, name, dtype in fields:
        lines.extend(_span_lines(data, abs_off + off, length, kind, name, dtype, field_idx=fi))
        fi += 1
    return fi


def _dissect_cramfs_super(data, abs_off, lines, fi):
    if abs_off + 64 > len(data):
        return fi
    magic_bytes = data[abs_off : abs_off + 4]
    if magic_bytes == CRAMFS_MAGIC_LE:
        endian, endian_str = "<", "little-endian"
    elif magic_bytes == CRAMFS_MAGIC_BE:
        endian, endian_str = ">", "big-endian"
    else:
        return fi
    e = endian
    lines.append(_section_header(f"CramFS Superblock (0x{abs_off:X}, {endian_str})"))
    fields = [
        (0, 4, "magic", "magic", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (4, 4, "u32", "filesystem_size", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (8, 4, "u32", "flags", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (12, 4, "u32", "future", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (16, 16, "string", "signature", "char[16]"),
        (32, 4, "crc", "crc32", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (36, 4, "u32", "edition", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (40, 4, "u32", "blocks", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (44, 4, "u32", "files", f"u32 {('LE' if endian == '<' else 'BE')}"),
        (48, 12, "raw", "root_inode", "inode[12]"),
    ]
    for off, length, kind, name, dtype in fields:
        lines.extend(_span_lines(data, abs_off + off, length, kind, name, dtype, endian=e, field_idx=fi))
        fi += 1
    return fi


def _dissect_channel_range(data, sec_off, sec_size, base_off, stride, label, lines, fi):
    idx = 0
    off = base_off
    while off + stride <= sec_size:
        end = data[sec_off + off : sec_off + off + stride].find(b"\x00")
        if end <= 0:
            break
        try:
            s = data[sec_off + off : sec_off + off + end].decode("ascii")
        except UnicodeDecodeError:
            break
        if not _is_printable(s):
            break
        idx += 1
        name = f"{label}_{idx:02d}"
        show_len = min(stride, 32)
        lines.extend(
            _span_lines(
                data,
                sec_off + off,
                show_len,
                "string",
                name,
                f"char[{stride}]",
                field_idx=fi,
            )
        )
        fi += 1
        off += stride
    return fi


BROOKLYN2_CAPABILITY_HEAD_FIELDS = [
    (0x0380, 32, "board_name"),
]

BROOKLYN2_CAPABILITY_TAIL_FIELDS = [
    (0x8D5C, 32, "model_id"),
    (0x8D70, 16, "manufacturer_short"),
    (0x8D80, 64, "manufacturer"),
    (0x8E00, 128, "product_name"),
]

ULTIMO_CAPABILITY_HEAD_FIELDS = [
    ("board_name", 32, [0x01D4]),
    ("model_id", 32, [0x0244, 0x0268, 0x0254, 0x0278]),
]

ULTIMO_CAPABILITY_TAIL_FIELDS = [
    ("manufacturer", 32, [0x0A70, 0x0A98, 0x0A80, 0x0AA8]),
    ("product_name", 64, [0x0AF0, 0x0B18, 0x0B00, 0x0B28]),
]

ULTIMO_RECEIVER_CHANNEL_BASES = [0x080C, 0x0834, 0x081C, 0x0844]
ULTIMO_TRANSMITTER_CHANNEL_BASES = [0x060C, 0x0634, 0x061C, 0x0644]


def _dissect_string_field(data, sec_off, off, maxlen, show_len, name, lines, fi):
    lines.extend(_span_lines(data, sec_off + off, show_len, "string", name, f"char[{maxlen}]", field_idx=fi))
    return fi + 1


def _dissect_fixed_string_fields(data, sec_off, sec_size, fields, lines, fi):
    for off, maxlen, name in fields:
        if off + maxlen > sec_size:
            continue
        fi = _dissect_string_field(data, sec_off, off, maxlen, min(maxlen, 32), name, lines, fi)
    return fi


def _dissect_candidate_string_fields(data, sec_off, sec_size, cfg, fields, lines, fi):
    for name, maxlen, candidates in fields:
        for off in candidates:
            if off + maxlen <= sec_size and _is_printable_string(cfg, off, maxlen):
                fi = _dissect_string_field(data, sec_off, off, maxlen, maxlen, name, lines, fi)
                break
    return fi


def _is_printable_string(cfg, off, maxlen):
    text = _read_str(cfg, off, maxlen)
    return _is_printable(text) and len(text) >= 2


def _dissect_candidate_channel_range(data, sec_off, sec_size, cfg, bases, name, lines, fi):
    for base in bases:
        if _is_printable_string(cfg, base, 32):
            return _dissect_channel_range(data, sec_off, sec_size, base, 32, name, lines, fi)
    return fi


def _dissect_brooklyn2_capability(data, sec_off, sec_size, lines, fi):
    fi = _dissect_fixed_string_fields(data, sec_off, sec_size, BROOKLYN2_CAPABILITY_HEAD_FIELDS, lines, fi)
    fi = _dissect_channel_range(data, sec_off, sec_size, 0x053C, 32, "tx_channels", lines, fi)
    fi = _dissect_channel_range(data, sec_off, sec_size, 0x453C, 32, "rx_channels", lines, fi)
    return _dissect_fixed_string_fields(data, sec_off, sec_size, BROOKLYN2_CAPABILITY_TAIL_FIELDS, lines, fi)


def _dissect_ultimo_capability(data, sec_off, sec_size, lines, fi):
    cfg = data[sec_off : sec_off + sec_size]
    fi = _dissect_candidate_string_fields(data, sec_off, sec_size, cfg, ULTIMO_CAPABILITY_HEAD_FIELDS, lines, fi)
    fi = _dissect_candidate_channel_range(
        data, sec_off, sec_size, cfg, ULTIMO_TRANSMITTER_CHANNEL_BASES, "tx_channels", lines, fi
    )
    fi = _dissect_candidate_channel_range(
        data, sec_off, sec_size, cfg, ULTIMO_RECEIVER_CHANNEL_BASES, "rx_channels", lines, fi
    )
    return _dissect_candidate_string_fields(data, sec_off, sec_size, cfg, ULTIMO_CAPABILITY_TAIL_FIELDS, lines, fi)


def _dissect_capability_partition(data, sec_off, sec_size, partition_id, lines, fi):
    platform_name = "Brooklyn II" if partition_id == 9 else "Ultimo"
    partition_name = PARTITION_NAMES[partition_id]
    lines.append(_section_header(f"Capability Payload ({partition_name}, {platform_name}, 0x{sec_off:X})"))

    if partition_id == 9:
        return _dissect_brooklyn2_capability(data, sec_off, sec_size, lines, fi)
    if partition_id == 14:
        return _dissect_ultimo_capability(data, sec_off, sec_size, lines, fi)
    return fi


def _dissect_header(data):
    lines = []
    fi = 0

    lines.append(_section_header("AUDI Header"))
    for off, length, kind, name, dtype in HEADER_FIELDS:
        if off + length > len(data):
            break
        lines.extend(_span_lines(data, off, length, kind, name, dtype, field_idx=fi))
        fi += 1

    hdr_len = struct.unpack(">I", data[4:8])[0]
    section_table_offset, _, section_entry_size = _section_table_layout(data, hdr_len)
    parsed_sections = _parse_sections(data, hdr_len)
    lines.append("")
    lines.append(_section_header("Section Table"))

    sections = []
    for section_index, section in enumerate(parsed_sections):
        base = section_table_offset + section_index * section_entry_size
        lines.append(f"  {_COLOR_TYPE}── section {section_index} ({section['partition_name']}) ──{_RESET}")
        for field_off, field_len, kind, name, dtype in SECTION_ENTRY_FIELDS:
            lines.extend(_span_lines(data, base + field_off, field_len, kind, name, dtype, field_idx=fi))
            fi += 1
        sections.append(
            (
                section_index,
                section["partition_id"],
                section["file_offset"],
                section["size"],
            )
        )

    for sec_idx, s_type, s_off, s_size in sections:
        if s_off + s_size > len(data):
            continue

        if s_type == 1:
            lines.append("")
            sec_data = data[s_off : s_off + s_size]
            uimage_off = sec_data.find(b"\x27\x05\x19\x56")
            if uimage_off != -1:
                fi = _dissect_uimage(data, s_off + uimage_off, lines, fi)

            for magic_bytes in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
                pos = sec_data.find(magic_bytes)
                if pos != -1:
                    lines.append("")
                    fi = _dissect_cramfs_super(data, s_off + pos, lines, fi)
                    break

        elif s_type in (9, 14):
            lines.append("")
            fi = _dissect_capability_partition(data, s_off, s_size, s_type, lines, fi)

        elif s_type == 6:
            if data[s_off : s_off + 4] == b"AUDI":
                lines.append("")
                lines.append(_section_header(f"Nested AUDI (boot, 0x{s_off:X})"))
                nested_fields = [
                    (0, 4, "magic", "magic", "char[4]"),
                    (4, 4, "u32", "header_length", "u32 BE"),
                    (16, 4, "type", "device_type", "u32 BE"),
                    (20, 4, "version", "firmware_version", "u8[4]"),
                    (24, 4, "crc", "crc32", "u32 BE"),
                ]
                for off, length, kind, name, dtype in nested_fields:
                    lines.extend(_span_lines(data, s_off + off, length, kind, name, dtype, field_idx=fi))
                    fi += 1

    return "\n".join(lines)
