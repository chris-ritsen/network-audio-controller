from __future__ import annotations

import json
import os
import stat
import struct
import sys
import zlib
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Optional

import typer

app = typer.Typer(help="Analyze Dante firmware (.dnt) files.", no_args_is_help=True)

SECTION_TYPES = {
    1: "firmware",
    2: "secondary",
    6: "bootloader",
    9: "config-brooklyn",
    11: "unknown-11",
    14: "config-ultimo",
}

CRAMFS_MAGIC_LE = b"\x45\x3d\xcd\x28"
CRAMFS_MAGIC_BE = b"\x28\xcd\x3d\x45"
GZIP_MAGIC = b"\x1f\x8b\x08"


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


def _extract_config_9(cfg, mfg_header=""):
    facts = {}
    facts["board_name"] = _find_str(cfg, [0x0380, 0x0388, 0x0378, 0x0390], 32)
    facts["tx_channel_names"] = _find_channel_names(cfg, [0x053C, 0x052C, 0x054C, 0x0520], max_channels=64)
    facts["tx_channel_count"] = len(facts["tx_channel_names"])
    facts["rx_channel_names"] = _find_channel_names(cfg, [0x453C, 0x452C, 0x454C, 0x4520], max_channels=64)
    facts["rx_channel_count"] = len(facts["rx_channel_names"])
    facts["model_id"] = _find_str(cfg, [0x8D5C, 0x8D4C, 0x8D6C, 0x8D3C], 32)
    mfg, product = _find_oem_strings(cfg, mfg_header, 0x8C00)
    facts["manufacturer_short"] = mfg_header
    facts["manufacturer"] = mfg
    facts["product_name"] = product
    return facts


def _extract_config_14(cfg, mfg_header=""):
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


def _parse_sections(data, hdr_len):
    max_entries = (hdr_len - 0x40) // 16
    sections = []
    for i in range(min(max_entries, 16)):
        off = 0x40 + i * 16
        if off + 16 > len(data):
            break
        s_type, s_ver, s_off, s_size = struct.unpack(">IIII", data[off : off + 16])
        if s_type > 0xFF:
            break
        if s_type == 0 and s_ver == 0 and s_off == 0 and s_size == 0:
            continue
        sections.append(
            {
                "type": s_type,
                "type_name": SECTION_TYPES.get(s_type, f"unknown-{s_type}"),
                "version": f"{(s_ver >> 24) & 0xFF}.{(s_ver >> 16) & 0xFF}.{(s_ver >> 8) & 0xFF}.{s_ver & 0xFF}",
                "offset": s_off,
                "size": s_size,
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


def _scan_for_embedded(data, sec_offset, sec_size):
    found = []
    blob = data[sec_offset : sec_offset + sec_size]
    for magic, label, endian in [
        (CRAMFS_MAGIC_LE, "cramfs", "<"),
        (CRAMFS_MAGIC_BE, "cramfs", ">"),
    ]:
        idx = 0
        while True:
            pos = blob.find(magic, idx)
            if pos == -1:
                break
            abs_off = sec_offset + pos
            fs_size = 0
            if pos + 8 <= len(blob):
                fs_size = struct.unpack(f"{endian}I", blob[pos + 4 : pos + 8])[0]
            found.append(
                {
                    "type": label,
                    "offset": abs_off,
                    "section_offset": pos,
                    "size": fs_size,
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
                    "offset": sec_offset + pos,
                    "section_offset": pos,
                    "text": banner,
                }
            )
    return found


def parse_dnt(path):
    with open(path, "rb") as f:
        data = f.read()

    if len(data) < 0x50 or data[:4] != b"AUDI":
        return None

    hdr_len = struct.unpack(">I", data[4:8])[0]
    dev_type_id = struct.unpack(">I", data[16:20])[0]
    fw = data[20:24]

    result = {
        "file": str(path),
        "file_size": len(data),
        "header_length": hdr_len,
        "device_type_id": dev_type_id,
        "firmware_version": f"{fw[0]}.{fw[1]}.{fw[2]}.{fw[3]}",
        "manufacturer_header": _read_str(data, 0x1C, 8),
    }

    sections = _parse_sections(data, hdr_len)
    result["sections"] = sections

    for sec in sections:
        if sec["offset"] + sec["size"] > len(data):
            continue
        cfg = data[sec["offset"] : sec["offset"] + sec["size"]]
        if sec["type"] == 9:
            result.update(_extract_config_9(cfg, result["manufacturer_header"]))
            result["config_section_type"] = 9
            break
        elif sec["type"] == 14:
            result.update(_extract_config_14(cfg, result["manufacturer_header"]))
            result["config_section_type"] = 14
            break

    if not result.get("product_name"):
        for needle in [b"Audinate Dante ", b"AVIO-"]:
            idx = data.find(needle)
            if idx != -1:
                result["product_name"] = _read_str(data, idx, 64)
                result["config_section_type"] = -1
                break

    return result


def _collect_dnt_files(paths):
    dnt_files = []
    for p in paths:
        p = Path(p)
        if p.is_file() and p.suffix == ".dnt":
            dnt_files.append(p)
        elif p.is_dir():
            dnt_files.extend(sorted(p.rglob("*.dnt")))
    return dnt_files


@app.command("info")
def firmware_info(
    paths: list[Path] = typer.Argument(..., help=".dnt files or directories to scan."),
    save: Optional[Path] = typer.Option(None, "--save", help="Save JSON results to file."),
    resume: Optional[Path] = typer.Option(None, "--resume", help="Skip files already in this JSON output."),
):
    """Extract product identity facts from .dnt firmware files."""
    from netaudio._common import output_table
    from netaudio.cli import OutputFormat, state

    dnt_files = _collect_dnt_files(paths)
    if not dnt_files:
        typer.echo("No .dnt files found.", err=True)
        raise typer.Exit(code=1)

    already_done = set()
    results = []
    if resume and resume.exists():
        with open(resume) as f:
            results = json.load(f)
        already_done = {r["file"] for r in results}
        typer.echo(f"Resuming: {len(already_done)} already processed", err=True)

    total = len(dnt_files)
    processed = 0
    errors = 0

    for i, path in enumerate(dnt_files):
        if str(path) in already_done:
            continue

        if (i % 10 == 0 or i == total - 1) and state.output_format != OutputFormat.json:
            print(f"\r[{i + 1}/{total}] {path.name[:50]:<50}", end="", file=sys.stderr)

        try:
            result = parse_dnt(path)
            if result:
                results.append(result)
                processed += 1
        except Exception as e:
            errors += 1
            typer.echo(f"\n  ERROR: {path}: {e}", err=True)

    if state.output_format != OutputFormat.json:
        msg = f"\n{processed} extracted"
        if errors:
            msg += f", {errors} errors"
        typer.echo(msg, err=True)

    if save:
        with open(save, "w") as f:
            json.dump(results, f, indent=2)
        typer.echo(f"Saved to {save}", err=True)

    headers = ["dev_id", "fw_ver", "mfg", "product", "model_id", "tx", "rx"]
    rows = []
    for r in results:
        rows.append(
            [
                str(r.get("device_type_id", "")),
                r.get("firmware_version", ""),
                r.get("manufacturer_header", ""),
                (r.get("product_name", "") or "")[:30],
                (r.get("model_id", "") or "")[:20],
                str(r.get("tx_channel_count", "")),
                str(r.get("rx_channel_count", "")),
            ]
        )

    output_table(headers, rows, json_data=results)


def _init_db(db_path):
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS firmware (
            sha256          TEXT PRIMARY KEY,
            file_size       INTEGER,
            device_type_id  INTEGER,
            firmware_version TEXT,
            manufacturer_header TEXT,
            config_section_type INTEGER,
            board_name      TEXT,
            model_id        TEXT,
            manufacturer_short TEXT,
            manufacturer    TEXT,
            product_name    TEXT,
            tx_channel_count INTEGER,
            rx_channel_count INTEGER,
            tx_channel_names TEXT,
            rx_channel_names TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sections (
            sha256      TEXT NOT NULL,
            idx         INTEGER NOT NULL,
            type        INTEGER,
            type_name   TEXT,
            version     TEXT,
            offset      INTEGER,
            size        INTEGER,
            PRIMARY KEY (sha256, idx),
            FOREIGN KEY (sha256) REFERENCES firmware(sha256)
        )
    """)
    conn.commit()
    return conn


@app.command("db")
def firmware_db(
    paths: list[Path] = typer.Argument(..., help=".dnt files or directories to scan."),
    db: Path = typer.Option("firmware.db", "--db", help="SQLite database path."),
):
    """Parse .dnt firmware files into a SQLite database."""
    import hashlib
    import sqlite3

    dnt_files = _collect_dnt_files(paths)
    if not dnt_files:
        typer.echo("No .dnt files found.", err=True)
        raise typer.Exit(code=1)

    conn = _init_db(db)

    existing = {row[0] for row in conn.execute("SELECT sha256 FROM firmware").fetchall()}
    typer.echo(f"{len(existing)} already in db, {len(dnt_files)} files to scan", err=True)

    total = len(dnt_files)
    inserted = 0
    skipped = 0
    errors = 0

    for i, path in enumerate(dnt_files):
        if i % 10 == 0 or i == total - 1:
            print(f"\r[{i + 1}/{total}] {path.name[:50]:<50}", end="", file=sys.stderr)

        try:
            with open(path, "rb") as f:
                raw = f.read()
            sha = hashlib.sha256(raw).hexdigest()

            if sha in existing:
                skipped += 1
                continue

            result = parse_dnt(path)
            if not result:
                errors += 1
                continue

            conn.execute(
                """INSERT OR REPLACE INTO firmware
                   (sha256, file_size, device_type_id, firmware_version,
                    manufacturer_header, config_section_type, board_name,
                    model_id, manufacturer_short, manufacturer, product_name,
                    tx_channel_count, rx_channel_count,
                    tx_channel_names, rx_channel_names)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    sha,
                    result.get("file_size"),
                    result.get("device_type_id"),
                    result.get("firmware_version"),
                    result.get("manufacturer_header"),
                    result.get("config_section_type"),
                    result.get("board_name"),
                    result.get("model_id"),
                    result.get("manufacturer_short"),
                    result.get("manufacturer"),
                    result.get("product_name"),
                    result.get("tx_channel_count", 0),
                    result.get("rx_channel_count", 0),
                    json.dumps(result.get("tx_channel_names", [])),
                    json.dumps(result.get("rx_channel_names", [])),
                ),
            )

            sections = result.get("sections", [])
            for idx, sec in enumerate(sections):
                conn.execute(
                    """INSERT OR REPLACE INTO sections
                       (sha256, idx, type, type_name, version, offset, size)
                       VALUES (?,?,?,?,?,?,?)""",
                    (sha, idx, sec["type"], sec["type_name"], sec["version"], sec["offset"], sec["size"]),
                )

            existing.add(sha)
            inserted += 1

            if inserted % 50 == 0:
                conn.commit()

        except Exception as e:
            errors += 1
            typer.echo(f"\n  ERROR: {path}: {e}", err=True)

    conn.commit()
    conn.close()

    msg = f"\n{inserted} added, {skipped} already present"
    if errors:
        msg += f", {errors} errors"
    typer.echo(msg, err=True)
    typer.echo(f"Database: {db}", err=True)


@app.command("sections")
def firmware_sections(
    paths: list[Path] = typer.Argument(..., help=".dnt files to inspect."),
    scan: bool = typer.Option(False, "--scan", help="Scan section contents for embedded filesystems."),
):
    """Show the section table of .dnt files."""
    from netaudio._common import output_table
    from netaudio.cli import state

    dnt_files = _collect_dnt_files(paths)
    if not dnt_files:
        typer.echo("No .dnt files found.", err=True)
        raise typer.Exit(code=1)

    for file_path in dnt_files:
        with open(file_path, "rb") as file_handle:
            data = file_handle.read()

        if len(data) < 0x50 or data[:4] != b"AUDI":
            typer.echo(f"Not a .dnt file: {file_path}", err=True)
            continue

        if state.dissect:
            if len(dnt_files) > 1:
                typer.echo(f"\n{'═' * 90}")
                typer.echo(f"  {file_path}")
                typer.echo(f"{'═' * 90}\n")
            typer.echo(_dissect_header(data))
            continue

        hdr_len = struct.unpack(">I", data[4:8])[0]
        dev_type_id = struct.unpack(">I", data[16:20])[0]
        firmware_version = data[20:24]

        typer.echo(f"File: {file_path} ({len(data):,} bytes)", err=True)
        typer.echo(
            f"Device type: {dev_type_id}  Firmware: {firmware_version[0]}.{firmware_version[1]}.{firmware_version[2]}.{firmware_version[3]}  Manufacturer: {_read_str(data, 0x1C, 8)}",
            err=True,
        )

        sections = _parse_sections(data, hdr_len)

        headers = ["index", "type", "name", "version", "offset", "size", "content"]
        rows = []
        json_data = []

        for section_index, section in enumerate(sections):
            content = _detect_content(data, section["offset"], section["size"]) or ""
            row = [
                str(section_index),
                str(section["type"]),
                section["type_name"],
                section["version"],
                f"0x{section['offset']:X}",
                f"{section['size']:,}",
                content,
            ]
            rows.append(row)

            section_json = dict(section)
            section_json["index"] = section_index
            section_json["content"] = content

            if scan and section["offset"] + section["size"] <= len(data):
                embedded = _scan_for_embedded(data, section["offset"], section["size"])
                if embedded:
                    section_json["embedded"] = embedded
                    for entry in embedded:
                        size_str = f"{entry['size']:,}" if "size" in entry else ""
                        detail = entry.get("text", f"{entry['type']} ({size_str} bytes)")
                        rows.append(["", "", "", "", f"  0x{entry['offset']:X}", size_str, detail[:60]])

            json_data.append(section_json)

        output_table(headers, rows, json_data=json_data)

        if len(dnt_files) > 1:
            typer.echo("")


@app.command("extract")
def firmware_extract(
    path: Path = typer.Argument(..., help=".dnt file to extract from."),
    section: int = typer.Argument(..., help="Section index to extract."),
    output: Path = typer.Option(None, "-o", "--output", help="Output file (default: stdout)."),
):
    """Extract a raw section from a .dnt file."""
    with open(path, "rb") as f:
        data = f.read()

    if len(data) < 0x50 or data[:4] != b"AUDI":
        typer.echo(f"Not a .dnt file: {path}", err=True)
        raise typer.Exit(code=1)

    hdr_len = struct.unpack(">I", data[4:8])[0]
    sections = _parse_sections(data, hdr_len)

    if section < 0 or section >= len(sections):
        typer.echo(f"Section {section} out of range (0-{len(sections) - 1}).", err=True)
        raise typer.Exit(code=1)

    sec = sections[section]
    if sec["offset"] + sec["size"] > len(data):
        typer.echo(f"Section {section} extends past end of file.", err=True)
        raise typer.Exit(code=1)

    blob = data[sec["offset"] : sec["offset"] + sec["size"]]

    if output:
        with open(output, "wb") as f:
            f.write(blob)
        typer.echo(f"Wrote {len(blob):,} bytes to {output}", err=True)
    else:
        sys.stdout.buffer.write(blob)


@app.command("hexdump")
def firmware_hexdump(
    path: Path = typer.Argument(..., help=".dnt file."),
    offset: int = typer.Option(0, "--offset", help="Start offset in file."),
    length: int = typer.Option(256, "--length", "-l", help="Number of bytes to dump."),
    section: Optional[int] = typer.Option(
        None, "--section", "-s", help="Dump from this section index instead of file offset."
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
        data = data[sec["offset"] : sec["offset"] + sec["size"]]

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
    (0, 4, "type", "section_type", "u32 BE"),
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
        name = SECTION_TYPES.get(value, "")
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
        lines.extend(_span_lines(data, sec_off + off, show_len, "string", name, f"char[{stride}]", field_idx=fi))
        fi += 1
        off += stride
    return fi


def _dissect_config_section(data, sec_off, sec_size, sec_type, lines, fi):
    type_name = "Brooklyn II" if sec_type == 9 else "Ultimo"
    lines.append(_section_header(f"Config Section (type {sec_type}, {type_name}, 0x{sec_off:X})"))

    if sec_type == 9:
        field_map = [
            (0x0380, 32, "board_name"),
        ]
        for off, maxlen, name in field_map:
            if off + maxlen > sec_size:
                continue
            show_len = min(maxlen, 32)
            lines.extend(_span_lines(data, sec_off + off, show_len, "string", name, f"char[{maxlen}]", field_idx=fi))
            fi += 1
        fi = _dissect_channel_range(data, sec_off, sec_size, 0x053C, 32, "tx_channels", lines, fi)
        fi = _dissect_channel_range(data, sec_off, sec_size, 0x453C, 32, "rx_channels", lines, fi)
        tail_fields = [
            (0x8D5C, 32, "model_id"),
            (0x8D70, 16, "manufacturer_short"),
            (0x8D80, 64, "manufacturer"),
            (0x8E00, 128, "product_name"),
        ]
        for off, maxlen, name in tail_fields:
            if off + maxlen > sec_size:
                continue
            show_len = min(maxlen, 32)
            lines.extend(_span_lines(data, sec_off + off, show_len, "string", name, f"char[{maxlen}]", field_idx=fi))
            fi += 1
    elif sec_type == 14:
        cfg = data[sec_off : sec_off + sec_size]
        str_fields = [
            ("board_name", 32, [0x01D4]),
            ("model_id", 32, [0x0244, 0x0268, 0x0254, 0x0278]),
        ]
        for name, maxlen, candidates in str_fields:
            for off in candidates:
                if off + maxlen <= sec_size:
                    s = _read_str(cfg, off, maxlen)
                    if _is_printable(s) and len(s) >= 2:
                        lines.extend(
                            _span_lines(data, sec_off + off, maxlen, "string", name, f"char[{maxlen}]", field_idx=fi)
                        )
                        fi += 1
                        break
        for tx_base in [0x060C, 0x0634, 0x061C, 0x0644]:
            s = _read_str(cfg, tx_base, 32)
            if _is_printable(s) and len(s) >= 2:
                fi = _dissect_channel_range(data, sec_off, sec_size, tx_base, 32, "tx_channels", lines, fi)
                break
        for rx_base in [0x080C, 0x0834, 0x081C, 0x0844]:
            s = _read_str(cfg, rx_base, 32)
            if _is_printable(s) and len(s) >= 2:
                fi = _dissect_channel_range(data, sec_off, sec_size, rx_base, 32, "rx_channels", lines, fi)
                break
        tail_str_fields = [
            ("manufacturer", 32, [0x0A70, 0x0A98, 0x0A80, 0x0AA8]),
            ("product_name", 64, [0x0AF0, 0x0B18, 0x0B00, 0x0B28]),
        ]
        for name, maxlen, candidates in tail_str_fields:
            for off in candidates:
                if off + maxlen <= sec_size:
                    s = _read_str(cfg, off, maxlen)
                    if _is_printable(s) and len(s) >= 2:
                        lines.extend(
                            _span_lines(data, sec_off + off, maxlen, "string", name, f"char[{maxlen}]", field_idx=fi)
                        )
                        fi += 1
                        break

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
    max_entries = (hdr_len - 0x40) // 16
    lines.append("")
    lines.append(_section_header("Section Table"))

    sections = []
    sec_idx = 0
    for i in range(min(max_entries, 16)):
        base = 0x40 + i * 16
        if base + 16 > len(data):
            break
        s_type = struct.unpack(">I", data[base : base + 4])[0]
        s_ver = struct.unpack(">I", data[base + 4 : base + 8])[0]
        s_off = struct.unpack(">I", data[base + 8 : base + 12])[0]
        s_size = struct.unpack(">I", data[base + 12 : base + 16])[0]
        if s_type > 0xFF:
            break
        if s_type == 0 and s_ver == 0 and s_off == 0 and s_size == 0:
            continue
        type_name = SECTION_TYPES.get(s_type, f"type {s_type}")
        lines.append(f"  {_COLOR_TYPE}── section {sec_idx} ({type_name}) ──{_RESET}")
        for field_off, field_len, kind, name, dtype in SECTION_ENTRY_FIELDS:
            lines.extend(_span_lines(data, base + field_off, field_len, kind, name, dtype, field_idx=fi))
            fi += 1
        sections.append((sec_idx, s_type, s_off, s_size))
        sec_idx += 1

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
            fi = _dissect_config_section(data, s_off, s_size, s_type, lines, fi)

        elif s_type == 6:
            if data[s_off : s_off + 4] == b"AUDI":
                lines.append("")
                lines.append(_section_header(f"Nested AUDI (bootloader, 0x{s_off:X})"))
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


PAGE_SIZE = 4096


def _le32(data, off):
    return struct.unpack("<I", data[off : off + 4])[0]


class _CramfsInode:
    def __init__(self, data, offset):
        w0 = _le32(data, offset)
        w1 = _le32(data, offset + 4)
        w2 = _le32(data, offset + 8)
        self.mode = w0 & 0xFFFF
        self.uid = (w0 >> 16) & 0xFFFF
        self.size = w1 & 0xFFFFFF
        self.gid = (w1 >> 24) & 0xFF
        self.namelen = w2 & 0x3F
        self.offset = (w2 >> 6) & 0x3FFFFFF
        self.data_offset = self.offset * 4

    def is_dir(self):
        return stat.S_ISDIR(self.mode)

    def is_reg(self):
        return stat.S_ISREG(self.mode)

    def is_lnk(self):
        return stat.S_ISLNK(self.mode)

    def type_char(self):
        if self.is_dir():
            return "d"
        if self.is_reg():
            return "-"
        if self.is_lnk():
            return "l"
        if stat.S_ISCHR(self.mode):
            return "c"
        if stat.S_ISBLK(self.mode):
            return "b"
        if stat.S_ISFIFO(self.mode):
            return "p"
        return "?"

    def mode_str(self):
        chars = self.type_char()
        for shift in (6, 3, 0):
            bits = (self.mode >> shift) & 7
            chars += "r" if bits & 4 else "-"
            chars += "w" if bits & 2 else "-"
            chars += "x" if bits & 1 else "-"
        return chars


class _CramfsExtractionError(ValueError):
    pass


def _validate_cramfs_name(name: str) -> str:
    if not name:
        raise _CramfsExtractionError("empty inode name")
    if "\x00" in name:
        raise _CramfsExtractionError("inode name contains a NUL byte")
    if name in (".", ".."):
        raise _CramfsExtractionError(f"unsafe inode name: {name!r}")
    if "/" in name or "\\" in name:
        raise _CramfsExtractionError(f"inode name contains a path separator: {name!r}")
    if PurePosixPath(name).is_absolute() or PureWindowsPath(name).drive:
        raise _CramfsExtractionError(f"absolute inode name is not allowed: {name!r}")
    return name


def _decode_cramfs_name(raw_name: bytes) -> str:
    raw_name = raw_name.rstrip(b"\x00")
    if b"\x00" in raw_name:
        raise _CramfsExtractionError("inode name contains a NUL byte")
    try:
        name = raw_name.decode("ascii")
    except UnicodeDecodeError as exception:
        raise _CramfsExtractionError("inode name is not valid ASCII") from exception
    return _validate_cramfs_name(name)


def _require_cramfs_containment(root: Path, candidate: Path, description: str) -> Path:
    try:
        resolved = candidate.resolve(strict=False)
    except (OSError, RuntimeError) as exception:
        raise _CramfsExtractionError(f"could not resolve {description}: {exception}") from exception

    try:
        resolved.relative_to(root)
    except ValueError as exception:
        raise _CramfsExtractionError(f"{description} escapes extraction root: {candidate}") from exception
    return resolved


def _safe_cramfs_destination(root: Path, components: tuple[str, ...]) -> Path:
    for component in components:
        _validate_cramfs_name(component)
    destination = root.joinpath(*components)
    _require_cramfs_containment(root, destination, "inode path")
    return destination


def _safe_cramfs_symlink_target(root: Path, destination: Path, target: str) -> str:
    if not target:
        raise _CramfsExtractionError(f"empty symlink target for {destination}")
    if "\x00" in target:
        raise _CramfsExtractionError(f"symlink target contains a NUL byte: {destination}")
    posix_target = PurePosixPath(target)
    has_windows_drive = PureWindowsPath(target).drive or any(PureWindowsPath(part).drive for part in posix_target.parts)
    if "\\" in target or has_windows_drive:
        raise _CramfsExtractionError(f"symlink target uses an unsafe platform-specific path: {target!r}")

    if posix_target.is_absolute():
        candidate = root.joinpath(*posix_target.parts[1:])
        _require_cramfs_containment(root, candidate, "symlink target")
        return os.path.relpath(candidate, start=destination.parent)

    candidate = destination.parent.joinpath(*posix_target.parts)
    _require_cramfs_containment(root, candidate, "symlink target")
    return target


def _cramfs_extract_file(data, inode):
    if inode.size == 0:
        return b""
    num_blocks = (inode.size + PAGE_SIZE - 1) // PAGE_SIZE
    ptr_start = inode.data_offset
    if ptr_start + num_blocks * 4 > len(data):
        return None
    block_ends = [_le32(data, ptr_start + i * 4) for i in range(num_blocks)]
    prev = ptr_start + num_blocks * 4
    result = b""
    for bend in block_ends:
        if bend > len(data) or bend <= prev:
            break
        chunk = data[prev:bend]
        try:
            result += zlib.decompress(chunk)
        except zlib.error:
            result += chunk
        prev = bend
    return result[: inode.size]


def _cramfs_walk_directory(data, inode, components, root, verbose, visited):
    if not inode.is_dir():
        return

    directory_key = (inode.data_offset, inode.size)
    if directory_key in visited:
        raise _CramfsExtractionError(f"recursive directory inode at offset 0x{inode.data_offset:X}")
    visited.add(directory_key)

    pos = inode.data_offset
    end = pos + inode.size
    while pos + 12 <= end and pos + 12 <= len(data):
        child = _CramfsInode(data, pos)
        pos += 12
        name_bytes = child.namelen * 4
        if name_bytes > 0 and pos + name_bytes <= end and pos + name_bytes <= len(data):
            name = _decode_cramfs_name(data[pos : pos + name_bytes])
            pos += name_bytes
        else:
            raise _CramfsExtractionError(f"invalid inode name length at directory offset 0x{inode.data_offset:X}")

        child_components = (*components, name)
        full = "/" + "/".join(child_components)
        dest = _safe_cramfs_destination(root, child_components)

        if child.is_dir():
            if verbose:
                typer.echo(f"{child.mode_str()} {full}/")
            if os.path.lexists(dest):
                raise _CramfsExtractionError(f"duplicate inode path: {full}")
            try:
                dest.mkdir()
            except OSError as exception:
                raise _CramfsExtractionError(f"could not create directory {full}: {exception}") from exception
            _cramfs_walk_directory(
                data,
                child,
                child_components,
                root,
                verbose,
                visited,
            )
        elif child.is_lnk():
            target_data = _cramfs_extract_file(data, child)
            if target_data is None or len(target_data) != child.size:
                raise _CramfsExtractionError(f"invalid symlink data for {full}")
            try:
                target = target_data.decode("ascii")
            except UnicodeDecodeError as exception:
                raise _CramfsExtractionError(f"symlink target is not valid ASCII: {full}") from exception
            safe_target = _safe_cramfs_symlink_target(root, dest, target)
            if verbose:
                typer.echo(f"{child.mode_str()} {full} -> {safe_target}")
            if os.path.lexists(dest):
                raise _CramfsExtractionError(f"duplicate inode path: {full}")
            try:
                os.symlink(safe_target, dest)
            except OSError as exception:
                raise _CramfsExtractionError(f"could not create symlink {full}: {exception}") from exception
        elif child.is_reg():
            fdata = _cramfs_extract_file(data, child)
            if fdata is None or len(fdata) != child.size:
                raise _CramfsExtractionError(f"invalid file data for {full}")
            sz = len(fdata)
            if verbose:
                typer.echo(f"{child.mode_str()} {full}  ({child.size} -> {sz} bytes)")
            if os.path.lexists(dest):
                raise _CramfsExtractionError(f"duplicate inode path: {full}")
            try:
                with open(dest, "xb") as f:
                    f.write(fdata)
                if child.mode & 0o111:
                    os.chmod(dest, child.mode & 0o7777)
            except OSError as exception:
                raise _CramfsExtractionError(f"could not write file {full}: {exception}") from exception
        else:
            if verbose:
                typer.echo(f"{child.type_char()} {full}  (special)")

    visited.remove(directory_key)


def _cramfs_walk(data, inode, path, outdir, verbose=False):
    output = Path(outdir)
    if output.is_symlink():
        raise _CramfsExtractionError(f"extraction root must not be a symlink: {output}")
    try:
        root = output.resolve(strict=True)
    except (OSError, RuntimeError) as exception:
        raise _CramfsExtractionError(f"could not resolve extraction root {output}: {exception}") from exception
    if not root.is_dir():
        raise _CramfsExtractionError(f"extraction root is not a directory: {root}")

    if path:
        raw_components = tuple(component for component in path.split("/") if component)
        components = tuple(_validate_cramfs_name(component) for component in raw_components)
    else:
        components = ()
    _cramfs_walk_directory(data, inode, components, root, verbose, set())


def _cramfs_find_file(data, root_inode, target_path):
    parts = [p for p in target_path.strip("/").split("/") if p]
    current = root_inode
    for i, part in enumerate(parts):
        if not current.is_dir():
            return None
        pos = current.data_offset
        end = pos + current.size
        found = False
        while pos + 12 <= end and pos + 12 <= len(data):
            child = _CramfsInode(data, pos)
            pos += 12
            name_bytes = child.namelen * 4
            if name_bytes > 0 and pos + name_bytes <= len(data):
                name = data[pos : pos + name_bytes].rstrip(b"\x00").decode("ascii", errors="replace")
                pos += name_bytes
            else:
                break
            if name == part:
                current = child
                found = True
                break
        if not found:
            return None
    if current.is_reg():
        return _cramfs_extract_file(data, current)
    return None


def _find_cramfs_in_dnt(data):
    sections = []
    if len(data) >= 0x50 and data[:4] == b"AUDI":
        hdr_len = struct.unpack(">I", data[4:8])[0]
        sections = _parse_sections(data, hdr_len)

    for sec in sections:
        if sec["offset"] + sec["size"] > len(data):
            continue
        blob = data[sec["offset"] : sec["offset"] + sec["size"]]
        for magic_bytes in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
            pos = blob.find(magic_bytes)
            if pos != -1:
                abs_off = sec["offset"] + pos
                is_be = magic_bytes == CRAMFS_MAGIC_BE
                endian = ">" if is_be else "<"
                fs_size = struct.unpack(f"{endian}I", data[abs_off + 4 : abs_off + 8])[0]
                return abs_off, fs_size, is_be

    for magic_bytes, is_be in [(CRAMFS_MAGIC_LE, False), (CRAMFS_MAGIC_BE, True)]:
        pos = data.find(magic_bytes)
        if pos != -1:
            endian = ">" if is_be else "<"
            fs_size = struct.unpack(f"{endian}I", data[pos + 4 : pos + 8])[0]
            return pos, fs_size, is_be

    return None, None, None


def _cramfs_to_le(data, cramfs_off, cramfs_size, is_be):
    blob = bytearray(data[cramfs_off : cramfs_off + cramfs_size])
    if is_be:
        for i in range(0, len(blob) - 3, 4):
            blob[i], blob[i + 1], blob[i + 2], blob[i + 3] = blob[i + 3], blob[i + 2], blob[i + 1], blob[i]
    return bytes(blob)


def _prepare_rootfs_output(output: Path, force: bool = False) -> None:
    output_exists = output.exists() or output.is_symlink()
    if output_exists and not force:
        typer.echo(
            f"Error: output path already exists: {output}. Use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=1)

    if output_exists:
        resolved = output.expanduser().resolve()
        home = Path.home().resolve()
        cwd = Path.cwd().resolve()
        protected_paths = {
            Path(resolved.anchor),
            home,
            cwd,
            *home.parents,
            *cwd.parents,
        }
        if resolved in protected_paths:
            typer.echo(f"Error: refusing to replace unsafe output path: {resolved}", err=True)
            raise typer.Exit(code=1)

        if output.is_symlink() or not output.is_dir():
            output.unlink()
        else:
            import shutil

            shutil.rmtree(output)

    output.mkdir(parents=True, exist_ok=False)


@app.command("rootfs")
def firmware_rootfs(
    path: Path = typer.Argument(..., help=".dnt file containing a CramFS rootfs."),
    output: Path = typer.Argument(..., help="Output directory for extracted filesystem."),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Print each extracted file."),
    force: bool = typer.Option(
        False,
        "--force",
        help="Delete and replace an existing output path.",
    ),
):
    """Extract the Linux root filesystem from a .dnt firmware image."""
    with open(path, "rb") as f:
        data = f.read()

    cramfs_off, cramfs_size, is_be = _find_cramfs_in_dnt(data)
    if cramfs_off is None:
        typer.echo("No CramFS filesystem found in this firmware.", err=True)
        raise typer.Exit(code=1)

    endian_str = "big-endian" if is_be else "little-endian"
    typer.echo(f"CramFS at offset 0x{cramfs_off:X}, {cramfs_size:,} bytes ({endian_str})", err=True)

    cramfs_data = _cramfs_to_le(data, cramfs_off, cramfs_size, is_be)

    magic = _le32(cramfs_data, 0)
    if magic != 0x28CD3D45:
        typer.echo(f"CramFS magic mismatch after conversion: 0x{magic:08X}", err=True)
        raise typer.Exit(code=1)

    fs_size = _le32(cramfs_data, 4)
    file_count = _le32(cramfs_data, 44)
    typer.echo(f"Filesystem: {fs_size:,} bytes, {file_count} files", err=True)

    root = _CramfsInode(cramfs_data, 0x40)

    _prepare_rootfs_output(output, force=force)

    try:
        _cramfs_walk(cramfs_data, root, "", output, verbose=verbose)
    except _CramfsExtractionError as exception:
        typer.echo(f"Error: could not safely extract CramFS: {exception}", err=True)
        raise typer.Exit(code=1) from exception
    typer.echo(f"Extracted to {output}", err=True)


@app.command("password")
def firmware_password(
    path: Path = typer.Argument(..., help=".dnt file containing a CramFS rootfs."),
):
    """Extract the root password hash from a .dnt firmware image.

    Reads /etc/passwd and /etc/shadow from the embedded CramFS filesystem.
    Brooklyn II devices use DES crypt (hashcat mode 1500, max 8 characters).
    """
    with open(path, "rb") as f:
        data = f.read()

    cramfs_off, cramfs_size, is_be = _find_cramfs_in_dnt(data)
    if cramfs_off is None:
        typer.echo("No CramFS filesystem found.", err=True)
        raise typer.Exit(code=1)

    cramfs_data = _cramfs_to_le(data, cramfs_off, cramfs_size, is_be)
    root = _CramfsInode(cramfs_data, 0x40)

    for passwd_path in ("etc/passwd", "etc/shadow"):
        content = _cramfs_find_file(cramfs_data, root, passwd_path)
        if content is None:
            continue
        for line in content.decode("ascii", errors="replace").splitlines():
            if line.startswith("root:"):
                fields = line.split(":")
                pw_hash = fields[1]
                if pw_hash and pw_hash not in ("*", "!", "x", "!!"):
                    typer.echo(pw_hash)
                    return

    typer.echo("No root password hash found.", err=True)
    raise typer.Exit(code=1)
