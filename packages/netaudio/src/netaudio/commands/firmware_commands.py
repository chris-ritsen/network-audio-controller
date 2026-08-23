from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.commands.firmware_app import app
from netaudio.commands.firmware_constants import FIRMWARE_DATABASE_SCHEMA_VERSION
from netaudio.commands.firmware_parser import (
    _collect_dnt_files,
    _detect_content,
    _load_resume_results,
    _parse_sections,
    _scan_for_embedded,
    parse_dnt,
)


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
        results = _load_resume_results(resume)
        already_done = {r["file"] for r in results}
        typer.echo(f"Resuming: {len(already_done)} already processed", err=True)

    total = len(dnt_files)
    processed = 0
    errors = 0

    for i, path in enumerate(dnt_files):
        if str(path) in already_done:
            continue

        if (i % 10 == 0 or i == total - 1) and state.output_format != OutputFormat.json:
            print(f"\r[{i + 1}/{total}] {path.name}", end="", file=sys.stderr)

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
                r.get("product_name", "") or "",
                r.get("model_id", "") or "",
                str(r.get("tx_channel_count", "")),
                str(r.get("rx_channel_count", "")),
            ]
        )

    output_table(headers, rows, json_data=results)


def _init_db(db_path):
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    existing_tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('firmware', 'sections')"
        ).fetchall()
    }
    schema_version = conn.execute("PRAGMA user_version").fetchone()[0]
    if existing_tables and schema_version != FIRMWARE_DATABASE_SCHEMA_VERSION:
        conn.close()
        raise RuntimeError(
            f"Firmware database schema version {schema_version} is incompatible with version "
            f"{FIRMWARE_DATABASE_SCHEMA_VERSION}; create a new database"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS firmware (
            sha256          TEXT PRIMARY KEY,
            file_size       INTEGER,
            device_type_id  INTEGER,
            firmware_version TEXT,
            manufacturer_header TEXT,
            capability_partition_id INTEGER,
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
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sections (
            sha256      TEXT NOT NULL,
            idx         INTEGER NOT NULL,
            partition_id INTEGER,
            partition_name TEXT,
            version     TEXT,
            body_offset INTEGER,
            file_offset INTEGER,
            size        INTEGER,
            PRIMARY KEY (sha256, idx),
            FOREIGN KEY (sha256) REFERENCES firmware(sha256)
        )
    """
    )
    section_columns = {row[1] for row in conn.execute("PRAGMA table_info(sections)").fetchall()}
    required_section_columns = {
        "sha256",
        "idx",
        "partition_id",
        "partition_name",
        "version",
        "body_offset",
        "file_offset",
        "size",
    }
    if not required_section_columns.issubset(section_columns):
        conn.close()
        raise RuntimeError("Firmware database sections schema is invalid; create a new database")
    conn.execute(f"PRAGMA user_version = {FIRMWARE_DATABASE_SCHEMA_VERSION}")
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
            print(f"\r[{i + 1}/{total}] {path.name}", end="", file=sys.stderr)

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
                    manufacturer_header, capability_partition_id, board_name,
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
                    result.get("capability_partition_id"),
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
                       (sha256, idx, partition_id, partition_name, version, body_offset,
                        file_offset, size)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        sha,
                        idx,
                        sec["partition_id"],
                        sec["partition_name"],
                        sec["version"],
                        sec["body_offset"],
                        sec["file_offset"],
                        sec["size"],
                    ),
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

        headers = [
            "index",
            "partition_id",
            "partition_name",
            "version",
            "body_offset",
            "file_offset",
            "size",
            "content",
        ]
        rows = []
        json_data = []

        for section_index, section in enumerate(sections):
            content = _detect_content(data, section["file_offset"], section["size"]) or ""
            row = [
                str(section_index),
                str(section["partition_id"]),
                section["partition_name"],
                section["version"],
                f"0x{section['body_offset']:X}",
                f"0x{section['file_offset']:X}",
                f"{section['size']:,}",
                content,
            ]
            rows.append(row)

            section_json = dict(section)
            section_json["index"] = section_index
            section_json["content"] = content

            if scan:
                embedded = _scan_for_embedded(data, section["file_offset"], section["size"])
                if embedded:
                    section_json["embedded"] = embedded
                    for entry in embedded:
                        size_str = f"{entry['size']:,}" if "size" in entry else ""
                        detail = entry.get("text", f"{entry['type']} ({size_str} bytes)")
                        rows.append(
                            [
                                "",
                                "",
                                "",
                                "",
                                "",
                                f"  0x{entry['file_offset']:X}",
                                size_str,
                                detail,
                            ]
                        )

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
    blob = data[sec["file_offset"] : sec["file_offset"] + sec["size"]]

    if output:
        with open(output, "wb") as f:
            f.write(blob)
        typer.echo(f"Wrote {len(blob):,} bytes to {output}", err=True)
    else:
        sys.stdout.buffer.write(blob)
