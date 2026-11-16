from __future__ import annotations

import json
import struct
import sys
from pathlib import Path
from typing import Optional

import typer

from netaudio.cli_support.output import output_table
from netaudio.commands.firmware.capabilities import _read_str
from netaudio.commands.firmware.dissection import _dissect_header
from netaudio.commands.firmware.parser import (
    _collect_dnt_files,
    _detect_content,
    _load_resume_results,
    _parse_sections,
    _scan_for_embedded,
    parse_dnt,
)


def progress_visible() -> bool:
    from netaudio.cli import OutputFormat, state

    return state.output_format in (OutputFormat.plain, OutputFormat.pretty, OutputFormat.table)


def report_progress(index: int, total: int, path: Path) -> None:
    if (index % 10 == 0 or index == total - 1) and progress_visible():
        typer.echo(f"\r[{index + 1}/{total}] {path.name}", nl=False, err=True)


def firmware_info(
    paths: list[Path] = typer.Argument(..., help=".dnt files or directories to scan."),
    save: Optional[Path] = typer.Option(None, "--save", help="Save JSON results to file."),
    resume: Optional[Path] = typer.Option(None, "--resume", help="Skip files already in this JSON output."),
):
    """Extract product identity facts from .dnt firmware files."""
    dnt_files = _collect_dnt_files(paths)
    if not dnt_files:
        typer.echo("No .dnt files found.", err=True)
        raise typer.Exit(code=1)

    already_done = set()
    results = []
    if resume and resume.exists():
        results = _load_resume_results(resume)
        already_done = {record["file"] for record in results}
        typer.echo(f"Resuming: {len(already_done)} already processed", err=True)

    total = len(dnt_files)
    processed = 0
    errors = 0

    for index, path in enumerate(dnt_files):
        if str(path) in already_done:
            continue

        report_progress(index, total, path)

        try:
            result = parse_dnt(path)
            if result:
                results.append(result)
                processed += 1
        except Exception as exception:
            errors += 1
            typer.echo(f"\n  ERROR: {path}: {exception}", err=True)

    if progress_visible():
        message = f"\n{processed} extracted"
        if errors:
            message += f", {errors} errors"
        typer.echo(message, err=True)

    if save:
        with open(save, "w") as file_handle:
            json.dump(results, file_handle, indent=2)
        typer.echo(f"Saved to {save}", err=True)

    headers = ["dev_id", "fw_ver", "mfg", "product", "model_id", "tx", "rx"]
    rows = []
    for record in results:
        rows.append(
            [
                str(record.get("device_type_id", "")),
                record.get("firmware_version", ""),
                record.get("manufacturer_header", ""),
                record.get("product_name", "") or "",
                record.get("model_id", "") or "",
                str(record.get("tx_channel_count", "")),
                str(record.get("rx_channel_count", "")),
            ]
        )

    output_table(headers, rows, json_data=results)


def firmware_sections(
    paths: list[Path] = typer.Argument(..., help=".dnt files to inspect."),
    scan: bool = typer.Option(False, "--scan", help="Scan section contents for embedded filesystems."),
):
    """Show the section table of .dnt files."""
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

        header_length = struct.unpack(">I", data[4:8])[0]
        device_type_id = struct.unpack(">I", data[16:20])[0]
        firmware_version = data[20:24]

        typer.echo(f"File: {file_path} ({len(data):,} bytes)", err=True)
        typer.echo(
            f"Device type: {device_type_id}  Firmware: {firmware_version[0]}.{firmware_version[1]}.{firmware_version[2]}.{firmware_version[3]}  Manufacturer: {_read_str(data, 0x1C, 8)}",
            err=True,
        )

        sections = _parse_sections(data, header_length)

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
                        size_text = f"{entry['size']:,}" if "size" in entry else ""
                        detail = entry.get("text", f"{entry['type']} ({size_text} bytes)")
                        rows.append(
                            [
                                "",
                                "",
                                "",
                                "",
                                "",
                                f"  0x{entry['file_offset']:X}",
                                size_text,
                                detail,
                            ]
                        )

            json_data.append(section_json)

        output_table(headers, rows, json_data=json_data)

        if len(dnt_files) > 1:
            typer.echo("")


def firmware_extract(
    path: Path = typer.Argument(..., help=".dnt file to extract from."),
    section: int = typer.Argument(..., help="Section index to extract."),
    output: Path = typer.Option(None, "-o", "--output", help="Output file (default: stdout)."),
):
    """Extract a raw section from a .dnt file."""
    with open(path, "rb") as file_handle:
        data = file_handle.read()

    if len(data) < 0x50 or data[:4] != b"AUDI":
        typer.echo(f"Not a .dnt file: {path}", err=True)
        raise typer.Exit(code=1)

    header_length = struct.unpack(">I", data[4:8])[0]
    sections = _parse_sections(data, header_length)

    if section < 0 or section >= len(sections):
        typer.echo(f"Section {section} out of range (0-{len(sections) - 1}).", err=True)
        raise typer.Exit(code=1)

    selected_section = sections[section]
    blob = data[selected_section["file_offset"] : selected_section["file_offset"] + selected_section["size"]]

    if output:
        with open(output, "wb") as file_handle:
            file_handle.write(blob)
        typer.echo(f"Wrote {len(blob):,} bytes to {output}", err=True)
    else:
        sys.stdout.buffer.write(blob)
