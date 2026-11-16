from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import typer

from netaudio.cli_support.output import output_table
from netaudio.commands.firmware.archive import progress_visible, report_progress
from netaudio.commands.firmware.constants import FIRMWARE_DATABASE_SCHEMA_VERSION
from netaudio.commands.firmware.parser import _collect_dnt_files, parse_dnt


def _init_db(database_path):
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode=WAL")
    existing_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('firmware', 'sections')"
        ).fetchall()
    }
    schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
    if existing_tables and schema_version != FIRMWARE_DATABASE_SCHEMA_VERSION:
        connection.close()
        raise RuntimeError(
            f"Firmware database schema version {schema_version} is incompatible with version "
            f"{FIRMWARE_DATABASE_SCHEMA_VERSION}; create a new database"
        )
    connection.execute(
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
    connection.execute(
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
    section_columns = {row[1] for row in connection.execute("PRAGMA table_info(sections)").fetchall()}
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
        connection.close()
        raise RuntimeError("Firmware database sections schema is invalid; create a new database")
    connection.execute(f"PRAGMA user_version = {FIRMWARE_DATABASE_SCHEMA_VERSION}")
    connection.commit()
    return connection


def firmware_db(
    paths: list[Path] = typer.Argument(..., help=".dnt files or directories to scan."),
    db: Path = typer.Option("firmware.db", "--db", help="SQLite database path."),
):
    """Parse .dnt firmware files into a SQLite database."""
    dnt_files = _collect_dnt_files(paths)
    if not dnt_files:
        typer.echo("No .dnt files found.", err=True)
        raise typer.Exit(code=1)

    connection = _init_db(db)

    existing = {row[0] for row in connection.execute("SELECT sha256 FROM firmware").fetchall()}
    typer.echo(f"{len(existing)} already in db, {len(dnt_files)} files to scan", err=True)

    total = len(dnt_files)
    inserted = 0
    skipped = 0
    errors = 0

    for index, path in enumerate(dnt_files):
        report_progress(index, total, path)

        try:
            with open(path, "rb") as file:
                raw = file.read()
            digest = hashlib.sha256(raw).hexdigest()

            if digest in existing:
                skipped += 1
                continue

            result = parse_dnt(path)
            if not result:
                errors += 1
                continue

            connection.execute(
                """INSERT OR REPLACE INTO firmware
                   (sha256, file_size, device_type_id, firmware_version,
                    manufacturer_header, capability_partition_id, board_name,
                    model_id, manufacturer_short, manufacturer, product_name,
                    tx_channel_count, rx_channel_count,
                    tx_channel_names, rx_channel_names)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    digest,
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
            for section_index, section in enumerate(sections):
                connection.execute(
                    """INSERT OR REPLACE INTO sections
                       (sha256, idx, partition_id, partition_name, version, body_offset,
                        file_offset, size)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        digest,
                        section_index,
                        section["partition_id"],
                        section["partition_name"],
                        section["version"],
                        section["body_offset"],
                        section["file_offset"],
                        section["size"],
                    ),
                )

            existing.add(digest)
            inserted += 1

            if inserted % 50 == 0:
                connection.commit()

        except Exception as error:
            errors += 1
            typer.echo(f"\n  ERROR: {path}: {error}", err=True)

    connection.commit()
    connection.close()

    if progress_visible():
        typer.echo("", err=True)
    output_table(
        ["added", "already_present", "errors", "database"],
        [[str(inserted), str(skipped), str(errors), str(db)]],
        json_data={"added": inserted, "already_present": skipped, "database": str(db), "errors": errors},
    )
