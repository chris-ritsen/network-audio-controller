from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio.commands.firmware_app import app
from netaudio.common.manifest import manifest_bytes
from netaudio.commands.firmware_constants import (
    BROOKLYN2_FLASH_PARTITIONS,
    BROOKLYN2_FLASH_SIZE,
    BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
    PARTITION_NAMES,
    UIMAGE_HEADER_SIZE,
)
from netaudio.commands.firmware_evidence import _verify_brooklyn2_hardware_profile_evidence
from netaudio.commands.firmware_models import (
    _brooklyn2_board_information_manifest,
    _brooklyn2_hardware_profile_manifest,
    _build_brooklyn2_board_information_partition,
    _load_brooklyn2_board_information_descriptor,
    _load_brooklyn2_hardware_profile,
)
from netaudio.commands.firmware_parser import (
    _parse_dnt_bytes,
    _publish_output_directory_without_replacement,
    _write_synchronized_file,
)
from netaudio.commands.firmware_validation import (
    _validate_brooklyn2_flash_layout,
    _validate_brooklyn2_kernel_payload,
    _validate_capability_payload,
    _validate_cramfs_payload,
    _validate_fpga_payload,
    _validate_uimage,
)


def _build_brooklyn2_image(
    path: Path,
    output_directory: Path,
    hardware_profile_path: Path,
    board_information_path: Path,
    trusted_evidence_root: Path,
    protected_capability_partition_path: Optional[Path] = None,
) -> dict[str, Any]:
    if sys.platform not in {"linux", "darwin"}:
        raise ValueError("Brooklyn II image publication is supported only on Linux and macOS")
    _validate_brooklyn2_flash_layout()

    hardware_profile_input = _load_brooklyn2_hardware_profile(hardware_profile_path)
    hardware_profile = hardware_profile_input.profile
    payload_partition_identifiers = dict(hardware_profile.payload_partition_identifiers)
    board_information_input = _load_brooklyn2_board_information_descriptor(board_information_path)
    board_information_payload = _build_brooklyn2_board_information_partition(board_information_input.descriptor)
    board_information_sha256 = hashlib.sha256(board_information_payload).hexdigest()
    if board_information_sha256 != hardware_profile.compatible_board_information_sha256:
        raise ValueError(
            f"Hardware profile {hardware_profile.profile_name} requires board-information SHA-256 "
            f"{hardware_profile.compatible_board_information_sha256}, found {board_information_sha256}"
        )

    protected_capability_partition_payload = None
    protected_capability_partition_validation = None
    if protected_capability_partition_path is not None:
        protected_capability_partition_payload = protected_capability_partition_path.read_bytes()
        if len(protected_capability_partition_payload) > BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE:
            raise ValueError(
                "Protected capability partition is "
                f"{len(protected_capability_partition_payload)} bytes, exceeding the "
                f"capacity of {BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE}"
            )
        protected_capability_partition_validation = _validate_cramfs_payload(protected_capability_partition_payload)

    dnt_data = path.read_bytes()
    firmware_sha256 = hashlib.sha256(dnt_data).hexdigest()
    if firmware_sha256 != hardware_profile.compatible_firmware_sha256:
        raise ValueError(
            f"Hardware profile {hardware_profile.profile_name} requires firmware SHA-256 "
            f"{hardware_profile.compatible_firmware_sha256}, found {firmware_sha256}"
        )
    dnt_result = _parse_dnt_bytes(dnt_data, path)
    if dnt_result is None:
        raise ValueError(f"Not a DNT file: {path}")
    if dnt_result["device_type_id"] != hardware_profile.device_type_identifier:
        raise ValueError(
            f"Hardware profile {hardware_profile.profile_name} requires device type "
            f"{hardware_profile.device_type_identifier}, found {dnt_result['device_type_id']}"
        )
    dnt_result.pop("file", None)
    evidence_assessment = _verify_brooklyn2_hardware_profile_evidence(
        hardware_profile,
        trusted_evidence_root,
        dnt_data,
        dnt_result,
        board_information_payload,
    )

    sections_by_partition_id = {}
    for section in dnt_result["sections"]:
        sections_by_partition_id.setdefault(section["partition_id"], []).append(section)

    required_partition_ids = set(payload_partition_identifiers.values())
    for partition_id in required_partition_ids:
        matching_sections = sections_by_partition_id.get(partition_id, [])
        if len(matching_sections) != 1:
            raise ValueError(
                f"Expected exactly one {PARTITION_NAMES[partition_id]} partition, found {len(matching_sections)}"
            )

    flash_data = bytearray([0xFF]) * BROOKLYN2_FLASH_SIZE
    partition_records = []
    physical_partition_records = []
    bootloader_payload = None
    bootloader_metadata = None

    for (
        physical_partition_name,
        flash_offset,
        partition_capacity,
    ) in BROOKLYN2_FLASH_PARTITIONS:
        if physical_partition_name == "brdinfo":
            if len(board_information_payload) != partition_capacity:
                raise RuntimeError(
                    f"Generated board-information partition is {len(board_information_payload)} bytes, "
                    f"expected {partition_capacity}"
                )
            flash_data[flash_offset : flash_offset + partition_capacity] = board_information_payload
            physical_partition_records.append(
                {
                    "physical_partition_name": physical_partition_name,
                    "flash_offset": flash_offset,
                    "capacity": partition_capacity,
                    "state": "generated",
                    "artifact_filename": "brdinfo.bin",
                    "size": len(board_information_payload),
                    "sha256": hashlib.sha256(board_information_payload).hexdigest(),
                }
            )
            continue

        if (
            physical_partition_name == "cap"
            and protected_capability_partition_payload is not None
            and protected_capability_partition_validation is not None
        ):
            flash_data[flash_offset : flash_offset + len(protected_capability_partition_payload)] = (
                protected_capability_partition_payload
            )
            physical_partition_records.append(
                {
                    "physical_partition_name": physical_partition_name,
                    "flash_offset": flash_offset,
                    "capacity": partition_capacity,
                    "state": "payload",
                    "artifact_filename": "cap.bin",
                    "size": len(protected_capability_partition_payload),
                    "sha256": hashlib.sha256(protected_capability_partition_payload).hexdigest(),
                    "validation": protected_capability_partition_validation,
                }
            )
            continue

        partition_id = payload_partition_identifiers.get(physical_partition_name)
        if partition_id is None:
            physical_partition_records.append(
                {
                    "physical_partition_name": physical_partition_name,
                    "flash_offset": flash_offset,
                    "capacity": partition_capacity,
                    "state": "erased",
                }
            )
            continue

        section = sections_by_partition_id[partition_id][0]
        source_offset = section["file_offset"]
        payload = dnt_data[source_offset : source_offset + section["size"]]
        if len(payload) > partition_capacity:
            raise ValueError(
                f"{section['partition_name']} payload is {len(payload)} bytes, exceeding "
                f"the {physical_partition_name} capacity of {partition_capacity}"
            )

        payload_sha256 = hashlib.sha256(payload).hexdigest()
        if physical_partition_name == "boot":
            validation = _validate_uimage(payload, 5, 0x29FC0000)
            bootloader_payload = payload[UIMAGE_HEADER_SIZE:]
            bootloader_metadata = validation
        elif physical_partition_name == "fpga":
            validation = _validate_fpga_payload(payload)
            expected_payload_sha256 = hardware_profile.selection_evidence.observed_fpga_payload_sha256
            if payload_sha256 != expected_payload_sha256:
                raise ValueError(
                    f"Hardware profile {hardware_profile.profile_name} requires FPGA payload SHA-256 "
                    f"{expected_payload_sha256}, found {payload_sha256}"
                )
        elif physical_partition_name == "image":
            validation = _validate_uimage(payload, 2, 0x28000000)
            validation["brooklyn2_payload"] = _validate_brooklyn2_kernel_payload(payload)
        elif physical_partition_name == "userarea":
            validation = _validate_cramfs_payload(payload)
        elif physical_partition_name == "cap1":
            validation = _validate_capability_payload(payload)
        else:
            raise RuntimeError(f"No validator for physical partition {physical_partition_name}")

        flash_data[flash_offset : flash_offset + len(payload)] = payload
        payload_record = {
            "source_partition_id": partition_id,
            "source_partition_name": section["partition_name"],
            "source_version": section["version"],
            "source_file_offset": source_offset,
            "physical_partition_name": physical_partition_name,
            "flash_offset": flash_offset,
            "size": len(payload),
            "capacity": partition_capacity,
            "sha256": payload_sha256,
            "validation": validation,
        }
        partition_records.append(payload_record)
        physical_partition_records.append({**payload_record, "state": "payload"})

    if bootloader_payload is None or bootloader_metadata is None:
        raise RuntimeError("Bootloader payload was not produced")

    flash_bytes = bytes(flash_data)
    unused_sections = [
        section for section in dnt_result["sections"] if section["partition_id"] not in required_partition_ids
    ]
    board_information_manifest = _brooklyn2_board_information_manifest(
        board_information_input,
        board_information_payload,
    )

    manifest = {
        "format_version": BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
        "hardware_profile": _brooklyn2_hardware_profile_manifest(
            hardware_profile_input,
            evidence_assessment,
        ),
        "dnt": dnt_result,
        "source_sha256": firmware_sha256,
        "board_information": board_information_manifest,
        "flash": {
            "filename": "flash.bin",
            "size": len(flash_bytes),
            "sha256": hashlib.sha256(flash_bytes).hexdigest(),
            "erased_byte": "ff",
            "physical_partitions": physical_partition_records,
        },
        "bootloader": {
            "filename": "bootloader.bin",
            "size": len(bootloader_payload),
            "sha256": hashlib.sha256(bootloader_payload).hexdigest(),
            **bootloader_metadata,
        },
        "payloads": partition_records,
        "unused_dnt_sections": unused_sections,
    }

    if output_directory.name in {"", ".", ".."}:
        raise ValueError("Output directory must name a new child directory")
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    output_parent = output_directory.parent.resolve(strict=True)
    publication_directory = output_parent / output_directory.name
    temporary_directory = Path(tempfile.mkdtemp(prefix=f".{output_directory.name}.", dir=output_parent))
    try:
        _write_synchronized_file(temporary_directory / "flash.bin", flash_bytes)
        _write_synchronized_file(
            temporary_directory / "bootloader.bin",
            bootloader_payload,
        )
        _write_synchronized_file(
            temporary_directory / "brdinfo.bin",
            board_information_payload,
        )
        if protected_capability_partition_payload is not None:
            _write_synchronized_file(
                temporary_directory / "cap.bin",
                protected_capability_partition_payload,
            )
        _write_synchronized_file(temporary_directory / "manifest.json", manifest_bytes(manifest))
        _publish_output_directory_without_replacement(
            temporary_directory,
            publication_directory,
        )
    except BaseException:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise

    return manifest


@app.command("build-brooklyn2-image")
def firmware_build_brooklyn2_image(
    path: Path = typer.Argument(..., help="Brooklyn II DNT firmware file."),
    output_directory: Path = typer.Option(..., "--output-directory", help="New output directory."),
    hardware_profile: Path = typer.Option(
        ...,
        "--hardware-profile",
        help="Versioned evidence-backed hardware-profile descriptor.",
    ),
    board_information: Path = typer.Option(
        ...,
        "--board-information",
        help="Versioned JSON board-information descriptor.",
    ),
    trusted_evidence_root: Path = typer.Option(
        ...,
        "--trusted-evidence-root",
        help="Root containing the immutable evidence artifacts referenced by the hardware profile.",
    ),
    protected_capability_partition: Optional[Path] = typer.Option(
        None,
        "--protected-capability-partition",
        help="Validated CramFS image for the protected per-device capability partition.",
    ),
):
    """Build a validated evidence-scoped Brooklyn II emulator image."""
    try:
        manifest = _build_brooklyn2_image(
            path,
            output_directory,
            hardware_profile,
            board_information,
            trusted_evidence_root,
            protected_capability_partition,
        )
    except (OSError, ValueError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1) from error
    typer.echo(f"Flash: {output_directory / manifest['flash']['filename']}")
    typer.echo(f"Bootloader: {output_directory / manifest['bootloader']['filename']}")
    if manifest["board_information"]["state"] == "generated":
        typer.echo(f"Board information: {output_directory / manifest['board_information']['filename']}")
    if protected_capability_partition is not None:
        typer.echo(f"Protected capability: {output_directory / 'cap.bin'}")
    typer.echo(f"Manifest: {output_directory / 'manifest.json'}")
