from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio.commands.firmware.constants import (
    BROOKLYN2_FLASH_PARTITIONS,
    BROOKLYN2_FLASH_SIZE,
    BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
    PARTITION_NAMES,
    UIMAGE_HEADER_SIZE,
)
from netaudio.commands.firmware.evidence import _verify_brooklyn2_hardware_profile_evidence
from netaudio.commands.firmware.models import (
    Brooklyn2BoardInformationInput,
    Brooklyn2HardwareProfile,
    Brooklyn2HardwareProfileEvidenceAssessment,
    Brooklyn2HardwareProfileInput,
    _brooklyn2_board_information_manifest,
    _brooklyn2_hardware_profile_manifest,
    _build_brooklyn2_board_information_partition,
    _load_brooklyn2_board_information_descriptor,
    _load_brooklyn2_hardware_profile,
)
from netaudio.commands.firmware.parser import (
    _parse_dnt_bytes,
    _publish_output_directory_without_replacement,
    _write_synchronized_file,
)
from netaudio.commands.firmware.validation import (
    _validate_brooklyn2_flash_layout,
    _validate_brooklyn2_kernel_payload,
    _validate_capability_payload,
    _validate_cramfs_payload,
    _validate_fpga_payload,
    _validate_uimage,
)
from netaudio.common.manifest import manifest_bytes


@dataclass
class Brooklyn2BuildInputs:
    board_information_input: Brooklyn2BoardInformationInput
    board_information_payload: bytes
    dnt_data: bytes
    dnt_result: dict[str, Any]
    evidence_assessment: Brooklyn2HardwareProfileEvidenceAssessment
    firmware_sha256: str
    hardware_profile_input: Brooklyn2HardwareProfileInput
    protected_capability_partition_payload: Optional[bytes] = None
    protected_capability_partition_validation: Optional[dict[str, Any]] = None
    sections_by_partition_id: dict[int, list[dict[str, Any]]] = field(default_factory=dict)

    @property
    def hardware_profile(self) -> Brooklyn2HardwareProfile:
        return self.hardware_profile_input.profile

    @property
    def payload_partition_identifiers(self) -> dict[str, int]:
        return dict(self.hardware_profile.payload_partition_identifiers)


@dataclass
class Brooklyn2PlacedFlash:
    bootloader_metadata: Optional[dict[str, Any]] = None
    bootloader_payload: Optional[bytes] = None
    flash_data: bytearray = field(default_factory=lambda: bytearray([0xFF]) * BROOKLYN2_FLASH_SIZE)
    partition_records: list[dict[str, Any]] = field(default_factory=list)
    physical_partition_records: list[dict[str, Any]] = field(default_factory=list)


def _load_board_information(
    board_information_path: Path, hardware_profile: Brooklyn2HardwareProfile
) -> tuple[Brooklyn2BoardInformationInput, bytes]:
    board_information_input = _load_brooklyn2_board_information_descriptor(board_information_path)
    board_information_payload = _build_brooklyn2_board_information_partition(board_information_input.descriptor)
    board_information_sha256 = hashlib.sha256(board_information_payload).hexdigest()
    if board_information_sha256 != hardware_profile.compatible_board_information_sha256:
        raise ValueError(
            f"Hardware profile {hardware_profile.profile_name} requires board-information SHA-256 "
            f"{hardware_profile.compatible_board_information_sha256}, found {board_information_sha256}"
        )
    return board_information_input, board_information_payload


def _load_protected_capability_partition(path: Optional[Path]) -> tuple[Optional[bytes], Optional[dict[str, Any]]]:
    if path is None:
        return None, None
    payload = path.read_bytes()
    if len(payload) > BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE:
        raise ValueError(
            "Protected capability partition is "
            f"{len(payload)} bytes, exceeding the "
            f"capacity of {BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE}"
        )
    return payload, _validate_cramfs_payload(payload)


def _load_compatible_firmware(path: Path, hardware_profile: Brooklyn2HardwareProfile) -> tuple[bytes, dict, str]:
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
    return dnt_data, dnt_result, firmware_sha256


def _load_brooklyn2_build_inputs(
    path: Path,
    hardware_profile_path: Path,
    board_information_path: Path,
    trusted_evidence_root: Path,
    protected_capability_partition_path: Optional[Path],
) -> Brooklyn2BuildInputs:
    hardware_profile_input = _load_brooklyn2_hardware_profile(hardware_profile_path)
    hardware_profile = hardware_profile_input.profile
    board_information_input, board_information_payload = _load_board_information(
        board_information_path, hardware_profile
    )
    protected_payload, protected_validation = _load_protected_capability_partition(protected_capability_partition_path)
    dnt_data, dnt_result, firmware_sha256 = _load_compatible_firmware(path, hardware_profile)
    evidence_assessment = _verify_brooklyn2_hardware_profile_evidence(
        hardware_profile,
        trusted_evidence_root,
        dnt_data,
        dnt_result,
        board_information_payload,
    )
    inputs = Brooklyn2BuildInputs(
        board_information_input=board_information_input,
        board_information_payload=board_information_payload,
        dnt_data=dnt_data,
        dnt_result=dnt_result,
        evidence_assessment=evidence_assessment,
        firmware_sha256=firmware_sha256,
        hardware_profile_input=hardware_profile_input,
        protected_capability_partition_payload=protected_payload,
        protected_capability_partition_validation=protected_validation,
    )
    for section in dnt_result["sections"]:
        inputs.sections_by_partition_id.setdefault(section["partition_id"], []).append(section)
    for partition_id in set(inputs.payload_partition_identifiers.values()):
        matching_sections = inputs.sections_by_partition_id.get(partition_id, [])
        if len(matching_sections) != 1:
            raise ValueError(
                f"Expected exactly one {PARTITION_NAMES[partition_id]} partition, found {len(matching_sections)}"
            )
    return inputs


def _validate_physical_payload(
    physical_partition_name: str, payload: bytes, payload_sha256: str, placed: Brooklyn2PlacedFlash, inputs
) -> dict[str, Any]:
    if physical_partition_name == "boot":
        validation = _validate_uimage(payload, 5, 0x29FC0000)
        placed.bootloader_payload = payload[UIMAGE_HEADER_SIZE:]
        placed.bootloader_metadata = validation
        return validation
    if physical_partition_name == "fpga":
        validation = _validate_fpga_payload(payload)
        expected_payload_sha256 = inputs.hardware_profile.selection_evidence.observed_fpga_payload_sha256
        if payload_sha256 != expected_payload_sha256:
            raise ValueError(
                f"Hardware profile {inputs.hardware_profile.profile_name} requires FPGA payload SHA-256 "
                f"{expected_payload_sha256}, found {payload_sha256}"
            )
        return validation
    if physical_partition_name == "image":
        validation = _validate_uimage(payload, 2, 0x28000000)
        validation["brooklyn2_payload"] = _validate_brooklyn2_kernel_payload(payload)
        return validation
    if physical_partition_name == "userarea":
        return _validate_cramfs_payload(payload)
    if physical_partition_name == "cap1":
        return _validate_capability_payload(payload)
    raise RuntimeError(f"No validator for physical partition {physical_partition_name}")


def _place_generated_partition(
    placed: Brooklyn2PlacedFlash, physical_partition_name: str, flash_offset: int, partition_capacity: int, inputs
) -> bool:
    if physical_partition_name == "brdinfo":
        payload = inputs.board_information_payload
        if len(payload) != partition_capacity:
            raise RuntimeError(
                f"Generated board-information partition is {len(payload)} bytes, expected {partition_capacity}"
            )
        placed.flash_data[flash_offset : flash_offset + partition_capacity] = payload
        placed.physical_partition_records.append(
            {
                "physical_partition_name": physical_partition_name,
                "flash_offset": flash_offset,
                "capacity": partition_capacity,
                "state": "generated",
                "artifact_filename": "brdinfo.bin",
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
        return True
    payload = inputs.protected_capability_partition_payload
    validation = inputs.protected_capability_partition_validation
    if physical_partition_name == "cap" and payload is not None and validation is not None:
        placed.flash_data[flash_offset : flash_offset + len(payload)] = payload
        placed.physical_partition_records.append(
            {
                "physical_partition_name": physical_partition_name,
                "flash_offset": flash_offset,
                "capacity": partition_capacity,
                "state": "payload",
                "artifact_filename": "cap.bin",
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "validation": validation,
            }
        )
        return True
    return False


def _place_payload_partition(
    placed: Brooklyn2PlacedFlash, physical_partition_name: str, flash_offset: int, partition_capacity: int, inputs
) -> None:
    partition_id = inputs.payload_partition_identifiers.get(physical_partition_name)
    if partition_id is None:
        placed.physical_partition_records.append(
            {
                "physical_partition_name": physical_partition_name,
                "flash_offset": flash_offset,
                "capacity": partition_capacity,
                "state": "erased",
            }
        )
        return
    section = inputs.sections_by_partition_id[partition_id][0]
    source_offset = section["file_offset"]
    payload = inputs.dnt_data[source_offset : source_offset + section["size"]]
    if len(payload) > partition_capacity:
        raise ValueError(
            f"{section['partition_name']} payload is {len(payload)} bytes, exceeding "
            f"the {physical_partition_name} capacity of {partition_capacity}"
        )
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    validation = _validate_physical_payload(physical_partition_name, payload, payload_sha256, placed, inputs)
    placed.flash_data[flash_offset : flash_offset + len(payload)] = payload
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
    placed.partition_records.append(payload_record)
    placed.physical_partition_records.append({**payload_record, "state": "payload"})


def _place_brooklyn2_partitions(inputs: Brooklyn2BuildInputs) -> Brooklyn2PlacedFlash:
    placed = Brooklyn2PlacedFlash()
    for physical_partition_name, flash_offset, partition_capacity in BROOKLYN2_FLASH_PARTITIONS:
        if _place_generated_partition(placed, physical_partition_name, flash_offset, partition_capacity, inputs):
            continue
        _place_payload_partition(placed, physical_partition_name, flash_offset, partition_capacity, inputs)
    if placed.bootloader_payload is None or placed.bootloader_metadata is None:
        raise RuntimeError("Bootloader payload was not produced")
    return placed


def _brooklyn2_image_manifest(
    inputs: Brooklyn2BuildInputs, placed: Brooklyn2PlacedFlash, flash_bytes: bytes
) -> dict[str, Any]:
    required_partition_ids = set(inputs.payload_partition_identifiers.values())
    unused_sections = [
        section for section in inputs.dnt_result["sections"] if section["partition_id"] not in required_partition_ids
    ]
    return {
        "format_version": BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
        "hardware_profile": _brooklyn2_hardware_profile_manifest(
            inputs.hardware_profile_input,
            inputs.evidence_assessment,
        ),
        "dnt": inputs.dnt_result,
        "source_sha256": inputs.firmware_sha256,
        "board_information": _brooklyn2_board_information_manifest(
            inputs.board_information_input,
            inputs.board_information_payload,
        ),
        "flash": {
            "filename": "flash.bin",
            "size": len(flash_bytes),
            "sha256": hashlib.sha256(flash_bytes).hexdigest(),
            "erased_byte": "ff",
            "physical_partitions": placed.physical_partition_records,
        },
        "bootloader": {
            "filename": "bootloader.bin",
            "size": len(placed.bootloader_payload),
            "sha256": hashlib.sha256(placed.bootloader_payload).hexdigest(),
            **placed.bootloader_metadata,
        },
        "payloads": placed.partition_records,
        "unused_dnt_sections": unused_sections,
    }


def _publish_brooklyn2_image(
    output_directory: Path,
    inputs: Brooklyn2BuildInputs,
    placed: Brooklyn2PlacedFlash,
    flash_bytes: bytes,
    manifest: dict[str, Any],
) -> None:
    if output_directory.name in {"", ".", ".."}:
        raise ValueError("Output directory must name a new child directory")
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    output_parent = output_directory.parent.resolve(strict=True)
    publication_directory = output_parent / output_directory.name
    temporary_directory = Path(tempfile.mkdtemp(prefix=f".{output_directory.name}.", dir=output_parent))
    try:
        _write_synchronized_file(temporary_directory / "flash.bin", flash_bytes)
        _write_synchronized_file(temporary_directory / "bootloader.bin", placed.bootloader_payload)
        _write_synchronized_file(temporary_directory / "brdinfo.bin", inputs.board_information_payload)
        if inputs.protected_capability_partition_payload is not None:
            _write_synchronized_file(temporary_directory / "cap.bin", inputs.protected_capability_partition_payload)
        _write_synchronized_file(temporary_directory / "manifest.json", manifest_bytes(manifest))
        _publish_output_directory_without_replacement(
            temporary_directory,
            publication_directory,
        )
    except BaseException:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise


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
    inputs = _load_brooklyn2_build_inputs(
        path, hardware_profile_path, board_information_path, trusted_evidence_root, protected_capability_partition_path
    )
    placed = _place_brooklyn2_partitions(inputs)
    flash_bytes = bytes(placed.flash_data)
    manifest = _brooklyn2_image_manifest(inputs, placed, flash_bytes)
    _publish_brooklyn2_image(output_directory, inputs, placed, flash_bytes, manifest)
    return manifest


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
