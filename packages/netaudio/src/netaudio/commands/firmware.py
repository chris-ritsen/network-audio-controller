from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import struct
import sys
import tempfile
import zlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal, Optional

import typer

app = typer.Typer(help="Analyze Dante firmware (.dnt) files.", no_args_is_help=True)

PARTITION_NAMES = {
    0: "all",
    1: "image",
    2: "fpga",
    3: "cap",
    4: "config",
    5: "_temp",
    6: "boot",
    7: "fpga96",
    8: "_psk",
    9: "cap1",
    10: "env",
    11: "user",
    12: "fpgar3",
    13: "safe",
    14: "capu",
    15: "flashlayout",
    16: "board",
    17: "_reserved",
    18: "cap2_manf",
    19: "switchphy",
    20: "data",
    21: "imxrt",
    22: "vcodec",
    23: "sii9777s",
    24: "cert",
    25: "vconfig",
    26: "tps65987d",
    27: "fpgar4",
}

CRAMFS_MAGIC_LE = b"\x45\x3d\xcd\x28"
CRAMFS_MAGIC_BE = b"\x28\xcd\x3d\x45"
GZIP_MAGIC = b"\x1f\x8b\x08"
DNT_PARSER_VERSION = 2
FIRMWARE_DATABASE_SCHEMA_VERSION = 2
UIMAGE_HEADER_SIZE = 64
CAPABILITY_9_DEVICE_DESCRIPTOR_SIZE = 0x1E
CAPABILITY_9_OEM_DESCRIPTOR_SIZE = 0x114
CAPABILITY_9_CHANNEL_NAME_SIZE = 32
BROOKLYN2_FLASH_SIZE = 0x800000
BROOKLYN2_FLASH_PARTITIONS = (
    ("safe", 0x000000, 0x150000),
    ("brdinfo", 0x150000, 0x010000),
    ("bootenv", 0x160000, 0x010000),
    ("boot", 0x170000, 0x030000),
    ("fpga", 0x1A0000, 0x100000),
    ("image", 0x2A0000, 0x320000),
    ("userarea", 0x5C0000, 0x200000),
    ("config", 0x7C0000, 0x020000),
    ("cap1", 0x7E0000, 0x010000),
    ("cap", 0x7F0000, 0x010000),
)
BROOKLYN2_PAYLOAD_PARTITION_NAMES = ("boot", "fpga", "image", "userarea", "cap1")
BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION = 2
BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION = 2
BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION = 4
BROOKLYN2_EVIDENCE_MANIFEST_MAXIMUM_SIZE = 4 * 1024 * 1024
BROOKLYN2_EVIDENCE_REQUEST_MAXIMUM_SIZE = 1024 * 1024
BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE = 64 * 1024 * 1024
BROOKLYN2_EVIDENCE_ARTIFACT_MAXIMUM_SIZE = 128 * 1024 * 1024
BROOKLYN2_EVIDENCE_ALL_ARTIFACTS_MAXIMUM_SIZE = 256 * 1024 * 1024
LINUX_CURRENT_WORKING_DIRECTORY_DESCRIPTOR = -100
LINUX_RENAME_WITHOUT_REPLACEMENT = 1
MACOS_RENAME_EXCLUSIVE = 4
(
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
) = next(
    (partition_offset, partition_size)
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS
    if partition_name == "brdinfo"
)
(
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
) = next(
    (partition_offset, partition_size)
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS
    if partition_name == "cap"
)


@dataclass(frozen=True)
class Brooklyn2BoardInformationDescriptor:
    format_version: int
    identity_name: str
    identity_kind: Literal["synthetic", "physical"]
    identity_provenance: Optional[str]
    media_access_control_address: bytes
    serial_number: int
    hardware_revision_major: int
    hardware_revision_minor: int
    configuration_date: bytes
    unidentified_prefix: bytes
    unidentified_prefix_provenance: Optional[str]


@dataclass(frozen=True)
class Brooklyn2BoardInformationInput:
    descriptor: Brooklyn2BoardInformationDescriptor
    source_sha256: str


@dataclass(frozen=True)
class Brooklyn2HardwareProfileSelectionEvidence:
    kind: Literal["isolated_firmware_upgrade"]
    run_name: str
    run_manifest_path: str
    run_manifest_sha256: str
    request_frame_path: str
    request_frame_sha256: str
    result_flash_path: str
    result_flash_sha256: str
    observed_fpga_partition_identifier: int
    observed_fpga_payload_sha256: str


@dataclass(frozen=True)
class Brooklyn2HardwareProfile:
    format_version: int
    profile_name: str
    device_type_identifier: int
    compatible_board_information_sha256: str
    compatible_firmware_sha256: str
    payload_partition_identifiers: tuple[tuple[str, int], ...]
    selection_scope: Literal["exact_board_information_and_firmware"]
    causal_selector_status: Literal["unknown"]
    selection_evidence: Brooklyn2HardwareProfileSelectionEvidence


@dataclass(frozen=True)
class Brooklyn2HardwareProfileInput:
    profile: Brooklyn2HardwareProfile
    source_sha256: str


@dataclass(frozen=True)
class VerifiedEvidenceFile:
    path: Path
    relative_path: PurePosixPath
    data: bytes
    size: int
    sha256: str


@dataclass(frozen=True)
class TrustedEvidenceRoot:
    path: Path
    file_descriptor: int


@dataclass(frozen=True)
class Brooklyn2HardwareProfileEvidenceAssessment:
    profile_name: str
    run_manifest_sha256: str
    request_frame_sha256: str
    result_flash_sha256: str


def _reject_duplicate_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value: str) -> object:
    raise ValueError(f"Non-standard JSON constant: {value}")


def _require_integer_field(data: dict[str, Any], field_name: str, maximum: int) -> int:
    value = data[field_name]
    if type(value) is not int:
        raise ValueError(f"Board-information field {field_name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(
            f"Board-information field {field_name} must be between 0 and {maximum}"
        )
    return value


def _require_provenance(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or value != value.strip()
        or not value.isprintable()
    ):
        raise ValueError(
            f"Board-information field {field_name} must be a non-empty printable source description"
        )
    return value


def _require_hardware_profile_integer(
    data: dict[str, Any], field_name: str, maximum: int
) -> int:
    value = data[field_name]
    if type(value) is not int:
        raise ValueError(f"Hardware-profile field {field_name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(
            f"Hardware-profile field {field_name} must be between 0 and {maximum}"
        )
    return value


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(
            f"Hardware-profile field {field_name} must be a lowercase SHA-256 digest"
        )
    return value


def _require_evidence_reference(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or not value.isprintable()
    ):
        raise ValueError(
            f"Hardware-profile field {field_name} must be a canonical relative POSIX path"
        )
    reference = PurePosixPath(value)
    if reference.is_absolute() or ".." in reference.parts or value != str(reference):
        raise ValueError(
            f"Hardware-profile field {field_name} must be a canonical relative POSIX path"
        )
    return value


def _load_brooklyn2_hardware_profile(path: Path) -> Brooklyn2HardwareProfileInput:
    profile_bytes = path.read_bytes()
    try:
        profile_text = profile_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"Hardware-profile descriptor is not UTF-8: {path}") from error
    try:
        data = json.loads(
            profile_text,
            object_pairs_hook=_reject_duplicate_json_fields,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid hardware-profile JSON: {error}") from error
    if not isinstance(data, dict):
        raise ValueError("Hardware-profile descriptor must be a JSON object")

    required_fields = {
        "format_version",
        "profile_name",
        "device_type_identifier",
        "compatible_board_information_sha256",
        "compatible_firmware_sha256",
        "payload_partition_identifiers",
        "selection_scope",
        "causal_selector_status",
        "selection_evidence",
    }
    actual_fields = set(data)
    missing_fields = sorted(required_fields - actual_fields)
    unexpected_fields = sorted(actual_fields - required_fields)
    if missing_fields or unexpected_fields:
        details = []
        if missing_fields:
            details.append(f"missing fields: {', '.join(missing_fields)}")
        if unexpected_fields:
            details.append(f"unexpected fields: {', '.join(unexpected_fields)}")
        raise ValueError(f"Invalid hardware-profile schema ({'; '.join(details)})")

    format_version = _require_hardware_profile_integer(
        data, "format_version", 0xFFFFFFFF
    )
    if format_version != BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION:
        raise ValueError(
            "Unsupported hardware-profile format version: "
            f"{format_version}; expected {BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION}"
        )

    profile_name = data["profile_name"]
    if not isinstance(profile_name, str) or not re.fullmatch(
        r"[a-z0-9]+(?:-[a-z0-9]+)*", profile_name
    ):
        raise ValueError(
            "Hardware-profile field profile_name must be a lowercase kebab-case name"
        )

    device_type_identifier = _require_hardware_profile_integer(
        data, "device_type_identifier", 0xFFFFFFFF
    )
    compatible_board_information_sha256 = _require_sha256(
        data["compatible_board_information_sha256"],
        "compatible_board_information_sha256",
    )
    compatible_firmware_sha256 = _require_sha256(
        data["compatible_firmware_sha256"],
        "compatible_firmware_sha256",
    )

    payload_partition_identifiers_data = data["payload_partition_identifiers"]
    if not isinstance(payload_partition_identifiers_data, dict):
        raise ValueError(
            "Hardware-profile field payload_partition_identifiers must be an object"
        )
    expected_payload_partition_names = set(BROOKLYN2_PAYLOAD_PARTITION_NAMES)
    actual_payload_partition_names = set(payload_partition_identifiers_data)
    missing_payload_partition_names = sorted(
        expected_payload_partition_names - actual_payload_partition_names
    )
    unexpected_payload_partition_names = sorted(
        actual_payload_partition_names - expected_payload_partition_names
    )
    if missing_payload_partition_names or unexpected_payload_partition_names:
        details = []
        if missing_payload_partition_names:
            details.append(
                f"missing partitions: {', '.join(missing_payload_partition_names)}"
            )
        if unexpected_payload_partition_names:
            details.append(
                f"unexpected partitions: {', '.join(unexpected_payload_partition_names)}"
            )
        raise ValueError(
            f"Invalid hardware-profile payload mapping ({'; '.join(details)})"
        )

    payload_partition_identifiers = []
    for physical_partition_name in BROOKLYN2_PAYLOAD_PARTITION_NAMES:
        partition_identifier = payload_partition_identifiers_data[
            physical_partition_name
        ]
        if type(partition_identifier) is not int:
            raise ValueError(
                "Hardware-profile payload partition identifier for "
                f"{physical_partition_name} must be an integer"
            )
        if partition_identifier not in PARTITION_NAMES or partition_identifier == 0:
            raise ValueError(
                "Hardware-profile payload partition identifier for "
                f"{physical_partition_name} is unsupported: {partition_identifier}"
            )
        payload_partition_identifiers.append(
            (physical_partition_name, partition_identifier)
        )
    partition_identifier_values = [
        partition_identifier
        for _, partition_identifier in payload_partition_identifiers
    ]
    if len(set(partition_identifier_values)) != len(partition_identifier_values):
        raise ValueError(
            "Hardware-profile payload partition identifiers must be unique"
        )

    selection_scope = data["selection_scope"]
    if selection_scope != "exact_board_information_and_firmware":
        raise ValueError(
            "Hardware-profile field selection_scope must be exact_board_information_and_firmware"
        )
    causal_selector_status = data["causal_selector_status"]
    if causal_selector_status != "unknown":
        raise ValueError(
            "Hardware-profile field causal_selector_status must be unknown"
        )

    selection_evidence_data = data["selection_evidence"]
    if not isinstance(selection_evidence_data, dict):
        raise ValueError("Hardware-profile field selection_evidence must be an object")
    required_evidence_fields = {
        "kind",
        "run_name",
        "run_manifest_path",
        "run_manifest_sha256",
        "request_frame_path",
        "request_frame_sha256",
        "result_flash_path",
        "result_flash_sha256",
        "observed_fpga_partition_identifier",
        "observed_fpga_payload_sha256",
    }
    actual_evidence_fields = set(selection_evidence_data)
    missing_evidence_fields = sorted(required_evidence_fields - actual_evidence_fields)
    unexpected_evidence_fields = sorted(
        actual_evidence_fields - required_evidence_fields
    )
    if missing_evidence_fields or unexpected_evidence_fields:
        details = []
        if missing_evidence_fields:
            details.append(f"missing fields: {', '.join(missing_evidence_fields)}")
        if unexpected_evidence_fields:
            details.append(
                f"unexpected fields: {', '.join(unexpected_evidence_fields)}"
            )
        raise ValueError(
            f"Invalid hardware-profile selection evidence ({'; '.join(details)})"
        )

    evidence_kind = selection_evidence_data["kind"]
    if evidence_kind != "isolated_firmware_upgrade":
        raise ValueError(
            "Hardware-profile selection evidence kind must be isolated_firmware_upgrade"
        )
    run_name = selection_evidence_data["run_name"]
    if not isinstance(run_name, str) or not re.fullmatch(
        r"[a-z0-9]+(?:-[a-z0-9]+)*", run_name
    ):
        raise ValueError(
            "Hardware-profile selection evidence run_name must be a lowercase kebab-case name"
        )
    observed_fpga_partition_identifier = selection_evidence_data[
        "observed_fpga_partition_identifier"
    ]
    if type(observed_fpga_partition_identifier) is not int:
        raise ValueError(
            "Hardware-profile selection evidence observed_fpga_partition_identifier must be an integer"
        )
    payload_partition_identifier_map = dict(payload_partition_identifiers)
    if observed_fpga_partition_identifier != payload_partition_identifier_map["fpga"]:
        raise ValueError(
            "Hardware-profile FPGA payload mapping does not match the observed selection evidence"
        )

    selection_evidence = Brooklyn2HardwareProfileSelectionEvidence(
        kind=evidence_kind,
        run_name=run_name,
        run_manifest_path=_require_evidence_reference(
            selection_evidence_data["run_manifest_path"],
            "selection_evidence.run_manifest_path",
        ),
        run_manifest_sha256=_require_sha256(
            selection_evidence_data["run_manifest_sha256"],
            "selection_evidence.run_manifest_sha256",
        ),
        request_frame_path=_require_evidence_reference(
            selection_evidence_data["request_frame_path"],
            "selection_evidence.request_frame_path",
        ),
        request_frame_sha256=_require_sha256(
            selection_evidence_data["request_frame_sha256"],
            "selection_evidence.request_frame_sha256",
        ),
        result_flash_path=_require_evidence_reference(
            selection_evidence_data["result_flash_path"],
            "selection_evidence.result_flash_path",
        ),
        result_flash_sha256=_require_sha256(
            selection_evidence_data["result_flash_sha256"],
            "selection_evidence.result_flash_sha256",
        ),
        observed_fpga_partition_identifier=observed_fpga_partition_identifier,
        observed_fpga_payload_sha256=_require_sha256(
            selection_evidence_data["observed_fpga_payload_sha256"],
            "selection_evidence.observed_fpga_payload_sha256",
        ),
    )

    return Brooklyn2HardwareProfileInput(
        profile=Brooklyn2HardwareProfile(
            format_version=format_version,
            profile_name=profile_name,
            device_type_identifier=device_type_identifier,
            compatible_board_information_sha256=compatible_board_information_sha256,
            compatible_firmware_sha256=compatible_firmware_sha256,
            payload_partition_identifiers=tuple(payload_partition_identifiers),
            selection_scope=selection_scope,
            causal_selector_status=causal_selector_status,
            selection_evidence=selection_evidence,
        ),
        source_sha256=hashlib.sha256(profile_bytes).hexdigest(),
    )


def _load_brooklyn2_board_information_descriptor(
    path: Path,
) -> Brooklyn2BoardInformationInput:
    descriptor_bytes = path.read_bytes()
    try:
        descriptor_text = descriptor_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Board-information descriptor is not UTF-8: {path}"
        ) from error
    try:
        data = json.loads(
            descriptor_text,
            object_pairs_hook=_reject_duplicate_json_fields,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid board-information JSON: {error}") from error
    if not isinstance(data, dict):
        raise ValueError("Board-information descriptor must be a JSON object")

    required_fields = {
        "format_version",
        "identity_name",
        "identity_kind",
        "media_access_control_address",
        "serial_number",
        "hardware_revision_major",
        "hardware_revision_minor",
        "configuration_date",
    }
    optional_fields = {
        "identity_provenance",
        "unidentified_prefix_hex",
        "unidentified_prefix_provenance",
    }
    actual_fields = set(data)
    missing_fields = sorted(required_fields - actual_fields)
    unexpected_fields = sorted(actual_fields - required_fields - optional_fields)
    if missing_fields or unexpected_fields:
        details = []
        if missing_fields:
            details.append(f"missing fields: {', '.join(missing_fields)}")
        if unexpected_fields:
            details.append(f"unexpected fields: {', '.join(unexpected_fields)}")
        raise ValueError(f"Invalid board-information schema ({'; '.join(details)})")

    format_version = _require_integer_field(data, "format_version", 0xFFFFFFFF)
    if format_version != BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION:
        raise ValueError(
            "Unsupported board-information format version: "
            f"{format_version}; expected {BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION}"
        )

    identity_kind = data["identity_kind"]
    if identity_kind not in ("synthetic", "physical"):
        raise ValueError(
            "Board-information field identity_kind must be synthetic or physical"
        )
    identity_provenance_value = data.get("identity_provenance")
    if identity_kind == "physical":
        identity_provenance = _require_provenance(
            identity_provenance_value, "identity_provenance"
        )
    else:
        if identity_provenance_value is not None:
            raise ValueError(
                "Board-information field identity_provenance is only valid for physical identity"
            )
        identity_provenance = None

    identity_name = data["identity_name"]
    if not isinstance(identity_name, str) or not re.fullmatch(
        r"[a-z0-9]+(?:-[a-z0-9]+)*", identity_name
    ):
        raise ValueError(
            "Board-information field identity_name must be a lowercase kebab-case name"
        )

    media_access_control_address_text = data["media_access_control_address"]
    if not isinstance(media_access_control_address_text, str) or not re.fullmatch(
        r"[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}", media_access_control_address_text
    ):
        raise ValueError(
            "Board-information media_access_control_address must contain six hexadecimal octets"
        )
    media_access_control_address = bytes.fromhex(
        media_access_control_address_text.replace(":", "")
    )
    if media_access_control_address[0] & 1:
        raise ValueError(
            "Board-information media_access_control_address must be unicast"
        )
    if not any(media_access_control_address):
        raise ValueError(
            "Board-information media_access_control_address must not be all zeroes"
        )
    if identity_kind == "synthetic" and not media_access_control_address[0] & 2:
        raise ValueError(
            "Synthetic board information requires a locally administered media_access_control_address"
        )

    serial_number = _require_integer_field(data, "serial_number", 0xFFFFFFFF)
    if identity_kind == "synthetic" and serial_number > 0x7FFFFFFF:
        raise ValueError(
            "Synthetic board-information serial_number must not exceed 2147483647"
        )

    configuration_date_text = data["configuration_date"]
    if not isinstance(configuration_date_text, str) or not re.fullmatch(
        r"[0-9]{8}", configuration_date_text
    ):
        raise ValueError(
            "Board-information field configuration_date must contain exactly 8 ASCII decimal digits"
        )
    configuration_date = configuration_date_text.encode("ascii")

    unidentified_prefix_hex = data.get("unidentified_prefix_hex", "ff" * 10)
    if not isinstance(unidentified_prefix_hex, str) or not re.fullmatch(
        r"[0-9a-fA-F]{20}", unidentified_prefix_hex
    ):
        raise ValueError(
            "Board-information field unidentified_prefix_hex must encode exactly 10 bytes"
        )
    unidentified_prefix = bytes.fromhex(unidentified_prefix_hex)
    unidentified_prefix_provenance = data.get("unidentified_prefix_provenance")
    if unidentified_prefix == bytes([0xFF]) * 10:
        if unidentified_prefix_provenance is not None:
            raise ValueError(
                "Board-information field unidentified_prefix_provenance is only valid for a non-erased prefix"
            )
    else:
        if identity_kind != "physical":
            raise ValueError(
                "A non-erased unidentified_prefix_hex is only supported for physical identity"
            )
        unidentified_prefix_provenance = _require_provenance(
            unidentified_prefix_provenance,
            "unidentified_prefix_provenance",
        )

    return Brooklyn2BoardInformationInput(
        descriptor=Brooklyn2BoardInformationDescriptor(
            format_version=format_version,
            identity_name=identity_name,
            identity_kind=identity_kind,
            identity_provenance=identity_provenance,
            media_access_control_address=media_access_control_address,
            serial_number=serial_number,
            hardware_revision_major=_require_integer_field(
                data, "hardware_revision_major", 0xFFFF
            ),
            hardware_revision_minor=_require_integer_field(
                data, "hardware_revision_minor", 0xFFFF
            ),
            configuration_date=configuration_date,
            unidentified_prefix=unidentified_prefix,
            unidentified_prefix_provenance=unidentified_prefix_provenance,
        ),
        source_sha256=hashlib.sha256(descriptor_bytes).hexdigest(),
    )


def _build_brooklyn2_board_information_partition(
    descriptor: Brooklyn2BoardInformationDescriptor,
) -> bytes:
    partition = bytearray([0xFF]) * BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE
    partition[0:0x0A] = descriptor.unidentified_prefix
    partition[0x0A:0x10] = descriptor.media_access_control_address
    struct.pack_into(">I", partition, 0x10, descriptor.serial_number)
    struct.pack_into(">H", partition, 0x14, descriptor.hardware_revision_major)
    struct.pack_into(">H", partition, 0x16, descriptor.hardware_revision_minor)
    partition[0x18:0x20] = descriptor.configuration_date
    return bytes(partition)


def _brooklyn2_board_information_manifest(
    board_information_input: Brooklyn2BoardInformationInput,
    partition: bytes,
) -> dict[str, Any]:
    descriptor = board_information_input.descriptor
    media_access_control_address = ":".join(
        f"{octet:02x}" for octet in descriptor.media_access_control_address
    )
    return {
        "state": "generated",
        "filename": "brdinfo.bin",
        "flash_offset": BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
        "size": len(partition),
        "sha256": hashlib.sha256(partition).hexdigest(),
        "descriptor": {
            "format_version": descriptor.format_version,
            "source_sha256": board_information_input.source_sha256,
        },
        "identity_name": descriptor.identity_name,
        "identity_kind": descriptor.identity_kind,
        "identity_provenance": descriptor.identity_provenance,
        "identity_assertion_scope": "known_fields_only",
        "generated_partition_is_physical_dump": False,
        "known_fields": {
            "media_access_control_address": media_access_control_address,
            "media_access_control_address_is_locally_administered": bool(
                descriptor.media_access_control_address[0] & 2
            ),
            "serial_number": descriptor.serial_number,
            "bootloader_signed_serial_number": (
                descriptor.serial_number
                if descriptor.serial_number <= 0x7FFFFFFF
                else descriptor.serial_number - 0x100000000
            ),
            "hardware_revision_major": descriptor.hardware_revision_major,
            "hardware_revision_minor": descriptor.hardware_revision_minor,
            "configuration_date": descriptor.configuration_date.decode("ascii"),
        },
        "evidence_limited_unknowns": [
            {
                "offset": 0,
                "size": len(descriptor.unidentified_prefix),
                "value_hex": descriptor.unidentified_prefix.hex(),
                "provenance": descriptor.unidentified_prefix_provenance,
                "evidence": (
                    "Retained at the erased value because the field semantics are unknown"
                    if descriptor.unidentified_prefix_provenance is None
                    else "Descriptor-supplied opaque bytes with explicit provenance; field semantics are unknown"
                ),
            },
            {
                "offset": 0x20,
                "size": 4,
                "value_hex": "ffffffff",
                "evidence": (
                    "Retained at the erased value because an in-memory PTP default does not establish a flash value"
                ),
            },
            {
                "offset": 0x44,
                "size": 1,
                "value_hex": "ff",
                "evidence": "Retained at the erased value because its semantics and required value are unknown",
            },
        ],
    }


def _brooklyn2_hardware_profile_manifest(
    hardware_profile_input: Brooklyn2HardwareProfileInput,
    evidence_assessment: Brooklyn2HardwareProfileEvidenceAssessment,
) -> dict[str, Any]:
    profile = hardware_profile_input.profile
    evidence = profile.selection_evidence
    if evidence_assessment != Brooklyn2HardwareProfileEvidenceAssessment(
        profile_name=profile.profile_name,
        run_manifest_sha256=evidence.run_manifest_sha256,
        request_frame_sha256=evidence.request_frame_sha256,
        result_flash_sha256=evidence.result_flash_sha256,
    ):
        raise RuntimeError(
            "Hardware-profile evidence verification does not match the selected profile"
        )
    return {
        "profile_name": profile.profile_name,
        "descriptor": {
            "format_version": profile.format_version,
            "source_sha256": hardware_profile_input.source_sha256,
        },
        "device_type_identifier": profile.device_type_identifier,
        "compatible_board_information_sha256": profile.compatible_board_information_sha256,
        "compatible_firmware_sha256": profile.compatible_firmware_sha256,
        "payload_partition_identifiers": dict(profile.payload_partition_identifiers),
        "selection_scope": profile.selection_scope,
        "causal_selector_status": profile.causal_selector_status,
        "evidence_assessment": {
            "artifact_integrity": "verified_against_digest_bound_run_manifest",
            "compatible_firmware_identity": "verified_by_content",
            "result_flash_binding": "verified_by_content",
            "selected_fpga_payload": "verified_by_content",
            "compatible_board_information": "verified_by_content",
            "run_manifest_claims": {
                "completion": "asserted_by_digest_bound_run_manifest",
                "network_isolation": "asserted_by_digest_bound_run_manifest",
                "source_flash_immutability": "asserted_by_digest_bound_run_manifest",
                "staged_firmware_immutability": "asserted_by_digest_bound_run_manifest",
            },
            "transaction_semantics": "not_verified_by_image_builder",
        },
        "selection_evidence": {
            "kind": evidence.kind,
            "run_name": evidence.run_name,
            "run_manifest_sha256": evidence.run_manifest_sha256,
            "request_frame_sha256": evidence.request_frame_sha256,
            "result_flash_sha256": evidence.result_flash_sha256,
            "observed_fpga_partition_identifier": evidence.observed_fpga_partition_identifier,
            "observed_fpga_payload_sha256": evidence.observed_fpga_payload_sha256,
        },
    }


def _require_evidence_mapping(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be a JSON object")
    return value


def _require_evidence_file_record(value: Any, context: str) -> dict[str, Any]:
    record = _require_evidence_mapping(value, context)
    if set(record) != {"path", "sha256", "size"}:
        raise ValueError(f"{context} must contain exactly path, sha256, and size")
    if not isinstance(record["path"], str) or not record["path"]:
        raise ValueError(f"{context} path is invalid")
    if not isinstance(record["sha256"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", record["sha256"]
    ):
        raise ValueError(f"{context} SHA-256 is invalid")
    if type(record["size"]) is not int or record["size"] < 0:
        raise ValueError(f"{context} size is invalid")
    return record


def _directory_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _file_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _open_trusted_evidence_root(path: Path) -> TrustedEvidenceRoot:
    absolute_path = Path(os.path.abspath(os.fspath(path)))
    components = PurePosixPath(absolute_path.as_posix()).parts
    if not components or components[0] != "/":
        raise ValueError(f"Trusted evidence root is not absolute: {absolute_path}")
    try:
        directory_descriptor = os.open("/", _directory_open_flags())
    except OSError as error:
        raise ValueError("Trusted evidence filesystem root cannot be opened") from error
    try:
        for component in components[1:]:
            try:
                next_descriptor = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=directory_descriptor,
                )
            except OSError as error:
                raise ValueError(
                    f"Trusted evidence root contains an inaccessible or symbolic-link component: {absolute_path}"
                ) from error
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
        opened_status = os.fstat(directory_descriptor)
        if not stat.S_ISDIR(opened_status.st_mode):
            raise ValueError(
                f"Trusted evidence root is not a directory: {absolute_path}"
            )
        return TrustedEvidenceRoot(
            path=absolute_path,
            file_descriptor=directory_descriptor,
        )
    except Exception:
        os.close(directory_descriptor)
        raise


def _read_stable_bounded_file_descriptor(
    file_descriptor: int,
    parent_directory_descriptor: int,
    filename: str,
    path: Path,
    relative_path: PurePosixPath,
    maximum_size: int,
    context: str,
) -> VerifiedEvidenceFile:
    try:
        initial_status = os.fstat(file_descriptor)
    except OSError as error:
        raise ValueError(f"{context} cannot be inspected") from error
    if not stat.S_ISREG(initial_status.st_mode):
        raise ValueError(f"{context} is not a regular file")
    if initial_status.st_size > maximum_size:
        raise ValueError(f"{context} exceeds the {maximum_size}-byte size limit")
    chunks = []
    remaining = maximum_size + 1
    while remaining:
        chunk = os.read(file_descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    final_status = os.fstat(file_descriptor)
    if len(data) > maximum_size:
        raise ValueError(f"{context} exceeds the {maximum_size}-byte size limit")
    try:
        current_status = os.stat(
            filename,
            dir_fd=parent_directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise ValueError(f"{context} changed while it was read") from error
    stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(
        getattr(initial_status, field) != getattr(final_status, field)
        or getattr(final_status, field) != getattr(current_status, field)
        for field in stable_fields
    ):
        raise ValueError(f"{context} changed while it was read")
    if len(data) != initial_status.st_size:
        raise ValueError(f"{context} size changed while it was read")
    return VerifiedEvidenceFile(
        path=path,
        relative_path=relative_path,
        data=data,
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


def _resolve_trusted_evidence_file(
    trusted_evidence_root: TrustedEvidenceRoot,
    reference: str,
    relative_base: PurePosixPath,
    maximum_size: int,
    context: str,
) -> VerifiedEvidenceFile:
    if (
        not isinstance(reference, str)
        or not reference
        or len(reference) > 4096
        or "\x00" in reference
        or not reference.isprintable()
    ):
        raise ValueError(f"{context} path reference is invalid")
    if "\\" in reference:
        raise ValueError(f"{context} path reference is not POSIX")
    reference_path = PurePosixPath(reference)
    if ".." in reference_path.parts:
        raise ValueError(f"{context} path reference contains parent traversal")
    if reference_path.is_absolute():
        try:
            relative_path = reference_path.relative_to(
                PurePosixPath(trusted_evidence_root.path.as_posix())
            )
        except ValueError as error:
            raise ValueError(
                f"{context} resolves outside the trusted evidence root"
            ) from error
    else:
        relative_path = relative_base.joinpath(*reference_path.parts)
    if (
        relative_path.is_absolute()
        or not relative_path.parts
        or any(component in {"", ".", ".."} for component in relative_path.parts)
    ):
        raise ValueError(f"{context} path reference is invalid")
    directory_descriptor = os.dup(trusted_evidence_root.file_descriptor)
    try:
        for component in relative_path.parts[:-1]:
            try:
                next_descriptor = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=directory_descriptor,
                )
            except OSError as error:
                raise ValueError(
                    f"{context} contains an inaccessible or symbolic-link directory"
                ) from error
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
        filename = relative_path.parts[-1]
        try:
            file_descriptor = os.open(
                filename,
                _file_open_flags(),
                dir_fd=directory_descriptor,
            )
        except OSError as error:
            raise ValueError(f"{context} is inaccessible or a symbolic link") from error
        try:
            return _read_stable_bounded_file_descriptor(
                file_descriptor,
                directory_descriptor,
                filename,
                trusted_evidence_root.path.joinpath(*relative_path.parts),
                relative_path,
                maximum_size,
                context,
            )
        finally:
            os.close(file_descriptor)
    finally:
        os.close(directory_descriptor)


def _load_duplicate_safe_json(data: bytes, context: str) -> dict[str, Any]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{context} is not UTF-8") from error
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_fields,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"{context} is invalid JSON: {error}") from error
    return _require_evidence_mapping(value, context)


def _verify_declared_evidence_file(
    record: dict[str, Any],
    artifact: VerifiedEvidenceFile,
    context: str,
) -> None:
    if record["size"] != artifact.size:
        raise ValueError(f"{context} declared size does not match the evidence file")
    if record["sha256"] != artifact.sha256:
        raise ValueError(f"{context} declared SHA-256 does not match the evidence file")


def _verify_brooklyn2_hardware_profile_evidence_from_root(
    hardware_profile: Brooklyn2HardwareProfile,
    trusted_evidence_root: TrustedEvidenceRoot,
    firmware_data: bytes,
    firmware_result: dict[str, Any],
    board_information_payload: bytes,
) -> Brooklyn2HardwareProfileEvidenceAssessment:
    evidence = hardware_profile.selection_evidence
    run_manifest_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        evidence.run_manifest_path,
        PurePosixPath(),
        BROOKLYN2_EVIDENCE_MANIFEST_MAXIMUM_SIZE,
        "Upgrade evidence run manifest",
    )
    if run_manifest_artifact.sha256 != evidence.run_manifest_sha256:
        raise ValueError(
            "Upgrade evidence run manifest SHA-256 does not match the hardware profile"
        )
    if (
        run_manifest_artifact.path.name != "manifest.json"
        or run_manifest_artifact.path.parent.name != evidence.run_name
    ):
        raise ValueError(
            "Upgrade evidence run manifest path does not match the declared run name"
        )
    run_manifest = _load_duplicate_safe_json(
        run_manifest_artifact.data, "Upgrade evidence run manifest"
    )
    if run_manifest.get("schema_version") != 1:
        raise ValueError("Upgrade evidence run manifest schema version is unsupported")
    if run_manifest.get("state") != "completed":
        raise ValueError("Upgrade evidence run did not complete")
    if (
        type(run_manifest.get("process_returncode")) is not int
        or run_manifest["process_returncode"] != 0
    ):
        raise ValueError("Upgrade evidence run process did not exit successfully")

    network = _require_evidence_mapping(
        run_manifest.get("network"), "Upgrade evidence network record"
    )
    if (
        network.get("transport") != "isolated-hub"
        or network.get("live_interface_exposure") is not False
    ):
        raise ValueError("Upgrade evidence run was not isolated from live interfaces")
    if network.get("tftp_export_enabled") is not True:
        raise ValueError("Upgrade evidence run did not use its isolated TFTP export")

    run_directory = run_manifest_artifact.relative_path.parent
    artifacts = _require_evidence_mapping(
        run_manifest.get("artifacts"), "Upgrade evidence artifact table"
    )
    if len(artifacts) > 1024:
        raise ValueError("Upgrade evidence artifact table contains too many entries")
    verified_artifacts: dict[str, VerifiedEvidenceFile] = {}
    total_artifact_size = 0
    for artifact_name, artifact_value in artifacts.items():
        canonical_name = _require_evidence_reference(
            artifact_name, "upgrade_evidence.artifact_name"
        )
        artifact_record = _require_evidence_file_record(
            artifact_value,
            f"Upgrade evidence artifact {canonical_name}",
        )
        total_artifact_size += artifact_record["size"]
        if total_artifact_size > BROOKLYN2_EVIDENCE_ALL_ARTIFACTS_MAXIMUM_SIZE:
            raise ValueError(
                "Upgrade evidence artifact table exceeds its aggregate size limit"
            )
        artifact = _resolve_trusted_evidence_file(
            trusted_evidence_root,
            artifact_record["path"],
            run_directory,
            BROOKLYN2_EVIDENCE_ARTIFACT_MAXIMUM_SIZE,
            f"Upgrade evidence artifact {canonical_name}",
        )
        expected_artifact_path = run_directory.joinpath(
            *PurePosixPath(canonical_name).parts
        )
        if artifact.relative_path != expected_artifact_path:
            raise ValueError(
                f"Upgrade evidence artifact {canonical_name} path does not match its table key"
            )
        _verify_declared_evidence_file(
            artifact_record, artifact, f"Upgrade evidence artifact {canonical_name}"
        )
        verified_artifacts[canonical_name] = artifact

    stimulus_log = _require_evidence_mapping(
        run_manifest.get("stimulus_log"), "Upgrade evidence stimulus log"
    )
    if stimulus_log.get("count") != 1:
        raise ValueError(
            "Upgrade evidence stimulus log must contain exactly one request frame"
        )
    request_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        evidence.request_frame_path,
        PurePosixPath(),
        BROOKLYN2_EVIDENCE_REQUEST_MAXIMUM_SIZE,
        "Upgrade evidence request frame",
    )
    if request_artifact.sha256 != evidence.request_frame_sha256:
        raise ValueError(
            "Upgrade evidence request frame SHA-256 does not match the hardware profile"
        )
    stimulus_data_path = _require_evidence_reference(
        stimulus_log.get("data_path"),
        "upgrade_evidence.stimulus_log.data_path",
    )
    if (
        stimulus_data_path not in verified_artifacts
        or verified_artifacts[stimulus_data_path].relative_path
        != request_artifact.relative_path
    ):
        raise ValueError(
            "Upgrade evidence request frame is not bound to the stimulus artifact table"
        )
    stimulus_index_path = _require_evidence_reference(
        stimulus_log.get("index_path"),
        "upgrade_evidence.stimulus_log.index_path",
    )
    if stimulus_index_path not in verified_artifacts:
        raise ValueError(
            "Upgrade evidence stimulus index is absent from the artifact table"
        )

    tftp_export = _require_evidence_mapping(
        run_manifest.get("tftp_export"), "Upgrade evidence TFTP export"
    )
    if (
        tftp_export.get("schema_version") != 1
        or tftp_export.get("verified") is not True
    ):
        raise ValueError(
            "Upgrade evidence TFTP export is not verified schema version 1"
        )
    if tftp_export.get("staged_unchanged") is not True:
        raise ValueError("Upgrade evidence staged firmware was not immutable")
    maximum_size = tftp_export.get("maximum_size")
    if (
        type(maximum_size) is not int
        or maximum_size < len(firmware_data)
        or maximum_size > BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE
    ):
        raise ValueError("Upgrade evidence TFTP size bound is invalid")
    tftp_artifacts = []
    for record_name in ("source", "staged", "staged_final"):
        record = _require_evidence_file_record(
            tftp_export.get(record_name),
            f"Upgrade evidence TFTP {record_name}",
        )
        artifact = _resolve_trusted_evidence_file(
            trusted_evidence_root,
            record["path"],
            run_directory,
            BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE,
            f"Upgrade evidence TFTP {record_name}",
        )
        _verify_declared_evidence_file(
            record, artifact, f"Upgrade evidence TFTP {record_name}"
        )
        if (
            artifact.sha256 != hardware_profile.compatible_firmware_sha256
            or artifact.data != firmware_data
        ):
            raise ValueError(
                f"Upgrade evidence TFTP {record_name} does not match the compatible firmware"
            )
        tftp_artifacts.append(artifact)
    if tftp_artifacts[1].relative_path != tftp_artifacts[2].relative_path:
        raise ValueError(
            "Upgrade evidence staged and final staged firmware paths differ"
        )
    staged_reference = tftp_export["staged"]["path"]
    if not Path(staged_reference).is_absolute():
        staged_artifact_name = str(PurePosixPath(staged_reference))
        if (
            staged_artifact_name not in verified_artifacts
            or verified_artifacts[staged_artifact_name].relative_path
            != tftp_artifacts[1].relative_path
        ):
            raise ValueError(
                "Upgrade evidence staged firmware is not bound to the artifact table"
            )

    flash = _require_evidence_mapping(
        run_manifest.get("flash"), "Upgrade evidence flash record"
    )
    result_flash_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        evidence.result_flash_path,
        PurePosixPath(),
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence result flash",
    )
    if (
        result_flash_artifact.size != BROOKLYN2_FLASH_SIZE
        or result_flash_artifact.sha256 != evidence.result_flash_sha256
    ):
        raise ValueError(
            "Upgrade evidence result flash does not match the hardware profile"
        )
    runtime_final_record = _require_evidence_file_record(
        flash.get("runtime_final"),
        "Upgrade evidence final runtime flash",
    )
    runtime_final_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        runtime_final_record["path"],
        run_directory,
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence final runtime flash",
    )
    _verify_declared_evidence_file(
        runtime_final_record,
        runtime_final_artifact,
        "Upgrade evidence final runtime flash",
    )
    if runtime_final_artifact.relative_path != result_flash_artifact.relative_path:
        raise ValueError(
            "Upgrade evidence result flash is not bound to the run manifest"
        )
    if flash.get("source_unchanged") is not True:
        raise ValueError("Upgrade evidence source flash was not immutable")
    source_initial_record = _require_evidence_file_record(
        flash.get("source_initial"), "Upgrade evidence initial source flash"
    )
    source_final_record = _require_evidence_file_record(
        flash.get("source_final"), "Upgrade evidence final source flash"
    )
    source_initial_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        source_initial_record["path"],
        run_directory,
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence initial source flash",
    )
    source_final_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        source_final_record["path"],
        run_directory,
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence final source flash",
    )
    _verify_declared_evidence_file(
        source_initial_record,
        source_initial_artifact,
        "Upgrade evidence initial source flash",
    )
    _verify_declared_evidence_file(
        source_final_record,
        source_final_artifact,
        "Upgrade evidence final source flash",
    )
    if (
        source_initial_artifact.relative_path != source_final_artifact.relative_path
        or source_initial_record != source_final_record
    ):
        raise ValueError("Upgrade evidence source flash changed during the run")
    runtime_initial_record = _require_evidence_file_record(
        flash.get("runtime_initial"), "Upgrade evidence initial runtime flash"
    )
    runtime_initial_path = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        runtime_initial_record["path"],
        run_directory,
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence initial runtime flash path",
    )
    if runtime_initial_path.relative_path != result_flash_artifact.relative_path:
        raise ValueError("Upgrade evidence runtime flash path changed during the run")
    if (
        runtime_initial_record["sha256"] != source_initial_record["sha256"]
        or runtime_initial_record["size"] != source_initial_record["size"]
    ):
        raise ValueError(
            "Upgrade evidence initial runtime flash is not bound to the immutable source flash"
        )

    matching_fpga_sections = [
        section
        for section in firmware_result["sections"]
        if section["partition_id"] == evidence.observed_fpga_partition_identifier
    ]
    if len(matching_fpga_sections) != 1:
        raise ValueError(
            "Upgrade evidence selected FPGA partition is not unique in the compatible firmware"
        )
    fpga_section = matching_fpga_sections[0]
    fpga_payload = firmware_data[
        fpga_section["file_offset"] : fpga_section["file_offset"] + fpga_section["size"]
    ]
    if (
        hashlib.sha256(fpga_payload).hexdigest()
        != evidence.observed_fpga_payload_sha256
    ):
        raise ValueError(
            "Upgrade evidence selected FPGA payload SHA-256 is inconsistent with the compatible firmware"
        )
    fpga_offset = next(
        partition_offset
        for partition_name, partition_offset, _ in BROOKLYN2_FLASH_PARTITIONS
        if partition_name == "fpga"
    )
    if (
        result_flash_artifact.data[fpga_offset : fpga_offset + len(fpga_payload)]
        != fpga_payload
    ):
        raise ValueError(
            "Upgrade evidence result flash does not contain the selected FPGA payload"
        )
    board_information_end = BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET + len(
        board_information_payload
    )
    if (
        result_flash_artifact.data[
            BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET:board_information_end
        ]
        != board_information_payload
    ):
        raise ValueError(
            "Upgrade evidence result flash is not bound to the compatible board information"
        )
    return Brooklyn2HardwareProfileEvidenceAssessment(
        profile_name=hardware_profile.profile_name,
        run_manifest_sha256=evidence.run_manifest_sha256,
        request_frame_sha256=evidence.request_frame_sha256,
        result_flash_sha256=evidence.result_flash_sha256,
    )


def _verify_brooklyn2_hardware_profile_evidence(
    hardware_profile: Brooklyn2HardwareProfile,
    trusted_evidence_root_path: Path,
    firmware_data: bytes,
    firmware_result: dict[str, Any],
    board_information_payload: bytes,
) -> Brooklyn2HardwareProfileEvidenceAssessment:
    trusted_evidence_root = _open_trusted_evidence_root(trusted_evidence_root_path)
    try:
        return _verify_brooklyn2_hardware_profile_evidence_from_root(
            hardware_profile,
            trusted_evidence_root,
            firmware_data,
            firmware_result,
            board_information_payload,
        )
    finally:
        os.close(trusted_evidence_root.file_descriptor)


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

    data_size, load_address, entry_point, stored_data_checksum = struct.unpack_from(
        ">IIII", payload, 12
    )
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
    if (
        payload[28] != 5
        or payload[29] != 14
        or payload[30] != expected_image_type
        or payload[31] != 0
    ):
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
        raise ValueError(
            f"Invalid capability size: header declares {declared_size}, file has {len(payload)}"
        )
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
        raise ValueError(
            f"Invalid CramFS size: header declares {declared_size}, file has {len(payload)}"
        )
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
        raise ValueError(
            "Brooklyn II kernel payload is too short for its embedded root filesystem"
        )
    rootfs_size = struct.unpack_from("<I", kernel_data, rootfs_offset + 4)[0]
    rootfs_end = rootfs_offset + rootfs_size
    if rootfs_end + 4 != len(kernel_data):
        raise ValueError(
            "Brooklyn II kernel payload does not end with a four-byte checksum after its root filesystem"
        )
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


def _validate_fpga_payload(payload):
    if (
        len(payload) < 20
        or payload[:16] != bytes([0xFF]) * 16
        or payload[16:20] != b"\xaa\x99\x55\x66"
    ):
        raise ValueError("Invalid Spartan-6 bitstream prefix")
    if len(payload) % 2:
        raise ValueError("Spartan-6 bitstream has an odd byte count")

    word_offset = 20
    packet_count = 0
    type_two_payloads = []
    idcodes = []
    explicit_checksums = []
    commands = []
    while word_offset < len(payload):
        packet_offset = word_offset
        header = struct.unpack_from(">H", payload, word_offset)[0]
        word_offset += 2
        packet_type = header >> 13
        operation = (header >> 11) & 3
        register = (header >> 5) & 0x3F
        packet_count += 1

        if packet_type == 1:
            word_count = header & 0x1F
            data_end = word_offset + word_count * 2
            if data_end > len(payload):
                raise ValueError(
                    f"Truncated Spartan-6 type-1 packet at 0x{packet_offset:x}"
                )
            data_words = (
                struct.unpack_from(f">{word_count}H", payload, word_offset)
                if word_count
                else ()
            )
            word_offset = data_end
            if operation == 2 and register == 14 and word_count == 2:
                idcodes.append((data_words[0] << 16) | data_words[1])
            elif operation == 2 and register == 0 and word_count == 2:
                explicit_checksums.append((data_words[0] << 16) | data_words[1])
            elif operation == 2 and register == 5 and word_count == 1:
                commands.append(data_words[0])
        elif packet_type == 2:
            if header & 0x1F:
                raise ValueError(
                    f"Invalid Spartan-6 type-2 header at 0x{packet_offset:x}"
                )
            if word_offset + 4 > len(payload):
                raise ValueError(
                    f"Truncated Spartan-6 type-2 count at 0x{packet_offset:x}"
                )
            count_high, count_low = struct.unpack_from(">HH", payload, word_offset)
            word_offset += 4
            word_count = (count_high << 16) | count_low
            data_end = word_offset + word_count * 2
            if data_end > len(payload):
                raise ValueError(
                    f"Truncated Spartan-6 type-2 payload at 0x{packet_offset:x}"
                )
            word_offset = data_end
            if operation == 2:
                if word_offset + 4 > len(payload):
                    raise ValueError(
                        f"Missing Spartan-6 automatic CRC at 0x{packet_offset:x}"
                    )
                automatic_checksum = struct.unpack_from(">I", payload, word_offset)[0]
                word_offset += 4
                type_two_payloads.append(
                    {
                        "packet_offset": packet_offset,
                        "word_count": word_count,
                        "automatic_crc32": f"{automatic_checksum:08x}",
                    }
                )
        else:
            raise ValueError(
                f"Unsupported Spartan-6 packet type {packet_type} at 0x{packet_offset:x}"
            )

    if not idcodes or any(idcode != 0x04004093 for idcode in idcodes):
        raise ValueError(
            f"Unexpected Spartan-6 IDCODE values: {[f'{idcode:08x}' for idcode in idcodes]}"
        )
    if not type_two_payloads:
        raise ValueError("Spartan-6 bitstream contains no type-2 FDRI payload")
    if not explicit_checksums:
        raise ValueError("Spartan-6 bitstream contains no explicit CRC write")
    if not commands or commands[-1] != 13:
        raise ValueError(
            "Spartan-6 bitstream does not end its command sequence with DESYNC"
        )

    return {
        "sync_word_offset": 16,
        "packet_count": packet_count,
        "idcodes": [f"{idcode:08x}" for idcode in idcodes],
        "type_two_payloads": type_two_payloads,
        "explicit_crc32_writes": [f"{checksum:08x}" for checksum in explicit_checksums],
        "commands": commands,
    }


def _validate_brooklyn2_flash_layout():
    expected_offset = 0
    for partition_name, partition_offset, partition_size in BROOKLYN2_FLASH_PARTITIONS:
        if partition_offset != expected_offset:
            raise RuntimeError(
                f"Brooklyn II flash layout is discontinuous before {partition_name}"
            )
        expected_offset += partition_size
    if expected_offset != BROOKLYN2_FLASH_SIZE:
        raise RuntimeError(f"Brooklyn II flash layout ends at 0x{expected_offset:x}")


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
        raise ValueError(
            f"{field_name} extends past capability payload: {offset + size} > {len(payload)}"
        )


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

    transmit_channel_count, receive_channel_count = struct.unpack_from(
        ">HH", payload, device_descriptor_offset + 8
    )
    transmit_channel_names_offset, receive_channel_names_offset = struct.unpack_from(
        ">HH", payload, device_descriptor_offset + 0x10
    )
    oem_descriptor_offset = struct.unpack_from(
        ">H", payload, device_descriptor_offset + 0x1C
    )[0]

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
        raise ValueError(
            f"DNT manufacturer header must contain 8 bytes, received {len(manufacturer_header)}"
        )

    layout = _parse_capability_9_layout(cfg)
    device_descriptor_offset = layout["device_descriptor_offset"]
    oem_descriptor_offset = layout["oem_descriptor_offset"]
    manufacturer_short_bytes = cfg[
        oem_descriptor_offset + 4 : oem_descriptor_offset + 12
    ]
    if manufacturer_short_bytes != manufacturer_header:
        raise ValueError(
            "Capability OEM manufacturer short name does not match the DNT manufacturer header"
        )

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
        "board_name": _decode_capability_utf8(
            cfg, device_descriptor_offset, 8, "Capability device board name"
        ),
        "tx_channel_names": transmit_channel_names,
        "tx_channel_count": layout["transmit_channel_count"],
        "rx_channel_names": receive_channel_names,
        "rx_channel_count": layout["receive_channel_count"],
        "model_id": cfg[
            oem_descriptor_offset + 0x0C : oem_descriptor_offset + 0x14
        ].hex(),
        "manufacturer_short": _decode_capability_utf8(
            cfg, oem_descriptor_offset + 4, 8, "Capability OEM manufacturer short name"
        ),
        "manufacturer": _decode_capability_utf8(
            cfg, oem_descriptor_offset + 0x14, 128, "Capability OEM manufacturer"
        ),
        "product_name": _decode_capability_utf8(
            cfg, oem_descriptor_offset + 0x94, 128, "Capability OEM product name"
        ),
    }


def _extract_capability_14(cfg, mfg_header=""):
    facts = {}
    board = _read_str(cfg, 0x01D4, 32)
    facts["board_name"] = board.rstrip("'") if board else ""
    facts["model_id"] = _find_str(cfg, [0x0244, 0x0268, 0x0254, 0x0278], 32)
    facts["tx_channel_names"] = _find_channel_names(
        cfg, [0x060C, 0x0634, 0x061C, 0x0644], max_channels=8
    )
    facts["tx_channel_count"] = len(facts["tx_channel_names"])
    facts["rx_channel_names"] = _find_channel_names(
        cfg, [0x080C, 0x0834, 0x081C, 0x0844], max_channels=8
    )
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


def _section_table_layout(data, header_length):
    if not 0x40 <= header_length <= len(data):
        raise ValueError(f"Invalid DNT header length: {header_length}")

    section_table_offset, section_count, section_entry_size = struct.unpack_from(
        ">III", data, 0x24
    )
    if section_table_offset < 0x40:
        raise ValueError(f"Invalid DNT section table offset: {section_table_offset}")
    if section_entry_size < 16:
        raise ValueError(f"Invalid DNT section entry size: {section_entry_size}")

    section_table_end = section_table_offset + section_count * section_entry_size
    if section_table_end > header_length:
        raise ValueError(
            f"DNT section table extends past header: {section_table_end} > {header_length}"
        )

    return section_table_offset, section_count, section_entry_size


def _parse_sections(data, header_length):
    section_table_offset, section_count, section_entry_size = _section_table_layout(
        data, header_length
    )
    sections = []
    for section_index in range(section_count):
        entry_offset = section_table_offset + section_index * section_entry_size
        partition_id, section_version, body_offset, section_size = struct.unpack_from(
            ">IIII", data, entry_offset
        )
        file_offset = header_length + body_offset
        if file_offset > len(data) or section_size > len(data) - file_offset:
            raise ValueError(
                f"DNT section {section_index} extends past end of file: {file_offset + section_size} > {len(data)}"
            )
        sections.append(
            {
                "partition_id": partition_id,
                "partition_name": PARTITION_NAMES.get(
                    partition_id, f"unknown-{partition_id}"
                ),
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
                filesystem_size = struct.unpack(f"{endian}I", blob[pos + 4 : pos + 8])[
                    0
                ]
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
        if (
            not isinstance(result, dict)
            or result.get("dnt_parser_version") != DNT_PARSER_VERSION
        ):
            raise ValueError(
                f"Firmware resume file is incompatible with DNT parser version {DNT_PARSER_VERSION}"
            )
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
            raise RuntimeError(
                "Atomic no-replace directory publication is unavailable on this Linux libc"
            )
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
            raise RuntimeError(
                "Atomic no-replace directory publication is unavailable on this macOS libc"
            )
        operation.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(
            os.fsencode(temporary_directory),
            os.fsencode(output_directory),
            MACOS_RENAME_EXCLUSIVE,
        )
        error_number = ctypes.get_errno()
    else:
        raise RuntimeError(
            f"Atomic no-replace directory publication is unsupported on {sys.platform}"
        )
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
        _atomic_publish_directory_without_replacement(
            temporary_directory, output_directory
        )
    except FileExistsError as error:
        raise ValueError(
            f"Output directory already exists: {output_directory}"
        ) from error
    _synchronize_directory(output_directory.parent)


def _build_brooklyn2_image(
    path: Path,
    output_directory: Path,
    hardware_profile_path: Path,
    board_information_path: Path,
    trusted_evidence_root: Path,
    protected_capability_partition_path: Optional[Path] = None,
) -> dict[str, Any]:
    _validate_brooklyn2_flash_layout()

    hardware_profile_input = _load_brooklyn2_hardware_profile(hardware_profile_path)
    hardware_profile = hardware_profile_input.profile
    payload_partition_identifiers = dict(hardware_profile.payload_partition_identifiers)
    board_information_input = _load_brooklyn2_board_information_descriptor(
        board_information_path
    )
    board_information_payload = _build_brooklyn2_board_information_partition(
        board_information_input.descriptor
    )
    board_information_sha256 = hashlib.sha256(board_information_payload).hexdigest()
    if board_information_sha256 != hardware_profile.compatible_board_information_sha256:
        raise ValueError(
            f"Hardware profile {hardware_profile.profile_name} requires board-information SHA-256 "
            f"{hardware_profile.compatible_board_information_sha256}, found {board_information_sha256}"
        )

    protected_capability_partition_payload = None
    protected_capability_partition_validation = None
    if protected_capability_partition_path is not None:
        protected_capability_partition_payload = (
            protected_capability_partition_path.read_bytes()
        )
        if (
            len(protected_capability_partition_payload)
            > BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE
        ):
            raise ValueError(
                "Protected capability partition is "
                f"{len(protected_capability_partition_payload)} bytes, exceeding the "
                f"capacity of {BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE}"
            )
        protected_capability_partition_validation = _validate_cramfs_payload(
            protected_capability_partition_payload
        )

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
            flash_data[flash_offset : flash_offset + partition_capacity] = (
                board_information_payload
            )
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
            flash_data[
                flash_offset : flash_offset
                + len(protected_capability_partition_payload)
            ] = protected_capability_partition_payload
            physical_partition_records.append(
                {
                    "physical_partition_name": physical_partition_name,
                    "flash_offset": flash_offset,
                    "capacity": partition_capacity,
                    "state": "payload",
                    "artifact_filename": "cap.bin",
                    "size": len(protected_capability_partition_payload),
                    "sha256": hashlib.sha256(
                        protected_capability_partition_payload
                    ).hexdigest(),
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
            expected_payload_sha256 = (
                hardware_profile.selection_evidence.observed_fpga_payload_sha256
            )
            if payload_sha256 != expected_payload_sha256:
                raise ValueError(
                    f"Hardware profile {hardware_profile.profile_name} requires FPGA payload SHA-256 "
                    f"{expected_payload_sha256}, found {payload_sha256}"
                )
        elif physical_partition_name == "image":
            validation = _validate_uimage(payload, 2, 0x28000000)
            validation["brooklyn2_payload"] = _validate_brooklyn2_kernel_payload(
                payload
            )
        elif physical_partition_name == "userarea":
            validation = _validate_cramfs_payload(payload)
        elif physical_partition_name == "cap1":
            validation = _validate_capability_payload(payload)
        else:
            raise RuntimeError(
                f"No validator for physical partition {physical_partition_name}"
            )

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
        section
        for section in dnt_result["sections"]
        if section["partition_id"] not in required_partition_ids
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
    temporary_directory = Path(
        tempfile.mkdtemp(prefix=f".{output_directory.name}.", dir=output_parent)
    )
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
        _write_synchronized_file(
            temporary_directory / "manifest.json",
            (json.dumps(manifest, indent=2) + "\n").encode("utf-8"),
        )
        _publish_output_directory_without_replacement(
            temporary_directory,
            publication_directory,
        )
    except Exception:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise

    return manifest


@app.command("build-brooklyn2-image")
def firmware_build_brooklyn2_image(
    path: Path = typer.Argument(..., help="Brooklyn II DNT firmware file."),
    output_directory: Path = typer.Option(
        ..., "--output-directory", help="New output directory."
    ),
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
        typer.echo(
            f"Board information: {output_directory / manifest['board_information']['filename']}"
        )
    if protected_capability_partition is not None:
        typer.echo(f"Protected capability: {output_directory / 'cap.bin'}")
    typer.echo(f"Manifest: {output_directory / 'manifest.json'}")


@app.command("info")
def firmware_info(
    paths: list[Path] = typer.Argument(..., help=".dnt files or directories to scan."),
    save: Optional[Path] = typer.Option(
        None, "--save", help="Save JSON results to file."
    ),
    resume: Optional[Path] = typer.Option(
        None, "--resume", help="Skip files already in this JSON output."
    ),
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
    section_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(sections)").fetchall()
    }
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
        raise RuntimeError(
            "Firmware database sections schema is invalid; create a new database"
        )
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

    existing = {
        row[0] for row in conn.execute("SELECT sha256 FROM firmware").fetchall()
    }
    typer.echo(
        f"{len(existing)} already in db, {len(dnt_files)} files to scan", err=True
    )

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
    scan: bool = typer.Option(
        False, "--scan", help="Scan section contents for embedded filesystems."
    ),
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
            content = (
                _detect_content(data, section["file_offset"], section["size"]) or ""
            )
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
                embedded = _scan_for_embedded(
                    data, section["file_offset"], section["size"]
                )
                if embedded:
                    section_json["embedded"] = embedded
                    for entry in embedded:
                        size_str = f"{entry['size']:,}" if "size" in entry else ""
                        detail = entry.get(
                            "text", f"{entry['type']} ({size_str} bytes)"
                        )
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
    output: Path = typer.Option(
        None, "-o", "--output", help="Output file (default: stdout)."
    ),
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


@app.command("hexdump")
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
        trailing_hex = " ".join(
            f"{byte:02x}" for byte in first_row[colored_byte_count:]
        )
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
                colored_hex = " ".join(
                    f"{byte:02x}" for byte in row_bytes[:colored_in_row]
                )
                trailing_hex = " ".join(
                    f"{byte:02x}" for byte in row_bytes[colored_in_row:]
                )
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
        lines.extend(
            _span_lines(data, abs_off + off, length, kind, name, dtype, field_idx=fi)
        )
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
        lines.extend(
            _span_lines(
                data, abs_off + off, length, kind, name, dtype, endian=e, field_idx=fi
            )
        )
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


def _dissect_capability_partition(data, sec_off, sec_size, partition_id, lines, fi):
    platform_name = "Brooklyn II" if partition_id == 9 else "Ultimo"
    partition_name = PARTITION_NAMES[partition_id]
    lines.append(
        _section_header(
            f"Capability Payload ({partition_name}, {platform_name}, 0x{sec_off:X})"
        )
    )

    if partition_id == 9:
        field_map = [
            (0x0380, 32, "board_name"),
        ]
        for off, maxlen, name in field_map:
            if off + maxlen > sec_size:
                continue
            show_len = min(maxlen, 32)
            lines.extend(
                _span_lines(
                    data,
                    sec_off + off,
                    show_len,
                    "string",
                    name,
                    f"char[{maxlen}]",
                    field_idx=fi,
                )
            )
            fi += 1
        fi = _dissect_channel_range(
            data, sec_off, sec_size, 0x053C, 32, "tx_channels", lines, fi
        )
        fi = _dissect_channel_range(
            data, sec_off, sec_size, 0x453C, 32, "rx_channels", lines, fi
        )
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
            lines.extend(
                _span_lines(
                    data,
                    sec_off + off,
                    show_len,
                    "string",
                    name,
                    f"char[{maxlen}]",
                    field_idx=fi,
                )
            )
            fi += 1
    elif partition_id == 14:
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
                            _span_lines(
                                data,
                                sec_off + off,
                                maxlen,
                                "string",
                                name,
                                f"char[{maxlen}]",
                                field_idx=fi,
                            )
                        )
                        fi += 1
                        break
        for tx_base in [0x060C, 0x0634, 0x061C, 0x0644]:
            s = _read_str(cfg, tx_base, 32)
            if _is_printable(s) and len(s) >= 2:
                fi = _dissect_channel_range(
                    data, sec_off, sec_size, tx_base, 32, "tx_channels", lines, fi
                )
                break
        for rx_base in [0x080C, 0x0834, 0x081C, 0x0844]:
            s = _read_str(cfg, rx_base, 32)
            if _is_printable(s) and len(s) >= 2:
                fi = _dissect_channel_range(
                    data, sec_off, sec_size, rx_base, 32, "rx_channels", lines, fi
                )
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
                            _span_lines(
                                data,
                                sec_off + off,
                                maxlen,
                                "string",
                                name,
                                f"char[{maxlen}]",
                                field_idx=fi,
                            )
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
    section_table_offset, _, section_entry_size = _section_table_layout(data, hdr_len)
    parsed_sections = _parse_sections(data, hdr_len)
    lines.append("")
    lines.append(_section_header("Section Table"))

    sections = []
    for section_index, section in enumerate(parsed_sections):
        base = section_table_offset + section_index * section_entry_size
        lines.append(
            f"  {_COLOR_TYPE}── section {section_index} ({section['partition_name']}) ──{_RESET}"
        )
        for field_off, field_len, kind, name, dtype in SECTION_ENTRY_FIELDS:
            lines.extend(
                _span_lines(
                    data, base + field_off, field_len, kind, name, dtype, field_idx=fi
                )
            )
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
                    lines.extend(
                        _span_lines(
                            data, s_off + off, length, kind, name, dtype, field_idx=fi
                        )
                    )
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
                name = (
                    data[pos : pos + name_bytes]
                    .rstrip(b"\x00")
                    .decode("ascii", errors="replace")
                )
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
        blob = data[sec["file_offset"] : sec["file_offset"] + sec["size"]]
        for magic_bytes in (CRAMFS_MAGIC_LE, CRAMFS_MAGIC_BE):
            pos = blob.find(magic_bytes)
            if pos != -1:
                abs_off = sec["file_offset"] + pos
                is_be = magic_bytes == CRAMFS_MAGIC_BE
                endian = ">" if is_be else "<"
                fs_size = struct.unpack(f"{endian}I", data[abs_off + 4 : abs_off + 8])[
                    0
                ]
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
            blob[i], blob[i + 1], blob[i + 2], blob[i + 3] = (
                blob[i + 3],
                blob[i + 2],
                blob[i + 1],
                blob[i],
            )
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
    output: Path = typer.Argument(
        ..., help="Output directory for extracted filesystem."
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Print each extracted file."
    ),
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
    typer.echo(
        f"CramFS at offset 0x{cramfs_off:X}, {cramfs_size:,} bytes ({endian_str})",
        err=True,
    )

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
