from __future__ import annotations

import hashlib
import json
import re
import struct
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Optional

from netaudio.commands.firmware_constants import (
    BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
    BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION,
    BROOKLYN2_PAYLOAD_PARTITION_NAMES,
    PARTITION_NAMES,
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
        raise ValueError(f"Board-information field {field_name} must be between 0 and {maximum}")
    return value


def _require_provenance(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or value != value.strip()
        or not value.isprintable()
    ):
        raise ValueError(f"Board-information field {field_name} must be a non-empty printable source description")
    return value


def _require_hardware_profile_integer(data: dict[str, Any], field_name: str, maximum: int) -> int:
    value = data[field_name]
    if type(value) is not int:
        raise ValueError(f"Hardware-profile field {field_name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"Hardware-profile field {field_name} must be between 0 and {maximum}")
    return value


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"Hardware-profile field {field_name} must be a lowercase SHA-256 digest")
    return value


def _require_evidence_reference(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or not value.isprintable():
        raise ValueError(f"Hardware-profile field {field_name} must be a canonical relative POSIX path")
    reference = PurePosixPath(value)
    if reference.is_absolute() or ".." in reference.parts or value != str(reference):
        raise ValueError(f"Hardware-profile field {field_name} must be a canonical relative POSIX path")
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

    format_version = _require_hardware_profile_integer(data, "format_version", 0xFFFFFFFF)
    if format_version != BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION:
        raise ValueError(
            "Unsupported hardware-profile format version: "
            f"{format_version}; expected {BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION}"
        )

    profile_name = data["profile_name"]
    if not isinstance(profile_name, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", profile_name):
        raise ValueError("Hardware-profile field profile_name must be a lowercase kebab-case name")

    device_type_identifier = _require_hardware_profile_integer(data, "device_type_identifier", 0xFFFFFFFF)
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
        raise ValueError("Hardware-profile field payload_partition_identifiers must be an object")
    expected_payload_partition_names = set(BROOKLYN2_PAYLOAD_PARTITION_NAMES)
    actual_payload_partition_names = set(payload_partition_identifiers_data)
    missing_payload_partition_names = sorted(expected_payload_partition_names - actual_payload_partition_names)
    unexpected_payload_partition_names = sorted(actual_payload_partition_names - expected_payload_partition_names)
    if missing_payload_partition_names or unexpected_payload_partition_names:
        details = []
        if missing_payload_partition_names:
            details.append(f"missing partitions: {', '.join(missing_payload_partition_names)}")
        if unexpected_payload_partition_names:
            details.append(f"unexpected partitions: {', '.join(unexpected_payload_partition_names)}")
        raise ValueError(f"Invalid hardware-profile payload mapping ({'; '.join(details)})")

    payload_partition_identifiers = []
    for physical_partition_name in BROOKLYN2_PAYLOAD_PARTITION_NAMES:
        partition_identifier = payload_partition_identifiers_data[physical_partition_name]
        if type(partition_identifier) is not int:
            raise ValueError(
                f"Hardware-profile payload partition identifier for {physical_partition_name} must be an integer"
            )
        if partition_identifier not in PARTITION_NAMES or partition_identifier == 0:
            raise ValueError(
                "Hardware-profile payload partition identifier for "
                f"{physical_partition_name} is unsupported: {partition_identifier}"
            )
        payload_partition_identifiers.append((physical_partition_name, partition_identifier))
    partition_identifier_values = [partition_identifier for _, partition_identifier in payload_partition_identifiers]
    if len(set(partition_identifier_values)) != len(partition_identifier_values):
        raise ValueError("Hardware-profile payload partition identifiers must be unique")

    selection_scope = data["selection_scope"]
    if selection_scope != "exact_board_information_and_firmware":
        raise ValueError("Hardware-profile field selection_scope must be exact_board_information_and_firmware")
    causal_selector_status = data["causal_selector_status"]
    if causal_selector_status != "unknown":
        raise ValueError("Hardware-profile field causal_selector_status must be unknown")

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
    unexpected_evidence_fields = sorted(actual_evidence_fields - required_evidence_fields)
    if missing_evidence_fields or unexpected_evidence_fields:
        details = []
        if missing_evidence_fields:
            details.append(f"missing fields: {', '.join(missing_evidence_fields)}")
        if unexpected_evidence_fields:
            details.append(f"unexpected fields: {', '.join(unexpected_evidence_fields)}")
        raise ValueError(f"Invalid hardware-profile selection evidence ({'; '.join(details)})")

    evidence_kind = selection_evidence_data["kind"]
    if evidence_kind != "isolated_firmware_upgrade":
        raise ValueError("Hardware-profile selection evidence kind must be isolated_firmware_upgrade")
    run_name = selection_evidence_data["run_name"]
    if not isinstance(run_name, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", run_name):
        raise ValueError("Hardware-profile selection evidence run_name must be a lowercase kebab-case name")
    observed_fpga_partition_identifier = selection_evidence_data["observed_fpga_partition_identifier"]
    if type(observed_fpga_partition_identifier) is not int:
        raise ValueError("Hardware-profile selection evidence observed_fpga_partition_identifier must be an integer")
    payload_partition_identifier_map = dict(payload_partition_identifiers)
    if observed_fpga_partition_identifier != payload_partition_identifier_map["fpga"]:
        raise ValueError("Hardware-profile FPGA payload mapping does not match the observed selection evidence")

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
        raise ValueError(f"Board-information descriptor is not UTF-8: {path}") from error
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
        raise ValueError("Board-information field identity_kind must be synthetic or physical")
    identity_provenance_value = data.get("identity_provenance")
    if identity_kind == "physical":
        identity_provenance = _require_provenance(identity_provenance_value, "identity_provenance")
    else:
        if identity_provenance_value is not None:
            raise ValueError("Board-information field identity_provenance is only valid for physical identity")
        identity_provenance = None

    identity_name = data["identity_name"]
    if not isinstance(identity_name, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identity_name):
        raise ValueError("Board-information field identity_name must be a lowercase kebab-case name")

    media_access_control_address_text = data["media_access_control_address"]
    if not isinstance(media_access_control_address_text, str) or not re.fullmatch(
        r"[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}", media_access_control_address_text
    ):
        raise ValueError("Board-information media_access_control_address must contain six hexadecimal octets")
    media_access_control_address = bytes.fromhex(media_access_control_address_text.replace(":", ""))
    if media_access_control_address[0] & 1:
        raise ValueError("Board-information media_access_control_address must be unicast")
    if not any(media_access_control_address):
        raise ValueError("Board-information media_access_control_address must not be all zeroes")
    if identity_kind == "synthetic" and not media_access_control_address[0] & 2:
        raise ValueError("Synthetic board information requires a locally administered media_access_control_address")

    serial_number = _require_integer_field(data, "serial_number", 0xFFFFFFFF)
    if identity_kind == "synthetic" and serial_number > 0x7FFFFFFF:
        raise ValueError("Synthetic board-information serial_number must not exceed 2147483647")

    configuration_date_text = data["configuration_date"]
    if not isinstance(configuration_date_text, str) or not re.fullmatch(r"[0-9]{8}", configuration_date_text):
        raise ValueError("Board-information field configuration_date must contain exactly 8 ASCII decimal digits")
    configuration_date = configuration_date_text.encode("ascii")

    unidentified_prefix_hex = data.get("unidentified_prefix_hex", "ff" * 10)
    if not isinstance(unidentified_prefix_hex, str) or not re.fullmatch(r"[0-9a-fA-F]{20}", unidentified_prefix_hex):
        raise ValueError("Board-information field unidentified_prefix_hex must encode exactly 10 bytes")
    unidentified_prefix = bytes.fromhex(unidentified_prefix_hex)
    unidentified_prefix_provenance = data.get("unidentified_prefix_provenance")
    if unidentified_prefix == bytes([0xFF]) * 10:
        if unidentified_prefix_provenance is not None:
            raise ValueError(
                "Board-information field unidentified_prefix_provenance is only valid for a non-erased prefix"
            )
    else:
        if identity_kind != "physical":
            raise ValueError("A non-erased unidentified_prefix_hex is only supported for physical identity")
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
            hardware_revision_major=_require_integer_field(data, "hardware_revision_major", 0xFFFF),
            hardware_revision_minor=_require_integer_field(data, "hardware_revision_minor", 0xFFFF),
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
    media_access_control_address = ":".join(f"{octet:02x}" for octet in descriptor.media_access_control_address)
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
        raise RuntimeError("Hardware-profile evidence verification does not match the selected profile")
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
