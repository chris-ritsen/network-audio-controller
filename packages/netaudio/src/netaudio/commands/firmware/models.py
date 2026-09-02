from __future__ import annotations

import hashlib
import json
import re
import struct
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Optional

from netaudio.commands.firmware.constants import (
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


KEBAB_CASE_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")

HARDWARE_PROFILE_FIELDS = {
    "causal_selector_status",
    "compatible_board_information_sha256",
    "compatible_firmware_sha256",
    "device_type_identifier",
    "format_version",
    "payload_partition_identifiers",
    "profile_name",
    "selection_evidence",
    "selection_scope",
}

SELECTION_EVIDENCE_FIELDS = {
    "kind",
    "observed_fpga_partition_identifier",
    "observed_fpga_payload_sha256",
    "request_frame_path",
    "request_frame_sha256",
    "result_flash_path",
    "result_flash_sha256",
    "run_manifest_path",
    "run_manifest_sha256",
    "run_name",
}

BOARD_INFORMATION_REQUIRED_FIELDS = {
    "configuration_date",
    "format_version",
    "hardware_revision_major",
    "hardware_revision_minor",
    "identity_kind",
    "identity_name",
    "media_access_control_address",
    "serial_number",
}

BOARD_INFORMATION_OPTIONAL_FIELDS = {
    "identity_provenance",
    "unidentified_prefix_hex",
    "unidentified_prefix_provenance",
}


def _load_json_descriptor(path: Path, descriptor_label: str, json_label: str) -> tuple[bytes, dict]:
    descriptor_bytes = path.read_bytes()
    try:
        descriptor_text = descriptor_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{descriptor_label} is not UTF-8: {path}") from error
    try:
        data = json.loads(
            descriptor_text,
            object_pairs_hook=_reject_duplicate_json_fields,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid {json_label} JSON: {error}") from error
    if not isinstance(data, dict):
        raise ValueError(f"{descriptor_label} must be a JSON object")
    return descriptor_bytes, data


def _require_exact_keys(
    actual: set[str], required: set[str], optional: set[str], schema_label: str, noun: str = "fields"
) -> None:
    missing = sorted(required - actual)
    unexpected = sorted(actual - required - optional)
    if not missing and not unexpected:
        return
    details = []
    if missing:
        details.append(f"missing {noun}: {', '.join(missing)}")
    if unexpected:
        details.append(f"unexpected {noun}: {', '.join(unexpected)}")
    raise ValueError(f"Invalid {schema_label} ({'; '.join(details)})")


def _require_kebab_case_name(value: Any, context: str) -> str:
    if not isinstance(value, str) or not KEBAB_CASE_NAME.fullmatch(value):
        raise ValueError(f"{context} must be a lowercase kebab-case name")
    return value


def _parse_payload_partition_identifiers(data: dict) -> list[tuple[str, int]]:
    payload_partition_identifiers_data = data["payload_partition_identifiers"]
    if not isinstance(payload_partition_identifiers_data, dict):
        raise ValueError("Hardware-profile field payload_partition_identifiers must be an object")
    _require_exact_keys(
        set(payload_partition_identifiers_data),
        set(BROOKLYN2_PAYLOAD_PARTITION_NAMES),
        set(),
        "hardware-profile payload mapping",
        "partitions",
    )
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
    return payload_partition_identifiers


def _parse_selection_evidence(data: dict, fpga_partition_identifier: int) -> Brooklyn2HardwareProfileSelectionEvidence:
    selection_evidence_data = data["selection_evidence"]
    if not isinstance(selection_evidence_data, dict):
        raise ValueError("Hardware-profile field selection_evidence must be an object")
    _require_exact_keys(
        set(selection_evidence_data), SELECTION_EVIDENCE_FIELDS, set(), "hardware-profile selection evidence"
    )
    evidence_kind = selection_evidence_data["kind"]
    if evidence_kind != "isolated_firmware_upgrade":
        raise ValueError("Hardware-profile selection evidence kind must be isolated_firmware_upgrade")
    run_name = _require_kebab_case_name(
        selection_evidence_data["run_name"], "Hardware-profile selection evidence run_name"
    )
    observed_fpga_partition_identifier = selection_evidence_data["observed_fpga_partition_identifier"]
    if type(observed_fpga_partition_identifier) is not int:
        raise ValueError("Hardware-profile selection evidence observed_fpga_partition_identifier must be an integer")
    if observed_fpga_partition_identifier != fpga_partition_identifier:
        raise ValueError("Hardware-profile FPGA payload mapping does not match the observed selection evidence")

    def reference(name: str) -> str:
        return _require_evidence_reference(selection_evidence_data[name], f"selection_evidence.{name}")

    def digest(name: str) -> str:
        return _require_sha256(selection_evidence_data[name], f"selection_evidence.{name}")

    return Brooklyn2HardwareProfileSelectionEvidence(
        kind=evidence_kind,
        run_name=run_name,
        run_manifest_path=reference("run_manifest_path"),
        run_manifest_sha256=digest("run_manifest_sha256"),
        request_frame_path=reference("request_frame_path"),
        request_frame_sha256=digest("request_frame_sha256"),
        result_flash_path=reference("result_flash_path"),
        result_flash_sha256=digest("result_flash_sha256"),
        observed_fpga_partition_identifier=observed_fpga_partition_identifier,
        observed_fpga_payload_sha256=digest("observed_fpga_payload_sha256"),
    )


def _load_brooklyn2_hardware_profile(path: Path) -> Brooklyn2HardwareProfileInput:
    profile_bytes, data = _load_json_descriptor(path, "Hardware-profile descriptor", "hardware-profile")
    _require_exact_keys(set(data), HARDWARE_PROFILE_FIELDS, set(), "hardware-profile schema")

    format_version = _require_hardware_profile_integer(data, "format_version", 0xFFFFFFFF)
    if format_version != BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION:
        raise ValueError(
            "Unsupported hardware-profile format version: "
            f"{format_version}; expected {BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION}"
        )
    profile_name = _require_kebab_case_name(data["profile_name"], "Hardware-profile field profile_name")
    device_type_identifier = _require_hardware_profile_integer(data, "device_type_identifier", 0xFFFFFFFF)
    compatible_board_information_sha256 = _require_sha256(
        data["compatible_board_information_sha256"],
        "compatible_board_information_sha256",
    )
    compatible_firmware_sha256 = _require_sha256(
        data["compatible_firmware_sha256"],
        "compatible_firmware_sha256",
    )
    payload_partition_identifiers = _parse_payload_partition_identifiers(data)

    selection_scope = data["selection_scope"]
    if selection_scope != "exact_board_information_and_firmware":
        raise ValueError("Hardware-profile field selection_scope must be exact_board_information_and_firmware")
    causal_selector_status = data["causal_selector_status"]
    if causal_selector_status != "unknown":
        raise ValueError("Hardware-profile field causal_selector_status must be unknown")
    selection_evidence = _parse_selection_evidence(data, dict(payload_partition_identifiers)["fpga"])

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


def _parse_board_identity(data: dict) -> tuple[str, str | None]:
    identity_kind = data["identity_kind"]
    if identity_kind not in ("synthetic", "physical"):
        raise ValueError("Board-information field identity_kind must be synthetic or physical")
    identity_provenance_value = data.get("identity_provenance")
    if identity_kind == "physical":
        return identity_kind, _require_provenance(identity_provenance_value, "identity_provenance")
    if identity_provenance_value is not None:
        raise ValueError("Board-information field identity_provenance is only valid for physical identity")
    return identity_kind, None


def _parse_media_access_control_address(data: dict, identity_kind: str) -> bytes:
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
    return media_access_control_address


def _parse_configuration_date(data: dict) -> bytes:
    configuration_date_text = data["configuration_date"]
    if not isinstance(configuration_date_text, str) or not re.fullmatch(r"[0-9]{8}", configuration_date_text):
        raise ValueError("Board-information field configuration_date must contain exactly 8 ASCII decimal digits")
    return configuration_date_text.encode("ascii")


def _parse_unidentified_prefix(data: dict, identity_kind: str) -> tuple[bytes, str | None]:
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
        return unidentified_prefix, None
    if identity_kind != "physical":
        raise ValueError("A non-erased unidentified_prefix_hex is only supported for physical identity")
    return unidentified_prefix, _require_provenance(unidentified_prefix_provenance, "unidentified_prefix_provenance")


def _load_brooklyn2_board_information_descriptor(
    path: Path,
) -> Brooklyn2BoardInformationInput:
    descriptor_bytes, data = _load_json_descriptor(path, "Board-information descriptor", "board-information")
    _require_exact_keys(
        set(data), BOARD_INFORMATION_REQUIRED_FIELDS, BOARD_INFORMATION_OPTIONAL_FIELDS, "board-information schema"
    )

    format_version = _require_integer_field(data, "format_version", 0xFFFFFFFF)
    if format_version != BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION:
        raise ValueError(
            "Unsupported board-information format version: "
            f"{format_version}; expected {BROOKLYN2_BOARD_INFORMATION_DESCRIPTOR_FORMAT_VERSION}"
        )
    identity_kind, identity_provenance = _parse_board_identity(data)
    identity_name = _require_kebab_case_name(data["identity_name"], "Board-information field identity_name")
    media_access_control_address = _parse_media_access_control_address(data, identity_kind)
    serial_number = _require_integer_field(data, "serial_number", 0xFFFFFFFF)
    if identity_kind == "synthetic" and serial_number > 0x7FFFFFFF:
        raise ValueError("Synthetic board-information serial_number must not exceed 2147483647")
    configuration_date = _parse_configuration_date(data)
    unidentified_prefix, unidentified_prefix_provenance = _parse_unidentified_prefix(data, identity_kind)

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
