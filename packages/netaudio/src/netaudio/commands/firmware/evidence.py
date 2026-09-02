from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from netaudio.commands.firmware.constants import (
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_EVIDENCE_ALL_ARTIFACTS_MAXIMUM_SIZE,
    BROOKLYN2_EVIDENCE_ARTIFACT_MAXIMUM_SIZE,
    BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE,
    BROOKLYN2_EVIDENCE_MANIFEST_MAXIMUM_SIZE,
    BROOKLYN2_EVIDENCE_REQUEST_MAXIMUM_SIZE,
    BROOKLYN2_FLASH_PARTITIONS,
    BROOKLYN2_FLASH_SIZE,
)
from netaudio.commands.firmware.models import (
    Brooklyn2HardwareProfile,
    Brooklyn2HardwareProfileEvidenceAssessment,
    Brooklyn2HardwareProfileSelectionEvidence,
    TrustedEvidenceRoot,
    VerifiedEvidenceFile,
    _reject_duplicate_json_fields,
    _reject_nonstandard_json_constant,
    _require_evidence_reference,
)


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
    if not isinstance(record["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", record["sha256"]):
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
            raise ValueError(f"Trusted evidence root is not a directory: {absolute_path}")
        return TrustedEvidenceRoot(
            path=absolute_path,
            file_descriptor=directory_descriptor,
        )
    except BaseException:
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
            relative_path = reference_path.relative_to(PurePosixPath(trusted_evidence_root.path.as_posix()))
        except ValueError as error:
            raise ValueError(f"{context} resolves outside the trusted evidence root") from error
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
                raise ValueError(f"{context} contains an inaccessible or symbolic-link directory") from error
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


@dataclass
class EvidenceRun:
    run_directory: PurePosixPath
    run_manifest: dict[str, Any]
    trusted_evidence_root: TrustedEvidenceRoot
    verified_artifacts: dict[str, VerifiedEvidenceFile] = field(default_factory=dict)

    def resolve(self, path: str, maximum_size: int, context: str) -> VerifiedEvidenceFile:
        return _resolve_trusted_evidence_file(
            self.trusted_evidence_root, path, self.run_directory, maximum_size, context
        )

    def resolve_declared(self, record: dict[str, Any], maximum_size: int, context: str) -> VerifiedEvidenceFile:
        artifact = self.resolve(record["path"], maximum_size, context)
        _verify_declared_evidence_file(record, artifact, context)
        return artifact


def _load_evidence_run(
    evidence: Brooklyn2HardwareProfileSelectionEvidence, trusted_evidence_root: TrustedEvidenceRoot
) -> EvidenceRun:
    run_manifest_artifact = _resolve_trusted_evidence_file(
        trusted_evidence_root,
        evidence.run_manifest_path,
        PurePosixPath(),
        BROOKLYN2_EVIDENCE_MANIFEST_MAXIMUM_SIZE,
        "Upgrade evidence run manifest",
    )
    if run_manifest_artifact.sha256 != evidence.run_manifest_sha256:
        raise ValueError("Upgrade evidence run manifest SHA-256 does not match the hardware profile")
    if (
        run_manifest_artifact.path.name != "manifest.json"
        or run_manifest_artifact.path.parent.name != evidence.run_name
    ):
        raise ValueError("Upgrade evidence run manifest path does not match the declared run name")
    run_manifest = _load_duplicate_safe_json(run_manifest_artifact.data, "Upgrade evidence run manifest")
    if run_manifest.get("schema_version") != 1:
        raise ValueError("Upgrade evidence run manifest schema version is unsupported")
    if run_manifest.get("state") != "completed":
        raise ValueError("Upgrade evidence run did not complete")
    if type(run_manifest.get("process_returncode")) is not int or run_manifest["process_returncode"] != 0:
        raise ValueError("Upgrade evidence run process did not exit successfully")

    network = _require_evidence_mapping(run_manifest.get("network"), "Upgrade evidence network record")
    if network.get("transport") != "isolated-hub" or network.get("live_interface_exposure") is not False:
        raise ValueError("Upgrade evidence run was not isolated from live interfaces")
    if network.get("tftp_export_enabled") is not True:
        raise ValueError("Upgrade evidence run did not use its isolated TFTP export")
    return EvidenceRun(
        run_directory=run_manifest_artifact.relative_path.parent,
        run_manifest=run_manifest,
        trusted_evidence_root=trusted_evidence_root,
    )


def _verify_evidence_artifact_table(run: EvidenceRun) -> None:
    artifacts = _require_evidence_mapping(run.run_manifest.get("artifacts"), "Upgrade evidence artifact table")
    if len(artifacts) > 1024:
        raise ValueError("Upgrade evidence artifact table contains too many entries")
    total_artifact_size = 0
    for artifact_name, artifact_value in artifacts.items():
        canonical_name = _require_evidence_reference(artifact_name, "upgrade_evidence.artifact_name")
        context = f"Upgrade evidence artifact {canonical_name}"
        artifact_record = _require_evidence_file_record(artifact_value, context)
        total_artifact_size += artifact_record["size"]
        if total_artifact_size > BROOKLYN2_EVIDENCE_ALL_ARTIFACTS_MAXIMUM_SIZE:
            raise ValueError("Upgrade evidence artifact table exceeds its aggregate size limit")
        artifact = run.resolve(artifact_record["path"], BROOKLYN2_EVIDENCE_ARTIFACT_MAXIMUM_SIZE, context)
        expected_artifact_path = run.run_directory.joinpath(*PurePosixPath(canonical_name).parts)
        if artifact.relative_path != expected_artifact_path:
            raise ValueError(f"Upgrade evidence artifact {canonical_name} path does not match its table key")
        _verify_declared_evidence_file(artifact_record, artifact, context)
        run.verified_artifacts[canonical_name] = artifact


def _verify_evidence_stimulus(run: EvidenceRun, evidence: Brooklyn2HardwareProfileSelectionEvidence) -> None:
    stimulus_log = _require_evidence_mapping(run.run_manifest.get("stimulus_log"), "Upgrade evidence stimulus log")
    if stimulus_log.get("count") != 1:
        raise ValueError("Upgrade evidence stimulus log must contain exactly one request frame")
    request_artifact = _resolve_trusted_evidence_file(
        run.trusted_evidence_root,
        evidence.request_frame_path,
        PurePosixPath(),
        BROOKLYN2_EVIDENCE_REQUEST_MAXIMUM_SIZE,
        "Upgrade evidence request frame",
    )
    if request_artifact.sha256 != evidence.request_frame_sha256:
        raise ValueError("Upgrade evidence request frame SHA-256 does not match the hardware profile")
    stimulus_data_path = _require_evidence_reference(
        stimulus_log.get("data_path"),
        "upgrade_evidence.stimulus_log.data_path",
    )
    if (
        stimulus_data_path not in run.verified_artifacts
        or run.verified_artifacts[stimulus_data_path].relative_path != request_artifact.relative_path
    ):
        raise ValueError("Upgrade evidence request frame is not bound to the stimulus artifact table")
    stimulus_index_path = _require_evidence_reference(
        stimulus_log.get("index_path"),
        "upgrade_evidence.stimulus_log.index_path",
    )
    if stimulus_index_path not in run.verified_artifacts:
        raise ValueError("Upgrade evidence stimulus index is absent from the artifact table")


def _verify_evidence_tftp_export(
    run: EvidenceRun, hardware_profile: Brooklyn2HardwareProfile, firmware_data: bytes
) -> None:
    tftp_export = _require_evidence_mapping(run.run_manifest.get("tftp_export"), "Upgrade evidence TFTP export")
    if tftp_export.get("schema_version") != 1 or tftp_export.get("verified") is not True:
        raise ValueError("Upgrade evidence TFTP export is not verified schema version 1")
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
        context = f"Upgrade evidence TFTP {record_name}"
        record = _require_evidence_file_record(tftp_export.get(record_name), context)
        artifact = run.resolve_declared(record, BROOKLYN2_EVIDENCE_FIRMWARE_MAXIMUM_SIZE, context)
        if artifact.sha256 != hardware_profile.compatible_firmware_sha256 or artifact.data != firmware_data:
            raise ValueError(f"Upgrade evidence TFTP {record_name} does not match the compatible firmware")
        tftp_artifacts.append(artifact)
    if tftp_artifacts[1].relative_path != tftp_artifacts[2].relative_path:
        raise ValueError("Upgrade evidence staged and final staged firmware paths differ")
    staged_reference = tftp_export["staged"]["path"]
    if not Path(staged_reference).is_absolute():
        staged_artifact_name = str(PurePosixPath(staged_reference))
        if (
            staged_artifact_name not in run.verified_artifacts
            or run.verified_artifacts[staged_artifact_name].relative_path != tftp_artifacts[1].relative_path
        ):
            raise ValueError("Upgrade evidence staged firmware is not bound to the artifact table")


def _verify_evidence_flash_records(
    run: EvidenceRun, evidence: Brooklyn2HardwareProfileSelectionEvidence
) -> VerifiedEvidenceFile:
    flash = _require_evidence_mapping(run.run_manifest.get("flash"), "Upgrade evidence flash record")
    result_flash_artifact = _resolve_trusted_evidence_file(
        run.trusted_evidence_root,
        evidence.result_flash_path,
        PurePosixPath(),
        BROOKLYN2_FLASH_SIZE,
        "Upgrade evidence result flash",
    )
    if (
        result_flash_artifact.size != BROOKLYN2_FLASH_SIZE
        or result_flash_artifact.sha256 != evidence.result_flash_sha256
    ):
        raise ValueError("Upgrade evidence result flash does not match the hardware profile")
    runtime_final_record = _require_evidence_file_record(
        flash.get("runtime_final"),
        "Upgrade evidence final runtime flash",
    )
    runtime_final_artifact = run.resolve_declared(
        runtime_final_record, BROOKLYN2_FLASH_SIZE, "Upgrade evidence final runtime flash"
    )
    if runtime_final_artifact.relative_path != result_flash_artifact.relative_path:
        raise ValueError("Upgrade evidence result flash is not bound to the run manifest")
    if flash.get("source_unchanged") is not True:
        raise ValueError("Upgrade evidence source flash was not immutable")
    source_initial_record = _require_evidence_file_record(
        flash.get("source_initial"), "Upgrade evidence initial source flash"
    )
    source_final_record = _require_evidence_file_record(
        flash.get("source_final"), "Upgrade evidence final source flash"
    )
    source_initial_artifact = run.resolve(
        source_initial_record["path"], BROOKLYN2_FLASH_SIZE, "Upgrade evidence initial source flash"
    )
    source_final_artifact = run.resolve(
        source_final_record["path"], BROOKLYN2_FLASH_SIZE, "Upgrade evidence final source flash"
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
    runtime_initial_path = run.resolve(
        runtime_initial_record["path"], BROOKLYN2_FLASH_SIZE, "Upgrade evidence initial runtime flash path"
    )
    if runtime_initial_path.relative_path != result_flash_artifact.relative_path:
        raise ValueError("Upgrade evidence runtime flash path changed during the run")
    if (
        runtime_initial_record["sha256"] != source_initial_record["sha256"]
        or runtime_initial_record["size"] != source_initial_record["size"]
    ):
        raise ValueError("Upgrade evidence initial runtime flash is not bound to the immutable source flash")
    return result_flash_artifact


def _verify_evidence_flash_contents(
    evidence: Brooklyn2HardwareProfileSelectionEvidence,
    result_flash_artifact: VerifiedEvidenceFile,
    firmware_data: bytes,
    firmware_result: dict[str, Any],
    board_information_payload: bytes,
) -> None:
    matching_fpga_sections = [
        section
        for section in firmware_result["sections"]
        if section["partition_id"] == evidence.observed_fpga_partition_identifier
    ]
    if len(matching_fpga_sections) != 1:
        raise ValueError("Upgrade evidence selected FPGA partition is not unique in the compatible firmware")
    fpga_section = matching_fpga_sections[0]
    fpga_payload = firmware_data[fpga_section["file_offset"] : fpga_section["file_offset"] + fpga_section["size"]]
    if hashlib.sha256(fpga_payload).hexdigest() != evidence.observed_fpga_payload_sha256:
        raise ValueError("Upgrade evidence selected FPGA payload SHA-256 is inconsistent with the compatible firmware")
    fpga_offset = next(
        partition_offset
        for partition_name, partition_offset, _ in BROOKLYN2_FLASH_PARTITIONS
        if partition_name == "fpga"
    )
    if result_flash_artifact.data[fpga_offset : fpga_offset + len(fpga_payload)] != fpga_payload:
        raise ValueError("Upgrade evidence result flash does not contain the selected FPGA payload")
    board_information_end = BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET + len(board_information_payload)
    if (
        result_flash_artifact.data[BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET:board_information_end]
        != board_information_payload
    ):
        raise ValueError("Upgrade evidence result flash is not bound to the compatible board information")


def _verify_brooklyn2_hardware_profile_evidence_from_root(
    hardware_profile: Brooklyn2HardwareProfile,
    trusted_evidence_root: TrustedEvidenceRoot,
    firmware_data: bytes,
    firmware_result: dict[str, Any],
    board_information_payload: bytes,
) -> Brooklyn2HardwareProfileEvidenceAssessment:
    evidence = hardware_profile.selection_evidence
    run = _load_evidence_run(evidence, trusted_evidence_root)
    _verify_evidence_artifact_table(run)
    _verify_evidence_stimulus(run, evidence)
    _verify_evidence_tftp_export(run, hardware_profile, firmware_data)
    result_flash_artifact = _verify_evidence_flash_records(run, evidence)
    _verify_evidence_flash_contents(
        evidence, result_flash_artifact, firmware_data, firmware_result, board_information_payload
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
