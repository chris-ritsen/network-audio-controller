import hashlib
import json
import shutil
import struct
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"},
    reason="Brooklyn II image publication requires Linux or macOS",
)

from netaudio.commands import firmware as firmware_commands
from netaudio.commands import firmware_parser
from netaudio.commands.firmware import (
    BROOKLYN2_FLASH_SIZE,
    BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION,
    BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
    _build_brooklyn2_image,
    _load_brooklyn2_hardware_profile,
    parse_dnt,
)
from tests.firmware_test_support import (
    build_board_information_descriptor,
    build_cramfs_payload,
    finish_dnt_checksums,
    write_brooklyn2_test_build_inputs,
    write_brooklyn2_test_dnt,
    write_test_run_manifest_and_refresh_profile,
)


def test_build_brooklyn2_image_uses_evidence_scoped_hardware_profile_mapping(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    output_directory = tmp_path / "image"

    manifest = _build_brooklyn2_image(
        source,
        output_directory,
        hardware_profile_path,
        board_information_path,
        evidence_root,
    )

    flash = (output_directory / "flash.bin").read_bytes()
    assert len(flash) == BROOKLYN2_FLASH_SIZE
    assert flash[0x170000 : 0x170000 + len(payloads["boot"])] == payloads["boot"]
    assert flash[0x1A0000 : 0x1A0000 + len(payloads["standard_fpga"])] == payloads["standard_fpga"]
    assert payloads["revision_three_fpga"] not in flash
    assert flash[0x2A0000 : 0x2A0000 + len(payloads["image"])] == payloads["image"]
    assert flash[0x5C0000 : 0x5C0000 + len(payloads["user"])] == payloads["user"]
    assert flash[0x7C0000:0x7E0000] == bytes([0xFF]) * 0x20000
    assert flash[0x7E0000 : 0x7E0000 + len(payloads["capability"])] == payloads["capability"]
    assert (output_directory / "bootloader.bin").read_bytes() == payloads["bootloader_data"]
    assert manifest["format_version"] == BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION
    assert manifest["hardware_profile"]["profile_name"] == "brooklyn2-standard-test-profile"
    assert manifest["hardware_profile"]["payload_partition_identifiers"]["fpga"] == 2
    assert manifest["hardware_profile"]["selection_scope"] == "exact_board_information_and_firmware"
    assert manifest["hardware_profile"]["causal_selector_status"] == "unknown"
    assert manifest["board_information"]["state"] == "generated"
    assert (output_directory / "brdinfo.bin").exists()
    assert [section["partition_id"] for section in manifest["unused_dnt_sections"]] == [12]
    assert json.loads((output_directory / "manifest.json").read_text()) == manifest


def test_build_brooklyn2_image_installs_validated_protected_capability_partition(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    protected_capability_partition_path = tmp_path / "protected-capability.bin"
    protected_capability_partition = build_cramfs_payload()
    protected_capability_partition_path.write_bytes(protected_capability_partition)
    output_directory = tmp_path / "image"

    manifest = _build_brooklyn2_image(
        source,
        output_directory,
        hardware_profile_path,
        board_information_path,
        evidence_root,
        protected_capability_partition_path,
    )

    flash = (output_directory / "flash.bin").read_bytes()
    protected_capability_partition_end = BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET + len(
        protected_capability_partition
    )
    assert (
        flash[BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET:protected_capability_partition_end]
        == protected_capability_partition
    )
    assert flash[
        protected_capability_partition_end : BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET
        + BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE
    ] == bytes([0xFF]) * (BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE - len(protected_capability_partition))
    assert (output_directory / "cap.bin").read_bytes() == protected_capability_partition
    physical_partition = next(
        partition
        for partition in manifest["flash"]["physical_partitions"]
        if partition["physical_partition_name"] == "cap"
    )
    assert physical_partition == {
        "physical_partition_name": "cap",
        "flash_offset": BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET,
        "capacity": BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
        "state": "payload",
        "artifact_filename": "cap.bin",
        "size": len(protected_capability_partition),
        "sha256": hashlib.sha256(protected_capability_partition).hexdigest(),
        "validation": firmware_commands._validate_cramfs_payload(protected_capability_partition),
    }
    assert json.loads((output_directory / "manifest.json").read_text()) == manifest


def test_build_brooklyn2_image_rejects_invalid_protected_capability_partition(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    protected_capability_partition_path = tmp_path / "protected-capability.bin"
    protected_capability_partition_path.write_bytes(b"not a CramFS image")
    output_directory = tmp_path / "image"

    with pytest.raises(ValueError, match="Invalid little-endian CramFS header"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
            protected_capability_partition_path,
        )

    assert not output_directory.exists()


def test_build_brooklyn2_image_rejects_oversized_protected_capability_partition(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    protected_capability_partition_path = tmp_path / "protected-capability.bin"
    protected_capability_partition_path.write_bytes(
        build_cramfs_payload(BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE + 1)
    )
    output_directory = tmp_path / "image"

    with pytest.raises(ValueError, match="exceeding the capacity"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
            protected_capability_partition_path,
        )

    assert not output_directory.exists()


def test_build_brooklyn2_image_is_content_reproducible_across_input_paths(tmp_path, monkeypatch):
    first_input_directory = tmp_path / "first-inputs"
    first_input_directory.mkdir()
    first_source = first_input_directory / "first-firmware-name.dnt"
    payloads = write_brooklyn2_test_dnt(first_source)
    first_profile, first_board_information, first_evidence_root = write_brooklyn2_test_build_inputs(
        first_input_directory,
        first_source,
        payloads,
    )
    second_input_directory = tmp_path / "renamed-inputs"
    second_input_directory.mkdir()
    second_source = second_input_directory / "renamed-firmware.dnt"
    second_profile = second_input_directory / "renamed-profile.json"
    second_board_information = second_input_directory / "renamed-identity.json"
    second_evidence_root = second_input_directory / "renamed-evidence-root"
    second_source.write_bytes(first_source.read_bytes())
    second_profile.write_bytes(first_profile.read_bytes())
    second_board_information.write_bytes(first_board_information.read_bytes())
    shutil.copytree(first_evidence_root, second_evidence_root)
    first_output = tmp_path / "first-image"
    second_output = tmp_path / "second-image"

    _build_brooklyn2_image(
        first_source.resolve(),
        first_output.resolve(),
        first_profile.resolve(),
        first_board_information.resolve(),
        first_evidence_root.resolve(),
    )
    monkeypatch.chdir(tmp_path)
    _build_brooklyn2_image(
        second_source.relative_to(tmp_path),
        second_output.relative_to(tmp_path),
        second_profile.relative_to(tmp_path),
        second_board_information.relative_to(tmp_path),
        second_evidence_root.relative_to(tmp_path),
    )

    assert sorted(path.name for path in first_output.iterdir()) == sorted(path.name for path in second_output.iterdir())
    for first_artifact in first_output.iterdir():
        assert first_artifact.read_bytes() == (second_output / first_artifact.name).read_bytes()


def test_hardware_profile_manifest_preserves_exact_selection_evidence(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_text = profile_path.read_text()
    profile_descriptor = json.loads(profile_text)
    manifest = _build_brooklyn2_image(
        source,
        tmp_path / "image",
        profile_path,
        board_information_path,
        evidence_root,
    )["hardware_profile"]

    assert manifest == {
        "profile_name": "brooklyn2-standard-test-profile",
        "descriptor": {
            "format_version": BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION,
            "source_sha256": hashlib.sha256(profile_text.encode()).hexdigest(),
        },
        "device_type_identifier": 9,
        "compatible_board_information_sha256": profile_descriptor["compatible_board_information_sha256"],
        "compatible_firmware_sha256": profile_descriptor["compatible_firmware_sha256"],
        "payload_partition_identifiers": {
            "boot": 6,
            "fpga": 2,
            "image": 1,
            "userarea": 11,
            "cap1": 9,
        },
        "selection_scope": "exact_board_information_and_firmware",
        "causal_selector_status": "unknown",
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
            key: value for key, value in profile_descriptor["selection_evidence"].items() if not key.endswith("_path")
        },
    }


@pytest.mark.parametrize(
    ("field_name", "value", "error_pattern"),
    [
        ("format_version", True, "format_version must be an integer"),
        ("format_version", 3, "Unsupported hardware-profile format version"),
        ("profile_name", "Brooklyn II", "lowercase kebab-case"),
        ("compatible_firmware_sha256", "A" * 64, "lowercase SHA-256 digest"),
        ("selection_scope", "all_firmware", "exact_board_information_and_firmware"),
        ("causal_selector_status", "hardware_revision", "must be unknown"),
    ],
)
def test_hardware_profile_rejects_invalid_top_level_fields(tmp_path, field_name, value, error_pattern):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor[field_name] = value
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(ValueError, match=error_pattern):
        _load_brooklyn2_hardware_profile(profile_path)


def test_hardware_profile_rejects_duplicate_payload_partition_identifiers(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor["payload_partition_identifiers"]["fpga"] = 6
    profile_descriptor["selection_evidence"]["observed_fpga_partition_identifier"] = 6
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(ValueError, match="payload partition identifiers must be unique"):
        _load_brooklyn2_hardware_profile(profile_path)


def test_hardware_profile_rejects_mapping_that_disagrees_with_observation(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor["selection_evidence"]["observed_fpga_partition_identifier"] = 12
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(ValueError, match="does not match the observed selection evidence"):
        _load_brooklyn2_hardware_profile(profile_path)


def test_hardware_profile_rejects_evidence_path_parent_traversal(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor["selection_evidence"]["result_flash_path"] = "../outside/flash.bin"
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(ValueError, match="canonical relative POSIX path"):
        _load_brooklyn2_hardware_profile(profile_path)


def test_build_brooklyn2_image_rejects_board_information_outside_profile_scope(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    board_information_path.write_text(json.dumps(build_board_information_descriptor(serial_number=7)))
    output_directory = tmp_path / "image"

    with pytest.raises(ValueError, match="requires board-information SHA-256"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )

    assert not output_directory.exists()


def test_build_brooklyn2_image_rejects_firmware_outside_profile_scope(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    source.write_bytes(source.read_bytes() + b"different")
    output_directory = tmp_path / "image"

    with pytest.raises(ValueError, match="requires firmware SHA-256"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )

    assert not output_directory.exists()


@pytest.mark.parametrize(
    ("evidence_failure", "error_pattern"),
    [
        ("schema", "schema version is unsupported"),
        ("state", "did not complete"),
        ("returncode", "did not exit successfully"),
        ("network", "was not isolated"),
        ("source", "source flash was not immutable"),
        ("staged", "staged firmware was not immutable"),
    ],
)
def test_build_brooklyn2_image_rejects_invalid_evidence_run_contract(
    tmp_path,
    evidence_failure,
    error_pattern,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    run_manifest_path = evidence_root / "runs/test-isolated-upgrade/manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text())
    if evidence_failure == "schema":
        run_manifest["schema_version"] = 2
    elif evidence_failure == "state":
        run_manifest["state"] = "failed"
    elif evidence_failure == "returncode":
        run_manifest["process_returncode"] = 1
    elif evidence_failure == "network":
        run_manifest["network"]["live_interface_exposure"] = True
    elif evidence_failure == "source":
        run_manifest["flash"]["source_unchanged"] = False
    elif evidence_failure == "staged":
        run_manifest["tftp_export"]["staged_unchanged"] = False
    write_test_run_manifest_and_refresh_profile(
        run_manifest_path,
        run_manifest,
        hardware_profile_path,
    )

    with pytest.raises(ValueError, match=error_pattern):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_build_brooklyn2_image_rejects_duplicate_keys_in_evidence_manifest(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    run_manifest_path = evidence_root / "runs/test-isolated-upgrade/manifest.json"
    run_manifest_text = run_manifest_path.read_text().replace(
        '"schema_version": 1,',
        '"schema_version": 1,\n  "schema_version": 1,',
        1,
    )
    run_manifest_path.write_text(run_manifest_text)
    hardware_profile = json.loads(hardware_profile_path.read_text())
    hardware_profile["selection_evidence"]["run_manifest_sha256"] = hashlib.sha256(
        run_manifest_text.encode()
    ).hexdigest()
    hardware_profile_path.write_text(json.dumps(hardware_profile, indent=2) + "\n")

    with pytest.raises(ValueError, match="Duplicate JSON field: schema_version"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_build_brooklyn2_image_rejects_tampered_request_artifact(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    request_path = evidence_root / "runs/test-isolated-upgrade/stimuli/frames.bin"
    request_data = bytearray(request_path.read_bytes())
    request_data[0] ^= 0x01
    request_path.write_bytes(request_data)

    with pytest.raises(ValueError, match="declared SHA-256 does not match"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_build_brooklyn2_image_rejects_manifest_path_outside_trusted_evidence_root(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    outside_source = tmp_path / "outside-source.dnt"
    outside_source.write_bytes(source.read_bytes())
    run_manifest_path = evidence_root / "runs/test-isolated-upgrade/manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text())
    run_manifest["tftp_export"]["source"]["path"] = str(outside_source.resolve())
    write_test_run_manifest_and_refresh_profile(
        run_manifest_path,
        run_manifest,
        hardware_profile_path,
    )

    with pytest.raises(ValueError, match="resolves outside the trusted evidence root"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_trusted_evidence_root_descriptor_survives_pathname_replacement(tmp_path, monkeypatch):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path, source, payloads
    )
    moved_evidence_root = tmp_path / "opened-evidence-root"
    replacement_evidence_root = tmp_path / "replacement-evidence-root"
    replacement_evidence_root.mkdir()
    original_open = firmware_commands.os.open
    replaced = False

    def replace_root_pathname(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal replaced
        if path == "runs" and dir_fd is not None and not replaced:
            replaced = True
            evidence_root.rename(moved_evidence_root)
            evidence_root.symlink_to(
                replacement_evidence_root,
                target_is_directory=True,
            )
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(firmware_commands.os, "open", replace_root_pathname)

    manifest = _build_brooklyn2_image(
        source,
        tmp_path / "image",
        hardware_profile_path,
        board_information_path,
        evidence_root,
    )

    assert replaced
    assert manifest["flash"]["size"] == BROOKLYN2_FLASH_SIZE


def test_trusted_evidence_reader_rejects_directory_replaced_by_symbolic_link(tmp_path, monkeypatch):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path, source, payloads
    )
    runs_directory = evidence_root / "runs"
    moved_runs_directory = evidence_root / "opened-runs"
    replacement_runs_directory = tmp_path / "replacement-runs"
    replacement_runs_directory.mkdir()
    original_open = firmware_commands.os.open
    replaced = False

    def replace_runs_directory(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal replaced
        if path == "runs" and dir_fd is not None and not replaced:
            replaced = True
            runs_directory.rename(moved_runs_directory)
            runs_directory.symlink_to(
                replacement_runs_directory,
                target_is_directory=True,
            )
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(firmware_commands.os, "open", replace_runs_directory)

    with pytest.raises(ValueError, match="symbolic-link directory"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )

    assert replaced
    assert not (tmp_path / "image").exists()


def test_build_brooklyn2_image_rejects_staged_firmware_not_matching_source(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    run_directory = evidence_root / "runs/test-isolated-upgrade"
    staged_path = run_directory / "tftp/upgrade.dnt"
    staged_data = bytearray(staged_path.read_bytes())
    staged_data[-1] ^= 0x01
    staged_path.write_bytes(staged_data)
    staged_record = {
        "path": "tftp/upgrade.dnt",
        "sha256": hashlib.sha256(staged_data).hexdigest(),
        "size": len(staged_data),
    }
    run_manifest_path = run_directory / "manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text())
    run_manifest["tftp_export"]["staged"] = staged_record
    run_manifest["tftp_export"]["staged_final"] = staged_record
    run_manifest["artifacts"]["tftp/upgrade.dnt"] = staged_record
    write_test_run_manifest_and_refresh_profile(
        run_manifest_path,
        run_manifest,
        hardware_profile_path,
    )

    with pytest.raises(ValueError, match="does not match the compatible firmware"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_build_brooklyn2_image_rejects_result_flash_with_different_fpga_payload(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    run_directory = evidence_root / "runs/test-isolated-upgrade"
    result_flash_path = run_directory / "flash.runtime.bin"
    result_flash = bytearray(result_flash_path.read_bytes())
    result_flash[0x1A0000] ^= 0x01
    result_flash_path.write_bytes(result_flash)
    result_digest = hashlib.sha256(result_flash).hexdigest()
    run_manifest_path = run_directory / "manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text())
    run_manifest["flash"]["runtime_final"]["sha256"] = result_digest
    hardware_profile = json.loads(hardware_profile_path.read_text())
    hardware_profile["selection_evidence"]["result_flash_sha256"] = result_digest
    hardware_profile_path.write_text(json.dumps(hardware_profile, indent=2) + "\n")
    write_test_run_manifest_and_refresh_profile(
        run_manifest_path,
        run_manifest,
        hardware_profile_path,
    )

    with pytest.raises(ValueError, match="does not contain the selected FPGA payload"):
        _build_brooklyn2_image(
            source,
            tmp_path / "image",
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )


def test_build_brooklyn2_image_uses_one_immutable_source_snapshot(tmp_path, monkeypatch):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    original = source.read_bytes()
    parsed = parse_dnt(source)
    standard_fpga = next(section for section in parsed["sections"] if section["partition_id"] == 2)
    replacement = bytearray(original)
    replacement[standard_fpga["file_offset"]] ^= 0x01
    finish_dnt_checksums(replacement, parsed["header_length"])
    replacement = bytes(replacement)
    output_directory = tmp_path / "image"
    original_validation = firmware_parser._validate_dnt_checksums
    validation_count = 0

    def replace_source_after_validation(data, header_length):
        nonlocal validation_count
        original_validation(data, header_length)
        validation_count += 1
        if validation_count == 1:
            source.write_bytes(replacement)

    monkeypatch.setattr(firmware_parser, "_validate_dnt_checksums", replace_source_after_validation)

    manifest = _build_brooklyn2_image(
        source,
        output_directory,
        hardware_profile_path,
        board_information_path,
        evidence_root,
    )

    assert source.read_bytes() == replacement
    assert validation_count == 1
    assert manifest["source_sha256"] == hashlib.sha256(original).hexdigest()
    assert manifest["dnt"]["crc32"] == f"{struct.unpack_from('>I', original, 0x18)[0]:08x}"
