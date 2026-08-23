import hashlib
import json
import sys

import pytest

requires_firmware_image_publication = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"},
    reason="Brooklyn II image publication requires Linux or macOS",
)

from netaudio.commands import firmware_parser
from netaudio.commands.firmware import (
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
    _brooklyn2_board_information_manifest,
    _build_brooklyn2_board_information_partition,
    _build_brooklyn2_image,
    _load_brooklyn2_board_information_descriptor,
    _publish_output_directory_without_replacement,
)
from tests.firmware_test_support import (
    build_board_information_descriptor,
    build_expected_board_information_partition,
    write_brooklyn2_test_build_inputs,
    write_brooklyn2_test_dnt,
)


@requires_firmware_image_publication
def test_build_brooklyn2_image_generates_exact_board_information_partition(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, descriptor_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    descriptor_text = descriptor_path.read_text()
    board_information_descriptor = json.loads(descriptor_text)
    expected_partition = build_expected_board_information_partition(board_information_descriptor)
    output_directory = tmp_path / "image"

    manifest = _build_brooklyn2_image(
        source,
        output_directory,
        hardware_profile_path,
        descriptor_path,
        evidence_root,
    )

    board_information = (output_directory / "brdinfo.bin").read_bytes()
    flash = (output_directory / "flash.bin").read_bytes()
    assert board_information == expected_partition
    assert (
        flash[
            BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET : BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET
            + BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE
        ]
        == expected_partition
    )
    assert board_information[0x44] == 0xFF

    board_manifest = manifest["board_information"]
    assert board_manifest == {
        "state": "generated",
        "filename": "brdinfo.bin",
        "flash_offset": BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
        "size": BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
        "sha256": hashlib.sha256(expected_partition).hexdigest(),
        "descriptor": {
            "format_version": 2,
            "source_sha256": hashlib.sha256(descriptor_text.encode()).hexdigest(),
        },
        "identity_name": "ferrofish-a32-test-synthetic",
        "identity_kind": "synthetic",
        "identity_provenance": None,
        "identity_assertion_scope": "known_fields_only",
        "generated_partition_is_physical_dump": False,
        "known_fields": {
            "media_access_control_address": "02:1d:c1:aa:bb:cc",
            "media_access_control_address_is_locally_administered": True,
            "serial_number": 0x10203040,
            "bootloader_signed_serial_number": 0x10203040,
            "hardware_revision_major": 3,
            "hardware_revision_minor": 7,
            "configuration_date": "20260722",
        },
        "evidence_limited_unknowns": [
            {
                "offset": 0,
                "size": 10,
                "value_hex": "ffffffffffffffffffff",
                "provenance": None,
                "evidence": "Retained at the erased value because the field semantics are unknown",
            },
            {
                "offset": 0x20,
                "size": 4,
                "value_hex": "ffffffff",
                "evidence": "Retained at the erased value because an in-memory PTP default does not establish a flash value",
            },
            {
                "offset": 0x44,
                "size": 1,
                "value_hex": "ff",
                "evidence": "Retained at the erased value because its semantics and required value are unknown",
            },
        ],
    }
    physical_partition = next(
        partition
        for partition in manifest["flash"]["physical_partitions"]
        if partition["physical_partition_name"] == "brdinfo"
    )
    assert physical_partition == {
        "physical_partition_name": "brdinfo",
        "flash_offset": BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
        "capacity": BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
        "state": "generated",
        "artifact_filename": "brdinfo.bin",
        "size": BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
        "sha256": hashlib.sha256(expected_partition).hexdigest(),
    }
    assert json.loads((output_directory / "manifest.json").read_text()) == manifest


def test_board_information_manifest_records_globally_administered_address(tmp_path):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(
        json.dumps(
            build_board_information_descriptor(
                identity_kind="physical",
                identity_provenance="MAC and identity fields read from the physical A32 console",
                media_access_control_address="00:1d:c1:10:73:32",
                serial_number=0xFFFFFFFF,
            )
        )
    )

    board_information_input = _load_brooklyn2_board_information_descriptor(descriptor_path)
    partition = _build_brooklyn2_board_information_partition(board_information_input.descriptor)
    manifest = _brooklyn2_board_information_manifest(board_information_input, partition)

    assert manifest["identity_kind"] == "physical"
    assert manifest["identity_provenance"] == "MAC and identity fields read from the physical A32 console"
    assert manifest["identity_assertion_scope"] == "known_fields_only"
    assert manifest["generated_partition_is_physical_dump"] is False
    assert manifest["known_fields"]["media_access_control_address"] == "00:1d:c1:10:73:32"
    assert manifest["known_fields"]["media_access_control_address_is_locally_administered"] is False
    assert manifest["known_fields"]["serial_number"] == 0xFFFFFFFF
    assert manifest["known_fields"]["bootloader_signed_serial_number"] == -1
    assert partition[0x10:0x14] == bytes.fromhex("ffffffff")


def test_board_information_descriptor_records_provenance_for_opaque_prefix(tmp_path):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(
        json.dumps(
            build_board_information_descriptor(
                identity_kind="physical",
                identity_provenance="Prefix bytes read from the physical A32 through U-Boot",
                media_access_control_address="00:1d:c1:10:73:32",
                unidentified_prefix_hex="00010203040506070809",
                unidentified_prefix_provenance="Physical brdinfo capture sha256:0123456789abcdef",
            )
        )
    )

    board_information_input = _load_brooklyn2_board_information_descriptor(descriptor_path)
    partition = _build_brooklyn2_board_information_partition(board_information_input.descriptor)
    manifest = _brooklyn2_board_information_manifest(board_information_input, partition)

    assert partition[:10] == bytes.fromhex("00010203040506070809")
    assert manifest["evidence_limited_unknowns"][0] == {
        "offset": 0,
        "size": 10,
        "value_hex": "00010203040506070809",
        "provenance": "Physical brdinfo capture sha256:0123456789abcdef",
        "evidence": "Descriptor-supplied opaque bytes with explicit provenance; field semantics are unknown",
    }


@pytest.mark.parametrize(
    ("overrides", "error_pattern"),
    [
        ({"format_version": True}, "format_version must be an integer"),
        ({"format_version": 3}, "Unsupported board-information format version"),
        (
            {"identity_name": "A32 Synthetic"},
            "identity_name must be a lowercase kebab-case name",
        ),
        ({"identity_kind": "derived"}, "identity_kind must be synthetic or physical"),
        ({"identity_kind": 1}, "identity_kind must be synthetic or physical"),
        (
            {"identity_kind": "physical"},
            "identity_provenance must be a non-empty printable source description",
        ),
        (
            {"identity_provenance": "Synthetic source"},
            "identity_provenance is only valid for physical identity",
        ),
        ({"media_access_control_address": "021dc1aabbcc"}, "six hexadecimal octets"),
        ({"media_access_control_address": "03:1d:c1:aa:bb:cc"}, "must be unicast"),
        (
            {"media_access_control_address": "00:00:00:00:00:00"},
            "must not be all zeroes",
        ),
        (
            {"media_access_control_address": "00:1d:c1:aa:bb:cc"},
            "requires a locally administered",
        ),
        ({"serial_number": False}, "serial_number must be an integer"),
        ({"serial_number": -1}, "serial_number must be between"),
        ({"serial_number": 0x80000000}, "serial_number must not exceed 2147483647"),
        ({"serial_number": 0x100000000}, "serial_number must be between"),
        ({"hardware_revision_major": -1}, "hardware_revision_major must be between"),
        (
            {"hardware_revision_major": 0x10000},
            "hardware_revision_major must be between",
        ),
        (
            {"hardware_revision_minor": 1.5},
            "hardware_revision_minor must be an integer",
        ),
        (
            {"hardware_revision_minor": 0x10000},
            "hardware_revision_minor must be between",
        ),
        (
            {"configuration_date": 20260722},
            "configuration_date must contain exactly 8 ASCII decimal digits",
        ),
        (
            {"configuration_date": "2026072"},
            "configuration_date must contain exactly 8 ASCII decimal digits",
        ),
        (
            {"configuration_date": "2026é722"},
            "configuration_date must contain exactly 8 ASCII decimal digits",
        ),
        (
            {"configuration_date": "BAD!DATE"},
            "configuration_date must contain exactly 8 ASCII decimal digits",
        ),
        (
            {"unidentified_prefix_hex": 1},
            "unidentified_prefix_hex must encode exactly 10 bytes",
        ),
        (
            {"unidentified_prefix_hex": "000102030405060708"},
            "unidentified_prefix_hex must encode exactly 10 bytes",
        ),
        (
            {"unidentified_prefix_hex": "0001020304050607080z"},
            "unidentified_prefix_hex must encode exactly 10 bytes",
        ),
        (
            {"unidentified_prefix_hex": "00010203040506070809"},
            "only supported for physical identity",
        ),
        (
            {
                "identity_kind": "physical",
                "identity_provenance": "Physical A32 console capture",
                "media_access_control_address": "00:1d:c1:10:73:32",
                "unidentified_prefix_hex": "00010203040506070809",
                "unidentified_prefix_provenance": " evidence",
            },
            "unidentified_prefix_provenance must be a non-empty printable source description",
        ),
        (
            {
                "unidentified_prefix_hex": "ffffffffffffffffffff",
                "unidentified_prefix_provenance": "Unneeded evidence",
            },
            "only valid for a non-erased prefix",
        ),
    ],
)
def test_board_information_descriptor_rejects_invalid_fields(tmp_path, overrides, error_pattern):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(json.dumps(build_board_information_descriptor(**overrides)))

    with pytest.raises(ValueError, match=error_pattern):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


def test_board_information_descriptor_rejects_schema_mismatch(tmp_path):
    descriptor = build_board_information_descriptor(unexpected=True)
    del descriptor["serial_number"]
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(json.dumps(descriptor))

    with pytest.raises(ValueError, match="missing fields: serial_number; unexpected fields: unexpected"):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


@pytest.mark.parametrize(
    ("contents", "error_pattern"),
    [
        ("[]", "must be a JSON object"),
        ("{", "Invalid board-information JSON"),
        (
            '{"format_version": 1, "format_version": 1}',
            "Duplicate JSON field: format_version",
        ),
        ('{"format_version": NaN}', "Non-standard JSON constant: NaN"),
    ],
)
def test_board_information_descriptor_rejects_invalid_json(tmp_path, contents, error_pattern):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(contents)

    with pytest.raises(ValueError, match=error_pattern):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


def test_board_information_descriptor_rejects_non_utf8_input(tmp_path):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_bytes(b"\xff")

    with pytest.raises(ValueError, match="not UTF-8"):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


@requires_firmware_image_publication
def test_output_publisher_refuses_concurrently_created_destination(tmp_path, monkeypatch):
    temporary_directory = tmp_path / "temporary"
    temporary_directory.mkdir()
    (temporary_directory / "flash.bin").write_text("complete flash")
    (temporary_directory / "manifest.json").write_text("complete manifest")
    output_directory = tmp_path / "image"
    observed_directory_contents = []
    native_publication = firmware_parser._atomic_publish_directory_without_replacement

    def create_concurrent_destination(staging_directory, destination_directory):
        destination_directory.mkdir()
        (destination_directory / "concurrent-owner").write_text("preserve")
        observed_directory_contents.append(sorted(path.name for path in destination_directory.iterdir()))
        native_publication(staging_directory, destination_directory)

    monkeypatch.setattr(
        firmware_parser,
        "_atomic_publish_directory_without_replacement",
        create_concurrent_destination,
    )

    with pytest.raises(ValueError, match="already exists"):
        _publish_output_directory_without_replacement(
            temporary_directory,
            output_directory,
        )

    assert observed_directory_contents == [["concurrent-owner"]]
    assert sorted(path.name for path in output_directory.iterdir()) == ["concurrent-owner"]
    assert (output_directory / "concurrent-owner").read_text() == "preserve"
    assert sorted(path.name for path in temporary_directory.iterdir()) == [
        "flash.bin",
        "manifest.json",
    ]


@requires_firmware_image_publication
def test_build_brooklyn2_image_refuses_dangling_output_symlink(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    output_directory = tmp_path / "image"
    output_directory.symlink_to(tmp_path / "missing", target_is_directory=True)

    with pytest.raises(ValueError, match="already exists"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )

    assert output_directory.is_symlink()


@requires_firmware_image_publication
def test_build_brooklyn2_image_refuses_existing_output_directory(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = write_brooklyn2_test_build_inputs(
        tmp_path,
        source,
        payloads,
    )
    output_directory = tmp_path / "image"
    output_directory.mkdir()

    with pytest.raises(ValueError, match="already exists"):
        _build_brooklyn2_image(
            source,
            output_directory,
            hardware_profile_path,
            board_information_path,
            evidence_root,
        )
