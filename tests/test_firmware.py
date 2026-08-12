import hashlib
import json
import shutil
import sqlite3
import struct
import zlib
from pathlib import Path

import pytest

from netaudio.commands import firmware as firmware_commands
from netaudio.commands.firmware import (
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
    BROOKLYN2_FLASH_SIZE,
    BROOKLYN2_HARDWARE_PROFILE_FORMAT_VERSION,
    BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET,
    BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE,
    DNT_PARSER_VERSION,
    FIRMWARE_DATABASE_SCHEMA_VERSION,
    PARTITION_NAMES,
    _brooklyn2_board_information_manifest,
    _build_brooklyn2_board_information_partition,
    _build_brooklyn2_image,
    _extract_capability_9,
    _init_db,
    _load_brooklyn2_board_information_descriptor,
    _load_brooklyn2_hardware_profile,
    _load_resume_results,
    _parse_sections,
    _publish_output_directory_without_replacement,
    firmware_extract,
    parse_dnt,
)


def finish_dnt_checksums(data, header_length):
    header = bytearray(data[:header_length])
    header[0x18:0x1C] = bytes(4)
    header[0x3C:0x40] = bytes(4)
    struct.pack_into(">I", data, 0x3C, zlib.crc32(header))
    checksum_data = bytearray(data)
    checksum_data[0x18:0x1C] = bytes(4)
    struct.pack_into(">I", data, 0x18, zlib.crc32(checksum_data))


def build_dnt(section_body=b"firmware"):
    header_length = 0x80
    section_body_offset = 0x20
    data = bytearray(header_length + section_body_offset + len(section_body))
    data[:4] = b"AUDI"
    struct.pack_into(">I", data, 0x04, header_length)
    struct.pack_into(">III", data, 0x24, 0x40, 1, 0x10)
    struct.pack_into(">III", data, 0x30, 0x50, 2, 0x10)
    struct.pack_into(
        ">IIII", data, 0x40, 1, 0x01020304, section_body_offset, len(section_body)
    )
    struct.pack_into(">IIII", data, 0x50, 0, 0, section_body_offset, 15)
    struct.pack_into(">IIII", data, 0x60, 0, 1, section_body_offset, 48)
    data[header_length + section_body_offset :] = section_body
    finish_dnt_checksums(data, header_length)
    return data


def build_uimage(data, image_type, load_address):
    header = bytearray(64)
    struct.pack_into(">I", header, 0, 0x27051956)
    struct.pack_into(
        ">IIII", header, 12, len(data), load_address, load_address, zlib.crc32(data)
    )
    header[28:32] = bytes((5, 14, image_type, 0))
    header[32:36] = b"Test"
    checksum_header = bytearray(header)
    checksum_header[4:8] = bytes(4)
    struct.pack_into(">I", header, 4, zlib.crc32(checksum_header))
    return bytes(header) + data


def encode_fixed_utf8(value, size):
    encoded = value.encode("utf-8")
    if len(encoded) > size:
        raise ValueError(
            f"Value requires {len(encoded)} bytes but field contains {size}"
        )
    return encoded.ljust(size, b"\x00")


def build_capability_payload(
    transmit_channel_names=("Input 01", "", "Input 03"),
    receive_channel_names=("Output 01", "Output 02"),
    board_field=b"A32\x00l\x00ls",
    manufacturer_short="Test",
    model_id=b"\x00\x00\x00\x00\x00\x00\x00\x01",
    manufacturer="Ferrofish GmbH",
    product_name="A32 Dante AD/DA Converter²",
):
    if len(board_field) != 8:
        raise ValueError("Board field must contain 8 bytes")
    if len(model_id) != 8:
        raise ValueError("Model ID must contain 8 bytes")

    device_descriptor_offset = 0x20
    transmit_channel_names_offset = 0x40
    receive_channel_names_offset = (
        transmit_channel_names_offset + len(transmit_channel_names) * 32
    )
    oem_descriptor_offset = (
        receive_channel_names_offset + len(receive_channel_names) * 32
    )
    size = oem_descriptor_offset + 0x114
    payload = bytearray(size)
    payload[:8] = b"Audinate"
    struct.pack_into(">BBHHH", payload, 8, 1, 0, size, 20, device_descriptor_offset)
    payload[device_descriptor_offset : device_descriptor_offset + 8] = board_field
    struct.pack_into(
        ">HH",
        payload,
        device_descriptor_offset + 8,
        len(transmit_channel_names),
        len(receive_channel_names),
    )
    struct.pack_into(
        ">HH",
        payload,
        device_descriptor_offset + 0x10,
        transmit_channel_names_offset,
        receive_channel_names_offset,
    )
    struct.pack_into(
        ">H", payload, device_descriptor_offset + 0x1C, oem_descriptor_offset
    )
    for channel_index, channel_name in enumerate(transmit_channel_names):
        channel_offset = transmit_channel_names_offset + channel_index * 32
        payload[channel_offset : channel_offset + 32] = encode_fixed_utf8(
            channel_name, 32
        )
    for channel_index, channel_name in enumerate(receive_channel_names):
        channel_offset = receive_channel_names_offset + channel_index * 32
        payload[channel_offset : channel_offset + 32] = encode_fixed_utf8(
            channel_name, 32
        )
    payload[oem_descriptor_offset + 4 : oem_descriptor_offset + 12] = encode_fixed_utf8(
        manufacturer_short, 8
    )
    payload[oem_descriptor_offset + 0x0C : oem_descriptor_offset + 0x14] = model_id
    payload[oem_descriptor_offset + 0x14 : oem_descriptor_offset + 0x94] = (
        encode_fixed_utf8(manufacturer, 128)
    )
    payload[oem_descriptor_offset + 0x94 : oem_descriptor_offset + 0x114] = (
        encode_fixed_utf8(product_name, 128)
    )
    checksum_payload = bytearray(payload)
    checksum_payload[16:20] = bytes(4)
    struct.pack_into(">I", payload, 16, zlib.crc32(checksum_payload))
    return bytes(payload)


def build_cramfs_payload(size=0x4C):
    payload = bytearray(size)
    struct.pack_into("<III", payload, 0, 0x28CD3D45, size, 1)
    struct.pack_into("<I", payload, 0x2C, 1)
    checksum_payload = bytearray(payload)
    checksum_payload[0x20:0x24] = bytes(4)
    struct.pack_into("<I", payload, 0x20, zlib.crc32(checksum_payload))
    return bytes(payload)


def build_fpga_payload(checksum):
    words = (
        0x31C2,
        0x0400,
        0x4093,
        0x3040,
        0x5000,
        0x0000,
        0x0001,
        0x1234,
        0x5678,
        0x9ABC,
        0x3002,
        checksum >> 16,
        checksum & 0xFFFF,
        0x30A1,
        0x000D,
        0x2000,
    )
    return (
        bytes([0xFF]) * 16
        + b"\xaa\x99\x55\x66"
        + struct.pack(f">{len(words)}H", *words)
    )


def build_brooklyn2_kernel_payload():
    payload = bytearray(0xF8000)
    payload.extend(build_cramfs_payload())
    checksum = sum(payload)
    checksum = (checksum & 0xFFFF) + (checksum >> 16)
    checksum = (checksum & 0xFFFF) + (checksum >> 16)
    payload.extend(struct.pack(">I", checksum))
    return build_uimage(bytes(payload), 2, 0x28000000)


def build_partitioned_dnt(partitions):
    header_length = 0xC0
    body_size = sum(len(payload) for _, _, payload in partitions)
    data = bytearray(header_length + body_size)
    data[:4] = b"AUDI"
    struct.pack_into(">IIII", data, 4, header_length, 3, 1, 9)
    data[20:24] = bytes((4, 0, 8, 2))
    data[0x1C:0x24] = b"Test\x00\x00\x00\x00"
    struct.pack_into(">III", data, 0x24, 0x40, len(partitions), 0x10)
    struct.pack_into(">III", data, 0x30, 0x40 + len(partitions) * 0x10, 0, 0x10)
    body_offset = 0
    for partition_index, (partition_id, version, payload) in enumerate(partitions):
        struct.pack_into(
            ">IIII",
            data,
            0x40 + partition_index * 0x10,
            partition_id,
            version,
            body_offset,
            len(payload),
        )
        data[
            header_length + body_offset : header_length + body_offset + len(payload)
        ] = payload
        body_offset += len(payload)
    finish_dnt_checksums(data, header_length)
    return data


def write_brooklyn2_test_dnt(path):
    payloads = {
        "bootloader_data": b"production bootloader",
        "standard_fpga": build_fpga_payload(0x00102451),
        "revision_three_fpga": build_fpga_payload(0x003CAC01),
        "image": build_brooklyn2_kernel_payload(),
        "capability": build_capability_payload(),
        "user": build_cramfs_payload(),
    }
    payloads["boot"] = build_uimage(payloads["bootloader_data"], 5, 0x29FC0000)
    path.write_bytes(
        build_partitioned_dnt(
            [
                (6, 0x01020304, payloads["boot"]),
                (2, 0x01020304, payloads["standard_fpga"]),
                (12, 0x01020304, payloads["revision_three_fpga"]),
                (1, 0x01020304, payloads["image"]),
                (9, 0x01020304, payloads["capability"]),
                (11, 0x01020304, payloads["user"]),
            ]
        )
    )
    return payloads


def build_board_information_descriptor(**overrides):
    descriptor = {
        "format_version": 2,
        "identity_name": "ferrofish-a32-test-synthetic",
        "identity_kind": "synthetic",
        "media_access_control_address": "02:1D:C1:AA:BB:CC",
        "serial_number": 0x10203040,
        "hardware_revision_major": 3,
        "hardware_revision_minor": 7,
        "configuration_date": "20260722",
    }
    descriptor.update(overrides)
    return descriptor


def build_expected_board_information_partition(descriptor):
    partition = bytearray([0xFF]) * BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE
    partition[0x0A:0x10] = bytes.fromhex(
        descriptor["media_access_control_address"].replace(":", "")
    )
    struct.pack_into(">I", partition, 0x10, descriptor["serial_number"])
    struct.pack_into(">H", partition, 0x14, descriptor["hardware_revision_major"])
    struct.pack_into(">H", partition, 0x16, descriptor["hardware_revision_minor"])
    partition[0x18:0x20] = descriptor["configuration_date"].encode("ascii")
    return bytes(partition)


def build_hardware_profile_descriptor(
    firmware_sha256,
    board_information_sha256,
    selection_evidence,
    **overrides,
):
    descriptor = {
        "format_version": 2,
        "profile_name": "brooklyn2-standard-test-profile",
        "device_type_identifier": 9,
        "compatible_board_information_sha256": board_information_sha256,
        "compatible_firmware_sha256": firmware_sha256,
        "payload_partition_identifiers": {
            "boot": 6,
            "fpga": 2,
            "image": 1,
            "userarea": 11,
            "cap1": 9,
        },
        "selection_scope": "exact_board_information_and_firmware",
        "causal_selector_status": "unknown",
        "selection_evidence": selection_evidence,
    }
    descriptor.update(overrides)
    return descriptor


def write_brooklyn2_test_build_inputs(tmp_path, source, payloads):
    board_information_descriptor = build_board_information_descriptor()
    board_information_path = tmp_path / "board-information.json"
    board_information_path.write_text(
        json.dumps(board_information_descriptor, indent=2) + "\n"
    )
    board_information_payload = build_expected_board_information_partition(
        board_information_descriptor
    )
    evidence_root = tmp_path / "evidence-root"
    run_directory = evidence_root / "runs" / "test-isolated-upgrade"
    tftp_directory = run_directory / "tftp"
    stimulus_directory = run_directory / "stimuli"
    source_flash_directory = run_directory / "source"
    tftp_directory.mkdir(parents=True)
    stimulus_directory.mkdir()
    source_flash_directory.mkdir()
    firmware_data = source.read_bytes()
    tftp_source_path = tftp_directory / "source.dnt"
    tftp_staged_path = tftp_directory / "upgrade.dnt"
    tftp_source_path.write_bytes(firmware_data)
    tftp_staged_path.write_bytes(firmware_data)
    request_frame_path = stimulus_directory / "frames.bin"
    request_index_path = stimulus_directory / "index.bin"
    request_frame_path.write_bytes(b"structured upgrade request")
    request_index_path.write_bytes(b"one request index")
    source_flash = bytearray([0xFF]) * BROOKLYN2_FLASH_SIZE
    source_flash[
        BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET : BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET
        + len(board_information_payload)
    ] = board_information_payload
    source_flash[0x1A0000 : 0x1A0000 + len(payloads["revision_three_fpga"])] = payloads[
        "revision_three_fpga"
    ]
    source_flash_path = source_flash_directory / "flash.bin"
    source_flash_path.write_bytes(source_flash)
    result_flash = bytearray(source_flash)
    result_flash[0x1A0000:0x2A0000] = bytes([0xFF]) * 0x100000
    result_flash[0x1A0000 : 0x1A0000 + len(payloads["standard_fpga"])] = payloads[
        "standard_fpga"
    ]
    result_flash_path = run_directory / "flash.runtime.bin"
    result_flash_path.write_bytes(result_flash)

    def file_record(path, reference):
        data = path.read_bytes()
        return {
            "path": reference,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
        }

    tftp_source_record = file_record(tftp_source_path, "tftp/source.dnt")
    tftp_staged_record = file_record(tftp_staged_path, "tftp/upgrade.dnt")
    request_frame_record = file_record(request_frame_path, "stimuli/frames.bin")
    request_index_record = file_record(request_index_path, "stimuli/index.bin")
    source_flash_record = file_record(source_flash_path, "source/flash.bin")
    result_flash_record = file_record(result_flash_path, "flash.runtime.bin")
    runtime_initial_record = {
        "path": "flash.runtime.bin",
        "sha256": source_flash_record["sha256"],
        "size": source_flash_record["size"],
    }
    run_manifest = {
        "schema_version": 1,
        "state": "completed",
        "process_returncode": 0,
        "network": {
            "transport": "isolated-hub",
            "live_interface_exposure": False,
            "tftp_export_enabled": True,
        },
        "stimulus_log": {
            "count": 1,
            "data_path": "stimuli/frames.bin",
            "index_path": "stimuli/index.bin",
            "index_record_size": len(request_index_path.read_bytes()),
        },
        "tftp_export": {
            "schema_version": 1,
            "maximum_size": 64 * 1024 * 1024,
            "source": tftp_source_record,
            "staged": tftp_staged_record,
            "staged_final": tftp_staged_record,
            "staged_unchanged": True,
            "verified": True,
        },
        "flash": {
            "runtime_initial": runtime_initial_record,
            "runtime_final": result_flash_record,
            "source_initial": source_flash_record,
            "source_final": source_flash_record,
            "source_unchanged": True,
        },
        "artifacts": {
            "stimuli/frames.bin": request_frame_record,
            "stimuli/index.bin": request_index_record,
            "tftp/upgrade.dnt": tftp_staged_record,
        },
    }
    run_manifest_path = run_directory / "manifest.json"
    run_manifest_path.write_text(json.dumps(run_manifest, indent=2) + "\n")
    run_manifest_sha256 = hashlib.sha256(run_manifest_path.read_bytes()).hexdigest()
    selection_evidence = {
        "kind": "isolated_firmware_upgrade",
        "run_name": "test-isolated-upgrade",
        "run_manifest_path": "runs/test-isolated-upgrade/manifest.json",
        "run_manifest_sha256": run_manifest_sha256,
        "request_frame_path": "runs/test-isolated-upgrade/stimuli/frames.bin",
        "request_frame_sha256": request_frame_record["sha256"],
        "result_flash_path": "runs/test-isolated-upgrade/flash.runtime.bin",
        "result_flash_sha256": result_flash_record["sha256"],
        "observed_fpga_partition_identifier": 2,
        "observed_fpga_payload_sha256": hashlib.sha256(
            payloads["standard_fpga"]
        ).hexdigest(),
    }
    hardware_profile_descriptor = build_hardware_profile_descriptor(
        hashlib.sha256(firmware_data).hexdigest(),
        hashlib.sha256(board_information_payload).hexdigest(),
        selection_evidence,
    )
    hardware_profile_path = tmp_path / "hardware-profile.json"
    hardware_profile_path.write_text(
        json.dumps(hardware_profile_descriptor, indent=2) + "\n"
    )
    return hardware_profile_path, board_information_path, evidence_root


def write_test_run_manifest_and_refresh_profile(
    run_manifest_path, run_manifest, hardware_profile_path
):
    run_manifest_path.write_text(json.dumps(run_manifest, indent=2) + "\n")
    hardware_profile = json.loads(hardware_profile_path.read_text())
    hardware_profile["selection_evidence"]["run_manifest_sha256"] = hashlib.sha256(
        run_manifest_path.read_bytes()
    ).hexdigest()
    hardware_profile_path.write_text(json.dumps(hardware_profile, indent=2) + "\n")


def test_parse_sections_uses_declared_table_and_body_relative_offsets():
    data = build_dnt()

    assert _parse_sections(data, 0x80) == [
        {
            "partition_id": 1,
            "partition_name": "image",
            "version": "1.2.3.4",
            "body_offset": 0x20,
            "file_offset": 0xA0,
            "size": 8,
        }
    ]


def test_parse_sections_rejects_table_outside_header():
    data = build_dnt()
    struct.pack_into(">I", data, 0x28, 5)

    with pytest.raises(ValueError, match="section table extends past header"):
        _parse_sections(data, 0x80)


@pytest.mark.parametrize("header_length", [0x3F, 0x1000])
def test_parse_sections_rejects_invalid_header_length(header_length):
    with pytest.raises(ValueError, match="Invalid DNT header length"):
        _parse_sections(build_dnt(), header_length)


def test_parse_sections_rejects_invalid_table_offset():
    data = build_dnt()
    struct.pack_into(">I", data, 0x24, 0x20)

    with pytest.raises(ValueError, match="Invalid DNT section table offset"):
        _parse_sections(data, 0x80)


def test_parse_sections_rejects_invalid_entry_size():
    data = build_dnt()
    struct.pack_into(">I", data, 0x2C, 8)

    with pytest.raises(ValueError, match="Invalid DNT section entry size"):
        _parse_sections(data, 0x80)


def test_parse_sections_rejects_section_outside_file():
    data = build_dnt()
    struct.pack_into(">I", data, 0x4C, len(data))

    with pytest.raises(ValueError, match="section 0 extends past end of file"):
        _parse_sections(data, 0x80)


def test_partition_names_match_updater_partition_identifiers():
    assert PARTITION_NAMES[1] == "image"
    assert PARTITION_NAMES[2] == "fpga"
    assert PARTITION_NAMES[6] == "boot"
    assert PARTITION_NAMES[9] == "cap1"
    assert PARTITION_NAMES[11] == "user"
    assert PARTITION_NAMES[12] == "fpgar3"


def test_firmware_extract_reads_from_absolute_section_offset(tmp_path):
    source = tmp_path / "source.dnt"
    output = tmp_path / "section.bin"
    source.write_bytes(build_dnt(b"authentic-section"))

    firmware_extract(source, 0, output)

    assert output.read_bytes() == b"authentic-section"


def test_parse_dnt_versions_result_and_exposes_explicit_offsets(tmp_path):
    source = tmp_path / "source.dnt"
    source.write_bytes(build_dnt())

    result = parse_dnt(source)

    assert result["dnt_parser_version"] == DNT_PARSER_VERSION
    assert result["sections"][0]["body_offset"] == 0x20
    assert result["sections"][0]["file_offset"] == 0xA0


def test_parse_dnt_reads_structural_capability_descriptors(tmp_path):
    capability_payload = build_capability_payload()
    source = tmp_path / "source.dnt"
    source.write_bytes(build_partitioned_dnt([(9, 0x01020304, capability_payload)]))

    result = parse_dnt(source)

    assert result["board_name"] == "A32"
    assert result["tx_channel_names"] == ["Input 01", "", "Input 03"]
    assert result["tx_channel_count"] == 3
    assert result["rx_channel_names"] == ["Output 01", "Output 02"]
    assert result["rx_channel_count"] == 2
    assert result["model_id"] == "0000000000000001"
    assert result["manufacturer_short"] == "Test"
    assert result["manufacturer"] == "Ferrofish GmbH"
    assert result["product_name"] == "A32 Dante AD/DA Converter²"


def test_capability_parser_has_no_fixed_channel_count_limit():
    transmit_channel_names = tuple(
        f"Input {channel_index:03d}" for channel_index in range(129)
    )
    capability_payload = build_capability_payload(
        transmit_channel_names=transmit_channel_names
    )

    result = _extract_capability_9(capability_payload, b"Test\x00\x00\x00\x00")

    assert result["tx_channel_count"] == 129
    assert result["tx_channel_names"] == list(transmit_channel_names)


def test_capability_parser_rejects_channel_array_outside_payload():
    capability_payload = bytearray(build_capability_payload())
    device_descriptor_offset = struct.unpack_from(">H", capability_payload, 14)[0]
    struct.pack_into(">H", capability_payload, device_descriptor_offset + 8, 0xFFFF)
    capability_payload[16:20] = bytes(4)
    struct.pack_into(">I", capability_payload, 16, zlib.crc32(capability_payload))

    with pytest.raises(
        ValueError, match="transmit channel name array extends past capability payload"
    ):
        _extract_capability_9(bytes(capability_payload), b"Test\x00\x00\x00\x00")


def test_capability_parser_rejects_manufacturer_header_mismatch():
    with pytest.raises(ValueError, match="does not match the DNT manufacturer header"):
        _extract_capability_9(build_capability_payload(), b"Other\x00\x00\x00")


def test_parse_dnt_rejects_invalid_file_checksum(tmp_path):
    source = tmp_path / "source.dnt"
    data = build_dnt()
    data[-1] ^= 0xFF
    source.write_bytes(data)

    with pytest.raises(ValueError, match="Invalid DNT checksum"):
        parse_dnt(source)


def test_parse_dnt_rejects_invalid_header_checksum(tmp_path):
    source = tmp_path / "source.dnt"
    data = build_dnt()
    data[0x08] ^= 0xFF
    checksum_data = bytearray(data)
    checksum_data[0x18:0x1C] = bytes(4)
    struct.pack_into(">I", data, 0x18, zlib.crc32(checksum_data))
    source.write_bytes(data)

    with pytest.raises(ValueError, match="Invalid DNT header checksum"):
        parse_dnt(source)


def test_resume_results_require_current_parser_version(tmp_path):
    resume_path = tmp_path / "resume.json"
    resume_path.write_text(json.dumps([{"file": "source.dnt"}]))

    with pytest.raises(ValueError, match="incompatible with DNT parser version"):
        _load_resume_results(resume_path)


def test_resume_results_accept_current_parser_version(tmp_path):
    expected_results = [
        {"dnt_parser_version": DNT_PARSER_VERSION, "file": "source.dnt"}
    ]
    resume_path = tmp_path / "resume.json"
    resume_path.write_text(json.dumps(expected_results))

    assert _load_resume_results(resume_path) == expected_results


def test_firmware_database_has_explicit_offset_columns(tmp_path):
    connection = _init_db(tmp_path / "firmware.db")

    assert (
        connection.execute("PRAGMA user_version").fetchone()[0]
        == FIRMWARE_DATABASE_SCHEMA_VERSION
    )
    section_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(sections)").fetchall()
    }
    assert "body_offset" in section_columns
    assert "file_offset" in section_columns
    assert "offset" not in section_columns
    connection.close()


def test_firmware_database_rejects_unversioned_schema(tmp_path):
    database_path = tmp_path / "firmware.db"
    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE firmware (sha256 TEXT PRIMARY KEY)")
    connection.execute(
        "CREATE TABLE sections (sha256 TEXT, idx INTEGER, offset INTEGER)"
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="schema version 0 is incompatible"):
        _init_db(database_path)


def test_build_brooklyn2_image_uses_evidence_scoped_hardware_profile_mapping(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    assert (
        flash[0x1A0000 : 0x1A0000 + len(payloads["standard_fpga"])]
        == payloads["standard_fpga"]
    )
    assert payloads["revision_three_fpga"] not in flash
    assert flash[0x2A0000 : 0x2A0000 + len(payloads["image"])] == payloads["image"]
    assert flash[0x5C0000 : 0x5C0000 + len(payloads["user"])] == payloads["user"]
    assert flash[0x7C0000:0x7E0000] == bytes([0xFF]) * 0x20000
    assert (
        flash[0x7E0000 : 0x7E0000 + len(payloads["capability"])]
        == payloads["capability"]
    )
    assert (output_directory / "bootloader.bin").read_bytes() == payloads[
        "bootloader_data"
    ]
    assert manifest["format_version"] == BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION
    assert (
        manifest["hardware_profile"]["profile_name"]
        == "brooklyn2-standard-test-profile"
    )
    assert manifest["hardware_profile"]["payload_partition_identifiers"]["fpga"] == 2
    assert (
        manifest["hardware_profile"]["selection_scope"]
        == "exact_board_information_and_firmware"
    )
    assert manifest["hardware_profile"]["causal_selector_status"] == "unknown"
    assert manifest["board_information"]["state"] == "generated"
    assert (output_directory / "brdinfo.bin").exists()
    assert [section["partition_id"] for section in manifest["unused_dnt_sections"]] == [
        12
    ]
    assert json.loads((output_directory / "manifest.json").read_text()) == manifest


def test_build_brooklyn2_image_installs_validated_protected_capability_partition(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    protected_capability_partition_end = (
        BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET
        + len(protected_capability_partition)
    )
    assert (
        flash[
            BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET:protected_capability_partition_end
        ]
        == protected_capability_partition
    )
    assert (
        flash[
            protected_capability_partition_end : BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_OFFSET
            + BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE
        ]
        == bytes([0xFF])
        * (
            BROOKLYN2_PROTECTED_CAPABILITY_PARTITION_SIZE
            - len(protected_capability_partition)
        )
    )
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
        "validation": firmware_commands._validate_cramfs_payload(
            protected_capability_partition
        ),
    }
    assert json.loads((output_directory / "manifest.json").read_text()) == manifest


def test_build_brooklyn2_image_rejects_invalid_protected_capability_partition(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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


def test_build_brooklyn2_image_is_content_reproducible_across_input_paths(
    tmp_path, monkeypatch
):
    first_input_directory = tmp_path / "first-inputs"
    first_input_directory.mkdir()
    first_source = first_input_directory / "first-firmware-name.dnt"
    payloads = write_brooklyn2_test_dnt(first_source)
    first_profile, first_board_information, first_evidence_root = (
        write_brooklyn2_test_build_inputs(
            first_input_directory,
            first_source,
            payloads,
        )
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

    assert sorted(path.name for path in first_output.iterdir()) == sorted(
        path.name for path in second_output.iterdir()
    )
    for first_artifact in first_output.iterdir():
        assert (
            first_artifact.read_bytes()
            == (second_output / first_artifact.name).read_bytes()
        )


def test_hardware_profile_manifest_preserves_exact_selection_evidence(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    )
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
        "compatible_board_information_sha256": profile_descriptor[
            "compatible_board_information_sha256"
        ],
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
            key: value
            for key, value in profile_descriptor["selection_evidence"].items()
            if not key.endswith("_path")
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
def test_hardware_profile_rejects_invalid_top_level_fields(
    tmp_path, field_name, value, error_pattern
):
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

    with pytest.raises(
        ValueError, match="payload partition identifiers must be unique"
    ):
        _load_brooklyn2_hardware_profile(profile_path)


def test_hardware_profile_rejects_mapping_that_disagrees_with_observation(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor["selection_evidence"]["observed_fpga_partition_identifier"] = 12
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(
        ValueError, match="does not match the observed selection evidence"
    ):
        _load_brooklyn2_hardware_profile(profile_path)


def test_hardware_profile_rejects_evidence_path_parent_traversal(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    profile_path, _, _ = write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
    profile_descriptor = json.loads(profile_path.read_text())
    profile_descriptor["selection_evidence"][
        "result_flash_path"
    ] = "../outside/flash.bin"
    profile_path.write_text(json.dumps(profile_descriptor))

    with pytest.raises(ValueError, match="canonical relative POSIX path"):
        _load_brooklyn2_hardware_profile(profile_path)


def test_build_brooklyn2_image_rejects_board_information_outside_profile_scope(
    tmp_path,
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
    )
    board_information_path.write_text(
        json.dumps(build_board_information_descriptor(serial_number=7))
    )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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


def test_trusted_evidence_root_descriptor_survives_pathname_replacement(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
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


def test_trusted_evidence_reader_rejects_directory_replaced_by_symbolic_link(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(tmp_path, source, payloads)
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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


def test_build_brooklyn2_image_uses_one_immutable_source_snapshot(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
    )
    original = source.read_bytes()
    parsed = parse_dnt(source)
    standard_fpga = next(
        section for section in parsed["sections"] if section["partition_id"] == 2
    )
    replacement = bytearray(original)
    replacement[standard_fpga["file_offset"]] ^= 0x01
    finish_dnt_checksums(replacement, parsed["header_length"])
    replacement = bytes(replacement)
    output_directory = tmp_path / "image"
    original_validation = firmware_commands._validate_dnt_checksums
    validation_count = 0

    def replace_source_after_validation(data, header_length):
        nonlocal validation_count
        original_validation(data, header_length)
        validation_count += 1
        if validation_count == 1:
            source.write_bytes(replacement)

    monkeypatch.setattr(
        firmware_commands, "_validate_dnt_checksums", replace_source_after_validation
    )

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
    assert (
        manifest["dnt"]["crc32"] == f"{struct.unpack_from('>I', original, 0x18)[0]:08x}"
    )


def test_production_profile_rebuilds_canonical_evidence_assessed_image(tmp_path):
    repository_root = Path(__file__).resolve().parents[2]
    firmware_sha256 = "ed46a64237c72433f33de491bffe3c95cf7e85f22279fc3758aa39c9ffc706e6"
    canonical_directory = (
        repository_root
        / "firmware/emulator_images/ferrofish-a32-4.0.8.2-brooklyn2-standard-artifact-integrity-verified-synthetic-001"
    )
    output_directory = tmp_path / "image"

    manifest = _build_brooklyn2_image(
        repository_root / f"firmware/dnt_cache/{firmware_sha256}.dnt",
        output_directory,
        repository_root
        / "firmware/emulator_hardware_profiles/brooklyn2-standard-a32-synthetic-001.json",
        repository_root
        / "firmware/emulator_identities/ferrofish-a32-synthetic-001.json",
        repository_root,
    )

    assert manifest["format_version"] == BROOKLYN2_IMAGE_MANIFEST_FORMAT_VERSION
    assessment = manifest["hardware_profile"]["evidence_assessment"]
    assert assessment["transaction_semantics"] == "not_verified_by_image_builder"
    assert assessment["run_manifest_claims"] == {
        "completion": "asserted_by_digest_bound_run_manifest",
        "network_isolation": "asserted_by_digest_bound_run_manifest",
        "source_flash_immutability": "asserted_by_digest_bound_run_manifest",
        "staged_firmware_immutability": "asserted_by_digest_bound_run_manifest",
    }
    assert sorted(path.name for path in output_directory.iterdir()) == sorted(
        path.name for path in canonical_directory.iterdir()
    )
    for generated_artifact in output_directory.iterdir():
        assert (
            generated_artifact.read_bytes()
            == (canonical_directory / generated_artifact.name).read_bytes()
        )


def test_build_brooklyn2_image_generates_exact_board_information_partition(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, descriptor_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
    )
    descriptor_text = descriptor_path.read_text()
    board_information_descriptor = json.loads(descriptor_text)
    expected_partition = build_expected_board_information_partition(
        board_information_descriptor
    )
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

    board_information_input = _load_brooklyn2_board_information_descriptor(
        descriptor_path
    )
    partition = _build_brooklyn2_board_information_partition(
        board_information_input.descriptor
    )
    manifest = _brooklyn2_board_information_manifest(board_information_input, partition)

    assert manifest["identity_kind"] == "physical"
    assert (
        manifest["identity_provenance"]
        == "MAC and identity fields read from the physical A32 console"
    )
    assert manifest["identity_assertion_scope"] == "known_fields_only"
    assert manifest["generated_partition_is_physical_dump"] is False
    assert (
        manifest["known_fields"]["media_access_control_address"] == "00:1d:c1:10:73:32"
    )
    assert (
        manifest["known_fields"]["media_access_control_address_is_locally_administered"]
        is False
    )
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

    board_information_input = _load_brooklyn2_board_information_descriptor(
        descriptor_path
    )
    partition = _build_brooklyn2_board_information_partition(
        board_information_input.descriptor
    )
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
def test_board_information_descriptor_rejects_invalid_fields(
    tmp_path, overrides, error_pattern
):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(
        json.dumps(build_board_information_descriptor(**overrides))
    )

    with pytest.raises(ValueError, match=error_pattern):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


def test_board_information_descriptor_rejects_schema_mismatch(tmp_path):
    descriptor = build_board_information_descriptor(unexpected=True)
    del descriptor["serial_number"]
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(json.dumps(descriptor))

    with pytest.raises(
        ValueError, match="missing fields: serial_number; unexpected fields: unexpected"
    ):
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
def test_board_information_descriptor_rejects_invalid_json(
    tmp_path, contents, error_pattern
):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_text(contents)

    with pytest.raises(ValueError, match=error_pattern):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


def test_board_information_descriptor_rejects_non_utf8_input(tmp_path):
    descriptor_path = tmp_path / "board-information.json"
    descriptor_path.write_bytes(b"\xff")

    with pytest.raises(ValueError, match="not UTF-8"):
        _load_brooklyn2_board_information_descriptor(descriptor_path)


def test_output_publisher_refuses_concurrently_created_destination(
    tmp_path, monkeypatch
):
    temporary_directory = tmp_path / "temporary"
    temporary_directory.mkdir()
    (temporary_directory / "flash.bin").write_text("complete flash")
    (temporary_directory / "manifest.json").write_text("complete manifest")
    output_directory = tmp_path / "image"
    observed_directory_contents = []
    native_publication = firmware_commands._atomic_publish_directory_without_replacement

    def create_concurrent_destination(staging_directory, destination_directory):
        destination_directory.mkdir()
        (destination_directory / "concurrent-owner").write_text("preserve")
        observed_directory_contents.append(
            sorted(path.name for path in destination_directory.iterdir())
        )
        native_publication(staging_directory, destination_directory)

    monkeypatch.setattr(
        firmware_commands,
        "_atomic_publish_directory_without_replacement",
        create_concurrent_destination,
    )

    with pytest.raises(ValueError, match="already exists"):
        _publish_output_directory_without_replacement(
            temporary_directory,
            output_directory,
        )

    assert observed_directory_contents == [["concurrent-owner"]]
    assert sorted(path.name for path in output_directory.iterdir()) == [
        "concurrent-owner"
    ]
    assert (output_directory / "concurrent-owner").read_text() == "preserve"
    assert sorted(path.name for path in temporary_directory.iterdir()) == [
        "flash.bin",
        "manifest.json",
    ]


def test_build_brooklyn2_image_refuses_dangling_output_symlink(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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


def test_build_brooklyn2_image_refuses_existing_output_directory(tmp_path):
    source = tmp_path / "source.dnt"
    payloads = write_brooklyn2_test_dnt(source)
    hardware_profile_path, board_information_path, evidence_root = (
        write_brooklyn2_test_build_inputs(
            tmp_path,
            source,
            payloads,
        )
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
