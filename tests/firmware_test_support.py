import hashlib
import json
import struct
import zlib

from netaudio.commands.firmware.constants import (
    BROOKLYN2_BOARD_INFORMATION_PARTITION_OFFSET,
    BROOKLYN2_BOARD_INFORMATION_PARTITION_SIZE,
    BROOKLYN2_FLASH_SIZE,
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
    struct.pack_into(">IIII", data, 0x40, 1, 0x01020304, section_body_offset, len(section_body))
    struct.pack_into(">IIII", data, 0x50, 0, 0, section_body_offset, 15)
    struct.pack_into(">IIII", data, 0x60, 0, 1, section_body_offset, 48)
    data[header_length + section_body_offset :] = section_body
    finish_dnt_checksums(data, header_length)
    return data


def build_uimage(data, image_type, load_address):
    header = bytearray(64)
    struct.pack_into(">I", header, 0, 0x27051956)
    struct.pack_into(">IIII", header, 12, len(data), load_address, load_address, zlib.crc32(data))
    header[28:32] = bytes((5, 14, image_type, 0))
    header[32:36] = b"Test"
    checksum_header = bytearray(header)
    checksum_header[4:8] = bytes(4)
    struct.pack_into(">I", header, 4, zlib.crc32(checksum_header))
    return bytes(header) + data


def encode_fixed_utf8(value, size):
    encoded = value.encode("utf-8")
    if len(encoded) > size:
        raise ValueError(f"Value requires {len(encoded)} bytes but field contains {size}")
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
    receive_channel_names_offset = transmit_channel_names_offset + len(transmit_channel_names) * 32
    oem_descriptor_offset = receive_channel_names_offset + len(receive_channel_names) * 32
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
    struct.pack_into(">H", payload, device_descriptor_offset + 0x1C, oem_descriptor_offset)
    for channel_index, channel_name in enumerate(transmit_channel_names):
        channel_offset = transmit_channel_names_offset + channel_index * 32
        payload[channel_offset : channel_offset + 32] = encode_fixed_utf8(channel_name, 32)
    for channel_index, channel_name in enumerate(receive_channel_names):
        channel_offset = receive_channel_names_offset + channel_index * 32
        payload[channel_offset : channel_offset + 32] = encode_fixed_utf8(channel_name, 32)
    payload[oem_descriptor_offset + 4 : oem_descriptor_offset + 12] = encode_fixed_utf8(manufacturer_short, 8)
    payload[oem_descriptor_offset + 0x0C : oem_descriptor_offset + 0x14] = model_id
    payload[oem_descriptor_offset + 0x14 : oem_descriptor_offset + 0x94] = encode_fixed_utf8(manufacturer, 128)
    payload[oem_descriptor_offset + 0x94 : oem_descriptor_offset + 0x114] = encode_fixed_utf8(product_name, 128)
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
    return bytes([0xFF]) * 16 + b"\xaa\x99\x55\x66" + struct.pack(f">{len(words)}H", *words)


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
        data[header_length + body_offset : header_length + body_offset + len(payload)] = payload
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
    partition[0x0A:0x10] = bytes.fromhex(descriptor["media_access_control_address"].replace(":", ""))
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
    board_information_path.write_text(json.dumps(board_information_descriptor, indent=2) + "\n")
    board_information_payload = build_expected_board_information_partition(board_information_descriptor)
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
    source_flash[0x1A0000 : 0x1A0000 + len(payloads["revision_three_fpga"])] = payloads["revision_three_fpga"]
    source_flash_path = source_flash_directory / "flash.bin"
    source_flash_path.write_bytes(source_flash)
    result_flash = bytearray(source_flash)
    result_flash[0x1A0000:0x2A0000] = bytes([0xFF]) * 0x100000
    result_flash[0x1A0000 : 0x1A0000 + len(payloads["standard_fpga"])] = payloads["standard_fpga"]
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
        "observed_fpga_payload_sha256": hashlib.sha256(payloads["standard_fpga"]).hexdigest(),
    }
    hardware_profile_descriptor = build_hardware_profile_descriptor(
        hashlib.sha256(firmware_data).hexdigest(),
        hashlib.sha256(board_information_payload).hexdigest(),
        selection_evidence,
    )
    hardware_profile_path = tmp_path / "hardware-profile.json"
    hardware_profile_path.write_text(json.dumps(hardware_profile_descriptor, indent=2) + "\n")
    return hardware_profile_path, board_information_path, evidence_root


def write_test_run_manifest_and_refresh_profile(run_manifest_path, run_manifest, hardware_profile_path):
    run_manifest_path.write_text(json.dumps(run_manifest, indent=2) + "\n")
    hardware_profile = json.loads(hardware_profile_path.read_text())
    hardware_profile["selection_evidence"]["run_manifest_sha256"] = hashlib.sha256(
        run_manifest_path.read_bytes()
    ).hexdigest()
    hardware_profile_path.write_text(json.dumps(hardware_profile, indent=2) + "\n")
