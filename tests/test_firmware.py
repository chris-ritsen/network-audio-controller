import json
import sqlite3
import struct
import zlib

import pytest
from netaudio.commands.firmware.archive import firmware_extract
from netaudio.commands.firmware.database import _init_db
from netaudio.commands.firmware.capabilities import _extract_capability_9
from netaudio.commands.firmware.constants import DNT_PARSER_VERSION, FIRMWARE_DATABASE_SCHEMA_VERSION
from netaudio.commands.firmware.parser import _load_resume_results, _parse_sections, parse_dnt

from tests.firmware_test_support import build_capability_payload, build_dnt, build_partitioned_dnt


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


def test_parse_sections_labels_each_partition_in_a_multi_section_table():
    data = build_partitioned_dnt([(identifier, 0x01020304, b"test") for identifier in [1, 2, 6, 9, 11, 12]])
    header_length = int.from_bytes(data[4:8], "big")

    sections = _parse_sections(data, header_length)

    assert [(section["partition_id"], section["partition_name"]) for section in sections] == [
        (1, "image"),
        (2, "fpga"),
        (6, "boot"),
        (9, "cap1"),
        (11, "user"),
        (12, "fpgar3"),
    ]


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
    transmit_channel_names = tuple(f"Input {channel_index:03d}" for channel_index in range(129))
    capability_payload = build_capability_payload(transmit_channel_names=transmit_channel_names)

    result = _extract_capability_9(capability_payload, b"Test\x00\x00\x00\x00")

    assert result["tx_channel_count"] == 129
    assert result["tx_channel_names"] == list(transmit_channel_names)


def test_capability_parser_rejects_channel_array_outside_payload():
    capability_payload = bytearray(build_capability_payload())
    device_descriptor_offset = struct.unpack_from(">H", capability_payload, 14)[0]
    struct.pack_into(">H", capability_payload, device_descriptor_offset + 8, 0xFFFF)
    capability_payload[16:20] = bytes(4)
    struct.pack_into(">I", capability_payload, 16, zlib.crc32(capability_payload))

    with pytest.raises(ValueError, match="transmit channel name array extends past capability payload"):
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
    expected_results = [{"dnt_parser_version": DNT_PARSER_VERSION, "file": "source.dnt"}]
    resume_path = tmp_path / "resume.json"
    resume_path.write_text(json.dumps(expected_results))

    assert _load_resume_results(resume_path) == expected_results


def test_firmware_database_has_explicit_offset_columns(tmp_path):
    connection = _init_db(tmp_path / "firmware.db")

    assert connection.execute("PRAGMA user_version").fetchone()[0] == FIRMWARE_DATABASE_SCHEMA_VERSION
    section_columns = {row[1] for row in connection.execute("PRAGMA table_info(sections)").fetchall()}
    assert "body_offset" in section_columns
    assert "file_offset" in section_columns
    assert "offset" not in section_columns
    connection.close()


def test_firmware_database_rejects_unversioned_schema(tmp_path):
    database_path = tmp_path / "firmware.db"
    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE firmware (sha256 TEXT PRIMARY KEY)")
    connection.execute("CREATE TABLE sections (sha256 TEXT, idx INTEGER, offset INTEGER)")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="schema version 0 is incompatible"):
        _init_db(database_path)
