import json
import sys
from pathlib import Path

from netaudio import core


REPOSITORY_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.core_golden import response_input_bytes


FIXTURES_DIRECTORY = REPOSITORY_ROOT / "tests" / "fixtures"
COMMAND_GOLDEN_PATH = FIXTURES_DIRECTORY / "core_commands_golden.json"
RESPONSE_GOLDEN_PATH = FIXTURES_DIRECTORY / "core_responses_golden.json"

CAPTURED_COMMAND_CASES = {
    "metering_start_ad4d_packet_7298186": {
        "spec": {
            "command": "metering_start",
            "device_name": "ad4d",
            "ipv4": "192.168.1.156",
            "mac": "3e42274cff24",
            "port": 8752,
        },
    },
    "metering_start_a32_packet_7298185": {
        "spec": {
            "command": "metering_start",
            "device_name": "a32",
            "ipv4": "192.168.1.156",
            "mac": "3e42274cff24",
            "port": 8752,
        },
    },
    "property_directory_packet_548323": {
        "spec": {
            "command": "property_directory",
            "transaction_id": 0x1234,
        },
    },
    "probe_sample_rate_packet_4170622": {
        "spec": {
            "command": "probe_sample_rate",
            "host_mac": "3e42274cff24",
            "sequence": 0x0042,
        },
    },
    "probe_encoding_packet_204680": {
        "spec": {
            "command": "probe_encoding",
            "host_mac": "3e42274cff24",
            "sequence": 0x985A,
        },
    },
    "probe_gain_level_packet_716": {
        "spec": {
            "command": "probe_gain_level",
            "host_mac": "842f5774e86d",
            "sequence": 0x045A,
        },
    },
    "set_gain_level:27": {
        "spec": {
            "command": "set_gain_level",
            "channel_number": 1,
            "gain_level": 4,
            "device_type": "input",
            "host_mac": "842f5774e86d",
            "sequence": 0xC001,
        },
    },
    "set_gain_level:28": {
        "spec": {
            "command": "set_gain_level",
            "channel_number": 2,
            "gain_level": 1,
            "device_type": "output",
            "host_mac": "842f5774e86d",
            "sequence": 0xD101,
        },
    },
}

CAPTURED_RESPONSE_CASES = {
    "property_directory_lx_dante_packet_12358036": {
        "hex": (
            "2729007000411102000100198020000180210003002200030024000180600003"
            "00f00003020100038204000382050003020a0001020b00010210000302110003"
            "0212000302130001021400018301000383060001830200010310000303110001"
            "031200010303000383f0000106010001"
        ),
        "kind": "property_directory",
    },
    "property_directory_avio_packet_12358133": {
        "hex": (
            "28090088006c11020001001f8020000180210003002200030023000300240001"
            "806000030062000300630001020100038204000382050003020a0001020b0001"
            "0210000302110003021200030213000102140001022200038301000383060001"
            "83020001832100010310000103110001031200010303000383f0000106010001"
            "0309000102090001"
        ),
        "kind": "property_directory",
    },
    "20250517_200646_289003_lx-dante_get_receivers_response.bin": {
        "kind": "channel_audio_metadata",
    },
    "sample_rate_status_lx-dante": {
        "hex": (
            "ffff004816310000001dc10812580000417564696e6174650724008000000000"
            "001800060000ac4400000000000200000000ac440000bb800001588800017700"
            "0002b1100002ee00"
        ),
        "kind": "sample_rate_status",
    },
    "sample_rate_status_packet_4170820": {
        "hex": (
            "ffff0034061a0000001dc10812580000417564696e6174650724008000000000001800010000bb8000000000000200000000bb80"
        ),
        "kind": "sample_rate_status",
    },
    "sample_rate_status_packet_9695783": {
        "hex": (
            "ffff0040fd2a0000001dc1fffe53ef37417564696e6174650738008000000000"
            "001800040000bb800000bb80000200000000ac440000bb800001588800017700"
        ),
        "kind": "sample_rate_status",
    },
    "encoding_status_packet_204720": {
        "hex": (
            "ffff003c21020000001dc11073320000417564696e6174650724008200000000"
            "00180003000000180000000000020000000000180000001000000020"
        ),
        "kind": "encoding_status",
    },
    "encoding_status_packet_645566": {
        "hex": (
            "ffff003479b20000001dc1fffe50cac5417564696e61746507380082000000000018000100000018000000180001000000000018"
        ),
        "kind": "encoding_status",
    },
    "gain_status_input_packet_1528": {
        "hex": (
            "ffff003806110000001dc1fffe50692e417564696e6174650727100b00000000"
            "000000010008001001020002000400180000000500000001"
        ),
        "kind": "gain_status",
    },
    "gain_status_output_packet_1585": {
        "hex": (
            "ffff003808100000001dc1fffe507b8d417564696e6174650727100b00000000"
            "000000010008001002010002000400180000000400000004"
        ),
        "kind": "gain_status",
    },
    "metering_frame_ad4d_packet_7298532": {
        "hex": (
            "ffff005d1fad0000000eddfd4e130000417564696e617465024001fe"
            "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe"
            "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe00"
        ),
        "kind": "metering",
    },
    "metering_frame_a32_packet_7298422": {
        "hex": (
            "ffff009c1f810000001dc119245c0000417564696e617465024040fe"
            "fefefefefefefefefefefefefefefe7d89a0a1a0a2a2a0fefefefefefefefefe"
            "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe"
            "fefefe7dfefefefefefefe7dfefefefefefefefefefefefefefefefefefefefe"
            "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe00"
        ),
        "kind": "metering",
    },
    "interface_status_lx_dante_packet_12362179": {
        "hex": (
            "ffff009412b70000001dc10812580000417564696e6174650724001100000000"
            "00020000000003e80001001dc1081258c0a8016cffffff00c0a80101c0a80101"
            "0000000a0000001dc1081259ac1fb93affff000000000000000000000018004c"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000"
        ),
        "kind": "interface_status",
    },
    "interface_status_avio_input_packet_12362180": {
        "hex": (
            "ffff0060d9f00000001dc1fffe50692e417564696e6174650727001100000000"
            "00010000000000640003001dc150692ec0a8012affffff000808080808080404"
            "0018003000000000000000000000000000000000000000000000000000000000"
        ),
        "kind": "interface_status",
    },
    "interface_status_avio_applied_dhcp_packet_12362281": {
        "hex": (
            "ffff006c05920000001dc1fffe50692e417564696e6174650727001100000000"
            "00010000000000640001001dc150692ec0a8018bffffff00c0a80101c0a80101"
            "0018003000000000000400000000000000000000000000000000000000480000"
            "6c6f63616c646f6d61696e00"
        ),
        "kind": "interface_status",
    },
    "aes67_configured_packet_1479697": {
        "hex": (
            "28090094003611000001171702010001820400688205006c0210001002110010"
            "0000821800008219830100708302007483060078031000100311001003030002"
            "8021007c000000f08060008c002200010063000100000064000000650222138c"
            "0212003083210090000f42400f424000000f42400135f1b4000f424000000000"
            "000000000000000000000000ef450000001e8480"
        ),
        "kind": "aes67_configured",
    },
    "aes67_unsupported_placeholder_packet_9088832": {
        "hex": (
            "2729008c006711000001171702010001820400688205006c0210001002110010"
            "0000821800008219830100708302007483060078031000100311000203030004"
            "8021007c00f00000000080600022000100000063000000640000006500000222"
            "0212003000008321001e8480000f4240000f4240028b0aab0003d09000000000"
            "000000000000000000000000"
        ),
        "kind": "aes67_configured",
    },
}


def regenerate_command_goldens():
    golden_cases = json.loads(COMMAND_GOLDEN_PATH.read_text())
    golden_cases.update(CAPTURED_COMMAND_CASES)
    for golden_case in golden_cases.values():
        golden_case["hex"] = core.build_command(golden_case["spec"]).hex()
    COMMAND_GOLDEN_PATH.write_text(json.dumps(golden_cases, indent=2) + "\n")


def regenerate_response_goldens():
    golden_cases = json.loads(RESPONSE_GOLDEN_PATH.read_text())
    golden_cases.update(CAPTURED_RESPONSE_CASES)
    for name, golden_case in golden_cases.items():
        golden_case["parsed"] = core.parse_response(
            golden_case["kind"],
            response_input_bytes(FIXTURES_DIRECTORY, name, golden_case),
        )
    RESPONSE_GOLDEN_PATH.write_text(json.dumps(golden_cases, indent=2) + "\n")


def main():
    regenerate_command_goldens()
    regenerate_response_goldens()


if __name__ == "__main__":
    main()
