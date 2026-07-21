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
}

CAPTURED_RESPONSE_CASES = {
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
            "ffff003479b20000001dc1fffe50cac5417564696e6174650738008200000000"
            "0018000100000018000000180001000000000018"
        ),
        "kind": "encoding_status",
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
