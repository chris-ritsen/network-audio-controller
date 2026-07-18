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
