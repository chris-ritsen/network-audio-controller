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


def regenerate_command_goldens():
    golden_cases = json.loads(COMMAND_GOLDEN_PATH.read_text())
    for golden_case in golden_cases.values():
        golden_case["hex"] = core.build_command(golden_case["spec"]).hex()
    COMMAND_GOLDEN_PATH.write_text(json.dumps(golden_cases, indent=2) + "\n")


def regenerate_response_goldens():
    golden_cases = json.loads(RESPONSE_GOLDEN_PATH.read_text())
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
