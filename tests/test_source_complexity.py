import subprocess
import sys
import ast
from pathlib import Path

from scripts import check_complexity


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_python_cyclomatic_complexity_baseline_is_current():
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "check_complexity.py")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_complexity_baseline_transition_is_reduction_only():
    previous = {
        "old": check_complexity.ComplexityException(20, "a" * 64),
    }

    assert (
        check_complexity._transition_differences(
            previous,
            {"old": check_complexity.ComplexityException(19, "b" * 64)},
        )
        == []
    )
    assert check_complexity._transition_differences(
        previous,
        {"old": check_complexity.ComplexityException(20, "b" * 64)},
    ) == ["baseline fingerprint changed without a complexity reduction: old"]
    assert check_complexity._transition_differences(
        previous,
        {"old": check_complexity.ComplexityException(21, "a" * 64)},
    ) == ["baseline allowance was raised: old = 21 (previous 20)"]
    assert check_complexity._transition_differences(
        previous,
        {
            "old": previous["old"],
            "new": check_complexity.ComplexityException(16, "c" * 64),
        },
    ) == ["baseline allowance was added: new = 16"]


def test_complexity_history_cannot_launder_an_earlier_increase():
    original = {"legacy": check_complexity.ComplexityException(20, "a" * 64)}
    raised = {"legacy": check_complexity.ComplexityException(21, "b" * 64)}

    assert check_complexity._history_transition_differences(
        raised,
        [
            ("newer", raised),
            ("older", original),
        ],
    ) == ["newer relative to older: baseline allowance was raised: legacy = 21 (previous 20)"]


def test_changed_legacy_callable_must_reduce_complexity():
    baseline = {"legacy": check_complexity.ComplexityException(20, "a" * 64)}
    current = {"legacy": check_complexity.ComplexityException(20, "b" * 64)}

    assert check_complexity._source_differences(baseline, current) == [
        "legacy callable changed without reducing complexity: legacy"
    ]


def test_property_accessors_have_distinct_baseline_names():
    definitions = set(
        check_complexity._qualified_definitions(
            PROJECT_ROOT / "packages" / "netaudio" / "src" / "netaudio" / "common" / "app_config.py"
        ).values()
    )
    names = {definition.baseline_name for definition in definitions}

    assert "AppSettings.device_lock_key[getter]" in names
    assert "AppSettings.device_lock_key[setter]" in names


def test_enclosing_fingerprint_tracks_nested_class_state():
    before = ast.parse("def legacy():\n    class State:\n        value = 1\n").body[0]
    after = ast.parse("def legacy():\n    class State:\n        value = 2\n").body[0]

    assert check_complexity._definition_fingerprint(before) != check_complexity._definition_fingerprint(after)
