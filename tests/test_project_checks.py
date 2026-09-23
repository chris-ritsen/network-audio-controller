import json
import sys

import pytest

from scripts.check_project import Check, run_checks


def command(name, source, scope="python"):
    return Check(name, scope, [sys.executable, "-c", source])


def test_report_records_passed_and_unselected_checks_without_running_them(tmp_path):
    report = tmp_path / "report.json"
    checks = [command("selected", "print('checked')"), command("unselected", "raise SystemExit(7)", "native")]

    assert run_checks(checks, {"python"}, report, timeout=5) == 0
    result = json.loads(report.read_text())

    assert result["status"] == "passed"
    assert result["revision"]
    assert len(result["source_sha256"]) == 64
    assert [check["status"] for check in result["checks"]] == ["passed", "not_selected"]
    assert (tmp_path / "selected.log").read_text().strip() == "checked"


@pytest.mark.parametrize(
    "failure", [[sys.executable, "-c", "raise SystemExit(7)"], ["netaudio-nonexistent-check-command"]]
)
def test_failed_check_stops_the_run_and_reports_unexecuted_checks(tmp_path, failure):
    report = tmp_path / "report.json"
    checks = [Check("failure", "python", failure), command("later", "raise SystemExit(99)")]

    assert run_checks(checks, {"python"}, report, timeout=5) != 0
    result = json.loads(report.read_text())

    assert result["status"] == "failed"
    assert [check["status"] for check in result["checks"]] == ["failed", "not_run"]
    assert not (tmp_path / "later.log").exists()


def test_timeout_is_incomplete_not_success_and_stops_following_checks(tmp_path):
    report = tmp_path / "report.json"
    checks = [command("slow", "import time; time.sleep(30)"), command("later", "print('should not run')")]

    assert run_checks(checks, {"python"}, report, timeout=1) == 124
    result = json.loads(report.read_text())

    assert result["status"] == "incomplete"
    assert [check["status"] for check in result["checks"]] == ["timed_out", "not_run"]


def test_report_identifies_the_explicit_native_artifact(tmp_path):
    library = tmp_path / "test-library"
    library.write_bytes(b"test artifact")
    report = tmp_path / "report.json"
    check = command("environment", "import os; print(os.environ['NETAUDIO_CORE_LIB'])")

    assert run_checks([check], {"python"}, report, timeout=5, library=library) == 0
    result = json.loads(report.read_text())

    assert result["native_library"]["path"] == str(library.resolve())
    assert len(result["native_library"]["sha256"]) == 64
    assert (tmp_path / "environment.log").read_text().strip() == str(library.resolve())


def test_native_artifact_changing_during_python_checks_invalidates_the_result(tmp_path):
    library = tmp_path / "test-library"
    library.write_bytes(b"original")
    report = tmp_path / "report.json"
    check = command(
        "changed-artifact",
        "import os; from pathlib import Path; Path(os.environ['NETAUDIO_CORE_LIB']).write_bytes(b'changed')",
    )

    assert run_checks([check], {"python"}, report, timeout=5, library=library) == 124
    result = json.loads(report.read_text())

    assert result["status"] == "incomplete"
    assert result["checks"][0]["status"] == "artifact_changed"
