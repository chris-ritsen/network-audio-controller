import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "generate_core_binding.py"


@pytest.mark.parametrize(
    "declaration", ["long double netaudio_unknown(void);", "void netaudio_callback(void (*callback)(void));"]
)
def test_binding_generation_refuses_unrecognized_c_types(tmp_path, declaration):
    header = tmp_path / "unsupported.h"
    header.write_text(declaration)
    output = tmp_path / "abi.py"
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--header", str(header), "--output", str(output)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "unsupported" in result.stderr.lower()
    assert not output.exists()


def test_binding_generation_check_does_not_overwrite_stale_output(tmp_path):
    output = tmp_path / "abi.py"
    output.write_text("stale\n")
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--check", "--output", str(output)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "stale" in result.stderr.lower()
    assert output.read_text() == "stale\n"
