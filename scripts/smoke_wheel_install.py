from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _venv_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "python.exe"
    return environment / "bin" / "python"


def _console_script(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "netaudio.exe"
    return environment / "bin" / "netaudio"


def _run(command: list[str], *, environment: dict[str, str], cwd: Path) -> None:
    subprocess.run(command, env=environment, cwd=cwd, check=True, timeout=180)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()

    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required for the isolated install smoke test")
    wheel = args.wheel.resolve()
    smoke_script = Path(__file__).with_name("smoke_wheel.py").resolve()
    clean_environment = os.environ.copy()
    clean_environment.pop("NETAUDIO_CORE_LIB", None)
    clean_environment.pop("PYTHONPATH", None)

    with tempfile.TemporaryDirectory(prefix="netaudio-wheel-") as temporary:
        root = Path(temporary)
        environment = root / "venv"
        _run(
            [uv, "venv", "--python", sys.executable, str(environment)],
            environment=clean_environment,
            cwd=root,
        )
        python = _venv_python(environment)
        _run(
            [uv, "pip", "install", "--python", str(python), str(wheel)],
            environment=clean_environment,
            cwd=root,
        )
        console_script = _console_script(environment)
        if not console_script.is_file():
            raise RuntimeError(f"installed console script is missing: {console_script}")
        installed_environment = clean_environment.copy()
        installed_environment["PATH"] = os.pathsep.join(
            part for part in (str(console_script.parent), clean_environment.get("PATH")) if part
        )
        _run([str(python), "-I", str(smoke_script)], environment=installed_environment, cwd=root)

        _run(
            [uv, "pip", "uninstall", "--python", str(python), "netaudio"],
            environment=clean_environment,
            cwd=root,
        )
        if console_script.exists():
            raise RuntimeError(f"uninstall left the console script behind: {console_script}")
        probe = subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import importlib.util; raise SystemExit(importlib.util.find_spec('netaudio') is not None)",
            ],
            env=clean_environment,
            cwd=root,
            timeout=30,
            check=False,
        )
        if probe.returncode != 0:
            raise RuntimeError("uninstall left an importable netaudio package behind")

    print(f"clean install and uninstall passed for {wheel.name} on Python {sys.version.split()[0]}")


if __name__ == "__main__":
    main()
