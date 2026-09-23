from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
CRATE = "packages/netaudio-core/Cargo.toml"
TARGET = ROOT / "packages/netaudio-core/target"


@dataclass(frozen=True)
class Check:
    name: str
    scope: str
    command: list[str]


def digest(path):
    result = hashlib.sha256()

    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)

    return result.hexdigest()


def source_identity():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, timeout=10)

    revision = git("rev-parse", "HEAD").decode().strip()
    paths = sorted(set(git("ls-files", "-z", "--cached", "--others", "--exclude-standard").split(b"\0")) - {b""})
    fingerprint = hashlib.sha256()

    for name in paths:
        path = ROOT / os.fsdecode(name)
        fingerprint.update(name + b"\0")
        fingerprint.update(digest(path).encode() if path.is_file() else b"missing")

    return {"revision": revision, "source_sha256": fingerprint.hexdigest(), "dirty": bool(git("status", "--porcelain"))}


def stop(process):
    if process.poll() is not None:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(process.pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    process.wait(timeout=10)


def run_checks(checks, scopes, report_path, *, timeout=300, library=None):
    if not 0 < timeout <= 300:
        raise ValueError("validation timeout must be between zero and 300 seconds")

    started = time.monotonic()
    report_path = Path(report_path).resolve()

    if ROOT == report_path.parent or ROOT in report_path.parents:
        raise ValueError("write validation reports outside the source tree")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join([str(ROOT / "packages/netaudio/src"), str(ROOT)])
    environment["CARGO_TARGET_DIR"] = str(TARGET)

    if library is not None:
        library = Path(library).resolve()
        environment["NETAUDIO_CORE_LIB"] = str(library)

    scopes = set(scopes)

    if scopes & {"python", "native"}:
        scopes.add("core")

    if "native" in scopes:
        scopes.add("rust")

    report = {
        "status": "incomplete",
        "host": {"system": platform.system(), "machine": platform.machine(), "python": platform.python_version()},
        "checks": [],
        "separate_checks": ["installed wheel", "browser UI", "other operating systems and Python versions"],
    }
    exit_code = 0

    try:
        report.update(source_identity())

        for check in checks:
            entry = {"name": check.name, "command": check.command, "status": "not_run"}
            report["checks"].append(entry)

            if check.scope not in scopes:
                entry["status"] = "not_selected"
                continue

            if exit_code:
                continue

            remaining = timeout - (time.monotonic() - started)

            if remaining <= 0:
                entry["status"] = "timed_out"
                exit_code = 124
                continue

            process = None
            check_started = time.monotonic()
            log_path = report_path.parent / f"{check.name}.log"
            entry["log"] = str(log_path)
            print(f"Checking {check.name}", flush=True)

            try:
                if library is not None and check.scope == "python":
                    entry["native_library_sha256"] = digest(library)

                with log_path.open("w", encoding="utf-8") as log:
                    process = subprocess.Popen(
                        check.command,
                        cwd=ROOT,
                        env=environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=os.name != "nt",
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                    )
                    code = process.wait(timeout=remaining)

                entry["exit_code"] = code
                entry["status"] = "passed" if code == 0 else "failed"
                exit_code = 0 if code == 0 else 1

                if "native_library_sha256" in entry and (
                    not library.is_file() or digest(library) != entry["native_library_sha256"]
                ):
                    entry["status"] = "artifact_changed"
                    exit_code = 124
            except subprocess.TimeoutExpired:
                entry["status"] = "timed_out"
                exit_code = 124
            except KeyboardInterrupt:
                entry["status"] = "interrupted"
                exit_code = 130
            except OSError as error:
                entry.update(status="failed", error=str(error))
                exit_code = 1
            finally:
                if process is not None:
                    stop(process)

                entry["elapsed_seconds"] = round(time.monotonic() - check_started, 3)

        if source_identity()["source_sha256"] != report["source_sha256"]:
            report["error"] = "Source changed during validation"
            exit_code = 124

        report["status"] = "passed" if exit_code == 0 else "incomplete" if exit_code in (124, 130) else "failed"
    except (OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
        exit_code = 124
    finally:
        if library is not None:
            report["native_library"] = {"path": str(library), "sha256": digest(library) if library.is_file() else None}

        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"{report['status']}: {report_path}", flush=True)

    return exit_code


def project_checks(library, *, offline=False):
    python = [sys.executable]
    cargo = ["--locked", *(["--offline"] if offline else []), "--manifest-path", CRATE]
    return [
        Check("native-build", "core", ["cargo", "build", *cargo, "--target-dir", str(TARGET)]),
        Check("abi", "core", [*python, "scripts/check_native_abi.py", str(library)]),
        Check("lockfile", "native", ["uv", "lock", "--check", *(["--offline"] if offline else [])]),
        Check("ruff", "native", [*python, "-m", "ruff", "check", "."]),
        Check("python-format", "native", [*python, "-m", "ruff", "format", "--check", "."]),
        Check("python-syntax", "native", [*python, "-m", "compileall", "-q", "packages/netaudio/src/netaudio"]),
        Check("types", "native", [*python, "-m", "pyright"]),
        Check(
            "core-types",
            "native",
            [*python, "scripts/generate_core_types.py", "--check", *(["--offline"] if offline else [])],
        ),
        Check(
            "header",
            "native",
            [
                "cbindgen",
                "--verify",
                "--config",
                "packages/netaudio-core/cbindgen.toml",
                "--crate",
                "netaudio-core",
                "--output",
                "packages/netaudio-core/include/netaudio_core.h",
                "packages/netaudio-core",
            ],
        ),
        Check("rust-format", "native", ["cargo", "fmt", "--manifest-path", CRATE, "--", "--check"]),
        Check(
            "clippy",
            "native",
            ["cargo", "clippy", *cargo, "--all-targets", "--features", "schema", "--", "-D", "warnings"],
        ),
        Check("rust-tests", "rust", ["cargo", "test", *cargo, "--features", "schema"]),
        Check("python-tests", "python", [*python, "-m", "pytest", "-q"]),
        Check(
            "web-tests",
            "webapp",
            [
                "node",
                "--import",
                "./tests/webapp/loader.mjs",
                "--import",
                "./tests/webapp/setup.mjs",
                "--test",
                *[str(path.relative_to(ROOT)) for path in sorted((ROOT / "tests/webapp").glob("*.test.mjs"))],
            ],
        ),
    ]


def main():
    parser = argparse.ArgumentParser(description="Run shared local/CI checks on the current host.")
    parser.add_argument("--scope", choices=["all", "native", "python", "runtime", "webapp"], default="all")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    library_name = (
        "netaudio_core.dll"
        if sys.platform == "win32"
        else "libnetaudio_core.dylib"
        if sys.platform == "darwin"
        else "libnetaudio_core.so"
    )
    library = TARGET / "debug" / library_name
    report = args.report or Path(tempfile.mkdtemp(prefix="netaudio-checks-")) / "report.json"
    scopes = (
        {"native", "python", "webapp"}
        if args.scope == "all"
        else {"python", "rust"}
        if args.scope == "runtime"
        else {args.scope}
    )
    return run_checks(
        project_checks(library, offline=args.offline),
        scopes,
        report,
        timeout=args.timeout,
        library=library if args.scope != "webapp" else None,
    )


if __name__ == "__main__":
    sys.exit(main())
