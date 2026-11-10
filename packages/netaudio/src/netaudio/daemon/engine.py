from __future__ import annotations

import atexit
import logging
import os
import subprocess
import sys
from pathlib import Path

STOP_TIMEOUT = 10.0

_process: subprocess.Popen | None = None
_port: int | None = None


class EngineFailure(RuntimeError):
    pass


def executable() -> Path:
    name = "netaudio-daemon.exe" if sys.platform == "win32" else "netaudio-daemon"
    return Path(__file__).resolve().parents[1] / "bin" / name


def log_level() -> str:
    level = logging.getLevelName(logging.getLogger().getEffectiveLevel()).lower()
    return {"warning": "warn", "critical": "error", "notset": "trace"}.get(level, level)


def port() -> int | None:
    return _port


def start() -> int:
    global _process, _port

    if _port is not None:
        return _port

    from netaudio.common.app_config import settings

    path = executable()
    if not path.is_file():
        raise EngineFailure("the native engine is missing; rebuild or reinstall NetAudio")

    arguments = [str(path), "--private"]
    if settings.interface:
        arguments.extend(["--interface", settings.interface])
    environment = dict(os.environ)
    environment.pop("NETAUDIO_DAEMON_PORT", None)
    environment.setdefault("RUST_LOG", log_level())

    process = subprocess.Popen(arguments, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    address = process.stdout.readline().decode().strip()
    if not address:
        process.stdin.close()
        raise EngineFailure(f"the native engine stopped while starting (exit status {process.wait()})")

    _process = process
    _port = int(address.rpartition(":")[2])
    atexit.register(stop)
    return _port


def stop() -> None:
    global _process, _port

    process, _process, _port = _process, None, None
    if process is None:
        return

    process.stdin.close()
    try:
        process.wait(timeout=STOP_TIMEOUT)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
