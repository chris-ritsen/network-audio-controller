from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any, TypeVar

from netaudio.common.config_loader import default_config_path

PreferenceResult = TypeVar("PreferenceResult")
LOCK_FILENAME = ".preferences.lock"
PREFERENCES_FILENAME = "preferences.json"
TEMPORARY_PREFIX = ".preferences-"
PRIVATE_FILE_MODE = 0o600
PRIVATE_DIRECTORY_MODE = 0o700
WINDOWS_LOCK_RANGE_WORD = (1 << 32) - 1


def _read_document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}

    if not isinstance(value, dict):
        raise ValueError("preferences must be a JSON object")

    return value


def read_preferences() -> dict[str, Any]:
    return _read_document(default_config_path().parent / PREFERENCES_FILENAME)


def _lock_preferences(descriptor: int) -> None:
    if os.name == "nt":
        import msvcrt

        import pywintypes
        import win32con
        import win32file

        win32file.LockFileEx(
            msvcrt.get_osfhandle(descriptor),
            win32con.LOCKFILE_EXCLUSIVE_LOCK,
            WINDOWS_LOCK_RANGE_WORD,
            WINDOWS_LOCK_RANGE_WORD,
            pywintypes.OVERLAPPED(),
        )
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX)


def update_preferences(
    update: Callable[[dict[str, Any]], PreferenceResult],
) -> PreferenceResult:
    path = default_config_path().parent / PREFERENCES_FILENAME
    path.parent.mkdir(mode=PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    lock_descriptor = os.open(path.parent / LOCK_FILENAME, os.O_RDWR | os.O_CREAT, PRIVATE_FILE_MODE)

    with os.fdopen(lock_descriptor, "r+b") as lock:
        _lock_preferences(lock.fileno())
        preferences = _read_document(path)
        previous = deepcopy(preferences)
        result = update(preferences)

        if preferences == previous:
            return result

        descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=TEMPORARY_PREFIX)

        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(preferences, stream, allow_nan=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())

            os.replace(temporary, path)

            if os.name != "nt":
                directory = os.open(path.parent, os.O_RDONLY)

                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

        return result


def save_monitoring_port(port: int):
    if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError("Monitoring port must be an integer between 1024 and 65535")
    save_preference("monitoring_port", port)


def save_preference(key: str, value):
    def update(preferences: dict[str, Any]) -> None:
        preferences[key] = value

    update_preferences(update)
