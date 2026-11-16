from __future__ import annotations

import os
import re
import sys
from pathlib import Path

HEXADECIMAL_KEY_PATTERN = re.compile(rb"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")
KEY_LENGTH = 32
LIBRARY_NAMES = ("libDanteController.dylib", "DanteController.dll")
LIBRARY_PATHS = (
    Path("Contents") / "MacOS" / "libDanteController.dylib",
    Path("libDanteController.dylib"),
    Path("DanteController.dll"),
)
MINIMUM_DISTINCT_STEPS = 8
WINDOWS_INSTALLATION = Path("Audinate") / "Dante Controller"


def _is_table_pattern(key_bytes: bytes) -> bool:
    steps = {key_bytes[index] - key_bytes[index - 1] for index in range(1, len(key_bytes))}
    return len(steps) < MINIMUM_DISTINCT_STEPS


def _windows_program_directories(drive: Path | None = None) -> list[Path]:
    if drive is not None:
        return [drive / "Program Files", drive / "Program Files (x86)"]
    configured = [os.environ.get(name) for name in ("ProgramFiles", "ProgramW6432", "ProgramFiles(x86)")]
    return [Path(value) for value in configured if value] + [
        Path(r"C:\Program Files"),
        Path(r"C:\Program Files (x86)"),
    ]


def installation_directories() -> list[Path]:
    if sys.platform == "darwin":
        directories = [
            Path("/Applications/Dante Controller.app"),
            Path.home() / "Applications" / "Dante Controller.app",
        ]
    elif sys.platform == "win32":
        directories = [directory / WINDOWS_INSTALLATION for directory in _windows_program_directories()]
    else:
        prefix = Path(os.environ.get("WINEPREFIX") or Path.home() / ".wine")
        directories = [
            directory / WINDOWS_INSTALLATION for directory in _windows_program_directories(prefix / "drive_c")
        ]
    return list(dict.fromkeys(directories))


def libraries_in(location: Path) -> list[Path]:
    if location.is_dir():
        return [location / path for path in LIBRARY_PATHS if (location / path).is_file()]
    if not location.is_file():
        return []
    if location.name in LIBRARY_NAMES:
        return [location]
    beside = [location.parent / name for name in LIBRARY_NAMES if (location.parent / name).is_file()]
    return beside or [location]


def lock_key_candidates(library: Path) -> list[str]:
    data = library.read_bytes()
    terminated = set()
    unterminated = set()
    for match in HEXADECIMAL_KEY_PATTERN.finditer(data):
        text = match.group(0).decode("ascii")
        if _is_table_pattern(bytes.fromhex(text)):
            continue
        is_terminated = (
            data[match.start() - 1 : match.start()] == b"\0" and data[match.end() : match.end() + 1] == b"\0"
        )
        (terminated if is_terminated else unterminated).add(text[:KEY_LENGTH])
    return sorted(terminated or unterminated)
