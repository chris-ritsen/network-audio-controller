from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlencode, urlunsplit

import segno

LOCK_KEY_PATTERN = r"[0-9a-fA-F]{32}"
LOCK_KEY_URI_SCHEME = "netaudio"
LOCK_KEY_URI_HOST = "lock-key"
LOCK_KEY_URI_PATH = "/import"
LOCK_KEY_URI_VERSION = "1"
TERMINAL_QR_BORDER = 1
ANSI_RESET = "\x1b[0m"
ANSI_BLACK_FOREGROUND = "\x1b[38;2;0;0;0m"
ANSI_WHITE_FOREGROUND = "\x1b[38;2;255;255;255m"
ANSI_BLACK_BACKGROUND = "\x1b[48;2;0;0;0m"
ANSI_WHITE_BACKGROUND = "\x1b[48;2;255;255;255m"
ANSI_DEFAULT_BACKGROUND = "\x1b[49m"


def normalize_lock_key(lock_key: str) -> str:
    if re.fullmatch(LOCK_KEY_PATTERN, lock_key) is None:
        raise ValueError("key must be a 32-character hex string")
    return lock_key.lower()


def lock_key_import_uri(lock_key: str) -> str:
    normalized_lock_key = normalize_lock_key(lock_key)
    query = urlencode((("version", LOCK_KEY_URI_VERSION), ("key", normalized_lock_key)))
    return urlunsplit((LOCK_KEY_URI_SCHEME, LOCK_KEY_URI_HOST, LOCK_KEY_URI_PATH, query, ""))


def render_lock_key_qr(lock_key: str) -> str:
    qr_code = segno.make_qr(lock_key_import_uri(lock_key), error="m")
    rows = [tuple(bool(module) for module in row) for row in qr_code.matrix_iter(border=TERMINAL_QR_BORDER)]

    output = []
    if len(rows) % 2:
        for lower_dark in rows[0]:
            output.append(ANSI_BLACK_FOREGROUND if lower_dark else ANSI_WHITE_FOREGROUND)
            output.append(ANSI_DEFAULT_BACKGROUND)
            output.append("▄")
        output.append(f"{ANSI_RESET}\n")
        rows = rows[1:]

    for upper_row, lower_row in zip(rows[::2], rows[1::2]):
        for upper_dark, lower_dark in zip(upper_row, lower_row):
            output.append(ANSI_BLACK_FOREGROUND if upper_dark else ANSI_WHITE_FOREGROUND)
            output.append(ANSI_BLACK_BACKGROUND if lower_dark else ANSI_WHITE_BACKGROUND)
            output.append("▀")
        output.append(f"{ANSI_RESET}\n")
    return "".join(output)


def write_lock_key_qr(lock_key: str, output_path: Path | None = None) -> Path:
    if output_path is None:
        file_descriptor, temporary_path = tempfile.mkstemp(prefix="netaudio-lock-key-", suffix=".png")
        os.close(file_descriptor)
        destination = Path(temporary_path)
    else:
        destination = output_path.expanduser().resolve()
        if destination.suffix.lower() not in {".png", ".svg"}:
            raise ValueError("QR output path must end in .png or .svg")
        destination.parent.mkdir(parents=True, exist_ok=True)

    qr_code = segno.make_qr(lock_key_import_uri(lock_key), error="m")
    qr_code.save(str(destination), scale=12, border=4, dark="black", light="white")
    destination.chmod(0o600)
    return destination


def open_path(path: Path) -> None:
    if sys.platform == "darwin":
        command = ["open", str(path)]
    elif sys.platform == "win32":
        command = ["explorer", str(path)]
    else:
        command = ["xdg-open", str(path)]
    subprocess.run(command, check=True)
