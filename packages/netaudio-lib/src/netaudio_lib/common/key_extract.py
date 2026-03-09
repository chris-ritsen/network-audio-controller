import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger("netaudio")

DANTE_CONTROLLER_PATHS = {
    "darwin": [
        Path("/Applications/Dante Controller.app/Contents/MacOS/libDanteController.dylib"),
        Path.home() / "Applications" / "Dante Controller.app" / "Contents" / "MacOS" / "libDanteController.dylib",
    ],
    "win32": [
        Path(r"C:\Program Files (x86)\Audinate\Dante Controller\DanteController.exe"),
        Path(r"C:\Program Files\Audinate\Dante Controller\DanteController.exe"),
        Path(r"C:\Program Files (x86)\Audinate\Dante Controller\DanteController.dll"),
        Path(r"C:\Program Files\Audinate\Dante Controller\DanteController.dll"),
    ],
}

HEX_KEY_PATTERN = re.compile(rb"[0-9a-f]{64}")
EXPECTED_KEY_LENGTH = 32
MIN_UNIQUE_DIFFS = 8


def _is_table_pattern(key_bytes: bytes) -> bool:
    diffs = set()
    for i in range(1, len(key_bytes)):
        diffs.add(key_bytes[i] - key_bytes[i - 1])
    return len(diffs) < MIN_UNIQUE_DIFFS


def find_dante_controller_binary() -> Path | None:
    candidates = DANTE_CONTROLLER_PATHS.get(sys.platform, [])
    for path in candidates:
        if path.exists():
            return path
    return None


def extract_key_from_binary(binary_path: Path) -> bytes | None:
    try:
        data = binary_path.read_bytes()
    except (OSError, PermissionError) as exception:
        logger.debug(f"Cannot read {binary_path}: {exception}")
        return None

    matches = set()
    for match in HEX_KEY_PATTERN.finditer(data):
        hex_string = match.group(0).decode("ascii")
        try:
            key_bytes = bytes.fromhex(hex_string)
        except ValueError:
            continue
        if len(key_bytes) != EXPECTED_KEY_LENGTH:
            continue
        if _is_table_pattern(key_bytes):
            continue
        matches.add(hex_string[:EXPECTED_KEY_LENGTH])

    if len(matches) == 1:
        return matches.pop().encode("ascii")

    if len(matches) > 1:
        logger.debug(f"Found {len(matches)} candidate keys in {binary_path}, cannot disambiguate")

    return None


def extract_lock_key() -> bytes | None:
    binary_path = find_dante_controller_binary()
    if binary_path is None:
        return None
    return extract_key_from_binary(binary_path)
