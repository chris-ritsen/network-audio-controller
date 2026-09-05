from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger("netaudio")

LOG_SIZE_LIMIT_BYTES = 8 * 1024 * 1024


def daemon_log_path() -> Path:
    return Path(tempfile.gettempdir()) / "netaudio" / "daemon.log"


def ensure_log_directory() -> Path:
    path = daemon_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def truncate_when_oversized(path: Path, limit_bytes: int = LOG_SIZE_LIMIT_BYTES) -> bool:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return False
    if size <= limit_bytes:
        return False
    os.truncate(path, 0)
    logger.info(f"Truncated {path} after it exceeded {limit_bytes} bytes")
    return True
