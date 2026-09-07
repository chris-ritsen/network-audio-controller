from __future__ import annotations

import json
import os
import tempfile

from netaudio.common.config_loader import default_config_path


def read_preferences():
    path = default_config_path().parent / "preferences.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def save_monitoring_port(port: int):
    if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError("Monitoring port must be an integer between 1024 and 65535")
    path = default_config_path().parent / "preferences.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    preferences = read_preferences()
    preferences["monitoring_port"] = port
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".preferences-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(preferences, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
