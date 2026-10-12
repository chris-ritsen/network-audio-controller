from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger("netaudio")

REMEMBER_SECONDS = 30 * 24 * 3600
TYPE_WORDS = {
    "p10t": (
        "in-ear monitor transmitter",
        ("in-ear", "in ear", "in-ears", "iem", "iems", "monitor", "monitors", "psm"),
    ),
    "ad4d": ("wireless mic receiver", ("wireless", "mic", "mics", "microphone", "receiver")),
}


def default_shure_memory_path() -> Path:
    return Path.home() / ".local" / "share" / "netaudio" / "shure_devices.json"


def save_shure_memory(manager, path: Path) -> None:
    oldest = time.time() - REMEMBER_SECONDS
    records = {**manager.remembered, **{mac: device.to_json() for mac, device in manager.devices.items()}}
    kept = {mac: record for mac, record in records.items() if (record.get("last_seen") or 0) >= oldest}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(kept, ensure_ascii=False, default=str), encoding="utf-8")
    os.replace(temporary, path)
    logger.info("Saved %d Shure devices to %s", len(kept), path)


def load_shure_memory(manager, path: Path) -> None:
    if not path.exists():
        return
    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exception:
        logger.warning("Could not restore Shure devices from %s: %s", path, exception)
        return
    if isinstance(records, dict):
        manager.remembered = {mac: record for mac, record in records.items() if isinstance(record, dict)}
        logger.info("Restored %d remembered Shure devices", len(manager.remembered))


def describe_type(device_type: str | None) -> str | None:
    entry = TYPE_WORDS.get(str(device_type or "").casefold())
    return entry[0] if entry else None


def type_words(device_type: str | None) -> tuple[str, ...]:
    entry = TYPE_WORDS.get(str(device_type or "").casefold())
    return entry[1] if entry else ()
