from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger("netaudio")

FINGERPRINT_PROPERTIES = (
    "arcp_min",
    "arcp_vers",
    "cmcp_min",
    "cmcp_vers",
    "mf",
    "model",
    "router_debug",
    "router_info",
    "router_vers",
    "server_vers",
)
MISSES_BEFORE_SKIPPING = 2
SKIP_SECONDS = 24 * 60 * 60


def default_probe_answer_cache_path() -> Path:
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return base / "netaudio" / "unanswered-probes.json"


def device_fingerprint(device) -> dict[str, str]:
    fingerprint = {}
    for service in (getattr(device, "services", None) or {}).values():
        if not isinstance(service, dict):
            continue
        properties = service.get("properties")
        if not isinstance(properties, dict):
            continue
        for name in FINGERPRINT_PROPERTIES:
            value = properties.get(name)
            if value is not None:
                fingerprint[f"{service.get('type')}:{name}"] = str(value)
    return dict(sorted(fingerprint.items()))


class ProbeAnswerCache:
    def __init__(self, path: Path, clock: Callable[[], float] = time.time):
        self.changed = False
        self.clock = clock
        self.path = path
        self.devices = self._load()

    def record(self, device, probe: str, answered: bool) -> None:
        identity, entry = self._entry(device)
        if identity is None:
            return
        if answered:
            if entry["probes"].pop(probe, None) is None:
                return
        else:
            misses = entry["probes"].get(probe, {}).get("misses", 0) + 1
            entry["probes"][probe] = {"last_miss": self.clock(), "misses": misses}
        self.devices[identity] = entry
        self.changed = True

    def save(self) -> None:
        if not self.changed:
            return
        temporary_path = self.path.with_suffix(".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path.write_text(json.dumps({"devices": self.devices}, indent=2, sort_keys=True))
            os.replace(temporary_path, self.path)
        except OSError as error:
            logger.warning(f"Could not save the probe answer cache to {self.path}: {error}")
            return
        self.changed = False

    def skips(self, device, probe: str) -> bool:
        identity, entry = self._entry(device)
        if identity is None:
            return False
        record = entry["probes"].get(probe)
        return (
            record is not None
            and record["misses"] >= MISSES_BEFORE_SKIPPING
            and self.clock() - record["last_miss"] < SKIP_SECONDS
        )

    def _entry(self, device) -> tuple[str | None, dict]:
        identity = getattr(device, "mac_address", None)
        fingerprint = device_fingerprint(device)
        if not identity or not fingerprint:
            return None, {}
        entry = self.devices.get(identity)
        if not isinstance(entry, dict) or entry.get("fingerprint") != fingerprint:
            entry = {"fingerprint": fingerprint, "probes": {}}
        return identity, entry

    def _load(self) -> dict:
        try:
            document = json.loads(self.path.read_text())
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as error:
            logger.warning(f"Ignoring unreadable probe answer cache {self.path}: {error}")
            return {}
        devices = document.get("devices") if isinstance(document, dict) else None
        return devices if isinstance(devices, dict) else {}
