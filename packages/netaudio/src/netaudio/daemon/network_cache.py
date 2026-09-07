from __future__ import annotations

import json
import logging
import os
import tempfile
from copy import deepcopy
from pathlib import Path

logger = logging.getLogger("netaudio")


class NetworkStatusCache:
    """Last-known interface observations, scoped to a discovered control endpoint."""

    def __init__(self, path: Path):
        self.path = path
        try:
            records = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            records = {}
        self.records = records if isinstance(records, dict) else {}

    @staticmethod
    def _key(device):
        return json.dumps([device.server_name, str(device.ipv4), device._arc_port()])

    def restore(self, device):
        record = self.records.get(self._key(device))
        if not isinstance(record, dict):
            return False
        speed = record.get("link_speed_mbps")
        interfaces = record.get("interfaces")
        if isinstance(speed, bool) or not isinstance(speed, int) or speed < 0:
            return False
        if not isinstance(interfaces, list) or any(not isinstance(entry, dict) for entry in interfaces):
            return False
        changed = False
        if device.link_speed_mbps is None:
            device.link_speed_mbps = speed
            changed = True
        if not device.interfaces:
            device.interfaces = deepcopy(interfaces)
            changed = True
        return changed

    def remember(self, device):
        speed = device.link_speed_mbps
        if isinstance(speed, bool) or not isinstance(speed, int) or speed < 0 or device.interfaces is None:
            return
        record = {"link_speed_mbps": speed, "interfaces": deepcopy(device.interfaces)}
        key = self._key(device)
        if self.records.get(key) == record:
            return
        records = {**self.records, key: record}
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary = tempfile.mkstemp(dir=self.path.parent, prefix=".network-status-")
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(records, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            self.records = records
        except OSError as exception:
            logger.warning("Could not save network status cache: %s", exception)
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)
