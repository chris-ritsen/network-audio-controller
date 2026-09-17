from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy


class NetworkStatusCache:
    """Last-known interface observations, scoped to a discovered control endpoint."""

    def __init__(self, *, max_devices: int = 256):
        if isinstance(max_devices, bool) or not isinstance(max_devices, int) or max_devices <= 0:
            raise ValueError("network cache max_devices must be a positive integer")
        self.max_devices = max_devices
        self.records: OrderedDict[tuple, dict] = OrderedDict()

    @staticmethod
    def _key(device):
        return device.server_name, str(device.ipv4), device._arc_port()

    def restore(self, device):
        record = self.records.get(self._key(device))
        if not isinstance(record, dict):
            return False
        self.records.move_to_end(self._key(device))
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
        record = {"link_speed_mbps": speed, "interfaces": device.interfaces}
        key = self._key(device)
        if self.records.get(key) == record:
            self.records.move_to_end(key)
            return
        self.records[key] = deepcopy(record)
        self.records.move_to_end(key)
        while len(self.records) > self.max_devices:
            self.records.popitem(last=False)
