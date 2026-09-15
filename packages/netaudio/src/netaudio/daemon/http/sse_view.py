from __future__ import annotations

DEVICE_EVENTS = frozenset({"device_discovered", "device_updated"})

TELEMETRY_FIELDS = frozenset(
    {
        "clock_frequency_offset_parts_per_billion",
        "last_seen",
        "network_interface_traffic",
        "receiver_flow_connection_health",
    }
)


class SseDeviceView:
    def __init__(self, *, patches: bool, telemetry: bool):
        self.devices: dict[str, dict] = {}
        self.managed = None
        self.patches = patches
        self.telemetry = telemetry

    @classmethod
    def from_query(cls, query: dict[str, list[str]]) -> SseDeviceView | None:
        patches = query.get("patches", [""])[-1] == "1"
        telemetry = query.get("telemetry", [""])[-1] != "0"
        if not patches and telemetry:
            return None
        return cls(patches=patches, telemetry=telemetry)

    def events_for(self, data: dict) -> list[dict]:
        event = data.get("event")
        if event in DEVICE_EVENTS and isinstance(data.get("device"), dict):
            return self._device_events(data.get("server_name"), data["device"], event)
        if event == "device_removed":
            self.devices.pop(data.get("server_name"), None)
            return [data]
        if event == "snapshot":
            return self._snapshot_events(data)
        return [data]

    def initial_snapshot(self, snapshot: dict) -> dict:
        devices = {
            server_name: self._visible(record) for server_name, record in (snapshot.get("devices") or {}).items()
        }
        self.devices = dict(devices)
        self.managed = snapshot.get("managed")
        return {**snapshot, "devices": devices}

    def _device_events(self, server_name, record: dict, event: str) -> list[dict]:
        visible = self._visible(record)
        previous = self.devices.get(server_name)
        self.devices[server_name] = visible
        if previous is not None and previous == visible:
            return []
        if previous is None or not self.patches:
            return [{"event": event, "server_name": server_name, "device": visible}]
        changed = {key: value for key, value in visible.items() if previous.get(key) != value}
        removed = sorted(key for key in previous if key not in visible)
        return [{"event": "device_patch", "server_name": server_name, "changed": changed, "removed": removed}]

    def _snapshot_events(self, snapshot: dict) -> list[dict]:
        devices = snapshot.get("devices") or {}
        if not self.patches:
            return [self.initial_snapshot(snapshot)]
        events = []
        for server_name in sorted(set(self.devices) - set(devices)):
            del self.devices[server_name]
            events.append({"event": "device_removed", "server_name": server_name})
        for server_name, record in devices.items():
            events.extend(self._device_events(server_name, record, "device_discovered"))
        managed = snapshot.get("managed")
        if managed != self.managed:
            self.managed = managed
            events.append({"event": "managed_status", "managed": managed})
        return events

    def _visible(self, record: dict) -> dict:
        if self.telemetry:
            return record
        return {key: value for key, value in record.items() if key not in TELEMETRY_FIELDS}
