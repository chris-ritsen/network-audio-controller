from __future__ import annotations

DEVICE_EVENTS = frozenset({"device_discovered", "device_updated"})

NOTIFIED_TELEMETRY_FIELDS = frozenset({"receiver_flow_connection_health"})

TELEMETRY_FIELDS = frozenset(
    {
        "clock_frequency_offset_parts_per_billion",
        "last_seen",
        "network_interface_traffic",
        "receiver_flow_connection_health",
        "clock_observations",
    }
)


class SseDeviceView:
    def __init__(self, *, patches: bool, telemetry: bool):
        self.devices: dict[str, dict] = {}
        self.managed = None
        self.notified_telemetry: dict[str, dict] = {}
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
            self.notified_telemetry.pop(data.get("server_name"), None)
            return [data]
        if event == "snapshot":
            return self._snapshot_events(data)
        return [data]

    def initial_snapshot(self, snapshot: dict) -> dict:
        records = snapshot.get("devices") or {}
        devices = {server_name: self._visible(record) for server_name, record in records.items()}
        self.devices = dict(devices)
        self.notified_telemetry = {server_name: self._notified(record) for server_name, record in records.items()}
        self.managed = snapshot.get("managed")
        return {**snapshot, "devices": devices}

    def _device_events(self, server_name, record: dict, event: str) -> list[dict]:
        events = self._record_events(server_name, record, event)
        fields = self._changed_notified_telemetry(server_name, record)
        if fields:
            events.append({"event": "telemetry_updated", "server_name": server_name, "fields": fields})
        return events

    def _changed_notified_telemetry(self, server_name, record: dict) -> list[str]:
        if self.telemetry:
            return []
        current = self._notified(record)
        previous = self.notified_telemetry.get(server_name)
        self.notified_telemetry[server_name] = current
        if previous is None:
            return []
        return sorted(key for key in current.keys() | previous.keys() if current.get(key) != previous.get(key))

    def _notified(self, record: dict) -> dict:
        return {key: record[key] for key in NOTIFIED_TELEMETRY_FIELDS if key in record}

    def _record_events(self, server_name, record: dict, event: str) -> list[dict]:
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
            self.notified_telemetry.pop(server_name, None)
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
