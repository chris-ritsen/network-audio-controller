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

VOLATILE_RECORD_FIELDS = frozenset({"ddm_last_sync", "clock_observed_at"})

METER_EVENTS = frozenset({"meter_values", "shure_meter_values"})

CLOCK_STATUS_TELEMETRY_KEYS = frozenset({"clock_frequency_offset_parts_per_billion", "raw_record"})


def substantive_record(record: dict) -> dict:
    substantive = {key: value for key, value in record.items() if key not in VOLATILE_RECORD_FIELDS}
    clock_status = substantive.get("clock_status")
    if isinstance(clock_status, dict):
        substantive["clock_status"] = {
            key: value for key, value in clock_status.items() if key not in CLOCK_STATUS_TELEMETRY_KEYS
        }
    channels = substantive.get("channels")
    if isinstance(channels, dict):
        substantive["channels"] = {
            direction: {
                number: {key: value for key, value in channel.items() if key != "ddm_signal_presence"}
                if isinstance(channel, dict)
                else channel
                for number, channel in entries.items()
            }
            if isinstance(entries, dict)
            else entries
            for direction, entries in channels.items()
        }
    return substantive


class SseDeviceView:
    def __init__(self, *, patches: bool, telemetry: bool | frozenset[str], meters: bool = True, notices: bool = True):
        self.devices: dict[str, dict] = {}
        self.managed = None
        self.notified_telemetry: dict[str, dict] = {}
        self.patches = patches
        self.meters = meters
        self.notices = notices
        if telemetry is True:
            self.telemetry_fields = TELEMETRY_FIELDS
        elif telemetry is False:
            self.telemetry_fields = frozenset()
        else:
            self.telemetry_fields = frozenset(telemetry) & TELEMETRY_FIELDS
        self.telemetry = telemetry is not False
        self.hidden_fields = TELEMETRY_FIELDS - self.telemetry_fields
        self.notified_fields = NOTIFIED_TELEMETRY_FIELDS & self.hidden_fields

    @classmethod
    def from_query(cls, query: dict[str, list[str]]) -> SseDeviceView | None:
        patches = query.get("patches", [""])[-1] == "1"
        meters = query.get("meters", [""])[-1] != "0"
        requested = query.get("telemetry", [""])[-1]
        notices = requested != "none"
        if requested in ("", "1"):
            telemetry: bool | frozenset[str] = True
        elif requested in ("0", "none"):
            telemetry = False
        else:
            telemetry = frozenset(name.strip() for name in requested.split(",") if name.strip())
        if not patches and telemetry is True and meters:
            return None
        return cls(patches=patches, telemetry=telemetry, meters=meters, notices=notices)

    def events_for(self, data: dict) -> list[dict]:
        event = data.get("event")
        if event in METER_EVENTS:
            return [data] if self.meters else []
        if event == "telemetry_updated":
            events = self._telemetry_events(data)
            return events if self.notices else [item for item in events if item.get("event") != "telemetry_updated"]
        if event in DEVICE_EVENTS and isinstance(data.get("device"), dict):
            return self._device_events(data.get("server_name"), data["device"], event)
        if event == "device_removed":
            self.devices.pop(data.get("server_name"), None)
            self.notified_telemetry.pop(data.get("server_name"), None)
            return [data]
        if event == "snapshot":
            return self._snapshot_events(data)
        if event == "managed_status":
            if data.get("managed") == self.managed:
                return []
            self.managed = data.get("managed")
            return [data]
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
        fields = self._changed_notified_telemetry(server_name, record) if self.notices else []
        if fields:
            events.append({"event": "telemetry_updated", "server_name": server_name, "fields": fields})
        return events

    def _changed_notified_telemetry(self, server_name, record: dict) -> list[str]:
        if not self.notified_fields:
            return []
        current = self._notified(record)
        previous = self.notified_telemetry.get(server_name)
        self.notified_telemetry[server_name] = current
        if previous is None:
            return []
        return sorted(key for key in current.keys() | previous.keys() if current.get(key) != previous.get(key))

    def _notified(self, record: dict) -> dict:
        return {key: record[key] for key in self.notified_fields if key in record}

    def _record_events(self, server_name, record: dict, event: str) -> list[dict]:
        visible = self._visible(record)
        previous = self.devices.get(server_name)
        self.devices[server_name] = visible
        if previous is not None and previous == visible:
            return []
        if previous is not None and not self.patches and substantive_record(previous) == substantive_record(visible):
            return []
        if previous is None or not self.patches:
            return [{"event": event, "server_name": server_name, "device": visible}]
        changed = {key: value for key, value in visible.items() if previous.get(key) != value}
        removed = sorted(key for key in previous if key not in visible)
        return [{"event": "device_patch", "server_name": server_name, "changed": changed, "removed": removed}]

    def _telemetry_events(self, data: dict) -> list[dict]:
        server_name = data.get("server_name")
        fields = data.get("fields") or []
        if not self.telemetry:
            return [{"event": "telemetry_updated", "server_name": server_name, "fields": fields}]
        notices = sorted(field for field in fields if field in self.notified_fields)
        events = [{"event": "telemetry_updated", "server_name": server_name, "fields": notices}] if notices else []
        previous = self.devices.get(server_name)
        if previous is None:
            return events
        telemetry = {
            key: value for key, value in (data.get("telemetry") or {}).items() if key not in self.hidden_fields
        }
        changed = {key: value for key, value in telemetry.items() if value is not None and previous.get(key) != value}
        removed = sorted(key for key, value in telemetry.items() if value is None and key in previous)
        if not changed and not removed:
            return events
        current = {key: value for key, value in previous.items() if key not in removed}
        current.update(changed)
        self.devices[server_name] = current
        if not self.patches:
            return [{"event": "device_updated", "server_name": server_name, "device": current}, *events]
        return [{"event": "device_patch", "server_name": server_name, "changed": changed, "removed": removed}, *events]

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
        if not self.hidden_fields:
            return record
        return {key: value for key, value in record.items() if key not in self.hidden_fields}
