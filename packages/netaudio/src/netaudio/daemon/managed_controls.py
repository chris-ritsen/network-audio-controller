from __future__ import annotations

import copy

from netaudio.dante.device_serializer import DEVICE_SCALAR_FIELDS, DanteDeviceSerializer


class ManagedDeviceControls:
    """Keep managed control state attached to the application across inventory polls."""

    def __init__(self, application):
        self.application = application
        self._owned = {}
        self._scopes = {}
        self._snapshots = {}

    def direct_devices(self):
        return {key: device for key, device in self.application.devices.items() if self._owned.get(key) is not device}

    def reconcile(self, records):
        managed_keys = {key for key, record in records.items() if record.get("management_state") == "managed"}
        for key, record in records.items():
            if record.get("management_state") != "unenrolled":
                continue
            device = self.application.devices.get(key)
            if device is not None and self._owned.get(key) is not device:
                for field in DEVICE_SCALAR_FIELDS:
                    if field.startswith("ddm_") or field == "management_state":
                        setattr(device, field, copy.deepcopy(record.get(field)))
        for key in self._scopes.keys() - managed_keys:
            device = self._owned.pop(key, None)
            if device is not None:
                device.online = False
                if self.application.devices.get(key) is device:
                    self.application.devices.pop(key)
            else:
                device = self.application.devices.get(key)
                if device is not None:
                    if records.get(key, {}).get("management_state") == "unenrolled":
                        for field in DEVICE_SCALAR_FIELDS:
                            if field.startswith("ddm_") or field == "management_state":
                                setattr(device, field, copy.deepcopy(records[key].get(field)))
                    else:
                        # Losing the manager does not unenroll the physical device.
                        device.online = False
            self._scopes.pop(key, None)
            self._snapshots.pop(key, None)

        for key in managed_keys:
            record = records[key]
            scope = tuple(
                record.get(field) for field in ("ddm_server_profile", "ddm_context", "ddm_domain_id", "ddm_device_id")
            )
            device = self.application.devices.get(key)
            if device is None or self._scopes.get(key) != scope:
                previous = device
                device = DanteDeviceSerializer.device_from_json(record)
                device._app = self.application
                self.application.devices[key] = device
                if previous is None or key in self._owned:
                    self._owned[key] = device
                if previous is not None and key in self._scopes:
                    previous.online = False
                self._scopes[key] = scope
                self._snapshots.pop(key, None)

            # Inventory owns identity, enrollment, availability and channel metadata.
            # Readbacks own configuration; an unchanged poll must not erase them.
            snapshot = {
                field: value
                for field, value in record.items()
                if field.startswith("ddm_")
                or field
                in {
                    "name",
                    "online",
                    "ipv4",
                    "availability_state",
                    "channels",
                    "subscriptions",
                    "inventory_id",
                    "inventory_sources",
                    "control_transports",
                    "direct_control_available",
                    "management_state",
                    "preferred_leader",
                    "rx_count",
                    "tx_count",
                }
            }
            if self._snapshots.get(key) != snapshot:
                fresh = DanteDeviceSerializer.device_from_json(record)
                for field in DEVICE_SCALAR_FIELDS:
                    if field in snapshot:
                        setattr(device, field, copy.deepcopy(getattr(fresh, field)))
                device.name = fresh.name
                device.online = fresh.online
                device.ipv4 = fresh.ipv4
                device.rx_channels = fresh.rx_channels
                device.tx_channels = fresh.tx_channels
                for channel in (*device.rx_channels.values(), *device.tx_channels.values()):
                    channel.device = device
                device.subscriptions = fresh.subscriptions
                self._snapshots[key] = copy.deepcopy(snapshot)
            records[key] = DanteDeviceSerializer.to_json(device)
        return records

    def clear(self):
        self.reconcile({})
