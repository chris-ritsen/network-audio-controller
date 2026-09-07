from __future__ import annotations

import asyncio
import hashlib
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict
from datetime import datetime, timezone

from netaudio.core.binding import NetaudioCoreError
from netaudio.presets.loading import (
    MatchedPresetDevice,
    PresetLoadReport,
    build_preset_plan,
    apply_preset_plan,
)
from netaudio.presets.parsing import parse_preset_xml
from netaudio.presets.serialization import format_devices_xml

MAX_PRESET_BYTES = 4 * 1024 * 1024
MAX_PRESET_DEVICES = 128
PRESET_READ_TIMEOUT = 60
PRESET_APPLY_TIMEOUT = 180
PRESET_SECTIONS = {"routing", "audio", "network"}


def _xml_input(params):
    content = params.get("xml")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Select a preset XML file first.")
    encoded = content.encode("utf-8")
    if len(encoded) > MAX_PRESET_BYTES:
        raise ValueError("Preset files must be no larger than 4 MiB.")
    name, devices = parse_preset_xml(content)
    if not devices or len(devices) > MAX_PRESET_DEVICES:
        raise ValueError("A preset must contain between 1 and 128 named devices.")
    return name, devices, hashlib.sha256(encoded).hexdigest()


def _device_ids(value, *, allow_empty=False):
    if not isinstance(value, list) or len(value) > MAX_PRESET_DEVICES or (not value and not allow_empty):
        raise ValueError("Select between 1 and 128 devices.")
    if any(not isinstance(identifier, str) or not identifier for identifier in value):
        raise ValueError("Devices must be identified by their inventory IDs.")
    if len(value) != len(set(value)):
        raise ValueError("A device may only be selected once.")
    return value


def _configuration_summary(config):
    entries = []
    for field, label, suffix in (
        ("sample_rate", "Sample rate", " Hz"),
        ("encoding", "Encoding", "-bit"),
        ("latency", "Latency", " ms"),
    ):
        if field in config:
            entries.append({"label": label, "value": f"{config[field]}{suffix}"})
    if "preferred_leader" in config:
        entries.append({"label": "Preferred leader", "value": "On" if config["preferred_leader"] else "Off"})
    if "transmitter_channel_names" in config:
        entries.append({"label": "Transmitter names", "value": f"{len(config['transmitter_channel_names'])} channels"})
    if "rx_subscriptions" in config:
        subscriptions = config["rx_subscriptions"]
        subscribed = sum(value is not None for value in subscriptions.values())
        entries.append(
            {
                "label": "Receiver routing",
                "value": f"{subscribed} subscribed, {len(subscriptions) - subscribed} unsubscribed",
            }
        )
    if "interface_mode" in config:
        mode = config["interface_mode"]
        value = f"Static {config.get('ip_address', '')}" if mode == "static" else mode.upper()
        entries.append({"label": "Network", "value": value})
    return entries


class DaemonPresetHandlers:
    def _preset_records(self, identifiers):
        records = self._serialized_devices()
        missing = [identifier for identifier in identifiers if identifier not in records]
        if missing:
            raise ValueError("The selected inventory has changed. Review the device selection again.")
        return {identifier: records[identifier] for identifier in identifiers}

    def _preset_device(self, identifier, record):
        device = self._find_device(identifier)
        if device is None or not record.get("online") or not device.online:
            raise ValueError(f"{record.get('name') or identifier}: device is unavailable or offline.")
        if not getattr(device, "requires_managed_control", False) and device.ipv4 is None:
            raise ValueError(f"{device.name}: device has no control address.")
        return device

    async def _preset_identity(self, device, expected_name):
        name = await device.fetch_device_name()
        if not name or name != expected_name:
            raise ValueError(f"{expected_name}: fresh device identity did not match. Review the preset again.")

    async def _preset_audio_snapshot(self, device):
        sample_rate, supported_rates = await self.application.probe_sample_rate_status(device)
        encoding, supported_encodings = await self.application.probe_encoding_status(device)
        settings = await self.application.get_device_settings(device)
        if (
            not sample_rate
            or not encoding
            or not isinstance(settings, dict)
            or settings.get("configured_latency_ns") is None
        ):
            raise ValueError(f"{device.name}: audio settings could not be read completely.")
        device.configured_latency = settings["configured_latency_ns"] / 1_000_000
        device.sample_rate = sample_rate
        device.supported_sample_rates = supported_rates
        device.encoding = encoding
        device.supported_encodings = supported_encodings
        device.preferred_leader = await self.application.probe_preferred_leader_state(device)
        if device.preferred_leader is None:
            raise ValueError(f"{device.name}: preferred-leader state could not be read.")

    async def _handle_save_preset(self, writer, params):
        try:
            name = params.get("name")
            if not isinstance(name, str) or not name.strip() or len(name) > 120:
                raise ValueError("Enter a preset name of 1–120 characters.")
            name = name.strip()
            identifiers = _device_ids(params.get("devices"))
            sections = params.get("sections")
            if (
                not isinstance(sections, list)
                or not sections
                or any(not isinstance(section, str) or section not in PRESET_SECTIONS for section in sections)
            ):
                raise ValueError("Choose routing, audio, or network settings to save.")
            records = self._preset_records(identifiers)
            selected = {identifier: self._preset_device(identifier, record) for identifier, record in records.items()}
            names = [device.name for device in selected.values()]
            if any(not name for name in names) or len(set(names)) != len(names):
                raise ValueError("Selected devices must have distinct names so the preset can be loaded unambiguously.")
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        if self._preset_operation_lock.locked():
            await self._send_json(writer, {"error": "Another preset operation is in progress."}, 409)
            return
        try:
            async with self._preset_operation_lock, asyncio.timeout(PRESET_READ_TIMEOUT):
                for device in selected.values():
                    await self._preset_identity(device, device.name)
                    if "routing" in sections:
                        await device.get_rx_channels()
                        await device.get_tx_channels()
                        for direction in ("rx", "tx"):
                            count = getattr(device, f"{direction}_count", None)
                            channels = getattr(device, f"{direction}_channels")
                            if count is not None and len(channels) != count:
                                raise ValueError(f"{device.name}: incomplete {direction.upper()} channel inventory.")
                    if "audio" in sections:
                        await self._preset_audio_snapshot(device)
                    if "network" in sections:
                        interfaces = await self.application.probe_interface_status(device)
                        if not interfaces:
                            raise ValueError(f"{device.name}: network settings could not be read.")
                        if len(interfaces) != 1:
                            raise ValueError(
                                f"{device.name}: multiple-interface network presets are not supported; omit network settings."
                            )
                        if interfaces[0].get("mode") not in ("static", "dynamic", "dhcp"):
                            raise ValueError(f"{device.name}: network mode is unknown; omit network settings.")
                        device.interfaces = interfaces
                content = format_devices_xml(selected, preset_name=name, sections=sections)
                if len(content.encode("utf-8")) > MAX_PRESET_BYTES:
                    raise ValueError("This preset exceeds 4 MiB. Save fewer devices together.")
        except (ValueError, OSError, RuntimeError, NetaudioCoreError) as exception:
            await self._send_json(writer, {"error": f"Preset was not saved: {exception}"}, 409)
            return
        filename = re.sub(r"[^\w .-]", "_", name).strip(" .")[:100] or "preset"
        await self._send_json(
            writer,
            {
                "name": name,
                "filename": f"{filename}.xml",
                "xml": content,
                "device_count": len(selected),
                "saved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def _handle_preview_preset(self, writer, params):
        try:
            name, configs, digest = _xml_input(params)
            records = self._preset_records(_device_ids(params.get("devices"), allow_empty=True))
        except (ValueError, ET.ParseError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        devices = []
        for device_name, config in configs.items():
            devices.append(
                {
                    "name": device_name,
                    "settings": _configuration_summary(config),
                    "unsupported": ["Additional network interfaces"] if "additional_interfaces" in config else [],
                    "targets": [
                        {
                            "id": identifier,
                            "name": record["name"],
                            "address": record.get("ipv4"),
                            "context": record.get("ddm_context"),
                            "online": bool(record.get("online")),
                        }
                        for identifier, record in records.items()
                        if record.get("name") == device_name
                    ],
                }
            )
        await self._send_json(writer, {"name": name, "digest": digest, "devices": devices})

    async def _handle_load_preset(self, writer, params):
        try:
            name, configs, digest = _xml_input(params)
            if params.get("confirmed") is not True or params.get("digest") != digest:
                raise ValueError("Review and confirm this preset before applying it.")
            confirm_destructive = params.get("confirm_destructive", False)
            if not isinstance(confirm_destructive, bool):
                raise ValueError("Destructive-change confirmation must be a boolean.")
            targets = params.get("targets")
            if not isinstance(targets, dict) or not targets or set(targets) - configs.keys():
                raise ValueError("Select named preset devices to load.")
            identifiers = _device_ids(list(targets.values()))
            excluded = params.get("excluded")
            if not isinstance(excluded, list) or any(not isinstance(value, str) for value in excluded):
                raise ValueError("Explicitly identify any preset devices being skipped.")
            if set(targets) & set(excluded) or set(targets) | set(excluded) != set(configs):
                raise ValueError("Every preset device must be selected or explicitly skipped.")
            records = self._preset_records(identifiers)
            matched = []
            for device_name, identifier in targets.items():
                record = records[identifier]
                if record.get("name") != device_name:
                    raise ValueError(f"{device_name}: the selected target no longer matches this preset device.")
                device = self._preset_device(identifier, record)
                matched.append(MatchedPresetDevice(configs[device_name], device, device_name, identifier))
        except (ValueError, ET.ParseError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        if self._preset_operation_lock.locked():
            await self._send_json(writer, {"error": "Another preset operation is in progress."}, 409)
            return
        async with self._preset_operation_lock:
            try:
                async with asyncio.timeout(PRESET_READ_TIMEOUT):
                    for entry in matched:
                        await self._preset_identity(entry.device, entry.device_name)
                        if "sample_rate" in entry.config:
                            _, entry.device.supported_sample_rates = await self.application.probe_sample_rate_status(
                                entry.device
                            )
                        if "encoding" in entry.config:
                            _, entry.device.supported_encodings = await self.application.probe_encoding_status(
                                entry.device
                            )
                    plan = await build_preset_plan(matched)
                    if not any(entry.actions for entry in plan.device_actions):
                        raise ValueError("This selection contains no supported preset settings.")
            except (ValueError, OSError, RuntimeError, NetaudioCoreError) as exception:
                await self._send_json(writer, {"error": f"No changes were sent: {exception}"}, 409)
                return
            report = PresetLoadReport()
            interrupted = False
            try:
                async with asyncio.timeout(PRESET_APPLY_TIMEOUT):
                    await apply_preset_plan(
                        self.application,
                        plan,
                        confirm_destructive=confirm_destructive,
                        report=report,
                        stop_on_failure=True,
                    )
            except (ValueError, OSError, RuntimeError, NetaudioCoreError) as exception:
                interrupted = True
                report.record(
                    "Preset",
                    f"Load interrupted; some changes may have been applied: {exception}",
                    failed=True,
                    verified=False,
                )
            await self.publish_inventory_snapshot()
            await self._send_json(
                writer,
                {
                    "name": name,
                    "complete": not (report.failures or report.unverified or interrupted),
                    "interrupted": interrupted,
                    "report": asdict(report),
                },
            )
