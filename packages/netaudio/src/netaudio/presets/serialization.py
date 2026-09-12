from __future__ import annotations

import binascii
import copy
import json
import xml.etree.ElementTree as ET
from collections.abc import Collection, Mapping
from typing import Any

from netaudio import DanteDevice
from netaudio.dante.latency import milliseconds_to_microseconds
from netaudio.dante.transmit_flow import MediaMode, TransmitFlowSpecification
from netaudio.presets.schema import (
    PRESET_EXTENSION_CONTENT_TAG,
    PRESET_EXTENSION_TAG,
    PRESET_SCHEMA_VERSION,
    ParsedPresetDevices,
    json_ready_config,
    normalize_device_config,
)

PRESET_SECTIONS = frozenset({"routing", "audio", "network"})


def _hex_encode(text: str, pad_to: int = 16) -> str:
    encoded = binascii.hexlify(text.encode()).decode().upper()
    return encoded.ljust(pad_to, "0")


def _sub_text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    child = ET.SubElement(parent, tag)
    child.text = text
    return child


def _configured_interface(entry: Mapping[str, Any], index: int) -> dict[str, Any]:
    configured = entry.get("configured")
    selected = dict(configured) if isinstance(configured, Mapping) else dict(entry)
    identity = entry.get("interface", entry.get("identity"))
    if identity is None:
        identity = "primary" if index == 0 else "secondary" if index == 1 else str(index)
    selected["identity"] = str(identity)
    if selected.get("mode") == "dhcp":
        selected["mode"] = "dynamic"
    keep = {"identity", "mode", "ip_address", "netmask", "gateway", "dns_server", "unknown_fields"}
    return {key: copy.deepcopy(value) for key, value in selected.items() if key in keep and value is not None}


def _canonical_transmit_flows(device: DanteDevice) -> list[dict[str, Any]]:
    records = getattr(device, "transmitter_flows", None)
    if records is None:
        return []
    if not isinstance(records, list):
        raise ValueError(f"{device.name}: transmitter-flow inventory is malformed")
    protocol_id = getattr(device, "flow_protocol_id", None)
    if protocol_id is None and records:
        raise ValueError(f"{device.name}: transmitter-flow protocol identity is unavailable")
    result = []
    for record in records:
        try:
            if isinstance(record, Mapping) and "media_mode" in record:
                specification = TransmitFlowSpecification.from_dict(record)
            else:
                specification = TransmitFlowSpecification.from_inventory_record(
                    record,
                    protocol_id=protocol_id,
                    media_mode=MediaMode.NATIVE_DANTE,
                )
        except (TypeError, ValueError) as exception:
            raise ValueError(
                f"{device.name}: transmitter-flow inventory cannot be represented losslessly: {exception}"
            ) from exception
        result.append(specification.to_dict())
    return result


def device_preset_config(device: DanteDevice, sections: Collection[str]) -> dict[str, Any]:
    """Build the canonical preset configuration from one freshly populated device."""
    sections = set(sections)
    config: dict[str, Any] = {
        "name": device.name or device.server_name,
        "device_name": device.name or device.server_name,
        "device_identity": {
            key: value
            for key, value in {
                "server_name": device.server_name or None,
                "mac_address": device.mac_address,
                "inventory_id": getattr(device, "inventory_id", None),
            }.items()
            if value
        },
    }
    if "audio" in sections:
        for field_name in ("preferred_leader", "sample_rate", "encoding"):
            value = getattr(device, field_name, None)
            if value is not None:
                config[field_name] = value
        latency = device.configured_latency
        if latency is None and device.active_latency is None:
            latency = device.latency
        if latency is not None:
            config["latency"] = latency
        pullup = getattr(device, "sample_rate_pullup_raw_value", None)
        if pullup is not None:
            config["sample_rate_pullup"] = pullup
        if getattr(device, "clock_source_code", None) is not None:
            config["clock_source_code"] = device.clock_source_code
        clock_preferences = getattr(device, "ddm_clock_preferences", None)
        if isinstance(clock_preferences, Mapping) and clock_preferences.get("external_word_clock") is not None:
            config["external_word_clock"] = clock_preferences["external_word_clock"]
        gain_type = getattr(device, "gain_device_type", None)
        gain_levels = getattr(device, "gain_levels", None)
        if gain_type in ("input", "output") and isinstance(gain_levels, list):
            config["codec_gain"] = [
                {"channel": index, "device_type": gain_type, "level": level}
                for index, level in enumerate(gain_levels, start=1)
            ]
    if "network" in sections:
        if device.interfaces is not None:
            config["interfaces"] = [
                _configured_interface(entry, index) for index, entry in enumerate(device.interfaces)
            ]
        redundancy = getattr(device, "dante_redundancy", None)
        if isinstance(redundancy, Mapping) and redundancy.get("configured") is not None:
            config["redundancy_mode"] = redundancy["configured"]
    if "routing" in sections:
        transmitter_names = {
            channel.number: channel.friendly_name or channel.name
            for channel in sorted(device.tx_channels.values(), key=lambda item: item.number)
        }
        receiver_names = {
            channel.number: channel.friendly_name or channel.name
            for channel in sorted(device.rx_channels.values(), key=lambda item: item.number)
        }
        if transmitter_names:
            config["transmitter_channel_names"] = transmitter_names
        if receiver_names:
            config["receiver_channel_names"] = receiver_names
        if receiver_names:
            subscriptions: dict[int, dict[str, Any] | None] = {number: None for number in receiver_names}
            channels_by_name = {
                name: number
                for channel in device.rx_channels.values()
                for name in (channel.name, channel.friendly_name)
                if name
                for number in (channel.number,)
            }
            for subscription in device.subscriptions:
                number = getattr(subscription, "_netaudio_rx_channel_number", None)
                if number is None:
                    number = channels_by_name.get(subscription.rx_channel_name)
                if number not in subscriptions:
                    continue
                if subscription.tx_channel_name and subscription.tx_device_name:
                    subscriptions[number] = {
                        "kind": "native_dante",
                        "tx_channel": subscription.tx_channel_name,
                        "tx_device": subscription.tx_device_name,
                    }
            config["rx_subscriptions"] = subscriptions
        if device.transmitter_flows is not None:
            config["transmit_flows"] = _canonical_transmit_flows(device)
    return normalize_device_config(config)


def _append_preset_identity(element: ET.Element, config: Mapping[str, Any], device: DanteDevice | None) -> None:
    name = str(config["name"])
    _sub_text(element, "name", name)
    server_name = getattr(device, "server_name", "") if device is not None else ""
    _sub_text(element, "default_name", server_name.replace(".local.", "") if server_name else "")
    instance_id = ET.SubElement(element, "instance_id")
    mac = (getattr(device, "mac_address", None) or "").replace(":", "").upper() if device else ""
    if mac:
        if len(mac) == 12:
            mac = mac[:6] + "FFFE" + mac[6:]
        _sub_text(instance_id, "device_id", mac)
    _sub_text(instance_id, "process_id", "0")
    if device is not None and device.manufacturer:
        _sub_text(element, "manufacturer_id", _hex_encode(device.manufacturer))
        _sub_text(element, "manufacturer_name", device.manufacturer)
    model_id = (device.model_id or "") if device is not None else ""
    dante_model = (device.dante_model or model_id) if device is not None else ""
    if model_id:
        model_id_hex = _hex_encode(model_id)
        _sub_text(element, "model_id", model_id_hex)
        _sub_text(element, "model_name", dante_model)
        if device is not None and device.product_version:
            _sub_text(element, "model_version", device.product_version)
        _sub_text(element, "device_type", model_id_hex)
        _sub_text(element, "device_type_string", model_id)
    _sub_text(element, "friendly_name", name)


def _append_preset_audio_settings(element: ET.Element, config: Mapping[str, Any]) -> None:
    if config.get("preferred_leader") is not None:
        ET.SubElement(element, "preferred_master", value=str(config["preferred_leader"]).lower())
    if config.get("sample_rate") is not None:
        _sub_text(element, "samplerate", str(config["sample_rate"]))
    if config.get("encoding") is not None:
        _sub_text(element, "encoding", str(config["encoding"]))
    if config.get("latency") is not None:
        _sub_text(element, "unicast_latency", str(milliseconds_to_microseconds(config["latency"])))


def _append_preset_interfaces(element: ET.Element, config: Mapping[str, Any]) -> None:
    for index, interface in enumerate(config.get("interfaces", [])):
        identity = interface.get("identity")
        network = "0" if identity == "primary" else "1" if identity == "secondary" else str(identity or index)
        interface_element = ET.SubElement(element, "interface", network=network)
        mode = interface.get("mode", "dynamic")
        if mode == "static":
            address = ET.SubElement(interface_element, "ipv4_address", mode="static")
            _sub_text(address, "ip_address", interface["ip_address"])
            _sub_text(address, "subnet_mask", interface["netmask"])
            _sub_text(address, "gateway", interface["gateway"])
            _sub_text(address, "dns_server", interface["dns_server"])
        else:
            ET.SubElement(interface_element, "ipv4_address", mode="dynamic")


def _append_preset_channels(element: ET.Element, config: Mapping[str, Any]) -> None:
    for number, name in sorted(config.get("transmitter_channel_names", {}).items()):
        tx_element = ET.SubElement(element, "txchannel", danteId=str(number), mediaType="audio")
        _sub_text(tx_element, "label", name)
    subscriptions = config.get("rx_subscriptions", {})
    for number, name in sorted(config.get("receiver_channel_names", {}).items()):
        rx_element = ET.SubElement(element, "rxchannel", danteId=str(number), mediaType="audio")
        _sub_text(rx_element, "name", name)
        subscription = subscriptions.get(number)
        if isinstance(subscription, Mapping) and subscription.get("kind", "native_dante") == "native_dante":
            if subscription.get("tx_channel"):
                _sub_text(rx_element, "subscribed_channel", str(subscription["tx_channel"]))
            if subscription.get("tx_device"):
                _sub_text(rx_element, "subscribed_device", str(subscription["tx_device"]))


def _append_unknown_xml(parent: ET.Element, values: Any) -> None:
    if not isinstance(values, list):
        return
    for value in values:
        if isinstance(value, str):
            parent.append(ET.fromstring(value))


def _device_to_preset_xml(
    config: Mapping[str, Any], sections: Collection[str], device: DanteDevice | None = None
) -> ET.Element:
    unknown = config.get("unknown_fields") if isinstance(config.get("unknown_fields"), Mapping) else {}
    attributes = unknown.get("xml_attributes") if isinstance(unknown, Mapping) else {}
    element = ET.Element("device", attributes if isinstance(attributes, dict) else {})
    _append_preset_identity(element, config, device)
    if "audio" in sections:
        _append_preset_audio_settings(element, config)
    if "network" in sections:
        _append_preset_interfaces(element, config)
    if "routing" in sections:
        _append_preset_channels(element, config)
    extension = ET.SubElement(element, PRESET_EXTENSION_TAG, schema_version=str(PRESET_SCHEMA_VERSION))
    _sub_text(
        extension,
        PRESET_EXTENSION_CONTENT_TAG,
        json.dumps(json_ready_config(config), sort_keys=True, separators=(",", ":")),
    )
    if isinstance(unknown, Mapping):
        _append_unknown_xml(element, unknown.get("xml_elements"))
    return element


def _validated_sections(sections: Collection[str] | None) -> set[str]:
    result = set(PRESET_SECTIONS if sections is None else sections)
    if not result or result - PRESET_SECTIONS:
        raise ValueError("preset sections must be routing, audio, or network")
    return result


def format_preset_configs(
    configs: Mapping[str, Mapping[str, Any]],
    preset_name: str = "netaudio",
    *,
    sections: Collection[str] | None = None,
) -> str:
    selected_sections = _validated_sections(sections)
    source_version = getattr(configs, "source_version", None)
    root_attributes = copy.deepcopy(getattr(configs, "root_attributes", {}))
    root = ET.Element("preset", {"version": source_version or "2.1.0", **root_attributes})
    _sub_text(root, "name", preset_name)
    _sub_text(root, "description", "Dante Controller preset with NetAudio schema-v2 extension")
    for device_name, raw_config in sorted(configs.items()):
        config = normalize_device_config(raw_config)
        if config["name"] != device_name:
            raise ValueError(f"preset device key {device_name!r} does not match its name")
        root.append(_device_to_preset_xml(config, selected_sections))
    _append_unknown_xml(root, getattr(configs, "unknown_root_elements", []))
    ET.indent(root, space="    ")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(root, encoding="unicode")


def format_devices_xml(
    devices: dict[str, DanteDevice], preset_name: str = "netaudio", *, sections: Collection[str] | None = None
) -> str:
    selected_sections = _validated_sections(sections)
    configs = ParsedPresetDevices()
    device_by_name = {}
    for server_name, device in sorted(devices.items(), key=lambda item: item[1].name or item[0]):
        config = device_preset_config(device, selected_sections)
        name = config["name"]
        if name in configs:
            raise ValueError(f"duplicate preset device name: {name!r}")
        configs[name] = config
        device_by_name[name] = device
    root = ET.Element("preset", version="2.1.0")
    _sub_text(root, "name", preset_name)
    _sub_text(root, "description", "Dante Controller preset with NetAudio schema-v2 extension")
    for name, config in configs.items():
        root.append(_device_to_preset_xml(config, selected_sections, device_by_name[name]))
    ET.indent(root, space="    ")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(root, encoding="unicode")
