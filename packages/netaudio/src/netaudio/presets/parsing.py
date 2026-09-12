from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from netaudio.dante.latency import MICROSECONDS_PER_MILLISECOND
from netaudio.presets.schema import (
    PRESET_EXTENSION_CONTENT_TAG,
    PRESET_EXTENSION_TAG,
    PRESET_SCHEMA_VERSION,
    ParsedPresetDevices,
    normalize_device_config,
)

INTERFACE_MODES = ("dynamic", "dhcp", "static")
STATIC_INTERFACE_FIELDS = (
    ("dns", "dns_server"),
    ("gateway", "gateway"),
    ("ip", "ip_address"),
    ("netmask", "netmask"),
)


def parse_preset(preset_path: Path) -> tuple[str, dict[str, dict[str, Any]]]:
    root = ET.parse(preset_path).getroot()
    return _parse_root(root)


def parse_preset_xml(content: str) -> tuple[str, dict[str, dict[str, Any]]]:
    if "<!DOCTYPE" in content.upper() or "<!ENTITY" in content.upper():
        raise ValueError("preset XML must not contain document type or entity declarations")
    return _parse_root(ET.fromstring(content))


def _parse_root(root: ET.Element) -> tuple[str, dict[str, dict[str, Any]]]:
    if root.tag != "preset":
        raise ValueError("expected a Dante preset XML document")
    preset_name = root.findtext("name", "unknown")
    known_root_tags = {"name", "description", "device", PRESET_EXTENSION_TAG}
    preset_devices: ParsedPresetDevices = ParsedPresetDevices(
        source_version=root.get("version"),
        root_attributes={key: value for key, value in root.attrib.items() if key != "version"},
        unknown_root_elements=[
            ET.tostring(element, encoding="unicode") for element in root if element.tag not in known_root_tags
        ],
    )
    for device_element in root.findall("device"):
        device_config = _parse_device_element(device_element)
        if device_config is None:
            continue
        device_name = device_config["name"]
        if device_name in preset_devices:
            raise ValueError(f"duplicate preset device name: {device_name!r}")
        preset_devices[device_name] = device_config
    return preset_name, preset_devices


def _parse_device_element(device_element: ET.Element) -> dict[str, Any] | None:
    device_name = device_element.findtext("friendly_name") or device_element.findtext("name", "")
    if not device_name:
        return None
    device_config: dict[str, Any] = {"name": device_name}
    device_config.update(_parse_clock_and_audio_fields(device_element, device_name))
    interfaces = _parse_interfaces(device_element)
    if interfaces:
        device_config["interfaces"] = interfaces
        device_config.update(_legacy_primary_interface_fields(interfaces[0]))
    transmitter_channel_names = _parse_transmitter_channel_names(device_element)
    if transmitter_channel_names:
        device_config["transmitter_channel_names"] = transmitter_channel_names
    receiver_channel_names = _parse_receiver_channel_names(device_element, device_name)
    if receiver_channel_names:
        device_config["receiver_channel_names"] = receiver_channel_names
    receiver_channel_elements = device_element.findall("rxchannel")
    if receiver_channel_elements:
        device_config["rx_subscriptions"] = _parse_receiver_subscriptions(receiver_channel_elements, device_name)
    extension = _parse_netaudio_extension(device_element, device_name)
    if extension:
        _validate_extension_projection(device_name, device_config, extension)
        device_config = extension
    known_device_tags = {
        "name",
        "default_name",
        "instance_id",
        "manufacturer_id",
        "manufacturer_name",
        "model_id",
        "model_name",
        "model_version",
        "device_type",
        "device_type_string",
        "friendly_name",
        "preferred_master",
        "samplerate",
        "encoding",
        "unicast_latency",
        "interface",
        "txchannel",
        "rxchannel",
        PRESET_EXTENSION_TAG,
    }
    unknown_elements = [
        ET.tostring(element, encoding="unicode") for element in device_element if element.tag not in known_device_tags
    ]
    unknown_attributes = dict(device_element.attrib)
    if unknown_elements or unknown_attributes:
        unknown_fields = device_config.setdefault("unknown_fields", {})
        if unknown_attributes:
            unknown_fields.setdefault("xml_attributes", {}).update(unknown_attributes)
        if unknown_elements and not (extension and unknown_fields.get("xml_elements")):
            preserved_elements = unknown_fields.setdefault("xml_elements", [])
            preserved_elements.extend(element for element in unknown_elements if element not in preserved_elements)
    return normalize_device_config(device_config)


def _validate_extension_projection(
    device_name: str,
    conventional: dict[str, Any],
    extension: dict[str, Any],
) -> None:
    if extension.get("name") != device_name:
        raise ValueError(f"{device_name}: extension device identity does not match the XML device name")
    for field_name, conventional_value in conventional.items():
        if field_name in {"interface_mode", "ip_address", "netmask", "gateway", "dns_server"}:
            continue
        if field_name not in extension:
            raise ValueError(f"{device_name}: NetAudio extension omits XML field {field_name}")
        extension_value = _conventional_projection(field_name, extension[field_name])
        if extension_value != conventional_value:
            raise ValueError(f"{device_name}: conflicting {field_name} values in XML and NetAudio extension")


def _conventional_projection(field_name: str, value: Any) -> Any:
    if field_name == "interfaces":
        return [
            {
                key: copy_value
                for key, copy_value in interface.items()
                if key in {"identity", "mode", "ip_address", "netmask", "gateway", "dns_server"}
            }
            for interface in value
        ]
    if field_name == "rx_subscriptions":
        projected = {}
        for channel, subscription in value.items():
            if not isinstance(subscription, dict) or subscription.get("kind", "native_dante") != "native_dante":
                projected[channel] = None
            else:
                projected[channel] = {
                    "tx_channel": subscription.get("tx_channel"),
                    "tx_device": subscription.get("tx_device", "."),
                }
        return projected
    return value


def _parse_clock_and_audio_fields(device_element: ET.Element, device_name: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    preferred_element = device_element.find("preferred_master")
    if preferred_element is not None:
        preferred_value = preferred_element.get("value", "").strip().lower()
        if preferred_value not in ("true", "false"):
            raise ValueError(f"{device_name}: preferred_master value must be true or false")
        fields["preferred_leader"] = preferred_value == "true"
    sample_rate = device_element.findtext("samplerate")
    if sample_rate:
        fields["sample_rate"] = int(sample_rate)
    encoding = device_element.findtext("encoding")
    if encoding:
        fields["encoding"] = int(encoding)
    latency = device_element.findtext("unicast_latency")
    if latency:
        fields["latency"] = int(latency) / MICROSECONDS_PER_MILLISECOND
    return fields


def _parse_interfaces(device_element: ET.Element) -> list[dict[str, Any]]:
    interfaces = []
    seen_identities = set()
    for index, interface_element in enumerate(device_element.findall("interface")):
        network = interface_element.get("network")
        identity = "primary" if network in (None, "0") and index == 0 else "secondary" if network == "1" else network
        identity = identity or str(index)
        if identity in seen_identities:
            raise ValueError(f"duplicate interface identity: {identity}")
        seen_identities.add(identity)
        address_element = interface_element.find("ipv4_address")
        if address_element is None:
            continue
        mode = address_element.get("mode", "dynamic")
        entry: dict[str, Any] = {"identity": identity, "mode": mode}
        if mode == "static":
            entry.update(
                {
                    "ip_address": address_element.findtext("ip_address", ""),
                    "netmask": address_element.findtext("subnet_mask", ""),
                    "gateway": address_element.findtext("gateway", ""),
                    "dns_server": address_element.findtext("dns_server", ""),
                }
            )
        unknown_attributes = {key: value for key, value in interface_element.attrib.items() if key != "network"}
        unknown_children = [
            ET.tostring(child, encoding="unicode") for child in interface_element if child.tag != "ipv4_address"
        ]
        if unknown_attributes or unknown_children:
            entry["unknown_fields"] = {
                "xml_attributes": unknown_attributes,
                "xml_elements": unknown_children,
            }
        interfaces.append(entry)
    return interfaces


def _legacy_primary_interface_fields(interface: dict[str, Any]) -> dict[str, Any]:
    fields = {"interface_mode": interface["mode"]}
    if interface["mode"] == "static":
        fields.update({key: interface[key] for _, key in STATIC_INTERFACE_FIELDS})
    return fields


def _parse_transmitter_channel_names(device_element: ET.Element) -> dict[int, str]:
    transmitter_channel_names: dict[int, str] = {}
    for transmitter_element in device_element.findall("txchannel"):
        dante_identifier = transmitter_element.get("danteId")
        label = transmitter_element.findtext("label", "")
        if dante_identifier and label:
            identifier = int(dante_identifier)
            if not 1 <= identifier <= 65535 or identifier in transmitter_channel_names:
                raise ValueError("transmitter channel danteId must be unique and from 1 through 65535")
            transmitter_channel_names[identifier] = label
    return transmitter_channel_names


def _parse_receiver_channel_names(device_element: ET.Element, device_name: str) -> dict[int, str]:
    names = {}
    for receiver_element in device_element.findall("rxchannel"):
        identifier = _parse_receiver_channel_number(receiver_element, device_name)
        name = receiver_element.findtext("name")
        if name is not None:
            names[identifier] = name
    return names


def _parse_netaudio_extension(device_element: ET.Element, device_name: str) -> dict[str, Any]:
    extensions = device_element.findall(PRESET_EXTENSION_TAG)
    if not extensions:
        return {}
    if len(extensions) != 1:
        raise ValueError(f"{device_name}: duplicate NetAudio preset extension")
    extension = extensions[0]
    try:
        version = int(extension.get("schema_version", ""))
    except ValueError as exception:
        raise ValueError(f"{device_name}: invalid NetAudio preset schema version") from exception
    if version > PRESET_SCHEMA_VERSION or version < 1:
        raise ValueError(f"{device_name}: unsupported NetAudio preset schema version {version}")
    content = extension.findtext(PRESET_EXTENSION_CONTENT_TAG)
    if content is None:
        raise ValueError(f"{device_name}: NetAudio preset extension has no JSON configuration")
    try:
        value = json.loads(content)
    except json.JSONDecodeError as exception:
        raise ValueError(f"{device_name}: invalid NetAudio preset extension JSON") from exception
    if not isinstance(value, dict):
        raise ValueError(f"{device_name}: NetAudio preset extension JSON must be an object")
    return normalize_device_config(value)


def _parse_receiver_subscriptions(
    receiver_channel_elements: list[ET.Element], device_name: str
) -> dict[int, dict[str, str] | None]:
    receiver_subscriptions: dict[int, dict[str, str] | None] = {}
    for receiver_channel_element in receiver_channel_elements:
        receiver_channel_number = _parse_receiver_channel_number(receiver_channel_element, device_name)
        if receiver_channel_number in receiver_subscriptions:
            raise ValueError(f"{device_name}: duplicate receiver channel danteId {receiver_channel_number}")
        subscribed_channel = (receiver_channel_element.findtext("subscribed_channel") or "").strip()
        subscribed_device = (receiver_channel_element.findtext("subscribed_device") or "").strip()
        if subscribed_device and not subscribed_channel:
            raise ValueError(
                f"{device_name}: receiver channel {receiver_channel_number} has a subscribed device without a channel"
            )
        if subscribed_channel:
            receiver_subscriptions[receiver_channel_number] = {
                "tx_channel": subscribed_channel,
                "tx_device": subscribed_device or ".",
            }
        else:
            receiver_subscriptions[receiver_channel_number] = None
    return receiver_subscriptions


def _parse_receiver_channel_number(receiver_channel_element: ET.Element, device_name: str) -> int:
    dante_identifier = receiver_channel_element.get("danteId")
    if dante_identifier is None:
        raise ValueError(f"{device_name}: receiver channel is missing danteId")
    try:
        receiver_channel_number = int(dante_identifier)
    except ValueError as exception:
        raise ValueError(f"{device_name}: receiver channel danteId must be an integer") from exception
    if not 1 <= receiver_channel_number <= 0xFFFF:
        raise ValueError(f"{device_name}: receiver channel danteId must be from 1 through 65535")
    return receiver_channel_number
