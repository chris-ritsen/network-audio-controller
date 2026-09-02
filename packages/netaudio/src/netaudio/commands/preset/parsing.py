from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from netaudio.dante.latency import MICROSECONDS_PER_MILLISECOND

INTERFACE_MODES = ("dynamic", "dhcp", "static")
STATIC_INTERFACE_FIELDS = (
    ("dns", "dns_server"),
    ("gateway", "gateway"),
    ("ip", "ip_address"),
    ("netmask", "netmask"),
)


def parse_preset(preset_path: Path) -> tuple[str, dict[str, dict[str, Any]]]:
    root = ET.parse(preset_path).getroot()
    preset_name = root.findtext("name", "unknown")
    preset_devices: dict[str, dict[str, Any]] = {}
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
    device_config.update(_parse_interface_fields(device_element))
    transmitter_channel_names = _parse_transmitter_channel_names(device_element)
    if transmitter_channel_names:
        device_config["transmitter_channel_names"] = transmitter_channel_names
    receiver_channel_elements = device_element.findall("rxchannel")
    if receiver_channel_elements:
        device_config["rx_subscriptions"] = _parse_receiver_subscriptions(receiver_channel_elements, device_name)
    return device_config


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


def _parse_interface_fields(device_element: ET.Element) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    interface_elements = device_element.findall("interface")
    if len(interface_elements) > 1:
        fields["additional_interfaces"] = len(interface_elements) - 1
    if not interface_elements:
        return fields
    address_element = interface_elements[0].find("ipv4_address")
    if address_element is None:
        return fields
    mode = address_element.get("mode", "dynamic")
    fields["interface_mode"] = mode
    if mode == "static":
        fields["ip_address"] = address_element.findtext("ip_address", "")
        fields["netmask"] = address_element.findtext("subnet_mask", "")
        fields["gateway"] = address_element.findtext("gateway", "")
        fields["dns_server"] = address_element.findtext("dns_server", "")
    return fields


def _parse_transmitter_channel_names(device_element: ET.Element) -> dict[int, str]:
    transmitter_channel_names: dict[int, str] = {}
    for transmitter_element in device_element.findall("txchannel"):
        dante_identifier = transmitter_element.get("danteId")
        label = transmitter_element.findtext("label", "")
        if dante_identifier and label:
            transmitter_channel_names[int(dante_identifier)] = label
    return transmitter_channel_names


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
