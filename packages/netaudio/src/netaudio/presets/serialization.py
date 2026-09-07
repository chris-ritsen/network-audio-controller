from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Collection

from netaudio import DanteDevice
from netaudio.dante.latency import milliseconds_to_microseconds


def _hex_encode(text: str, pad_to: int = 16) -> str:
    import binascii

    encoded = binascii.hexlify(text.encode()).decode().upper()
    return encoded.ljust(pad_to, "0")


def _append_preset_identity(element: ET.Element, device: DanteDevice) -> None:
    _sub_text(element, "name", device.name or "")
    _sub_text(element, "default_name", device.server_name.replace(".local.", "") if device.server_name else "")

    instance_id = ET.SubElement(element, "instance_id")
    mac = (device.mac_address or "").replace(":", "").upper()
    if mac:
        if len(mac) == 12:
            mac = mac[:6] + "FFFE" + mac[6:]
        _sub_text(instance_id, "device_id", mac)
    _sub_text(instance_id, "process_id", "0")

    if device.manufacturer:
        _sub_text(element, "manufacturer_id", _hex_encode(device.manufacturer))
        _sub_text(element, "manufacturer_name", device.manufacturer)

    dante_model = device.dante_model or device.model_id or ""
    model_id = device.model_id or ""

    if model_id:
        model_id_hex = _hex_encode(model_id)
        _sub_text(element, "model_id", model_id_hex)
        _sub_text(element, "model_name", dante_model or model_id)
        if device.product_version:
            _sub_text(element, "model_version", device.product_version)
        _sub_text(element, "device_type", model_id_hex)
        _sub_text(element, "device_type_string", model_id)

    _sub_text(element, "friendly_name", device.name or "")


def _append_preset_audio_settings(element: ET.Element, device: DanteDevice) -> None:
    if device.preferred_leader is not None:
        ET.SubElement(element, "preferred_master", value=str(device.preferred_leader).lower())

    if device.sample_rate:
        _sub_text(element, "samplerate", str(device.sample_rate))

    if device.encoding:
        _sub_text(element, "encoding", str(device.encoding))

    latency = device.configured_latency
    if latency is None and device.active_latency is None:
        latency = device.latency
    if latency:
        _sub_text(element, "unicast_latency", str(milliseconds_to_microseconds(latency)))


def _append_preset_interfaces(element: ET.Element, device: DanteDevice) -> None:
    for index, iface in enumerate(device.interfaces or []):
        iface_element = ET.SubElement(element, "interface", network=str(index))
        mode = iface.get("mode", "")
        if mode == "static":
            ip_element = ET.SubElement(iface_element, "ipv4_address", mode="static")
            _sub_text(ip_element, "ip_address", iface.get("ip_address", ""))
            _sub_text(ip_element, "subnet_mask", iface.get("netmask", ""))
            _sub_text(ip_element, "gateway", iface.get("gateway", ""))
            _sub_text(ip_element, "dns_server", iface.get("dns_server", ""))
        else:
            ET.SubElement(iface_element, "ipv4_address", mode="dynamic")


def _append_preset_channels(element: ET.Element, device: DanteDevice) -> None:
    for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number):
        tx_element = ET.SubElement(element, "txchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(tx_element, "label", channel.friendly_name or channel.name)

    for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number):
        rx_element = ET.SubElement(element, "rxchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(rx_element, "name", channel.friendly_name or channel.name)

        for subscription in device.subscriptions:
            if subscription.rx_channel_name == channel.name or subscription.rx_channel_name == channel.friendly_name:
                if subscription.tx_channel_name:
                    _sub_text(rx_element, "subscribed_channel", subscription.tx_channel_name)
                if subscription.tx_device_name:
                    _sub_text(rx_element, "subscribed_device", subscription.tx_device_name)
                break


def _device_to_preset_xml(device: DanteDevice, sections: Collection[str]) -> ET.Element:
    element = ET.Element("device")
    _append_preset_identity(element, device)
    if "audio" in sections:
        _append_preset_audio_settings(element, device)
    if "network" in sections:
        _append_preset_interfaces(element, device)
    if "routing" in sections:
        _append_preset_channels(element, device)
    return element


def _sub_text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    child = ET.SubElement(parent, tag)
    child.text = text
    return child


def format_devices_xml(
    devices: dict[str, DanteDevice], preset_name: str = "netaudio", *, sections: Collection[str] | None = None
) -> str:
    sections = {"routing", "audio", "network"} if sections is None else set(sections)
    if not sections or sections - {"routing", "audio", "network"}:
        raise ValueError("preset sections must be routing, audio, or network")
    root = ET.Element("preset", version="2.1.0")
    _sub_text(root, "name", preset_name)
    _sub_text(root, "description", "Dante Controller preset")

    for server_name, device in sorted(devices.items(), key=lambda item: item[1].name or item[0]):
        root.append(_device_to_preset_xml(device, sections))

    ET.indent(root, space="    ")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(root, encoding="unicode")
