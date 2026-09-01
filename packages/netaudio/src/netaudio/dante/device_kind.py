from __future__ import annotations

import ipaddress

DEVICE_KIND_EMULATED = "emulated"
DEVICE_KIND_HARDWARE = "hardware"
DEVICE_KIND_VIRTUAL = "virtual"

EMULATED_DEVICE_NAME_PREFIX = "netaudio-page-probe"
EMULATED_PRIVATE_NETWORK_MAC_PREFIX = "0000c0a8"
QEMU_ORGANIZATIONALLY_UNIQUE_IDENTIFIER = "525400"
VIRTUAL_DEVICE_MANUFACTURER = "netaudio"
VIRTUAL_DEVICE_MODEL = "netaudio"


def _normalized_mac_address(mac_address) -> str:
    if not mac_address:
        return ""
    raw = str(mac_address).replace(":", "").replace("-", "").lower()
    if len(raw) == 16 and raw[6:10] == "fffe":
        return raw[:6] + raw[10:]
    if len(raw) == 16 and raw.endswith("0000"):
        return raw[:12]
    return raw


def _packed_ipv4_hexadecimal(ipv4) -> str | None:
    if ipv4 is None:
        return None
    try:
        return ipaddress.IPv4Address(str(ipv4)).packed.hex()
    except (ipaddress.AddressValueError, ValueError):
        return None


def mac_address_encodes_ipv4(mac_address, ipv4=None) -> bool:
    normalized = _normalized_mac_address(mac_address)
    if len(normalized) != 12 or not normalized.startswith("0000"):
        return False
    if normalized.startswith(EMULATED_PRIVATE_NETWORK_MAC_PREFIX):
        return True
    packed = _packed_ipv4_hexadecimal(ipv4)
    return packed is not None and normalized[4:] == packed


def _identity_values(device) -> tuple[str, ...]:
    return tuple(
        str(value).strip().casefold()
        for value in (
            getattr(device, "dante_model", None),
            getattr(device, "manufacturer", None),
            getattr(device, "manufacturer_mdns", None),
            getattr(device, "model", None),
            getattr(device, "model_id", None),
        )
        if value
    )


def _names(device) -> tuple[str, ...]:
    return tuple(
        str(value).strip().casefold()
        for value in (getattr(device, "name", None), getattr(device, "server_name", None))
        if value
    )


def device_kind(device) -> str:
    identity_values = _identity_values(device)
    if VIRTUAL_DEVICE_MODEL.casefold() in identity_values or VIRTUAL_DEVICE_MANUFACTURER.casefold() in identity_values:
        return DEVICE_KIND_VIRTUAL
    mac_address = _normalized_mac_address(getattr(device, "mac_address", None))
    if mac_address.startswith(QEMU_ORGANIZATIONALLY_UNIQUE_IDENTIFIER):
        return DEVICE_KIND_EMULATED
    if mac_address_encodes_ipv4(mac_address, getattr(device, "ipv4", None)):
        return DEVICE_KIND_EMULATED
    if any(name.startswith(EMULATED_DEVICE_NAME_PREFIX) for name in _names(device)):
        return DEVICE_KIND_EMULATED
    return DEVICE_KIND_HARDWARE
