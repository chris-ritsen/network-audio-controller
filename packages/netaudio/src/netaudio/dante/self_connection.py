from __future__ import annotations

from collections.abc import Iterable


class SelfConnectionCapabilityError(RuntimeError):
    """A self-connection request cannot pass capability preflight."""


class SelfConnectionUnsupportedError(SelfConnectionCapabilityError):
    pass


class SelfConnectionCapabilityUnavailableError(SelfConnectionCapabilityError):
    pass


def receiver_self_connection_support(channels: Iterable[object]) -> str:
    values = [
        channel.get("can_subscribe_self") if isinstance(channel, dict) else getattr(channel, "can_subscribe_self", None)
        for channel in channels
    ]
    conclusive = {value for value in values if isinstance(value, bool)}
    has_unknown = any(not isinstance(value, bool) for value in values)
    if not conclusive:
        return "unknown"
    if has_unknown:
        return "partial"
    if conclusive == {True}:
        return "supported"
    if conclusive == {False}:
        return "unsupported"
    return "mixed"


def normalized_device_identities(device: object) -> frozenset[tuple[str, str]]:
    identities: set[tuple[str, str]] = set()
    for namespace, attribute in (
        ("inventory", "inventory_id"),
        ("managed", "ddm_device_id"),
        ("service", "server_name"),
    ):
        value = getattr(device, attribute, None)
        if isinstance(value, str) and value:
            identities.add((namespace, value.casefold()))

    mac_values = [getattr(device, "mac_address", None)]
    for interface in getattr(device, "interfaces", None) or ():
        if isinstance(interface, dict):
            mac_values.append(interface.get("mac_address"))
    for value in mac_values:
        if not isinstance(value, str):
            continue
        normalized = value.replace(":", "").replace("-", "").casefold()
        if len(normalized) == 16:
            if normalized[6:10] == "fffe":
                normalized = f"{normalized[:6]}{normalized[10:]}"
            elif normalized.endswith("0000"):
                normalized = normalized[:12]
        if len(normalized) in {12, 16} and all(character in "0123456789abcdef" for character in normalized):
            identities.add(("mac", normalized))
    return frozenset(identities)


def same_canonical_device(first: object, second: object) -> bool:
    if first is second:
        return True
    first_identities = normalized_device_identities(first)
    second_identities = normalized_device_identities(second)
    return bool(first_identities and first_identities.intersection(second_identities))


def resolve_device_reference(devices: Iterable[object], reference: object) -> object | None:
    if not isinstance(reference, str) or not reference:
        return reference if reference is not None else None
    normalized = reference.casefold()
    matches = []
    for device in devices:
        values = (
            getattr(device, "server_name", None),
            getattr(device, "inventory_id", None),
            getattr(device, "ddm_device_id", None),
            getattr(device, "name", None),
            str(getattr(device, "ipv4", "") or ""),
        )
        if any(isinstance(value, str) and value.casefold() == normalized for value in values):
            matches.append(device)
    if not matches:
        return None
    first = matches[0]
    return first if all(same_canonical_device(first, candidate) for candidate in matches[1:]) else None


def is_self_connection_request(receiver: object, transmitter: object, devices: Iterable[object]) -> bool:
    if transmitter == ".":
        return True
    resolved = resolve_device_reference(devices, transmitter)
    return resolved is not None and same_canonical_device(receiver, resolved)
