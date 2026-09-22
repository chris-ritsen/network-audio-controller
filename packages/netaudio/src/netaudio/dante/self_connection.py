from __future__ import annotations

from collections.abc import Iterable

from netaudio.core import canonical_device_mac, receiver_self_connection_capabilities


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
    return receiver_self_connection_capabilities(
        {
            "authority": "observed",
            "channels": [{"direct": value, "managed": None, "managed_fresh": False} for value in values],
        }
    )["support"]


def self_connection_capability(direct, managed, managed_fresh, *, authority):
    return receiver_self_connection_capabilities(
        {
            "authority": authority,
            "channels": [{"direct": direct, "managed": managed, "managed_fresh": managed_fresh is True}],
        }
    )["channels"][0]


def apply_self_connection_capability(channel, *, authority):
    result = self_connection_capability(
        channel.direct_can_subscribe_self,
        channel.managed_can_subscribe_self,
        channel.managed_can_subscribe_self_fresh,
        authority=authority,
    )
    channel.can_subscribe_self = result["supported"]
    channel.can_subscribe_self_conflict = True if result["conflict"] else None


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
        normalized = canonical_device_mac(value)

        if normalized is not None:
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
