from __future__ import annotations

from netaudio import core
from netaudio.core import _types
from netaudio.dante.const import SERVICE_ARC


class ArcProtocolError(RuntimeError):
    pass


def _advertised_facts(device):
    managed = bool(getattr(device, "requires_managed_control", False))
    version = None
    services = getattr(device, "services", None)

    if not managed and isinstance(services, dict):
        for service in services.values():
            if not isinstance(service, dict) or service.get("type") != SERVICE_ARC:
                continue

            properties = service.get("properties")

            if isinstance(properties, dict):
                version = properties.get("arcp_vers")

            break

    return version, managed


def arc_protocol_for_device(device) -> _types.ArcProtocol | None:
    version, managed = _advertised_facts(device)

    try:
        return core.arc_protocol(version, managed=managed)
    except (core.NetaudioCoreError, ValueError) as error:
        raise ArcProtocolError("unsupported ARC protocol version") from error


def flow_inventory_protocol_identifier_for_device(device) -> int | None:
    version, managed = _advertised_facts(device)

    try:
        return core.flow_inventory_protocol(
            {
                "observed": getattr(device, "flow_protocol_id", None),
                "version": version,
                "managed": managed,
            }
        )
    except (core.NetaudioCoreError, ValueError) as error:
        raise ArcProtocolError("unsupported flow inventory protocol") from error


def require_arc_protocol_for_device(device) -> _types.ArcProtocol:
    protocol = arc_protocol_for_device(device)

    if protocol is None:
        raise ArcProtocolError("device has no ARC service metadata")

    return protocol


def advertised_arc_protocol_identifier_for_device(device) -> int | None:
    protocol = arc_protocol_for_device(device)

    return protocol["protocol_id"] if protocol is not None else None


def modern_arc_protocol_identifier_for_device(device) -> int:
    protocol = require_arc_protocol_for_device(device)

    if not protocol["modern_channel_inventory"]:
        raise ArcProtocolError("device protocol does not support modern channel inventory")

    return protocol["protocol_id"]
