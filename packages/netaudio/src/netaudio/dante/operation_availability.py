from __future__ import annotations

from dataclasses import asdict, dataclass

from netaudio.common.managed_api import MANAGED_PERMISSION_OPERATIONS
from netaudio import core
from netaudio.core import _requests


@dataclass(frozen=True)
class OperationAvailability:
    supported: bool | None
    readable: bool
    writable: bool
    reasons: tuple[str, ...]
    write_permitted: bool
    read_only: bool | None

    def to_dict(self) -> dict:
        value = asdict(self)
        value.pop("write_permitted")
        value.pop("read_only")
        value["reasons"] = list(self.reasons)
        return value


_CAPABILITY_FIELDS: dict[_requests.Operation, str] = {
    "identify": "identify_supported",
    "sample_rate": "sample_rate_configuration_supported",
    "encoding": "encoding_configuration_supported",
    "sample_rate_pullup": "sample_rate_pullup_configuration_supported",
    "aes67": "aes67_configuration_supported",
    "static_ipv4": "static_ipv4_configuration_supported",
    "redundancy": "switch_redundancy_supported",
    "codec_control": "generic_codec_control_supported",
    "locking": "device_locking_supported",
}

assert frozenset(_CAPABILITY_FIELDS) == MANAGED_PERMISSION_OPERATIONS

_COMPONENT_FIELDS = {
    "sample_rate": ("sample_rate", "sample_rate_update_mode", "supported_sample_rates"),
    "encoding": ("encoding", "encoding_update_mode", "supported_encodings"),
    "sample_rate_pullup": (
        "sample_rate_pullup_raw_value",
        "sample_rate_pullup_update_mode",
        "supported_sample_rate_pullup_raw_values",
    ),
}

_READ_ONLY_FIELDS = {
    "static_ipv4": "static_ipv4_configuration_read_only",
    "redundancy": "switch_redundancy_read_only",
}


def operation_availability(device, operation: _requests.Operation, requested_value=None) -> OperationAvailability:
    if operation not in _CAPABILITY_FIELDS:
        raise ValueError(f"unknown operation {operation!r}")

    read_only_field = _READ_ONLY_FIELDS.get(operation)
    facts: _requests.AvailabilityRequest = {
        "operation": operation,
        "supported": getattr(device, _CAPABILITY_FIELDS[operation], None),
        "readable": _readable(device, operation),
        "read_only": getattr(device, read_only_field, None) if read_only_field else None,
        "locked": getattr(device, "is_locked", None),
        "transport_available": True,
        "has_adapter": getattr(device, "gain_adapter", None) is not None,
        "managed": bool(getattr(device, "requires_managed_control", False)),
    }

    if operation == "redundancy":
        from netaudio.dante.network_configuration import redundancy_transport_available

        facts.update(
            supported_source=getattr(device, "redundancy_advertised_support_source", None),
            read_only_source=getattr(device, "redundancy_read_only_source", None),
            redundancy={"state": getattr(device, "dante_redundancy", None), "mode": requested_value},
            transport_available=redundancy_transport_available(device),
        )

    component = _COMPONENT_FIELDS.get(operation)

    if component is not None:
        _, mode_field, choices_field = component
        facts["audio"] = {
            "update_mode": getattr(device, mode_field, None),
            "available_values": getattr(device, choices_field, None),
            "requested_value": requested_value,
            "host_disabled": (
                getattr(device, "sample_rate_pullup_host_disabled", None) if operation == "sample_rate_pullup" else None
            ),
        }

    permissions = getattr(device, "managed_operation_permissions", None)
    facts["permission"] = permissions.get(operation) if isinstance(permissions, dict) else None
    result = core.operation_availability(facts)

    return OperationAvailability(
        supported=result["supported"],
        readable=result["readable"],
        writable=result["writable"],
        reasons=tuple(result["reasons"]),
        write_permitted=result["write_permitted"],
        read_only=result["read_only"],
    )


def metering_availability(device) -> OperationAvailability:
    if getattr(device, "requires_managed_control", False):
        return OperationAvailability(
            supported=False,
            readable=False,
            writable=False,
            reasons=("managed_metering_not_implemented",),
            write_permitted=False,
            read_only=None,
        )
    return OperationAvailability(
        supported=True, readable=True, writable=True, reasons=(), write_permitted=True, read_only=None
    )


def operation_availability_map(device) -> dict[str, dict]:
    availability = {name: operation_availability(device, name).to_dict() for name in _CAPABILITY_FIELDS}
    availability["metering"] = metering_availability(device).to_dict()
    return availability


def probe_supported(device, operation: _requests.Operation) -> bool:
    return operation_availability(device, operation).supported is not False


def require_writable(device, operation: _requests.Operation, requested_value=None) -> None:
    availability = operation_availability(device, operation, requested_value)

    if availability.write_permitted:
        return

    details = ", ".join(availability.reasons)
    raise RuntimeError(f"{operation.replace('_', ' ')} is not writable: {details}")


def _readable(device, operation: str) -> bool:
    if operation in _COMPONENT_FIELDS:
        current_field = _COMPONENT_FIELDS[operation][0]
        return getattr(device, current_field, None) is not None

    if operation == "aes67":
        return (
            getattr(device, "aes67_current", None) is not None or getattr(device, "aes67_configured", None) is not None
        )

    if operation == "static_ipv4":
        return getattr(device, "interfaces", None) is not None

    if operation == "redundancy":
        return getattr(device, "dante_redundancy", None) is not None

    if operation == "codec_control":
        return getattr(device, "codec_parameters", None) is not None

    if operation == "locking":
        return getattr(device, "is_locked", None) is not None

    return False
