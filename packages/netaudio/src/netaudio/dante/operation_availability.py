from __future__ import annotations

from dataclasses import asdict, dataclass

from netaudio.common.managed_api import MANAGED_PERMISSION_OPERATIONS

UNVERIFIED_REASONS = frozenset({"capability_unknown", "lock_state_unknown", "update_mode_unknown"})
WRITABLE_UPDATE_MODES = frozenset({1, 2})
PULLUP_HOST_DISABLED_MASK = 0x0000_0001


@dataclass(frozen=True)
class OperationAvailability:
    supported: bool | None
    readable: bool
    writable: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict:
        value = asdict(self)
        value["reasons"] = list(self.reasons)
        return value


_CAPABILITY_FIELDS = {
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


def operation_availability(device, operation: str, requested_value=None) -> OperationAvailability:
    if operation not in _CAPABILITY_FIELDS:
        raise ValueError(f"unknown operation {operation!r}")
    if operation == "redundancy":
        from netaudio.dante.network_configuration import advertised_redundancy_support

        supported = advertised_redundancy_support(device)
    else:
        supported = getattr(device, _CAPABILITY_FIELDS[operation], None)
    reasons = []
    readable = _readable(device, operation)

    if supported is not True:
        reasons.append("capability_unknown" if supported is None else "unsupported")
    read_only_field = _READ_ONLY_FIELDS.get(operation)
    if operation == "redundancy":
        from netaudio.dante.network_configuration import (
            redundancy_read_only_applicable,
            redundancy_serializer_cohort,
            redundancy_transport_available,
            reported_redundancy_read_only,
            switch_configuration_choice,
        )

        read_only = reported_redundancy_read_only(device)
        if read_only is True:
            reasons.append("read_only")
        elif read_only is None and redundancy_read_only_applicable(device):
            reasons.append("read_only_unknown")

        state = getattr(device, "dante_redundancy", None)
        if not isinstance(state, dict) or state.get("current") is None or state.get("configured") is None:
            reasons.append("state_unavailable")
        elif state.get("state_fresh") is not True:
            reasons.append("state_stale")
        choices = state.get("available_modes") if isinstance(state, dict) else None
        if not isinstance(choices, list):
            reasons.append("available_modes_unknown")
        elif state.get("available_modes_fresh") is not True:
            reasons.append("available_modes_stale")
        else:
            known_modes = [choice.get("mode") for choice in choices if isinstance(choice, dict)]
            if not known_modes:
                reasons.append("available_modes_empty")
            if requested_value is not None and requested_value not in known_modes:
                reasons.append("requested_mode_not_advertised")
        serializer_cohort = redundancy_serializer_cohort(device)
        if serializer_cohort is None:
            reasons.append("protocol_unsupported")
        elif (
            requested_value is not None
            and serializer_cohort == "switch_configuration_choice_table"
            and switch_configuration_choice(device, requested_value) is None
        ):
            reasons.append("serializer_unavailable")
        if not redundancy_transport_available(device):
            reasons.append("transport_unavailable")
    elif read_only_field is not None and getattr(device, read_only_field, None) is True:
        reasons.append("read_only")

    component = _COMPONENT_FIELDS.get(operation)
    if component is not None:
        _, mode_field, choices_field = component
        mode = getattr(device, mode_field, None)
        if mode not in WRITABLE_UPDATE_MODES:
            reasons.append("fixed" if mode == 0 else "update_mode_unknown")
        choices = getattr(device, choices_field, None)
        if requested_value is not None and choices and requested_value not in choices:
            reasons.append("value_not_advertised")
    if operation == "sample_rate_pullup" and (
        (getattr(device, "sample_rate_pullup_flags", None) or 0) & PULLUP_HOST_DISABLED_MASK
    ):
        reasons.append("host_disabled")

    if operation == "codec_control" and getattr(device, "gain_adapter", None) is None:
        reasons.append("no_device_adapter")

    if operation not in {"identify", "locking"}:
        lock_state = getattr(device, "is_locked", None)
        if lock_state is True:
            reasons.append("device_locked")
        elif lock_state is None:
            reasons.append("lock_state_unknown")

    if operation == "locking":
        if getattr(device, "is_locked", None) is None:
            reasons.append("lock_state_unknown")

    if getattr(device, "requires_managed_control", False):
        permissions = getattr(device, "managed_operation_permissions", None)
        if not isinstance(permissions, dict) or operation not in permissions:
            reasons.append("managed_permission_missing")
        elif permissions[operation] is not True:
            reasons.append("managed_permission_denied")
        if operation == "locking":
            reasons.append("managed_transport_unavailable")

    return OperationAvailability(
        supported=supported,
        readable=readable,
        writable=not reasons,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def operation_availability_map(device) -> dict[str, dict]:
    return {name: operation_availability(device, name).to_dict() for name in _CAPABILITY_FIELDS}


def probe_supported(device, operation: str) -> bool:
    return operation_availability(device, operation).supported is not False


def require_writable(device, operation: str, requested_value=None) -> None:
    availability = operation_availability(device, operation, requested_value)
    if availability.writable:
        return
    if operation != "redundancy" and all(reason in UNVERIFIED_REASONS for reason in availability.reasons):
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
