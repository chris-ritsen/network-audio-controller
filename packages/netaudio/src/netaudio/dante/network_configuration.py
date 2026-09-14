from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from ipaddress import IPv4Address, IPv4Network
import time
from typing import Any

from netaudio.dante.operation_availability import operation_availability, require_writable


class NetworkConfigurationError(RuntimeError):
    pass


class NetworkConfigurationUnverified(NetworkConfigurationError):
    def __init__(self, message: str, evidence: dict | None = None):
        super().__init__(message)
        self.evidence = evidence


def validate_interface_configuration(mode, configuration=None):
    if mode == "dhcp":
        return {"mode": "dynamic"}
    if mode != "static":
        raise ValueError("mode must be 'dhcp' or 'static'")
    configuration = configuration or {}
    result = {"mode": "static"}
    for key in ("ip_address", "netmask", "dns_server", "gateway"):
        value = configuration.get(key)
        if key in {"dns_server", "gateway"} and value in (None, ""):
            value = "0.0.0.0"
        if not isinstance(value, str):
            raise ValueError(f"{key} must be an IPv4 address")
        address = IPv4Address(value)
        if key != "netmask" and (address.is_multicast or address.is_loopback or int(address) == 0xFFFFFFFF):
            raise ValueError(f"{key} must be a unicast IPv4 address")
        result[key] = str(address)
    if result["ip_address"] == "0.0.0.0":
        raise ValueError("ip_address must not be unspecified")
    mask = result["netmask"]
    network = IPv4Network(f"{result['ip_address']}/{mask}", strict=False)
    if str(network.netmask) != mask or network.prefixlen == 0:
        raise ValueError("netmask must be a contiguous, nonzero IPv4 subnet mask")
    if network.prefixlen < 31 and IPv4Address(result["ip_address"]) in (
        network.network_address,
        network.broadcast_address,
    ):
        raise ValueError("ip_address must be a host address")
    return result


async def set_interface(application, device, mode, configuration=None, *, interface="primary", timeout=2.0):
    expected = validate_interface_configuration(mode, configuration)
    require_writable(device, "static_ipv4")
    if not isinstance(interface, str) or interface not in {"primary", "secondary"}:
        raise ValueError("interface must be 'primary' or 'secondary'")
    async with device.topology_mutation_lock:
        before = deepcopy(await application.probe_interface_status(device, timeout=timeout))
        before_redundancy = deepcopy(device.dante_redundancy)
        selected = interface_configuration(before, interface)
        if mode not in interface_configuration_modes(selected, device):
            raise NetworkConfigurationError("Network configuration is unavailable for this interface")
        if all(selected["configured"].get(key) == value for key, value in expected.items()):
            return before
        try:
            if mode == "dhcp":
                await application.send_set_interface_dhcp(
                    device, interface=interface, record_protocol_identifier=device.interface_status_protocol
                )
            else:
                await application.send_set_interface_static(
                    device,
                    expected["ip_address"],
                    expected["netmask"],
                    expected["dns_server"],
                    expected["gateway"],
                    interface=interface,
                    record_protocol_identifier=device.interface_status_protocol,
                )
            after = await application.probe_interface_status(device, timeout=timeout)
            configured = interface_configuration(after, interface).get("configured") or {}
            if not all(configured.get(key) == value for key, value in expected.items()):
                raise NetworkConfigurationError("configured value did not match")
            if interface_configuration_context(before, interface) != interface_configuration_context(after, interface):
                raise NetworkConfigurationError("Other interface settings changed during verification")
            if device.dante_redundancy != before_redundancy:
                raise NetworkConfigurationError("Dante redundancy changed during verification")
        except (OSError, RuntimeError, TimeoutError) as exception:
            raise NetworkConfigurationUnverified(
                "Network change was requested, but could not be verified; no retry or reboot was sent"
            ) from exception
        return after


def interface_configuration_context(interfaces, interface):
    context = deepcopy(interfaces)
    selected = interface_configuration(context, interface)
    selected.pop("configured", None)
    selected.pop("reboot_required", None)
    return context


def interface_configuration(interfaces, interface: str) -> dict:
    if not isinstance(interface, str) or interface not in {"primary", "secondary"}:
        raise ValueError("interface must be 'primary' or 'secondary'")
    if not isinstance(interfaces, list) or any(not isinstance(entry, dict) for entry in interfaces):
        raise NetworkConfigurationError(f"{interface.capitalize()} interface is unavailable")
    matches = [entry for entry in interfaces or [] if entry.get("interface") == interface]
    if len(matches) != 1:
        raise NetworkConfigurationError(f"{interface.capitalize()} interface is unavailable")
    return matches[0]


def network_snapshot(device) -> dict:
    redundancy = redundancy_snapshot(device)
    return {
        "interfaces": deepcopy(device.interfaces or []),
        "interface_configuration_modes": network_configuration_modes(device),
        "redundancy": redundancy,
        "link_speed_mbps": device.link_speed_mbps,
        "reboot_required": device.interface_reboot_required or redundancy["reboot_required"],
        "operation_availability": {
            operation: operation_availability(device, operation).to_dict()
            for operation in ("static_ipv4", "redundancy")
        },
    }


def network_configuration_modes(device) -> dict:
    return {
        entry["interface"]: interface_configuration_modes(entry, device)
        for entry in device.interfaces or []
        if entry.get("interface") in {"primary", "secondary"} and entry.get("configured") is not None
    }


def interface_configuration_modes(entry, device) -> list[str]:
    if (
        isinstance(entry, dict)
        and entry.get("configured") is not None
        and operation_availability(device, "static_ipv4").writable
    ):
        return ["dhcp", "static"]
    return []


REDUNDANCY_MODE_LABELS = {
    "Redundant": "redundant",
    "Split/Redundant": "split_redundant",
    "Switched": "switched",
}

REDUNDANCY_FLAG_PROTOCOLS = frozenset({0x0724})


@dataclass(frozen=True)
class RedundancyMutationResult:
    state: str
    requested_mode: str
    mutation_sent: bool
    request_acknowledgement: dict[str, Any] | None
    device_side_confirmation: bool | None
    effective_state_confirmation: bool | None
    effective_readback: dict[str, Any]
    persistence_confirmation: bool | None
    reboot_evidence: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _reported_source(source) -> bool:
    return isinstance(source, dict) and source.get("fresh") is True and source.get("field_reported") is True


def advertised_redundancy_support(device) -> bool | None:
    source = getattr(device, "redundancy_advertised_support_source", None)
    value = getattr(device, "switch_redundancy_supported", None)
    if not _reported_source(source) or not isinstance(value, bool):
        return None
    return value


def reported_redundancy_read_only(device) -> bool | None:
    source = getattr(device, "redundancy_read_only_source", None)
    value = getattr(device, "switch_redundancy_read_only", None)
    if not _reported_source(source) or not isinstance(value, bool):
        return None
    return value


def redundancy_read_only_applicable(device) -> bool:
    source = getattr(device, "redundancy_read_only_source", None)
    support_source = getattr(device, "redundancy_advertised_support_source", None)
    version = source.get("record_protocol_version") if isinstance(source, dict) else None
    if not isinstance(version, int) and isinstance(support_source, dict):
        version = support_source.get("record_protocol_version")
    if isinstance(version, int):
        return version >= 0x070A
    if isinstance(source, dict):
        return source.get("field_reported") is True
    return False


def _mode_observation(code: int | None, choices: list[dict]) -> dict:
    choice = next((entry for entry in choices if entry.get("code") == code), None)
    if choice is None:
        return {"status": "unknown_raw" if code is not None else "unavailable", "mode": None, "raw_code": code}
    return {
        "status": "known" if choice.get("mode") is not None else "unknown_raw",
        "mode": choice.get("mode"),
        "raw_code": code,
        "raw_label": choice.get("label"),
        "raw_choice_hexadecimal": choice.get("raw_choice_hexadecimal"),
    }


def _flag_mode_observation(flags: int | None, mask: int, mode: str | None) -> dict:
    return {
        "status": "known" if mode is not None and flags is not None else "unavailable",
        "mode": mode,
        "raw_flags": flags,
        "flag_mask": mask,
        "flag_set": bool(flags & mask) if flags is not None else None,
    }


def switch_configuration_fields(parsed: dict) -> dict:
    choices = [
        {
            **choice,
            "mode": REDUNDANCY_MODE_LABELS.get(choice["label"]),
        }
        for choice in parsed.get("choices") or []
    ]
    mode_codes = parsed.get("mode_codes_at_record_offsets_20_and_22") or [None, None]
    parsed_state = parsed.get("redundancy") or {}
    state = {
        "current": parsed_state.get("current"),
        "configured": parsed_state.get("configured"),
        "supported": [choice["mode"] for choice in choices if choice["mode"] is not None],
        "reboot_required": parsed_state.get("reboot_required") is True,
        "current_mode_evidence": _mode_observation(mode_codes[0], choices),
        "configured_mode_evidence": _mode_observation(mode_codes[1], choices),
        "available_modes": choices,
        "available_modes_source": "switch_configuration_choice_table",
        "available_modes_fresh": True,
        "state_source": {
            "kind": "switch_configuration_status",
            "opcode": 0x0014,
            "record_protocol_identifier": parsed.get("record_protocol_identifier"),
            "cohort": "choice_table",
            "observed_at_unix": time.time(),
        },
        "state_fresh": True,
        "raw_record_hexadecimal": parsed.get("raw_record_hexadecimal"),
    }
    return {"dante_redundancy": state, "switch_configuration_choices": choices}


def switch_configuration_choice(device, mode: str) -> int | None:
    state = getattr(device, "dante_redundancy", None)
    if isinstance(state, dict):
        if state.get("available_modes_source") != "switch_configuration_choice_table":
            return None
        choices = state.get("available_modes")
    else:
        choices = getattr(device, "switch_configuration_choices", None)
    for choice in choices or []:
        if choice.get("mode") == mode:
            code = choice.get("code")
            return code if isinstance(code, int) and not isinstance(code, bool) else None
    return None


def interface_redundancy_status(parsed: dict, device) -> dict | None:
    status = deepcopy(parsed.get("redundancy"))
    flags = parsed.get("redundancy_flags")
    if status is None and not isinstance(flags, int):
        return None
    known_flag_variant = status is not None
    if status is None:
        status = {
            "current": None,
            "configured": None,
            "supported": [],
            "reboot_required": False,
        }
    choices = deepcopy(getattr(device, "switch_configuration_choices", None))
    previous = getattr(device, "dante_redundancy", None)
    if choices is not None:
        available_modes = choices
        available_modes_source = "switch_configuration_choice_table"
        available_modes_fresh = bool(isinstance(previous, dict) and previous.get("available_modes_fresh") is True)
    elif known_flag_variant:
        available_modes = [
            {"code": 0, "label": "Switched", "mode": "switched"},
            {"code": 1, "label": "Redundant", "mode": "redundant"},
        ]
        available_modes_source = "interface_status_flag_cohort"
        available_modes_fresh = True
    else:
        available_modes = None
        available_modes_source = None
        available_modes_fresh = False
    current_evidence = (
        _flag_mode_observation(flags, 1, status.get("current"))
        if known_flag_variant
        else {"status": "unknown_raw", "mode": None, "raw_flags": flags, "known_mask": 3}
    )
    configured_evidence = (
        _flag_mode_observation(flags, 2, status.get("configured"))
        if known_flag_variant
        else {"status": "unknown_raw", "mode": None, "raw_flags": flags, "known_mask": 3}
    )
    status.update(
        {
            "current_mode_evidence": current_evidence,
            "configured_mode_evidence": configured_evidence,
            "available_modes": available_modes,
            "available_modes_source": available_modes_source,
            "available_modes_fresh": available_modes_fresh,
            "supported": [choice["mode"] for choice in available_modes or [] if choice.get("mode") is not None],
            "state_source": {
                "kind": "interface_status",
                "opcode": 0x0011,
                "record_protocol_identifier": parsed.get("record_protocol_identifier"),
                "cohort": "flag_bits_0_and_1" if known_flag_variant else "unrecognized_flag_variant",
                "observed_at_unix": time.time(),
            },
            "state_fresh": True,
            "raw_record_hexadecimal": parsed.get("raw_record_hexadecimal"),
        }
    )
    return status


def interface_inventory_completeness(device) -> str:
    interfaces = getattr(device, "interfaces", None)
    if not isinstance(interfaces, list):
        return "unknown"
    support = advertised_redundancy_support(device)
    if support is True and len(interfaces) < 2:
        return "partial"
    if support is None:
        return "unknown"
    return "complete"


def redundancy_serializer_cohort(device) -> str | None:
    state = getattr(device, "dante_redundancy", None)
    if not isinstance(state, dict):
        return None
    if state.get("available_modes_source") == "switch_configuration_choice_table":
        return "switch_configuration_choice_table"
    if (
        state.get("available_modes_source") == "interface_status_flag_cohort"
        and getattr(device, "interface_status_protocol", None) in REDUNDANCY_FLAG_PROTOCOLS
    ):
        return "interface_status_flags_0x0724"
    return None


def redundancy_transport_available(device) -> bool:
    transports = getattr(device, "control_transports", None)
    if getattr(device, "requires_managed_control", False):
        return isinstance(transports, list) and "ddm" in transports
    if isinstance(transports, list):
        return "direct" in transports
    return getattr(device, "ipv4", None) is not None


def redundancy_snapshot(device) -> dict:
    state = deepcopy(getattr(device, "dante_redundancy", None))
    state = state if isinstance(state, dict) else {}
    choices = deepcopy(state.get("available_modes"))
    if not isinstance(choices, list):
        choices = deepcopy(getattr(device, "switch_configuration_choices", None))
    if not isinstance(choices, list):
        choices = None
    interfaces = getattr(device, "interfaces", None)
    licensed = getattr(device, "licensed_redundancy_enabled", None)
    result = {
        "advertised_support": advertised_redundancy_support(device),
        "advertised_support_source": deepcopy(getattr(device, "redundancy_advertised_support_source", None)),
        "read_only": reported_redundancy_read_only(device),
        "read_only_source": deepcopy(getattr(device, "redundancy_read_only_source", None)),
        "current_mode": state.get("current"),
        "current_mode_evidence": deepcopy(state.get("current_mode_evidence"))
        or {"status": "unavailable", "mode": None},
        "configured_mode": state.get("configured"),
        "configured_mode_evidence": deepcopy(state.get("configured_mode_evidence"))
        or {"status": "unavailable", "mode": None},
        "state_source": deepcopy(state.get("state_source")),
        "state_fresh": state.get("state_fresh") is True,
        "available_modes": choices,
        "available_modes_source": state.get("available_modes_source"),
        "available_modes_fresh": state.get("available_modes_fresh") is True,
        "interface_inventory": {
            "reported_count": len(interfaces) if isinstance(interfaces, list) else None,
            "completeness": interface_inventory_completeness(device),
        },
        "licensed_redundancy": {
            "enabled": licensed if isinstance(licensed, bool) else None,
            "source": "diagnostic_log_export" if isinstance(licensed, bool) else None,
        },
        "serializer_cohort": redundancy_serializer_cohort(device),
        "probe_outcomes": deepcopy(getattr(device, "redundancy_probe_outcomes", None) or {}),
        "reboot_required": state.get("reboot_required") is True,
    }
    from netaudio.dante.operation_availability import operation_availability

    result["operation_availability"] = operation_availability(device, "redundancy").to_dict()
    return result


def _record_probe_outcome(device, probe: str, outcome: str, *, observational: bool = False) -> None:
    outcomes = dict(getattr(device, "redundancy_probe_outcomes", None) or {})
    outcomes[probe] = {
        "outcome": outcome,
        "observational": observational,
        "observed_at_unix": time.time(),
    }
    device.redundancy_probe_outcomes = outcomes


def _mark_redundancy_stale(device, *, choices: bool = False) -> None:
    state = getattr(device, "dante_redundancy", None)
    if not isinstance(state, dict):
        return
    state = deepcopy(state)
    if choices:
        if state.get("available_modes_source") == "switch_configuration_choice_table":
            state["available_modes_fresh"] = False
        source = state.get("state_source")
        if isinstance(source, dict) and source.get("kind") == "switch_configuration_status":
            state["state_fresh"] = False
    else:
        state["state_fresh"] = False
    device.dante_redundancy = state


async def probe_switch_configuration_if_reported(application, device, timeout: float = 2.0) -> dict | None:
    from netaudio.dante.application import CapabilityProbeTimeout

    support = advertised_redundancy_support(device)
    if support is False:
        _record_probe_outcome(device, "switch_configuration", "skipped_unsupported")
        return None
    try:
        result = await application.probe_switch_configuration(device, timeout=timeout)
        _record_probe_outcome(device, "switch_configuration", "response", observational=support is None)
        return result
    except (CapabilityProbeTimeout, TimeoutError):
        _record_probe_outcome(device, "switch_configuration", "timeout", observational=support is None)
        _mark_redundancy_stale(device, choices=True)
        return None


async def probe_redundancy(application, device, timeout: float = 2.0) -> dict:
    from netaudio.dante.application import CapabilityProbeTimeout

    support = advertised_redundancy_support(device)
    if support is False:
        _record_probe_outcome(device, "interface_status", "skipped_unsupported")
        _record_probe_outcome(device, "switch_configuration", "skipped_unsupported")
        return redundancy_snapshot(device)
    try:
        await application.probe_interface_status(device, timeout=timeout)
        _record_probe_outcome(device, "interface_status", "response", observational=support is None)
    except (CapabilityProbeTimeout, TimeoutError):
        _record_probe_outcome(device, "interface_status", "timeout", observational=support is None)
        _mark_redundancy_stale(device)
        return redundancy_snapshot(device)
    await probe_switch_configuration_if_reported(application, device, timeout)
    return redundancy_snapshot(device)


def _mutation_acknowledgement(response) -> dict[str, Any] | None:
    if response is None:
        return None
    return {
        "received": True,
        "accepted": None,
        "kind": "transport_response",
    }


def _unverified_result(device, mode: str, acknowledgement, *, mutation_sent: bool) -> dict:
    return RedundancyMutationResult(
        state="unverified",
        requested_mode=mode,
        mutation_sent=mutation_sent,
        request_acknowledgement=acknowledgement,
        device_side_confirmation=None,
        effective_state_confirmation=None,
        effective_readback=redundancy_snapshot(device),
        persistence_confirmation=None,
        reboot_evidence=None,
    ).to_dict()


async def set_redundancy(application, device, mode: str, timeout: float = 2.0) -> dict:
    if not isinstance(mode, str) or mode not in {"switched", "redundant", "split_redundant"}:
        raise ValueError("mode must be switched, redundant, or split_redundant")
    initial = operation_availability(device, "redundancy", mode)
    preflight_refresh_reasons = {
        "state_unavailable",
        "state_stale",
        "available_modes_unknown",
        "available_modes_stale",
        "requested_mode_not_advertised",
        "protocol_unsupported",
        "serializer_unavailable",
    }
    if any(reason not in preflight_refresh_reasons for reason in initial.reasons):
        require_writable(device, "redundancy", mode)
    async with device.topology_mutation_lock:
        before = await probe_redundancy(application, device, timeout)
        require_writable(device, "redundancy", mode)
        before_interfaces = deepcopy(device.interfaces)
        if before["configured_mode"] == mode:
            return RedundancyMutationResult(
                state="effective_state_confirmed",
                requested_mode=mode,
                mutation_sent=False,
                request_acknowledgement=None,
                device_side_confirmation=None,
                effective_state_confirmation=True,
                effective_readback=before,
                persistence_confirmation=None,
                reboot_evidence=None,
            ).to_dict()
        choice = switch_configuration_choice(device, mode)
        if redundancy_serializer_cohort(device) == "switch_configuration_choice_table" and choice is None:
            raise NetworkConfigurationError("The advertised redundancy mode has no supported serializer value")
        specification = application.commands.set_dante_redundancy(device.interface_status_protocol, mode, choice)
        acknowledgement = None
        try:
            response = await application._send_settings(device, specification)
            acknowledgement = _mutation_acknowledgement(response)
            after = await probe_redundancy(application, device, timeout)
        except (OSError, RuntimeError, TimeoutError) as exception:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but readback is unavailable; no retry was sent",
                _unverified_result(device, mode, acknowledgement, mutation_sent=True),
            ) from exception
        if after.get("configured_mode") != mode or after.get("state_fresh") is not True:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but the configured value did not match",
                _unverified_result(device, mode, acknowledgement, mutation_sent=True),
            )
        if device.interfaces != before_interfaces or after.get("current_mode") not in {
            before.get("current_mode"),
            mode,
        }:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but other network state changed; no retry or reboot was sent",
                _unverified_result(device, mode, acknowledgement, mutation_sent=True),
            )
        return RedundancyMutationResult(
            state="effective_state_confirmed",
            requested_mode=mode,
            mutation_sent=True,
            request_acknowledgement=acknowledgement,
            device_side_confirmation=None,
            effective_state_confirmation=True,
            effective_readback=after,
            persistence_confirmation=None,
            reboot_evidence=None,
        ).to_dict()
