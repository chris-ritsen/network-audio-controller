from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import time
from typing import Any

from netaudio import core
from netaudio.core import _requests, _types
from netaudio.core._abi import STATUS_INVALID_IP, STATUS_INVALID_JSON
from netaudio.dante.operation_availability import operation_availability, require_writable


class NetworkConfigurationError(RuntimeError):
    pass


class NetworkConfigurationUnverified(NetworkConfigurationError):
    def __init__(self, message: str, evidence: dict | None = None):
        super().__init__(message)
        self.evidence = evidence


def _interface_configuration_request(mode, configuration) -> _requests.InterfaceConfigurationRequest:
    if mode == "static":
        configuration = configuration or {}
        address = configuration.get("ip_address")
        netmask = configuration.get("netmask")

        if not isinstance(address, str) or not isinstance(netmask, str):
            raise ValueError("Static configuration requires an IPv4 address and netmask.")

        return {
            "mode": "static",
            "ip_address": address,
            "netmask": netmask,
            "dns_server": configuration.get("dns_server"),
            "gateway": configuration.get("gateway"),
        }

    return {"mode": mode}


def validate_interface_configuration(mode, configuration=None):
    try:
        return core.interface_configuration(_interface_configuration_request(mode, configuration))
    except core.NetaudioCoreError as error:
        if error.status not in (STATUS_INVALID_IP, STATUS_INVALID_JSON):
            raise

        raise ValueError(error.detail or str(error)) from error


async def set_interface(
    application, device, mode, configuration=None, *, interface: _requests.NetworkInterface = "primary", timeout=2.0
):
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
            if expected["mode"] == "dynamic":
                await application.send_set_interface_dhcp(device, interface=interface)

            else:
                await application.send_set_interface_static(
                    device,
                    expected["ip_address"],
                    expected["netmask"],
                    expected["dns_server"],
                    expected["gateway"],
                    interface=interface,
                )

            after = await application.probe_interface_status(device, timeout=timeout)
            core.verify_interface_configuration(
                {
                    "configuration": _interface_configuration_request(mode, configuration),
                    "interface": interface,
                    "before": before,
                    "after": after,
                    "before_redundancy": before_redundancy,
                    "after_redundancy": device.dante_redundancy,
                }
            )

        except (OSError, RuntimeError, TimeoutError) as exception:
            raise NetworkConfigurationUnverified(
                "Network change was requested, but could not be verified; no retry or reboot was sent"
            ) from exception

        return after


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
    return _network_control_state(device, entry=entry, writable=operation_availability(device, "static_ipv4").writable)[
        "configuration_modes"
    ]


def _network_control_state(device, *, entry=None, writable=False, support=None) -> _types.NetworkControlState:
    return core.network_control_state(
        {
            "entry": entry,
            "interfaces": getattr(device, "interfaces", None),
            "writable": writable,
            "redundancy_supported": support,
            "managed": bool(getattr(device, "requires_managed_control", False)),
            "transports": getattr(device, "control_transports", None),
            "address_available": getattr(device, "ipv4", None) is not None,
        }
    )


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


def advertised_redundancy_support(device) -> bool | None:
    return operation_availability(device, "redundancy").supported


def switch_configuration_fields(parsed: dict) -> dict:
    state = deepcopy(parsed["state"])
    state["state_source"]["observed_at_unix"] = time.time()
    return {"dante_redundancy": state}


def interface_redundancy_status(parsed: dict, device) -> _types.InterfaceRedundancyResult | None:
    return core.interface_redundancy_status(
        {
            "flags": parsed.get("redundancy_flags"),
            "previous": getattr(device, "dante_redundancy", None),
            "record_protocol_identifier": parsed.get("record_protocol_identifier"),
            "raw_record_hexadecimal": parsed.get("raw_record_hexadecimal"),
            "observed_at_unix": time.time(),
        }
    )


def redundancy_transport_available(device) -> bool:
    return _network_control_state(device)["transport_available"]


def redundancy_snapshot(device) -> dict:
    availability = operation_availability(device, "redundancy")
    state = deepcopy(getattr(device, "dante_redundancy", None))
    state = state if isinstance(state, dict) else {}
    choices = deepcopy(state.get("available_modes"))
    if not isinstance(choices, list):
        choices = None
    network = _network_control_state(device, support=availability.supported)
    licensed = getattr(device, "licensed_redundancy_enabled", None)
    result = {
        "advertised_support": availability.supported,
        "advertised_support_source": deepcopy(getattr(device, "redundancy_advertised_support_source", None)),
        "read_only": availability.read_only,
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
            "reported_count": network["reported_count"],
            "completeness": network["inventory_completeness"],
        },
        "licensed_redundancy": {
            "enabled": licensed if isinstance(licensed, bool) else None,
            "source": "diagnostic_log_export" if isinstance(licensed, bool) else None,
        },
        "serializer_cohort": core.redundancy_control(state)["serializer_cohort"],
        "probe_outcomes": deepcopy(getattr(device, "redundancy_probe_outcomes", None) or {}),
        "reboot_required": state.get("reboot_required") is True,
    }
    result["operation_availability"] = availability.to_dict()
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
    if mode is None:
        raise ValueError("A redundancy mode is required")

    try:
        initial = operation_availability(device, "redundancy", mode)
    except core.NetaudioCoreError as exception:
        raise ValueError("Invalid redundancy mode") from exception

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
        control = core.redundancy_control(device.dante_redundancy, mode)

        if control["configuration_matched"]:
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
        choice = control["switch_configuration_choice"]
        specification = application.commands.set_dante_redundancy(mode, choice)
        acknowledgement = None
        try:
            response = await application._send_settings(device, specification)
            acknowledgement = _mutation_acknowledgement(response)
            after = await probe_redundancy(application, device, timeout)
            confirmation = core.redundancy_control(
                device.dante_redundancy,
                mode,
                readback={
                    "before_mode": before["current_mode"],
                    "before_interfaces": before_interfaces,
                    "after_interfaces": device.interfaces,
                },
            )["readback"]

        except (OSError, RuntimeError, TimeoutError) as exception:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but readback is unavailable; no retry was sent",
                _unverified_result(device, mode, acknowledgement, mutation_sent=True),
            ) from exception
        if confirmation == "configuration_unconfirmed":
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but the configured value did not match",
                _unverified_result(device, mode, acknowledgement, mutation_sent=True),
            )
        if confirmation != "verified":
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
