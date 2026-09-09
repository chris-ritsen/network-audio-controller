from __future__ import annotations

from copy import deepcopy
from ipaddress import IPv4Address, IPv4Network


class NetworkConfigurationError(RuntimeError):
    pass


class NetworkConfigurationUnverified(NetworkConfigurationError):
    pass


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
    if not isinstance(interface, str) or interface not in {"primary", "secondary"}:
        raise ValueError("interface must be 'primary' or 'secondary'")
    async with device.topology_mutation_lock:
        before = deepcopy(await application.probe_interface_status(device, timeout=timeout))
        before_redundancy = deepcopy(device.dante_redundancy)
        selected = interface_configuration(before, interface)
        if (
            mode not in interface_configuration_modes(device.interface_status_protocol, interface)
            or selected.get("configured") is None
        ):
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
    return {
        "interfaces": deepcopy(device.interfaces or []),
        "interface_configuration_modes": {
            entry["interface"]: interface_configuration_modes(device.interface_status_protocol, entry["interface"])
            for entry in device.interfaces or []
            if entry.get("interface") in {"primary", "secondary"} and entry.get("configured") is not None
        },
        "redundancy": deepcopy(device.dante_redundancy),
        "link_speed_mbps": device.link_speed_mbps,
        "reboot_required": device.interface_reboot_required
        or bool((device.dante_redundancy or {}).get("reboot_required")),
    }


def interface_configuration_modes(record_protocol_identifier, interface):
    if interface == "primary" and record_protocol_identifier in {0x0724, 0x0727, 0x072E, 0x0738, 0x073D}:
        return ["dhcp", "static"]
    if interface == "secondary" and record_protocol_identifier == 0x073D:
        return ["dhcp", "static"]
    return []


def interface_redundancy_status(parsed: dict, device) -> dict | None:
    status = deepcopy(parsed.get("redundancy"))
    if status is None:
        return None
    # A zero mode flag on a single-port product is not evidence that it can
    # become redundant. Keep current-state reads separate from write support.
    known_hardware = (
        len(parsed.get("interfaces", [])) == 2
        or getattr(device, "licensed_redundancy_enabled", None) is True
        or getattr(device, "model", None) == "A32 Dante AD/DA Converter"
        or getattr(device, "dante_model", None) == "A32 Dante AD/DA Converter"
    )
    if not known_hardware:
        status["supported"] = []
    return status


async def probe_redundancy(application, device, timeout: float = 2.0) -> dict:
    await application.probe_interface_status(device, timeout=timeout)
    protocol = device.interface_status_protocol
    if protocol in {0x072E, 0x073D}:
        await application.probe_switch_configuration(device, timeout=timeout)
    elif protocol != 0x0724:
        raise NetworkConfigurationError("Dante redundancy is unavailable for this network protocol")
    status = device.dante_redundancy
    if not isinstance(status, dict) or status.get("current") is None or status.get("configured") is None:
        raise NetworkConfigurationError("Dante redundancy status was not reported")
    return deepcopy(status)


async def set_redundancy(application, device, mode: str, timeout: float = 2.0) -> dict:
    if not isinstance(mode, str) or mode not in {"switched", "redundant", "split_redundant"}:
        raise ValueError("mode must be switched, redundant, or split_redundant")
    async with device.topology_mutation_lock:
        before = await probe_redundancy(application, device, timeout)
        before_interfaces = deepcopy(device.interfaces)
        if getattr(device, "requires_managed_control", False) and device.interface_status_protocol != 0x073D:
            raise NetworkConfigurationError("Managed redundancy changes are unavailable for this network protocol")
        if mode not in before.get("supported", []):
            raise NetworkConfigurationError("The device does not support the selected Dante redundancy mode")
        if before["configured"] == mode:
            return before
        specification = application.commands.set_dante_redundancy(device.interface_status_protocol, mode)
        try:
            await application._send_settings(device, specification)
            after = await probe_redundancy(application, device, timeout)
        except (OSError, RuntimeError, TimeoutError) as exception:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but readback is unavailable; no retry was sent"
            ) from exception
        if after.get("configured") != mode:
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but the configured value did not match"
            )
        if device.interfaces != before_interfaces or after.get("current") != before.get("current"):
            raise NetworkConfigurationUnverified(
                "Dante redundancy was requested, but other network state changed; no retry or reboot was sent"
            )
        return after
