from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio import core
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.network_configuration import (
    NetworkConfigurationError,
    NetworkConfigurationUnverified,
    interface_configuration,
    interface_redundancy_status,
    probe_redundancy,
    set_interface,
    set_redundancy,
    validate_interface_configuration,
)
from tests.http_api_test_support import get, make_device, make_http_server, post


@pytest.mark.parametrize("case,pending", [("before", False), ("pending", True), ("restored", False)])
def test_managed_network_fixture_digest_and_pending_readback(case, pending):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "managed_network_configuration.json").read_text())
    record = fixture["cases"][case]
    packet = bytes.fromhex(record["hexadecimal"])
    assert hashlib.sha256(packet).hexdigest() == record["sha256"]
    parsed = core.parse_response("interface_status", packet)
    primary = parsed["interfaces"][0]
    assert primary["dns_server"] == "8.8.8.8"
    assert primary["configured"]["dns_server"] == ("192.0.2.1" if pending else "8.8.8.8")
    assert parsed["reboot_required"] is pending


def switch_choices(*entries):
    entries = entries or (("Switched", 1), ("Split/Redundant", 2))
    modes = {"Switched": "switched", "Redundant": "redundant", "Split/Redundant": "split_redundant"}
    return [{"code": code, "label": label, "mode": modes[label]} for label, code in entries]


def network_device(protocol=0x0724):
    return SimpleNamespace(
        interface_status_protocol=protocol,
        dante_redundancy=None,
        interfaces=[],
        switch_configuration_choices=None,
        interface_reboot_required=False,
        link_speed_mbps=1000,
        requires_managed_control=False,
        topology_mutation_lock=DeferredAsyncioLock(),
    )


def redundancy_status(current="switched", configured="switched", supported=None):
    return {
        "current": current,
        "configured": configured,
        "supported": ["switched", "redundant"] if supported is None else supported,
        "reboot_required": current != configured,
    }


def redundancy_application(device, observations):
    async def probe(_device, timeout):
        observation = next(observations)
        if isinstance(observation, Exception):
            raise observation
        device.dante_redundancy = deepcopy(observation)
        return []

    return SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=probe),
        probe_switch_configuration=AsyncMock(),
        commands=SimpleNamespace(set_dante_redundancy=Mock(return_value={"command": "set_dante_redundancy"})),
        _send_settings=AsyncMock(),
        reboot=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_redundancy_verifies_configured_not_active_without_reboot():
    device = network_device()
    application = redundancy_application(device, iter([redundancy_status(), redundancy_status(configured="redundant")]))
    result = await set_redundancy(application, device, "redundant")
    assert result == redundancy_status(configured="redundant")
    application.commands.set_dante_redundancy.assert_called_once_with(0x0724, "redundant", None)
    application._send_settings.assert_awaited_once()
    assert application.probe_interface_status.await_count == 2
    application.reboot.assert_not_awaited()
    assert not device.topology_mutation_lock.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("after", [TimeoutError(), redundancy_status()])
async def test_redundancy_unverified_write_is_never_retried(after):
    device = network_device()
    application = redundancy_application(device, iter([redundancy_status(), after]))
    with pytest.raises(NetworkConfigurationUnverified):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_awaited_once()
    application.reboot.assert_not_awaited()


@pytest.mark.asyncio
async def test_redundancy_preflight_failure_sends_nothing():
    device = network_device()
    application = redundancy_application(device, iter([TimeoutError()]))
    with pytest.raises(TimeoutError):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_redundancy_noop_requires_fresh_observation():
    device = network_device()
    application = redundancy_application(device, iter([redundancy_status(configured="redundant")]))
    await set_redundancy(application, device, "redundant")
    application.probe_interface_status.assert_awaited_once()
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [None, [], 1, "unknown", "bridge"])
async def test_invalid_redundancy_never_queries_or_writes(mode):
    device = network_device()
    application = redundancy_application(device, iter([]))
    with pytest.raises(ValueError):
        await set_redundancy(application, device, mode)
    application.probe_interface_status.assert_not_awaited()
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsupported_choice_fails_closed():
    device = network_device()
    application = redundancy_application(device, iter([redundancy_status(supported=[])]))
    with pytest.raises(NetworkConfigurationError):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_unobserved_revision_with_reported_support_is_written_and_verified():
    device = network_device(0x0777)
    application = redundancy_application(device, iter([redundancy_status(), redundancy_status(configured="redundant")]))
    assert await set_redundancy(application, device, "redundant") == redundancy_status(configured="redundant")
    application.commands.set_dante_redundancy.assert_called_once_with(0x0777, "redundant", None)
    application._send_settings.assert_awaited_once()


@pytest.mark.asyncio
async def test_reported_choice_table_selects_the_choice_code_for_the_mode():
    device = network_device(0x0777)
    device.switch_configuration_choices = switch_choices()
    application = redundancy_application(
        device,
        iter(
            [
                redundancy_status(supported=["switched", "split_redundant"]),
                redundancy_status(configured="split_redundant", supported=["switched", "split_redundant"]),
            ]
        ),
    )
    await set_redundancy(application, device, "split_redundant")
    application.commands.set_dante_redundancy.assert_called_once_with(0x0777, "split_redundant", 2)
    assert application.probe_switch_configuration.await_count == 2


@pytest.mark.asyncio
async def test_ad4d_uses_fresh_choice_status():
    device = network_device(0x072E)
    device.switch_configuration_choices = switch_choices()
    application = redundancy_application(device, iter([None]))
    expected = redundancy_status(configured="split_redundant", supported=["switched", "split_redundant"])

    async def choices(_device, timeout):
        device.dante_redundancy = expected

    application.probe_switch_configuration.side_effect = choices
    assert await probe_redundancy(application, device) == expected
    application.probe_switch_configuration.assert_awaited_once_with(device, timeout=2.0)


def test_single_port_zero_flags_do_not_advertise_redundancy_changes():
    parsed = {"interfaces": [{}], "redundancy": redundancy_status()}
    result = interface_redundancy_status(parsed, network_device())
    assert result["supported"] == []
    assert parsed["redundancy"]["supported"] == ["switched", "redundant"]


def interface_state(role, configured):
    return {"interface": role, "mode": "static", "configured": configured}


@pytest.mark.asyncio
async def test_matching_secondary_does_not_verify_primary_change():
    device = network_device()
    primary = interface_state("primary", {"mode": "static"})
    secondary = interface_state("secondary", {"mode": "dynamic"})
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=[[primary, secondary], [primary, secondary]]),
        send_set_interface_dhcp=AsyncMock(),
        reboot=AsyncMock(),
    )
    with pytest.raises(NetworkConfigurationUnverified):
        await set_interface(application, device, "dhcp")
    application.send_set_interface_dhcp.assert_awaited_once_with(
        device, interface="primary", record_protocol_identifier=0x0724
    )
    application.reboot.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol,managed", [(0x0724, False), (0x0738, True)])
async def test_matching_primary_pending_change_is_verified(protocol, managed):
    device = network_device(protocol)
    device.requires_managed_control = managed
    primary = interface_state("primary", {"mode": "static"})
    pending = interface_state("primary", {"mode": "dynamic"})
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=[[primary], [pending]]),
        send_set_interface_dhcp=AsyncMock(),
    )
    assert await set_interface(application, device, "dhcp") == [pending]
    application.send_set_interface_dhcp.assert_awaited_once_with(
        device, interface="primary", record_protocol_identifier=protocol
    )


@pytest.mark.asyncio
async def test_secondary_write_unavailable_without_a_configured_record():
    secondary = interface_state("secondary", None)
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(return_value=[secondary]), send_set_interface_dhcp=AsyncMock()
    )
    with pytest.raises(NetworkConfigurationError, match="unavailable for this interface"):
        await set_interface(application, network_device(), "dhcp", interface="secondary")
    application.probe_interface_status.assert_awaited_once()
    application.send_set_interface_dhcp.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_write_proceeds_for_an_unobserved_revision_with_a_configured_record():
    device = network_device(0x07FE)
    before = interface_state("secondary", {"mode": "static"})
    after = interface_state("secondary", {"mode": "dynamic"})
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=[[before], [after]]), send_set_interface_dhcp=AsyncMock()
    )
    assert await set_interface(application, device, "dhcp", interface="secondary") == [after]
    application.send_set_interface_dhcp.assert_awaited_once_with(
        device, interface="secondary", record_protocol_identifier=0x07FE
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["dhcp", "static"])
async def test_secondary_configuration_reaches_the_selected_interface(mode):
    device = network_device(0x073D)
    fields = {
        "mode": "static",
        "ip_address": "198.51.100.102",
        "netmask": "255.255.255.0",
        "dns_server": "203.0.113.53",
        "gateway": "203.0.113.2",
    }
    before = [
        interface_state("primary", {"mode": "dynamic"}),
        interface_state("secondary", {"mode": "static", "ip_address": "198.51.100.99"}),
    ]
    after = deepcopy(before)
    after[1]["configured"] = {"mode": "dynamic"} if mode == "dhcp" else fields
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=[before, after]),
        send_set_interface_dhcp=AsyncMock(),
        send_set_interface_static=AsyncMock(),
    )
    assert await set_interface(application, device, mode, fields, interface="secondary") == after
    if mode == "dhcp":
        application.send_set_interface_dhcp.assert_awaited_once_with(
            device, interface="secondary", record_protocol_identifier=0x073D
        )
        application.send_set_interface_static.assert_not_awaited()
    else:
        application.send_set_interface_static.assert_awaited_once_with(
            device,
            fields["ip_address"],
            fields["netmask"],
            fields["dns_server"],
            fields["gateway"],
            interface="secondary",
            record_protocol_identifier=0x073D,
        )
        application.send_set_interface_dhcp.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_secondary_configuration_preserves_target_and_reports_supported_modes():
    device = make_device()
    device.interface_status_protocol = 0x073D
    server = make_http_server({"dev1": device})
    server.application.set_interface.return_value = [
        interface_state("primary", {"mode": "dynamic"}),
        interface_state("secondary", {"mode": "dynamic"}),
    ]
    status, result = await post(server, "/interface", {"device": "dev1", "interface": "secondary", "mode": "dhcp"})
    assert status == 200
    assert server.application.set_interface.await_args.kwargs == {"interface": "secondary"}
    assert result["interface_configuration_modes"] == {"primary": ["dhcp", "static"], "secondary": ["dhcp", "static"]}


@pytest.mark.parametrize("entries", [None, b"reply", [{}], [{"interface": "primary"}] * 2])
def test_interface_selection_requires_one_identified_target(entries):
    with pytest.raises(NetworkConfigurationError):
        interface_configuration(entries, "primary")


@pytest.mark.parametrize(
    "override",
    [
        {"ip_address": "224.0.0.1"},
        {"ip_address": "0.0.0.0"},
        {"ip_address": "192.0.2.255"},
        {"ip_address": "192.0.2.0"},
        {"ip_address": "::1"},
        {"netmask": "255.0.255.0"},
        {"netmask": "0.0.0.0"},
        {"netmask": "0.0.0.255"},
        {"gateway": "224.0.0.1"},
        {"dns_server": []},
    ],
)
def test_invalid_static_configuration_is_rejected(override):
    with pytest.raises(ValueError):
        validate_interface_configuration("static", {"ip_address": "192.0.2.34", "netmask": "255.255.255.0", **override})


def test_controller_configured_gateway_and_dns_are_preserved_as_distinct_fields():
    fixture = json.loads((Path(__file__).parent / "fixtures" / "wing_network_fields.json").read_text())
    for fields in fixture["configured_fields"].values():
        assert validate_interface_configuration("static", fields) == fields


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["secondary", "active_address", "redundancy"])
async def test_matching_target_does_not_hide_changes_to_other_network_state(changed):
    device = network_device(0x073D)
    device.dante_redundancy = redundancy_status(current="redundant", configured="redundant")
    before = [interface_state("primary", {"mode": "static"}), interface_state("secondary", {"mode": "static"})]
    after = deepcopy(before)
    after[0]["configured"] = {"mode": "dynamic"}
    if changed == "secondary":
        after[1]["configured"] = {"mode": "dynamic"}
    elif changed == "active_address":
        after[0]["ip_address"] = "192.0.2.99"
    observations = iter([before, after])

    async def probe(_device, timeout):
        result = next(observations)
        if result is after and changed == "redundancy":
            device.dante_redundancy["configured"] = "switched"
        return result

    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=probe), send_set_interface_dhcp=AsyncMock()
    )
    with pytest.raises(NetworkConfigurationUnverified):
        await set_interface(application, device, "dhcp")
    application.send_set_interface_dhcp.assert_awaited_once()


@pytest.mark.asyncio
async def test_probe_applies_exact_returned_status_before_dispatcher_delivery():
    device = network_device()
    status = {
        "interfaces": [interface_state("secondary", {"mode": "dynamic"})],
        "interface_status_protocol": 0x0724,
        "dante_redundancy": redundancy_status(configured="redundant"),
        "interface_reboot_required": True,
        "link_speed_mbps": 1000,
    }
    application = SimpleNamespace(_probe_once=AsyncMock(return_value=status), send_probe_interface_status=AsyncMock())
    assert await DanteApplication.probe_interface_status(application, device) == status["interfaces"]
    assert device.dante_redundancy == status["dante_redundancy"]
    assert device.interface_reboot_required


@pytest.mark.asyncio
async def test_redundancy_http_get_and_set_use_verified_application_methods():
    device = make_device()
    server = make_http_server({"dev1": device})
    value = redundancy_status(configured="redundant")
    server.application.probe_dante_redundancy = AsyncMock(return_value=value)
    server.application.set_dante_redundancy = AsyncMock(return_value=value)
    status, result = await get(server, "/redundancy/dev1")
    assert status == 200 and result["redundancy"] == value
    status, result = await post(server, "/redundancy", {"device": "dev1", "mode": "redundant"})
    assert status == 200 and result == {"success": True, "redundancy": value}
    server.application.set_dante_redundancy.assert_awaited_once_with(device, "redundant")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,status",
    [
        (ValueError("invalid"), 400),
        (NetworkConfigurationError("unsupported"), 409),
        (NetworkConfigurationUnverified("unknown outcome"), 502),
    ],
)
async def test_redundancy_http_reports_failure(error, status):
    device = make_device()
    server = make_http_server({"dev1": device})
    server.application.set_dante_redundancy = AsyncMock(side_effect=error)
    actual, result = await post(server, "/redundancy", {"device": "dev1", "mode": "redundant"})
    assert actual == status and result == {"error": str(error)}


@pytest.mark.asyncio
async def test_managed_wing_redundancy_restores_active_mode_without_reboot():
    device = network_device(0x073D)
    device.requires_managed_control = True
    device.switch_configuration_choices = switch_choices(("Switched", 1), ("Redundant", 2))
    application = redundancy_application(
        device,
        iter(
            [
                redundancy_status(current="redundant"),
                redundancy_status(current="redundant", configured="redundant"),
            ]
        ),
    )
    result = await set_redundancy(application, device, "redundant")
    assert result["configured"] == result["current"] == "redundant"
    assert result["reboot_required"] is False
    application._send_settings.assert_awaited_once()
    assert application.probe_switch_configuration.await_count == 2
    application.reboot.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["interfaces", "current"])
async def test_redundancy_detects_unrelated_network_changes(change):
    device = network_device(0x073D)
    after = redundancy_status(configured="redundant")
    if change == "current":
        after["current"] = "redundant"
    application = redundancy_application(device, iter([redundancy_status(), after]))

    async def send(*_args):
        if change == "interfaces":
            device.interfaces = [{"interface": "primary", "configured": {"mode": "static"}}]

    application._send_settings.side_effect = send
    with pytest.raises(NetworkConfigurationUnverified, match="other network state changed"):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_awaited_once()
    application.reboot.assert_not_awaited()


@pytest.mark.parametrize(
    "protocol,secondary_configured,secondary_modes",
    [
        (0x073D, {"mode": "dynamic"}, ["dhcp", "static"]),
        (0x0727, {"mode": "dynamic"}, ["dhcp", "static"]),
        (0x07FE, None, []),
    ],
)
def test_inventory_includes_network_controls_before_a_page_query(protocol, secondary_configured, secondary_modes):
    device = DanteDevice(server_name="network.local.")
    device.interface_status_protocol = protocol
    device.interfaces = [
        {"interface": "primary", "mode": "dynamic", "configured": {"mode": "dynamic"}},
        {"interface": "secondary", "mode": "dynamic", "configured": secondary_configured},
    ]

    expected = {"primary": ["dhcp", "static"]}
    if secondary_configured is not None:
        expected["secondary"] = secondary_modes
    assert device.to_json()["interface_configuration_modes"] == expected
