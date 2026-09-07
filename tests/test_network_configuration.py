from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.application import DanteApplication
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


def network_device(protocol=0x0724):
    return SimpleNamespace(
        interface_status_protocol=protocol,
        dante_redundancy=None,
        interfaces=[],
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
    application.commands.set_dante_redundancy.assert_called_once_with(0x0724, "redundant")
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
async def test_unknown_revision_and_unsupported_choice_fail_closed():
    for protocol, supported in [(0x0777, ["switched", "redundant"]), (0x0724, [])]:
        device = network_device(protocol)
        application = redundancy_application(device, iter([redundancy_status(supported=supported)]))
        with pytest.raises(NetworkConfigurationError):
            await set_redundancy(application, device, "redundant")
        application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_ad4d_uses_fresh_choice_status():
    device = network_device(0x072E)
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
    application.send_set_interface_dhcp.assert_awaited_once_with(device)
    application.reboot.assert_not_awaited()


@pytest.mark.asyncio
async def test_matching_primary_pending_change_is_verified():
    device = network_device()
    primary = interface_state("primary", {"mode": "static"})
    pending = interface_state("primary", {"mode": "dynamic"})
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=[[primary], [pending]]),
        send_set_interface_dhcp=AsyncMock(),
    )
    assert await set_interface(application, device, "dhcp") == [pending]
    application.send_set_interface_dhcp.assert_awaited_once_with(device)


@pytest.mark.asyncio
async def test_secondary_write_unavailable_without_sending_primary_command():
    application = SimpleNamespace(probe_interface_status=AsyncMock(), send_set_interface_dhcp=AsyncMock())
    with pytest.raises(NetworkConfigurationError, match="Secondary interface writes"):
        await set_interface(application, network_device(), "dhcp", interface="secondary")
    application.probe_interface_status.assert_not_awaited()
    application.send_set_interface_dhcp.assert_not_awaited()


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
        {"gateway": "198.51.100.1"},
        {"dns_server": []},
    ],
)
def test_invalid_static_configuration_is_rejected(override):
    with pytest.raises(ValueError):
        validate_interface_configuration("static", {"ip_address": "192.0.2.34", "netmask": "255.255.255.0", **override})


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
