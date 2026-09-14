from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.application import CapabilityProbeTimeout
from netaudio.dante.network_configuration import (
    advertised_redundancy_support,
    interface_redundancy_status,
    probe_redundancy,
    probe_switch_configuration_if_reported,
    redundancy_snapshot,
    set_redundancy,
    switch_configuration_fields,
)
from netaudio.dante.operation_availability import operation_availability


def _source(*, reported=True, version=0x0724):
    return {
        "kind": "conmon_dante_model",
        "record_protocol_version": version,
        "field_reported": reported,
        "fresh": True,
    }


def _flag_state(current="switched", configured="switched"):
    return interface_redundancy_status(
        {
            "record_protocol_identifier": 0x0724,
            "interfaces": [{"interface": "primary"}],
            "redundancy_flags": (1 if current == "redundant" else 0) | (2 if configured == "redundant" else 0),
            "redundancy": {
                "current": current,
                "configured": configured,
                "supported": ["switched", "redundant"],
                "reboot_required": current != configured,
            },
        },
        SimpleNamespace(switch_configuration_choices=None, dante_redundancy=None),
    )


def _device(*, support=True, read_only=False, interfaces=None, managed=False):
    return SimpleNamespace(
        ipv4="192.0.2.10",
        control_transports=["ddm" if managed else "direct"],
        requires_managed_control=managed,
        managed_operation_permissions={"redundancy": True},
        is_locked=False,
        interface_status_protocol=0x0724,
        interfaces=[{"interface": "primary"}] if interfaces is None else interfaces,
        dante_redundancy=_flag_state(),
        switch_configuration_choices=None,
        switch_redundancy_supported=support,
        redundancy_advertised_support_source=_source(),
        switch_redundancy_read_only=read_only,
        redundancy_read_only_source=_source(),
        licensed_redundancy_enabled=None,
        redundancy_probe_outcomes={},
        topology_mutation_lock=DeferredAsyncioLock(),
    )


def _choice_status(*entries, current=1, configured=1):
    labels = {"Switched": "switched", "Redundant": "redundant", "Split/Redundant": "split_redundant"}
    choices = [
        {
            "code": code,
            "label": label,
            "raw_label_field_hexadecimal": f"{code:04x}",
            "raw_choice_hexadecimal": f"feed{code:04x}",
        }
        for code, label in entries
    ]
    by_code = {entry[0]: labels.get(entry[1]) for entry in entries}
    return {
        "record_protocol_identifier": 0x072E,
        "mode_codes_at_record_offsets_20_and_22": [current, configured],
        "choices": choices,
        "redundancy": {
            "current": by_code.get(current),
            "configured": by_code.get(configured),
            "supported": [mode for mode in by_code.values() if mode is not None],
            "reboot_required": current != configured,
        },
        "raw_record_hexadecimal": "cafe",
    }


@pytest.mark.asyncio
async def test_advertised_capability_with_one_interface_keeps_state_and_probes():
    device = _device()
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(return_value=device.interfaces),
        probe_switch_configuration=AsyncMock(return_value={}),
    )

    assert advertised_redundancy_support(device) is True
    assert device.dante_redundancy["current"] == "switched"
    assert redundancy_snapshot(device)["interface_inventory"] == {
        "reported_count": 1,
        "completeness": "partial",
    }
    observed = await probe_redundancy(application, device)
    assert observed["current_mode"] == "switched"
    application.probe_interface_status.assert_awaited_once()
    application.probe_switch_configuration.assert_awaited_once()

    device.is_locked = None
    assert "lock_state_unknown" in operation_availability(device, "redundancy", "redundant").reasons


@pytest.mark.asyncio
async def test_explicit_unsupported_with_two_interfaces_never_probes_or_writes():
    device = _device(support=False, interfaces=[{"interface": "primary"}, {"interface": "secondary"}])
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(),
        probe_switch_configuration=AsyncMock(),
        _send_settings=AsyncMock(),
    )

    result = await probe_redundancy(application, device)
    assert result["advertised_support"] is False
    application.probe_interface_status.assert_not_awaited()
    application.probe_switch_configuration.assert_not_awaited()
    with pytest.raises(RuntimeError, match="unsupported"):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_versions_record_stays_unknown_after_diagnostic_choice_probe():
    device = _device(interfaces=[{"interface": "primary"}, {"interface": "secondary"}])
    device.switch_redundancy_supported = None
    device.redundancy_advertised_support_source = None
    parsed = _choice_status((1, "Switched"), (2, "Redundant"))

    async def choices(_device, timeout):
        fields = switch_configuration_fields(parsed)
        device.dante_redundancy = fields["dante_redundancy"]
        device.switch_configuration_choices = fields["switch_configuration_choices"]
        return parsed

    application = SimpleNamespace(probe_switch_configuration=AsyncMock(side_effect=choices))
    await probe_switch_configuration_if_reported(application, device)

    assert redundancy_snapshot(device)["advertised_support"] is None
    assert device.redundancy_probe_outcomes["switch_configuration"]["observational"] is True
    with pytest.raises(RuntimeError, match="capability_unknown"):
        await set_redundancy(application, device, "redundant")


@pytest.mark.asyncio
async def test_read_only_capability_keeps_readable_state_and_refuses_mutation():
    device = _device(read_only=True)
    availability = operation_availability(device, "redundancy", "redundant")
    assert availability.readable is True
    assert "read_only" in availability.reasons
    application = SimpleNamespace(_send_settings=AsyncMock())
    with pytest.raises(RuntimeError, match="read_only"):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


def test_older_versions_preserve_unavailable_read_only_field_without_inventing_bit():
    device = _device()
    device.switch_redundancy_read_only = None
    device.redundancy_read_only_source = {
        **_source(reported=False, version=0x0709),
        "unavailable_reason": "record_protocol_version",
    }

    availability = operation_availability(device, "redundancy", "redundant")
    assert "read_only" not in availability.reasons
    assert "read_only_unknown" not in availability.reasons
    assert redundancy_snapshot(device)["read_only"] is None


def test_stale_applicable_read_only_observation_blocks_mutation():
    device = _device()
    device.redundancy_read_only_source["fresh"] = False

    snapshot = redundancy_snapshot(device)
    assert snapshot["read_only"] is None
    assert "read_only_unknown" in snapshot["operation_availability"]["reasons"]


def test_missing_read_only_value_for_applicable_version_blocks_mutation():
    device = _device()
    device.switch_redundancy_read_only = None
    device.redundancy_read_only_source["field_reported"] = False

    snapshot = redundancy_snapshot(device)
    assert snapshot["read_only"] is None
    assert "read_only_unknown" in snapshot["operation_availability"]["reasons"]


def test_stale_versions_record_makes_capability_unknown_instead_of_unsupported():
    device = _device(support=False)
    device.redundancy_advertised_support_source["fresh"] = False

    snapshot = redundancy_snapshot(device)
    assert snapshot["advertised_support"] is None
    assert "capability_unknown" in snapshot["operation_availability"]["reasons"]
    assert "unsupported" not in snapshot["operation_availability"]["reasons"]


def test_partial_interface_inventory_does_not_discard_valid_flag_state():
    device = _device()
    snapshot = redundancy_snapshot(device)
    assert snapshot["current_mode"] == "switched"
    assert snapshot["configured_mode"] == "switched"
    assert snapshot["state_fresh"] is True
    assert snapshot["interface_inventory"]["completeness"] == "partial"


def test_unknown_interface_flags_preserve_raw_state_without_guessing_modes():
    device = _device()
    device.dante_redundancy = interface_redundancy_status(
        {
            "record_protocol_identifier": 0x07FE,
            "interfaces": [{"interface": "primary"}],
            "redundancy_flags": 8,
            "redundancy": None,
            "raw_record_hexadecimal": "cafe",
        },
        device,
    )

    snapshot = redundancy_snapshot(device)
    assert snapshot["current_mode"] is None
    assert snapshot["current_mode_evidence"] == {
        "status": "unknown_raw",
        "mode": None,
        "raw_flags": 8,
        "known_mask": 3,
    }
    assert snapshot["available_modes"] is None
    assert "state_unavailable" in snapshot["operation_availability"]["reasons"]
    assert "protocol_unsupported" in snapshot["operation_availability"]["reasons"]


@pytest.mark.asyncio
async def test_choice_table_is_exact_and_unadvertised_mode_is_rejected_before_send():
    parsed = _choice_status((1, "Switched"), (2, "Split/Redundant"))
    fields = switch_configuration_fields(parsed)
    device = _device()
    device.interface_status_protocol = 0x072E
    device.dante_redundancy = fields["dante_redundancy"]
    device.switch_configuration_choices = fields["switch_configuration_choices"]
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(return_value=device.interfaces),
        probe_switch_configuration=AsyncMock(return_value=parsed),
        _send_settings=AsyncMock(),
    )

    snapshot = redundancy_snapshot(device)
    assert [choice["mode"] for choice in snapshot["available_modes"]] == ["switched", "split_redundant"]
    with pytest.raises(RuntimeError, match="requested_mode_not_advertised"):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


def test_unknown_choice_code_and_label_survive_without_guessed_mode():
    parsed = _choice_status((1, "Switched"), (99, "Future Mode"), current=99, configured=99)
    fields = switch_configuration_fields(parsed)
    device = _device()
    device.interface_status_protocol = 0x072E
    device.dante_redundancy = fields["dante_redundancy"]
    device.switch_configuration_choices = fields["switch_configuration_choices"]
    snapshot = redundancy_snapshot(device)

    assert snapshot["current_mode"] is None
    assert snapshot["current_mode_evidence"] == {
        "status": "unknown_raw",
        "mode": None,
        "raw_code": 99,
        "raw_label": "Future Mode",
        "raw_choice_hexadecimal": "feed0063",
    }
    assert snapshot["available_modes"][1]["label"] == "Future Mode"
    assert snapshot["available_modes"][1]["mode"] is None
    assert "state_unavailable" in operation_availability(device, "redundancy").reasons


@pytest.mark.asyncio
async def test_choice_mode_without_a_raw_code_cannot_select_a_serializer():
    parsed = _choice_status((1, "Switched"), (2, "Redundant"))
    fields = switch_configuration_fields(parsed)
    device = _device()
    device.interface_status_protocol = 0x072E
    device.dante_redundancy = fields["dante_redundancy"]
    device.dante_redundancy["available_modes"][1].pop("code")
    device.switch_configuration_choices = fields["switch_configuration_choices"]
    application = SimpleNamespace(
        probe_interface_status=AsyncMock(return_value=device.interfaces),
        probe_switch_configuration=AsyncMock(return_value=parsed),
        _send_settings=AsyncMock(),
    )

    assert "serializer_unavailable" in operation_availability(device, "redundancy", "redundant").reasons
    with pytest.raises(RuntimeError, match="serializer_unavailable"):
        await set_redundancy(application, device, "redundant")
    application._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_choice_probe_timeout_preserves_choices_marks_stale_and_keeps_capability():
    parsed = _choice_status((1, "Switched"), (2, "Redundant"))
    fields = switch_configuration_fields(parsed)
    device = _device()
    device.dante_redundancy = fields["dante_redundancy"]
    device.switch_configuration_choices = fields["switch_configuration_choices"]
    before = deepcopy(device.switch_configuration_choices)
    application = SimpleNamespace(probe_switch_configuration=AsyncMock(side_effect=CapabilityProbeTimeout("timed out")))

    assert await probe_switch_configuration_if_reported(application, device) is None
    snapshot = redundancy_snapshot(device)
    assert device.switch_configuration_choices == before
    assert snapshot["available_modes"] == before
    assert snapshot["available_modes_fresh"] is False
    assert snapshot["state_fresh"] is False
    assert snapshot["advertised_support"] is True


def test_diagnostic_license_evidence_cannot_create_capability_or_writability():
    device = _device()
    device.switch_redundancy_supported = None
    device.redundancy_advertised_support_source = None
    device.licensed_redundancy_enabled = True
    snapshot = redundancy_snapshot(device)

    assert snapshot["licensed_redundancy"] == {
        "enabled": True,
        "source": "diagnostic_log_export",
    }
    assert snapshot["advertised_support"] is None
    assert "capability_unknown" in snapshot["operation_availability"]["reasons"]


@pytest.mark.parametrize(
    ("permissions", "reason"),
    [({}, "managed_permission_missing"), ({"redundancy": False}, "managed_permission_denied")],
)
def test_managed_support_does_not_bypass_permission(permissions, reason):
    device = _device(managed=True)
    device.managed_operation_permissions = permissions
    assert reason in operation_availability(device, "redundancy", "redundant").reasons


@pytest.mark.asyncio
async def test_mutation_is_sent_once_and_evidence_layers_remain_distinct():
    device = _device()
    observations = iter([_flag_state(), _flag_state(configured="redundant")])

    async def probe_interface(_device, timeout):
        device.dante_redundancy = next(observations)
        return device.interfaces

    application = SimpleNamespace(
        probe_interface_status=AsyncMock(side_effect=probe_interface),
        probe_switch_configuration=AsyncMock(return_value={}),
        commands=SimpleNamespace(set_dante_redundancy=Mock(return_value={"command": "set_dante_redundancy"})),
        _send_settings=AsyncMock(return_value=b"response"),
    )

    result = await set_redundancy(application, device, "redundant")
    application._send_settings.assert_awaited_once()
    assert application.probe_interface_status.await_count == 2
    assert application.probe_switch_configuration.await_count == 2
    assert result["request_acknowledgement"] == {
        "received": True,
        "accepted": None,
        "kind": "transport_response",
    }
    assert result["device_side_confirmation"] is None
    assert result["effective_state_confirmation"] is True
    assert result["effective_readback"]["configured_mode"] == "redundant"
    assert result["persistence_confirmation"] is None
    assert result["reboot_evidence"] is None
