from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio import core
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
        SimpleNamespace(dante_redundancy=None),
    )


@pytest.mark.parametrize(
    ("flags", "current", "configured", "reboot"),
    [
        (0, "switched", "switched", False),
        (1, "redundant", "switched", True),
        (2, "switched", "redundant", True),
        (3, "redundant", "redundant", False),
    ],
)
def test_native_interface_flags_supply_modes_choices_and_evidence(flags, current, configured, reboot):
    facts = core.interface_redundancy_status({"flags": flags, "observed_at_unix": 0})

    assert {key: facts[key] for key in ("current", "configured", "supported", "reboot_required")} == {
        "current": current,
        "configured": configured,
        "supported": ["switched", "redundant"],
        "reboot_required": reboot,
    }
    assert [choice["mode"] for choice in facts["available_modes"]] == ["switched", "redundant"]
    assert facts["current_mode_evidence"]["mode"] == current
    assert facts["configured_mode_evidence"]["mode"] == configured
    assert facts["current_mode_evidence"]["flag_set"] is (current == "redundant")
    assert facts["configured_mode_evidence"]["flag_set"] is (configured == "redundant")


@pytest.mark.parametrize("flags", [4, 8, 65535])
def test_native_unknown_interface_flags_do_not_advertise_modes(flags):
    facts = core.interface_redundancy_status({"flags": flags, "observed_at_unix": 0})

    assert facts["current"] is None
    assert facts["configured"] is None
    assert facts["available_modes"] is None
    assert facts["current_mode_evidence"]["status"] == "unknown_raw"


def test_native_absent_interface_flags_are_unavailable():
    assert core.interface_redundancy_status({"flags": None, "observed_at_unix": 0}) is None


@pytest.mark.parametrize("flags", [True, "3", -1, 65536])
def test_native_interface_flags_reject_invalid_types_or_width(flags):
    with pytest.raises(core.NetaudioCoreError):
        core.interface_redundancy_status({"flags": flags, "observed_at_unix": 0})


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
        switch_redundancy_supported=support,
        redundancy_advertised_support_source=_source(),
        switch_redundancy_read_only=read_only,
        redundancy_read_only_source=_source(),
        licensed_redundancy_enabled=None,
        redundancy_probe_outcomes={},
        topology_mutation_lock=DeferredAsyncioLock(),
    )


def _choice_status(*entries, current=1, configured=1):
    fixture = Path(__file__).parent / "fixtures/switch_configuration/ad4d-switched-0014.hex"
    packet = bytearray.fromhex(fixture.read_text().strip())
    assert len(entries) == 2
    packet[44:46] = current.to_bytes(2, "big")
    packet[46:48] = configured.to_bytes(2, "big")

    for index, (code, label) in enumerate(entries):
        offset = 48 + index * 148
        packet[offset : offset + 2] = code.to_bytes(2, "big")
        packet[offset + 4 : offset + 132] = label.encode().ljust(128, b"\0")

    return core.parse_response("switch_configuration_status", bytes(packet))


@pytest.mark.parametrize(
    "code,label,mode,status",
    [(7, "Redundant", "redundant", "known"), (7, "Future Mode", None, "unknown_raw"), (99, None, None, "unknown_raw")],
)
def test_native_switch_evidence_tracks_advertised_choices(code, label, mode, status):
    parsed = _choice_status((1, "Switched"), (7, label or "Redundant"), current=code, configured=1)
    evidence = parsed["current_mode_evidence"]

    assert evidence["mode"] == parsed["redundancy"]["current"] == mode
    assert evidence["status"] == status
    assert evidence["raw_code"] == code
    assert evidence.get("raw_label") == label
    assert parsed["configured_mode_evidence"]["mode"] == "switched"
    assert parsed["redundancy"]["reboot_required"] is (mode == "redundant")

    fields = switch_configuration_fields(parsed)["dante_redundancy"]
    native_state = parsed["state"]
    assert native_state["current"] == mode
    assert native_state["configured"] == "switched"
    assert native_state["available_modes"] == parsed["choices"]
    assert native_state["available_modes_source"] == "switch_configuration_choice_table"
    assert native_state["state_fresh"] is True
    assert fields["state_source"]["observed_at_unix"] > 0
    assert fields["current_mode_evidence"] == evidence
    assert fields["configured_mode_evidence"] == parsed["configured_mode_evidence"]


@pytest.mark.parametrize("fresh", [False, True])
@pytest.mark.parametrize("flags", [None, 0, 8])
def test_interface_refresh_preserves_choice_inventory_and_its_freshness(flags, fresh):
    parsed = _choice_status((1, "Switched"), (7, "Split/Redundant"))
    previous = switch_configuration_fields(parsed)["dante_redundancy"]
    previous["available_modes_fresh"] = fresh
    device = SimpleNamespace(dante_redundancy=previous)
    expected_choices = deepcopy(previous["available_modes"])

    updated = interface_redundancy_status({"redundancy_flags": flags}, device)

    assert updated["available_modes"] == expected_choices
    assert updated["available_modes_source"] == "switch_configuration_choice_table"
    assert updated["available_modes_fresh"] is fresh
    if flags is None:
        assert updated["state_fresh"] is False

    updated["available_modes"].clear()
    assert previous["available_modes"] == expected_choices


@pytest.mark.parametrize("code", [7, 65535])
def test_native_redundancy_control_selects_the_advertised_serializer_value(code):
    parsed = _choice_status((1, "Switched"), (code, "Redundant"))
    state = switch_configuration_fields(parsed)["dante_redundancy"]

    control = core.redundancy_control(state, "redundant")

    assert control["reasons"] == []
    assert control["serializer_cohort"] == "switch_configuration_choice_table"
    assert control["switch_configuration_choice"] == code


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"state_fresh": False}, "state_stale"),
        ({"current": None}, "state_unavailable"),
        ({"available_modes": None}, "available_modes_unknown"),
        ({"available_modes_fresh": False}, "available_modes_stale"),
        ({"available_modes": []}, "available_modes_empty"),
        ({"available_modes": [{"code": 7, "mode": None}]}, "available_modes_empty"),
        ({"available_modes_source": "unrecognized"}, "protocol_unsupported"),
        ({"available_modes": [{"mode": "redundant"}]}, "serializer_unavailable"),
        ({"available_modes": [{"code": True, "mode": "redundant"}]}, "serializer_unavailable"),
        ({"available_modes": [{"code": 65536, "mode": "redundant"}]}, "serializer_unavailable"),
    ],
)
def test_native_redundancy_control_rejects_unusable_observations(change, reason):
    parsed = _choice_status((1, "Switched"), (7, "Redundant"))
    state = switch_configuration_fields(parsed)["dante_redundancy"]
    state.update(change)

    assert reason in core.redundancy_control(state, "redundant")["reasons"]


def test_native_redundancy_control_does_not_treat_unknown_choices_as_available_modes():
    state = _flag_state()
    state["available_modes"] = [{"mode": None}]
    device = _device()
    device.dante_redundancy = state

    assert "available_modes_empty" in core.redundancy_control(state)["reasons"]
    assert not operation_availability(device, "redundancy").writable


def test_native_redundancy_control_uses_flag_serializer_without_a_choice_code():
    control = core.redundancy_control(_flag_state(), "redundant")

    assert control["reasons"] == []
    assert control["serializer_cohort"] == "interface_status_flags"
    assert control["switch_configuration_choice"] is None
    assert "requested_mode_not_advertised" in core.redundancy_control(_flag_state(), "split_redundant")["reasons"]


@pytest.mark.parametrize("mode", ["bogus", True, 1, []])
def test_native_redundancy_control_rejects_invalid_requested_modes(mode):
    with pytest.raises(core.NetaudioCoreError):
        core.redundancy_control(_flag_state(), mode)


def test_flag_serializer_cannot_express_a_split_mode_even_if_listed():
    state = _flag_state()
    state["available_modes"].append({"mode": "split_redundant", "code": 2})

    assert "serializer_unavailable" in core.redundancy_control(state, "split_redundant")["reasons"]


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


def test_missing_read_only_applicability_cannot_authorize_mutation():
    device = _device()
    device.switch_redundancy_read_only = None
    device.redundancy_read_only_source = None
    device.redundancy_advertised_support_source.pop("record_protocol_version")

    assert "read_only_unknown" in operation_availability(device, "redundancy", "redundant").reasons


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
    snapshot = redundancy_snapshot(device)

    assert snapshot["current_mode"] is None
    assert snapshot["current_mode_evidence"] == {
        "status": "unknown_raw",
        "mode": None,
        "raw_code": 99,
        "raw_label": "Future Mode",
        "raw_choice_hexadecimal": parsed["choices"][1]["raw_choice_hexadecimal"],
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
    before = deepcopy(device.dante_redundancy["available_modes"])
    application = SimpleNamespace(probe_switch_configuration=AsyncMock(side_effect=CapabilityProbeTimeout("timed out")))

    assert await probe_switch_configuration_if_reported(application, device) is None
    snapshot = redundancy_snapshot(device)
    assert device.dante_redundancy["available_modes"] == before
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
