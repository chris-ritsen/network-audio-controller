from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.clock_control import preview_clock_configuration
from netaudio.monitoring import IssueEngine, IssueKind
from tests.status_test_support import application_with_device, receive_packets


def status(**overrides):
    return {
        "status_supported": True,
        "record_revision": 0x073A,
        "clock_capabilities": 0x0204,
        "extension_flags": 0,
        "clock_source_code": 0,
        "preferred_leader": False,
        "clock_subdomain": [0] * 16,
        "servo_state_code": 3,
        "synchronization": "synchronized",
        "mute_flags": 0,
        "mute_reasons": [],
        **overrides,
    }


def device_and_app():
    app, device = application_with_device("clock.local.", "192.0.2.1")
    device.clock_status = status()
    return app, device


@pytest.mark.parametrize(
    "revision,caps,flags",
    [
        (0x0100, 0x204, 0),
        (0x0101, 0x204, 0),
        (0x0602, 8, 0),
        (0x0603, 8, 0),
        (0x071E, 0x200, 0),
        (0x071F, 0x200, 0),
        (0x072D, 0, None),
        (0x072E, 0, None),
        (0x073A, 0x204, 0),
        (0x073A, 0x204, 0x1000),
        (0x073A, 0x120, 0),
        (0x073A, None, 0),
    ],
)
def test_native_clock_permissions_match_command_acceptance(revision, caps, flags):
    observed = status(record_revision=revision, clock_capabilities=caps, extension_flags=flags)
    permissions = core.clock_control_availability(observed)

    for field, value in {
        "clock_source": 0,
        "preferred_leader": True,
        "subdomain": [0] * 16,
        "global_unicast_delay_requests": True,
        "aggregate_ptpv1_unicast_delay_requests": True,
    }.items():
        command = {
            "command": "clock_control",
            "host_mac": "020000000001",
            "message_id": 1,
            "control": {
                "status_revision": revision,
                "clock_capabilities": caps,
                "extension_flags": flags,
                field: value,
            },
        }

        if permissions[field]:
            assert core.build_command(command)
        else:
            with pytest.raises(core.NetaudioCoreError):
                core.build_command(command)

    assert not any(core.clock_control_availability({**observed, "status_supported": False}).values())


def test_serialized_clock_permissions_are_derived_by_core():
    _, device = device_and_app()
    assert device.to_json()["clock_control_availability"] == core.clock_control_availability(device.clock_status)


@pytest.mark.parametrize(
    "observed,requested,expected",
    [
        (status(), {}, True),
        (status(), {"clock_source": 0, "preferred_leader": False}, True),
        (status(), {"clock_source": 1}, False),
        (status(preferred_leader=True), {"preferred_leader": False}, False),
        (status(preferred_leader=0), {"preferred_leader": False}, False),
        (status(clock_source_code=False), {"clock_source": 0}, False),
        (status(), {"subdomain": [0] * 16}, True),
        (status(clock_subdomain=bytes(16)), {"subdomain": [0] * 16}, True),
        (status(), {"subdomain": [1] + [0] * 15}, False),
        (status(), {"global_unicast_delay_requests": False}, False),
        (status(global_unicast_delay_requests=False), {"global_unicast_delay_requests": False}, True),
        (status(aggregate_ptpv1_unicast_delay_requests=True), {"aggregate_ptpv1_unicast_delay_requests": True}, True),
        (status(status_supported=False), {}, False),
        ({}, {}, False),
        (status(status_supported=1), {}, False),
    ],
)
def test_native_clock_readback_requires_supported_status_and_exact_values(observed, requested, expected):
    assert core.clock_configuration_matches(observed, requested) is expected


@pytest.mark.parametrize(
    "requested",
    [
        {"unknown_setting": True},
        {"preferred_leader": None},
        {"preferred_leader": 1},
        {"clock_source": True},
        {"clock_source": -1},
        {"clock_source": 65536},
        {"subdomain": "house"},
        {"subdomain": [0] * 15},
        {"subdomain": [256] + [0] * 15},
    ],
)
def test_native_clock_readback_rejects_invalid_requested_state(requested):
    with pytest.raises(core.NetaudioCoreError):
        core.clock_configuration_matches(status(), requested)


@pytest.mark.asyncio
async def test_one_mutation_with_delayed_readback():
    app, device = device_and_app()
    app.probe_clocking_status = AsyncMock(side_effect=[status(), status(), status(preferred_leader=True)])
    app._send_settings = AsyncMock()
    result = await app.set_clock_configuration(device, {"preferred_leader": True}, timeout=1)
    assert result["effective_state_confirmed"] is True
    assert result["request_acknowledged"] is None
    assert result["persistence"] == "unknown"
    assert app.probe_clocking_status.await_count == 3
    app._send_settings.assert_awaited_once()
    packet = core.build_command(app._send_settings.call_args.args[1] | {"host_mac": "020000000001"})
    assert packet[32:40] == bytes.fromhex("0002000001000000")


@pytest.mark.asyncio
async def test_unchanged_configuration_does_not_send():
    app, device = device_and_app()
    app.probe_clocking_status = AsyncMock(return_value=status())
    app._send_settings = AsyncMock()
    result = await app.set_clock_configuration(device, {"preferred_leader": False})
    assert result["request_sent"] is False
    app._send_settings.assert_not_awaited()


@pytest.mark.parametrize("caps,flags", [(0x20, 0), (0x100, 0), (0x204, 0x1000), (None, 0)])
@pytest.mark.asyncio
async def test_preferred_capability_gates_prevent_sending(caps, flags):
    app, device = device_and_app()
    app.probe_clocking_status = AsyncMock(return_value=status(clock_capabilities=caps, extension_flags=flags))
    app._send_settings = AsyncMock()
    with pytest.raises(RuntimeError, match="permit"):
        await app.set_clock_configuration(device, {"preferred_leader": True})
    app._send_settings.assert_not_awaited()


def test_external_sources_require_independent_capability_and_subdomain_terminator():
    _, device = device_and_app()
    with pytest.raises(RuntimeError):
        preview_clock_configuration(device, status(clock_source_code=1), {"clock_source": 2})
    device.supported_clock_sources = [1, 2]
    assert preview_clock_configuration(device, status(), {"clock_source": 1})["changes"] == {"clock_source": 1}
    with pytest.raises(core.NetaudioCoreError, match="15 bytes"):
        preview_clock_configuration(device, status(), {"subdomain": "a" * 16})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed,changes",
    [
        ({"record_revision": 0x0101}, {"preferred_leader": True}),
        ({"clock_capabilities": True}, {"preferred_leader": True}),
        ({"clock_capabilities": 0x10000}, {"preferred_leader": True}),
        ({"extension_flags": True}, {"preferred_leader": True}),
        ({"clock_capabilities": 0}, {"subdomain": "house"}),
        ({"record_revision": 0x0602, "clock_capabilities": 8}, {"global_unicast_delay_requests": True}),
        ({"clock_capabilities": 0x208}, {"global_unicast_delay_requests": True}),
        ({"record_revision": 0x071E}, {"aggregate_ptpv1_unicast_delay_requests": True}),
        ({"clock_capabilities": 4}, {"aggregate_ptpv1_unicast_delay_requests": True}),
    ],
)
async def test_native_clock_constraints_reject_preview_before_transport(observed, changes):
    app, device = device_and_app()
    device.clock_status = status(**observed)
    app.probe_clocking_status = AsyncMock(return_value=device.clock_status)
    app._send_settings = AsyncMock()

    with pytest.raises(RuntimeError):
        await app.set_clock_configuration(device, changes)

    app._send_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_devices_fail_closed_before_direct_clock_queries():
    app, device = device_and_app()
    device.management_state = "managed"
    app.probe_clocking_status = AsyncMock()
    app._send_settings = AsyncMock()
    with pytest.raises(RuntimeError, match="managed"):
        await app.set_clock_configuration(device, {"clock_source": 0})
    app.probe_clocking_status.assert_not_awaited()
    app._send_settings.assert_not_awaited()


def snapshot(second, observation=None, **changes):
    when = datetime(2026, 9, 18, tzinfo=timezone.utc) + timedelta(seconds=second)
    return {
        "device_identity": "clock.local.",
        "server_name": "clock.local.",
        "name": "Clock",
        "online": True,
        "clock_status": status(**changes),
        "clock_observed_at": when.isoformat() if observation is None else observation,
    }, when.isoformat()


def observe(engine, second, **changes):
    value, timestamp = snapshot(second, **changes)
    engine.observe_snapshot(value, timestamp=timestamp)
    return engine.list_issues(state="open")


def test_sync_and_mute_need_five_continuous_seconds_and_missing_is_not_recovery():
    engine = IssueEngine()
    issues = observe(
        engine,
        0,
        servo_state_code=2,
        synchronization="lost",
        mute_flags=3,
        mute_reasons=["synchronization loss", "external-clock problem"],
    )
    assert {issue.kind for issue in issues} == {IssueKind.CLOCK_SYNCHRONIZATION, IssueKind.CLOCK_MUTED}
    assert len(observe(engine, 1)) == 2
    value, timestamp = snapshot(4)
    value["clock_status"] = None
    engine.observe_snapshot(value, timestamp=timestamp)
    assert all(i.observation_state.value == "unobservable" for i in engine.list_issues(state="open"))
    assert len(observe(engine, 5)) == 2
    assert len(observe(engine, 9.99)) == 2
    assert observe(engine, 10) == []


def test_offline_and_stale_do_not_resolve_existing_issues():
    engine = IssueEngine()
    observe(engine, 0, servo_state_code=2, synchronization="lost", mute_flags=1)
    value, timestamp = snapshot(1)
    value["online"] = False
    engine.observe_snapshot(value, timestamp=timestamp)
    assert len(engine.list_issues(state="open")) == 2
    assert len(observe(engine, 2)) == 2
    value, timestamp = snapshot(20)
    value["clock_observed_at"] = snapshot(2)[1]
    engine.observe_snapshot(value, timestamp=timestamp)
    assert all(i.observation_state.value == "unobservable" for i in engine.list_issues(state="open"))
    assert len(observe(engine, 21)) == 2
    assert observe(engine, 26) == []


def test_auxiliary_diagnostics_never_update_clock_or_confirm_a_write():
    app, device = device_and_app()
    previous = deepcopy(device.clock_status)
    packet = bytes.fromhex(
        "ffff0030001a00000200000000010000417564696e617465072400240000000000010008001000000000000000030000"
    )
    receive_packets(app, [packet], (str(device.ipv4), 8700))
    assert device.clock_status == previous
    assert device.clock_diagnostics["clock_unicast_status"]["raw_words"] == [0x10008, 0x100000, 0, 0x30000]
    assert not core.clock_configuration_matches(
        device.clock_diagnostics["clock_unicast_status"], {"preferred_leader": True}
    )


def test_framed_but_malformed_clock_observation_invalidates_freshness():
    from tests.test_clock_port_records import CLOCK_STATUS_PACKET

    app, device = device_and_app()
    truncated = CLOCK_STATUS_PACKET[:-1]
    packet = truncated[:2] + len(truncated).to_bytes(2, "big") + truncated[4:]
    receive_packets(app, [packet], (str(device.ipv4), 8700))
    assert device.clock_status["status_supported"] is False
    assert device.clock_observed_at is None


def test_unchanged_locked_settings_are_skipped_and_unknown_fields_rejected():
    _, device = device_and_app()
    preview = preview_clock_configuration(device, status(extension_flags=0x1000), {"preferred_leader": False})
    assert preview["changes"] == {}
    with pytest.raises(core.NetaudioCoreError, match="Unsupported"):
        preview_clock_configuration(device, status(), {"unknown_clock_setting": 2})


@pytest.mark.asyncio
async def test_journal_confirms_effective_state_without_claiming_ack_or_persistence():
    from netaudio.monitoring import MonitoringEventJournal, MonitoringEventKind, MutationAuditRecorder

    app, device = device_and_app()
    journal = MonitoringEventJournal()
    app.operation_recorder = MutationAuditRecorder.from_journal(journal)
    app.probe_clocking_status = AsyncMock(side_effect=[status(), status(preferred_leader=True)])
    app._send_settings = AsyncMock()
    await app.set_clock_configuration(device, {"preferred_leader": True})
    events = journal.list_events(kind=MonitoringEventKind.CONFIGURATION_OPERATION)
    assert {event.lifecycle_phase for event in events} == {"requested", "effective_state_confirmed"}
    assert all(event.persistence_confirmation is None for event in events)


@pytest.mark.asyncio
async def test_http_clock_configuration_returns_confirmation_separately():
    from tests.http_api_test_support import make_http_server, post

    _, device = device_and_app()
    server = make_http_server({device.server_name: device})
    server.application.set_clock_configuration = AsyncMock(
        return_value={
            "request_sent": True,
            "request_acknowledged": None,
            "effective_state_confirmed": True,
            "persistence": "unknown",
            "status": status(),
        }
    )
    code, result = await post(
        server, "/set-clock-configuration", {"device": device.server_name, "changes": {"preferred_leader": True}}
    )
    assert code == 200
    assert result["request_acknowledged"] is None
    assert result["status"]["synchronization"] == "synchronized"
    assert result["persistence"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,parameters,method",
    [
        ("/set-clock-configuration", {"changes": {"preferred_leader": True}}, "set_clock_configuration"),
        ("/set-clock-source", {"clock_source": 1}, "set_clock_source"),
        ("/set-preferred-leader", {"preferred": True}, "set_preferred_leader"),
        ("/set-clock-subdomain", {"subdomain": "house"}, "set_clock_subdomain"),
        ("/refresh-clock", {}, "probe_clocking_status"),
    ],
)
@pytest.mark.parametrize("failure,expected_status", [("revision", 409), ("timeout", 500)])
async def test_http_clock_native_rejection_is_a_conflict_not_server_failure(
    path, parameters, method, failure, expected_status
):
    import json

    from tests.http_api_test_support import FakeWriter, make_http_server

    _, device = device_and_app()
    server = make_http_server({device.server_name: device})

    async def rejected(*args, **kwargs):
        if failure == "timeout":
            raise core.NetaudioCoreError(core.STATUS_TIMEOUT, "clock transport")

        core.clock_control_profile({"control_profile": 0x0724})

    setattr(server.application, method, rejected)
    writer = FakeWriter()
    body = json.dumps({"device": device.server_name, **parameters}).encode()

    await server._route("POST", path, body, writer, None)
    code, result = writer.response()

    assert code == expected_status

    if failure == "revision":
        assert "Unsupported" in result["error"]

    assert writer.closed


def test_serialized_clock_synchronization_becomes_unknown_when_stale():
    _, device = device_and_app()
    device.clock_observed_at = "2000-01-01T00:00:00+00:00"
    result = device.to_json()["clock_status"]
    assert result["synchronization"] == "unknown"
    assert result["observed_synchronization"] == "synchronized"
    assert result["observation_state"] == "stale"
    assert device.clock_status["synchronization"] == "synchronized"


def test_journal_keeps_ports_with_unavailable_interface_indices():
    from netaudio.monitoring import MonitoringEventJournal, MonitoringEventKind
    from tests.test_event_journal import snapshot as journal_snapshot, observe as journal_observe

    journal = MonitoringEventJournal()
    original = journal_snapshot()
    original["clock_port_records"][0]["network_interface_index"] = None
    journal_observe(journal, original, 0)
    changed = deepcopy(original)
    changed["clock_port_records"][0]["user_disabled"] = True
    events = journal_observe(journal, changed, 1)
    assert any(event.kind is MonitoringEventKind.PTP_PORT_STATE_CHANGED for event in events)


@pytest.mark.parametrize("command", ["clock_master_query", "clock_unicast_control", "clock_identifier_query"])
def test_auxiliary_writers_are_unsupported(command):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({"command": command, "host_mac": "020000000001"})


@pytest.mark.parametrize(
    "facts,expected",
    [
        ({}, 0x073A),
        ({"control_profile": 0x073A}, 0x073A),
        ({"control_profile": 0x0734}, 0x0734),
    ],
)
def test_native_clock_profile_is_local(facts, expected):
    assert core.clock_control_profile(facts) == expected


@pytest.mark.parametrize(
    "facts",
    [
        {"clock_revision": True, "model_revision": 0x073A},
        {"clock_revision": 0, "interface_revision": 0x073A},
        {"model_revision": 65536},
        {"explicit_revision": -1},
        {"clock_revision": 0x073A, "explicit_revision": 0x0724},
        {"unexpected_revision": 0x073A},
    ],
)
def test_native_clock_revision_never_guesses_past_invalid_evidence(facts):
    with pytest.raises(core.NetaudioCoreError):
        core.clock_control_profile(facts)


@pytest.mark.parametrize("subdomain", ["house", [104, 111, 117, 115, 101], b"house"])
def test_native_clock_plan_normalizes_and_roundtrips_readback(subdomain):
    plan = core.plan_clock_configuration({"status": status(), "changes": {"subdomain": subdomain}})
    normalized = list(b"house" + bytes(11))

    assert plan["requested"] == plan["changes"] == {"subdomain": normalized}
    assert plan["before"] == {"subdomain": [0] * 16}
    packet = core.build_command(
        {"command": "clock_control", "control": plan["control"], "host_mac": "020000000001", "message_id": 1}
    )

    assert packet[40:56] == bytes(normalized)
    assert core.clock_configuration_matches(status(clock_subdomain=normalized), plan["requested"])


@pytest.mark.parametrize(
    "changes",
    [
        {"subdomain": "a" * 16},
        {"subdomain": "a\0b"},
        {"subdomain": [True]},
        {"subdomain": "𝄞"},
        {"clock_source": True},
        {"preferred_leader": 1},
        {"unknown": None},
    ],
)
def test_native_clock_plan_rejects_invalid_changes(changes):
    with pytest.raises(core.NetaudioCoreError):
        core.plan_clock_configuration({"status": status(), "changes": changes})


def test_native_clock_plan_does_not_treat_numeric_false_as_unchanged():
    plan = core.plan_clock_configuration({"status": status(preferred_leader=0), "changes": {"preferred_leader": False}})

    assert plan["changes"] == {"preferred_leader": False}


def test_native_clock_plan_uses_local_profile_and_requires_fresh_status():
    plan = core.plan_clock_configuration({"status": status(), "changes": {}, "control_profile": 0x0734})
    assert plan["control"]["control_profile"] == 0x0734
    assert plan["control"]["status_revision"] == 0x073A

    with pytest.raises(core.NetaudioCoreError, match="supported clock status"):
        core.plan_clock_configuration({"status": status(status_supported=False), "changes": {}})
