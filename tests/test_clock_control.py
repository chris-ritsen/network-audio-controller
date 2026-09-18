from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.clock_control import clock_configuration_matches, preview_clock_configuration
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
    with pytest.raises(RuntimeError, match="independently"):
        preview_clock_configuration(device, status(clock_source_code=1), {"clock_source": 2})
    device.supported_clock_sources = [1, 2]
    assert preview_clock_configuration(device, status(), {"clock_source": 1})["changes"] == {"clock_source": 1}
    with pytest.raises(ValueError, match="15 bytes"):
        preview_clock_configuration(device, status(), {"subdomain": "a" * 16})


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
    assert not clock_configuration_matches(device.clock_diagnostics["clock_unicast_status"], {"preferred_leader": True})


def test_malformed_clock_observation_invalidates_freshness():
    from tests.test_clock_port_records import CLOCK_STATUS_PACKET

    app, device = device_and_app()
    receive_packets(app, [CLOCK_STATUS_PACKET[:-1]], (str(device.ipv4), 8700))
    assert device.clock_status["status_supported"] is False
    assert device.clock_observed_at is None


def test_unchanged_locked_settings_are_skipped_and_unknown_fields_rejected():
    _, device = device_and_app()
    preview = preview_clock_configuration(device, status(extension_flags=0x1000), {"preferred_leader": False})
    assert preview["changes"] == {}
    with pytest.raises(ValueError, match="Unsupported"):
        preview_clock_configuration(device, status(), {"ptpv2_domain": 2})


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


def test_revision_override_cannot_replace_a_known_clock_revision():
    from netaudio.dante.clock_control import clock_record_revision

    _, device = device_and_app()
    with pytest.raises(ValueError, match="differs"):
        clock_record_revision(device, 0x0724)
    device.clock_status = None
    assert clock_record_revision(device, 0x0724) == 0x0724
