from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from netaudio.cli import app
from netaudio.commands import events as events_cli
from netaudio.daemon.server import NetaudioDaemon
from netaudio.dante.events import DanteEvent, EventType
from netaudio.monitoring import MonitoringEventJournal
from tests.http_api_test_support import FakeWriter, get, make_http_server, post

runner = CliRunner()


def journal_snapshot(role: str = "Follower") -> dict:
    return {
        "server_name": "receiver.local.",
        "name": "Receiver",
        "online": True,
        "clock_role": role,
        "leader_clock_identity": "001dc1000001",
    }


@pytest.mark.asyncio
async def test_http_event_journal_filters_and_clear_is_local_only():
    journal = MonitoringEventJournal()
    initial = journal_snapshot()
    journal.observe_snapshot(initial, timestamp="2026-09-11T12:00:00Z")
    changed = deepcopy(initial)
    changed["clock_role"] = "Leader"
    journal.observe_snapshot(changed, timestamp="2026-09-11T12:00:01Z")
    server = make_http_server()
    server.event_journal = journal

    status, payload = await get(server, "/event-journal?kind=clock_role_changed&severity=info&limit=1")
    assert status == 200
    assert payload["count"] == 1
    assert payload["events"][0]["raw"]["current_clock_state"]["clock_role"] == "Leader"

    writer = FakeWriter()
    await server._dispatch("DELETE", "/event-journal", None, writer)
    status, payload = writer.response()
    assert status == 200
    assert payload == {"cleared": 1, "device_commands_sent": 0, "scope": "local_event_journal"}
    server.application.identify.assert_not_awaited()
    server.application.reboot.assert_not_awaited()
    server.application.set_interface.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_event_journal_rejects_invalid_filter():
    server = make_http_server()
    status, payload = await get(server, "/event-journal?kind=not_an_event")
    assert status == 400
    assert "not_an_event" in payload["error"]


@pytest.mark.asyncio
async def test_loopback_operation_event_endpoint_persists_and_publishes_once():
    server = make_http_server()
    server.publish_journal_event = AsyncMock()
    payload = {
        "kind": "configuration_operation",
        "operation_id": "operation-1",
        "correlation_id": "operation-1",
        "parent_preset_run_id": None,
        "operation_name": "set_receive_flow_default_slots",
        "lifecycle_phase": "requested",
        "requested_values": {"default_slots": 8},
        "acknowledgement_result_code": None,
        "transport": "direct",
        "effective_values": None,
        "final_operation_state": "requested",
        "persistence_request_acknowledgement": None,
        "persistence_confirmation": None,
        "evidence": {"request": {"default_slots": 8}},
        "observation_source": "user_request",
        "derivation_status": "derived",
        "interface_identity": None,
        "channel_identity": None,
        "flow_identity": None,
        "severity": "info",
        "device_snapshot": {
            "server_name": "receiver.local.",
            "name": "Receiver",
        },
        "observe_device": False,
    }

    status, response = await post(server, "/event-journal/operations", payload)

    assert status == 200
    [event] = server.event_journal.list_events()
    assert response["event"] == event.to_dict()
    server.publish_journal_event.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_daemon_device_update_observes_and_publishes_journal_events():
    device = SimpleNamespace(server_name="receiver.local.")
    journal_event = object()
    daemon = object.__new__(NetaudioDaemon)
    daemon.application = SimpleNamespace(devices={"receiver.local.": device})
    daemon.event_journal = MagicMock()
    daemon.event_journal.observe_device.return_value = [journal_event]
    daemon.http_api = SimpleNamespace(publish_journal_event=AsyncMock())
    daemon._publish_device_to_redis = AsyncMock()
    daemon._republish_correlated_shure = AsyncMock()

    await daemon._on_device_updated(
        DanteEvent(
            type=EventType.DEVICE_UPDATED,
            device_name="Receiver",
            server_name="receiver.local.",
        )
    )

    daemon.event_journal.observe_device.assert_called_once_with(device)
    daemon.http_api.publish_journal_event.assert_awaited_once_with(journal_event)


def test_cli_list_uses_existing_structured_output(monkeypatch):
    payload = {
        "schema_version": 1,
        "retention_limit": 1000,
        "count": 1,
        "events": [
            {
                "sequence": 1,
                "timestamp": "2026-09-11T12:00:01Z",
                "kind": "clock_role_changed",
                "severity": "info",
                "device_identity": "receiver.local.",
                "device_name": "Receiver",
                "server_name": "receiver.local.",
                "interface_identity": None,
                "channel_identity": None,
                "flow_identity": None,
                "previous_value": "Follower",
                "current_value": "Leader",
                "raw": {"current_clock_state": {"clock_role": "Leader"}},
                "observation_source": "device_clock_status",
                "derivation_status": "observed",
            }
        ],
    }
    monkeypatch.setattr(events_cli, "get_event_journal_from_daemon", AsyncMock(return_value=(200, payload)))

    result = runner.invoke(app, ["-j", "events", "list", "--kind", "clock_role_changed"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload


def test_cli_export_is_stable_json_with_raw_evidence(monkeypatch, tmp_path):
    payload = {
        "schema_version": 1,
        "retention_limit": 1000,
        "count": 1,
        "events": [{"sequence": 4, "raw": {"late_packet_count": 9}}],
    }
    fetch = AsyncMock(return_value=(200, payload))
    monkeypatch.setattr(events_cli, "get_event_journal_from_daemon", fetch)
    destination = tmp_path / "journal.json"

    result = runner.invoke(app, ["events", "export", "--file", str(destination)])
    assert result.exit_code == 0, result.output
    assert json.loads(destination.read_text()) == payload
    assert destination.read_text() == json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def test_cli_clear_reports_local_count(monkeypatch):
    clear = AsyncMock(return_value=(200, {"cleared": 7, "device_commands_sent": 0, "scope": "local_event_journal"}))
    monkeypatch.setattr(events_cli, "clear_event_journal_on_daemon", clear)

    result = runner.invoke(app, ["events", "clear"])
    assert result.exit_code == 0, result.output
    assert result.stdout == "Cleared 7 locally retained event(s).\n"
    clear.assert_awaited_once_with()
