from __future__ import annotations

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from netaudio.cli import app
from netaudio.commands import issues as issues_cli
from netaudio.monitoring import (
    IssueEngine,
    IssueKind,
    IssueLifecycleState,
    IssueTransitionKind,
    MonitoringEventJournal,
    MonitoringEventKind,
)
from tests.http_api_test_support import get, make_http_server

runner = CliRunner()


def snapshot(identity: str = "receiver.local.") -> dict:
    return {
        "server_name": identity,
        "device_identity": identity,
        "name": identity.removesuffix(".local.").title(),
        "online": True,
        "failed_queries": [],
        "subscriptions": [],
        "interfaces": [],
    }


def failed_subscription() -> dict:
    value = snapshot()
    value["subscriptions"] = [
        {
            "rx_channel_number": 1,
            "rx_channel": "Input 1",
            "tx_channel": "Output 1",
            "tx_device": "Source",
            "status": {
                "code": 15,
                "state": "error",
                "severity": "error",
                "status": "NO_CONNECTION",
                "detail": "could not contact transmitter",
            },
        }
    ]
    return value


def test_issue_lifecycle_is_stable_and_missing_evidence_does_not_infer_resolution():
    engine = IssueEngine()
    value = failed_subscription()
    [opened] = engine.observe_snapshot(value, timestamp="2026-09-12T12:00:00Z")
    issue_id = opened.current.issue_id
    assert opened.kind is IssueTransitionKind.OPENED
    assert opened.current.evidence_class.value == "direct_observation"
    assert opened.current.raw_source_fields["status"]["code"] == 15

    assert engine.observe_snapshot(value, timestamp="2026-09-12T12:00:01Z") == []
    current = engine.list_issues(state=IssueLifecycleState.OPEN)[0]
    assert current.issue_id == issue_id
    assert current.occurrence_count == 2

    missing = deepcopy(value)
    missing["subscriptions"][0].pop("status")
    assert engine.observe_snapshot(missing, timestamp="2026-09-12T12:00:02Z") == []
    unobservable = engine.list_issues(state="open")[0]
    assert unobservable.issue_id == issue_id
    assert unobservable.observation_state.value == "unobservable"
    assert unobservable.occurrence_count == 2
    assert unobservable.last_seen == "2026-09-12T12:00:01Z"

    healthy = deepcopy(missing)
    healthy["subscriptions"][0]["status"] = {"state": "connected", "severity": "ok"}
    [resolved] = engine.observe_snapshot(healthy, timestamp="2026-09-12T12:00:03Z")
    assert resolved.kind is IssueTransitionKind.RESOLVED
    assert resolved.current.issue_id == issue_id
    assert resolved.current.resolved_at == "2026-09-12T12:00:03Z"
    assert resolved.current.last_seen == "2026-09-12T12:00:01Z"
    assert engine.list_issues(state="open") == []
    assert engine.list_issues(state="resolved")[0].issue_id == issue_id


def test_disappearance_preserves_failure_until_fresh_healthy_observation():
    engine = IssueEngine()
    failed = failed_subscription()
    [opened] = engine.observe_snapshot(failed, timestamp="2026-09-12T12:00:00Z")

    offline = deepcopy(failed)
    offline["online"] = False
    assert engine.observe_snapshot(offline, timestamp="2026-09-12T12:00:01Z") == []
    [current] = engine.list_issues(state="open")
    assert current.issue_id == opened.current.issue_id
    assert current.observation_state.value == "unobservable"
    assert current.occurrence_count == 1
    assert current.last_seen == "2026-09-12T12:00:00Z"

    missing = snapshot()
    assert engine.observe_snapshot(missing, timestamp="2026-09-12T12:00:02Z") == []
    [current] = engine.list_issues(state="open")
    assert current.observation_state.value == "unobservable"
    assert current.last_seen == "2026-09-12T12:00:00Z"

    healthy = failed_subscription()
    healthy["subscriptions"][0]["status"] = {"state": "connected", "severity": "ok"}
    [resolved] = engine.observe_snapshot(healthy, timestamp="2026-09-12T12:00:03Z")
    assert resolved.kind is IssueTransitionKind.RESOLVED
    assert resolved.current.resolved_at == "2026-09-12T12:00:03Z"
    assert resolved.current.last_seen == "2026-09-12T12:00:00Z"


def test_unrelated_device_observation_does_not_refresh_or_resolve_local_issue():
    engine = IssueEngine()
    [opened] = engine.observe_snapshot(failed_subscription(), timestamp="2026-09-12T12:00:00Z")

    assert engine.observe_snapshot(snapshot("healthy.local."), timestamp="2026-09-12T12:00:10Z") == []

    [current] = engine.list_issues(state="open")
    assert current.issue_id == opened.current.issue_id
    assert current.occurrence_count == 1
    assert current.last_seen == "2026-09-12T12:00:00Z"


def test_engine_covers_device_health_configuration_and_telemetry_categories():
    value = failed_subscription()
    value.update(
        {
            "interfaces": [
                {
                    "interface": "primary",
                    "active": {"ip_address": "192.0.2.10", "netmask": "255.255.255.0"},
                    "configured": {
                        "mode": "static",
                        "ip_address": "192.0.2.20",
                        "netmask": "255.255.255.0",
                        "gateway": "198.51.100.1",
                    },
                },
                {
                    "interface": "secondary",
                    "active": {"ip_address": "192.0.2.11", "netmask": "255.255.255.0"},
                    "configured": {"mode": "static", "ip_address": "192.0.2.11", "netmask": "255.255.255.0"},
                },
            ],
            "ddm_clocking_state": {"locked": "UNLOCKED", "follower_without_leader": True},
            "receiver_flow_connection_health": {
                "fresh": False,
                "flows": [{"receiver_flow_slot": 2, "connected": False, "error_code": 7}],
            },
            "network_interface_traffic": {"fresh": False, "interfaces": []},
            "is_licensed": False,
            "safe_mode": True,
            "upgrade_required": True,
            "interface_reboot_required": True,
            "configuration_partition_status": "failed",
            "persistence_status": False,
            "failed_queries": ["clock status"],
            "requested_sample_rate": 96000,
            "sample_rate_hz": 48000,
        }
    )
    engine = IssueEngine()
    engine.observe_snapshot(value, timestamp="2026-09-12T12:00:00Z", emit_transitions=False)
    kinds = {issue.kind for issue in engine.list_issues(state="open")}

    assert {
        IssueKind.SUBNET_CONFLICT,
        IssueKind.CLOCK_SYNCHRONIZATION,
        IssueKind.SUBSCRIPTION_FAILURE,
        IssueKind.RECEIVER_HEALTH_DEGRADED,
        IssueKind.LICENSING_FAILURE,
        IssueKind.SAFE_STATE,
        IssueKind.UPGRADE_REQUIRED,
        IssueKind.REBOOT_REQUIRED,
        IssueKind.CONFIGURATION_PARTITION_FAULT,
        IssueKind.PERSISTENCE_FAULT,
        IssueKind.TELEMETRY_STALE,
        IssueKind.TELEMETRY_MISSING,
        IssueKind.CONFIGURATION_DIVERGENCE,
    } <= kinds


def test_cross_device_address_and_pullup_conflicts_reconcile_for_each_device():
    engine = IssueEngine()
    first = snapshot("first.local.")
    first["interfaces"] = [{"interface": "primary", "active": {"ip_address": "192.0.2.9", "netmask": "255.255.255.0"}}]
    first["sample_rate_pullup_raw_value"] = 0
    second = snapshot("second.local.")
    second["interfaces"] = deepcopy(first["interfaces"])
    second["sample_rate_pullup_raw_value"] = 1

    assert engine.observe_snapshot(first, timestamp="2026-09-12T12:00:00Z") == []
    transitions = engine.observe_snapshot(second, timestamp="2026-09-12T12:00:01Z")
    opened_kinds = {item.current.kind for item in transitions if item.kind is IssueTransitionKind.OPENED}
    assert opened_kinds == {IssueKind.ADDRESS_CONFLICT, IssueKind.PULLUP_MISMATCH}
    assert len(engine.list_issues(kind="address_conflict", state="open")) == 2
    assert len(engine.list_issues(kind="pullup_mismatch", state="open")) == 2

    second["interfaces"][0]["active"]["ip_address"] = "192.0.2.10"
    second["sample_rate_pullup_raw_value"] = 0
    transitions = engine.observe_snapshot(second, timestamp="2026-09-12T12:00:02Z")
    assert {item.kind for item in transitions} == {IssueTransitionKind.RESOLVED}
    assert engine.list_issues(state="open") == []
    assert len(engine.list_issues(state="resolved")) == 4


def test_journal_retains_issues_and_emits_transitions_in_memory():
    journal = MonitoringEventJournal()
    journal.observe_snapshot(snapshot(), timestamp="2026-09-12T12:00:00Z")
    failed = failed_subscription()
    events = journal.observe_snapshot(failed, timestamp="2026-09-12T12:00:01Z")
    opened = next(event for event in events if event.kind is MonitoringEventKind.ISSUE_OPENED)
    assert opened.raw["evidence_class"] == "direct_observation"
    assert opened.channel_identity == "rx:1"
    [issue] = journal.issue_engine.list_issues(state="open")
    assert issue.issue_id == opened.raw["issue_id"]
    assert issue.occurrence_count == 1

    assert journal.observe_snapshot(failed, timestamp="2026-09-12T12:00:02Z") == []
    [issue] = journal.issue_engine.list_issues(state="open")
    assert issue.occurrence_count == 2
    assert issue.last_seen == "2026-09-12T12:00:02Z"
    assert MonitoringEventJournal().issue_engine.list_issues() == []


def test_journal_keeps_unobservable_issues_without_an_event():
    journal = MonitoringEventJournal()
    failed = failed_subscription()
    journal.observe_snapshot(failed, timestamp="2026-09-12T12:00:00Z")
    failed["subscriptions"][0].pop("status")
    assert journal.observe_snapshot(failed, timestamp="2026-09-12T12:00:01Z") == []
    [issue] = journal.issue_engine.list_issues(state="open")
    assert issue.observation_state.value == "unobservable"
    assert issue.occurrence_count == 1


@pytest.mark.asyncio
async def test_http_exposes_current_and_historical_issue_filters():
    journal = MonitoringEventJournal()
    journal.observe_snapshot(failed_subscription(), timestamp="2026-09-12T12:00:00Z")
    server = make_http_server()
    server.event_journal = journal

    status, payload = await get(server, "/issues?state=open&kind=subscription_failure&severity=error")
    assert status == 200
    assert payload["active_count"] == payload["count"] == 1
    assert payload["issues"][0]["raw_source_fields"]["status"]["code"] == 15

    status, payload = await get(server, "/issues?kind=unknown")
    assert status == 400
    assert "unknown" in payload["error"]


def test_cli_lists_current_issues_with_structured_output(monkeypatch):
    issue = IssueEngine()
    issue.observe_snapshot(failed_subscription(), timestamp="2026-09-12T12:00:00Z")
    payload = issue.export(state="open")
    fetch = AsyncMock(return_value=(200, payload))
    monkeypatch.setattr(issues_cli, "get_issues_from_daemon", fetch)

    result = runner.invoke(app, ["-j", "issues", "list", "--state", "open"])
    assert result.exit_code == 0, result.output
    assert "subscription_failure" in result.stdout
    fetch.assert_awaited_once()


def test_partial_receiver_inventory_cannot_resolve_an_absent_flow_issue():
    engine = IssueEngine()
    value = snapshot()
    value["receiver_flow_completeness"] = "complete"
    value["receiver_flows"] = [{"global_flow_id": number} for number in range(1, 17)]
    value["receiver_flow_connection_health"] = {"flows": [{"receiver_flow_slot": 16, "healthy": False}]}
    transitions = engine.observe_snapshot(value, timestamp="2026-09-12T12:00:00Z")
    issue = next(
        transition.current
        for transition in transitions
        if transition.current.kind is IssueKind.RECEIVER_HEALTH_DEGRADED
    )
    partial = deepcopy(value)
    partial["receiver_flow_completeness"] = "partial"
    partial["receiver_flow_status_page"] = {
        "page_disposition": "more_pages",
        "result_code": 0x8112,
        "flows": value["receiver_flows"][:15],
    }
    partial["receiver_flow_connection_health"] = {"flows": []}
    transitions = engine.observe_snapshot(partial, timestamp="2026-09-12T12:00:01Z")
    assert all(transition.kind is not IssueTransitionKind.RESOLVED for transition in transitions)
    assert any(current.issue_id == issue.issue_id for current in engine.list_issues(state="open"))
