from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.transmit_flow import TransmitFlowSpecification
from netaudio.monitoring import MonitoringEventJournal, MonitoringEventKind, MutationAuditRecorder, remote_recorder
from netaudio.presets.loading import (
    PresetAction,
    PresetActionState,
    PresetDeviceActions,
    PresetLoadPlan,
    apply_preset_plan,
)


def device() -> DanteDevice:
    value = DanteDevice("receiver.local.")
    value.name = "Receiver"
    value.online = True
    value.rx_channels = {}
    value.tx_channels = {}
    return value


def operation_events(journal: MonitoringEventJournal):
    return list(reversed(journal.list_events(kind=MonitoringEventKind.CONFIGURATION_OPERATION)))


@pytest.mark.asyncio
async def test_acknowledgement_without_readback_is_partial_and_never_confirmed():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    handle = await recorder.begin_operation(target, "subscribe_external_rtp", {"receiver_channel_ids": [1]})

    await recorder.complete_operation(
        target,
        handle,
        {
            "state": "request_acknowledged",
            "request_acknowledgement": {"accepted": True, "result_code": 1},
            "effective_state_confirmation": None,
        },
    )

    events = operation_events(journal)
    assert [event.lifecycle_phase for event in events] == [
        "requested",
        "request_acknowledged",
        "partial_unobservable",
    ]
    assert all(event.final_operation_state != "confirmed" for event in events)


@pytest.mark.asyncio
async def test_operation_evidence_is_redacted_and_bounded_before_persistence():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    handle = await recorder.begin_operation(
        target,
        "set_receive_flow_default_slots",
        {
            "authorization": "Bearer test-secret",
            "nested": {"credential_file": "/private/key", "value": "x" * 3000},
            "items": list(range(80)),
        },
    )

    await recorder.complete_operation(
        target,
        handle,
        {
            "state": "unavailable",
            "message": "readback unavailable",
            "verification_observations": [
                {"token": "test-token", "polls": list(range(80))},
            ],
        },
    )

    requested, terminal = operation_events(journal)
    assert requested.requested_values["authorization"] == "[redacted]"
    assert requested.requested_values["nested"]["credential_file"] == "[redacted]"
    assert requested.requested_values["nested"]["value"].endswith("…")
    assert len(requested.requested_values["nested"]["value"]) == 2049
    assert requested.requested_values["items"][-1] == {"_truncated": 16}
    assert terminal.raw["verification_observations"][0]["token"] == "[redacted]"
    assert terminal.raw["verification_observations"][0]["polls"][-1] == {"_truncated": 16}


@pytest.mark.asyncio
async def test_storage_acknowledgement_does_not_claim_persistence_confirmation():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    handle = await recorder.begin_operation(target, "store_current_configuration", {})

    await recorder.complete_operation(
        target,
        handle,
        {
            "state": "request_acknowledged",
            "persistence_request_acknowledgement": {"accepted": True, "result_code": 1},
            "persistence_confirmation": None,
        },
    )

    event = operation_events(journal)[-1]
    assert event.lifecycle_phase == "persistence_request_acknowledged"
    assert event.persistence_request_acknowledgement["accepted"] is True
    assert event.persistence_confirmation is None


@pytest.mark.asyncio
async def test_unrelated_topology_evidence_does_not_make_a_confirmed_operation_inconsistent():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    target.clock_role = "Follower"
    journal.observe_device(target)
    handle = await recorder.begin_operation(target, "create_transmit_flow", {"name": "Program"})
    target.clock_role = "Leader"

    await recorder.complete_operation(
        target,
        handle,
        {
            "state": "confirmed",
            "effective_state_confirmation": True,
            "effective": {"name": "Program"},
            "verification_observations": [
                {"outcome": "matched", "details": {"concurrent_topology_activity": {"other_flow": 9}}}
            ],
        },
    )

    assert operation_events(journal)[-1].lifecycle_phase == "effective_state_confirmed"
    assert operation_events(journal)[-1].final_operation_state != "inconsistent"
    [topology_event] = journal.list_events(kind=MonitoringEventKind.CLOCK_ROLE_CHANGED)
    assert topology_event.current_value == "Leader"


@pytest.mark.asyncio
async def test_rejected_and_unobservable_results_have_distinct_terminal_phases():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    rejected = await recorder.begin_operation(target, "set_unicast_performance", {"frames": 4})
    await recorder.complete_operation(
        target,
        rejected,
        {
            "state": "rejected",
            "request_acknowledgement": {"accepted": False, "result_code": 5},
        },
    )
    unavailable = await recorder.begin_operation(target, "set_unicast_performance", {"frames": 8})
    await recorder.complete_operation(target, unavailable, {"state": "unavailable"})

    terminals = [
        event
        for event in operation_events(journal)
        if event.operation_id in {rejected.operation_id, unavailable.operation_id}
    ]
    assert [
        (event.operation_id, event.lifecycle_phase) for event in terminals if event.lifecycle_phase != "requested"
    ] == [
        (rejected.operation_id, "request_rejected"),
        (unavailable.operation_id, "partial_unobservable"),
    ]


@pytest.mark.asyncio
async def test_preset_children_share_parent_correlation_and_unchanged_actions_are_summary_only():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    application = SimpleNamespace(
        operation_recorder=recorder,
        set_preferred_leader=AsyncMock(),
        probe_preferred_leader_state=AsyncMock(return_value=True),
    )
    plan = PresetLoadPlan(
        [
            PresetDeviceActions(
                actions=[
                    PresetAction("preferred_leader", True, PresetActionState.CHANGE, current=False),
                    PresetAction("encoding", 24, PresetActionState.UNCHANGED, current=24),
                ],
                config={"preferred_leader": True, "encoding": 24},
                device=target,
                device_name="Receiver",
                server_name=target.server_name,
            )
        ]
    )

    await apply_preset_plan(application, plan)

    parent_events = list(reversed(journal.list_events(kind=MonitoringEventKind.PRESET_RUN)))
    child_events = operation_events(journal)
    parent_id = parent_events[0].operation_id
    assert parent_id is not None
    assert {event.correlation_id for event in child_events} == {parent_id}
    assert {event.parent_preset_run_id for event in child_events} == {parent_id}
    assert {event.operation_name for event in child_events} == {"preferred_leader"}
    summary = parent_events[-1].effective_values
    assert summary["confirmed"] == 1
    assert summary["unchanged"] == 1


@pytest.mark.asyncio
async def test_preset_summary_counts_actions_instead_of_grouped_transport_requests():
    journal = MonitoringEventJournal(path=None)
    recorder = MutationAuditRecorder.from_journal(journal)
    target = device()
    advertised = {
        ("192.0.2.1", 1): SimpleNamespace(source_ipv4="192.0.2.1", session_id=1),
        ("192.0.2.2", 2): SimpleNamespace(source_ipv4="192.0.2.2", session_id=2),
    }
    application = SimpleNamespace(
        operation_recorder=recorder,
        external_flows=SimpleNamespace(get=lambda source, session_id: advertised.get((source, session_id))),
        subscribe_external_rtp=AsyncMock(
            return_value={"request_acknowledged": True, "result_code": 1},
        ),
    )
    plan = PresetLoadPlan(
        [
            PresetDeviceActions(
                actions=[
                    PresetAction(
                        "external_receiver_subscriptions",
                        [
                            (
                                1,
                                {
                                    "flow_identity": {"source_ipv4": "192.0.2.1", "session_id": 1},
                                    "flow_slot": 1,
                                },
                            ),
                            (
                                2,
                                {
                                    "flow_identity": {"source_ipv4": "192.0.2.2", "session_id": 2},
                                    "flow_slot": 1,
                                },
                            ),
                        ],
                        PresetActionState.CHANGE,
                    ),
                ],
                config={},
                device=target,
                device_name="Receiver",
                server_name=target.server_name,
            )
        ]
    )

    await apply_preset_plan(application, plan)

    parent_events = list(reversed(journal.list_events(kind=MonitoringEventKind.PRESET_RUN)))
    assert parent_events[-1].effective_values == {
        "confirmed": 0,
        "partial": 1,
        "inconsistent": 0,
        "rejected": 0,
        "unavailable": 0,
        "failed": 0,
        "unchanged": 0,
        "skipped": 0,
    }


@pytest.mark.asyncio
async def test_persisted_transitions_are_published_once_and_reload_after_restart(tmp_path):
    path = tmp_path / "event-journal.json"
    journal = MonitoringEventJournal(path)
    publish = AsyncMock()
    recorder = MutationAuditRecorder.from_journal(journal, publish=publish)
    target = device()
    handle = await recorder.begin_operation(target, "remove_subscription_associations", {"channels": [1]})
    await recorder.complete_operation(
        target,
        handle,
        {
            "state": "request_acknowledged",
            "request_acknowledgement": {"accepted": True, "result_code": 0},
        },
    )

    persisted = operation_events(journal)
    published = [call.args[0] for call in publish.await_args_list]
    assert [event.sequence for event in published] == [event.sequence for event in persisted]
    assert len({event.sequence for event in published}) == len(persisted)
    restored = MonitoringEventJournal(path)
    assert [event.to_dict() for event in operation_events(restored)] == [event.to_dict() for event in persisted]


def test_schema_v1_events_without_operation_fields_still_load(tmp_path):
    path = tmp_path / "event-journal.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "events": [
                    {
                        "sequence": 1,
                        "timestamp": "2026-09-12T12:00:00Z",
                        "kind": "clock_role_changed",
                        "severity": "info",
                        "device_identity": "receiver.local.",
                        "device_name": "Receiver",
                        "server_name": "receiver.local.",
                        "previous_value": "Follower",
                        "current_value": "Leader",
                        "raw": {},
                        "observation_source": "device_clock_status",
                        "derivation_status": "observed",
                    }
                ],
            }
        )
    )

    [event] = MonitoringEventJournal(path).list_events()
    assert event.operation_id is None
    assert event.kind is MonitoringEventKind.CLOCK_ROLE_CHANGED


@pytest.mark.asyncio
async def test_remote_recorder_serializes_preset_scope_for_the_daemon_sink():
    payloads = []

    async def append(payload):
        payloads.append(payload)

    recorder = remote_recorder(append)
    scope = {"device_identity": "preset-run:one", "name": "Preset", "server_name": ""}
    handle = await recorder.begin_operation(
        scope,
        "apply_preset",
        {"action_count": 1},
        operation_id="one",
        transport="preset",
        kind=MonitoringEventKind.PRESET_RUN,
    )
    await recorder.complete_operation(
        scope,
        handle,
        {"state": "confirmed", "effective_state_confirmation": True},
    )

    assert [payload["lifecycle_phase"] for payload in payloads] == ["requested", "effective_state_confirmed"]
    assert all(payload["device_snapshot"] == scope for payload in payloads)


@pytest.mark.asyncio
async def test_application_configuration_surfaces_delegate_to_one_recorder(monkeypatch):
    calls = []

    class Recorder:
        async def run_operation(
            self,
            target,
            operation_name,
            requested_values,
            operation,
            *,
            transport=None,
            result_adapter=None,
        ):
            calls.append(operation_name)
            result = await operation()
            return result

    application = DanteApplication.__new__(DanteApplication)
    application.operation_recorder = Recorder()
    application.commands = MagicMock()
    application.mutate_and_wait_for_notification = AsyncMock(return_value=b"response")
    target = device()
    target.platform_software_version = "4.0.0"
    target.settings_properties = []
    flow = SimpleNamespace(source_ipv4="192.0.2.1", session_id=7)
    result = SimpleNamespace(to_dict=lambda: {"state": "partial"}, verification_observations=())

    import netaudio.dante.flow_lifecycle as lifecycle
    import netaudio.dante.flows as flows
    import netaudio.dante.performance_configuration as performance

    monkeypatch.setattr(lifecycle, "create_transmit_flow", AsyncMock(return_value=result))
    monkeypatch.setattr(lifecycle, "delete_transmit_flow", AsyncMock(return_value=result))
    monkeypatch.setattr(flows, "subscribe_external_rtp", AsyncMock(return_value={"request_acknowledged": True}))
    for name in (
        "set_receive_flow_performance",
        "set_transmit_flow_performance",
        "set_unicast_performance",
        "set_receive_flow_default_slots",
        "store_current_configuration",
    ):
        monkeypatch.setattr(performance, name, AsyncMock(return_value=result))

    specification = TransmitFlowSpecification.from_dict(
        {
            "media_mode": "native_dante",
            "flow_type": "multicast",
            "channel_slots": [{"slot": 1, "transmitter_channel": 1}],
            "sample_rate_hz": 48000,
            "encoding_bits": 24,
            "redundancy": "device_default",
            "protocol": {"protocol_id": 0x2729, "cohort": "legacy_2729"},
        }
    )
    await application.create_transmit_flow(target, specification)
    await application.delete_transmit_flow(target, 1)
    await application.subscribe_external_rtp(
        target,
        flow,
        [1],
        [1],
        receiver_supports_multiple_interfaces=False,
    )
    await application.remove_subscriptions(target, [1])
    await application.set_receive_flow_performance(target, 1000, 4)
    await application.set_transmit_flow_performance(target, 1000, 4)
    await application.set_unicast_performance(target, 1000, 4)
    await application.set_receive_flow_default_slots(target, 8)
    await application.store_current_configuration(target)

    assert calls == [
        "create_transmit_flow",
        "delete_transmit_flow",
        "subscribe_external_rtp",
        "remove_subscription_associations",
        "set_receive_flow_performance",
        "set_transmit_flow_performance",
        "set_unicast_performance",
        "set_receive_flow_default_slots",
        "store_current_configuration",
    ]


@pytest.mark.asyncio
async def test_application_validation_failure_is_a_terminal_journal_transition():
    journal = MonitoringEventJournal(path=None)
    application = DanteApplication.__new__(DanteApplication)
    application.operation_recorder = MutationAuditRecorder.from_journal(journal)
    target = device()

    with pytest.raises((TypeError, ValueError)):
        await application.create_transmit_flow(target, {"flow_type": "invalid"})

    events = operation_events(journal)
    assert [event.lifecycle_phase for event in events] == [
        "requested",
        "transport_or_validation_failure",
    ]
    assert events[-1].raw["exception_type"] in {"TypeError", "ValueError"}
