from __future__ import annotations

import json
from copy import deepcopy

import pytest

from netaudio.monitoring import (
    DerivationStatus,
    EventJournalThresholds,
    EventSeverity,
    MonitoringEventJournal,
    MonitoringEventKind,
)


def snapshot(server_name: str = "receiver.local.") -> dict:
    return {
        "server_name": server_name,
        "name": "Receiver",
        "online": True,
        "last_seen": 1_700_000_000.0,
        "clock_role": "Follower",
        "ptpv1_device_uuid": "001dc1000001",
        "ptpv1_master_uuid": "001dc1000002",
        "clock_port_state_code": 9,
        "clock_frequency_offset_parts_per_billion": 12,
        "clock_port_records": [
            {
                "record_number": 1,
                "network_interface_index": 2,
                "ptp_version": 1,
                "transport_path": "multicast",
                "state_code": 9,
                "role": "Follower",
                "link_down": False,
                "status_flags": 7,
            }
        ],
        "channels": {
            "receivers": {"1": {"name": "Input 1", "muted": False}},
            "transmitters": {},
        },
        "subscriptions": [
            {
                "rx_channel_number": 1,
                "rx_channel": "Input 1",
                "rx_device": "Receiver",
                "tx_channel": "Output 1",
                "tx_device": "Transmitter",
                "status": {"code": 9, "state": "connected", "severity": "ok", "status": "DYNAMIC"},
            }
        ],
        "receiver_flow_latency_ns": 1_000_000,
        "receiver_flow_connection_health": {
            "fresh": True,
            "latency_stream": {"sequence": 1, "observed_at": "2026-09-11T12:00:00Z", "fresh": True},
            "late_packet_stream": {"sequence": 1, "observed_at": "2026-09-11T12:00:00Z", "fresh": True},
            "flows": [
                {
                    "receiver_flow_index": 0,
                    "receiver_flow_slot": 1,
                    "current_latency_nanoseconds": 500_000,
                    "average_latency_nanoseconds": 450_000,
                    "peak_latency_nanoseconds": 600_000,
                    "late_packet_count": 5,
                    "late_packet_delta": 0,
                }
            ],
        },
        "link_speed_mbps": 100,
        "network_interface_traffic": {
            "sequence": 1,
            "interfaces": [
                {
                    "transmit_rate_raw": 625_000,
                    "receive_rate_raw": 500_000,
                    "transmit_rate_bits_per_second": 5_000_000,
                    "receive_rate_bits_per_second": 4_000_000,
                    "transmit_error_count": 2,
                    "receive_error_count": 3,
                }
            ],
        },
        "error": None,
        "failed_queries": [],
    }


def observe(journal: MonitoringEventJournal, value: dict, second: int) -> list:
    return journal.observe_snapshot(value, timestamp=f"2026-09-11T12:00:{second:02d}Z")


def test_transition_produces_one_event_and_unchanged_samples_do_not_duplicate():
    journal = MonitoringEventJournal()
    initial = snapshot()
    assert observe(journal, initial, 0) == []

    changed = deepcopy(initial)
    changed["clock_role"] = "Leader"
    [event] = observe(journal, changed, 1)
    assert event.kind is MonitoringEventKind.CLOCK_ROLE_CHANGED
    assert event.previous_value == "Follower"
    assert event.current_value == "Leader"
    assert event.derivation_status is DerivationStatus.OBSERVED
    assert observe(journal, changed, 2) == []


def test_subscription_failure_and_recovery_are_distinct_observed_events():
    journal = MonitoringEventJournal()
    initial = snapshot()
    observe(journal, initial, 0)

    failed = deepcopy(initial)
    failed["subscriptions"][0]["status"] = {
        "code": 15,
        "state": "error",
        "severity": "error",
        "status": "NO_CONNECTION",
        "detail": "receiver could not contact transmitter",
    }
    failure_events = observe(journal, failed, 1)
    failure = next(event for event in failure_events if event.kind is MonitoringEventKind.SUBSCRIPTION_FAILED)
    issue_opened = next(event for event in failure_events if event.kind is MonitoringEventKind.ISSUE_OPENED)
    assert failure.kind is MonitoringEventKind.SUBSCRIPTION_FAILED
    assert failure.severity is EventSeverity.ERROR
    assert failure.raw["current_subscription"]["status"]["code"] == 15
    assert issue_opened.raw["issue_kind"] == "subscription_failure"
    assert observe(journal, failed, 2) == []

    recovered = deepcopy(failed)
    recovered["subscriptions"][0]["status"] = deepcopy(initial["subscriptions"][0]["status"])
    recovery_events = observe(journal, recovered, 3)
    recovery = next(event for event in recovery_events if event.kind is MonitoringEventKind.SUBSCRIPTION_RECOVERED)
    issue_resolved = next(event for event in recovery_events if event.kind is MonitoringEventKind.ISSUE_RESOLVED)
    assert recovery.kind is MonitoringEventKind.SUBSCRIPTION_RECOVERED
    assert recovery.previous_value["status"] == "NO_CONNECTION"
    assert recovery.current_value["state"] == "connected"
    assert issue_resolved.current_value["state"] == "resolved"


def test_late_packet_counter_increase_and_reset_are_not_conflated():
    journal = MonitoringEventJournal()
    initial = snapshot()
    observe(journal, initial, 0)

    increased = deepcopy(initial)
    increased["receiver_flow_connection_health"]["flows"][0]["late_packet_count"] = 8
    [event] = observe(journal, increased, 1)
    assert event.kind is MonitoringEventKind.LATE_PACKET_COUNT_INCREASED
    assert event.raw["delta"] == 3

    reset = deepcopy(increased)
    reset["receiver_flow_connection_health"]["flows"][0]["late_packet_count"] = 1
    [event] = observe(journal, reset, 2)
    assert event.kind is MonitoringEventKind.LATE_PACKET_COUNTER_RESET
    assert event.raw["delta"] is None
    assert event.severity is EventSeverity.INFO


def test_interface_error_counter_increase_and_reset_preserve_raw_measurements():
    journal = MonitoringEventJournal()
    initial = snapshot()
    observe(journal, initial, 0)

    increased = deepcopy(initial)
    increased["network_interface_traffic"]["sequence"] = 2
    increased["network_interface_traffic"]["interfaces"][0]["receive_error_count"] = 5
    [event] = observe(journal, increased, 1)
    assert event.kind is MonitoringEventKind.INTERFACE_ERROR_COUNTER_INCREASED
    assert event.interface_identity == "interface:1"
    assert event.raw["counter"] == "receive_error_count"
    assert event.raw["delta"] == 2

    reset = deepcopy(increased)
    reset["network_interface_traffic"]["sequence"] = 3
    reset["network_interface_traffic"]["interfaces"][0]["receive_error_count"] = 0
    [event] = observe(journal, reset, 2)
    assert event.kind is MonitoringEventKind.INTERFACE_ERROR_COUNTER_RESET
    assert event.current_value == 0


def test_threshold_hysteresis_prevents_latency_event_storms():
    journal = MonitoringEventJournal(
        thresholds=EventJournalThresholds(flow_latency_warning_ratio=0.8, flow_latency_recovery_ratio=0.6),
    )
    value = snapshot()
    observe(journal, value, 0)

    kinds = []
    for second, latency in enumerate((810_000, 790_000, 610_000, 590_000, 610_000), start=1):
        value = deepcopy(value)
        flow = value["receiver_flow_connection_health"]["flows"][0]
        flow["current_latency_nanoseconds"] = latency
        flow["average_latency_nanoseconds"] = latency
        flow["peak_latency_nanoseconds"] = max(flow["peak_latency_nanoseconds"], latency)
        kinds.extend(event.kind for event in observe(journal, value, second))

    assert kinds == [
        MonitoringEventKind.RECEIVER_FLOW_LATENCY_HIGH,
        MonitoringEventKind.RECEIVER_FLOW_LATENCY_RECOVERED,
    ]
    event = journal.list_events(kind=MonitoringEventKind.RECEIVER_FLOW_LATENCY_HIGH)[0]
    assert event.derivation_status is DerivationStatus.DERIVED
    assert event.raw["flow_measurement"]["peak_latency_nanoseconds"] == 810_000


def test_interface_utilization_hysteresis_uses_reported_rates_and_link_speed():
    journal = MonitoringEventJournal()
    value = snapshot()
    observe(journal, value, 0)

    kinds = []
    for second, utilization in enumerate((81, 79, 61, 59), start=1):
        value = deepcopy(value)
        interface = value["network_interface_traffic"]["interfaces"][0]
        interface["transmit_rate_bits_per_second"] = utilization * 1_000_000
        interface["receive_rate_bits_per_second"] = 1_000_000
        kinds.extend(event.kind for event in observe(journal, value, second))

    assert kinds == [
        MonitoringEventKind.INTERFACE_UTILIZATION_HIGH,
        MonitoringEventKind.INTERFACE_UTILIZATION_RECOVERED,
    ]


def test_unknown_or_missing_data_does_not_create_recovery_or_change_events():
    journal = MonitoringEventJournal()
    failed = snapshot()
    failed["subscriptions"][0]["status"] = {"state": "error", "severity": "error", "status": "NO_CONNECTION"}
    observe(journal, failed, 0)

    missing = deepcopy(failed)
    missing.pop("clock_role")
    missing["subscriptions"][0].pop("status")
    missing["receiver_flow_connection_health"]["flows"][0].pop("current_latency_nanoseconds")
    assert observe(journal, missing, 1) == []

    available_again = deepcopy(missing)
    available_again["clock_role"] = "Leader"
    available_again["subscriptions"][0]["status"] = {
        "state": "connected",
        "severity": "ok",
        "status": "DYNAMIC",
    }
    [resolved] = observe(journal, available_again, 2)
    assert resolved.kind is MonitoringEventKind.ISSUE_RESOLVED
    assert resolved.raw["issue_kind"] == "subscription_failure"


def test_bounded_history_is_evicted_and_starts_empty_after_restart():
    journal = MonitoringEventJournal(max_events=2)
    value = snapshot()
    observe(journal, value, 0)
    for second, role in enumerate(("Leader", "Follower", "Leader"), start=1):
        value = deepcopy(value)
        value["clock_role"] = role
        observe(journal, value, second)

    assert [event.sequence for event in journal.list_events()] == [3, 2]
    assert MonitoringEventJournal(max_events=2).list_events() == []


def test_filtering_and_json_export_are_stable_and_preserve_raw_evidence():
    journal = MonitoringEventJournal()
    first = snapshot("first.local.")
    second = snapshot("second.local.")
    second["name"] = "Second"
    observe(journal, first, 0)
    observe(journal, second, 0)

    first_changed = deepcopy(first)
    first_changed["clock_role"] = "Leader"
    observe(journal, first_changed, 1)
    second_changed = deepcopy(second)
    second_changed["ptpv1_master_uuid"] = "001dc1000003"
    observe(journal, second_changed, 2)

    payload = journal.export(
        device="Second",
        kind="leader_identity_changed",
        severity="warning",
        since="2026-09-11T12:00:02Z",
    )
    assert payload["schema_version"] == 1
    assert payload["count"] == 1
    [event] = payload["events"]
    assert event["device_identity"] == "second.local."
    assert event["derivation_status"] == "observed"
    assert event["raw"]["current_clock_state"]["ptpv1_master_uuid"] == "001dc1000003"
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload


def test_local_clear_retains_transition_baseline_and_has_no_device_side_effect(tmp_path):
    journal = MonitoringEventJournal()
    value = snapshot()
    observe(journal, value, 0)
    changed = deepcopy(value)
    changed["clock_role"] = "Leader"
    observe(journal, changed, 1)
    device_state_before = deepcopy(changed)

    assert journal.clear() == 1
    assert journal.export()["events"] == []
    assert changed == device_state_before
    assert observe(journal, changed, 2) == []


def test_repeated_observations_and_empty_clear_keep_history_empty():
    journal = MonitoringEventJournal()
    value = snapshot()
    assert observe(journal, value, 0) == []
    assert observe(journal, value, 1) == []
    assert journal.clear() == 0
    assert journal.list_events() == []


def test_mutating_input_does_not_change_the_retained_observation():
    journal = MonitoringEventJournal()
    value = snapshot()
    assert observe(journal, value, 0) == []

    value["channels"]["receivers"]["1"]["muted"] = True
    events = observe(journal, value, 1)

    mute = next(event for event in events if event.kind is MonitoringEventKind.MUTE_STATE_CHANGED)
    assert mute.previous_value is False
    assert mute.current_value is True


def test_device_disappearance_and_reappearance_are_deduplicated():
    journal = MonitoringEventJournal()
    value = snapshot()
    observe(journal, value, 0)

    [disappeared] = journal.observe_disappearance(value, timestamp="2026-09-11T12:00:01Z")
    assert disappeared.kind is MonitoringEventKind.DEVICE_DISAPPEARED
    assert journal.observe_disappearance(value, timestamp="2026-09-11T12:00:02Z") == []

    reappeared = deepcopy(value)
    reappeared["online"] = True
    [event] = observe(journal, reappeared, 3)
    assert event.kind is MonitoringEventKind.DEVICE_REAPPEARED


def test_mute_leader_and_ptp_port_transitions_keep_subject_identity():
    journal = MonitoringEventJournal()
    initial = snapshot()
    observe(journal, initial, 0)
    changed = deepcopy(initial)
    changed["ptpv1_master_uuid"] = "001dc1000003"
    changed["channels"]["receivers"]["1"]["muted"] = True
    changed["clock_port_records"][0]["state_code"] = 3
    changed["clock_port_records"][0]["role"] = "Leader"

    events = observe(journal, changed, 1)
    assert {event.kind for event in events} == {
        MonitoringEventKind.LEADER_IDENTITY_CHANGED,
        MonitoringEventKind.MUTE_STATE_CHANGED,
        MonitoringEventKind.PTP_PORT_STATE_CHANGED,
    }
    mute = next(event for event in events if event.kind is MonitoringEventKind.MUTE_STATE_CHANGED)
    assert mute.channel_identity == "rx:1"
    port = next(event for event in events if event.kind is MonitoringEventKind.PTP_PORT_STATE_CHANGED)
    assert port.interface_identity == "ptp:1/multicast/record:1"


def test_invalid_threshold_relationships_fail_closed():
    with pytest.raises(ValueError, match="recovery < warning"):
        EventJournalThresholds(flow_latency_warning_ratio=0.6, flow_latency_recovery_ratio=0.8)


def test_device_adapter_uses_serializer_state_and_keeps_observation_errors():
    from netaudio.dante.device import DanteDevice

    device = DanteDevice(server_name="receiver.local.")
    device.name = "Receiver"
    device.clock_role = "Follower"
    device.error = RuntimeError("partial readback")
    device.failed_queries.add("clock status")
    device.ddm_status = {"errors": [{"message": "partial managed response"}]}
    journal = MonitoringEventJournal()
    assert journal.observe_device(device, timestamp="2026-09-11T12:00:00Z") == []

    device.clock_role = "Leader"
    [event] = journal.observe_device(device, timestamp="2026-09-11T12:00:01Z")
    assert event.raw["observation_context"] == {
        "ddm_status": {"errors": [{"message": "partial managed response"}]},
        "device_error": "partial readback",
        "failed_queries": ["clock status"],
    }


def test_partial_receiver_inventory_cannot_report_latency_recovery(tmp_path):
    journal = MonitoringEventJournal()
    value = snapshot()
    value["receiver_flow_completeness"] = "complete"
    observe(journal, value, 0)
    value["receiver_flow_connection_health"]["flows"][0]["current_latency_nanoseconds"] = 950_000
    observe(journal, value, 1)
    partial = deepcopy(value)
    partial["receiver_flow_completeness"] = "partial"
    partial["receiver_flow_status_page"] = {"page_disposition": "more_pages", "result_code": 0x8112, "flows": []}
    partial["receiver_flow_connection_health"]["flows"][0]["current_latency_nanoseconds"] = 100_000
    generated = observe(journal, partial, 2)
    assert all(event.kind is not MonitoringEventKind.RECEIVER_FLOW_LATENCY_RECOVERED for event in generated)
    assert partial["receiver_flow_status_page"]["result_code"] == 0x8112


def test_forgetting_releases_baselines_and_current_issues_but_keeps_bounded_events():
    journal = MonitoringEventJournal(max_events=2)
    current = snapshot()
    journal.observe_snapshot(current)
    current["channels"]["receivers"]["1"]["muted"] = True
    current["subscriptions"][0]["status"] = {"state": "error", "severity": "error", "code": 15}
    journal.observe_snapshot(current)
    events = journal.list_events()
    assert events
    assert journal.issue_engine.list_issues(state="open")
    identity = current["server_name"]
    journal.forget_device(identity)
    assert identity not in journal._snapshots
    assert identity not in journal.issue_engine._snapshots
    assert not any(key[0] == identity for key in journal._conditions)
    assert journal.issue_engine.list_issues(state="open") == []
    assert journal.list_events() == events
    assert journal.observe_snapshot(current) == []
