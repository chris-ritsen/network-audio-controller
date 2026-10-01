from netaudio.daemon.http.mcp_views import (
    clock_status_view,
    compact,
    device_summary,
    device_view,
    events_view,
    issues_view,
    routing_view,
    signal_levels_view,
)

CONNECTED = {"severity": "ok", "label": "Subscribed (unicast)", "detail": "Active", "icon": "\x1b[32m●\x1b[0m"}
UNRESOLVED = {"severity": "error", "label": "Unresolved", "detail": "The transmitting device isn't on the network."}
DEVICE = {
    "channels": {
        "receivers": {"1": {"name": "wireless-mic:1"}, "2": {"name": "wireless-mic:2"}, "10": {"name": "adat:left"}},
        "transmitters": {"1": {"name": "01"}},
    },
    "clock_role": "Leader",
    "encoding": 24,
    "interfaces": [{"interface": "primary", "ip_address": "192.168.1.108", "gateway": None}],
    "ipv4": "192.168.1.108",
    "is_locked": False,
    "kind": "hardware",
    "latency_ms": 1.0,
    "management_state": "unenrolled",
    "manufacturer": "Digigram",
    "model": "LX-DANTE",
    "name": "lx-dante",
    "online": True,
    "operation_availability": {
        "identify": {"writable": True, "reasons": []},
        "sample_rate": {"writable": False, "reasons": ["capability_unknown"]},
    },
    "preferred_leader": True,
    "rx_count": 128,
    "sample_rate_hz": 48000,
    "server_name": "lx-dante.local.",
    "subscriptions": [
        {"rx_channel": "wireless-mic:1", "tx_channel": "01", "tx_device": "ad4d", "status": UNRESOLVED},
        {"rx_channel": "adat:left", "tx_channel": "adat-1", "tx_device": "a32", "status": CONNECTED},
        {
            "rx_channel": "wireless-mic:2",
            "tx_channel": "wireless-mic:2",
            "tx_device": None,
            "status": {"state": "none", "severity": "none", "label": "Not subscribed"},
        },
    ],
    "tx_count": 128,
    "unused": None,
}


def test_compact_drops_nulls_empty_strings_and_icons():
    assert compact({"a": None, "b": {"icon": "x", "c": [None, {"d": "", "e": 1}]}}) == {"b": {"c": [None, {"e": 1}]}}


def test_managed_device_summary_uses_ddm_clock_and_model_fallbacks():
    managed = {
        "ddm_clock_preferences": {"leader": False},
        "ddm_clocking_state": {
            "grand_leader": True,
            "locked": "LOCKED",
            "frequency_offset": 0,
            "mute_status": "NOT_MUTED",
        },
        "inventory_id": "ddm:lab:abc",
        "management_state": "managed",
        "model": "",
        "model_id": "DIOBT",
        "name": "avio-bt-1",
        "server_name": "ddm:lab:abc",
    }
    view = device_view(managed, ["summary"])
    assert view["model"] == "DIOBT"
    assert "server_name" not in view
    assert view["clock"] == {
        "ddm_frequency_offset": 0,
        "locked": "LOCKED",
        "mute_status": "NOT_MUTED",
        "preferred_leader": False,
        "role": "Leader",
        "role_source": "ddm",
        "synchronization": "synchronized",
    }


def test_clock_status_groups_devices_by_domain_and_names_leaders():
    payload = {
        "lx-dante.local.": {**DEVICE, "ptpv1_device_uuid": "001dc1081258", "inventory_id": "ddm:lab:unenrolled:1"},
        "avio-bt-1.local.": {
            "clock_role": "Follower",
            "inventory_id": "ddm:lab:unenrolled:2",
            "ptpv1_master_uuid": "001dc1081258",
            "name": "avio-bt-1",
            "online": True,
        },
        "Windows-PC.local.": {
            "ddm_clocking_state": {"grand_leader": False, "locked": "LOCKED", "frequency_offset": 0},
            "inventory_id": "ddm:lab:unenrolled:3",
            "name": "Windows-PC",
            "online": True,
        },
        "ddm:lab:x:y": {
            "ddm_clocking_state": {"grand_leader": True, "locked": "LOCKED"},
            "inventory_id": "ddm:lab:x:y",
            "management_state": "managed",
            "name": "wing-4e4701",
            "online": True,
        },
        "ddm:lab:x:z": {
            "ddm_clocking_state": {"grand_leader": False, "locked": "LOCKED"},
            "inventory_id": "ddm:lab:x:z",
            "management_state": "managed",
            "name": "avio-input-2",
            "online": True,
        },
        "unnamed.local.": {"inventory_id": "ddm:lab:unenrolled:4", "name": None},
    }
    view = clock_status_view(payload, {})
    assert view["domains"]["unmanaged"]["devices"][-1] == {
        "leader": "lx-dante",
        "leader_evidence": "domain",
        "name": "unnamed.local.",
    }
    assert list(view["domains"]) == ["ddm:lab:x", "unmanaged"]
    assert view["domains"]["ddm:lab:x"]["leaders"] == ["wing-4e4701"]
    assert view["domains"]["unmanaged"]["leaders"] == ["lx-dante"]
    unmanaged = {entry["name"]: entry for entry in view["domains"]["unmanaged"]["devices"]}
    assert "ddm_frequency_offset" not in unmanaged["Windows-PC"]
    assert unmanaged["avio-bt-1"]["leader"] == "lx-dante" and unmanaged["avio-bt-1"]["leader_evidence"] == "reported"
    assert unmanaged["Windows-PC"]["leader"] == "lx-dante" and unmanaged["Windows-PC"]["leader_evidence"] == "domain"
    managed = {entry["name"]: entry for entry in view["domains"]["ddm:lab:x"]["devices"]}
    assert managed["avio-input-2"]["leader"] == "wing-4e4701" and managed["avio-input-2"]["leader_evidence"] == "domain"
    assert "leader" not in managed["wing-4e4701"]


def test_device_summary_lists_only_problem_routes():
    summary = device_summary(DEVICE)
    assert summary["name"] == "lx-dante"
    assert summary["subscription_count"] == 2
    assert summary["subscription_problems"] == [
        "wireless-mic:1 ← 01 on ad4d: The transmitting device isn't on the network."
    ]
    assert "channels" not in summary and "unused" not in summary


def test_device_view_sections():
    view = device_view(DEVICE, ["summary", "channels", "subscriptions", "network", "availability"])
    assert view["clock"] == {"preferred_leader": True, "role": "Leader", "role_source": "direct"}
    assert view["server_name"] == "lx-dante.local."
    assert view["channels"] == {
        "rx": {"1": "wireless-mic:1", "2": "wireless-mic:2", "10": "adat:left"},
        "tx": {"factory names": "1"},
    }
    assert view["subscriptions"][0]["problem"] == UNRESOLVED["detail"]
    assert "problem" not in view["subscriptions"][1]
    assert len(view["subscriptions"]) == 2 and view["subscription_count"] == 2
    assert view["network"]["interfaces"] == [{"interface": "primary", "ip_address": "192.168.1.108"}]
    assert view["operation_availability"] == {
        "identify": "writable",
        "sample_rate": "unconfirmed, a write will be tried: capability_unknown",
    }
    assert device_view(DEVICE, ["full"])["channels"]["receivers"]["1"] == {"name": "wireless-mic:1"}


def test_issues_view_filters_state_device_and_limit():
    payload = {
        "count": 3,
        "active_count": 2,
        "issues": [
            {
                "issue_id": "a",
                "state": "open",
                "last_seen": "2026-09-14T10:00:00Z",
                "occurrence_count": 504,
                "scope": {"device_name": "lx-dante"},
            },
            {"issue_id": "b", "state": "open", "last_seen": "2026-09-14T11:00:00Z", "scope": {"device_name": "wing"}},
            {
                "issue_id": "c",
                "state": "resolved",
                "last_seen": "2026-09-14T12:00:00Z",
                "scope": {"device_name": "wing"},
            },
        ],
    }
    view = issues_view(payload, {"state": "open", "limit": 50})
    assert [issue["issue_id"] for issue in view["issues"]] == ["b", "a"]
    assert "occurrence_count" not in view["issues"][1]
    assert view["matched"] == 2 and view["total"] == 3
    view = issues_view(payload, {"state": "all", "device": "wing", "limit": 1})
    assert [issue["issue_id"] for issue in view["issues"]] == ["c"]
    assert view["matched"] == 2 and view["returned"] == 1
    assert view["next_offset"] == 1
    assert [
        issue["issue_id"]
        for issue in issues_view(payload, {"state": "all", "device": "wing", "limit": 1, "offset": 1})["issues"]
    ] == ["b"]


def test_events_view_is_newest_first():
    payload = {
        "count": 2,
        "events": [
            {"sequence": 1, "kind": "late_packets", "device_name": "lx-dante", "raw": {"big": True}},
            {"sequence": 2, "kind": "latency", "device_name": "wing", "raw": {"big": True}},
        ],
    }
    view = events_view(payload, {"limit": 50})
    assert [event["sequence"] for event in view["events"]] == [2, 1]
    assert "raw" not in view["events"][0]
    assert events_view(payload, {"kind": "latency", "limit": 50})["matched"] == 1
    assert events_view(payload, {"device": "lx-dante", "limit": 50})["events"][0]["sequence"] == 1
    assert events_view(payload, {"limit": 1})["next_before_sequence"] == 2
    assert events_view(payload, {"limit": 1, "before_sequence": 2})["events"][0]["sequence"] == 1


def test_clock_and_device_defaults_hide_protocol_records_but_debug_retains_them():
    raw = {"raw_record": [1] * 4000, "synchronization": "synchronized", "clock_state": "disciplined"}
    device = {**DEVICE, "clock_status": raw, "ptpv1_device_uuid": "device-uuid"}
    payload = {"lx-dante.local.": device}
    entry = clock_status_view(payload, {})["domains"]["unmanaged"]["devices"][0]
    assert entry["synchronization"] == "synchronized"
    assert "clock_state" not in entry
    assert "notes" not in clock_status_view(payload, {})
    assert "clock_status" not in entry and "ptpv1_device_uuid" not in entry
    assert "raw_record" not in str(device_view(device, ["summary"]))
    assert (
        clock_status_view(payload, {"detail": "debug"})["raw_clock_records"]["lx-dante.local."]["clock_status"] == raw
    )
    assert device_view(device, ["full"])["clock_status"] == raw


def test_event_default_summarizes_issue_evidence_and_debug_preserves_it():
    issue = {
        "issue_id": "one",
        "state": "open",
        "summary": "Missing transmitter",
        "raw_source_fields": {"raw": [1] * 4000},
    }
    payload = {"count": 1, "events": [{"sequence": 7, "kind": "issue_updated", "current_value": issue}]}
    compact_event = events_view(payload, {"limit": 50})["events"][0]
    assert compact_event["current_value"] == {"issue_id": "one", "state": "open", "summary": "Missing transmitter"}
    assert compact_event["debug_detail_available"] is True
    assert events_view(payload, {"limit": 50, "detail": "debug"})["events"][0]["current_value"] == issue


def test_event_repeats_collapse_without_hiding_distinct_issues():
    events = [
        {
            "sequence": number,
            "timestamp": f"2026-09-12T12:00:0{number}Z",
            "kind": "late_packet_count_increased",
            "device_name": "lx-dante",
            "flow_identity": "flow-1",
            "current_value": number,
        }
        for number in (1, 2, 3)
    ]
    events.append(
        {
            "sequence": 4,
            "kind": "issue_updated",
            "device_name": "lx-dante",
            "current_value": {"issue_id": "a", "summary": "Missing source"},
        }
    )
    payload = {"count": 4, "events": events}
    view = events_view(payload, {})
    assert view["matched"] == 2 and view["observation_count"] == 4
    assert [event["kind"] for event in view["events"]] == ["issue_updated", "late_packet_count_increased"]
    assert view["events"][1]["observation_count"] == 3
    assert view["events"][1]["current_value"] == 3
    assert len(events_view(payload, {"collapse_repeats": False})["events"]) == 4


def test_meter_view_uses_core_scale_and_leaves_raw_codes_for_debug():
    payload = {"device.local.": {"metering_source": "signal_presence", "tx": {"1": 193, "2": 254}}}
    view = signal_levels_view(payload, {})["device"]
    assert view["tx"]["1"]["state"] == "below_threshold"
    assert view["tx"]["2"]["state"] == "mute_or_floor"
    assert "raw" not in view["tx"]["1"]
    assert signal_levels_view(payload, {"detail": "debug"})["device"]["tx"]["1"]["raw"] == 193


def test_routing_view_exposes_receive_number_and_label_with_problem_status():
    payload = {"lx-dante.local.": DEVICE}
    routes = routing_view(payload, {})["routes"]["lx-dante"]
    assert len(routes) == 2
    assert routes[0] == f"1 wireless-mic:1 ← 01 on ad4d: Unresolved, {UNRESOLVED['detail']}"
    assert routing_view(payload, {"status": "problem"})["count"] == 1
    assert routing_view(payload, {"status": "ok"})["count"] == 1
    assert routing_view(payload, {"rx_channel": 10})["routes"]["lx-dante"] == ["10 adat:left ← adat-1 on a32"]
    duplicate_names = {
        **DEVICE,
        "channels": {"receivers": {"1": "same", "2": "same"}},
        "subscriptions": [{"rx_channel": "same", "tx_device": "sender", "tx_channel": "01"}],
    }
    assert routing_view({"dup.local.": duplicate_names}, {})["routes"]["lx-dante"] == ["same ← 01 on sender"]
