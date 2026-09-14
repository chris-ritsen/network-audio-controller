from netaudio.daemon.http.mcp_views import (
    clock_status_view,
    compact,
    device_summary,
    device_view,
    events_view,
    issues_view,
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
        "model": "",
        "model_id": "DIOBT",
        "name": "avio-bt-1",
        "server_name": "ddm:lab:abc",
    }
    view = device_view(managed, ["summary"])
    assert view["model"] == "DIOBT"
    assert "server_name" not in view
    assert view["clock"] == {
        "frequency_offset_parts_per_billion": 0,
        "locked": "LOCKED",
        "mute_status": "NOT_MUTED",
        "preferred_leader": False,
        "role": "Leader",
    }


def test_clock_status_groups_devices_by_domain_and_names_leaders():
    payload = {
        "lx-dante.local.": {**DEVICE, "clock_identity": "001dc1081258", "inventory_id": "ddm:lab:unenrolled:1"},
        "avio-bt-1.local.": {
            "clock_role": "Follower",
            "inventory_id": "ddm:lab:unenrolled:2",
            "leader_clock_identity": "001dc1081258",
            "name": "avio-bt-1",
            "online": True,
        },
        "ddm:lab:x:y": {
            "ddm_clocking_state": {"grand_leader": True, "locked": "LOCKED"},
            "inventory_id": "ddm:lab:x:y",
            "management_state": "managed",
            "name": "wing-4e4701",
            "online": True,
        },
    }
    view = clock_status_view(payload, {})
    assert list(view["domains"]) == ["ddm:lab", "unmanaged"]
    assert view["domains"]["ddm:lab"]["leaders"] == ["wing-4e4701"]
    assert view["domains"]["unmanaged"]["leaders"] == ["lx-dante"]
    follower = view["domains"]["unmanaged"]["devices"][0]
    assert follower["name"] == "avio-bt-1" and follower["leader"] == "lx-dante"


def test_device_summary_lists_only_problem_routes():
    summary = device_summary(DEVICE)
    assert summary["name"] == "lx-dante"
    assert summary["subscription_count"] == 2
    assert summary["subscription_problems"] == [
        "wireless-mic:1 <- ad4d:01: Unresolved (The transmitting device isn't on the network.)"
    ]
    assert "channels" not in summary and "unused" not in summary


def test_device_view_sections():
    view = device_view(DEVICE, ["summary", "channels", "subscriptions", "network", "availability"])
    assert view["clock"] == {"preferred_leader": True, "role": "Leader"}
    assert view["server_name"] == "lx-dante.local."
    assert view["channels"] == {
        "rx": {"1": "wireless-mic:1", "2": "wireless-mic:2", "10": "adat:left"},
        "tx": {"1": "01"},
    }
    assert view["subscriptions"][0]["problem"] == UNRESOLVED["detail"]
    assert "problem" not in view["subscriptions"][1]
    assert view["network"]["interfaces"] == [{"interface": "primary", "ip_address": "192.168.1.108"}]
    assert view["operation_availability"] == {"identify": "writable", "sample_rate": "read-only: capability_unknown"}
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
    assert view["matched"] == 2 and view["total"] == 3
    view = issues_view(payload, {"state": "all", "device": "wing", "limit": 1})
    assert [issue["issue_id"] for issue in view["issues"]] == ["c"]
    assert view["matched"] == 2 and view["returned"] == 1


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
