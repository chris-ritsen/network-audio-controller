"""Synthetic observations based on the specification; no device effects claimed."""

import time
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.panel_plan import plan_panel
from netaudio.dante.panel_state import observe_panel, panel_family, panel_snapshot
from netaudio.presets.device_controls import capture_device_controls, validate_device_controls
from netaudio.monitoring import IssueEngine, IssueKind
from tests.status_test_support import application_with_device


def device(family="bluetooth"):
    app, d = application_with_device("controls.local.", "192.0.2.5")
    d.platform_versions_record = {
        "platform_model_identifier": "DIOBT" if family == "bluetooth" else "DanteAV",
        "plugin_identifiers": [],
    }
    d.virtual_panel_supported = True
    d.video_transmission_supported = family == "dante_av"
    d.is_locked = False
    return app, d


def observation(d, category, value, age=0):
    observe_panel(
        d,
        {
            "observed_at_unix": time.time() - age,
            "record_revision": 0x738,
            "requester": 0,
            "sequence": 5,
            "observations": [{"category": category, "value": value}],
            "raw_record": [1, 2, 3],
        },
    )


def video(d, direction=0):
    value = {
        "configured": {"resolution": 16, "bit_depth": 2, "color_space": 1},
        "selection": {"manual_resolution": True, "manual_bit_depth": True, "manual_color_space": True},
        "actual": {"resolution": 16, "bit_depth": 2, "color_space": 1},
        "supported": [
            {"resolution": 16, "bit_depth": 14, "color_space": 25},
            {"resolution": 257, "bit_depth": 14, "color_space": 25},
        ],
        "direction": direction,
    }
    observation(d, "video_format", value)
    observation(d, "visca", {"capability": 1, "reply": []})
    return value


def synthetic_peer(app, d, values, *, delayed=0, wrong=False, revoke=False):
    sent = []
    reads = 0

    async def send(target, spec):
        nonlocal reads
        core.build_command(spec | {"host_mac": "020000000001"})
        sent.append(deepcopy(spec))
        request = spec["request"]
        op = request["operation"]
        selectors = (
            {1: "bluetooth_connection", 2: "bluetooth_identification", 3: "bluetooth_discovery", 4: "bluetooth_pairing"}
            if panel_family(d) == "bluetooth"
            else {
                1: "video_format",
                2: "codec_format",
                3: "video_channel",
                4: "serial",
                5: "bandwidth",
                6: "hdcp",
                7: "visca",
            }
        )
        if op.endswith("query") and "selector" in request:
            category = selectors[request["selector"]]
            if category not in values:
                return
            reads += 1
            status = {
                "requester": 99 if wrong else spec["requester"],
                "sequence": spec["sequence"],
                "record_revision": 0x738,
                "observed_at_unix": time.time(),
                "observations": [{"category": category, "value": deepcopy(values[category])}],
            }
            if delayed and reads <= delayed and category == "bluetooth_discovery":
                status["observations"][0]["value"] = 2
            app.notifications.notify_waiters("panel_status", app._control_key(d), status)
        elif op == "bluetooth_discovery":
            values[op] = 1 if request["discoverable"] else 2
        elif op == "bluetooth_identification":
            values[op] = {k: request[k] for k in ("name_source", "custom_name")}
        elif op == "video_format":
            values[op].update(configured=request["format"], selection=request["selection"])
        if revoke and op == "video_format":
            d.is_locked = True

    app._send_settings = AsyncMock(side_effect=send)
    return sent


@pytest.mark.parametrize(
    "fixture,state",
    [("avio-bt-1_bluetooth_status_connected.bin", 1), ("avio-bt-1_bluetooth_status_disconnected.bin", 2)],
)
def test_retained_bluetooth_fixtures_decode_typed_envelope(load_fixture, fixture, state):
    parsed = core.parse_response("panel_bluetooth_status", load_fixture(fixture))
    assert parsed["observations"][0]["value"]["state"] == state
    assert parsed["raw_record"] and parsed["source_identifier"]
    assert parsed["diagnostic_error"] is None


def test_advertised_panel_identity_precedes_model_and_never_uses_payload_guess():
    _, d = device()
    assert panel_family(d) == "bluetooth"
    d.platform_versions_record["plugin_identifiers"] = ["DanteAV"]
    assert panel_family(d) == "dante_av"
    d.platform_versions_record["plugin_identifiers"] = ["unknown"]
    assert panel_family(d) is None


@pytest.mark.asyncio
async def test_initial_bluetooth_queries_serialized_with_independent_sequences():
    app, d = device()
    sent = synthetic_peer(
        app,
        d,
        {
            "bluetooth_connection": {"state": 3, "peer_name": ""},
            "bluetooth_identification": {"name_source": 2, "custom_name": "é"},
            "bluetooth_discovery": 2,
            "bluetooth_pairing": 0,
        },
    )
    result = await app.inspect_device_controls(d, timeout=0.01)
    assert [s["request"]["selector"] for s in sent] == [1, 2, 3, 4]
    assert len({s["sequence"] for s in sent}) == 4
    assert result["observations"]["bluetooth_connection"]["value"]["state"] == 3
    assert d.bluetooth_connected is None


@pytest.mark.asyncio
async def test_one_mutation_delayed_queries_and_separate_evidence():
    app, d = device()
    sent = synthetic_peer(app, d, {"bluetooth_discovery": 2}, delayed=2)
    result = await app.apply_device_control(d, "bluetooth_discovery", True, timeout=0.3)
    assert result["effective_state_confirmed"]
    assert len([s for s in sent if s["request"]["operation"] == "bluetooth_discovery"]) == 1
    assert result["request_acknowledged"] is None
    assert result["persistence"] == result["media_readiness"] == "unknown"
    assert result["correlated_status"]


@pytest.mark.asyncio
async def test_wrong_requester_cannot_confirm_or_enable_mutation():
    app, d = device()
    sent = synthetic_peer(app, d, {"bluetooth_discovery": 2}, wrong=True)
    result = await app.apply_device_control(d, "bluetooth_discovery", True, timeout=0.01)
    assert result["plan"]["action"] == "unavailable"
    assert not result["request_sent"]
    assert all(s["request"]["operation"] == "bluetooth_query" for s in sent)


@pytest.mark.asyncio
async def test_managed_and_locked_fail_before_sending():
    app, d = device()
    app._send_settings = AsyncMock()
    d.management_state = "managed"
    with pytest.raises(RuntimeError, match="Managed"):
        await app.apply_device_control(d, "bluetooth_discovery", True)
    d.management_state = None
    d.is_locked = True
    with pytest.raises(RuntimeError, match="locked"):
        await app.apply_device_control(d, "bluetooth_discovery", True)
    app._send_settings.assert_not_awaited()


def test_name_source_change_not_elided_and_names_count_characters():
    _, d = device()
    observation(d, "bluetooth_identification", {"name_source": 1, "custom_name": "é" * 32})
    assert plan_panel(d, "bluetooth_identification", {"name_source": 2, "custom_name": "é" * 32})["action"] == "change"
    assert (
        plan_panel(d, "bluetooth_identification", {"name_source": 2, "custom_name": "é" * 33})["action"]
        == "unsupported"
    )


def test_stale_and_missing_not_false_and_pairing_excluded_from_presets():
    _, d = device()
    observation(d, "bluetooth_discovery", 2, age=11)
    assert plan_panel(d, "bluetooth_discovery", True)["action"] == "unavailable"
    observation(d, "bluetooth_pairing", 2)
    assert plan_panel(d, "bluetooth_pairing", "clear")["action"] == "unsupported"
    assert plan_panel(d, "bluetooth_pairing", "clear", confirm_clear=True)["action"] == "change"
    assert "bluetooth_pairing" not in capture_device_controls(d)["settings"]
    with pytest.raises(ValueError, match="Pairing"):
        validate_device_controls({"settings": {"bluetooth_pairing": "clear"}})


@pytest.mark.parametrize("direction", [0, 1, 99])
def test_video_direction_and_supported_masks(direction):
    _, d = device("dante_av")
    v = video(d, direction)
    request = {
        "format": {**v["configured"], "resolution": 257, "color_space": 16, "bit_depth": 8},
        "selection": v["selection"],
    }
    plan = plan_panel(d, "video_format", request)
    assert plan["action"] == ("change" if direction == 0 else "unsupported")
    if direction == 0:
        assert [r["operation"] for r in plan["requests"]] == ["video_format", "video_visca_format"]
        request["format"]["color_space"] = 17
        assert plan_panel(d, "video_format", request)["action"] == "unsupported"


@pytest.mark.asyncio
async def test_video_mutations_order_once_and_automatic_omits_visca():
    app, d = device("dante_av")
    v = video(d)
    values = {"video_format": v, "visca": {"capability": 1, "reply": []}}
    sent = synthetic_peer(app, d, values)
    requested = {"format": {**v["configured"], "resolution": 257}, "selection": v["selection"]}
    result = await app.apply_device_control(d, "video_format", requested, timeout=0.1)
    assert result["effective_state_confirmed"]
    assert [s["request"]["operation"] for s in sent if not s["request"]["operation"].endswith("query")] == [
        "video_format",
        "video_visca_format",
    ]
    requested = {
        "format": {"resolution": 0, "color_space": 0, "bit_depth": 0},
        "selection": {"manual_resolution": False, "manual_color_space": False, "manual_bit_depth": False},
    }
    result = await app.apply_device_control(d, "video_format", requested, timeout=0.1)
    assert result["sent_operations"] == ["video_format"]


def test_serial_and_bandwidth_guardrails():
    _, d = device("dante_av")
    video(d)
    s = {
        "baud_rate": 9600,
        "hardware_flow_control": 0,
        "software_flow_control": 0,
        "data_bits": 8,
        "parity": 0,
        "stop_bits": 1,
    }
    observation(d, "serial", s)
    assert plan_panel(d, "serial", {**s, "data_bits": 7})["action"] == "unsupported"
    assert plan_panel(d, "serial", {**s, "baud_rate": 12345})["action"] == "unsupported"
    observation(d, "serial", {**s, "software_flow_control": 1})
    assert plan_panel(d, "serial", s)["action"] == "unsupported"
    observation(d, "bandwidth", {"target": 100, "minimum": 50, "maximum": 800, "enabled": 1})
    assert plan_panel(d, "bandwidth", {"target": 701, "enabled": True})["action"] == "unsupported"
    assert plan_panel(d, "bandwidth", {"target": 700, "enabled": True})["action"] == "change"
    assert plan_panel(d, "bandwidth", {"target": 0, "enabled": False})["action"] == "change"
    video(d, 1)
    assert plan_panel(d, "bandwidth", {"target": 600, "enabled": True})["action"] == "unsupported"


def test_hdcp_modes_use_supported_list_and_presets_skip_noops():
    _, d = device("dante_av")
    observation(d, "hdcp", {"configured_mode": 3, "supported_modes": [1, 3, 99]})
    assert plan_panel(d, "hdcp", {"mode": 2})["action"] == "unsupported"
    assert plan_panel(d, "hdcp", {"mode": 99})["action"] == "unsupported"
    assert plan_panel(d, "hdcp", {"mode": 3})["action"] == "unchanged"
    capture = capture_device_controls(d)
    assert capture["settings"]["hdcp"] == {"mode": 3}
    assert capture["observed_extensions"]["latest_diagnostic"]["raw_record"] == [1, 2, 3]


def test_malformed_observation_retains_raw_and_marks_existing_state_unavailable():
    _, d = device()
    observation(d, "bluetooth_connection", {"state": 3, "peer_name": "private"})
    observe_panel(d, {"diagnostic_error": "malformed", "observations": [], "raw_record": [9]})
    assert not panel_snapshot(d)["observations"]["bluetooth_connection"]["fresh"]
    assert d.bluetooth_connected is None


def test_video_issue_missing_observation_does_not_resolve():
    _, d = device("dante_av")
    observation(d, "video_channel", {"status_code": 52, "direction": 0, "observed_hdcp_version": None})
    now = datetime.now(timezone.utc).isoformat()
    snap = {"device_identity": "test", "server_name": "test", "online": True, "device_controls": panel_snapshot(d)}
    engine = IssueEngine()
    transitions = engine.observe_snapshot(snap, timestamp=now)
    assert any(t.current.kind == IssueKind.VIDEO_SIGNAL_FAILURE for t in transitions)
    missing = {**snap, "device_controls": {}}
    transitions = engine.observe_snapshot(missing, timestamp=now)
    assert not any(t.current.resolved_at for t in transitions)


@pytest.mark.asyncio
async def test_permission_revocation_between_format_steps_reports_partial_without_second_write():
    app, d = device("dante_av")
    v = video(d)
    sent = synthetic_peer(app, d, {"video_format": v, "visca": {"capability": 1, "reply": []}}, revoke=True)
    result = await app.apply_device_control(
        d, "video_format", {"format": {**v["configured"], "resolution": 257}, "selection": v["selection"]}, timeout=0.01
    )
    assert result["request_sent"] and not result["effective_state_confirmed"]
    assert "locked" in result["reason"]
    assert [s["request"]["operation"] for s in sent if not s["request"]["operation"].endswith("query")] == [
        "video_format"
    ]


@pytest.mark.asyncio
async def test_http_plan_and_apply_share_categories_and_explicit_clear_confirmation():
    from tests.http_api_test_support import make_http_server, post

    _, d = device()
    server = make_http_server({d.server_name: d})
    server.application.plan_device_control = AsyncMock(return_value={"action": "unchanged", "requests": []})
    code, result = await post(
        server,
        "/device-controls",
        {"device": d.server_name, "action": "plan", "category": "bluetooth_discovery", "requested": False},
    )
    assert code == 200 and result["action"] == "unchanged"
    server.application.plan_device_control.assert_awaited_once_with(
        d, "bluetooth_discovery", False, confirm_clear=False
    )
    server.application.apply_device_control = AsyncMock(
        return_value={"request_sent": True, "effective_state_confirmed": False, "persistence": "unknown"}
    )
    code, result = await post(
        server,
        "/device-controls",
        {
            "device": d.server_name,
            "action": "apply",
            "category": "bluetooth_pairing",
            "requested": "clear",
            "confirm_clear": True,
        },
    )
    assert code == 409 and not result["success"] and result["persistence"] == "unknown"
    server.application.apply_device_control.assert_awaited_once_with(
        d, "bluetooth_pairing", "clear", confirm_clear=True
    )


@pytest.mark.asyncio
async def test_journal_effective_confirmation_does_not_claim_transport_ack_or_persistence():
    from netaudio.monitoring import MonitoringEventJournal, MonitoringEventKind, MutationAuditRecorder

    app, d = device()
    journal = MonitoringEventJournal()
    app.operation_recorder = MutationAuditRecorder.from_journal(journal)
    synthetic_peer(app, d, {"bluetooth_discovery": 2})
    result = await app.apply_device_control(d, "bluetooth_discovery", True, timeout=0.01)
    assert result["effective_state_confirmed"]
    events = journal.list_events(kind=MonitoringEventKind.CONFIGURATION_OPERATION)
    assert {e.lifecycle_phase for e in events} == {"requested", "effective_state_confirmed"}
    assert all(e.persistence_confirmation is None for e in events)


def test_journal_does_not_copy_bluetooth_peer_identity():
    from netaudio.monitoring import MonitoringEventJournal, MonitoringEventKind
    from tests.test_event_journal import snapshot, observe

    journal = MonitoringEventJournal()
    before = snapshot()
    obs = {
        "fresh": True,
        "available": True,
        "observed_at_unix": time.time(),
        "value": {"state": 1, "peer_name": "private"},
    }
    before["device_controls"] = {"observations": {"bluetooth_connection": obs}}
    observe(journal, before, 0)
    after = deepcopy(before)
    after["device_controls"]["observations"]["bluetooth_connection"]["value"]["state"] = 3
    events = observe(journal, after, 1)
    events = [e for e in events if e.kind == MonitoringEventKind.DEVICE_CONTROL_CHANGED]
    assert events and "private" not in repr(events)


def test_preset_control_categories_and_unknown_fields_roundtrip():
    from netaudio.presets.serialization import format_preset_configs
    from netaudio.presets.parsing import parse_preset_xml

    # Unknown settings are preserved as data; the planner has no writer for them.
    config = {
        "name": "Panel",
        "device_controls": {
            "settings": {"hdcp": {"mode": 3}, "future_setting": {"raw": 99}},
            "identity": {"model": "DanteAV"},
            "observed_extensions": {"raw_record": [128, 255]},
        },
    }
    content = format_preset_configs({"Panel": config})
    _, parsed = parse_preset_xml(content)
    assert parsed["Panel"]["device_controls"] == config["device_controls"]


def test_exact_advertised_model_selects_default_panel_but_never_overrides_panel_identity():
    _, d = device()
    d.platform_versions_record = {}
    d.model_id = "DIOBT"
    assert panel_family(d) == "bluetooth"
    d.platform_versions_record["plugin_identifiers"] = ["DanteAV"]
    assert panel_family(d) == "dante_av"
    d.platform_versions_record["plugin_identifiers"] = ["unsupported"]
    assert panel_family(d) is None
