from netaudio.daemon.http.sse_view import SseDeviceView


def device(**fields):
    record = {"name": "lx-dante", "online": True, "sample_rate": 48000, "last_seen": 1.0}
    record.update(fields)
    return record


def patch_view():
    view = SseDeviceView.from_query({"patches": ["1"], "telemetry": ["0"]})
    view.initial_snapshot({"event": "snapshot", "devices": {"lx-dante.local.": device()}, "managed": None})
    return view


def test_clients_without_options_get_no_view():
    assert SseDeviceView.from_query({}) is None


def test_initial_snapshot_strips_telemetry():
    view = SseDeviceView.from_query({"telemetry": ["0"]})

    snapshot = view.initial_snapshot({"event": "snapshot", "devices": {"lx-dante.local.": device()}})

    assert "last_seen" not in snapshot["devices"]["lx-dante.local."]


def test_meter_events_pass_through_unchanged():
    view = patch_view()
    meter = {"event": "meter_values", "server_name": "lx-dante.local.", "tx": {"1": 3}}

    assert view.events_for(meter) == [meter]


def test_new_device_is_sent_in_full():
    view = patch_view()

    events = view.events_for({"event": "device_discovered", "server_name": "a32.local.", "device": device(name="a32")})

    assert events == [
        {
            "event": "device_discovered",
            "server_name": "a32.local.",
            "device": {"name": "a32", "online": True, "sample_rate": 48000},
        }
    ]


def test_patch_carries_only_changed_and_removed_fields():
    view = patch_view()
    updated = device(sample_rate=96000)
    del updated["online"]

    events = view.events_for({"event": "device_updated", "server_name": "lx-dante.local.", "device": updated})

    assert events == [
        {
            "event": "device_patch",
            "server_name": "lx-dante.local.",
            "changed": {"sample_rate": 96000},
            "removed": ["online"],
        }
    ]


def test_repeated_snapshot_becomes_patches_removals_and_managed_status():
    view = patch_view()
    view.events_for({"event": "device_discovered", "server_name": "a32.local.", "device": device(name="a32")})

    events = view.events_for(
        {
            "event": "snapshot",
            "devices": {"lx-dante.local.": device(online=False, last_seen=5.0)},
            "managed": {"status": {"state": "ready"}},
        }
    )

    assert events == [
        {"event": "device_removed", "server_name": "a32.local."},
        {"event": "device_patch", "server_name": "lx-dante.local.", "changed": {"online": False}, "removed": []},
        {"event": "managed_status", "managed": {"status": {"state": "ready"}}},
    ]


def test_telemetry_only_update_sends_nothing():
    view = patch_view()

    events = view.events_for(
        {"event": "device_updated", "server_name": "lx-dante.local.", "device": device(last_seen=9.0)}
    )

    assert events == []


def test_unchanged_snapshot_sends_nothing():
    view = patch_view()

    events = view.events_for(
        {"event": "snapshot", "devices": {"lx-dante.local.": device(last_seen=2.0)}, "managed": None}
    )

    assert events == []


def test_clock_observations_and_nullable_port_flags_reach_patch_clients():
    view = patch_view()
    clock = {
        "synchronization": "lost",
        "mute_flags": 3,
        "clock_port_records": [{"network_interface_index": None, "link_down": None, "user_disabled": None}],
    }
    events = view.events_for(
        {
            "event": "device_updated",
            "server_name": "lx-dante.local.",
            "device": device(clock_status=clock, clock_observed_at="2026-09-18T00:00:00Z"),
        }
    )
    assert events[0]["changed"]["clock_status"] == clock
    assert events[0]["changed"]["clock_observed_at"] == "2026-09-18T00:00:00Z"
