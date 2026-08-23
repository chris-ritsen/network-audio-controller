from types import SimpleNamespace

from netaudio.commands.status import (
    _dante_row_from_device,
    _has_presence,
    _shure_row,
    _visible_shure_summaries,
)


def test_offline_device_without_last_seen_is_not_shown():
    assert _has_presence(False, None) is False
    assert _has_presence(False, 1_700_000_000.0) is True
    assert _has_presence(True, None) is True
    assert _has_presence(None, None) is True


def test_visible_shure_summaries_keep_offline_rows_that_have_last_seen():
    summaries = {
        "offline-no-seen": {"name": "AD4D-A", "online": False},
        "offline-seen": {"name": "AD4D-A-seen", "online": False, "last_seen": 1_700_000_000.0},
        "live": {"name": "P10T", "online": True, "last_seen": 1_700_000_001.0},
    }

    visible = _visible_shure_summaries(summaries)

    assert [entry["name"] for entry in visible] == ["AD4D-A-seen", "P10T"]


def test_shure_status_row_uses_wireless_type_not_dante_clock_columns():
    row = _shure_row(
        {
            "name": "P10T",
            "online": True,
            "model": "p10t",
            "ip": "192.168.1.12",
            "device_type": "p10t",
            "channels": {"1": {}, "2": {}},
            "last_seen": 1_700_000_000.0,
        }
    )

    assert row[0] == "P10T"
    assert row[1] == "online"
    assert row[4] == "transmitter"
    assert row[5] == "2"
    assert row[6] != ""


def test_dante_status_row_shows_offline_and_last_seen():
    device = SimpleNamespace(
        name="ad4d",
        online=False,
        manufacturer="Shure Inc.",
        dante_model="AD4D",
        model_id="Bklyn2",
        ipv4="192.168.1.37",
        tx_channels={},
        rx_channels={},
        tx_count=64,
        rx_count=1,
        clock_role="Follower",
        is_locked=None,
        last_seen=1_700_000_000.0,
    )

    row = _dante_row_from_device(device)

    assert row[0] == "ad4d"
    assert row[1] == "offline"
    assert row[9] != ""
