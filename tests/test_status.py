import pytest

from netaudio.commands.status import (
    DANTE_STATUS_HEADERS,
    _dante_row_from_device,
    _has_presence,
    _shure_row,
    _visible_shure_summaries,
)
from netaudio.dante.device import DanteDevice


def make_status_device() -> DanteDevice:
    device = DanteDevice(server_name="AD4D-fd4e13.local.")
    device.name = "ad4d"
    device.online = False
    device.manufacturer = "Shure Inc."
    device.dante_model = "AD4D"
    device.model_id = "Bklyn2"
    device.mac_address = "000eddfd4e130000"
    device.ipv4 = "192.168.1.37"
    device.tx_count = 64
    device.rx_count = 1
    device.sample_rate = 48_000
    device.encoding = 24
    device.latency = 1.0
    device.clock_role = "Follower"
    device.is_locked = None
    device.last_seen = 1_700_000_000.0
    return device


def test_dante_status_headers_read_left_to_right():
    assert DANTE_STATUS_HEADERS == [
        "Name",
        "Status",
        "Kind",
        "Manufacturer",
        "Model",
        "IP Address",
        "TX",
        "RX",
        "Sample Rate",
        "Encoding",
        "Latency",
        "Clock",
        "Lock",
        "Last Seen",
    ]


def test_dante_status_row_leaves_unqueried_lock_blank_and_labels_failed_lock_query():
    device = make_status_device()
    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(device)))
    assert row["Lock"] == ""

    device.failed_queries.add("is_locked")
    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(device)))
    assert row["Lock"] == "unknown"

    device.is_locked = False
    device.failed_queries.discard("is_locked")
    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(device)))
    assert row["Lock"] == "unlocked"


def test_dante_status_row_shows_offline_and_last_seen():
    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(make_status_device())))

    assert row["Name"] == "ad4d"
    assert row["Status"] == "offline"
    assert row["Kind"] == "hardware"
    assert row["TX"] == "64"
    assert row["RX"] == "1"
    assert row["Last Seen"] != ""


def test_dante_status_row_tags_emulated_devices():
    device = make_status_device()
    device.mac_address = "5254001234560000"

    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(device)))

    assert row["Kind"] == "emulated"


def test_dante_status_row_uses_shared_formatters_for_audio_columns():
    row = dict(zip(DANTE_STATUS_HEADERS, _dante_row_from_device(make_status_device())))

    assert row["Sample Rate"] == "48 kHz"
    assert row["Encoding"] == "PCM24"
    assert row["Latency"] == "1 ms"


@pytest.mark.asyncio
async def test_gather_status_verbose_uses_device_list_layout(monkeypatch):
    from netaudio import _common as common_module
    from netaudio.commands import status as status_module
    from netaudio.commands.device_display import device_list_headers, device_list_row
    from netaudio.daemon import client as daemon_client

    device = make_status_device()
    device.online = True

    async def load_display_devices():
        return {device.server_name: device}

    monkeypatch.setattr(common_module, "_load_display_devices", load_display_devices)
    monkeypatch.setattr(daemon_client, "daemon_is_accessible", lambda: False)

    headers, rows, shure_rows, json_data = await status_module._gather_status(verbose=True)

    assert headers == device_list_headers(True)
    assert rows == [device_list_row(device.server_name, device, verbose=True)]
    assert shure_rows == []
    assert list(json_data["dante"]) == [device.server_name]

    headers, rows, _, _ = await status_module._gather_status(verbose=False)

    assert headers == DANTE_STATUS_HEADERS
    assert rows == [_dante_row_from_device(device)]


def test_offline_device_without_last_seen_is_not_shown():
    assert _has_presence(False, None) is False
    assert _has_presence(False, 1_700_000_000.0) is True
    assert _has_presence(True, None) is True
    assert _has_presence(None, None) is True


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


def test_visible_shure_summaries_keep_offline_rows_that_have_last_seen():
    summaries = {
        "offline-no-seen": {"name": "AD4D-A", "online": False},
        "offline-seen": {"name": "AD4D-A-seen", "online": False, "last_seen": 1_700_000_000.0},
        "live": {"name": "P10T", "online": True, "last_seen": 1_700_000_001.0},
    }

    visible = _visible_shure_summaries(summaries)

    assert [entry["name"] for entry in visible] == ["AD4D-A-seen", "P10T"]
