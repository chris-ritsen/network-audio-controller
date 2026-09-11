from types import SimpleNamespace

import pytest

from netaudio.cli import state
from netaudio.cli_support.selection import filter_devices


@pytest.fixture(autouse=True)
def _restore_state():
    snapshot = dict(vars(state))
    yield
    vars(state).clear()
    vars(state).update(snapshot)


def _device(name, ipv4="192.0.2.10"):
    return SimpleNamespace(name=name, ipv4=ipv4, mac_address="00:1D:C1:00:00:01", ddm_context=None)


@pytest.mark.parametrize("pattern", ["avio-usb-tv-1", "AVIO-USB-TV-1", "avio-usb-tv-1.local.", "avio-*", "*.local."])
def test_name_filter_matches_device_and_server_names_case_insensitively(pattern):
    state.names = [pattern]
    devices = {"avio-usb-tv-1.local.": _device("avio-usb-tv-1"), "lx-dante.local.": _device("lx-dante")}
    selected = filter_devices(devices)
    assert "avio-usb-tv-1.local." in selected
    if pattern not in ("*.local.",):
        assert selected == {"avio-usb-tv-1.local.": devices["avio-usb-tv-1.local."]}


def test_name_filter_rejects_unrelated_names():
    state.names = ["wing*"]
    assert filter_devices({"avio-usb-tv-1.local.": _device("avio-usb-tv-1")}) == {}
