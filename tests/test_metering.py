import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio.daemon.metering import MeteringManager
from netaudio.dante.metering import parse_metering_levels

METERING_FRAME = bytes.fromhex("ffff00211f810000001dc119245c0000417564696e617465020302fefe7da08800")


def test_metering_levels_use_embedded_channel_counts():
    assert parse_metering_levels(METERING_FRAME) == {
        "tx": {1: 0xFE, 2: 0x7D, 3: 0xA0},
        "rx": {1: 0x88, 2: 0x00},
    }


def test_metering_levels_reject_count_mismatch():
    data = bytes.fromhex("ffff00211f810000001dc119245c0000417564696e617465020402fefe7da08800")

    with pytest.raises(core.NetaudioCoreError, match="malformed response"):
        parse_metering_levels(data)


def test_metering_manager_uses_frame_counts_without_device_inventory():
    device = SimpleNamespace(
        ipv4="192.168.1.34",
        server_name="a32",
        update_last_seen=MagicMock(),
    )
    application = SimpleNamespace(devices={"a32": device})
    manager = MeteringManager(application)

    manager._on_metering_packet(METERING_FRAME, ("192.168.1.34", 8752))

    assert manager._latest_levels["a32"]["tx"] == {1: 0xFE, 2: 0x7D, 3: 0xA0}
    assert manager._latest_levels["a32"]["rx"] == {1: 0x88, 2: 0x00}
    device.update_last_seen.assert_called_once_with()


def test_metering_manager_logs_and_ignores_malformed_frame(caplog):
    device = SimpleNamespace(
        ipv4="192.168.1.34",
        server_name="a32",
        update_last_seen=MagicMock(),
    )
    application = SimpleNamespace(devices={"a32": device})
    manager = MeteringManager(application)

    with caplog.at_level(logging.WARNING, logger="netaudio"):
        manager._on_metering_packet(METERING_FRAME[:-1], ("192.168.1.34", 8752))

    assert manager._latest_levels == {}
    assert "Ignoring malformed metering packet from 192.168.1.34" in caplog.text
