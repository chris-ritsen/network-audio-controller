import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio.daemon import metering as metering_module
from netaudio.daemon.metering import MeteringManager
from netaudio.dante.events import EventType
from netaudio.dante.metering import parse_metering_levels

METERING_FRAME = bytes.fromhex("ffff00211f810000001dc119245c0000417564696e617465020302fefe7da08800")
METERING_FRAME_V3 = bytes.fromhex("ffff011eddfb0000001dc10812580000417564696e617465030000800080") + bytes([0xFE] * 256)

PASSIVE_RECORD = {
    "record_length": 28,
    "extension_length": 4,
    "payload_length": 16,
    "sequence": 0x4DEC,
    "tx_count": 2,
    "tx_first_channel_index": 0,
    "rx_count": 1,
    "rx_first_channel_index": 0,
    "level_vector_offset": 24,
    "tx_levels": [0xFE, 0xFE],
    "rx_levels": [0x6D],
    "padding_length": 1,
}


def make_manager():
    device = SimpleNamespace(
        ipv4="192.168.1.61",
        server_name="avio-bt-1",
        name="AVIO Bluetooth",
        online=True,
        update_last_seen=MagicMock(),
    )
    application = SimpleNamespace(
        devices={"avio-bt-1": device},
        cmc=SimpleNamespace(start_metering=MagicMock(), stop_metering=MagicMock()),
        dispatcher=SimpleNamespace(emit_nowait=MagicMock()),
    )
    return MeteringManager(application), application, device


def test_metering_levels_use_embedded_channel_counts():
    assert parse_metering_levels(METERING_FRAME) == {
        "tx": {1: 0xFE, 2: 0x7D, 3: 0xA0},
        "rx": {1: 0x88, 2: 0x00},
    }


def test_metering_levels_reject_count_mismatch():
    data = bytes.fromhex("ffff00211f810000001dc119245c0000417564696e617465020402fefe7da08800")

    with pytest.raises(core.NetaudioCoreError, match="malformed response"):
        parse_metering_levels(data)


def test_metering_v3_levels_use_sixteen_bit_channel_counts():
    assert len(METERING_FRAME_V3) == 286

    levels = parse_metering_levels(METERING_FRAME_V3)

    assert len(levels["tx"]) == 128
    assert len(levels["rx"]) == 128
    assert levels["tx"][17] == 0xFE
    assert levels["rx"][4] == 0xFE


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


@pytest.mark.asyncio
async def test_metering_manager_start_uses_public_cmc_host_address(monkeypatch):
    class ProbeReached(Exception):
        pass

    host_address = b"\x00\x1d\xc1\x50\x23\x68"
    application = SimpleNamespace(
        devices={},
        cmc=SimpleNamespace(host_media_access_control_address=host_address),
    )
    manager = MeteringManager(application)
    monkeypatch.setattr(metering_module, "_get_local_ip", lambda: "192.0.2.1")

    def stop_before_socket_bind(_port):
        assert manager._host_mac == host_address
        raise ProbeReached

    monkeypatch.setattr(manager, "_probe_port", stop_before_socket_bind)

    with pytest.raises(ProbeReached):
        await manager.start()


def test_passive_sample_reaches_transient_cache_with_raw_values_and_source_metadata():
    manager, _, _ = make_manager()

    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    cached = manager.get_cached_levels("avio-bt-1")
    assert cached["metering_source"] == "signal_presence"
    assert cached["tx"] == {1: 0xFE, 2: 0xFE}
    assert cached["rx"] == {1: 0x6D}
    assert cached["tx_raw"] == [0xFE, 0xFE]
    assert cached["rx_raw"] == [0x6D]
    assert cached["tx_signal_presence"] == {1: "muted", 2: "muted"}
    assert cached["rx_signal_presence"] == {1: "signal_present"}


@pytest.mark.asyncio
async def test_passive_sample_completes_waiting_snapshot_consumer():
    manager, application, _ = make_manager()
    snapshot_task = asyncio.create_task(manager.snapshot("avio-bt-1", timeout=1.0))
    await asyncio.sleep(0)

    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    result = await snapshot_task
    assert result["metering_source"] == "signal_presence"
    assert result["rx"] == {1: 0x6D}
    application.cmc.start_metering.assert_called_once()
    application.cmc.stop_metering.assert_called_once()


def test_fresh_detailed_sample_takes_precedence_over_newer_passive_sample():
    manager, _, _ = make_manager()
    manager._on_metering_packet(METERING_FRAME, ("192.168.1.61", 8752))

    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    cached = manager.get_cached_levels("avio-bt-1")
    assert cached["metering_source"] == "detailed"
    assert cached["tx"] == {1: 0xFE, 2: 0x7D, 3: 0xA0}
    assert manager._signal_presence_levels["avio-bt-1"]["rx"] == {1: 0x6D}


def test_stale_detailed_sample_falls_back_to_fresh_passive_sample():
    manager, _, _ = make_manager()
    manager._on_metering_packet(METERING_FRAME, ("192.168.1.61", 8752))
    manager._detailed_levels["avio-bt-1"]["timestamp"] -= 3.0

    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    assert manager.get_cached_levels("avio-bt-1")["metering_source"] == "signal_presence"


def test_stale_passive_sample_is_not_returned():
    manager, _, _ = make_manager()
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))
    manager._signal_presence_levels["avio-bt-1"]["timestamp"] -= 3.0

    assert manager.get_cached_levels("avio-bt-1") is None


def test_nonzero_first_channel_indexes_map_to_existing_one_based_interface():
    manager, _, _ = make_manager()
    record = {
        **PASSIVE_RECORD,
        "tx_first_channel_index": 4,
        "rx_first_channel_index": 8,
    }

    manager.record_signal_presence(record, ("192.168.1.61", 8700))

    cached = manager.get_cached_levels("avio-bt-1")
    assert cached["tx"] == {5: 0xFE, 6: 0xFE}
    assert cached["rx"] == {9: 0x6D}


@pytest.mark.asyncio
async def test_fresh_passive_snapshot_sends_no_metering_start_or_stop_request():
    manager, application, _ = make_manager()
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    result = await manager.snapshot("avio-bt-1", timeout=0.1)

    assert result["metering_source"] == "signal_presence"
    application.cmc.start_metering.assert_not_called()
    application.cmc.stop_metering.assert_not_called()


def test_passive_values_emit_meter_event_without_detailed_reference_or_cmc_commands():
    manager, application, _ = make_manager()

    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))
    manager._broadcast_pending()

    event = application.dispatcher.emit_nowait.call_args.args[0]
    assert event.type is EventType.METER_VALUES
    assert event.server_name == "avio-bt-1"
    assert event.data["metering_source"] == "signal_presence"
    assert event.data["rx"] == {1: 0x6D}
    assert manager._persistent_refs == {}
    application.cmc.start_metering.assert_not_called()
    application.cmc.stop_metering.assert_not_called()


def test_each_passive_heartbeat_emits_even_when_levels_are_unchanged():
    manager, application, _ = make_manager()
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))
    manager._broadcast_pending()
    application.dispatcher.emit_nowait.reset_mock()

    same_values_new_sequence = {**PASSIVE_RECORD, "sequence": PASSIVE_RECORD["sequence"] + 1}
    manager.record_signal_presence(same_values_new_sequence, ("192.168.1.61", 8700))
    manager._broadcast_pending()

    event = application.dispatcher.emit_nowait.call_args.args[0]
    assert event.data["sequence"] == PASSIVE_RECORD["sequence"] + 1


def test_cache_by_server_is_fresh_cache_only_and_preserves_detailed_precedence():
    manager, application, _ = make_manager()
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))

    cached = manager.get_cached_levels_by_server()

    assert cached["avio-bt-1"]["metering_source"] == "signal_presence"
    assert cached["avio-bt-1"]["rx"] == {1: 0x6D}
    application.cmc.start_metering.assert_not_called()
    application.cmc.stop_metering.assert_not_called()

    manager._on_metering_packet(METERING_FRAME, ("192.168.1.61", 8752))
    assert manager.get_cached_levels_by_server()["avio-bt-1"]["metering_source"] == "detailed"

    manager._detailed_levels["avio-bt-1"]["timestamp"] -= 3.0
    assert manager.get_cached_levels_by_server()["avio-bt-1"]["metering_source"] == "signal_presence"

    manager._signal_presence_levels["avio-bt-1"]["timestamp"] -= 3.0
    assert manager.get_cached_levels_by_server() == {}


def test_read_promotion_does_not_hide_detailed_to_passive_notification_transition():
    manager, application, _ = make_manager()
    manager._persistent_refs["avio-bt-1"] = {"client"}
    manager._on_metering_packet(METERING_FRAME, ("192.168.1.61", 8752))
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))
    manager._broadcast_pending()
    assert application.dispatcher.emit_nowait.call_args.args[0].data["metering_source"] == "detailed"
    application.dispatcher.emit_nowait.reset_mock()
    manager._detailed_levels["avio-bt-1"]["timestamp"] -= 3.0

    assert manager.get_cached_levels("avio-bt-1")["metering_source"] == "signal_presence"
    same_values_new_sequence = {**PASSIVE_RECORD, "sequence": PASSIVE_RECORD["sequence"] + 1}
    manager.record_signal_presence(same_values_new_sequence, ("192.168.1.61", 8700))
    manager._broadcast_pending()

    event = application.dispatcher.emit_nowait.call_args.args[0]
    assert event.data["metering_source"] == "signal_presence"


def test_passive_record_validation_is_atomic_before_cache_update():
    manager, _, _ = make_manager()
    manager.record_signal_presence(PASSIVE_RECORD, ("192.168.1.61", 8700))
    previous = manager.get_cached_levels("avio-bt-1")
    malformed = {**PASSIVE_RECORD, "rx_count": 2}

    manager.record_signal_presence(malformed, ("192.168.1.61", 8700))

    assert manager.get_cached_levels("avio-bt-1") == previous
