from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio.dante.services.heartbeat import (
    DanteHeartbeatService,
    parse_interface_traffic_records,
)


def heartbeat_packet(record_hexadecimal: str) -> bytes:
    record = bytes.fromhex(record_hexadecimal)
    packet_length = 32 + len(record)
    header = bytearray(b"\xff\xfe" + packet_length.to_bytes(2, "big") + bytes(28))
    header[8:16] = bytes.fromhex("001dc1fffe50368b")
    return bytes(header) + record


BASELINE_PACKET = heartbeat_packet("0024800000040004a3e6000000100000000100100008562f000988fd0000000000000000")
TREATMENT_PACKET = heartbeat_packet("0024800000040004a3e7000000100000000100100008743f000bd5d00000000000000000")
LX_DANTE_PACKET = heartbeat_packet(
    "00348000000400045dcf00000010000000020010002067a20014ccb2000000000000000000000000000000000000000000000000"
)


def test_causal_avio_and_two_interface_lx_dante_records_parse():
    [baseline] = core.parse_response("heartbeat_interface_traffic", BASELINE_PACKET)
    [treatment] = parse_interface_traffic_records(TREATMENT_PACKET)
    [lx_dante] = parse_interface_traffic_records(LX_DANTE_PACKET)

    assert baseline["sequence"] == 41958
    assert baseline["interfaces"][0]["transmit_octets"] == 546351
    assert baseline["interfaces"][0]["receive_octets"] == 624893
    assert treatment["sequence"] == 41959
    assert treatment["interfaces"][0]["transmit_octets"] == 554047
    assert treatment["interfaces"][0]["receive_octets"] == 775632
    assert lx_dante["interface_entry_count"] == 2
    assert lx_dante["interfaces"][1]["transmit_octets"] == 0
    assert lx_dante["interfaces"][1]["receive_octets"] == 0


def test_consecutive_samples_update_device_with_raw_octets_and_estimated_rates():
    monotonic_times = iter([100.0, 101.2])
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        monotonic_clock=lambda: next(monotonic_times),
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    assert device.network_interface_traffic["interval_seconds"] is None

    service._on_packet(TREATMENT_PACKET, ("192.168.1.247", 8700))
    traffic = device.network_interface_traffic

    assert traffic["interval_seconds"] == pytest.approx(1.2)
    assert traffic["total_transmit_octets"] == 554047
    assert traffic["total_receive_octets"] == 775632
    assert traffic["estimated_total_transmit_bits_per_second"] == pytest.approx(554047 * 8 / 1.2)
    assert traffic["estimated_total_receive_bits_per_second"] == pytest.approx(775632 * 8 / 1.2)
    assert traffic["interfaces"][0]["estimated_transmit_bits_per_second"] == pytest.approx(554047 * 8 / 1.2)
    assert traffic["interfaces"][0]["estimated_receive_bits_per_second"] == pytest.approx(775632 * 8 / 1.2)


def test_sequence_gap_preserves_raw_octets_without_deriving_a_rate():
    monotonic_times = iter([100.0, 101.2])
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        monotonic_clock=lambda: next(monotonic_times),
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    skipped_sequence_packet = bytearray(TREATMENT_PACKET)
    skipped_sequence_packet[40:42] = (41960).to_bytes(2, "big")
    service._on_packet(bytes(skipped_sequence_packet), ("192.168.1.247", 8700))

    traffic = device.network_interface_traffic
    assert traffic["interval_seconds"] is None
    assert "estimated_total_transmit_bits_per_second" not in traffic
    assert "estimated_transmit_bits_per_second" not in traffic["interfaces"][0]


def test_duplicate_sequence_does_not_shift_the_next_rate_interval():
    monotonic_times = iter([100.0, 100.2, 101.2])
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        monotonic_clock=lambda: next(monotonic_times),
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    service._on_packet(TREATMENT_PACKET, ("192.168.1.247", 8700))

    assert device.network_interface_traffic["interval_seconds"] == pytest.approx(1.2)


def test_malformed_packet_does_not_replace_live_state():
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic={"sequence": 41958},
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(device_by_ip=lambda _source_ip: device)

    service._on_packet(TREATMENT_PACKET[:-1], ("192.168.1.247", 8700))

    assert device.network_interface_traffic == {"sequence": 41958}
