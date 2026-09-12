from types import SimpleNamespace
from unittest.mock import MagicMock

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
    assert baseline["interfaces"][0]["transmit_rate_raw"] == 546351
    assert baseline["interfaces"][0]["receive_rate_raw"] == 624893
    assert baseline["interfaces"][0]["transmit_rate_bits_per_second"] == 546351 * 8
    assert baseline["interfaces"][0]["receive_rate_bits_per_second"] == 624893 * 8
    assert baseline["interfaces"][0]["transmit_error_count"] == 0
    assert baseline["interfaces"][0]["receive_error_count"] == 0
    assert treatment["sequence"] == 41959
    assert treatment["interfaces"][0]["transmit_rate_raw"] == 554047
    assert treatment["interfaces"][0]["receive_rate_raw"] == 775632
    assert lx_dante["interface_entry_count"] == 2
    assert lx_dante["interfaces"][1]["transmit_rate_raw"] == 0
    assert lx_dante["interfaces"][1]["receive_rate_raw"] == 0


def test_each_sample_exposes_wire_rates_without_local_arrival_time_calculation():
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    assert device.network_interface_traffic["total_transmit_rate_bits_per_second"] == 546351 * 8

    service._on_packet(TREATMENT_PACKET, ("192.168.1.247", 8700))
    traffic = device.network_interface_traffic

    assert traffic["total_transmit_rate_bits_per_second"] == 554047 * 8
    assert traffic["total_receive_rate_bits_per_second"] == 775632 * 8
    assert traffic["total_transmit_error_count"] == 0
    assert traffic["total_receive_error_count"] == 0
    assert traffic["interfaces"][0]["transmit_rate_bits_per_second"] == 554047 * 8
    assert traffic["interfaces"][0]["receive_rate_bits_per_second"] == 775632 * 8


def test_sequence_gap_still_preserves_the_reported_rate():
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    skipped_sequence_packet = bytearray(TREATMENT_PACKET)
    skipped_sequence_packet[40:42] = (41960).to_bytes(2, "big")
    service._on_packet(bytes(skipped_sequence_packet), ("192.168.1.247", 8700))

    traffic = device.network_interface_traffic
    assert traffic["total_transmit_rate_bits_per_second"] == 554047 * 8
    assert traffic["interfaces"][0]["transmit_rate_raw"] == 554047


def test_duplicate_sequence_is_ignored_and_next_sequence_replaces_state():
    device = SimpleNamespace(
        server_name="avio-usb-1",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        network_interface_traffic=None,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
    )

    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    service._on_packet(BASELINE_PACKET, ("192.168.1.247", 8700))
    service._on_packet(TREATMENT_PACKET, ("192.168.1.247", 8700))

    assert device.network_interface_traffic["sequence"] == 41959
    assert device.network_interface_traffic["total_transmit_rate_bits_per_second"] == 554047 * 8


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
