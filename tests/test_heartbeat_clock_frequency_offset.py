from types import SimpleNamespace
from unittest.mock import MagicMock
import json

from netaudio import core
from netaudio.dante.services.heartbeat import (
    DanteHeartbeatService,
    parse_clock_frequency_offset_records,
)


BASELINE_PACKET = bytes.fromhex(
    "fffe005433f200000200000000040000417564696e6174650008000110000000"
    "002480000004000419f800000010000000010010000000000000000000000000"
    "00000000001080010004000419f80000fff9f9fb"
)
TREATMENT_PACKET = bytes.fromhex(
    "fffe0054343a00000200000000040000417564696e6174650008000110000000"
    "00248000000400041a1c00000010000000010010000000000000000000000000"
    "0000000000108001000400041a1c0000fffc9bf2"
)


def test_authentic_qemu_control_and_treatment_decode_as_signed_parts_per_billion():
    baseline = core.parse_response("heartbeat_clock_frequency_offset", BASELINE_PACKET)
    treatment = parse_clock_frequency_offset_records(TREATMENT_PACKET)

    assert baseline == [
        {
            "raw_record": list(BASELINE_PACKET[-16:]),
            "record_length": 16,
            "extension_length": 4,
            "payload_length": 4,
            "sequence": 6648,
            "unknown_word_at_offset_10": 0,
            "clock_frequency_offset_parts_per_billion": -394757,
            "trailing_payload": [],
        }
    ]
    assert treatment[0]["sequence"] == 6684
    assert treatment[0]["clock_frequency_offset_parts_per_billion"] == -222222


def test_heartbeat_updates_live_device_clock_offset_from_latest_valid_record():
    device = SimpleNamespace(
        server_name="a32-root-live",
        online=True,
        clock_frequency_offset_parts_per_billion=None,
        update_last_seen=MagicMock(),
    )
    on_device_updated = MagicMock()
    service = DanteHeartbeatService(
        device_by_ip=lambda _source_ip: device,
        on_device_updated=on_device_updated,
    )

    service._on_packet(TREATMENT_PACKET, ("192.168.1.232", 1030))

    device.update_last_seen.assert_called_once_with()
    assert device.clock_frequency_offset_parts_per_billion == -222222
    on_device_updated.assert_called_once_with(device)

    service._on_packet(TREATMENT_PACKET, ("192.168.1.232", 1030))
    assert on_device_updated.call_count == 1


def test_malformed_packet_does_not_replace_clock_offset():
    device = SimpleNamespace(
        server_name="a32-root-live",
        online=True,
        clock_frequency_offset_parts_per_billion=-394757,
        update_last_seen=MagicMock(),
    )
    service = DanteHeartbeatService(device_by_ip=lambda _source_ip: device)

    service._on_packet(TREATMENT_PACKET[:-1], ("192.168.1.232", 1030))

    assert device.clock_frequency_offset_parts_per_billion == -394757


def test_clock_history_stays_native_while_device_updates_remain_bounded(tmp_path):
    from netaudio.dante.device import DanteDevice
    from netaudio.dante.device_serializer import DanteDeviceSerializer
    from netaudio.dante.state import apply_device_status
    from netaudio.dante.services.notification_packet_handlers import _parse_ptp_clock_status
    from tests.test_clock_port_records import CLOCK_STATUS_PACKET

    device = DanteDevice("receiver")
    device.online = True
    status = _parse_ptp_clock_status(CLOCK_STATUS_PACKET, "192.0.2.1", device)
    apply_device_status(device, status.kind, status.status)
    now = [status.status["_clock_received_monotonic"] + 0.01]
    publications = []
    service = DanteHeartbeatService(
        device_by_ip=lambda _ip: device,
        on_device_updated=lambda updated: publications.append(len(json.dumps(DanteDeviceSerializer.to_json(updated)))),
        monotonic_clock=lambda: now[0],
    )

    for sequence in range(305):
        packet = bytearray(TREATMENT_PACKET)
        packet[-8:-6] = sequence.to_bytes(2, "big")
        service._on_packet(bytes(packet), ("192.0.2.1", 1030))
        now[0] += 0.01

    summary = device.clock_observations
    assert not summary["heartbeat"].get("history")
    assert len(publications) == 305
    assert publications[-1] < publications[0] + 2000
    history = service.diagnostics_snapshot(device)["clock"]
    samples = history["heartbeat"]["history"]
    assert len(samples) == 300
    assert [sample["sequence"] for sample in samples] == list(range(5, 305))
    assert len(history["conmon"]["history"]) == 1
    assert samples[-1]["clock_state_evidence"]["observed_at"] == status.status["clock_observed_at"]

    service._on_packet(bytes(packet), ("192.0.2.1", 1030))
    assert len(publications) == 305
    assert len(service.diagnostics_snapshot(device)["clock"]["heartbeat"]["history"]) == 300
    (tmp_path / "clock-history-cost.json").write_text(
        json.dumps(
            {
                "first_publication_bytes": publications[0],
                "last_publication_bytes": publications[-1],
                "retained_samples": len(samples),
            }
        )
    )
