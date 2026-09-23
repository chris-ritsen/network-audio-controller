from pathlib import Path

import pytest

from netaudio import core


def record():
    return {
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 7,
        "flow_type": "multicast",
        "flow_name": "Program",
        "transmitter_channel_ids_by_slot": [1, 0, 2],
        "sample_rate": 48000,
        "encoding": 24,
        "destination_internet_protocol_version_four_address": "239.69.1.10",
        "destination_user_datagram_port": 5004,
        "unmapped_extension": {"value": 7},
    }


def test_native_readback_preserves_slots_identity_unknown_media_and_extensions():
    observed = record()
    result = core.transmit_flow_specification(observed, protocol_id=0x2809)
    assert result["channel_slots"] == [{"slot": 1, "transmitter_channel": 1}, {"slot": 3, "transmitter_channel": 2}]
    assert result["identity"] == {"global_flow_id": 2, "media_type_code": 3, "media_local_flow_id": 7}
    assert result["media_mode"] == "unknown"
    assert result["protocol"]["cohort"] == "modern_2809"
    assert result["raw_fields"] == observed
    assert result["primary_destination"] == {"address": "239.69.1.10", "port": 5004, "interface": None}


@pytest.mark.parametrize(
    "change",
    [
        {"transmitter_channel_ids_by_slot": []},
        {"transmitter_channel_ids_by_slot": [0]},
        {"transmitter_channel_ids_by_slot": [1, 1]},
        {"transmitter_channel_ids_by_slot": [-1]},
        {"media_mode": "invented"},
        {"flow_type": None},
        {"global_flow_id": 0},
        {"encoding": 0},
        {"flow_name": ""},
        {"destination_internet_protocol_version_four_address": "192.0.2.10"},
    ],
)
def test_native_readback_rejects_unrepresentable_records(change):
    with pytest.raises(core.NetaudioCoreError):
        core.transmit_flow_specification({**record(), **change}, protocol_id=0x2809)


def test_native_readback_refuses_unknown_protocol():
    with pytest.raises(core.NetaudioCoreError):
        core.transmit_flow_specification(record(), protocol_id=0x2810)


@pytest.mark.parametrize("protocol,known", [(0x2729, True), (0x2809, False)])
def test_native_readback_reports_which_semantic_fields_are_established(protocol, known):
    result = core.transmit_flow_specification(record(), protocol_id=protocol)
    assert ("media_mode" in result["observed_fields"]) is known
    assert "channel_slots" in result["observed_fields"]
    assert "primary_destination" in result["observed_fields"]
    assert "secondary_destination" not in result["observed_fields"]


def test_matching_unknown_media_modes_cannot_confirm_effective_state():
    effective = core.transmit_flow_specification({**record(), "media_mode": "unknown"}, protocol_id=0x2809)
    comparison = core.compare_transmit_flows(effective, effective)
    assert not comparison["matches"]
    assert "media_mode" in comparison["unavailable_fields"]


def test_capture_readback_preserves_flow_identity_and_raw_observations():
    response = (Path(__file__).parent / "fixtures/transmit_flow_lifecycle/modern-2809-create-readback.bin").read_bytes()
    observed = core.parse_response("transmitter_flow_status_page", response)["flows"][0]
    native = core.transmit_flow_specification(observed, protocol_id=0x2809)
    assert native["identity"]["global_flow_id"] == 2
    assert native["raw_fields"] == observed


@pytest.mark.parametrize("protocol", [0x2809, 0x280F])
def test_native_flow_topology_retains_empty_slots_and_modern_unicast_retirement(protocol):
    result = core.transmit_flow_topology(
        {**record(), "flow_type": "unicast", "channel_slot_count": 3}, protocol_id=protocol
    )
    assert result == {
        "flow_number": 2,
        "flow_type": "unicast",
        "channel_count": 3,
        "channel_members": [1, 0, 2],
        "sample_rate_hertz": 48000,
        "encoding": 24,
        "frames_per_packet": None,
        "may_retire_after_sample_rate_change": True,
    }


def test_native_legacy_unicast_topology_preserves_unknown_membership():
    result = core.transmit_flow_topology(
        {
            "flow_number": 1,
            "flow_type": "unicast",
            "channel_count": 2,
            "channels": [],
            "sample_rate": 48000,
            "encoding": 24,
            "frames_per_packet": 32,
        },
        protocol_id=0x2729,
    )
    assert result["channel_count"] == 2
    assert result["channel_members"] == []
    assert result["frames_per_packet"] == 32
    assert result["may_retire_after_sample_rate_change"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"channel_slot_count": 2},
        {"channel_slot_count": 0},
        {"media_type_code": 4},
        {"sample_rate": None},
        {"encoding": False},
        {"transmitter_channel_ids_by_slot": [1, 1, 2]},
    ],
)
def test_native_flow_topology_rejects_ambiguous_audio_state(change):
    with pytest.raises(core.NetaudioCoreError):
        core.transmit_flow_topology({**record(), "channel_slot_count": 3, **change}, protocol_id=0x2809)
