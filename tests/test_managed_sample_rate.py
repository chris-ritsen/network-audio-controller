from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.device import DanteDevice
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyVerificationError,
    change_sample_rate_topology_safe,
)


def setup_adapter(monkeypatch, model, *, changed_members=False, incorrect_counts=False, retire_flow=False):
    device = DanteDevice("managed-adapter")
    device.model = model
    device.management_state = "managed"
    device.managed_operation_permissions = {"sample_rate": True}
    device.sample_rate_configuration_supported = True
    device.is_locked = False
    device.ipv4 = None
    device.execute = AsyncMock(return_value=b"synthetic channel counts")
    output = model == "AVIO-DAO2"
    device.sample_rate_channel_capacities = [
        {
            "sample_rate_hertz": rate,
            "receive_channel_count": 2 if output else 0,
            "transmit_channel_count": 0 if output else 2,
        }
        for rate in (44100, 48000, 88200, 96000)
    ]
    phase = {"rate": 48000}
    monkeypatch.setattr(
        core,
        "parse_response",
        lambda kind, data: {
            "rx_count": 1 if incorrect_counts else (2 if output else 0),
            "tx_count": 0 if output else 2,
        },
    )

    async def receivers():
        device.apply_receiver_channel_inventory(
            {
                "records": [
                    {
                        "channel_number": number,
                        "local_channel_name": f"Input {number}",
                        "source_device_name": "Source",
                        "source_channel_name": f"Output {number}",
                    }
                    for number in range(1, 3)
                ]
            }
        )

    device.get_rx_channels = AsyncMock(side_effect=receivers)
    detect = AsyncMock(return_value=0x2809)

    async def inventory(*args, **kwargs):
        assert kwargs["device"] is device
        if retire_flow and phase["rate"] == 96000:
            return {"flows": []}
        return {
            "flows": [
                {
                    "global_flow_id": 1,
                    "flow_type": "unicast",
                    "media_type_code": 3,
                    "channel_slot_count": 2,
                    "transmitter_channel_ids_by_slot": [1, 0 if changed_members and phase["rate"] == 96000 else 2],
                    "sample_rate": phase["rate"],
                    "encoding": 24,
                }
            ]
        }

    query = AsyncMock(side_effect=inventory)
    monkeypatch.setattr(flows, "detect_flow_protocol", detect)
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)

    async def probe():
        return {
            "current_value": phase["rate"],
            "requested_value": phase["rate"],
            "update_mode": 2,
            "available_values": [44100, 48000, 88200, 96000],
            "flags": None,
        }

    async def mutate():
        phase["rate"] = 96000

    return device, probe, AsyncMock(side_effect=mutate), detect, query










@pytest.mark.asyncio
async def test_managed_preflight_rejects_unexpected_channel_counts_before_writing(monkeypatch):
    device, probe, mutate, detect, query = setup_adapter(monkeypatch, "AVIO-DAO2", incorrect_counts=True)
    with pytest.raises(SampleRateTopologyVerificationError, match="fresh channel counts"):
        await change_sample_rate_topology_safe(device, 96000, probe, mutate)
    mutate.assert_not_awaited()
    detect.assert_not_awaited()
    query.assert_not_awaited()
