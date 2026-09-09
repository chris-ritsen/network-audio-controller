from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.device import DanteDevice
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyVerificationError,
    change_sample_rate_topology_safe,
    preflight_sample_rate_change,
)


def setup_adapter(monkeypatch, model, *, changed_members=False, incorrect_counts=False, retire_flow=False):
    device = DanteDevice("managed-adapter")
    device.model = model
    device.management_state = "managed"
    device.ipv4 = None
    device.execute = AsyncMock(return_value=b"synthetic channel counts")
    output = model == "AVIO-DAO2"
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
        device.apply_receiver_channel_status_page(
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
        return phase["rate"], [44100, 48000, 88200, 96000]

    async def mutate():
        phase["rate"] = 96000

    return device, probe, AsyncMock(side_effect=mutate), detect, query


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["AVIO-DAI2", "AVIO-DAO2"])
async def test_analog_avio_change_verifies_managed_channels_and_routes(monkeypatch, model):
    device, probe, mutate, detect, query = setup_adapter(monkeypatch, model)
    result = await change_sample_rate_topology_safe(device, 96000, probe, mutate)
    mutate.assert_awaited_once()
    assert result.changed is True
    assert result.observed_sample_rate_hertz == 96000
    assert device.execute.await_count == 2
    if model == "AVIO-DAI2":
        device.get_rx_channels.assert_not_awaited()
        assert query.await_count == 2
        before = result.preflight.current_snapshot.transmitter_flows[0]
        assert before.channel_members == (1, 2)
        assert before.frames_per_packet is None
    else:
        assert device.get_rx_channels.await_count == 2
        detect.assert_not_awaited()
        query.assert_not_awaited()
        assert len(result.resulting_snapshot.receiver_subscriptions) == 2


@pytest.mark.asyncio
async def test_managed_sample_rate_rejects_changed_transmitter_membership(monkeypatch):
    device, probe, mutate, _, _ = setup_adapter(monkeypatch, "AVIO-DAI2", changed_members=True)
    with pytest.raises(SampleRateTopologyChangedButUnverifiedError, match="membership and metadata"):
        await change_sample_rate_topology_safe(device, 96000, probe, mutate)
    mutate.assert_awaited_once()


@pytest.mark.asyncio
async def test_noncontracting_rate_change_allows_automatic_unicast_flow_retirement(monkeypatch):
    device, probe, mutate, _, _ = setup_adapter(monkeypatch, "AVIO-DAI2", retire_flow=True)
    result = await change_sample_rate_topology_safe(device, 96000, probe, mutate)
    assert result.changed is True
    assert result.preflight.current_snapshot.transmitter_flows[0].flow_type == "unicast"
    assert result.resulting_snapshot.transmitter_flows == ()


@pytest.mark.asyncio
async def test_noncontracting_rate_change_still_rejects_multicast_flow_loss(monkeypatch):
    device, probe, mutate, _, query = setup_adapter(monkeypatch, "AVIO-DAI2", retire_flow=True)
    original = query.side_effect

    async def multicast_inventory(*args, **kwargs):
        inventory = await original(*args, **kwargs)
        for flow in inventory["flows"]:
            flow["flow_type"] = "multicast"
        return inventory

    query.side_effect = multicast_inventory
    with pytest.raises(SampleRateTopologyChangedButUnverifiedError, match="membership and metadata"):
        await change_sample_rate_topology_safe(device, 96000, probe, mutate)


@pytest.mark.asyncio
async def test_managed_preflight_rejects_unexpected_channel_counts_before_writing(monkeypatch):
    device, probe, mutate, detect, query = setup_adapter(monkeypatch, "AVIO-DAO2", incorrect_counts=True)
    with pytest.raises(SampleRateTopologyVerificationError, match="fresh channel counts"):
        await change_sample_rate_topology_safe(device, 96000, probe, mutate)
    mutate.assert_not_awaited()
    detect.assert_not_awaited()
    query.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_preflight_does_not_treat_non_audio_flows_as_audio(monkeypatch):
    device, probe, _, _, _ = setup_adapter(monkeypatch, "AVIO-DAI2")
    monkeypatch.setattr(
        flows,
        "query_tx_flow_inventory",
        AsyncMock(
            return_value={
                "flows": [
                    {
                        "global_flow_id": 1,
                        "flow_type": "unicast",
                        "media_type_code": 1,
                        "channel_slot_count": 1,
                        "transmitter_channel_ids_by_slot": [1],
                        "sample_rate": 48000,
                        "encoding": 24,
                    }
                ]
            }
        ),
    )
    with pytest.raises(SampleRateTopologyVerificationError, match="supported audio flow"):
        await preflight_sample_rate_change(device, 96000, probe)
