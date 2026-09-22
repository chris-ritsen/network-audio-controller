from types import SimpleNamespace

import pytest

from netaudio import core


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["rx", "tx"])
async def test_channel_refresh_without_address_rejects_cached_inventory(direction):
    from unittest.mock import AsyncMock

    from netaudio.dante.const import SERVICE_ARC
    from netaudio.dante.device import DanteDevice

    device = DanteDevice(server_name="receiver.local.")
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    channels = {1: SimpleNamespace(number=1)}
    setattr(device, f"{direction}_channels", channels)
    transport = SimpleNamespace(call=AsyncMock())
    device._app = SimpleNamespace(transport=transport)

    with pytest.raises(RuntimeError, match="no control address"):
        await getattr(device, f"get_{direction}_channels")()

    assert getattr(device, f"{direction}_channels") is channels
    transport.call.assert_not_called()


@pytest.mark.parametrize(
    "advertised,observed,expected",
    [
        (0x2729, 0x2729, [0x2809, 0x2729]),
        (0x27FF, 0x2801, [0x2809, 0x2801]),
        (0x2729, 0x280F, [0x280F]),
        (0x2809, 0x2729, [0x2809]),
        (0x280F, 0x280F, [0x280F]),
    ],
)
def test_native_transmitter_inventory_plan_uses_explicit_revisions(advertised, observed, expected):
    assert core.transmit_flow_inventory_protocols(advertised, observed) == expected


@pytest.mark.parametrize("advertised,observed", [(0x2810, 0x2729), (0x2809, 0x2810), (0x2729, 0x27FF)])
def test_native_transmitter_inventory_plan_rejects_unsupported_revisions(advertised, observed):
    with pytest.raises(core.NetaudioCoreError):
        core.transmit_flow_inventory_protocols(advertised, observed)


@pytest.mark.parametrize(
    "version,protocol_id,modern",
    [
        ("2.7.41", 0x2729, False),
        ("2.7.255", 0x27FF, False),
        ("2.8.1", 0x2801, False),
        ("2.8.9", 0x2809, True),
        ("2.8.15", 0x280F, True),
    ],
)
def test_core_resolves_advertised_arc_protocol(version, protocol_id, modern):
    assert core.arc_protocol(version) == {
        "protocol_id": protocol_id,
        "modern_channel_inventory": modern,
        "subscription_page": protocol_id == 0x280F,
        "subscription_batch_limit": 32 if protocol_id == 0x280F else 16,
        "channel_name_probe_protocol_id": protocol_id if modern else 0x2809,
        "flow_query_protocol_ids": [protocol_id] if modern else [0x2729, 0x2801, 0x2809, 0x280F],
    }


@pytest.mark.parametrize("version", ["", "2.8", "2.8.16", "2.8.256", "18.8.9", "2.8.-1", "2.8.9.0", " 2.8.9", "٢.٨.٩"])
def test_core_rejects_malformed_or_unestablished_revision(version):
    with pytest.raises(core.NetaudioCoreError, match="unsupported ARC protocol version"):
        core.arc_protocol(version)


def test_missing_revision_stays_unknown_and_managed_transport_is_explicit():
    assert core.arc_protocol(None) is None
    assert core.arc_protocol(None, managed=True) == {
        "protocol_id": 0x2809,
        "modern_channel_inventory": True,
        "subscription_page": False,
        "subscription_batch_limit": 32,
        "channel_name_probe_protocol_id": 0x2809,
        "flow_query_protocol_ids": [0x2809],
    }


def test_device_protocol_adapter_preserves_missing_metadata_and_rejects_legacy_modern_operation():
    from netaudio.dante.arc_protocol import (
        ArcProtocolError,
        advertised_arc_protocol_identifier_for_device,
        modern_arc_protocol_identifier_for_device,
    )
    from netaudio.dante.const import SERVICE_ARC

    device = SimpleNamespace(services={})
    assert advertised_arc_protocol_identifier_for_device(device) is None

    with pytest.raises(ArcProtocolError, match="no ARC service metadata"):
        modern_arc_protocol_identifier_for_device(device)

    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    assert advertised_arc_protocol_identifier_for_device(device) == 0x27FF

    with pytest.raises(ArcProtocolError, match="does not support modern channel inventory"):
        modern_arc_protocol_identifier_for_device(device)
