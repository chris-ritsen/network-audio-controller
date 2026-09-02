from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio.dante.device_operations import DanteDeviceOperations


def _make_sample_rate_operations(supported_sample_rates):
    device = SimpleNamespace(
        application=SimpleNamespace(set_sample_rate=AsyncMock(return_value="verified")),
        execute=AsyncMock(),
        supported_sample_rates=supported_sample_rates,
    )
    return DanteDeviceOperations(device), device


async def _assert_sample_rate_operation_delegates(supported_sample_rates):
    operations, device = _make_sample_rate_operations(supported_sample_rates)

    result = await operations.set_sample_rate(96_000)

    assert result == "verified"
    device.application.set_sample_rate.assert_awaited_once_with(
        device,
        96_000,
        confirm_destructive=False,
    )
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_sample_rate_operation_propagates_topology_safe_rejection_without_raw_send():
    operations, device = _make_sample_rate_operations([48_000])
    device.application.set_sample_rate.side_effect = ValueError("requested sample rate 96000 is not supported")

    with pytest.raises(ValueError, match="requested sample rate 96000 is not supported"):
        await operations.set_sample_rate(96_000)

    device.application.set_sample_rate.assert_awaited_once_with(
        device,
        96_000,
        confirm_destructive=False,
    )
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_sample_rate_operation_delegates_with_known_capabilities():
    await _assert_sample_rate_operation_delegates([48_000, 96_000])


@pytest.mark.asyncio
async def test_sample_rate_operation_delegates_when_cached_capabilities_are_unknown():
    await _assert_sample_rate_operation_delegates(None)


@pytest.mark.asyncio
async def test_sample_rate_public_operation_requires_active_application():
    operations, device = _make_sample_rate_operations([48_000, 96_000])
    device.application = None

    with pytest.raises(RuntimeError, match="requires an active Dante application"):
        await operations.set_sample_rate(96_000)

    device.execute.assert_not_awaited()
