from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_cli_write_verification import _make_audio_capability_operations


def _make_sample_rate_operations(supported_sample_rates):
    operations, commands, device = _make_audio_capability_operations(
        "command_set_sample_rate",
        b"sample-rate",
        "supported_sample_rates",
        supported_sample_rates,
    )
    device._app = SimpleNamespace(set_sample_rate_state=AsyncMock(return_value="verified"))
    return operations, commands, device


async def _assert_sample_rate_operation_delegates(supported_sample_rates):
    operations, commands, device = _make_sample_rate_operations(supported_sample_rates)

    result = await operations.set_sample_rate(96_000)

    assert result == "verified"
    device._app.set_sample_rate_state.assert_awaited_once_with(
        device,
        96_000,
        confirm_destructive=False,
    )
    commands.command_set_sample_rate.assert_not_called()
    device.dante_send_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_sample_rate_operation_propagates_topology_safe_rejection_without_raw_send():
    operations, commands, device = _make_sample_rate_operations([48_000])
    device._app.set_sample_rate_state.side_effect = ValueError("requested sample rate 96000 is not supported")

    with pytest.raises(ValueError, match="requested sample rate 96000 is not supported"):
        await operations.set_sample_rate(96_000)

    device._app.set_sample_rate_state.assert_awaited_once_with(
        device,
        96_000,
        confirm_destructive=False,
    )
    commands.command_set_sample_rate.assert_not_called()
    device.dante_send_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_sample_rate_operation_delegates_with_known_capabilities():
    await _assert_sample_rate_operation_delegates([48_000, 96_000])


@pytest.mark.asyncio
async def test_sample_rate_operation_delegates_when_cached_capabilities_are_unknown():
    await _assert_sample_rate_operation_delegates(None)


@pytest.mark.asyncio
async def test_sample_rate_raw_request_is_separate_from_public_safe_operation():
    operations, commands, device = _make_sample_rate_operations([48_000, 96_000])

    result = await operations._request_sample_rate_change(96_000)

    assert result is None
    commands.command_set_sample_rate.assert_called_once_with(96_000)
    device.dante_send_command.assert_awaited_once_with(b"sample-rate", None, 8700)


@pytest.mark.asyncio
async def test_sample_rate_public_operation_requires_active_application():
    operations, commands, device = _make_sample_rate_operations([48_000, 96_000])
    device._app = None

    with pytest.raises(RuntimeError, match="requires an active Dante application"):
        await operations.set_sample_rate(96_000)

    commands.command_set_sample_rate.assert_not_called()
    device.dante_send_command.assert_not_awaited()
