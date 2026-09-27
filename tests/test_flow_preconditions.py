from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio.dante.flow_preconditions import refresh_flow_state


@pytest.mark.asyncio
@pytest.mark.parametrize("locked", [True, None])
async def test_fresh_lock_rejection_is_read_only(locked):
    application = SimpleNamespace(probe_lock_status=AsyncMock(return_value=SimpleNamespace(is_locked=locked)))
    device = SimpleNamespace(
        requires_managed_control=False,
        application=application,
        populate_from_core=AsyncMock(return_value=True),
        execute=AsyncMock(),
    )
    assert await refresh_flow_state(device, rtp=False) is not None
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_rtp_facts_replace_cached_enablement_without_changing_device():
    application = SimpleNamespace(
        probe_lock_status=AsyncMock(return_value=SimpleNamespace(is_locked=False)),
        probe_aes67_state=AsyncMock(return_value=(False, True)),
    )
    device = SimpleNamespace(
        requires_managed_control=False,
        application=application,
        aes67_current=True,
        populate_from_core=AsyncMock(return_value=True),
        execute=AsyncMock(),
    )
    assert await refresh_flow_state(device, rtp=True) is not None
    assert device.aes67_current is False
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_audio_format_replaces_cached_values():
    application = SimpleNamespace(
        probe_lock_status=AsyncMock(return_value=SimpleNamespace(is_locked=False)),
        probe_sample_rate_status=AsyncMock(return_value={"current_value": 96000}),
        probe_encoding_status=AsyncMock(return_value={"current_value": 16}),
    )
    device = SimpleNamespace(
        application=application,
        sample_rate=48000,
        encoding=24,
        populate_from_core=AsyncMock(return_value=True),
        execute=AsyncMock(),
    )
    assert await refresh_flow_state(device, rtp=False) is None
    assert (device.sample_rate, device.encoding) == (96000, 16)
    device.execute.assert_not_awaited()
