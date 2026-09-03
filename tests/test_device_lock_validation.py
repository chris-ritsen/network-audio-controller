from types import SimpleNamespace

import pytest

from netaudio.dante.application import DanteApplication
from netaudio.dante.lock import core_lock_device


@pytest.mark.asyncio
async def test_core_lock_device_rejects_missing_key_before_client(monkeypatch):
    called = False

    class Client:
        def __init__(self, *args):
            nonlocal called
            called = True

    monkeypatch.setattr("netaudio.core.CoreClient", Client)

    result = await core_lock_device("192.0.2.10", "1234", b"")

    assert called is False
    assert result == {
        "status": None,
        "lock_state": None,
        "success": False,
        "already": False,
        "error": "device_lock_key not configured",
        "not_configured": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [b"short", b"x" * 31, b"x" * 33])
async def test_core_lock_device_rejects_bad_key_length_before_client(monkeypatch, key):
    called = False

    class Client:
        def __init__(self, *args):
            nonlocal called
            called = True

    monkeypatch.setattr("netaudio.core.CoreClient", Client)

    result = await core_lock_device("192.0.2.10", "1234", key)

    assert called is False
    assert result["status"] is None
    assert result["lock_state"] is None
    assert result["success"] is False
    assert result["already"] is False
    assert result["error"] == f"device_lock_key must be 32 bytes, got {len(key)}"
    assert result["not_configured"] is False


@pytest.mark.asyncio
async def test_lock_device_uses_core_helper_without_trusting_acknowledgement_state(monkeypatch):
    async def fake_core_lock_device(device_ip, pin, key):
        assert device_ip == "192.0.2.10"
        assert pin == "1234"
        assert key == b"x" * 32
        return {"status": 0, "lock_state": 1, "success": True, "already": False}

    monkeypatch.setattr("netaudio.dante.application.core_lock_device", fake_core_lock_device)

    device = SimpleNamespace(ipv4="192.0.2.10", _app=None, is_locked=False)
    result = await DanteApplication().lock_device(device, "1234", b"x" * 32)

    assert result["success"] is True
    assert device.is_locked is False


@pytest.mark.asyncio
async def test_unlock_device_uses_core_helper_without_trusting_acknowledgement_state(monkeypatch):
    async def fake_core_unlock_device(device_ip, pin, key):
        assert device_ip == "192.0.2.10"
        assert pin == "1234"
        assert key == b"x" * 32
        return {"status": 0, "lock_state": 0, "success": True, "already": False}

    monkeypatch.setattr("netaudio.dante.application.core_unlock_device", fake_core_unlock_device)

    device = SimpleNamespace(ipv4="192.0.2.10", _app=None, is_locked=True)
    result = await DanteApplication().unlock_device(device, "1234", b"x" * 32)

    assert result["success"] is True
    assert device.is_locked is True


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["lock_device", "unlock_device"])
async def test_application_lock_operations_reject_devices_without_an_address(operation):
    device = SimpleNamespace(ipv4=None)

    with pytest.raises(RuntimeError, match="no control address"):
        await getattr(DanteApplication(), operation)(device, "1234", b"x" * 32)


@pytest.mark.asyncio
async def test_core_lock_device_returns_core_error(monkeypatch):
    from netaudio.core import NetaudioCoreError

    class Client:
        def __init__(self, device_ip):
            assert device_ip == "192.0.2.10"

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return None

        def lock(self, pin, key):
            raise NetaudioCoreError(9, "netaudio_client_lock")

    monkeypatch.setattr("netaudio.core.CoreClient", Client)

    result = await core_lock_device("192.0.2.10", "1234", b"x" * 32)

    assert result["status"] == 9
    assert result["success"] is False
    assert result["already"] is False
    assert result["error"] == "netaudio_client_lock: device did not respond"
    assert result["not_configured"] is False


@pytest.mark.asyncio
async def test_core_lock_device_uses_rust_client(monkeypatch):
    called = False

    class Client:
        def __init__(self, device_ip):
            assert device_ip == "192.0.2.10"

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return None

        def lock(self, pin, key):
            nonlocal called
            called = True
            assert pin == "1234"
            assert key == b"x" * 32
            return {"status": 0, "lock_state": 1}

    monkeypatch.setattr("netaudio.core.CoreClient", Client)

    result = await core_lock_device("192.0.2.10", "1234", b"x" * 32)

    assert called is True
    assert result == {"status": 0, "lock_state": 1, "success": True, "already": False}
