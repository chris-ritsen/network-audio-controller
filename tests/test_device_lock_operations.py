import pytest

from netaudio import core
from netaudio.dante.device_operations import (
    LOCK_OPERATION_LOCK,
    LOCK_OPERATION_UNLOCK,
    _device_lock_operation,
)


class _FakeCoreClient:
    instances = []

    def __init__(self, device_ip):
        self.device_ip = device_ip
        self.calls = []
        self.closed = False
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info):
        self.closed = True

    def lock(self, pin, key):
        self.calls.append(("lock", pin, key))
        return {"success": True, "status": 0, "lock_state": 1, "already": False}

    def unlock(self, pin, key):
        self.calls.append(("unlock", pin, key))
        return {"success": True, "status": 0, "lock_state": 0, "already": False}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected_call", "expected_state"),
    [
        (LOCK_OPERATION_LOCK, "lock", 1),
        (LOCK_OPERATION_UNLOCK, "unlock", 0),
    ],
)
async def test_lock_exchange_delegates_to_native_core(monkeypatch, operation, expected_call, expected_state):
    _FakeCoreClient.instances.clear()
    monkeypatch.setattr(core, "CoreClient", _FakeCoreClient)
    key = b"k" * 32

    result = await _device_lock_operation("192.0.2.10", "1234", key, operation)

    assert result["lock_state"] == expected_state
    assert len(_FakeCoreClient.instances) == 1
    client = _FakeCoreClient.instances[0]
    assert client.device_ip == "192.0.2.10"
    assert client.calls == [(expected_call, "1234", key)]
    assert client.closed is True


@pytest.mark.asyncio
async def test_lock_exchange_rejects_unknown_operation_before_opening_client(monkeypatch):
    _FakeCoreClient.instances.clear()
    monkeypatch.setattr(core, "CoreClient", _FakeCoreClient)

    with pytest.raises(ValueError, match="unknown lock operation"):
        await _device_lock_operation("192.0.2.10", "1234", b"k" * 32, 99)

    assert _FakeCoreClient.instances == []
