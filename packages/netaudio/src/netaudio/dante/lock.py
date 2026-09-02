from __future__ import annotations

import asyncio

LOCK_OPERATION_LOCK = 1
LOCK_OPERATION_UNLOCK = 2

LOCK_STATUS_ALREADY = 0x1102
LOCK_STATUS_SUCCESS = 0x0000


def validate_pin(pin: str) -> str | None:
    if len(pin) != 4:
        return "PIN must be exactly 4 digits"
    if not pin.isdigit():
        return "PIN must contain only digits"
    return None


def _lock_key_not_configured() -> dict:
    return {
        "already": False,
        "error": "device_lock_key not configured",
        "lock_state": None,
        "not_configured": True,
        "status": None,
        "success": False,
    }


def _lock_key_invalid(actual_length: int, expected_length: int) -> dict:
    return {
        "already": False,
        "error": f"device_lock_key must be {expected_length} bytes, got {actual_length}",
        "lock_state": None,
        "not_configured": False,
        "status": None,
        "success": False,
    }


def _validate_lock_key(key: bytes) -> dict | None:
    if not key:
        return _lock_key_not_configured()
    from netaudio.core.binding import LOCK_KEY_LENGTH

    if len(key) != LOCK_KEY_LENGTH:
        return _lock_key_invalid(len(key), LOCK_KEY_LENGTH)
    return None


async def core_lock_device(device_ip: str, pin: str, key: bytes) -> dict:
    key_error = _validate_lock_key(key)
    if key_error:
        return key_error
    return await _device_lock_operation(device_ip, pin, key, LOCK_OPERATION_LOCK)


async def core_unlock_device(device_ip: str, pin: str, key: bytes) -> dict:
    key_error = _validate_lock_key(key)
    if key_error:
        return key_error
    return await _device_lock_operation(device_ip, pin, key, LOCK_OPERATION_UNLOCK)


async def _device_lock_operation(device_ip: str, pin: str, key: bytes, operation: int) -> dict:
    from netaudio import core

    if operation not in (LOCK_OPERATION_LOCK, LOCK_OPERATION_UNLOCK):
        raise ValueError(f"unknown lock operation: {operation}")

    def _run():
        with core.CoreClient(device_ip) as client:
            if operation == LOCK_OPERATION_LOCK:
                return client.lock(pin, key)
            return client.unlock(pin, key)

    try:
        result = await asyncio.to_thread(_run)
    except core.NetaudioCoreError as error:
        return _lock_core_error(error)
    result.setdefault("success", result.get("status") in (LOCK_STATUS_SUCCESS, LOCK_STATUS_ALREADY))
    result.setdefault("already", result.get("status") == LOCK_STATUS_ALREADY)
    return result


def _lock_core_error(error: Exception) -> dict:
    return {
        "already": False,
        "error": str(error),
        "lock_state": None,
        "not_configured": False,
        "status": getattr(error, "status", None),
        "success": False,
    }
