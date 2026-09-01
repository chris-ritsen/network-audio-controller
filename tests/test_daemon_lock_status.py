import asyncio
from unittest.mock import AsyncMock

import pytest

from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.device import DanteDevice
from netaudio.dante.lock_status import LockStatusObservation
from tests.http_api_test_support import FakeWriter, get, make_device, make_http_server, post


class TestDaemonLockStatus:
    @pytest.mark.asyncio
    async def test_failed_lock_result_uses_conflict_status(self, monkeypatch):
        device = make_device()
        previous_status = {"lock_state_code": 1, "is_locked": True, "status_code": 0}
        device.is_locked = True
        device.lock_reset_status = previous_status
        device.operations.lock_device.return_value = {"success": False, "error": "locked"}
        http_server = make_http_server({"dev1": device})
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")

        status, response = await post(http_server, "/lock", {"device": "dev1", "pin": "1234"})

        assert status == 409
        assert response == {"success": False, "error": "locked"}
        assert device.is_locked is True
        assert device.lock_reset_status is previous_status
        http_server.application.dispatcher.emit_nowait.assert_not_called()

    @pytest.mark.asyncio
    async def test_lock_timeout_uses_gateway_timeout_status(self, monkeypatch):
        device = make_device()
        device.is_locked = True
        device.lock_reset_status = {"lock_state_code": 1, "is_locked": True, "status_code": 0}
        device.operations.lock_device.return_value = {
            "success": False,
            "status": 9,
            "error": "netaudio_client_lock: device did not respond",
        }
        http_server = make_http_server({"dev1": device})
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")

        status, response = await post(http_server, "/lock", {"device": "dev1", "pin": "1234"})

        assert status == 504
        assert response == {
            "success": False,
            "status": 9,
            "error": "netaudio_client_lock: device did not respond",
        }
        assert device.is_locked is None
        assert device.lock_reset_status is None
        event = http_server.application.dispatcher.emit_nowait.call_args.args[0]
        assert event.type == EventType.DEVICE_UPDATED

    @pytest.mark.parametrize(
        ("path", "operation_name", "requested_is_locked", "lock_state_code"),
        [
            ("/lock", "lock_device", True, 1),
            ("/unlock", "unlock_device", False, 0),
        ],
    )
    @pytest.mark.asyncio
    async def test_lock_operation_finishes_with_observation_after_0x1008(
        self,
        monkeypatch,
        path,
        operation_name,
        requested_is_locked,
        lock_state_code,
    ):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.probe_lock_status.return_value = LockStatusObservation(
            lock_reset_status={
                "lock_state_code": lock_state_code,
                "is_locked": requested_is_locked,
                "status_code": 0,
            },
            observed_at="2026-08-21T20:57:35.396345+00:00",
        )
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")

        status, response = await post(http_server, path, {"device": "dev1", "pin": "1234"})

        assert status == 200
        assert response["success"] is True
        assert response["is_locked"] is requested_is_locked
        assert response["lock_state_code"] == lock_state_code
        assert response["observation_source"] == "observed_after_0x1008"
        assert device.is_locked is requested_is_locked
        assert device.lock_reset_status["lock_state_code"] == lock_state_code
        getattr(device.operations, operation_name).assert_awaited_once_with("1234", b"key")
        http_server.application.probe_lock_status.assert_awaited_once_with("192.168.1.50", timeout=4.0)

    @pytest.mark.asyncio
    async def test_lock_operation_reports_mismatched_post_request_observation(self, monkeypatch):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")

        status, response = await post(http_server, "/lock", {"device": "dev1", "pin": "1234"})

        assert status == 409
        assert response["error"] == "lock operation did not reach the requested state"
        assert response["requested_is_locked"] is True
        assert response["is_locked"] is False
        assert response["lock_state_code"] == 0
        assert device.is_locked is False
        assert device.lock_reset_status["lock_state_code"] == 0

    @pytest.mark.asyncio
    async def test_lock_operation_timeout_does_not_use_operation_or_cached_state(self, monkeypatch):
        device = make_device()
        device.is_locked = True
        device.lock_reset_status = {"lock_state_code": 1, "is_locked": True, "status_code": 0}
        http_server = make_http_server({"dev1": device})
        http_server.application.probe_lock_status.return_value = None
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")

        status, response = await post(http_server, "/lock", {"device": "dev1", "pin": "1234"})

        assert status == 504
        assert response["error"] == "lock status readback was not reported"
        assert response["is_locked"] is None
        assert response["operation_result"] == {"success": True, "lock_state": 1}
        assert device.is_locked is None
        assert device.lock_reset_status is None
        event = http_server.application.dispatcher.emit_nowait.call_args.args[0]
        assert event.type == EventType.DEVICE_UPDATED

    @pytest.mark.asyncio
    async def test_lock_status_uses_observation_after_0x1008_instead_of_cached_state(self):
        device = make_device()
        device.is_locked = True
        http_server = make_http_server({"dev1": device})

        status, body = await get(http_server, "/lock-status/Device1")

        assert status == 200
        assert body == {
            "device": "dev1",
            "is_locked": False,
            "lock_state_code": 0,
            "status_code": 0,
            "observed_at": "2026-08-21T20:57:35.396345+00:00",
            "observation_source": "observed_after_0x1008",
        }
        assert device.is_locked is False
        assert device.lock_reset_status["lock_state_code"] == 0
        http_server.application.probe_lock_status.assert_awaited_once_with("192.168.1.50", timeout=4.0)

    @pytest.mark.asyncio
    async def test_lock_status_timeout_does_not_return_cached_state(self):
        device = make_device()
        device.is_locked = True
        device.lock_reset_status = {"lock_state_code": 1, "is_locked": True, "status_code": 0}
        http_server = make_http_server({"dev1": device})
        http_server.application.probe_lock_status.return_value = None

        status, body = await get(http_server, "/lock-status/dev1")

        assert status == 504
        assert body == {
            "error": "lock status was not reported",
            "device": "dev1",
            "is_locked": None,
        }
        assert device.is_locked is None
        assert device.lock_reset_status is None
        event = http_server.application.dispatcher.emit_nowait.call_args.args[0]
        assert event.type == EventType.DEVICE_UPDATED

    @pytest.mark.asyncio
    async def test_unknown_lock_state_is_returned_without_coercion(self):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        http_server.application.probe_lock_status.return_value = LockStatusObservation(
            lock_reset_status={
                "lock_state_code": 2,
                "is_locked": None,
                "status_code": 4,
            },
            observed_at="2026-08-21T20:57:35.396345+00:00",
        )

        status, body = await get(http_server, "/lock-status/dev1")

        assert status == 200
        assert body["is_locked"] is None
        assert body["lock_state_code"] == 2
        assert body["status_code"] == 4
        assert device.is_locked is None
        assert device.lock_reset_status["lock_state_code"] == 2

    @pytest.mark.asyncio
    async def test_lock_status_refuses_offline_device_without_probing(self):
        device = make_device()
        device.online = False
        http_server = make_http_server({"dev1": device})

        status, body = await get(http_server, "/lock-status/dev1")

        assert status == 409
        assert body == {"error": "device is offline"}
        http_server.application.probe_lock_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lock_status_json_responses_are_never_cacheable(self):
        cases = []

        http_server = make_http_server({"dev1": make_device()})
        cases.append((http_server, "/lock-status/dev1"))

        timeout_http_server = make_http_server({"dev1": make_device()})
        timeout_http_server.application.probe_lock_status.return_value = None
        cases.append((timeout_http_server, "/lock-status/dev1"))

        offline_device = make_device()
        offline_device.online = False
        cases.append((make_http_server({"dev1": offline_device}), "/lock-status/dev1"))

        no_ip_device = make_device()
        no_ip_device.ipv4 = None
        cases.append((make_http_server({"dev1": no_ip_device}), "/lock-status/dev1"))
        cases.append((make_http_server({}), "/lock-status/missing"))

        statuses = []
        for case_http_server, path in cases:
            writer = FakeWriter()
            await case_http_server._dispatch("GET", path, None, writer)
            statuses.append(writer.response()[0])
            assert writer.response_headers()["cache-control"] == "no-store"

        assert statuses == [200, 504, 409, 409, 404]

    @pytest.mark.asyncio
    async def test_sse_device_update_carries_explicit_unknown_lock_state(self):
        device = DanteDevice(server_name="dev1")
        device.name = "Device1"
        device.ipv4 = "192.168.1.50"
        device.is_locked = None
        http_server = make_http_server({"dev1": device})
        http_server._broadcast_sse = AsyncMock()

        await http_server._on_device_event(DanteEvent(type=EventType.DEVICE_UPDATED, server_name=device.server_name))

        payload = http_server._broadcast_sse.await_args.args[0]
        assert "is_locked" in payload["device"]
        assert payload["device"]["is_locked"] is None

    @pytest.mark.asyncio
    async def test_lock_operations_are_serialized_through_post_request_observation(self, monkeypatch):
        device = make_device()
        http_server = make_http_server({"dev1": device})
        monkeypatch.setattr(http_server, "_get_lock_key", lambda: b"key")
        events = []
        first_probe_started = asyncio.Event()
        release_first_probe = asyncio.Event()

        async def lock_device(pin, key):
            events.append("lock")
            return {"success": True, "lock_state": 1}

        async def unlock_device(pin, key):
            events.append("unlock")
            return {"success": True, "lock_state": 0}

        async def probe_lock_status(device_ip_address, timeout):
            probe_number = sum(event.startswith("probe") for event in events) + 1
            events.append(f"probe-{probe_number}")
            if probe_number == 1:
                first_probe_started.set()
                await release_first_probe.wait()
                is_locked = True
                lock_state_code = 1
            else:
                is_locked = False
                lock_state_code = 0
            return LockStatusObservation(
                lock_reset_status={
                    "lock_state_code": lock_state_code,
                    "is_locked": is_locked,
                    "status_code": 0,
                },
                observed_at="2026-08-21T20:57:35.396345+00:00",
            )

        device.operations.lock_device = lock_device
        device.operations.unlock_device = unlock_device
        http_server.application.probe_lock_status = probe_lock_status

        lock_task = asyncio.create_task(post(http_server, "/lock", {"device": "dev1", "pin": "1234"}))
        await asyncio.wait_for(first_probe_started.wait(), timeout=1)
        unlock_task = asyncio.create_task(post(http_server, "/unlock", {"device": "dev1", "pin": "1234"}))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert events == ["lock", "probe-1"]

        release_first_probe.set()
        lock_response, unlock_response = await asyncio.gather(lock_task, unlock_task)

        assert lock_response[0] == 200
        assert unlock_response[0] == 200
        assert events == ["lock", "probe-1", "unlock", "probe-2"]
        assert device.is_locked is False
