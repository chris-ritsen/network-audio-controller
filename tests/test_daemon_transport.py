import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante import flows

from tests.http_api_test_support import FakeWriter, get, make_device, make_http_server, post


class BlockingReader:
    def __init__(self):
        self.disconnected = asyncio.Event()

    async def read(self, _size):
        await self.disconnected.wait()
        return b""


class NeverDrainsWriter(FakeWriter):
    async def drain(self):
        await asyncio.Event().wait()


class TestSseLifecycle:
    @pytest.mark.asyncio
    async def test_initial_snapshot_includes_fresh_metering_cache_by_server(self):
        metering = SimpleNamespace(
            get_cached_levels_by_server=MagicMock(
                return_value={
                    "dev1": {
                        "tx": {17: 0x31},
                        "rx": {},
                        "metering_source": "signal_presence",
                    }
                }
            )
        )
        http_server = make_http_server(metering=metering)
        writer = FakeWriter()
        reader = BlockingReader()
        handler = asyncio.create_task(http_server._handle_sse(writer, reader))

        for _ in range(20):
            if b'"metering"' in writer.data:
                break
            await asyncio.sleep(0)

        reader.disconnected.set()
        await handler

        event_bytes = bytes(writer.data).split(b"\r\n\r\n", 1)[1]
        event = json.loads(event_bytes.removeprefix(b"data: ").split(b"\n\n", 1)[0])
        assert event["metering"] == {
            "dev1": {
                "tx": {"17": 0x31},
                "rx": {},
                "metering_source": "signal_presence",
            }
        }
        metering.get_cached_levels_by_server.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_real_tcp_peer_disconnect_removes_sse_client(self, monkeypatch):
        http_server = make_http_server()
        http_server.port = 0
        monkeypatch.setattr(http_server, "_reconcile_bonjour", AsyncMock())

        await http_server.start()
        try:
            port = http_server.tcp_server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()

            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=1)
            snapshot = await asyncio.wait_for(reader.readuntil(b"\n\n"), timeout=1)
            assert b"200 OK" in header
            assert b'"event": "snapshot"' in snapshot
            assert len(http_server.sse_clients) == 1

            writer.close()
            await writer.wait_closed()
            for _ in range(100):
                if not http_server.sse_clients:
                    break
                await asyncio.sleep(0.01)
            assert http_server.sse_clients == {}
        finally:
            await http_server.stop()

    @pytest.mark.asyncio
    async def test_bounded_timeout_drains_cancelled_child_finally(self):
        import netaudio.daemon.http_api as http_api_module

        started = asyncio.Event()
        finalized = asyncio.Event()

        async def child():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.set()

        with pytest.raises(asyncio.TimeoutError):
            await http_api_module._bounded(child(), 0.01)

        assert started.is_set()
        assert finalized.is_set()

    @pytest.mark.asyncio
    async def test_bounded_outer_cancellation_drains_cancelled_child_finally(self):
        import netaudio.daemon.http_api as http_api_module

        started = asyncio.Event()
        finalized = asyncio.Event()

        async def child():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.set()

        parent = asyncio.create_task(http_api_module._bounded(child(), 30))
        await started.wait()
        parent.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parent

        assert finalized.is_set()

    @pytest.mark.asyncio
    async def test_snapshot_and_events_are_fifo_per_client(self):
        http_server = make_http_server()
        writer = FakeWriter()
        reader = BlockingReader()
        handler = asyncio.create_task(http_server._handle_sse(writer, reader))

        for _ in range(20):
            if http_server.sse_clients:
                break
            await asyncio.sleep(0)
        assert len(http_server.sse_clients) == 1

        await http_server._broadcast_sse({"event": "update", "sequence": 1})
        await http_server._broadcast_sse({"event": "update", "sequence": 2})
        for _ in range(20):
            if b'"sequence": 2' in writer.data:
                break
            await asyncio.sleep(0)

        reader.disconnected.set()
        await handler

        payload = bytes(writer.data)
        assert payload.index(b'"event": "snapshot"') < payload.index(b'"sequence": 1')
        assert payload.index(b'"sequence": 1') < payload.index(b'"sequence": 2')
        assert http_server.sse_clients == {}
        assert writer.closed is True

    @pytest.mark.asyncio
    async def test_full_client_queue_disconnects_only_that_client(self):
        import netaudio.daemon.http_api as http_api_module

        http_server = make_http_server()
        slow_writer = FakeWriter()
        fast_writer = FakeWriter()
        slow = http_api_module._SseClient(slow_writer, queue=asyncio.Queue(maxsize=1))
        fast = http_api_module._SseClient(fast_writer, queue=asyncio.Queue(maxsize=2))
        slow.queue.put_nowait(b"already queued")
        http_server.sse_clients = {slow_writer: slow, fast_writer: fast}

        await http_server._broadcast_sse({"event": "device_updated"})

        assert slow_writer not in http_server.sse_clients
        assert slow.closed.is_set()
        assert slow_writer.closed is True
        assert http_server.sse_clients[fast_writer] is fast
        assert b"device_updated" in fast.queue.get_nowait()

    @pytest.mark.asyncio
    async def test_timed_out_writer_is_removed_without_blocking_broadcast(self, monkeypatch):
        import netaudio.daemon.http_api as http_api_module

        monkeypatch.setattr(http_api_module, "SSE_DRAIN_TIMEOUT_SECONDS", 0.01)
        http_server = make_http_server()
        writer = NeverDrainsWriter()
        client = http_api_module._SseClient(writer)
        http_server.sse_clients[writer] = client
        client.sender_task = asyncio.create_task(http_server._sse_sender(client))

        await http_server._broadcast_sse({"event": "meter_values"})
        await asyncio.wait_for(client.closed.wait(), timeout=0.2)

        assert writer not in http_server.sse_clients
        assert writer.closed is True
        await asyncio.gather(client.sender_task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_cancelled_handler_cleans_reader_sender_and_writer(self):
        http_server = make_http_server()
        writer = FakeWriter()
        reader = BlockingReader()
        handler = asyncio.create_task(http_server._handle_sse(writer, reader))
        for _ in range(20):
            if http_server.sse_clients:
                break
            await asyncio.sleep(0)

        handler.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handler

        assert http_server.sse_clients == {}
        assert writer.closed is True

    @pytest.mark.asyncio
    async def test_stop_is_idempotent_and_closes_client_tasks(self):
        import netaudio.daemon.http_api as http_api_module

        http_server = make_http_server()
        http_server.tcp_server = FakeTcpServer()
        writer = NeverDrainsWriter()
        client = http_api_module._SseClient(writer)
        http_server.sse_clients[writer] = client
        client.sender_task = asyncio.create_task(http_server._sse_sender(client))
        client.queue.put_nowait(b"pending")

        await http_server.stop()
        await http_server.stop()

        assert http_server.tcp_server is None
        assert http_server.sse_clients == {}
        assert writer.closed is True
        assert client.sender_task.done()


class FakeTcpServer:
    def __init__(self):
        self.closed = False
        self.wait_closed_called = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.wait_closed_called = True


class FakeAsyncZeroconf:
    instances = []
    fail_next_register = False
    rename_next_register = False

    def __init__(self, **kwargs):
        self.options = kwargs
        self.registered = []
        self.register_options = []
        self.updated = []
        self.unregistered = []
        self.closed = False
        FakeAsyncZeroconf.instances.append(self)

    async def async_register_service(self, service_info, **kwargs):
        self.register_options.append(kwargs)
        if FakeAsyncZeroconf.fail_next_register:
            FakeAsyncZeroconf.fail_next_register = False
            raise RuntimeError("register failed")
        if FakeAsyncZeroconf.rename_next_register:
            FakeAsyncZeroconf.rename_next_register = False
            service_info.name = f"netaudio-daemon conflict-2.{service_info.type}"
        self.registered.append(service_info)

    async def async_update_service(self, service_info):
        self.updated.append(service_info)

    async def async_unregister_service(self, service_info):
        self.unregistered.append(service_info)

    async def async_close(self):
        self.closed = True


def _adapter(*addresses, name="en0"):
    return SimpleNamespace(
        nice_name=name,
        ips=[SimpleNamespace(ip=address) for address in addresses],
    )


@pytest.fixture
def bonjour_http_server(monkeypatch):
    import netaudio.daemon.http_api as http_api_module

    FakeAsyncZeroconf.instances = []
    FakeAsyncZeroconf.fail_next_register = False
    FakeAsyncZeroconf.rename_next_register = False

    async def fake_start_server(*_args, **_kwargs):
        return FakeTcpServer()

    monkeypatch.setattr(http_api_module.asyncio, "start_server", fake_start_server)
    monkeypatch.setattr(http_api_module, "AsyncZeroconf", FakeAsyncZeroconf)
    monkeypatch.setattr(http_api_module.socket, "gethostname", lambda: "netaudio-test-host")
    monkeypatch.setattr(http_api_module.app_settings, "_interface", None, raising=False)
    monkeypatch.setattr(http_api_module.app_settings, "_interface_ip", None, raising=False)

    return make_http_server()


class TestBonjourReconcile:
    @pytest.mark.asyncio
    async def test_long_hostname_produces_bounded_stable_service_identity(self, bonjour_http_server, monkeypatch):
        import netaudio.daemon.http_api as http_api_module

        hostname = "sat12-bq150-7dbdb9ac-5e81-4902-96fc-5a5171b62dcd-F2CBA1AA41D9"
        monkeypatch.setattr(http_api_module.socket, "gethostname", lambda: hostname)
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))

        await bonjour_http_server._reconcile_bonjour(force=True)

        service_info = bonjour_http_server.service_info
        instance_label = service_info.name.removesuffix(f".{http_api_module.DAEMON_SERVICE_TYPE}")
        assert instance_label == "netaudio-daemon (sat12-bq150-7dbdb9ac-5e81-4902-9-4b24fff9a31b)"
        assert len(instance_label.encode("utf-8")) == http_api_module.SERVICE_LABEL_MAXIMUM_BYTES
        assert service_info.server == f"{hostname}.local."

        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_start_registers_all_non_loopback_ipv4_addresses(self, bonjour_http_server, monkeypatch):
        import netaudio.daemon.http_api as http_api_module

        monkeypatch.setattr(
            http_api_module.ifaddr,
            "get_adapters",
            lambda: [
                _adapter("127.0.0.1"),
                _adapter("192.168.1.44", "169.254.9.10"),
            ],
        )

        await bonjour_http_server.start()

        assert FakeAsyncZeroconf.instances[0].registered
        service_info = FakeAsyncZeroconf.instances[0].registered[0]
        assert set(service_info.parsed_addresses()) == {"192.168.1.44", "169.254.9.10"}
        assert FakeAsyncZeroconf.instances[0].options == {
            "interfaces": ["169.254.9.10", "192.168.1.44"],
            "ip_version": http_api_module.IPVersion.V4Only,
        }
        assert FakeAsyncZeroconf.instances[0].register_options == [{"allow_name_change": True}]
        assert service_info.port == bonjour_http_server.port
        assert service_info.server.endswith(".local.")

        await bonjour_http_server.stop()

    @pytest.mark.asyncio
    async def test_reregisters_when_advertised_addresses_change(self, bonjour_http_server, monkeypatch):
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        await bonjour_http_server._reconcile_bonjour(force=True)

        original_zeroconf = bonjour_http_server.zeroconf
        original_service_info = bonjour_http_server.service_info

        monkeypatch.setattr(
            bonjour_http_server,
            "_get_advertisement_addresses",
            lambda: ("10.0.0.20", "192.168.1.44"),
        )

        await bonjour_http_server._reconcile_bonjour()

        assert original_zeroconf.unregistered == [original_service_info]
        assert original_zeroconf.closed is True
        assert bonjour_http_server.service_info is not None
        assert set(bonjour_http_server.service_info.parsed_addresses()) == {"192.168.1.44", "10.0.0.20"}
        assert len(FakeAsyncZeroconf.instances) == 2

        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_address_loss_unpublishes_and_later_recovers(self, bonjour_http_server, monkeypatch):
        addresses = ["192.168.1.44"]
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: tuple(addresses))

        await bonjour_http_server._reconcile_bonjour(force=True)
        first_zeroconf = bonjour_http_server.zeroconf
        first_service = bonjour_http_server.service_info

        addresses.clear()
        await bonjour_http_server._reconcile_bonjour()

        assert first_zeroconf.unregistered == [first_service]
        assert first_zeroconf.closed is True
        assert bonjour_http_server.zeroconf is None
        assert bonjour_http_server.service_info is None

        addresses.append("10.0.0.20")
        await bonjour_http_server._reconcile_bonjour()

        assert bonjour_http_server.zeroconf is not None
        assert bonjour_http_server.zeroconf is not first_zeroconf
        assert bonjour_http_server.service_info.parsed_addresses() == ["10.0.0.20"]
        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_selected_interface_limits_registration_addresses(self, bonjour_http_server, monkeypatch):
        import netaudio.daemon.http_api as http_api_module

        monkeypatch.setattr(http_api_module.app_settings, "_interface", "en7", raising=False)
        monkeypatch.setattr(
            http_api_module.ifaddr,
            "get_adapters",
            lambda: [
                _adapter("192.168.1.44", name="en0"),
                _adapter("10.0.0.20", "127.0.0.1", name="en7"),
            ],
        )

        await bonjour_http_server._reconcile_bonjour(force=True)

        assert bonjour_http_server._bonjour_addresses == ("10.0.0.20",)
        assert FakeAsyncZeroconf.instances[0].options["interfaces"] == ["10.0.0.20"]
        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_conflict_renamed_service_keeps_registered_name_on_update(self, bonjour_http_server, monkeypatch):
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        FakeAsyncZeroconf.rename_next_register = True

        await bonjour_http_server._reconcile_bonjour(force=True)
        registered_name = bonjour_http_server.service_info.name
        assert "conflict-2" in registered_name

        await bonjour_http_server._publish_bonjour(
            ("192.168.1.44",),
            reason="periodic refresh",
            recreate_service=False,
        )

        assert FakeAsyncZeroconf.instances[0].updated[0].name == registered_name
        assert bonjour_http_server.service_info.name == registered_name
        assert len(FakeAsyncZeroconf.instances) == 1
        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_start_failure_closes_tcp_server_and_unregisters_listeners(self, bonjour_http_server, monkeypatch):
        import netaudio.daemon.http_api as http_api_module

        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        monkeypatch.setattr(
            http_api_module,
            "AsyncZeroconf",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("constructor failed")),
        )

        with pytest.raises(RuntimeError, match="constructor failed"):
            await bonjour_http_server.start()

        assert bonjour_http_server.tcp_server is None
        assert bonjour_http_server._events_registered is False
        assert bonjour_http_server.application.dispatcher.off.call_count == 8


class TestTxFlows:
    def test_flow_query_and_mutation_protocol_sets_are_distinct(self):
        from netaudio.dante.const import (
            FLOW_CREATE_PROTOCOL_IDS,
            FLOW_DELETE_PROTOCOL_IDS,
            FLOW_QUERY_PROTOCOL_IDS,
        )

        assert FLOW_QUERY_PROTOCOL_IDS == (0x2729, 0x2801, 0x2809)
        assert FLOW_CREATE_PROTOCOL_IDS == (0x2729, 0x2801)
        assert FLOW_DELETE_PROTOCOL_IDS == (0x2729, 0x2801, 0x2809)

    @staticmethod
    def _flow(slot=17, flow_type="multicast", channels=None):
        return {
            "flow_number": slot,
            "flow_type": flow_type,
            "sample_rate": 48000,
            "encoding": 24,
            "frames_per_packet": 48,
            "channel_count": len(channels or [1, 2]),
            "channels": channels or [1, 2],
        }

    @staticmethod
    def _mock_api(monkeypatch, device_flows=None, max_flow_slots=32):
        detect = AsyncMock(return_value=0x2729)
        query = AsyncMock(return_value={"max_flow_slots": max_flow_slots, "flows": device_flows or []})
        create = AsyncMock(return_value=0x0001)
        delete = AsyncMock(return_value=0x0001)
        monkeypatch.setattr(flows, "detect_flow_protocol", detect)
        monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
        monkeypatch.setattr(flows, "create_tx_flow", create)
        monkeypatch.setattr(flows, "delete_tx_flow", delete)
        return detect, query, create, delete

    @pytest.mark.asyncio
    async def test_lists_device_tx_flows(self, monkeypatch):
        device = make_device()
        flow = self._flow()
        detect, query, _, _ = self._mock_api(monkeypatch, [flow])
        http_server = make_http_server({"dev1": device})

        status, body = await get(http_server, "/flows/dev1")

        assert status == 200
        assert body == {
            "device": "dev1",
            "flow_protocol_id": 0x2729,
            "max_flow_slots": 32,
            "flows": [flow],
        }
        detect.assert_awaited_once_with("192.168.1.50", 4440)
        query.assert_awaited_once_with("192.168.1.50", 4440, 0x2809)

    @pytest.mark.asyncio
    async def test_create_requires_explicit_confirmation(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, query, create, _ = self._mock_api(monkeypatch)
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1]})

        assert status == 400
        assert body == {"error": "confirmed must be true"}
        query.assert_not_awaited()
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_rejects_unknown_channel_and_occupied_slot(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, _, create, _ = self._mock_api(monkeypatch, [self._flow(slot=17, channels=[1])])
        http_server = make_http_server({"dev1": device})

        status, body = await post(
            http_server, "/flows/create", {"device": "dev1", "flow_slot": 18, "channels": [2], "confirmed": True}
        )
        assert status == 404
        assert body == {"error": "tx channel not found: 2"}

        status, body = await post(
            http_server, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1], "confirmed": True}
        )
        assert status == 409
        assert body == {"error": "flow slot 17 is already in use"}
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_rejects_slot_above_device_capacity(self, monkeypatch):
        device = make_device()
        device.tx_channels = {1: SimpleNamespace(number=1)}
        _, _, create, _ = self._mock_api(monkeypatch, max_flow_slots=4)
        http_server = make_http_server({"dev1": device})

        status, body = await post(
            http_server, "/flows/create", {"device": "dev1", "flow_slot": 5, "channels": [1], "confirmed": True}
        )

        assert status == 409
        assert body == {"error": "flow slot 5 exceeds the device capacity of 4"}
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_sends_guarded_channel_selection(self, monkeypatch):
        device = make_device()
        device.tx_channels = {
            1: SimpleNamespace(number=1),
            2: SimpleNamespace(number=2),
        }
        _, _, create, _ = self._mock_api(monkeypatch)

        async def create_while_locked(*_arguments):
            assert device.topology_mutation_lock.locked()
            return 0x0001

        create.side_effect = create_while_locked
        http_server = make_http_server({"dev1": device})

        status, body = await post(
            http_server, "/flows/create", {"device": "dev1", "flow_slot": 17, "channels": [1, 2], "confirmed": True}
        )

        assert status == 200
        assert body["success"] is True
        create.assert_awaited_once_with("192.168.1.50", 4440, 0x2729, 17, [1, 2])

    @pytest.mark.asyncio
    async def test_delete_only_allows_an_existing_multicast_flow(self, monkeypatch):
        device = make_device()
        _, _, _, delete = self._mock_api(monkeypatch, [self._flow(flow_type="0x0001")])
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/flows/delete", {"device": "dev1", "flow_slot": 17, "confirmed": True})

        assert status == 409
        assert body == {"error": "flow slot 17 is not multicast"}
        delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_sends_confirmed_multicast_slot(self, monkeypatch):
        device = make_device()
        _, _, _, delete = self._mock_api(monkeypatch, [self._flow()])
        http_server = make_http_server({"dev1": device})

        status, body = await post(http_server, "/flows/delete", {"device": "dev1", "flow_slot": 17, "confirmed": True})

        assert status == 200
        assert body["success"] is True
        delete.assert_awaited_once_with("192.168.1.50", 4440, 0x2729, 17)


class TestBonjourRecovery:
    @pytest.mark.asyncio
    async def test_retries_after_failed_bonjour_registration(self, bonjour_http_server, monkeypatch):
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))

        FakeAsyncZeroconf.fail_next_register = True
        await bonjour_http_server._reconcile_bonjour(force=True)

        assert bonjour_http_server.zeroconf is None
        assert bonjour_http_server.service_info is None
        assert len(FakeAsyncZeroconf.instances[0].unregistered) == 1
        assert FakeAsyncZeroconf.instances[0].closed is True

        await bonjour_http_server._reconcile_bonjour()

        assert bonjour_http_server.zeroconf is not None
        assert bonjour_http_server.service_info is not None
        assert bonjour_http_server.service_info.parsed_addresses() == ["192.168.1.44"]

        await bonjour_http_server._close_bonjour()

    @pytest.mark.asyncio
    async def test_reregisters_after_wake_gap_even_when_address_is_unchanged(self, bonjour_http_server, monkeypatch):
        monkeypatch.setattr(bonjour_http_server, "_get_advertisement_addresses", lambda: ("192.168.1.44",))
        await bonjour_http_server._reconcile_bonjour(force=True)

        original_zeroconf = bonjour_http_server.zeroconf
        original_service_info = bonjour_http_server.service_info

        await bonjour_http_server._reconcile_bonjour(woke_from_sleep=True)

        assert original_zeroconf.unregistered == [original_service_info]
        assert original_zeroconf.closed is True
        assert bonjour_http_server.zeroconf is not original_zeroconf
        assert bonjour_http_server.service_info is not None
        assert bonjour_http_server.service_info.parsed_addresses() == ["192.168.1.44"]

        await bonjour_http_server._close_bonjour()
