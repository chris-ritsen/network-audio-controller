import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.daemon.relay import RelayServer


class FakeWriter:
    def __init__(self, peer=("127.0.0.1", 40000)):
        self.data = bytearray()
        self.closed = False
        self.peer = peer

    def get_extra_info(self, name):
        if name == "peername":
            return self.peer
        return None

    def write(self, payload):
        self.data.extend(payload)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    def response(self):
        raw = bytes(self.data).decode()
        header, _, body = raw.partition("\r\n\r\n")
        status = int(header.split(" ")[1])
        return status, json.loads(body) if body else None


def make_device(server_name="dev1", name="Device1", ipv4="192.168.1.50"):
    device = SimpleNamespace(
        server_name=server_name,
        name=name,
        ipv4=ipv4,
        rx_channels={},
        is_locked=False,
        operations=MagicMock(),
    )
    for method in (
        "add_subscription_by_name",
        "add_subscriptions_by_name",
        "remove_subscription",
        "remove_subscriptions",
        "identify",
        "set_name",
        "set_channel_name",
        "set_latency",
        "set_sample_rate",
        "set_encoding",
        "set_gain_level",
        "enable_aes67",
        "reboot",
        "lock_device",
        "unlock_device",
    ):
        setattr(device.operations, method, AsyncMock(return_value=None))
    return device


def make_relay(devices=None, metering=None, on_shutdown=None):
    application = SimpleNamespace(
        devices=devices or {},
        dispatcher=MagicMock(),
        settings=MagicMock(),
        mark_device_offline=MagicMock(),
    )
    state = SimpleNamespace(
        refresh_device=AsyncMock(),
        refresh_all_devices=AsyncMock(),
    )
    return RelayServer(application, state, metering=metering, on_shutdown=on_shutdown)


async def post(relay, path, body):
    writer = FakeWriter()
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
    await relay._dispatch("POST", path, body, writer)
    return writer.response()


class TestRouting:
    @pytest.mark.asyncio
    async def test_unknown_path_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/nonexistent", {"device": "x"})
        assert status == 404
        assert body == {"error": "not found"}

    @pytest.mark.asyncio
    async def test_unknown_method_returns_404(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("DELETE", "/devices", None, writer)
        status, body = writer.response()
        assert status == 404

    @pytest.mark.asyncio
    async def test_missing_body_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", None)
        assert status == 400
        assert body == {"error": "missing body"}

    @pytest.mark.asyncio
    async def test_malformed_json_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", b"{not json")
        assert status == 400
        assert "invalid json" in body["error"]

    @pytest.mark.asyncio
    async def test_non_object_body_returns_400(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", b'["a", "b"]')
        assert status == 400
        assert body == {"error": "body must be a json object"}

    @pytest.mark.asyncio
    async def test_refresh_allows_missing_body(self):
        relay = make_relay()
        status, body = await post(relay, "/refresh", None)
        assert status == 200
        assert body == {"success": True}
        relay.state.refresh_all_devices.assert_awaited_once()


class TestErrorMapping:
    @pytest.mark.asyncio
    async def test_handler_exception_returns_500(self):
        device = make_device()
        device.operations.identify.side_effect = RuntimeError("socket exploded")
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._route("POST", "/identify", json.dumps({"device": "dev1"}).encode(), writer, None)
        status, body = writer.response()
        assert status == 500
        assert body == {"error": "socket exploded"}

    @pytest.mark.asyncio
    async def test_handler_timeout_returns_504(self):
        device = make_device()
        device.operations.set_latency.side_effect = TimeoutError()
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._route(
            "POST", "/set-latency", json.dumps({"device": "dev1", "latency": 5}).encode(), writer, None
        )
        status, body = writer.response()
        assert status == 504
        assert body == {"error": "device did not respond"}


class TestDeviceLookup:
    @pytest.mark.asyncio
    async def test_unknown_device_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/identify", {"device": "ghost"})
        assert status == 404
        assert body == {"error": "device not found"}

    @pytest.mark.asyncio
    async def test_lookup_by_friendly_name_case_insensitive(self):
        device = make_device(server_name="dev1", name="Studio-AVIO")
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/identify", {"device": "studio-avio"})
        assert status == 200
        device.operations.identify.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lookup_by_ip_address(self):
        device = make_device(ipv4="192.168.1.50")
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/identify", {"device": "192.168.1.50"})
        assert status == 200


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_single_subscription(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {
            "rx_device": "dev1",
            "rx_channel": 3,
            "tx_channel": "Out1",
            "tx_device": "Mixer",
        })
        assert status == 200
        assert body == {"success": True}
        device.operations.add_subscription_by_name.assert_awaited_once_with(3, "Out1", "Mixer")

    @pytest.mark.asyncio
    async def test_single_subscription_missing_fields_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {"rx_device": "dev1", "rx_channel": 3})
        assert status == 400
        device.operations.add_subscription_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bulk_subscriptions(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {
            "rx_device": "dev1",
            "subscriptions": [
                {"rx_channel": 1, "tx_channel": "Out1", "tx_device": "Mixer"},
                {"rx_channel": 2, "tx_channel": "Out2", "tx_device": "Mixer"},
            ],
        })
        assert status == 200
        assert body == {"success": True, "count": 2}
        device.operations.add_subscriptions_by_name.assert_awaited_once_with(
            [(1, "Out1", "Mixer"), (2, "Out2", "Mixer")]
        )

    @pytest.mark.asyncio
    async def test_bulk_subscription_malformed_entry_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {
            "rx_device": "dev1",
            "subscriptions": [{"rx_channel": 1, "tx_channel": "Out1"}],
        })
        assert status == 400
        assert "invalid subscription entry" in body["error"]
        device.operations.add_subscriptions_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bulk_subscription_empty_list_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/subscribe", {"rx_device": "dev1", "subscriptions": []})
        assert status == 400
        assert body == {"error": "subscriptions list is empty"}

    @pytest.mark.asyncio
    async def test_unknown_rx_device_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/subscribe", {
            "rx_device": "ghost",
            "rx_channel": 1,
            "tx_channel": "Out1",
            "tx_device": "Mixer",
        })
        assert status == 404
        assert body == {"error": "rx device not found"}


class TestUnsubscribe:
    @pytest.mark.asyncio
    async def test_single_unsubscribe(self):
        device = make_device()
        channel = SimpleNamespace(number=3)
        device.rx_channels = {3: channel}
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channel": 3})
        assert status == 200
        device.operations.remove_subscription.assert_awaited_once_with(channel)

    @pytest.mark.asyncio
    async def test_bulk_unsubscribe(self):
        device = make_device()
        channels = {1: SimpleNamespace(number=1), 2: SimpleNamespace(number=2)}
        device.rx_channels = channels
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channels": [1, 2]})
        assert status == 200
        assert body == {"success": True, "count": 2}
        device.operations.remove_subscriptions.assert_awaited_once_with([channels[1], channels[2]])

    @pytest.mark.asyncio
    async def test_bulk_unsubscribe_unknown_channel_returns_404(self):
        device = make_device()
        device.rx_channels = {1: SimpleNamespace(number=1)}
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/unsubscribe", {"rx_device": "dev1", "rx_channels": [1, 9]})
        assert status == 404
        assert body == {"error": "rx channel 9 not found"}
        device.operations.remove_subscriptions.assert_not_awaited()


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_from_loopback_succeeds(self):
        on_shutdown = MagicMock()
        relay = make_relay(on_shutdown=on_shutdown)
        writer = FakeWriter(peer=("127.0.0.1", 40000))
        await relay._dispatch("POST", "/shutdown", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {"success": True}
        on_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_from_remote_peer_forbidden(self):
        on_shutdown = MagicMock()
        relay = make_relay(on_shutdown=on_shutdown)
        writer = FakeWriter(peer=("192.168.1.50", 40000))
        await relay._dispatch("POST", "/shutdown", None, writer)
        status, body = writer.response()
        assert status == 403
        on_shutdown.assert_not_called()


class TestReportUnresponsive:
    @pytest.mark.asyncio
    async def test_marks_online_device_offline(self):
        device = make_device()
        device.online = True
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/report-unresponsive", {"device": "dev1"})
        assert status == 200
        relay.application.mark_device_offline.assert_called_once_with("dev1")

    @pytest.mark.asyncio
    async def test_unknown_device_returns_404(self):
        relay = make_relay()
        status, body = await post(relay, "/report-unresponsive", {"device": "ghost"})
        assert status == 404


class TestMetering:
    @pytest.mark.asyncio
    async def test_status_without_metering_returns_empty(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/status", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {}

    @pytest.mark.asyncio
    async def test_status_returns_manager_status(self):
        metering = SimpleNamespace(get_status=MagicMock(return_value={"active": ["dev1"]}))
        relay = make_relay(metering=metering)
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/status", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body == {"active": ["dev1"]}

    @pytest.mark.asyncio
    async def test_snapshot_unknown_device_returns_404(self):
        relay = make_relay()
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/ghost", None, writer)
        status, body = writer.response()
        assert status == 404

    @pytest.mark.asyncio
    async def test_snapshot_without_metering_returns_503(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)
        status, body = writer.response()
        assert status == 503

    @pytest.mark.asyncio
    async def test_snapshot_returns_named_levels(self):
        device = make_device()
        channel = SimpleNamespace(number=1, name="ch1", friendly_name="Mic")
        device.tx_channels = {1: channel}
        device.rx_channels = {}
        metering = SimpleNamespace(
            snapshot=AsyncMock(return_value={"tx": {1: -20.0}, "rx": {}, "wall_time": 123.0, "source_ip": "192.168.1.50"})
        )
        relay = make_relay({"dev1": device}, metering=metering)
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body["tx"]["1"] == {"name": "Mic", "level": -20.0}
        assert body["wall_time"] == 123.0


class TestSetInterface:
    @pytest.mark.asyncio
    async def test_invalid_mode_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/interface", {"device": "dev1", "mode": "bogus"})
        assert status == 400
        assert body == {"error": "mode must be 'dhcp' or 'static'"}

    @pytest.mark.asyncio
    async def test_static_mode_requires_ip_and_netmask(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/interface", {"device": "dev1", "mode": "static"})
        assert status == 400
        assert body == {"error": "static mode requires ip, netmask"}
