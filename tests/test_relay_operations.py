import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.daemon.relay import RelayServer
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.services.notification import DanteNotificationService
from tests.relay_test_support import FakeWriter, get, make_device, make_relay, post


class TestRename:
    @pytest.mark.asyncio
    async def test_rename_device_sets_name(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(relay, "/rename-device", {"device": "dev1", "name": "Desk-IO"})
        assert status == 200
        assert body == {"success": True}
        device.operations.set_name.assert_awaited_once_with("Desk-IO")
        device.operations.reset_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rename_device_with_empty_name_resets_name(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, response = await post(relay, "/rename-device", {"device": "dev1", "name": ""})
        assert status == 200
        assert response == {"success": True}
        device.operations.reset_name.assert_awaited_once()
        device.operations.set_name.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [{"device": "dev1"}, {"device": "dev1", "name": None}])
    async def test_rename_device_requires_string_name(self, body):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, response = await post(relay, "/rename-device", body)
        assert status == 400
        assert response == {"error": "name must be a string"}
        device.operations.reset_name.assert_not_awaited()
        device.operations.set_name.assert_not_awaited()


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_single_subscription(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "rx_channel": 3,
                "tx_channel": "Out1",
                "tx_device": "Mixer",
            },
        )
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
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "subscriptions": [
                    {"rx_channel": 1, "tx_channel": "Out1", "tx_device": "Mixer"},
                    {"rx_channel": 2, "tx_channel": "Out2", "tx_device": "Mixer"},
                ],
            },
        )
        assert status == 200
        assert body == {"success": True, "count": 2}
        device.operations.add_subscriptions_by_name.assert_awaited_once_with(
            [(1, "Out1", "Mixer"), (2, "Out2", "Mixer")]
        )

    @pytest.mark.asyncio
    async def test_bulk_subscription_malformed_entry_returns_400(self):
        device = make_device()
        relay = make_relay({"dev1": device})
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "dev1",
                "subscriptions": [{"rx_channel": 1, "tx_channel": "Out1"}],
            },
        )
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
        status, body = await post(
            relay,
            "/subscribe",
            {
                "rx_device": "ghost",
                "rx_channel": 1,
                "tx_channel": "Out1",
                "tx_device": "Mixer",
            },
        )
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
    async def test_cache_without_metering_returns_empty(self):
        relay = make_relay()
        writer = FakeWriter()

        await relay._dispatch("GET", "/metering/cache", None, writer)

        assert writer.response() == (200, {})

    @pytest.mark.asyncio
    async def test_cache_is_keyed_by_server_and_never_requests_snapshot(self):
        levels = {
            "dev1": {
                "tx": {1: 0x7B},
                "rx": {},
                "metering_source": "signal_presence",
            }
        }
        metering = SimpleNamespace(
            get_cached_levels_by_server=MagicMock(return_value=levels),
            snapshot=AsyncMock(),
        )
        relay = make_relay(metering=metering)
        writer = FakeWriter()

        await relay._dispatch("GET", "/metering/cache", None, writer)

        assert writer.response() == (
            200,
            {
                "dev1": {
                    "tx": {"1": 0x7B},
                    "rx": {},
                    "metering_source": "signal_presence",
                }
            },
        )
        metering.get_cached_levels_by_server.assert_called_once_with()
        metering.snapshot.assert_not_awaited()

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
            snapshot=AsyncMock(
                return_value={
                    "tx": {1: -20.0},
                    "rx": {},
                    "wall_time": 123.0,
                    "source_ip": "192.168.1.50",
                    "metering_source": "detailed",
                }
            )
        )
        relay = make_relay({"dev1": device}, metering=metering)
        writer = FakeWriter()
        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)
        status, body = writer.response()
        assert status == 200
        assert body["tx"]["1"] == {"name": "Mic", "level": -20.0}
        assert body["wall_time"] == 123.0
        assert body["metering_source"] == "detailed"

    @pytest.mark.asyncio
    async def test_passive_snapshot_returns_basic_indication_and_source(self):
        device = make_device()
        channel = SimpleNamespace(number=1, name="ch1", friendly_name="Mic")
        device.tx_channels = {1: channel}
        device.rx_channels = {}
        metering = SimpleNamespace(
            snapshot=AsyncMock(
                return_value={
                    "tx": {1: 0x7B},
                    "rx": {},
                    "tx_signal_presence": {1: "signal_present"},
                    "wall_time": 123.0,
                    "source_ip": "192.168.1.50",
                    "source_port": 8700,
                    "metering_source": "signal_presence",
                }
            )
        )
        relay = make_relay({"dev1": device}, metering=metering)
        writer = FakeWriter()

        await relay._dispatch("GET", "/metering/snapshot/dev1", None, writer)

        status, body = writer.response()
        assert status == 200
        assert body["tx"]["1"] == {"name": "Mic", "level": 0x7B, "signal_presence": "signal_present"}
        assert body["metering_source"] == "signal_presence"
        assert body["source_port"] == 8700

    @pytest.mark.asyncio
    async def test_meter_values_sse_preserves_source_metadata(self):
        relay = make_relay()
        relay._broadcast_sse = AsyncMock()
        event = DanteEvent(
            type=EventType.METER_VALUES,
            server_name="dev1",
            data={
                "tx": {1: 0xFE},
                "rx": {},
                "metering_source": "signal_presence",
                "wall_time": 123.0,
                "source_ip": "192.168.1.61",
                "source_port": 8700,
                "tx_signal_presence": {1: "muted"},
            },
        )

        await relay._on_meter_values(event)

        payload = relay._broadcast_sse.call_args.args[0]
        assert payload["metering_source"] == "signal_presence"
        assert payload["source_ip"] == "192.168.1.61"
        assert payload["source_port"] == 8700
        assert payload["tx_signal_presence"] == {1: "muted"}


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
