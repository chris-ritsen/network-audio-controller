import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.state import DanteStateService


def make_device(server_name="dev1.local.", name="Device1"):
    device = DanteDevice(server_name=server_name)
    device.name = name
    device.ipv4 = "192.168.1.50"
    return device


def make_application(devices):
    return SimpleNamespace(
        devices=devices,
        dispatcher=MagicMock(),
        settings=MagicMock(),
        cmc=SimpleNamespace(register_device=AsyncMock()),
        notifications=MagicMock(),
        get_arc_port=lambda device: 4440,
        probe_aes67_state=AsyncMock(return_value=None),
        probe_preferred_leader_state=AsyncMock(return_value=None),
        probe_interface_status=AsyncMock(return_value=None),
        _send_conmon_query_for_device=MagicMock(),
    )


def emitted_events(application):
    return [call.args[0] for call in application.dispatcher.emit_nowait.call_args_list]


class TestRefetchDeviceControls:
    @pytest.mark.asyncio
    async def test_applies_data_and_emits_device_updated(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(
            return_value={"name": "Renamed", "tx_count": 4, "rx_count": 2, "is_locked": True}
        )
        state = DanteStateService(application)

        await state.refetch_device_controls("dev1.local.")

        assert device.name == "Renamed"
        assert device.tx_count == 4
        assert device.rx_count == 2
        assert device.is_locked is True
        events = emitted_events(application)
        assert len(events) == 1
        assert events[0].type == EventType.DEVICE_UPDATED
        assert events[0].server_name == "dev1.local."

    @pytest.mark.asyncio
    async def test_failure_does_not_emit(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(side_effect=TimeoutError("no response"))
        state = DanteStateService(application)

        await state.refetch_device_controls("dev1.local.")

        application.dispatcher.emit_nowait.assert_not_called()

    @pytest.mark.asyncio
    async def test_offline_device_is_skipped(self):
        device = make_device()
        device.online = False
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock()
        state = DanteStateService(application)

        await state.refetch_device_controls("dev1.local.")

        device.fetch_controls_data.assert_not_awaited()


class TestPerDeviceSerialization:
    @pytest.mark.asyncio
    async def test_concurrent_refetches_do_not_interleave(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        order = []

        async def slow_get_rx_channels():
            order.append("enter")
            await asyncio.sleep(0.01)
            order.append("exit")

        device.get_rx_channels = slow_get_rx_channels
        state = DanteStateService(application)
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await asyncio.gather(
            state._on_routing_changed(event),
            state._on_routing_changed(event),
        )

        assert order == ["enter", "exit", "enter", "exit"]


class TestFetchDeviceControls:
    @pytest.mark.asyncio
    async def test_concurrent_fetch_is_dropped_while_populating(self):
        device = make_device()
        application = make_application({"dev1.local.": device})

        async def slow_get_controls_data():
            await asyncio.sleep(0.02)
            return {"name": "Device1", "tx_count": 2, "rx_count": 2}

        device.fetch_controls_data = AsyncMock(side_effect=slow_get_controls_data)
        state = DanteStateService(application)

        await asyncio.gather(
            state.fetch_device_controls("dev1.local."),
            state.fetch_device_controls("dev1.local."),
        )

        assert device.fetch_controls_data.await_count == 1
        events = emitted_events(application)
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_refresh_clears_populating_guard(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(
            return_value={"name": "Device1", "tx_count": 2, "rx_count": 2}
        )
        state = DanteStateService(application)

        await state.refresh_device("dev1.local.")
        await state.refresh_device("dev1.local.")

        assert device.fetch_controls_data.await_count == 2


class TestConmonRetry:
    @pytest.mark.asyncio
    async def test_returns_as_soon_as_response_arrives(self):
        device = make_device()
        device.mac_address = "001dc1aabbcc"
        application = make_application({"dev1.local.": device})
        waiter = asyncio.Event()

        def send_query(target_device, opcode):
            target_device.dante_model_id = "DAI2"
            waiter.set()

        application._send_conmon_query_for_device = MagicMock(side_effect=send_query)
        application.notifications.register_conmon_waiter = MagicMock(return_value=waiter)
        application.notifications.unregister_conmon_waiter = MagicMock()
        state = DanteStateService(application)

        await asyncio.wait_for(state.retry_conmon_query("dev1.local."), timeout=1.0)

        application._send_conmon_query_for_device.assert_called_once_with(device, "dante_model")
        application.notifications.unregister_conmon_waiter.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_model_already_known(self):
        device = make_device()
        device.mac_address = "001dc1aabbcc"
        device.dante_model_id = "DAI2"
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.retry_conmon_query("dev1.local.")

        application._send_conmon_query_for_device.assert_not_called()


class TestRefreshAffectedSubscriptions:
    @pytest.mark.asyncio
    async def test_refetches_subscribers_of_offline_device(self):
        offline_device = make_device(server_name="tx.local.", name="Sender")
        offline_device.online = False
        subscriber = make_device(server_name="rx.local.", name="Receiver")
        subscriber.subscriptions = [SimpleNamespace(tx_device_name="Sender")]
        bystander = make_device(server_name="other.local.", name="Other")

        application = make_application({
            "tx.local.": offline_device,
            "rx.local.": subscriber,
            "other.local.": bystander,
        })
        subscriber.get_rx_channels = AsyncMock()
        bystander.get_rx_channels = AsyncMock()
        state = DanteStateService(application)

        await state.refresh_affected_subscriptions(offline_device)

        subscriber.get_rx_channels.assert_awaited_once()
        bystander.get_rx_channels.assert_not_awaited()
