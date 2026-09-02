import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.services.notification import (
    NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_ROUTING_READY,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
)
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
        probe_sample_rate_status=AsyncMock(return_value=None),
        probe_encoding_status=AsyncMock(return_value=None),
        probe_gain_status=AsyncMock(return_value=None),
        probe_preferred_leader_state=AsyncMock(return_value=None),
        probe_interface_status=AsyncMock(return_value=None),
        probe_clocking_status=AsyncMock(return_value=None),
        probe_lock_status=AsyncMock(return_value=None),
        _send_conmon_query_for_device=MagicMock(),
        on_notification=MagicMock(),
    )


def emitted_events(application):
    return [call.args[0] for call in application.dispatcher.emit_nowait.call_args_list]


def test_notification_registration_is_idempotent():
    application = make_application({})
    state = DanteStateService(application)

    state.register()
    registered_count = application.on_notification.call_count
    state.register()

    assert registered_count > 0
    assert application.on_notification.call_count == registered_count


class TestRoutingNotifications:
    @pytest.mark.asyncio
    async def test_receiver_channel_change_refreshes_only_receiver_channels(self):
        device = make_device()
        device.get_rx_channels = AsyncMock()
        device.get_tx_channels = AsyncMock()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_RX_CHANNEL_CHANGE},
        )

        await state._on_receiver_channel_changed(event)

        device.get_rx_channels.assert_awaited_once()
        device.get_tx_channels.assert_not_awaited()
        assert len(emitted_events(application)) == 1

    @pytest.mark.asyncio
    async def test_transmitter_label_change_refreshes_only_transmitter_channels(self):
        device = make_device()
        device.get_rx_channels = AsyncMock()
        device.get_tx_channels = AsyncMock()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_TX_LABEL_CHANGE},
        )

        await state._on_transmitter_channel_changed(event)

        device.get_tx_channels.assert_awaited_once()
        device.get_rx_channels.assert_not_awaited()
        assert len(emitted_events(application)) == 1

    @pytest.mark.asyncio
    async def test_transmitter_flow_change_detects_frontend_and_refreshes_inventory(self, monkeypatch):
        from netaudio.dante import flows

        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        detect = AsyncMock(return_value=0x2729)
        query = AsyncMock(
            return_value={
                "max_flow_slots": 16,
                "flows": [
                    {
                        "flow_number": 32,
                        "flow_type": "multicast",
                        "channel_count": 8,
                        "sample_rate": 48000,
                        "encoding": 24,
                    }
                ],
            }
        )
        monkeypatch.setattr(flows, "detect_flow_protocol", detect)
        monkeypatch.setattr(flows, "query_preferred_tx_flow_inventory", query)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_TX_FLOW_CHANGE},
        )

        await state._on_transmitter_flow_changed(event)

        detect.assert_awaited_once_with("192.168.1.50", 4440)
        query.assert_awaited_once_with("192.168.1.50", 4440, 0x2729)
        assert device.flow_protocol_id == 0x2729
        assert device.tx_flow_count == 1
        assert device.transmitter_flows == [
            {
                "flow_number": 32,
                "flow_type": "multicast",
                "flow_type_code": None,
                "channel_count": 8,
                "sample_rate": 48000,
                "encoding": 24,
                "destination_internet_protocol_version_four_address": None,
                "destination_user_datagram_port": None,
                "subscriber_device_name": None,
                "subscriber_flow_name": None,
            }
        ]
        assert len(emitted_events(application)) == 1

    @pytest.mark.asyncio
    async def test_receiver_flow_change_refreshes_channels_and_flow_inventory(self, monkeypatch):
        from netaudio.dante import flows

        device = make_device()
        device.get_rx_channels = AsyncMock()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        query = AsyncMock(
            return_value={
                "maximum_flow_slots": 16,
                "flows": [
                    {
                        "flow_number": 1,
                        "flow_type": "unicast",
                        "latency_nanoseconds": 1000000,
                    }
                ],
            }
        )
        monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", query)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_RX_FLOW_CHANGE},
        )

        await state._on_receiver_flow_changed(event)

        device.get_rx_channels.assert_awaited_once()
        query.assert_awaited_once_with(device)
        assert device.rx_flow_count == 1
        assert device.receiver_flow_latency_nanoseconds == 1000000
        assert device.receiver_flows == [
            {
                "flow_number": 1,
                "flow_type": "unicast",
                "latency_nanoseconds": 1000000,
            }
        ]
        assert len(emitted_events(application)) == 1

    @pytest.mark.asyncio
    async def test_receiver_flow_change_emits_channel_refresh_when_flow_inventory_is_unavailable(self, monkeypatch):
        from netaudio.dante import flows

        device = make_device()
        device.get_rx_channels = AsyncMock()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", AsyncMock(return_value=None))
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_RX_FLOW_CHANGE},
        )

        await state._on_receiver_flow_changed(event)

        device.get_rx_channels.assert_awaited_once()
        assert len(emitted_events(application)) == 1


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
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.refresh_device("dev1.local.")
        await state.refresh_device("dev1.local.")

        assert device.fetch_controls_data.await_count == 2

    @pytest.mark.asyncio
    async def test_initial_fetch_probes_unknown_sample_rate_capabilities(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_sample_rate_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_initial_fetch_preserves_known_sample_rate_capabilities_without_reprobing(self):
        device = make_device()
        device.supported_sample_rates = [48_000]
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_sample_rate_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_initial_fetch_probes_unknown_encoding_capabilities(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_encoding_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_initial_fetch_preserves_known_encoding_capabilities_without_reprobing(self):
        device = make_device()
        device.supported_encodings = [24]
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_encoding_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_initial_fetch_probes_unknown_gain_capabilities(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_gain_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_initial_fetch_preserves_known_gain_capabilities_without_reprobing(self):
        device = make_device()
        device.supported_gain_levels = [1, 2, 3, 4, 5]
        application = make_application({"dev1.local.": device})
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        application.probe_gain_status.assert_not_awaited()


class TestControlNotifications:
    @pytest.mark.asyncio
    async def test_settled_routing_capacity_refreshes_device_controls(self):
        device = make_device()
        device.routing_ready = True
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.fetch_device_controls = AsyncMock()
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_ROUTING_READY, "conmon_response": True},
        )

        await state._on_device_state_changed(event)

        state.fetch_device_controls.assert_awaited_once_with("dev1.local.")

    @pytest.mark.asyncio
    async def test_transitional_routing_capacity_does_not_refresh_device_controls(self):
        device = make_device()
        device.routing_ready = False
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.fetch_device_controls = AsyncMock()
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_ROUTING_READY, "conmon_response": True},
        )

        await state._on_device_state_changed(event)

        state.fetch_device_controls.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_typed_state_update_does_not_refetch_controls(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.refetch_device_controls = AsyncMock()
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"state_applied": True},
        )

        await state._on_controls_changed(event)

        state.refetch_device_controls.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unparsed_state_update_still_refetches_controls(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.refetch_device_controls = AsyncMock()
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await state._on_controls_changed(event)

        state.refetch_device_controls.assert_awaited_once_with("dev1.local.")

    @pytest.mark.asyncio
    async def test_unparsed_sample_rate_notification_probes_typed_status(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await state._on_sample_rate_status(event)

        application.probe_sample_rate_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_typed_sample_rate_notification_does_not_probe_again(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        device.operations.get_device_settings = AsyncMock()
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={
                "state_applied": True,
                "conmon_response": True,
                "current_value_changed": False,
                "supported_values_changed": True,
            },
        )

        await state._on_sample_rate_status(event)

        application.probe_sample_rate_status.assert_not_awaited()
        device.operations.get_device_settings.assert_not_awaited()
        application.dispatcher.emit_nowait.assert_not_called()

    @pytest.mark.asyncio
    async def test_typed_sample_rate_value_change_replaces_active_inventory_then_emits(self):
        device = make_device()
        device.tx_count = 64
        device.rx_count = 64
        device.tx_channels = {number: SimpleNamespace(number=number) for number in range(1, 65)}
        device.rx_channels = {number: SimpleNamespace(number=number) for number in range(1, 65)}
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        replacement_tx = {number: SimpleNamespace(number=number) for number in range(1, 17)}
        replacement_rx = {number: SimpleNamespace(number=number) for number in range(1, 17)}
        device.fetch_controls_data = AsyncMock(
            return_value={
                "tx_count": 16,
                "rx_count": 16,
                "tx_channels": replacement_tx,
                "rx_channels": replacement_rx,
                "subscriptions": [],
                "min_latency": 0.5,
                "max_latency": 5.0,
            }
        )

        def assert_state_was_replaced_before_emit(_event):
            assert device.tx_count == device.rx_count == 16
            assert device.tx_channels == replacement_tx
            assert device.rx_channels == replacement_rx

        application.dispatcher.emit_nowait.side_effect = assert_state_was_replaced_before_emit
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={
                "state_applied": True,
                "conmon_response": True,
                "current_value_changed": True,
                "supported_values_changed": False,
            },
        )

        await state._on_sample_rate_status(event)

        device.fetch_controls_data.assert_awaited_once_with(include_channels=True)
        assert device.tx_count == device.rx_count == 16
        assert list(device.tx_channels) == list(range(1, 17))
        assert list(device.rx_channels) == list(range(1, 17))
        assert device.min_latency == 0.5
        assert device.max_latency == 5.0
        events = emitted_events(application)
        assert len(events) == 1
        assert events[0].type == EventType.DEVICE_UPDATED
        assert events[0].server_name == "dev1.local."

    @pytest.mark.asyncio
    @pytest.mark.parametrize("controls_result", [None, TimeoutError("no controls response")])
    async def test_failed_sample_rate_controls_refresh_clears_stale_latency_state(self, controls_result):
        device = make_device()
        device.latency = 1.0
        device.active_latency = 1.0
        device.configured_latency = 0.25
        device.default_latency = 1.0
        device.min_latency = 0.15
        device.max_latency = 21.333334
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        if isinstance(controls_result, BaseException):
            device.fetch_controls_data = AsyncMock(side_effect=controls_result)
        else:
            device.fetch_controls_data = AsyncMock(return_value=controls_result)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"conmon_response": True, "current_value_changed": True},
        )

        await state._on_sample_rate_status(event)

        assert device.latency is None
        assert device.active_latency is None
        assert device.configured_latency is None
        assert device.default_latency is None
        assert device.min_latency is None
        assert device.max_latency is None
        events = emitted_events(application)
        assert len(events) == 1
        assert events[0].type == EventType.DEVICE_UPDATED

    @pytest.mark.asyncio
    async def test_sample_rate_controls_refresh_uses_device_lock(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        controls_entered = asyncio.Event()
        release_controls = asyncio.Event()
        routing_started = asyncio.Event()
        operation_order = []

        async def fetch_controls_data(*, include_channels):
            assert include_channels is True
            operation_order.append("controls enter")
            controls_entered.set()
            await release_controls.wait()
            operation_order.append("controls exit")
            return {}

        async def get_rx_channels():
            operation_order.append("routing")

        async def refresh_routing():
            routing_started.set()
            await state._on_routing_changed(DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local."))

        device.fetch_controls_data = AsyncMock(side_effect=fetch_controls_data)
        device.get_rx_channels = AsyncMock(side_effect=get_rx_channels)
        sample_rate_event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"conmon_response": True, "current_value_changed": True},
        )

        sample_rate_task = asyncio.create_task(state._on_sample_rate_status(sample_rate_event))
        await controls_entered.wait()
        routing_task = asyncio.create_task(refresh_routing())
        await routing_started.wait()
        release_controls.set()
        await asyncio.gather(sample_rate_task, routing_task)

        assert operation_order == ["controls enter", "controls exit", "routing"]

    @pytest.mark.asyncio
    async def test_unparsed_conmon_response_does_not_create_probe_loop(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"state_applied": False, "conmon_response": True},
        )

        await state._on_sample_rate_status(event)

        application.probe_sample_rate_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unparsed_encoding_notification_probes_typed_status(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await state._on_encoding_status(event)

        application.probe_encoding_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_typed_encoding_notification_does_not_probe_again(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"state_applied": True},
        )

        await state._on_encoding_status(event)

        application.probe_encoding_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unparsed_encoding_conmon_response_does_not_create_probe_loop(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"state_applied": False, "conmon_response": True},
        )

        await state._on_encoding_status(event)

        application.probe_encoding_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aes67_notification_refreshes_audio_capabilities(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        application.probe_aes67_state.return_value = (True, True)
        state = DanteStateService(application)
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await state._on_aes67_status(event)

        application.probe_sample_rate_status.assert_awaited_once_with("192.168.1.50")
        application.probe_encoding_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_clear_configuration_notification_refreshes_audio_capabilities(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.fetch_device_controls = AsyncMock()
        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="dev1.local.",
            data={"notification_id": NOTIFICATION_CLEAR_CONFIG_STATUS},
        )

        await state._on_device_state_changed(event)

        state.fetch_device_controls.assert_awaited_once_with("dev1.local.")
        application.probe_sample_rate_status.assert_awaited_once_with("192.168.1.50")
        application.probe_encoding_status.assert_awaited_once_with("192.168.1.50")


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

        application = make_application(
            {
                "tx.local.": offline_device,
                "rx.local.": subscriber,
                "other.local.": bystander,
            }
        )
        subscriber.get_rx_channels = AsyncMock()
        bystander.get_rx_channels = AsyncMock()
        state = DanteStateService(application)

        await state.refresh_affected_subscriptions(offline_device)

        subscriber.get_rx_channels.assert_awaited_once()
        bystander.get_rx_channels.assert_not_awaited()
