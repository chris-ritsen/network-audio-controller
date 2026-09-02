import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.const import (
    NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
)
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.state import DanteStateService, apply_device_status


def make_device(server_name="dev1.local.", name="Device1"):
    device = DanteDevice(server_name=server_name)
    device.name = name
    device.ipv4 = "192.168.1.50"
    return device


def make_application(devices):
    def device_by_ip(device_ip_address):
        for device in devices.values():
            if device.ipv4 and str(device.ipv4) == device_ip_address:
                return device
        return None

    return SimpleNamespace(
        _device_by_ip=device_by_ip,
        _send_conmon_query_for_device=AsyncMock(),
        cmc=SimpleNamespace(register_device=AsyncMock()),
        devices=devices,
        dispatcher=MagicMock(),
        get_arc_port=lambda device: 4440,
        notifications=MagicMock(),
        probe_aes67_state=AsyncMock(return_value=None),
        probe_clocking_status=AsyncMock(return_value=None),
        probe_encoding_status=AsyncMock(return_value=None),
        probe_gain_status=AsyncMock(return_value=None),
        probe_interface_status=AsyncMock(return_value=None),
        probe_lock_status=AsyncMock(return_value=None),
        probe_preferred_leader_state=AsyncMock(return_value=None),
        probe_sample_rate_status=AsyncMock(return_value=None),
        settings=SimpleNamespace(
            request_bluetooth_status=AsyncMock(),
            request_dante_model=AsyncMock(),
            request_make_model=AsyncMock(),
        ),
    )


def emitted_events(application):
    return [call.args[0] for call in application.dispatcher.emit_nowait.call_args_list]


def status_event(kind, status, server_name="dev1.local.", source_ip="192.168.1.50", notification_id=None):
    return DanteEvent(
        type=EventType.DEVICE_STATUS_RECEIVED,
        server_name=server_name,
        data={
            "kind": kind,
            "notification_id": notification_id,
            "raw": b"",
            "source_ip": source_ip,
            "status": status,
        },
    )


def test_registration_is_idempotent():
    application = make_application({})
    state = DanteStateService(application)

    state.register()
    registered_count = application.dispatcher.on.call_count
    state.register()

    assert registered_count == 2
    assert application.dispatcher.on.call_count == registered_count
    assert state.refetching is True


def test_attach_only_subscribes_status_application():
    application = make_application({})
    state = DanteStateService(application)

    state.attach()

    assert application.dispatcher.on.call_count == 1
    assert application.dispatcher.on.call_args.args == (EventType.DEVICE_STATUS_RECEIVED, state.on_device_status)
    assert state.refetching is False


class TestApplyDeviceStatus:
    def test_sample_rate_status_reports_change_once(self):
        device = make_device()
        status = {"sample_rate": 48_000, "supported_sample_rates": [44_100, 48_000]}

        assert apply_device_status(device, "sample_rate", status) is True
        assert apply_device_status(device, "sample_rate", status) is False
        assert device.sample_rate == 48_000
        assert device.supported_sample_rates == [44_100, 48_000]

    def test_routing_capacity_fills_missing_active_counts_only(self):
        device = make_device()
        device.tx_count = 2
        status = {
            "routing_capacity_receive_channel_count": 16,
            "routing_capacity_transmit_channel_count": 16,
            "routing_ready": True,
            "routing_ready_state_code": 0x0101,
        }

        assert apply_device_status(device, "routing_capacity", status) is True
        assert device.tx_count == 2
        assert device.rx_count == device.rx_count_raw == 16

    def test_model_fields_never_overwrite_known_values_except_manufacturer(self):
        device = make_device()
        device.dante_model = "Known"
        device.manufacturer = "Old"

        assert apply_device_status(device, "make_model", {"dante_model": "Other", "manufacturer": "New"}) is True
        assert device.dante_model == "Known"
        assert device.manufacturer == "New"

    def test_unknown_kind_changes_nothing(self):
        device = make_device()

        assert apply_device_status(device, "link_status", object()) is False


class TestDeviceStatusEvents:
    @pytest.mark.asyncio
    async def test_status_for_unknown_address_is_kept_until_the_device_appears(self):
        application = make_application({})
        state = DanteStateService(application)
        await state.on_device_status(
            status_event("encoding", {"encoding": 24, "supported_encodings": [24]}, source_ip="192.168.1.99")
        )
        device = make_device()
        device.ipv4 = "192.168.1.99"

        state.apply_pending_for_device(device)

        assert device.encoding == 24
        assert device.supported_encodings == [24]
        assert emitted_events(application) == []

    @pytest.mark.asyncio
    async def test_offline_device_ignores_capability_status_but_accepts_model_identity(self):
        device = make_device()
        device.online = False
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.on_device_status(status_event("encoding", {"encoding": 24, "supported_encodings": [24]}))
        await state.on_device_status(status_event("make_model", {"dante_model": "AVIO"}))

        assert device.encoding is None
        assert device.dante_model == "AVIO"

    @pytest.mark.asyncio
    async def test_unchanged_status_emits_nothing(self):
        device = make_device()
        device.encoding = 24
        device.supported_encodings = [24]
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.on_device_status(status_event("encoding", {"encoding": 24, "supported_encodings": [24]}))

        assert emitted_events(application) == []
        application.probe_encoding_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_changed_status_emits_device_updated_once(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.on_device_status(status_event("encoding", {"encoding": 24, "supported_encodings": [24]}))

        events = emitted_events(application)
        assert [event.type for event in events] == [EventType.DEVICE_UPDATED]
        assert events[0].server_name == "dev1.local."


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

    @pytest.mark.asyncio
    async def test_initial_fetch_tolerates_probe_timeouts(self):
        from netaudio.dante.application import CapabilityProbeTimeout

        device = make_device()
        application = make_application({"dev1.local.": device})
        application.probe_aes67_state.side_effect = CapabilityProbeTimeout("AES67 status readback timed out")
        application.probe_preferred_leader_state.side_effect = CapabilityProbeTimeout("timed out")
        application.probe_interface_status.side_effect = CapabilityProbeTimeout("timed out")
        device.fetch_controls_data = AsyncMock(return_value={"name": "Device1", "tx_count": 2, "rx_count": 2})
        state = DanteStateService(application)

        await state.fetch_device_controls("dev1.local.")

        assert application.probe_aes67_state.await_count == 3
        assert application.probe_preferred_leader_state.await_count == 3
        events = emitted_events(application)
        assert [event.type for event in events] == [EventType.DEVICE_UPDATED]


ROUTING_READY_STATUS = {
    "routing_capacity_receive_channel_count": 128,
    "routing_capacity_transmit_channel_count": 128,
    "routing_ready": True,
    "routing_ready_state_code": 0x0101,
}
ROUTING_TRANSITION_STATUS = {
    "routing_capacity_receive_channel_count": 0,
    "routing_capacity_transmit_channel_count": 0,
    "routing_ready": False,
    "routing_ready_state_code": 0x0001,
}


class TestControlNotifications:
    @pytest.mark.asyncio
    async def test_settled_routing_capacity_refreshes_device_controls(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
        state.fetch_device_controls = AsyncMock()

        await state.on_device_status(status_event("routing_capacity", ROUTING_READY_STATUS))

        state.fetch_device_controls.assert_awaited_once_with("dev1.local.")

    @pytest.mark.asyncio
    async def test_transitional_routing_capacity_does_not_refresh_device_controls(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
        state.fetch_device_controls = AsyncMock()

        await state.on_device_status(status_event("routing_capacity", ROUTING_TRANSITION_STATUS))

        state.fetch_device_controls.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_routing_capacity_without_refetching_only_applies_state(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.fetch_device_controls = AsyncMock()

        await state.on_device_status(status_event("routing_capacity", ROUTING_READY_STATUS))

        state.fetch_device_controls.assert_not_awaited()
        assert device.routing_ready is True
        assert len(emitted_events(application)) == 1

    @pytest.mark.asyncio
    async def test_property_change_notification_refetches_controls(self):
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
    async def test_typed_sample_rate_status_does_not_probe_again(self):
        device = make_device()
        device.sample_rate = 48_000
        device.supported_sample_rates = [48_000]
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
        device.operations.get_device_settings = AsyncMock()

        await state.on_device_status(
            status_event("sample_rate", {"sample_rate": 48_000, "supported_sample_rates": [44_100, 48_000]})
        )

        application.probe_sample_rate_status.assert_not_awaited()
        device.operations.get_device_settings.assert_not_awaited()
        assert device.supported_sample_rates == [44_100, 48_000]
        assert [event.type for event in emitted_events(application)] == [EventType.DEVICE_UPDATED]

    @pytest.mark.asyncio
    async def test_typed_sample_rate_value_change_replaces_active_inventory_then_emits(self):
        device = make_device()
        device.sample_rate = 48_000
        device.supported_sample_rates = [48_000, 96_000]
        device.tx_count = 64
        device.rx_count = 64
        device.tx_channels = {number: SimpleNamespace(number=number) for number in range(1, 65)}
        device.rx_channels = {number: SimpleNamespace(number=number) for number in range(1, 65)}
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
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

        await state.on_device_status(
            status_event("sample_rate", {"sample_rate": 96_000, "supported_sample_rates": [48_000, 96_000]})
        )

        device.fetch_controls_data.assert_awaited_once_with(include_channels=True)
        assert device.sample_rate == 96_000
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
        device.sample_rate = 48_000
        device.latency = 1.0
        device.active_latency = 1.0
        device.configured_latency = 0.25
        device.default_latency = 1.0
        device.min_latency = 0.15
        device.max_latency = 21.333334
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
        if isinstance(controls_result, BaseException):
            device.fetch_controls_data = AsyncMock(side_effect=controls_result)
        else:
            device.fetch_controls_data = AsyncMock(return_value=controls_result)

        await state.on_device_status(
            status_event("sample_rate", {"sample_rate": 96_000, "supported_sample_rates": [48_000, 96_000]})
        )

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
        device.sample_rate = 48_000
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
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

        sample_rate_task = asyncio.create_task(
            state.on_device_status(
                status_event("sample_rate", {"sample_rate": 96_000, "supported_sample_rates": [48_000, 96_000]})
            )
        )
        await controls_entered.wait()
        routing_task = asyncio.create_task(refresh_routing())
        await routing_started.wait()
        release_controls.set()
        await asyncio.gather(sample_rate_task, routing_task)

        assert operation_order == ["controls enter", "controls exit", "routing"]

    @pytest.mark.asyncio
    async def test_sample_rate_value_change_without_refetching_emits_without_controls_refresh(self):
        device = make_device()
        device.sample_rate = 48_000
        device.fetch_controls_data = AsyncMock()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.on_device_status(
            status_event("sample_rate", {"sample_rate": 96_000, "supported_sample_rates": [48_000, 96_000]})
        )

        device.fetch_controls_data.assert_not_awaited()
        assert device.sample_rate == 96_000
        assert [event.type for event in emitted_events(application)] == [EventType.DEVICE_UPDATED]

    @pytest.mark.asyncio
    async def test_unparsed_encoding_notification_probes_typed_status(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        event = DanteEvent(type=EventType.NOTIFICATION_RECEIVED, server_name="dev1.local.")

        await state._on_encoding_status(event)

        application.probe_encoding_status.assert_awaited_once_with("192.168.1.50")

    @pytest.mark.asyncio
    async def test_typed_encoding_status_does_not_probe_again(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()

        await state.on_device_status(status_event("encoding", {"encoding": 24, "supported_encodings": [24]}))

        application.probe_encoding_status.assert_not_awaited()
        assert device.encoding == 24

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

    @pytest.mark.asyncio
    async def test_clear_configuration_status_refreshes_controls_and_capabilities_when_refetching(self):
        device = make_device()
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)
        state.register()
        state.fetch_device_controls = AsyncMock()

        await state.on_device_status(
            status_event("clear_configuration_status", {"action_result_code": 1, "available_actions_mask": 3})
        )

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

        async def send_query(target_device, request):
            target_device.dante_model_id = "DAI2"
            waiter.set()

        application._send_conmon_query_for_device = AsyncMock(side_effect=send_query)
        application.notifications.register_conmon_waiter = MagicMock(return_value=waiter)
        application.notifications.unregister_waiter = MagicMock()
        state = DanteStateService(application)

        await asyncio.wait_for(state.retry_conmon_query("dev1.local."), timeout=1.0)

        application._send_conmon_query_for_device.assert_awaited_once_with(
            device, application.settings.request_dante_model
        )
        application.notifications.unregister_waiter.assert_called_once_with(waiter)

    @pytest.mark.asyncio
    async def test_skips_when_model_already_known(self):
        device = make_device()
        device.mac_address = "001dc1aabbcc"
        device.dante_model_id = "DAI2"
        application = make_application({"dev1.local.": device})
        state = DanteStateService(application)

        await state.retry_conmon_query("dev1.local.")

        application._send_conmon_query_for_device.assert_not_awaited()


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
