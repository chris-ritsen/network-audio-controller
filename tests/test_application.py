import asyncio
import warnings
from unittest.mock import MagicMock

import pytest

from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import EventType


class TestDanteApplication:
    def test_instantiation(self):
        application = DanteApplication()
        assert application.devices == {}
        assert application.dispatcher is not None
        assert application.settings is not None
        assert application.cmc is not None
        assert application.notifications is not None

    @pytest.mark.asyncio
    async def test_startup_shutdown(self):
        application = DanteApplication()
        await application.startup()
        assert application._started is True

        await application.shutdown()
        assert application._started is False

    @pytest.mark.asyncio
    async def test_startup_idempotent(self):
        application = DanteApplication()
        await application.startup()
        await application.startup()  # Should not raise
        assert application._started is True
        await application.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self):
        application = DanteApplication()
        await application.shutdown()  # Not started, should not raise
        await application.shutdown()

    def test_register_device(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")
        device.ipv4 = "192.168.1.100"
        device.name = "Test Device"

        application.register_device("test.local.", device)

        assert "test.local." in application.devices
        assert application.devices["test.local."] is device
        assert device._app is application

    @pytest.mark.asyncio
    async def test_register_device_emits_discovered(self):
        application = DanteApplication()
        received_event = asyncio.Event()
        received_dante_event = None

        async def receive_dante_event(dante_event):
            nonlocal received_dante_event
            received_dante_event = dante_event
            received_event.set()

        application.dispatcher.on(EventType.DEVICE_DISCOVERED, receive_dante_event)
        await application.dispatcher.start()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        try:
            application.register_device("test.local.", device)
            await asyncio.wait_for(received_event.wait(), timeout=1)
        finally:
            await application.dispatcher.stop()

        assert received_dante_event is not None
        assert received_dante_event.type == EventType.DEVICE_DISCOVERED
        assert received_dante_event.server_name == "test.local."

    @pytest.mark.asyncio
    async def test_register_existing_device_emits_updated(self):
        application = DanteApplication()
        received_event = asyncio.Event()
        received_dante_event = None

        async def receive_dante_event(dante_event):
            nonlocal received_dante_event
            received_dante_event = dante_event
            received_event.set()

        application.dispatcher.on(EventType.DEVICE_UPDATED, receive_dante_event)
        await application.dispatcher.start()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        try:
            application.register_device("test.local.", device)
            application.register_device("test.local.", device)
            await asyncio.wait_for(received_event.wait(), timeout=1)
        finally:
            await application.dispatcher.stop()

        assert received_dante_event is not None
        assert received_dante_event.type == EventType.DEVICE_UPDATED
        assert received_dante_event.server_name == "test.local."

    def test_unregister_device(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")
        device.name = "Test"

        application.register_device("test.local.", device)
        application.unregister_device("test.local.")

        assert "test.local." not in application.devices

    def test_unregister_nonexistent_device(self):
        application = DanteApplication()
        application.unregister_device("nonexistent.local.")  # Should not raise

    def test_mark_device_offline_clears_device_capabilities(self):
        application = DanteApplication()
        device = DanteDevice(server_name="test.local.")
        device.supported_sample_rates = [44_100, 48_000]
        device.supported_encodings = [16, 24, 32]
        application.register_device("test.local.", device)

        application.mark_device_offline("test.local.")

        assert device.online is False
        assert device.supported_sample_rates is None
        assert device.supported_encodings is None

    @pytest.mark.asyncio
    async def test_probe_sample_rates_all_probes_online_devices_and_applies_results(self):
        application = DanteApplication()
        online_device = DanteDevice(server_name="online.local.")
        online_device.ipv4 = "192.168.1.108"
        duplicate_address_device = DanteDevice(server_name="duplicate.local.")
        duplicate_address_device.ipv4 = "192.168.1.108"
        known_device = DanteDevice(server_name="known.local.")
        known_device.ipv4 = "192.168.1.110"
        known_device.supported_sample_rates = [48_000]
        offline_device = DanteDevice(server_name="offline.local.")
        offline_device.ipv4 = "192.168.1.109"
        offline_device.online = False
        application.devices = {
            online_device.server_name: online_device,
            duplicate_address_device.server_name: duplicate_address_device,
            known_device.server_name: known_device,
            offline_device.server_name: offline_device,
        }

        def respond_to_probe(device_ip_address):
            application.notifications._notify_sample_rate_waiter(device_ip_address, 48_000, [44_100, 48_000])

        application.settings.probe_sample_rate = MagicMock(side_effect=respond_to_probe)

        await application._probe_sample_rates_all(timeout=0.1)

        application.settings.probe_sample_rate.assert_called_once_with("192.168.1.108")
        assert online_device.sample_rate == 48_000
        assert online_device.supported_sample_rates == [44_100, 48_000]
        assert duplicate_address_device.sample_rate == 48_000
        assert duplicate_address_device.supported_sample_rates == [44_100, 48_000]
        assert known_device.supported_sample_rates == [48_000]
        assert offline_device.supported_sample_rates is None

    @pytest.mark.asyncio
    async def test_probe_sample_rate_status_returns_typed_result_and_unregisters_waiter(self):
        application = DanteApplication()

        def respond_to_probe(device_ip_address):
            application.notifications._notify_sample_rate_waiter(device_ip_address, 48_000, [48_000])

        application.settings.probe_sample_rate = MagicMock(side_effect=respond_to_probe)

        result = await application.probe_sample_rate_status("192.168.1.108", timeout=0.1)

        assert result == (48_000, [48_000])
        assert not application.notifications._waiters.is_registered("sample_rate", "192.168.1.108")

    @pytest.mark.asyncio
    async def test_concurrent_sample_rate_probes_for_one_device_are_serialized(self):
        application = DanteApplication()
        probe_results = [(48_000, [48_000]), (96_000, [48_000, 96_000])]

        def respond_to_probe(device_ip_address):
            current_sample_rate, supported_sample_rates = probe_results.pop(0)
            application.notifications._notify_sample_rate_waiter(
                device_ip_address,
                current_sample_rate,
                supported_sample_rates,
            )

        application.settings.probe_sample_rate = MagicMock(side_effect=respond_to_probe)

        results = await asyncio.gather(
            application.probe_sample_rate_status("192.168.1.108", timeout=0.1),
            application.probe_sample_rate_status("192.168.1.108", timeout=0.1),
        )

        assert results == [(48_000, [48_000]), (96_000, [48_000, 96_000])]
        assert application.settings.probe_sample_rate.call_count == 2

    @pytest.mark.asyncio
    async def test_probe_encodings_all_probes_online_devices_and_applies_advertised_results(self):
        application = DanteApplication()
        online_device = DanteDevice(server_name="online.local.")
        online_device.ipv4 = "192.168.1.108"
        duplicate_address_device = DanteDevice(server_name="duplicate.local.")
        duplicate_address_device.ipv4 = "192.168.1.108"
        known_device = DanteDevice(server_name="known.local.")
        known_device.ipv4 = "192.168.1.110"
        known_device.supported_encodings = [24]
        offline_device = DanteDevice(server_name="offline.local.")
        offline_device.ipv4 = "192.168.1.109"
        offline_device.online = False
        application.devices = {
            online_device.server_name: online_device,
            duplicate_address_device.server_name: duplicate_address_device,
            known_device.server_name: known_device,
            offline_device.server_name: offline_device,
        }

        def respond_to_probe(device_ip_address):
            application.notifications._notify_encoding_waiter(device_ip_address, 24, [24, 16, 32])

        application.settings.probe_encoding = MagicMock(side_effect=respond_to_probe)

        await application._probe_encodings_all(timeout=0.1)

        application.settings.probe_encoding.assert_called_once_with("192.168.1.108")
        assert online_device.encoding == 24
        assert online_device.supported_encodings == [24, 16, 32]
        assert duplicate_address_device.encoding == 24
        assert duplicate_address_device.supported_encodings == [24, 16, 32]
        assert known_device.supported_encodings == [24]
        assert offline_device.supported_encodings is None

    @pytest.mark.asyncio
    async def test_probe_encoding_status_returns_typed_result_and_unregisters_waiter(self):
        application = DanteApplication()

        def respond_to_probe(device_ip_address):
            application.notifications._notify_encoding_waiter(device_ip_address, 24, [24])

        application.settings.probe_encoding = MagicMock(side_effect=respond_to_probe)

        result = await application.probe_encoding_status("192.168.1.108", timeout=0.1)

        assert result == (24, [24])
        assert not application.notifications._waiters.is_registered("encoding", "192.168.1.108")

    @pytest.mark.asyncio
    async def test_concurrent_encoding_probes_for_one_device_are_serialized(self):
        application = DanteApplication()
        probe_results = [(24, [24]), (16, [24, 16, 32])]

        def respond_to_probe(device_ip_address):
            current_encoding, supported_encodings = probe_results.pop(0)
            application.notifications._notify_encoding_waiter(
                device_ip_address,
                current_encoding,
                supported_encodings,
            )

        application.settings.probe_encoding = MagicMock(side_effect=respond_to_probe)

        results = await asyncio.gather(
            application.probe_encoding_status("192.168.1.108", timeout=0.1),
            application.probe_encoding_status("192.168.1.108", timeout=0.1),
        )

        assert results == [(24, [24]), (16, [24, 16, 32])]
        assert application.settings.probe_encoding.call_count == 2

    def test_get_arc_port(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        device.services = {
            "test._netaudio-arc._udp.local.": {
                "type": "_netaudio-arc._udp.local.",
                "port": 4440,
                "ipv4": "192.168.1.100",
            }
        }

        assert application.get_arc_port(device) == 4440

    def test_get_arc_port_no_services(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")
        assert application.get_arc_port(device) is None

    def test_get_arc_port_no_arc_service(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        device.services = {
            "test._netaudio-cmc._udp.local.": {
                "type": "_netaudio-cmc._udp.local.",
                "port": 8800,
            }
        }

        assert application.get_arc_port(device) is None

    def test_device_by_ip(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")
        device.ipv4 = "192.168.1.100"

        application.register_device("test.local.", device)

        found = application._device_by_ip("192.168.1.100")
        assert found is device

        not_found = application._device_by_ip("10.0.0.1")
        assert not_found is None

    def test_on_notification_registers_handler(self):
        application = DanteApplication()
        received = []

        async def handler(event):
            received.append(event)

        application.on_notification(262, handler)
        assert 262 in application._notification_handlers
        assert handler in application._notification_handlers[262]

    def test_on_notification_multiple_handlers(self):
        application = DanteApplication()

        async def handler_a(event):
            pass

        async def handler_b(event):
            pass

        application.on_notification(262, handler_a)
        application.on_notification(262, handler_b)
        assert len(application._notification_handlers[262]) == 2

    @pytest.mark.asyncio
    async def test_dispatch_notification(self):
        from netaudio.dante.events import DanteEvent

        application = DanteApplication()
        received = []

        async def handler(event):
            received.append(event)

        application.on_notification(262, handler)

        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="test.local.",
            data={"notification_id": 262, "notification_name": "Property Change"},
        )
        await application._dispatch_notification(event)

        assert len(received) == 1
        assert received[0].data["notification_id"] == 262

    @pytest.mark.asyncio
    async def test_dispatch_notification_unhandled(self):
        from netaudio.dante.events import DanteEvent

        application = DanteApplication()

        event = DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name="test.local.",
            data={"notification_id": 9999, "notification_name": "Unknown"},
        )
        await application._dispatch_notification(event)
