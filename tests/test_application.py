import asyncio
import warnings

import pytest

from netaudio_lib.dante.application import DanteApplication
from netaudio_lib.dante.device import DanteDevice
from netaudio_lib.dante.events import EventType


class TestDanteApplication:
    def test_instantiation(self):
        application = DanteApplication()
        assert application.devices == {}
        assert application.dispatcher is not None
        assert application.arc is not None
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

    def test_register_device_emits_discovered(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        application.register_device("test.local.", device)

        # Check the event was queued
        assert application.dispatcher._queue.qsize() == 1

    def test_register_existing_device_emits_updated(self):
        application = DanteApplication()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            device = DanteDevice(server_name="test.local.")

        application.register_device("test.local.", device)
        # Drain first event
        application.dispatcher._queue.get_nowait()

        application.register_device("test.local.", device)
        event = application.dispatcher._queue.get_nowait()
        assert event.type == EventType.DEVICE_UPDATED

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
