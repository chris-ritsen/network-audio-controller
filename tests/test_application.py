import asyncio
import warnings
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import EventType


def make_arc_device(server_name: str, ip_address: str) -> DanteDevice:
    device = DanteDevice(server_name=server_name)
    device.ipv4 = ip_address
    device.services = {
        f"{server_name}_netaudio-arc._udp.local.": {
            "type": "_netaudio-arc._udp.local.",
            "port": 4440,
            "ipv4": ip_address,
        }
    }
    return device


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
    async def test_startup_failure_stops_partially_started_services(self):
        application = DanteApplication()
        application.dispatcher.start = AsyncMock()
        application.dispatcher.stop = AsyncMock()
        application.notifications.start = AsyncMock(side_effect=RuntimeError("listener failed"))
        application.notifications.stop = AsyncMock()
        application.settings.stop = AsyncMock()
        application.cmc.stop = AsyncMock()

        with pytest.raises(RuntimeError, match="listener failed"):
            await application.startup()

        assert application._started is False
        application.notifications.stop.assert_awaited_once()
        application.settings.stop.assert_awaited_once()
        application.cmc.stop.assert_awaited_once()
        application.dispatcher.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wait_for_discovery_returns_resolved_devices_at_deadline(self, monkeypatch):
        import netaudio.dante.browser as browser_module

        device = make_arc_device("resolved.local.", "192.168.1.10")

        class FakeBrowser:
            instances = []

            def __init__(self, mdns_timeout, app):
                self.devices = {device.server_name: device}
                self.closed = False
                self.instances.append(self)

            async def get_devices(self):
                await asyncio.Event().wait()

            async def async_close(self):
                self.closed = True

        monkeypatch.setattr(browser_module, "DanteBrowser", FakeBrowser)
        application = DanteApplication()

        devices = await application.wait_for_discovery(timeout=0.01)

        assert devices == {device.server_name: device}
        assert application.devices == devices
        assert FakeBrowser.instances[0].closed is True
        assert application._browser is None

    def test_discovered_service_mapping_preserves_dante_via_identity(self):
        application = DanteApplication()
        service_name = "via._netaudio-arc._udp.local."

        device = application._apply_discovered_services(
            "via.local.",
            {
                service_name: {
                    "ipv4": "192.168.1.20",
                    "name": service_name,
                    "port": 4440,
                    "properties": {"router_info": '"Dante Via"'},
                    "server_name": "via.local.",
                    "type": "_netaudio-arc._udp.local.",
                }
            },
        )

        assert device.software == "Dante Via"

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
    async def test_probe_sample_rate_status_retries_once_within_deadline(self):
        application = DanteApplication()
        probe_count = 0

        def respond_to_second_probe(device_ip_address):
            nonlocal probe_count
            probe_count += 1
            if probe_count == 2:
                application.notifications._notify_sample_rate_waiter(device_ip_address, 48_000, [48_000])

        application.settings.probe_sample_rate = MagicMock(side_effect=respond_to_second_probe)

        result = await application.probe_sample_rate_status("192.168.1.108", timeout=0.1)

        assert result == (48_000, [48_000])
        assert application.settings.probe_sample_rate.call_count == 2
        assert not application.notifications._waiters.is_registered("sample_rate", "192.168.1.108")

    @pytest.mark.asyncio
    async def test_probe_sample_rate_status_uses_three_attempts_within_deadline(self):
        application = DanteApplication()
        probe_count = 0

        def respond_to_third_probe(device_ip_address):
            nonlocal probe_count
            probe_count += 1
            if probe_count == 3:
                application.notifications._notify_sample_rate_waiter(device_ip_address, 48_000, [48_000])

        application.settings.probe_sample_rate = MagicMock(side_effect=respond_to_third_probe)

        result = await application.probe_sample_rate_status("192.168.1.108", timeout=0.1)

        assert result == (48_000, [48_000])
        assert application.settings.probe_sample_rate.call_count == 3
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

    @pytest.mark.asyncio
    async def test_probe_gain_levels_all_probes_online_devices_and_applies_results(self):
        application = DanteApplication()
        online_device = DanteDevice(server_name="online.local.")
        online_device.ipv4 = "192.168.1.108"
        duplicate_address_device = DanteDevice(server_name="duplicate.local.")
        duplicate_address_device.ipv4 = "192.168.1.108"
        known_device = DanteDevice(server_name="known.local.")
        known_device.ipv4 = "192.168.1.110"
        known_device.supported_gain_levels = [1, 2, 3, 4, 5]
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
            application.notifications._notify_gain_status_waiters(device_ip_address, "input", [5, 1])

        application.settings.probe_gain_level = MagicMock(side_effect=respond_to_probe)

        await application._probe_gain_levels_all(timeout=0.1)

        application.settings.probe_gain_level.assert_called_once_with("192.168.1.108")
        assert online_device.gain_device_type == "input"
        assert online_device.gain_levels == [5, 1]
        assert online_device.supported_gain_levels == [1, 2, 3, 4, 5]
        assert duplicate_address_device.gain_levels == [5, 1]
        assert known_device.supported_gain_levels == [1, 2, 3, 4, 5]
        assert offline_device.supported_gain_levels is None

    @pytest.mark.asyncio
    async def test_probe_gain_status_returns_typed_result_and_unregisters_waiter(self):
        application = DanteApplication()

        def respond_to_probe(device_ip_address):
            application.notifications._notify_gain_status_waiters(device_ip_address, "output", [4, 4])

        application.settings.probe_gain_level = MagicMock(side_effect=respond_to_probe)

        result = await application.probe_gain_status("192.168.1.108", timeout=0.1)

        assert result == ("output", [4, 4])
        assert "192.168.1.108" not in application.notifications._gain_status_waiters

    @pytest.mark.asyncio
    async def test_set_gain_level_state_requires_matching_multicast_readback(self):
        application = DanteApplication()
        device = DanteDevice(server_name="avio-input.local.")
        device.ipv4 = "192.168.1.108"
        device.gain_device_type = "input"
        device.gain_levels = [5, 1]
        device.supported_gain_levels = [1, 2, 3, 4, 5]

        def respond_to_write(device_ip_address, channel_number, gain_level, device_type):
            assert channel_number == 1
            assert gain_level == 3
            assert device_type == "input"
            application.notifications._notify_gain_status_waiters(device_ip_address, "input", [3, 1])

        application.settings.set_gain_level = MagicMock(side_effect=respond_to_write)

        result = await application.set_gain_level_state(device, 1, 3, "input", timeout=0.1)

        assert result == ("input", [3, 1])
        assert device.gain_levels == [3, 1]
        assert "192.168.1.108" not in application.notifications._gain_status_waiters

    @pytest.mark.asyncio
    async def test_set_gain_level_state_retries_and_returns_nonmatching_readback_without_success(self):
        application = DanteApplication()
        device = DanteDevice(server_name="avio-input.local.")
        device.ipv4 = "192.168.1.108"
        device.gain_device_type = "input"
        device.gain_levels = [5, 1]
        device.supported_gain_levels = [1, 2, 3, 4, 5]

        def respond_with_unchanged_status(device_ip_address, channel_number, gain_level, device_type):
            application.notifications._notify_gain_status_waiters(device_ip_address, "input", [5, 1])

        application.settings.set_gain_level = MagicMock(side_effect=respond_with_unchanged_status)

        result = await application.set_gain_level_state(device, 1, 3, "input", timeout=0.04)

        assert result == ("input", [5, 1])
        assert device.gain_levels == [5, 1]
        assert application.settings.set_gain_level.call_count == 3
        assert "192.168.1.108" not in application.notifications._gain_status_waiters

    @pytest.mark.asyncio
    async def test_set_gain_level_state_rejects_direction_before_sending(self):
        application = DanteApplication()
        application.settings.set_gain_level = MagicMock()
        device = DanteDevice(server_name="avio-input.local.")
        device.ipv4 = "192.168.1.108"
        device.gain_device_type = "input"

        with pytest.raises(ValueError, match="input gain controls"):
            await application.set_gain_level_state(device, 1, 3, "output", timeout=0.1)

        application.settings.set_gain_level.assert_not_called()

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

    @pytest.mark.asyncio
    async def test_populate_devices_scopes_all_work_to_selected_devices(self):
        application = DanteApplication()
        selected_device = make_arc_device("selected.local.", "192.168.1.10")
        other_device = make_arc_device("other.local.", "192.168.1.11")
        application.devices = {
            selected_device.server_name: selected_device,
            other_device.server_name: other_device,
        }
        selected_devices = {selected_device.server_name: selected_device}
        application.cmc.register_all = AsyncMock()
        application._query_settings_fields = AsyncMock()
        application._query_conmon_all = AsyncMock()
        application._probe_interface_status = AsyncMock()
        application._probe_preferred_leader_all = AsyncMock()
        application._probe_aes67_all = AsyncMock()
        application._probe_sample_rates_all = AsyncMock()
        application._probe_encodings_all = AsyncMock()
        application._populate_device_controls = AsyncMock()

        await application.populate_devices(selected_devices, timeout=0.1, include_channels=False)

        application.cmc.register_all.assert_awaited_once_with(["192.168.1.10"])
        application._query_settings_fields.assert_awaited_once_with(selected_devices)
        application._query_conmon_all.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._probe_interface_status.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._probe_preferred_leader_all.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._probe_aes67_all.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._probe_sample_rates_all.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._probe_encodings_all.assert_awaited_once_with(timeout=0.1, devices=selected_devices)
        application._populate_device_controls.assert_awaited_once_with(
            selected_device,
            include_channels=False,
            request_timeout_milliseconds=500,
            request_attempts=1,
        )
        assert application.devices == {
            selected_device.server_name: selected_device,
            other_device.server_name: other_device,
        }

    @pytest.mark.asyncio
    async def test_populate_devices_starts_independent_phases_concurrently(self):
        application = DanteApplication()
        selected_device = make_arc_device("selected.local.", "192.168.1.10")
        selected_devices = {selected_device.server_name: selected_device}
        application.devices = selected_devices
        application.cmc.register_all = AsyncMock()
        expected_phases = {
            "settings",
            "ConMon",
            "interfaces",
            "preferred leader",
            "AES67",
            "sample rates",
            "encodings",
            "controls",
            "CMC registration",
        }
        started_phases = set()
        all_phases_started = asyncio.Event()
        release_phases = asyncio.Event()

        def make_phase(phase_name):
            async def run_phase(*arguments, **keyword_arguments):
                started_phases.add(phase_name)
                if started_phases == expected_phases:
                    all_phases_started.set()
                await release_phases.wait()

            return run_phase

        application._query_settings_fields = make_phase("settings")
        application._query_conmon_all = make_phase("ConMon")
        application._probe_interface_status = make_phase("interfaces")
        application._probe_preferred_leader_all = make_phase("preferred leader")
        application._probe_aes67_all = make_phase("AES67")
        application._probe_sample_rates_all = make_phase("sample rates")
        application._probe_encodings_all = make_phase("encodings")
        application._populate_device_controls = make_phase("controls")
        application.cmc.register_all = make_phase("CMC registration")

        population_task = asyncio.create_task(application.populate_devices(selected_devices, timeout=1.0))
        try:
            await asyncio.wait_for(all_phases_started.wait(), timeout=0.5)
        finally:
            release_phases.set()
            await population_task

        assert started_phases == expected_phases

    @pytest.mark.asyncio
    async def test_populate_devices_cancels_and_awaits_work_at_shared_deadline(self):
        application = DanteApplication()
        selected_device = make_arc_device("selected.local.", "192.168.1.10")
        selected_devices = {selected_device.server_name: selected_device}
        application.devices = selected_devices
        cleanup_events = {phase_name: asyncio.Event() for phase_name in range(8)}

        def make_blocked_phase(phase_name):
            async def run_phase(*arguments, **keyword_arguments):
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup_events[phase_name].set()

            return run_phase

        application._query_settings_fields = make_blocked_phase(0)
        application._query_conmon_all = make_blocked_phase(1)
        application._probe_interface_status = make_blocked_phase(2)
        application._probe_preferred_leader_all = make_blocked_phase(3)
        application._probe_aes67_all = make_blocked_phase(4)
        application._probe_sample_rates_all = make_blocked_phase(5)
        application._probe_encodings_all = make_blocked_phase(6)
        application.cmc.register_all = make_blocked_phase(7)
        application._populate_device_controls = AsyncMock()

        await application.populate_devices(selected_devices, timeout=0.01)

        assert all(cleanup_event.is_set() for cleanup_event in cleanup_events.values())

    @pytest.mark.asyncio
    async def test_populate_devices_retains_controls_that_finish_after_phase_deadline(self):
        application = DanteApplication()
        selected_device = make_arc_device("selected.local.", "192.168.1.10")
        selected_devices = {selected_device.server_name: selected_device}
        application.devices = selected_devices
        phase_count = 8
        cancelled_phase_count = 0
        all_phases_cancelled = asyncio.Event()
        control_started = asyncio.Event()
        release_control = asyncio.Event()

        async def blocked_phase(*arguments, **keyword_arguments):
            nonlocal cancelled_phase_count
            try:
                await asyncio.Event().wait()
            finally:
                cancelled_phase_count += 1
                if cancelled_phase_count == phase_count:
                    all_phases_cancelled.set()

        async def populate_controls(*arguments, **keyword_arguments):
            control_started.set()
            await release_control.wait()
            selected_device.tx_count = 128

        application._query_settings_fields = blocked_phase
        application._query_conmon_all = blocked_phase
        application._probe_interface_status = blocked_phase
        application._probe_preferred_leader_all = blocked_phase
        application._probe_aes67_all = blocked_phase
        application._probe_sample_rates_all = blocked_phase
        application._probe_encodings_all = blocked_phase
        application.cmc.register_all = blocked_phase
        application._populate_device_controls = populate_controls

        population_task = asyncio.create_task(application.populate_devices(selected_devices, timeout=0.01))
        await control_started.wait()
        await all_phases_cancelled.wait()

        assert population_task.done() is False
        release_control.set()
        await population_task
        assert selected_device.tx_count == 128

    @pytest.mark.asyncio
    async def test_discover_named_device_anchors_services_to_arc_record(self, monkeypatch):
        import netaudio.dante.application as application_module
        import netaudio.dante.browser as browser_module
        from zeroconf import ServiceStateChange
        from netaudio.dante.const import SERVICE_ARC, SERVICE_CMC, SERVICE_DBC

        class FakeAsyncZeroconf:
            instances = []

            def __init__(self, **keyword_arguments):
                self.zeroconf = object()
                self.closed = False
                self.instances.append(self)

            async def async_close(self):
                self.closed = True

        class FakeBrowser:
            instances = []

            def __init__(self, mdns_timeout, app):
                self.aio_zc = None
                self.instances.append(self)

            def get_zeroconf_kwargs(self):
                return {}

            async def async_parse_netaudio_service(self, zeroconf, service_type, service_name):
                server_name = "device-server.local." if service_type != SERVICE_DBC else "other-server.local."
                properties = {}
                if service_type == SERVICE_ARC:
                    properties = {"model": "LX-DANTE", "mf": "Digigram", "router_vers": "4.0.1"}
                elif service_type == SERVICE_CMC:
                    properties = {"id": "001dc10812580000", "server_vers": "4.0.0"}
                return {
                    "ipv4": "192.168.1.108",
                    "name": service_name,
                    "port": 4440 if service_type == SERVICE_ARC else 8800,
                    "properties": properties,
                    "server_name": server_name,
                    "type": service_type,
                }

            async def async_close(self):
                await self.aio_browser.async_cancel()
                await self.aio_zc.async_close()

        class FakeAsyncServiceBrowser:
            def __init__(self, zeroconf, service_types, handlers):
                self.cancelled = False
                asyncio.get_running_loop().call_soon(
                    handlers[0],
                    zeroconf,
                    SERVICE_ARC,
                    f"lx-dante.{SERVICE_ARC}",
                    ServiceStateChange.Added,
                )

            async def async_cancel(self):
                self.cancelled = True

        monkeypatch.setattr(application_module, "AsyncZeroconf", FakeAsyncZeroconf)
        monkeypatch.setattr(application_module, "AsyncServiceBrowser", FakeAsyncServiceBrowser)
        monkeypatch.setattr(browser_module, "DanteBrowser", FakeBrowser)
        application = DanteApplication()

        devices = await application.discover_named_device("lx-dante", timeout=0.1)

        assert set(devices) == {"device-server.local."}
        device = devices["device-server.local."]
        assert device is application.devices["device-server.local."]
        assert device._app is application
        assert str(device.ipv4) == "192.168.1.108"
        assert device.model_id == "LX-DANTE"
        assert device.manufacturer == "Digigram"
        assert device.firmware_version == "4.0.1"
        assert device.software_version == "4.0.0"
        assert device.mac_address == "001dc10812580000"
        assert set(device.services) == {
            f"lx-dante.{SERVICE_ARC}",
            f"lx-dante.{SERVICE_CMC}",
        }
        assert f"lx-dante.{SERVICE_DBC}" not in device.services
        assert FakeAsyncZeroconf.instances[0].closed is True
        assert application._browser is None
