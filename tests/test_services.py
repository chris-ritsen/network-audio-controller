import pytest

from netaudio_lib.dante.services.arc import DanteARCService
from netaudio_lib.dante.services.cmc import DanteCMCService
from netaudio_lib.dante.services.notification import (
    DanteNotificationService,
    NOTIFICATION_EVENT_MAP,
    NOTIFICATION_NAMES,
    NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_AES67_STATUS,
    NOTIFICATION_INTERFACE_STATUS,
)
from netaudio_lib.dante.services.settings import DanteSettingsService
from netaudio_lib.dante.events import DanteEventDispatcher, EventType


class TestDanteARCService:
    def test_instantiation(self):
        service = DanteARCService()
        assert service._commands is not None
        assert service._parser is not None

    def test_instantiation_with_packet_store(self):
        service = DanteARCService(packet_store="fake_store")
        assert service._packet_store == "fake_store"

    @pytest.mark.asyncio
    async def test_get_device_name_not_started(self):
        service = DanteARCService()
        result = await service.get_device_name("192.168.1.1", 4440)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_channel_count_not_started(self):
        service = DanteARCService()
        result = await service.get_channel_count("192.168.1.1", 4440)
        assert result is None


class TestDanteSettingsService:
    def test_instantiation(self):
        service = DanteSettingsService()
        assert service._commands is not None

    @pytest.mark.asyncio
    async def test_identify_not_started(self):
        service = DanteSettingsService()
        result = await service.identify("192.168.1.1")
        assert result is None


class TestDanteCMCService:
    def test_instantiation(self):
        service = DanteCMCService()
        assert service._commands is not None


class TestDanteNotificationService:
    def test_instantiation(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        assert service._dispatcher is dispatcher
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702

    def test_notification_names(self):
        assert NOTIFICATION_NAMES[128] == "Sample Rate Status"
        assert NOTIFICATION_NAMES[257] == "TX Channel Change"
        assert NOTIFICATION_NAMES[258] == "RX Channel Change"
        assert NOTIFICATION_NAMES[4103] == "AES67 Status"

    def test_notification_event_map(self):
        assert NOTIFICATION_EVENT_MAP[NOTIFICATION_SAMPLE_RATE_STATUS] == EventType.SAMPLE_RATE_CHANGED
        assert NOTIFICATION_EVENT_MAP[NOTIFICATION_TX_CHANNEL_CHANGE] == EventType.CHANNEL_NAME_UPDATED
        assert NOTIFICATION_EVENT_MAP[NOTIFICATION_RX_CHANNEL_CHANGE] == EventType.CHANNEL_NAME_UPDATED
        assert NOTIFICATION_EVENT_MAP[NOTIFICATION_AES67_STATUS] == EventType.AES67_CHANGED
        assert NOTIFICATION_EVENT_MAP[NOTIFICATION_INTERFACE_STATUS] == EventType.DEVICE_UPDATED

    def test_set_device_lookup(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)

        def lookup(ip):
            return None

        service.set_device_lookup(lookup)
        assert service._device_lookup is lookup

    def test_on_packet_short_data(self):
        dispatcher = DanteEventDispatcher()
        service = DanteNotificationService(dispatcher=dispatcher)
        # Should not raise for short packets
        service._on_packet(b'\x00' * 10, ("192.168.1.1", 8702))
