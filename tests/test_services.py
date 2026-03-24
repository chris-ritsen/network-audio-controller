import pytest

from netaudio.dante.services.arc import DanteARCService
from netaudio.dante.services.cmc import DanteCMCService
from netaudio.dante.services.notification import (
    DanteNotificationService,
    NOTIFICATION_NAMES,
)
from netaudio.dante.services.settings import DanteSettingsService
from netaudio.dante.events import DanteEventDispatcher


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

    def test_identify_not_started(self):
        service = DanteSettingsService()
        service.identify("192.168.1.1")


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
        service._on_packet(b'\x00' * 10, ("192.168.1.1", 8702))


class TestHeartbeatLockStateParsing:
    def test_locked_device(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state
        payload = bytes.fromhex(
            "fffe00b82d8c0000001dc1fffe5279b6"
            "4175646963617465000800011000000000"
            "1c800100040010"
            "2c2400" "00ffff" "a27600" "000000" "000000" "000000" "000000" "00"
        )
        payload = bytes.fromhex(
            "fffe00b82d8c0000001dc1fffe5279b6"
            "41756469636e617465000800011000000000"
        )
        locked_block = bytes.fromhex(
            "001c800200040010"
            "2c240000"
            "00020000"
            "00010000"
            "00180000"
            "fefe3400"
        )
        header = b'\xff\xfe\x00\xb8' + b'\x00' * 28
        result = _parse_lock_state(header + locked_block)
        assert result is True

    def test_unlocked_device(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state
        unlocked_block = bytes.fromhex(
            "001c800200040010"
            "1b6d0000"
            "00020000"
            "00020000"
            "00180000"
            "fefe7c7c"
        )
        header = b'\xff\xfe\x00\x54' + b'\x00' * 28
        result = _parse_lock_state(header + unlocked_block)
        assert result is False

    def test_no_lock_block(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state
        other_block = bytes.fromhex(
            "0010800100040004"
            "28360000"
            "ffff8a74"
        )
        header = b'\xff\xfe\x00\x54' + b'\x00' * 28
        result = _parse_lock_state(header + other_block)
        assert result is None

    def test_short_packet(self):
        from netaudio.dante.services.heartbeat import _parse_lock_state
        result = _parse_lock_state(b'\x00' * 10)
        assert result is None


class TestKeyExtraction:
    def test_table_pattern_sequential(self):
        from netaudio.common.key_extract import _is_table_pattern
        sequential = bytes(range(32))
        assert _is_table_pattern(sequential) is True

    def test_table_pattern_stride2(self):
        from netaudio.common.key_extract import _is_table_pattern
        stride2 = bytes([i for i in range(0, 64, 2)])
        assert _is_table_pattern(stride2) is True

    def test_high_entropy_not_table(self):
        from netaudio.common.key_extract import _is_table_pattern
        import os
        random_key = os.urandom(32)
        assert _is_table_pattern(random_key) is False

    def test_extract_from_nonexistent(self):
        from pathlib import Path
        from netaudio.common.key_extract import extract_key_from_binary
        result = extract_key_from_binary(Path("/nonexistent/file"))
        assert result is None
