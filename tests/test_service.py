import pytest

from netaudio.dante.service import DanteMulticastService


class TestMulticastService:
    def test_initial_state(self):
        service = DanteMulticastService("224.0.0.231", 8702)
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702
        assert service._protocol is None

    @pytest.mark.asyncio
    async def test_start_stop(self):
        service = DanteMulticastService("224.0.0.231", 8702)
        await service.start()
        assert service._protocol is not None
        assert service._protocol.transport is not None
        assert service._protocol.transport.get_extra_info("sockname") == ("0.0.0.0", 8702)

        await service.stop()
        assert service._protocol is None
