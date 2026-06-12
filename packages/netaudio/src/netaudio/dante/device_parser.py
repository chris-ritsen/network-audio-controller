import logging

logger = logging.getLogger("netaudio")


class DanteDeviceParser:
    @staticmethod
    def parse_bluetooth_status(response):
        from netaudio import core

        if not response:
            return None
        try:
            status = core.parse_response("bluetooth_status", response)
        except core.NetaudioCoreError:
            return False
        if status and status["connected"]:
            return status["device_name"]
        return None
