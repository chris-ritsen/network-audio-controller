import logging

logger = logging.getLogger("netaudio")


class DanteDeviceParser:
    @staticmethod
    def parse_bluetooth_status_state(response):
        from netaudio import core

        if not response:
            return None
        try:
            return core.parse_response("bluetooth_status", response)
        except core.NetaudioCoreError:
            return False

    @staticmethod
    def parse_bluetooth_status(response):
        status = DanteDeviceParser.parse_bluetooth_status_state(response)
        if not isinstance(status, dict):
            return status
        if status and status["connected"]:
            return status["device_name"]
        return None
