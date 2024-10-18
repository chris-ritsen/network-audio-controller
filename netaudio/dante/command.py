import codecs
import ipaddress
import random
import socket
import traceback

from netaudio.dante.const import (
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    FEATURE_VOLUME_UNSUPPORTED,
    PORTS,
    SERVICE_ARC,
    SERVICE_CHAN,
)


class DanteCommand:
    def __init__(self, device):
        self.device = device

    async def send(self, command, service_type=None, port=None, expect_response=True):
        sock = self.device._sockets.get_socket(service_type, port)

        if not sock:
            return None

        binary_str = codecs.decode(command, "hex")

        try:
            sock.send(binary_str)
            if expect_response:
                response = sock.recvfrom(2048)[0]
                return response
        except (TimeoutError, socket.timeout):
            pass
        except Exception as e:
            print(e)
            traceback.print_exc()

        return None
