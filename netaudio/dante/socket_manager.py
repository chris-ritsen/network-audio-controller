import codecs
import socket

from netaudio.dante.const import (
    PORTS,
    SERVICE_CHAN,
)


class DanteSocketManager:
    def __init__(self, device):
        self.device = device
        self._sockets = {}

    def get_socket(self, service_type=None, port=None):
        if service_type:
            service = self.device.get_service(service_type)

            if service and service["port"] in self._sockets:
                return self._sockets[service["port"]]

        if port in self._sockets:
            return self._sockets[port]

        return None

    def create_service_sockets(self):
        for _, service in self.device.services.items():
            if service["type"] == SERVICE_CHAN:
                continue

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", 0))
            sock.settimeout(1)
            sock.connect((str(self.device.ipv4), service["port"]))
            self._sockets[service["port"]] = sock

    def create_port_sockets(self):
        for port in PORTS:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", 0))
            sock.settimeout(0.01)
            sock.connect((str(self.device.ipv4), port))
            self._sockets[port] = sock

    def get_or_create_socket(self, ipv4, port):
        if port in self._sockets:
            return self._sockets[port]

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.1)
        sock.bind((str(ipv4), port))
        self._sockets[port] = sock

        return sock

    @property
    def sockets(self):
        return self._sockets

    @sockets.setter
    def sockets(self, sockets):
        self._sockets = sockets
