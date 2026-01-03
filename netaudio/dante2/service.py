from enum import Enum
from ipaddress import IPv4Address
import platform
from queue import Queue
import select
import socket
import struct
from threading import Thread

import ifaddr

# ~ if TYPE_CHECKING:
from zeroconf import ServiceInfo as MDNSServiceInfo

from .util import decode_integer, LOGGER


_PORT_MAGIC: int = 40000


class MessageIndex:
    _num: int = 0

    def generate(self):
        self._num = self._num + 1
        return self._num


class MessageType(bytes, Enum):
    SEND = b'\x00\x00'
    RECV = b'\x00\x01'


class _DanteService:

    RECV_BUFFER_SIZE: int = 1024
    SERVICE_HEADER_LENGTH: int
    SERVICE_PORT: str
    SERVICE_TYPE: str | None

    _ignored_addrs: list[str] = ['127.0.0.1']

    def __init__(self, application):
        self._app = application
        self._thread: Thread | None = None

        self._message_index: MessageIndex = MessageIndex()
        self._message_store: dict = {}
        self._receive_queue: Queue = Queue()
        self._send_queue: Queue = Queue()
        self._shutdown_requested: bool = False

    @property
    def port(self):
        return _PORT_MAGIC + self.SERVICE_PORT

    @classmethod
    def build_service_descriptor(cls, mdns_service_info: MDNSServiceInfo) -> None:
        raise NotImplementedError

    def is_ignored_address(self, address):
        return address in self._ignored_addrs

    def register_ignored_address(self, adapter, address, message):
        if address.ip in self._ignored_addrs:
            return
        self._ignored_addrs.append(address.ip)

        if platform.system() == "Windows":
            interface = f"'{adapter.nice_name}' (aka '{address.nice_name}')"
        else:
            interface = f"'{adapter.nice_name}'"
        LOGGER.debug(message, address.ip, interface)

    def run(self) -> None:
        raise NotImplementedError

    def start(self):
        if not self._thread:
            self._shutdown_requested = False
            self._thread = Thread(target=self.run)
            self._thread.start()

    def stop(self):
        if self._thread:
            self._shutdown_requested = True
            self._thread.join()
            self._thread = None

class DanteMulticastService(_DanteService):

    SERVICE_MCAST_GRP: str | None = None
    WINDOWS_KEEPALIVE_TIMEOUT: int = 30 # seconds

    def __init__(self, application):
        super().__init__(application)
        self._sockets: list[socket.socket] = []

    def _receive(self, address, message) -> None:
        raise NotImplementedError

    def bind(self) -> None:
        '''
        On computers with multiple NICs, binding using `socket.INADDR_ANY` only connects to the
        "default" adapter, which is not necessarily the adapter through which the devices we want to
        listen to may be found. (Also, if the "default" adapter is offline, attempting to bind to it
        may throw an exception.)
        '''
        for adapter in ifaddr.get_adapters():
            for addr in adapter.ips:
                if not addr.is_IPv4 or self.is_ignored_address(addr.ip):
                    LOGGER.debug("Not binding to %s on %s", addr.ip, addr.nice_name)
                    continue
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                try:
                    # On Windows, one binds to the local address on the interface;
                    if platform.system() == "Windows":
                        sock.bind((addr.ip, self.SERVICE_PORT))

                    # On *nix systems, one binds to the Multicast group address.
                    else:
                        sock.bind((self.SERVICE_MCAST_GRP, self.SERVICE_PORT))

                except OSError as error:
                    self.handle_binding_error(error, adapter, address)
                    continue

                mreq = struct.pack("4s4s", socket.inet_aton(self.SERVICE_MCAST_GRP), socket.inet_aton(addr.ip))
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                self._sockets.append(sock)

    def handle_binding_error(self, error, adapter, address):
        if error.errno == 10049 and address.ip.startswith("169.254."):
            # Windows (10) assigns a link-local address to unconnected adapters,
            # despite them being, y'know, *not connected* to anything.
            message = "Skipping unbindable address %s on unconnected interface %s"

        else:
            message = f"[{error.errno}] {error.strerror} (%s on %s)"

        self.register_ignored_address(adapter, address, message)

    def run(self) -> None:
        self.bind()

        write_address = (self.SERVICE_MCAST_GRP, self.SERVICE_PORT)
        system = platform.system()
        if system == "Windows":
            # On Windows messages only "appear" on the socket they arrive on...
            rsocks = self.listener_sockets
        else:
            # ...but on *nix systems all messages "appear" on all our listening sockets.
            #
            # This is possibly because whilst they're all assigned to different interfaces, they're
            # all bound to the same address.
            #
            # Whatever the reason, we only need to `select.select()` one of the sockets.
            rsocks = [self._sockets[0]]

        while not self._shutdown_requested:
            if system == "Windows":
                # From experimentation, it appears that on Windows we can only *receive* messages
                # from a multicast group if we've recently *sent* a message to the group. It does
                # not matter what we send for this to work, although ideally it should be something
                # that doesn't fudge up Dante.
                now = int(time.perf_counter())
                win_keepalive = now - self.last_keepalive_sent > self.WINDOWS_KEEPALIVE_TIMEOUT
            else:
                win_keepalive = False

            wsocks = rsocks if not self._send_queue.empty() or win_keepalive else []
            read_socks, write_socks, error_socks = select.select(rsocks, wsocks, rsocks, .2)

            if win_keepalive:
                for sock in write_socks:
                    sock.sendto(b'', write_address)
                self.last_keepalive_sent = now

            for sock in read_socks:
                try:
                    response, address = sock.recvfrom(self.RECV_BUFFER_SIZE)
                except Exception as error:
                    # TODO: Write better error handling
                    LOGGER.error("RX ERROR: %s\t%s", sock, error)
                    continue
                else:
                    self._receive((IPv4Address(address[0]), address[1]), response)

            for sock in write_socks:
                _, bytestring = self._send_queue.get()
                try:
                    sock.sendto(bytestring, write_address)
                except Exception as error:
                    # TODO: Write better error handling
                    LOGGER.error("TX ERROR IP: %s String: %s\t%s", write_address, bytestring, error)

            for sock in error_socks:
                # TODO: Write better error handling
                LOGGER.error("SOCK ERROR: %s", sock)

        self.unbind()

    def send(self, message: bytes) -> None:
        self._send_queue.put((None, message))

    def unbind(self) -> None:
        for sock in self._sockets:
            sock.close()
        self._sockets = []

class DanteUnicastService(_DanteService):

    def __init__(self, application):
        super().__init__(application)
        self._sock: socket.socket | None = None

    def _receive(self, address, message):
        message_id = decode_integer(message, 4)
        message_type = message[8:10]

        if message_type == MessageType.SEND:
            # Not ready to handle that sort of message yet
            print("MsgType is SEND")
            return

        if message_id not in self._message_store:
            LOGGER.warning("Received a response from %s to a message not sent: %s", address, message)
            return

        if 'callback' in self._message_store[message_id] and self._message_store[message_id]['callback']:
            self._message_store[message_id]['callback'](message)
        else:
            print(message)

        del self._message_store[message_id]

    def bind(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._sock.bind(("", self.port))

    def run(self):
        self.bind()
        rsocks = [self._sock]
        while not self._shutdown_requested:
            wsocks = [self._sock] if not self._send_queue.empty() else []
            read_socks, write_socks, error_socks = select.select(rsocks, wsocks, rsocks, .2)

            for sock in read_socks:
                try:
                    response, address = sock.recvfrom(self.RECV_BUFFER_SIZE)
                except Exception as error:
                    # TODO: Write better error handling
                    LOGGER.error("RX ERROR: %s\t%s", sock, error)
                    continue
                else:
                    self._receive(address, response)

            for sock in write_socks:
                address, bytestring = self._send_queue.get()
                try:
                    sock.sendto(bytestring, address)
                except Exception as error:
                    # TODO: Write better error handling
                    LOGGER.error("TX ERROR IP: %s String: %s\t%s", address, bytestring, error)

            for sock in error_socks:
                # TODO: Write better error handling
                LOGGER.error("SOCK ERROR: %s", sock)

        self.unbind()

    def send(self, message: bytes, destination: str) -> None:
        if not destination:
            LOGGER.warning("Attempt to send with no destination!")
            return
        self._send_queue.put((destination, message))

    def unbind(self) -> None:
        if self._sock:
            self._sock.close()
