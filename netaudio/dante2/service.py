from enum import Enum
import logging
import platform
from queue import Queue
import select
import socket
from threading import Thread

# ~ if TYPE_CHECKING:
from zeroconf import ServiceInfo as MDNSServiceInfo

from .util import decode_integer


_PORT_MAGIC: int = 40000


class MessageIndex:
    _num: int = 0

    def generate(self):
        self._num = self._num + 1
        return self._num


class MessageType(bytes, Enum):
    SEND = b'\x00\x00'
    RECV = b'\x00\x01'


class DanteService:

    RECV_BUFFER_SIZE: int = 1024
    SERVICE_HEADER_LENGTH: int
    SERVICE_MCAST_GRP: str | None = None
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
        self._sock: socket.socket | None = None

    @property
    def port(self):
        return _PORT_MAGIC + self.SERVICE_PORT

    @classmethod
    def build_service_descriptor(cls, mdns_service_info: MDNSServiceInfo) -> None:
        raise NotImplementedError

    def _receive(self, address, message):
        message_id = decode_integer(message, 4)
        message_type = message[8:10]

        if message_type == MessageType.SEND:
            # Not ready to handle that sort of message yet
            print("MsgType is SEND")
            return

        if message_id not in self._message_store:
            logging.warning("Received a response from %s to a message not sent: %s", address, message)
            return

        if 'callback' in self._message_store[message_id] and self._message_store[message_id]['callback']:
            self._message_store[message_id]['callback'](message)
        else:
            print(message)

        del self._message_store[message_id]

    def bind(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._sock.bind(("", self.port))

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
        logging.debug(message, address.ip, interface)

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
                    logging.error("RX ERROR: %s\t%s", sock, error)
                    continue
                else:
                    self._receive(address, response)

            for sock in write_socks:
                address, bytestring = self._send_queue.get()
                if not address:
                    address = (self.SERVICE_MCAST_GRP, self.SERVICE_PORT)
                try:
                    sock.sendto(bytestring, address)
                except Exception as error:
                    # TODO: Write better error handling
                    logging.error("TX ERROR IP: %s String: %s\t%s", address, bytestring, error)

            for sock in error_socks:
                # TODO: Write better error handling
                logging.error("SOCK ERROR: %s", sock)

    def send(self, message: bytes, destination: str|None = None) -> None:
        if not destination and not self.SERVICE_MCAST_GRP:
            logging.warning("Attempt to send with no destination!")
            return
        self._send_queue.put((destination, message))

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
