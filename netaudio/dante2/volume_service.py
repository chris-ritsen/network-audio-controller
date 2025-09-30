# ~ from __future__ import annotations
# ~ from typing import NamedTuple#, TYPE_CHECKING

# ~ if TYPE_CHECKING:
# ~ from zeroconf import ServiceInfo as MDNSServiceInfo

from .service import DanteService, MessageType
# ~ from .util import (
    # ~ decode_integer,
# ~ )

# ~ if TYPE_CHECKING:
# ~ from .device import DanteDevice
# ~ from .util import ProtocolVersion


class DanteVolumeService(DanteService):
    """
    Receives and handles volume status messages
    (when such things are requested via CMC)
    """
    # ~ SERVICE_HEADER_LENGTH: int = 10
    SERVICE_PORT: int = 8751
    SERVICE_TYPE_MDNS: None = None
    SERVICE_TYPE_SHORT: str = 'vol'

    def _receive(self, address, message):
        # ~ message_id = decode_integer(message, 4)
        message_type = message[8:10]

        if message_type == MessageType.SEND:
            # Not ready to handle that sort of message yet
            print("MsgType is SEND")
            return

        print(address, message)
