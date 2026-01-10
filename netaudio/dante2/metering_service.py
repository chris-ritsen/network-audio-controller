from .events import DanteEventType
from .service import DanteUnicastService, MessageType
from .util import (
    decode_integer,
    decode_mac_address,
)


class DanteMeteringService(DanteUnicastService):
    """
    Receives and handles level metering messages
    (when such things are requested via CMC)
    """
    SERVICE_PORT: int = 8751
    SERVICE_TYPE_MDNS: None = None
    SERVICE_TYPE_SHORT: str = 'mtr'

    def _receive(self, address, message):
        #  0- 1: \xff\xff
        #  2- 3: length
        #  4- 5: seq-id
        #  6- 7: null hextet
        # ~ mac_address = decode_mac_address(message[8:14])
        # 14-15: null octet
        #    15: \x00 == normal
        #        \xff == volume not supported
        # 16-23: `Audinate`
        #    24: ??
        #    25: tx count
        #    26: rx count
        # 27-{+tx_count}: tx meter values
        # ..-{+rx_count}: rx meter values
        # final oct: \x0a
        
        idx = 27

        tx_vols = []
        for tx_idx in range(decode_integer(message, 25, 1)):
            tx_vols.append(255 - message[idx])
            idx += 1

        rx_vols = []
        for rx_idx in range(decode_integer(message, 26, 1)):
            rx_vols.append(255 - message[idx])
            idx += 1

        device = self._app.get_device_by_ipv4(address[0])
        if device:
            self._app.events.notify(DanteEventType.METER_VALUES, device, [tx_vols, rx_vols])
