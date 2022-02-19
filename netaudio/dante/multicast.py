from twisted.internet.protocol import DatagramProtocol


class DanteMulticast(DatagramProtocol):
    def __init__(self, group, port):
        self.group = group
        self.port = port

    def startProtocol(self):
        self.transport.joinGroup(self.group)

    def datagramReceived(self, datagram, address):
        ee.emit(
            "received_multicast",
            data=datagram,
            addr=address,
            group=self.group,
            port=self.port,
        )
