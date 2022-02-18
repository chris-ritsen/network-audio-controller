from twisted.internet.protocol import DatagramProtocol


class DanteControl(DatagramProtocol):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def startProtocol(self):
        self.transport.connect(self.host, self.port)

    def sendMessage(self, data):
        self.transport.write(data)

    def datagramReceived(self, datagram, addr):
        pass
