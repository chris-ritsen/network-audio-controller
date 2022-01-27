#!/bin/python3

import codecs
import socket

class Device(object):
    def __init__(self):
        self._error = None
        self._ipv4 = ''
        self._manufacturer = ''
        self._model = ''
        self._name = ''
        self._port = None
        self._rx_channels = {}
        self._rx_count = 0
        self._socket = None
        self._subscriptions = ()
        self._tx_channels = {}
        self._tx_count = 0


    def dante_command(self, command):
        binary_str = codecs.decode(command, 'hex')
        self.socket.send(binary_str)
        response = self.socket.recvfrom(1024)[0]
        return response


    def get_device_controls(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', 0))
        self.socket.settimeout(5)
        self.socket.connect((self.ipv4, self.port))

        try:
            if not self.name:
                self.name = self.dante_command(command_device_name())[10:-1].decode('ascii')

            # get reported rx/tx channel counts
            if not self.rx_count or not self.tx_count:
                channel_count = self.dante_command(command_channel_count())
                self.rx_count = int.from_bytes(channel_count[15:16], 'big')
                self.tx_count = int.from_bytes(channel_count[13:14], 'big')

            # get tx channels
            if not self.tx_channels and self.tx_count:
                self.get_tx_channels()

            # get rx channels
            if not self.rx_channels and self.rx_count:
                self.get_rx_channels()

            self.error = None
        except Exception as e:
            self.error = e
            print(e)

    def get_rx_channels(self):
        rx_channels = {}
        subscriptions = []

        try:
            for page in range(0, max(int(self.rx_count / 16), 1)):
                receivers = self.dante_command(command_receivers(page))
                hex_rx_response = receivers.hex()

                for index in range(0, min(self.rx_count, 16)):
                    n = 4
                    str1 = hex_rx_response[(24 + (index * 40)):(56 + (index * 40))]
                    channel = [str1[i:i + n] for i in range(0, len(str1), n)]

                    if channel:
                        channel_number = int(channel[0], 16)
                        channel_offset = channel[3]
                        device_offset = channel[4]
                        input_channel_offset = channel[5]
                        status1 = channel[6]
                        status2 = channel[7]

                        self_connected = status1 == '0000' and status2 == '0004'
                        connected_not_self_connected = status1 == '0101' and status2 == '0009'
                        not_connected_not_subscribed = status1 == '0000' and status2 == '0000'
                        not_connected_subscribed = status1 == '0000' and status2 == '0001'

                        input_channel_label = rx_label(hex_rx_response, input_channel_offset)

                        if not device_offset == '0000':
                            output_device_label = rx_label(hex_rx_response, device_offset)

                            if hex_rx_response[int(device_offset, 16) * 2:].rsplit('00')[0] == '2e':
                                output_device_label = self.name
                        else:
                            output_device_label = self.name

                        if not channel_offset == '0000':
                            output_channel_label = rx_label(hex_rx_response, channel_offset)
                        else:
                            output_channel_label = input_channel_label

                        rx_channels[channel_number] = input_channel_label

                        if self_connected or connected_not_self_connected:
                            subscriptions.append((f"{input_channel_label}@{self.name}", f"{output_channel_label}@{output_device_label}"))

                        #      log(f"Rx: {input_channel_label}@{self.name} -> {output_channel_label}@{output_device_label}\n")
                        #  if not_connected_not_subscribed:
                        #      log(f"Rx: {input_channel_label}@{self.name}\n")
                        #  if not_connected_subscribed:
                        #      log(f"Rx: {input_channel_label}@{self.name} -> {output_channel_label}@{output_device_label} [subscription unresolved]\n")
        except Exception as e:
            self.error = e
            print(e)

        self.rx_channels = rx_channels
        self.subscriptions = subscriptions


    def get_tx_channels(self):
        tx_channels = {}
        tx_channel_names = []

        try:
            for page in range(0, max(1, int(self.tx_count / 16), ), 2):
                transmitters = self.dante_command(command_transmitters(page)).hex()
                has_disabled_channels = transmitters.count('bb80') == 2
                first_channel = []

                for index in range(0, min(self.tx_count, 32)):
                    str1 = transmitters[(24 + (index * 16)):(40 + (index * 16))]
                    n = 4
                    channel = [str1[i:i + 4] for i in range(0, len(str1), n)]

                    if index == 0:
                        first_channel = channel

                    if channel:
                        channel_number = int(channel[0], 16)
                        channel_status = channel[1][2:]
                        channel_group = channel[2]
                        channel_offset = channel[3]

                        channel_enabled = channel_group == first_channel[2]
                        channel_disabled = channel_group != first_channel[2]

                        if channel_disabled:
                            break

                        output_channel_label = rx_label(transmitters, channel_offset)

                        tx_channels[channel_number] = output_channel_label

                if has_disabled_channels:
                    break

        except Exception as e:
            self.error = e
            print(e)

        self.tx_channels = tx_channels


    @property
    def port(self):
        return self._port


    @port.setter
    def port(self, port):
        self._port = port


    @property
    def ipv4(self):
        return self._ipv4


    @ipv4.setter
    def ipv4(self, ipv4):
        self._ipv4 = ipv4


    @property
    def model(self):
        return self._model


    @model.setter
    def model(self, model):
        self._model = model


    @property
    def manufacturer(self):
        return self._manufacturer


    @manufacturer.setter
    def manufacturer(self, manufacturer):
        self._manufacturer = manufacturer


    @property
    def error(self):
        return self._error


    @error.setter
    def error(self, error):
        self._error = error


    @property
    def name(self):
        return self._name


    @name.setter
    def name(self, name):
        self._name = name


    @property
    def socket(self):
        return self._socket


    @socket.setter
    def socket(self, socket):
        self._socket = socket


    @property
    def rx_channels(self):
        return self._rx_channels


    @rx_channels.setter
    def rx_channels(self, rx_channels):
        self._rx_channels = rx_channels


    @property
    def tx_channels(self):
        return self._tx_channels


    @tx_channels.setter
    def tx_channels(self, tx_channels):
        self._tx_channels = tx_channels


    @property
    def subscriptions(self):
        return self._subscriptions


    @subscriptions.setter
    def subscriptions(self, subscriptions):
        self._subscriptions = subscriptions


    @property
    def tx_count(self):
        return self._tx_count


    @tx_count.setter
    def tx_count(self, tx_count):
        self._tx_count = tx_count


    @property
    def rx_count(self):
        return self._rx_count


    @rx_count.setter
    def rx_count(self, rx_count):
        self._rx_count = rx_count


def rx_label(data, offset):
    return bytes.fromhex(data[int(offset, 16) * 2:].rsplit('00')[0]).decode('utf-8')


def command_device_info():
    return'27ff000affff100300000'


def command_device_name():
    return '27ff000affff10020000'


def command_channel_count():
    return '27ff000affff10000000'


def command_receivers(page=0):
    page_hex = format(page, 'x')
    return f'27ff0010ffff30000000000100{page_hex}10000'


def command_transmitters(page=0):
    page_hex = format(page, 'x')
    return f'27ff0010ffff20000000000100{page_hex}10000'
