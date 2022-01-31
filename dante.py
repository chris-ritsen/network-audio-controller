#!/bin/python3

import codecs
import socket

def log(message):
    file = open('debug.log', 'a')
    file.write(message)
    file.close()

class Channel(object):
    def __init__(self):
        self._channel_type = None
        self._device = None
        self._index = None
        self._name = None


    @property
    def device(self):
        return self._device


    @device.setter
    def device(self, device):
        self._device = device


    @property
    def index(self):
        return self._index


    @index.setter
    def index(self, index):
        self._index = index


    @property
    def channel_type(self):
        return self._channel_type


    @channel_type.setter
    def channel_type(self, channel_type):
        self._channel_type = channel_type


    @property
    def name(self):
        return self._name


    @name.setter
    def name(self, name):
        self._name = name


    def to_json(self):
        return {
            #  'channel_type': self.channel_type,
            'device': self.device.name,
            'index': self.index,
            'name': self.name
       }


class Subscription(object):
    def __init__(self):
        self._rx_channel = None
        self._rx_device = None
        self._tx_channel = None
        self._tx_device = None


    @property
    def rx_channel(self):
        return self._rx_channel


    @rx_channel.setter
    def rx_channel(self, rx_channel):
        self._rx_channel = rx_channel


    @property
    def tx_channel(self):
        return self._tx_channel


    @tx_channel.setter
    def tx_channel(self, tx_channel):
        self._tx_channel = tx_channel


    @property
    def rx_device(self):
        return self._rx_device


    @rx_device.setter
    def rx_device(self, rx_device):
        self._rx_device = rx_device


    @property
    def tx_device(self):
        return self._tx_device


    @tx_device.setter
    def tx_device(self, tx_device):
        self._tx_device = tx_device


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
        self._tx_channels = set()
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
                        rx_channel_offset = channel[5]
                        status1 = channel[6]
                        status2 = channel[7]

                        self_connected = status1 == '0000' and status2 == '0004'
                        connected_not_self_connected = status1 == '0101' and status2 == '0009'
                        not_connected_not_subscribed = status1 == '0000' and status2 == '0000'
                        not_connected_subscribed = status1 == '0000' and status2 == '0001'

                        rx_channel_name = channel_name(hex_rx_response, rx_channel_offset)

                        if not device_offset == '0000':
                            tx_device_label = channel_name(hex_rx_response, device_offset)

                            if tx_device_label == '.':
                                tx_device_label = self.name
                        else:
                            tx_device_label = self.name

                        if not channel_offset == '0000':
                            tx_channel_name = channel_name(hex_rx_response, channel_offset)
                        else:
                            tx_channel_name = rx_channel_name

                        rx_channels[channel_number] = rx_channel_name

                        if self_connected or connected_not_self_connected:
                            subscriptions.append((f"{rx_channel_name}@{self.name}", f"{tx_channel_name}@{tx_device_label}"))
                            #  subscription = Subscription()
                            #  subscription.rx_channel = 
                            #  subscription.rx_device = 
                            #  subscription.tx_channel = 
                            #  subscription.tx_device = 
        except Exception as e:
            self.error = e
            print(e)

        self.rx_channels = rx_channels
        self.subscriptions = subscriptions


    def get_tx_channels(self):
        tx_channels = set()

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

                        tx_channel_name = channel_name(transmitters, channel_offset)

                        tx_channel = Channel()
                        tx_channel.channel_type = 'tx'
                        tx_channel.index = channel_number
                        tx_channel.device = self
                        tx_channel.name = tx_channel_name

                        tx_channels.add(tx_channel)

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


    def to_json(self):
        return {
            'ipv4': self.ipv4,
            'name': self.name,
            'receivers': self.rx_channels,
            'subscriptions': self.subscriptions,
            'transmitters': list(self.tx_channels)
       }


def channel_name(hex_str, offset):
    parsed_channel_name = None

    try:
        hex_substring = hex_str[int(offset, 16) * 2:]
        partitioned_bytes = bytes.fromhex(hex_substring).partition(b'\x00')[0]
        parsed_channel_name = partitioned_bytes.decode('utf-8')
    except Exception as e:
        pass
    return parsed_channel_name


def command_string(command=None, command_args='0000', sequence1='ff', sequence2='ffff'):
    if command == 'channel_count':
        command_length = '0a'
        command_str = '1000'
    if command == 'device_info':
        command_length = '0a'
        command_str = '1003'
    if command == 'device_name':
        command_length = '0a'
        command_str = '1002'
    if command == 'rx_channels':
        command_length = '10'
        command_str = '3000'
    if command == 'tx_channels':
        command_length = '10'
        command_str = '2000'

    return f'27{sequence1}00{command_length}{sequence2}{command_str}{command_args}'


def command_device_info():
    return command_string('device_info')


def command_device_name():
    return command_string('device_name')


def command_channel_count():
    return command_string('channel_count')


def channel_pagination(page):
    page_hex = format(page, 'x')
    return f'0000000100{page_hex}10000'


def command_receivers(page=0):
    return command_string('rx_channels', channel_pagination(page))


def command_transmitters(page=0):
    return command_string('tx_channels', channel_pagination(page))


class MdnsListener:
    def __init__(self):
        self._devices = {}


    @property
    def devices(self):
        return self._devices


    @devices.setter
    def devices(self, devices):
        self._devices = devices


    def update_service(self, zeroconf, type, name):
        pass


    def remove_service(self, zeroconf, type, name):
        del self.devices[name]


    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        host = zeroconf.cache.entries_with_name(name)
        cache = zeroconf.cache.cache
        ipv4 = info.parsed_addresses()[0]

        service_properties = {k.decode('utf-8'):v.decode('utf-8') for (k, v) in info.properties.items()}
        device = Device()

        if 'mf' in service_properties:
            device.manufacturer = service_properties['mf']
        if 'model' in service_properties:
            device.model = service_properties['model']

        device.ipv4 = ipv4
        device.port = info.port

        self.devices[name] = device
