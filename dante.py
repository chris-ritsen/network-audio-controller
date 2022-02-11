#!/bin/python3

import codecs
import socket
import struct
import time

from pyee import AsyncIOEventEmitter
from pyee.cls import evented, on
from zeroconf import DNSService

ee = AsyncIOEventEmitter()

devices = {}
sockets = {}

multicast_groups = {
    'device_info': '224.0.0.231'
}

ports = {
    'device_control': 8800,
    'device_info': 8702,
    'device_settings': 8700
}

service_types = {
    'arc': '_netaudio-arc._udp.local.',
    'chan': '_netaudio-chan._udp.local.',
    'cmc': '_netaudio-cmc._udp.local.',
    'dbc': '_netaudio-dbc._udp.local.'
}

status = {
    'connected': 'Connected',
    'connected_unicast': 'Connected (Unicast)',
    'incorrect_channel_format': 'Incorrect channel format',
    'self_subscribed': 'Subscribed to own signal',
    'unresolved': 'Subscription unresolved'
}

class Channel(object):
    def __init__(self):
        self._channel_type = None
        self._device = None
        self._friendly_name = None
        self._name = None
        self._number = None
        self._status_codes = None
        self._status_text = None
        self._volume = None


    def __str__(self):
        name = self.name

        if self.friendly_name:
            name = self.friendly_name

        if self.volume and self.volume != 254:
            return (f'{self.number}:{name} [{self.volume}]')
        else:
            return(f'{self.number}:{self.name}')


    @property
    def device(self):
        return self._device


    @device.setter
    def device(self, device):
        self._device = device


    @property
    def number(self):
        return self._number


    @number.setter
    def number(self, number):
        self._number = number


    @property
    def status_codes(self):
        return self._status_codes


    @status_codes.setter
    def status_codes(self, status_codes):
        self._status_codes = status_codes


    @property
    def status_text(self):
        return self._status_text


    @status_text.setter
    def status_text(self, status_text):
        self._status_text = status_text


    @property
    def channel_type(self):
        return self._channel_type


    @channel_type.setter
    def channel_type(self, channel_type):
        self._channel_type = channel_type


    @property
    def friendly_name(self):
        return self._friendly_name


    @friendly_name.setter
    def friendly_name(self, friendly_name):
        self._friendly_name = friendly_name


    @property
    def name(self):
        return self._name


    @name.setter
    def name(self, name):
        self._name = name


    @property
    def volume(self):
        return self._volume


    @volume.setter
    def volume(self, volume):
        self._volume = volume


    def to_json(self):
        as_json = {
            'number': self.number,
            'name': self.name
        }

        if self.friendly_name:
            as_json['friendly_name'] = self.friendly_name

        if self.status_text:
            as_json['status_text'] = self.status_text

        if self.volume:
            as_json['volume'] = self.volume

        return {key:as_json[key] for key in sorted(as_json.keys())}


class Subscription(object):
    def __init__(self):
        self._error = None
        self._rx_channel = None
        self._rx_channel_name = None
        self._rx_device = None
        self._rx_device_name = None
        self._status_codes = None
        self._status_text = None
        self._tx_channel = None
        self._tx_channel_name = None
        self._tx_device = None
        self._tx_device_name = None


    def __str__(self):
        text = f'{self.rx_channel_name}@{self.rx_device_name} <- {self.tx_channel_name}@{self.tx_device_name}'

        if self.status_text:
            return f'{text} [{self.status_text}]'
        else:
            return text


    def to_json(self):
        as_json = {
            'rx_channel': self.rx_channel_name,
            'rx_device': self.rx_device_name,
            'tx_channel': self.tx_channel_name,
            'tx_device': self.tx_device_name,
        }

        if self.status_text:
            as_json['status_text'] = self.status_text

        return as_json


    @property
    def error(self):
        return self._error


    @error.setter
    def error(self, error):
        self._error = error


    @property
    def rx_channel_name(self):
        return self._rx_channel_name


    @rx_channel_name.setter
    def rx_channel_name(self, rx_channel_name):
        self._rx_channel_name = rx_channel_name


    @property
    def tx_channel_name(self):
        return self._tx_channel_name


    @tx_channel_name.setter
    def tx_channel_name(self, tx_channel_name):
        self._tx_channel_name = tx_channel_name


    @property
    def rx_device_name(self):
        return self._rx_device_name


    @rx_device_name.setter
    def rx_device_name(self, rx_device_name):
        self._rx_device_name = rx_device_name


    @property
    def status_codes(self):
        return self._status_codes


    @status_codes.setter
    def status_codes(self, status_codes):
        self._status_codes = status_codes


    @property
    def status_text(self):
        return self._status_text


    @status_text.setter
    def status_text(self, status_text):
        self._status_text = status_text


    @property
    def tx_device_name(self):
        return self._tx_device_name


    @tx_device_name.setter
    def tx_device_name(self, tx_device_name):
        self._tx_device_name = tx_device_name


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


@evented
class Device(object):
    def __init__(self):
        self._dante_model = ''
        self._dante_model_id = ''
        self._error = None
        self._ipv4 = ''
        self._latency = None
        self._mac_address = None
        self._manufacturer = ''
        self._model = ''
        self._model_id = ''
        self._name = ''
        self._rx_channels = {}
        self._rx_count = 0
        self._rx_count_raw = 0
        self._sample_rate = None
        self._server_name = ''
        self._services = {}
        self._sockets = {}
        self._software = None
        self._subscriptions = []
        self._tx_channels = {}
        self._tx_count_raw = 0


    def __str__(self):
        return (f'{self.name}')


    def dante_command(self, command, service_type=None, port=None):
        response = None

        if service_type:
            service = self.get_service(service_type)
            sock = self.sockets[service['port']]

        if port:
            sock = self.sockets[port]

        binary_str = codecs.decode(command, 'hex')

        try:
            sock.send(binary_str)
            response = sock.recvfrom(2048)[0]
        except:
            pass

        return response


    def set_channel_name(self, channel_type, channel_number, new_channel_name):
        response = self.dante_command(*command_set_channel_name(channel_type, channel_number, new_channel_name))
        return response


    def identify(self):
        response = self.dante_command(*command_identify())
        return response


    def set_latency(self, latency):
        response = self.dante_command(*command_set_latency(latency))
        return response


    def set_gain_level(self, channel_number, gain_level, device_type):
        response = self.dante_command(*command_set_gain_level(channel_number, gain_level, device_type))
        return response


    def set_encoding(self, encoding):
        response = self.dante_command(*command_set_encoding(encoding))
        return response


    def set_sample_rate(self, sample_rate):
        response = self.dante_command(*command_set_sample_rate(sample_rate))
        return response


    def add_subscription(self, rx_channel_number, tx_channel_name, tx_device_name):
        response = self.dante_command(*command_add_subscription(rx_channel_number, tx_channel_name, tx_device_name))
        return response


    def remove_subscription(self, rx_channel_number):
        response = self.dante_command(*command_remove_subscription(rx_channel_number))
        return response


    def reset_channel_name(self, channel_type, channel_number):
        response = self.dante_command(*command_reset_channel_name(channel_type, channel_number))
        return response


    def set_device_name(self, name):
        response = self.dante_command(*command_set_device_name(name))
        return response


    def reset_device_name(self):
        response = self.dante_command(*command_reset_device_name())
        return response


    def get_service(self, service_type):
        service = None

        try:
            service = next(filter(lambda x: x[1]['type'] == service_type, self.services.items()))[1]
        except Exception as e:
            self.error = e
            print(e)

        return service


    @on('dante_model_info')
    def event_handler(self, *args, **kwargs):
        ipv4 = kwargs['ipv4']
        mac = kwargs['mac']
        model = kwargs['model']
        model_id = kwargs['model_id']

        self.dante_model = model
        self.dante_model_id = model_id


    @on('parse_dante_model_info')
    def event_handler(self, *args, **kwargs):
        addr = kwargs['addr']
        data = kwargs['data']
        mac = kwargs['mac']

        ipv4 = addr[0]

        model = data[88:].partition(b'\x00')[0].decode('utf-8')
        model_id = data[43:].partition(b'\x00')[0].decode('utf-8').replace('\u0003', '')

        self.event_emitter.emit('dante_model_info', model_id=model_id, model=model, ipv4=ipv4, mac=mac)


    @on('device_make_model_info')
    def event_handler(self, *args, **kwargs):
        ipv4 = kwargs['ipv4']
        mac = kwargs['mac']
        manufacturer = kwargs['manufacturer']
        model = kwargs['model']

        self.manufacturer = manufacturer
        self.model = model


    @on('parse_device_make_model_info')
    def event_handler(self, *args, **kwargs):
        addr = kwargs['addr']
        data = kwargs['data']
        mac = kwargs['mac']

        ipv4 = addr[0]

        manufacturer = data[76:].partition(b'\x00')[0].decode('utf-8')
        model = data[204:].partition(b'\x00')[0].decode('utf-8')

        self.event_emitter.emit('device_make_model_info', manufacturer=manufacturer, model=model, ipv4=ipv4, mac=mac)


    @on('subscription_changed')
    def event_handler(self, *args, **kwargs):
        addr = kwargs['addr']
        data = kwargs['data']
        mac = kwargs['mac']


    @on('received_multicast')
    def event_handler(self, *args, **kwargs):
        addr = kwargs['addr']
        data = kwargs['data']
        group = kwargs['group']
        port = kwargs['port']

        data_hex = data.hex()
        device_ipv4 = addr[0]

        device_mac = data_hex[16:32]

        if self.ipv4 != device_ipv4:
            return

        sequence_id = data[4:6]
        command = int.from_bytes(data[26:28], 'big')

        data_len = int.from_bytes(data[2:4], 'big')

        if command == 96:
            self.event_emitter.emit('parse_dante_model_info', data=data, addr=addr, group=group, port=port, mac=device_mac)
        elif command == 192:
            self.event_emitter.emit('parse_device_make_model_info', data=data, addr=addr, group=group, port=port, mac=device_mac)


    def get_device_controls(self):
        try:
            for key, service in self.services.items():
                if service['type'] == service_types['chan']:
                    continue

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(('', 0))
                sock.settimeout(20)
                sock.connect((self.ipv4, service['port']))
                self.sockets[service['port']] = sock

            for key, port in ports.items():
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(('', 0))
                sock.settimeout(0.01)
                sock.connect((self.ipv4, port))
                self.sockets[port] = sock
        except Exception as e:
            self.error = e
            print(e)

        try:
            if not self.name:
                self.name = self.dante_command(*command_device_name())[10:-1].decode('ascii')

            # get reported rx/tx channel counts
            if not self.rx_count or not self.tx_count:
                channel_count = self.dante_command(*command_channel_count())
                self.rx_count_raw = self.rx_count = int.from_bytes(channel_count[15:16], 'big')
                self.tx_count_raw = self.tx_count = int.from_bytes(channel_count[13:14], 'big')

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


    def parse_volume(self, bytes_volume):
        rx_channels = bytes_volume[-1 - self.rx_count_raw:-1]
        tx_channels = bytes_volume[-1 - self.rx_count_raw - self.tx_count_raw:-1 - self.rx_count_raw]

        try:
            for index, channel in self.tx_channels.items():
                channel.volume = tx_channels[channel.number - 1]

            for channel_number, channel in self.rx_channels.items():
                channel.volume = rx_channels[channel.number - 1]

        except Exception as e:
            print(e)


    def get_volume(self, ipv4, mac, port):
        try:
            if self.software == 'Dante Via' or (self.model_id and (self.manufacturer == 'Audinate' and self.model_id in ['DAI1', 'DAI2', 'DAO1', 'DAO2', 'DIOUSB', 'DIAES3', 'DIUSBC'] or self.model_id in ['_86012780000a0003'])):
                return

            if port in sockets:
                sock = sockets[port]
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(0.1)
                sock.bind((ipv4, port))
                sockets[port] = sock

            volume_start = self.dante_command(*command_volume_start(self.name, ipv4, mac, port))

            if volume_start:
                while True:
                    try:
                        data, addr = sock.recvfrom(2048)
                        if addr[0] == self.ipv4:
                            volume_stop = self.dante_command(*command_volume_stop(self.name, ipv4, mac, port))
                            self.parse_volume(data)
                        break
                    except:
                        break

        except Exception as e:
            print(e)


    def get_rx_channels(self):
        rx_channels = {}
        subscriptions = []

        try:
            for page in range(0, max(int(self.rx_count / 16), 1)):
                receivers = self.dante_command(*command_receivers(page))
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

                        rx_channel_name = get_label(hex_rx_response, rx_channel_offset)

                        tx_device_name = get_label(hex_rx_response, device_offset)

                        if not channel_offset == '0000':
                            tx_channel_name = get_label(hex_rx_response, channel_offset)
                        else:
                            tx_channel_name = rx_channel_name

                        if index == 0 and not device_offset == '0000':
                            o1 = (int(channel[2], 16) * 2) + 2
                            o2 = o1 + 6
                            sample_rate_hex = hex_rx_response[o1:o2]

                            if sample_rate_hex != '000000':
                                self.sample_rate = int(sample_rate_hex, 16)

                        self_connected = status1 == '0000' and status2 == '0004'
                        connected_not_self_connected = status1 == '0101' and status2 == '0009'
                        not_connected_not_subscribed = status1 == '0000' and status2 == '0000'
                        unresolved = status1 == '0000' and status2 == '0001'
                        incorrect_channel_format = status1 == '0000' and status2 == '0010'

                        channel_status_text = None
                        subscription_status_text = None

                        subscription = Subscription()
                        rx_channel = Channel()

                        if self_connected:
                            subscription.status_text = channel_status_text = status['self_subscribed']

                        if connected_not_self_connected:
                            subscription.status_text = channel_status_text = status['connected']

                        if unresolved:
                            subscription.error = subscription.status_text = status['unresolved']

                        if incorrect_channel_format:
                            subscription.error = subscription.status_text = status['incorrect_channel_format']

                        rx_channel.channel_type = 'rx'
                        rx_channel.device = self
                        rx_channel.name = rx_channel_name
                        rx_channel.number = channel_number
                        rx_channel.status_codes = (status1, status2)

                        if channel_status_text:
                            rx_channel.status_text = channel_status_text

                        rx_channels[channel_number] = rx_channel

                        if subscription_status_text:
                            subscription.status_text = subscription_status_text

                        if tx_device_name:
                            subscription.rx_channel_name = rx_channel_name
                            subscription.rx_device_name = self.name
                            subscription.tx_channel_name = tx_channel_name
                            subscription.status_codes = (status1, status2)

                            if tx_device_name == '.':
                                subscription.tx_device_name = self.name
                            else:
                                subscription.tx_device_name = tx_device_name

                            subscriptions.append(subscription)
        except Exception as e:
            self.error = e
            print(e)

        self.rx_channels = rx_channels
        self.subscriptions = subscriptions


    def get_tx_channels(self):
        tx_channels = {}
        tx_friendly_channel_names = {}

        try:
            for page in range(0, max(1, int(self.tx_count / 16)), 2):
                tx_friendly_names = self.dante_command(*command_transmitters(page, friendly_names=True)).hex()

                for index in range(0, min(self.tx_count, 32)):
                    str1 = tx_friendly_names[(24 + (index * 12)):(36 + (index * 12))]
                    n = 4
                    channel = [str1[i:i + 4] for i in range(0, len(str1), n)]
                    channel_index = int(channel[0], 16)
                    channel_number = int(channel[1], 16)
                    channel_offset = channel[2]
                    tx_channel_friendly_name = get_label(tx_friendly_names, channel_offset)

                    if tx_channel_friendly_name:
                        tx_friendly_channel_names[channel_number] = tx_channel_friendly_name

            for page in range(0, max(1, int(self.tx_count / 16)), 2):
                transmitters = self.dante_command(*command_transmitters(page, friendly_names=False)).hex()

                has_disabled_channels = False

                # TODO: Find the sample rate in the response instead of relying on it being already set from elsewhere
                if self.sample_rate:
                    has_disabled_channels = transmitters.count(f'{self.sample_rate:06x}') == 2

                first_channel = []

                for index in range(0, min(self.tx_count, 32)):
                    str1 = transmitters[(24 + (index * 16)):(40 + (index * 16))]
                    n = 4
                    channel = [str1[i:i + 4] for i in range(0, len(str1), n)]

                    if index == 0:
                        first_channel = channel

                    if channel:
                        o1 = (int(channel[2], 16) * 2) + 2
                        o2 = o1 + 6
                        sample_rate_hex = transmitters[o1:o2]

                        if sample_rate_hex != '000000':
                            self.sample_rate = int(sample_rate_hex, 16)

                        channel_number = int(channel[0], 16)
                        channel_status = channel[1][2:]
                        channel_group = channel[2]
                        channel_offset = channel[3]

                        channel_enabled = channel_group == first_channel[2]
                        channel_disabled = channel_group != first_channel[2]

                        if channel_disabled:
                            break

                        tx_channel_name = get_label(transmitters, channel_offset)

                        tx_channel = Channel()
                        tx_channel.channel_type = 'tx'
                        tx_channel.number = channel_number
                        tx_channel.device = self
                        tx_channel.name = tx_channel_name

                        if channel_number in tx_friendly_channel_names:
                            tx_channel.friendly_name = tx_friendly_channel_names[channel_number]

                        tx_channels[channel_number] = tx_channel

                if has_disabled_channels:
                    break

        except Exception as e:
            self.error = e
            print(e)

        self.tx_channels = tx_channels


    @property
    def ipv4(self):
        return self._ipv4


    @ipv4.setter
    def ipv4(self, ipv4):
        self._ipv4 = ipv4


    @property
    def dante_model(self):
        return self._dante_model


    @dante_model.setter
    def dante_model(self, dante_model):
        self._dante_model = dante_model


    @property
    def dante_model_id(self):
        return self._dante_model_id


    @dante_model_id.setter
    def dante_model_id(self, dante_model_id):
        self._dante_model_id = dante_model_id


    @property
    def model(self):
        return self._model


    @model.setter
    def model(self, model):
        self._model = model


    @property
    def model_id(self):
        return self._model_id


    @model_id.setter
    def model_id(self, model_id):
        self._model_id = model_id


    @property
    def latency(self):
        return self._latency


    @latency.setter
    def latency(self, latency):
        self._latency = latency


    @property
    def mac_address(self):
        return self._mac_address


    @mac_address.setter
    def mac_address(self, mac_address):
        self._mac_address = mac_address


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
    def sample_rate(self):
        return self._sample_rate


    @sample_rate.setter
    def sample_rate(self, sample_rate):
        self._sample_rate = sample_rate


    @property
    def server_name(self):
        return self._server_name


    @server_name.setter
    def server_name(self, server_name):
        self._server_name = server_name


    @property
    def sockets(self):
        return self._sockets


    @sockets.setter
    def sockets(self, sockets):
        self._sockets = sockets


    @property
    def software(self):
        return self._software


    @software.setter
    def software(self, software):
        self._software = software


    @property
    def rx_channels(self):
        return self._rx_channels


    @rx_channels.setter
    def rx_channels(self, rx_channels):
        self._rx_channels = rx_channels


    @property
    def services(self):
        return self._services


    @services.setter
    def services(self, services):
        self._services = services


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


    @property
    def tx_count_raw(self):
        return self._tx_count_raw


    @tx_count_raw.setter
    def tx_count_raw(self, tx_count_raw):
        self._tx_count_raw = tx_count_raw


    @property
    def rx_count_raw(self):
        return self._rx_count_raw


    @rx_count_raw.setter
    def rx_count_raw(self, rx_count_raw):
        self._rx_count_raw = rx_count_raw


    def to_json(self):
        rx_channels = dict(sorted(self.rx_channels.items(), key=lambda x: x[1].number))
        tx_channels = dict(sorted(self.tx_channels.items(), key=lambda x: x[1].number))

        as_json = {
            'channels': {
                'receivers': rx_channels,
                'transmitters': tx_channels
            },
            'ipv4': self.ipv4,
            'name': self.name,
            'server_name': self.server_name,
            'services': self.services,
            'subscriptions': self.subscriptions,
        }

        if self.sample_rate:
            as_json['sample_rate'] = self.sample_rate

        if self.latency:
            as_json['latency'] = self.latency

        if self.manufacturer:
            as_json['manufacturer'] = self.manufacturer


        if self.dante_model:
            as_json['dante_model'] = self.dante_model

        if self.dante_model_id:
            as_json['dante_model_id'] = self.dante_model_id

        if self.model:
            as_json['model'] = self.model

        if self.model_id:
            as_json['model_id'] = self.model_id

        if self.mac_address:
            as_json['mac_address'] = self.mac_address

        return {key:as_json[key] for key in sorted(as_json.keys())}


def get_label(hex_str, offset):
    parsed_get_label = None

    try:
        hex_substring = hex_str[int(offset, 16) * 2:]
        partitioned_bytes = bytes.fromhex(hex_substring).partition(b'\x00')[0]
        parsed_get_label = partitioned_bytes.decode('utf-8')
    except Exception as e:
        pass
    return parsed_get_label


def command_string(command=None, command_str=None, command_args='0000', command_length='00', sequence1='ff', sequence2='ffff'):
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
    if command == 'reset_device_name':
        command_length = '0a'
        command_str = '1001'
        command_args = '0000'
    if command == 'set_device_name':
        command_str = '1001'

    command_hex = f'27{sequence1}00{command_length}{sequence2}{command_str}{command_args}'

    if command == 'add_subscription':
        command_length = f'{int(len(command_hex) / 2):02x}'
        command_hex = f'27{sequence1}00{command_length}{sequence2}{command_str}{command_args}'

    return command_hex


def get_name_lengths(device_name):
    name_len = len(device_name)
    offset = (name_len & 1) - 2
    padding = 10 - (name_len + offset)
    name_len1 = (len(device_name) * 2) + padding
    name_len2 = name_len1 + 2
    name_len3 = name_len2 + 4

    return (name_len1, name_len2, name_len3)


def command_make_model(mac):
    cmd_args = '00c100000000'
    command_string = f'ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}'

    return (command_string, None, ports['device_settings'])


def command_dante_model(mac):
    cmd_args='006100000000'
    command_string = f'ffff00200fdb0000{mac}0000417564696e6174650731{cmd_args}'

    return (command_string, None, ports['device_settings'])


def command_volume_start(device_name, ipv4, mac, port, timeout=True):
    data_len = 0
    device_name_hex = device_name.encode().hex()
    ip_hex = socket.inet_aton(ipv4).hex()

    name_len1, name_len2, name_len3 = get_name_lengths(device_name)

    if len(device_name) % 2 == 0:
        device_name_hex = f'{device_name_hex}00'

    if len(device_name) < 2:
        data_len = 54
    elif len(device_name) < 4:
        data_len = 56
    else:
        data_len = len(device_name) + (len(device_name) & 1) + 54

    unknown_arg = '16'
    command_string = f'120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}{unknown_arg}0001000100{name_len3:02x}0001{port:04x}{timeout:04x}0000{ip_hex}{port:04x}0000'

    return (command_string, None, ports['device_control'])


def command_volume_stop(device_name, ipv4, mac, port):
    data_len = 0
    device_name_hex = device_name.encode().hex()
    ip_hex = '00000000'

    name_len1, name_len2, name_len3 = get_name_lengths(device_name)

    if len(device_name) % 2 == 0:
        device_name_hex = f'{device_name_hex}00'

    if len(device_name) < 2:
        data_len = 54
    elif len(device_name) < 4:
        data_len = 56
    else:
        data_len = len(device_name) + (len(device_name) & 1) + 54

    command_string = f'120000{data_len:02x}ffff301000000000{mac}0000000400{name_len1:02x}000100{name_len2:02x}000a{device_name_hex}010016000100{name_len3:02x}0001{port:04x}00010000{ip_hex}{0:04x}0000'

    return (command_string, None, ports['device_control'])


def command_set_latency(latency):
    command_str = '1101'
    command_length = '28'
    latency = int(latency * 1000000)
    latency_hex = f'{latency:06x}'

    command_args = f'00000503820500200211001083010024821983018302830600{latency_hex}00{latency_hex}'

    return (command_string('set_latency', command_length=command_length, command_str=command_str, command_args=command_args), service_types['arc'])


def command_identify():
    mac = '000000000000'
    data_len = 32

    command_string = f'ffff00{data_len:02x}0bc80000{mac}0000417564696e6174650731006300000064'

    return (command_string, None, ports['device_settings'])


def command_set_encoding(encoding):
    ipv4 = '000000'
    data_len = 40

    command_string = f'ffff00{data_len}03d70000525400{ipv4}0000417564696e617465072700830000006400000001000000{encoding:02x}'

    return (command_string, None, ports['device_settings'])


def command_set_gain_level(channel_number, gain_level, device_type):
    ipv4 = '000000'
    data_len = 52

    if device_type == 'input':
        target = f'ffff00{data_len:02x}03440000525400{ipv4}0000417564696e6174650727100a0000000000010001000c001001020000000000'
    elif device_type == 'output':
        target = f'ffff00{data_len:02x}03260000525400{ipv4}0000417564696e6174650727100a0000000000010001000c001002010000000000'

    command_string = f'{target}{channel_number:02x}000000{gain_level:02x}'

    return (command_string, None, ports['device_settings'])


def command_set_sample_rate(sample_rate):
    ipv4 = '000000'
    data_len = 40

    command_string = f'ffff00{data_len:02x}03d40000525400{ipv4}0000417564696e61746507270081000000640000000100{sample_rate:06x}'

    return (command_string, None, ports['device_settings'])


def command_add_subscription(rx_channel_number, tx_channel_name, tx_device_name):
    rx_channel_hex = f'{int(rx_channel_number):02x}'
    command_str = '3010'
    tx_channel_name_hex =  tx_channel_name.encode().hex()
    tx_device_name_hex = tx_device_name.encode().hex()

    tx_channel_name_offset = f'{52:02x}'
    tx_device_name_offset = f'{52 + (len(tx_channel_name) + 1):02x}'

    command_args = f'0000020100{rx_channel_hex}00{tx_channel_name_offset}00{tx_device_name_offset}00000000000000000000000000000000000000000000000000000000000000000000{tx_channel_name_hex}00{tx_device_name_hex}00'

    return (command_string('add_subscription', command_str=command_str, command_args=command_args), service_types['arc'])


def command_remove_subscription(rx_channel):
    rx_channel_hex = f'{int(rx_channel):02x}'
    command_str = '3014'
    args_length = '10'
    command_args = f'00000001000000{rx_channel_hex}'

    return (command_string('remove_subscription', command_str=command_str, command_length=args_length, command_args=command_args), service_types['arc'])


def command_device_info():
    return (command_string('device_info'), service_types['arc'])


def command_device_name():
    return (command_string('device_name'), service_types['arc'])


def command_channel_count():
    return (command_string('channel_count'), service_types['arc'])


def command_set_device_name(name):
    args_length = chr(len(name.encode('utf-8')) + 11)
    args_length = bytes(args_length.encode('utf-8')).hex()

    return (command_string('set_device_name', command_length=args_length, command_args=device_name(name)), service_types['arc'])


def command_reset_device_name():
    return (command_string('reset_device_name'), service_types['arc'])


def command_reset_channel_name(channel_type, channel_number):
    channel_hex = f'{channel_number:02x}'

    if channel_type == 'rx':
        args_length = f'{int(21):02x}'
        command_args = f'0000020100{channel_hex}00140000000000'
        command_str = '3001'
    if channel_type == 'tx':
        args_length = f'{int(25):02x}'
        command_args = f'00000201000000{channel_hex}001800000000000000'
        command_str = '2013'

    return (command_string('reset_channel_name', command_str=command_str, command_args=command_args, command_length=args_length), service_types['arc'])


def command_set_channel_name(channel_type, channel_number, new_channel_name):
    name_hex = new_channel_name.encode().hex()
    channel_hex = f'{channel_number:02x}'

    if channel_type == 'rx':
        command_str = '3001'
        command_args = f'0000020100{channel_hex}001400000000{name_hex}00'
        args_length = chr(len(new_channel_name.encode('utf-8')) + 21)
    if channel_type == 'tx':
        command_str = '2013'
        command_args = f'00000201000000{channel_hex}0018000000000000{name_hex}00'
        args_length = chr(len(new_channel_name.encode('utf-8')) + 25)

    args_length = bytes(args_length.encode('utf-8')).hex()

    return (command_string('set_channel_name', command_str=command_str, command_length=args_length, command_args=command_args), service_types['arc'])


def device_name(name):
    name_hex = name.encode().hex()
    return f'0000{name_hex}00'


def channel_pagination(page):
    page_hex = format(page, 'x')
    command_args = f'0000000100{page_hex}10000'

    return command_args


def command_receivers(page=0):
    return (command_string('rx_channels', command_args=channel_pagination(page)), service_types['arc'])


def command_transmitters(page=0, friendly_names=False):
    if friendly_names:
        command_str = '2010'
    else:
        command_str = '2000'

    command_length = '10'
    command_args = channel_pagination(page=page)

    return (command_string('tx_channels', command_length=command_length, command_str=command_str, command_args=command_args), service_types['arc'])


def get_devices():
    return devices


@ee.on('received_multicast')
def event_handler(*args, **kwargs):
    addr = kwargs['addr']
    data = kwargs['data']
    group = kwargs['group']
    port = kwargs['port']

    data_hex = data.hex()

    device_ipv4 = addr[0]
    device_mac = data_hex[16:32]
    data_len = int.from_bytes(data[2:4], 'big')

    devices_filtered = dict(filter(lambda x: x[1].ipv4 == device_ipv4 or x[1].mac_address == device_mac, devices.items()))

    if len(devices_filtered) == 1:
        device = list(devices_filtered.values())[0]

        try:
            device.event_emitter.emit('received_multicast', *args, **kwargs)
        except Exception as e:
            print(e)


def get_make_model_info(mac):
    multicast_group = multicast_groups['device_info']
    port = ports['device_info']

    if port in sockets:
        sock = sockets[port]
    else:
        server_address = ('', port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.1)
        sock.bind(server_address)
        group = socket.inet_aton(multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sockets[port] = sock

    for key, device in devices.items():
        if device.software:
            continue
        try:
            while True:
                if device.manufacturer and device.model:
                    break

                device.dante_command(*command_make_model(mac))

                data, addr = sock.recvfrom(2048)
                data_hex = data.hex()

                device_ipv4 = addr[0]
                device_mac = data_hex[16:32]

                command = int.from_bytes(data[26:28], 'big')

                if command == 192:
                    ee.emit('received_multicast', data=data, addr=addr, group=multicast_group, port=port)

            while True:
                try:
                    if device.dante_model:
                        break

                    device.dante_command(*command_dante_model(mac))

                    data, addr = sock.recvfrom(2048)
                    data_hex = data.hex()

                    device_ipv4 = addr[0]
                    device_mac = data_hex[16:32]

                    command = int.from_bytes(data[26:28], 'big')

                    if command == 96:
                        ee.emit('received_multicast', data=data, addr=addr, group=multicast_group, port=port)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)


def parse_netaudio_services(services):
    for name, service in dict(services).items():
        zeroconf = service['zeroconf']

        info = zeroconf.get_service_info(service['type'], name)
        host = zeroconf.cache.entries_with_name(name)
        ipv4 = info.parsed_addresses()[0]
        host = zeroconf.cache.entries_with_name(name)

        service_properties = {}

        for key, value in info.properties.items():
            key = key.decode('utf-8')

            if isinstance(value, bytes):
                value = value.decode('utf-8')

            service_properties[key] = value

        for record in host:
            if isinstance(record, DNSService):
                if record.server in devices:
                    device = devices[record.server]
                else:
                    device = Device()

                if 'id' in service_properties and service['type'] == service_types['cmc']:
                    device.mac_address = service_properties['id']

                if 'model' in service_properties:
                    device.model_id = service_properties['model']

                if 'rate' in service_properties:
                    device.sample_rate = int(service_properties['rate'])

                if 'router_info' in service_properties and service_properties['router_info'] == '"Dante Via"':
                    device.software = 'Dante Via'

                if 'latency_ns' in service_properties:
                    device.latency = int(service_properties['latency_ns'])

                device.ipv4 = ipv4
                device.server_name = record.server

                device.services[name] = {
                    'name': name,
                    'port': info.port,
                    'properties': service_properties,
                    'type': info.type,
                }

                devices[record.server] = device


def get_service_types():
    return service_types


def get_service_type(key):
    if key in service_types:
        return service_types[key]


def log(message):
    file = open('debug.log', 'a')
    file.write(message)
    file.close()
