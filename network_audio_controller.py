#!/usr/bin/env python3

import argcomplete
import argparse
import asyncio
import enum
import json
import logging
import netifaces
import os
import signal
import socket
import sys
import time

from pyee import AsyncIOEventEmitter
from json import JSONEncoder

import dante

ee = AsyncIOEventEmitter()

dante_devices = {}

def _default(self, obj):
    return getattr(obj.__class__, 'to_json', _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


def parse_args():
    parser = argparse.ArgumentParser(
        description='List and control network audio devices',
        usage='%(prog)s [options]',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.set_defaults(device_type='dante')

    channels = parser.add_argument_group('channels')
    devices = parser.add_argument_group('devices')
    output = parser.add_mutually_exclusive_group()
    settings = parser.add_argument_group('settings')
    subscriptions = parser.add_argument_group('subscriptions')
    text_output = parser.add_argument_group('text output')
    debug = parser.add_argument_group('debug')

    parser.add_argument('--timeout', '-w', default=1.25, help='Timeout for mDNS discovery', metavar='<timeout>', type=float)

    text_output.add_argument('--list-address', '-a', action='store_true', default=False, help='List device IP addresses')
    text_output.add_argument('--list-devices', '-l', action='store_true', default=False, help='List devices')
    text_output.add_argument('--list-rx', action='store_true', default=False, help='List receiver channels')
    text_output.add_argument('--list-sample-rate', action='store_true', default=False, help='List device sample rate')
    text_output.add_argument('--list-tx', action='store_true', default=False, help='List transmitter channels')
    text_output.add_argument('--list-volume', action='store_true', default=False, help='List volume levels of channels. Not supported on all devices. Partially implemented')

    channels.add_argument('--channel-number', default=None, help='Specify a channel for control by number', metavar='<number>', type=int)
    channels.add_argument('--channel-type', choices=['rx', 'tx'], default=None, help='Channel type to target for operations', type=str)
    channels.add_argument('--tx-channel-name', default=None, help='Specify a transmitter channel name', metavar='<name>', type=str)
    channels.add_argument('--tx-device-name', default=None, help='Specify a transmitter device name', metavar='<name>', type=str)

    devices.add_argument('--dante', action='store_const', const='dante', dest='device_type', help='Use Dante devices for commands')
    devices.add_argument('--device', '-d', default=None, help='Filter results by device name or network address', metavar='<device>', type=str)
    devices.add_argument('--shure', action='store_const', const='shure', dest='device_type', help='Use Shure devices for commands')
    devices.add_argument('--identify', action='store_true', default=False, help='Identify a hardware device by flashing a red LED')

    subscriptions.add_argument('--add-subscription', default=None, help='Add a subscription. Specify by Rx channel number', metavar='<channel_number>', type=str)
    subscriptions.add_argument('--list-subscriptions', action='store_true', default=False, help='List all subscriptions')
    subscriptions.add_argument('--remove-subscription', default=None, help='Remove a subscription. Specify by Rx channel number', metavar='<channel_number>', type=str)

    output.add_argument('--json', action='store_true', default=False, help='Format output in JSON')
    output.add_argument('--tui', '-t', action='store_true', default=False, help='Enable a text user interface')
    output.add_argument('--xml', action='store_true', default=False, help='Format output in XML')

    settings.add_argument('--reset-channel-name', action='store_true', default=False, help='Reset the channel name to the manufacturer default')
    settings.add_argument('--reset-device-name', action='store_true', default=False, help='Reset the device name to the manufacturer default')
    settings.add_argument('--set-channel-name', default=None, help='Specify a value when changing a channel name', dest='new_channel_name', metavar='<name>', type=str)
    settings.add_argument('--set-device-name', default=None, help='Set the device name', dest='new_device_name', metavar='<name>', type=str)
    settings.add_argument('--set-encoding', choices=[16, 24, 32], default=None, dest='encoding', help='Set the encoding of a device', type=int)
    settings.add_argument('--set-gain-level', choices=list(range(1, 6)), default=None, dest='gain_level', help='Set the gain level on a an AVIO device. Lower numbers are higher gain', type=int)
    settings.add_argument('--set-latency', default=None, help='Set the device latency in milliseconds', dest='latency', metavar='<latency>', type=float)
    settings.add_argument('--set-sample-rate', choices=[44100, 48000, 88200, 96000, 176400, 192000], default=None, dest='sample_rate', help='Set the sample rate of a device', type=int)

    debug.add_argument('--debug-multicast', action='store_true', default=False, help='Show multicast data')
    parser.add_argument('--debug', action='store_true', default=False, help='Set log level to DEBUG')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()

    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    return args


def log(message):
    file = open('debug.log', 'a')
    file.write(message)
    file.close()


def print_devices(devices):
    args = parse_args()

    for index, (key, device) in enumerate(devices.items()):
        if args.list_devices:
            if args.list_address:
                print(f'{device} {device.ipv4}')
            else:
                print(f'{device}')
        else:
            if args.list_address:
                print(f'{device.ipv4}')

        if args.list_sample_rate and device.sample_rate:
            print(f'Sample rate: {device.sample_rate}')

        # TODO: list channel volumes
        if args.list_rx:
            rx_channels = device.rx_channels

            if (len(rx_channels)):
                print('Rx channels:')

            for key, channel in rx_channels.items():
                print(channel)

        if args.list_tx:
            tx_channels = device.tx_channels

            if (len(tx_channels)):
                print('Tx channels:')

            for key, channel in tx_channels.items():
                print(channel)

        if args.list_subscriptions:
            if (len(device.subscriptions)):
                print('Subscriptions:')
            for subscription in device.subscriptions:
                print(f'{subscription}')


def control_dante_device(device):
    args = parse_args()

    if args.add_subscription:
        if not args.tx_channel_name:
            print('Must specify a Tx channel name')
        else:
            tx_device_name = args.tx_device_name

            if not tx_device_name:
                tx_device_name = device.name

            rx_channel_number = args.add_subscription
            device.add_subscription(rx_channel_number, args.tx_channel_name, tx_device_name)

    if args.remove_subscription:
        device.remove_subscription(rx_channel_number=args.remove_subscription)

    if args.reset_device_name:
        print(f'Resetting device name for {device.name} {device.ipv4}')
        device.reset_device_name()

    if args.new_device_name:
        print(f'Setting device name for {device.name} {device.ipv4} to {args.new_device_name}')
        device.set_device_name(args.new_device_name)

    if args.reset_channel_name:
        print(f'Resetting name of {args.channel_type} channel {args.channel_number} for {device.name} {device.ipv4}')
        device.reset_channel_name(args.channel_type, args.channel_number)

    if args.sample_rate:
        print(f'Setting sample rate of {device.name} {device.ipv4} to {args.sample_rate}')
        device.set_sample_rate(args.sample_rate)

    if args.gain_level:
        device_type = None
        label = None

        if device.model == 'DAI2' or device.model == 'DAI1':
            device_type = 'input'

            label = {
                1: '+24 dBu',
                2: '+4dBu',
                3: '+0 dBu',
                4: '0 dBV',
                5: '-10 dBV'
            }
        elif device.model == 'DAO2' or device.model == 'DAO1':
            device_type = 'output'

            label = {
                1: '+18 dBu',
                2: '+4 dBu',
                3: '+0 dBu',
                4: '0 dBV',
                5: '-10 dBV'
            }
        else:
            print('This device does not support gain control')

        if device_type:
            if args.channel_number:
                print(f'Setting gain level of {device.name} {device.ipv4} to {label[args.gain_level]} on channel {args.channel_number}')
                device.set_gain_level(args.channel_number, args.gain_level, device_type)
            else:
                print(f'Must specify a channel number')

    if args.encoding:
        print(f'Setting encoding of {device.name} {device.ipv4} to {args.encoding}')
        device.set_encoding(args.encoding)

    if args.new_channel_name:
        print(f'Setting name of {args.channel_type} channel {args.channel_number} for {device.name} {device.ipv4} to {args.new_channel_name}')
        device.set_channel_name(args.channel_type, args.channel_number, args.new_channel_name)

    if args.latency:
        print(f'Setting latency of {device} to {args.latency:g} ms')
        device.set_latency(args.latency)

    if args.identify:
        print(f'Identifying {device} {device.ipv4}')
        device.identify()


@ee.on('received_multicast')
def event_handler(*args, **kwargs):
    addr = kwargs['addr']
    data = kwargs['data']
    group = kwargs['group']
    port = kwargs['port']

    data_hex = data.hex()

    sequence_id = data[4:6]
    command = int.from_bytes(data[26:28], 'big')
    print(command, data[26:28].hex())

    device_ipv4 = addr[0]
    device_mac = data_hex[16:32]
    data_len = int.from_bytes(data[2:4], 'big')

    if command == 96:
        ee.emit('parse_dante_model_info', data=data, addr=addr, group=group, port=port, mac=device_mac)
    elif command == 98:
        print('identify')
    if command == 112:
        print('firmware update related')
    elif command == 146:
        print('device reboot')
    elif command == 192:
        ee.emit('parse_device_make_model_info', data=data, addr=addr, group=group, port=port, mac=device_mac)
    elif command == 258:
        print(device_ipv4, command, 'sub/unsub')
    elif command == 261:
        print(device_ipv4, command, 'sub/unsub')


def debug_multicast():
    multicast_group = '224.0.0.231'
    multicast_port = 8702
    server_address = ('', multicast_port)
    mc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mc_sock.bind(server_address)
    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    mc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while True:
        try:
            data, addr = mc_sock.recvfrom(2048)
            ee.emit('received_multicast', data=data, addr=addr, group=multicast_group, port=multicast_port)
        except Exception as e:
            print(e)


def control_dante_devices(devices):
    args = parse_args()

    if args.debug_multicast:
        debug_multicast()

    try:
        interface = netifaces.ifaddresses(list(netifaces.gateways()['default'].values())[0][1])
        ipv4 = interface[netifaces.AF_INET][0]['addr']
        mac = interface[netifaces.AF_LINK][0]['addr'].replace(':', '')
    except Exception as e:
        pass

    if (args.gain_level or args.encoding or args.sample_rate or args.identify or args.latency or args.add_subscription or args.remove_subscription or args.new_channel_name or args.new_device_name or args.device) or True in [args.reset_channel_name, args.reset_device_name, args.json, args.xml, args.list_sample_rate, args.list_tx, args.list_subscriptions, args.list_rx, args.list_address, args.list_devices]:
        for key, device in devices.items():
            device.get_device_controls()

            if args.list_volume:
                try:
                    device.get_volume(ipv4, mac, 8751)
                except Exception as e:
                    pass

        #  dante.get_make_model_info(mac)

        if args.device:
            devices = dict(filter(lambda x: x[1].name == args.device or x[1].ipv4 == args.device, devices.items()))
        else:
            devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        if not args.json and (args.device and len(devices) == 0):
            print('The specified device was not found')
        else:
            if args.gain_level or args.encoding or args.sample_rate or args.identify or args.latency or args.add_subscription or args.remove_subscription or args.reset_device_name or args.new_device_name or args.reset_channel_name or args.new_channel_name:
                if not args.device:
                    print('Must specify a device name')
                else:
                    device = list(devices.values())[0]
                    control_dante_device(device)

    if args.json:
        if args.device:
            if len(devices.values()) == 1:
                device = list(devices.values())[0]
            else:
                device = None
            json_object = json.dumps(device, indent=2)
        else:
            json_object = json.dumps(list(devices.values()), indent=2)
        print(f'{str(json_object)}')
    elif args.xml:
        print('Not implemented')
    else:
        print_devices(devices)


async def cli_mode():
    args = parse_args()

    if args.device_type == 'dante':
        logger = logging.getLogger('dante')

        if args.debug:
            logger.setLevel(logging.DEBUG)

        dante_browser = dante.DanteBrowser(mdns_timeout=args.timeout)
        await dante_browser.get_devices()
        dante_devices = dante_browser.devices
        logger.debug(f'Initialized {len(dante_devices)} Dante device(s)')

        if len(dante_devices) == 0:
            if not args.json:
                print('No devices detected. Try increasing the mDNS timeout.')
        else:
            if not args.json:
                print(f'{len(dante_devices)} device(s) found')

            control_dante_devices(dante_devices)
    else:
        print('Not implemented')


async def tui_mode():
    args = parse_args()
    print('Not implemented')


async def main():
    args = parse_args()

    logging.basicConfig(level=logging.ERROR)

    if args.tui:
        await tui_mode()
    else:
        await cli_mode()

if __name__ == '__main__':
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
