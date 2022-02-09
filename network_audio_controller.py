#!/usr/bin/env python3

import argcomplete
import argparse
import enum
import json
import os
import signal
import socket
import sys
import time

from json import JSONEncoder
from zeroconf import ServiceBrowser, Zeroconf, DNSAddress

import dante

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

    parser.add_argument('--timeout', '-w', default=1.25, help='Timeout for mDNS discovery', metavar='<timeout>', type=float)

    text_output.add_argument('--list-address', '-a', action='store_true', default=False, help='List device IP addresses')
    text_output.add_argument('--list-devices', '-l', action='store_true', default=False, help='List devices')
    text_output.add_argument('--list-rx', action='store_true', default=False, help='List receiver channels')
    text_output.add_argument('--list-sample-rate', action='store_true', default=False, help='List device sample rate')
    text_output.add_argument('--list-tx', action='store_true', default=False, help='List transmitter channels')

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


class MdnsListener:
    def __init__(self):
        self._services = {}


    @property
    def services(self):
        return self._services


    @services.setter
    def services(self, services):
        self._services = services


    def update_service(self, zeroconf, type, name):
        #  print(f'service updated\t{name}')
        pass


    def remove_service(self, zeroconf, type, name):
        if name in self.services:
            del self.services[name]
            #  dante.parse_netaudio_services(self.services)
            print(f'service removed\t{name}')


    def add_service(self, zeroconf, type, name):
        self.services[name] = {
            'type': type,
            'zeroconf': zeroconf
        }

        #  print(f'service added \t{name}')


def get_dante_services(timeout):
    zeroconf = Zeroconf()
    listener = MdnsListener()

    for key, service_type in dante.get_service_types().items():
        ServiceBrowser(zeroconf, service_type, listener)

    time.sleep(timeout)

    return listener.services


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
        device.device_name(args.new_device_name)

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
        print(f'Setting latency of {device} to {args.latency}')
        device.set_latency(args.latency)

    if args.identify:
        print(f'Identifying {device} {device.ipv4}')
        device.identify()


def control_dante_devices(devices):
    args = parse_args()

    if (args.gain_level or args.encoding or args.sample_rate or args.identify or args.latency or args.add_subscription or args.remove_subscription or args.new_channel_name or args.new_device_name or args.device) or True in [args.reset_channel_name, args.reset_device_name, args.json, args.xml, args.list_sample_rate, args.list_tx, args.list_subscriptions, args.list_rx, args.list_address, args.list_devices]:
        for key, device in devices.items():
            device.get_device_controls()

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


def cli_mode():
    args = parse_args()

    if args.device_type == 'dante':
        services = get_dante_services(args.timeout)
        dante.parse_netaudio_services(services)
        dante_devices = dante.get_devices()

        if len(dante_devices) == 0:
            if not args.json:
                print('No devices detected. Try increasing the mDNS timeout.')
        else:
            if not args.json:
                print(f'{len(dante_devices)} devices found')
            control_dante_devices(dante_devices)
    else:
        print('Not implemented')


def tui_mode():
    args = parse_args()
    print('Not implemented')


def main():
    args = parse_args()

    if args.tui:
        tui_mode()
    else:
        cli_mode()

if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        pass
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
