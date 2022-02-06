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
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default

def handler(signum, frame):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description='List and control network audio devices',
        usage='%(prog)s [options]',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-w',
        '--timeout',
        type=float,
        default=1.25,
        help='Timeout for mDNS discovery')

    parser.add_argument(
        '--add-subscription',
        type=str,
        default=None,
        help='Add a subscription')

    parser.add_argument(
        '--remove-subscription',
        type=str,
        default=None,
        help='Remove a subscription')

    parser.add_argument(
        '--reset-channel-name',
        action='store_true',
        default=False,
        help='Reset the channel name to the manufacturer default')

    parser.add_argument(
        '--reset-device-name',
        action='store_true',
        default=False,
        help='Reset the device name to the manufacturer default')

    parser.add_argument(
        '--set-latency',
        type=float,
        default=None,
        help='Set the device latency in milliseconds')

    parser.add_argument(
        '--set-device-name',
        type=str,
        default=None,
        help='Set the device name')

    parser.add_argument(
        '--tx-device-name',
        type=str,
        default=None,
        help='Specify a Tx device name')

    parser.add_argument(
        '--tx-channel-name',
        type=str,
        default=None,
        help='Specify a Tx channel name')

    parser.add_argument(
        '--channel-type',
        type=str,
        choices=['rx', 'tx'],
        default=None,
        help='Channel type to target for operations')

    parser.add_argument(
        '--channel-number',
        type=str,
        default=None,
        help='Specify a channel for control by number')

    parser.add_argument(
        '--set-channel-name',
        type=str,
        default=None,
        help='Specify a value when changing a channel name')

    parser.add_argument(
        '-d',
        '--device',
        type=str,
        default=None,
        help='Filter results by device name or network address')

    parser.add_argument(
        "-t",
        "--tui",
        action='store_true',
        default=False,
        help='Enable a text user interface'
    )

    parser.add_argument(
        "--json",
        action='store_true',
        default=False,
        help='Format output in JSON'
    )

    parser.add_argument(
        "--xml",
        action='store_true',
        default=False,
        help='Format output in XML'
    )

    parser.add_argument(
        "-l",
        "--list-devices",
        action='store_true',
        default=False,
        help='List devices'
    )

    parser.add_argument(
        "--dante",
        action='store_true',
        default=True, # for now
        help='Use Dante devices for operations'
    )

    parser.add_argument(
        "--list-tx",
        action='store_true',
        default=False,
        help='List all Transmitter channels'
    )

    parser.add_argument(
        "--list-rx",
        action='store_true',
        default=False,
        help='List all Receive channels'
    )

    parser.add_argument(
        "--list-subscriptions",
        action='store_true',
        default=False,
        help='List all subscriptions'
    )

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

    # TODO get all netaudio service types
    browser_arc = ServiceBrowser(zeroconf, "_netaudio-arc._udp.local.", listener)
    browser_dbc = ServiceBrowser(zeroconf, "_netaudio-dbc._udp.local.", listener)
    browser_cmc = ServiceBrowser(zeroconf, "_netaudio-cmc._udp.local.", listener)
    browser_chan = ServiceBrowser(zeroconf, "_netaudio-chan._udp.local.", listener)
    time.sleep(timeout)

    return listener.services


def print_devices(devices):
    args = parse_args()

    for key, device in devices.items():
        if args.list_devices:
            print(f"{device}")

        if args.list_rx:
            rx_channels = device.rx_channels

            for key, channel in rx_channels.items():
                print(channel)

        if args.list_tx:
            tx_channels = device.tx_channels

            for key, channel in tx_channels.items():
                print(channel)

        if args.list_subscriptions:
            for subscription in device.subscriptions:
                #  print(f"{subscription[0]} -> {subscription[1]}")
                print(f"{subscription}")


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
        print(f'Resetting device name for "{device.name}" {device.ipv4}')
        device.reset_device_name()

    if args.set_device_name:
        print(f'Setting device name for "{device.name}" {device.ipv4} to {args.set_device_name}')
        device.set_device_name(args.set_device_name)

    if args.reset_channel_name:
        print(f'Resetting name of {args.channel_type} channel {args.channel_number} for "{device.name}" {device.ipv4}')
        device.reset_channel_name(args.channel_type, args.channel_number)

    if args.set_channel_name:
        print(f'Setting name of {args.channel_type} channel {args.channel_number} for "{device.name}" {device.ipv4} to {args.set_channel_name}')
        device.set_channel_name(args.channel_type, args.channel_number, args.set_channel_name)

    if args.set_latency:
        print(f'Setting latency of "{device}" to {args.set_latency}')
        device.set_latency(args.set_latency)


def control_dante_devices(devices):
    args = parse_args()

    if (args.set_latency or args.add_subscription or args.remove_subscription or args.set_channel_name or args.set_device_name or args.device) or True in [args.reset_channel_name, args.reset_device_name, args.json, args.xml, args.list_tx, args.list_subscriptions, args.list_rx, args.list_devices]:
        for key, device in devices.items():
            device.get_device_controls()

        if args.device:
            devices = dict(filter(lambda x: x[1].name == args.device or x[1].ipv4 == args.device, devices.items()))
        else:
            devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        if not args.json and args.device and len(devices) == 0:
            print('The specified device was not found')

        if args.set_latency or args.add_subscription or args.remove_subscription or args.reset_device_name or args.set_device_name or args.reset_channel_name or args.set_channel_name:
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
        print(f"{str(json_object)}")
    elif args.xml:
        print('Not implemented')
    else:
        print_devices(devices)


def cli_mode():
    args = parse_args()

    if args.dante:
        services = get_dante_services(args.timeout)
        dante.parse_netaudio_services(services)
        dante_devices = dante.get_devices()

        if len(dante_devices) == 0:
            print('No devices detected. Try increasing the mDNS timeout.')
        else:
            control_dante_devices(dante_devices)


def tui_mode():
    args = parse_args()
    print('Not implemented')


def main():
    signal.signal(signal.SIGWINCH, handler)

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
