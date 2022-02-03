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
        default=0.5,
        help='Timeout for mDNS discovery')

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
        '--set-device-name',
        type=str,
        default=None,
        help='Set the device name')

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


def get_dante_devices(timeout):
    zeroconf = Zeroconf()
    listener = dante.MdnsListener()

    # TODO get all netaudio service types
    browser = ServiceBrowser(zeroconf, "_netaudio-arc._udp.local.", listener)
    time.sleep(timeout)

    return dict(listener.devices)


def print_devices(devices):
    args = parse_args()

    for key, device in devices.items():
        if args.list_devices:
            print(f"{device.name}")

        if args.list_rx:
            for channel_number, channel_name in device.rx_channels.items():
                print(f"{channel_number}:{channel_name}")

        if args.list_tx:
            tx_channels = sorted(list(device.tx_channels), key=lambda x: x.number)

            for channel in tx_channels:
                if channel.friendly_name:
                    print(f"{channel.number}:{channel.friendly_name}")
                else:
                    print(f"{channel.number}:{channel.name}")

        if args.list_subscriptions:
            for subscription in device.subscriptions:
                print(f"{subscription[0]} -> {subscription[1]}")


def control_dante_devices(devices):
    args = parse_args()

    if (args.set_channel_name or args.set_device_name or args.device) or True in [args.reset_channel_name, args.reset_device_name, args.json, args.xml, args.list_tx, args.list_subscriptions, args.list_rx, args.list_devices]:
        for key, device in devices.items():
            device.get_device_controls()

        if args.device:
            devices = dict(filter(lambda x: x[1].name == args.device or x[1].ipv4 == args.device, devices.items()))

        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        if args.device and len(devices) == 0:
            print('The specified device was not found')

        if args.reset_device_name or args.set_device_name or args.reset_channel_name or args.set_channel_name:
            if not args.device:
                print('Must specify a device name')
            else:
                if len(devices) == 1:
                    for key, device in devices.items():
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

    if args.json:
        json_object = json.dumps(devices, indent=2)
        print(f"{str(json_object)}")
    elif args.xml:
        print('Not implemented')
    else:
        print_devices(devices)


def cli_mode():
    args = parse_args()

    if args.dante:
        devices = get_dante_devices(args.timeout)

        if len(devices) == 0:
            print('No devices detected. Try increasing the mDNS timeout.')
        else:
            control_dante_devices(devices)


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
