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
        description='List and control Dante network audio devices',
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
        "-l",
        "--list-devices",
        action='store_true',
        default=False,
        help='List all Dante devices'
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
        device = dante.Device()

        if service_properties['mf']:
            device.manufacturer = service_properties['mf']
        if service_properties['model']:
            device.model = service_properties['model']

        device.ipv4 = ipv4
        device.port = info.port

        self.devices[name] = device


def get_devices(timeout):
    zeroconf = Zeroconf()
    listener = MdnsListener()

    browser = ServiceBrowser(zeroconf, "_netaudio-arc._udp.local.", listener)
    time.sleep(timeout)

    return listener.devices


def print_devices(devices):
    args = parse_args()

    for key, device in devices.items():
        if args.list_devices:
            print(f"{device.name}")

        if args.list_rx:
            for channel_index, channel_name in device.rx_channels.items():
                print(f"{channel_index}:{channel_name}")

        if args.list_tx:
            for channel in device.tx_channels:
                print(f"{channel.index}:{channel.name}")

        if args.list_subscriptions:
            for subscription in device.subscriptions:
                print(f"{subscription[0]} -> {subscription[1]}")


def cli_mode():
    args = parse_args()

    if args:
        print(args)

    devices = get_devices(args.timeout)

    if True in [args.json, args.list_tx, args.list_subscriptions, args.list_rx, args.list_devices, args.device]:
        for key, device in devices.items():
            device.get_device_controls()

        if args.device:
            devices = dict(filter(lambda x: x[1].name == args.device or x[1].ipv4 == args.device, devices.items()))

        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    if args.json:
        json_object = json.dumps(devices, indent=2)
        print(f"{str(json_object)}")
    else:
        print_devices(devices)


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
