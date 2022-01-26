#!/usr/bin/python3

import codecs
import curses
import enum
import math
import os
import random
import signal
import socket
import subprocess
import sys
import time

from zeroconf import ServiceBrowser, Zeroconf, DNSAddress

import dante

# TODO: cli mode
# TODO: Read channel names
# TODO: Read connections

def handler(signum, frame):
    curses.endwin()
    screen = curses.initscr()


def dante_command(device, command):
    binary_str = codecs.decode(command, 'hex')
    device.socket.send(binary_str)
    response = device.socket.recvfrom(1024)[0]
    return response


def log(message):
    file = open('debug.log', 'a')
    file.write(message)
    file.close()


def rx_label(data, offset):
    return bytes.fromhex(data[int(offset, 16) * 2:].rsplit('00')[0]).decode('utf-8')


def main():
    class MyListener:
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
            info = zeroconf.get_service_info(type, name)
            ipv4 = info.parsed_addresses()[0]

            #  log(f"{info.server}:{info.port}\n")
            #  log(f"{info.properties}\n")

            service_properties = {k.decode('utf-8'):v.decode('utf-8') for (k, v) in info.properties.items()}

            device = dante.Device()
            device.ipv4 = ipv4
            device.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            device.socket.bind(('', 0))
            device.socket.settimeout(5)
            device.socket.connect((ipv4, info.port))
            device.manufacturer = service_properties['mf']
            device.model = service_properties['model']

            try:
                if not device.name:
                    device.name = dante_command(device, dante.device_name())[10:-1].decode('ascii')

                # get reported rx/tx channel counts
                if not device.rx_count or not device.tx_count:
                    screen.erase()
                    channel_count = dante_command(device, dante.channel_count())
                    device.rx_count = int.from_bytes(channel_count[15:16], 'big')
                    device.tx_count = int.from_bytes(channel_count[13:14], 'big')
                
                # get tx channels
                if not device.tx_channels and device.tx_count:
                    screen.erase()
                    tx_channels = {}
                    tx_channel_names = []

                    for page in range(0, max(1, int(device.tx_count / 16), ), 2):
                        try:
                            transmitters = dante_command(device, dante.transmitters(page)).hex()
                            has_disabled_channels = transmitters.count('bb80') == 2
                            first_channel = []

                            for index in range(0, min(device.tx_count, 32)):
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
                                    log(f"Tx: {output_channel_label}@{device.name}\n")

                                    tx_channels[channel_number] = output_channel_label

                            if has_disabled_channels:
                                break

                        except Exception as e:
                            device.error = e
                            log(f"device:{device.name} page:{page} {e}\n")

                    device.tx_channels = tx_channels

                #  log(f"\n")

                # get rx channels
                if not device.rx_channels and device.rx_count:
                    rx_channels = {}

                    for page in range(0, max(int(device.rx_count / 16), 1)):
                        receivers = dante_command(device, dante.receivers(page))
                        hex_rx_response = receivers.hex()

                        for index in range(0, min(device.rx_count, 16)):
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
                                        output_device_label = device.name
                                else:
                                    output_device_label = device.name

                                if not channel_offset == '0000':
                                    output_channel_label = rx_label(hex_rx_response, channel_offset)
                                else:
                                    output_channel_label = input_channel_label

                                rx_channels[channel_number] = input_channel_label

                                if self_connected or connected_not_self_connected:
                                    log(f"Rx: {input_channel_label}@{device.name} -> {output_channel_label}@{output_device_label}\n")
                                if not_connected_not_subscribed:
                                    log(f"Rx: {input_channel_label}@{device.name}\n")
                                if not_connected_subscribed:
                                    log(f"Rx: {input_channel_label}@{device.name} -> {output_channel_label}@{output_device_label} [subscription unresolved]\n")

                    device.rx_channels = rx_channels

                device.error = None
            except Exception as e:
                device.error = e
                log(f"{e}\n")

            self.devices[name] = device

            screen.erase()
            device_window.erase()
            rx_channel_window.erase()

    signal.signal(signal.SIGWINCH, handler)

    pos_y = 0

    #  window = curses.newwin(height, width, begin_y, begin_x)

    screen_max_y, screen_max_x = screen.getmaxyx()

    debug_window = curses.newwin(10, screen_max_x, screen_max_y - 10, 0)
    device_window = curses.newwin(30, int(screen_max_x / 3), 0, 0)
    rx_channel_window = curses.newwin(16, int(screen_max_x / 2), 17, int(screen_max_x / 3))
    tx_channel_window = curses.newwin(16, int(screen_max_x / 2), 0, int(screen_max_x / 3))

    zeroconf = Zeroconf()
    listener = MyListener()

    # port 4455 (Audio Control) [Excluding Via] 
    # browser = ServiceBrowser(zeroconf, "_netaudio-dbc._udp.local.", listener)

    # port 4455 (Audio Control) [Excluding Via] Individual channels
    #  browser = ServiceBrowser(zeroconf, "_netaudio-chan._udp.local.", listener)

    # port 8800, Control & Monitoring
    # browser = ServiceBrowser(zeroconf, "_netaudio-cmc._udp.local.", listener)

    # port 4440 (Audio Control) [Excluding Via]
    browser = ServiceBrowser(zeroconf, "_netaudio-arc._udp.local.", listener)

    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_RED)
    curses.init_pair(2, curses.COLOR_BLUE, curses.COLOR_WHITE)
    #  window.addstr(4, 0, f'{screen.getmaxyx()}')

    max_name_width = 31
    selected_device = None
    selected_channel = None
    ordered_devices = None
    ordered_channels = None

    class Window(enum.Enum):
        devices = 1
        channels = 2

    selected_window = Window.devices
    #  selected_window = Window.channels

    while 1:
        pos_y = 0

        devices = dict(sorted(listener.devices.items(), key=lambda x: x[1].name))
        ordered_devices = devices.keys()

        if devices:
            max_name_width = max([len(d.name) for key, d in devices.items()])

        for key, device in devices.items():
            if not selected_device:
                selected_device = key

            line = f'{device.name}'

            if selected_device == key:
                if selected_window == Window.devices:
                    screen.addstr(pos_y, 0, line, curses.color_pair(1))
                else:
                    screen.addstr(pos_y, 0, line, curses.color_pair(2))
            else:
                screen.addstr(pos_y, 0, line)

            pos_y += 1

            if selected_device == key:
                if not device.error:
                    line = f'IP: {device.ipv4} | Tx {str(len(device.tx_channels)).rjust(3)} | Rx: {str(len(device.rx_channels)).rjust(3)}'
                else:
                    line = f'IP: {device.ipv4} | Error: {device.error}'

                device_window.addstr(pos_y, 0, line)
                pos_y += 1

                rx_channel_window.erase()
                tx_channel_window.erase()

                if device.tx_channels:
                    max_y, max_x = tx_channel_window.getmaxyx()
                    channel_pos_y = 0
                    channel_pos_x = 0

                    channels = dict(device.tx_channels)
                    ordered_channels = list(device.tx_channels.keys())

                    if not selected_channel:
                        selected_channel = 0

                    max_channel_name_width = max([len(channel) for key, channel in channels.items()])
                    max_channel_index_width = max([len(str(key)) for key, channel in channels.items()]) 

                    for index, channel in channels.items():
                        if selected_channel == index and selected_window == Window.channels:
                            tx_channel_window.addstr(channel_pos_y, channel_pos_x, f'{str(index).rjust(max_channel_index_width + 1)} {str(channel).ljust(max_channel_name_width + 1)}', curses.color_pair(1))
                        else:
                            tx_channel_window.addstr(channel_pos_y, channel_pos_x, f'{str(index).rjust(max_channel_index_width + 1)} {str(channel).ljust(max_channel_name_width + 1)}')

                        channel_pos_y += 1

                        if channel_pos_y >= max_y:
                            channel_pos_y = 0
                            channel_pos_x += max_channel_name_width + max_channel_index_width + 3

                if device.rx_channels:
                    max_y, max_x = rx_channel_window.getmaxyx()
                    channel_pos_y = 0
                    channel_pos_x = 0

                    channels = dict(device.rx_channels)
                    ordered_channels = list(device.rx_channels.keys())

                    if not selected_channel:
                        selected_channel = 0

                    max_channel_name_width = max([len(channel) for key, channel in channels.items()])
                    max_channel_index_width = max([len(str(key)) for key, channel in channels.items()]) 

                    for index, channel in channels.items():
                        if selected_channel == index and selected_window == Window.channels:
                            rx_channel_window.addstr(channel_pos_y, channel_pos_x, f'{str(index).rjust(max_channel_index_width + 1)} {str(channel).ljust(max_channel_name_width + 1)}', curses.color_pair(1))
                        else:
                            rx_channel_window.addstr(channel_pos_y, channel_pos_x, f'{str(index).rjust(max_channel_index_width + 1)} {str(channel).ljust(max_channel_name_width + 1)}')

                        channel_pos_y += 1

                        if channel_pos_y >= max_y:
                            channel_pos_y = 0
                            channel_pos_x += max_channel_name_width + max_channel_index_width + 3


        debug_window.refresh()
        device_window.refresh()
        rx_channel_window.refresh()
        tx_channel_window.refresh()

        try:
            c = screen.getch()

            if c != -1:
                debug_window.erase()
                debug_window.addstr(0, 0, f'{c}')
                debug_window.addstr(1, 0, f'{curses.keyname(c).decode("utf-8")}')

                if c == 9:
                    if selected_window == Window.devices: 
                        selected_window = Window.channels
                        if not selected_channel:
                            selected_channel = 1

                    elif selected_window == Window.channels:
                        selected_window = Window.devices

                if c == curses.KEY_UP or chr(c) == 'k':
                    if selected_window == Window.devices: 
                        current_device_index = list(devices.keys()).index(selected_device)
                        previous_index = current_device_index - 1

                        if previous_index >= 0:
                            selected_device = list(devices.keys())[previous_index]

                            if selected_window == Window.devices: 
                                selected_channel = None

                            device_window.erase()
                    elif selected_window == Window.channels:
                        current_channel_index = list(channels.keys()).index(selected_channel)
                        previous_index = current_channel_index - 1

                        if previous_index >= 0:
                            selected_channel = list(channels.keys())[previous_index]
                            rx_channel_window.erase()

                if c == curses.KEY_DOWN or chr(c) == 'j':
                    if selected_window == Window.devices: 
                        current_device_index = list(devices.keys()).index(selected_device)
                        next_index = current_device_index + 1

                        if next_index < len(list(devices.keys())):
                            selected_device = list(devices.keys())[next_index]

                            if selected_window == Window.devices: 
                                selected_channel = None

                            device_window.erase()
                    elif selected_window == Window.channels:
                        current_channel_index = list(channels.keys()).index(selected_channel)
                        next_index = current_channel_index + 1

                        if next_index < len(list(channels.keys())):
                            selected_channel = list(channels.keys())[next_index]

                debug_window.refresh()
        except Exception as e:
            file = open('debug.log', 'a')
            file.write(f"error: e\n")
            file.close()
            pass

        #  window.addstr(4, 0, f'{screen.getmaxyx()}')
        curses.napms(5)


if __name__ == '__main__':
    screen = curses.initscr()

    curses.cbreak()
    curses.curs_set(0)
    curses.noecho()
    curses.start_color()
    curses.use_default_colors()

    screen.clear()
    screen.nodelay(True)
    screen.keypad(True)

    try:
        main()
    except KeyboardInterrupt:
        screen.clear()
        curses.endwin()
    try:
        os.system('clear')
        sys.exit(0)
    except SystemExit:
        os._exit(0)
