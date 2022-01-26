#!/bin/python3

import codecs

class Device(object):
    def __init__(self):
        self._error = None
        self._ipv4 = ''
        self._manufacturer = ''
        self._model = ''
        self._name = ''
        self._rx_channels = {}
        self._rx_count = 0
        self._socket = None
        self._tx_channels = {}
        self._tx_count = 0


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


def device_info():
    return'27ff000affff100300000'


def device_name():
    return '27ff000affff10020000'


def channel_count():
    return '27ff000affff10000000'


def receivers(page=0):
    page_hex = format(page, 'x')
    return f'27ff0010ffff30000000000100{page_hex}10000'


def transmitters(page=0):
    page_hex = format(page, 'x')
    return f'27ff0010ffff20000000000100{page_hex}10000'
