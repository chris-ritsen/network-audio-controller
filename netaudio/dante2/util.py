import codecs
from enum import Enum
import ipaddress
import logging
import socket
from typing import TypeAlias
import uuid

import psutil

ProtocolVersion: TypeAlias = tuple[int, int, int] # Python < 3.12
# ~ type ProtocolVersion = tuple[int, int, int] # Python 3.12+

LOGGER = logging.getLogger('netaudio.dante2')
NULL_HEXTET = b'\x00\x00'


def decode_integer(source: bytes, ptr: int, length: int = 2) -> int:
    return int.from_bytes(source[ptr : ptr + length], byteorder='big')

def encode_integer(integer: int, length: int = 2) -> bytes:
    return integer.to_bytes(length, byteorder='big')


def decode_string(source: bytes, ptr: int) -> str:
    if not ptr:
        return None
    substr = source[ptr:]
    return substr[:substr.index(b'\x00')].decode('ascii')

def encode_string(string: str) -> bytes:
    return string.encode('ascii') + b'\x00'


def decode_protocol_version(source: bytes) -> ProtocolVersion:
    protocol_version = source[0:2].hex()
    return (
        int(protocol_version[0], 16),
        int(protocol_version[1], 16),
        int(protocol_version[2:4], 16),
    )

def decode_protocol_version_from_mdns(source: bytes) -> ProtocolVersion:
    return tuple(
        int(x) for x in source.split(b".")
    )

def encode_protocol_version(protocol_version: ProtocolVersion) -> bytes:
    return codecs.decode(
        f"{protocol_version[0]}{protocol_version[1]}{protocol_version[2]:02x}",
        "hex"
    )


def decode_mac_address(source: bytes) -> str:
    return ':'.join(f"{b:02x}" for b in source)
    # ~ mac_addr = source.decode('ascii')
    # ~ return ':'.join([mac_addr[idx:idx+2] for idx in range(0, 12, 2)])

def encode_mac_address(mac_address: str) -> bytes:
    # ~ return ''.join(mac_address.split(':')).encode('ascii')
    return codecs.decode(''.join(mac_address.split(':')), 'hex')

def get_ip_addr_serving_ipv4(ipv4_address: ipaddress.IPv4Address) -> ipaddress.IPv4Address:
    for adapter in psutil.net_if_addrs().values():
        for nic_address in adapter:
            if nic_address.family == socket.AF_INET: # is IPv4
                network = ipaddress.IPv4Network((nic_address.address, nic_address.netmask), False)
                if ipv4_address in network:
                    return ipaddress.IPv4Address(nic_address.address)

    # TODO: return address of default adapter
    return None

def get_mac_addr_serving_ipv4(ipv4_address: ipaddress.IPv4Address) -> str:
    '''
    Given an IPv4 address, this function determines which local network adapter would be used to
    communicate with it and returns its MAC address.

    Note that we assume that all IPv4 addresses of a network adapter are listed (by `psutil`)
    before the MAC address is.
    '''
    # TODO: Name of this function sucks - think of a better one.
    for adapter in psutil.net_if_addrs().values():
        is_this_adapter = False
        for nic_address in adapter:

            if nic_address.family == socket.AF_INET: # is IPv4
                network = ipaddress.IPv4Network((nic_address.address, nic_address.netmask), False)
                if ipv4_address in network:
                    is_this_adapter = True

            elif nic_address.family == psutil.AF_LINK: # is MAC
                if is_this_adapter:
                    return nic_address.address

    # TODO: return the MAC address of the *default* interface, which the following is not
    # guaranteed to be (see documentation of `uuid.getnode()`)
    return ":".join(f"{b:02x}" for b in uuid.getnode().to_bytes(6, byteorder='big'))


class Encoding(Enum):
    PCM_16 = 16
    PCM_24 = 24
    PCM_32 = 32


class SampleRate(Enum):
    SR_44100 = 44100
    SR_48000 = 48000
    SR_88200 = 88200
    SR_96000 = 96000
    SR_176400 = 176400
    SR_192000 = 192000
