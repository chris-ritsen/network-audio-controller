import asyncio
import ipaddress
import logging
import socket
import struct

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.const import (
    MULTICAST_GROUP_CONTROL_MONITORING,
)

logger = logging.getLogger("netaudio")


def parse_metering_levels(data: bytes, tx_count: int, rx_count: int) -> dict:
    levels = {"tx": {}, "rx": {}}

    if not tx_count and not rx_count:
        return levels

    total = tx_count + rx_count
    if len(data) < total:
        return levels

    offset = len(data) - total
    if offset >= 27 and data[25] == tx_count and data[26] == rx_count:
        offset = 27
    tx_bytes = data[offset : offset + tx_count]
    rx_bytes = data[offset + tx_count : offset + tx_count + rx_count]

    for index in range(min(tx_count, len(tx_bytes))):
        levels["tx"][index + 1] = tx_bytes[index]

    for index in range(min(rx_count, len(rx_bytes))):
        levels["rx"][index + 1] = rx_bytes[index]

    return levels


def _get_local_ip() -> ipaddress.IPv4Address:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("224.0.0.231", 1))
        local_ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ipaddress.IPv4Address(local_ip)


async def meter_device(device, application, timeout: float = 3.0) -> dict:
    device_ip = str(device.ipv4)
    device_name = device.name or device.server_name
    host_ip = _get_local_ip()
    host_mac = application.cmc._host_mac
    metering_port = app_settings.metering_port

    tx_count = device.tx_count_raw or device.tx_count or 0
    rx_count = device.rx_count_raw or device.rx_count or 0

    if not tx_count and not rx_count:
        raise RuntimeError(f"No channel counts for {device_name}")

    received = asyncio.Event()
    result = {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", metering_port))

    mreq = struct.pack(
        "4s4s",
        socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
        socket.inet_aton("0.0.0.0"),
    )
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    loop = asyncio.get_running_loop()

    class MeteringProtocol(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            src_ip = addr[0]
            if src_ip == device_ip:
                result.update(parse_metering_levels(data, tx_count, rx_count))
                received.set()

    transport, _ = await loop.create_datagram_endpoint(
        MeteringProtocol,
        sock=sock,
    )

    try:
        logger.debug(f"Requesting metering from {device_name} ({device_ip})")
        application.cmc.start_metering(
            device_ip, device_name, host_ip, host_mac, metering_port,
        )

        try:
            await asyncio.wait_for(received.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"No metering response from {device_name} within {timeout}s"
            )
    finally:
        logger.debug(f"Stopping metering for {device_name}")
        application.cmc.stop_metering(
            device_ip, device_name, host_ip, host_mac, metering_port,
        )
        transport.close()

    return result
