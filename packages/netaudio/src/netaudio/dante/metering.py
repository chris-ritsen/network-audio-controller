from __future__ import annotations

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


def metering_value_dbfs(value: int) -> float | None:
    if not 0 <= value <= 0xFF:
        raise ValueError("metering value must fit in one byte")
    if value == 0x01:
        return 0.0
    if 0x02 <= value <= 0xFD:
        return -((value - 1) / 2)
    return None


def classify_signal_presence(value: int) -> str:
    if not 0 <= value <= 0xFF:
        raise ValueError("metering value must fit in one byte")
    if value == 0x00:
        return "clipping"
    if value <= 0x7B:
        return "signal_present"
    if value <= 0xFD:
        return "below_threshold"
    if value == 0xFE:
        return "muted"
    return "unknown"


def parse_metering_levels(data: bytes) -> dict:
    from netaudio import core

    parsed = core.parse_response("metering", data)
    return {
        "tx": {index: level for index, level in enumerate(parsed["tx_levels"], start=1)},
        "rx": {index: level for index, level in enumerate(parsed["rx_levels"], start=1)},
    }


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
    host_mac = application.cmc.host_media_access_control_address
    metering_port = app_settings.metering_port

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
            source_address = addr
            source_ip = source_address[0]
            if source_ip == device_ip:
                from netaudio import core

                try:
                    result.update(parse_metering_levels(data))
                except core.NetaudioCoreError as error:
                    logger.warning(f"Ignoring malformed metering packet from {source_ip}: {error}")
                    return
                received.set()

    transport, _ = await loop.create_datagram_endpoint(
        MeteringProtocol,
        sock=sock,
    )

    try:
        logger.debug(f"Requesting metering from {device_name} ({device_ip})")
        application.cmc.start_metering(
            device_ip,
            device_name,
            host_ip,
            host_mac,
            metering_port,
        )

        try:
            await asyncio.wait_for(received.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"No metering response from {device_name} within {timeout}s")
    finally:
        logger.debug(f"Stopping metering for {device_name}")
        application.cmc.stop_metering(
            device_ip,
            device_name,
            host_ip,
            host_mac,
            metering_port,
        )
        transport.close()

    return result
