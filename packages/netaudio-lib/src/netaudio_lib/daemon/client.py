import asyncio
import json
import logging
import pickle
import struct

from netaudio_lib.common.socket_path import daemon_is_accessible, open_daemon_connection
from netaudio_lib.daemon.protocol import (
    CMD_DEVICE_REQUEST,
    CMD_GET_DEVICES,
    CMD_METER_SNAPSHOT,
    CMD_METER_START,
    CMD_METER_STATUS,
    CMD_METER_STOP,
    CMD_REPORT_UNRESPONSIVE,
)
from netaudio_lib.dante.device import DanteDevice

logger = logging.getLogger("netaudio")


async def get_devices_from_daemon() -> dict[str, DanteDevice] | None:
    if not daemon_is_accessible():
        return None

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        writer.write(CMD_GET_DEVICES)
        await writer.drain()

        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=1.0)
        length = struct.unpack(">I", length_data)[0]

        data = await asyncio.wait_for(reader.readexactly(length), timeout=2.0)
        devices = pickle.loads(data)

        writer.close()
        await writer.wait_closed()

        logger.info(f"Daemon: {len(devices)} devices")
        return devices

    except FileNotFoundError:
        return None
    except ConnectionRefusedError:
        return None
    except asyncio.TimeoutError:
        logger.warning("Daemon connection timed out")
        return None
    except Exception as e:
        logger.debug(f"Daemon connection error: {e}")
        return None


async def report_unresponsive_device(server_name: str) -> None:
    if not daemon_is_accessible():
        return

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        name_bytes = server_name.encode("utf-8")
        writer.write(CMD_REPORT_UNRESPONSIVE)
        writer.write(struct.pack(">I", len(name_bytes)))
        writer.write(name_bytes)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    except Exception as e:
        logger.debug(f"Failed to report dead device: {e}")


async def meter_snapshot_from_daemon(server_name: str) -> dict | None:
    if not daemon_is_accessible():
        return None

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        name_bytes = server_name.encode("utf-8")
        writer.write(CMD_METER_SNAPSHOT)
        writer.write(struct.pack(">I", len(name_bytes)))
        writer.write(name_bytes)
        await writer.drain()

        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
        length = struct.unpack(">I", length_data)[0]

        data = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)
        result = json.loads(data)

        writer.close()
        await writer.wait_closed()

        if "error" in result:
            logger.debug(f"Daemon metering error: {result['error']}")
            return None

        return result

    except FileNotFoundError:
        return None
    except ConnectionRefusedError:
        return None
    except asyncio.TimeoutError:
        logger.debug("Daemon metering snapshot timed out")
        return None
    except Exception as e:
        logger.debug(f"Daemon metering error: {e}")
        return None


async def meter_start_on_daemon(server_name: str, client_id: str) -> None:
    if not daemon_is_accessible():
        return

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        name_bytes = server_name.encode("utf-8")
        id_bytes = client_id.encode("utf-8")
        writer.write(CMD_METER_START)
        writer.write(struct.pack(">I", len(name_bytes)))
        writer.write(name_bytes)
        writer.write(struct.pack(">I", len(id_bytes)))
        writer.write(id_bytes)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    except Exception as e:
        logger.debug(f"Failed to start daemon metering: {e}")


async def meter_stop_on_daemon(server_name: str, client_id: str) -> None:
    if not daemon_is_accessible():
        return

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        name_bytes = server_name.encode("utf-8")
        id_bytes = client_id.encode("utf-8")
        writer.write(CMD_METER_STOP)
        writer.write(struct.pack(">I", len(name_bytes)))
        writer.write(name_bytes)
        writer.write(struct.pack(">I", len(id_bytes)))
        writer.write(id_bytes)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    except Exception as e:
        logger.debug(f"Failed to stop daemon metering: {e}")


async def meter_status_from_daemon() -> dict | None:
    if not daemon_is_accessible():
        return None

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        writer.write(CMD_METER_STATUS)
        await writer.drain()

        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=2.0)
        length = struct.unpack(">I", length_data)[0]

        data = await asyncio.wait_for(reader.readexactly(length), timeout=2.0)
        result = json.loads(data)

        writer.close()
        await writer.wait_closed()

        return result

    except FileNotFoundError:
        return None
    except ConnectionRefusedError:
        return None
    except asyncio.TimeoutError:
        logger.debug("Daemon metering status timed out")
        return None
    except Exception as e:
        logger.debug(f"Daemon metering status error: {e}")
        return None


async def device_request_via_daemon(
    packet: bytes, device_ip: str, port: int
) -> bytes | None:
    if not daemon_is_accessible():
        return None

    try:
        reader, writer = await asyncio.wait_for(
            open_daemon_connection(),
            timeout=1.0,
        )

        ip_bytes = device_ip.encode("utf-8")
        writer.write(CMD_DEVICE_REQUEST)
        writer.write(struct.pack(">I", len(ip_bytes)))
        writer.write(ip_bytes)
        writer.write(struct.pack(">H", port))
        writer.write(struct.pack(">I", len(packet)))
        writer.write(packet)
        await writer.drain()

        status = await asyncio.wait_for(reader.readexactly(1), timeout=5.0)
        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
        length = struct.unpack(">I", length_data)[0]

        response = None
        if length > 0:
            response = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)

        writer.close()
        await writer.wait_closed()

        if status == b'\x01':
            return response
        return None

    except FileNotFoundError:
        return None
    except ConnectionRefusedError:
        return None
    except asyncio.TimeoutError:
        logger.debug("Daemon device request timed out")
        return None
    except Exception as e:
        logger.debug(f"Daemon device request error: {e}")
        return None
