import asyncio
import logging
import pickle
import struct

from netaudio_lib.common.socket_path import daemon_is_accessible, open_daemon_connection
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

        writer.write(b'\x00')
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
        writer.write(b'\x01')
        writer.write(struct.pack(">I", len(name_bytes)))
        writer.write(name_bytes)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    except Exception as e:
        logger.debug(f"Failed to report dead device: {e}")
