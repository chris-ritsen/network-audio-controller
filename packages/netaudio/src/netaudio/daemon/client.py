import asyncio
import json
import logging
from urllib.parse import quote

from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer

logger = logging.getLogger("netaudio")

DEFAULT_RELAY_PORT = 9000
RELAY_HOST = "127.0.0.1"


def relay_port() -> int:
    from netaudio.common.app_config import settings as app_settings
    return app_settings.relay_port or DEFAULT_RELAY_PORT


async def _relay_request(method: str, path: str, body=None, timeout: float = 5.0):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(RELAY_HOST, relay_port()),
            timeout=1.0,
        )
    except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
        return None, None

    try:
        payload = json.dumps(body).encode() if body is not None else b""
        head = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: {RELAY_HOST}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode()
        writer.write(head + payload)
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not status_line:
            return None, None
        status = int(status_line.decode().split(" ", 2)[1])

        content_length = 0
        while True:
            header_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if header_line in (b"\r\n", b"\n", b""):
                break
            key, _, value = header_line.decode().partition(":")
            if key.strip().lower() == "content-length":
                content_length = int(value.strip())

        data = b""
        if content_length:
            data = await asyncio.wait_for(reader.readexactly(content_length), timeout=timeout)

        return status, json.loads(data) if data else None
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionResetError,
            BrokenPipeError, ValueError, json.JSONDecodeError) as exception:
        logger.debug(f"Relay request {method} {path} failed: {exception}")
        return None, None
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError, OSError) as exception:
            logger.debug(f"Relay connection close error: {exception}")


def daemon_is_accessible() -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.5)
        return probe.connect_ex((RELAY_HOST, relay_port())) == 0


async def get_devices_from_daemon() -> dict[str, DanteDevice] | None:
    status, data = await _relay_request("GET", "/devices")
    if status != 200 or data is None:
        return None

    devices = {
        server_name: DanteDeviceSerializer.device_from_json(device_json)
        for server_name, device_json in data.items()
    }
    logger.info(f"Daemon: {len(devices)} devices")
    return devices


async def get_device_summaries_from_daemon() -> dict | None:
    status, data = await _relay_request("GET", "/devices")
    if status != 200:
        return None
    return data


async def get_shure_devices_from_daemon() -> dict | None:
    status, data = await _relay_request("GET", "/shure/devices")
    if status != 200:
        return None
    return data


async def report_unresponsive_device(server_name: str) -> None:
    await _relay_request("POST", "/report-unresponsive", body={"device": server_name})


async def shutdown_daemon() -> bool:
    status, data = await _relay_request("POST", "/shutdown")
    return status == 200 and bool(data and data.get("success"))


async def meter_snapshot_from_daemon(server_name: str) -> dict | None:
    status, data = await _relay_request(
        "GET", f"/metering/snapshot/{quote(server_name, safe='')}", timeout=6.0
    )
    if status != 200 or data is None:
        if data and data.get("error"):
            logger.debug(f"Daemon metering error: {data['error']}")
        return None
    return data


async def meter_start_on_daemon(server_name: str, client_id: str) -> None:
    await _relay_request(
        "POST", "/metering/start", body={"device": server_name, "client_id": client_id}
    )


async def meter_stop_on_daemon(server_name: str, client_id: str) -> None:
    await _relay_request(
        "POST", "/metering/stop", body={"device": server_name, "client_id": client_id}
    )


async def meter_status_from_daemon() -> dict | None:
    status, data = await _relay_request("GET", "/metering/status")
    if status != 200:
        return None
    return data
