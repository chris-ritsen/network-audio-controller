from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from urllib.parse import quote

from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer

logger = logging.getLogger("netaudio")

DEFAULT_RELAY_PORT = 9000
RELAY_HOST = "127.0.0.1"
EVENT_STREAM_CLOSE_TIMEOUT = 1.0
EVENT_STREAM_LINE_LIMIT = 1024 * 1024
EVENT_STREAM_EVENT_LIMIT = 4 * 1024 * 1024


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
    except (
        asyncio.TimeoutError,
        asyncio.IncompleteReadError,
        ConnectionResetError,
        BrokenPipeError,
        ValueError,
        json.JSONDecodeError,
    ) as exception:
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
        server_name: DanteDeviceSerializer.device_from_json(device_json) for server_name, device_json in data.items()
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


async def refresh_clock_on_daemon(device_name: str) -> dict | None:
    status, data = await _relay_request("POST", "/refresh-clock", {"device": device_name}, timeout=5.0)
    if status != 200 or not isinstance(data, dict):
        return None
    return data


async def meter_snapshot_from_daemon(server_name: str) -> dict | None:
    status, data = await _relay_request("GET", f"/metering/snapshot/{quote(server_name, safe='')}", timeout=6.0)
    if status != 200 or data is None:
        if data and data.get("error"):
            logger.debug(f"Daemon metering error: {data['error']}")
        return None
    return data


async def meter_cache_from_daemon() -> dict[str, dict] | None:
    status, data = await _relay_request("GET", "/metering/cache")
    if status != 200 or not isinstance(data, dict):
        return None
    return data


async def meter_start_on_daemon(server_name: str, client_id: str) -> bool:
    status, data = await _relay_request(
        "POST",
        "/metering/start",
        body={"device": server_name, "client_id": client_id},
    )
    return status == 200 and bool(data and data.get("success"))


async def meter_stop_on_daemon(server_name: str, client_id: str) -> bool:
    status, data = await _relay_request(
        "POST",
        "/metering/stop",
        body={"device": server_name, "client_id": client_id},
    )
    return status == 200 and bool(data and data.get("success"))


async def meter_status_from_daemon() -> dict | None:
    status, data = await _relay_request("GET", "/metering/status")
    if status != 200:
        return None
    return data


def _parse_http_status_line(line: bytes) -> int:
    text = line.decode("ascii").rstrip("\r\n")
    parts = text.split(" ", 2)
    if len(parts) < 2 or parts[0] not in ("HTTP/1.0", "HTTP/1.1"):
        raise ValueError("invalid HTTP status line")
    if len(parts[1]) != 3 or not parts[1].isdigit():
        raise ValueError("invalid HTTP status code")
    status = int(parts[1])
    if not 100 <= status <= 599:
        raise ValueError("invalid HTTP status code")
    return status


async def _read_http_headers(reader: asyncio.StreamReader, timeout: float) -> dict[str, str]:
    headers: dict[str, str] = {}
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not line:
            raise ValueError("incomplete HTTP response headers")
        if line in (b"\r\n", b"\n"):
            return headers
        text = line.decode("iso-8859-1").rstrip("\r\n")
        name, separator, value = text.partition(":")
        if not separator or not name.strip():
            raise ValueError("invalid HTTP response header")
        normalized_name = name.strip().lower()
        normalized_value = value.strip()
        if normalized_name in headers:
            headers[normalized_name] = f"{headers[normalized_name]}, {normalized_value}"
        else:
            headers[normalized_name] = normalized_value


async def stream_daemon_events(
    *,
    connect_timeout: float = 1.0,
    handshake_timeout: float = 5.0,
) -> AsyncIterator[dict]:
    writer: asyncio.StreamWriter | None = None
    aborted = False
    try:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    RELAY_HOST,
                    relay_port(),
                    limit=EVENT_STREAM_LINE_LIMIT,
                ),
                timeout=connect_timeout,
            )
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as exception:
            logger.debug(f"Relay event connection failed: {exception}")
            return

        request = (
            "GET /events HTTP/1.1\r\n"
            f"Host: {RELAY_HOST}\r\n"
            "Accept: text/event-stream\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode()
        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=handshake_timeout)

        status_line = await asyncio.wait_for(reader.readline(), timeout=handshake_timeout)
        status = _parse_http_status_line(status_line)
        headers = await _read_http_headers(reader, handshake_timeout)
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if status != 200 or content_type != "text/event-stream":
            logger.debug(
                "Relay event stream rejected: HTTP %s, content-type %r",
                status,
                headers.get("content-type"),
            )
            return

        data_lines: list[str] = []
        data_size = 0
        discard_event = False
        while True:
            line = await reader.readline()
            if not line:
                return
            line = line.rstrip(b"\n")
            if line.endswith(b"\r"):
                line = line[:-1]

            if not line:
                if data_lines and not discard_event:
                    try:
                        event = json.loads("\n".join(data_lines))
                    except json.JSONDecodeError as exception:
                        logger.debug(f"Ignoring malformed relay SSE JSON: {exception}")
                    else:
                        if isinstance(event, dict):
                            yield event
                        else:
                            logger.debug("Ignoring non-object relay SSE event")
                data_lines.clear()
                data_size = 0
                discard_event = False
                continue

            if line.startswith(b":"):
                continue

            field, separator, value = line.partition(b":")
            if separator and value.startswith(b" "):
                value = value[1:]
            if field != b"data":
                continue
            data_size += len(value)
            if data_size > EVENT_STREAM_EVENT_LIMIT:
                data_lines.clear()
                discard_event = True
                continue
            try:
                data_lines.append(value.decode("utf-8"))
            except UnicodeDecodeError as exception:
                logger.debug(f"Ignoring malformed relay SSE UTF-8: {exception}")
                discard_event = True
    except (asyncio.CancelledError, GeneratorExit):
        aborted = True
        raise
    except (
        asyncio.TimeoutError,
        asyncio.IncompleteReadError,
        ConnectionResetError,
        BrokenPipeError,
        OSError,
        UnicodeDecodeError,
        ValueError,
    ) as exception:
        logger.debug(f"Relay event stream failed: {exception}")
    finally:
        if writer is not None:
            writer.close()
            if not aborted:
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=EVENT_STREAM_CLOSE_TIMEOUT)
                except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exception:
                    logger.debug(f"Relay event connection close error: {exception}")
