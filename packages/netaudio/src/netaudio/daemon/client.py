from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

from netaudio.common.app_config import DEFAULT_DAEMON_PORT
from netaudio.daemon import engine
from netaudio.device_record import Device

logger = logging.getLogger("netaudio")

DAEMON_HOST = "127.0.0.1"
EVENT_STREAM_CLOSE_TIMEOUT = 1.0
EVENT_STREAM_EVENT_LIMIT = 32 * 1024 * 1024
EVENT_STREAM_LINE_LIMIT = EVENT_STREAM_EVENT_LIMIT + 1024


def daemon_port() -> int:
    from netaudio.common.app_config import settings as app_settings

    return engine.port() or app_settings.daemon_port or DEFAULT_DAEMON_PORT


async def get_diagnostics_from_daemon(device: str, *, reset: bool = False):
    if reset:
        return await _daemon_request("POST", "/diagnostics/reset", {"device": device})
    return await _daemon_request("GET", "/diagnostics/" + quote(device, safe=""))


class DaemonUnavailable(RuntimeError):
    def __init__(self, reason: object = None) -> None:
        super().__init__(str(reason) if reason else "the NetAudio daemon is not running")


class DaemonSilent(RuntimeError):
    def __init__(self, method: str, path: str, reason: object) -> None:
        super().__init__(f"the NetAudio daemon did not answer {method} {path}: {reason or 'no response'}")


class DaemonRefusal(RuntimeError):
    def __init__(self, status: int, data) -> None:
        self.status = status
        self.data = data
        details = [data.get(key) for key in ("error", "message", "readback_error")] if isinstance(data, dict) else []
        detail = next((detail for detail in details if detail), None)
        super().__init__(str(detail) if detail else f"the NetAudio daemon answered with status {status}")


async def _connect():
    try:
        return await asyncio.wait_for(asyncio.open_connection(DAEMON_HOST, daemon_port()), timeout=1.0)
    except (OSError, asyncio.TimeoutError):
        return None


async def _exchange(method: str, path: str, body, timeout: float, *, own_engine: bool = True) -> tuple[int, Any]:
    connection = await _connect()
    if connection is None and own_engine and engine.port() is None:
        try:
            await asyncio.to_thread(engine.start)
        except engine.EngineFailure as failure:
            raise DaemonUnavailable(failure) from None
        connection = await _connect()
    if connection is None:
        raise DaemonUnavailable()
    reader, writer = connection

    try:
        payload = json.dumps(body).encode() if body is not None else b""
        head = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: {DAEMON_HOST}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode()
        writer.write(head + payload)
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not status_line:
            raise DaemonSilent(method, path, "connection closed")
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
    except asyncio.TimeoutError:
        raise DaemonSilent(method, path, f"no answer within {timeout:g} seconds") from None
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, ValueError) as exception:
        raise DaemonSilent(method, path, exception) from None
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError as exception:
            logger.debug(f"Daemon connection close error: {exception}")


async def ask_daemon(method: str, path: str, body=None, *, timeout: float = 90.0) -> tuple[int, Any]:
    return await _exchange(method, path, body, timeout)


async def call_daemon(method: str, path: str, body=None, *, timeout: float = 90.0) -> Any:
    status, data = await _exchange(method, path, body, timeout)
    if status not in (200, 202):
        raise DaemonRefusal(status, data)
    return data


async def _daemon_request(method: str, path: str, body=None, timeout: float = 5.0, *, own_engine: bool = True):
    try:
        return await _exchange(method, path, body, timeout, own_engine=own_engine)
    except (DaemonUnavailable, DaemonSilent) as exception:
        logger.debug(f"Daemon request {method} {path} failed: {exception}")
        return None, None


async def get_devices_from_daemon(context: str | None = None) -> dict[str, Device] | None:
    path = f"/devices?context={quote(context, safe='')}" if context is not None else "/devices"
    status, data = await _daemon_request("GET", path)
    if status != 200 or data is None:
        return None

    devices = {}
    for server_name, record in data.items():
        devices[server_name] = Device(record)
    logger.info(f"Daemon: {len(devices)} devices")
    return devices


async def forget_devices_on_daemon(
    *,
    device_name: str | None = None,
    emulated: bool = False,
    offline: bool = False,
) -> tuple[int | None, dict | None]:
    if device_name is not None:
        path = f"/devices/{quote(device_name, safe='')}"
    else:
        selections = [name for name, selected in (("emulated", emulated), ("offline", offline)) if selected]
        path = f"/devices?selection={','.join(selections)}"
    return await _daemon_request("DELETE", path)


async def get_device_summaries_from_daemon() -> dict | None:
    status, data = await _daemon_request("GET", "/devices")
    if status != 200:
        return None
    return data


async def get_shure_devices_from_daemon() -> dict | None:
    status, data = await _daemon_request("GET", "/shure/devices")
    if status != 200:
        return None
    return data


async def shutdown_daemon() -> bool:
    status, data = await _daemon_request("POST", "/shutdown", own_engine=False)
    return status == 200 and bool(data and data.get("success"))


async def meter_snapshot_from_daemon(server_name: str) -> dict | None:
    status, data = await _daemon_request("GET", f"/metering/snapshot/{quote(server_name, safe='')}", timeout=6.0)
    if status != 200 or data is None:
        if data and data.get("error"):
            logger.debug(f"Daemon metering error: {data['error']}")
        return None
    return data


async def meter_cache_from_daemon() -> dict[str, dict] | None:
    status, data = await _daemon_request("GET", "/metering/cache")
    if status != 200 or not isinstance(data, dict):
        return None
    return data


async def meter_start_on_daemon(server_name: str, client_id: str) -> bool:
    status, data = await _daemon_request(
        "POST",
        "/metering/start",
        body={"device": server_name, "client_id": client_id},
    )
    return status == 200 and bool(data and data.get("success"))


async def meter_stop_on_daemon(server_name: str, client_id: str) -> bool:
    status, data = await _daemon_request(
        "POST",
        "/metering/stop",
        body={"device": server_name, "client_id": client_id},
    )
    return status == 200 and bool(data and data.get("success"))


async def meter_status_from_daemon() -> dict | None:
    status, data = await _daemon_request("GET", "/metering/status")
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
                    DAEMON_HOST,
                    daemon_port(),
                    limit=EVENT_STREAM_LINE_LIMIT,
                ),
                timeout=connect_timeout,
            )
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as exception:
            logger.debug(f"Daemon event connection failed: {exception}")
            return

        request = (
            "GET /events HTTP/1.1\r\n"
            f"Host: {DAEMON_HOST}\r\n"
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
                "Daemon event stream rejected: HTTP %s, content-type %r",
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
                        logger.debug(f"Ignoring malformed daemon SSE JSON: {exception}")
                    else:
                        if isinstance(event, dict):
                            yield event
                        else:
                            logger.debug("Ignoring non-object daemon SSE event")
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
                logger.debug(f"Ignoring malformed daemon SSE UTF-8: {exception}")
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
        logger.debug(f"Daemon event stream failed: {exception}")
    finally:
        if writer is not None:
            writer.close()
            if not aborted:
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=EVENT_STREAM_CLOSE_TIMEOUT)
                except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exception:
                    logger.debug(f"Daemon event connection close error: {exception}")
