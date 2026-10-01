from __future__ import annotations

import asyncio
import json
import logging

from netaudio.daemon.http.mcp_protocol import META_SUBSCRIPTION_ID

logger = logging.getLogger("netaudio.mcp")

KEEPALIVE_SECONDS = 15.0
DRAIN_TIMEOUT_SECONDS = 5.0
NOTIFICATION_METHODS = {
    "toolsListChanged": "notifications/tools/list_changed",
    "promptsListChanged": "notifications/prompts/list_changed",
    "resourcesListChanged": "notifications/resources/list_changed",
}


class McpListenStream:
    def __init__(self, writer, subscription_id, accepted: dict[str, bool]):
        self.writer = writer
        self.subscription_id = subscription_id
        self.accepted = accepted
        self.pending: asyncio.Queue[str] = asyncio.Queue()

    def message(self, method: str, params: dict | None = None) -> bytes:
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": {**(params or {}), "_meta": {META_SUBSCRIPTION_ID: self.subscription_id}},
        }
        return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


class McpSubscriptions:
    def __init__(self):
        self._streams: set[McpListenStream] = set()

    @property
    def stream_count(self) -> int:
        return len(self._streams)

    def notify(self, notification: str) -> None:
        for stream in tuple(self._streams):
            if stream.accepted.get(notification):
                stream.pending.put_nowait(notification)

    async def serve(
        self, writer, reader, subscription_id, requested: dict, client_name: str = "unknown client"
    ) -> None:
        accepted = {notification: True for notification in NOTIFICATION_METHODS if requested.get(notification) is True}
        stream = McpListenStream(writer, subscription_id, accepted)
        writer.write(
            (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-store\r\n"
                "X-Accel-Buffering: no\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode()
        )
        writer.write(stream.message("notifications/subscriptions/acknowledged", {"notifications": accepted}))
        for notification in accepted:
            writer.write(stream.message(NOTIFICATION_METHODS[notification]))
        self._streams.add(stream)
        logger.info("MCP %s opened a listen stream for %s", client_name, ", ".join(accepted) or "nothing")
        disconnected = (
            asyncio.ensure_future(reader.read(1)) if reader is not None else asyncio.get_running_loop().create_future()
        )
        try:
            await asyncio.wait_for(writer.drain(), DRAIN_TIMEOUT_SECONDS)
            while True:
                pending = asyncio.ensure_future(stream.pending.get())
                done, _ = await asyncio.wait(
                    (disconnected, pending), timeout=KEEPALIVE_SECONDS, return_when=asyncio.FIRST_COMPLETED
                )
                if disconnected in done:
                    pending.cancel()
                    return
                if pending in done:
                    writer.write(stream.message(NOTIFICATION_METHODS[pending.result()]))
                else:
                    pending.cancel()
                    writer.write(b": keepalive\n\n")
                await asyncio.wait_for(writer.drain(), DRAIN_TIMEOUT_SECONDS)
        except (asyncio.TimeoutError, ConnectionError, OSError) as exception:
            logger.debug(f"MCP listen stream {subscription_id!r} ended: {exception}")
        finally:
            self._streams.discard(stream)
            disconnected.cancel()
            logger.info("MCP %s closed its listen stream", client_name)

    def close_all(self) -> None:
        for stream in tuple(self._streams):
            self._streams.discard(stream)
            try:
                stream.writer.close()
            except (ConnectionError, OSError) as exception:
                logger.debug(f"MCP listen stream close ended with {exception}")
