import asyncio
from unittest.mock import AsyncMock

import pytest

import netaudio.daemon.client as daemon_client


@pytest.mark.asyncio
async def test_meter_cache_from_daemon_uses_cache_only_endpoint(monkeypatch):
    request = AsyncMock(
        return_value=(
            200,
            {"dev1": {"metering_source": "signal_presence", "tx": {"1": 0x7B}, "rx": {}}},
        )
    )
    monkeypatch.setattr(daemon_client, "_relay_request", request)

    result = await daemon_client.meter_cache_from_daemon()

    assert result["dev1"]["metering_source"] == "signal_presence"
    request.assert_awaited_once_with("GET", "/metering/cache")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("function_name", "path", "response", "expected"),
    [
        ("meter_start_on_daemon", "/metering/start", (200, {"success": True}), True),
        ("meter_start_on_daemon", "/metering/start", (503, {"error": "unavailable"}), False),
        ("meter_stop_on_daemon", "/metering/stop", (200, {"success": True}), True),
        ("meter_stop_on_daemon", "/metering/stop", (200, {"success": False}), False),
    ],
)
async def test_meter_start_and_stop_report_success(monkeypatch, function_name, path, response, expected):
    request = AsyncMock(return_value=response)
    monkeypatch.setattr(daemon_client, "_relay_request", request)

    result = await getattr(daemon_client, function_name)("dev1", "tui-1")

    assert result is expected
    request.assert_awaited_once_with(
        "POST",
        path,
        body={"device": "dev1", "client_id": "tui-1"},
    )


@pytest.mark.asyncio
async def test_stream_daemon_events_handles_fragmentation_and_isolates_malformed_json(monkeypatch):
    peer_closed = asyncio.Event()

    async def handle(reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            assert request.startswith(b"GET /events HTTP/1.1\r\n")
            chunks = (
                b"HTTP/1.1 2",
                b"00 OK\r\ncontent-T",
                b"ype: text/event-stream; charset=utf-8\r\nX-Test: yes\r\n\r\n",
                b": heartbeat\r\n\r\n",
                b"data: {not-json}\r\n\r\n",
                b"data: \xff\r\n\r\n",
                b'data: {"event": "snapshot",\r\n',
                b'data: "metering": {"dev1": {"metering_source": "signal_presence"}}}\r\n\r\n',
            )
            for chunk in chunks:
                writer.write(chunk)
                await writer.drain()
                await asyncio.sleep(0)
            await reader.read()
        finally:
            peer_closed.set()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(daemon_client, "relay_port", lambda: port)
    stream = daemon_client.stream_daemon_events()

    try:
        event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        assert event == {
            "event": "snapshot",
            "metering": {"dev1": {"metering_source": "signal_presence"}},
        }
        await stream.aclose()
        await asyncio.wait_for(peer_closed.wait(), timeout=1.0)
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_stream_daemon_events_rejects_invalid_http_response_and_closes(monkeypatch):
    peer_closed = asyncio.Event()

    async def handle(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"NOT-HTTP\r\nContent-Type: text/event-stream\r\n\r\n")
            await writer.drain()
            await reader.read()
        finally:
            peer_closed.set()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(daemon_client, "relay_port", lambda: port)
    stream = daemon_client.stream_daemon_events()

    try:
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        await asyncio.wait_for(peer_closed.wait(), timeout=1.0)
    finally:
        await stream.aclose()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_stream_daemon_events_cancellation_closes_connection(monkeypatch):
    peer_closed = asyncio.Event()

    async def handle(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata: {"event": "ready"}\r\n\r\n')
            await writer.drain()
            await reader.read()
        finally:
            peer_closed.set()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(daemon_client, "relay_port", lambda: port)
    stream = daemon_client.stream_daemon_events()
    assert await asyncio.wait_for(stream.__anext__(), timeout=1.0) == {"event": "ready"}
    pending_event = asyncio.create_task(stream.__anext__())

    try:
        pending_event.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending_event
        await asyncio.wait_for(peer_closed.wait(), timeout=1.0)
    finally:
        await stream.aclose()
        server.close()
        await server.wait_closed()
