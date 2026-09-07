import json

import pytest

from netaudio.daemon.http.api import _MeterUpdate, _SseClient
from tests.http_api_test_support import FakeWriter, make_http_server


def decode(entry):
    payload = entry.payload if isinstance(entry, _MeterUpdate) else entry
    return json.loads(payload.decode().removeprefix("data: "))


@pytest.mark.asyncio
async def test_meter_fanout_coalesces_slow_clients_independently():
    server = make_http_server()
    clients = [_SseClient(FakeWriter()) for _ in range(20)]
    server.sse_clients = {client.writer: client for client in clients}
    for level in range(200):
        await server._broadcast_sse(
            {"event": "meter_values", "metering_source": "detailed", "server_name": "device", "rx": {"1": level}}
        )
        fast = clients[0]
        entry = fast.queue.get_nowait()
        del fast.pending_meters[entry.key]
        assert decode(entry)["rx"]["1"] == level
    for client in clients[1:]:
        assert client.queue.qsize() == 1
        assert decode(client.queue.get_nowait())["rx"]["1"] == 199
        assert not client.closed.is_set()


@pytest.mark.asyncio
async def test_control_and_partial_updates_are_ordering_barriers():
    server = make_http_server()
    client = _SseClient(FakeWriter())
    server.sse_clients = {client.writer: client}
    detailed = {"event": "meter_values", "metering_source": "detailed", "server_name": "device"}
    events = [
        dict(detailed, rx={"1": 1}),
        {"event": "device_removed", "server_name": "device"},
        dict(detailed, rx={"1": 2}),
        {"event": "meter_values", "metering_source": "signal_presence", "server_name": "device", "rx": {"2": 3}},
        dict(detailed, rx={"1": 4}),
    ]
    for event in events:
        await server._broadcast_sse(event)
    assert [decode(client.queue.get_nowait()) for _ in events] == events
