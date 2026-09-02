from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from tests.http_api_test_support import FakeWriter, make_device, make_http_server
from netaudio.ddm import GraphQLResult, TransportError


class FakeInventory:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self.client = SimpleNamespace(execute_async=AsyncMock(return_value=GraphQLResult(data={"me": None}, errors=())))
        self.refresh = AsyncMock(return_value=True)

    def domains(self):
        return [{"id": "d1", "name": "test", "devices": []}]

    def serialize_devices(self, devices):
        merged = {
            server_name: {"name": device.name, "server_name": server_name, "inventory_sources": ["direct"]}
            for server_name, device in devices.items()
        }
        merged["ddm:001dc1fffe50692e:0"] = {
            "ddm_device_id": "001dc1fffe50692e:0",
            "inventory_sources": ["ddm"],
            "name": "avio-input-2",
            "server_name": "ddm:001dc1fffe50692e:0",
        }
        return merged

    def status(self):
        return {"enabled": self.enabled, "state": "ready"}


async def _get(server, path):
    writer = FakeWriter()
    await server._dispatch("GET", path, None, writer)
    return writer.response()


async def _post(server, path, body):
    writer = FakeWriter()
    await server._dispatch("POST", path, body, writer)
    return writer.response()


@pytest.mark.asyncio
async def test_ddm_routes_report_disabled_without_an_inventory():
    server = make_http_server(devices={"dev1": make_device()})
    assert await _get(server, "/ddm/status") == (200, {"enabled": False, "state": "disabled"})
    assert await _get(server, "/ddm/domains") == (200, [])
    status, body = await _post(server, "/ddm/refresh", b"")
    assert status == 409
    status, body = await _post(server, "/ddm/graphql", b'{"query": "{ me { id } }"}')
    assert status == 409


@pytest.mark.asyncio
async def test_ddm_routes_expose_status_domains_and_managed_devices():
    server = make_http_server(devices={"dev1": make_device()})
    server.managed_inventory = FakeInventory()
    assert await _get(server, "/ddm/status") == (200, {"enabled": True, "state": "ready"})
    assert await _get(server, "/ddm/domains") == (200, [{"id": "d1", "name": "test", "devices": []}])
    status, devices = await _get(server, "/ddm/devices")
    assert status == 200 and list(devices) == ["ddm:001dc1fffe50692e:0"]
    status, everything = await _get(server, "/devices")
    assert status == 200 and set(everything) == {"dev1", "ddm:001dc1fffe50692e:0"}
    status, one = await _get(server, "/devices/001dc1fffe50692e:0")
    assert status == 200 and one["name"] == "avio-input-2"


@pytest.mark.asyncio
async def test_ddm_graphql_proxy_validates_and_forwards():
    server = make_http_server(devices={})
    inventory = FakeInventory()
    server.managed_inventory = inventory
    status, body = await _post(server, "/ddm/graphql", b'{"query": ""}')
    assert status == 400
    status, body = await _post(server, "/ddm/graphql", b'{"query": "{ me { id } }", "variables": []}')
    assert status == 400
    status, body = await _post(
        server, "/ddm/graphql", b'{"query": "query Me { me { id } }", "variables": {"a": 1}, "operation_name": "Me"}'
    )
    assert status == 200 and body == {"data": {"me": None}, "errors": []}
    inventory.client.execute_async.assert_awaited_once_with("query Me { me { id } }", {"a": 1}, "Me")
    inventory.client.execute_async.side_effect = TransportError("Managed API transport failed: down")
    status, body = await _post(server, "/ddm/graphql", b'{"query": "{ me { id } }"}')
    assert status == 502 and "down" in body["error"]


@pytest.mark.asyncio
async def test_ddm_refresh_reports_the_inventory_status():
    server = make_http_server(devices={})
    inventory = FakeInventory()
    server.managed_inventory = inventory
    assert await _post(server, "/ddm/refresh", b"") == (200, {"enabled": True, "state": "ready"})
    inventory.refresh.return_value = False
    status, _ = await _post(server, "/ddm/refresh", b"")
    assert status == 502


@pytest.mark.asyncio
async def test_daemon_device_records_keep_legacy_key_aliases_for_fleet_consumers():
    device = make_device()
    device.sample_rate = 48000
    device.latency = 1.0
    server = make_http_server(devices={"dev1": device})
    status, devices = await _get(server, "/devices")
    assert status == 200
    record = devices["dev1"]
    assert record["sample_rate_hz"] == 48000 and record["sample_rate"] == 48000
    assert record["latency_ms"] == 1.0 and record["latency"] == 1.0
    assert list(record) == sorted(record)
