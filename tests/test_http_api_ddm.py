from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from tests.http_api_test_support import FakeWriter, make_device, make_http_server
from netaudio.dante.device import DanteDevice
from netaudio.ddm import GraphQLResult, TransportError


class FakeInventory:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self.client = SimpleNamespace(execute_async=AsyncMock(return_value=GraphQLResult(data={"me": None}, errors=())))
        self.refresh = AsyncMock(return_value=True)

    def domains(self):
        return [{"id": "d1", "name": "test", "devices": []}]

    def client_for_context(self, context_name=None):
        self.selected_context = context_name
        return self.client

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
    status, _ = await _post(
        server,
        "/ddm/graphql",
        b'{"query": "{ me { id } }", "context": "west-main"}',
    )
    assert status == 200 and inventory.selected_context == "west-main"
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
async def test_inventory_get_routes_accept_a_request_local_context_filter():
    class ContextInventory(FakeInventory):
        def domains(self):
            return [
                {"id": "east", "name": "Main", "devices": [], "ddm_context": "east-main"},
                {"id": "west", "name": "Main", "devices": [], "ddm_context": "west-main"},
            ]

        def serialize_devices(self, devices):
            records = super().serialize_devices(devices)
            records["ddm:001dc1fffe50692e:0"]["ddm_context"] = "east-main"
            records["ddm:west:device"] = {
                "ddm_context": "west-main",
                "ddm_device_id": "001dc1fffe50692e:0",
                "inventory_sources": ["ddm"],
                "name": "west-device",
                "server_name": "ddm:west:device",
            }
            return records

    server = make_http_server(devices={"dev1": make_device()})
    server.managed_inventory = ContextInventory()

    status, devices = await _get(server, "/devices?context=east-main")
    domain_status, domains = await _get(server, "/ddm/domains?context=west-main")
    ambiguous_status, ambiguous = await _get(server, "/devices/001dc1fffe50692e:0")
    selected_status, selected = await _get(server, "/devices/001dc1fffe50692e:0?context=east-main")

    assert status == 200 and set(devices) == {"dev1", "ddm:001dc1fffe50692e:0"}
    assert domain_status == 200 and [domain["id"] for domain in domains] == ["west"]
    assert ambiguous_status == 409 and "context-qualified" in ambiguous["error"]
    assert selected_status == 200 and selected["ddm_context"] == "east-main"


@pytest.mark.asyncio
async def test_daemon_device_records_keep_legacy_key_aliases_for_fleet_consumers():
    device = DanteDevice(server_name="dev1")
    device.name = "Device1"
    device.ipv4 = "192.168.1.50"
    device.sample_rate = 48000
    device.latency = 1.0
    server = make_http_server(devices={"dev1": device})
    status, devices = await _get(server, "/devices")
    assert status == 200
    record = devices["dev1"]
    assert record["sample_rate_hz"] == 48000 and record["sample_rate"] == 48000
    assert record["latency_ms"] == 1.0 and record["latency"] == 1.0
    assert list(record) == sorted(record)
