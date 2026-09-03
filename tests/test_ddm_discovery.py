from __future__ import annotations

import asyncio

import pytest

from netaudio.ddm.discovery import (
    DDM_CONTROLLER_SERVICE,
    DDM_DEVICE_SERVICE,
    discover_ddm_servers,
)


@pytest.mark.asyncio
async def test_discovery_correlates_controller_and_device_services(monkeypatch):
    import zeroconf.asyncio as zeroconf_asyncio
    from zeroconf import ServiceStateChange

    records = {
        DDM_CONTROLLER_SERVICE: {
            "name": f"default.{DDM_CONTROLLER_SERVICE}",
            "server": "ddm.local.",
            "port": 8443,
            "addresses": ["192.168.1.217"],
            "properties": {b"version": b"1.9"},
        },
        DDM_DEVICE_SERVICE: {
            "name": f"default.{DDM_DEVICE_SERVICE}",
            "server": "ddm.local.",
            "port": 8000,
            "addresses": ["192.168.1.217"],
            "properties": {},
        },
    }

    class FakeAsyncZeroconf:
        instances = []

        def __init__(self, **options):
            self.options = options
            self.zeroconf = object()
            self.closed = False
            self.instances.append(self)

        async def async_close(self):
            self.closed = True

    class FakeAsyncServiceInfo:
        def __init__(self, service_type, name):
            record = records[service_type]
            assert name == record["name"]
            self.type = service_type
            self.name = name
            self.server = record["server"]
            self.port = record["port"]
            self.properties = record["properties"]
            self._addresses = record["addresses"]

        async def async_request(self, zeroconf, timeout):
            assert timeout > 0
            return True

        def parsed_addresses(self):
            return list(self._addresses)

    class FakeAsyncServiceBrowser:
        instances = []

        def __init__(self, zeroconf, service_types, handlers):
            self.cancelled = False
            self.instances.append(self)
            assert service_types == [DDM_CONTROLLER_SERVICE, DDM_DEVICE_SERVICE]
            loop = asyncio.get_running_loop()
            for service_type, record in records.items():
                loop.call_soon(handlers[0], zeroconf, service_type, record["name"], ServiceStateChange.Added)

        async def async_cancel(self):
            self.cancelled = True

    monkeypatch.setattr(zeroconf_asyncio, "AsyncZeroconf", FakeAsyncZeroconf)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceBrowser", FakeAsyncServiceBrowser)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceInfo", FakeAsyncServiceInfo)

    servers = await discover_ddm_servers(timeout=0.01, interfaces=["192.168.1.62"])

    assert len(servers) == 1
    server = servers[0]
    assert server.server_name == "ddm.local."
    assert server.ipv4_addresses == ("192.168.1.217",)
    assert server.controller_service.instance_name == "default"
    assert server.controller_service.port == 8443
    assert server.controller_service.properties == (("version", "1.9"),)
    assert server.device_service.instance_name == "default"
    assert server.device_service.port == 8000
    assert server.to_json() == {
        "server_name": "ddm.local.",
        "ipv4_addresses": ["192.168.1.217"],
        "controller_service": {
            "instance_name": "default",
            "service_type": DDM_CONTROLLER_SERVICE,
            "port": 8443,
            "properties": {"version": "1.9"},
        },
        "device_service": {
            "instance_name": "default",
            "service_type": DDM_DEVICE_SERVICE,
            "port": 8000,
            "properties": {},
        },
    }
    assert FakeAsyncZeroconf.instances[0].closed is True
    assert FakeAsyncServiceBrowser.instances[0].cancelled is True


@pytest.mark.asyncio
async def test_discovery_retains_a_controller_only_advertisement(monkeypatch):
    import zeroconf.asyncio as zeroconf_asyncio
    from zeroconf import ServiceStateChange

    class FakeAsyncZeroconf:
        def __init__(self, **options):
            self.zeroconf = object()

        async def async_close(self):
            return None

    class FakeAsyncServiceInfo:
        def __init__(self, service_type, name):
            self.type = service_type
            self.name = name
            self.server = "manager.example.local."
            self.port = 9443
            self.properties = {}

        async def async_request(self, zeroconf, timeout):
            return True

        def parsed_addresses(self):
            return ["192.0.2.10"]

    class FakeAsyncServiceBrowser:
        def __init__(self, zeroconf, service_types, handlers):
            asyncio.get_running_loop().call_soon(
                handlers[0],
                zeroconf,
                DDM_CONTROLLER_SERVICE,
                f"primary.{DDM_CONTROLLER_SERVICE}",
                ServiceStateChange.Added,
            )

        async def async_cancel(self):
            return None

    monkeypatch.setattr(zeroconf_asyncio, "AsyncZeroconf", FakeAsyncZeroconf)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceBrowser", FakeAsyncServiceBrowser)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceInfo", FakeAsyncServiceInfo)

    [server] = await discover_ddm_servers(timeout=0.01)

    assert server.controller_service.port == 9443
    assert server.device_service is None


@pytest.mark.asyncio
async def test_discovery_ignores_unresolved_or_addressless_services(monkeypatch):
    import zeroconf.asyncio as zeroconf_asyncio
    from zeroconf import ServiceStateChange

    class FakeAsyncZeroconf:
        def __init__(self, **options):
            self.zeroconf = object()

        async def async_close(self):
            return None

    class FakeAsyncServiceInfo:
        def __init__(self, service_type, name):
            self.type = service_type
            self.name = name
            self.server = "ddm.local."
            self.port = 8443
            self.properties = {}

        async def async_request(self, zeroconf, timeout):
            return "unresolved" not in self.name

        def parsed_addresses(self):
            return []

    class FakeAsyncServiceBrowser:
        def __init__(self, zeroconf, service_types, handlers):
            loop = asyncio.get_running_loop()
            loop.call_soon(
                handlers[0],
                zeroconf,
                DDM_CONTROLLER_SERVICE,
                f"unresolved.{DDM_CONTROLLER_SERVICE}",
                ServiceStateChange.Added,
            )
            loop.call_soon(
                handlers[0],
                zeroconf,
                DDM_CONTROLLER_SERVICE,
                f"addressless.{DDM_CONTROLLER_SERVICE}",
                ServiceStateChange.Added,
            )

        async def async_cancel(self):
            return None

    monkeypatch.setattr(zeroconf_asyncio, "AsyncZeroconf", FakeAsyncZeroconf)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceBrowser", FakeAsyncServiceBrowser)
    monkeypatch.setattr(zeroconf_asyncio, "AsyncServiceInfo", FakeAsyncServiceInfo)

    assert await discover_ddm_servers(timeout=0.01) == ()
