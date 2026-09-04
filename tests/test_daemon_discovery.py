from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from zeroconf import ServiceStateChange

from netaudio.daemon import discovery
from netaudio.daemon.discovery import DanteDiscoveryMixin
from netaudio.dante.const import SERVICE_ARC, SERVICE_CMC, SERVICE_VIDEO
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import EventType


class _DiscoveryHarness(DanteDiscoveryMixin):
    def __init__(self, devices=None):
        devices = {} if devices is None else devices
        self.application = SimpleNamespace(
            devices=devices,
            cmc=SimpleNamespace(register_device=AsyncMock()),
            dispatcher=SimpleNamespace(emit_nowait=MagicMock()),
            get_arc_port=self._arc_port,
            send_dante_model_request=AsyncMock(),
            send_make_model_request=AsyncMock(),
            _send_conmon_query_for_device=AsyncMock(),
        )
        self.application.register_device = MagicMock(side_effect=self._register_device)
        self.state = SimpleNamespace(
            retry_conmon_query=AsyncMock(),
            fetch_device_controls=AsyncMock(),
        )
        self.metering = SimpleNamespace(reactivate_device=MagicMock())
        self.cleared_candidates = []
        self.offline_candidates = []
        self.published = []
        self.spawned = []

    @property
    def devices(self):
        return self.application.devices

    def _register_device(self, server_name, new_device):
        existing = self.devices.get(server_name)
        if existing is None:
            new_device._app = self.application
            self.devices[server_name] = new_device
            return

        if not existing.online:
            existing.online = True
            existing.update_last_seen()
            if new_device.ipv4:
                existing.ipv4 = new_device.ipv4
            if new_device.services:
                existing.services = new_device.services
        self.devices[server_name] = existing

    @staticmethod
    def _arc_port(device):
        for service in device.services.values():
            if service.get("type") == SERVICE_ARC:
                return service.get("port")
        return None

    def clear_offline_candidate(self, server_name):
        self.cleared_candidates.append(server_name)

    def mark_device_offline(self, server_name):
        self.offline_candidates.append(server_name)

    async def _publish_device_to_redis(self, device):
        self.published.append(device)

    def _spawn_background(self, coroutine, *, name):
        self.spawned.append(name)
        coroutine.close()


def _install_service_info(
    monkeypatch,
    *,
    addresses=("192.0.2.10",),
    port=8800,
    properties=None,
    request_result=True,
):
    instances = []

    class _ServiceInfo:
        def __init__(self, service_type, name):
            self.service_type = service_type
            self.name = name
            self.port = port
            self.properties = {} if properties is None else properties
            self.requests = []
            instances.append(self)

        async def async_request(self, zeroconf, timeout):
            self.requests.append((zeroconf, timeout))
            return request_result

        def parsed_addresses(self):
            return list(addresses)

    monkeypatch.setattr(discovery, "AsyncServiceInfo", _ServiceInfo)
    return instances


def _zeroconf(*records):
    entries_with_name = MagicMock(return_value=list(records))
    return SimpleNamespace(cache=SimpleNamespace(entries_with_name=entries_with_name))


@pytest.mark.asyncio
async def test_removed_service_marks_only_matching_device_candidates(monkeypatch):
    instances = _install_service_info(monkeypatch)
    daemon = _DiscoveryHarness(
        {
            "rack.local.": DanteDevice(server_name="rack.local."),
            "other.local.": DanteDevice(server_name="other.local."),
        }
    )

    await daemon.handle_service_change(
        _zeroconf(),
        SERVICE_CMC,
        f"rack.{SERVICE_CMC}",
        ServiceStateChange.Removed,
    )

    assert daemon.offline_candidates == ["rack.local."]
    assert instances[0].requests == []
    assert daemon.cleared_candidates == []
    assert daemon.published == []


@pytest.mark.asyncio
async def test_new_cmc_service_registers_identity_metadata_and_queries(monkeypatch):
    properties = {
        b"": b"ignored",
        b"id": b"00:11:22:33:44:55",
        b"model": b"A32 Dante",
        b"mf": b"Ferrofish",
        b"server_vers": b"4.2.1",
        b"router_vers": b"4.2.0",
        b"rate": b"48000",
        b"latency_ns": b"1000000",
    }
    instances = _install_service_info(monkeypatch, properties=properties)
    notify = MagicMock()
    monkeypatch.setattr(discovery, "notify_systemd", notify)
    daemon = _DiscoveryHarness()
    zeroconf = _zeroconf(SimpleNamespace(server="rack.local."))
    instance_name = f"rack.{SERVICE_CMC}"

    await daemon.handle_service_change(
        zeroconf,
        SERVICE_CMC,
        instance_name,
        ServiceStateChange.Added,
    )

    device = daemon.devices["rack.local."]
    assert instances[0].requests == [(zeroconf, 3000)]
    assert daemon.cleared_candidates == ["rack.local."]
    assert str(device.ipv4) == "192.0.2.10"
    assert device.mac_address == "00:11:22:33:44:55"
    assert device.model_id == "A32 Dante"
    assert device.manufacturer_mdns == "Ferrofish"
    assert device.manufacturer == "Ferrofish"
    assert device.software_version == "4.2.1"
    assert device.firmware_version == "4.2.0"
    assert device.sample_rate == 48000
    assert device.latency == 1.0
    assert device.services[instance_name] == {
        "ipv4": "192.0.2.10",
        "name": instance_name,
        "port": 8800,
        "properties": {
            "id": "00:11:22:33:44:55",
            "model": "A32 Dante",
            "mf": "Ferrofish",
            "server_vers": "4.2.1",
            "router_vers": "4.2.0",
            "rate": "48000",
            "latency_ns": "1000000",
        },
        "server_name": "rack.local.",
        "type": SERVICE_CMC,
    }
    daemon.application.register_device.assert_called_once()
    daemon.application.cmc.register_device.assert_awaited_once_with("192.0.2.10")
    assert daemon.application._send_conmon_query_for_device.call_args_list == [
        call(device, daemon.application.send_make_model_request),
        call(device, daemon.application.send_dante_model_request),
    ]
    daemon.state.retry_conmon_query.assert_called_once_with("rack.local.")
    assert daemon.spawned == ["retry-conmon:rack.local."]
    assert daemon.published == [device]
    notify.assert_called_once_with("STATUS=1 device(s) online")
    daemon.application.dispatcher.emit_nowait.assert_not_called()


@pytest.mark.asyncio
async def test_offline_device_reuses_authoritative_object_and_stale_state(monkeypatch):
    _install_service_info(monkeypatch, addresses=("192.0.2.20",))
    monkeypatch.setattr(discovery, "notify_systemd", MagicMock())
    device = DanteDevice(server_name="rack.local.")
    device.ipv4 = "192.0.2.19"
    device.online = False
    device.services = {"stale": {"type": "_other._udp.local.", "port": 1}}
    device.tx_channels = {1: object()}
    daemon = _DiscoveryHarness({"rack.local.": device})
    instance_name = f"rack.{SERVICE_CMC}"

    await daemon.handle_service_change(
        _zeroconf(SimpleNamespace(server="rack.local.")),
        SERVICE_CMC,
        instance_name,
        ServiceStateChange.Added,
    )

    assert daemon.devices["rack.local."] is device
    assert device.online is True
    assert str(device.ipv4) == "192.0.2.20"
    assert "stale" in device.services
    assert instance_name in device.services
    assert list(device.tx_channels) == [1]
    daemon.metering.reactivate_device.assert_called_once_with("rack.local.")
    daemon.application.cmc.register_device.assert_awaited_once_with("192.0.2.20")
    assert daemon.published == [device]


@pytest.mark.asyncio
async def test_existing_arc_service_emits_single_update_and_fetches_controls(monkeypatch):
    _install_service_info(
        monkeypatch,
        addresses=("192.0.2.31",),
        port=4440,
        properties={b"server_vers": b"must-not-apply"},
    )
    device = DanteDevice(server_name="rack.local.")
    device.ipv4 = "192.0.2.30"
    device.name = "Old Name"
    device.software_version = "keep-me"
    device.fetch_device_name = AsyncMock(return_value="New Name")
    daemon = _DiscoveryHarness({"rack.local.": device})

    await daemon.handle_service_change(
        _zeroconf(SimpleNamespace(server="rack.local.")),
        SERVICE_ARC,
        f"rack.{SERVICE_ARC}",
        ServiceStateChange.Updated,
    )

    daemon.application.register_device.assert_not_called()
    assert daemon.devices["rack.local."] is device
    assert str(device.ipv4) == "192.0.2.31"
    assert device.name == "New Name"
    assert device.software_version == "keep-me"
    device.fetch_device_name.assert_awaited_once()
    daemon.state.fetch_device_controls.assert_called_once_with("rack.local.")
    assert daemon.spawned == ["delayed-controls:rack.local."]
    daemon.application.dispatcher.emit_nowait.assert_called_once()
    event = daemon.application.dispatcher.emit_nowait.call_args.args[0]
    assert event.type == EventType.DEVICE_UPDATED
    assert event.device_name == "New Name"
    assert event.server_name == "rack.local."


@pytest.mark.asyncio
async def test_unresolved_first_server_record_does_not_adopt_later_identity(monkeypatch):
    instances = _install_service_info(monkeypatch)
    daemon = _DiscoveryHarness()
    zeroconf = _zeroconf(
        SimpleNamespace(server=None),
        SimpleNamespace(server="rack.local."),
    )

    await daemon.handle_service_change(
        zeroconf,
        SERVICE_CMC,
        f"rack.{SERVICE_CMC}",
        ServiceStateChange.Added,
    )

    assert instances[0].requests == [(zeroconf, 3000)]
    assert daemon.devices == {}
    daemon.application.register_device.assert_not_called()
    daemon.application.cmc.register_device.assert_not_awaited()
    assert daemon.cleared_candidates == []
    assert daemon.published == []


@pytest.mark.asyncio
async def test_services_with_different_srv_targets_join_the_same_logical_device(monkeypatch):
    _install_service_info(monkeypatch, addresses=("192.0.2.107",), port=8802)
    monkeypatch.setattr(discovery, "notify_systemd", MagicMock())
    daemon = _DiscoveryHarness()
    instance_name = "studio-media-b"

    await daemon.handle_service_change(
        _zeroconf(SimpleNamespace(server="www.local.")),
        SERVICE_CMC,
        f"{instance_name}.{SERVICE_CMC}",
        ServiceStateChange.Added,
    )
    device = daemon.devices[f"{instance_name}.local."]
    device.fetch_device_name = AsyncMock(return_value=None)

    _install_service_info(monkeypatch, addresses=("192.0.2.38",), port=4540)
    await daemon.handle_service_change(
        _zeroconf(SimpleNamespace(server="W.local.")),
        SERVICE_ARC,
        f"{instance_name}.{SERVICE_ARC}",
        ServiceStateChange.Added,
    )

    assert list(daemon.devices) == [f"{instance_name}.local."]
    assert str(device.ipv4) == "192.0.2.38"
    assert set(device.services) == {
        f"{instance_name}.{SERVICE_CMC}",
        f"{instance_name}.{SERVICE_ARC}",
    }
    daemon.application.cmc.register_device.assert_awaited_once_with("192.0.2.107")


@pytest.mark.asyncio
async def test_video_source_service_attaches_without_creating_a_device(monkeypatch):
    _install_service_info(monkeypatch, addresses=("192.0.2.38",), port=4555)
    device = DanteDevice(server_name="studio-media-b.local.")
    daemon = _DiscoveryHarness({"studio-media-b.local.": device})
    daemon.application._attach_media_services = MagicMock()
    service_name = f"01@studio-media-b.{SERVICE_VIDEO}"

    await daemon.handle_service_change(
        _zeroconf(SimpleNamespace(server="W.local.")),
        SERVICE_VIDEO,
        service_name,
        ServiceStateChange.Added,
    )

    assert list(daemon.devices) == ["studio-media-b.local."]
    assert daemon.application.media_services[service_name]["ipv4"] == "192.0.2.38"
    assert daemon.application.media_services[service_name]["port"] == 4555
    daemon.application._attach_media_services.assert_called_once_with(device)
    daemon.application.register_device.assert_not_called()
