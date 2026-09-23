import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from netaudio import core
from netaudio.daemon.managed_signals import ManagedSignalReceiver
from tests.test_ddm_controller import _device_announcement, _session_description


def receiver():
    device = SimpleNamespace(
        server_name="input",
        online=True,
        requires_managed_control=True,
        ddm_server_profile="server",
        ddm_context="context",
        ddm_domain_id="domain",
        ddm_device_id="001dc1fffe50692e:0",
    )
    application = SimpleNamespace(devices={"input": device})
    metering = SimpleNamespace(record_signal_presence=Mock())
    service = ManagedSignalReceiver(application, metering)
    key = ("server", "context", "domain")
    service.targets[key] = {"001dc1fffe50692e": "input"}
    return service, device, key


def test_successive_avio_publications_are_forwarded_with_exact_identity():
    fixture = json.loads((Path(__file__).parent / "fixtures/managed_signal_presence.json").read_text())
    publication = core.parse_response(
        "dapi_signal_presence_publication", bytes.fromhex(fixture["frames"][0]["frame_hex"])
    )
    service, device, key = receiver()
    source = ("192.0.2.20", 443)
    service.accept(key, publication, source)
    service.accept(key, publication, source)
    assert service.metering.record_signal_presence.call_count == 2
    service.metering.record_signal_presence.assert_called_with(publication["records"][0], source, server_name="input")
    device.ddm_domain_id = "different"
    service.accept(key, publication, source)
    assert service.metering.record_signal_presence.call_count == 2
    device.ddm_domain_id = "domain"
    device.online = False
    service.accept(key, publication, source)
    assert service.metering.record_signal_presence.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["00" * 16, "11" * 16])
async def test_signal_stream_uses_native_session_and_validates_domain(monkeypatch, domain):
    fixture = json.loads((Path(__file__).parent / "fixtures/managed_signal_presence.json").read_text())
    publication_frame = bytes.fromhex(fixture["frames"][0]["frame_hex"])
    service, device, previous_key = receiver()
    device.ddm_domain_id = domain
    key = ("server", "context", domain)
    service.targets = {key: service.targets[previous_key]}
    transport = SimpleNamespace(server="ddm.example", _credential=lambda: "x" * 43)
    service.application.managed_transport = lambda device: transport
    monkeypatch.setattr(
        "netaudio.daemon.managed_signals.ControllerAPIClient",
        lambda *args, **kwargs: SimpleNamespace(
            server="ddm.example",
            ssl_context=None,
            endpoints=lambda: {"service_port": 8001, "device_port": 8000, "graphql_url": "http://ddm.example/graphql"},
        ),
    )
    reader = asyncio.StreamReader()
    reader.feed_data(_device_announcement(0) + _session_description() + publication_frame * 2)
    reader.feed_eof()
    sent = []

    async def drain():
        pass

    writer = SimpleNamespace(
        write=sent.append,
        drain=drain,
        close=Mock(),
        wait_closed=drain,
        get_extra_info=lambda name: ("127.0.0.1", 12345),
    )

    async def connect(*args, **kwargs):
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", connect)
    await service._run(key, device)

    assert service.metering.record_signal_presence.call_count == (2 if domain == "00" * 16 else 0)
    assert sent[:2] == [core.build_dapi_session_open(), core.build_dapi_authentication("x" * 43)]
    assert writer.close.called


@pytest.mark.asyncio
async def test_signal_tasks_are_cancelled_when_context_disappears_and_on_stop():
    service, device, _ = receiver()
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def run(key, target):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    service._run = run
    service.reconcile()
    await started.wait()
    service.reconcile()
    assert len(service.tasks) == 1
    device.online = False
    service.reconcile()
    await stopped.wait()
    assert not service.tasks
    device.online = True
    service.reconcile()
    await service.stop()
    assert not service.tasks
    assert not service.targets


@pytest.mark.asyncio
async def test_repeated_signal_failures_warn_once_per_retained_target(caplog):
    service, device, key = receiver()
    service.application.managed_transport = Mock(side_effect=OSError("unreachable"))
    with caplog.at_level(logging.DEBUG, logger="netaudio"):
        await service._run(key, device)
        await service._run(key, device)
        device.online = False
        service.reconcile()
        assert not service._failed_targets
        await service._run(key, device)
    records = [record for record in caplog.records if "Managed signal updates disconnected" in record.message]
    assert [record.levelno for record in records] == [logging.WARNING, logging.DEBUG, logging.WARNING]
