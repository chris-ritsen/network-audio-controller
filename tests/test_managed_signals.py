import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from netaudio import core
from netaudio.daemon.managed_signals import ManagedSignalReceiver


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
