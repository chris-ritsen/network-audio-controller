from __future__ import annotations

import asyncio

from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType


def application_with_device(server_name: str, device_ip_address: str, name: str | None = None):
    application = DanteApplication()
    device = DanteDevice(server_name=server_name)
    device.name = name if name is not None else server_name.removesuffix(".local.")
    device.ipv4 = device_ip_address
    application.attach_devices({server_name: device})
    return application, device


async def deliver_status_events(application: DanteApplication) -> list[DanteEvent]:
    pending = application.dispatcher._pending_events
    delivered: list[DanteEvent] = []
    while pending:
        event = pending.popleft()
        delivered.append(event)
        if event.type is EventType.DEVICE_STATUS_RECEIVED:
            await application.state.on_device_status(event)
    return delivered


def receive_packets(application: DanteApplication, packets, source_address) -> list[DanteEvent]:
    for packet in packets:
        application.notifications._on_packet(packet, source_address)
    return asyncio.run(deliver_status_events(application))


def count_events(events: list[DanteEvent], event_type: EventType) -> int:
    return sum(event.type is event_type for event in events)


def status_events(events: list[DanteEvent]) -> list[DanteEvent]:
    return [event for event in events if event.type is EventType.DEVICE_STATUS_RECEIVED]
