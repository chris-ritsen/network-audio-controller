import asyncio
import os
from types import SimpleNamespace

import pytest

pytest.importorskip("dbus_fast", reason="dbus-fast is only a default dependency on Linux")

from dbus_fast.aio import MessageBus
from dbus_fast import BusType

from netaudio.daemon.dbus_service import DBusService
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("NETAUDIO_REAL_DBUS_TEST") != "1",
        reason="set NETAUDIO_REAL_DBUS_TEST=1 inside dbus-run-session",
    ),
]


async def test_real_session_bus_exports_properties_and_change_signal():
    device = DanteDevice(server_name="rack.local.")
    device.name = "Stage Rack"
    device.online = True
    device.latency = 0.15
    device.aes67_current = True
    daemon = SimpleNamespace(
        devices={device.server_name: device},
        shure=None,
        application=SimpleNamespace(dispatcher=DanteEventDispatcher()),
    )
    service = DBusService(daemon)
    client = None

    try:
        await service.start()
        client = await MessageBus(bus_type=BusType.SESSION).connect()

        manager_xml = await client.introspect("com.netaudio.Daemon", "/com/netaudio")
        manager_object = client.get_proxy_object("com.netaudio.Daemon", "/com/netaudio", manager_xml)
        manager_properties = manager_object.get_interface("org.freedesktop.DBus.Properties")
        manager_values = await manager_properties.call_get_all("com.netaudio.Manager")
        assert manager_values["DanteDeviceCount"].value == 1
        assert manager_values["DanteDevices"].value == ["rack.local."]

        device_path = service._dante_paths[device.server_name]
        device_xml = await client.introspect("com.netaudio.Daemon", device_path)
        device_object = client.get_proxy_object("com.netaudio.Daemon", device_path, device_xml)
        device_properties = device_object.get_interface("org.freedesktop.DBus.Properties")
        initial = await device_properties.call_get_all("com.netaudio.DanteDevice")
        assert initial["Name"].value == "Stage Rack"
        assert initial["Latency"].value == 0.15
        assert initial["Aes67Enabled"].value is True

        changed_event = asyncio.Event()
        changed_values = {}

        def properties_changed(interface_name, changed, _invalidated):
            if interface_name == "com.netaudio.DanteDevice":
                changed_values.update(changed)
                changed_event.set()

        device_properties.on_properties_changed(properties_changed)
        device.name = "Stage Rack Renamed"
        await service._on_dante_updated(DanteEvent(type=EventType.DEVICE_UPDATED, server_name=device.server_name))

        await asyncio.wait_for(changed_event.wait(), timeout=2)
        assert changed_values["Name"].value == "Stage Rack Renamed"
    finally:
        if client is not None:
            client.disconnect()
        await service.stop()
