from __future__ import annotations

import asyncio
import csv
import io
import json as json_module
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any, Awaitable, Callable, Optional

import typer

from netaudio import DanteDevice
from netaudio.common.app_config import settings
from netaudio.daemon.client import get_devices_from_daemon
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.latency import milliseconds_to_microseconds

from netaudio._exit_codes import ExitCode
from netaudio.icons import icon


@dataclass(frozen=True)
class ReadbackResult:
    matched: bool
    observed: Any = None
    observed_available: bool = False
    error: Optional[Exception] = None


async def readback_after_notification(
    read: Callable[[], Awaitable[Any]],
    expected: Any,
) -> ReadbackResult:
    try:
        observed = await read()
    except Exception as exception:
        return ReadbackResult(matched=False, error=exception)
    return ReadbackResult(
        matched=observed == expected,
        observed=observed,
        observed_available=True,
    )


class CoreCommandSender:
    def __init__(self, observer=None, devices=None, packet_store=None, session_id=None):
        from netaudio import core

        self._core = core
        self._clients: dict[str, Any] = {}
        self._host_mac = core.host_mac()
        self._observer = observer
        self._devices = devices or {}
        self._packet_store = packet_store
        self._session_id = session_id
        self._dispatcher = None
        self._notifications = None

    async def __call__(
        self,
        packet: bytes,
        device_ip_address,
        port: int,
        *,
        expect_response: bool = True,
        repeat: int = 1,
        interval_ms: int = 0,
    ) -> bytes | None:
        address = str(device_ip_address)
        client = self._clients.get(address)
        if client is None:
            client = self._core.CoreClient(address)
            if self._host_mac:
                client.set_host_mac(self._host_mac)
            client.observer = self._observer
            self._clients[address] = client
        return await asyncio.to_thread(
            client.request,
            packet,
            port,
            expect_response,
            repeat,
            interval_ms,
        )

    async def _ensure_notifications(self):
        if self._notifications is not None:
            return self._notifications

        from netaudio.common.app_config import settings as app_settings
        from netaudio.dante.events import DanteEventDispatcher
        from netaudio.dante.services.notification import DanteNotificationService

        def device_lookup(device_ip_address):
            for device in self._devices.values():
                if device.ipv4 and str(device.ipv4) == device_ip_address:
                    return device
            return None

        dispatcher = DanteEventDispatcher()
        notifications = DanteNotificationService(
            dispatcher=dispatcher,
            device_lookup=device_lookup,
            packet_store=self._packet_store,
            interface_ip=app_settings.interface_ip,
            dissect=_get_state().dissect,
        )
        notifications.session_id = self._session_id
        await dispatcher.start()
        try:
            await notifications.start()
        except BaseException:
            await dispatcher.stop()
            raise
        self._dispatcher = dispatcher
        self._notifications = notifications
        return notifications

    async def send_and_wait_for_notification(
        self,
        packet: bytes,
        device_ip_address,
        port: int,
        notification_ids,
        *,
        notification_timeout: float = 2.0,
        **send_options,
    ) -> bytes | None:
        notifications = await self._ensure_notifications()
        waiter = notifications.register_notification_waiter(str(device_ip_address), notification_ids)
        try:
            response = await self(packet, device_ip_address, port, **send_options)
            try:
                await asyncio.wait_for(waiter.event.wait(), timeout=notification_timeout)
            except asyncio.TimeoutError:
                return response
            return response
        finally:
            notifications.unregister_notification_waiter(waiter)

    async def close(self) -> None:
        notifications = self._notifications
        dispatcher = self._dispatcher
        self._notifications = None
        self._dispatcher = None
        if notifications is not None:
            await notifications.stop()
        if dispatcher is not None:
            await dispatcher.stop()
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            client.close()


async def send_and_wait_for_notification(
    send,
    packet,
    device_ip_address,
    port,
    notification_ids,
    **send_options,
):
    notification_sender = getattr(send, "send_and_wait_for_notification", None)
    if notification_sender is not None:
        return await notification_sender(
            packet,
            device_ip_address,
            port,
            notification_ids,
            **send_options,
        )
    return await send(packet, device_ip_address, port, **send_options)


def ansi(code: str, text: str) -> str:
    if settings.no_color:
        return str(text)
    return f"\033[{code}m{text}\033[0m"


HEADER_ICONS = {
    "Name": "name",
    "IP Address": "ip",
    "IP": "ip",
    "MAC Address": "mac",
    "Clock MAC": "mac",
    "Model": "model",
    "TX": "tx",
    "RX": "rx",
    "Last Seen": "last_seen",
    "Server Name": "server",
    "Manufacturer": "manufacturer",
    "Product Version": "version",
    "Board": "board",
    "Firmware": "firmware",
    "Software": "software",
    "Sample Rate": "sample_rate",
    "Encoding": "encoding",
    "Bit Depth": "bit_depth",
    "Latency": "latency",
    "Flows": "flow",
    "Bluetooth": "bluetooth",
    "Status": "status",
    "Label": "label",
    "Summary": "summary",
    "Reported": "reported",
    "Updated": "updated",
    "Sessions": "session",
    "Tags": "tag",
    "Context": "context",
    "RX Channel": "rx",
    "RX Device": "device",
    "TX Channel": "tx",
    "TX Device": "device",
    "#": "number",
    "Friendly Name": "friendly_name",
    "Role": "role",
    "Grandmaster": "grandmaster",
    "Direction": "direction",
    "Channel": "channel",
    "Channel Name": "channel",
    "Level": "level",
    "Timestamp": "wall_time",
    "Online": "online",
    "Receiving": "receiving",
}


def _iconize_headers(headers: list[str]) -> list[str]:
    return [f"{icon(HEADER_ICONS[header])}{header}" if header in HEADER_ICONS else header for header in headers]


def _get_state():
    from netaudio.cli import state

    return state


async def _discover(packet_store=None, session_id=None) -> dict[str, DanteDevice]:
    devices = await get_devices_from_daemon()

    if devices is None:
        owns_store = False
        if packet_store is None:
            from netaudio._capture import open_capture_session

            packet_store, session_id = open_capture_session()
            owns_store = packet_store is not None
        application = DanteApplication(packet_store=packet_store, dissect=_get_state().dissect)
        if packet_store and session_id:
            application.capture_session_id = session_id
            for service in (application.settings, application.cmc, application.notifications):
                service.session_id = session_id
        await application.startup()
        try:
            devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
        finally:
            await application.shutdown()
            if owns_store:
                packet_store.close()

    return devices or {}


def discover() -> dict[str, DanteDevice]:
    return asyncio.run(_discover())


def _get_arc_port(device: DanteDevice) -> int:
    if device.services:
        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port", 4440)
    return 4440


def _resolve_one(devices: dict[str, DanteDevice]) -> tuple[str, DanteDevice]:
    if len(devices) == 0:
        typer.echo("Error: device not found.", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    if len(devices) > 1:
        names = ", ".join(d.name or sn for sn, d in devices.items())
        typer.echo(f"Error: multiple devices matched: {names}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return next(iter(devices.items()))


def _make_core_sender(observer=None, devices=None, packet_store=None, session_id=None) -> CoreCommandSender:
    return CoreCommandSender(
        observer=observer,
        devices=devices,
        packet_store=packet_store,
        session_id=session_id,
    )


def _capture_observer():
    state = _get_state()
    if not state.capture and not state.dissect:
        return None, None
    from netaudio._capture import make_observer, open_capture_session

    store, session_id = open_capture_session()
    observer = make_observer(store, session_id, state.dissect)
    if store and session_id:
        typer.echo(f"Capture: recording to session #{session_id}", err=True)
    return observer, store


@asynccontextmanager
async def _command_context():
    observer, store = _capture_observer()
    session_id = None
    if store:
        active = store.get_latest_session(active_only=True)
        session_id = active["id"] if active else None
    try:
        devices = await get_devices_from_daemon()
        if devices is None:
            devices = await _discover(packet_store=store, session_id=session_id)
            if observer is not None:
                for device in devices.values():
                    device.rx_channels = {}
                    device.tx_channels = {}
            await _populate_controls(devices, observer=observer)

        devices = devices or {}
        sender = _make_core_sender(
            observer=observer,
            devices=devices,
            packet_store=store,
            session_id=session_id,
        )
        try:
            yield devices, sender
        finally:
            await sender.close()
    finally:
        if observer is not None:
            observer.flush()
        if store:
            store.close()


async def _populate_controls(devices: dict[str, DanteDevice], observer=None) -> None:
    unpopulated = [
        device for device in devices.values() if not device.tx_channels and not device.rx_channels and device.ipv4
    ]

    if not unpopulated:
        return

    if observer is not None:
        from netaudio._capture import populate_instrumented

        await asyncio.gather(
            *(populate_instrumented(device, observer) for device in unpopulated),
            return_exceptions=True,
        )
        return

    await asyncio.gather(
        *(device.populate_from_core() for device in unpopulated),
        return_exceptions=True,
    )


def _normalize_mac(mac: str) -> str:
    raw = mac.replace(":", "").replace("-", "").replace(".", "").lower()
    if len(raw) == 16 and raw[6:10] == "fffe":
        raw = raw[:6] + raw[10:]
    elif len(raw) == 16 and raw.endswith("0000"):
        raw = raw[:12]
    return raw


def _strip_separators(mac: str) -> str:
    return mac.replace(":", "").replace("-", "").replace(".", "").lower()


def _mac_matches(device_mac: str, pattern: str) -> bool:
    raw_device = _strip_separators(device_mac)
    raw_pattern = _strip_separators(pattern)

    if raw_device == raw_pattern:
        return True

    return _normalize_mac(device_mac) == _normalize_mac(pattern)


def filter_devices(devices: dict[str, DanteDevice]) -> dict[str, DanteDevice]:
    state = _get_state()

    if not state.names and not state.hosts and not state.server_names and not state.macs:
        return devices

    filtered = {}

    for server_name, device in devices.items():
        if state.names and not any(fnmatch(device.name or "", pat) for pat in state.names):
            continue

        if state.hosts and not any(str(device.ipv4) == h for h in state.hosts):
            continue

        if state.server_names and not any(fnmatch(server_name, pat) for pat in state.server_names):
            continue

        if state.macs and not any(_mac_matches(device.mac_address or "", pat) for pat in state.macs):
            continue

        filtered[server_name] = device

    return filtered


def sort_devices(devices: dict[str, DanteDevice]) -> list[tuple[str, DanteDevice]]:
    state = _get_state()

    sort_keys = {
        "mac": lambda item: item[1].mac_address or "",
        "name": lambda item: item[1].name or "",
        "ip": lambda item: tuple(int(part) for part in str(item[1].ipv4).split(".")) if item[1].ipv4 else (0,),
        "model": lambda item: item[1].model_id or "",
        "server-name": lambda item: item[0],
    }

    return sorted(devices.items(), key=sort_keys[state.sort_field], reverse=state.sort_reverse)


def set_device_filter(device_arg: str) -> None:
    state = _get_state()
    state.names = [device_arg]


def parse_qualified_name(s: str) -> tuple[str, str]:
    if "@" not in s:
        typer.echo(f"Error: expected channel@device format, got: {s}", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    channel, device = s.rsplit("@", 1)
    return channel, device


def _format_text(headers: list[str], rows: list[list[str]]) -> str:
    all_rows = [headers] + [[str(value) for value in row] for row in rows]
    widths = [max(len(row[i]) for row in all_rows) for i in range(len(headers))]
    numeric = [all(row[i].isdigit() for row in all_rows[1:] if row[i]) for i in range(len(headers))]
    lines = []
    for row in all_rows:
        parts = [
            row[i].rjust(widths[i]) if numeric[i] and row is not all_rows[0] else row[i].ljust(widths[i])
            for i in range(len(row))
        ]
        lines.append("  ".join(parts).rstrip())
    return "\n".join(lines)


def _format_csv(headers: list[str], rows: list[list[str]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    return buffer.getvalue().rstrip("\n")


def _format_json(data: Any) -> str:
    return json_module.dumps(data, indent=2, default=str)


def _hex_encode(text: str, pad_to: int = 16) -> str:
    import binascii

    encoded = binascii.hexlify(text.encode()).decode().upper()
    return encoded.ljust(pad_to, "0")


def _device_to_preset_xml(device: DanteDevice) -> ET.Element:
    element = ET.Element("device")

    _sub_text(element, "name", device.name or "")
    _sub_text(element, "default_name", device.server_name.replace(".local.", "") if device.server_name else "")

    instance_id = ET.SubElement(element, "instance_id")
    mac = (device.mac_address or "").replace(":", "").upper()
    if mac:
        if len(mac) == 12:
            mac = mac[:6] + "FFFE" + mac[6:]
        _sub_text(instance_id, "device_id", mac)
    _sub_text(instance_id, "process_id", "0")

    if device.manufacturer:
        _sub_text(element, "manufacturer_id", _hex_encode(device.manufacturer))
        _sub_text(element, "manufacturer_name", device.manufacturer)

    dante_model = device.dante_model or device.model_id or ""
    model_id = device.model_id or ""

    if model_id:
        model_id_hex = _hex_encode(model_id)
        _sub_text(element, "model_id", model_id_hex)
        _sub_text(element, "model_name", dante_model or model_id)
        if device.product_version:
            _sub_text(element, "model_version", device.product_version)
        _sub_text(element, "device_type", model_id_hex)
        _sub_text(element, "device_type_string", model_id)

    _sub_text(element, "friendly_name", device.name or "")

    if device.preferred_leader is not None:
        ET.SubElement(element, "preferred_master", value=str(device.preferred_leader).lower())

    if device.sample_rate:
        _sub_text(element, "samplerate", str(device.sample_rate))

    if device.encoding:
        _sub_text(element, "encoding", str(device.encoding))

    latency = device.latency
    if latency:
        _sub_text(element, "unicast_latency", str(milliseconds_to_microseconds(latency)))

    if device.interfaces:
        for index, iface in enumerate(device.interfaces):
            iface_element = ET.SubElement(element, "interface", network=str(index))
            mode = iface.get("mode", "")
            if mode == "static":
                ip_element = ET.SubElement(iface_element, "ipv4_address", mode="static")
                _sub_text(ip_element, "ip_address", iface.get("ip_address", ""))
                _sub_text(ip_element, "subnet_mask", iface.get("netmask", ""))
                _sub_text(ip_element, "gateway", iface.get("gateway", ""))
                _sub_text(ip_element, "dns_server", iface.get("dns_server", ""))
            else:
                ET.SubElement(iface_element, "ipv4_address", mode="dynamic")

    for channel in sorted(device.tx_channels.values(), key=lambda channel: channel.number):
        tx_element = ET.SubElement(element, "txchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(tx_element, "label", channel.friendly_name or channel.name)

    for channel in sorted(device.rx_channels.values(), key=lambda channel: channel.number):
        rx_element = ET.SubElement(element, "rxchannel", danteId=str(channel.number), mediaType="audio")
        _sub_text(rx_element, "name", channel.friendly_name or channel.name)

        for subscription in device.subscriptions:
            if subscription.rx_channel_name == channel.name or subscription.rx_channel_name == channel.friendly_name:
                if subscription.tx_channel_name:
                    _sub_text(rx_element, "subscribed_channel", subscription.tx_channel_name)
                if subscription.tx_device_name:
                    _sub_text(rx_element, "subscribed_device", subscription.tx_device_name)
                break

    return element


def _sub_text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    child = ET.SubElement(parent, tag)
    child.text = text
    return child


def format_devices_xml(devices: dict[str, DanteDevice], preset_name: str = "netaudio") -> str:
    root = ET.Element("preset", version="2.1.0")
    _sub_text(root, "name", preset_name)
    _sub_text(root, "description", "Dante Controller preset")

    for server_name, device in sorted(devices.items(), key=lambda item: item[1].name or item[0]):
        root.append(_device_to_preset_xml(device))

    ET.indent(root, space="    ")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(root, encoding="unicode")


def _format_yaml(data: Any) -> str:
    try:
        import yaml
    except ImportError:
        typer.echo("Error: pyyaml not installed. Run: uv add pyyaml", err=True)
        raise typer.Exit(code=ExitCode.ERROR)

    return yaml.dump(data, default_flow_style=False, sort_keys=False).rstrip("\n")


def _format_table(headers: list[str], rows: list[list[str]], title: Optional[str] = None) -> str:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    state = _get_state()
    table = Table(title=title)

    for header in headers:
        table.add_column(header)

    for row in rows:
        table.add_row(*[Text.from_ansi(str(value)) for value in row])

    console = Console(no_color=state.no_color)
    with console.capture() as capture:
        console.print(table)
    return capture.get().rstrip("\n")


def output_table(
    headers: list[str],
    rows: list[list[str]],
    json_data: Any = None,
    title: Optional[str] = None,
    devices: Optional[dict[str, DanteDevice]] = None,
) -> None:
    from netaudio.cli import OutputFormat

    state = _get_state()
    output_format = state.output_format

    if json_data is None:
        json_data = [dict(zip(headers, row)) for row in rows]

    display_headers = _iconize_headers(headers)

    if output_format == OutputFormat.plain:
        typer.echo(_format_text(display_headers, rows))
    elif output_format == OutputFormat.table:
        typer.echo(_format_text(display_headers, rows))
    elif output_format == OutputFormat.pretty:
        typer.echo(_format_table(display_headers, rows, title=title))
    elif output_format == OutputFormat.json:
        typer.echo(_format_json(json_data))
    elif output_format == OutputFormat.xml:
        if devices:
            typer.echo(format_devices_xml(devices))
        else:
            typer.echo(_format_json(json_data))
    elif output_format == OutputFormat.csv:
        typer.echo(_format_csv(headers, rows))
    elif output_format == OutputFormat.yaml:
        typer.echo(_format_yaml(json_data))


def output_single(data: Any, device: Optional[DanteDevice] = None) -> None:
    from netaudio.cli import OutputFormat

    state = _get_state()
    output_format = state.output_format

    if output_format == OutputFormat.json:
        typer.echo(_format_json(data))
    elif output_format == OutputFormat.xml:
        if device:
            devices = {device.server_name or "device": device}
            typer.echo(format_devices_xml(devices))
        else:
            typer.echo(_format_json(data))
    elif output_format == OutputFormat.yaml:
        typer.echo(_format_yaml(data))
    else:
        typer.echo(data)


def find_device(devices: dict[str, DanteDevice], identifier: str) -> Optional[DanteDevice]:
    for server_name, device in devices.items():
        if device.name == identifier:
            return device
        if device.ipv4 and str(device.ipv4) == identifier:
            return device
        if server_name == identifier or server_name.startswith(identifier + "."):
            return device

    return None


def find_channel(device: DanteDevice, channel_id: str, channel_type: str):
    channels = device.rx_channels if channel_type == "rx" else device.tx_channels

    try:
        number = int(channel_id)
        for channel in channels.values():
            if channel.number == number:
                return channel
    except ValueError:
        pass

    for channel in channels.values():
        if channel.name == channel_id or channel.friendly_name == channel_id:
            return channel

    return None
