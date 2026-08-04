from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
import time
from dataclasses import dataclass, field
from urllib.parse import unquote

import ifaddr
from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio.common.app_config import settings as app_settings
from netaudio.dante import flows
from netaudio.dante.const import RESULT_CODE_SUCCESS
from netaudio.dante.device_operations import validate_pin
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.services.notification import mutate_and_wait_for_capability_value

logger = logging.getLogger("netaudio")

RELAY_SERVICE_TYPE = "_netaudio-relay._tcp.local."
DEFAULT_RELAY_PORT = 9000
BONJOUR_MONITOR_INTERVAL_SECONDS = 5
BONJOUR_REFRESH_INTERVAL_SECONDS = 60
BONJOUR_SLEEP_GAP_MULTIPLIER = 3
SSE_CLIENT_QUEUE_SIZE = 128
SSE_DRAIN_TIMEOUT_SECONDS = 5
SSE_CLOSE_TIMEOUT_SECONDS = 1
AUDIO_CAPABILITY_VERIFICATION_TIMEOUT_SECONDS = 2

STATUS_TEXT = {
    200: "OK",
    202: "Accepted",
    400: "Bad Request",
    404: "Not Found",
    409: "Conflict",
    500: "Internal Server Error",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


@dataclass(eq=False)
class _SseClient:
    writer: asyncio.StreamWriter
    queue: asyncio.Queue[bytes] = field(default_factory=lambda: asyncio.Queue(maxsize=SSE_CLIENT_QUEUE_SIZE))
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    sender_task: asyncio.Task | None = None


async def _bounded(awaitable, timeout: float):
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise
    if not done:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise asyncio.TimeoutError
    return task.result()


class RelayServer:
    def __init__(self, application, state, metering=None, shure=None, port=None, on_shutdown=None, mark_offline=None):
        self.application = application
        self.state = state
        self.metering = metering
        self.shure = shure
        self.on_shutdown = on_shutdown
        self.mark_offline = mark_offline or application.mark_device_offline
        self.port: int = int(port if port is not None else DEFAULT_RELAY_PORT)
        self.tcp_server = None
        self.zeroconf = None
        self.service_info = None
        self.sse_clients: dict[asyncio.StreamWriter, _SseClient] = {}
        self._events_registered = False
        self._stop_lock: asyncio.Lock | None = None
        self._bonjour_addresses: tuple[str, ...] = ()
        self._bonjour_monitor_task: asyncio.Task | None = None
        self._bonjour_registered_monotonic: float | None = None
        self._last_bonjour_probe_wall_time: float | None = None
        self.audio_capability_verification_timeout = AUDIO_CAPABILITY_VERIFICATION_TIMEOUT_SECONDS
        self.post_handlers = {
            "/subscribe": self._handle_subscribe,
            "/unsubscribe": self._handle_unsubscribe,
            "/identify": self._handle_identify,
            "/rename-device": self._handle_rename_device,
            "/rename-channel": self._handle_rename_channel,
            "/set-latency": self._handle_set_latency,
            "/lock": self._handle_lock,
            "/unlock": self._handle_unlock,
            "/refresh": self._handle_refresh,
            "/set-sample-rate": self._handle_set_sample_rate,
            "/set-encoding": self._handle_set_encoding,
            "/set-gain": self._handle_set_gain,
            "/set-aes67": self._handle_set_aes67,
            "/set-preferred-leader": self._handle_set_preferred_leader,
            "/reboot": self._handle_reboot,
            "/interface": self._handle_set_interface,
            "/metering/start": self._handle_metering_start,
            "/metering/stop": self._handle_metering_stop,
            "/report-unresponsive": self._handle_report_unresponsive,
            "/flows/create": self._handle_create_tx_flow,
            "/flows/delete": self._handle_delete_tx_flow,
            "/shutdown": self._handle_shutdown,
        }
        self.post_body_optional = {"/refresh", "/shutdown"}
        self.loopback_only_paths = {"/shutdown"}

    async def start(self):
        if self.tcp_server is not None:
            return
        self._stop_lock = asyncio.Lock()
        self.tcp_server = await asyncio.start_server(self.handle_connection, "0.0.0.0", self.port)
        logger.info(f"Relay server listening on port {self.port}")

        try:
            self._register_events()
            await self._reconcile_bonjour(force=True)
            self._bonjour_monitor_task = asyncio.create_task(self._bonjour_monitor_loop())
        except BaseException:
            await self.stop()
            raise

    async def stop(self):
        stop_lock = self._stop_lock
        if stop_lock is None:
            stop_lock = asyncio.Lock()
            self._stop_lock = stop_lock

        async with stop_lock:
            monitor_task = self._bonjour_monitor_task
            self._bonjour_monitor_task = None
            if monitor_task:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    logger.debug("Bonjour monitor stopped")

            await self._close_bonjour()
            self._last_bonjour_probe_wall_time = None

            clients = list(self.sse_clients.values())
            if clients:
                await asyncio.gather(
                    *(self._close_sse_client(client, "relay shutdown") for client in clients),
                    return_exceptions=True,
                )

            server = self.tcp_server
            self.tcp_server = None
            if server:
                server.close()
                try:
                    await _bounded(server.wait_closed(), 5)
                except asyncio.TimeoutError:
                    logger.warning("Relay connections did not drain within 5s, abandoning them")

            self._unregister_events()

    def _register_events(self):
        if self._events_registered:
            return
        dispatcher = self.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.on(EventType.METER_VALUES, self._on_meter_values)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        dispatcher.on(EventType.SHURE_METER_VALUES, self._on_shure_meter)
        self._events_registered = True

    def _unregister_events(self):
        if not self._events_registered:
            return
        dispatcher = self.application.dispatcher
        dispatcher.off(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.off(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.off(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.off(EventType.METER_VALUES, self._on_meter_values)
        dispatcher.off(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_event)
        dispatcher.off(EventType.SHURE_DEVICE_UPDATED, self._on_shure_event)
        dispatcher.off(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        dispatcher.off(EventType.SHURE_METER_VALUES, self._on_shure_meter)
        self._events_registered = False

    async def _on_device_event(self, event: DanteEvent):
        device = self.application.devices.get(event.server_name)
        if not device:
            return

        device_json = DanteDeviceSerializer.to_json(device)

        await self._broadcast_sse(
            {
                "event": event.type.name.lower(),
                "server_name": event.server_name,
                "device": device_json,
            }
        )

    async def _on_device_removed(self, event: DanteEvent):
        await self._broadcast_sse(
            {
                "event": "device_removed",
                "server_name": event.server_name,
            }
        )

    async def _on_meter_values(self, event: DanteEvent):
        await self._broadcast_sse(
            {
                "event": "meter_values",
                "server_name": event.server_name,
                "tx": event.data.get("tx", {}),
                "rx": event.data.get("rx", {}),
            }
        )

    async def _on_shure_event(self, event: DanteEvent):
        if not self.shure:
            return
        device = self.shure.devices.get(event.device_name)
        if not device:
            return
        await self._broadcast_sse(
            {
                "event": event.type.name.lower(),
                "mac": event.device_name,
                "device": device.to_json(),
            }
        )

    async def _on_shure_removed(self, event: DanteEvent):
        await self._broadcast_sse(
            {
                "event": "shure_device_removed",
                "mac": event.device_name,
            }
        )

    async def _on_shure_meter(self, event: DanteEvent):
        await self._broadcast_sse(
            {
                "event": "shure_meter_values",
                "mac": event.device_name,
                "channel": event.data.get("channel"),
                "key": event.data.get("key"),
                "value": event.data.get("value"),
            }
        )

    async def _broadcast_sse(self, data):
        payload = f"data: {json.dumps(data, default=str)}\n\n".encode()
        for client in tuple(self.sse_clients.values()):
            try:
                client.queue.put_nowait(payload)
            except asyncio.QueueFull:
                self._drop_sse_client(client, "outbound event queue full")

    async def _sse_sender(self, client: _SseClient):
        try:
            while True:
                payload = await client.queue.get()
                client.writer.write(payload)
                await _bounded(
                    client.writer.drain(),
                    SSE_DRAIN_TIMEOUT_SECONDS,
                )
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"SSE client writer stopped: {exception}")
        finally:
            self._drop_sse_client(client, "writer stopped", cancel_sender=False)

    def _drop_sse_client(self, client: _SseClient, reason: str, cancel_sender: bool = True):
        if self.sse_clients.get(client.writer) is client:
            self.sse_clients.pop(client.writer, None)
            logger.debug(f"SSE client disconnected: {reason}")
        client.closed.set()
        if (
            cancel_sender
            and client.sender_task
            and client.sender_task is not asyncio.current_task()
            and not client.sender_task.done()
        ):
            client.sender_task.cancel()
        try:
            client.writer.close()
        except Exception as exception:
            logger.debug(f"SSE client close error: {exception}")

    async def _close_sse_client(self, client: _SseClient, reason: str):
        self._drop_sse_client(client, reason)
        task = client.sender_task
        if task and task is not asyncio.current_task():
            done, _ = await asyncio.wait({task}, timeout=SSE_CLOSE_TIMEOUT_SECONDS)
            if not done:
                logger.debug("SSE sender task did not cancel within the close timeout")
        try:
            await _bounded(
                client.writer.wait_closed(),
                SSE_CLOSE_TIMEOUT_SECONDS,
            )
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"SSE client shutdown ended with {exception}")

    async def _bonjour_monitor_loop(self):
        self._last_bonjour_probe_wall_time = time.time()
        while True:
            try:
                await asyncio.sleep(BONJOUR_MONITOR_INTERVAL_SECONDS)
                current_wall_time = time.time()
                elapsed_wall_time = current_wall_time - self._last_bonjour_probe_wall_time
                self._last_bonjour_probe_wall_time = current_wall_time

                await self._reconcile_bonjour(
                    woke_from_sleep=(
                        elapsed_wall_time > BONJOUR_MONITOR_INTERVAL_SECONDS * BONJOUR_SLEEP_GAP_MULTIPLIER
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                logger.warning(f"Relay Bonjour monitor error: {exception}")

    async def _reconcile_bonjour(self, force=False, woke_from_sleep=False):
        current_addresses = self._get_advertisement_addresses()
        if not current_addresses:
            if self.zeroconf or self.service_info:
                logger.info("Relay Bonjour advertisement removed because no non-loopback IPv4 addresses are available")
                await self._close_bonjour()
            return

        refresh_reason = None
        recreate_service = False

        if force:
            refresh_reason = "startup"
            recreate_service = True
        elif not self.zeroconf or not self.service_info:
            refresh_reason = "registration missing"
            recreate_service = True
        elif current_addresses != self._bonjour_addresses:
            previous_addresses = ", ".join(self._bonjour_addresses) or "none"
            refresh_reason = f"address change: {previous_addresses} -> {', '.join(current_addresses)}"
            recreate_service = True
        elif woke_from_sleep:
            refresh_reason = "wake from sleep"
            recreate_service = True
        elif (
            self._bonjour_registered_monotonic is None
            or time.monotonic() - self._bonjour_registered_monotonic >= BONJOUR_REFRESH_INTERVAL_SECONDS
        ):
            refresh_reason = "periodic refresh"

        if not refresh_reason:
            return

        await self._publish_bonjour(
            current_addresses,
            reason=refresh_reason,
            recreate_service=recreate_service,
        )

    async def _publish_bonjour(self, addresses, reason, recreate_service):
        registered_name = self.service_info.name if self.service_info else None
        service_info = self._build_service_info(addresses, name=registered_name)

        if not recreate_service and self.zeroconf and self.service_info:
            try:
                await self.zeroconf.async_update_service(service_info)
                self.service_info = service_info
                self._bonjour_addresses = addresses
                self._bonjour_registered_monotonic = time.monotonic()
                logger.info(f"Relay Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")
                return
            except Exception as exception:
                logger.warning(f"Relay Bonjour update failed ({reason}); recreating advertisement: {exception}")

        await self._close_bonjour()

        zeroconf = AsyncZeroconf(
            interfaces=list(addresses),
            ip_version=IPVersion.V4Only,
        )
        try:
            await zeroconf.async_register_service(service_info, allow_name_change=True)
        except asyncio.CancelledError:
            await self._dispose_bonjour(zeroconf, service_info)
            raise
        except Exception as exception:
            await self._dispose_bonjour(zeroconf, service_info)
            logger.warning(f"Relay Bonjour advertisement failed ({reason}): {exception}")
            return

        self.zeroconf = zeroconf
        self.service_info = service_info
        self._bonjour_addresses = addresses
        self._bonjour_registered_monotonic = time.monotonic()
        logger.info(f"Relay Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")

    async def _dispose_bonjour(self, zeroconf, service_info):
        if service_info:
            try:
                await _bounded(zeroconf.async_unregister_service(service_info), 5)
            except Exception as exception:
                logger.debug(f"Relay Bonjour unregister failed: {exception}")

        try:
            await _bounded(zeroconf.async_close(), 5)
        except Exception as exception:
            logger.debug(f"Relay Bonjour close failed: {exception}")

    async def _close_bonjour(self):
        zeroconf = self.zeroconf
        service_info = self.service_info

        self.zeroconf = None
        self.service_info = None
        self._bonjour_addresses = ()
        self._bonjour_registered_monotonic = None

        if zeroconf:
            cleanup_task = asyncio.create_task(self._dispose_bonjour(zeroconf, service_info))
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise

    def _build_service_info(self, addresses, name=None):
        hostname = socket.gethostname().removesuffix(".local")
        return ServiceInfo(
            RELAY_SERVICE_TYPE,
            name or f"netaudio-relay ({hostname}).{RELAY_SERVICE_TYPE}",
            addresses=[socket.inet_aton(address) for address in addresses],
            port=self.port,
            properties={"version": "1"},
            server=f"{hostname}.local.",
        )

    def _get_advertisement_addresses(self):
        selected_interface = app_settings.interface
        addresses = set()
        for adapter in ifaddr.get_adapters():
            if selected_interface and adapter.nice_name != selected_interface:
                continue
            for adapter_ip in adapter.ips:
                address = adapter_ip.ip
                if not isinstance(address, str):
                    continue
                try:
                    parsed = ipaddress.IPv4Address(address)
                except ipaddress.AddressValueError:
                    continue
                if parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast:
                    continue
                addresses.add(str(parsed))

        return tuple(sorted(addresses))

    async def handle_connection(self, reader, writer):
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw:
                return

            request = raw.decode().strip()
            parts = request.split(" ", 2)
            if len(parts) < 2:
                await self._send_json(writer, {"error": "bad request"}, 400)
                return

            method = parts[0]
            path = parts[1]

            headers = {}
            while True:
                header_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if header_line in (b"\r\n", b"\n", b""):
                    break
                decoded = header_line.decode().strip()
                if ":" in decoded:
                    key, value = decoded.split(":", 1)
                    headers[key.strip().lower()] = value.strip()

            body = None
            if method == "POST":
                content_length = int(headers.get("content-length", "0"))
                if content_length > 0:
                    body = await asyncio.wait_for(reader.readexactly(content_length), timeout=5.0)

            await self._route(method, path, body, writer, reader)

        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError) as exception:
            logger.debug(f"Relay peer disconnected: {exception}")
        except Exception as exception:
            logger.debug(f"Relay connection error: {exception}")

    async def _route(self, method, path, body, writer, reader):
        if method == "GET" and path == "/events":
            await self._handle_sse(writer, reader)
            return

        try:
            await self._dispatch(method, path, body, writer)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except Exception as exception:
            logger.exception(f"Relay error handling {method} {path}")
            await self._send_json(writer, {"error": str(exception)}, 500)

        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"Relay writer close ended with {exception}")

    async def _dispatch(self, method, path, body, writer):
        if method == "GET":
            if path == "/shure/devices":
                await self._handle_get_shure_devices(writer)
            elif path.startswith("/shure/devices/"):
                await self._handle_get_shure_device(writer, path[len("/shure/devices/") :])
            elif path == "/devices":
                await self._handle_get_devices(writer)
            elif path.startswith("/devices/"):
                await self._handle_get_device(writer, unquote(path[len("/devices/") :]))
            elif path.startswith("/flows/"):
                await self._handle_get_tx_flows(writer, unquote(path[len("/flows/") :]))
            elif path == "/metering/status":
                await self._handle_metering_status(writer)
            elif path.startswith("/metering/snapshot/"):
                await self._handle_metering_snapshot(writer, unquote(path[len("/metering/snapshot/") :]))
            else:
                await self._send_json(writer, {"error": "not found"}, 404)
            return

        if method != "POST":
            await self._send_json(writer, {"error": "not found"}, 404)
            return

        handler = self.post_handlers.get(path)
        if not handler:
            await self._send_json(writer, {"error": "not found"}, 404)
            return

        if path in self.loopback_only_paths and not self._peer_is_loopback(writer):
            await self._send_json(writer, {"error": "forbidden"}, 403)
            return

        params = {}
        if body:
            try:
                params = json.loads(body)
            except json.JSONDecodeError as exception:
                await self._send_json(writer, {"error": f"invalid json: {exception}"}, 400)
                return
            if not isinstance(params, dict):
                await self._send_json(writer, {"error": "body must be a json object"}, 400)
                return
        elif path not in self.post_body_optional:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        await handler(writer, params)

    async def _handle_sse(self, writer, reader):
        client = None
        try:
            full_state = {
                server_name: DanteDeviceSerializer.to_json(device)
                for server_name, device in self.application.devices.items()
            }
            shure_state = {}
            if self.shure:
                shure_state = {mac: device.to_json() for mac, device in self.shure.devices.items()}

            initial = (
                f"data: {json.dumps({'event': 'snapshot', 'devices': full_state, 'shure_devices': shure_state}, default=str)}\n\n"
            ).encode()

            client = _SseClient(writer=writer)
            client.queue.put_nowait(initial)
            self.sse_clients[writer] = client

            response_header = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "\r\n"
            ).encode()
            writer.write(response_header)
            await _bounded(writer.drain(), SSE_DRAIN_TIMEOUT_SECONDS)

            if client.closed.is_set():
                return
            client.sender_task = asyncio.create_task(self._sse_sender(client))

            while True:
                read_task = asyncio.create_task(reader.read(1))
                closed_task = asyncio.create_task(client.closed.wait())
                try:
                    done, _ = await asyncio.wait(
                        (read_task, closed_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    pending = [task for task in (read_task, closed_task) if not task.done()]
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.wait(pending, timeout=SSE_CLOSE_TIMEOUT_SECONDS)
                if closed_task in done or not read_task.result():
                    break
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exception:
            logger.debug(f"SSE connection ended with {exception}")
        finally:
            if client is not None:
                await self._close_sse_client(client, "peer disconnected")
            else:
                try:
                    writer.close()
                    await _bounded(writer.wait_closed(), SSE_CLOSE_TIMEOUT_SECONDS)
                except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
                    logger.debug(f"SSE writer close ended with {exception}")

    async def _handle_get_shure_devices(self, writer):
        if not self.shure:
            await self._send_json(writer, {})
            return
        result = {}
        for mac, device in self.shure.devices.items():
            result[mac] = device.to_json()
        await self._send_json(writer, result)

    async def _handle_get_shure_device(self, writer, mac):
        if not self.shure:
            await self._send_json(writer, {"error": "shure not available"}, 404)
            return
        device = self.shure.devices.get(mac)
        if not device:
            for m, d in self.shure.devices.items():
                if d.name and d.name.lower() == mac.lower():
                    device = d
                    break
                if d.ip == mac:
                    device = d
                    break
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        await self._send_json(writer, device.to_json())

    async def _handle_get_devices(self, writer):
        devices_json = {
            server_name: DanteDeviceSerializer.to_json(device)
            for server_name, device in self.application.devices.items()
        }
        await self._send_json(writer, devices_json)

    async def _handle_get_device(self, writer, server_name):
        device = self.application.devices.get(server_name)
        if not device:
            for name, candidate in self.application.devices.items():
                if candidate.name and candidate.name.lower() == server_name.lower():
                    device = candidate
                    break

        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        await self._send_json(writer, DanteDeviceSerializer.to_json(device))

    async def _handle_subscribe(self, writer, params):
        rx_device_name = params.get("rx_device")
        device = await self._require_device(writer, rx_device_name, "rx device not found")
        if not device:
            return

        subscriptions = params.get("subscriptions")
        if subscriptions is not None:
            try:
                records = [(entry["rx_channel"], entry["tx_channel"], entry["tx_device"]) for entry in subscriptions]
            except (KeyError, TypeError) as exception:
                await self._send_json(writer, {"error": f"invalid subscription entry: {exception}"}, 400)
                return
            if not records:
                await self._send_json(writer, {"error": "subscriptions list is empty"}, 400)
                return

            for rx_channel_number, tx_channel_name, tx_device_name in records:
                await self._broadcast_sse(
                    {
                        "event": "subscription_pending",
                        "action": "add",
                        "rx_device": rx_device_name,
                        "rx_channel": rx_channel_number,
                        "tx_channel": tx_channel_name,
                        "tx_device": tx_device_name,
                    }
                )

            response = await device.operations.add_subscriptions_by_name(records)
            if not await self._require_arc_write_success(writer, response, "subscription change"):
                return
            await self._send_json(writer, {"success": True, "count": len(records)})
            return

        rx_channel_number = params.get("rx_channel")
        tx_channel_name = params.get("tx_channel")
        tx_device_name = params.get("tx_device")
        if rx_channel_number is None or not tx_channel_name or not tx_device_name:
            await self._send_json(writer, {"error": "rx_channel, tx_channel, tx_device required"}, 400)
            return

        await self._broadcast_sse(
            {
                "event": "subscription_pending",
                "action": "add",
                "rx_device": rx_device_name,
                "rx_channel": rx_channel_number,
                "tx_channel": tx_channel_name,
                "tx_device": tx_device_name,
            }
        )

        response = await device.operations.add_subscription_by_name(
            rx_channel_number,
            tx_channel_name,
            tx_device_name,
        )
        if not await self._require_arc_write_success(writer, response, "subscription change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_unsubscribe(self, writer, params):
        device = await self._require_device(writer, params.get("rx_device"), "rx device not found")
        if not device:
            return

        rx_channel_numbers = params.get("rx_channels")
        if rx_channel_numbers:
            rx_channels = []
            for number in rx_channel_numbers:
                channel = device.rx_channels.get(number)
                if not channel:
                    await self._send_json(writer, {"error": f"rx channel {number} not found"}, 404)
                    return
                rx_channels.append(channel)

            response = await device.operations.remove_subscriptions(rx_channels)
            if not await self._require_arc_write_success(writer, response, "subscription removal"):
                return
            await self._send_json(writer, {"success": True, "count": len(rx_channels)})
            return

        rx_channel = device.rx_channels.get(params.get("rx_channel"))
        if not rx_channel:
            await self._send_json(writer, {"error": "rx channel not found"}, 404)
            return

        response = await device.operations.remove_subscription(rx_channel)
        if not await self._require_arc_write_success(writer, response, "subscription removal"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_identify(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await device.operations.identify()
        await self._broadcast_sse(
            {
                "event": "identify_started",
                "server_name": device.server_name,
                "duration": 6,
            }
        )
        await self._send_json(writer, {"accepted": True, "verified": False}, 202)

    async def _handle_rename_device(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        name = params.get("name")
        if not isinstance(name, str):
            await self._send_json(writer, {"error": "name must be a string"}, 400)
            return
        if name.strip():
            response = await device.operations.set_name(name)
        else:
            response = await device.operations.reset_name()
        if not await self._require_arc_write_success(writer, response, "device name change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_rename_channel(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        name = params.get("name")
        if not isinstance(name, str):
            await self._send_json(writer, {"error": "name must be a string"}, 400)
            return
        channel_type = params.get("channel_type")
        channel_number = params.get("channel_number")
        if name.strip():
            response = await device.operations.set_channel_name(channel_type, channel_number, name)
        else:
            response = await device.operations.reset_channel_name(channel_type, channel_number)
        if not await self._require_arc_write_success(writer, response, "channel name change"):
            return
        await self._send_json(writer, {"success": True})

    async def _require_arc_write_success(self, writer, response, operation):
        if not response:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return False
        try:
            from netaudio import core

            result_code = core.parse_response("result_code", response)
        except Exception as exception:
            await self._send_json(writer, {"error": f"invalid device response: {exception}"}, 500)
            return False
        if not isinstance(result_code, int):
            await self._send_json(writer, {"error": "invalid device response: missing result code"}, 500)
            return False
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected {operation} with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return False
        return True

    async def _handle_set_latency(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        response = await device.operations.set_latency(params.get("latency"))
        if not await self._require_arc_write_success(writer, response, "latency change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_lock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=True)

    async def _handle_unlock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=False)

    async def _handle_lock_operation(self, writer, params, locking):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        lock_key = self._get_lock_key()
        if not lock_key:
            await self._send_json(writer, {"error": "device_lock_key not configured"}, 503)
            return

        pin = params.get("pin")
        error = validate_pin(pin or "")
        if error:
            await self._send_json(writer, {"error": error}, 400)
            return

        if locking:
            result = await device.operations.lock_device(pin, lock_key)
        else:
            result = await device.operations.unlock_device(pin, lock_key)

        if not isinstance(result, dict):
            await self._send_json(writer, {"error": "invalid device response"}, 500)
            return
        if result.get("success") is not True:
            await self._send_json(writer, result, 409)
            return
        await self._send_json(writer, result)

    def _get_lock_key(self):
        from netaudio.common.app_config import settings as app_settings

        if app_settings.device_lock_key:
            return app_settings.device_lock_key
        from netaudio.common.key_extract import extract_lock_key

        key = extract_lock_key()
        if key:
            app_settings.device_lock_key = key
            logger.info("Extracted device lock key from Dante Controller")
        return key

    async def _handle_refresh(self, writer, params):
        device_name = params.get("device")
        if device_name:
            device = await self._require_device(writer, device_name)
            if not device:
                return
            await self.state.refresh_device(device.server_name)
        else:
            await self.state.refresh_all_devices()
        await self._send_json(writer, {"success": True})

    @staticmethod
    def _peer_is_loopback(writer):
        peername = writer.get_extra_info("peername")
        if not peername:
            return False
        return peername[0] in ("127.0.0.1", "::1", "::ffff:127.0.0.1")

    async def _handle_shutdown(self, writer, params):
        await self._send_json(writer, {"success": True})
        if self.on_shutdown is not None:
            self.on_shutdown()

    async def _handle_report_unresponsive(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if device.online:
            logger.info(f"Device reported unresponsive, marking offline candidate: {device.server_name}")
            self.mark_offline(device.server_name)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_status(self, writer):
        if not self.metering:
            await self._send_json(writer, {})
            return
        await self._send_json(writer, self.metering.get_status())

    async def _handle_metering_snapshot(self, writer, name):
        device = self._find_device(name)
        if not device or not device.ipv4:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return

        levels = await self.metering.snapshot(device.server_name, timeout=3.0)
        if levels is None:
            await self._send_json(writer, {"error": "no metering data"}, 504)
            return

        tx_names = {}
        if device.tx_channels:
            for channel in device.tx_channels.values():
                tx_names[channel.number] = channel.friendly_name or channel.name
        rx_names = {}
        if device.rx_channels:
            for channel in device.rx_channels.values():
                rx_names[channel.number] = channel.friendly_name or channel.name

        response = {
            "tx": {},
            "rx": {},
            "wall_time": levels.get("wall_time"),
            "source_ip": levels.get("source_ip"),
        }
        for channel_number, level in levels.get("tx", {}).items():
            response["tx"][channel_number] = {
                "name": tx_names.get(channel_number, ""),
                "level": level,
            }
        for channel_number, level in levels.get("rx", {}).items():
            response["rx"][channel_number] = {
                "name": rx_names.get(channel_number, ""),
                "level": level,
            }
        await self._send_json(writer, response)

    async def _handle_metering_start(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        client_id = params.get("client_id", "relay_http")
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return
        self.metering.add_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_stop(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        client_id = params.get("client_id", "relay_http")
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return
        self.metering.remove_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_set_sample_rate(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        requested_sample_rate = params.get("sample_rate")
        if not await self._require_audio_capability_value(writer, requested_sample_rate, "sample_rate"):
            return
        await self._set_and_verify_audio_capability(
            writer,
            device,
            requested_sample_rate,
            device.operations.set_sample_rate,
            self.application.probe_sample_rate_status,
            "sample_rate",
            "supported_sample_rates",
            "sample rate",
        )

    async def _handle_set_encoding(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        requested_encoding = params.get("encoding")
        if not await self._require_audio_capability_value(writer, requested_encoding, "encoding"):
            return
        await self._set_and_verify_audio_capability(
            writer,
            device,
            requested_encoding,
            device.operations.set_encoding,
            self.application.probe_encoding_status,
            "encoding",
            "supported_encodings",
            "encoding",
        )

    async def _require_audio_capability_value(self, writer, value, field_name):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 0xFFFFFFFF:
            await self._send_json(writer, {"error": f"{field_name} must be an integer from 1 through 4294967295"}, 400)
            return False
        return True

    async def _set_and_verify_audio_capability(
        self,
        writer,
        device,
        requested_value,
        set_value,
        probe_status,
        current_value_field,
        supported_values_field,
        capability_description,
    ):
        device_ip_address = str(device.ipv4)

        async def mutate() -> None:
            await set_value(requested_value)

        async def probe():
            return await probe_status(device_ip_address)

        try:
            status = await mutate_and_wait_for_capability_value(
                self.application.notifications,
                current_value_field,
                device_ip_address,
                requested_value,
                mutate,
                probe,
                self.audio_capability_verification_timeout,
            )
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return

        if status is None:
            await self._send_json(writer, {"error": f"{capability_description} readback was unavailable"}, 504)
            return

        observed_value, supported_values = status
        setattr(device, current_value_field, observed_value)
        setattr(device, supported_values_field, supported_values)
        if observed_value != requested_value:
            await self._send_json(
                writer,
                {
                    "error": f"{capability_description} change was not applied",
                    "observed": observed_value,
                    "supported": supported_values,
                },
                409,
            )
            return

        await self._send_json(writer, {"success": True})

    async def _handle_set_gain(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        channel_number = params.get("channel_number")
        gain_level = params.get("gain_level")
        device_type = params.get("device_type", "")
        try:
            status = await device.operations.set_gain_level(channel_number, gain_level, device_type)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if status is None:
            await self._send_json(writer, {"error": "gain readback was unavailable"}, 504)
            return
        observed_device_type, channel_levels = status
        channel_index = channel_number - 1
        observed_level = channel_levels[channel_index] if 0 <= channel_index < len(channel_levels) else None
        if observed_device_type != device_type or observed_level != gain_level:
            await self._send_json(
                writer,
                {
                    "error": "gain change was not applied",
                    "observed_device_type": observed_device_type,
                    "observed_level": observed_level,
                },
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_set_preferred_leader(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        expected = params.get("preferred")
        if not isinstance(expected, bool):
            await self._send_json(writer, {"error": "preferred must be a boolean"}, 400)
            return
        observed = await self.application.set_preferred_leader_state(str(device.ipv4), expected)
        if observed is None:
            await self._send_json(writer, {"error": "preferred leader readback was unavailable"}, 504)
            return
        if observed != expected:
            await self._send_json(
                writer,
                {"error": "preferred leader change was not applied", "observed": observed},
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_set_aes67(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        expected = params.get("enabled")
        if not isinstance(expected, bool):
            await self._send_json(writer, {"error": "enabled must be a boolean"}, 400)
            return
        result = await self.application.set_aes67_state(device, expected)
        configured = result[1] if result is not None else None
        if configured is None:
            await self._send_json(writer, {"error": "AES67 readback was unavailable"}, 504)
            return
        if configured != expected:
            await self._send_json(
                writer,
                {"error": "AES67 change was not applied", "observed": configured},
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_reboot(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.reboot()
        await self._send_json(writer, {"accepted": True, "verified": False}, 202)

    async def _handle_set_interface(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        mode = params.get("mode", "").lower()
        if mode not in ("dhcp", "static"):
            await self._send_json(writer, {"error": "mode must be 'dhcp' or 'static'"}, 400)
            return

        device_ip = str(device.ipv4)
        ip_address = ""
        netmask = ""
        if mode == "dhcp":
            result = await self.application.set_interface_dhcp(device_ip)
        else:
            ip_address = params.get("ip")
            netmask = params.get("netmask")
            if not isinstance(ip_address, str) or not ip_address or not isinstance(netmask, str) or not netmask:
                await self._send_json(writer, {"error": "static mode requires ip, netmask"}, 400)
                return
            result = await self.application.set_interface_static(
                device_ip, ip_address, netmask, params.get("dns") or "", params.get("gateway") or ""
            )
        if result is None:
            await self._send_json(writer, {"error": "interface readback was unavailable"}, 504)
            return
        expected_mode = "dynamic" if mode == "dhcp" else "static"
        candidates = list(result)
        pending_config = device.interface_pending_config
        if isinstance(pending_config, dict):
            candidates.append(pending_config)
        expected_fields = {"mode": expected_mode}
        if mode == "static":
            expected_fields.update(
                {
                    "ip_address": ip_address,
                    "netmask": netmask,
                    "dns_server": params.get("dns") or "",
                    "gateway": params.get("gateway") or "",
                }
            )
        matched = any(
            all(candidate.get(key) == value for key, value in expected_fields.items()) for candidate in candidates
        )
        if not matched:
            await self._send_json(
                writer,
                {"error": "interface change was not applied", "interfaces": result},
                409,
            )
            return
        await self._send_json(
            writer,
            {
                "success": True,
                "reboot_required": device.interface_reboot_required,
                "interfaces": result,
            },
        )

    async def _handle_get_tx_flows(self, writer, device_name):
        snapshot = await self._tx_flow_snapshot(writer, device_name)
        if snapshot is None:
            return

        device, flow_protocol_id, device_flows = snapshot
        await self._send_json(
            writer,
            {
                "device": device.server_name,
                "flow_protocol_id": flow_protocol_id,
                "flows": device_flows,
            },
        )

    async def _handle_create_tx_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return

        try:
            flow_slot = flows.validate_flow_slot(params.get("flow_slot"))
            channel_numbers = flows.validate_flow_channels(params.get("channels"))
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        device_name = params.get("device")
        snapshot = await self._tx_flow_snapshot(writer, device_name)
        if snapshot is None:
            return
        device, flow_protocol_id, device_flows = snapshot

        available_channels = {int(number) for number in (device.tx_channels or {}).keys()}
        try:
            flows.require_available_tx_channels(channel_numbers, available_channels)
            flows.require_available_flow_slot(device_flows, flow_slot)
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        result_code = await flows.create_tx_flow(
            str(device.ipv4), self._flow_arc_port(device), flow_protocol_id, flow_slot, channel_numbers
        )
        if result_code is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected flow creation with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return

        await self._send_json(
            writer,
            {
                "success": True,
                "flow_protocol_id": flow_protocol_id,
                "flow_slot": flow_slot,
                "channels": channel_numbers,
            },
        )

    async def _handle_delete_tx_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return

        try:
            flow_slot = flows.validate_flow_slot(params.get("flow_slot"))
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        snapshot = await self._tx_flow_snapshot(writer, params.get("device"))
        if snapshot is None:
            return
        device, flow_protocol_id, device_flows = snapshot

        try:
            flows.require_multicast_flow(device_flows, flow_slot)
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        result_code = await flows.delete_tx_flow(
            str(device.ipv4), self._flow_arc_port(device), flow_protocol_id, flow_slot
        )
        if result_code is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected flow deletion with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return

        await self._send_json(
            writer,
            {
                "success": True,
                "flow_protocol_id": flow_protocol_id,
                "flow_slot": flow_slot,
            },
        )

    async def _tx_flow_snapshot(self, writer, device_name):
        device = await self._require_device(writer, device_name)
        if not device:
            return None
        if not device.online:
            await self._send_json(writer, {"error": "device is offline"}, 503)
            return None
        if not device.ipv4:
            await self._send_json(writer, {"error": "device has no control address"}, 503)
            return None

        flow_protocol_id = device.flow_protocol_id
        if flow_protocol_id is None:
            flow_protocol_id = await flows.detect_flow_protocol(str(device.ipv4), self._flow_arc_port(device))
            if flow_protocol_id is None:
                await self._send_json(writer, {"error": "flow protocol is not supported or did not respond"}, 503)
                return None
            device.flow_protocol_id = flow_protocol_id

        device_flows = await flows.query_tx_flows(str(device.ipv4), self._flow_arc_port(device), flow_protocol_id)
        if device_flows is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return None
        return device, flow_protocol_id, device_flows

    @staticmethod
    def _flow_arc_port(device) -> int:
        return device._arc_port()

    async def _require_device(self, writer, name, error="device not found"):
        device = self._find_device(name)
        if not device:
            await self._send_json(writer, {"error": error}, 404)
        return device

    def _find_device(self, name):
        if not name:
            return None
        device = self.application.devices.get(name)
        if device:
            return device
        for server_name, candidate in self.application.devices.items():
            if candidate.name and candidate.name.lower() == name.lower():
                return candidate
            if candidate.ipv4 and str(candidate.ipv4) == name:
                return candidate
        return None

    async def _send_json(self, writer, data, status=200):
        body = json.dumps(data, default=str).encode()
        status_text = STATUS_TEXT.get(status, "Error")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
        ).encode() + body
        writer.write(response)
        await writer.drain()
