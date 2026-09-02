from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import socket
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote

import ifaddr
from zeroconf import Error as ZeroconfError
from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio.common.app_config import DEFAULT_DAEMON_PORT
from netaudio.common.app_config import settings as app_settings
from netaudio.daemon.http.configuration import DaemonConfigurationHandlers
from netaudio.daemon.http.devices import DaemonDeviceHandlers
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

DAEMON_SERVICE_TYPE = "_netaudio-relay._tcp.local."
BONJOUR_MONITOR_INTERVAL_SECONDS = 5
BONJOUR_REFRESH_INTERVAL_SECONDS = 60
BONJOUR_SLEEP_GAP_MULTIPLIER = 3
SSE_CLIENT_QUEUE_SIZE = 128
SSE_DRAIN_TIMEOUT_SECONDS = 5
SSE_CLOSE_TIMEOUT_SECONDS = 1
AUDIO_CAPABILITY_VERIFICATION_TIMEOUT_SECONDS = 2
SERVICE_LABEL_MAXIMUM_BYTES = 63
DAEMON_SERVICE_INSTANCE_PREFIX = "netaudio-daemon ("
DAEMON_SERVICE_INSTANCE_SUFFIX = ")"


def _bounded_service_label(value: str, maximum_bytes: int) -> str:
    encoded_value = value.encode("utf-8")
    if len(encoded_value) <= maximum_bytes:
        return value
    digest_suffix = f"-{hashlib.sha256(encoded_value).hexdigest()[:12]}"
    prefix_byte_count = maximum_bytes - len(digest_suffix.encode("ascii"))
    if prefix_byte_count <= 0:
        raise ValueError("Service label limit is too small for a stable identity suffix")
    bounded_prefix = encoded_value[:prefix_byte_count].decode("utf-8", errors="ignore")
    return f"{bounded_prefix}{digest_suffix}"


def _daemon_service_instance_label(hostname: str) -> str:
    hostname_byte_limit = SERVICE_LABEL_MAXIMUM_BYTES - len(
        f"{DAEMON_SERVICE_INSTANCE_PREFIX}{DAEMON_SERVICE_INSTANCE_SUFFIX}".encode("ascii")
    )
    bounded_hostname = _bounded_service_label(hostname, hostname_byte_limit)
    return f"{DAEMON_SERVICE_INSTANCE_PREFIX}{bounded_hostname}{DAEMON_SERVICE_INSTANCE_SUFFIX}"


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


class DaemonHTTPServer(DaemonDeviceHandlers, DaemonConfigurationHandlers):
    def __init__(
        self,
        application,
        state,
        metering=None,
        shure=None,
        port=None,
        on_shutdown=None,
        mark_offline=None,
        forget_device=None,
    ):
        self.application = application
        self.state = state
        self.metering = metering
        self.shure = shure
        self.on_shutdown = on_shutdown
        self.mark_offline = mark_offline or application.mark_device_offline
        self.forget_device = forget_device or application.unregister_device
        self.port: int = int(port if port is not None else DEFAULT_DAEMON_PORT)
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
        self._device_lock_operation_locks: dict[str, asyncio.Lock] = {}
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
            "/set-aes67-multicast-prefix": self._handle_set_aes67_multicast_prefix,
            "/set-sample-rate-pullup": self._handle_set_sample_rate_pullup,
            "/set-preferred-leader": self._handle_set_preferred_leader,
            "/set-clock-source": self._handle_set_clock_source,
            "/set-clock-subdomain": self._handle_set_clock_subdomain,
            "/refresh-clock": self._handle_refresh_clock,
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
        logger.info(f"Daemon HTTP API listening on port {self.port}")

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
                    *(self._close_sse_client(client, "daemon shutdown") for client in clients),
                    return_exceptions=True,
                )

            server = self.tcp_server
            self.tcp_server = None
            if server:
                server.close()
                try:
                    await _bounded(server.wait_closed(), 5)
                except asyncio.TimeoutError:
                    logger.warning("Daemon HTTP API connections did not drain within 5s, abandoning them")

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
                "metering_source": event.data.get("metering_source"),
                "wall_time": event.data.get("wall_time"),
                "source_ip": event.data.get("source_ip"),
                "source_port": event.data.get("source_port"),
                "tx_signal_presence": event.data.get("tx_signal_presence", {}),
                "rx_signal_presence": event.data.get("rx_signal_presence", {}),
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
            logger.warning(f"SSE client writer stopped: {exception}")
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
        except (OSError, RuntimeError) as exception:
            logger.warning(f"SSE client close error: {exception}", exc_info=True)

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
            logger.warning(f"SSE client shutdown ended with {exception}")

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
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour monitor error: {exception}")

    async def _reconcile_bonjour(self, force=False, woke_from_sleep=False):
        current_addresses = self._get_advertisement_addresses()
        if not current_addresses:
            if self.zeroconf or self.service_info:
                logger.info("Daemon Bonjour advertisement removed because no non-loopback IPv4 addresses are available")
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
                logger.info(f"Daemon Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")
                return
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour update failed ({reason}); recreating advertisement: {exception}")

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
        except (OSError, RuntimeError, ZeroconfError) as exception:
            await self._dispose_bonjour(zeroconf, service_info)
            logger.warning(f"Daemon Bonjour advertisement failed ({reason}): {exception}")
            return

        self.zeroconf = zeroconf
        self.service_info = service_info
        self._bonjour_addresses = addresses
        self._bonjour_registered_monotonic = time.monotonic()
        logger.info(f"Daemon Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")

    async def _dispose_bonjour(self, zeroconf, service_info):
        if service_info:
            try:
                await _bounded(zeroconf.async_unregister_service(service_info), 5)
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour unregister failed: {exception}", exc_info=True)

        try:
            await _bounded(zeroconf.async_close(), 5)
        except (OSError, RuntimeError, ZeroconfError) as exception:
            logger.warning(f"Daemon Bonjour close failed: {exception}", exc_info=True)

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
        server_hostname = _bounded_service_label(hostname, SERVICE_LABEL_MAXIMUM_BYTES)
        return ServiceInfo(
            DAEMON_SERVICE_TYPE,
            name or f"{_daemon_service_instance_label(hostname)}.{DAEMON_SERVICE_TYPE}",
            addresses=[socket.inet_aton(address) for address in addresses],
            port=self.port,
            properties={"version": "1"},
            server=f"{server_hostname}.local.",
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
            logger.warning(f"Daemon HTTP API peer disconnected: {exception}")
        except Exception:
            logger.warning("Daemon HTTP API connection error", exc_info=True)

    async def _route(self, method, path, body, writer, reader):
        if method == "GET" and path == "/events":
            await self._handle_sse(writer, reader)
            return

        try:
            await self._dispatch(method, path, body, writer)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except Exception as exception:
            logger.exception(f"Daemon HTTP API error handling {method} {path}")
            await self._send_json(writer, {"error": str(exception)}, 500)

        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.warning(f"Daemon HTTP API writer close ended with {exception}")

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
            elif path.startswith("/interfaces/"):
                await self._handle_get_interfaces(writer, unquote(path[len("/interfaces/") :]))
            elif path.startswith("/lock-status/"):
                await self._handle_get_lock_status(writer, unquote(path[len("/lock-status/") :]))
            elif path.startswith("/flows/"):
                await self._handle_get_tx_flows(writer, unquote(path[len("/flows/") :]))
            elif path == "/metering/status":
                await self._handle_metering_status(writer)
            elif path == "/metering/cache":
                await self._handle_metering_cache(writer)
            elif path.startswith("/metering/snapshot/"):
                await self._handle_metering_snapshot(writer, unquote(path[len("/metering/snapshot/") :]))
            else:
                await self._send_json(writer, {"error": "not found"}, 404)
            return

        if method == "DELETE":
            route, _, query = path.partition("?")
            if route == "/devices":
                await self._handle_forget_devices(writer, parse_qs(query))
            elif route.startswith("/devices/"):
                await self._handle_forget_device(writer, unquote(route[len("/devices/") :]))
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
            metering_state = self.metering.get_cached_levels_by_server() if self.metering else {}

            initial = (
                f"data: {json.dumps({'event': 'snapshot', 'devices': full_state, 'shure_devices': shure_state, 'metering': metering_state}, default=str)}\n\n"
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
            logger.warning(f"SSE connection ended with {exception}")
        finally:
            if client is not None:
                await self._close_sse_client(client, "peer disconnected")
            else:
                try:
                    writer.close()
                    await _bounded(writer.wait_closed(), SSE_CLOSE_TIMEOUT_SECONDS)
                except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
                    logger.warning(f"SSE writer close ended with {exception}")
